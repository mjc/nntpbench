//! Accumulated input and its request decoder have one owner. Only append reads
//! may change input while decoding; completed prefixes leave as immutable bytes.

use bytes::{Bytes, BytesMut};
use std::ops::Range;
use tokio::io::AsyncRead;

use super::{
    Article, ArticleParseError, ArticleState, ContentEnd, FramedArticleState, RequestKind,
    ResponseFrameDecoder, ResponseFrameParse, ResponseInitial, ResponseInitialParse, StatusCode,
    StatusLineEnd, ValidatedOwnedArticle, ValidatedResponseContent,
};
use crate::client::{ClientError, OWNED_RESPONSE_PREALLOC_BYTES, read_into_pending_bytes};
use crate::terminator::{MultilineFrameProgress, MultilineFramer};

const STREAMING_STATUS_LINE_BYTES: usize = super::MAX_AUTHINFO_SASL_RESPONSE_LINE_BYTES;

pub(crate) struct BufferedResponseReceiver<R> {
    reader: R,
    pending: BytesMut,
    state: ReceiverState,
}

enum ReceiverState {
    Ready,
    Unavailable,
}

impl<R: AsyncRead + Unpin> BufferedResponseReceiver<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            pending: BytesMut::with_capacity(OWNED_RESPONSE_PREALLOC_BYTES),
            state: ReceiverState::Ready,
        }
    }

    pub(crate) async fn receive(
        &mut self,
        kind: RequestKind,
        read_chunk_bytes: usize,
    ) -> Result<OwnedResponse, ClientError> {
        self.start_response()?;
        let mut response = ReceivingResponse::new(Receiving {
            receiver: self,
            decoder: ResponseDecoder::new(kind),
        });
        loop {
            if let Some(completed) = response.extract()? {
                return Ok(completed);
            }
            let receiving = response.as_inner_mut();
            if read_into_pending_bytes(
                &mut receiving.receiver.reader,
                &mut receiving.receiver.pending,
                read_chunk_bytes,
            )
            .await?
                == 0
            {
                return Err(ClientError::UnexpectedEof);
            }
        }
    }

    fn start_response(&mut self) -> Result<(), ClientError> {
        match self.state {
            ReceiverState::Ready => self.state = ReceiverState::Unavailable,
            ReceiverState::Unavailable => return Err(ClientError::ConnectionClosed),
        }
        Ok(())
    }
}

/// Receiving state keeps accumulated input and its request decoder together.
///
/// The borrow prevents either resource from being replaced while framing is
/// in progress; completion moves the state into the framed owner below.
struct Receiving<'a, R> {
    receiver: &'a mut BufferedResponseReceiver<R>,
    decoder: ResponseDecoder,
}

type ReceivingResponse<'a, R> = ArticleState<Receiving<'a, R>>;

impl<R: AsyncRead + Unpin> Receiving<'_, R> {
    fn extract(&mut self) -> Result<Option<OwnedResponse>, ClientError> {
        let Some(framed) = self.decoder.extract_framed(&mut self.receiver.pending)? else {
            return Ok(None);
        };
        let response = framed.validate()?;
        self.receiver.state = ReceiverState::Ready;
        Ok(Some(response))
    }
}

impl<R: AsyncRead + Unpin> ArticleState<Receiving<'_, R>> {
    fn extract(&mut self) -> Result<Option<OwnedResponse>, ClientError> {
        self.as_inner_mut().extract()
    }
}

pub(crate) fn benchmark_receive(
    kind: RequestKind,
    response: &[u8],
    chunk_bytes: usize,
) -> Result<(StatusCode, usize), ClientError> {
    receive_fragments(kind, response, chunk_bytes)
        .map(|response| (response.status(), response.as_bytes().len()))
}

pub(crate) async fn benchmark_receive_async(
    kind: RequestKind,
    response: &[u8],
    chunk_bytes: usize,
) -> Result<(StatusCode, usize), ClientError> {
    let mut receiver = BufferedResponseReceiver::new(std::io::Cursor::new(response));
    receiver
        .receive(kind, chunk_bytes)
        .await
        .map(|response| (response.status(), response.as_bytes().len()))
}

pub(crate) async fn benchmark_receive_packed_async(
    first_kind: RequestKind,
    second_kind: RequestKind,
    response: &[u8],
    chunk_bytes: usize,
) -> Result<(StatusCode, StatusCode, usize), ClientError> {
    let mut receiver = BufferedResponseReceiver::new(std::io::Cursor::new(response));
    let first = receiver.receive(first_kind, chunk_bytes).await?;
    let second = receiver.receive(second_kind, chunk_bytes).await?;
    Ok((
        first.status(),
        second.status(),
        first.as_bytes().len() + second.as_bytes().len(),
    ))
}

pub(crate) fn owned_from_bytes(
    kind: RequestKind,
    bytes: &[u8],
) -> Result<OwnedResponse, ClientError> {
    receive_fragments(kind, bytes, bytes.len())
}

fn receive_fragments(
    kind: RequestKind,
    response: &[u8],
    chunk_bytes: usize,
) -> Result<OwnedResponse, ClientError> {
    let mut receiver = BufferedResponseReceiver {
        reader: tokio::io::empty(),
        pending: BytesMut::with_capacity(response.len()),
        state: ReceiverState::Ready,
    };
    receiver.start_response()?;
    let mut pending = ReceivingResponse::new(Receiving {
        receiver: &mut receiver,
        decoder: ResponseDecoder::new(kind),
    });
    for chunk in response.chunks(chunk_bytes.max(1)) {
        pending
            .as_inner_mut()
            .receiver
            .pending
            .extend_from_slice(chunk);
        if let Some(response) = pending.extract()? {
            return Ok(response);
        }
    }
    Err(ClientError::UnexpectedEof)
}

/// Framing authorizes extraction only. Semantic validation consumes the owned
/// wire frame before an article layout can be exposed. The extracted bytes and
/// the status-line boundary travel together; no decoder borrow or detached
/// range can be supplied by a caller.
struct FramedResponse {
    framed: ArticleState<FramedArticleState<Bytes>>,
    initial: ResponseInitial,
}

impl FramedResponse {
    fn validate(self) -> Result<OwnedResponse, ClientError> {
        let Self { framed, initial } = self;
        let kind = framed.kind();
        let status = framed.status();
        if matches!(
            (kind, status.as_u16()),
            (RequestKind::Article, 220)
                | (RequestKind::Head, 221)
                | (RequestKind::Body, 222)
                | (RequestKind::Stat, 223)
        ) {
            let article = framed
                .into_inner()
                .validate()
                .map_err(|_| ClientError::InvalidStatusLine)?;
            return Ok(OwnedResponse {
                kind,
                status,
                content: OwnedResponseContent::Article(article),
            });
        }

        let ResponseFrameParse::Complete(frame) =
            ResponseFrameDecoder::new(kind).complete_framed_after_initial(&framed, initial)
        else {
            return Err(ClientError::InvalidStatusLine);
        };
        Ok(OwnedResponse {
            kind,
            status: frame.status(),
            content: OwnedResponseContent::from_frame(
                framed.clone_bytes(),
                frame.content_start(),
                frame.content_end(),
                frame.content_validation(),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};
    use tokio::io::{AsyncWriteExt, ReadBuf};

    struct FragmentedInput<'a>(VecDeque<&'a [u8]>);

    impl AsyncRead for FragmentedInput<'_> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            destination: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if let Some(fragment) = self.0.pop_front() {
                let count = fragment.len().min(destination.remaining());
                destination.put_slice(&fragment[..count]);
                if count < fragment.len() {
                    self.0.push_front(&fragment[count..]);
                }
            }
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn receives_exact_frames_at_every_split() {
        let frames: &[(RequestKind, &[u8])] = &[
            (RequestKind::Stat, b"223 1 <stat@test> article exists\r\n"),
            (RequestKind::Body, b"430 no article\r\n"),
            (
                RequestKind::Body,
                b"222 1 <body@test> body follows\r\n.\r\n",
            ),
            (
                RequestKind::Body,
                b"222 1 <body@test> body follows\r\n..stuffed\r\n.\r\n",
            ),
            (
                RequestKind::Head,
                b"221 1 <head@test> headers follow\r\nSubject: folded\r\n continuation\r\n.\r\n",
            ),
            (
                RequestKind::Article,
                b"220 1 <article@test> article follows\r\nSubject: article\r\n\r\nbody\r\n.\r\n",
            ),
        ];
        for &(kind, wire) in frames {
            for split in 1..wire.len() {
                let input = FragmentedInput([&wire[..split], &wire[split..]].into());
                let mut receiver = BufferedResponseReceiver::new(input);
                let response = receiver.receive(kind, wire.len()).await.unwrap();
                assert_eq!(response.as_bytes(), wire, "{kind:?}, split {split}");
                assert!(receiver.pending.is_empty());
            }
            let input = FragmentedInput(wire.chunks(1).collect());
            let mut receiver = BufferedResponseReceiver::new(input);
            assert_eq!(
                receiver.receive(kind, wire.len()).await.unwrap().as_bytes(),
                wire
            );
        }
    }

    #[tokio::test]
    async fn three_fragment_extraction_preserves_the_packed_next_response() {
        let frames: &[(RequestKind, &[u8])] = &[
            (RequestKind::Stat, b"223 1 <stat@test> exists\r\n"),
            (RequestKind::Body, b"430 missing\r\n"),
            (RequestKind::Body, b"222 1 <body@test> follows\r\n.\r\n"),
            (
                RequestKind::Body,
                b"222 1 <body@test> follows\r\n..dot\r\n.\r\n",
            ),
            (
                RequestKind::Head,
                b"221 1 <head@test> follows\r\nH: a\r\n b\r\n.\r\n",
            ),
            (
                RequestKind::Article,
                b"220 1 <article@test> follows\r\nH: a\r\n\r\nx\r\n.\r\n",
            ),
        ];
        let next = b"223 2 <next@test> exists\r\n";
        for &(kind, frame) in frames {
            let packed = [frame, next].concat();
            for first in 1..frame.len() - 1 {
                for second in first + 1..frame.len() {
                    let input = FragmentedInput(VecDeque::from([
                        &packed[..first],
                        &packed[first..second],
                        &packed[second..],
                    ]));
                    let mut receiver = BufferedResponseReceiver::new(input);
                    let response = receiver.receive(kind, packed.len()).await.unwrap();
                    assert_eq!(response.as_bytes(), frame, "{kind:?} {first}/{second}");
                    assert_eq!(receiver.pending.as_ref(), next);
                    let following = receiver
                        .receive(RequestKind::Stat, packed.len())
                        .await
                        .unwrap();
                    assert_eq!(following.as_bytes(), next);
                    assert!(receiver.pending.is_empty());
                    assert_eq!(response.as_bytes(), frame);
                }
            }
        }
    }

    #[tokio::test]
    async fn received_plain_article_access_borrows_without_allocating() {
        use std::borrow::Cow;
        use std::sync::atomic::Ordering;

        for body_len in [64 * 1024, 768 * 1024] {
            let mut wire = b"222 1 <plain@test> follows\r\n".to_vec();
            for _ in 0..body_len / 64 {
                wire.extend_from_slice(&[b'x'; 62]);
                wire.extend_from_slice(b"\r\n");
            }
            wire.extend_from_slice(b".\r\n");
            let input = FragmentedInput(VecDeque::from([wire.as_slice()]));
            let mut receiver = BufferedResponseReceiver::new(input);
            let article = OwnedArticle::try_from(
                receiver
                    .receive(RequestKind::Body, 16 * 1024)
                    .await
                    .unwrap(),
            )
            .unwrap();
            crate::TEST_ALLOCATIONS.store(0, Ordering::Relaxed);
            crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(true));
            for _ in 0..16 {
                let parsed = article.article();
                match parsed.body {
                    Some(Cow::Borrowed(body)) => {
                        assert_eq!(body.len(), body_len);
                        std::hint::black_box(body);
                    }
                    Some(Cow::Owned(_)) | None => panic!("plain body must be borrowed"),
                }
            }
            crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
            assert_eq!(crate::TEST_ALLOCATIONS.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn packed_prefix_is_frozen_without_the_next_responses_bytes() {
        let first = b"222 1 <body@test> body follows\r\nbody\r\n.\r\n";
        let second = b"223 2 <stat@test> article exists\r\n";
        let third = b"430 no article\r\n";
        let packed = [first.as_slice(), second.as_slice(), &third[..5]].concat();
        let input = FragmentedInput([packed.as_slice(), &third[5..]].into());
        let mut receiver = BufferedResponseReceiver::new(input);
        let response = receiver.receive(RequestKind::Body, 4096).await.unwrap();
        assert_eq!(response.as_bytes(), first);
        assert_eq!(
            receiver.pending.as_ref(),
            [second.as_slice(), &third[..5]].concat()
        );
        let article = OwnedArticle::try_from(response).unwrap();
        assert_eq!(
            article.article().body.as_deref(),
            Some(b"body\r\n".as_slice())
        );
        assert_eq!(article.clone(), article);
        assert_eq!(
            receiver
                .receive(RequestKind::Stat, 4096)
                .await
                .unwrap()
                .as_bytes(),
            second
        );
        assert_eq!(receiver.pending.as_ref(), &third[..5]);
        assert_eq!(
            receiver
                .receive(RequestKind::Body, 4096)
                .await
                .unwrap()
                .as_bytes(),
            third
        );
        assert!(receiver.pending.is_empty());
        assert_eq!(
            article.article().body.as_deref(),
            Some(b"body\r\n".as_slice())
        );
    }

    #[tokio::test]
    async fn cancelling_partial_receive_prevents_reclassification_as_a_new_response() {
        let (mut peer, input) = tokio::io::duplex(128);
        peer.write_all(b"223 1 <stat@test> article exists\r")
            .await
            .unwrap();
        let mut receiver = BufferedResponseReceiver::new(input);
        {
            let mut receive = std::pin::pin!(receiver.receive(RequestKind::Stat, 128));
            assert!(
                receive
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        peer.write_all(b"\n").await.unwrap();
        assert!(matches!(
            receiver.receive(RequestKind::Body, 128).await,
            Err(ClientError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn same_status_has_request_scoped_shape_in_packed_input() {
        let group = b"211 2 1 2 alt.test\r\n";
        let listgroup = b"211 2 1 2 alt.test\r\n1\r\n2\r\n.\r\n";
        let packed = [group.as_slice(), listgroup.as_slice()].concat();
        let mut receiver = BufferedResponseReceiver::new(packed.as_slice());
        assert_eq!(
            receiver
                .receive(RequestKind::Group, 4096)
                .await
                .unwrap()
                .as_bytes(),
            group
        );
        assert_eq!(receiver.pending.as_ref(), listgroup);
        assert_eq!(
            receiver
                .receive(RequestKind::ListGroup, 4096)
                .await
                .unwrap()
                .as_bytes(),
            listgroup
        );
        assert!(receiver.pending.is_empty());
    }

    #[tokio::test]
    async fn malformed_article_never_becomes_a_validated_owned_response() {
        let wire = b"222 1 <body@test> body follows\r\nbo\0dy\r\n.\r\n";
        for split in 1..wire.len() {
            let input = FragmentedInput([&wire[..split], &wire[split..]].into());
            let mut receiver = BufferedResponseReceiver::new(input);
            assert!(
                matches!(
                    receiver.receive(RequestKind::Body, 4096).await,
                    Err(ClientError::InvalidStatusLine)
                ),
                "split={split}"
            );
            assert!(matches!(
                receiver.receive(RequestKind::Body, 4096).await,
                Err(ClientError::ConnectionClosed)
            ));
        }
    }

    #[tokio::test]
    async fn dropping_an_unpolled_receive_does_not_abandon_a_response() {
        let wire = b"223 1 <stat@test> article exists\r\n";
        let mut receiver = BufferedResponseReceiver::new(wire.as_slice());
        drop(receiver.receive(RequestKind::Body, 4096));
        assert_eq!(
            receiver
                .receive(RequestKind::Stat, 4096)
                .await
                .unwrap()
                .as_bytes(),
            wire
        );
    }
}

/// Owned response bytes for the client path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedResponse {
    kind: RequestKind,
    status: StatusCode,
    content: OwnedResponseContent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OwnedResponseContent {
    Generic {
        bytes: Bytes,
        content: ResponseContentRange,
    },
    Article(ValidatedOwnedArticle),
}

/// Exclusive content coordinates relative to the owned framed response.
///
/// This is deliberately kept with the generic response bytes. Callers cannot
/// accidentally pair a content range from one response with another buffer,
/// or swap a start/end coordinate at the enum boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResponseContentRange(Range<usize>);

impl ResponseContentRange {
    fn new(start: usize, end: usize, response_len: usize) -> Option<Self> {
        (start <= end && end <= response_len).then_some(Self(start..end))
    }

    fn slice<'a>(&self, response: &'a [u8]) -> &'a [u8] {
        response
            .get(self.0.clone())
            .expect("validated response content range remains in its response")
    }
}

impl OwnedResponseContent {
    fn from_frame(
        bytes: Bytes,
        content_start: usize,
        content_end: usize,
        validation: ValidatedResponseContent<'_>,
    ) -> Self {
        match validation {
            ValidatedResponseContent::Generic => Self::Generic {
                content: ResponseContentRange::new(content_start, content_end, bytes.len())
                    .expect("response parser established an in-bounds content range"),
                bytes,
            },
            ValidatedResponseContent::Article(validated) => {
                Self::Article(validated.into_owned(bytes))
            }
        }
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Self::Generic { bytes, .. } => bytes,
            Self::Article(article) => article.bytes(),
        }
    }

    fn content(&self) -> &[u8] {
        match self {
            Self::Generic { bytes, content } => content.slice(bytes),
            Self::Article(article) => article.content(),
        }
    }
}

impl OwnedResponse {
    /// Request kind that produced this response.
    #[must_use]
    pub const fn kind(&self) -> RequestKind {
        self.kind
    }

    /// Parsed status code from the response status line.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Raw response bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.content.bytes()
    }

    /// Borrowed response payload bytes, excluding the initial line and any dot terminator.
    #[must_use]
    pub fn content(&self) -> &[u8] {
        self.content.content()
    }

    /// Parse the response as an ARTICLE/HEAD/BODY/STAT article-style frame.
    pub fn parse_article(&self) -> Result<Article<'_>, ArticleParseError> {
        match &self.content {
            OwnedResponseContent::Article(article) => Ok(article.materialize()),
            OwnedResponseContent::Generic { bytes, .. } => Article::parse(bytes),
        }
    }
}

/// Owned client article-style response that materializes its retained validated layout on demand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedArticle {
    response: OwnedResponse,
}

impl OwnedArticle {
    /// Request kind that produced this article-style response.
    #[must_use]
    pub const fn kind(&self) -> RequestKind {
        self.response.kind()
    }

    /// Parsed status code from the response status line.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.response.status()
    }

    /// Raw response bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.response.as_bytes()
    }

    /// Borrow the parsed article/body view from the owned wire bytes.
    pub fn article(&self) -> Article<'_> {
        let OwnedResponseContent::Article(ref article) = self.response.content else {
            unreachable!("OwnedArticle is constructed only from article validation");
        };
        article.materialize()
    }

    /// Borrow the underlying raw response wrapper.
    #[must_use]
    pub const fn response(&self) -> &OwnedResponse {
        &self.response
    }

    /// Consume the client article-style wrapper and return the raw response.
    #[must_use]
    pub fn into_response(self) -> OwnedResponse {
        self.response
    }
}

impl TryFrom<OwnedResponse> for OwnedArticle {
    type Error = ClientError;

    fn try_from(response: OwnedResponse) -> Result<Self, Self::Error> {
        let expected_status = match response.kind {
            RequestKind::Article => 220,
            RequestKind::Head => 221,
            RequestKind::Body => 222,
            RequestKind::Stat => 223,
            _ => 0,
        };
        if response.status.as_u16() != expected_status {
            return Err(ClientError::UnexpectedArticleResponse { response });
        }

        match response.content {
            OwnedResponseContent::Article(_) => {}
            OwnedResponseContent::Generic { .. } => {
                return Err(ClientError::UnexpectedArticleResponse { response });
            }
        }
        Ok(Self { response })
    }
}

#[derive(Debug)]
struct ResponseDecoder {
    streaming: StreamingResponseDecoder,
    scanned: FrameEnd,
}

impl ResponseDecoder {
    fn new(kind: RequestKind) -> Self {
        Self {
            streaming: StreamingResponseDecoder::new(kind),
            scanned: FrameEnd(0),
        }
    }

    fn push_framing(&mut self, buffer: &[u8]) -> Result<FramingDecodeProgress, ClientError> {
        let start = self.scanned;
        let chunk = &buffer[start.0..];
        self.scanned = FrameEnd(buffer.len());

        match self.streaming.push(chunk)? {
            StreamingDecodeProgress::NeedMore { .. } => Ok(FramingDecodeProgress::NeedMore),
            StreamingDecodeProgress::Complete {
                status,
                consumed: chunk_consumed,
                bounds,
            } => Ok(FramingDecodeProgress::Complete {
                status,
                frame_end: start.after_chunk(chunk_consumed),
                bounds,
            }),
        }
    }

    /// Complete and extract one frame while the decoder still owns the
    /// request-scoped framing state. The returned state owns the frozen bytes
    /// and the metadata needed for semantic validation; callers cannot pair
    /// an extracted buffer with a different decoder.
    fn extract_framed(
        &mut self,
        pending: &mut BytesMut,
    ) -> Result<Option<FramedResponse>, ClientError> {
        let FramingDecodeProgress::Complete {
            status,
            frame_end,
            bounds,
        } = self.push_framing(pending)?
        else {
            return Ok(None);
        };
        let initial = self
            .streaming
            .initial()
            .ok_or(ClientError::InvalidStatusLine)?;
        let status_line_end = self.streaming.status_line_end();
        let content_end = bounds
            .as_ref()
            .map_or(Ok(status_line_end.get()), |bounds| {
                status_line_end
                    .get()
                    .checked_add(bounds.content_end().get())
                    .ok_or(ClientError::InvalidStatusLine)
            })?;

        Ok(Some(FramedResponse {
            framed: ArticleState::new(FramedArticleState::new(
                frame_end.extract(pending),
                self.streaming.kind,
                status,
                bounds,
                status_line_end,
                ContentEnd::new(content_end),
            )),
            initial,
        }))
    }
}

#[derive(Debug)]
enum FramingDecodeProgress {
    NeedMore,
    Complete {
        status: StatusCode,
        frame_end: FrameEnd,
        bounds: Option<crate::terminator::MultilineFrameBounds>,
    },
}

/// Exclusive position relative to the first byte of the accumulated response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrameEnd(usize);

impl FrameEnd {
    fn after_chunk(self, consumed: DecoderChunkConsumed) -> Self {
        Self(self.0 + consumed.0)
    }

    fn extract(self, pending: &mut BytesMut) -> Bytes {
        pending.split_to(self.0).freeze()
    }
}

/// Count consumed from this decoder push, relative to the newly supplied
/// response chunk. This includes status-line bytes and is distinct from the
/// multiline framer's body-relative chunk coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecoderChunkConsumed(usize);

#[derive(Debug)]
struct StreamingResponseDecoder {
    kind: RequestKind,
    status: Option<StatusCode>,
    initial: Option<ResponseInitial>,
    status_line_end: StatusLineEnd,
    framer: MultilineFramer,
    status_buf: [u8; STREAMING_STATUS_LINE_BYTES],
    status_len: usize,
}

impl StreamingResponseDecoder {
    fn new(kind: RequestKind) -> Self {
        Self {
            kind,
            status: None,
            initial: None,
            status_line_end: StatusLineEnd::new(0),
            framer: MultilineFramer::default(),
            status_buf: [0; STREAMING_STATUS_LINE_BYTES],
            status_len: 0,
        }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<StreamingDecodeProgress, ClientError> {
        let mut content_start = 0;
        let status = match self.status {
            Some(status) => status,
            None => {
                let mut consumed = 0;
                while consumed < chunk.len() {
                    if self.status_len == self.status_buf.len() {
                        return Err(ClientError::InvalidStatusLine);
                    }
                    self.status_buf[self.status_len] = chunk[consumed];
                    self.status_len += 1;
                    consumed += 1;

                    match crate::protocol::ResponseInitial::parse(
                        self.kind,
                        &self.status_buf[..self.status_len],
                    ) {
                        ResponseInitialParse::Complete(initial) => {
                            let status = initial.status();
                            self.status = Some(status);
                            self.initial = Some(initial);
                            self.status_line_end = StatusLineEnd::new(self.status_len);
                            if !initial.descriptor().framing().is_multiline() {
                                return Ok(StreamingDecodeProgress::Complete {
                                    status,
                                    consumed: DecoderChunkConsumed(consumed),
                                    bounds: None,
                                });
                            }
                            content_start = consumed;
                            break;
                        }
                        ResponseInitialParse::NeedMore => {}
                        ResponseInitialParse::Invalid => {
                            return Err(ClientError::InvalidStatusLine);
                        }
                    }
                }

                let Some(status) = self.status else {
                    return Ok(StreamingDecodeProgress::NeedMore {
                        consumed: DecoderChunkConsumed(chunk.len()),
                    });
                };
                status
            }
        };

        if content_start >= chunk.len() {
            return Ok(StreamingDecodeProgress::NeedMore {
                consumed: DecoderChunkConsumed(chunk.len()),
            });
        }

        let content_chunk = &chunk[content_start..];
        match self.framer.push(content_chunk) {
            MultilineFrameProgress::Complete(bounds) => Ok(StreamingDecodeProgress::Complete {
                status,
                consumed: DecoderChunkConsumed(content_start + bounds.chunk_consumed().get()),
                bounds: Some(bounds),
            }),
            MultilineFrameProgress::NeedMore => Ok(StreamingDecodeProgress::NeedMore {
                consumed: DecoderChunkConsumed(chunk.len()),
            }),
        }
    }

    fn status_line_end(&self) -> StatusLineEnd {
        self.status_line_end
    }

    fn initial(&self) -> Option<ResponseInitial> {
        self.initial
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
enum StreamingDecodeProgress {
    NeedMore {
        consumed: DecoderChunkConsumed,
    },
    Complete {
        status: StatusCode,
        consumed: DecoderChunkConsumed,
        bounds: Option<crate::terminator::MultilineFrameBounds>,
    },
}

#[doc(hidden)]
pub fn bench_streaming_decode_response(
    kind: RequestKind,
    response: &[u8],
) -> Result<(StatusCode, usize), ClientError> {
    let mut decoder = StreamingResponseDecoder::new(kind);
    match decoder.push(response)? {
        StreamingDecodeProgress::Complete {
            status, consumed, ..
        } => Ok((status, consumed.0)),
        StreamingDecodeProgress::NeedMore { .. } => Err(ClientError::UnexpectedEof),
    }
}

#[allow(unexpected_cfgs)]
#[cfg(response_contract)]
mod response_contracts {
    use super::*;

    /// Positive controls compile with the production coordinate and ownership
    /// boundaries in place.
    fn positive() {
        let consumed = DecoderChunkConsumed(1);
        let _ = FrameEnd(0).after_chunk(consumed);
        let _ = std::hint::black_box(consumed);
    }

    #[cfg(response_contract = "coordinate")]
    fn coordinate_substitution_must_fail() {
        // A chunk-relative count cannot be used as an accumulated-buffer end.
        let _ = FrameEnd(0).after_chunk(FrameEnd(1));
    }

    #[cfg(response_contract = "receive_alias")]
    async fn receiving_borrow_must_fail<R: AsyncRead + Unpin>(
        receiver: &mut BufferedResponseReceiver<R>,
    ) {
        let decoder = ResponseDecoder::new(RequestKind::Stat);
        let _first = Receiving { receiver, decoder };
        let _second = &mut *receiver;
        std::hint::black_box(_first);
    }

    #[cfg(response_contract = "validated_mutation")]
    fn validated_borrow_must_fail() {
        let mut bytes = vec![0u8];
        let borrowed = &bytes[..];
        let _mutable = &mut bytes;
        std::hint::black_box(borrowed);
    }
}

#[cfg(test)]
#[path = "response_receiver_tests.rs"]
mod characterization_tests;
