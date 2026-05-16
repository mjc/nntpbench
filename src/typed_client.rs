use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, Notify, mpsc, oneshot, watch};

use crate::protocol::Request;
use crate::tail_buffer::{TailBuffer, TerminatorStatus};
use crate::{Article, ArticleParseError, MessageId, RequestKind, StatusCode};

/// Options for the typed one-connection client prototype.
#[derive(Debug, Clone, Copy)]
pub struct TypedClientOptions {
    pub read_buffer_bytes: usize,
    pub nodelay: bool,
    pub socket_recv_buffer: usize,
    pub socket_send_buffer: usize,
    pub pipeline_depth: usize,
}

impl Default for TypedClientOptions {
    fn default() -> Self {
        Self {
            read_buffer_bytes: crate::CLIENT_READER_CAPACITY,
            nodelay: true,
            socket_recv_buffer: 0,
            socket_send_buffer: 0,
            pipeline_depth: 64,
        }
    }
}

/// Primary request-specific client surface for typed NNTP calls.
#[derive(Debug, Clone)]
pub struct Client {
    connection: TypedClientConnection,
}

impl Client {
    /// Connect and consume the NNTP greeting.
    pub async fn connect(addr: SocketAddr) -> Result<Self, TypedClientError> {
        Self::connect_with_options(addr, TypedClientOptions::default()).await
    }

    /// Connect with explicit socket and buffer options.
    pub async fn connect_with_options(
        addr: SocketAddr,
        options: TypedClientOptions,
    ) -> Result<Self, TypedClientError> {
        Ok(Self {
            connection: TypedClientConnection::connect_with_options(addr, options).await?,
        })
    }

    /// Execute a typed article-style request directly and return a typed owned article-style response.
    pub async fn execute(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let response = self.connection.execute(request).await?;
        OwnedArticle::try_from(response)
    }

    /// Execute a typed request directly and return the completed request/response pair.
    pub async fn execute_exchange(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let exchange = self.connection.execute_exchange(request).await?;
        OwnedArticleExchange::try_from(exchange)
    }

    /// Execute a typed request directly and return the owned raw response frame.
    pub async fn execute_raw(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.connection.execute(request).await
    }

    /// Execute a typed request directly and return the completed raw request/response pair.
    pub async fn execute_raw_exchange(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.connection.execute_exchange(request).await
    }

    /// Send an ARTICLE request and return a typed owned article response.
    pub async fn article(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request =
            Request::article(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute(request).await
    }

    /// Send an ARTICLE request and return the completed request/response pair.
    pub async fn article_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request =
            Request::article(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_exchange(request).await
    }

    /// Send a BODY request and return a typed owned article-style response.
    pub async fn body(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request = Request::body(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute(request).await
    }

    /// Send a BODY request and return the completed request/response pair.
    pub async fn body_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::body(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_exchange(request).await
    }

    /// Send a HEAD request and return a typed owned article-style response.
    pub async fn head(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request = Request::head(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute(request).await
    }

    /// Send a HEAD request and return the completed request/response pair.
    pub async fn head_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::head(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_exchange(request).await
    }

    /// Send a STAT request and return a typed owned article-style response.
    pub async fn stat(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticle, TypedClientError> {
        let request = Request::stat(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute(request).await
    }

    /// Send a STAT request and return the completed request/response pair.
    pub async fn stat_exchange(
        &self,
        message_id: impl AsRef<str>,
    ) -> Result<OwnedArticleExchange, TypedClientError> {
        let request = Request::stat(message_id).map_err(|_| TypedClientError::InvalidMessageId)?;
        self.execute_exchange(request).await
    }

    /// Send a CAPABILITIES request and return the owned raw response frame.
    pub async fn capabilities(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::capabilities()).await
    }

    /// Send a CAPABILITIES request and return the completed raw request/response pair.
    pub async fn capabilities_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::capabilities()).await
    }

    /// Send a DATE request and return the owned raw response frame.
    pub async fn date(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::date()).await
    }

    /// Send a DATE request and return the completed raw request/response pair.
    pub async fn date_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::date()).await
    }

    /// Send a MODE READER request and return the owned raw response frame.
    pub async fn mode_reader(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute_raw(Request::mode_reader()).await
    }

    /// Send a MODE READER request and return the completed raw request/response pair.
    pub async fn mode_reader_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_raw_exchange(Request::mode_reader()).await
    }

    /// Access the lower-level raw-frame connection.
    #[must_use]
    pub fn connection(&self) -> &TypedClientConnection {
        &self.connection
    }
}

/// One TCP connection exposing request-specific async methods.
#[derive(Debug, Clone)]
pub struct TypedClientConnection {
    inner: Arc<ConnectionHandle>,
}

impl TypedClientConnection {
    /// Connect and consume the NNTP greeting.
    pub async fn connect(addr: SocketAddr) -> Result<Self, TypedClientError> {
        Self::connect_with_options(addr, TypedClientOptions::default()).await
    }

    /// Connect with explicit socket and buffer options.
    pub async fn connect_with_options(
        addr: SocketAddr,
        options: TypedClientOptions,
    ) -> Result<Self, TypedClientError> {
        let mut stream = crate::connect_client_socket(
            addr,
            options.nodelay,
            options.socket_recv_buffer,
            options.socket_send_buffer,
        )
        .await?;
        let mut read_buffer = vec![
            0;
            options
                .read_buffer_bytes
                .max(crate::tail_buffer::TERMINATOR_TAIL_SIZE)
        ]
        .into_boxed_slice();
        crate::read_greeting(&mut stream, &mut read_buffer).await?;
        let (reader, writer) = stream.into_split();
        let (request_tx, request_rx) = mpsc::channel(options.pipeline_depth.max(1));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poisoned = Arc::new(Mutex::new(None));
        let inflight = Arc::new(Mutex::new(VecDeque::new()));
        let inflight_notify = Arc::new(Notify::new());

        tokio::spawn(run_writer_task(
            writer,
            request_rx,
            inflight.clone(),
            inflight_notify.clone(),
            poisoned.clone(),
            shutdown_rx.clone(),
        ));
        tokio::spawn(run_reader_task(
            reader,
            read_buffer,
            inflight,
            inflight_notify,
            poisoned.clone(),
            shutdown_rx,
        ));

        Ok(Self {
            inner: Arc::new(ConnectionHandle {
                request_tx,
                shutdown_tx,
                poisoned,
            }),
        })
    }

    /// Send an ARTICLE request and return the owned response frame.
    pub async fn article(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Article { message_id }).await
    }

    /// Send an ARTICLE request and return the completed request/response pair.
    pub async fn article_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Article { message_id }).await
    }

    /// Send a BODY request and return the owned response frame.
    pub async fn body(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Body { message_id }).await
    }

    /// Send a BODY request and return the completed request/response pair.
    pub async fn body_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Body { message_id }).await
    }

    /// Send a HEAD request and return the owned response frame.
    pub async fn head(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Head { message_id }).await
    }

    /// Send a HEAD request and return the completed request/response pair.
    pub async fn head_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Head { message_id }).await
    }

    /// Send a STAT request and return the owned response frame.
    pub async fn stat(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Stat { message_id }).await
    }

    /// Send a STAT request and return the completed request/response pair.
    pub async fn stat_exchange(
        &self,
        message_id: MessageId<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Stat { message_id }).await
    }

    /// Send a CAPABILITIES request and return the owned response frame.
    pub async fn capabilities(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Capabilities).await
    }

    /// Send a CAPABILITIES request and return the completed request/response pair.
    pub async fn capabilities_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Capabilities).await
    }

    /// Send a DATE request and return the owned response frame.
    pub async fn date(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::Date).await
    }

    /// Send a DATE request and return the completed request/response pair.
    pub async fn date_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::Date).await
    }

    /// Send a MODE READER request and return the owned response frame.
    pub async fn mode_reader(&self) -> Result<OwnedResponse, TypedClientError> {
        self.execute(Request::ModeReader).await
    }

    /// Send a MODE READER request and return the completed request/response pair.
    pub async fn mode_reader_exchange(&self) -> Result<OwnedExchange, TypedClientError> {
        self.execute_exchange(Request::ModeReader).await
    }

    /// Execute a typed request on this connection.
    pub async fn execute(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedResponse, TypedClientError> {
        self.queue_request(request).await?.receive().await
    }

    pub(crate) async fn queue_request(
        &self,
        request: Request<'static>,
    ) -> Result<PendingResponse, TypedClientError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.inner
            .request_tx
            .send(QueuedRequest {
                request,
                response_tx,
            })
            .await
            .map_err(|_| TypedClientError::ConnectionClosed)?;

        Ok(PendingResponse {
            inner: self.inner.clone(),
            response_rx,
        })
    }

    /// Execute a typed request and return the completed request/response pair.
    pub async fn execute_exchange(
        &self,
        request: Request<'static>,
    ) -> Result<OwnedExchange, TypedClientError> {
        let response = self.execute(request.clone()).await?;
        Ok(OwnedExchange { request, response })
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

#[derive(Debug)]
struct ConnectionHandle {
    request_tx: mpsc::Sender<QueuedRequest>,
    shutdown_tx: watch::Sender<bool>,
    poisoned: Arc<Mutex<Option<SharedEngineError>>>,
}

/// Owned response bytes for the typed client path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedResponse {
    kind: RequestKind,
    status: StatusCode,
    bytes: Box<[u8]>,
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
        &self.bytes
    }

    /// Parse the response as an ARTICLE/HEAD/BODY/STAT article-style frame.
    pub fn parse_article(&self) -> Result<Article<'_>, ArticleParseError> {
        Article::parse(&self.bytes)
    }
}

/// Owned request/response pair for the typed connection surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedExchange {
    request: Request<'static>,
    response: OwnedResponse,
}

impl OwnedExchange {
    /// Original typed request.
    #[must_use]
    pub const fn request(&self) -> &Request<'static> {
        &self.request
    }

    /// Completed owned response.
    #[must_use]
    pub const fn response(&self) -> &OwnedResponse {
        &self.response
    }

    /// Consume the exchange into its original request and completed response.
    #[must_use]
    pub fn into_parts(self) -> (Request<'static>, OwnedResponse) {
        (self.request, self.response)
    }
}

/// Owned typed article-style response that reparses the zero-copy article view on demand.
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
    pub fn article(&self) -> Result<Article<'_>, ArticleParseError> {
        self.response.parse_article()
    }

    /// Borrow the underlying raw response wrapper.
    #[must_use]
    pub const fn response(&self) -> &OwnedResponse {
        &self.response
    }

    /// Consume the typed article-style wrapper and return the raw response.
    #[must_use]
    pub fn into_response(self) -> OwnedResponse {
        self.response
    }
}

/// Owned request/article-response pair for the high-level typed client surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedArticleExchange {
    request: Request<'static>,
    article: OwnedArticle,
}

impl OwnedArticleExchange {
    /// Original typed request.
    #[must_use]
    pub const fn request(&self) -> &Request<'static> {
        &self.request
    }

    /// Completed typed article-style response.
    #[must_use]
    pub const fn article(&self) -> &OwnedArticle {
        &self.article
    }

    /// Consume the exchange into its original request and typed article-style response.
    #[must_use]
    pub fn into_parts(self) -> (Request<'static>, OwnedArticle) {
        (self.request, self.article)
    }
}

impl TryFrom<OwnedExchange> for OwnedArticleExchange {
    type Error = TypedClientError;

    fn try_from(exchange: OwnedExchange) -> Result<Self, Self::Error> {
        let article = OwnedArticle::try_from(exchange.response)?;
        Ok(Self {
            request: exchange.request,
            article,
        })
    }
}

impl TryFrom<OwnedResponse> for OwnedArticle {
    type Error = TypedClientError;

    fn try_from(response: OwnedResponse) -> Result<Self, Self::Error> {
        if let Err(source) = response.parse_article() {
            return Err(TypedClientError::UnexpectedArticleResponse { response, source });
        }

        Ok(Self { response })
    }
}

/// Errors from the typed client prototype.
#[derive(Debug)]
pub enum TypedClientError {
    Io(io::Error),
    UnexpectedEof,
    InvalidStatusLine,
    InvalidMessageId,
    MissingMessageId,
    ConnectionClosed,
    UnexpectedArticleResponse {
        response: OwnedResponse,
        source: ArticleParseError,
    },
}

impl fmt::Display for TypedClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::UnexpectedEof => write!(f, "server closed before completing response"),
            Self::InvalidStatusLine => write!(f, "invalid NNTP status line"),
            Self::InvalidMessageId => write!(f, "invalid message-id"),
            Self::MissingMessageId => write!(f, "message-id is required for this request"),
            Self::ConnectionClosed => write!(f, "connection engine closed"),
            Self::UnexpectedArticleResponse { response, source } => write!(
                f,
                "unexpected article response status {}: {source}",
                response.status().as_u16()
            ),
        }
    }
}

impl std::error::Error for TypedClientError {}

impl From<io::Error> for TypedClientError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<SharedEngineError> for TypedClientError {
    fn from(value: SharedEngineError) -> Self {
        match value {
            SharedEngineError::UnexpectedEof => Self::UnexpectedEof,
            SharedEngineError::InvalidStatusLine => Self::InvalidStatusLine,
            SharedEngineError::ConnectionClosed => Self::ConnectionClosed,
            SharedEngineError::Io { kind, message } => Self::Io(io::Error::new(kind, message)),
        }
    }
}

#[derive(Debug)]
struct ResponseDecoder {
    kind: RequestKind,
    buffered_len: usize,
    status: Option<(StatusCode, usize)>,
    tail: TailBuffer,
}

impl ResponseDecoder {
    fn new(kind: RequestKind) -> Self {
        Self {
            kind,
            buffered_len: 0,
            status: None,
            tail: TailBuffer::default(),
        }
    }

    fn push(&mut self, buffer: &[u8]) -> Result<DecodeProgress, TypedClientError> {
        let chunk_start = self.buffered_len;
        self.buffered_len = buffer.len();

        let (status, status_end) = match self.status {
            Some(value) => value,
            None => {
                let Some(status_end) = status_line_end(buffer) else {
                    return Ok(DecodeProgress::NeedMore);
                };
                let status =
                    StatusCode::parse(buffer).ok_or(TypedClientError::InvalidStatusLine)?;
                self.status = Some((status, status_end));
                if !self.kind.expects_multiline_response(status) {
                    return Ok(DecodeProgress::Complete {
                        response: self.finish(buffer, status, status_end),
                        consumed: status_end,
                    });
                }
                (status, status_end)
            }
        };

        let content_chunk_start = chunk_start.max(status_end);
        if content_chunk_start >= buffer.len() {
            return Ok(DecodeProgress::NeedMore);
        }

        let content_chunk = &buffer[content_chunk_start..];
        match self.tail.detect_terminator(content_chunk) {
            TerminatorStatus::FoundAt(end) => Ok(DecodeProgress::Complete {
                response: self.finish(buffer, status, content_chunk_start + end),
                consumed: content_chunk_start + end,
            }),
            TerminatorStatus::NotFound => {
                self.tail.update(content_chunk);
                Ok(DecodeProgress::NeedMore)
            }
        }
    }

    fn finish(&mut self, buffer: &[u8], status: StatusCode, end: usize) -> OwnedResponse {
        OwnedResponse {
            kind: self.kind,
            status,
            bytes: buffer[..end].to_vec().into_boxed_slice(),
        }
    }
}

#[derive(Debug)]
enum DecodeProgress {
    NeedMore,
    Complete {
        response: OwnedResponse,
        consumed: usize,
    },
}

#[derive(Debug)]
struct QueuedRequest {
    request: Request<'static>,
    response_tx: oneshot::Sender<Result<OwnedResponse, SharedEngineError>>,
}

#[derive(Debug)]
pub(crate) struct PendingResponse {
    inner: Arc<ConnectionHandle>,
    response_rx: oneshot::Receiver<Result<OwnedResponse, SharedEngineError>>,
}

impl PendingResponse {
    pub(crate) async fn receive(self) -> Result<OwnedResponse, TypedClientError> {
        let response = match self.response_rx.await {
            Ok(response) => response,
            Err(_) => {
                return Err(self
                    .inner
                    .poisoned
                    .lock()
                    .await
                    .clone()
                    .map(TypedClientError::from)
                    .unwrap_or(TypedClientError::ConnectionClosed));
            }
        };

        response.map_err(Into::into)
    }
}

#[derive(Debug)]
struct InFlightRequest {
    kind: RequestKind,
    response_tx: oneshot::Sender<Result<OwnedResponse, SharedEngineError>>,
}

#[derive(Debug, Clone)]
enum SharedEngineError {
    Io {
        kind: io::ErrorKind,
        message: String,
    },
    UnexpectedEof,
    InvalidStatusLine,
    ConnectionClosed,
}

async fn run_writer_task(
    mut writer: OwnedWriteHalf,
    mut request_rx: mpsc::Receiver<QueuedRequest>,
    inflight: Arc<Mutex<VecDeque<InFlightRequest>>>,
    inflight_notify: Arc<Notify>,
    poisoned: Arc<Mutex<Option<SharedEngineError>>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut write_buffer = Vec::with_capacity(crate::MAX_CLIENT_COMMAND_BYTES);

    loop {
        let queued = tokio::select! {
            maybe = request_rx.recv() => maybe,
            changed = shutdown_rx.changed() => {
                if changed.is_ok() || changed.is_err() {
                    break;
                }
                None
            }
        };

        let Some(queued) = queued else {
            break;
        };

        let kind = queued.request.kind();
        {
            let mut guard = inflight.lock().await;
            guard.push_back(InFlightRequest {
                kind,
                response_tx: queued.response_tx,
            });
        }

        write_buffer.clear();
        queued.request.write_wire_to(&mut write_buffer);
        if let Err(err) = writer.write_all(&write_buffer).await {
            let error = SharedEngineError::Io {
                kind: err.kind(),
                message: err.to_string(),
            };
            if let Some(failed) = inflight.lock().await.pop_back() {
                let _ = failed.response_tx.send(Err(error.clone()));
            }
            poison_engine(&poisoned, &inflight, &mut request_rx, error).await;
            return;
        }

        inflight_notify.notify_one();
    }
}

async fn run_reader_task(
    mut reader: OwnedReadHalf,
    mut read_buffer: Box<[u8]>,
    inflight: Arc<Mutex<VecDeque<InFlightRequest>>>,
    inflight_notify: Arc<Notify>,
    poisoned: Arc<Mutex<Option<SharedEngineError>>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut pending_read = Vec::with_capacity(read_buffer.len().saturating_mul(2));
    let mut pending_read_start = 0;

    loop {
        let Some(inflight_request) =
            next_inflight(&inflight, &inflight_notify, &mut shutdown_rx).await
        else {
            return;
        };
        let mut decoder = ResponseDecoder::new(inflight_request.kind);
        let response_tx = inflight_request.response_tx;

        loop {
            if pending_read_start < pending_read.len() {
                match decoder.push(&pending_read[pending_read_start..]) {
                    Ok(DecodeProgress::NeedMore) => {}
                    Ok(DecodeProgress::Complete { response, consumed }) => {
                        pending_read_start += consumed;
                        compact_pending_read(&mut pending_read, &mut pending_read_start);
                        let _ = response_tx.send(Ok(response));
                        break;
                    }
                    Err(TypedClientError::InvalidStatusLine) => {
                        let error = SharedEngineError::InvalidStatusLine;
                        let _ = response_tx.send(Err(error.clone()));
                        poison_engine_without_requests(&poisoned, &inflight, error).await;
                        return;
                    }
                    Err(err) => {
                        let error = shared_engine_error_from_typed(err);
                        let _ = response_tx.send(Err(error.clone()));
                        poison_engine_without_requests(&poisoned, &inflight, error).await;
                        return;
                    }
                }
            }

            let read = tokio::select! {
                result = reader.read(&mut read_buffer) => result,
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() || changed.is_err() {
                        return;
                    }
                    Ok(0)
                }
            };

            let read = match read {
                Ok(read) => read,
                Err(err) => {
                    let error = SharedEngineError::Io {
                        kind: err.kind(),
                        message: err.to_string(),
                    };
                    let _ = response_tx.send(Err(error.clone()));
                    poison_engine_without_requests(&poisoned, &inflight, error).await;
                    return;
                }
            };

            if read == 0 {
                let error = SharedEngineError::UnexpectedEof;
                let _ = response_tx.send(Err(error.clone()));
                poison_engine_without_requests(&poisoned, &inflight, error).await;
                return;
            }

            compact_pending_read(&mut pending_read, &mut pending_read_start);
            pending_read.extend_from_slice(&read_buffer[..read]);
        }
    }
}

fn compact_pending_read(buffer: &mut Vec<u8>, start: &mut usize) {
    if *start == 0 {
        return;
    }

    let len = buffer.len();
    if *start >= len {
        buffer.clear();
        *start = 0;
        return;
    }

    if *start >= len / 2 {
        buffer.copy_within(*start.., 0);
        buffer.truncate(len - *start);
        *start = 0;
    }
}

async fn next_inflight(
    inflight: &Arc<Mutex<VecDeque<InFlightRequest>>>,
    inflight_notify: &Arc<Notify>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Option<InFlightRequest> {
    loop {
        if let Some(request) = inflight.lock().await.pop_front() {
            return Some(request);
        }

        let notified = inflight_notify.notified();
        if *shutdown_rx.borrow() {
            return None;
        }

        tokio::select! {
            _ = notified => {}
            changed = shutdown_rx.changed() => {
                if changed.is_ok() || changed.is_err() {
                    return None;
                }
            }
        }
    }
}

async fn poison_engine(
    poisoned: &Arc<Mutex<Option<SharedEngineError>>>,
    inflight: &Arc<Mutex<VecDeque<InFlightRequest>>>,
    request_rx: &mut mpsc::Receiver<QueuedRequest>,
    error: SharedEngineError,
) {
    {
        let mut guard = poisoned.lock().await;
        if guard.is_none() {
            *guard = Some(error.clone());
        }
    }

    while let Ok(queued) = request_rx.try_recv() {
        let _ = queued.response_tx.send(Err(error.clone()));
    }

    drain_inflight(inflight, error).await;
}

async fn poison_engine_without_requests(
    poisoned: &Arc<Mutex<Option<SharedEngineError>>>,
    inflight: &Arc<Mutex<VecDeque<InFlightRequest>>>,
    error: SharedEngineError,
) {
    {
        let mut guard = poisoned.lock().await;
        if guard.is_none() {
            *guard = Some(error.clone());
        }
    }

    drain_inflight(inflight, error).await;
}

async fn drain_inflight(
    inflight: &Arc<Mutex<VecDeque<InFlightRequest>>>,
    error: SharedEngineError,
) {
    let mut queued = inflight.lock().await;
    while let Some(request) = queued.pop_front() {
        let _ = request.response_tx.send(Err(error.clone()));
    }
}

fn shared_engine_error_from_typed(err: TypedClientError) -> SharedEngineError {
    match err {
        TypedClientError::Io(err) => SharedEngineError::Io {
            kind: err.kind(),
            message: err.to_string(),
        },
        TypedClientError::UnexpectedEof => SharedEngineError::UnexpectedEof,
        TypedClientError::InvalidStatusLine => SharedEngineError::InvalidStatusLine,
        TypedClientError::ConnectionClosed => SharedEngineError::ConnectionClosed,
        TypedClientError::InvalidMessageId
        | TypedClientError::MissingMessageId
        | TypedClientError::UnexpectedArticleResponse { .. } => SharedEngineError::ConnectionClosed,
    }
}

fn status_line_end(buffer: &[u8]) -> Option<usize> {
    memchr::memchr(b'\n', buffer).map(|index| index + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoder_completes_single_line_error_without_waiting_for_terminator() {
        let mut decoder = ResponseDecoder::new(RequestKind::Article);
        let DecodeProgress::Complete { response, consumed } = decoder
            .push(b"430 no article with that message-id\r\n")
            .unwrap()
        else {
            panic!("decoder should complete");
        };

        assert_eq!(consumed, b"430 no article with that message-id\r\n".len());
        assert_eq!(response.kind(), RequestKind::Article);
        assert_eq!(response.status().as_u16(), 430);
        assert_eq!(
            response.as_bytes(),
            b"430 no article with that message-id\r\n"
        );
    }

    #[test]
    fn decoder_completes_multiline_response_across_chunks() {
        let mut decoder = ResponseDecoder::new(RequestKind::Body);
        let mut buffer = b"222 1 <a@b> body follows\r\nbody\r".to_vec();
        assert!(matches!(
            decoder.push(&buffer).unwrap(),
            DecodeProgress::NeedMore
        ));
        buffer.extend_from_slice(b"\n.\r\n");
        let DecodeProgress::Complete { response, consumed } = decoder.push(&buffer).unwrap() else {
            panic!("decoder should complete");
        };

        assert_eq!(consumed, b"222 1 <a@b> body follows\r\nbody\r\n.\r\n".len());
        assert_eq!(response.status().as_u16(), 222);
        assert_eq!(
            response.as_bytes(),
            b"222 1 <a@b> body follows\r\nbody\r\n.\r\n"
        );
    }

    #[test]
    fn decoder_reports_consumed_bytes_and_preserves_leftover_chunk_data() {
        let chunk =
            b"222 1 <a@b> body follows\r\nbody\r\n.\r\n220 1 <b@c> article follows\r\nh: v\r\n\r\nx\r\n.\r\n";

        let mut first = ResponseDecoder::new(RequestKind::Body);
        let DecodeProgress::Complete { response, consumed } = first.push(chunk).unwrap() else {
            panic!("first decoder should complete");
        };
        assert_eq!(response.status().as_u16(), 222);
        assert_eq!(
            response.as_bytes(),
            b"222 1 <a@b> body follows\r\nbody\r\n.\r\n"
        );

        let mut second = ResponseDecoder::new(RequestKind::Article);
        let DecodeProgress::Complete {
            response: second_response,
            consumed: second_consumed,
        } = second.push(&chunk[consumed..]).unwrap()
        else {
            panic!("second decoder should complete");
        };
        assert_eq!(second_consumed, chunk.len() - consumed);
        assert_eq!(second_response.status().as_u16(), 220);
    }

    #[test]
    fn compact_pending_read_keeps_live_suffix_without_front_drain() {
        let mut buffer = b"consumed-live-bytes".to_vec();
        let mut start = b"consumed-".len();
        compact_pending_read(&mut buffer, &mut start);
        assert_eq!(&buffer, b"live-bytes");
        assert_eq!(start, 0);

        let mut untouched = b"abcdef".to_vec();
        let mut untouched_start = 2;
        compact_pending_read(&mut untouched, &mut untouched_start);
        assert_eq!(&untouched, b"abcdef");
        assert_eq!(untouched_start, 2);
    }

    #[tokio::test]
    async fn typed_connection_fetches_article_and_parses_zero_copy_view() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"ARTICLE <typed@test>\r\n");

            stream
                .write_all(
                    b"220 1 <typed@test> article follows\r\nSubject: Typed\r\n\r\npayload\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let response = connection
            .article(MessageId::from_str_or_wrap("typed@test").unwrap())
            .await
            .unwrap();
        let article = response.parse_article().unwrap();

        assert_eq!(response.kind(), RequestKind::Article);
        assert_eq!(response.status().as_u16(), 220);
        assert_eq!(article.message_id.as_str(), "<typed@test>");
        assert_eq!(article.headers.unwrap().get("Subject"), Some(&b"Typed"[..]));
        assert_eq!(article.body, Some(&b"payload\r\n"[..]));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_fetches_head_and_stat_frames() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"HEAD <head@test>\r\n");
            stream
                .write_all(b"221 1 <head@test> article retrieved\r\nSubject: Head\r\n.\r\n")
                .await
                .unwrap();

            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"STAT <stat@test>\r\n");
            stream
                .write_all(b"223 1 <stat@test> article retrieved\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let head = connection
            .head(MessageId::from_str_or_wrap("head@test").unwrap())
            .await
            .unwrap();
        let stat = connection
            .stat(MessageId::from_str_or_wrap("stat@test").unwrap())
            .await
            .unwrap();

        assert_eq!(head.kind(), RequestKind::Head);
        assert_eq!(head.status().as_u16(), 221);
        assert_eq!(
            head.parse_article()
                .unwrap()
                .headers
                .unwrap()
                .get("Subject"),
            Some(&b"Head"[..])
        );

        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(stat.status().as_u16(), 223);
        assert_eq!(
            stat.parse_article().unwrap().message_id.as_str(),
            "<stat@test>"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_returns_single_line_article_error_frame() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"ARTICLE <missing@test>\r\n");

            stream
                .write_all(b"430 no article with that message-id\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let response = connection
            .article(MessageId::from_str_or_wrap("missing@test").unwrap())
            .await
            .unwrap();

        assert_eq!(response.kind(), RequestKind::Article);
        assert_eq!(response.status().as_u16(), 430);
        assert_eq!(
            response.as_bytes(),
            b"430 no article with that message-id\r\n"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_execute_exchange_returns_request_and_response() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"BODY <pair@test>\r\n");

            stream
                .write_all(b"222 1 <pair@test> body follows\r\npair body\r\n.\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let exchange = connection
            .body_exchange(MessageId::from_str_or_wrap("pair@test").unwrap())
            .await
            .unwrap();

        assert_eq!(
            exchange.request(),
            &Request::Body {
                message_id: MessageId::from_str_or_wrap("pair@test").unwrap()
            }
        );
        assert_eq!(exchange.response().status().as_u16(), 222);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_exchange_into_parts_returns_literal_pair() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"STAT <parts@test>\r\n");

            stream
                .write_all(b"223 1 <parts@test> article retrieved\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let exchange = connection
            .stat_exchange(MessageId::from_str_or_wrap("parts@test").unwrap())
            .await
            .unwrap();
        let (request, response) = exchange.into_parts();

        assert_eq!(
            request,
            Request::Stat {
                message_id: MessageId::from_str_or_wrap("parts@test").unwrap()
            }
        );
        assert_eq!(response.kind(), RequestKind::Stat);
        assert_eq!(response.status().as_u16(), 223);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_fetches_capabilities_date_and_mode_reader_frames() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"CAPABILITIES\r\n");
            stream
                .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                .await
                .unwrap();

            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"DATE\r\n");
            stream.write_all(b"111 20260515120000\r\n").await.unwrap();

            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"MODE READER\r\n");
            stream
                .write_all(b"201 posting not permitted\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let capabilities = connection.capabilities().await.unwrap();
        let date = connection.date().await.unwrap();
        let mode_reader = connection.mode_reader().await.unwrap();

        assert_eq!(capabilities.kind(), RequestKind::Capabilities);
        assert_eq!(capabilities.status().as_u16(), 101);
        assert_eq!(
            capabilities.as_bytes(),
            b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n"
        );

        assert_eq!(date.kind(), RequestKind::Date);
        assert_eq!(date.status().as_u16(), 111);
        assert_eq!(date.as_bytes(), b"111 20260515120000\r\n");

        assert_eq!(mode_reader.kind(), RequestKind::ModeReader);
        assert_eq!(mode_reader.status().as_u16(), 201);
        assert_eq!(mode_reader.as_bytes(), b"201 posting not permitted\r\n");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_article_returns_typed_owned_article_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"ARTICLE <surface@test>\r\n");

            stream
                .write_all(
                    b"220 1 <surface@test> article follows\r\nSubject: Surface\r\n\r\npayload\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let article = client.article("surface@test").await.unwrap();
        let parsed = article.article().unwrap();

        assert_eq!(article.kind(), RequestKind::Article);
        assert_eq!(article.status().as_u16(), 220);
        assert_eq!(parsed.message_id.as_str(), "<surface@test>");
        assert_eq!(
            parsed.headers.unwrap().get("Subject"),
            Some(&b"Surface"[..])
        );
        assert_eq!(parsed.body, Some(&b"payload\r\n"[..]));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_article_exchange_returns_request_and_typed_article() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"ARTICLE <exchange@test>\r\n");

            stream
                .write_all(
                    b"220 1 <exchange@test> article follows\r\nSubject: Exchange\r\n\r\npayload\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let exchange = client.article_exchange("exchange@test").await.unwrap();
        let parsed = exchange.article().article().unwrap();

        assert_eq!(
            exchange.request(),
            &Request::Article {
                message_id: MessageId::from_str_or_wrap("exchange@test").unwrap()
            }
        );
        assert_eq!(exchange.article().status().as_u16(), 220);
        assert_eq!(parsed.message_id.as_str(), "<exchange@test>");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_article_exchange_into_parts_returns_literal_pair() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"ARTICLE <pair-surface@test>\r\n");

            stream
                .write_all(
                    b"220 1 <pair-surface@test> article follows\r\nSubject: Pair\r\n\r\npayload\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let exchange = client.article_exchange("pair-surface@test").await.unwrap();
        let (request, article) = exchange.into_parts();
        let raw = article.clone().into_response();

        assert_eq!(
            request,
            Request::Article {
                message_id: MessageId::from_str_or_wrap("pair-surface@test").unwrap()
            }
        );
        assert_eq!(article.status().as_u16(), 220);
        assert_eq!(raw.kind(), RequestKind::Article);
        assert_eq!(
            article.article().unwrap().message_id.as_str(),
            "<pair-surface@test>"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_raw_methods_expose_general_request_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"CAPABILITIES\r\n");
            stream
                .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                .await
                .unwrap();

            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"DATE\r\n");
            stream.write_all(b"111 20260515120000\r\n").await.unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let capabilities = client.capabilities().await.unwrap();
        let exchange = client.date_exchange().await.unwrap();

        assert_eq!(capabilities.kind(), RequestKind::Capabilities);
        assert_eq!(capabilities.status().as_u16(), 101);
        assert_eq!(exchange.request(), &Request::Date);
        assert_eq!(exchange.response().status().as_u16(), 111);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_head_and_stat_return_typed_article_surface() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"HEAD <head-surface@test>\r\n");
            stream
                .write_all(
                    b"221 1 <head-surface@test> article retrieved\r\nSubject: Surface Head\r\n.\r\n",
                )
                .await
                .unwrap();

            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"STAT <stat-surface@test>\r\n");
            stream
                .write_all(b"223 1 <stat-surface@test> article retrieved\r\n")
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let head = client.head("head-surface@test").await.unwrap();
        let stat = client.stat("stat-surface@test").await.unwrap();

        assert_eq!(head.kind(), RequestKind::Head);
        assert_eq!(head.status().as_u16(), 221);
        assert_eq!(
            head.article().unwrap().headers.unwrap().get("Subject"),
            Some(&b"Surface Head"[..])
        );

        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(stat.status().as_u16(), 223);
        assert_eq!(
            stat.article().unwrap().message_id.as_str(),
            "<stat-surface@test>"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_execute_exchange_accepts_typed_request_directly() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"BODY <direct@test>\r\n");

            stream
                .write_all(b"222 1 <direct@test> body follows\r\ndirect body\r\n.\r\n")
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let request = Request::Body {
            message_id: MessageId::from_str_or_wrap("direct@test").unwrap(),
        };
        let exchange = client.execute_exchange(request.clone()).await.unwrap();

        assert_eq!(exchange.request(), &request);
        assert_eq!(exchange.article().status().as_u16(), 222);
        assert_eq!(
            exchange.article().article().unwrap().body,
            Some(&b"direct body\r\n"[..])
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_article_reports_unexpected_single_line_error_as_typed_error() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"ARTICLE <missing-surface@test>\r\n");

            stream
                .write_all(b"430 no article with that message-id\r\n")
                .await
                .unwrap();
        });

        let client = Client::connect(addr).await.unwrap();
        let err = client.article("missing-surface@test").await.unwrap_err();
        assert!(matches!(
            err,
            TypedClientError::UnexpectedArticleResponse { .. }
        ));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_connection_supports_concurrent_request_futures_on_one_connection() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut scratch = [0_u8; 128];
            let mut pending = Vec::new();
            while pending.iter().filter(|byte| **byte == b'\n').count() < 2 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }
            assert_eq!(
                &pending[..],
                b"ARTICLE <first@test>\r\nBODY <second@test>\r\n"
            );

            stream
                .write_all(
                    b"220 1 <first@test> article follows\r\nSubject: First\r\n\r\none\r\n.\r\n",
                )
                .await
                .unwrap();
            stream
                .write_all(b"222 1 <second@test> body follows\r\ntwo\r\n.\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let (first, second) = tokio::join!(
            connection.article(MessageId::from_str_or_wrap("first@test").unwrap()),
            connection.body(MessageId::from_str_or_wrap("second@test").unwrap())
        );

        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.status().as_u16(), 220);
        assert_eq!(second.status().as_u16(), 222);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn cloned_typed_connection_shares_one_engine() {
        let listener = crate::bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 typed ready\r\n").await.unwrap();

            let mut pending = Vec::new();
            let mut scratch = [0_u8; 128];
            while pending.iter().filter(|byte| **byte == b'\n').count() < 2 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }

            assert_eq!(
                &pending[..],
                b"ARTICLE <clone-a@test>\r\nBODY <clone-b@test>\r\n"
            );

            stream
                .write_all(
                    b"220 1 <clone-a@test> article follows\r\nSubject: Clone\r\n\r\none\r\n.\r\n",
                )
                .await
                .unwrap();
            stream
                .write_all(b"222 1 <clone-b@test> body follows\r\ntwo\r\n.\r\n")
                .await
                .unwrap();
        });

        let connection = TypedClientConnection::connect(addr).await.unwrap();
        let clone = connection.clone();
        let (first, second) = tokio::join!(
            connection.article(MessageId::from_str_or_wrap("clone-a@test").unwrap()),
            clone.body(MessageId::from_str_or_wrap("clone-b@test").unwrap())
        );

        assert_eq!(first.unwrap().status().as_u16(), 220);
        assert_eq!(second.unwrap().status().as_u16(), 222);

        server.await.unwrap();
    }
}
