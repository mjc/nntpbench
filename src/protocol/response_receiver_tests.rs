//! Characterization tests for the private scanner and semantic boundary.
use super::*;
use crate::protocol::ResponseFrameParse;
use proptest::collection::vec;
use proptest::prelude::*;
use std::cell::RefCell;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

thread_local! {
    static RUNTIME: RefCell<Option<tokio::runtime::Runtime>> = const { RefCell::new(None) };
}

struct FragmentedInput {
    bytes: Vec<u8>,
    offset: usize,
    chunk_bytes: usize,
}

impl AsyncRead for FragmentedInput {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.bytes.len().saturating_sub(self.offset);
        let count = remaining
            .min(self.chunk_bytes.max(1))
            .min(destination.remaining());
        destination.put_slice(&self.bytes[self.offset..self.offset + count]);
        self.offset += count;
        Poll::Ready(Ok(()))
    }
}

fn dangerous_wire_bytes() -> impl Strategy<Value = u8> {
    prop_oneof![
        Just(b'\r'),
        Just(b'\n'),
        Just(b'.'),
        Just(b' '),
        b'0'..=b'9',
        b'a'..=b'z',
    ]
}

fn body_content_bytes() -> impl Strategy<Value = u8> {
    prop_oneof![Just(b' '), b'0'..=b'9', b'a'..=b'z']
}

fn terminator_end_oracle(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(crate::TERMINATOR.len())
        .position(|window| window == crate::TERMINATOR)
        .map(|start| start + crate::TERMINATOR.len())
}

fn remove_rfc_multiline_terminators(buffer: &mut [u8]) {
    while let Some(start) = buffer
        .windows(crate::TERMINATOR.len())
        .position(|window| window == crate::TERMINATOR)
    {
        buffer[start + 2] = b'x';
    }
}

fn complete_after_split(kind: RequestKind, frame: &[u8], split: usize) -> (StatusCode, usize) {
    let response = receive_response(kind, frame, split.max(1))
        .unwrap_or_else(|error| panic!("receiver did not complete at split {split}: {error:?}"));
    (response.status(), response.as_bytes().len())
}

fn assert_framing_completion_reports_buffer_offset(
    kind: RequestKind,
    buffer: &[u8],
    split: usize,
    expected_frame_end: usize,
) {
    let mut decoder = ResponseDecoder::new(kind);
    match decoder
        .push_framing(&buffer[..split])
        .expect("first framing push should succeed")
    {
        FramingDecodeProgress::NeedMore => {}
        FramingDecodeProgress::Complete { .. } => {
            panic!("framing completed before the final chunk")
        }
    }

    let FramingDecodeProgress::Complete { frame_end, .. } = decoder
        .push_framing(buffer)
        .expect("second framing push should succeed")
    else {
        panic!("the final chunk should complete the response")
    };
    assert_eq!(frame_end, FrameEnd(expected_frame_end));
}

fn assert_decoder_completes_on_all_three_push_schedules(
    kind: RequestKind,
    frame: &[u8],
    expected_status: u16,
    expected_consumed: usize,
) {
    for chunk_bytes in 1..=frame.len() {
        let response = receive_response(kind, frame, chunk_bytes).unwrap_or_else(|error| {
            panic!("receiver failed with chunks of {chunk_bytes}: {error:?}")
        });
        assert_eq!(response.status().as_u16(), expected_status);
        assert_eq!(response.as_bytes().len(), expected_consumed);
    }
}

fn assert_incremental_matches_stateless_for_all_two_push_schedules(
    kind: RequestKind,
    frame: &[u8],
) {
    for chunk_bytes in 1..=frame.len().max(1) {
        let expected = ResponseFrameDecoder::new(kind).decode(frame);
        let actual = receive_response(kind, frame, chunk_bytes);
        match (expected, actual) {
            (ResponseFrameParse::Complete(expected), Ok(actual)) => {
                assert_eq!(actual.status(), expected.status());
                assert_eq!(actual.as_bytes(), &frame[..expected.consumed()]);
            }
            (ResponseFrameParse::NeedMore, Err(ClientError::UnexpectedEof)) => {}
            (expected, actual) => panic!(
                "production receiver did not match stateless frame: expected {expected:?}, got {actual:?}"
            ),
        }
    }
}

fn receive_response(
    kind: RequestKind,
    bytes: &[u8],
    chunk_bytes: usize,
) -> Result<OwnedResponse, ClientError> {
    let receiver = BufferedResponseReceiver::new(FragmentedInput {
        bytes: bytes.to_vec(),
        offset: 0,
        chunk_bytes,
    });
    RUNTIME.with(|runtime| {
        let mut runtime = runtime.borrow_mut();
        let runtime = runtime.get_or_insert_with(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build")
        });
        runtime.block_on(async move {
            let mut receiver = receiver;
            receiver.receive(kind, chunk_bytes).await
        })
    })
}

fn response_from_bytes(kind: RequestKind, status: StatusCode, bytes: &[u8]) -> OwnedResponse {
    let response = receive_fragments(kind, bytes, bytes.len()).expect("response should parse");
    assert_eq!(response.status(), status);
    response
}

#[test]
fn owned_article_requires_decoder_article_proof() {
    let mut response = response_from_bytes(
        RequestKind::Body,
        StatusCode::parse(b"222").unwrap(),
        b"222 1 <body@test> body follows\r\nbody\r\n.\r\n",
    );
    response.content = OwnedResponseContent::Generic {
        bytes: response.content.bytes().to_vec().into(),
        content: ResponseContentRange::new(0, 0, response.content.bytes().len()).unwrap(),
    };

    let Err(ClientError::UnexpectedArticleResponse { .. }) = OwnedArticle::try_from(response)
    else {
        panic!("article promotion should require decoder proof");
    };
}

#[test]
fn owned_article_access_is_infallible_after_promotion() {
    let response = response_from_bytes(
        RequestKind::Body,
        StatusCode::parse(b"222").unwrap(),
        b"222 1 <body@test> body follows\r\nbody\r\n.\r\n",
    );
    let article = OwnedArticle::try_from(response).unwrap();

    let parsed: Article<'_> = article.article();
    assert_eq!(parsed.body.as_deref(), Some(&b"body\r\n"[..]));
}

#[test]
fn owned_article_round_trip_rebuilds_the_same_response_without_reparsing() {
    let response = response_from_bytes(
        RequestKind::Body,
        StatusCode::parse(b"222").unwrap(),
        b"222 1 <body@test> body follows\r\nbody\r\n.\r\n",
    );
    let article = OwnedArticle::try_from(response.clone()).unwrap();

    assert_eq!(article.into_response(), response);
}

#[test]
fn equivalent_owned_article_responses_compare_equal_across_allocations() {
    let first = response_from_bytes(
        RequestKind::Body,
        StatusCode::parse(b"222").unwrap(),
        b"222 1 <body@test> body follows\r\nbody\r\n.\r\n",
    );
    let second = response_from_bytes(
        RequestKind::Body,
        StatusCode::parse(b"222").unwrap(),
        b"222 1 <body@test> body follows\r\nbody\r\n.\r\n",
    );

    assert_ne!(first.as_bytes().as_ptr(), second.as_bytes().as_ptr());
    assert_eq!(first, second);
}

#[test]
fn decoder_completes_single_line_error_without_waiting_for_terminator() {
    // RFC 3977 section 3.1 says the response initial line is CRLF-terminated.
    // Error statuses for ARTICLE are single-line responses, so the decoder must stop
    // after that CRLF without waiting for any multiline terminator:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
    let response = receive_response(
        RequestKind::Article,
        b"430 no article with that message-id\r\n",
        1,
    )
    .unwrap();

    assert_eq!(
        response.as_bytes().len(),
        b"430 no article with that message-id\r\n".len()
    );
    assert_eq!(response.kind(), RequestKind::Article);
    assert_eq!(response.status().as_u16(), 430);
    assert_eq!(
        response.as_bytes(),
        b"430 no article with that message-id\r\n"
    );
}

#[test]
fn decoder_compact_frames_do_not_allocate() {
    // RFC 3977 section 9.4 frames responses as either an initial response
    // line alone or that line followed by a multi-line data block.
    crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
    crate::TEST_ALLOCATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
    crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(true));

    let mut stat_decoder = StreamingResponseDecoder::new(RequestKind::Stat);
    let mut body_decoder = StreamingResponseDecoder::new(RequestKind::Body);
    assert!(matches!(
        stat_decoder.push(b"223 1 <stat@test> article retrieved\r\n"),
        Ok(StreamingDecodeProgress::Complete { status, consumed, .. })
            if status.as_u16() == 223
                && consumed
                    == DecoderChunkConsumed(b"223 1 <stat@test> article retrieved\r\n".len())
    ));
    assert!(matches!(
        body_decoder.push(b"222 1 <body@test> body follows\r\nbody\r\n.\r\n"),
        Ok(StreamingDecodeProgress::Complete { status, consumed, .. })
            if status.as_u16() == 222
                && consumed
                    == DecoderChunkConsumed(b"222 1 <body@test> body follows\r\nbody\r\n.\r\n".len())
    ));

    crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
    let allocations = crate::TEST_ALLOCATIONS.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(allocations, 0, "compact streaming decoder allocated");
}

#[test]
fn decoder_waits_for_complete_crlf_status_line() {
    // RFC 3977 section 3.1 requires CRLF, not a lone final CR, to terminate the
    // response initial line. The decoder must keep waiting until LF arrives:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
    assert!(matches!(
        receive_response(
            RequestKind::Article,
            b"430 no article with that message-id\r",
            1,
        ),
        Err(ClientError::UnexpectedEof)
    ));
    let response = receive_response(
        RequestKind::Article,
        b"430 no article with that message-id\r\n",
        1,
    )
    .unwrap();
    assert_eq!(
        response.as_bytes().len(),
        b"430 no article with that message-id\r\n".len()
    );
    assert_eq!(response.status().as_u16(), 430);
}

#[test]
fn decoder_rejects_bare_lf_status_line() {
    // RFC 3977 section 3.1 defines response lines as CRLF-terminated.
    // A bare LF before CRLF is malformed and must not be treated as a line ending:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
    for input in [
        b"430 no article with that message-id\n".as_slice(),
        b"430 no article with that message-id\nextra\r\n".as_slice(),
        b"430 no article with that message-id\n\r\n".as_slice(),
    ] {
        assert!(
            matches!(
                receive_response(RequestKind::Article, input, input.len()),
                Err(ClientError::InvalidStatusLine)
            ),
            "{input:?}"
        );
    }
}

#[test]
fn decoder_rejects_embedded_cr_in_status_line() {
    // RFC 3977 section 3.1 gives CR meaning only as the first byte of CRLF.
    // Embedded or doubled CR before the status-line terminator is invalid:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
    for input in [
        b"430 no article with that message-id\r extra\r\n".as_slice(),
        b"430 no article with that message-id\r\r\n".as_slice(),
    ] {
        assert!(
            matches!(
                receive_response(RequestKind::Article, input, input.len()),
                Err(ClientError::InvalidStatusLine)
            ),
            "{input:?}"
        );
    }
}

#[test]
fn decoder_enforces_rfc_initial_response_line_limit() {
    // RFC 3977 section 3.1 limits the response initial line to 512 octets,
    // including the status code and terminating CRLF.
    let mut exact = Vec::from(b"223 1 <stat@test> ".as_slice());
    exact.resize(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES - 2, b'x');
    exact.extend_from_slice(b"\r\n");
    assert_eq!(
        exact.len(),
        crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES
    );
    assert!(receive_response(RequestKind::Stat, &exact, exact.len()).is_ok());

    let mut too_long_complete = Vec::from(b"223 1 <stat@test> ".as_slice());
    too_long_complete.resize(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES - 1, b'x');
    too_long_complete.extend_from_slice(b"\r\n");
    assert_eq!(
        too_long_complete.len(),
        crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES + 1
    );
    assert!(matches!(
        receive_response(
            RequestKind::Stat,
            &too_long_complete,
            too_long_complete.len()
        ),
        Err(ClientError::InvalidStatusLine)
    ));

    let mut too_long_incomplete = Vec::from(b"223 1 <stat@test> ".as_slice());
    too_long_incomplete.resize(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES, b'x');
    assert!(matches!(
        receive_response(
            RequestKind::Stat,
            &too_long_incomplete,
            too_long_incomplete.len()
        ),
        Err(ClientError::InvalidStatusLine)
    ));
}

#[test]
fn decoder_accepts_rfc4643_long_authinfo_sasl_response_lines() {
    // RFC 4643 sections 2.4.1 and 7.2 allow AUTHINFO SASL 283 and 383
    // challenge response lines to exceed RFC 3977's base 512-octet response
    // initial-line limit:
    // https://www.rfc-editor.org/rfc/rfc4643#section-2.4.1
    let challenge = "A".repeat(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES);
    for (status, expected) in [("283", 283), ("383", 383)] {
        let wire = format!("{status} {challenge}\r\n");
        assert!(wire.len() > crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES);

        assert!(matches!(
            receive_response(RequestKind::AuthInfo, wire.as_bytes(), wire.len()),
            Ok(response) if response.status().as_u16() == expected
        ));

        let split = crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES;
        let mut decoder = StreamingResponseDecoder::new(RequestKind::AuthInfo);
        assert!(matches!(
            decoder.push(&wire.as_bytes()[..split]),
            Ok(StreamingDecodeProgress::NeedMore { consumed })
                if consumed == DecoderChunkConsumed(split)
        ));
        assert!(matches!(
            decoder.push(&wire.as_bytes()[split..]),
            Ok(StreamingDecodeProgress::Complete { status, consumed, .. })
                if status.as_u16() == expected
                    && consumed == DecoderChunkConsumed(wire.len() - split)
        ));
    }
}

#[test]
fn streaming_decoder_enforces_rfc_initial_response_line_limit() {
    // RFC 3977 section 3.1 applies the same 512-octet initial-line limit
    // when the line arrives across streaming chunks.
    let mut exact = Vec::from(b"223 1 <stat@test> ".as_slice());
    exact.resize(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES - 2, b'x');
    exact.extend_from_slice(b"\r\n");
    let split = 17;
    let mut decoder = StreamingResponseDecoder::new(RequestKind::Stat);
    assert!(matches!(
        decoder.push(&exact[..split]),
            Ok(StreamingDecodeProgress::NeedMore { consumed })
                if consumed == DecoderChunkConsumed(split)
    ));
    assert!(matches!(
        decoder.push(&exact[split..]),
        Ok(StreamingDecodeProgress::Complete { status, consumed, .. })
            if status.as_u16() == 223
                && consumed == DecoderChunkConsumed(exact.len() - split)
    ));

    let mut too_long = Vec::from(b"223 1 <stat@test> ".as_slice());
    too_long.resize(crate::protocol::MAX_INITIAL_RESPONSE_LINE_BYTES, b'x');
    too_long.push(b'x');
    let mut decoder = StreamingResponseDecoder::new(RequestKind::Stat);
    assert!(matches!(
        decoder.push(&too_long),
        Err(ClientError::InvalidStatusLine)
    ));
}

#[test]
fn framing_completion_reports_accumulated_single_line_offset() {
    let response = b"223 1 <stat@test> article exists\r\n";
    assert_framing_completion_reports_buffer_offset(
        RequestKind::Stat,
        response,
        response.len() - 1,
        response.len(),
    );
}

#[test]
fn framing_completion_reports_accumulated_multiline_offset() {
    let response = b"222 1 <body@test> body follows\r\nbody\r\n.\r\n";
    assert_framing_completion_reports_buffer_offset(
        RequestKind::Body,
        response,
        response.len() - 1,
        response.len(),
    );
}

#[test]
fn framing_completion_excludes_a_packed_following_response() {
    let first = b"223 1 <stat@test> article exists\r\n";
    let mut packed = first.to_vec();
    packed.extend_from_slice(b"205 closing connection\r\n");

    assert_framing_completion_reports_buffer_offset(
        RequestKind::Stat,
        &packed,
        first.len() - 1,
        first.len(),
    );
}

#[test]
fn received_article_validation_excludes_packed_following_response() {
    let first = b"222 1 <body@test> body follows\r\nwire body\r\n.\r\n";
    let mut packed = first.to_vec();
    packed.extend_from_slice(b"223 1 <next@test> article exists\r\n");

    let response = receive_response(RequestKind::Body, &packed, 1).unwrap();
    let article = response.parse_article().unwrap();

    assert_eq!(article.body.as_deref(), Some(&b"wire body\r\n"[..]));
}

#[test]
fn decoder_completes_multiline_response_across_chunks() {
    // RFC 3977 section 3.1.1 terminates multiline data with CRLF "." CRLF.
    // The decoder must retain enough state to recognize that sequence across reads:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
    let mut buffer = b"222 1 <a@b> body follows\r\nbody\r".to_vec();
    assert!(matches!(
        receive_response(RequestKind::Body, &buffer, 1),
        Err(ClientError::UnexpectedEof)
    ));
    buffer.extend_from_slice(b"\n.\r\n");
    let response = receive_response(RequestKind::Body, &buffer, 1).unwrap();

    assert_eq!(
        response.as_bytes().len(),
        b"222 1 <a@b> body follows\r\nbody\r\n.\r\n".len()
    );
    assert_eq!(response.status().as_u16(), 222);
    assert_eq!(
        response.as_bytes(),
        b"222 1 <a@b> body follows\r\nbody\r\n.\r\n"
    );
}

#[test]
fn decoder_treats_rfc2980_xhdr_221_as_multiline() {
    // RFC 2980 section 2.6 specifies XHDR as a 221 multiline response.
    // The decoder must consume through the dot line so pipelined reads do
    // not leave XHDR payload bytes in the socket buffer.
    let buffer = b"221 Header follows\r\n1 Subject\r\n.\r\nNEXT";
    let response = receive_response(RequestKind::Xhdr, buffer, buffer.len()).unwrap();

    assert_eq!(
        response.as_bytes().len(),
        b"221 Header follows\r\n1 Subject\r\n.\r\n".len()
    );
    assert_eq!(response.status().as_u16(), 221);
    assert_eq!(
        response.as_bytes(),
        b"221 Header follows\r\n1 Subject\r\n.\r\n"
    );
}

#[test]
fn decoder_completes_empty_multiline_response() {
    // RFC 3977 section 3.1.1 represents an empty multiline response as "." CRLF
    // immediately after the response initial line:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
    let buffer = b"100 help text follows\r\n.\r\n";
    let response = receive_response(RequestKind::Help, buffer, buffer.len()).unwrap();

    assert_eq!(response.as_bytes().len(), buffer.len());
    assert_eq!(response.status().as_u16(), 100);
    assert_eq!(response.as_bytes(), buffer);
}

#[test]
fn decoder_completes_empty_multiline_response_across_pushes() {
    // RFC 3977 section 3.1.1 allows the empty "." CRLF terminator to arrive in a
    // later read; the decoder must still treat it as content-start termination:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
    let mut buffer = b"100 help text follows\r\n".to_vec();
    assert!(matches!(
        receive_response(RequestKind::Help, &buffer, 1),
        Err(ClientError::UnexpectedEof)
    ));

    buffer.extend_from_slice(b".\r\n");
    let response = receive_response(RequestKind::Help, &buffer, 1).unwrap();

    assert_eq!(response.as_bytes().len(), buffer.len());
    assert_eq!(response.status().as_u16(), 100);
    assert_eq!(response.as_bytes(), buffer);
}

#[test]
fn decoder_completes_empty_multiline_response_with_split_terminator() {
    // RFC 3977 section 3.1.1 defines the empty multiline body as exactly "." CRLF.
    // This exercises all split positions inside that three-byte terminator:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
    for split in 1..3 {
        let mut buffer = b"100 help text follows\r\n".to_vec();
        buffer.extend_from_slice(&b".\r\n"[..split]);
        assert!(matches!(
            receive_response(RequestKind::Help, &buffer, 1),
            Err(ClientError::UnexpectedEof)
        ));

        buffer.extend_from_slice(&b".\r\n"[split..]);
        let response = receive_response(RequestKind::Help, &buffer, 1).unwrap();

        assert_eq!(response.as_bytes().len(), buffer.len());
        assert_eq!(response.status().as_u16(), 100);
        assert_eq!(response.as_bytes(), buffer);
    }
}

#[test]
fn decoder_does_not_treat_start_of_next_chunk_as_terminator() {
    // RFC 3977 section 3.1.1 requires CRLF before the dot line. A dot that merely
    // starts the next read after body bytes is data, not the terminator:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
    let mut buffer = b"222 1 <a@b> body follows\r\nbody".to_vec();
    assert!(matches!(
        receive_response(RequestKind::Body, &buffer, 1),
        Err(ClientError::UnexpectedEof)
    ));

    buffer.extend_from_slice(b".\r\nstill body\r\n.\r\n");
    let response = receive_response(RequestKind::Body, &buffer, 1).unwrap();

    assert_eq!(
        response.as_bytes().len(),
        b"222 1 <a@b> body follows\r\nbody.\r\nstill body\r\n.\r\n".len()
    );
    assert_eq!(response.status().as_u16(), 222);
    assert_eq!(
        response.as_bytes(),
        b"222 1 <a@b> body follows\r\nbody.\r\nstill body\r\n.\r\n"
    );
}

#[test]
fn decoder_rejects_bare_lf_before_later_crlf_status_line() {
    // RFC 3977 section 3.1 requires the response initial line to end with CRLF.
    // A malformed line like "210 foo\nboo \r\n" must fail at the bare LF instead of
    // resynchronizing on the later CRLF:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
    assert!(matches!(
        receive_response(RequestKind::Article, b"210 foo\nboo \r\n", 1),
        Err(ClientError::InvalidStatusLine)
    ));
}

#[test]
fn decoder_handles_all_three_push_schedules_for_overlapping_terminators() {
    // RFC 3977 sections 3.1 and 3.1.1 define exact response-line and multiline
    // terminators. This exhausts every three-push schedule for compact frames with
    // trailers and overlapping dot-line shapes, so state carried between pushes cannot
    // move completion earlier or later than the first RFC terminator:
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
    // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
    let single = b"430 no article\r\n222 later\r\nx\r\n";
    assert_decoder_completes_on_all_three_push_schedules(
        RequestKind::Article,
        single,
        430,
        b"430 no article\r\n".len(),
    );

    let empty = b"100 help text follows\r\n.\r\n222 later\r\n.\r\n";
    assert_decoder_completes_on_all_three_push_schedules(
        RequestKind::Help,
        empty,
        100,
        b"100 help text follows\r\n.\r\n".len(),
    );

    let non_empty = b"222 1 <a@b> body follows\r\nxx\r\n.\r\n.\r\n";
    assert_decoder_completes_on_all_three_push_schedules(
        RequestKind::Body,
        non_empty,
        222,
        b"222 1 <a@b> body follows\r\nxx\r\n.\r\n".len(),
    );

    let near_miss = b"222 1 <a@b> body follows\r\nx.foo\r\nx\r\n.\r\n";
    assert_decoder_completes_on_all_three_push_schedules(
        RequestKind::Body,
        near_miss,
        222,
        b"222 1 <a@b> body follows\r\nx.foo\r\nx\r\n.\r\n".len(),
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn decoder_consumes_single_line_response_at_rfc_crlf_for_every_split(
        trailer in vec(dangerous_wire_bytes(), 0..24),
    ) {
        // RFC 3977 section 3.1 terminates single-line responses at the status-line CRLF.
        // Extra bytes may belong to a later response, so every read split must report
        // consumption at exactly that CRLF and never include trailer bytes:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        let status_line = b"430 no article with that message-id\r\n";
        let mut frame = status_line.to_vec();
        frame.extend_from_slice(&trailer);
        remove_rfc_multiline_terminators(&mut frame);

        for split in 0..=frame.len() {
            let (status, consumed) = complete_after_split(RequestKind::Article, &frame, split);
            prop_assert_eq!(status.as_u16(), 430);
            prop_assert_eq!(
                consumed,
                status_line.len(),
                "split {} frame {:?}",
                split,
                frame,
            );
        }
    }

    #[test]
    fn decoder_consumes_empty_multiline_response_at_rfc_dot_crlf_for_every_split(
        trailer in vec(dangerous_wire_bytes(), 0..24),
    ) {
        // RFC 3977 section 3.1.1 allows an empty multiline response whose content is
        // exactly "." CRLF after the response initial line. The decoder must consume
        // that frame, across every split, and leave any trailer for the next response:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let response = b"100 help text follows\r\n.\r\n";
        let mut frame = response.to_vec();
        frame.extend_from_slice(&trailer);

        for split in 0..=frame.len() {
            let (status, consumed) = complete_after_split(RequestKind::Help, &frame, split);
            prop_assert_eq!(status.as_u16(), 100);
            prop_assert_eq!(
                consumed,
                response.len(),
                "split {} frame {:?}",
                split,
                frame,
            );
        }
    }

    #[test]
    fn decoder_consumes_non_empty_multiline_response_at_first_rfc_terminator_for_every_split(
        mut body in vec(body_content_bytes(), 0..48),
        trailer in vec(dangerous_wire_bytes(), 0..24),
    ) {
        // RFC 3977 section 3.1.1 terminates multiline data at the first CRLF "." CRLF.
        // Generated body bytes are scrubbed of that exact sequence before appending the
        // real terminator, so any early completion is a decoder bug:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        remove_rfc_multiline_terminators(&mut body);
        body.insert(0, b'x');
        body.push(b'x');
        let status_line = b"222 1 <a@b> body follows\r\n";
        let mut frame = status_line.to_vec();
        frame.extend_from_slice(&body);
        frame.extend_from_slice(crate::TERMINATOR);
        frame.extend_from_slice(&trailer);
        let expected_consumed = status_line.len() + body.len() + crate::TERMINATOR.len();

        for split in 0..=frame.len() {
            let (status, consumed) = complete_after_split(RequestKind::Body, &frame, split);
            prop_assert_eq!(status.as_u16(), 222);
            prop_assert_eq!(
                consumed,
                expected_consumed,
                "split {} frame {:?}",
                split,
                frame,
            );
        }
    }

    #[test]
    fn decoder_rejects_malformed_status_lines_before_any_later_crlf(
        before in "[0-9]{3} [A-Za-z0-9 ]{0,20}",
        after in "[A-Za-z0-9 ]{0,20}",
        bad_separator in prop::sample::select(vec![b"\n".to_vec(), b"\r ".to_vec(), b"\r\r".to_vec()]),
    ) {
        // RFC 3977 section 3.1 uses CRLF as the only response-line terminator.
        // If bare LF or non-terminal CR appears first, the decoder must reject the
        // frame immediately instead of scanning forward to a later CRLF:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        let mut frame = before.into_bytes();
        frame.extend_from_slice(&bad_separator);
        frame.extend_from_slice(after.as_bytes());
        frame.extend_from_slice(b"\r\n");

        prop_assert!(matches!(
            receive_response(RequestKind::Article, &frame, 1),
            Err(ClientError::InvalidStatusLine),
        ));
    }

    #[test]
    fn decoder_ignores_multiline_near_misses_until_first_rfc_terminator_for_every_split(
        mut prefix in vec(body_content_bytes(), 0..24),
        mut suffix in vec(body_content_bytes(), 0..24),
        near_miss in prop::sample::select(vec![
            b"..\r\n".to_vec(),
            b"x.foo\r\n".to_vec(),
            b"body.\r\n".to_vec(),
        ]),
        trailer in vec(dangerous_wire_bytes(), 0..16),
    ) {
        // RFC 3977 section 3.1.1 names only CRLF "." CRLF as the multiline
        // terminator. Dot-prefixed near misses must remain body data until the first
        // exact terminator is reached:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        remove_rfc_multiline_terminators(&mut prefix);
        remove_rfc_multiline_terminators(&mut suffix);
        let status_line = b"222 1 <a@b> body follows\r\n";
        let mut body = prefix;
        body.extend_from_slice(&near_miss);
        body.extend_from_slice(&suffix);
        remove_rfc_multiline_terminators(&mut body);
        body.insert(0, b'x');
        body.push(b'x');

        let mut frame = status_line.to_vec();
        frame.extend_from_slice(&body);
        frame.extend_from_slice(crate::TERMINATOR);
        frame.extend_from_slice(&trailer);
        let expected_consumed = status_line.len()
            + terminator_end_oracle(&frame[status_line.len()..]).expect("terminator");

        for split in 0..=frame.len() {
            let (status, consumed) = complete_after_split(RequestKind::Body, &frame, split);
            prop_assert_eq!(status.as_u16(), 222);
            prop_assert_eq!(
                consumed,
                expected_consumed,
                "split {} frame {:?}",
                split,
                frame,
            );
        }
    }

    #[test]
    fn streaming_decoder_consumes_large_multiline_response_at_rfc_terminator_across_chunk_schedules(
        response_case in 0usize..4,
        line_count in 0usize..4096,
        chunk_sizes in vec(1usize..=65536, 0..48),
        trailer in vec(dangerous_wire_bytes(), 0..64),
    ) {
        // RFC 3977 section 3.1.1: https://datatracker.ietf.org/doc/html/rfc3977#section-3.1.1
        // Multiline responses end at the first dot line. The drained streaming
        // decoder must find that exact terminator across arbitrary read chunks,
        // return the bytes consumed through the terminator, and leave trailers for
        // the next pipelined response without buffering the payload.
        let (kind, status_line, line, expected_status) = match response_case {
            0 => (
                RequestKind::Article,
                b"220 1 <article@test> article follows\r\n".as_slice(),
                b"Header: value\r\n\r\narticle body line\r\n".as_slice(),
                220,
            ),
            1 => (
                RequestKind::Body,
                b"222 1 <body@test> body follows\r\n".as_slice(),
                b"article body line for a large body response\r\n".as_slice(),
                222,
            ),
            2 => (
                RequestKind::Over,
                b"224 Overview information follows\r\n".as_slice(),
                b"1\tSubject\tposter@example.test\tFri, 15 May 2026 00:00:00 +0000\t<message@example.test>\t<ref@example.test>\t1048576\t12000\r\n".as_slice(),
                224,
            ),
            _ => (
                RequestKind::Xover,
                b"224 Overview information follows\r\n".as_slice(),
                b"2\tSubject\tposter@example.test\tFri, 15 May 2026 00:00:00 +0000\t<message@example.test>\t<ref@example.test>\t1048576\t12000\r\n".as_slice(),
                224,
            ),
        };

        let mut frame = status_line.to_vec();
        for _ in 0..line_count {
            frame.extend_from_slice(line);
        }
        frame.extend_from_slice(b".\r\n");
        let expected_consumed = frame.len();
        frame.extend_from_slice(&trailer);

        let mut decoder = StreamingResponseDecoder::new(kind);
        let mut offset = 0;
        let mut chunk_index = 0;
        let mut completed = false;

        while offset < frame.len() {
            let requested = chunk_sizes
                .get(chunk_index)
                .copied()
                .unwrap_or(frame.len() - offset);
            chunk_index += 1;
            let end = (offset + requested).min(frame.len());
            let chunk = &frame[offset..end];

            match decoder.push(chunk)? {
                StreamingDecodeProgress::NeedMore { consumed } => {
                    prop_assert_eq!(consumed, DecoderChunkConsumed(chunk.len()));
                    offset += consumed.0;
                    prop_assert!(
                        offset < expected_consumed,
                        "decoder needed more after passing RFC terminator: offset {offset} expected {expected_consumed}",
                    );
                }
                StreamingDecodeProgress::Complete { status, consumed, .. } => {
                    prop_assert_eq!(status.as_u16(), expected_status);
                    prop_assert!(consumed.0 <= chunk.len());
                    prop_assert_eq!(
                        offset + consumed.0,
                        expected_consumed,
                        "streaming decoder consumed trailer bytes or stopped before terminator",
                    );
                    completed = true;
                    break;
                }
            }
        }

        prop_assert!(completed, "streaming decoder did not complete");
    }
}

#[test]
fn decoder_reports_consumed_bytes_and_preserves_leftover_chunk_data() {
    let chunk =
            b"222 1 <a@b> body follows\r\nbody\r\n.\r\n220 1 <b@c> article follows\r\nh: v\r\n\r\nx\r\n.\r\n";

    let response = receive_response(RequestKind::Body, chunk, 1).unwrap();
    assert_eq!(response.status().as_u16(), 222);
    assert_eq!(
        response.as_bytes(),
        b"222 1 <a@b> body follows\r\nbody\r\n.\r\n"
    );

    let first_len = response.as_bytes().len();
    let second_response = receive_response(RequestKind::Article, &chunk[first_len..], 1).unwrap();
    assert_eq!(second_response.as_bytes().len(), chunk.len() - first_len);
    assert_eq!(second_response.status().as_u16(), 220);
}

#[test]
fn incremental_decoder_matches_stateless_layout_for_split_valid_and_incomplete_frames() {
    assert_incremental_matches_stateless_for_all_two_push_schedules(
        RequestKind::Body,
        b"222 1 <body@test> body follows\r\nHeader: value\r\n\r\nbody\r\n.\r\ntrailer",
    );
    assert_incremental_matches_stateless_for_all_two_push_schedules(
        RequestKind::Body,
        b"222 1 <body@test> body follows\r\nHeader: value\r\n\r\nbody\r\n",
    );
}

#[test]
fn incremental_decoder_rejects_malformed_body_like_stateless_parser() {
    let frame = b"222 1 <body@test> body follows\r\nnot an article\n\r\n.\r\n";
    assert!(matches!(
        ResponseFrameDecoder::new(RequestKind::Body).decode(frame),
        ResponseFrameParse::Invalid
    ));

    for chunk_bytes in 1..=frame.len() {
        assert!(matches!(
            receive_response(RequestKind::Body, frame, chunk_bytes),
            Err(ClientError::InvalidStatusLine)
        ));
    }
}

#[test]
fn streaming_drained_decoder_does_not_allocate_for_large_multiline_responses() {
    let mut body_decoder = StreamingResponseDecoder::new(RequestKind::Body);
    let body_status = b"222 1 <large@test> body follows\r\n";
    let mut over_decoder = StreamingResponseDecoder::new(RequestKind::Over);
    let over_status = b"224 Overview information follows\r\n";
    let body_line =
        b"This is synthetic NNTP article payload for throughput and latency benchmarking\r\n";
    let over_line = b"123456\tSubject\tposter@example.test\tFri, 15 May 2026 00:00:00 +0000\t<message@example.test>\t<ref@example.test>\t1048576\t12000\r\n";
    let terminator = b".\r\n";
    let mut bytes = 0usize;

    crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
    crate::TEST_ALLOCATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
    crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(true));

    assert!(matches!(
        body_decoder.push(body_status).unwrap(),
        StreamingDecodeProgress::NeedMore { .. }
    ));
    bytes += body_status.len();

    while bytes < 1024 * 1024 {
        assert!(matches!(
            body_decoder.push(body_line).unwrap(),
            StreamingDecodeProgress::NeedMore { .. }
        ));
        bytes += body_line.len();
    }

    let StreamingDecodeProgress::Complete { consumed, .. } = body_decoder.push(terminator).unwrap()
    else {
        panic!("streaming decoder should complete at RFC terminator");
    };
    assert_eq!(consumed, DecoderChunkConsumed(terminator.len()));

    bytes = 0;
    assert!(matches!(
        over_decoder.push(over_status).unwrap(),
        StreamingDecodeProgress::NeedMore { .. }
    ));
    bytes += over_status.len();

    while bytes < 5 * 1024 * 1024 {
        assert!(matches!(
            over_decoder.push(over_line).unwrap(),
            StreamingDecodeProgress::NeedMore { .. }
        ));
        bytes += over_line.len();
    }

    let StreamingDecodeProgress::Complete { consumed, .. } = over_decoder.push(terminator).unwrap()
    else {
        panic!("streaming OVER decoder should complete at RFC terminator");
    };
    assert_eq!(consumed, DecoderChunkConsumed(terminator.len()));

    crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
    assert_eq!(
        crate::TEST_ALLOCATIONS.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}
