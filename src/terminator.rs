//! CRLF line and multiline terminator helpers for NNTP protocol frames.

pub const TERMINATOR_TAIL_SIZE: usize = 4;
pub const DOT_TERMINATOR: &[u8] = b".\r\n";

/// Status of strict NNTP response-line CRLF scanning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseLineStatus {
    /// Complete CRLF-terminated line found at this position, after the CRLF.
    CompleteAt(usize),
    /// More bytes are needed before the line can be classified.
    NeedMore,
    /// A bare LF or non-terminal CR was found before the line terminator.
    Invalid,
}

/// Status of terminator detection in a chunk.
#[derive(Debug, Clone, Copy)]
pub enum TerminatorStatus {
    /// Complete terminator found at this position, after the terminator.
    FoundAt(usize),
    /// No terminator found.
    NotFound,
}

impl TerminatorStatus {
    /// Returns true if a terminator was found.
    #[must_use]
    pub const fn is_found(self) -> bool {
        matches!(self, Self::FoundAt(_))
    }

    /// Get the number of bytes to write from the chunk.
    #[must_use]
    pub const fn write_len(self, chunk_size: usize) -> usize {
        match self {
            Self::FoundAt(pos) => pos,
            Self::NotFound => chunk_size,
        }
    }
}

/// Find a strict CRLF-terminated NNTP response line.
#[must_use]
pub fn detect_response_line_end(buffer: &[u8]) -> ResponseLineStatus {
    for index in 0..buffer.len() {
        match buffer[index] {
            b'\r' if index + 1 == buffer.len() => return ResponseLineStatus::NeedMore,
            b'\r' if buffer[index + 1] == b'\n' => {
                return ResponseLineStatus::CompleteAt(index + 2);
            }
            b'\r' | b'\n' => return ResponseLineStatus::Invalid,
            _ => {}
        }
    }

    ResponseLineStatus::NeedMore
}

/// Find a strict CRLF-terminated NNTP response line from `start`.
///
/// `CompleteAt` reports the absolute position after the CRLF.
#[must_use]
pub fn detect_response_line_end_from(buffer: &[u8], start: usize) -> ResponseLineStatus {
    let Some(slice) = buffer.get(start..) else {
        return ResponseLineStatus::NeedMore;
    };

    match detect_response_line_end(slice) {
        ResponseLineStatus::CompleteAt(end) => ResponseLineStatus::CompleteAt(start + end),
        status => status,
    }
}

/// Return the line without a final CRLF terminator.
#[must_use]
pub fn strip_crlf(line: &[u8]) -> Option<&[u8]> {
    line.strip_suffix(crate::CRLF)
}

/// Return the content of a single strict CRLF line.
///
/// Unlike [`strip_crlf`], this rejects bare LF/CR and rejects bytes after the
/// first complete CRLF line.
#[must_use]
pub fn strip_complete_crlf_line(line: &[u8]) -> Option<&[u8]> {
    match detect_response_line_end(line) {
        ResponseLineStatus::CompleteAt(end) if end == line.len() => {
            Some(&line[..line.len() - crate::CRLF.len()])
        }
        ResponseLineStatus::CompleteAt(_)
        | ResponseLineStatus::NeedMore
        | ResponseLineStatus::Invalid => None,
    }
}

/// Return the byte position after the next CRLF-terminated line.
#[must_use]
pub fn find_crlf_line_end(data: &[u8], start: usize) -> Option<usize> {
    let slice = data.get(start..)?;
    memchr::memmem::find(slice, crate::CRLF).map(|relative| start + relative + crate::CRLF.len())
}

/// Append a CRLF line terminator.
pub fn append_crlf(output: &mut Vec<u8>) {
    output.extend_from_slice(crate::CRLF);
}

/// Streaming detector for NNTP multiline response terminators.
///
/// Internally keeps enough trailing bytes to detect `\r\n.\r\n` across chunk boundaries.
#[derive(Debug, Default)]
pub struct MultilineTerminatorDetector {
    data: [u8; TERMINATOR_TAIL_SIZE],
    len: usize,
}

impl MultilineTerminatorDetector {
    /// Update detector state with the last bytes from a chunk.
    ///
    /// Maintains the last `TERMINATOR_TAIL_SIZE` bytes of the concatenation of
    /// all prior chunks. When `chunk` is smaller than `TERMINATOR_TAIL_SIZE`,
    /// the prior tail bytes are shifted to preserve the rolling window.
    pub fn update(&mut self, chunk: &[u8]) {
        if chunk.len() >= TERMINATOR_TAIL_SIZE {
            self.data
                .copy_from_slice(&chunk[chunk.len() - TERMINATOR_TAIL_SIZE..]);
            self.len = TERMINATOR_TAIL_SIZE;
        } else if !chunk.is_empty() {
            let combined_len = self.len + chunk.len();
            if combined_len >= TERMINATOR_TAIL_SIZE {
                let keep = TERMINATOR_TAIL_SIZE - chunk.len();
                self.data.copy_within(self.len - keep..self.len, 0);
                self.data[keep..keep + chunk.len()].copy_from_slice(chunk);
                self.len = TERMINATOR_TAIL_SIZE;
            } else {
                self.data[self.len..self.len + chunk.len()].copy_from_slice(chunk);
                self.len = combined_len;
            }
        }
    }

    /// Get the retained trailing data as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..self.len]
    }

    /// Get the current length of valid tail data.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the buffer is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Find spanning terminator offset in chunk.
    ///
    /// Returns the byte offset in the chunk where the terminator ends,
    /// or None if no spanning terminator is found.
    #[must_use]
    pub fn find_spanning_terminator(&self, chunk: &[u8]) -> Option<usize> {
        if self.is_empty() {
            return None;
        }
        find_spanning_terminator(self.as_slice(), self.len(), chunk, chunk.len())
    }

    /// Detect terminator in chunk, considering possible boundary spanning.
    ///
    /// Returns the earliest complete terminator touching the current chunk.
    #[must_use]
    pub fn detect_terminator(&self, chunk: &[u8]) -> TerminatorStatus {
        match (
            self.find_spanning_terminator(chunk),
            find_terminator_end(chunk),
        ) {
            (Some(spanning), Some(in_chunk)) => TerminatorStatus::FoundAt(spanning.min(in_chunk)),
            (Some(spanning), None) => TerminatorStatus::FoundAt(spanning),
            (None, Some(in_chunk)) => TerminatorStatus::FoundAt(in_chunk),
            (None, None) => TerminatorStatus::NotFound,
        }
    }
}

/// Tracks whether an empty multiline block terminator has started at content start.
#[derive(Debug, Default)]
pub struct EmptyMultilineTerminator {
    prefix_len: usize,
}

impl EmptyMultilineTerminator {
    /// Detect `.\r\n` only when the caller knows scanning is at multiline content start.
    #[must_use]
    pub fn detect(&mut self, chunk: &[u8]) -> EmptyTerminatorStatus {
        let already_matched = self.prefix_len;
        if already_matched == 0 && chunk.first().is_some_and(|byte| *byte != b'.') {
            return EmptyTerminatorStatus::NotFound {
                previous_prefix_len: 0,
            };
        }

        let remaining = &DOT_TERMINATOR[already_matched..];
        let matched = chunk
            .iter()
            .zip(remaining.iter())
            .take_while(|(actual, expected)| actual == expected)
            .count();

        if already_matched + matched == DOT_TERMINATOR.len() {
            self.prefix_len = 0;
            return EmptyTerminatorStatus::FoundAt(matched);
        }

        if matched == chunk.len() {
            self.prefix_len += matched;
            return EmptyTerminatorStatus::NeedMore;
        }

        let previous_prefix_len = self.prefix_len;
        self.prefix_len = 0;
        EmptyTerminatorStatus::NotFound {
            previous_prefix_len,
        }
    }

    /// Returns true when a prior call matched a prefix of `.\r\n`.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.prefix_len != 0
    }
}

/// Status of empty multiline terminator detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyTerminatorStatus {
    /// Complete empty terminator found at this position within the current chunk.
    FoundAt(usize),
    /// Current bytes are a prefix of `.\r\n`; more bytes are needed.
    NeedMore,
    /// The bytes did not continue the empty terminator. Any prior prefix length is returned
    /// so callers can fold those bytes into normal multiline terminator tail state.
    NotFound { previous_prefix_len: usize },
}

/// Find the position of the NNTP multiline terminator in data.
///
/// Returns the position after the terminator, or None if not found.
#[inline]
fn find_terminator_end(data: &[u8]) -> Option<usize> {
    memchr::memmem::find(data, crate::TERMINATOR).map(|start| start + crate::TERMINATOR.len())
}

/// Find the content end for a complete multiline response frame.
///
/// For `body\r\n.\r\n`, this returns the index after `body\r\n`.
/// For a response whose content is empty and starts directly with `.\r\n`,
/// this returns `start`.
#[inline]
pub(crate) fn find_terminator_content_end(data: &[u8], start: usize) -> Option<usize> {
    let slice = data.get(start..)?;
    find_terminator_end(slice).map(|end| start + end - 3)
}

/// Find the end of a dot-terminated multiline block starting at `start`.
///
/// Matches either an empty block beginning with "." CRLF or a non-empty block
/// whose first terminator is CRLF "." CRLF.
#[must_use]
pub fn find_dot_terminated_block_end(data: &[u8], start: usize) -> Option<usize> {
    let slice = data.get(start..)?;
    if slice.starts_with(DOT_TERMINATOR) {
        return Some(start + DOT_TERMINATOR.len());
    }

    memchr::memmem::find(slice, crate::TERMINATOR)
        .map(|relative| start + relative + crate::TERMINATOR.len())
}

/// Remove a final dot terminator line from a complete multiline payload.
#[must_use]
pub fn strip_dot_terminator_suffix(data: &[u8]) -> Option<&[u8]> {
    data.strip_suffix(DOT_TERMINATOR)
}

/// Append the RFC multiline terminator, adding a missing CRLF before the dot line.
pub fn append_dot_terminator(output: &mut Vec<u8>) {
    if !output.ends_with(crate::CRLF) {
        append_crlf(output);
    }
    output.extend_from_slice(DOT_TERMINATOR);
}

/// Return the target length available before a final dot terminator line.
#[must_use]
pub fn target_before_dot_terminator(target_bytes: usize) -> usize {
    target_bytes.saturating_sub(DOT_TERMINATOR.len())
}

/// Whether the current line is a valid dot terminator after a CRLF-ended prior line.
#[must_use]
pub fn is_dot_terminator_line(previous_line_ended_with_crlf: bool, line: &[u8]) -> bool {
    previous_line_ended_with_crlf && strip_crlf(line) == Some(b".".as_slice())
}

/// Iterate payload lines normalized for NNTP multiline output.
#[must_use]
pub fn crlf_normalized_payload_lines(payload: &[u8]) -> CrlfNormalizedPayloadLines<'_> {
    CrlfNormalizedPayloadLines { payload, start: 0 }
}

pub struct CrlfNormalizedPayloadLines<'a> {
    payload: &'a [u8],
    start: usize,
}

impl<'a> Iterator for CrlfNormalizedPayloadLines<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.payload.len() {
            return None;
        }

        let mut index = self.start;
        while index < self.payload.len() {
            match self.payload[index] {
                b'\r' => {
                    let line = &self.payload[self.start..index];
                    index += 1;
                    if index < self.payload.len() && self.payload[index] == b'\n' {
                        index += 1;
                    }
                    self.start = index;
                    return Some(line);
                }
                b'\n' => {
                    let line = &self.payload[self.start..index];
                    self.start = index + 1;
                    return Some(line);
                }
                _ => index += 1,
            }
        }

        let line = &self.payload[self.start..];
        self.start = self.payload.len();
        Some(line)
    }
}

/// Find spanning terminator across boundary between tail and current chunk.
#[inline]
fn find_spanning_terminator(
    tail: &[u8],
    tail_len: usize,
    current: &[u8],
    current_len: usize,
) -> Option<usize> {
    if tail_len < 1 || current_len < 1 {
        return None;
    }

    if tail_len >= 4
        && current_len >= 1
        && tail[tail_len - 4..tail_len] == *b"\r\n.\r"
        && current[0] == b'\n'
    {
        return Some(1);
    }
    if tail_len >= 3
        && current_len >= 2
        && tail[tail_len - 3..tail_len] == *b"\r\n."
        && current[..2] == *b"\r\n"
    {
        return Some(2);
    }
    if tail_len >= 2
        && current_len >= 3
        && tail[tail_len - 2..tail_len] == *b"\r\n"
        && current[..3] == *b".\r\n"
    {
        return Some(3);
    }
    if tail_len >= 1
        && current_len >= 4
        && tail[tail_len - 1] == b'\r'
        && current[..4] == *b"\n.\r\n"
    {
        return Some(4);
    }

    None
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

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

    fn strict_response_line_crlf_oracle(buffer: &[u8]) -> ResponseLineStatus {
        let mut index = 0;
        while index < buffer.len() {
            let byte = buffer[index];
            if byte == b'\n' {
                return ResponseLineStatus::Invalid;
            }
            if byte == b'\r' {
                return match buffer.get(index + 1) {
                    Some(b'\n') => ResponseLineStatus::CompleteAt(index + 2),
                    Some(_) => ResponseLineStatus::Invalid,
                    None => ResponseLineStatus::NeedMore,
                };
            }
            index += 1;
        }

        ResponseLineStatus::NeedMore
    }

    fn strict_response_line_crlf_oracle_from(buffer: &[u8], start: usize) -> ResponseLineStatus {
        let Some(slice) = buffer.get(start..) else {
            return ResponseLineStatus::NeedMore;
        };

        match strict_response_line_crlf_oracle(slice) {
            ResponseLineStatus::CompleteAt(end) => ResponseLineStatus::CompleteAt(start + end),
            status => status,
        }
    }

    fn crlf_line_end_oracle(buffer: &[u8], start: usize) -> Option<usize> {
        if start > buffer.len() {
            return None;
        }

        let mut index = start;
        while index + 1 < buffer.len() {
            if buffer[index] == b'\r' && buffer[index + 1] == b'\n' {
                return Some(index + 2);
            }
            index += 1;
        }

        None
    }

    fn terminator_end_oracle(buffer: &[u8]) -> Option<usize> {
        let mut index = 0;
        while index + 4 < buffer.len() {
            if buffer[index] == b'\r'
                && buffer[index + 1] == b'\n'
                && buffer[index + 2] == b'.'
                && buffer[index + 3] == b'\r'
                && buffer[index + 4] == b'\n'
            {
                return Some(index + 5);
            }
            index += 1;
        }

        None
    }

    fn dot_terminated_block_end_oracle(buffer: &[u8], start: usize) -> Option<usize> {
        let slice = buffer.get(start..)?;
        let empty_end =
            (slice.len() >= 3 && slice[0] == b'.' && slice[1] == b'\r' && slice[2] == b'\n')
                .then_some(start + 3);

        let non_empty_end = terminator_end_oracle(slice).map(|end| start + end);
        match (empty_end, non_empty_end) {
            (Some(empty), Some(non_empty)) => Some(empty.min(non_empty)),
            (Some(empty), None) => Some(empty),
            (None, Some(non_empty)) => Some(non_empty),
            (None, None) => None,
        }
    }

    fn strip_crlf_oracle(buffer: &[u8]) -> Option<&[u8]> {
        if buffer.len() >= 2
            && buffer[buffer.len() - 2] == b'\r'
            && buffer[buffer.len() - 1] == b'\n'
        {
            Some(&buffer[..buffer.len() - 2])
        } else {
            None
        }
    }

    fn strip_complete_crlf_line_oracle(buffer: &[u8]) -> Option<&[u8]> {
        match strict_response_line_crlf_oracle(buffer) {
            ResponseLineStatus::CompleteAt(end) if end == buffer.len() => {
                Some(&buffer[..buffer.len() - 2])
            }
            ResponseLineStatus::CompleteAt(_)
            | ResponseLineStatus::NeedMore
            | ResponseLineStatus::Invalid => None,
        }
    }

    fn strip_dot_terminator_suffix_oracle(buffer: &[u8]) -> Option<&[u8]> {
        if buffer.len() >= 3
            && buffer[buffer.len() - 3] == b'.'
            && buffer[buffer.len() - 2] == b'\r'
            && buffer[buffer.len() - 1] == b'\n'
        {
            Some(&buffer[..buffer.len() - 3])
        } else {
            None
        }
    }

    fn append_dot_terminator_oracle(mut buffer: Vec<u8>) -> Vec<u8> {
        if !matches!(buffer.as_slice(), [.., b'\r', b'\n']) {
            buffer.push(b'\r');
            buffer.push(b'\n');
        }
        buffer.push(b'.');
        buffer.push(b'\r');
        buffer.push(b'\n');
        buffer
    }

    #[allow(clippy::manual_saturating_arithmetic)]
    fn target_before_dot_terminator_oracle(target_bytes: usize) -> usize {
        target_bytes.checked_sub(3).unwrap_or_default()
    }

    fn is_dot_terminator_line_oracle(previous_line_ended_with_crlf: bool, line: &[u8]) -> bool {
        previous_line_ended_with_crlf
            && line.len() == 3
            && line[0] == b'.'
            && line[1] == b'\r'
            && line[2] == b'\n'
    }

    fn remove_rfc_multiline_terminators(buffer: &mut [u8]) {
        while let Some(start) = buffer
            .windows(crate::TERMINATOR.len())
            .position(|window| window == crate::TERMINATOR)
        {
            buffer[start + 2] = b'x';
        }
    }

    fn streaming_terminator_end(chunks: &[&[u8]]) -> Option<usize> {
        let mut detector = MultilineTerminatorDetector::default();
        let mut consumed_before_chunk = 0;

        for chunk in chunks {
            match detector.detect_terminator(chunk) {
                TerminatorStatus::FoundAt(end) => return Some(consumed_before_chunk + end),
                TerminatorStatus::NotFound => {
                    detector.update(chunk);
                    consumed_before_chunk += chunk.len();
                }
            }
        }

        None
    }

    fn streaming_terminator_oracle(chunks: &[&[u8]]) -> Option<usize> {
        let mut concatenated = Vec::new();
        for chunk in chunks {
            concatenated.extend_from_slice(chunk);
        }
        terminator_end_oracle(&concatenated)
    }

    fn assert_streaming_matches_oracle_for_one_two_three_chunks(data: &[u8]) {
        let expected = streaming_terminator_oracle(&[data]);
        assert_eq!(
            streaming_terminator_end(&[data]),
            expected,
            "one chunk {data:?}"
        );

        for first_end in 0..=data.len() {
            let chunks = [&data[..first_end], &data[first_end..]];
            assert_eq!(
                streaming_terminator_end(&chunks),
                expected,
                "two chunks split {first_end} data {data:?}",
            );
        }

        for first_end in 0..=data.len() {
            for second_end in first_end..=data.len() {
                let chunks = [
                    &data[..first_end],
                    &data[first_end..second_end],
                    &data[second_end..],
                ];
                assert_eq!(
                    streaming_terminator_end(&chunks),
                    expected,
                    "three chunks splits {first_end}/{second_end} data {data:?}",
                );
            }
        }
    }

    fn empty_multiline_streaming_end(chunks: &[&[u8]]) -> Option<usize> {
        let mut detector = EmptyMultilineTerminator::default();
        let mut consumed_before_chunk = 0;

        for chunk in chunks {
            match detector.detect(chunk) {
                EmptyTerminatorStatus::FoundAt(end) => return Some(consumed_before_chunk + end),
                EmptyTerminatorStatus::NeedMore => consumed_before_chunk += chunk.len(),
                EmptyTerminatorStatus::NotFound { .. } => return None,
            }
        }

        None
    }

    fn empty_multiline_content_start_oracle(buffer: &[u8]) -> Option<usize> {
        (buffer.len() >= 3 && buffer[0] == b'.' && buffer[1] == b'\r' && buffer[2] == b'\n')
            .then_some(3)
    }

    fn streaming_terminator_end_for_all_partitions(data: &[u8]) {
        let expected = terminator_end_oracle(data);
        let partition_count = if data.is_empty() {
            1
        } else {
            1_usize << (data.len() - 1)
        };

        for mask in 0..partition_count {
            let mut chunks = Vec::new();
            let mut start = 0;
            for boundary in 1..data.len() {
                if (mask & (1_usize << (boundary - 1))) != 0 {
                    chunks.push(&data[start..boundary]);
                    start = boundary;
                }
            }
            chunks.push(&data[start..]);

            assert_eq!(
                streaming_terminator_end(&chunks),
                expected,
                "mask {mask:b} data {data:?}",
            );
        }
    }

    #[test]
    fn response_line_end_requires_crlf() {
        // RFC 3977 section 3.1 defines the response initial line as CRLF-terminated:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        for (input, expected) in [
            (
                b"200 service ready\r\n".as_slice(),
                ResponseLineStatus::CompleteAt(b"200 service ready\r\n".len()),
            ),
            (
                b"101 Capability list:\r\nVERSION 2\r\n.\r\n".as_slice(),
                ResponseLineStatus::CompleteAt(b"101 Capability list:\r\n".len()),
            ),
            (b"430 no article\r".as_slice(), ResponseLineStatus::NeedMore),
            (b"430 no article".as_slice(), ResponseLineStatus::NeedMore),
            (b"430 no article\n".as_slice(), ResponseLineStatus::Invalid),
            (
                b"430 no article\nbody\r\n".as_slice(),
                ResponseLineStatus::Invalid,
            ),
            (
                b"430 no article\r body\r\n".as_slice(),
                ResponseLineStatus::Invalid,
            ),
            (
                b"430 no article\r\r\n".as_slice(),
                ResponseLineStatus::Invalid,
            ),
        ] {
            assert_eq!(detect_response_line_end(input), expected, "{input:?}");
        }
    }

    #[test]
    fn centralized_helpers_own_line_and_dot_terminator_edges() {
        // RFC 3977 section 3.1 makes CRLF the line terminator, and section 3.1.1
        // reserves "." CRLF as the terminating line for multiline blocks:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        // These helpers are the single API for call sites that need to strip,
        // find, append, or size around protocol terminators.
        assert_eq!(
            strip_crlf(b"211 1 1 group\r\n"),
            Some(b"211 1 1 group".as_slice())
        );
        assert_eq!(strip_crlf(b"211 1 1 group\n"), None);
        assert_eq!(
            find_crlf_line_end(b"211 ok\r\nnext", 0),
            Some(b"211 ok\r\n".len())
        );
        assert_eq!(find_crlf_line_end(b"211 ok\nnext", 0), None);
        assert_eq!(
            strip_dot_terminator_suffix(b"body\r\n.\r\n"),
            Some(b"body\r\n".as_slice())
        );
        assert_eq!(
            strip_dot_terminator_suffix(b"body\n.\r\n"),
            Some(b"body\n".as_slice())
        );

        let mut output = b"body".to_vec();
        append_dot_terminator(&mut output);
        assert_eq!(output, b"body\r\n.\r\n");
        assert_eq!(target_before_dot_terminator(2), 0);
        assert_eq!(target_before_dot_terminator(10), 7);
    }

    #[test]
    fn line_helpers_match_rfc_crlf_oracles_on_regression_cases() {
        // RFC 3977 section 3.1 defines CRLF as the line terminator:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        for input in [
            b"".as_slice(),
            b"\r".as_slice(),
            b"\n".as_slice(),
            b"\r\n".as_slice(),
            b"line\r\ntrailer".as_slice(),
            b"line\ntrailer\r\n".as_slice(),
            b"line\rtrailer\r\n".as_slice(),
            b"line\r\r\n".as_slice(),
        ] {
            assert_eq!(
                detect_response_line_end(input),
                strict_response_line_crlf_oracle(input),
                "{input:?}",
            );
            assert_eq!(strip_crlf(input), strip_crlf_oracle(input), "{input:?}");
            assert_eq!(
                strip_complete_crlf_line(input),
                strip_complete_crlf_line_oracle(input),
                "{input:?}",
            );
            for start in 0..=input.len() + 1 {
                assert_eq!(
                    detect_response_line_end_from(input, start),
                    strict_response_line_crlf_oracle_from(input, start),
                    "start {start} input {input:?}",
                );
                assert_eq!(
                    find_crlf_line_end(input, start),
                    crlf_line_end_oracle(input, start),
                    "start {start} input {input:?}",
                );
            }
        }
    }

    #[test]
    fn dot_block_helpers_match_rfc_oracles_on_regression_cases() {
        // RFC 3977 section 3.1.1 defines "." CRLF as the empty multiline block and
        // CRLF "." CRLF as the terminator after non-empty multiline content:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        for input in [
            b".\r\n".as_slice(),
            b".\r\ntrailer".as_slice(),
            b"body\r\n.\r\n".as_slice(),
            b"body\r\n.\r\ntrailer".as_slice(),
            b"body\n.\r\nbody\r\n.\r\n".as_slice(),
            b"body\r.\r\nbody\r\n.\r\n".as_slice(),
            b"body\r\n.\nbody\r\n.\r\n".as_slice(),
            b"body\r\n.\rbody\r\n.\r\n".as_slice(),
            b"body\r\n..\r\nbody\r\n.\r\n".as_slice(),
            b"body\r\n.body\r\nbody\r\n.\r\n".as_slice(),
            b"prefix.\r\n".as_slice(),
        ] {
            assert_eq!(
                strip_dot_terminator_suffix(input),
                strip_dot_terminator_suffix_oracle(input),
                "{input:?}",
            );
            for start in 0..=input.len() + 1 {
                assert_eq!(
                    find_dot_terminated_block_end(input, start),
                    dot_terminated_block_end_oracle(input, start),
                    "start {start} input {input:?}",
                );
            }
        }

        for (previous, line) in [
            (true, b".\r\n".as_slice()),
            (false, b".\r\n".as_slice()),
            (true, b".\n".as_slice()),
            (true, b".\r".as_slice()),
            (true, b"..\r\n".as_slice()),
            (true, b".body\r\n".as_slice()),
        ] {
            assert_eq!(
                is_dot_terminator_line(previous, line),
                is_dot_terminator_line_oracle(previous, line),
                "previous {previous} line {line:?}",
            );
        }
    }

    #[test]
    fn append_and_target_helpers_match_rfc_oracles_on_regression_cases() {
        // RFC 3977 sections 3.1 and 3.1.1 require CRLF line endings and "." CRLF
        // multiline termination:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        for input in [
            b"".as_slice(),
            b"body".as_slice(),
            b"body\r".as_slice(),
            b"body\n".as_slice(),
            b"body\r\n".as_slice(),
            b"body\r\n.\r\n".as_slice(),
        ] {
            let mut crlf_output = input.to_vec();
            append_crlf(&mut crlf_output);
            let mut expected_crlf = input.to_vec();
            expected_crlf.extend_from_slice(b"\r\n");
            assert_eq!(crlf_output, expected_crlf, "{input:?}");

            let mut dot_output = input.to_vec();
            append_dot_terminator(&mut dot_output);
            assert_eq!(
                dot_output,
                append_dot_terminator_oracle(input.to_vec()),
                "{input:?}",
            );
        }

        for target_bytes in 0..8 {
            assert_eq!(
                target_before_dot_terminator(target_bytes),
                target_before_dot_terminator_oracle(target_bytes),
            );
        }
    }

    #[test]
    fn terminator_status_reports_found_and_write_length() {
        // RFC 3977 section 3.1.1 frames multiline data with a finite terminator.
        // This status helper must report the exact writable prefix before that terminator:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        assert!(TerminatorStatus::FoundAt(3).is_found());
        assert_eq!(TerminatorStatus::FoundAt(3).write_len(9), 3);

        assert!(!TerminatorStatus::NotFound.is_found());
        assert_eq!(TerminatorStatus::NotFound.write_len(9), 9);
    }

    #[test]
    fn detector_update_appends_until_full_then_rolls() {
        // RFC 3977 section 3.1.1 allows CRLF "." CRLF to span reads, so the detector
        // keeps the final four bytes needed to match the next byte of that sequence:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut detector = MultilineTerminatorDetector::default();
        detector.update(b"ab");
        assert_eq!(detector.as_slice(), b"ab");
        assert_eq!(detector.len(), 2);

        detector.update(b"cd");
        assert_eq!(detector.as_slice(), b"abcd");
        assert_eq!(detector.len(), 4);

        detector.update(b"ef");
        assert_eq!(detector.as_slice(), b"cdef");

        detector.update(b"012345");
        assert_eq!(detector.as_slice(), b"2345");

        detector.update(b"");
        assert_eq!(detector.as_slice(), b"2345");
    }

    #[test]
    fn detect_terminator_prefers_earliest_boundary_or_chunk_match() {
        // RFC 3977 section 3.1.1 defines non-empty multiline termination as CRLF "." CRLF:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut detector = MultilineTerminatorDetector::default();
        detector.update(b"abc\r");
        assert_eq!(
            detector.detect_terminator(b"\n.\r\npayload").write_len(99),
            4
        );

        let mut detector = MultilineTerminatorDetector::default();
        detector.update(b"abc\r");
        assert_eq!(
            detector
                .detect_terminator(b"\n.\r\n\r\n.\r\n")
                .write_len(99),
            4
        );

        let detector = MultilineTerminatorDetector::default();
        assert_eq!(detector.detect_terminator(b"abc\r\n.\r\n").write_len(99), 8);
        assert!(!detector.detect_terminator(b".\r\n").is_found());
        assert!(!detector.detect_terminator(b"abc").is_found());
    }

    #[test]
    fn spanning_terminator_handles_all_split_positions() {
        // RFC 3977 section 3.1.1: the terminator can arrive split across TCP reads.
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut detector = MultilineTerminatorDetector::default();
        assert_eq!(detector.find_spanning_terminator(b"\n.\r\n"), None);
        assert_eq!(find_spanning_terminator(b"", 0, b"", 0), None);

        detector.update(b"a\r");
        assert_eq!(detector.find_spanning_terminator(b"\n.\r\nx"), Some(4));

        let mut detector = MultilineTerminatorDetector::default();
        detector.update(b"a\r\n");
        assert_eq!(detector.find_spanning_terminator(b".\r\nx"), Some(3));

        let mut detector = MultilineTerminatorDetector::default();
        detector.update(b"a\r\n.");
        assert_eq!(detector.find_spanning_terminator(b"\r\nx"), Some(2));

        let mut detector = MultilineTerminatorDetector::default();
        detector.update(b"a\r\n.\r");
        assert_eq!(detector.find_spanning_terminator(b"\nx"), Some(1));

        // RFC 3977 section 3.1.1 requires the first complete CRLF "." CRLF to win.
        // When one terminator is completed by the first current byte and another starts
        // immediately after it, the boundary completion must be preferred:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut detector = MultilineTerminatorDetector::default();
        detector.update(b"xx\r\n.\r");
        assert_eq!(detector.find_spanning_terminator(b"\n.\r\n"), Some(1));
        assert_eq!(detector.detect_terminator(b"\n.\r\n").write_len(99), 1);
    }

    #[test]
    fn terminator_detection_handles_all_partitions_of_overlapping_sequences() {
        // RFC 3977 section 3.1.1 requires the first complete CRLF "." CRLF to terminate
        // a multiline block. These compact byte strings contain overlapping terminator
        // starts, near misses, and immediate trailers; every possible read partition must
        // produce the same earliest endpoint as the full-buffer oracle:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        for data in [
            b"\r\n.\r\n".as_slice(),
            b"x\r\n.\r\n".as_slice(),
            b"xx\r\n.\r\n".as_slice(),
            b"xx\r\n.\r\n.\r\n".as_slice(),
            b"x\r\n.\r\nx".as_slice(),
            b"x\r\n.\r\n.\r\nx".as_slice(),
            b"x\n.\r\n\r\n.\r\n".as_slice(),
            b"x\r\n.\n\r\n.\r\n".as_slice(),
            b"x\r\n.\r\r\n.\r\n".as_slice(),
            b"\r\n.\r\n\r\n.\r\n".as_slice(),
            b"\r\n.\r\n.\r\n".as_slice(),
            b"\r\n.\r\nx\r\n.\r\n".as_slice(),
        ] {
            streaming_terminator_end_for_all_partitions(data);
            assert_streaming_matches_oracle_for_one_two_three_chunks(data);
        }
    }

    #[test]
    fn terminator_content_end_handles_empty_and_non_empty_content() {
        // Full-frame parsing uses CRLF "." CRLF for non-empty content; the empty-content
        // "." CRLF case is intentionally handled by EmptyMultilineTerminator.
        // RFC 3977 section 3.1.1: https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        assert_eq!(find_terminator_content_end(b".\r\n", 0), None);
        assert_eq!(
            find_terminator_content_end(b"body\r\n.\r\nnext", 0),
            Some(b"body\r\n".len())
        );
        assert_eq!(
            find_terminator_content_end(b"xxbody\r\n.\r\n", 2),
            Some(2 + b"body\r\n".len())
        );
        assert_eq!(find_terminator_content_end(b"body", 0), None);
    }

    #[test]
    fn terminator_detection_requires_crlf_sequences() {
        // Bare LF and bare CR are not NNTP line terminators; multiline termination requires
        // the exact CRLF "." CRLF sequence from RFC 3977 section 3.1.1.
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let detector = MultilineTerminatorDetector::default();
        for data in [
            b"line1\nline2\n.\n".as_slice(),
            b"line1\rline2\r.\r".as_slice(),
            b"line1\r\nline2\n.\r\n".as_slice(),
            b"line1\r\n.\n".as_slice(),
            b"line1\r\n.\r".as_slice(),
        ] {
            assert!(!detector.detect_terminator(data).is_found(), "{data:?}");
            assert_eq!(find_terminator_content_end(data, 0), None, "{data:?}");
        }
    }

    #[test]
    fn empty_multiline_terminator_tracks_split_prefixes() {
        // RFC 3977 section 3.1.1 defines an empty multiline block as "." CRLF immediately
        // after the response initial line. This detector only handles that content-start case.
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut detector = EmptyMultilineTerminator::default();
        assert_eq!(detector.detect(b"."), EmptyTerminatorStatus::NeedMore);
        assert_eq!(detector.detect(b"\r"), EmptyTerminatorStatus::NeedMore);
        assert_eq!(
            detector.detect(b"\nbody"),
            EmptyTerminatorStatus::FoundAt(1)
        );

        let mut detector = EmptyMultilineTerminator::default();
        assert_eq!(
            detector.detect(b".x"),
            EmptyTerminatorStatus::NotFound {
                previous_prefix_len: 0
            }
        );

        let mut detector = EmptyMultilineTerminator::default();
        assert_eq!(detector.detect(b"."), EmptyTerminatorStatus::NeedMore);
        assert_eq!(
            detector.detect(b"x"),
            EmptyTerminatorStatus::NotFound {
                previous_prefix_len: 1
            }
        );
    }

    #[test]
    fn empty_multiline_terminator_handles_all_split_positions() {
        // RFC 3977 section 3.1.1 empty multiline block: "." CRLF.
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        for split in 0..3 {
            let mut detector = EmptyMultilineTerminator::default();
            let terminator = b".\r\n";

            if split > 0 {
                assert_eq!(
                    detector.detect(&terminator[..split]),
                    EmptyTerminatorStatus::NeedMore,
                    "split {split}"
                );
            }

            assert_eq!(
                detector.detect(&terminator[split..]),
                EmptyTerminatorStatus::FoundAt(terminator.len() - split),
                "split {split}"
            );
        }
    }

    #[test]
    fn empty_multiline_terminator_does_not_match_non_empty_content() {
        // RFC 3977 section 3.1.1 uses "." CRLF only for an empty block or a dot line.
        // If the content starts with any other byte, normal CRLF "." CRLF scanning must handle it.
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        for input in [
            b"body\r\n.\r\n".as_slice(),
            b"..dot-stuffed\r\n.\r\n".as_slice(),
            b".not-a-terminator\r\n.\r\n".as_slice(),
        ] {
            assert_eq!(
                EmptyMultilineTerminator::default().detect(input),
                EmptyTerminatorStatus::NotFound {
                    previous_prefix_len: 0,
                },
                "{input:?}"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]

        #[test]
        fn crlf_line_helpers_match_independent_oracles(
            input in vec(dangerous_wire_bytes(), 0..80),
            start in 0_usize..96,
        ) {
            // RFC 3977 section 3.1 requires CRLF line termination. These central helpers
            // must agree with independent byte-by-byte oracles for arbitrary buffers and
            // arbitrary caller offsets:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            prop_assert_eq!(
                detect_response_line_end(&input),
                strict_response_line_crlf_oracle(&input),
            );
            prop_assert_eq!(
                detect_response_line_end_from(&input, start),
                strict_response_line_crlf_oracle_from(&input, start),
            );
            prop_assert_eq!(strip_crlf(&input), strip_crlf_oracle(&input));
            prop_assert_eq!(
                strip_complete_crlf_line(&input),
                strip_complete_crlf_line_oracle(&input),
            );
            prop_assert_eq!(
                find_crlf_line_end(&input, start),
                crlf_line_end_oracle(&input, start),
            );
        }

        #[test]
        fn dot_block_helpers_match_independent_oracles(
            prefix in vec(dangerous_wire_bytes(), 0..16),
            mut body in vec(dangerous_wire_bytes(), 0..64),
            trailer in vec(dangerous_wire_bytes(), 0..16),
            start_offset in 0_usize..20,
            terminator_case in 0_u8..3,
        ) {
            // RFC 3977 section 3.1.1 defines the multiline terminator as "." CRLF at
            // content start for an empty block, or CRLF "." CRLF after non-empty content.
            // The block finder must honor caller offsets, near misses, and trailers:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            remove_rfc_multiline_terminators(&mut body);

            let mut input = prefix;
            let start = input.len().min(start_offset);
            input.extend_from_slice(&body);
            match terminator_case {
                0 => input.extend_from_slice(b".\r\n"),
                1 => input.extend_from_slice(b"\r\n.\r\n"),
                _ => input.extend_from_slice(b"\r\n.\n"),
            }
            input.extend_from_slice(&trailer);

            prop_assert_eq!(
                find_dot_terminated_block_end(&input, start),
                dot_terminated_block_end_oracle(&input, start),
            );
            prop_assert_eq!(
                strip_dot_terminator_suffix(&input),
                strip_dot_terminator_suffix_oracle(&input),
            );
        }

        #[test]
        fn append_and_small_predicate_helpers_match_independent_oracles(
            input in vec(dangerous_wire_bytes(), 0..80),
            target_bytes in any::<usize>(),
            previous_line_ended_with_crlf in any::<bool>(),
            line in vec(dangerous_wire_bytes(), 0..12),
        ) {
            // RFC 3977 sections 3.1 and 3.1.1 require CRLF line endings and "." CRLF
            // multiline termination. Append, sizing, and dot-line predicates should match
            // simple output/predicate oracles over arbitrary bytes:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            let mut crlf_output = input.clone();
            append_crlf(&mut crlf_output);
            let mut expected_crlf = input.clone();
            expected_crlf.push(b'\r');
            expected_crlf.push(b'\n');
            prop_assert_eq!(crlf_output, expected_crlf);

            let mut dot_output = input.clone();
            append_dot_terminator(&mut dot_output);
            prop_assert_eq!(dot_output, append_dot_terminator_oracle(input));

            prop_assert_eq!(
                target_before_dot_terminator(target_bytes),
                target_before_dot_terminator_oracle(target_bytes),
            );
            prop_assert_eq!(
                is_dot_terminator_line(previous_line_ended_with_crlf, &line),
                is_dot_terminator_line_oracle(previous_line_ended_with_crlf, &line),
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn response_line_end_matches_rfc_crlf_oracle(
            prefix in vec(dangerous_wire_bytes(), 0..32),
            suffix in vec(dangerous_wire_bytes(), 0..16),
            add_crlf in any::<bool>(),
        ) {
            // RFC 3977 section 3.1 requires the response initial line to end with CRLF.
            // The property compares the implementation against an independent oracle so
            // bare LF, embedded CR, and incomplete final CR cannot be accepted accidentally:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let mut input = prefix;
            if add_crlf {
                input.extend_from_slice(b"\r\n");
            }
            input.extend_from_slice(&suffix);

            prop_assert_eq!(
                detect_response_line_end(&input),
                strict_response_line_crlf_oracle(&input),
            );
        }

        #[test]
        fn non_empty_multiline_terminator_matches_rfc_oracle_for_every_split(
            mut body in vec(dangerous_wire_bytes(), 0..48),
            trailer in vec(dangerous_wire_bytes(), 0..16),
        ) {
            // RFC 3977 section 3.1.1 terminates non-empty multiline response data with
            // the first exact CRLF "." CRLF sequence. This exercises every TCP read split
            // for each generated byte stream and requires the streaming detector to stop
            // at the same earliest terminator as the oracle:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            remove_rfc_multiline_terminators(&mut body);
            body.insert(0, b'x');
            body.push(b'x');
            body.extend_from_slice(crate::TERMINATOR);
            body.extend_from_slice(&trailer);
            let expected = terminator_end_oracle(&body);

            for split in 0..=body.len() {
                prop_assert_eq!(
                    streaming_terminator_end(&[&body[..split], &body[split..]]),
                    expected,
                    "split {} body {:?}",
                    split,
                    body,
                );
            }
        }

        #[test]
        fn compact_multiline_terminator_matches_rfc_oracle_for_one_two_three_chunks(
            prefix in vec(dangerous_wire_bytes(), 0..8),
            mut body in vec(dangerous_wire_bytes(), 0..16),
            trailer in vec(dangerous_wire_bytes(), 0..8),
            near_miss in prop::sample::select(vec![
                b"".to_vec(),
                b"\n.\r\n".to_vec(),
                b"\r.\r\n".to_vec(),
                b"\r\n.\n".to_vec(),
                b"\r\n.\r".to_vec(),
                b"..\r\n".to_vec(),
                b".body\r\n".to_vec(),
            ]),
        ) {
            // RFC 3977 section 3.1.1 requires the first complete CRLF "." CRLF to win,
            // regardless of whether it is split across one, two, or three reads. Compact
            // generated buffers keep the nested split schedule exhaustive:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            remove_rfc_multiline_terminators(&mut body);
            let mut input = prefix;
            input.extend_from_slice(&body);
            input.extend_from_slice(&near_miss);
            input.extend_from_slice(b"\r\n.\r\n");
            input.extend_from_slice(&trailer);

            assert_streaming_matches_oracle_for_one_two_three_chunks(&input);
        }

        #[test]
        fn multiline_terminator_rejects_near_misses_for_every_split(
            prefix in vec(dangerous_wire_bytes(), 0..24),
            suffix in vec(dangerous_wire_bytes(), 0..24),
            near_miss in prop::sample::select(vec![
                b"\n.\r\n".to_vec(),
                b"\r.\r\n".to_vec(),
                b"\r\n.\n".to_vec(),
                b"\r\n.\r".to_vec(),
                b".\r\n".to_vec(),
                b".foo\r\n".to_vec(),
                b"..\r\n".to_vec(),
                b"body.\r\n".to_vec(),
            ]),
        ) {
            // RFC 3977 section 3.1.1 names one terminator byte sequence: CRLF "." CRLF.
            // These generated buffers contain tempting partial or dot-prefixed shapes, but
            // must not be treated as terminators unless the exact sequence appears elsewhere:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            let mut input = prefix;
            input.extend_from_slice(&near_miss);
            input.extend_from_slice(&suffix);
            let expected = terminator_end_oracle(&input);

            for split in 0..=input.len() {
                prop_assert_eq!(
                    streaming_terminator_end(&[&input[..split], &input[split..]]),
                    expected,
                    "split {} input {:?}",
                    split,
                    input,
                );
            }
        }

        #[test]
        fn terminator_content_end_matches_non_empty_rfc_oracle(
            prefix in vec(dangerous_wire_bytes(), 0..12),
            mut content in vec(dangerous_wire_bytes(), 0..48),
            trailer in vec(dangerous_wire_bytes(), 0..12),
        ) {
            // RFC 3977 section 3.1.1 excludes the terminator itself from multiline content.
            // For a complete non-empty frame, content ends immediately after the CRLF before
            // the dot line; the empty "." CRLF case remains a separate detector concern:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            content.extend_from_slice(crate::TERMINATOR);
            content.extend_from_slice(&trailer);
            let mut input = prefix;
            let start = input.len();
            input.extend_from_slice(&content);

            let expected = terminator_end_oracle(&input[start..]).map(|end| start + end - 3);
            prop_assert_eq!(find_terminator_content_end(&input, start), expected);
        }

        #[test]
        fn empty_multiline_terminator_matches_content_start_rfc_oracle(
            suffix in vec(dangerous_wire_bytes(), 0..16),
            split in 0_usize..=3,
        ) {
            // RFC 3977 section 3.1.1 allows an empty multiline response to be represented by
            // "." CRLF immediately after the response initial line. The empty detector must
            // accept every split of exactly that content-start sequence and consume no trailer:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            let mut input = b".\r\n".to_vec();
            input.extend_from_slice(&suffix);
            let mut detector = EmptyMultilineTerminator::default();

            let first = &input[..split];
            let second = &input[split..];
            if split == 3 {
                prop_assert_eq!(detector.detect(first), EmptyTerminatorStatus::FoundAt(3));
            } else {
                if !first.is_empty() {
                    prop_assert_eq!(detector.detect(first), EmptyTerminatorStatus::NeedMore);
                }
                prop_assert_eq!(
                    detector.detect(second),
                    EmptyTerminatorStatus::FoundAt(3 - split),
                    "split {} input {:?}",
                    split,
                    input,
                );
            }
        }

        #[test]
        fn empty_multiline_terminator_matches_content_start_oracle_for_one_two_three_chunks(
            trailer in vec(dangerous_wire_bytes(), 0..8),
        ) {
            // RFC 3977 section 3.1.1 represents an empty multiline response as "." CRLF
            // immediately at content start. This exhausts one-, two-, and three-read
            // schedules for the compact content-start buffer:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            let mut input = b".\r\n".to_vec();
            input.extend_from_slice(&trailer);
            let expected = empty_multiline_content_start_oracle(&input);

            prop_assert_eq!(empty_multiline_streaming_end(&[&input]), expected);
            for first_end in 0..=input.len() {
                prop_assert_eq!(
                    empty_multiline_streaming_end(&[&input[..first_end], &input[first_end..]]),
                    expected,
                    "two chunks split {} input {:?}",
                    first_end,
                    input,
                );
            }
            for first_end in 0..=input.len() {
                for second_end in first_end..=input.len() {
                    prop_assert_eq!(
                        empty_multiline_streaming_end(&[
                            &input[..first_end],
                            &input[first_end..second_end],
                            &input[second_end..],
                        ]),
                        expected,
                        "three chunks splits {}/{} input {:?}",
                        first_end,
                        second_end,
                        input,
                    );
                }
            }
        }

        #[test]
        fn empty_multiline_terminator_rejects_non_empty_dot_prefixed_content(
            suffix in vec(dangerous_wire_bytes(), 0..16),
            bad_start in prop::sample::select(vec![
                b".\n".to_vec(),
                b".\r ".to_vec(),
                b".x".to_vec(),
                b"..".to_vec(),
                b".body\r\n".to_vec(),
            ]),
        ) {
            // RFC 3977 section 3.1.1 gives "." CRLF special meaning only as the complete
            // empty content-start terminator. Other dot-prefixed starts are data or malformed
            // fragments and must not be accepted as complete empty responses:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            let mut input = bad_start;
            input.extend_from_slice(&suffix);

            prop_assert_ne!(
                EmptyMultilineTerminator::default().detect(&input),
                EmptyTerminatorStatus::FoundAt(3),
                "{:?}",
                input,
            );
        }
    }
}
