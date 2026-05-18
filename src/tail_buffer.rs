//! Response line and multiline terminator helpers for NNTP responses.

pub const TERMINATOR_TAIL_SIZE: usize = 4;

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
        const EMPTY_TERMINATOR: &[u8; 3] = b".\r\n";

        let already_matched = self.prefix_len;
        if already_matched == 0 && chunk.first().is_some_and(|byte| *byte != b'.') {
            return EmptyTerminatorStatus::NotFound {
                previous_prefix_len: 0,
            };
        }

        let remaining = &EMPTY_TERMINATOR[already_matched..];
        let matched = chunk
            .iter()
            .zip(remaining.iter())
            .take_while(|(actual, expected)| actual == expected)
            .count();

        if already_matched + matched == EMPTY_TERMINATOR.len() {
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

    if tail_len >= 1
        && current_len >= 4
        && tail[tail_len - 1] == b'\r'
        && current[..4] == *b"\n.\r\n"
    {
        return Some(4);
    }
    if tail_len >= 2
        && current_len >= 3
        && tail[tail_len - 2..tail_len] == *b"\r\n"
        && current[..3] == *b".\r\n"
    {
        return Some(3);
    }
    if tail_len >= 3
        && current_len >= 2
        && tail[tail_len - 3..tail_len] == *b"\r\n."
        && current[..2] == *b"\r\n"
    {
        return Some(2);
    }
    if tail_len >= 4
        && current_len >= 1
        && tail[tail_len - 4..tail_len] == *b"\r\n.\r"
        && current[0] == b'\n'
    {
        return Some(1);
    }

    None
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

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
    fn terminator_status_reports_found_and_write_length() {
        assert!(TerminatorStatus::FoundAt(3).is_found());
        assert_eq!(TerminatorStatus::FoundAt(3).write_len(9), 3);

        assert!(!TerminatorStatus::NotFound.is_found());
        assert_eq!(TerminatorStatus::NotFound.write_len(9), 9);
    }

    #[test]
    fn detector_update_appends_until_full_then_rolls() {
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
}
