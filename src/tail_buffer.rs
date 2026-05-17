//! Tail buffer and terminator helpers for multiline NNTP responses.

pub const TERMINATOR_TAIL_SIZE: usize = 4;

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

/// Helper for tracking the last few bytes of streamed data.
///
/// Used to detect terminators that span across chunk boundaries.
#[derive(Debug, Default)]
pub struct TailBuffer {
    data: [u8; TERMINATOR_TAIL_SIZE],
    len: usize,
}

impl TailBuffer {
    /// Update tail with the last bytes from a chunk.
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

    /// Get the tail data as a slice.
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
    if slice.starts_with(b".\r\n") {
        return Some(start);
    }

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
    fn terminator_status_reports_found_and_write_length() {
        assert!(TerminatorStatus::FoundAt(3).is_found());
        assert_eq!(TerminatorStatus::FoundAt(3).write_len(9), 3);

        assert!(!TerminatorStatus::NotFound.is_found());
        assert_eq!(TerminatorStatus::NotFound.write_len(9), 9);
    }

    #[test]
    fn tail_update_appends_until_full_then_rolls() {
        let mut tail = TailBuffer::default();
        tail.update(b"ab");
        assert_eq!(tail.as_slice(), b"ab");
        assert_eq!(tail.len(), 2);

        tail.update(b"cd");
        assert_eq!(tail.as_slice(), b"abcd");
        assert_eq!(tail.len(), 4);

        tail.update(b"ef");
        assert_eq!(tail.as_slice(), b"cdef");

        tail.update(b"012345");
        assert_eq!(tail.as_slice(), b"2345");

        tail.update(b"");
        assert_eq!(tail.as_slice(), b"2345");
    }

    #[test]
    fn detect_terminator_prefers_earliest_boundary_or_chunk_match() {
        let mut tail = TailBuffer::default();
        tail.update(b"abc\r");
        assert_eq!(tail.detect_terminator(b"\n.\r\npayload").write_len(99), 4);

        let mut tail = TailBuffer::default();
        tail.update(b"abc\r");
        assert_eq!(tail.detect_terminator(b"\n.\r\n\r\n.\r\n").write_len(99), 4);

        let tail = TailBuffer::default();
        assert_eq!(tail.detect_terminator(b"abc\r\n.\r\n").write_len(99), 8);
        assert!(!tail.detect_terminator(b".\r\n").is_found());
        assert!(!tail.detect_terminator(b"abc").is_found());
    }

    #[test]
    fn spanning_terminator_handles_all_split_positions() {
        let mut tail = TailBuffer::default();
        assert_eq!(tail.find_spanning_terminator(b"\n.\r\n"), None);
        assert_eq!(find_spanning_terminator(b"", 0, b"", 0), None);

        tail.update(b"a\r");
        assert_eq!(tail.find_spanning_terminator(b"\n.\r\nx"), Some(4));

        let mut tail = TailBuffer::default();
        tail.update(b"a\r\n");
        assert_eq!(tail.find_spanning_terminator(b".\r\nx"), Some(3));

        let mut tail = TailBuffer::default();
        tail.update(b"a\r\n.");
        assert_eq!(tail.find_spanning_terminator(b"\r\nx"), Some(2));

        let mut tail = TailBuffer::default();
        tail.update(b"a\r\n.\r");
        assert_eq!(tail.find_spanning_terminator(b"\nx"), Some(1));
    }

    #[test]
    fn terminator_content_end_handles_empty_and_non_empty_content() {
        assert_eq!(find_terminator_content_end(b".\r\n", 0), Some(0));
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
}
