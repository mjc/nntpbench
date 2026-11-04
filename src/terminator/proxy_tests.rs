//! Production scanner cases ported from nntp-proxy's multiline_framing.rs
//! at f885ea4a. Experimental scanner variants and proxy queue/write policies
//! remain with the proxy; these cases exercise nntpbench's shared scanner.

use super::*;
use crate::TERMINATOR;

fn check(data: &[u8]) {
    let expected = data
        .windows(crate::TERMINATOR.len())
        .position(|bytes| bytes == crate::TERMINATOR)
        .map(|start| start + crate::TERMINATOR.len());
    assert_eq!(find_terminator_end(data), expected, "raw input: {data:?}");
    assert_eq!(
        find_streaming_terminator_end(data),
        expected,
        "streaming input: {data:?}"
    );
    #[cfg(any(
        target_arch = "aarch64",
        all(target_arch = "x86_64", target_feature = "avx2")
    ))]
    assert_eq!(
        vector_scanner::find(data).map(|start| start + TERMINATOR.len()),
        expected,
        "vector scan: {data:?}",
    );
}

fn scan(data: &[u8]) -> (usize, usize) {
    let mut remaining = data;
    let mut count = 0;
    let mut checksum = 0usize;
    while let Some(end) = find_streaming_terminator_end(remaining) {
        remaining = &remaining[end..];
        count += 1;
        checksum = checksum.wrapping_add(data.len() - remaining.len());
    }
    (count, checksum)
}

fn benchmark_memchr_scanner(data: &[u8]) -> (usize, usize) {
    memchr::memmem::find_iter(data, TERMINATOR).fold((0, 0usize), |(count, checksum), start| {
        (count + 1, checksum.wrapping_add(start + TERMINATOR.len()))
    })
}
pub(crate) struct ScannerBenchmarkInput {
    bytes: Vec<u8>,
    expected: (usize, usize),
    pub(crate) requests: Option<Vec<(crate::protocol::RequestKind, usize)>>,
}

impl ScannerBenchmarkInput {
    pub fn new(body_len: usize, articles: usize) -> Self {
        let mut bytes = Vec::new();
        let mut checksum = 0usize;
        let mut requests = Vec::new();
        let mut random = 0x7ac3_85d1u32;
        for article in 0..articles {
            bytes.extend_from_slice(
                format!("222 {article} <scanner-{article}@example.com>\r\n").as_bytes(),
            );
            let mut written = 0;
            while written < body_len {
                let line_len = (body_len - written).min(128);
                for column in 0..line_len {
                    random ^= random << 13;
                    random ^= random >> 17;
                    random ^= random << 5;
                    // Varied printable bytes with ordinary dots and stuffed lines.
                    let byte = if column == 0 && written % 4096 == 0 {
                        b'.'
                    } else {
                        33 + u8::try_from(random % 94).expect("printable byte")
                    };
                    if column == 0 && byte == b'.' {
                        bytes.push(b'.');
                    }
                    bytes.push(byte);
                }
                bytes.extend_from_slice(b"\r\n");
                written += line_len;
            }
            bytes.extend_from_slice(b".\r\n");
            checksum = checksum.wrapping_add(bytes.len());
            requests.push((crate::protocol::RequestKind::Body, bytes.len()));
        }
        Self {
            bytes,
            expected: (articles, checksum),
            requests: Some(requests),
        }
    }

    pub fn new_yenc(decoded_len: usize, articles: usize) -> Self {
        let mut random = 0x7ac3_85d1u32;
        let decoded: Vec<u8> = (0..decoded_len)
            .map(|_| {
                random ^= random << 13;
                random ^= random >> 17;
                random ^= random << 5;
                random.to_le_bytes()[0]
            })
            .collect();
        let length = u64::try_from(decoded_len).expect("fixture length fits u64");
        let mut bytes = Vec::new();
        let mut checksum = 0usize;
        let mut requests = Vec::new();
        for article in 0..articles {
            bytes.extend_from_slice(
                format!("222 {article} <yenc-{article}@example.com>\r\n").as_bytes(),
            );
            yenc::EncodeOptions::new()
                .begin(1)
                .end(length)
                .encode_stream(
                    std::io::Cursor::new(&decoded),
                    &mut bytes,
                    length,
                    "scanner.bin",
                )
                .expect("encode binary fixture");
            bytes.extend_from_slice(b".\r\n");
            checksum = checksum.wrapping_add(bytes.len());
            requests.push((crate::protocol::RequestKind::Body, bytes.len()));
        }
        Self {
            bytes,
            expected: (articles, checksum),
            requests: Some(requests),
        }
    }

    pub fn mixed_yenc(decoded_len: usize, articles: usize) -> Self {
        use crate::protocol::RequestKind;
        let article = Self::new_yenc(decoded_len, 1);
        let mut bytes = Vec::new();
        let mut checksum = 0usize;
        let mut requests = Vec::new();
        for _ in 0..articles {
            for (reply, kind) in [
                (
                    b"223 42 <stat@example.com>\r\n".as_slice(),
                    RequestKind::Stat,
                ),
                (
                    b"211 100 1 100 alt.binaries.test\r\n".as_slice(),
                    RequestKind::Group,
                ),
                (b"430 No such article\r\n".as_slice(), RequestKind::Body),
            ] {
                bytes.extend_from_slice(reply);
                requests.push((kind, bytes.len()));
            }
            bytes.extend_from_slice(article.bytes());
            checksum = checksum.wrapping_add(bytes.len());
            requests.push((RequestKind::Body, bytes.len()));
            bytes.extend_from_slice(b"111 20261022120000\r\n");
            requests.push((RequestKind::Date, bytes.len()));
        }
        Self {
            bytes,
            expected: (articles, checksum),
            requests: Some(requests),
        }
    }

    pub fn first_read(&self, capacity: usize) -> Self {
        let mut sample = Self::read_sample(&self.bytes[..self.bytes.len().min(capacity)]);
        sample.requests.clone_from(&self.requests);
        sample
    }

    pub fn final_read(&self, capacity: usize) -> Self {
        Self::read_sample(&self.bytes[self.bytes.len().saturating_sub(capacity)..])
    }

    fn read_sample(data: &[u8]) -> Self {
        Self {
            bytes: data.to_vec(),
            expected: benchmark_memchr_scanner(data),
            requests: None,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn expected(&self) -> (usize, usize) {
        self.expected
    }
}

fn check_lane_alignments(alignments: std::ops::Range<usize>) {
    for len in 0usize..544 {
        for alignment in alignments.clone() {
            let storage = vec![b'x'; alignment + len];
            check(&storage[alignment..]);
        }
    }
    // Cover every candidate lane across two blocks, including matches
    // crossing each block end, without repeating every shorter prefix.
    for len in [31usize, 68, 132, 260, 516] {
        for alignment in alignments.clone() {
            let mut storage = vec![b'x'; alignment + len];
            for start in 0..len.saturating_sub(4) {
                storage[alignment + start..alignment + start + 5].copy_from_slice(TERMINATOR);
                check(&storage[alignment..]);
                for byte in 0..5 {
                    storage[alignment + start + byte] ^= 0x80;
                    check(&storage[alignment..]);
                    storage[alignment + start + byte] ^= 0x80;
                }
                storage[alignment + start..alignment + start + 5].fill(b'x');
            }
        }
    }
}

#[test]
fn simd_scanners_match_at_every_lane_alignment_and_tail() {
    check_lane_alignments(0..16);
}

#[test]
fn simd_scanners_match_alignments_16_through_31() {
    check_lane_alignments(16..32);
}

#[test]
fn simd_scanners_match_alignments_32_through_47() {
    check_lane_alignments(32..48);
}

#[test]
fn simd_scanners_match_alignments_48_through_63() {
    check_lane_alignments(48..64);
}

#[test]
fn simd_scanners_match_packed_stream_checksums() {
    for len in [1, 15, 16, 17, 31, 64, 127, 128, 1024, 65536] {
        let input = ScannerBenchmarkInput::new(len, 8);
        assert_eq!(scan(input.bytes()), input.expected());
    }
}

#[test]
fn simd_scanners_match_encoded_articles_and_read_samples() {
    for decoded_len in [1, 128, 716_800, 768_000] {
        let mixed = ScannerBenchmarkInput::mixed_yenc(decoded_len, 2);
        assert_eq!(scan(mixed.bytes()), mixed.expected());
        check(mixed.bytes());
        let input = ScannerBenchmarkInput::new_yenc(decoded_len, 2);
        assert_eq!(scan(input.bytes()), input.expected());
        check(input.bytes());
        for capacity in [4096, 16 * 1024, 64 * 1024, 1024 * 1024] {
            check(input.first_read(capacity).bytes());
            check(input.final_read(capacity).bytes());
        }
    }
}

proptest::proptest! {
    #[test]
    fn simd_scanners_match_arbitrary_input(data in proptest::collection::vec(proptest::num::u8::ANY, 0..4096)) {
        check(&data);
    }

    #[test]
    fn simd_scanners_reject_or_predicate_false_positives(data in proptest::collection::vec(
        proptest::sample::select(vec![0, 1, 2, 4, 5, 8, 9, 12, 13, b'\n', b'.', b'x']), 0..4096
    )) {
        check(&data);
    }

    #[test]
    fn simd_scanners_match_dense_framing_bytes(data in proptest::collection::vec(
        proptest::sample::select(vec![b'\r', b'\n', b'.', b'x']), 0..4096
    )) {
        check(&data);
    }
}

fn for_each_critical_byte_sequence(max_len: usize, f: &mut impl FnMut(&[u8])) {
    fn recurse(data: &mut Vec<u8>, max_len: usize, f: &mut impl FnMut(&[u8])) {
        if data.len() == max_len {
            return;
        }
        for byte in *b"\r\n.x" {
            data.push(byte);
            f(data);
            recurse(data, max_len, f);
            data.pop();
        }
    }
    let mut data = Vec::with_capacity(max_len);
    f(&data);
    recurse(&mut data, max_len, f);
}

fn run_streaming_detection(data: &[u8], boundary_mask: u32) -> Option<usize> {
    let mut detector = MultilineTerminatorDetector::default();
    let mut chunk_start = 0;
    for index in 0..data.len() {
        let is_last = index + 1 == data.len();
        let split_after = !is_last && ((boundary_mask >> index) & 1) == 1;
        if !is_last && !split_after {
            continue;
        }
        let chunk_end = index + 1;
        let chunk = &data[chunk_start..chunk_end];
        match detector.detect_terminator(chunk) {
            TerminatorStatus::FoundAt(end) => return Some(chunk_start + end),
            TerminatorStatus::NotFound => detector.update(chunk),
        }
        chunk_start = chunk_end;
    }
    None
}

#[test]
fn exhaustive_single_chunk_detection_matches_reference() {
    for_each_critical_byte_sequence(8, &mut |data| check(data));
}

#[test]
fn exhaustive_streaming_detection_matches_reference_for_all_chunkings() {
    for_each_critical_byte_sequence(7, &mut |data| {
        let expected = memchr::memmem::find(data, crate::TERMINATOR)
            .map(|start| start + crate::TERMINATOR.len());
        for boundary_mask in 0..1u32 << data.len().saturating_sub(1) {
            assert_eq!(
                run_streaming_detection(data, boundary_mask),
                expected,
                "streaming input {data:?}, boundaries {boundary_mask:b}"
            );
        }
    });
}
#[test]
fn find_terminator_end_finds_first_complete_terminator() {
    let data = b"222 1 <a@b>\r\nbody-1\r\n.\r\n222 2 <c@d>\r\nbody-2\r\n.\r\n";
    let end = find_terminator_end(data).expect("first terminator should be found");

    assert_eq!(end, b"222 1 <a@b>\r\nbody-1\r\n.\r\n".len());
    assert_eq!(&data[end - 5..end], crate::TERMINATOR);
    assert!(end < data.len());
}

#[test]
fn find_terminator_end_must_not_prefer_buffer_suffix() {
    const BUFFER_LEN: usize = 4096;
    let first_terminator_start = (BUFFER_LEN - crate::TERMINATOR.len()) / 2;
    let suffix_terminator_start = BUFFER_LEN - crate::TERMINATOR.len();
    let mut data = vec![b'x'; BUFFER_LEN];

    data[first_terminator_start..first_terminator_start + crate::TERMINATOR.len()]
        .copy_from_slice(crate::TERMINATOR);
    data[suffix_terminator_start..].copy_from_slice(crate::TERMINATOR);

    assert_eq!(
        find_terminator_end(&data),
        Some(first_terminator_start + crate::TERMINATOR.len()),
        "terminator detection must scan for the first terminator, not only check the end of the read buffer"
    );
    assert_ne!(
        find_terminator_end(&data),
        Some(BUFFER_LEN),
        "a suffix-only check would incorrectly return the terminator at the end of the buffer"
    );
}

#[test]
fn find_terminator_end_rejects_false_positives() {
    assert_eq!(find_terminator_end(b""), None);
    assert_eq!(find_terminator_end(b"line\r\n."), None);
    assert_eq!(find_terminator_end(b"data with \r\n but no dot"), None);
}
