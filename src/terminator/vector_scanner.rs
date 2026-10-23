//! Fixed-pattern scanner ported from nntp-proxy's multiline framer
//! (f885ea4a). The AVX2 filter folds CR positions before checking their distance.

use super::TERMINATOR_FINDER;
use crate::TERMINATOR;

// Keep candidate verification out of the vector loop's register allocation.
#[inline(never)]
fn first_match(block: &[u8]) -> Option<usize> {
    block
        .windows(TERMINATOR.len())
        .position(|bytes| bytes == TERMINATOR)
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
#[inline(always)]
fn find_repeat<const WIDTH: usize, const FIRST: usize, const BYTE: u8>(
    data: &[u8],
) -> Option<usize> {
    let mut offset = 0;
    while data.len() - offset >= WIDTH + 4 {
        let block = &data[offset..offset + WIDTH + 4];
        let mut possible = false;
        for lane in 0..WIDTH {
            possible |= (block[lane + FIRST] == BYTE) & (block[lane + FIRST + 3] == BYTE);
        }
        if possible && let Some(lane) = first_match(block) {
            return Some(offset + lane);
        }
        offset += WIDTH;
    }
    TERMINATOR_FINDER
        .find(&data[offset..])
        .map(|start| offset + start)
}

#[cfg(any(target_arch = "aarch64", target_feature = "avx512bw"))]
#[inline(always)]
fn find_or<const WIDTH: usize, const FIRST: usize, const BYTE: u8>(
    data: &[u8],
    verify: impl Fn(&[u8]) -> Option<usize>,
) -> Option<usize> {
    let mut offset = 0;
    while data.len() - offset >= WIDTH + 4 {
        let block = &data[offset..offset + WIDTH + 4];
        let mut possible = false;
        for lane in 0..WIDTH {
            // Repeated terminator bytes always pass; a verifier rejects
            // any control-byte combinations admitted by the OR predicate.
            possible |= (block[lane + FIRST] | block[lane + FIRST + 3]) == BYTE;
        }
        if possible && let Some(lane) = verify(block) {
            return Some(offset + lane);
        }
        offset += WIDTH;
    }
    TERMINATOR_FINDER
        .find(&data[offset..])
        .map(|start| offset + start)
}

// Reject control-byte OR collisions in SIMD before the full verifier.
#[cfg(any(target_arch = "aarch64", target_feature = "avx512bw"))]
#[cfg_attr(
    all(target_arch = "x86_64", target_feature = "avx512bw"),
    inline(always)
)]
#[cfg_attr(
    not(all(target_arch = "x86_64", target_feature = "avx512bw")),
    inline(never)
)]
fn exact_pair_match<const WIDTH: usize, const FIRST: usize, const BYTE: u8>(
    block: &[u8],
) -> Option<usize> {
    let mut possible = false;
    for lane in 0..WIDTH {
        possible |= (block[lane + FIRST] == BYTE) & (block[lane + FIRST + 3] == BYTE);
    }
    if possible { first_match(block) } else { None }
}

#[inline]
pub(super) fn find(data: &[u8]) -> Option<usize> {
    #[cfg(target_arch = "aarch64")]
    {
        find_or::<128, 0, b'\r'>(data, exact_pair_match::<128, 0, b'\r'>)
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
    {
        find_or::<256, 1, b'\n'>(data, exact_pair_match::<256, 1, b'\n'>)
    }
    #[cfg(all(
        target_arch = "x86_64",
        target_feature = "avx2",
        not(target_feature = "avx512bw")
    ))]
    {
        find_avx2(data)
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    {
        find_repeat::<128, 0, b'\r'>(data)
    }
}

#[cfg(all(
    target_arch = "x86_64",
    target_feature = "avx2",
    not(target_feature = "avx512bw")
))]
#[inline(always)]
fn find_avx2(mut data: &[u8]) -> Option<usize> {
    use core::arch::x86_64::{
        _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_or_si256,
        _mm256_set1_epi8,
    };
    let original_len = data.len();
    while data.len() >= 128 {
        // AVX2 is selected at compilation. The length check covers all four
        // unaligned loads; the input slice contains initialized bytes.
        let mask = unsafe {
            let needle = _mm256_set1_epi8(b'\r' as i8);
            let ptr = data.as_ptr();
            let a = _mm256_cmpeq_epi8(_mm256_loadu_si256(ptr.cast()), needle);
            let b = _mm256_cmpeq_epi8(_mm256_loadu_si256(ptr.add(32).cast()), needle);
            let c = _mm256_cmpeq_epi8(_mm256_loadu_si256(ptr.add(64).cast()), needle);
            let d = _mm256_cmpeq_epi8(_mm256_loadu_si256(ptr.add(96).cast()), needle);
            _mm256_movemask_epi8(_mm256_or_si256(
                _mm256_or_si256(a, b),
                _mm256_or_si256(c, d),
            )) as u32
        };
        // Folding the four lanes admits extra candidates but preserves every
        // CR pair three bytes apart, including pairs across a lane boundary.
        if mask & mask.rotate_right(3) != 0
            && let Some(lane) = first_match(&data[..128])
        {
            return Some(original_len - data.len() + lane);
        }
        // Four bytes of overlap preserve every five-byte terminator.
        data = &data[124..];
    }
    TERMINATOR_FINDER
        .find(data)
        .map(|start| original_len - data.len() + start)
}
