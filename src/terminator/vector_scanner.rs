//! Fixed-pattern scanner ported from nntp-proxy's multiline framer
//! (f885ea4a), with its production architecture selection and inlining policy.

use super::TERMINATOR_FINDER;
use crate::TERMINATOR;

// Keep candidate verification out of the vector loop's register allocation.
#[inline(never)]
fn first_match(block: &[u8]) -> Option<usize> {
    block
        .windows(TERMINATOR.len())
        .position(|bytes| bytes == TERMINATOR)
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx512bw")))]
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
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx512bw")))]
    {
        find_repeat::<128, 0, b'\r'>(data)
    }
}
