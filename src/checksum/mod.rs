//! CRC32C policy shared by train, slice and receive paths. The wire polynomial,
//! initial/final XOR and metadata prefix are unchanged.
use crc_fast::{CrcAlgorithm::Crc32Iscsi, Digest};

pub(crate) const PARALLEL_MIN: usize = 8 << 20;
pub(crate) const CHUNK_BYTES: usize = 1 << 20;

pub(crate) fn append(seed: u32, bytes: &[u8]) -> u32 {
    if bytes.len() < 256 {
        return crc32c::crc32c_append(seed, bytes);
    }
    chunks(seed, [bytes])
}

pub(crate) fn chunks<'a>(seed: u32, chunks: impl IntoIterator<Item = &'a [u8]>) -> u32 {
    // crc32c's public seed is finalized; Digest accepts the internal register.
    let mut digest = Digest::new_with_init_state(Crc32Iscsi, u64::from(!seed));
    for bytes in chunks {
        digest.update(bytes);
    }
    digest.finalize() as u32
}

/// Independent coarse blocks preserve order and use the same canonical prefix.
pub(crate) fn parallel_contiguous(seed: u32, bytes: &[u8]) -> u32 {
    use rayon::prelude::*;
    let chunks: Vec<_> = bytes
        .par_chunks(CHUNK_BYTES)
        .map(|s| (append(0, s), s.len()))
        .collect();
    chunks
        .into_iter()
        .fold(seed, |crc, (next, len)| combine(crc, next, len))
}

// Powers of the linear one-zero-byte CRC32C operator. Build at compile time:
// no first-frame initialization, heap, lock or per-chunk matrix squaring.
// Differential tests below verify against the maintained crc32c implementation.
static SHIFTS: [[u32; 32]; 32] = shift_powers();

#[inline]
const fn shift(matrix: &[u32; 32], mut value: u32) -> u32 {
    let mut result = 0;
    while value != 0 {
        result ^= matrix[value.trailing_zeros() as usize];
        value &= value - 1;
    }
    result
}

const fn shift_powers() -> [[u32; 32]; 32] {
    let mut powers = [[0u32; 32]; 32];
    let mut column = 0;
    while column < 32 {
        let mut value = 1u32 << column;
        let mut bit = 0;
        while bit < 8 {
            value = (value >> 1) ^ if value & 1 != 0 { 0x82f63b78 } else { 0 };
            bit += 1;
        }
        powers[0][column] = value;
        column += 1;
    }
    let mut power = 1;
    while power < 32 {
        let previous = powers[power - 1];
        column = 0;
        while column < 32 {
            powers[power][column] = shift(&previous, previous[column]);
            column += 1;
        }
        power += 1;
    }
    powers
}

/// Bounded 4 KiB read-only operator table. Wire lengths are u32; larger tooling
/// lengths fall back to the crate. This combines finalized CRCs, not payloads.
pub(crate) fn combine(mut first: u32, second: u32, mut len: usize) -> u32 {
    if len == 0 {
        return first;
    }
    if len > u32::MAX as usize {
        return crc32c::crc32c_combine(first, second, len);
    }
    while len != 0 {
        first = shift(&SHIFTS[len.trailing_zeros() as usize], first);
        len &= len - 1;
    }
    first ^ second
}

#[cfg(test)]
mod test;
