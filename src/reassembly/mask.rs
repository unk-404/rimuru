//! Common frames keep their fragment bitmap inline, with fallible heap fallback.
use crate::{Error, Result};
use bitfield::{Bit, BitMut};
use bitvec::prelude::*;

pub(crate) const INLINE_BITS: usize = 128;
pub(crate) enum Segments {
    Inline([u64; 2]),
    Heap(BitVec<usize, Lsb0>),
}
impl Segments {
    pub fn new(count: usize) -> Result<Self> {
        if count <= INLINE_BITS {
            return Ok(Self::Inline([0; 2]));
        }
        let words = count.div_ceil(usize::BITS as usize);
        let mut storage = Vec::new();
        storage
            .try_reserve_exact(words)
            .map_err(|_| Error::NoBuffer)?;
        storage.resize(words, 0);
        let mut bits = BitVec::from_vec(storage);
        bits.truncate(count);
        Ok(Self::Heap(bits))
    }
    #[inline]
    pub fn contains(&self, index: usize) -> bool {
        match self {
            Self::Inline(bits) => bits[index / 64].bit(index % 64),
            Self::Heap(bits) => bits[index],
        }
    }
    #[inline]
    pub fn insert(&mut self, index: usize) {
        match self {
            Self::Inline(bits) => bits[index / 64].set_bit(index % 64, true),
            Self::Heap(bits) => bits.set(index, true),
        }
    }
}
