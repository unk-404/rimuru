//! BEGIN body: message admission, layout and frame checksum seed.

use super::COMMON;
use crate::{Error, Result};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::little_endian::{U16, U32},
};

/// Length of the little-endian metadata prefix hashed into `seed`.
const SEED_BYTES: usize = 31;
/// Metadata prefix ends after `seq`.
const SEED_METADATA_END: usize = 26;
/// FEC code and k/r fields complete the seed.
const SEED_FEC_END: usize = 29;

#[derive(
    FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub(crate) struct Begin {
    pub layer: u8,
    pub idx_bytes: u8,
    pub integrity: u8,
    pub meta_flags: u8,
    pub channel: U16,
    pub reserved0: U16,
    pub total: U32,
    pub stride: U32,
    pub count: U32,
    pub seq: U32,
    pub crc: U32,
    pub fec: u8,
    pub reserved1: u8,
    pub fec_k: U16,
    pub fec_r: U16,
    pub reserved2: U16,
}

impl Begin {
    pub fn new(total: usize, d: usize, integrity: u8) -> Result<Self> {
        if total > u32::MAX as usize || !(128..=65507).contains(&d) {
            return Err(Error::MessageTooBig);
        }
        let narrow = total.div_ceil(d - 15) <= 65_536;
        let width = if narrow { 2 } else { 4 };
        let stride = d - COMMON - width;
        Ok(Self {
            layer: 1,
            idx_bytes: width as u8,
            integrity,
            meta_flags: 0,
            channel: 0.into(),
            reserved0: 0.into(),
            total: (total as u32).into(),
            stride: (stride as u32).into(),
            count: (total.div_ceil(stride) as u32).into(),
            seq: 0.into(),
            crc: 0.into(),
            fec: 0,
            reserved1: 0,
            fec_k: 0.into(),
            fec_r: 0.into(),
            reserved2: 0.into(),
        })
    }

    pub fn header_len(self) -> usize {
        COMMON + self.idx_bytes as usize
    }

    pub fn validate(self, max: usize, d: usize, integrity: u8) -> Result<()> {
        let total = self.total.get() as usize;
        let stride = self.stride.get() as usize;
        if !matches!(self.layer, 1..=3)
            || !matches!(self.idx_bytes, 2 | 4)
            || self.integrity != integrity
            || if self.layer == 3 {
                self.meta_flags != 1
            } else {
                self.meta_flags != 0 || self.seq.get() != 0
            }
            || self.reserved0.get() != 0
            || self.reserved1 != 0
            || self.reserved2.get() != 0
            || self.fec != 0
            || self.fec_k.get() != 0
            || self.fec_r.get() != 0
            || stride == 0
            || stride + self.header_len() > d
            || total > max
            || total.div_ceil(stride) != self.count.get() as usize
            || (self.idx_bytes == 2 && self.count.get() > 65_536)
            || (integrity == 2 && self.crc.get() != 0)
        {
            return Err(Error::Protocol("invalid BEGIN"));
        }
        Ok(())
    }

    /// Standardized metadata prefix for the whole-frame CRC32C.
    pub fn seed(self, id: u32) -> u32 {
        let mut bytes = [0u8; SEED_BYTES];
        bytes[..4].copy_from_slice(&id.to_le_bytes());
        let raw = self.as_bytes();
        bytes[4..10].copy_from_slice(&raw[..6]);
        bytes[10..SEED_METADATA_END].copy_from_slice(&raw[8..24]);
        bytes[SEED_METADATA_END] = self.fec;
        bytes[SEED_METADATA_END + 1..SEED_FEC_END].copy_from_slice(self.fec_k.as_bytes());
        bytes[SEED_FEC_END..].copy_from_slice(self.fec_r.as_bytes());
        crc32c::crc32c(&bytes)
    }
}
