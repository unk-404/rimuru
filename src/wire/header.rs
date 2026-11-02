//! Common 13-byte header and its 4-bit type/flags accessors.

use bitfield::bitfield;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::little_endian::{U32, U64},
};

bitfield! {
    /// Low nibble: flags; high nibble: packet type.
    pub(crate) struct TypeFlags(u8);
    impl Debug;
    pub flags, _: 3, 0;
    pub kind, _: 7, 4;
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct Header {
    pub kind_flags: u8,
    pub incarnation: U64,
    pub frame: U32,
}

impl Header {
    pub fn new(kind: u8, incarnation: u64, frame: u32) -> Self {
        Self {
            kind_flags: kind << 4,
            incarnation: incarnation.into(),
            frame: frame.into(),
        }
    }

    pub fn flags(self) -> u8 {
        self.kind_flags & 15
    }

    pub fn kind(self) -> u8 {
        TypeFlags(self.kind_flags).kind()
    }
}
