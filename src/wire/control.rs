//! Fixed-size control packet builder.

use super::{COMMON, CONTROL_BYTES, Header, MAX_CONTROL_BODY};
use zerocopy::IntoBytes;

#[derive(Clone)]
pub(crate) struct Control {
    bytes: [u8; CONTROL_BYTES],
    pub(crate) len: usize,
}

impl Control {
    pub fn new(kind: u8, incarnation: u64, frame: u32, body: &[u8]) -> Self {
        Self::with_flags(kind, 0, incarnation, frame, body)
    }

    pub fn with_flags(kind: u8, flags: u8, incarnation: u64, frame: u32, body: &[u8]) -> Self {
        assert!(body.len() <= MAX_CONTROL_BODY);
        let mut bytes = [0; CONTROL_BYTES];
        let mut header = Header::new(kind, incarnation, frame);
        header.kind_flags |= flags;
        bytes[..COMMON].copy_from_slice(header.as_bytes());
        bytes[COMMON..COMMON + body.len()].copy_from_slice(body);
        let end = COMMON + body.len();
        let crc = crc32c::crc32c(&bytes[..end]);
        bytes[end..end + 4].copy_from_slice(&crc.to_le_bytes());
        Self {
            bytes,
            len: end + 4,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    /// Flip one bit for the control-CRC rejection test.
    #[cfg(test)]
    pub(crate) fn corrupt(&mut self, index: usize) {
        self.bytes[index] ^= 1;
    }
}
