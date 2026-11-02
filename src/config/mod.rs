//! Public configuration types. Validation happens before resources are charged.

mod endpoint;
mod reliable;
mod runtime;

pub use endpoint::EndpointConfig;
pub use reliable::ReliableConfig;
pub(crate) use runtime::RuntimeConfig;

/// Whole-frame integrity profile negotiated during the handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Integrity {
    /// BEGIN carries a whole-frame CRC32C (default).
    Crc32c,
    /// UDP checksum only; explicit trusted-path profile.
    UdpChecksum,
}

impl Integrity {
    pub(crate) fn code(self) -> u8 {
        match self {
            Self::Crc32c => 1,
            Self::UdpChecksum => 2,
        }
    }
}
