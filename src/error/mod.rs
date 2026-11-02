use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("UDP I/O: {0}")]
    Io(#[from] io::Error),
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("protocol: {0}")]
    Protocol(&'static str),
    #[error("endpoint/session closed")]
    Closed,
    #[error("operation timed out")]
    TimedOut,
    #[error("bounded queue full/empty")]
    WouldBlock,
    #[error("use the async API from an async runtime")]
    WouldDeadlock,
    #[error("frame exceeds negotiated limit")]
    MessageTooBig,
    #[error("resource budget exhausted")]
    NoBuffer,
    #[error("ID exhausted; reconnect required")]
    IdExhausted,
    #[error("peer did not negotiate L2")]
    ReliableUnsupported,
    #[error("peer did not negotiate L3")]
    OrderedUnsupported,
    #[error("ordered channel closed")]
    ChannelClosed,
    #[error("delivery aborted: {0}")]
    DeliveryAborted(crate::AbortReason),
    #[error("remote outcome could not be confirmed")]
    OutcomeUnknown,
}

#[cfg(test)]
mod test;
