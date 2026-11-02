//! Protocol 0.0 layout: 13-byte common header, DATA16/32, control bodies.
//!
//! Multi-byte integers are little-endian. The wire layout is a network contract and
//! is deliberately independent of the C ABI structs; both are explicit and neither
//! is produced by casting runtime objects to bytes.

pub(crate) const PROTOCOL_MAJOR: u16 = 0;
pub(crate) const PROTOCOL_MINOR: u16 = 0;

mod begin;
mod control;
mod header;
mod packet;
mod pod;

pub(crate) use begin::Begin;
pub(crate) use control::Control;
pub(crate) use header::Header;
pub(crate) use packet::{Packet, decode, read_begin};
pub(crate) use pod::{
    Abort, AckHeader, AckRecord, Confirm, Credit, Grant, Hello, HelloAck, Range, Retry,
};

/// Common header size shared by every packet type.
pub(crate) const COMMON: usize = 13;
/// Encoded BEGIN body size (36) plus common header and control CRC.
#[cfg(test)]
pub(crate) const BEGIN_BYTES: usize = 53;

pub(crate) const HELLO: u8 = 2;
pub(crate) const RETRY: u8 = 3;
pub(crate) const HELLO_ACK: u8 = 4;
pub(crate) const CONFIRM: u8 = 5;
pub(crate) const BEGIN: u8 = 6;
pub(crate) const BEGIN_ACCEPT: u8 = 7;
pub(crate) const ACK: u8 = 8;
pub(crate) const CREDIT: u8 = 9;
pub(crate) const NACK: u8 = 10;
pub(crate) const COMPLETE: u8 = 11;
pub(crate) const ABORT: u8 = 12;
pub(crate) const ABORT_ACK: u8 = 13;
pub(crate) const PING: u8 = 14;
pub(crate) const EXT: u8 = 15;

pub(crate) const RELIABLE_CAP: u64 = 8;
pub(crate) const ORDERED_CAP: u64 = 16;
/// Largest control body accepted by a fixed `Control` buffer.
pub(crate) const MAX_CONTROL_BODY: usize = 111;
/// `Control` stores header + body + CRC inline.
pub(crate) const CONTROL_BYTES: usize = 128;

#[cfg(test)]
mod test;
