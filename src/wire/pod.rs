//! Fixed-layout control bodies. Flags/bitfields stay raw integers on the wire.

use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::little_endian::{U16, U32, U64},
};

macro_rules! pod {
    ($name:ident { $($field:ident: $ty:ty),* $(,)? }) => {
        #[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy)]
        #[repr(C)]
        pub(crate) struct $name {
            $(pub $field: $ty,)*
        }
    };
}

pod!(Hello {
    magic: [u8; 4],
    major: U16,
    minor: U16,
    client_nonce: [u8; 16],
    cookie: [u8; 32],
    caps: U64,
    max_message: U32,
    max_control: U16,
    max_d: U16,
    integrity: u8,
    reserved: [u8; 3]
});

pod!(HelloAck {
    client_nonce: [u8; 16],
    server_nonce: [u8; 16],
    caps: U64,
    max_message: U32,
    max_control: U16,
    max_d: U16,
    integrity: u8,
    reserved: [u8; 3]
});

pod!(Confirm {
    client_nonce: [u8; 16],
    server_nonce: [u8; 16],
    peer_incarnation: U64
});

pod!(Retry {
    client_nonce: [u8; 16],
    cookie: [u8; 32]
});

pod!(Grant {
    grant_id: U64,
    charged_bytes: U64,
    credit_seq: U64
});

pod!(Credit {
    seq: U64,
    bytes: U64,
    messages: U32,
    events: U32
});

pod!(AckHeader {
    delay: U32,
    records: U16,
    reserved: U16
});

pod!(AckRecord {
    id: U32,
    ranges: U16,
    reserved: U16
});

pod!(Range {
    start: U32,
    len: U32
});

pod!(Abort {
    reason: U16,
    scope: u8,
    direction: u8,
    channel: U16,
    reserved: U16,
    last_seq: U64
});
