//! Pending handshake state and its HELLO/HELLO_ACK/CONFIRM encoders.

use super::{Manager, Reply};
use crate::{
    buffer::Reservation,
    context::{Context, SessionState},
    wire::{self, Confirm, Control, Hello, HelloAck},
};
use std::{net::SocketAddr, sync::Arc, time::Instant};
use zerocopy::IntoBytes;

pub(super) struct Pending {
    pub(super) local: u64,
    pub(super) remote: u64,
    pub(super) client: [u8; 16],
    pub(super) server: [u8; 16],
    pub(super) cookie: [u8; 32],
    pub(super) initiator: bool,
    pub(super) d: usize,
    pub(super) max: usize,
    pub(super) confirming: bool,
    pub(super) reliable: bool,
    pub(super) ordered: bool,
    pub(super) started: Instant,
    pub(super) last: Instant,
    pub(super) attempts: u32,
    pub(super) waiters: Vec<Reply>,
    pub(super) charge: Reservation,
}

impl Pending {
    pub(super) fn new(
        shared: &Context,
        local: u64,
        client: [u8; 16],
        charge: Reservation,
        waiters: Vec<Reply>,
    ) -> Self {
        Self {
            local,
            remote: 0,
            client,
            server: [0; 16],
            cookie: [0; 32],
            initiator: true,
            d: shared.config.datagram_size,
            max: shared.config.max_frame_size,
            confirming: false,
            reliable: false,
            ordered: false,
            started: Instant::now(),
            last: Instant::now(),
            attempts: 0,
            waiters,
            charge,
        }
    }
}

impl Manager {
    pub(super) fn offer(&self, pending: &Pending) -> Control {
        if pending.confirming {
            return self.confirm(
                pending.local,
                pending.remote,
                pending.client,
                pending.server,
            );
        }
        let integrity = self.shared.config.integrity.code();
        if pending.initiator {
            let hello = Hello {
                magic: *b"RMRU",
                major: wire::PROTOCOL_MAJOR.into(),
                minor: wire::PROTOCOL_MINOR.into(),
                client_nonce: pending.client,
                cookie: pending.cookie,
                caps: (wire::ORDERED_CAP
                    | wire::RELIABLE_CAP
                    | if integrity == 1 { 1u64 } else { 2 })
                .into(),
                max_message: (pending.max as u32).into(),
                max_control: (pending.d.min(1200) as u16).into(),
                max_d: (pending.d as u16).into(),
                integrity,
                reserved: [0; 3],
            };
            Control::new(wire::HELLO, pending.local, 0, hello.as_bytes())
        } else {
            let hello = HelloAck {
                client_nonce: pending.client,
                server_nonce: pending.server,
                caps: ((if pending.reliable {
                    wire::RELIABLE_CAP
                } else {
                    0
                }) | (if pending.ordered {
                    wire::ORDERED_CAP
                } else {
                    0
                }) | if integrity == 1 { 1u64 } else { 2 })
                .into(),
                max_message: (pending.max as u32).into(),
                max_control: (pending.d.min(1200) as u16).into(),
                max_d: (pending.d as u16).into(),
                integrity,
                reserved: [0; 3],
            };
            Control::new(wire::HELLO_ACK, pending.local, 0, hello.as_bytes())
        }
    }

    pub(super) fn confirm(
        &self,
        local: u64,
        remote: u64,
        client: [u8; 16],
        server: [u8; 16],
    ) -> Control {
        Control::new(
            wire::CONFIRM,
            local,
            0,
            Confirm {
                client_nonce: client,
                server_nonce: server,
                peer_incarnation: remote.into(),
            }
            .as_bytes(),
        )
    }

    pub(super) fn session_state(&self, peer: SocketAddr, pending: Pending) -> Arc<SessionState> {
        Arc::new(SessionState {
            peer,
            local: pending.local,
            remote: pending.remote,
            d: pending.d,
            max_frame: pending.max,
            integrity: self.shared.config.integrity,
            next_id: std::sync::atomic::AtomicU32::new(1),
            tx_batch: std::sync::atomic::AtomicUsize::new(0),
            closed: std::sync::atomic::AtomicBool::new(false),
            client_nonce: pending.client,
            server_nonce: pending.server,
            initiator: pending.initiator,
            reliable: pending.reliable,
            ordered: pending.ordered,
            default_channel: std::sync::OnceLock::new(),
            _charge: pending.charge,
        })
    }
}
