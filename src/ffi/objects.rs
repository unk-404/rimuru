//! Immutable FFI object payloads: sessions, channels, tickets, messages, views and
//! transaction-style builders.

use super::{
    abi::{
        RM_ABORTED, RM_EVENT_MESSAGE, RM_EVENT_SEND_TERMINAL, RM_OK, RM_OUTCOME_UNKNOWN,
        RM_TICKET_ABORTED, RM_TICKET_FLAG_LOCAL_BUFFER_RELEASED, RM_TICKET_RECEIVED,
        RM_TICKET_UNKNOWN, RmEvent, RmEventKind, RmHandle, RmStatus,
    },
    cq::{Cq, EventPermit},
};
use crate::{
    Channel, DeliveryOutcome, Endpoint, Frame, FrameLayout, SendTicket, Session,
    buffer::Reservation,
};
use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex, Weak,
        atomic::{
            AtomicBool, AtomicU32, AtomicU64,
            Ordering::{AcqRel, Acquire, Relaxed, Release},
        },
    },
};

/// One FFI session handle: a Rust session plus stable local identifiers.
pub(crate) struct SessionObj {
    pub endpoint: Endpoint,
    pub session: Session,
    pub peer: SocketAddr,
    pub id: u64,
    pub default_cq: Arc<Cq>,
    pub closed: AtomicBool,
    /// Weak back-reference avoids an endpoint to session ownership cycle.
    pub owner: Weak<super::endpoint::EndpointObj>,
}

impl SessionObj {
    pub fn close(&self) {
        self.closed.store(true, Release);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Acquire)
    }
}

/// One FFI channel handle. Layers 1 and 2 are addressing metadata; layer 3 wraps
/// the Rust ordered `Channel`.
pub(crate) struct ChannelObj {
    pub session: Session,
    pub layer: u8,
    pub id: u16,
    pub channel: Option<Channel>,
    pub cq: Arc<Cq>,
    pub endpoint: Endpoint,
    pub closed: AtomicBool,
    pub owner: Weak<super::endpoint::EndpointObj>,
}

impl ChannelObj {
    pub fn is_closed(&self) -> bool {
        self.closed.load(Acquire)
            || self.channel.as_ref().is_some_and(Channel::is_closed)
            || self.session.is_closed()
    }
}

/// Immutable received message. A view keeps the same frame alive independently.
pub(crate) struct MessageObj {
    pub frame: Frame,
}

pub(crate) struct ViewObj {
    pub frame: Frame,
}

/// Completion ticket state. At most one terminal event is published.
pub(crate) struct TicketObj {
    pub ticket_id: u64,
    pub session_id: u64,
    pub channel_id: u16,
    pub layer: u8,
    pub user_data: u64,
    pub cq: Arc<Cq>,
    pub rust: Mutex<Option<SendTicket>>,
    state: AtomicU32,
    flags: AtomicU32,
    finished: AtomicBool,
    permit: Mutex<Option<EventPermit>>,
    pub owner: Weak<super::endpoint::EndpointObj>,
    _charge: Reservation,
}

impl TicketObj {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ticket_id: u64,
        session_id: u64,
        channel_id: u16,
        layer: u8,
        user_data: u64,
        cq: Arc<Cq>,
        permit: EventPermit,
        rust: Option<SendTicket>,
        charge: Reservation,
        owner: Weak<super::endpoint::EndpointObj>,
    ) -> Arc<Self> {
        Arc::new(Self {
            ticket_id,
            session_id,
            channel_id,
            layer,
            user_data,
            cq,
            rust: Mutex::new(rust),
            state: AtomicU32::new(super::abi::RM_TICKET_QUEUED as u32),
            flags: AtomicU32::new(0),
            finished: AtomicBool::new(false),
            permit: Mutex::new(Some(permit)),
            owner,
            _charge: charge,
        })
    }

    pub fn state(&self) -> super::abi::RmTicketState {
        super::abi::RmTicketState::from_raw(self.state.load(Acquire))
    }

    pub fn flags(&self) -> u32 {
        self.flags.load(Acquire)
    }

    pub fn take_rust(&self) -> Option<SendTicket> {
        self.rust
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Publish the single terminal event. Safe to race with cancellation.
    pub fn finish(&self, outcome: DeliveryOutcome) -> bool {
        if self.finished.swap(true, AcqRel) {
            return false;
        }
        let (state, status) = match outcome {
            DeliveryOutcome::Delivered(_) => (RM_TICKET_RECEIVED, RM_OK),
            DeliveryOutcome::Aborted(_) => (RM_TICKET_ABORTED, RM_ABORTED),
            DeliveryOutcome::Unknown => (RM_TICKET_UNKNOWN, RM_OUTCOME_UNKNOWN),
        };
        self.state.store(state as u32, Release);
        self.flags
            .fetch_or(RM_TICKET_FLAG_LOCAL_BUFFER_RELEASED, Release);
        let permit = self
            .permit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(permit) = permit else {
            return false;
        };
        let mut event = RmEvent::empty(RM_EVENT_SEND_TERMINAL, status);
        event.user_data = self.user_data;
        event.ticket_id = self.ticket_id;
        event.session_id = self.session_id;
        event.channel_id = self.channel_id;
        event.layer = self.layer;
        event.channel_seq = 0;
        self.cq.publish(event, permit);
        true
    }

    /// Immediate local completion used by L1 sends (no remote confirmation).
    pub fn finish_local(&self) {
        self.finish(DeliveryOutcome::Delivered(crate::DeliveryReceipt {
            frame_id: 0,
            application_bytes: 0,
            channel: self.channel_id,
            channel_sequence: None,
        }));
    }
}

/// Bitset-backed builder over a pool-owned frame layout.
pub(crate) struct BuilderObj {
    pub session: Session,
    pub channel: Option<Channel>,
    pub cq: Arc<Cq>,
    pub layer: u8,
    pub layout: Option<FrameLayout>,
    pub written: Vec<u64>,
    pub sealed: bool,
    pub submitted: bool,
    pub owner: Weak<super::endpoint::EndpointObj>,
    _charge: Reservation,
}

impl BuilderObj {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: Session,
        channel: Option<Channel>,
        layer: u8,
        cq: Arc<Cq>,
        layout: FrameLayout,
        written: Vec<u64>,
        charge: Reservation,
        owner: Weak<super::endpoint::EndpointObj>,
    ) -> Self {
        Self {
            session,
            channel,
            cq,
            layer,
            layout: Some(layout),
            written,
            sealed: false,
            submitted: false,
            owner,
            _charge: charge,
        }
    }

    pub fn total_len(&self) -> usize {
        self.layout.as_ref().map_or(0, FrameLayout::len)
    }

    pub fn mark(&mut self, offset: usize, len: usize) {
        for index in offset..offset + len {
            let word = index / 64;
            let bit = index % 64;
            self.written[word] |= 1u64 << bit;
        }
    }

    pub fn is_complete(&self) -> bool {
        let total = self.total_len();
        (0..total).all(|index| self.written[index / 64] & (1u64 << (index % 64)) != 0)
    }
}

/// Width of a builder coverage bitmap for `len` bytes.
pub(crate) fn bitmap_words(len: usize) -> usize {
    len.div_ceil(64)
}

/// Message event helper used by the endpoint receive pump.
#[allow(clippy::too_many_arguments)]
pub(crate) fn message_event(
    kind: RmEventKind,
    status: RmStatus,
    message: RmHandle,
    session_id: u64,
    delivery_seq: u64,
    channel_seq: u32,
    channel_id: u16,
    layer: u8,
) -> RmEvent {
    RmEvent {
        kind,
        status,
        user_data: 0,
        ticket_id: 0,
        session_id,
        delivery_seq,
        message,
        channel_seq,
        channel_id,
        layer,
        flags: 0,
    }
}

/// Monotonic source for local stable ids (not handles).
pub(crate) struct Ids {
    pub next: AtomicU64,
}

impl Ids {
    pub const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Relaxed)
    }
}

const _: () = assert!((RM_EVENT_MESSAGE as u32) == 1 && (RM_EVENT_SEND_TERMINAL as u32) == 2);
