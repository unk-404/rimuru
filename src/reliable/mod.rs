//! Shared reliable transport for unordered L2 and ordered L3 messages.
//!
//! The endpoint dispatcher owns every state transition. This module holds the
//! per-peer reliable state; receive-side assembly lives in `receive`, send-side
//! lifecycle in `send`, wire control parsing in `control`.

mod channel_control;
mod control;
mod ledger;
mod ordered;
mod receive;
mod recovery;
mod rx;
mod schedule;
mod send;
mod ticket;
mod tx;

use crate::{
    Error, Result,
    buffer::{BudgetAllocator, Permit, Reservation, table as map},
    context::{Context, SessionState},
    wire::{self, Control},
};
use allocator_api2::vec::Vec;
use ledger::{Ledger, Terminal};
use map::Map;
use recovery::Recovery;
use rx::Rx;
use std::{
    sync::{Arc, atomic::AtomicUsize},
    time::Instant,
};
pub(crate) use ticket::Signal;
pub use ticket::{AbortReason, DeliveryOutcome, DeliveryReceipt, SendTicket};
use tx::Tx;
use zerocopy::IntoBytes;

/// Fixed queue depth for queued control packets per peer.
pub(super) const CONTROL_QUEUE: usize = 64;
/// Up-front per-peer metadata charge.
const PEER_CHARGE_BYTES: usize = 128;

struct PendingTerminal {
    terminal: Terminal,
    layer: u8,
    channel: u16,
    last: Option<Instant>,
    started: Instant,
    attempts: u32,
    _payload: Option<rx::Payload>,
    _permit: Permit,
    _charge: Reservation,
}

pub(crate) struct ReliablePeer {
    shared: Arc<Context>,
    state: Arc<SessionState>,
    tx: Map<Tx>,
    rx: Map<Rx>,
    pending: Map<PendingTerminal>,
    sent: Ledger,
    received: Ledger,
    controls: Vec<Control, BudgetAllocator>,
    terminal_count: Arc<AtomicUsize>,
    recovery: Recovery,
    credit_seq: u64,
    last_credit: Option<Instant>,
    remote_credit_seq: u64,
    _charge: Reservation,
    pub blocked: bool,
    round_robin: usize,
    ordered: Option<Box<crate::ordered::State>>,
    checking: bool,
}

impl ReliablePeer {
    pub fn new(shared: Arc<Context>, state: Arc<SessionState>) -> Result<Self> {
        let charge = shared
            .pool
            .budget
            .reserve(size_of::<Self>() + PEER_CHARGE_BYTES)?;
        let budget = shared.pool.budget.clone();
        let mut controls = Vec::new_in(BudgetAllocator(budget.clone()));
        controls
            .try_reserve_exact(CONTROL_QUEUE)
            .map_err(|_| Error::NoBuffer)?;
        Ok(Self {
            tx: map::new(budget.clone()),
            rx: map::new(budget.clone()),
            pending: map::new(budget.clone()),
            sent: Ledger::new(budget.clone()),
            received: Ledger::new(budget),
            controls,
            terminal_count: Arc::new(AtomicUsize::new(0)),
            recovery: Recovery::new(
                state.d,
                shared.config.reliable.initial_rto,
                state.peer.ip().is_loopback(),
            ),
            shared,
            state,
            credit_seq: 0,
            last_credit: None,
            remote_credit_seq: 0,
            _charge: charge,
            blocked: false,
            round_robin: 0,
            ordered: None,
            checking: false,
        })
    }

    fn direction(&self) -> u8 {
        u8::from(!self.state.initiator)
    }

    fn queue(&mut self, packet: Control) {
        if self.controls.len() < CONTROL_QUEUE {
            self.controls.push(packet);
        }
    }

    fn terminal_packet(&self, id: u32, terminal: Terminal) -> Control {
        match terminal {
            Terminal::Complete => Control::with_flags(wire::COMPLETE, 1, self.state.local, id, &[]),
            Terminal::Abort(reason) => Control::new(
                wire::ABORT,
                self.state.local,
                id,
                control::abort_body(reason, 1 - self.direction()).as_bytes(),
            ),
        }
    }

    fn replay(&mut self, id: u32) -> bool {
        if let Some(terminal) = self.received.get(id) {
            self.queue(self.terminal_packet(id, terminal));
            true
        } else {
            false
        }
    }

    pub fn needs_tick(&self) -> bool {
        !self.tx.is_empty()
            || !self.rx.is_empty()
            || !self.pending.is_empty()
            || self
                .ordered
                .as_ref()
                .is_some_and(|ordered| ordered.active())
    }
}

impl Drop for ReliablePeer {
    fn drop(&mut self) {
        self.fail_all();
    }
}

#[cfg(test)]
mod test;
