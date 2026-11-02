//! Owner-side reliable receive: BEGIN admission, DATA ingest, SACK/NACK and ticks.

use super::{
    AbortReason, PendingTerminal, ReliablePeer, Terminal,
    rx::{Check, Payload, Rx},
};
use crate::{
    Error, Result,
    buffer::Permit,
    wire::{self, Begin, Control, Credit},
};
use std::{
    sync::atomic::Ordering::{Acquire, Relaxed, Release},
    time::Instant,
};
use zerocopy::IntoBytes;

/// Base bytes charged for an admitted reliable receive context.
const GRANT_BASE_BYTES: u64 = 1024;
/// Metadata allowance covered by every grant.
const GRANT_METADATA_BYTES: u64 = 64;
/// Bitmap/scratch bytes per 64 KiB of message.
const GRANT_BITMAP_BYTES: u64 = 16;
/// One credit update per millisecond while stalled.
const CREDIT_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1);

impl ReliablePeer {
    pub fn begin(&mut self, id: u32, meta: Begin) -> Result<()> {
        meta.validate(
            self.state.max_frame,
            self.state.d,
            self.state.integrity.code(),
        )?;
        if !matches!(meta.layer, 2 | 3) || (meta.layer == 2 && meta.channel.get() != 0) || id == 0 {
            return Err(Error::Protocol("L2 BEGIN fields"));
        }
        if self
            .ordered
            .as_ref()
            .and_then(|ordered| ordered.deferred.get(&id))
            .is_some_and(|deferred| deferred.meta != meta)
        {
            return Err(Error::Protocol("conflicting deferred frame identity"));
        }
        if let Some(rx) = self.rx.get(&id) {
            if rx.meta != meta {
                return Err(Error::Protocol("conflicting reliable BEGIN"));
            }
            self.queue(rx.grant_packet(self.state.local, id));
            return Ok(());
        }
        if self.replay(id) {
            return Ok(());
        }
        let lane_permit = if meta.layer == 3 {
            match self.ordered_gate(id, meta)? {
                Some(permit) => Some(permit),
                None => return Ok(()),
            }
        } else {
            None
        };
        self.received.admit(self.rx.len())?;
        self.rx.try_reserve(1).map_err(|_| Error::NoBuffer)?;
        self.pending
            .try_reserve(self.rx.len() + 1)
            .map_err(|_| Error::NoBuffer)?;
        let admitted = (|| {
            let permit = Permit::take(&self.terminal_count, self.shared.config.frame_capacity)?;
            Rx::new(&self.shared, meta, u64::from(id), permit)
        })();
        let mut rx = match admitted {
            Ok(rx) => rx,
            Err(Error::NoBuffer) => {
                if meta.layer == 3 {
                    let _ = self.defer_ordered(id, meta);
                }
                self.credit();
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        rx.lane_permit = lane_permit;
        if meta.layer == 3 {
            self.ordered_granted(id, meta);
        }
        self.credit_seq += 1;
        rx.grant.credit_seq = self.credit_seq.into();
        self.queue(rx.grant_packet(self.state.local, id));
        let check = rx.start_check(&self.shared, id);
        self.rx.insert(id, rx);
        self.check_started(id, check);
        self.shared
            .socket
            .metrics
            .reliable_grants
            .fetch_add(1, Relaxed);
        Ok(())
    }

    pub fn data(&mut self, id: u32, index: u32, width: u8, bytes: &[u8]) -> Result<()> {
        if let Some(rx) = self.rx.get_mut(&id) {
            if rx.stopped {
                return Ok(());
            }
            if !rx.data(index, width, bytes)? {
                self.shared
                    .socket
                    .metrics
                    .duplicate_segments
                    .fetch_add(1, Relaxed);
            }
            let check = rx.start_check(&self.shared, id);
            self.check_started(id, check);
        } else {
            self.replay(id);
        }
        Ok(())
    }

    pub(super) fn credit(&mut self) {
        let now = Instant::now();
        self.shared
            .socket
            .metrics
            .reliable_credit_stalls
            .fetch_add(1, Relaxed);
        if self
            .last_credit
            .is_some_and(|at| now.duration_since(at) < CREDIT_MIN_INTERVAL)
        {
            return;
        }
        self.last_credit = Some(now);
        self.credit_seq = self.credit_seq.saturating_add(1);
        let available = self
            .shared
            .config
            .frame_capacity
            .saturating_sub(self.shared.frames_in_use.load(Acquire));
        let credit = Credit {
            seq: self.credit_seq.into(),
            bytes: (self.shared.pool.budget.available() as u64).into(),
            messages: (available as u32).into(),
            events: (available as u32).into(),
        };
        self.queue(Control::new(
            wire::CREDIT,
            self.state.local,
            0,
            credit.as_bytes(),
        ));
    }

    pub(super) fn check_started(&mut self, id: u32, check: Check) {
        match check {
            Check::Incomplete => {}
            Check::Background => self.checking = true,
            Check::Inline => {
                let (assembly, valid) = self
                    .rx
                    .get_mut(&id)
                    .expect("inline check frame exists")
                    .checked()
                    .expect("inline check produced a result");
                self.finish_checked(id, assembly, valid);
            }
        }
    }

    pub(super) fn finish_ready(&mut self) {
        // Only background CRC completions need polling. Inline checks are handled
        // by frame ID at the packet that completes them, without scanning all RX.
        if !self.checking {
            return;
        }
        loop {
            let ready = self
                .rx
                .iter_mut()
                .find_map(|(&id, rx)| rx.checked().map(|result| (id, result)));
            let Some((id, (assembly, valid))) = ready else {
                self.checking = self.rx.values().any(Rx::checking);
                break;
            };
            self.finish_checked(id, assembly, valid);
        }
    }

    fn finish_checked(
        &mut self,
        id: u32,
        assembly: crate::reassembly::assembly::Assembly,
        valid: bool,
    ) {
        let mut rx = self.rx.remove(&id).expect("checked frame exists");
        if rx.meta.layer == 3 {
            if valid {
                self.ordered_ready(
                    id,
                    rx,
                    assembly
                        .into_frame(self.state.peer, id)
                        .with_incarnation(self.state.remote),
                );
            } else {
                self.shared
                    .socket
                    .metrics
                    .integrity_failures
                    .fetch_add(1, Relaxed);
                let channel = rx.meta.channel.get();
                rx.stopped = true;
                rx.payload = Some(Payload::Checked(assembly, false));
                self.rx.insert(id, rx);
                self.close_incoming(channel, AbortReason::Integrity);
            }
            return;
        }
        if valid {
            if self.shared.deliver(
                assembly
                    .into_frame(self.state.peer, id)
                    .with_incarnation(self.state.remote),
            ) {
                self.terminal_rx(id, rx, Terminal::Complete, None);
            } else {
                self.terminal_rx(id, rx, Terminal::Abort(AbortReason::Shutdown), None);
            }
        } else {
            self.shared
                .socket
                .metrics
                .integrity_failures
                .fetch_add(1, Relaxed);
            self.terminal_rx(
                id,
                rx,
                Terminal::Abort(AbortReason::Integrity),
                Some(Payload::Checked(assembly, false)),
            );
        }
    }

    pub(super) fn terminal_rx(
        &mut self,
        id: u32,
        rx: Rx,
        terminal: Terminal,
        hold: Option<Payload>,
    ) {
        self.received.insert(id, terminal);
        self.pending.insert(
            id,
            PendingTerminal {
                terminal,
                layer: rx.meta.layer,
                channel: rx.meta.channel.get(),
                last: None,
                started: Instant::now(),
                attempts: 0,
                _payload: hold,
                _permit: rx.terminal_permit,
                _charge: rx.charge,
            },
        );
    }

    pub fn tick(&mut self, now: Instant) {
        self.finish_ready();
        self.tick_ordered(now);
        let timeout = self.shared.config.reliable.message_timeout;
        let max_rounds = self.shared.config.reliable.max_recovery_rounds;
        loop {
            let channel = self.tx.values().find_map(|tx| {
                (tx.layout.meta.layer == 3
                    && self.ordered_tx_open(tx.layout.meta.channel.get())
                    && tx.exhausted(now, timeout, max_rounds))
                .then_some(tx.layout.meta.channel.get())
            });
            let Some(channel) = channel else {
                break;
            };
            self.close_outgoing(channel, AbortReason::Timeout);
        }
        loop {
            let channel = self.rx.values().find_map(|rx| {
                (rx.meta.layer == 3
                    && self.ordered_rx_open(rx.meta.channel.get())
                    && now.duration_since(rx.started) >= timeout)
                    .then_some(rx.meta.channel.get())
            });
            let Some(channel) = channel else {
                break;
            };
            self.close_incoming(channel, AbortReason::Timeout);
        }
        let config = &self.shared.config.reliable;
        for tx in self.tx.values_mut() {
            let (rack, round, tail) = tx.tick(now, &mut self.recovery);
            self.shared
                .socket
                .metrics
                .reliable_rack_losses
                .fetch_add(rack as u64, Relaxed);
            self.shared
                .socket
                .metrics
                .reliable_recovery_rounds
                .fetch_add(u64::from(round), Relaxed);
            self.shared
                .socket
                .metrics
                .reliable_tail_probes
                .fetch_add(u64::from(tail), Relaxed);
        }
        if self.tx.values().any(|tx| {
            tx.layout.meta.layer != 3
                && tx.exhausted(now, config.message_timeout, config.max_recovery_rounds)
        }) || self.rx.values().any(|rx| {
            rx.meta.layer != 3 && now.duration_since(rx.started) >= config.message_timeout
        }) || self.pending.values().any(|pending| {
            pending.layer != 3
                && (now.duration_since(pending.started) >= config.message_timeout
                    || pending.attempts >= config.max_recovery_rounds)
        }) {
            // Retire the incarnation instead of forgetting acknowledged storage or replay history.
            self.state.closed.store(true, Release);
            self.fail_all();
        }
    }
}

/// Bytes the receiver promises to keep charged for one admitted message.
pub(super) fn grant_charge(meta: Begin) -> u64 {
    let total = u64::from(meta.total.get()).max(1);
    total
        .next_power_of_two()
        .saturating_add(GRANT_METADATA_BYTES)
        .saturating_add(GRANT_BASE_BYTES)
        .saturating_add(
            u64::from(meta.count.get()).div_ceil(usize::BITS as u64) * size_of::<usize>() as u64,
        )
        .saturating_add(total.div_ceil(65_536) * GRANT_BITMAP_BYTES)
}
