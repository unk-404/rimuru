//! Owner-side reliable send: admission, cancellation and terminal transitions.

use super::{
    AbortReason, DeliveryOutcome, DeliveryReceipt, ReliablePeer, Signal, Terminal, tx::Tx,
};
use crate::{Error, FrameLayout, Result, buffer::Permit};
use std::sync::{Arc, atomic::Ordering::Relaxed};

impl ReliablePeer {
    pub fn submit(
        &mut self,
        layout: FrameLayout,
        signal: Arc<Signal>,
        permit: Permit,
    ) -> Result<()> {
        self.sent.admit(self.tx.len())?;
        self.tx.try_reserve(1).map_err(|_| Error::NoBuffer)?;
        let id = layout.id;
        self.tx.insert(
            id,
            Tx::new(layout, signal, permit, self.shared.pool.budget.clone())?,
        );
        self.shared
            .socket
            .metrics
            .reliable_submitted
            .fetch_add(1, Relaxed);
        Ok(())
    }

    pub fn cancel(&mut self, id: u32) {
        if let Some(tx) = self.tx.get(&id)
            && tx.layout.meta.layer == 3
        {
            self.close_outgoing(tx.layout.meta.channel.get(), AbortReason::Cancelled);
            return;
        }
        if let Some(tx) = self.tx.get_mut(&id) {
            tx.cancel();
        }
    }

    pub(super) fn finish_tx(&mut self, id: u32, terminal: Terminal) {
        if let Some(tx) = self.tx.get(&id)
            && tx.layout.meta.layer == 3
            && let Terminal::Abort(reason) = terminal
            && self.ordered_tx_open(tx.layout.meta.channel.get())
        {
            self.close_outgoing(tx.layout.meta.channel.get(), reason);
            return;
        }
        if let Some(tx) = self.tx.remove(&id) {
            if tx.layout.meta.layer == 3
                && terminal == Terminal::Complete
                && let Some(lane) = self.ordered.as_mut().and_then(|ordered| {
                    ordered.send.get_mut(&(tx.layout.meta.channel.get() as u32))
                })
            {
                lane.known_prefix = Some(lane.known_prefix.map_or(tx.layout.meta.seq.get(), |v| {
                    v.max(tx.layout.meta.seq.get())
                }));
            }
            tx.release_inflight(&mut self.recovery);
            let outcome = match terminal {
                Terminal::Complete => {
                    self.shared
                        .socket
                        .metrics
                        .reliable_completed
                        .fetch_add(1, Relaxed);
                    self.shared.socket.metrics.sent_frames.fetch_add(1, Relaxed);
                    self.shared
                        .socket
                        .metrics
                        .sent_application_bytes
                        .fetch_add(tx.layout.len() as u64, Relaxed);
                    DeliveryOutcome::Delivered(DeliveryReceipt {
                        frame_id: id,
                        application_bytes: tx.layout.len(),
                        channel: tx.layout.meta.channel.get(),
                        channel_sequence: (tx.layout.meta.layer == 3)
                            .then_some(tx.layout.meta.seq.get()),
                    })
                }
                Terminal::Abort(reason) => {
                    self.shared
                        .socket
                        .metrics
                        .reliable_aborted
                        .fetch_add(1, Relaxed);
                    DeliveryOutcome::Aborted(reason)
                }
            };
            // A terminal ticket promises its outgoing frame permit is reusable.
            // Drop Tx before waking waiters so a capacity-one sender can submit
            // its next message immediately after observing completion.
            let signal = tx.signal.clone();
            self.sent.insert(id, terminal);
            drop(tx);
            signal.finish(outcome);
        }
    }

    pub fn fail_all(&mut self) {
        self.fail_ordered();
        for tx in self.tx.values() {
            tx.unknown();
            self.shared
                .socket
                .metrics
                .reliable_unknown
                .fetch_add(1, Relaxed);
        }
        self.tx.clear();
        self.rx.clear();
        self.pending.clear();
    }
}
