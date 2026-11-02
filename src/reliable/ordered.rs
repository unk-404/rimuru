//! Owner-only bridge: L3 admission/publication shares L2 storage and recovery.
use super::{ReliablePeer, Signal, Terminal, Tx, control};
use crate::{
    AbortReason, Error, Frame, FrameLayout, Result,
    buffer::Permit,
    context::ChannelState,
    ordered::{Deferred, ReceiveLane, State},
    wire::{self, Begin},
};
use std::{
    sync::{
        Arc,
        atomic::Ordering::{AcqRel, Acquire, Relaxed},
    },
    time::Instant,
};

impl ReliablePeer {
    fn ordered_state(&mut self) -> Result<&mut State> {
        if !self.state.ordered {
            return Err(Error::OrderedUnsupported);
        }
        if self.ordered.is_none() {
            self.ordered = Some(Box::new(State::new(&self.shared)?));
        }
        Ok(self.ordered.as_deref_mut().unwrap())
    }
    pub fn open_channel(&mut self, id: u16) -> Result<Arc<ChannelState>> {
        self.ordered_state()?;
        let state = self.ordered.as_mut().unwrap().open(&self.shared, id)?;
        if id == 0 {
            let _ = self.state.default_channel.set(state.clone());
        }
        Ok(state)
    }
    pub fn submit_ordered(
        &mut self,
        layout: FrameLayout,
        crc: u32,
        signal: Arc<Signal>,
        permit: Permit,
        lane_permit: Permit,
        channel: Arc<ChannelState>,
    ) -> Result<(u32, u32)> {
        if !self.state.ordered || self.state.closed.load(Acquire) {
            return Err(Error::Closed);
        }
        let ordered = self.ordered.as_mut().ok_or(Error::ChannelClosed)?;
        let lane = ordered
            .send
            .get_mut(&(channel.id as u32))
            .ok_or(Error::ChannelClosed)?;
        if !Arc::ptr_eq(&lane.state, &channel) || channel.closing.load(Acquire) {
            return Err(Error::ChannelClosed);
        }
        if lane.next > u32::MAX as u64 {
            return Err(Error::IdExhausted);
        }
        self.sent.admit(self.tx.len())?;
        self.tx.try_reserve(1).map_err(|_| Error::NoBuffer)?;
        let mut tx = Tx::new(layout, signal, permit, self.shared.pool.budget.clone())?;
        // No fallible work remains after these two IDs are committed by this owner.
        let id = self
            .state
            .next_id
            .try_update(AcqRel, Acquire, |id| id.checked_add(1))
            .map_err(|_| Error::IdExhausted)?;
        let seq = lane.next as u32;
        tx.layout.identify_ordered(id, channel.id, seq, crc);
        tx.lane_permit = Some(lane_permit);
        lane.next += 1;
        self.tx.insert(id, tx);
        self.shared
            .socket
            .metrics
            .reliable_submitted
            .fetch_add(1, Relaxed);
        self.shared
            .socket
            .metrics
            .ordered_submitted
            .fetch_add(1, Relaxed);
        Ok((id, seq))
    }
    pub(super) fn ensure_receive_channel(&mut self, id: u16) -> Result<()> {
        self.ordered_state()?;
        let o = self.ordered.as_mut().unwrap();
        if o.receive.contains_key(&(id as u32)) {
            return Ok(());
        }
        o.receive.try_reserve(1).map_err(|_| Error::NoBuffer)?;
        let events = self.shared.channel_events()?.clone();
        let permit = Permit::take(&events.reserved, self.shared.config.max_channels)?;
        let lane = ReceiveLane::new(&self.shared, permit)?;
        o.receive.insert(id as u32, lane);
        o.events = Some(events);
        Ok(())
    }
    pub(super) fn ordered_gate(&mut self, id: u32, meta: Begin) -> Result<Option<Permit>> {
        self.ensure_receive_channel(meta.channel.get())?;
        if let Some(old) = self.ordered.as_ref().unwrap().deferred.get(&id)
            && old.meta != meta
        {
            return Err(Error::Protocol("conflicting deferred BEGIN"));
        }
        if self
            .ordered
            .as_ref()
            .unwrap()
            .deferred
            .iter()
            .any(|(&other, d)| {
                other != id && d.meta.channel == meta.channel && d.meta.seq == meta.seq
            })
        {
            return Err(Error::Protocol("conflicting deferred sequence"));
        }
        let lane = self
            .ordered
            .as_mut()
            .unwrap()
            .receive
            .get_mut(&(meta.channel.get() as u32))
            .unwrap();
        if let Some(end) = lane.end {
            let packet = super::channel_control::channel_packet(
                wire::ABORT,
                self.state.local,
                meta.channel.get(),
                1 - self.direction(),
                end.reason,
                end.last_delivered,
            );
            self.queue(packet);
            return Ok(None);
        }
        if lane.exchange.is_some() {
            return Ok(None);
        }
        let seq = u64::from(meta.seq.get());
        if seq < lane.grant_next {
            return Err(Error::Protocol("duplicate ordered sequence/ID"));
        }
        if seq != lane.grant_next {
            self.defer_ordered(id, meta)?;
            return Ok(None);
        }
        lane.ready
            .try_reserve(lane.sequence_ids.len() + 1)
            .map_err(|_| Error::NoBuffer)?;
        lane.sequence_ids
            .try_reserve(1)
            .map_err(|_| Error::NoBuffer)?;
        match Permit::take(
            &lane.inflight,
            self.shared
                .config
                .channel_window
                .min(self.shared.config.frame_capacity),
        ) {
            Ok(p) => Ok(Some(p)),
            Err(Error::NoBuffer) => {
                self.defer_ordered(id, meta)?;
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
    pub(super) fn defer_ordered(&mut self, id: u32, meta: Begin) -> Result<()> {
        let o = self.ordered.as_mut().ok_or(Error::OrderedUnsupported)?;
        if o.deferred.contains_key(&id) {
            return Ok(());
        }
        let quota = Permit::take(
            &self.shared.ordered_deferred,
            self.shared.config.frame_capacity,
        )?;
        let charge = self.shared.pool.budget.reserve(128)?;
        o.deferred.try_reserve(1).map_err(|_| Error::NoBuffer)?;
        o.deferred.insert(
            id,
            Deferred {
                meta,
                started: Instant::now(),
                _quota: quota,
                _charge: charge,
            },
        );
        Ok(())
    }
    pub(super) fn ordered_granted(&mut self, id: u32, meta: Begin) {
        let o = self.ordered.as_mut().unwrap();
        let lane = o.receive.get_mut(&(meta.channel.get() as u32)).unwrap();
        lane.sequence_ids.insert(meta.seq.get(), id);
        lane.grant_next += 1;
        o.deferred.remove(&id);
    }
    pub(super) fn promote_ordered(&mut self) {
        let window = self
            .shared
            .config
            .channel_window
            .min(self.shared.config.frame_capacity);
        for _ in 0..self.shared.config.batch_size {
            let next = self.ordered.as_ref().and_then(|o| {
                let len = o.deferred.len();
                let skip = o.defer_cursor % len.max(1);
                let eligible = |(&id, d): (&u32, &crate::ordered::Deferred)| {
                    o.receive
                        .get(&(d.meta.channel.get() as u32))
                        .filter(|r| {
                            r.exchange.is_none()
                                && r.end.is_none()
                                && r.grant_next == u64::from(d.meta.seq.get())
                                && r.inflight.load(Acquire) < window
                        })
                        .map(|_| (id, d.meta))
                };
                o.deferred
                    .iter()
                    .enumerate()
                    .skip(skip)
                    .find_map(|(i, p)| eligible(p).map(|(id, m)| (i, id, m)))
                    .or_else(|| {
                        o.deferred
                            .iter()
                            .enumerate()
                            .take(skip)
                            .find_map(|(i, p)| eligible(p).map(|(id, m)| (i, id, m)))
                    })
            });
            let Some((index, id, meta)) = next else {
                break;
            };
            let o = self.ordered.as_mut().unwrap();
            o.defer_cursor = (index + 1) % o.deferred.len().max(1);
            let _ = self.begin(id, meta);
        }
    }
    pub(super) fn ordered_ready(&mut self, id: u32, mut rx: super::rx::Rx, frame: Frame) {
        let channel = rx.meta.channel.get();
        let seq = rx.meta.seq.get();
        let len = frame.len();
        let o = self.ordered.as_mut().unwrap();
        let lane = o.receive.get_mut(&(channel as u32)).unwrap();
        let frame = frame.with_channel_permit(rx.lane_permit.take().unwrap());
        lane.ready.insert(
            seq,
            crate::ordered::Ready {
                frame,
                at: Instant::now(),
            },
        );
        rx.ready = true;
        self.rx.insert(id, rx);
        let m = &self.shared.socket.metrics;
        m.ordered_ready_frames.fetch_add(1, Relaxed);
        m.ordered_ready_bytes.fetch_add(len as u64, Relaxed);
        let n = m.ordered_buffered_frames.fetch_add(1, Relaxed) + 1;
        m.ordered_buffered_bytes.fetch_add(len as u64, Relaxed);
        m.ordered_reorder_peak.fetch_max(n, Relaxed);
        self.publish_ordered(channel);
        // COMPLETE already acknowledges every fragment of a published frame.
        // Only a Ready successor still waiting for its prefix needs a separate SACK.
        if let Some(rx) = self.rx.get(&id)
            && rx.meta.count.get() != 0
        {
            let ack = control::ack(
                self.state.local,
                id,
                &[wire::Range {
                    start: 0.into(),
                    len: rx.meta.count,
                }],
            );
            self.queue(ack);
        }
    }
    fn publish_ordered(&mut self, channel: u16) {
        loop {
            let lane = self
                .ordered
                .as_mut()
                .unwrap()
                .receive
                .get_mut(&(channel as u32))
                .unwrap();
            if lane.end.is_some() || lane.exchange.is_some() || lane.publish_next > u32::MAX as u64
            {
                return;
            }
            let seq = lane.publish_next as u32;
            let Some(ready) = lane.ready.remove(&seq) else {
                return;
            };
            let id = ready.frame.id();
            let len = ready.frame.len();
            let waited = ready.at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            let m = &self.shared.socket.metrics;
            m.ordered_buffered_frames.fetch_sub(1, Relaxed);
            m.ordered_buffered_bytes.fetch_sub(len as u64, Relaxed);
            if !self.shared.deliver(ready.frame) {
                self.close_incoming(channel, AbortReason::Shutdown);
                return;
            }
            lane.publish_next += 1;
            lane.sequence_ids.remove(&seq);
            m.ordered_delivered_frames.fetch_add(1, Relaxed);
            m.ordered_delivered_bytes.fetch_add(len as u64, Relaxed);
            m.ordered_hol_ns_total.fetch_add(waited, Relaxed);
            m.ordered_hol_ns_max.fetch_max(waited, Relaxed);
            let rx = self.rx.remove(&id).unwrap();
            self.terminal_rx(id, rx, Terminal::Complete, None);
        }
    }
    pub(super) fn ordered_rx_open(&self, channel: u16) -> bool {
        self.ordered
            .as_ref()
            .and_then(|o| o.receive.get(&(channel as u32)))
            .is_some_and(|r| r.end.is_none() && r.exchange.is_none())
    }
    pub(super) fn ordered_tx_open(&self, channel: u16) -> bool {
        self.ordered
            .as_ref()
            .and_then(|o| o.send.get(&(channel as u32)))
            .is_some_and(|s| !s.state.closing.load(Acquire))
    }
}
