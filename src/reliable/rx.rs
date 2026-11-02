use crate::{
    Result,
    buffer::{Permit, Reservation},
    context::Context,
    reassembly::assembly::Assembly,
    wire::{self, Begin, Control, Grant, Range},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
use zerocopy::IntoBytes;

pub(super) enum Payload {
    Receiving(Assembly),
    Checking(oneshot::Receiver<(Assembly, bool)>),
    Checked(Assembly, bool),
}
pub(super) enum Check {
    Incomplete,
    Inline,
    Background,
}
pub(super) struct Rx {
    pub meta: Begin,
    pub grant: Grant,
    pub payload: Option<Payload>,
    pub terminal_permit: Permit,
    pub charge: Reservation,
    pub lane_permit: Option<Permit>,
    pub ready: bool,
    pub stopped: bool,
    pub started: Instant,
    dirty: u32,
    ack_at: Instant,
    ack_cursor: u32,
    nack_at: Option<Instant>,
}
impl Rx {
    pub fn new(
        shared: &Context,
        meta: Begin,
        grant_id: u64,
        terminal_permit: Permit,
    ) -> Result<Self> {
        let charge = shared.pool.budget.reserve(512)?;
        let assembly = Assembly::new(shared, meta)?;
        let now = Instant::now();
        Ok(Self {
            meta,
            grant: Grant {
                grant_id: grant_id.into(),
                charged_bytes: super::receive::grant_charge(meta).into(),
                credit_seq: grant_id.into(),
            },
            payload: Some(Payload::Receiving(assembly)),
            terminal_permit,
            charge,
            lane_permit: None,
            ready: false,
            stopped: false,
            started: now,
            dirty: 0,
            ack_at: now,
            ack_cursor: 0,
            nack_at: None,
        })
    }
    pub fn grant_packet(&self, local: u64, id: u32) -> Control {
        Control::new(wire::BEGIN_ACCEPT, local, id, self.grant.as_bytes())
    }
    pub fn data(&mut self, index: u32, width: u8, bytes: &[u8]) -> Result<bool> {
        if let Some(Payload::Receiving(a)) = &mut self.payload {
            let fresh = a.ingest(index, width, bytes)?;
            self.dirty = self.dirty.saturating_add(1);
            Ok(fresh)
        } else {
            Ok(false)
        }
    }
    pub fn start_check(&mut self, shared: &Arc<Context>, id: u32) -> Check {
        if !matches!(self.payload,Some(Payload::Receiving(ref a)) if a.remaining==0) {
            return Check::Incomplete;
        }
        let Some(Payload::Receiving(a)) = self.payload.take() else {
            unreachable!()
        };
        if let Some(pool) = crate::runtime::receive_checksum_pool(&shared.compute, a.bytes.len())
            .filter(|_| a.meta.integrity == 1)
        {
            let (tx, rx) = oneshot::channel();
            shared
                .socket
                .metrics
                .crc_parallel_jobs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            pool.spawn(move || {
                let valid = a.verify(id, true);
                let _ = tx.send((a, valid));
            });
            self.payload = Some(Payload::Checking(rx));
            Check::Background
        } else {
            let valid = a.verify(id, false);
            self.payload = Some(Payload::Checked(a, valid));
            Check::Inline
        }
    }
    pub fn checking(&self) -> bool {
        !self.stopped && matches!(self.payload, Some(Payload::Checking(_)))
    }
    pub fn checked(&mut self) -> Option<(Assembly, bool)> {
        if self.stopped {
            return None;
        }
        if let Some(Payload::Checking(rx)) = &mut self.payload
            && let Ok((a, valid)) = rx.try_recv()
        {
            self.payload = Some(Payload::Checked(a, valid));
        }
        if matches!(self.payload, Some(Payload::Checked(..))) {
            let Some(Payload::Checked(a, v)) = self.payload.take() else {
                unreachable!()
            };
            Some((a, v))
        } else {
            None
        }
    }
    pub fn ack(&mut self, local: u64, id: u32, now: Instant, force: bool) -> Option<Control> {
        if !force
            && (self.dirty == 0
                || (self.dirty < 32 && now.duration_since(self.ack_at) < Duration::from_millis(1)))
        {
            return None;
        }
        if self.ready && force && self.meta.count.get() != 0 {
            return Some(super::control::ack(
                local,
                id,
                &[Range {
                    start: 0.into(),
                    len: self.meta.count,
                }],
            ));
        }
        let Some(Payload::Receiving(a)) = &self.payload else {
            return None;
        };
        let count = a.meta.count.get();
        let mut ranges = [Range {
            start: 0.into(),
            len: 0.into(),
        }; 11];
        let mut n = 0;
        let mut i = self.ack_cursor;
        while i < count && n < ranges.len() {
            while i < count && !a.received.contains(i as usize) {
                i += 1;
            }
            let start = i;
            while i < count && a.received.contains(i as usize) {
                i += 1;
            }
            if i > start {
                ranges[n] = Range {
                    start: start.into(),
                    len: (i - start).into(),
                };
                n += 1;
            }
        }
        self.ack_cursor = if i >= count { 0 } else { i };
        // Continue remaining ranges even without new arrivals.
        self.dirty = if self.ack_cursor == 0 { 0 } else { 32 };
        self.ack_at = now;
        (n != 0).then(|| super::control::ack(local, id, &ranges[..n]))
    }
    pub fn nack(&mut self, local: u64, id: u32, now: Instant, rto: Duration) -> Option<Control> {
        if now.duration_since(self.started) < Duration::from_millis(10)
            || self
                .nack_at
                .is_some_and(|last| now.duration_since(last) < rto)
        {
            return None;
        }
        let Some(Payload::Receiving(a)) = &self.payload else {
            return None;
        };
        let count = a.meta.count.get();
        let mut start = 0;
        while start < count && a.received.contains(start as usize) {
            start += 1;
        }
        let mut end = start;
        while end < count && !a.received.contains(end as usize) {
            end += 1;
        }
        if start == end || end == count {
            return None;
        }
        let mut body = [0; 12];
        body[..2].copy_from_slice(&1u16.to_le_bytes());
        body[4..].copy_from_slice(
            Range {
                start: start.into(),
                len: (end - start).into(),
            }
            .as_bytes(),
        );
        self.nack_at = Some(now);
        Some(Control::new(wire::NACK, local, id, &body))
    }
}
