use super::{AbortReason, DeliveryOutcome, Signal, recovery::Recovery};
use crate::{
    Error, FrameLayout, Result,
    buffer::{BudgetAllocator, Permit},
    wire::{self, Control, Range},
};
use allocator_api2::vec::Vec;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use zerocopy::IntoBytes;

#[derive(Clone, Copy, Default)]
struct Packet {
    sent: Option<Instant>,
    attempts: u8,
    acked: bool,
    inflight: bool,
    repair: bool,
}
pub(super) struct Tx {
    pub layout: FrameLayout,
    pub signal: Arc<Signal>,
    _permit: Permit,
    pub(super) lane_permit: Option<Permit>,
    packets: Vec<Packet, BudgetAllocator>,
    pub granted: bool,
    grant_id: u64,
    control_rounds: u32,
    begin_sent: bool,
    pub cancel: Option<AbortReason>,
    pub started: Instant,
    last_control: Option<Instant>,
    last_progress: Instant,
    probe: bool,
    rounds: u32,
    cursor: usize,
    acked_prefix: usize,
    latest_ack: Option<Instant>,
    unsent: usize,
    max_attempts: u8,
}
impl Tx {
    pub fn new(
        layout: FrameLayout,
        signal: Arc<Signal>,
        permit: Permit,
        budget: Arc<crate::buffer::Budget>,
    ) -> Result<Self> {
        let mut packets = Vec::new_in(BudgetAllocator(budget));
        packets
            .try_reserve_exact(layout.segment_count())
            .map_err(|_| Error::NoBuffer)?;
        packets.resize(layout.segment_count(), Packet::default());
        let now = Instant::now();
        let unsent = layout.segment_count();
        Ok(Self {
            layout,
            signal,
            _permit: permit,
            lane_permit: None,
            packets,
            granted: false,
            grant_id: 0,
            control_rounds: 0,
            begin_sent: false,
            cancel: None,
            started: now,
            last_control: None,
            last_progress: now,
            probe: false,
            rounds: 0,
            cursor: 0,
            acked_prefix: 0,
            latest_ack: None,
            unsent,
            max_attempts: 0,
        })
    }
    fn d(&self) -> usize {
        self.layout.meta.stride.get() as usize + self.layout.meta.header_len()
    }
    fn bytes(&self, i: usize) -> usize {
        self.layout.meta.header_len()
            + (self.layout.len() - i * self.layout.meta.stride.get() as usize)
                .min(self.layout.meta.stride.get() as usize)
    }
    pub fn control(&self, now: Instant, rto: Duration, direction: u8) -> Option<Control> {
        let timeout = rto
            .saturating_mul(1 << self.control_rounds.saturating_sub(1).min(7))
            .min(Duration::from_secs(60));
        if self
            .last_control
            .is_some_and(|last| now.duration_since(last) < timeout)
        {
            return None;
        }
        let local = self.layout.state.local;
        let id = self.layout.id;
        if let Some(reason) = self.cancel {
            return Some(Control::new(
                wire::ABORT,
                local,
                id,
                super::control::abort_body(reason, direction).as_bytes(),
            ));
        }
        if !self.granted {
            return Some(Control::new(
                wire::BEGIN,
                local,
                id,
                self.layout.meta.as_bytes(),
            ));
        }
        if self.unsent == 0 && now.saturating_duration_since(self.last_progress) >= rto / 2 {
            return Some(Control::new(
                wire::PING,
                local,
                id,
                &(id as u64).to_le_bytes(),
            ));
        }
        None
    }
    pub fn grant(&mut self, id: u64) -> Result<()> {
        if self.granted && self.grant_id != id {
            return Err(Error::Protocol("conflicting grant"));
        }
        if !self.granted {
            self.granted = true;
            self.grant_id = id;
            self.control_rounds = 0;
            self.last_control = None;
            self.last_progress = Instant::now();
        }
        Ok(())
    }
    pub fn cancel(&mut self) {
        if self.cancel.is_none() {
            self.cancel = Some(AbortReason::Cancelled);
            self.control_rounds = 0;
            self.last_control = None;
        }
    }
    pub fn can_complete(&self) -> bool {
        // For zero DATA, COMPLETE is stronger than a separately received grant.
        // Require our BEGIN to have been sent, and every nonempty segment to be sent.
        self.unsent == 0 && (self.granted || (self.layout.is_empty() && self.begin_sent))
    }
    pub fn control_sent(&mut self, now: Instant) {
        if !self.granted || self.cancel.is_some() {
            self.control_rounds += 1;
            if self.cancel.is_none() {
                self.begin_sent = true;
            }
        }
        self.last_control = Some(now);
    }
    pub fn next(&mut self, now: Instant, recovery: &Recovery) -> Option<usize> {
        if !self.granted || self.cancel.is_some() {
            return None;
        }
        // Cursor is reset only on a repair event; steady state visits each packet once.
        while self.cursor < self.packets.len() {
            let i = self.cursor;
            let p = self.packets[i];
            if !p.acked && (p.sent.is_none() || p.repair) {
                return recovery.can_send(self.bytes(i), now).then_some(i);
            }
            self.cursor += 1;
        }
        None
    }
    pub fn packet_len(&self, i: usize) -> usize {
        self.bytes(i)
    }
    #[cfg(windows)]
    pub fn initial_segment_run(&self, first: usize, limit: usize, segment_size: usize) -> usize {
        if self.layout.storage != crate::session::layout::Storage::Contiguous
            || segment_size == 0
            || self.d() != segment_size
        {
            return 0;
        }
        self.packets[first..]
            .iter()
            .take(limit.min(16))
            .take_while(|p| p.sent.is_none() && !p.acked && !p.repair)
            .count()
    }
    #[cfg(windows)]
    pub fn try_send_initial_segments(
        &self,
        first: usize,
        count: usize,
        socket: &crate::udp::DatagramSocket,
    ) -> std::io::Result<usize> {
        let meta = self.layout.meta;
        let mut common = wire::Header::new(
            if meta.idx_bytes == 2 { 0 } else { 1 },
            self.layout.state.local,
            self.layout.id,
        );
        common.kind_flags |= 1;
        let mut headers = [[0u8; 17]; 16];
        for (offset, header) in headers.iter_mut().take(count).enumerate() {
            header[..wire::COMMON].copy_from_slice(common.as_bytes());
            let index = first + offset;
            if meta.idx_bytes == 2 {
                header[13..15].copy_from_slice(&(index as u16).to_le_bytes());
            } else {
                header[13..17].copy_from_slice(&(index as u32).to_le_bytes());
            }
        }
        let mut buffers: [std::io::IoSlice<'_>; 32] =
            std::array::from_fn(|_| std::io::IoSlice::new(&[]));
        let stride = meta.stride.get() as usize;
        let mut expected = 0;
        for (offset, header) in headers.iter().take(count).enumerate() {
            let start = (first + offset) * stride;
            let end = (start + stride).min(self.layout.bytes.len());
            let payload = &self.layout.bytes[start..end];
            buffers[2 * offset] = std::io::IoSlice::new(&header[..meta.header_len()]);
            buffers[2 * offset + 1] = std::io::IoSlice::new(payload);
            expected += meta.header_len() + payload.len();
        }
        socket.try_send_segments(
            &buffers[..count * 2],
            self.layout.state.peer,
            count,
            expected,
        )
    }
    pub fn try_send(
        &self,
        i: usize,
        socket: &crate::udp::DatagramSocket,
    ) -> std::io::Result<usize> {
        if self.layout.storage == crate::session::layout::Storage::Train {
            let d = self.d();
            return socket.try_send_to(
                &self.layout.bytes[i * d..((i + 1) * d).min(self.layout.bytes.len())],
                self.layout.state.peer,
            );
        }
        let m = self.layout.meta;
        let mut header = [0u8; 17];
        let mut common = wire::Header::new(
            if m.idx_bytes == 2 { 0 } else { 1 },
            self.layout.state.local,
            self.layout.id,
        );
        common.kind_flags |= 1;
        header[..wire::COMMON].copy_from_slice(common.as_bytes());
        if m.idx_bytes == 2 {
            header[13..15].copy_from_slice(&(i as u16).to_le_bytes());
        } else {
            header[13..17].copy_from_slice(&(i as u32).to_le_bytes());
        }
        let offset = i * m.stride.get() as usize;
        let end = (offset + m.stride.get() as usize).min(self.layout.bytes.len());
        socket.try_send_vectored_to(
            &header[..m.header_len()],
            &self.layout.bytes[offset..end],
            self.layout.state.peer,
        )
    }
    pub fn sent(&mut self, i: usize, now: Instant, recovery: &mut Recovery) -> bool {
        let bytes = self.bytes(i);
        let p = &mut self.packets[i];
        let retransmit = p.attempts != 0;
        p.sent = Some(now);
        p.attempts = p.attempts.saturating_add(1);
        self.max_attempts = self.max_attempts.max(p.attempts);
        if !retransmit {
            self.unsent -= 1;
            // Queueing, grant latency and active first transmission are not
            // feedback silence. Repairs do not restart this clock: RTO must
            // still advance without ACK progress under a blackhole.
            self.last_progress = now;
        }
        p.repair = false;
        p.inflight = true;
        recovery.sent(bytes, now);
        self.cursor = i + 1;
        retransmit
    }
    pub fn validate(&self, ranges: &[Range]) -> Result<()> {
        for r in ranges {
            let end = (r.start.get() + r.len.get()) as usize;
            if end > self.packets.len()
                || self.packets[r.start.get() as usize..end]
                    .iter()
                    .any(|p| p.sent.is_none())
            {
                return Err(Error::Protocol("ACK/NACK for unsent fragment"));
            }
        }
        Ok(())
    }
    pub fn ack(&mut self, ranges: &[Range], now: Instant, recovery: &mut Recovery) {
        for r in ranges {
            for i in (r.start.get() as usize).max(self.acked_prefix)
                ..(r.start.get() + r.len.get()) as usize
            {
                let bytes = self.bytes(i);
                let p = &mut self.packets[i];
                if p.acked {
                    continue;
                }
                p.acked = true;
                p.repair = false;
                self.last_progress = now;
                self.probe = false;
                self.rounds = 0;
                if let Some(sent) = p.sent {
                    // A retransmitted packet's ACK cannot establish which transmission arrived.
                    if p.attempts == 1 {
                        self.latest_ack = Some(self.latest_ack.map_or(sent, |old| old.max(sent)));
                    }
                    let sample = (p.attempts == 1).then_some(now.duration_since(sent));
                    if p.inflight {
                        recovery.ack(bytes, sample);
                        p.inflight = false;
                    }
                }
            }
        }
        while self.acked_prefix < self.packets.len() && self.packets[self.acked_prefix].acked {
            self.acked_prefix += 1;
        }
    }
    fn repair(&mut self, i: usize, now: Instant, recovery: &mut Recovery) -> bool {
        let bytes = self.bytes(i);
        let p = &mut self.packets[i];
        if p.acked || p.sent.is_none() || p.repair {
            return false;
        }
        if p.inflight {
            recovery.lost(bytes, now);
            p.inflight = false;
        }
        p.repair = true;
        self.cursor = self.cursor.min(i);
        true
    }
    pub fn nack(&mut self, ranges: &[Range], now: Instant, recovery: &mut Recovery) {
        for r in ranges {
            for i in r.start.get() as usize..(r.start.get() + r.len.get()) as usize {
                self.repair(i, now, recovery);
            }
        }
    }
    /// Returns (RACK losses, new RTO round, tail probe).
    pub fn tick(&mut self, now: Instant, recovery: &mut Recovery) -> (usize, bool, bool) {
        if !self.granted || self.cancel.is_some() {
            return (0, false, false);
        }
        let mut rack = 0;
        if let Some(latest) = self.latest_ack {
            let delay = recovery.rack_delay();
            for i in self.acked_prefix..self.packets.len() {
                if self.packets[i]
                    .sent
                    .is_some_and(|sent| latest.saturating_duration_since(sent) >= delay)
                    && self.repair(i, now, recovery)
                {
                    rack += 1;
                }
            }
        }
        let quiet = now.duration_since(self.last_progress);
        let timeout = recovery
            .rto
            .saturating_mul(1u32 << self.rounds.min(7))
            .min(Duration::from_secs(60));
        let mut tail = false;
        if !self.probe && quiet >= timeout / 2 && self.unsent == 0 {
            if let Some(i) = self.packets.iter().rposition(|p| !p.acked) {
                tail = self.repair(i, now, recovery);
            }
            self.last_control = None;
            self.probe = true;
        }
        if quiet >= timeout {
            for i in self.acked_prefix..self.packets.len() {
                self.repair(i, now, recovery);
            }
            self.last_control = None;
            self.last_progress = now;
            self.rounds += 1;
            return (rack, true, tail);
        }
        (rack, false, tail)
    }
    pub fn exhausted(&self, now: Instant, timeout: Duration, rounds: u32) -> bool {
        now.duration_since(self.started) >= timeout
            || self.rounds >= rounds
            || self.control_rounds >= rounds
            || self.max_attempts >= 32
    }
    pub fn release_inflight(&self, recovery: &mut Recovery) {
        for (i, p) in self.packets.iter().enumerate() {
            if p.inflight {
                recovery.inflight = recovery.inflight.saturating_sub(self.bytes(i));
            }
        }
    }
    pub fn unknown(&self) {
        self.signal.finish(DeliveryOutcome::Unknown);
    }
}
