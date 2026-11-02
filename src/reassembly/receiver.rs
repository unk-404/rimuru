//! One peer's reassembly state: active frames, orphan cache and anti-replay window.

use super::{assembly::Assembly as Partial, contexts::Contexts};
use crate::{
    Error, Result,
    buffer::{BytePermit, Lease, Reservation},
    context::Context,
    wire::Begin,
};
use bitvec::prelude::*;
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Instant,
};

/// Anti-replay window for *new* L1 frame IDs. Existing partial frames stay addressable.
const SEEN_WINDOW: u32 = 8192;
/// Orphan datagram cache bounds per peer.
const MAX_ORPHANS: usize = 64;
const MAX_ORPHAN_BYTES: usize = 64 * 1024;
const ORPHAN_CHARGE_BYTES: usize = 128;

struct Orphan {
    id: u32,
    index: u32,
    width: u8,
    bytes: Lease,
    started: Instant,
    _quota: BytePermit,
    _charge: Reservation,
}

struct Seen {
    high: u32,
    bits: BitVec<usize, Lsb0>,
}

impl Seen {
    fn contains(&self, id: u32) -> bool {
        id <= self.high && (self.high - id >= SEEN_WINDOW || self.bits[(id % SEEN_WINDOW) as usize])
    }

    fn insert(&mut self, id: u32) {
        if id > self.high {
            if id - self.high >= SEEN_WINDOW {
                self.bits.fill(false);
            } else {
                for next in self.high + 1..=id {
                    self.bits.set((next % SEEN_WINDOW) as usize, false);
                }
            }
            self.high = id;
        }
        self.bits.set((id % SEEN_WINDOW) as usize, true);
    }
}

pub(crate) struct Receiver {
    shared: Arc<Context>,
    peer: SocketAddr,
    session: Option<Arc<crate::context::SessionState>>,
    frames: Contexts,
    orphans: VecDeque<Orphan>,
    orphan_bytes: usize,
    seen: Seen,
}

impl Receiver {
    pub fn new(shared: Arc<Context>, peer: SocketAddr) -> Self {
        let frames = Contexts::new(shared.pool.budget.clone());
        Self {
            shared,
            peer,
            session: None,
            frames,
            orphans: VecDeque::new(),
            orphan_bytes: 0,
            seen: Seen {
                high: 0,
                bits: bitvec![usize, Lsb0; 0; SEEN_WINDOW as usize],
            },
        }
    }

    pub fn for_session(shared: Arc<Context>, state: Arc<crate::context::SessionState>) -> Self {
        let mut receiver = Self::new(shared, state.peer);
        receiver.session = Some(state);
        receiver
    }

    pub fn begin(
        &mut self,
        id: u32,
        meta: Begin,
        max: usize,
        datagram: usize,
        integrity: u8,
    ) -> Result<()> {
        meta.validate(max, datagram, integrity)?;
        if meta.layer != 1 {
            return Err(Error::Protocol("L1 BEGIN layer"));
        }
        if id == 0 {
            return Err(Error::Protocol("zero frame ID"));
        }
        if let Some(old) = self.frames.get(id) {
            if old.meta != meta {
                self.frames.remove(id);
                return Err(Error::Protocol("conflicting BEGIN"));
            }
            return Ok(());
        }
        if self.seen.contains(id) {
            return Ok(());
        }
        self.frames.insert(id, Partial::new(&self.shared, meta)?)?;
        self.seen.insert(id);
        if meta.count.get() == 0 {
            self.complete(id);
        }
        // Replay orphan datagrams that arrived before their BEGIN.
        let orphans = self.orphans.len();
        for _ in 0..orphans {
            let orphan = self.orphans.pop_front().expect("counted above");
            if orphan.id == id {
                self.orphan_bytes -= orphan.bytes.len();
                self.fragment(id, orphan.index, orphan.width, &orphan.bytes, false)?;
            } else {
                self.orphans.push_back(orphan);
            }
        }
        Ok(())
    }

    pub fn data(&mut self, id: u32, index: u32, width: u8, payload: &[u8]) -> Result<()> {
        self.fragment(id, index, width, payload, true)
    }

    fn fragment(
        &mut self,
        id: u32,
        index: u32,
        width: u8,
        payload: &[u8],
        cache: bool,
    ) -> Result<()> {
        let Some(partial) = self.frames.get_mut(id) else {
            if cache && !self.seen.contains(id) {
                self.orphan(id, index, width, payload);
            }
            return Ok(());
        };
        if !partial.ingest(index, width, payload)? {
            self.shared
                .socket
                .metrics
                .duplicate_segments
                .fetch_add(1, Relaxed);
        }
        if partial.remaining == 0 {
            self.complete(id);
        }
        Ok(())
    }

    fn orphan(&mut self, id: u32, index: u32, width: u8, payload: &[u8]) {
        let result = (|| -> Result<Orphan> {
            if self.orphans.len() >= MAX_ORPHANS
                || self.orphan_bytes.saturating_add(payload.len()) > MAX_ORPHAN_BYTES
            {
                return Err(Error::NoBuffer);
            }
            let quota = BytePermit::take(
                &self.shared.orphan_bytes,
                payload.len(),
                self.shared.config.orphan_bytes,
            )?;
            let charge = self.shared.pool.budget.reserve(ORPHAN_CHARGE_BYTES)?;
            let mut bytes = self.shared.pool.take(payload.len())?;
            bytes.copy_from_slice(payload);
            Ok(Orphan {
                id,
                index,
                width,
                bytes,
                started: Instant::now(),
                _quota: quota,
                _charge: charge,
            })
        })();
        match result {
            Ok(orphan) => {
                self.orphan_bytes += payload.len();
                self.orphans.push_back(orphan);
            }
            Err(_) => {
                self.shared
                    .socket
                    .metrics
                    .orphan_drops
                    .fetch_add(1, Relaxed);
            }
        }
    }

    fn complete(&mut self, id: u32) {
        let partial = self.frames.remove(id).expect("completed frame exists");
        let pool = if partial.meta.integrity == 1 {
            crate::runtime::receive_checksum_pool(&self.shared.compute, partial.bytes.len())
                .cloned()
        } else {
            None
        };
        let parallel = pool.is_some();
        let shared = self.shared.clone();
        let peer = self.peer;
        let session = self.session.clone();
        let finish = move || {
            if session
                .as_ref()
                .is_some_and(|state| state.closed.load(std::sync::atomic::Ordering::Acquire))
            {
                return;
            }
            let incarnation = session.as_ref().map_or(0, |state| state.remote);
            if !partial.verify(id, parallel) {
                shared
                    .socket
                    .metrics
                    .integrity_failures
                    .fetch_add(1, Relaxed);
                return;
            }
            shared.deliver(partial.into_frame(peer, id).with_incarnation(incarnation));
        };
        if let Some(pool) = pool {
            // Admission already bounded outstanding jobs by frame_capacity and byte budget.
            self.shared
                .socket
                .metrics
                .crc_parallel_jobs
                .fetch_add(1, Relaxed);
            pool.spawn(finish);
        } else {
            finish();
        }
    }

    pub fn expire(&mut self, now: Instant) {
        let expired = self.frames.expire(now, self.shared.config.frame_timeout);
        self.shared
            .socket
            .metrics
            .expired_frames
            .fetch_add(expired as u64, Relaxed);
        while self.orphans.front().is_some_and(|orphan| {
            now.duration_since(orphan.started) >= self.shared.config.frame_timeout
        }) {
            self.orphan_bytes -= self.orphans.pop_front().expect("front exists").bytes.len();
            self.shared
                .socket
                .metrics
                .orphan_drops
                .fetch_add(1, Relaxed);
        }
    }
}
