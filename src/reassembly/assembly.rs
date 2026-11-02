//! Shared ownership, fragment validation and whole-message verification for L1/L2.
use super::mask::Segments;
use crate::{
    Error, Frame, Result,
    buffer::{Lease, Permit, Reservation},
    context::Context,
    wire::Begin,
};
use std::{net::SocketAddr, time::Instant};

pub(crate) struct Assembly {
    pub meta: Begin,
    pub bytes: Lease,
    pub received: Segments,
    pub remaining: u32,
    pub started: Instant,
    pub charge: Reservation,
    pub permit: Permit,
    pub(super) ordered_crc: Option<(u32, u32)>,
}
impl Assembly {
    pub fn new(shared: &Context, meta: Begin) -> Result<Self> {
        let permit = Permit::take(&shared.frames_in_use, shared.config.frame_capacity)?;
        let charge = shared.pool.budget.reserve(
            512 + (meta.count.get() as usize).div_ceil(usize::BITS as usize) * size_of::<usize>()
                + (meta.total.get() as usize).div_ceil(65536) * 16,
        )?;
        Ok(Self {
            meta,
            bytes: shared.pool.take(meta.total.get() as usize)?,
            received: Segments::new(meta.count.get() as usize)?,
            remaining: meta.count.get(),
            started: Instant::now(),
            charge,
            permit,
            ordered_crc: (meta.integrity == 1
                && meta.stride.get() >= 16384
                && (meta.total.get() as usize) < crate::checksum::PARALLEL_MIN)
                .then_some((0, 0)),
        })
    }
    /// Duplicate fragments do not overwrite acknowledged bytes.
    pub fn ingest(&mut self, index: u32, width: u8, payload: &[u8]) -> Result<bool> {
        let m = self.meta;
        if width != m.idx_bytes || index >= m.count.get() {
            return Err(Error::Protocol("fragment index/width"));
        }
        let offset = u64::from(index) * u64::from(m.stride.get());
        let expected = (u64::from(m.total.get()) - offset).min(u64::from(m.stride.get())) as usize;
        if payload.len() != expected {
            return Err(Error::Protocol("fragment length"));
        }
        if self.received.contains(index as usize) {
            return Ok(false);
        }
        self.bytes[offset as usize..offset as usize + expected].copy_from_slice(payload);
        if let Some((next, crc)) = &mut self.ordered_crc {
            if index == *next {
                *crc = crate::checksum::append(*crc, payload);
                *next += 1;
            } else {
                self.ordered_crc = None;
            }
        }
        self.received.insert(index as usize);
        self.remaining -= 1;
        Ok(true)
    }
    pub fn verify(&self, id: u32, parallel: bool) -> bool {
        if self.meta.integrity != 1 {
            return true;
        }
        let seed = self.meta.seed(id);
        let actual = if let Some((_, crc)) = self.ordered_crc.filter(|_| self.remaining == 0) {
            crate::checksum::combine(seed, crc, self.bytes.len())
        } else if parallel {
            crate::checksum::parallel_contiguous(seed, &self.bytes)
        } else {
            crate::checksum::append(seed, &self.bytes)
        };
        actual == self.meta.crc.get()
    }
    pub fn into_frame(self, peer: SocketAddr, id: u32) -> Frame {
        Frame::new(
            peer,
            id,
            self.meta.channel.get(),
            self.bytes,
            self.charge,
            self.permit,
        )
        .with_layer(self.meta.layer)
        .with_sequence(self.meta.seq.get())
    }
}
