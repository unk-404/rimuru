//! Single-owner frame storage and checksums for packet trains or contiguous
//! payloads. Ordered builders select storage without committing IDs.

use super::SessionState;
use crate::{
    buffer::{Lease, Reservation},
    wire::{Begin, COMMON, Header},
};
use rayon::prelude::*;
use std::sync::Arc;
use zerocopy::IntoBytes;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Storage {
    /// Pool-owned datagram train: headers are pre-filled and payload slots follow.
    Train,
    /// One contiguous payload buffer; headers are composed at send time.
    Contiguous,
}

/// A single-use, pool-owned frame. Applications may fill payload slots directly.
pub struct FrameLayout {
    pub(crate) state: Arc<SessionState>,
    pub(crate) id: u32,
    pub(crate) meta: Begin,
    pub(crate) bytes: Lease,
    pub(super) _scratch: Reservation,
    pub(crate) storage: Storage,
}

impl FrameLayout {
    /// Zero for an unsubmitted Channel layout; use the ordered ticket for its final ID.
    pub fn frame_id(&self) -> u32 {
        self.id
    }

    pub fn len(&self) -> usize {
        self.meta.total.get() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn segment_count(&self) -> usize {
        self.meta.count.get() as usize
    }

    pub fn payload_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        if index >= self.segment_count() {
            return None;
        }
        let header = if self.storage == Storage::Train {
            self.meta.header_len()
        } else {
            0
        };
        let datagram = self.meta.stride.get() as usize + header;
        let start = index * datagram + header;
        let end = (index * datagram + datagram).min(self.bytes.len());
        Some(&mut self.bytes[start..end])
    }

    pub(crate) fn identify_ordered(&mut self, id: u32, channel: u16, seq: u32, payload_crc: u32) {
        self.id = id;
        self.meta.layer = 3;
        self.meta.meta_flags = 1;
        self.meta.channel = channel.into();
        self.meta.seq = seq.into();
        if self.meta.integrity == 1 {
            self.meta.crc =
                crate::checksum::combine(self.meta.seed(id), payload_crc, self.len()).into();
        }
        if self.storage == Storage::Train {
            let datagram = self.meta.stride.get() as usize + self.meta.header_len();
            let mut header = Header::new(
                if self.meta.idx_bytes == 2 { 0 } else { 1 },
                self.state.local,
                id,
            );
            header.kind_flags |= 1;
            for packet in self.bytes.chunks_mut(datagram) {
                packet[..COMMON].copy_from_slice(header.as_bytes());
            }
        }
    }

    pub(crate) fn checksum(&self, parallel: bool) -> u32 {
        self.checksum_seed(self.meta.seed(self.id), parallel)
    }

    pub(crate) fn payload_crc(&self, parallel: bool) -> u32 {
        self.checksum_seed(0, parallel)
    }

    fn checksum_seed(&self, seed: u32, parallel: bool) -> u32 {
        if self.storage == Storage::Contiguous {
            return if parallel {
                crate::checksum::parallel_contiguous(seed, &self.bytes)
            } else {
                crate::checksum::append(seed, &self.bytes)
            };
        }
        let header = self.meta.header_len();
        let datagram = self.meta.stride.get() as usize + header;
        if !parallel {
            return crate::checksum::chunks(
                seed,
                self.bytes.chunks(datagram).map(|chunk| &chunk[header..]),
            );
        }
        // Indexed collection preserves segment order, unlike an unordered reduction.
        let group = datagram * (crate::checksum::CHUNK_BYTES / datagram).max(1);
        let partial: Vec<_> = self
            .bytes
            .par_chunks(group)
            .map(|chunks| {
                let crc = crate::checksum::chunks(
                    0,
                    chunks.chunks(datagram).map(|segment| &segment[header..]),
                );
                (crc, chunks.len() - chunks.len().div_ceil(datagram) * header)
            })
            .collect();
        partial.into_iter().fold(seed, |crc, (next, len)| {
            crate::checksum::combine(crc, next, len)
        })
    }
}
