//! Public session facade: peer identity, send paths and frame-layout ownership.

pub(crate) mod layout;
mod ordered;
mod reliable;

use crate::{
    Error, Integrity, Result,
    context::SessionState,
    dispatch::Owner,
    wire::{self, Begin, Header},
};
pub use layout::FrameLayout;
use layout::Storage;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::Ordering::{AcqRel, Acquire, Relaxed},
    },
};
use zerocopy::IntoBytes;

/// Datagram sizes at and above this avoid application-level train copies.
const SCATTER_THRESHOLD: usize = 16 * 1024;
/// Bounded scratch charge for a payload checksum job.
const SCRATCH_BASE_BYTES: usize = 512;
/// Per parallel group scratch charge.
const SCRATCH_GROUP_BYTES: usize = 16;
/// Segment indices per 16-bit group (wire `idx_bytes == 2`).
const SEGMENTS_PER_GROUP: usize = 65_536;

#[derive(Clone)]
pub struct Session {
    pub(crate) owner: Arc<Owner>,
    pub(crate) state: Arc<SessionState>,
}

#[derive(Clone, Copy, Debug)]
pub struct SendReceipt {
    pub frame_id: u32,
    pub application_bytes: usize,
    pub datagrams: usize,
}

impl Session {
    pub fn peer_addr(&self) -> SocketAddr {
        self.state.peer
    }

    pub fn datagram_size(&self) -> usize {
        self.state.d
    }

    pub fn max_frame_size(&self) -> usize {
        self.state.max_frame
    }

    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Acquire) || self.owner.shared.closed.load(Acquire)
    }

    pub fn allocate_frame(&self, len: usize) -> Result<FrameLayout> {
        self.allocate_train(len, true)
    }

    pub(crate) fn allocate_train(&self, len: usize, assign: bool) -> Result<FrameLayout> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        if len > self.state.max_frame {
            return Err(Error::MessageTooBig);
        }
        let meta = Begin::new(len, self.state.d, self.state.integrity.code())?;
        let total = len
            .checked_add(meta.count.get() as usize * meta.header_len())
            .ok_or(Error::MessageTooBig)?;
        let groups =
            (meta.count.get() as usize).div_ceil((SEGMENTS_PER_GROUP / self.state.d).max(1));
        let scratch = self.owner.shared.pool.budget.reserve(
            SCRATCH_BASE_BYTES
                .checked_add(groups * SCRATCH_GROUP_BYTES)
                .ok_or(Error::NoBuffer)?,
        )?;
        let mut bytes = self.owner.shared.pool.take(total)?;
        let id = if assign {
            self.state
                .next_id
                .try_update(AcqRel, Acquire, |id| id.checked_add(1))
                .map_err(|_| Error::IdExhausted)?
        } else {
            0
        };
        let datagram = meta.stride.get() as usize + meta.header_len();
        for (index, chunk) in bytes.chunks_mut(datagram).enumerate() {
            chunk[..wire::COMMON].copy_from_slice(
                Header::new(
                    if meta.idx_bytes == 2 { 0 } else { 1 },
                    self.state.local,
                    id,
                )
                .as_bytes(),
            );
            if meta.idx_bytes == 2 {
                chunk[13..15].copy_from_slice(&(index as u16).to_le_bytes());
            } else {
                chunk[13..17].copy_from_slice(&(index as u32).to_le_bytes());
            }
        }
        Ok(FrameLayout {
            state: self.state.clone(),
            id,
            meta,
            bytes,
            _scratch: scratch,
            storage: Storage::Train,
        })
    }

    pub fn send(&self, data: &[u8]) -> Result<SendReceipt> {
        self.owner.runtime.block_on(self.send_async(data))?
    }

    pub async fn send_async(&self, data: &[u8]) -> Result<SendReceipt> {
        if self.state.d >= SCATTER_THRESHOLD {
            return self.send_slice_async(data).await;
        }
        let mut frame = self.allocate_frame(data.len())?;
        let stride = frame.meta.stride.get() as usize;
        for (index, chunk) in data.chunks(stride).enumerate() {
            frame
                .payload_mut(index)
                .ok_or(Error::Protocol("layout segment"))?
                .copy_from_slice(chunk);
        }
        self.send_layout_async(frame).await
    }

    pub fn send_layout(&self, layout: FrameLayout) -> Result<SendReceipt> {
        self.owner
            .runtime
            .block_on(self.send_layout_async(layout))?
    }

    async fn yield_frame_boundary(&self, packets: usize) {
        let batch = self.owner.shared.config.batch_size;
        let previous = self.state.tx_batch.fetch_add(packets, Relaxed);
        if previous / batch != previous.wrapping_add(packets) / batch {
            tokio::task::yield_now().await;
        }
    }

    async fn send_slice_async(&self, data: &[u8]) -> Result<SendReceipt> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        if data.len() > self.state.max_frame {
            return Err(Error::MessageTooBig);
        }
        let _charge = self.owner.shared.pool.budget.reserve(SCRATCH_BASE_BYTES)?;
        let mut meta = Begin::new(data.len(), self.state.d, self.state.integrity.code())?;
        let id = self
            .state
            .next_id
            .try_update(AcqRel, Acquire, |id| id.checked_add(1))
            .map_err(|_| Error::IdExhausted)?;
        if self.state.integrity == Integrity::Crc32c {
            meta.crc = crate::checksum::append(meta.seed(id), data).into();
        }
        let socket = &self.owner.shared.socket;
        let begin = wire::Control::new(wire::BEGIN, self.state.local, id, meta.as_bytes());
        socket.send_to(begin.as_bytes(), self.state.peer).await?;
        let mut header = [0u8; wire::COMMON + 4];
        header[..wire::COMMON].copy_from_slice(
            Header::new(
                if meta.idx_bytes == 2 { 0 } else { 1 },
                self.state.local,
                id,
            )
            .as_bytes(),
        );
        for (index, payload) in data.chunks(meta.stride.get() as usize).enumerate() {
            if self.is_closed() {
                return Err(Error::Closed);
            }
            if meta.idx_bytes == 2 {
                header[13..15].copy_from_slice(&(index as u16).to_le_bytes());
            } else {
                header[13..17].copy_from_slice(&(index as u32).to_le_bytes());
            }
            socket
                .send_vectored_to(&header[..meta.header_len()], payload, self.state.peer)
                .await?;
            if (index + 1) % self.owner.shared.config.batch_size == 0 {
                tokio::task::yield_now().await;
            }
        }
        let packets = meta.count.get() as usize + 1;
        self.yield_frame_boundary(packets).await;
        socket.metrics.sent_frames.fetch_add(1, Relaxed);
        socket
            .metrics
            .sent_application_bytes
            .fetch_add(data.len() as u64, Relaxed);
        Ok(SendReceipt {
            frame_id: id,
            application_bytes: data.len(),
            datagrams: packets,
        })
    }

    pub async fn send_layout_async(&self, mut layout: FrameLayout) -> Result<SendReceipt> {
        if !Arc::ptr_eq(&layout.state, &self.state) {
            return Err(Error::Protocol("layout belongs to another session"));
        }
        if self.is_closed() {
            return Err(Error::Closed);
        }
        if self.state.integrity == Integrity::Crc32c {
            if let Some(pool) = crate::runtime::checksum_pool(
                &self.owner.runtime.0.compute,
                layout.len(),
                layout.meta.count.get(),
            ) {
                let (sender, receiver) = tokio::sync::oneshot::channel();
                self.owner
                    .shared
                    .socket
                    .metrics
                    .crc_parallel_jobs
                    .fetch_add(1, Relaxed);
                pool.spawn(move || {
                    layout.meta.crc = layout.checksum(true).into();
                    let _ = sender.send(layout);
                });
                layout = receiver.await.map_err(|_| Error::Closed)?;
            } else {
                layout.meta.crc = layout.checksum(false).into();
            }
        }
        let socket = &self.owner.shared.socket;
        let begin = wire::Control::new(
            wire::BEGIN,
            self.state.local,
            layout.id,
            layout.meta.as_bytes(),
        );
        socket.send_to(begin.as_bytes(), self.state.peer).await?;
        let datagram = layout.meta.stride.get() as usize + layout.meta.header_len();
        for (index, packet) in layout.bytes.chunks(datagram).enumerate() {
            if self.is_closed() {
                return Err(Error::Closed);
            }
            socket.send_to(packet, self.state.peer).await?;
            if (index + 1) % self.owner.shared.config.batch_size == 0 {
                tokio::task::yield_now().await;
            }
        }
        self.yield_frame_boundary(layout.segment_count() + 1).await;
        socket.metrics.sent_frames.fetch_add(1, Relaxed);
        socket
            .metrics
            .sent_application_bytes
            .fetch_add(layout.len() as u64, Relaxed);
        Ok(SendReceipt {
            frame_id: layout.id,
            application_bytes: layout.len(),
            datagrams: layout.segment_count() + 1,
        })
    }
}

#[cfg(test)]
mod test;
