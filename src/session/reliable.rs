//! Reliable-unordered (L2) send entry points on `Session`.

use super::{FrameLayout, Session, layout::Storage};
use crate::{
    DeliveryReceipt, Error, Integrity, Result, SendTicket, buffer::Permit, dispatch::Command,
    reliable::Signal, wire::Begin,
};
use std::sync::{
    Arc,
    atomic::Ordering::{AcqRel, Acquire, Relaxed},
};

impl Session {
    pub fn supports_reliable(&self) -> bool {
        self.state.reliable
    }

    /// Submit without waiting for remote delivery. Multiple tickets may be in flight.
    pub fn submit_reliable(&self, data: &[u8]) -> Result<SendTicket> {
        self.owner
            .runtime
            .block_on(self.submit_reliable_async(data))?
    }

    pub async fn submit_reliable_async(&self, data: &[u8]) -> Result<SendTicket> {
        if !self.supports_reliable() {
            return Err(Error::ReliableUnsupported);
        }
        if self.state.d >= super::SCATTER_THRESHOLD {
            let layout = self.retain_reliable_slice(data)?;
            return self.submit_reliable_layout_async(layout).await;
        }
        let mut layout = self.allocate_frame(data.len())?;
        let stride = layout.meta.stride.get() as usize;
        for (index, payload) in data.chunks(stride).enumerate() {
            layout
                .payload_mut(index)
                .ok_or(Error::Protocol("layout segment"))?
                .copy_from_slice(payload);
        }
        self.submit_reliable_layout_async(layout).await
    }

    pub(super) fn retain_reliable_slice(&self, data: &[u8]) -> Result<FrameLayout> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        if data.len() > self.state.max_frame {
            return Err(Error::MessageTooBig);
        }
        let mut meta = Begin::new(data.len(), self.state.d, self.state.integrity.code())?;
        meta.layer = 2;
        let scratch = self.owner.shared.pool.budget.reserve(
            super::SCRATCH_BASE_BYTES
                .checked_add(
                    data.len().div_ceil(crate::checksum::CHUNK_BYTES) * super::SCRATCH_GROUP_BYTES,
                )
                .ok_or(Error::NoBuffer)?,
        )?;
        let mut bytes = self.owner.shared.pool.take(data.len())?;
        bytes.copy_from_slice(data);
        let id = self
            .state
            .next_id
            .try_update(AcqRel, Acquire, |id| id.checked_add(1))
            .map_err(|_| Error::IdExhausted)?;
        Ok(FrameLayout {
            state: self.state.clone(),
            id,
            meta,
            bytes,
            _scratch: scratch,
            storage: Storage::Contiguous,
        })
    }

    pub fn submit_reliable_layout(&self, layout: FrameLayout) -> Result<SendTicket> {
        self.owner
            .runtime
            .block_on(self.submit_reliable_layout_async(layout))?
    }

    pub async fn submit_reliable_layout_async(
        &self,
        mut layout: FrameLayout,
    ) -> Result<SendTicket> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        if !self.state.reliable {
            return Err(Error::ReliableUnsupported);
        }
        if !Arc::ptr_eq(&layout.state, &self.state) {
            return Err(Error::Protocol("layout belongs to another session"));
        }
        let permit = Permit::take(
            &self.owner.shared.outgoing_reliable,
            self.owner.shared.config.frame_capacity,
        )?;
        let signal = Signal::new(self.owner.shared.pool.budget.reserve(256)?);
        layout.meta.layer = 2;
        let datagram = layout.meta.stride.get() as usize + layout.meta.header_len();
        if layout.storage == Storage::Train {
            for packet in layout.bytes.chunks_mut(datagram) {
                packet[0] |= 1;
            }
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
        let ticket = SendTicket {
            owner: self.owner.clone(),
            signal: signal.clone(),
            state: self.state.clone(),
            id: layout.id,
            channel: 0,
            sequence: None,
        };
        self.owner.try_command(Command::Submit {
            layout,
            signal,
            permit,
        })?;
        Ok(ticket)
    }

    pub async fn send_reliable_async(&self, data: &[u8]) -> Result<DeliveryReceipt> {
        self.submit_reliable_async(data).await?.wait_async().await
    }

    pub fn send_reliable(&self, data: &[u8]) -> Result<DeliveryReceipt> {
        self.owner
            .runtime
            .block_on(self.send_reliable_async(data))?
    }
}
