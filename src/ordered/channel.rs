//! Public ordered-channel handle. The peer's channel with the same ID is separate.

use crate::{
    DeliveryReceipt, Error, FrameLayout, Result, SendTicket, Session,
    buffer::Permit,
    context::{ChannelEnd, ChannelState},
    dispatch::Command,
    reliable::Signal,
};
use std::{
    sync::{Arc, atomic::Ordering::Acquire},
    time::Duration,
};

/// Cloneable sending direction of an ordered channel.
#[derive(Clone)]
pub struct Channel {
    pub(crate) session: Session,
    pub(crate) state: Arc<ChannelState>,
}

impl Channel {
    pub fn id(&self) -> u16 {
        self.state.id
    }

    pub fn is_closed(&self) -> bool {
        self.state.closing.load(Acquire) || self.session.is_closed()
    }

    pub fn end(&self) -> Option<ChannelEnd> {
        self.state.end()
    }

    pub fn allocate_frame(&self, len: usize) -> Result<FrameLayout> {
        if self.is_closed() {
            return Err(Error::ChannelClosed);
        }
        let mut frame = self.session.allocate_ordered_frame(len)?;
        frame.meta.channel = self.id().into();
        Ok(frame)
    }

    pub async fn submit_async(&self, data: &[u8]) -> Result<SendTicket> {
        let mut frame = self.allocate_frame(data.len())?;
        let stride = frame.meta.stride.get() as usize;
        for (index, chunk) in data.chunks(stride).enumerate() {
            frame
                .payload_mut(index)
                .ok_or(Error::Protocol("layout segment"))?
                .copy_from_slice(chunk);
        }
        self.submit_layout_async(frame).await
    }

    pub fn submit(&self, data: &[u8]) -> Result<SendTicket> {
        self.session
            .owner
            .runtime
            .block_on(self.submit_async(data))?
    }

    pub fn submit_layout(&self, frame: FrameLayout) -> Result<SendTicket> {
        self.session
            .owner
            .runtime
            .block_on(self.submit_layout_async(frame))?
    }

    pub async fn submit_layout_async(&self, mut layout: FrameLayout) -> Result<SendTicket> {
        if self.is_closed() {
            return Err(Error::ChannelClosed);
        }
        if !Arc::ptr_eq(&layout.state, &self.session.state) {
            return Err(Error::Protocol("layout belongs to another session"));
        }
        let shared = &self.session.owner.shared;
        let lane_permit = Permit::take(
            &self.state.inflight,
            shared
                .config
                .channel_window
                .min(shared.config.frame_capacity),
        )?;
        let permit = Permit::take(&shared.outgoing_reliable, shared.config.frame_capacity)?;
        let signal = Signal::new(shared.pool.budget.reserve(256)?);
        let payload_crc = if layout.meta.integrity == 1 {
            if let Some(pool) = crate::runtime::checksum_pool(
                &shared.compute,
                layout.len(),
                layout.meta.count.get(),
            ) {
                let (sender, receiver) = tokio::sync::oneshot::channel();
                shared
                    .socket
                    .metrics
                    .crc_parallel_jobs
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                pool.spawn(move || {
                    let crc = layout.payload_crc(true);
                    let _ = sender.send((layout, crc));
                });
                let (returned, crc) = receiver.await.map_err(|_| Error::Closed)?;
                layout = returned;
                crc
            } else {
                layout.payload_crc(false)
            }
        } else {
            0
        };
        let (reply, receiver) = tokio::sync::oneshot::channel();
        self.session.owner.try_command(Command::OrderedSubmit {
            layout,
            payload_crc,
            signal: signal.clone(),
            permit,
            lane_permit,
            channel: self.state.clone(),
            reply,
        })?;
        let (id, sequence) = receiver.await.map_err(|_| Error::Closed)??;
        Ok(SendTicket {
            owner: self.session.owner.clone(),
            signal,
            state: self.session.state.clone(),
            id,
            channel: self.id(),
            sequence: Some(sequence),
        })
    }

    pub async fn send_async(&self, data: &[u8]) -> Result<DeliveryReceipt> {
        self.submit_async(data).await?.wait_async().await
    }

    pub fn send(&self, data: &[u8]) -> Result<DeliveryReceipt> {
        self.session.owner.runtime.block_on(self.send_async(data))?
    }

    pub async fn close_async(&self) -> Result<ChannelEnd> {
        if let Some(end) = self.end() {
            return Ok(end);
        }
        self.session
            .owner
            .command(Command::CloseChannel {
                peer: self.session.state.peer,
                incarnation: self.session.state.local,
                channel: self.id(),
            })
            .await?;
        Ok(self.state.wait().await)
    }

    pub fn close(&self) -> Result<ChannelEnd> {
        self.session.owner.runtime.block_on(self.close_async())?
    }

    pub fn wait_closed_timeout(&self, timeout: Duration) -> Result<ChannelEnd> {
        self.session.owner.runtime.block_on(async {
            tokio::time::timeout(timeout, self.wait_closed_async())
                .await
                .map_err(|_| Error::TimedOut)
        })?
    }

    /// Wait for the channel's terminal state without blocking an executor thread.
    pub async fn wait_closed_async(&self) -> ChannelEnd {
        self.state.wait().await
    }
}
