//! Ordered-channel (L3) entry points on `Session`.

use super::{FrameLayout, Session, layout::Storage};
use crate::{Channel, DeliveryReceipt, Error, Result, SendTicket, dispatch::Command, wire::Begin};

impl Session {
    pub fn supports_ordered(&self) -> bool {
        self.state.ordered
    }

    pub async fn channel_async(&self, id: u16) -> Result<Channel> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        if !self.supports_ordered() {
            return Err(Error::OrderedUnsupported);
        }
        if id == 0
            && let Some(state) = self.state.default_channel.get()
        {
            return Ok(Channel {
                session: self.clone(),
                state: state.clone(),
            });
        }
        let (reply, receiver) = tokio::sync::oneshot::channel();
        self.owner.try_command(Command::OpenChannel {
            peer: self.state.peer,
            incarnation: self.state.local,
            id,
            reply,
        })?;
        let state = receiver.await.map_err(|_| Error::Closed)??;
        Ok(Channel {
            session: self.clone(),
            state,
        })
    }

    pub fn channel(&self, id: u16) -> Result<Channel> {
        self.owner.runtime.block_on(self.channel_async(id))?
    }

    pub async fn submit_ordered_async(&self, data: &[u8]) -> Result<SendTicket> {
        self.channel_async(0).await?.submit_async(data).await
    }

    pub fn submit_ordered(&self, data: &[u8]) -> Result<SendTicket> {
        self.owner
            .runtime
            .block_on(self.submit_ordered_async(data))?
    }

    pub async fn send_ordered_async(&self, data: &[u8]) -> Result<DeliveryReceipt> {
        self.submit_ordered_async(data).await?.wait_async().await
    }

    pub fn send_ordered(&self, data: &[u8]) -> Result<DeliveryReceipt> {
        self.owner.runtime.block_on(self.send_ordered_async(data))?
    }

    pub(crate) fn allocate_ordered_frame(&self, len: usize) -> Result<FrameLayout> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        if len > self.state.max_frame {
            return Err(Error::MessageTooBig);
        }
        let mut frame = if self.state.d >= super::SCATTER_THRESHOLD {
            let meta = Begin::new(len, self.state.d, self.state.integrity.code())?;
            let scratch = self.owner.shared.pool.budget.reserve(
                super::SCRATCH_BASE_BYTES
                    .checked_add(
                        len.div_ceil(crate::checksum::CHUNK_BYTES) * super::SCRATCH_GROUP_BYTES,
                    )
                    .ok_or(Error::NoBuffer)?,
            )?;
            FrameLayout {
                state: self.state.clone(),
                id: 0,
                meta,
                bytes: self.owner.shared.pool.take(len)?,
                _scratch: scratch,
                storage: Storage::Contiguous,
            }
        } else {
            self.allocate_train(len, false)?
        };
        frame.meta.layer = 3;
        frame.meta.meta_flags = 1;
        Ok(frame)
    }
}
