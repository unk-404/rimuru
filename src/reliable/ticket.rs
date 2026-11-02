//! Completion tickets for reliable messages.
//!
//! A ticket observes the remote publication outcome; dropping or timing out a wait
//! never cancels the message.

use crate::{
    Error, Result,
    buffer::Reservation,
    dispatch::{Command, Owner},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

/// Remote transport publication, not consumption by the remote application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryReceipt {
    pub frame_id: u32,
    pub application_bytes: usize,
    pub channel: u16,
    pub channel_sequence: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[repr(u16)]
pub enum AbortReason {
    #[error("normal")]
    Normal = 0,
    #[error("resource limit")]
    Resource = 1,
    #[error("integrity failure")]
    Integrity = 2,
    #[error("protocol violation")]
    Protocol = 3,
    #[error("recovery deadline")]
    Timeout = 4,
    #[error("cancelled")]
    Cancelled = 5,
    #[error("path MTU")]
    Mtu = 6,
    #[error("shutdown")]
    Shutdown = 7,
    #[error("ID exhausted")]
    IdExhausted = 8,
}

impl AbortReason {
    pub(crate) fn read(value: u16) -> Result<Self> {
        Ok(match value {
            0 => Self::Normal,
            1 => Self::Resource,
            2 => Self::Integrity,
            3 => Self::Protocol,
            4 => Self::Timeout,
            5 => Self::Cancelled,
            6 => Self::Mtu,
            7 => Self::Shutdown,
            8 => Self::IdExhausted,
            _ => return Err(Error::Protocol("unknown abort reason")),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryOutcome {
    Delivered(DeliveryReceipt),
    Aborted(AbortReason),
    Unknown,
}

impl DeliveryOutcome {
    fn result(self) -> Result<DeliveryReceipt> {
        match self {
            Self::Delivered(receipt) => Ok(receipt),
            Self::Aborted(reason) => Err(Error::DeliveryAborted(reason)),
            Self::Unknown => Err(Error::OutcomeUnknown),
        }
    }
}

/// One terminal outcome, published at most once.
pub(crate) struct Signal {
    value: Mutex<Option<DeliveryOutcome>>,
    ready: Notify,
    _charge: Reservation,
}

impl Signal {
    pub fn new(charge: Reservation) -> Arc<Self> {
        Arc::new(Self {
            value: Mutex::new(None),
            ready: Notify::new(),
            _charge: charge,
        })
    }

    pub fn finish(&self, value: DeliveryOutcome) {
        let mut slot = self
            .value
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.is_none() {
            *slot = Some(value);
            self.ready.notify_waiters();
        }
    }

    pub fn outcome(&self) -> Option<DeliveryOutcome> {
        *self
            .value
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn wait(&self) -> DeliveryOutcome {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(value) = self.outcome() {
                return value;
            }
            notified.await;
        }
    }
}

/// Cloneable, thread-safe completion ticket. Dropping or timing out a wait does not cancel.
#[derive(Clone)]
pub struct SendTicket {
    pub(crate) owner: Arc<Owner>,
    pub(crate) signal: Arc<Signal>,
    pub(crate) state: Arc<crate::context::SessionState>,
    pub(crate) id: u32,
    pub(crate) channel: u16,
    pub(crate) sequence: Option<u32>,
}

impl SendTicket {
    pub fn channel(&self) -> u16 {
        self.channel
    }

    pub fn channel_sequence(&self) -> Option<u32> {
        self.sequence
    }

    pub fn frame_id(&self) -> u32 {
        self.id
    }

    pub fn outcome(&self) -> Option<DeliveryOutcome> {
        self.signal.outcome()
    }

    pub async fn wait_async(&self) -> Result<DeliveryReceipt> {
        self.signal.wait().await.result()
    }

    pub fn wait(&self) -> Result<DeliveryReceipt> {
        self.owner.runtime.block_on(self.wait_async())?
    }

    pub fn wait_timeout(&self, timeout: Duration) -> Result<DeliveryReceipt> {
        self.owner.runtime.block_on(async {
            tokio::time::timeout(timeout, self.wait_async())
                .await
                .map_err(|_| Error::TimedOut)?
        })?
    }

    /// Resolves to Delivered if publication won the race, Aborted only after remote confirmation.
    pub async fn cancel_async(&self) -> Result<DeliveryOutcome> {
        if let Some(outcome) = self.outcome() {
            return Ok(outcome);
        }
        self.owner
            .command(Command::Cancel {
                peer: self.state.peer,
                incarnation: self.state.local,
                id: self.id,
            })
            .await?;
        Ok(self.signal.wait().await)
    }

    pub fn cancel(&self) -> Result<DeliveryOutcome> {
        self.owner.runtime.block_on(self.cancel_async())?
    }
}
