//! Owner-local receive lane for one ordered channel.

use super::Exchange;
use crate::{
    Frame,
    buffer::{
        Permit, Reservation,
        table::{self, Map},
    },
    context::{ChannelEnd, Context},
    wire::Begin,
};
use std::sync::{Arc, atomic::AtomicUsize};

pub(crate) struct ReceiveLane {
    pub grant_next: u64,
    pub publish_next: u64,
    pub ready: Map<Ready>,
    pub sequence_ids: Map<u32>,
    pub inflight: Arc<AtomicUsize>,
    pub exchange: Option<Exchange>,
    pub end: Option<ChannelEnd>,
    pub event_permit: Option<Permit>,
    pub _quota: Permit,
    pub _charge: Reservation,
}

impl ReceiveLane {
    pub fn new(shared: &Context, event_permit: Permit) -> crate::Result<Self> {
        Ok(Self {
            grant_next: 0,
            publish_next: 0,
            ready: table::new(shared.pool.budget.clone()),
            sequence_ids: table::new(shared.pool.budget.clone()),
            inflight: Arc::new(AtomicUsize::new(0)),
            exchange: None,
            end: None,
            event_permit: Some(event_permit),
            _quota: Permit::take(&shared.channels_in_use, shared.config.max_channels)?,
            _charge: shared.pool.budget.reserve(768)?,
        })
    }

    pub fn prefix(&self) -> Option<u32> {
        self.publish_next.checked_sub(1).map(|value| value as u32)
    }
}

pub(crate) struct Deferred {
    pub meta: Begin,
    pub started: std::time::Instant,
    pub _quota: Permit,
    pub _charge: Reservation,
}

pub(crate) struct Ready {
    pub frame: Frame,
    pub at: std::time::Instant,
}
