//! Channel admission and ordering above the shared reliable transport.

mod channel;
mod receive;

use crate::{
    AbortReason, Error, Result,
    buffer::{
        Reservation,
        table::{self, Map},
    },
    context::{ChannelState, Context, Events},
};
pub use channel::Channel;
pub(crate) use receive::{Deferred, Ready, ReceiveLane};
use std::{sync::Arc, time::Instant};

pub(crate) struct SendLane {
    pub state: Arc<ChannelState>,
    pub next: u64,
    pub known_prefix: Option<u32>,
    pub exchange: Option<Exchange>,
}

#[derive(Clone, Copy)]
pub(crate) struct Exchange {
    pub reason: AbortReason,
    pub prefix: Option<u32>,
    pub last: Option<Instant>,
    pub started: Instant,
    pub attempts: u32,
}

impl Exchange {
    pub fn new(reason: AbortReason, prefix: Option<u32>) -> Self {
        Self {
            reason,
            prefix,
            last: None,
            started: Instant::now(),
            attempts: 0,
        }
    }
}

/// Owner-local channel tables for one peer.
pub(crate) struct State {
    pub send: Map<SendLane>,
    pub receive: Map<ReceiveLane>,
    pub deferred: Map<Deferred>,
    pub events: Option<Arc<Events>>,
    pub defer_cursor: usize,
    _charge: Reservation,
}

impl State {
    pub fn new(shared: &Context) -> Result<Self> {
        let budget = &shared.pool.budget;
        Ok(Self {
            _charge: shared.pool.budget.reserve(size_of::<Self>() + 64)?,
            send: table::new(budget.clone()),
            receive: table::new(budget.clone()),
            deferred: table::new(budget.clone()),
            events: None,
            defer_cursor: 0,
        })
    }

    pub fn active(&self) -> bool {
        !self.deferred.is_empty()
            || self.send.values().any(|lane| lane.exchange.is_some())
            || self.receive.values().any(|lane| lane.exchange.is_some())
    }

    pub fn open(&mut self, shared: &Context, id: u16) -> Result<Arc<ChannelState>> {
        if let Some(lane) = self.send.get(&(id as u32)) {
            return Ok(lane.state.clone());
        }
        self.send.try_reserve(1).map_err(|_| Error::NoBuffer)?;
        let state = ChannelState::new(shared, id)?;
        self.send.insert(
            id as u32,
            SendLane {
                state: state.clone(),
                next: 0,
                known_prefix: None,
                exchange: None,
            },
        );
        Ok(state)
    }
}

#[cfg(test)]
mod test;
