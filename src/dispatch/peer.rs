//! Owner-side channel lookup and lazy reliable-receive state.

use super::Context;
use crate::{Error, Result, context::SessionState, reassembly::Receiver, reliable::ReliablePeer};
use std::sync::Arc;

pub(crate) struct Peer {
    pub state: Arc<SessionState>,
    pub receiver: Receiver,
    pub reliable: Option<Box<ReliablePeer>>,
    shared: Arc<Context>,
}

impl Peer {
    pub fn new(shared: Arc<Context>, state: Arc<SessionState>) -> Self {
        Self {
            receiver: Receiver::for_session(shared.clone(), state.clone()),
            state,
            shared,
            reliable: None,
        }
    }

    pub fn reliable(&mut self) -> Result<&mut ReliablePeer> {
        if !self.state.reliable {
            return Err(Error::ReliableUnsupported);
        }
        if self.reliable.is_none() {
            self.reliable = Some(Box::new(ReliablePeer::new(
                self.shared.clone(),
                self.state.clone(),
            )?));
        }
        Ok(self.reliable.as_deref_mut().expect("initialized above"))
    }
}
