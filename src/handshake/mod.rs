//! Handshake: source-cookie challenge, parameter negotiation and session creation.
//!
//! The handshake manager lives on the owner. It validates the source address and a
//! keyed time-bucket cookie before allocating any receive buffer, and it never
//! replaces an active session with a replayed old handshake.

mod cookie;
mod manager;
mod pending;

pub(crate) use manager::Manager;

use crate::{Result, context::SessionState};
use std::sync::Arc;
use tokio::sync::oneshot;

/// Reply channel for one `connect` caller; all concurrent callers share a session.
pub(crate) type Reply = oneshot::Sender<Result<Arc<SessionState>>>;
