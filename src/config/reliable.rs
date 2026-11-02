//! Reliable-layer recovery limits. These are bounds, not feature switches.

use crate::{Error, Result};
use std::time::Duration;

const DEFAULT_MESSAGE_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_INITIAL_RTO: Duration = Duration::from_millis(500);
const DEFAULT_MAX_ROUNDS: u32 = 32;
/// DOCS 13.1 freezes at most 32 recovery rounds.
const MAX_ROUNDS: u32 = 32;
const MIN_RTO: Duration = Duration::from_millis(1);
const MAX_RTO: Duration = Duration::from_secs(60);

/// Bounded recovery policy for L2/L3 messages.
#[derive(Clone, Debug)]
pub struct ReliableConfig {
    pub message_timeout: Duration,
    pub initial_rto: Duration,
    pub max_recovery_rounds: u32,
}

impl Default for ReliableConfig {
    fn default() -> Self {
        Self {
            message_timeout: DEFAULT_MESSAGE_TIMEOUT,
            initial_rto: DEFAULT_INITIAL_RTO,
            max_recovery_rounds: DEFAULT_MAX_ROUNDS,
        }
    }
}

impl ReliableConfig {
    pub(super) fn validate(&self) -> Result<()> {
        if self.message_timeout.is_zero()
            || self.initial_rto < MIN_RTO
            || self.initial_rto > MAX_RTO
            || !(1..=MAX_ROUNDS).contains(&self.max_recovery_rounds)
        {
            return Err(Error::InvalidConfig(
                "nonzero reliable deadline, RTO 1ms..60s, recovery rounds 1..=32",
            ));
        }
        Ok(())
    }
}
