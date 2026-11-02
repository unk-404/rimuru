//! Endpoint resource and policy configuration.

use super::{Integrity, ReliableConfig};
use crate::{Error, Result};
use std::time::Duration;

/// Smallest UDP payload the wire format can encode.
const MIN_DATAGRAM: usize = 128;
/// Largest UDP payload; IPv4 loopback experiment boundary, not a public-internet default.
const MAX_DATAGRAM: usize = 65_507;
/// Smallest endpoint budget accepted by validation.
const MIN_BUDGET: usize = 4 << 20;
/// Capacity/batch upper bounds for every per-endpoint resource.
const MAX_CAPACITY: usize = 65_536;
const MAX_BATCH: usize = 256;
/// Orphan caching may not exceed `memory_budget / ORPHAN_DIVISOR`.
const ORPHAN_BUDGET_DIVISOR: usize = 16;
const DEFAULT_DATAGRAM: usize = 1200;
const DEFAULT_FRAME: usize = 16 << 20;
const DEFAULT_BUDGET: usize = 64 << 20;
const DEFAULT_FRAME_CAPACITY: usize = 256;
const DEFAULT_PEERS: usize = 1024;
const DEFAULT_CHANNELS: usize = 256;
const DEFAULT_CHANNEL_WINDOW: usize = 32;
const DEFAULT_SOCKET_BUFFER: usize = 1 << 20;
const DEFAULT_FRAME_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_BATCH: usize = 32;
const DEFAULT_ORPHAN_BYTES: usize = 1 << 20;

#[derive(Clone, Debug)]
pub struct EndpointConfig {
    pub datagram_size: usize,
    pub max_frame_size: usize,
    pub memory_budget: usize,
    /// Partial, validating, queued and application-held frames share this limit.
    pub frame_capacity: usize,
    pub max_peers: usize,
    /// Endpoint-wide directional channel records, including closed tombstones.
    pub max_channels: usize,
    /// Per-direction in-flight/held frame limit, capped by `frame_capacity`.
    pub channel_window: usize,
    pub socket_buffer_bytes: usize,
    pub frame_timeout: Duration,
    pub handshake_timeout: Duration,
    pub integrity: Integrity,
    pub batch_size: usize,
    pub orphan_bytes: usize,
    pub reliable: ReliableConfig,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            datagram_size: DEFAULT_DATAGRAM,
            max_frame_size: DEFAULT_FRAME,
            memory_budget: DEFAULT_BUDGET,
            frame_capacity: DEFAULT_FRAME_CAPACITY,
            max_peers: DEFAULT_PEERS,
            max_channels: DEFAULT_CHANNELS,
            channel_window: DEFAULT_CHANNEL_WINDOW,
            socket_buffer_bytes: DEFAULT_SOCKET_BUFFER,
            frame_timeout: DEFAULT_FRAME_TIMEOUT,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            integrity: Integrity::Crc32c,
            batch_size: DEFAULT_BATCH,
            orphan_bytes: DEFAULT_ORPHAN_BYTES,
            reliable: ReliableConfig::default(),
        }
    }
}

impl EndpointConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        self.reliable.validate()?;
        if !(1..=MAX_CAPACITY).contains(&self.max_channels)
            || !(1..=MAX_CAPACITY).contains(&self.channel_window)
        {
            return Err(Error::InvalidConfig("channel capacity/window 1..=65536"));
        }
        if !(MIN_DATAGRAM..=MAX_DATAGRAM).contains(&self.datagram_size) {
            return Err(Error::InvalidConfig("UDP payload limit 128..=65507"));
        }
        if self.max_frame_size > u32::MAX as usize || self.memory_budget < MIN_BUDGET {
            return Err(Error::InvalidConfig(
                "u32 frame length and >=4 MiB budget required",
            ));
        }
        if self.frame_capacity == 0
            || self.frame_capacity > MAX_CAPACITY
            || self.max_peers == 0
            || self.max_peers > MAX_CAPACITY
        {
            return Err(Error::InvalidConfig("frame/peer capacities 1..=65536"));
        }
        if !(1..=MAX_BATCH).contains(&self.batch_size)
            || self.frame_timeout.is_zero()
            || self.handshake_timeout.is_zero()
        {
            return Err(Error::InvalidConfig(
                "batch 1..=256, nonzero deadlines required",
            ));
        }
        if self.orphan_bytes > self.memory_budget / ORPHAN_BUDGET_DIVISOR {
            return Err(Error::InvalidConfig("orphan budget exceeds budget/16"));
        }
        Ok(())
    }
}
