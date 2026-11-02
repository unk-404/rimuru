//! Public endpoint facade: bind, sessions, receive queue, lifecycle.
//!
//! The endpoint owns one kernel UDP socket and delegates all protocol work to the
//! single owner in `dispatch`. Cloning an `Endpoint` clones a handle, not a socket.

use crate::{
    EndpointConfig, Error, Frame, MetricsSnapshot, Result, Session,
    buffer::{Budget, Pool},
    context::{ChannelEvent, Context},
    dispatch::{Command, Owner, ReceiveLoop},
    runtime::Runtime,
    udp::DatagramSocket,
};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering::Acquire},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

/// Fixed per-endpoint metadata reservation (queues, counters, mailboxes).
const BASE_CHARGE_BYTES: usize = 96 * 1024;
/// Per-frame-slot metadata charged up front.
const FRAME_META_BYTES: usize = 64;
/// Per-peer metadata charged up front.
const PEER_META_BYTES: usize = 128;
/// Bounded owner command queue. One slot is one accepted operation.
const COMMAND_QUEUE_CAPACITY: usize = 128;
/// Upper bound for a single receive batch.
const MAX_RECEIVE_BATCH: usize = 256;
/// Grace period added to the handshake deadline for a foreign executor.
const HANDSHAKE_GRACE: Duration = Duration::from_millis(200);

/// Cloneable and thread-safe. One bind creates exactly one kernel UDP socket.
#[derive(Clone)]
pub struct Endpoint {
    owner: Arc<Owner>,
}

impl Endpoint {
    /// Bind directly. The library owns and shares its I/O and CPU workers.
    pub fn bind(address: SocketAddr, config: EndpointConfig) -> Result<Self> {
        Self::bind_on(&Runtime::shared()?, address, config)
    }

    pub(crate) fn bind_on(
        runtime: &Runtime,
        address: SocketAddr,
        config: EndpointConfig,
    ) -> Result<Self> {
        config.validate()?;
        let budget = Budget::new(config.memory_budget, Some(runtime.0.budget.clone()));
        let base = BASE_CHARGE_BYTES
            .checked_add(config.frame_capacity * FRAME_META_BYTES)
            .and_then(|bytes| bytes.checked_add(config.max_peers * PEER_META_BYTES))
            .ok_or(Error::NoBuffer)?;
        let base_charge = budget.reserve(base)?;
        let socket = DatagramSocket::bind_on(runtime, address, config.socket_buffer_bytes)?;
        #[cfg(windows)]
        socket.enable_segmentation(config.datagram_size);
        let pool = Pool::new(budget, config.memory_budget / 8);
        let shared = Context::new(config, socket, pool, runtime.0.compute.clone(), base_charge);
        let (commands, receiver) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let task = ReceiveLoop::new(shared.clone(), receiver, runtime.0.cookie_key);
        runtime.0.handle.spawn(task.run());
        Ok(Self {
            owner: Arc::new(Owner::new(runtime.clone(), shared, commands)),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.owner.shared.socket.local_addr()?)
    }

    pub fn connect(&self, peer: SocketAddr) -> Result<Session> {
        self.owner.runtime.block_on(self.connect_async(peer))?
    }

    pub async fn connect_async(&self, peer: SocketAddr) -> Result<Session> {
        if self.owner.shared.closed.load(Acquire) {
            return Err(Error::Closed);
        }
        if let Some(session) = self.session(peer) {
            return Ok(session);
        }
        if peer.is_ipv4() != self.local_addr()?.is_ipv4() {
            return Err(Error::InvalidConfig(
                "peer address family differs from bound socket",
            ));
        }
        let (reply, receiver) = oneshot::channel();
        self.owner.try_command(Command::Connect { peer, reply })?;
        // Timer creation stays on our driver, so polling this future from a foreign
        // executor does not require a caller-owned Tokio context.
        let timeout = self.owner.shared.config.handshake_timeout + HANDSHAKE_GRACE;
        let state = self
            .owner
            .runtime
            .spawn(async move {
                tokio::time::timeout(timeout, receiver)
                    .await
                    .map_err(|_| Error::TimedOut)?
                    .map_err(|_| Error::Closed)?
            })
            .await
            .map_err(|_| Error::Closed)??;
        Ok(Session {
            owner: self.owner.clone(),
            state,
        })
    }

    pub fn session(&self, peer: SocketAddr) -> Option<Session> {
        self.owner
            .shared
            .sessions
            .get(&peer)
            .filter(|state| !state.closed.load(Acquire))
            .map(|state| Session {
                owner: self.owner.clone(),
                state: state.clone(),
            })
    }

    pub fn try_channel_event(&self) -> Result<ChannelEvent> {
        self.owner.shared.pop_channel_event().ok_or_else(|| {
            if self.owner.shared.closed.load(Acquire) {
                Error::Closed
            } else {
                Error::WouldBlock
            }
        })
    }

    pub async fn channel_event_async(&self) -> Result<ChannelEvent> {
        let events = self.owner.shared.channel_events()?.clone();
        loop {
            let notified = events.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_channel_event() {
                Err(Error::WouldBlock) => notified.await,
                result => return result,
            }
        }
    }

    pub fn channel_event_timeout(&self, timeout: Duration) -> Result<ChannelEvent> {
        self.owner.runtime.block_on(async {
            tokio::time::timeout(timeout, self.channel_event_async())
                .await
                .map_err(|_| Error::TimedOut)?
        })?
    }

    pub fn try_recv(&self) -> Result<Frame> {
        self.owner.shared.pop_frame().ok_or_else(|| {
            if self.owner.shared.closed.load(Acquire) {
                Error::Closed
            } else {
                Error::WouldBlock
            }
        })
    }

    pub async fn recv_async(&self) -> Result<Frame> {
        loop {
            let notified = self.owner.shared.recv_notified();
            tokio::pin!(notified);
            // Register before checking the queue: multiple consumers must not lose an
            // enqueue between observing empty and beginning to wait.
            notified.as_mut().enable();
            match self.try_recv() {
                Err(Error::WouldBlock) => notified.await,
                result => return result,
            }
        }
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Frame> {
        self.owner.runtime.block_on(async {
            tokio::time::timeout(timeout, self.recv_async())
                .await
                .map_err(|_| Error::TimedOut)?
        })?
    }

    pub fn recv_batch(&self, out: &mut Vec<Frame>, max: usize) -> Result<usize> {
        if max == 0 || max > MAX_RECEIVE_BATCH {
            return Err(Error::InvalidConfig("batch limit 1..=256"));
        }
        let start = out.len();
        for _ in 0..max {
            match self.try_recv() {
                Ok(frame) => out.push(frame),
                Err(Error::WouldBlock | Error::Closed) if out.len() > start => break,
                Err(error) => return Err(error),
            }
        }
        Ok(out.len() - start)
    }

    /// Wait for one frame, then drain currently available frames without waiting
    /// for the batch to fill. Dropping the future before it returns consumes none.
    pub async fn recv_batch_async(&self, out: &mut Vec<Frame>, max: usize) -> Result<usize> {
        if max == 0 || max > MAX_RECEIVE_BATCH {
            return Err(Error::InvalidConfig("batch limit 1..=256"));
        }
        let first = self.recv_async().await?;
        out.push(first);
        if max == 1 {
            return Ok(1);
        }
        match self.recv_batch(out, max - 1) {
            Ok(count) => Ok(count + 1),
            Err(Error::WouldBlock | Error::Closed) => Ok(1),
            Err(error) => Err(error),
        }
    }

    pub async fn disconnect_async(&self, peer: SocketAddr) -> Result<()> {
        let (reply, receiver) = oneshot::channel();
        self.owner
            .command(Command::Disconnect { peer, reply })
            .await?;
        receiver.await.map_err(|_| Error::Closed)
    }

    pub fn disconnect(&self, peer: SocketAddr) -> Result<()> {
        self.owner.runtime.block_on(self.disconnect_async(peer))?
    }

    pub async fn close_async(&self) {
        self.owner.shared.signal_close();
        loop {
            let done = self.owner.shared.stopped_notified();
            tokio::pin!(done);
            done.as_mut().enable();
            if self.owner.shared.stopped() {
                return;
            }
            done.await;
        }
    }

    pub fn close(&self) -> Result<()> {
        self.owner.runtime.block_on(self.close_async())
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        let mut snapshot = self.owner.shared.socket.metrics();
        let pool = &self.owner.shared.pool;
        snapshot.memory_used = pool.budget.used();
        snapshot.memory_peak = pool.budget.peak();
        snapshot.pool_cached = pool.cached();
        snapshot
    }

    /// Owner access for the C FFI; not part of the public Rust contract.
    pub(crate) fn owner(&self) -> &Arc<Owner> {
        &self.owner
    }
}

#[cfg(test)]
mod test;
