//! One logical receive dispatcher: socket readiness, timers, commands, peers.

use super::{Command, Peer};
use crate::{context::Context, handshake::Manager, wire};
use hashbrown::HashMap;
use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::Ordering::{Acquire, Relaxed, Release},
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

/// Socket read buffer: maximum UDP payload on every supported target.
const RECEIVE_BUFFER_BYTES: usize = 65_536;
/// Housekeeping cadence for frame expiry, handshake retries and idle pool trim.
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_millis(100);
/// Reliable timers need finer granularity than frame expiry (RACK/RTO/NACK).
const RELIABLE_TICK: Duration = Duration::from_millis(1);
/// Idle pool blocks are returned in batches after this age (DOCS 13.1).
const IDLE_POOL_AGE: Duration = Duration::from_secs(5);

pub(crate) struct ReceiveLoop {
    pub(super) shared: Arc<Context>,
    pub(super) manager: Manager,
    pub(super) peers: HashMap<SocketAddr, Peer, foldhash::fast::RandomState>,
    pub(super) commands: mpsc::Receiver<Command>,
    pub(super) reliable_known: bool,
}

impl ReceiveLoop {
    pub(crate) fn new(
        shared: Arc<Context>,
        commands: mpsc::Receiver<Command>,
        cookie_key: [u8; 32],
    ) -> Self {
        Self {
            manager: Manager::new(shared.clone(), cookie_key),
            shared,
            peers: HashMap::with_hasher(foldhash::fast::RandomState::default()),
            commands,
            reliable_known: false,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut bytes = vec![0u8; RECEIVE_BUFFER_BYTES];
        let mut tick = tokio::time::interval(HOUSEKEEPING_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut trim = Instant::now();
        let mut reliable_tick = tokio::time::interval(RELIABLE_TICK);
        reliable_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut again = false;
        while !self.shared.closed.load(Acquire) {
            let has_reliable = self.reliable_known
                && self
                    .peers
                    .values()
                    .any(|peer| peer.reliable.as_ref().is_some_and(|r| r.needs_tick()));
            let blocked = self.reliable_known
                && self
                    .peers
                    .values()
                    .any(|peer| peer.reliable.as_ref().is_some_and(|r| r.blocked));
            tokio::select! {
                ready = self.shared.socket.writable(), if blocked => {
                    if ready.is_err() { break; }
                },
                _ = tokio::task::yield_now(), if again => {},
                _ = reliable_tick.tick(), if has_reliable => {
                    let now = Instant::now();
                    for peer in self.peers.values_mut() {
                        if let Some(reliable) = &mut peer.reliable { reliable.tick(now); }
                    }
                },
                _ = self.shared.shutdown_notified() => break,
                command = self.commands.recv() => match command {
                    Some(command) => self.drain_commands(command).await,
                    None => break,
                },
                _ = tick.tick() => {
                    let now = Instant::now();
                    for peer in self.peers.values_mut() { peer.receiver.expire(now); }
                    for (peer, packet) in self.manager.tick(now) {
                        let _ = self.shared.socket.send_to(packet.as_bytes(), peer).await;
                    }
                    if trim.elapsed() >= IDLE_POOL_AGE {
                        self.shared.pool.trim_idle(now, IDLE_POOL_AGE);
                        trim = now;
                    }
                },
                ready = self.shared.socket.readable() => {
                    if ready.is_err() {
                        self.shared.socket.metrics.io_errors.fetch_add(1, Relaxed);
                        break;
                    }
                    for _ in 0..self.shared.config.batch_size {
                        match self.shared.socket.try_recv_from(&mut bytes) {
                            Ok((len, peer)) => self.packet(peer, &bytes[..len]).await,
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                self.shared.socket.metrics.io_errors.fetch_add(1, Relaxed);
                                break;
                            }
                        }
                    }
                    // Always-ready UDP must yield cooperatively without one task per batch.
                    tokio::task::consume_budget().await;
                }
            }
            again = false;
            let now = Instant::now();
            if self.reliable_known {
                for peer in self.peers.values_mut() {
                    if let Some(reliable) = &mut peer.reliable {
                        again |= reliable.drive(now);
                    }
                }
                self.peers.retain(|peer, state| {
                    if !state.state.closed.load(Acquire) {
                        return true;
                    }
                    self.shared.sessions.remove(peer);
                    let close = wire::Control::new(
                        wire::EXT,
                        state.state.local,
                        0,
                        &[3, 0, 4, 0, 5, 0, 0, 0],
                    );
                    let _ = self.shared.socket.try_send_to(close.as_bytes(), *peer);
                    false
                });
                self.reliable_known = self.peers.values().any(|peer| peer.reliable.is_some());
            }
        }
        // A submit accepted into the command queue also owns a completion obligation.
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            if let Command::Submit { signal, .. } = command {
                signal.finish(crate::DeliveryOutcome::Aborted(
                    crate::AbortReason::Shutdown,
                ));
            }
        }
        self.shared.signal_close();
        for state in &self.shared.sessions {
            state.closed.store(true, Release);
        }
        self.shared.sessions.clear();
        self.peers.clear();
        self.shared.pool.trim();
        self.shared.mark_stopped();
    }
}
