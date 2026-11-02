//! Fault-injection relay shared by network integration tests and diagnostics.
//!
//! Two external relay sockets forward between the two real endpoints. Each actual
//! `Endpoint` still owns exactly one UDP socket; the relay is measurement tooling,
//! never part of the product path.

use rand_chacha::{
    ChaCha8Rng,
    rand_core::{RngCore, SeedableRng},
};
use rimuru::{Result, benchmarking::Environment, udp::DatagramSocket};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

/// Maximum datagrams drained from one side per turn.
const RELAY_BATCH: usize = 32;
/// Queue bounds for delayed/reordered datagrams.
const MAX_QUEUED_DATAGRAMS: usize = 4096;
const MAX_QUEUED_BYTES: usize = 8 << 20;
/// Extra delay applied to every Nth datagram to force reordering.
const REORDER_JITTER: Duration = Duration::from_millis(4);

#[derive(Clone, Debug, Default)]
pub struct Faults {
    pub seed: u64,
    pub loss_ppm: u32,
    /// One-way delay, applied in both directions, including all feedback.
    pub delay: Duration,
    pub duplicate_every: u64,
    pub reorder_every: u64,
    /// `(wire kind, flags, direction)`; drops the first match.
    pub drop_once: Vec<(u8, u8, u8)>,
    /// Drop the first forward reliable DATA datagram with this fragment index.
    pub drop_data_index_once: Option<u32>,
    pub corrupt_data_once: bool,
    pub hold_first_frame: Duration,
}

#[derive(Default)]
pub struct RelayStats {
    pub offered: AtomicU64,
    pub dropped: AtomicU64,
    pub forwarded: AtomicU64,
    pub duplicated: AtomicU64,
    pub queue_drops: AtomicU64,
    pub peak_queue_bytes: AtomicU64,
    pub direction_packets: [AtomicU64; 2],
    pub injected_drops: [AtomicU64; 2],
    pub wire_bytes: AtomicU64,
}

pub struct Relay {
    pub address: SocketAddr,
    pub stats: Arc<RelayStats>,
    pub blackhole: Arc<AtomicBool>,
    stop: Option<oneshot::Sender<()>>,
    worker: tokio::task::JoinHandle<()>,
}

struct Queued {
    at: Instant,
    direction: u8,
    bytes: Vec<u8>,
}

impl Relay {
    pub fn new(
        env: &Environment,
        sender: SocketAddr,
        receiver: SocketAddr,
        mut faults: Faults,
    ) -> Result<Self> {
        let left = env.bind_socket("127.0.0.1:0".parse().unwrap(), 4 << 20)?;
        let right = env.bind_socket("127.0.0.1:0".parse().unwrap(), 4 << 20)?;
        let address = left.local_addr()?;
        let stats = Arc::new(RelayStats::default());
        let counters = stats.clone();
        let blackhole = Arc::new(AtomicBool::new(false));
        let blocked = blackhole.clone();
        let (stop, mut stopping) = oneshot::channel();
        let worker = env.spawn(async move {
            let mut rng = ChaCha8Rng::seed_from_u64(faults.seed);
            let mut buffer = [0u8; 65_536];
            let mut queue = Vec::<Queued>::new();
            let mut hold_until: Option<Instant> = None;
            let mut queued_bytes = 0usize;
            let mut tick = tokio::time::interval(Duration::from_millis(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let direction = tokio::select! {
                    _ = &mut stopping => break,
                    _ = left.readable() => Some(0u8),
                    _ = right.readable() => Some(1u8),
                    _ = tick.tick(), if !queue.is_empty() => None,
                };
                if let Some(direction) = direction {
                    let source = if direction == 0 { &left } else { &right };
                    for _ in 0..RELAY_BATCH {
                        let (len, from) = match source.try_recv_from(&mut buffer) {
                            Ok(value) => value,
                            Err(_) => break,
                        };
                        if len == 0 {
                            continue;
                        }
                        if from != if direction == 0 { sender } else { receiver } {
                            continue;
                        }
                        let serial = counters.offered.fetch_add(1, Relaxed) + 1;
                        counters.direction_packets[direction as usize].fetch_add(1, Relaxed);
                        let kind = buffer[0] >> 4;
                        let flags = buffer[0] & 15;
                        if kind <= 1
                            && flags == 1
                            && len >= 15
                            && u32::from_le_bytes(buffer[9..13].try_into().unwrap()) == 1
                            && !faults.hold_first_frame.is_zero()
                        {
                            let until = *hold_until
                                .get_or_insert_with(|| Instant::now() + faults.hold_first_frame);
                            if Instant::now() < until {
                                counters.dropped.fetch_add(1, Relaxed);
                                counters.injected_drops[direction as usize].fetch_add(1, Relaxed);
                                continue;
                            }
                        }

                        let directed = faults
                            .drop_once
                            .iter()
                            .position(|rule| *rule == (kind, flags, direction));
                        if let Some(index) = directed {
                            faults.drop_once.remove(index);
                        }
                        let index = if direction == 0 && flags == 1 {
                            match kind {
                                0 if len >= 15 => {
                                    Some(u16::from_le_bytes([buffer[13], buffer[14]]) as u32)
                                }
                                1 if len >= 17 => {
                                    Some(u32::from_le_bytes(buffer[13..17].try_into().unwrap()))
                                }
                                _ => None,
                            }
                        } else {
                            None
                        };
                        let drop_index = index.is_some() && index == faults.drop_data_index_once;
                        if drop_index {
                            faults.drop_data_index_once = None;
                        }
                        if blocked.load(Relaxed)
                            || directed.is_some()
                            || drop_index
                            || rng.next_u32() % 1_000_000 < faults.loss_ppm
                        {
                            counters.dropped.fetch_add(1, Relaxed);
                            counters.injected_drops[direction as usize].fetch_add(1, Relaxed);
                            continue;
                        }
                        if faults.corrupt_data_once && kind <= 1 && flags == 1 && len > 17 {
                            buffer[len - 1] ^= 1;
                            faults.corrupt_data_once = false;
                        }
                        let copies = if faults.duplicate_every != 0
                            && serial % faults.duplicate_every == 0
                        {
                            2
                        } else {
                            1
                        };
                        for copy in 0..copies {
                            let jitter = if faults.reorder_every != 0
                                && serial % faults.reorder_every == 0
                            {
                                REORDER_JITTER
                            } else {
                                Duration::ZERO
                            };
                            let delay = faults.delay + jitter;
                            if delay.is_zero() {
                                forward(
                                    if direction == 0 { &right } else { &left },
                                    if direction == 0 { receiver } else { sender },
                                    &buffer[..len],
                                    &counters,
                                )
                                .await;
                            } else if queue.len() < MAX_QUEUED_DATAGRAMS
                                && queued_bytes + len <= MAX_QUEUED_BYTES
                            {
                                queue.push(Queued {
                                    at: Instant::now() + delay,
                                    direction,
                                    bytes: buffer[..len].to_vec(),
                                });
                                queued_bytes += len;
                                counters
                                    .peak_queue_bytes
                                    .fetch_max(queued_bytes as u64, Relaxed);
                            } else {
                                counters.queue_drops.fetch_add(1, Relaxed);
                            }
                            if copy == 1 {
                                counters.duplicated.fetch_add(1, Relaxed);
                            }
                        }
                    }
                }
                let now = Instant::now();
                let mut index = 0;
                while index < queue.len() {
                    if queue[index].at <= now {
                        let packet = queue.swap_remove(index);
                        queued_bytes -= packet.bytes.len();
                        forward(
                            if packet.direction == 0 { &right } else { &left },
                            if packet.direction == 0 {
                                receiver
                            } else {
                                sender
                            },
                            &packet.bytes,
                            &counters,
                        )
                        .await;
                    } else {
                        index += 1;
                    }
                }
                tokio::task::yield_now().await;
            }
        });
        Ok(Self {
            address,
            stats,
            blackhole,
            stop: Some(stop),
            worker,
        })
    }
}

async fn forward(socket: &DatagramSocket, peer: SocketAddr, bytes: &[u8], stats: &RelayStats) {
    if socket.send_to(bytes, peer).await.is_ok() {
        stats.forwarded.fetch_add(1, Relaxed);
        stats.wire_bytes.fetch_add(bytes.len() as u64, Relaxed);
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.worker.abort();
    }
}
