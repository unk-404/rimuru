//! Shared state owned by one endpoint and its implicit runtime.
//!
//! `Context` is the read-mostly hub passed to every protocol layer: the single UDP
//! socket, resource pools, session registry, delivery queue and channel lifecycle
//! records. It never calls the public facade; the owner (`dispatch`) is the only
//! place that mutates protocol state.

use crate::{
    Frame, Result,
    buffer::{Permit, Pool, Reservation},
    config::EndpointConfig,
    udp::DatagramSocket,
};
use crossbeam_queue::ArrayQueue;
use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{
            AtomicBool, AtomicU32, AtomicUsize,
            Ordering::{Acquire, Relaxed, Release},
        },
    },
};
use tokio::sync::Notify;

/// All channels and counters that share one UDP socket.
pub(crate) struct Context {
    pub config: EndpointConfig,
    pub socket: Arc<DatagramSocket>,
    pub pool: Arc<Pool>,
    pub compute: Option<Arc<rayon::ThreadPool>>,
    pub sessions: Sessions,
    frames: ArrayQueue<Frame>,
    ready: Notify,
    shutdown: Notify,
    pub closed: AtomicBool,
    delivery_gate: Mutex<()>,
    stopped: AtomicBool,
    stopped_notify: Notify,
    pub frames_in_use: Arc<AtomicUsize>,
    pub orphan_bytes: Arc<AtomicUsize>,
    pub outgoing_reliable: Arc<AtomicUsize>,
    pub channels_in_use: Arc<AtomicUsize>,
    pub ordered_deferred: Arc<AtomicUsize>,
    channel_events: OnceLock<Arc<Events>>,
    channel_events_gate: Mutex<()>,
    /// Charged once for per-endpoint metadata; dropped with the last owner.
    pub _base_charge: Reservation,
}

pub(crate) type Sessions =
    dashmap::DashMap<SocketAddr, Arc<SessionState>, foldhash::fast::RandomState>;

/// DashMap shards stay small: one endpoint has at most `max_peers` sessions, and
/// each shard is a lock. DOCS 7.2 forbids defaulting every small table to max shards.
const SESSION_SHARDS: usize = 4;

impl Context {
    pub fn new(
        config: EndpointConfig,
        socket: Arc<DatagramSocket>,
        pool: Arc<Pool>,
        compute: Option<Arc<rayon::ThreadPool>>,
        base_charge: Reservation,
    ) -> Arc<Self> {
        let frames = ArrayQueue::new(config.frame_capacity);
        Arc::new(Self {
            sessions: Sessions::with_hasher_and_shard_amount(
                foldhash::fast::RandomState::default(),
                SESSION_SHARDS,
            ),
            frames,
            ready: Notify::new(),
            shutdown: Notify::new(),
            closed: AtomicBool::new(false),
            delivery_gate: Mutex::new(()),
            stopped: AtomicBool::new(false),
            stopped_notify: Notify::new(),
            frames_in_use: Arc::new(AtomicUsize::new(0)),
            orphan_bytes: Arc::new(AtomicUsize::new(0)),
            outgoing_reliable: Arc::new(AtomicUsize::new(0)),
            channels_in_use: Arc::new(AtomicUsize::new(0)),
            ordered_deferred: Arc::new(AtomicUsize::new(0)),
            channel_events: OnceLock::new(),
            channel_events_gate: Mutex::new(()),
            _base_charge: base_charge,
            config,
            socket,
            pool,
            compute,
        })
    }

    pub fn pop_channel_event(&self) -> Option<ChannelEvent> {
        self.channel_events
            .get()
            .and_then(|events| events.queue.pop())
            .map(|queued| queued.event)
    }
    pub fn channel_events(&self) -> Result<&Arc<Events>> {
        if let Some(events) = self.channel_events.get() {
            return Ok(events);
        }
        let _gate = self
            .channel_events_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.channel_events.get().is_none() {
            let events = Events::new(self)?;
            let _ = self.channel_events.set(events);
        }
        Ok(self.channel_events.get().expect("events initialized above"))
    }

    pub fn deliver(&self, frame: Frame) -> bool {
        let _gate = self
            .delivery_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.closed.load(Acquire) {
            return false;
        }
        let len = frame.len();
        if self.frames.push(frame).is_ok() {
            self.socket.metrics.received_frames.fetch_add(1, Relaxed);
            self.socket
                .metrics
                .received_application_bytes
                .fetch_add(len as u64, Relaxed);
            self.ready.notify_one();
            true
        } else {
            self.socket.metrics.resource_drops.fetch_add(1, Relaxed);
            false
        }
    }

    pub fn signal_close(&self) {
        let _gate = self
            .delivery_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.closed.store(true, Release);
        self.ready.notify_waiters();
        if let Some(events) = self.channel_events.get() {
            events.ready.notify_waiters();
        }
        self.shutdown.notify_one();
    }

    pub fn recv_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.ready.notified()
    }

    pub fn shutdown_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.shutdown.notified()
    }

    pub fn pop_frame(&self) -> Option<Frame> {
        self.frames.pop()
    }

    pub fn stopped(&self) -> bool {
        self.stopped.load(Acquire)
    }

    pub fn mark_stopped(&self) {
        self.stopped.store(true, Release);
        self.stopped_notify.notify_waiters();
    }

    pub fn stopped_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.stopped_notify.notified()
    }
}

/// One `Session` identity and its per-direction counters. Pure data: mutation of
/// protocol state happens on the owner, not through this type.
pub(crate) struct SessionState {
    pub peer: SocketAddr,
    pub local: u64,
    pub remote: u64,
    pub d: usize,
    pub max_frame: usize,
    pub integrity: crate::Integrity,
    pub next_id: AtomicU32,
    pub tx_batch: AtomicUsize,
    pub closed: AtomicBool,
    pub client_nonce: [u8; 16],
    pub server_nonce: [u8; 16],
    pub initiator: bool,
    pub reliable: bool,
    pub ordered: bool,
    pub default_channel: OnceLock<Arc<ChannelState>>,
    pub _charge: Reservation,
}

/// Terminal outcome of one ordered channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChannelEnd {
    pub reason: crate::AbortReason,
    /// Last sequence published to the peer's receive queue, not consumed by its
    /// application. On a receiving `ChannelEvent` this is the local receive queue.
    pub last_delivered: Option<u32>,
    /// False when the remote terminal state could not be confirmed.
    pub peer_confirmed: bool,
}

/// Observable ordered-channel termination event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChannelEvent {
    pub peer: SocketAddr,
    pub sender_incarnation: u64,
    pub channel: u16,
    pub end: ChannelEnd,
}

pub(crate) struct QueuedEvent {
    pub event: ChannelEvent,
    pub _permit: Permit,
}

/// Bounded channel-event queue; one slot is reserved per receiving lane.
pub(crate) struct Events {
    pub queue: ArrayQueue<QueuedEvent>,
    pub ready: Notify,
    pub reserved: Arc<AtomicUsize>,
    _charge: Reservation,
}

impl Events {
    pub fn new(shared: &Context) -> Result<Arc<Self>> {
        let channels = shared.config.max_channels;
        let charge = shared
            .pool
            .budget
            .reserve(1024 + channels * (size_of::<QueuedEvent>() + 16))?;
        Ok(Arc::new(Self {
            queue: ArrayQueue::new(channels),
            ready: Notify::new(),
            reserved: Arc::new(AtomicUsize::new(0)),
            _charge: charge,
        }))
    }

    pub fn publish(&self, event: ChannelEvent, permit: Permit) {
        // INVARIANT: every receiving lane reserves one slot before its first grant,
        // so the bounded queue cannot overflow here.
        assert!(
            self.queue
                .push(QueuedEvent {
                    event,
                    _permit: permit,
                })
                .is_ok()
        );
        self.ready.notify_one();
    }
}

/// Cloneable sending-direction state of an ordered channel.
pub(crate) struct ChannelState {
    pub id: u16,
    pub closing: AtomicBool,
    pub inflight: Arc<AtomicUsize>,
    end: Mutex<Option<ChannelEnd>>,
    ready: Notify,
    _charge: Reservation,
    _quota: Permit,
}

impl ChannelState {
    pub fn new(shared: &Context, id: u16) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            id,
            closing: AtomicBool::new(false),
            inflight: Arc::new(AtomicUsize::new(0)),
            end: Mutex::new(None),
            ready: Notify::new(),
            _charge: shared.pool.budget.reserve(768)?,
            _quota: Permit::take(&shared.channels_in_use, shared.config.max_channels)?,
        }))
    }

    pub fn finish(&self, end: ChannelEnd) {
        self.closing.store(true, Release);
        let mut slot = self
            .end
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.is_none() {
            *slot = Some(end);
            self.ready.notify_waiters();
        }
    }

    pub fn end(&self) -> Option<ChannelEnd> {
        *self
            .end
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub async fn wait(&self) -> ChannelEnd {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(end) = self.end() {
                return end;
            }
            notified.await;
        }
    }
}
