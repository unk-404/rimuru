//! Bounded MPMC completion queue with epoch/arm wakeup support (DOCS 10.2–10.3).
//!
//! Polling only moves fixed-size records; it never allocates. Admission reserves a
//! slot before a terminal/message event is promised, so reliable outcomes cannot be
//! dropped because the queue was full.

use super::{
    abi::{RM_OK, RmEvent, RmEventKind, RmStatus},
    waiter::Waiter,
};
use crate::buffer::{Budget, Reservation};
use crossbeam_queue::ArrayQueue;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{
        AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize,
        Ordering::{AcqRel, Acquire, Release, SeqCst},
    },
};
use tokio::sync::Notify;

/// Reserved control slots used for close/fatal synthesis metadata.
pub(crate) const CONTROL_SLOTS: usize = 64;
/// Default cap on waiters signalled per publication (anti-thundering-herd).
const MAX_WAKE_BATCH: usize = 4;
/// Explicit CQ capacity bounds.
pub(crate) const MIN_CAPACITY: usize = 1;
pub(crate) const MAX_CAPACITY: usize = 65_536;

/// A published event reservation. Dropping without publishing returns the slot.
pub(crate) struct EventPermit {
    cq: Arc<Cq>,
    active: bool,
}

impl EventPermit {
    fn consume(&mut self) {
        self.active = false;
    }
}

impl Drop for EventPermit {
    fn drop(&mut self) {
        if self.active {
            self.cq.reserved.fetch_sub(1, AcqRel);
            self.cq.pending.fetch_sub(1, AcqRel);
            self.cq.space.notify_waiters();
        }
    }
}

/// One completion queue. Data events and terminal events share one slot budget;
/// close/fatal synthesis uses a fixed control ring plus persistent flags.
pub(crate) struct Cq {
    queue: ArrayQueue<RmEvent>,
    control: ArrayQueue<RmEvent>,
    capacity: usize,
    reserved: AtomicUsize,
    pending: AtomicUsize,
    epoch: AtomicU64,
    closed: AtomicBool,
    closer_kind: AtomicU32,
    closer_status: AtomicI32,
    closed_delivered: AtomicBool,
    pub(crate) armed: AtomicUsize,
    waiters: Mutex<Vec<Weak<Waiter>>>,
    space: Notify,
    budget: Arc<Budget>,
    _charge: Reservation,
}

impl Cq {
    pub fn new(budget: Arc<Budget>, capacity: usize) -> Option<Arc<Self>> {
        let capacity = capacity.clamp(MIN_CAPACITY, MAX_CAPACITY);
        let charge = budget
            .reserve(1024 + capacity * size_of::<RmEvent>())
            .ok()?;
        Some(Arc::new(Self {
            queue: ArrayQueue::new(capacity),
            control: ArrayQueue::new(CONTROL_SLOTS),
            capacity,
            reserved: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
            epoch: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            closer_kind: AtomicU32::new(0),
            closer_status: AtomicI32::new(RM_OK as i32),
            closed_delivered: AtomicBool::new(false),
            armed: AtomicUsize::new(0),
            waiters: Mutex::new(Vec::new()),
            space: Notify::new(),
            budget,
            _charge: charge,
        }))
    }

    pub fn budget(&self) -> Arc<Budget> {
        self.budget.clone()
    }

    pub fn register(self: &Arc<Self>, waiter: &Arc<Waiter>) {
        let mut registry = self
            .waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry.retain(|entry| entry.strong_count() > 0);
        registry.push(Arc::downgrade(waiter));
    }

    pub fn try_reserve(self: &Arc<Self>) -> Option<EventPermit> {
        if self.closed.load(Acquire) {
            return None;
        }
        let mut reserved = self.reserved.load(Acquire);
        loop {
            if reserved >= self.capacity {
                return None;
            }
            match self
                .reserved
                .compare_exchange_weak(reserved, reserved + 1, AcqRel, Acquire)
            {
                Ok(_) => {
                    self.pending.fetch_add(1, SeqCst);
                    return Some(EventPermit {
                        cq: self.clone(),
                        active: true,
                    });
                }
                Err(current) => reserved = current,
            }
        }
    }

    pub async fn reserve_async(self: &Arc<Self>) -> Option<EventPermit> {
        loop {
            if let Some(permit) = self.try_reserve() {
                return Some(permit);
            }
            if self.closed.load(Acquire) {
                return None;
            }
            let notified = self.space.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(permit) = self.try_reserve() {
                return Some(permit);
            }
            if self.closed.load(Acquire) {
                return None;
            }
            notified.await;
        }
    }

    /// Publish a reserved event. The reservation is only released when polled.
    pub fn publish(&self, event: RmEvent, mut permit: EventPermit) {
        permit.consume();
        if self.queue.push(event).is_err() {
            // Defensive: reservation accounting makes this unreachable.
            self.reserved.fetch_sub(1, AcqRel);
            self.pending.fetch_sub(1, AcqRel);
            self.space.notify_waiters();
            return;
        }
        self.epoch.fetch_add(1, SeqCst);
        self.wake_waiters(false);
    }

    /// Push a control/fatal event outside the data reservation budget.
    pub fn publish_control(&self, event: RmEvent) {
        if self.control.push(event).is_err() {
            // A full control ring still leaves the persistent close flag visible.
            self.wake_waiters(true);
        }
        self.epoch.fetch_add(1, SeqCst);
        self.wake_waiters(false);
    }

    pub fn pop(&self) -> Option<RmEvent> {
        if let Some(event) = self.queue.pop() {
            self.reserved.fetch_sub(1, AcqRel);
            self.pending.fetch_sub(1, AcqRel);
            self.space.notify_waiters();
            return Some(event);
        }
        if let Some(event) = self.control.pop() {
            return Some(event);
        }
        if self.closed.load(Acquire) && !self.closed_delivered.swap(true, AcqRel) {
            let kind = self.closer_kind.load(Acquire);
            if kind != 0 {
                return Some(RmEvent::empty(
                    RmEventKind::from_raw(kind),
                    RmStatus::from_raw(self.closer_status.load(Acquire)),
                ));
            }
        }
        None
    }

    /// Terminal close of this queue. The event is synthesized once on poll.
    pub fn close(&self, kind: RmEventKind, status: RmStatus) {
        if self.closed.swap(true, AcqRel) {
            return;
        }
        self.closer_kind.store(kind as u32, Release);
        self.closer_status.store(status as i32, Release);
        self.epoch.fetch_add(1, SeqCst);
        self.space.notify_waiters();
        self.wake_waiters(true);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Acquire)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Acquire)
    }

    pub fn pending_count(&self) -> usize {
        self.pending.load(Acquire)
    }

    pub fn wake_waiters(&self, all: bool) {
        let armed = self.armed.load(SeqCst);
        if armed == 0 {
            return;
        }
        let cap = if all {
            armed
        } else {
            armed.min(MAX_WAKE_BATCH)
        };
        let mut signalled = 0;
        let registry = self
            .waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for entry in registry.iter() {
            if signalled >= cap {
                break;
            }
            let Some(waiter) = entry.upgrade() else {
                continue;
            };
            if waiter.try_signal() {
                signalled += 1;
            }
        }
    }
}

#[cfg(test)]
impl Cq {
    /// Reservation accounting must return to zero once every event is polled.
    pub(crate) fn reserved_for_test(&self) -> usize {
        self.reserved.load(std::sync::atomic::Ordering::Relaxed)
    }
}

const _: () = assert!(CONTROL_SLOTS >= MAX_WAKE_BATCH);
