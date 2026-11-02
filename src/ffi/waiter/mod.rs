//! Native waiter: epoch/arm protocol plus a per-waiter OS wait source.
//!
//! The arm protocol is platform independent: clear the OS signal, publish ARMED,
//! then recheck the queue epoch under SeqCst. Publication increments the epoch and
//! signals only waiters currently ARMED, so both publish-before-arm and
//! publish-after-arm are observed exactly once.

mod os;

use super::{
    abi::{
        RM_ARMED, RM_INTERNAL, RM_INVALID_STATE, RM_IO, RM_OK, RM_READY, RM_TIMED_OUT,
        RM_UNSUPPORTED, RmStatus, RmWaitSource,
    },
    cq::Cq,
};
use crate::buffer::{Budget, Reservation};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{
        AtomicU32,
        Ordering::{Acquire, Release, SeqCst},
    },
};

const IDLE: u32 = 0;
const ARMED: u32 = 1;
const SIGNALED: u32 = 2;
const CANCELLED: u32 = 3;

/// Metadata charged per waiter (DOCS 13.1).
const WAITER_CHARGE_BYTES: usize = 512;

enum Bind {
    None,
    Iocp { port: usize, key: usize },
    Kqueue { kq: i32, ident: usize },
}

pub(crate) struct Waiter {
    cq: Arc<Cq>,
    state: AtomicU32,
    bind: Mutex<Bind>,
    source: OnceLock<os::Source>,
    _charge: Reservation,
}

impl Waiter {
    pub fn new(cq: Arc<Cq>, budget: Arc<Budget>) -> Option<Arc<Self>> {
        let charge = budget.reserve(WAITER_CHARGE_BYTES).ok()?;
        let waiter = Arc::new(Self {
            cq,
            state: AtomicU32::new(IDLE),
            bind: Mutex::new(Bind::None),
            source: OnceLock::new(),
            _charge: charge,
        });
        waiter.cq.register(&waiter);
        Some(waiter)
    }

    fn ensure_source(&self) -> Result<&os::Source, RmStatus> {
        if let Some(source) = self.source.get() {
            return Ok(source);
        }
        let source = os::Source::create()?;
        let _ = self.source.set(source);
        Ok(self.source.get().expect("source initialized above"))
    }

    pub fn arm(&self, observed_epoch: u64) -> RmStatus {
        loop {
            match self.state.load(SeqCst) {
                ARMED => return RM_ARMED,
                SIGNALED => return RM_READY,
                CANCELLED => return RM_INVALID_STATE,
                IDLE => {
                    if let Err(status) = self.ensure_source() {
                        return status;
                    }
                    if let Some(source) = self.source.get() {
                        source.clear();
                    }
                    if self
                        .state
                        .compare_exchange(IDLE, ARMED, SeqCst, SeqCst)
                        .is_err()
                    {
                        continue;
                    }
                    self.cq.armed.fetch_add(1, SeqCst);
                    if self.cq.epoch() != observed_epoch
                        || self.cq.pending_count() != 0
                        || self.cq.is_closed()
                    {
                        if self
                            .state
                            .compare_exchange(ARMED, SIGNALED, SeqCst, SeqCst)
                            .is_ok()
                        {
                            self.cq.armed.fetch_sub(1, SeqCst);
                        }
                        return RM_READY;
                    }
                    return RM_ARMED;
                }
                _ => return RM_INTERNAL,
            }
        }
    }

    /// Called by the queue for waiters that are still ARMED.
    pub fn try_signal(&self) -> bool {
        if self
            .state
            .compare_exchange(ARMED, SIGNALED, SeqCst, SeqCst)
            .is_err()
        {
            return false;
        }
        self.cq.armed.fetch_sub(1, SeqCst);
        if let Some(source) = self.source.get() {
            let _ = source.signal();
        }
        let _ = self.notify_bound();
        true
    }

    pub fn wait(&self, timeout_ns: u64) -> RmStatus {
        loop {
            match self.state.load(Acquire) {
                SIGNALED => {
                    self.state.store(IDLE, Release);
                    return RM_OK;
                }
                CANCELLED => return RM_OK,
                ARMED => {}
                IDLE => return RM_INVALID_STATE,
                _ => return RM_INTERNAL,
            }
            let source = match self.ensure_source() {
                Ok(source) => source,
                Err(status) => return status,
            };
            match source.wait(timeout_ns) {
                os::Wait::Signaled => {
                    source.clear();
                    if self.state.load(Acquire) == ARMED {
                        // Spurious OS wakeup: only a successful transition owns the
                        // armed-count decrement.
                        if self
                            .state
                            .compare_exchange(ARMED, SIGNALED, SeqCst, SeqCst)
                            .is_ok()
                        {
                            self.cq.armed.fetch_sub(1, SeqCst);
                        }
                    }
                }
                os::Wait::TimedOut => {
                    if self.state.load(Acquire) == ARMED {
                        return RM_TIMED_OUT;
                    }
                }
                os::Wait::Failed => return RM_IO,
            }
        }
    }

    pub fn cancel(&self) -> RmStatus {
        loop {
            let current = self.state.load(SeqCst);
            if current == CANCELLED {
                return RM_OK;
            }
            if self
                .state
                .compare_exchange(current, CANCELLED, SeqCst, SeqCst)
                .is_ok()
            {
                if current == ARMED {
                    self.cq.armed.fetch_sub(1, SeqCst);
                }
                if let Some(source) = self.source.get() {
                    let _ = source.signal();
                }
                let _ = self.notify_bound();
                return RM_OK;
            }
        }
    }

    pub fn reset(&self) -> RmStatus {
        loop {
            let current = self.state.load(SeqCst);
            match current {
                IDLE => return RM_OK,
                SIGNALED | CANCELLED => {
                    if self
                        .state
                        .compare_exchange(current, IDLE, SeqCst, SeqCst)
                        .is_ok()
                    {
                        if let Some(source) = self.source.get() {
                            source.clear();
                        }
                        return RM_OK;
                    }
                }
                ARMED => return RM_INVALID_STATE,
                _ => return RM_INTERNAL,
            }
        }
    }

    pub fn export(&self, out: &mut RmWaitSource) -> RmStatus {
        let source = match self.ensure_source() {
            Ok(source) => source,
            Err(status) => return status,
        };
        out.kind = os::KIND;
        out.flags = 0;
        out.native_value = source.raw();
        out.owner = 0;
        RM_OK
    }

    pub fn bind_iocp(&self, port: usize, key: usize) -> RmStatus {
        if !os::HAS_IOCP {
            return RM_UNSUPPORTED;
        }
        let mut bind = self
            .bind
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match *bind {
            Bind::Iocp {
                port: bound_port,
                key: bound_key,
            } if bound_port == port && bound_key == key => RM_OK,
            Bind::None => {
                *bind = Bind::Iocp { port, key };
                RM_OK
            }
            _ => RM_INVALID_STATE,
        }
    }

    pub fn bind_kqueue(&self, kq: i32, ident: usize) -> RmStatus {
        if !os::HAS_KQUEUE {
            return RM_UNSUPPORTED;
        }
        let mut bind = self
            .bind
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match *bind {
            Bind::Kqueue {
                kq: bound_kq,
                ident: bound_ident,
            } if bound_kq == kq && bound_ident == ident => RM_OK,
            Bind::None => match os::register_kqueue(kq, ident) {
                Ok(()) => {
                    *bind = Bind::Kqueue { kq, ident };
                    RM_OK
                }
                Err(status) => status,
            },
            _ => RM_INVALID_STATE,
        }
    }

    pub fn unbind(&self) -> RmStatus {
        let mut bind = self
            .bind
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Bind::Kqueue { kq, ident } = *bind {
            let _ = os::delete_kqueue(kq, ident);
        }
        *bind = Bind::None;
        RM_OK
    }

    fn notify_bound(&self) -> Result<(), RmStatus> {
        let bind = self
            .bind
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match *bind {
            Bind::None => Ok(()),
            Bind::Iocp { port, key } => os::post_iocp(port, key),
            Bind::Kqueue { kq, ident } => os::trigger_kqueue(kq, ident),
        }
    }
}
