use crate::{Error, Result};
mod allocator;
pub(crate) mod table;
pub(crate) use allocator::BudgetAllocator;
use std::{
    fmt,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    sync::{
        Arc, Mutex,
        atomic::{
            AtomicUsize,
            Ordering::{AcqRel, Acquire, Relaxed},
        },
    },
    time::{Duration, Instant},
};

pub(crate) struct Budget {
    limit: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    parent: Option<Arc<Budget>>,
}
impl Budget {
    pub fn new(limit: usize, parent: Option<Arc<Budget>>) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            parent,
        })
    }
    fn acquire(&self, bytes: usize) -> Result<()> {
        if let Some(p) = &self.parent {
            p.acquire(bytes)?;
        }
        match self.used.try_update(AcqRel, Acquire, |n| {
            n.checked_add(bytes).filter(|&v| v <= self.limit)
        }) {
            Ok(old) => {
                let used = old + bytes;
                if used > self.peak.load(Relaxed) {
                    self.peak.fetch_max(used, Relaxed);
                }
                Ok(())
            }
            Err(_) => {
                if let Some(p) = &self.parent {
                    p.release(bytes);
                }
                Err(Error::NoBuffer)
            }
        }
    }
    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, AcqRel);
        if let Some(p) = &self.parent {
            p.release(bytes);
        }
    }
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation> {
        self.acquire(bytes)?;
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
    pub fn available(&self) -> usize {
        self.limit.saturating_sub(self.used())
    }
    pub fn used(&self) -> usize {
        self.used.load(Relaxed)
    }
    pub fn peak(&self) -> usize {
        self.peak.load(Relaxed)
    }
}
pub(crate) struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

struct Cached {
    data: Vec<u8>,
    _charge: Reservation,
    idle_since: Instant,
}
pub(crate) struct Pool {
    pub budget: Arc<Budget>,
    idle_limit: usize,
    idle: Mutex<Vec<Cached>>,
    cached: AtomicUsize,
}
impl Pool {
    pub fn new(budget: Arc<Budget>, idle_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            budget,
            idle_limit,
            idle: Mutex::new(Vec::new()),
            cached: AtomicUsize::new(0),
        })
    }
    pub fn take(self: &Arc<Self>, len: usize) -> Result<Lease> {
        let capacity = len
            .max(1)
            .checked_next_power_of_two()
            .ok_or(Error::NoBuffer)?;
        let item = {
            let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
            idle.iter()
                .position(|x| x.data.capacity() == capacity)
                .map(|i| {
                    let item = idle.swap_remove(i);
                    self.cached.fetch_sub(item.data.capacity(), Relaxed);
                    item
                })
        };
        let mut item = match item {
            Some(item) => item,
            None => {
                let bytes = capacity.checked_add(64).ok_or(Error::NoBuffer)?;
                let charge = match self.budget.reserve(bytes) {
                    Ok(charge) => charge,
                    Err(_) => {
                        self.trim();
                        self.budget.reserve(bytes)?
                    }
                };
                let mut data = Vec::new();
                data.try_reserve_exact(capacity)
                    .map_err(|_| Error::NoBuffer)?;
                Cached {
                    data,
                    _charge: charge,
                    idle_since: Instant::now(),
                }
            }
        };
        item.data.resize(len, 0);
        Ok(Lease {
            pool: self.clone(),
            item: Some(item),
        })
    }
    pub fn cached(&self) -> usize {
        self.cached.load(Relaxed)
    }
    pub fn trim(&self) {
        let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        idle.clear();
        self.cached.store(0, Relaxed);
    }
    pub fn trim_idle(&self, now: Instant, age: Duration) {
        let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        let mut released = 0;
        idle.retain(|item| {
            let expired = now.saturating_duration_since(item.idle_since) >= age;
            if expired {
                released += item.data.capacity();
            }
            !expired
        });
        self.cached.fetch_sub(released, Relaxed);
    }
}
pub(crate) struct Lease {
    pool: Arc<Pool>,
    item: Option<Cached>,
}
impl Deref for Lease {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.item.as_ref().unwrap().data
    }
}
impl DerefMut for Lease {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.item.as_mut().unwrap().data
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut item = self.item.take().unwrap();
        let mut idle = self.pool.idle.lock().unwrap_or_else(|e| e.into_inner());
        if self
            .pool
            .cached
            .load(Relaxed)
            .saturating_add(item.data.capacity())
            <= self.pool.idle_limit
            && idle.len() < 256
        {
            item.idle_since = Instant::now();
            self.pool.cached.fetch_add(item.data.capacity(), Relaxed);
            idle.push(item);
        }
        // Otherwise both the allocation and its hierarchical charge are released.
    }
}

pub(crate) struct Permit(Arc<AtomicUsize>);
impl Permit {
    pub fn take(counter: &Arc<AtomicUsize>, limit: usize) -> Result<Self> {
        counter
            .try_update(AcqRel, Acquire, |n| (n < limit).then_some(n + 1))
            .map_err(|_| Error::NoBuffer)?;
        Ok(Self(counter.clone()))
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, AcqRel);
    }
}

pub(crate) struct BytePermit {
    counter: Arc<AtomicUsize>,
    bytes: usize,
}
impl BytePermit {
    pub fn take(counter: &Arc<AtomicUsize>, bytes: usize, limit: usize) -> Result<Self> {
        counter
            .try_update(AcqRel, Acquire, |n| {
                n.checked_add(bytes).filter(|&n| n <= limit)
            })
            .map_err(|_| Error::NoBuffer)?;
        Ok(Self {
            counter: counter.clone(),
            bytes,
        })
    }
}
impl Drop for BytePermit {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, AcqRel);
    }
}

#[derive(Clone)]
pub struct Frame(Arc<FrameData>);
struct FrameData {
    peer: SocketAddr,
    id: u32,
    channel: u16,
    layer: u8,
    sequence: u32,
    incarnation: u64,
    data: Lease,
    _charge: Reservation,
    _permit: Permit,
    _channel_permit: Option<Permit>,
}
impl Frame {
    pub(crate) fn new(
        peer: SocketAddr,
        id: u32,
        channel: u16,
        data: Lease,
        charge: Reservation,
        permit: Permit,
    ) -> Self {
        Self(Arc::new(FrameData {
            peer,
            id,
            channel,
            layer: 1,
            sequence: 0,
            incarnation: 0,
            data,
            _charge: charge,
            _permit: permit,
            _channel_permit: None,
        }))
    }
    pub(crate) fn with_layer(mut self, layer: u8) -> Self {
        Arc::get_mut(&mut self.0).unwrap().layer = layer;
        self
    }
    pub(crate) fn with_incarnation(mut self, value: u64) -> Self {
        Arc::get_mut(&mut self.0).unwrap().incarnation = value;
        self
    }
    pub fn sender_incarnation(&self) -> u64 {
        self.0.incarnation
    }
    pub(crate) fn with_channel_permit(mut self, permit: Permit) -> Self {
        Arc::get_mut(&mut self.0).unwrap()._channel_permit = Some(permit);
        self
    }
    pub(crate) fn with_sequence(mut self, seq: u32) -> Self {
        Arc::get_mut(&mut self.0).unwrap().sequence = seq;
        self
    }
    pub fn channel_sequence(&self) -> Option<u32> {
        (self.layer() == 3).then_some(self.0.sequence)
    }
    pub fn delivery_sequence(&self) -> Option<u64> {
        self.channel_sequence().map(u64::from)
    }
    /// Wire layer: 1 best effort, 2 reliable unordered, 3 reliable ordered.
    pub fn layer(&self) -> u8 {
        self.0.layer
    }
    pub fn peer_addr(&self) -> SocketAddr {
        self.0.peer
    }
    pub fn id(&self) -> u32 {
        self.0.id
    }
    pub fn channel(&self) -> u16 {
        self.0.channel
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.0.data
    }
    pub fn len(&self) -> usize {
        self.0.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl Deref for Frame {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Frame")
            .field("peer", &self.peer_addr())
            .field("id", &self.id())
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod test;
