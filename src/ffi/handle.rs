//! Process-wide sparse handle registry: generation and 1-based slot packed into a
//! pointer-width token, lock-free pin by `ArcSwapOption`, mutex only on allocation
//! and release (DOCS 10.1).
//!
//! The token is `uintptr_t`/`size_t` sized in C, not a fixed `uint64_t`:
//!
//! * 64-bit targets: 32 generation bits + 32 slot bits.
//! * 32-bit targets: 16 generation bits + 16 slot bits (65,535 slots).
//!
//! Generations are per slot, so a 32-bit build still reuses a slot thousands of
//! times before retiring it; the retired slot is never handed out again.

use super::{
    abi::{RM_INVALID_HANDLE, RM_NO_BUFFER, RmHandle, RmStatus},
    cq::Cq,
    endpoint::EndpointObj,
    objects::{BuilderObj, ChannelObj, MessageObj, SessionObj, TicketObj, ViewObj},
    waiter::Waiter,
};
use arc_swap::ArcSwapOption;
use std::{
    ptr,
    sync::{
        Arc, Mutex,
        atomic::{
            AtomicPtr, AtomicU32,
            Ordering::{Acquire, Relaxed, Release},
        },
    },
};

const SLOT_BITS: u32 = if usize::BITS == 64 { 32 } else { 16 };
const SLOT_MASK: usize = (1usize << SLOT_BITS) - 1;
const GENERATION_MASK: u32 = if usize::BITS == 64 { u32::MAX } else { 0xFFFF };
const SLOTS_PER_PAGE: usize = 1024;
const MAX_PAGES: usize = if usize::BITS == 64 { 4096 } else { 64 };
const MAX_SLOTS: usize = if usize::BITS == 64 {
    MAX_PAGES * SLOTS_PER_PAGE
} else {
    SLOT_MASK
};
const MAX_HANDLES: usize = if usize::BITS == 64 { 262_144 } else { 65_535 };

#[derive(Clone)]
pub(crate) enum Object {
    Endpoint(Arc<EndpointObj>),
    Session(Arc<SessionObj>),
    Channel(Arc<ChannelObj>),
    Ticket(Arc<TicketObj>),
    Cq(Arc<Cq>),
    Waiter(Arc<Waiter>),
    Builder(Arc<Mutex<BuilderObj>>),
    Message(Arc<MessageObj>),
    /// View tokens only keep a frame alive; the payload is intentionally owned.
    #[allow(dead_code)]
    View(Arc<ViewObj>),
}

pub(crate) struct Record {
    generation: u32,
    object: Object,
}

struct Page {
    slots: Box<[ArcSwapOption<Record>]>,
    /// Last issued generation per slot; survives release and drives reuse.
    generations: Box<[AtomicU32]>,
}

impl Page {
    fn new() -> Self {
        let slots = (0..SLOTS_PER_PAGE)
            .map(|_| ArcSwapOption::empty())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let generations = (0..SLOTS_PER_PAGE)
            .map(|_| AtomicU32::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { slots, generations }
    }
}

static PAGES: [AtomicPtr<Page>; MAX_PAGES] = [const { AtomicPtr::new(ptr::null_mut()) }; MAX_PAGES];

struct Allocator {
    free: Vec<u32>,
    next: u32,
    live: usize,
}

impl Allocator {
    const fn new() -> Self {
        Self {
            free: Vec::new(),
            next: 1,
            live: 0,
        }
    }
}

static ALLOCATOR: Mutex<Allocator> = Mutex::new(Allocator::new());

fn page(index: usize) -> *mut Page {
    PAGES[index].load(Acquire)
}

fn ensure_page(index: usize) -> *mut Page {
    let existing = PAGES[index].load(Acquire);
    if !existing.is_null() {
        return existing;
    }
    let allocated = Box::into_raw(Box::new(Page::new()));
    match PAGES[index].compare_exchange(ptr::null_mut(), allocated, Release, Acquire) {
        Ok(_) => allocated,
        Err(current) => {
            // SAFETY: this thread allocated `allocated` and lost the race.
            unsafe {
                drop(Box::from_raw(allocated));
            }
            current
        }
    }
}

fn pack(generation: u32, slot: u32) -> RmHandle {
    ((generation as usize) << SLOT_BITS) | slot as usize
}

fn unpack(handle: RmHandle) -> Option<(u32, u32)> {
    if handle == 0 {
        return None;
    }
    let generation = (handle >> SLOT_BITS) as u32;
    let slot = (handle & SLOT_MASK) as u32;
    (generation != 0 && slot != 0).then_some((generation, slot))
}

/// Insert a new owning token for `object`.
pub(crate) fn insert(object: Object) -> Result<RmHandle, RmStatus> {
    let mut allocator = ALLOCATOR
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if allocator.live >= MAX_HANDLES {
        return Err(RM_NO_BUFFER);
    }
    loop {
        let slot = match allocator.free.pop() {
            Some(slot) => slot,
            None => {
                let slot = allocator.next;
                if slot as usize > MAX_SLOTS {
                    return Err(RM_NO_BUFFER);
                }
                allocator.next = allocator.next.saturating_add(1);
                slot
            }
        };
        let index = (slot - 1) as usize;
        let page_index = index / SLOTS_PER_PAGE;
        let offset = index % SLOTS_PER_PAGE;
        let page = ensure_page(page_index);
        if page.is_null() {
            return Err(RM_NO_BUFFER);
        }
        // SAFETY: page is a live, uniquely-owned allocation published in PAGES.
        let page = unsafe { &*page };
        let previous = page.generations[offset].load(Relaxed);
        if previous == GENERATION_MASK {
            // Retired slot: never hand out the same generation twice.
            continue;
        }
        let generation = previous + 1;
        page.generations[offset].store(generation, Relaxed);
        page.slots[offset].store(Some(Arc::new(Record { generation, object })));
        allocator.live += 1;
        return Ok(pack(generation, slot));
    }
}

/// Pin a token for the duration of one call. A concurrent release may revoke a
/// not-yet-pinned token, but never an already pinned record.
pub(crate) fn pin(handle: RmHandle) -> Result<Arc<Record>, RmStatus> {
    let Some((generation, slot)) = unpack(handle) else {
        return Err(RM_INVALID_HANDLE);
    };
    let index = (slot - 1) as usize;
    if index >= MAX_SLOTS {
        return Err(RM_INVALID_HANDLE);
    }
    let page = page(index / SLOTS_PER_PAGE);
    if page.is_null() {
        return Err(RM_INVALID_HANDLE);
    }
    // SAFETY: live page; slot reads are atomic through ArcSwapOption.
    let page = unsafe { &*page };
    let record = page.slots[index % SLOTS_PER_PAGE]
        .load_full()
        .ok_or(RM_INVALID_HANDLE)?;
    if record.generation != generation {
        return Err(RM_INVALID_HANDLE);
    }
    Ok(record)
}

/// Release exactly one token. The slot returns to the free list only once.
pub(crate) fn release(handle: RmHandle) -> RmStatus {
    let Some((generation, slot)) = unpack(handle) else {
        return RM_INVALID_HANDLE;
    };
    let index = (slot - 1) as usize;
    if index >= MAX_SLOTS {
        return RM_INVALID_HANDLE;
    }
    let page = page(index / SLOTS_PER_PAGE);
    if page.is_null() {
        return RM_INVALID_HANDLE;
    }
    // SAFETY: live page; swap atomically revokes the token exactly once.
    let page = unsafe { &*page };
    let previous = page.slots[index % SLOTS_PER_PAGE].swap(None);
    let Some(record) = previous else {
        return RM_INVALID_HANDLE;
    };
    if record.generation != generation {
        return RM_INVALID_HANDLE;
    }
    let mut allocator = ALLOCATOR
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    allocator.live = allocator.live.saturating_sub(1);
    if record.generation < GENERATION_MASK {
        allocator.free.push(slot);
    }
    super::abi::RM_OK
}

pub(crate) fn clone_token(handle: RmHandle) -> Result<RmHandle, RmStatus> {
    let record = pin(handle)?;
    insert(record.object.clone())
}

macro_rules! pin_variant {
    ($handle:expr, $variant:ident, $ty:ty) => {{
        let record = pin($handle)?;
        match &record.object {
            Object::$variant(value) => Ok(value.clone()),
            _ => Err(RM_INVALID_HANDLE),
        }
    }};
}

pub(crate) fn pin_endpoint(handle: RmHandle) -> Result<Arc<EndpointObj>, RmStatus> {
    pin_variant!(handle, Endpoint, Arc<EndpointObj>)
}

pub(crate) fn pin_session(handle: RmHandle) -> Result<Arc<SessionObj>, RmStatus> {
    pin_variant!(handle, Session, Arc<SessionObj>)
}

pub(crate) fn pin_channel(handle: RmHandle) -> Result<Arc<ChannelObj>, RmStatus> {
    pin_variant!(handle, Channel, Arc<ChannelObj>)
}

pub(crate) fn pin_ticket(handle: RmHandle) -> Result<Arc<TicketObj>, RmStatus> {
    pin_variant!(handle, Ticket, Arc<TicketObj>)
}

pub(crate) fn pin_cq(handle: RmHandle) -> Result<Arc<Cq>, RmStatus> {
    pin_variant!(handle, Cq, Arc<Cq>)
}

pub(crate) fn pin_waiter(handle: RmHandle) -> Result<Arc<Waiter>, RmStatus> {
    pin_variant!(handle, Waiter, Arc<Waiter>)
}

pub(crate) fn pin_builder(handle: RmHandle) -> Result<Arc<Mutex<BuilderObj>>, RmStatus> {
    pin_variant!(handle, Builder, Arc<Mutex<BuilderObj>>)
}

pub(crate) fn pin_message(handle: RmHandle) -> Result<Arc<MessageObj>, RmStatus> {
    pin_variant!(handle, Message, Arc<MessageObj>)
}

#[cfg(test)]
pub(crate) fn live_handles() -> usize {
    ALLOCATOR
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .live
}
