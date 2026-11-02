//! Completion polling and native waiter exports.

use crate::ffi::{
    abi::{
        RM_CLOSED, RM_INVALID_ARGUMENT, RM_MAX_BATCH_ITEMS, RM_NO_BUFFER, RM_OK, RM_WOULD_BLOCK,
        RmEvent, RmHandle, RmStatus, RmWaitSource, RmWaitSourceKind,
    },
    api::{write_out, zero_out},
    cq::Cq,
    ffi_guard, handle,
    waiter::Waiter,
};
use std::{ptr, sync::Arc};

fn poll_impl(
    cq: &Arc<Cq>,
    out: *mut RmEvent,
    event_size: usize,
    capacity: u32,
    count: *mut u32,
    epoch: *mut u64,
) -> RmStatus {
    if count.is_null() || epoch.is_null() {
        return RM_INVALID_ARGUMENT;
    }
    // SAFETY: validated non-null outputs.
    unsafe {
        ptr::write_unaligned(count, 0);
        ptr::write_unaligned(epoch, cq.epoch());
    }
    if capacity as usize > RM_MAX_BATCH_ITEMS {
        return RM_INVALID_ARGUMENT;
    }
    if capacity == 0 {
        return RM_OK;
    }
    if out.is_null() || event_size < size_of::<RmEvent>() {
        return RM_INVALID_ARGUMENT;
    }
    let mut written = 0u32;
    while written < capacity {
        let Some(event) = cq.pop() else {
            break;
        };
        // SAFETY: caller reserved `capacity` events with the given stride.
        unsafe {
            ptr::write_unaligned(
                out.cast::<u8>()
                    .add(written as usize * event_size)
                    .cast::<RmEvent>(),
                event,
            );
        }
        written += 1;
    }
    // SAFETY: validated non-null outputs.
    unsafe {
        ptr::write_unaligned(count, written);
        ptr::write_unaligned(epoch, cq.epoch());
    }
    if written > 0 {
        RM_OK
    } else if cq.is_closed() {
        RM_CLOSED
    } else {
        RM_WOULD_BLOCK
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_cq_poll_batch(
    cq: RmHandle,
    out: *mut RmEvent,
    event_size: usize,
    capacity: u32,
    count: *mut u32,
    epoch: *mut u64,
) -> RmStatus {
    ffi_guard!({
        let cq = match handle::pin_cq(cq) {
            Ok(cq) => cq,
            Err(status) => return status,
        };
        poll_impl(&cq, out, event_size, capacity, count, epoch)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_endpoint_poll_batch(
    endpoint: RmHandle,
    out: *mut RmEvent,
    event_size: usize,
    capacity: u32,
    count: *mut u32,
    epoch: *mut u64,
) -> RmStatus {
    ffi_guard!({
        let endpoint = match handle::pin_endpoint(endpoint) {
            Ok(endpoint) => endpoint,
            Err(status) => return status,
        };
        let cq = endpoint.default_cq.clone();
        poll_impl(&cq, out, event_size, capacity, count, epoch)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_waiter_create(cq: RmHandle, out: *mut RmHandle) -> RmStatus {
    ffi_guard!({
        if out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(out);
        let cq = match handle::pin_cq(cq) {
            Ok(cq) => cq,
            Err(status) => return status,
        };
        let budget = cq.budget();
        let Some(waiter) = Waiter::new(cq, budget) else {
            return RM_NO_BUFFER;
        };
        match handle::insert(handle::Object::Waiter(waiter)) {
            Ok(value) => write_out(out, value),
            Err(status) => status,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_waiter_arm(waiter: RmHandle, observed_epoch: u64) -> RmStatus {
    ffi_guard!({
        let waiter = match handle::pin_waiter(waiter) {
            Ok(waiter) => waiter,
            Err(status) => return status,
        };
        waiter.arm(observed_epoch)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_waiter_wait(waiter: RmHandle, timeout_ns: u64) -> RmStatus {
    ffi_guard!({
        let waiter = match handle::pin_waiter(waiter) {
            Ok(waiter) => waiter,
            Err(status) => return status,
        };
        waiter.wait(timeout_ns)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_waiter_cancel(waiter: RmHandle) -> RmStatus {
    ffi_guard!({
        let waiter = match handle::pin_waiter(waiter) {
            Ok(waiter) => waiter,
            Err(status) => return status,
        };
        waiter.cancel()
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_waiter_reset(waiter: RmHandle) -> RmStatus {
    ffi_guard!({
        let waiter = match handle::pin_waiter(waiter) {
            Ok(waiter) => waiter,
            Err(status) => return status,
        };
        waiter.reset()
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_waiter_export(
    waiter: RmHandle,
    size: usize,
    out: *mut RmWaitSource,
) -> RmStatus {
    ffi_guard!({
        if size < size_of::<RmWaitSource>() || out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(out);
        let pinned = match handle::pin_waiter(waiter) {
            Ok(waiter) => waiter,
            Err(status) => return status,
        };
        let mut source = RmWaitSource {
            kind: RmWaitSourceKind::RM_WAIT_POSIX_FD,
            flags: 0,
            native_value: 0,
            owner: 0,
        };
        let status = pinned.export(&mut source);
        if status != RM_OK {
            return status;
        }
        let owner = match handle::clone_token(waiter) {
            Ok(owner) => owner,
            Err(status) => return status,
        };
        source.owner = owner;
        // SAFETY: validated non-null output.
        unsafe {
            ptr::write_unaligned(out, source);
        }
        RM_OK
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_waiter_bind_iocp(waiter: RmHandle, port: usize, key: usize) -> RmStatus {
    ffi_guard!({
        let waiter = match handle::pin_waiter(waiter) {
            Ok(waiter) => waiter,
            Err(status) => return status,
        };
        waiter.bind_iocp(port, key)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_waiter_bind_kqueue(waiter: RmHandle, kq: i32, ident: usize) -> RmStatus {
    ffi_guard!({
        let waiter = match handle::pin_waiter(waiter) {
            Ok(waiter) => waiter,
            Err(status) => return status,
        };
        waiter.bind_kqueue(kq, ident)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_waiter_unbind(waiter: RmHandle) -> RmStatus {
    ffi_guard!({
        let waiter = match handle::pin_waiter(waiter) {
            Ok(waiter) => waiter,
            Err(status) => return status,
        };
        waiter.unbind()
    })
}
