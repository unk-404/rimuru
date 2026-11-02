//! Version/config init and generic handle-token operations.

use crate::{
    EndpointConfig,
    ffi::{
        abi::{
            RM_ABI_VERSION, RM_INVALID_ARGUMENT, RM_MAX_BATCH_ITEMS, RM_OK, RM_VERSION_MISMATCH,
            RmEndpointConfig, RmHandle, RmStatus,
        },
        api::{default_cq_capacity, zero_out},
        ffi_guard, handle,
    },
};
use std::{ptr, slice};

#[unsafe(no_mangle)]
pub extern "C" fn rm_abi_version() -> u32 {
    RM_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_endpoint_config_init(
    version: u32,
    size: usize,
    out: *mut RmEndpointConfig,
) -> RmStatus {
    ffi_guard!({
        if out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        if version != RM_ABI_VERSION {
            return RM_VERSION_MISMATCH;
        }
        if size < size_of::<RmEndpointConfig>() {
            return RM_INVALID_ARGUMENT;
        }
        let config = EndpointConfig::default();
        let raw = RmEndpointConfig {
            abi_version: RM_ABI_VERSION,
            struct_size: size_of::<RmEndpointConfig>() as u32,
            memory_budget_bytes: config.memory_budget as u64,
            max_message_bytes: config.max_frame_size as u32,
            max_peers: config.max_peers as u32,
            max_channels: config.max_channels as u32,
            cq_capacity: default_cq_capacity(config.memory_budget) as u32,
            backend: crate::ffi::abi::RM_BACKEND_AUTO,
            flags: 0,
        };
        // SAFETY: validated non-null output.
        unsafe {
            ptr::write_unaligned(out, raw);
        }
        RM_OK
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_handle_clone(handle: RmHandle, out: *mut RmHandle) -> RmStatus {
    ffi_guard!({
        if out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(out);
        match crate::ffi::handle::clone_token(handle) {
            Ok(cloned) => {
                // SAFETY: validated non-null output.
                unsafe {
                    ptr::write_unaligned(out, cloned);
                }
                RM_OK
            }
            Err(status) => status,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_handle_release(handle: RmHandle) -> RmStatus {
    ffi_guard!({ registry_release(handle) })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_handle_release_batch(
    handles: *const RmHandle,
    count: u32,
    item_status: *mut RmStatus,
) -> RmStatus {
    ffi_guard!({
        if count as usize > RM_MAX_BATCH_ITEMS {
            return RM_INVALID_ARGUMENT;
        }
        if count == 0 {
            return RM_OK;
        }
        if handles.is_null() || item_status.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        // SAFETY: caller provides `count` readable handles and writable statuses.
        let inputs = unsafe { slice::from_raw_parts(handles, count as usize) };
        for (index, handle) in inputs.iter().enumerate() {
            let result = registry_release(*handle);
            // SAFETY: same caller-provided array, index in range.
            unsafe {
                ptr::write_unaligned(item_status.add(index), result);
            }
        }
        RM_OK
    })
}

fn registry_release(handle: RmHandle) -> RmStatus {
    handle::release(handle)
}
