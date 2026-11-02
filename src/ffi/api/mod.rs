//! Shared argument validation and conversions for the exported API.
//!
//! Raw pointers are safe for C callers to pass by contract: the API validates
//! null/length before dereferencing. The lint below is about Rust callers, not C.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

pub mod endpoint;
pub mod handle;
pub mod poll;
pub mod send;

use super::abi::{
    RM_ABI_MAJOR, RM_INVALID_ARGUMENT, RM_UNSUPPORTED, RM_VERSION_MISMATCH, RmAddress,
    RmEndpointConfig, RmStatus,
};
use crate::{EndpointConfig, Error};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
    ptr,
};

/// Map a Rust error to the frozen status table.
pub(super) fn status(error: Error) -> RmStatus {
    super::abi::status_from_error(error)
}

pub(super) fn version_matches(version: u32) -> bool {
    version >> 16 == RM_ABI_MAJOR
}

/// Decode `RmEndpointConfig`; null selects the safe default configuration.
pub(super) fn read_config(
    pointer: *const RmEndpointConfig,
) -> Result<(EndpointConfig, usize), RmStatus> {
    if pointer.is_null() {
        let config = EndpointConfig::default();
        return Ok((config.clone(), default_cq_capacity(config.memory_budget)));
    }
    // SAFETY: caller guarantees a valid readable `RmEndpointConfig`-sized object;
    // the struct is `repr(C)` and `unaligned` reads avoid alignment assumptions.
    let raw = unsafe { ptr::read_unaligned(pointer) };
    if !version_matches(raw.abi_version) {
        return Err(RM_VERSION_MISMATCH);
    }
    if (raw.struct_size as usize) < size_of::<RmEndpointConfig>() {
        return Err(RM_INVALID_ARGUMENT);
    }
    if raw.flags != 0 || (raw.backend as u32) > (super::abi::RM_BACKEND_NATIVE as u32) {
        return Err(RM_INVALID_ARGUMENT);
    }
    if raw.backend == super::abi::RM_BACKEND_NATIVE {
        return Err(RM_UNSUPPORTED);
    }
    let mut config = EndpointConfig::default();
    if raw.memory_budget_bytes != 0 {
        config.memory_budget =
            usize::try_from(raw.memory_budget_bytes).map_err(|_| RM_INVALID_ARGUMENT)?;
    }
    if raw.max_message_bytes != 0 {
        config.max_frame_size = raw.max_message_bytes as usize;
    }
    if raw.max_peers != 0 {
        config.max_peers = raw.max_peers as usize;
    }
    if raw.max_channels != 0 {
        config.max_channels = raw.max_channels as usize;
    }
    config.validate().map_err(status)?;
    let capacity = if raw.cq_capacity != 0 {
        raw.cq_capacity as usize
    } else {
        default_cq_capacity(config.memory_budget)
    };
    Ok((config, capacity))
}

pub(super) fn default_cq_capacity(memory_budget: usize) -> usize {
    (memory_budget / 2048).clamp(256, 4096)
}

pub(super) fn read_address(pointer: *const RmAddress) -> Result<SocketAddr, RmStatus> {
    if pointer.is_null() {
        return Err(RM_INVALID_ARGUMENT);
    }
    // SAFETY: caller-provided readable POD.
    let raw = unsafe { ptr::read_unaligned(pointer) };
    if raw.reserved != 0 {
        return Err(RM_INVALID_ARGUMENT);
    }
    match raw.family {
        4 => {
            if raw.address[4..].iter().any(|byte| *byte != 0) || raw.scope_id != 0 {
                return Err(RM_INVALID_ARGUMENT);
            }
            let octets: [u8; 4] = raw.address[..4]
                .try_into()
                .map_err(|_| RM_INVALID_ARGUMENT)?;
            Ok(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(octets)),
                raw.port,
            ))
        }
        6 => Ok(SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::from(raw.address),
            raw.port,
            0,
            raw.scope_id,
        ))),
        _ => Err(RM_INVALID_ARGUMENT),
    }
}

pub(super) fn write_address(address: SocketAddr, pointer: *mut RmAddress) -> RmStatus {
    if pointer.is_null() {
        return RM_INVALID_ARGUMENT;
    }
    let mut raw = RmAddress {
        family: 0,
        port: address.port(),
        reserved: 0,
        scope_id: 0,
        address: [0; 16],
    };
    match address {
        SocketAddr::V4(address) => {
            raw.family = 4;
            raw.address[..4].copy_from_slice(&address.ip().octets());
        }
        SocketAddr::V6(address) => {
            raw.family = 6;
            raw.address = address.ip().octets();
            raw.scope_id = address.scope_id();
        }
    }
    // SAFETY: caller-provided writable POD output.
    unsafe {
        ptr::write_unaligned(pointer, raw);
    }
    super::abi::RM_OK
}

/// Borrow caller memory for the duration of one call. `len == 0` accepts NULL.
pub(super) fn read_bytes<'a>(data: *const u8, len: u32) -> Result<&'a [u8], RmStatus> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(RM_INVALID_ARGUMENT);
    }
    // SAFETY: caller contract: readable for `len` bytes during this call.
    Ok(unsafe { std::slice::from_raw_parts(data, len as usize) })
}

pub(super) fn write_out<T>(pointer: *mut T, value: T) -> RmStatus {
    if pointer.is_null() {
        return RM_INVALID_ARGUMENT;
    }
    // SAFETY: caller-provided writable output.
    unsafe {
        ptr::write_unaligned(pointer, value);
    }
    super::abi::RM_OK
}

pub(super) fn zero_out<T: Copy + Default>(pointer: *mut T) {
    if !pointer.is_null() {
        // SAFETY: caller-provided writable output.
        unsafe {
            ptr::write_unaligned(pointer, T::default());
        }
    }
}
