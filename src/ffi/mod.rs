//! C ABI 1.0: fully thread-safe handles, batch completion queues and native waiters.
//!
//! The export surface is frozen by [`abi::EXPORTED_FUNCTIONS`] and is identical on
//! every target. Platform-specific operations return `RM_UNSUPPORTED`; they never
//! disappear from the header or change signature. No exported entry unwinds across
//! the C boundary.

pub mod abi;
mod cq;
mod endpoint;
mod handle;
mod objects;
mod waiter;

pub mod api;

pub use abi::{
    RmAddress, RmEndpointConfig, RmEvent, RmHandle, RmIoSlice, RmMetrics, RmSendItem, RmStatus,
    RmWaitSource,
};

/// Contain panics at every exported entry point.
macro_rules! ffi_guard {
    ($body:block) => {{
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| $body)) {
            Ok(status) => status,
            Err(_) => crate::ffi::abi::RM_INTERNAL,
        }
    }};
}
pub(crate) use ffi_guard;

#[cfg(test)]
mod test;

/// Descriptor for the versioned metrics POD. It returns the schema version and the
/// exact `sizeof(RmMetrics)` this build exports; the remaining fields are zero.
#[unsafe(no_mangle)]
pub extern "C" fn rm_metrics_layout() -> RmMetrics {
    // SAFETY: RmMetrics contains only integer fields, so all-zero is valid.
    let mut layout: RmMetrics = unsafe { std::mem::zeroed() };
    layout.schema_version = RmMetrics::SCHEMA_VERSION;
    layout.struct_size = size_of::<RmMetrics>() as u32;
    layout
}
