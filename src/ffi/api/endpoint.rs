//! Endpoint, session, channel, waiter-source and metrics exports.

use crate::{
    Endpoint,
    ffi::{
        abi::{
            RM_CLOSED, RM_EVENT_CHANNEL_CLOSED, RM_EVENT_SESSION_CLOSED, RM_INVALID_ARGUMENT,
            RM_NO_BUFFER, RM_OK, RM_UNSUPPORTED, RmAddress, RmEvent, RmEventKind, RmHandle,
            RmMetrics, RmStatus, RmWaitSource, RmWaitSourceKind,
        },
        api::{read_address, read_config, status, write_address, write_out, zero_out},
        cq::Cq,
        endpoint::EndpointObj,
        ffi_guard, handle,
        handle::Object,
        waiter::Waiter,
    },
};
use std::{ptr, sync::Arc};

#[unsafe(no_mangle)]
pub extern "C" fn rm_endpoint_bind(
    config: *const crate::ffi::abi::RmEndpointConfig,
    address: *const RmAddress,
    out: *mut RmHandle,
) -> RmStatus {
    ffi_guard!({
        if out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(out);
        let (config, cq_capacity) = match read_config(config) {
            Ok(values) => values,
            Err(status) => return status,
        };
        let address = match read_address(address) {
            Ok(address) => address,
            Err(status) => return status,
        };
        let endpoint = match Endpoint::bind(address, config) {
            Ok(endpoint) => endpoint,
            Err(error) => return status(error),
        };
        let object = match EndpointObj::new(endpoint, cq_capacity) {
            Ok(object) => object,
            Err(status) => return status,
        };
        match handle::insert(Object::Endpoint(object)) {
            Ok(handle) => write_out(out, handle),
            Err(status) => status,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_endpoint_local_address(
    endpoint: RmHandle,
    size: usize,
    out: *mut RmAddress,
) -> RmStatus {
    ffi_guard!({
        if size < size_of::<RmAddress>() {
            return RM_INVALID_ARGUMENT;
        }
        let endpoint = match handle::pin_endpoint(endpoint) {
            Ok(endpoint) => endpoint,
            Err(status) => return status,
        };
        match endpoint.endpoint.local_addr() {
            Ok(address) => write_address(address, out),
            Err(error) => status(error),
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_endpoint_default_cq(endpoint: RmHandle, out: *mut RmHandle) -> RmStatus {
    ffi_guard!({
        if out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(out);
        let endpoint = match handle::pin_endpoint(endpoint) {
            Ok(endpoint) => endpoint,
            Err(status) => return status,
        };
        match handle::insert(Object::Cq(endpoint.default_cq.clone())) {
            Ok(value) => write_out(out, value),
            Err(status) => status,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_cq_create(endpoint: RmHandle, capacity: u32, out: *mut RmHandle) -> RmStatus {
    ffi_guard!({
        if out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(out);
        let endpoint = match handle::pin_endpoint(endpoint) {
            Ok(endpoint) => endpoint,
            Err(status) => return status,
        };
        let budget = endpoint.default_cq.budget();
        if !(1..=65_536).contains(&capacity) {
            return RM_INVALID_ARGUMENT;
        }
        let Some(cq) = Cq::new(budget, capacity as usize) else {
            return RM_NO_BUFFER;
        };
        match handle::insert(Object::Cq(cq)) {
            Ok(value) => write_out(out, value),
            Err(status) => status,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_session_open(
    endpoint: RmHandle,
    peer: *const RmAddress,
    out: *mut RmHandle,
) -> RmStatus {
    ffi_guard!({
        if out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(out);
        let endpoint = match handle::pin_endpoint(endpoint) {
            Ok(endpoint) => endpoint,
            Err(status) => return status,
        };
        let peer = match read_address(peer) {
            Ok(peer) => peer,
            Err(status) => return status,
        };
        let session = match endpoint.session_for(peer) {
            Ok(session) => session,
            Err(status) => return status,
        };
        match handle::insert(Object::Session(session)) {
            Ok(value) => write_out(out, value),
            Err(status) => status,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_session_close(session: RmHandle) -> RmStatus {
    ffi_guard!({
        let session = match handle::pin_session(session) {
            Ok(session) => session,
            Err(status) => return status,
        };
        if session.is_closed() {
            return RM_OK;
        }
        session.close();
        let endpoint = session.endpoint.clone();
        let peer = session.peer;
        let runtime = endpoint.owner().runtime.clone();
        let result = runtime.block_on_ffi(async move { endpoint.disconnect_async(peer).await });
        publish_event(
            &session.default_cq,
            RM_EVENT_SESSION_CLOSED,
            RM_OK,
            session.id,
        );
        match result {
            Ok(Ok(())) => RM_OK,
            Ok(Err(crate::Error::Closed)) => RM_OK,
            Ok(Err(error)) => status(error),
            Err(crate::Error::Closed) => RM_OK,
            Err(error) => status(error),
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_channel_open(
    session: RmHandle,
    layer: u32,
    id: u16,
    cq: RmHandle,
    out: *mut RmHandle,
) -> RmStatus {
    ffi_guard!({
        if out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(out);
        let session = match handle::pin_session(session) {
            Ok(session) => session,
            Err(status) => return status,
        };
        let completion = if cq == 0 {
            session.default_cq.clone()
        } else {
            match handle::pin_cq(cq) {
                Ok(cq) => cq,
                Err(status) => return status,
            }
        };
        let Ok(layer) = u8::try_from(layer) else {
            return RM_INVALID_ARGUMENT;
        };
        let Some(endpoint) = session.owner.upgrade() else {
            return RM_CLOSED;
        };
        let channel = match endpoint.channel_for(&session, layer, id, completion) {
            Ok(channel) => channel,
            Err(status) => return status,
        };
        match handle::insert(Object::Channel(channel)) {
            Ok(value) => write_out(out, value),
            Err(status) => status,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_channel_close(channel: RmHandle) -> RmStatus {
    ffi_guard!({
        let channel = match handle::pin_channel(channel) {
            Ok(channel) => channel,
            Err(status) => return status,
        };
        if channel.is_closed() {
            return RM_OK;
        }
        channel
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        if channel.layer == 3 {
            if let Some(ordered) = channel.channel.clone() {
                let runtime = channel.endpoint.owner().runtime.clone();
                let result = runtime.block_on_ffi(async move { ordered.close_async().await });
                return match result {
                    Ok(_) => RM_OK,
                    Err(error) => status(error),
                };
            }
            return RM_UNSUPPORTED;
        }
        publish_event(&channel.cq, RM_EVENT_CHANNEL_CLOSED, RM_OK, 0);
        RM_OK
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_endpoint_wait_source(
    endpoint: RmHandle,
    size: usize,
    out: *mut RmWaitSource,
) -> RmStatus {
    ffi_guard!({
        if size < size_of::<RmWaitSource>() || out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(out);
        let endpoint = match handle::pin_endpoint(endpoint) {
            Ok(endpoint) => endpoint,
            Err(status) => return status,
        };
        let waiter = match endpoint.waiter() {
            Ok(waiter) => waiter,
            Err(status) => return status,
        };
        export_with_owner(&waiter, out)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_endpoint_arm(endpoint: RmHandle, observed_epoch: u64) -> RmStatus {
    ffi_guard!({
        let endpoint = match handle::pin_endpoint(endpoint) {
            Ok(endpoint) => endpoint,
            Err(status) => return status,
        };
        match endpoint.waiter() {
            Ok(waiter) => waiter.arm(observed_epoch),
            Err(status) => status,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_endpoint_close(endpoint: RmHandle) -> RmStatus {
    ffi_guard!({
        let endpoint = match handle::pin_endpoint(endpoint) {
            Ok(endpoint) => endpoint,
            Err(status) => return status,
        };
        endpoint.close();
        RM_OK
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_endpoint_metrics_copy(
    endpoint: RmHandle,
    dst: *mut u8,
    capacity: usize,
    required: *mut usize,
) -> RmStatus {
    ffi_guard!({
        if required.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        let need = size_of::<RmMetrics>();
        // SAFETY: caller-provided size output.
        unsafe {
            ptr::write_unaligned(required, need);
        }
        if dst.is_null() {
            return RM_OK;
        }
        if capacity < need {
            return RM_INVALID_ARGUMENT;
        }
        let endpoint = match handle::pin_endpoint(endpoint) {
            Ok(endpoint) => endpoint,
            Err(status) => return status,
        };
        let metrics = metrics_from_snapshot(endpoint.endpoint.metrics());
        // SAFETY: capacity was checked; dst may be unaligned.
        unsafe {
            ptr::write_unaligned(dst.cast::<RmMetrics>(), metrics);
        }
        RM_OK
    })
}

fn export_with_owner(waiter: &Arc<Waiter>, out: *mut RmWaitSource) -> RmStatus {
    let mut source = RmWaitSource {
        kind: RmWaitSourceKind::RM_WAIT_POSIX_FD,
        flags: 0,
        native_value: 0,
        owner: 0,
    };
    let status = waiter.export(&mut source);
    if status != RM_OK {
        return status;
    }
    let Ok(owner) = handle::insert(Object::Waiter(waiter.clone())) else {
        return RM_NO_BUFFER;
    };
    source.owner = owner;
    // SAFETY: validated non-null output.
    unsafe {
        ptr::write_unaligned(out, source);
    }
    RM_OK
}

fn publish_event(cq: &Arc<Cq>, kind: RmEventKind, status: RmStatus, session_id: u64) {
    let mut event = RmEvent::empty(kind, status);
    event.session_id = session_id;
    if let Some(permit) = cq.try_reserve() {
        cq.publish(event, permit);
    } else {
        cq.publish_control(event);
    }
}

fn metrics_from_snapshot(snapshot: crate::MetricsSnapshot) -> RmMetrics {
    RmMetrics {
        schema_version: RmMetrics::SCHEMA_VERSION,
        struct_size: size_of::<RmMetrics>() as u32,
        sent_datagrams: snapshot.sent_datagrams,
        sent_wire_bytes: snapshot.sent_wire_bytes,
        received_datagrams: snapshot.received_datagrams,
        received_wire_bytes: snapshot.received_wire_bytes,
        sent_frames: snapshot.sent_frames,
        sent_application_bytes: snapshot.sent_application_bytes,
        received_frames: snapshot.received_frames,
        received_application_bytes: snapshot.received_application_bytes,
        malformed_datagrams: snapshot.malformed_datagrams,
        integrity_failures: snapshot.integrity_failures,
        duplicate_segments: snapshot.duplicate_segments,
        expired_frames: snapshot.expired_frames,
        resource_drops: snapshot.resource_drops,
        unknown_sessions: snapshot.unknown_sessions,
        orphan_drops: snapshot.orphan_drops,
        io_errors: snapshot.io_errors,
        crc_parallel_jobs: snapshot.crc_parallel_jobs,
        send_syscalls: snapshot.send_syscalls,
        receive_syscalls: snapshot.receive_syscalls,
        reliable_submitted: snapshot.reliable_submitted,
        ordered_submitted: snapshot.ordered_submitted,
        ordered_ready_frames: snapshot.ordered_ready_frames,
        ordered_ready_bytes: snapshot.ordered_ready_bytes,
        ordered_delivered_frames: snapshot.ordered_delivered_frames,
        ordered_delivered_bytes: snapshot.ordered_delivered_bytes,
        ordered_buffered_frames: snapshot.ordered_buffered_frames,
        ordered_buffered_bytes: snapshot.ordered_buffered_bytes,
        ordered_reorder_peak: snapshot.ordered_reorder_peak,
        ordered_hol_ns_total: snapshot.ordered_hol_ns_total,
        ordered_hol_ns_max: snapshot.ordered_hol_ns_max,
        ordered_channels_closed: snapshot.ordered_channels_closed,
        reliable_completed: snapshot.reliable_completed,
        reliable_aborted: snapshot.reliable_aborted,
        reliable_unknown: snapshot.reliable_unknown,
        reliable_retransmits: snapshot.reliable_retransmits,
        reliable_recovery_rounds: snapshot.reliable_recovery_rounds,
        reliable_control_datagrams: snapshot.reliable_control_datagrams,
        reliable_control_bytes: snapshot.reliable_control_bytes,
        reliable_data_datagrams: snapshot.reliable_data_datagrams,
        reliable_segment_batches: snapshot.reliable_segment_batches,
        reliable_retransmitted_bytes: snapshot.reliable_retransmitted_bytes,
        reliable_grants: snapshot.reliable_grants,
        reliable_credit_stalls: snapshot.reliable_credit_stalls,
        reliable_rack_losses: snapshot.reliable_rack_losses,
        reliable_nacks: snapshot.reliable_nacks,
        reliable_tail_probes: snapshot.reliable_tail_probes,
        memory_used: snapshot.memory_used as u64,
        memory_peak: snapshot.memory_peak as u64,
        pool_cached: snapshot.pool_cached as u64,
        socket_recv_buffer: snapshot.socket_recv_buffer as u64,
        socket_send_buffer: snapshot.socket_send_buffer as u64,
        bound_udp_sockets: snapshot.bound_udp_sockets as u64,
    }
}
