//! Batch/single send paths, transaction builders, tickets and message views.

use crate::{
    Channel, DeliveryOutcome, Error, FrameLayout, SendTicket, Session,
    buffer::Reservation,
    ffi::{
        abi::{
            RM_ALREADY_COMPLETED, RM_CLOSED, RM_INVALID_ARGUMENT, RM_INVALID_STATE,
            RM_MAX_BATCH_ITEMS, RM_NO_BUFFER, RM_OK, RM_TICKET_ABORTED, RM_TICKET_RECEIVED,
            RM_TICKET_UNKNOWN, RM_UNSUPPORTED, RmHandle, RmIoSlice, RmSendItem, RmStatus,
            RmTicketState,
        },
        api::{read_address, read_bytes, status, write_out, zero_out},
        cq::{Cq, EventPermit},
        endpoint::EndpointObj,
        ffi_guard,
        handle::{self, Object},
        objects::{BuilderObj, ChannelObj, MessageObj, TicketObj, ViewObj, bitmap_words},
    },
};
use std::{
    ptr,
    sync::{Arc, Mutex},
};

/// Flatten the runtime/operation result pair used by `block_on_ffi`.
fn run<T>(result: crate::Result<Result<T, Error>>) -> Result<T, RmStatus> {
    match result {
        Ok(inner) => inner.map_err(status),
        Err(error) => Err(status(error)),
    }
}

/// Decide the wire layer from the channel handle.
fn layer_of(channel: Option<&ChannelObj>) -> u8 {
    channel.map_or(1, |channel| channel.layer)
}

fn ticket_charge(session: &Session) -> Result<Reservation, RmStatus> {
    session
        .owner
        .shared
        .pool
        .budget
        .reserve(256)
        .map_err(|_| RM_NO_BUFFER)
}

#[allow(clippy::too_many_arguments)]
fn reserve_ticket(
    endpoint: &Arc<EndpointObj>,
    session: &Session,
    session_id: u64,
    channel_id: u16,
    layer: u8,
    cq: &Arc<Cq>,
    user_data: u64,
    rust: Option<SendTicket>,
    permit: EventPermit,
) -> Result<(Arc<TicketObj>, RmHandle), RmStatus> {
    let ticket_id = endpoint.ids.next();
    let charge = ticket_charge(session)?;
    let ticket = TicketObj::new(
        ticket_id,
        session_id,
        channel_id,
        layer,
        user_data,
        cq.clone(),
        permit,
        rust,
        charge,
        Arc::downgrade(endpoint),
    );
    match handle::insert(Object::Ticket(ticket.clone())) {
        Ok(handle) => Ok((ticket, handle)),
        Err(status) => {
            // The submission already owns resources; publish an explicit terminal
            // so a completed send can never disappear from the event stream.
            ticket.finish(DeliveryOutcome::Unknown);
            Err(status)
        }
    }
}

fn spawn_monitor(endpoint: &Arc<EndpointObj>, ticket: &Arc<TicketObj>, rust: SendTicket) {
    let monitor = ticket.clone();
    endpoint.runtime().spawn(async move {
        let outcome = match rust.wait_async().await {
            Ok(receipt) => DeliveryOutcome::Delivered(receipt),
            Err(Error::DeliveryAborted(reason)) => DeliveryOutcome::Aborted(reason),
            Err(_) => DeliveryOutcome::Unknown,
        };
        monitor.finish(outcome);
    });
}

/// Reserve the terminal event before admission so a filled queue can never hide
/// an accepted message's result.
fn take_permit(cq: &Arc<Cq>) -> Result<EventPermit, RmStatus> {
    cq.try_reserve().ok_or(RM_NO_BUFFER)
}

#[allow(clippy::too_many_arguments)]
fn submit_parts(
    endpoint: &Arc<EndpointObj>,
    session: &Session,
    session_id: u64,
    channel_id: u16,
    layer: u8,
    ordered: Option<&Channel>,
    cq: &Arc<Cq>,
    data: &[u8],
    user_data: u64,
) -> Result<RmHandle, RmStatus> {
    if !matches!(layer, 1..=3) {
        return Err(RM_INVALID_ARGUMENT);
    }
    if layer == 2 && !session.supports_reliable() {
        return Err(RM_UNSUPPORTED);
    }
    if layer == 3 && !session.supports_ordered() {
        return Err(RM_UNSUPPORTED);
    }
    let runtime = endpoint.runtime();
    match layer {
        1 => {
            let permit = take_permit(cq)?;
            run(runtime.block_on_ffi(session.send_async(data)))?;
            let (ticket, handle) = reserve_ticket(
                endpoint, session, session_id, channel_id, 1, cq, user_data, None, permit,
            )?;
            ticket.finish_local();
            Ok(handle)
        }
        2 => {
            let permit = take_permit(cq)?;
            let rust = run(runtime.block_on_ffi(session.submit_reliable_async(data)))?;
            let (ticket, handle) = reserve_ticket(
                endpoint,
                session,
                session_id,
                channel_id,
                2,
                cq,
                user_data,
                Some(rust.clone()),
                permit,
            )?;
            spawn_monitor(endpoint, &ticket, rust);
            Ok(handle)
        }
        _ => {
            let ordered = ordered.ok_or(RM_INVALID_ARGUMENT)?;
            let permit = take_permit(cq)?;
            let rust = run(runtime.block_on_ffi(ordered.submit_async(data)))?;
            let channel_id = ordered.id();
            let (ticket, handle) = reserve_ticket(
                endpoint,
                session,
                session_id,
                channel_id,
                3,
                cq,
                user_data,
                Some(rust.clone()),
                permit,
            )?;
            spawn_monitor(endpoint, &ticket, rust);
            Ok(handle)
        }
    }
}

fn submit_slice(
    endpoint: &Arc<EndpointObj>,
    session: &Session,
    session_id: u64,
    channel: Option<&ChannelObj>,
    cq: &Arc<Cq>,
    data: &[u8],
    user_data: u64,
) -> Result<RmHandle, RmStatus> {
    let ordered = channel.and_then(|channel| channel.channel.as_ref());
    submit_parts(
        endpoint,
        session,
        session_id,
        channel.map_or(0, |channel| channel.id),
        layer_of(channel),
        ordered,
        cq,
        data,
        user_data,
    )
}

#[allow(clippy::too_many_arguments)]
fn submit_layout(
    endpoint: &Arc<EndpointObj>,
    session: &Session,
    session_id: u64,
    layer: u8,
    ordered: Option<&Channel>,
    cq: &Arc<Cq>,
    layout: FrameLayout,
    user_data: u64,
) -> Result<RmHandle, RmStatus> {
    let runtime = endpoint.runtime();
    match layer {
        1 => {
            let permit = take_permit(cq)?;
            run(runtime.block_on_ffi(session.send_layout_async(layout)))?;
            let (ticket, handle) = reserve_ticket(
                endpoint,
                session,
                session_id,
                ordered.map_or(0, |channel| channel.id()),
                1,
                cq,
                user_data,
                None,
                permit,
            )?;
            ticket.finish_local();
            Ok(handle)
        }
        2 => {
            let permit = take_permit(cq)?;
            let rust = run(runtime.block_on_ffi(session.submit_reliable_layout_async(layout)))?;
            let (ticket, handle) = reserve_ticket(
                endpoint,
                session,
                session_id,
                ordered.map_or(0, |channel| channel.id()),
                2,
                cq,
                user_data,
                Some(rust.clone()),
                permit,
            )?;
            spawn_monitor(endpoint, &ticket, rust);
            Ok(handle)
        }
        3 => {
            let ordered = ordered.ok_or(RM_INVALID_ARGUMENT)?;
            let permit = take_permit(cq)?;
            let rust = run(runtime.block_on_ffi(ordered.submit_layout_async(layout)))?;
            let channel_id = ordered.id();
            let (ticket, handle) = reserve_ticket(
                endpoint,
                session,
                session_id,
                channel_id,
                3,
                cq,
                user_data,
                Some(rust.clone()),
                permit,
            )?;
            spawn_monitor(endpoint, &ticket, rust);
            Ok(handle)
        }
        _ => Err(RM_INVALID_ARGUMENT),
    }
}

fn submit_core(
    endpoint: &Arc<EndpointObj>,
    peer: Option<std::net::SocketAddr>,
    channel_handle: RmHandle,
    cq_handle: RmHandle,
    data: &[u8],
    user_data: u64,
) -> Result<RmHandle, RmStatus> {
    let cq = if cq_handle == 0 {
        endpoint.default_cq.clone()
    } else {
        handle::pin_cq(cq_handle)?
    };
    let channel = if channel_handle == 0 {
        None
    } else {
        Some(handle::pin_channel(channel_handle)?)
    };
    let (session, session_id, peer) = match &channel {
        Some(channel) => {
            let peer = channel.session.peer_addr();
            let session_id = endpoint
                .session_handle(peer)
                .map_or(0, |session| session.id);
            (channel.session.clone(), session_id, peer)
        }
        None => {
            let peer = peer.ok_or(RM_INVALID_ARGUMENT)?;
            let session = endpoint.session_for(peer)?;
            (session.session.clone(), session.id, peer)
        }
    };
    if session.peer_addr() != peer {
        return Err(RM_INVALID_ARGUMENT);
    }
    submit_slice(
        endpoint,
        &session,
        session_id,
        channel.as_deref(),
        &cq,
        data,
        user_data,
    )
}

fn read_send_item(items: *const RmSendItem, item_size: usize, index: usize) -> RmSendItem {
    // SAFETY: caller validated count/stride and readability for the whole array.
    unsafe {
        ptr::read_unaligned(
            items
                .cast::<u8>()
                .add(index * item_size)
                .cast::<RmSendItem>(),
        )
    }
}

fn batch_send(
    endpoint: RmHandle,
    items: *const RmSendItem,
    item_size: usize,
    count: u32,
    tickets: *mut RmHandle,
    accepted: *mut u32,
) -> RmStatus {
    if accepted.is_null() || tickets.is_null() || items.is_null() {
        return RM_INVALID_ARGUMENT;
    }
    if count as usize > RM_MAX_BATCH_ITEMS || item_size < size_of::<RmSendItem>() || count == 0 {
        return RM_INVALID_ARGUMENT;
    }
    let endpoint = match handle::pin_endpoint(endpoint) {
        Ok(endpoint) => endpoint,
        Err(status) => return status,
    };
    let mut accepted_count = 0u32;
    let mut first_error = RM_OK;
    for index in 0..count as usize {
        // SAFETY: count/stride validated above; tickets has `count` slots.
        unsafe {
            ptr::write_unaligned(tickets.add(index), 0);
        }
        let item = read_send_item(items, item_size, index);
        let data = match read_bytes(item.data, item.len) {
            Ok(data) => data,
            Err(status) => {
                first_error = status;
                break;
            }
        };
        let peer = if item.channel == 0 {
            match read_address(&item.peer) {
                Ok(peer) => Some(peer),
                Err(status) => {
                    first_error = status;
                    break;
                }
            }
        } else {
            None
        };
        match submit_core(
            &endpoint,
            peer,
            item.channel,
            item.completion_queue,
            data,
            item.user_data,
        ) {
            Ok(ticket) => {
                // SAFETY: same caller array; index in range.
                unsafe {
                    ptr::write_unaligned(tickets.add(index), ticket);
                }
                accepted_count += 1;
            }
            Err(status) => {
                first_error = status;
                break;
            }
        }
    }
    // SAFETY: validated non-null output.
    unsafe {
        ptr::write_unaligned(accepted, accepted_count);
    }
    if accepted_count > 0 {
        RM_OK
    } else {
        first_error
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_send_copy(
    endpoint: RmHandle,
    peer: *const crate::ffi::abi::RmAddress,
    data: *const u8,
    len: u32,
    user_data: u64,
    ticket: *mut RmHandle,
) -> RmStatus {
    ffi_guard!({
        if ticket.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(ticket);
        let endpoint = match handle::pin_endpoint(endpoint) {
            Ok(endpoint) => endpoint,
            Err(status) => return status,
        };
        let peer = match read_address(peer) {
            Ok(peer) => peer,
            Err(status) => return status,
        };
        let data = match read_bytes(data, len) {
            Ok(data) => data,
            Err(status) => return status,
        };
        let session = match endpoint.session_for(peer) {
            Ok(session) => session,
            Err(status) => return status,
        };
        match submit_slice(
            &endpoint,
            &session.session,
            session.id,
            None,
            &endpoint.default_cq.clone(),
            data,
            user_data,
        ) {
            Ok(value) => write_out(ticket, value),
            Err(status) => status,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_send_borrowed(
    endpoint: RmHandle,
    peer: *const crate::ffi::abi::RmAddress,
    data: *const u8,
    len: u32,
    user_data: u64,
    ticket: *mut RmHandle,
) -> RmStatus {
    // The ABI lifetime contract is a superset of the current conservative
    // implementation, which copies on submit; LOCAL_BUFFER_RELEASED still marks the
    // terminal event. No kernel zero-copy is claimed.
    rm_send_copy(endpoint, peer, data, len, user_data, ticket)
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_send_batch(
    endpoint: RmHandle,
    items: *const RmSendItem,
    item_size: usize,
    count: u32,
    tickets: *mut RmHandle,
    accepted_count: *mut u32,
) -> RmStatus {
    ffi_guard!({ batch_send(endpoint, items, item_size, count, tickets, accepted_count) })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_tx_builder_create(
    channel: RmHandle,
    len: u32,
    out: *mut RmHandle,
) -> RmStatus {
    ffi_guard!({
        if out.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(out);
        let channel = match handle::pin_channel(channel) {
            Ok(channel) => channel,
            Err(status) => return status,
        };
        if channel.is_closed() {
            return RM_INVALID_STATE;
        }
        let layout = match (channel.layer, channel.channel.as_ref()) {
            (3, Some(ordered)) => ordered.allocate_frame(len as usize),
            _ => channel.session.allocate_frame(len as usize),
        };
        let layout = match layout {
            Ok(layout) => layout,
            Err(error) => return status(error),
        };
        let words = bitmap_words(len as usize);
        let mut bitmap = Vec::new();
        if bitmap.try_reserve_exact(words).is_err() {
            return RM_NO_BUFFER;
        }
        bitmap.resize(words, 0u64);
        let Ok(charge) = channel
            .endpoint
            .owner()
            .shared
            .pool
            .budget
            .reserve(words * size_of::<u64>() + 64)
        else {
            return RM_NO_BUFFER;
        };
        let builder = BuilderObj::new(
            channel.session.clone(),
            channel.channel.clone(),
            channel.layer,
            channel.cq.clone(),
            layout,
            bitmap,
            charge,
            channel.owner.clone(),
        );
        match handle::insert(Object::Builder(Arc::new(Mutex::new(builder)))) {
            Ok(value) => write_out(out, value),
            Err(status) => status,
        }
    })
}

fn write_into_builder(builder: &mut BuilderObj, offset: usize, data: &[u8]) -> RmStatus {
    if builder.sealed || builder.submitted {
        return RM_INVALID_STATE;
    }
    let total = builder.total_len();
    let Some(end) = offset.checked_add(data.len()) else {
        return RM_INVALID_ARGUMENT;
    };
    if end > total {
        return RM_INVALID_ARGUMENT;
    }
    let Some(layout) = builder.layout.as_mut() else {
        return RM_INVALID_STATE;
    };
    let stride = layout.meta.stride.get() as usize;
    if stride == 0 {
        return RM_INVALID_STATE;
    }
    let mut done = 0usize;
    while done < data.len() {
        let absolute = offset + done;
        let segment = absolute / stride;
        let within = absolute % stride;
        let take = (data.len() - done).min(stride - within);
        let Some(slot) = layout.payload_mut(segment) else {
            return RM_INVALID_STATE;
        };
        if within + take > slot.len() {
            return RM_INVALID_STATE;
        }
        slot[within..within + take].copy_from_slice(&data[done..done + take]);
        done += take;
    }
    builder.mark(offset, data.len());
    RM_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_tx_builder_write(
    builder: RmHandle,
    offset: u32,
    data: *const u8,
    len: u32,
) -> RmStatus {
    ffi_guard!({
        let builder = match handle::pin_builder(builder) {
            Ok(builder) => builder,
            Err(status) => return status,
        };
        let data = match read_bytes(data, len) {
            Ok(data) => data,
            Err(status) => return status,
        };
        let mut builder = builder
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        write_into_builder(&mut builder, offset as usize, data)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_tx_builder_writev(
    builder: RmHandle,
    offset: u32,
    slices: *const RmIoSlice,
    item_size: usize,
    count: u32,
) -> RmStatus {
    ffi_guard!({
        if count as usize > RM_MAX_BATCH_ITEMS {
            return RM_INVALID_ARGUMENT;
        }
        if count > 0 && (slices.is_null() || item_size < size_of::<RmIoSlice>()) {
            return RM_INVALID_ARGUMENT;
        }
        let builder = match handle::pin_builder(builder) {
            Ok(builder) => builder,
            Err(status) => return status,
        };
        let mut builder = builder
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut cursor = offset as usize;
        for index in 0..count as usize {
            // SAFETY: count/stride validated; array is readable by contract.
            let item = unsafe {
                ptr::read_unaligned(
                    slices
                        .cast::<u8>()
                        .add(index * item_size)
                        .cast::<RmIoSlice>(),
                )
            };
            let data = match read_bytes(item.data, item.len) {
                Ok(data) => data,
                Err(status) => return status,
            };
            let status = write_into_builder(&mut builder, cursor, data);
            if status != RM_OK {
                return status;
            }
            cursor = match cursor.checked_add(data.len()) {
                Some(value) => value,
                None => return RM_INVALID_ARGUMENT,
            };
        }
        RM_OK
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_tx_builder_seal(builder: RmHandle) -> RmStatus {
    ffi_guard!({
        let builder = match handle::pin_builder(builder) {
            Ok(builder) => builder,
            Err(status) => return status,
        };
        let mut builder = builder
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if builder.sealed || builder.submitted {
            return RM_INVALID_STATE;
        }
        if !builder.is_complete() {
            return RM_INVALID_STATE;
        }
        builder.sealed = true;
        RM_OK
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_tx_builder_submit(
    builder: RmHandle,
    user_data: u64,
    ticket: *mut RmHandle,
) -> RmStatus {
    ffi_guard!({
        if ticket.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(ticket);
        let builder = match handle::pin_builder(builder) {
            Ok(builder) => builder,
            Err(status) => return status,
        };
        let mut guard = builder
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !guard.sealed || guard.submitted {
            return RM_INVALID_STATE;
        }
        let Some(endpoint_obj) = guard.owner.upgrade() else {
            return RM_CLOSED;
        };
        let Some(layout) = guard.layout.take() else {
            return RM_INVALID_STATE;
        };
        guard.submitted = true;
        let session = guard.session.clone();
        let ordered = guard.channel.clone();
        let layer = guard.layer;
        let cq = guard.cq.clone();
        drop(guard);
        let session_id = endpoint_obj
            .session_handle(session.peer_addr())
            .map_or(0, |session| session.id);
        match submit_layout(
            &endpoint_obj,
            &session,
            session_id,
            layer,
            ordered.as_ref(),
            &cq,
            layout,
            user_data,
        ) {
            Ok(value) => write_out(ticket, value),
            Err(status) => status,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_ticket_query(
    ticket: RmHandle,
    state: *mut RmTicketState,
    flags: *mut u32,
) -> RmStatus {
    ffi_guard!({
        if state.is_null() || flags.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        let ticket = match handle::pin_ticket(ticket) {
            Ok(ticket) => ticket,
            Err(status) => return status,
        };
        // SAFETY: validated non-null outputs.
        unsafe {
            ptr::write_unaligned(state, ticket.state());
            ptr::write_unaligned(flags, ticket.flags());
        }
        RM_OK
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_ticket_cancel(ticket: RmHandle) -> RmStatus {
    ffi_guard!({
        let ticket = match handle::pin_ticket(ticket) {
            Ok(ticket) => ticket,
            Err(status) => return status,
        };
        if matches!(
            ticket.state(),
            RM_TICKET_RECEIVED | RM_TICKET_ABORTED | RM_TICKET_UNKNOWN
        ) {
            return RM_ALREADY_COMPLETED;
        }
        let Some(rust) = ticket.take_rust() else {
            return RM_ALREADY_COMPLETED;
        };
        let Some(endpoint) = ticket.owner.upgrade() else {
            return RM_INVALID_STATE;
        };
        let result = endpoint
            .runtime()
            .block_on_ffi(async move { rust.cancel_async().await });
        match result {
            Ok(Ok(outcome)) => {
                ticket.finish(outcome);
                RM_OK
            }
            Ok(Err(error)) => status(error),
            Err(error) => status(error),
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn rm_message_view(
    message: RmHandle,
    data: *mut *const u8,
    len: *mut u32,
    view_owner: *mut RmHandle,
) -> RmStatus {
    ffi_guard!({
        if data.is_null() || len.is_null() || view_owner.is_null() {
            return RM_INVALID_ARGUMENT;
        }
        zero_out(view_owner);
        let message = match handle::pin_message(message) {
            Ok(message) => message,
            Err(status) => return status,
        };
        let view = Arc::new(ViewObj {
            frame: message.frame.clone(),
        });
        let bytes = view.frame.as_slice();
        let pointer = bytes.as_ptr();
        let length = bytes.len() as u32;
        let owner = match handle::insert(Object::View(view)) {
            Ok(owner) => owner,
            Err(status) => return status,
        };
        // SAFETY: validated non-null outputs; pointer remains valid while the view
        // token or message token is alive.
        unsafe {
            ptr::write_unaligned(data, pointer);
            ptr::write_unaligned(len, length);
            ptr::write_unaligned(view_owner, owner);
        }
        RM_OK
    })
}

const _: () = assert!(size_of::<MessageObj>() > 0);
