//! ABI, handle, queue, waiter and end-to-end C FFI tests.

use super::{
    abi::*,
    api,
    cq::Cq,
    handle::{self, Object},
    waiter::Waiter,
};
use crate::buffer::Budget;
use std::{
    collections::BTreeSet,
    mem::{MaybeUninit, offset_of, size_of},
    ptr, thread,
    time::{Duration, Instant},
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn config() -> RmEndpointConfig {
    let mut out = MaybeUninit::<RmEndpointConfig>::uninit();
    let status = api::handle::rm_endpoint_config_init(
        RM_ABI_VERSION,
        size_of::<RmEndpointConfig>(),
        out.as_mut_ptr(),
    );
    assert_eq!(status, RM_OK);
    // SAFETY: the call initialized the whole struct.
    unsafe { out.assume_init() }
}

fn loopback(port: u16) -> RmAddress {
    let mut address = [0u8; 16];
    address[..4].copy_from_slice(&[127, 0, 0, 1]);
    RmAddress {
        family: 4,
        port,
        reserved: 0,
        scope_id: 0,
        address,
    }
}

fn bind(config: &RmEndpointConfig) -> RmHandle {
    let mut out = 0;
    let status = api::endpoint::rm_endpoint_bind(config, &loopback(0), &mut out);
    assert_eq!(status, RM_OK);
    assert_ne!(out, 0);
    out
}

fn local_address(endpoint: RmHandle) -> RmAddress {
    let mut out = MaybeUninit::<RmAddress>::uninit();
    let status = api::endpoint::rm_endpoint_local_address(
        endpoint,
        size_of::<RmAddress>(),
        out.as_mut_ptr(),
    );
    assert_eq!(status, RM_OK);
    // SAFETY: the call initialized the struct.
    unsafe { out.assume_init() }
}

fn default_cq(endpoint: RmHandle) -> RmHandle {
    let mut out = 0;
    assert_eq!(
        api::endpoint::rm_endpoint_default_cq(endpoint, &mut out),
        RM_OK
    );
    out
}

fn poll_one(endpoint: RmHandle) -> (RmStatus, Option<RmEvent>, u64) {
    let mut event = RmEvent::empty(RM_EVENT_RESERVED, RM_OK);
    let mut count = 0;
    let mut epoch = 0;
    let status = api::poll::rm_endpoint_poll_batch(
        endpoint,
        &mut event,
        size_of::<RmEvent>(),
        1,
        &mut count,
        &mut epoch,
    );
    (status, (count == 1).then_some(event), epoch)
}

/// Poll until the expected event kind arrives, using the arm/wait protocol.
fn next_event(endpoint: RmHandle, expected: RmEventKind) -> RmEvent {
    let cq = default_cq(endpoint);
    let mut waiter = 0;
    assert_eq!(api::poll::rm_waiter_create(cq, &mut waiter), RM_OK);
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let (status, event, epoch) = poll_one(endpoint);
        assert!(
            matches!(status, RM_OK | RM_WOULD_BLOCK | RM_CLOSED),
            "poll status {status:?}"
        );
        if let Some(event) = event {
            if event.kind == expected {
                let _ = api::handle::rm_handle_release(waiter);
                return event;
            }
            if event.message != 0 {
                let _ = api::handle::rm_handle_release(event.message);
            }
            continue;
        }
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for event kind {expected:?}"
        );
        let remaining = deadline.saturating_duration_since(now).as_nanos() as u64;
        match api::poll::rm_waiter_arm(waiter, epoch) {
            RM_READY => continue,
            RM_ARMED => {
                let status = api::poll::rm_waiter_wait(waiter, remaining);
                assert!(matches!(status, RM_OK | RM_TIMED_OUT), "wait {status:?}");
            }
            status => panic!("arm status {status:?}"),
        }
        let _ = api::poll::rm_waiter_reset(waiter);
    }
}

fn send_copy(endpoint: RmHandle, peer: &RmAddress, payload: &[u8]) -> RmHandle {
    let mut ticket = 0;
    let status = api::send::rm_send_copy(
        endpoint,
        peer,
        payload.as_ptr(),
        payload.len() as u32,
        0xfeed,
        &mut ticket,
    );
    assert_eq!(status, RM_OK);
    assert_ne!(ticket, 0);
    ticket
}

fn view_message(message: RmHandle) -> (Vec<u8>, RmHandle) {
    let mut data: *const u8 = ptr::null();
    let mut len = 0u32;
    let mut view = 0;
    assert_eq!(
        api::send::rm_message_view(message, &mut data, &mut len, &mut view),
        RM_OK
    );
    // SAFETY: the view owns the frame until `view` is released.
    let bytes = unsafe { std::slice::from_raw_parts(data, len as usize) }.to_vec();
    (bytes, view)
}

#[test]
fn abi_layout_and_status_codes_are_frozen() {
    assert_eq!(RM_ABI_VERSION, 0x0001_0000);
    assert_eq!(size_of::<RmHandle>(), size_of::<usize>());
    assert_eq!(size_of::<RmStatus>(), 4);
    // Frozen status values: changing one silently changes the ABI.
    assert_eq!(RM_OK as i32, 0);
    assert_eq!(RM_WOULD_BLOCK as i32, 1);
    assert_eq!(RM_TIMED_OUT as i32, 2);
    assert_eq!(RM_READY as i32, 3);
    assert_eq!(RM_ARMED as i32, 4);
    assert_eq!(RM_INVALID_ARGUMENT as i32, -1);
    assert_eq!(RM_INVALID_HANDLE as i32, -2);
    assert_eq!(RM_VERSION_MISMATCH as i32, -3);
    assert_eq!(RM_MESSAGE_TOO_BIG as i32, -4);
    assert_eq!(RM_NO_BUFFER as i32, -5);
    assert_eq!(RM_ABORTED as i32, -6);
    assert_eq!(RM_CLOSED as i32, -7);
    assert_eq!(RM_OUTCOME_UNKNOWN as i32, -8);
    assert_eq!(RM_INTERNAL as i32, -9);
    assert_eq!(RM_UNSUPPORTED as i32, -10);
    assert_eq!(RM_INVALID_STATE as i32, -11);
    assert_eq!(RM_WOULD_DEADLOCK as i32, -12);
    assert_eq!(RM_ALREADY_COMPLETED as i32, -13);
    assert_eq!(RM_IO as i32, -14);
    let metrics = api::handle::rm_abi_version();
    assert_eq!(metrics, RM_ABI_VERSION);
    let layout = super::rm_metrics_layout();
    assert_eq!(layout.schema_version, 1);
    assert_eq!(layout.struct_size as usize, size_of::<RmMetrics>());
}

/// Exact 64-bit layout, also asserted by the header's `_Static_assert`s.
#[cfg(target_pointer_width = "64")]
#[test]
fn abi_64bit_layout_is_frozen() {
    assert_eq!(size_of::<RmAddress>(), 28);
    assert_eq!(size_of::<RmEvent>(), 56);
    assert_eq!(size_of::<RmSendItem>(), 72);
    assert_eq!(size_of::<RmIoSlice>(), 16);
    assert_eq!(size_of::<RmWaitSource>(), 24);
    assert_eq!(size_of::<RmEndpointConfig>(), 40);
    assert_eq!(offset_of!(RmEvent, kind), 0);
    assert_eq!(offset_of!(RmEvent, status), 4);
    assert_eq!(offset_of!(RmEvent, user_data), 8);
    assert_eq!(offset_of!(RmEvent, ticket_id), 16);
    assert_eq!(offset_of!(RmEvent, session_id), 24);
    assert_eq!(offset_of!(RmEvent, delivery_seq), 32);
    assert_eq!(offset_of!(RmEvent, message), 40);
    assert_eq!(offset_of!(RmEvent, channel_seq), 48);
    assert_eq!(offset_of!(RmEvent, channel_id), 52);
    assert_eq!(offset_of!(RmEvent, layer), 54);
    assert_eq!(offset_of!(RmEvent, flags), 55);
    assert_eq!(offset_of!(RmSendItem, channel), 32);
    assert_eq!(offset_of!(RmSendItem, completion_queue), 40);
    assert_eq!(offset_of!(RmSendItem, data), 48);
    assert_eq!(offset_of!(RmSendItem, len), 56);
    assert_eq!(offset_of!(RmSendItem, user_data), 64);
}

#[test]
fn header_lists_exactly_the_frozen_export_manifest() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/include/rimuru.h");
    let header = std::fs::read_to_string(path).expect("generated header is committed");
    let mut functions = BTreeSet::new();
    for line in header.lines() {
        let line = line.trim();
        if !line.ends_with(");") || line.starts_with('#') || line.starts_with("typedef") {
            continue;
        }
        let Some(start) = line.find("rm_") else {
            continue;
        };
        let tail = &line[start..];
        let Some(end) = tail.find('(') else {
            continue;
        };
        functions.insert(tail[..end].to_string());
    }
    let expected: BTreeSet<String> = EXPORTED_FUNCTIONS
        .iter()
        .map(|name| name.to_string())
        .collect();
    assert_eq!(functions, expected);
    for structure in [
        "RmEndpointConfig",
        "RmAddress",
        "RmEvent",
        "RmSendItem",
        "RmIoSlice",
        "RmWaitSource",
        "RmMetrics",
    ] {
        assert!(header.contains(structure), "header is missing {structure}");
    }
    for constant in [
        "RM_OK",
        "RM_WOULD_BLOCK",
        "RM_INVALID_HANDLE",
        "RM_EVENT_MESSAGE",
        "RM_TICKET_RECEIVED",
    ] {
        assert!(header.contains(constant), "header is missing {constant}");
    }
}

#[test]
fn handle_generations_reject_stale_tokens() {
    let budget = Budget::new(1 << 20, None);
    let cq = Cq::new(budget.clone(), 8).unwrap();
    let handle = handle::insert(Object::Cq(cq.clone())).unwrap();
    assert_ne!(handle, 0);
    let cloned = handle::clone_token(handle).unwrap();
    assert_ne!(cloned, handle);
    assert_eq!(handle::release(handle), RM_OK);
    assert_eq!(handle::pin(handle).err(), Some(RM_INVALID_HANDLE));
    assert_eq!(handle::release(handle), RM_INVALID_HANDLE);
    assert!(handle::pin_cq(cloned).is_ok());
    // Wrong object kind is rejected with the same invalid-handle status.
    let waiter = Waiter::new(cq, budget).unwrap();
    let waiter_handle = handle::insert(Object::Waiter(waiter)).unwrap();
    assert_eq!(handle::pin_cq(waiter_handle).err(), Some(RM_INVALID_HANDLE));
    assert_eq!(handle::release(waiter_handle), RM_OK);
    assert_eq!(handle::release(cloned), RM_OK);
    assert_eq!(handle::release(cloned), RM_INVALID_HANDLE);
}

#[test]
fn handle_release_batch_reports_each_item() {
    let budget = Budget::new(1 << 20, None);
    let cq = Cq::new(budget, 8).unwrap();
    let first = handle::insert(Object::Cq(cq.clone())).unwrap();
    let second = handle::insert(Object::Cq(cq)).unwrap();
    let inputs = [first, RmHandle::MAX, second];
    let mut statuses = [RM_OK; 3];
    assert_eq!(
        api::handle::rm_handle_release_batch(
            inputs.as_ptr(),
            inputs.len() as u32,
            statuses.as_mut_ptr()
        ),
        RM_OK
    );
    assert_eq!(statuses[0], RM_OK);
    assert_eq!(statuses[1], RM_INVALID_HANDLE);
    assert_eq!(statuses[2], RM_OK);
}

#[test]
fn cq_publish_poll_epoch_and_close_synthesis() {
    let budget = Budget::new(1 << 20, None);
    let cq = Cq::new(budget, 4).unwrap();
    let handle = handle::insert(Object::Cq(cq.clone())).unwrap();
    for ticket in 1..=2 {
        let permit = cq.try_reserve().unwrap();
        let mut event = RmEvent::empty(RM_EVENT_SEND_TERMINAL, RM_OK);
        event.ticket_id = ticket;
        cq.publish(event, permit);
    }
    let mut output = [RmEvent::empty(RM_EVENT_RESERVED, RM_OK); 4];
    let mut count = 0;
    let mut epoch = 0;
    assert_eq!(
        api::poll::rm_cq_poll_batch(
            handle,
            output.as_mut_ptr(),
            size_of::<RmEvent>(),
            4,
            &mut count,
            &mut epoch,
        ),
        RM_OK
    );
    assert_eq!(count, 2);
    assert_eq!(epoch, 2);
    assert_eq!(output[0].ticket_id, 1);
    assert_eq!(output[1].ticket_id, 2);
    cq.close(RM_EVENT_ENDPOINT_CLOSED, RM_OK);
    let mut closed = RmEvent::empty(RM_EVENT_RESERVED, RM_OK);
    assert_eq!(
        api::poll::rm_cq_poll_batch(
            handle,
            &mut closed,
            size_of::<RmEvent>(),
            1,
            &mut count,
            &mut epoch,
        ),
        RM_OK
    );
    assert_eq!(count, 1);
    assert_eq!(closed.kind, RM_EVENT_ENDPOINT_CLOSED);
    assert_eq!(
        api::poll::rm_cq_poll_batch(
            handle,
            &mut closed,
            size_of::<RmEvent>(),
            1,
            &mut count,
            &mut epoch,
        ),
        RM_CLOSED
    );
    assert_eq!(cq.reserved_for_test(), 0);
    assert_eq!(handle::release(handle), RM_OK);
}

#[test]
fn waiter_wakes_on_publish_and_supports_timeout_cancel_reset() {
    let budget = Budget::new(1 << 20, None);
    let cq = Cq::new(budget, 4).unwrap();
    let cq_handle = handle::insert(Object::Cq(cq.clone())).unwrap();
    let mut waiter = 0;
    assert_eq!(api::poll::rm_waiter_create(cq_handle, &mut waiter), RM_OK);

    let (_, event, epoch) = poll_one_handle(cq_handle);
    assert!(event.is_none());
    assert_eq!(api::poll::rm_waiter_arm(waiter, epoch), RM_ARMED);
    let publisher = {
        let cq = cq.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            let permit = cq.try_reserve().unwrap();
            cq.publish(RmEvent::empty(RM_EVENT_MESSAGE, RM_OK), permit);
        })
    };
    assert_eq!(api::poll::rm_waiter_wait(waiter, 2_000_000_000), RM_OK);
    publisher.join().unwrap();
    let (_, event, _) = poll_one_handle(cq_handle);
    assert!(event.is_some());

    let (_, _, epoch) = poll_one_handle(cq_handle);
    assert_eq!(api::poll::rm_waiter_arm(waiter, epoch), RM_ARMED);
    assert_eq!(api::poll::rm_waiter_wait(waiter, 1), RM_TIMED_OUT);
    assert_eq!(api::poll::rm_waiter_cancel(waiter), RM_OK);
    assert_eq!(api::poll::rm_waiter_wait(waiter, 1_000_000), RM_OK);
    assert_eq!(api::poll::rm_waiter_reset(waiter), RM_OK);
    assert_eq!(api::poll::rm_waiter_arm(waiter, epoch), RM_ARMED);
    assert_eq!(api::poll::rm_waiter_reset(waiter), RM_INVALID_STATE);
    assert_eq!(api::poll::rm_waiter_cancel(waiter), RM_OK);
    assert_eq!(api::poll::rm_waiter_reset(waiter), RM_OK);
    assert_eq!(api::handle::rm_handle_release(waiter), RM_OK);
    assert_eq!(api::handle::rm_handle_release(cq_handle), RM_OK);
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
#[test]
fn ffi_waiter_wakes_host_kqueue_on_published_event() {
    let budget = Budget::new(1 << 20, None);
    let cq = Cq::new(budget, 4).unwrap();
    let cq_handle = handle::insert(Object::Cq(cq.clone())).unwrap();
    let mut waiter = 0;
    assert_eq!(api::poll::rm_waiter_create(cq_handle, &mut waiter), RM_OK);
    // SAFETY: the kqueue descriptor is closed after unbinding the waiter.
    let kq = unsafe { libc::kqueue() };
    assert!(kq >= 0);
    let ident = 73usize;
    assert_eq!(api::poll::rm_waiter_bind_kqueue(waiter, kq, ident), RM_OK);
    let (_, _, epoch) = poll_one_handle(cq_handle);
    assert_eq!(api::poll::rm_waiter_arm(waiter, epoch), RM_ARMED);
    cq.publish(
        RmEvent::empty(RM_EVENT_MESSAGE, RM_OK),
        cq.try_reserve().unwrap(),
    );
    let mut event: libc::kevent = unsafe { std::mem::zeroed() };
    let timeout = libc::timespec {
        tv_sec: 1,
        tv_nsec: 0,
    };
    // SAFETY: the queue and output event are valid for the call.
    let count = unsafe { libc::kevent(kq, std::ptr::null(), 0, &mut event, 1, &timeout) };
    assert_eq!(count, 1);
    assert_eq!(event.ident, ident);
    assert_eq!(event.filter, libc::EVFILT_USER);
    assert_eq!(api::poll::rm_waiter_wait(waiter, 0), RM_OK);
    assert_eq!(api::poll::rm_waiter_unbind(waiter), RM_OK);
    assert_eq!(api::handle::rm_handle_release(waiter), RM_OK);
    assert_eq!(handle::release(cq_handle), RM_OK);
    unsafe { libc::close(kq) };
}

fn poll_one_handle(cq: RmHandle) -> (RmStatus, Option<RmEvent>, u64) {
    let mut event = RmEvent::empty(RM_EVENT_RESERVED, RM_OK);
    let mut count = 0;
    let mut epoch = 0;
    let status = api::poll::rm_cq_poll_batch(
        cq,
        &mut event,
        size_of::<RmEvent>(),
        1,
        &mut count,
        &mut epoch,
    );
    (status, (count == 1).then_some(event), epoch)
}

#[test]
fn endpoint_send_receive_view_metrics_and_close() {
    let config = config();
    let sender = bind(&config);
    let receiver = bind(&config);
    let peer = local_address(receiver);
    let payload: Vec<u8> = (0..4096).map(|index| (index % 251) as u8).collect();
    let ticket = send_copy(sender, &peer, &payload);
    let mut state = RM_TICKET_QUEUED;
    let mut flags = 0;
    assert_eq!(
        api::send::rm_ticket_query(ticket, &mut state, &mut flags),
        RM_OK
    );
    assert_eq!(state, RM_TICKET_RECEIVED);
    assert_eq!(flags & RM_TICKET_FLAG_LOCAL_BUFFER_RELEASED, 1);
    let terminal = next_event(sender, RM_EVENT_SEND_TERMINAL);
    assert_eq!(terminal.user_data, 0xfeed);
    assert_ne!(terminal.ticket_id, 0);
    assert_eq!(terminal.status, RM_OK);
    assert_eq!(api::handle::rm_handle_release(ticket), RM_OK);

    let message = next_event(receiver, RM_EVENT_MESSAGE);
    assert_eq!(message.status, RM_OK);
    assert_eq!(message.layer, 1);
    let (bytes, view) = view_message(message.message);
    assert_eq!(bytes, payload);
    // The view must outlive the original message token.
    assert_eq!(api::handle::rm_handle_release(message.message), RM_OK);
    let mut data: *const u8 = ptr::null();
    let mut len = 0u32;
    let mut unused = 0;
    assert_eq!(
        api::send::rm_message_view(message.message, &mut data, &mut len, &mut unused),
        RM_INVALID_HANDLE
    );
    let preserved = {
        let mut again = 0;
        let mut view_data: *const u8 = ptr::null();
        let mut view_len = 0u32;
        assert_eq!(
            api::send::rm_message_view(view, &mut view_data, &mut view_len, &mut again),
            RM_INVALID_HANDLE
        );
        // Releasing the view ends the pointer contract.
        assert_eq!(api::handle::rm_handle_release(view), RM_OK);
        true
    };
    assert!(preserved);

    // Metrics: size query then a typed copy.
    let mut required = 0;
    assert_eq!(
        api::endpoint::rm_endpoint_metrics_copy(receiver, ptr::null_mut(), 0, &mut required),
        RM_OK
    );
    assert_eq!(required, size_of::<RmMetrics>());
    let mut metrics = MaybeUninit::<RmMetrics>::uninit();
    assert_eq!(
        api::endpoint::rm_endpoint_metrics_copy(
            receiver,
            metrics.as_mut_ptr().cast(),
            required,
            &mut required
        ),
        RM_OK
    );
    // SAFETY: the copy initialized the struct.
    let metrics = unsafe { metrics.assume_init() };
    assert_eq!(metrics.schema_version, 1);
    assert!(metrics.received_application_bytes >= payload.len() as u64);

    assert_eq!(api::endpoint::rm_endpoint_close(receiver), RM_OK);
    let closed = next_event(receiver, RM_EVENT_ENDPOINT_CLOSED);
    assert_eq!(closed.status, RM_OK);
    assert_eq!(api::endpoint::rm_endpoint_close(sender), RM_OK);
    let closed = next_event(sender, RM_EVENT_ENDPOINT_CLOSED);
    assert_eq!(closed.status, RM_OK);
    assert_eq!(api::handle::rm_handle_release(sender), RM_OK);
    assert_eq!(api::handle::rm_handle_release(receiver), RM_OK);
}

#[test]
fn batch_send_builder_and_borrowed_paths() {
    let config = config();
    let sender = bind(&config);
    let receiver = bind(&config);
    let peer = local_address(receiver);
    let payloads = [vec![1u8; 32], vec![2u8; 1200], vec![3u8; 5000]];
    let items: Vec<RmSendItem> = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| RmSendItem {
            peer,
            channel: 0,
            completion_queue: 0,
            data: payload.as_ptr(),
            len: payload.len() as u32,
            flags: 0,
            user_data: index as u64,
        })
        .collect();
    let mut tickets = [0 as RmHandle; 3];
    let mut accepted = 0;
    assert_eq!(
        api::send::rm_send_batch(
            sender,
            items.as_ptr(),
            size_of::<RmSendItem>(),
            items.len() as u32,
            tickets.as_mut_ptr(),
            &mut accepted,
        ),
        RM_OK
    );
    assert_eq!(accepted, 3);
    for ticket in tickets {
        assert_ne!(ticket, 0);
        let _ = next_event(sender, RM_EVENT_SEND_TERMINAL);
        assert_eq!(api::handle::rm_handle_release(ticket), RM_OK);
    }
    for payload in &payloads {
        let message = next_event(receiver, RM_EVENT_MESSAGE);
        let (bytes, view) = view_message(message.message);
        assert_eq!(&bytes, payload);
        let _ = api::handle::rm_handle_release(view);
        let _ = api::handle::rm_handle_release(message.message);
    }

    // Borrowed path returns a ticket with the same lifecycle contract.
    let borrowed_ticket = {
        let mut ticket = 0;
        assert_eq!(
            api::send::rm_send_borrowed(sender, &peer, b"borrowed".as_ptr(), 8, 7, &mut ticket),
            RM_OK
        );
        ticket
    };
    let _ = next_event(sender, RM_EVENT_SEND_TERMINAL);
    let message = next_event(receiver, RM_EVENT_MESSAGE);
    let (bytes, view) = view_message(message.message);
    assert_eq!(bytes, b"borrowed");
    let _ = api::handle::rm_handle_release(view);
    let _ = api::handle::rm_handle_release(message.message);
    let _ = api::handle::rm_handle_release(borrowed_ticket);

    // Builder: write two halves, seal, submit, and receive the exact frame.
    let mut session = 0;
    assert_eq!(
        api::endpoint::rm_session_open(sender, &peer, &mut session),
        RM_OK
    );
    let mut channel = 0;
    assert_eq!(
        api::endpoint::rm_channel_open(session, 1, 0, 0, &mut channel),
        RM_OK
    );
    let mut builder = 0;
    assert_eq!(
        api::send::rm_tx_builder_create(channel, 3000, &mut builder),
        RM_OK
    );
    let left = vec![0xa5u8; 1000];
    let right = vec![0x5au8; 2000];
    assert_eq!(
        api::send::rm_tx_builder_write(builder, 0, left.as_ptr(), left.len() as u32),
        RM_OK
    );
    assert_eq!(
        api::send::rm_tx_builder_write(builder, 1000, right.as_ptr(), right.len() as u32),
        RM_OK
    );
    assert_eq!(api::send::rm_tx_builder_seal(builder), RM_OK);
    let mut builder_ticket = 0;
    assert_eq!(
        api::send::rm_tx_builder_submit(builder, 99, &mut builder_ticket),
        RM_OK
    );
    let _ = next_event(sender, RM_EVENT_SEND_TERMINAL);
    let message = next_event(receiver, RM_EVENT_MESSAGE);
    let (bytes, view) = view_message(message.message);
    assert_eq!(bytes.len(), 3000);
    assert!(bytes[..1000].iter().all(|byte| *byte == 0xa5));
    assert!(bytes[1000..].iter().all(|byte| *byte == 0x5a));
    let _ = api::handle::rm_handle_release(view);
    let _ = api::handle::rm_handle_release(message.message);
    let _ = api::handle::rm_handle_release(builder_ticket);
    let _ = api::handle::rm_handle_release(builder);
    let _ = api::handle::rm_handle_release(channel);
    let _ = api::endpoint::rm_session_close(session);
    let _ = api::handle::rm_handle_release(session);

    assert_eq!(api::endpoint::rm_endpoint_close(sender), RM_OK);
    assert_eq!(api::endpoint::rm_endpoint_close(receiver), RM_OK);
    let _ = api::handle::rm_handle_release(sender);
    let _ = api::handle::rm_handle_release(receiver);
}

#[test]
fn version_validation_and_platform_binding_status() {
    let mut output = MaybeUninit::<RmEndpointConfig>::uninit();
    assert_eq!(
        api::handle::rm_endpoint_config_init(
            0x0002_0000,
            size_of::<RmEndpointConfig>(),
            output.as_mut_ptr()
        ),
        RM_VERSION_MISMATCH
    );
    assert_eq!(
        api::handle::rm_endpoint_config_init(0, 1, output.as_mut_ptr()),
        RM_VERSION_MISMATCH
    );
    assert_eq!(
        api::handle::rm_endpoint_config_init(
            RM_ABI_VERSION,
            size_of::<RmEndpointConfig>() - 1,
            output.as_mut_ptr()
        ),
        RM_INVALID_ARGUMENT
    );

    let mut native = config();
    native.backend = RM_BACKEND_NATIVE;
    let mut endpoint = 0;
    assert_eq!(
        api::endpoint::rm_endpoint_bind(&native, &loopback(0), &mut endpoint),
        RM_UNSUPPORTED
    );

    let config = config();
    let endpoint = bind(&config);
    let mut address = MaybeUninit::<RmAddress>::uninit();
    assert_eq!(
        api::endpoint::rm_endpoint_local_address(endpoint, 0, address.as_mut_ptr()),
        RM_INVALID_ARGUMENT
    );
    let cq = default_cq(endpoint);
    let mut waiter = 0;
    assert_eq!(api::poll::rm_waiter_create(cq, &mut waiter), RM_OK);
    #[cfg(not(windows))]
    assert_eq!(api::poll::rm_waiter_bind_iocp(waiter, 0, 0), RM_UNSUPPORTED);
    #[cfg(windows)]
    assert_eq!(api::poll::rm_waiter_bind_iocp(waiter, 0, 0), RM_OK);
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    assert_eq!(
        api::poll::rm_waiter_bind_kqueue(waiter, 0, 0),
        RM_UNSUPPORTED
    );
    #[cfg(all(windows, not(any(target_os = "macos", target_os = "ios"))))]
    assert_eq!(api::poll::rm_waiter_unbind(waiter), RM_OK);
    assert_eq!(api::poll::rm_waiter_unbind(waiter), RM_OK);

    let mut source = MaybeUninit::<RmWaitSource>::uninit();
    assert_eq!(
        api::poll::rm_waiter_export(waiter, size_of::<RmWaitSource>(), source.as_mut_ptr()),
        RM_OK
    );
    // SAFETY: export initialized the struct.
    let source = unsafe { source.assume_init() };
    assert_ne!(source.native_value, 0);
    assert_ne!(source.owner, 0);
    assert!(matches!(source.kind, RM_WAIT_POSIX_FD | RM_WAIT_WIN_EVENT));
    assert_eq!(api::handle::rm_handle_release(source.owner), RM_OK);
    assert_eq!(api::handle::rm_handle_release(waiter), RM_OK);
    let _ = api::handle::rm_handle_release(cq);
    assert_eq!(api::endpoint::rm_endpoint_close(endpoint), RM_OK);
    let _ = api::handle::rm_handle_release(endpoint);
}

#[test]
fn concurrent_token_clone_and_release_are_race_free() {
    let budget = Budget::new(1 << 20, None);
    let cq = Cq::new(budget, 8).unwrap();
    let base = handle::insert(Object::Cq(cq)).unwrap();
    assert!(handle::live_handles() >= 1);
    let threads: Vec<_> = (0..8)
        .map(|_| {
            thread::spawn(move || {
                for _ in 0..64 {
                    let token = handle::clone_token(base).unwrap();
                    assert!(handle::pin(token).is_ok());
                    assert_eq!(handle::release(token), RM_OK);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(handle::live_handles() >= 1);
    assert_eq!(handle::release(base), RM_OK);
}

/// Every frozen symbol must resolve on this target. CI runs this on Windows,
/// Linux and macOS, so a `cfg`-removed export fails the build/test everywhere.
#[test]
fn every_exported_symbol_exists_on_this_target() {
    let exports: &[usize] = &[
        api::handle::rm_abi_version as *const () as usize,
        api::handle::rm_endpoint_config_init as *const () as usize,
        api::handle::rm_handle_clone as *const () as usize,
        api::handle::rm_handle_release as *const () as usize,
        api::handle::rm_handle_release_batch as *const () as usize,
        api::endpoint::rm_endpoint_bind as *const () as usize,
        api::endpoint::rm_endpoint_local_address as *const () as usize,
        api::endpoint::rm_endpoint_default_cq as *const () as usize,
        api::endpoint::rm_cq_create as *const () as usize,
        api::endpoint::rm_session_open as *const () as usize,
        api::endpoint::rm_session_close as *const () as usize,
        api::endpoint::rm_channel_open as *const () as usize,
        api::endpoint::rm_channel_close as *const () as usize,
        api::send::rm_send_batch as *const () as usize,
        api::send::rm_send_copy as *const () as usize,
        api::send::rm_send_borrowed as *const () as usize,
        api::send::rm_tx_builder_create as *const () as usize,
        api::send::rm_tx_builder_write as *const () as usize,
        api::send::rm_tx_builder_writev as *const () as usize,
        api::send::rm_tx_builder_seal as *const () as usize,
        api::send::rm_tx_builder_submit as *const () as usize,
        api::send::rm_ticket_query as *const () as usize,
        api::send::rm_ticket_cancel as *const () as usize,
        api::send::rm_message_view as *const () as usize,
        api::poll::rm_cq_poll_batch as *const () as usize,
        api::poll::rm_endpoint_poll_batch as *const () as usize,
        api::endpoint::rm_endpoint_wait_source as *const () as usize,
        api::endpoint::rm_endpoint_arm as *const () as usize,
        api::poll::rm_waiter_create as *const () as usize,
        api::poll::rm_waiter_arm as *const () as usize,
        api::poll::rm_waiter_wait as *const () as usize,
        api::poll::rm_waiter_cancel as *const () as usize,
        api::poll::rm_waiter_reset as *const () as usize,
        api::poll::rm_waiter_export as *const () as usize,
        api::poll::rm_waiter_bind_iocp as *const () as usize,
        api::poll::rm_waiter_bind_kqueue as *const () as usize,
        api::poll::rm_waiter_unbind as *const () as usize,
        api::endpoint::rm_endpoint_close as *const () as usize,
        api::endpoint::rm_endpoint_metrics_copy as *const () as usize,
        super::rm_metrics_layout as *const () as usize,
    ];
    assert_eq!(exports.len(), EXPORTED_FUNCTIONS.len());
    for export in exports {
        assert_ne!(*export, 0);
    }
}
