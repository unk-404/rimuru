//! Frozen C ABI 1.0 types, status codes and the exported-function manifest.
//!
//! Every `rm_*` symbol in [`EXPORTED_FUNCTIONS`] is compiled and exported on every
//! supported target. Platform differences only change returned status codes, never
//! the export surface or a struct layout.

#![allow(dead_code)]

/// Pointer-width opaque token: 64-bit targets use 32+32 bits, 32-bit targets 16+16.
pub type RmHandle = usize;

/// Status codes (DOCS 10.5). Positive values are wait/poll outcomes.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum RmStatus {
    RM_OK = 0,
    RM_WOULD_BLOCK = 1,
    RM_TIMED_OUT = 2,
    RM_READY = 3,
    RM_ARMED = 4,
    RM_INVALID_ARGUMENT = -1,
    RM_INVALID_HANDLE = -2,
    RM_VERSION_MISMATCH = -3,
    RM_MESSAGE_TOO_BIG = -4,
    RM_NO_BUFFER = -5,
    RM_ABORTED = -6,
    RM_CLOSED = -7,
    RM_OUTCOME_UNKNOWN = -8,
    RM_INTERNAL = -9,
    RM_UNSUPPORTED = -10,
    RM_INVALID_STATE = -11,
    RM_WOULD_DEADLOCK = -12,
    RM_ALREADY_COMPLETED = -13,
    RM_IO = -14,
}

impl RmStatus {
    pub(crate) fn from_raw(value: i32) -> Self {
        match value {
            0 => Self::RM_OK,
            1 => Self::RM_WOULD_BLOCK,
            2 => Self::RM_TIMED_OUT,
            3 => Self::RM_READY,
            4 => Self::RM_ARMED,
            -1 => Self::RM_INVALID_ARGUMENT,
            -2 => Self::RM_INVALID_HANDLE,
            -3 => Self::RM_VERSION_MISMATCH,
            -4 => Self::RM_MESSAGE_TOO_BIG,
            -5 => Self::RM_NO_BUFFER,
            -6 => Self::RM_ABORTED,
            -7 => Self::RM_CLOSED,
            -8 => Self::RM_OUTCOME_UNKNOWN,
            -9 => Self::RM_INTERNAL,
            -10 => Self::RM_UNSUPPORTED,
            -11 => Self::RM_INVALID_STATE,
            -12 => Self::RM_WOULD_DEADLOCK,
            -13 => Self::RM_ALREADY_COMPLETED,
            _ => Self::RM_IO,
        }
    }
}

pub const RM_ABI_MAJOR: u32 = 1;
pub const RM_ABI_MINOR: u32 = 0;
pub const RM_ABI_VERSION: u32 = (RM_ABI_MAJOR << 16) | RM_ABI_MINOR;

/// Completion event kind.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum RmEventKind {
    RM_EVENT_MESSAGE = 1,
    RM_EVENT_SEND_TERMINAL = 2,
    RM_EVENT_WRITABLE = 3,
    RM_EVENT_SESSION_READY = 4,
    RM_EVENT_SESSION_CLOSED = 5,
    RM_EVENT_ENDPOINT_CLOSED = 6,
    RM_EVENT_RESERVED = 7,
    RM_EVENT_NOTIFY_FAILED = 8,
    RM_EVENT_CHANNEL_CLOSED = 9,
}

impl RmEventKind {
    pub(crate) fn from_raw(value: u32) -> Self {
        match value {
            1 => Self::RM_EVENT_MESSAGE,
            2 => Self::RM_EVENT_SEND_TERMINAL,
            3 => Self::RM_EVENT_WRITABLE,
            4 => Self::RM_EVENT_SESSION_READY,
            5 => Self::RM_EVENT_SESSION_CLOSED,
            6 => Self::RM_EVENT_ENDPOINT_CLOSED,
            8 => Self::RM_EVENT_NOTIFY_FAILED,
            9 => Self::RM_EVENT_CHANNEL_CLOSED,
            _ => Self::RM_EVENT_RESERVED,
        }
    }
}

/// Send-ticket lifecycle state.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum RmTicketState {
    RM_TICKET_QUEUED = 0,
    RM_TICKET_AWAIT_GRANT = 1,
    RM_TICKET_IN_FLIGHT = 2,
    RM_TICKET_RECEIVED = 3,
    RM_TICKET_ABORTED = 4,
    RM_TICKET_UNKNOWN = 5,
}

impl RmTicketState {
    pub(crate) fn from_raw(value: u32) -> Self {
        match value {
            0 => Self::RM_TICKET_QUEUED,
            1 => Self::RM_TICKET_AWAIT_GRANT,
            2 => Self::RM_TICKET_IN_FLIGHT,
            3 => Self::RM_TICKET_RECEIVED,
            4 => Self::RM_TICKET_ABORTED,
            _ => Self::RM_TICKET_UNKNOWN,
        }
    }
}

/// Ticket flag bit: the caller's borrowed source may be reused.
pub const RM_TICKET_FLAG_LOCAL_BUFFER_RELEASED: u32 = 1;

/// Exported OS wait-source kind.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum RmWaitSourceKind {
    RM_WAIT_POSIX_FD = 1,
    RM_WAIT_WIN_EVENT = 2,
}

/// Backend selection.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum RmBackend {
    RM_BACKEND_AUTO = 0,
    RM_BACKEND_PORTABLE = 1,
    RM_BACKEND_NATIVE = 2,
}

pub(crate) const RM_OK: RmStatus = RmStatus::RM_OK;
pub(crate) const RM_WOULD_BLOCK: RmStatus = RmStatus::RM_WOULD_BLOCK;
pub(crate) const RM_TIMED_OUT: RmStatus = RmStatus::RM_TIMED_OUT;
pub(crate) const RM_READY: RmStatus = RmStatus::RM_READY;
pub(crate) const RM_ARMED: RmStatus = RmStatus::RM_ARMED;
pub(crate) const RM_INVALID_ARGUMENT: RmStatus = RmStatus::RM_INVALID_ARGUMENT;
pub(crate) const RM_INVALID_HANDLE: RmStatus = RmStatus::RM_INVALID_HANDLE;
pub(crate) const RM_VERSION_MISMATCH: RmStatus = RmStatus::RM_VERSION_MISMATCH;
pub(crate) const RM_MESSAGE_TOO_BIG: RmStatus = RmStatus::RM_MESSAGE_TOO_BIG;
pub(crate) const RM_NO_BUFFER: RmStatus = RmStatus::RM_NO_BUFFER;
pub(crate) const RM_ABORTED: RmStatus = RmStatus::RM_ABORTED;
pub(crate) const RM_CLOSED: RmStatus = RmStatus::RM_CLOSED;
pub(crate) const RM_OUTCOME_UNKNOWN: RmStatus = RmStatus::RM_OUTCOME_UNKNOWN;
pub(crate) const RM_INTERNAL: RmStatus = RmStatus::RM_INTERNAL;
pub(crate) const RM_UNSUPPORTED: RmStatus = RmStatus::RM_UNSUPPORTED;
pub(crate) const RM_INVALID_STATE: RmStatus = RmStatus::RM_INVALID_STATE;
pub(crate) const RM_WOULD_DEADLOCK: RmStatus = RmStatus::RM_WOULD_DEADLOCK;
pub(crate) const RM_ALREADY_COMPLETED: RmStatus = RmStatus::RM_ALREADY_COMPLETED;
pub(crate) const RM_IO: RmStatus = RmStatus::RM_IO;

pub(crate) const RM_EVENT_MESSAGE: RmEventKind = RmEventKind::RM_EVENT_MESSAGE;
pub(crate) const RM_EVENT_SEND_TERMINAL: RmEventKind = RmEventKind::RM_EVENT_SEND_TERMINAL;
pub(crate) const RM_EVENT_WRITABLE: RmEventKind = RmEventKind::RM_EVENT_WRITABLE;
pub(crate) const RM_EVENT_SESSION_READY: RmEventKind = RmEventKind::RM_EVENT_SESSION_READY;
pub(crate) const RM_EVENT_SESSION_CLOSED: RmEventKind = RmEventKind::RM_EVENT_SESSION_CLOSED;
pub(crate) const RM_EVENT_ENDPOINT_CLOSED: RmEventKind = RmEventKind::RM_EVENT_ENDPOINT_CLOSED;
pub(crate) const RM_EVENT_RESERVED: RmEventKind = RmEventKind::RM_EVENT_RESERVED;
pub(crate) const RM_EVENT_NOTIFY_FAILED: RmEventKind = RmEventKind::RM_EVENT_NOTIFY_FAILED;
pub(crate) const RM_EVENT_CHANNEL_CLOSED: RmEventKind = RmEventKind::RM_EVENT_CHANNEL_CLOSED;

pub(crate) const RM_TICKET_QUEUED: RmTicketState = RmTicketState::RM_TICKET_QUEUED;
pub(crate) const RM_TICKET_AWAIT_GRANT: RmTicketState = RmTicketState::RM_TICKET_AWAIT_GRANT;
pub(crate) const RM_TICKET_IN_FLIGHT: RmTicketState = RmTicketState::RM_TICKET_IN_FLIGHT;
pub(crate) const RM_TICKET_RECEIVED: RmTicketState = RmTicketState::RM_TICKET_RECEIVED;
pub(crate) const RM_TICKET_ABORTED: RmTicketState = RmTicketState::RM_TICKET_ABORTED;
pub(crate) const RM_TICKET_UNKNOWN: RmTicketState = RmTicketState::RM_TICKET_UNKNOWN;

pub(crate) const RM_WAIT_POSIX_FD: RmWaitSourceKind = RmWaitSourceKind::RM_WAIT_POSIX_FD;
pub(crate) const RM_WAIT_WIN_EVENT: RmWaitSourceKind = RmWaitSourceKind::RM_WAIT_WIN_EVENT;
pub(crate) const RM_BACKEND_AUTO: RmBackend = RmBackend::RM_BACKEND_AUTO;
pub(crate) const RM_BACKEND_PORTABLE: RmBackend = RmBackend::RM_BACKEND_PORTABLE;
pub(crate) const RM_BACKEND_NATIVE: RmBackend = RmBackend::RM_BACKEND_NATIVE;

pub const RM_MAX_BATCH_ITEMS: usize = 256;

/// Configuration header. `abi_version` and `struct_size` must be first.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RmEndpointConfig {
    pub abi_version: u32,
    pub struct_size: u32,
    pub memory_budget_bytes: u64,
    pub max_message_bytes: u32,
    pub max_peers: u32,
    pub max_channels: u32,
    pub cq_capacity: u32,
    pub backend: RmBackend,
    pub flags: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RmAddress {
    pub family: u32,
    pub port: u16,
    pub reserved: u16,
    pub scope_id: u32,
    pub address: [u8; 16],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RmEvent {
    pub kind: RmEventKind,
    pub status: RmStatus,
    pub user_data: u64,
    pub ticket_id: u64,
    pub session_id: u64,
    pub delivery_seq: u64,
    pub message: RmHandle,
    pub channel_seq: u32,
    pub channel_id: u16,
    pub layer: u8,
    pub flags: u8,
}

impl RmEvent {
    pub(crate) fn empty(kind: RmEventKind, status: RmStatus) -> Self {
        Self {
            kind,
            status,
            user_data: 0,
            ticket_id: 0,
            session_id: 0,
            delivery_seq: 0,
            message: 0,
            channel_seq: 0,
            channel_id: 0,
            layer: 0,
            flags: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RmSendItem {
    pub peer: RmAddress,
    pub channel: RmHandle,
    pub completion_queue: RmHandle,
    pub data: *const u8,
    pub len: u32,
    pub flags: u32,
    pub user_data: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RmIoSlice {
    pub data: *const u8,
    pub len: u32,
    pub reserved: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RmWaitSource {
    pub kind: RmWaitSourceKind,
    pub flags: u32,
    pub native_value: u64,
    pub owner: RmHandle,
}

impl Default for RmWaitSource {
    fn default() -> Self {
        Self {
            kind: RmWaitSourceKind::RM_WAIT_POSIX_FD,
            flags: 0,
            native_value: 0,
            owner: 0,
        }
    }
}

/// Versioned binary metrics snapshot (schema 1).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RmMetrics {
    pub schema_version: u32,
    pub struct_size: u32,
    pub sent_datagrams: u64,
    pub sent_wire_bytes: u64,
    pub received_datagrams: u64,
    pub received_wire_bytes: u64,
    pub sent_frames: u64,
    pub sent_application_bytes: u64,
    pub received_frames: u64,
    pub received_application_bytes: u64,
    pub malformed_datagrams: u64,
    pub integrity_failures: u64,
    pub duplicate_segments: u64,
    pub expired_frames: u64,
    pub resource_drops: u64,
    pub unknown_sessions: u64,
    pub orphan_drops: u64,
    pub io_errors: u64,
    pub crc_parallel_jobs: u64,
    pub send_syscalls: u64,
    pub receive_syscalls: u64,
    pub reliable_submitted: u64,
    pub ordered_submitted: u64,
    pub ordered_ready_frames: u64,
    pub ordered_ready_bytes: u64,
    pub ordered_delivered_frames: u64,
    pub ordered_delivered_bytes: u64,
    pub ordered_buffered_frames: u64,
    pub ordered_buffered_bytes: u64,
    pub ordered_reorder_peak: u64,
    pub ordered_hol_ns_total: u64,
    pub ordered_hol_ns_max: u64,
    pub ordered_channels_closed: u64,
    pub reliable_completed: u64,
    pub reliable_aborted: u64,
    pub reliable_unknown: u64,
    pub reliable_retransmits: u64,
    pub reliable_recovery_rounds: u64,
    pub reliable_control_datagrams: u64,
    pub reliable_control_bytes: u64,
    pub reliable_data_datagrams: u64,
    pub reliable_segment_batches: u64,
    pub reliable_retransmitted_bytes: u64,
    pub reliable_grants: u64,
    pub reliable_credit_stalls: u64,
    pub reliable_rack_losses: u64,
    pub reliable_nacks: u64,
    pub reliable_tail_probes: u64,
    pub memory_used: u64,
    pub memory_peak: u64,
    pub pool_cached: u64,
    pub socket_recv_buffer: u64,
    pub socket_send_buffer: u64,
    pub bound_udp_sockets: u64,
}

impl RmMetrics {
    pub(crate) const SCHEMA_VERSION: u32 = 1;
}

/// The complete, platform-independent export surface. Tested against the header.
pub const EXPORTED_FUNCTIONS: &[&str] = &[
    "rm_abi_version",
    "rm_endpoint_config_init",
    "rm_handle_clone",
    "rm_handle_release",
    "rm_handle_release_batch",
    "rm_endpoint_bind",
    "rm_endpoint_local_address",
    "rm_endpoint_default_cq",
    "rm_cq_create",
    "rm_session_open",
    "rm_session_close",
    "rm_channel_open",
    "rm_channel_close",
    "rm_send_batch",
    "rm_send_copy",
    "rm_send_borrowed",
    "rm_tx_builder_create",
    "rm_tx_builder_write",
    "rm_tx_builder_writev",
    "rm_tx_builder_seal",
    "rm_tx_builder_submit",
    "rm_ticket_query",
    "rm_ticket_cancel",
    "rm_message_view",
    "rm_cq_poll_batch",
    "rm_endpoint_poll_batch",
    "rm_endpoint_wait_source",
    "rm_endpoint_arm",
    "rm_waiter_create",
    "rm_waiter_arm",
    "rm_waiter_wait",
    "rm_waiter_cancel",
    "rm_waiter_reset",
    "rm_waiter_export",
    "rm_waiter_bind_iocp",
    "rm_waiter_bind_kqueue",
    "rm_waiter_unbind",
    "rm_endpoint_close",
    "rm_endpoint_metrics_copy",
    "rm_metrics_layout",
];

/// Pointer-width-independent guards.
const _: () = {
    assert!(size_of::<RmAddress>() == 28);
    assert!(size_of::<RmStatus>() == 4);
    assert!(size_of::<RmEventKind>() == 4);
    assert!(size_of::<RmTicketState>() == 4);
};

/// Exact 64-bit C layout: the header asserts the enum/handle widths separately.
#[cfg(target_pointer_width = "64")]
const _: () = {
    assert!(size_of::<RmEvent>() == 56);
    assert!(size_of::<RmSendItem>() == 72);
    assert!(size_of::<RmIoSlice>() == 16);
    assert!(size_of::<RmWaitSource>() == 24);
    assert!(size_of::<RmEndpointConfig>() == 40);
};

pub(crate) fn status_from_error(error: crate::Error) -> RmStatus {
    use crate::Error;
    match error {
        Error::Io(ref io) if io.kind() == std::io::ErrorKind::WouldBlock => RM_WOULD_BLOCK,
        Error::Io(_) => RM_IO,
        Error::InvalidConfig(_) => RM_INVALID_ARGUMENT,
        Error::Protocol(_) => RM_INTERNAL,
        Error::Closed => RM_CLOSED,
        Error::TimedOut => RM_TIMED_OUT,
        Error::WouldBlock => RM_WOULD_BLOCK,
        Error::WouldDeadlock => RM_WOULD_DEADLOCK,
        Error::MessageTooBig => RM_MESSAGE_TOO_BIG,
        Error::NoBuffer => RM_NO_BUFFER,
        Error::IdExhausted => RM_NO_BUFFER,
        Error::ReliableUnsupported | Error::OrderedUnsupported => RM_UNSUPPORTED,
        Error::ChannelClosed => RM_INVALID_STATE,
        Error::DeliveryAborted(_) => RM_ABORTED,
        Error::OutcomeUnknown => RM_OUTCOME_UNKNOWN,
    }
}
