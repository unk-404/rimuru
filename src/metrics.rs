use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        #[derive(Default)] pub(crate) struct Metrics { $(pub $name: AtomicU64,)* }
        #[derive(Clone, Debug, Default)] pub struct MetricsSnapshot {
            $(pub $name: u64,)* pub memory_used: usize, pub memory_peak: usize,
            pub pool_cached: usize, pub socket_recv_buffer: usize,
            pub socket_send_buffer: usize, pub bound_udp_sockets: usize,
        }
        impl Metrics { pub fn snapshot(&self) -> MetricsSnapshot { MetricsSnapshot { $($name: self.$name.load(Relaxed),)* ..MetricsSnapshot::default() } } }
    }
}
counters!(
    sent_datagrams,
    sent_wire_bytes,
    received_datagrams,
    received_wire_bytes,
    sent_frames,
    sent_application_bytes,
    received_frames,
    received_application_bytes,
    malformed_datagrams,
    integrity_failures,
    duplicate_segments,
    expired_frames,
    resource_drops,
    unknown_sessions,
    orphan_drops,
    io_errors,
    crc_parallel_jobs,
    send_syscalls,
    receive_syscalls,
    reliable_submitted,
    ordered_submitted,
    ordered_ready_frames,
    ordered_ready_bytes,
    ordered_delivered_frames,
    ordered_delivered_bytes,
    ordered_buffered_frames,
    ordered_buffered_bytes,
    ordered_reorder_peak,
    ordered_hol_ns_total,
    ordered_hol_ns_max,
    ordered_channels_closed,
    reliable_completed,
    reliable_aborted,
    reliable_unknown,
    reliable_retransmits,
    reliable_recovery_rounds,
    reliable_control_datagrams,
    reliable_control_bytes,
    reliable_data_datagrams,
    reliable_segment_batches,
    reliable_retransmitted_bytes,
    reliable_grants,
    reliable_credit_stalls,
    reliable_rack_losses,
    reliable_nacks,
    reliable_tail_probes
);
