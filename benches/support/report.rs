//! Versioned JSON report for the matched L2/L3 diagnostic.
//!
//! `schema` is part of the on-disk contract; changing field meaning requires a
//! schema bump. Counters are deltas across one measured round.

use rimuru::MetricsSnapshot;
use serde::Serialize;

/// On-disk report schema for this diagnostic.
pub const SCHEMA: u32 = 1;

#[derive(Serialize)]
pub struct Counters {
    pub datagrams: u64,
    pub wire_bytes: u64,
    pub syscalls: u64,
    pub received_datagrams: u64,
    pub received_wire_bytes: u64,
    pub receive_syscalls: u64,
    pub io_errors: u64,
    pub resource_drops: u64,
    pub orphan_drops: u64,
    pub controls: u64,
    pub segment_batches: u64,
    pub control_bytes: u64,
    pub retransmitted_bytes: u64,
    pub retransmissions: u64,
    pub rack_losses: u64,
    pub recovery_rounds: u64,
    pub tail_probes: u64,
    pub completed: u64,
    pub aborted: u64,
    pub unknown: u64,
    pub crc_failures: u64,
    pub memory_peak: usize,
    pub ordered_ready_bytes: u64,
    pub ordered_delivered_bytes: u64,
    pub ready_buffered_peak: u64,
    pub hol_ns_total: u64,
    pub hol_ns_max: u64,
}

impl Counters {
    pub fn delta(before: &MetricsSnapshot, after: &MetricsSnapshot) -> Self {
        Self {
            datagrams: after.sent_datagrams - before.sent_datagrams,
            wire_bytes: after.sent_wire_bytes - before.sent_wire_bytes,
            syscalls: after.send_syscalls - before.send_syscalls,
            received_datagrams: after.received_datagrams - before.received_datagrams,
            received_wire_bytes: after.received_wire_bytes - before.received_wire_bytes,
            receive_syscalls: after.receive_syscalls - before.receive_syscalls,
            io_errors: after.io_errors - before.io_errors,
            resource_drops: after.resource_drops - before.resource_drops,
            orphan_drops: after.orphan_drops - before.orphan_drops,
            controls: after.reliable_control_datagrams - before.reliable_control_datagrams,
            segment_batches: after.reliable_segment_batches - before.reliable_segment_batches,
            control_bytes: after.reliable_control_bytes - before.reliable_control_bytes,
            retransmitted_bytes: after.reliable_retransmitted_bytes
                - before.reliable_retransmitted_bytes,
            retransmissions: after.reliable_retransmits - before.reliable_retransmits,
            rack_losses: after.reliable_rack_losses - before.reliable_rack_losses,
            recovery_rounds: after.reliable_recovery_rounds - before.reliable_recovery_rounds,
            tail_probes: after.reliable_tail_probes - before.reliable_tail_probes,
            completed: after.reliable_completed - before.reliable_completed,
            aborted: after.reliable_aborted - before.reliable_aborted,
            unknown: after.reliable_unknown - before.reliable_unknown,
            crc_failures: after.integrity_failures - before.integrity_failures,
            memory_peak: after.memory_peak,
            ordered_ready_bytes: after.ordered_ready_bytes - before.ordered_ready_bytes,
            ordered_delivered_bytes: after.ordered_delivered_bytes - before.ordered_delivered_bytes,
            ready_buffered_peak: after.ordered_reorder_peak,
            hol_ns_total: after.ordered_hol_ns_total - before.ordered_hol_ns_total,
            hol_ns_max: after.ordered_hol_ns_max,
        }
    }
}

#[derive(Serialize)]
pub struct Round {
    pub seconds: f64,
    pub unique_application_bytes: u64,
    pub gb_per_second: f64,
    pub windows: usize,
    pub window_p50_ms: f64,
    pub window_p99_ms: f64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub timeline: Vec<TimeBucket>,
    #[serde(skip_serializing_if = "is_false")]
    pub timeline_truncated: bool,
    pub tx: Counters,
    pub rx: Counters,
    pub error: Option<String>,
}

fn is_false(value: &bool) -> bool {
    !value
}

#[derive(Serialize)]
pub struct TimeBucket {
    pub seconds: f64,
    pub unique_application_bytes: u64,
    pub windows: usize,
    pub complete_second: bool,
}

#[derive(Serialize)]
pub struct Cell {
    pub mode: String,
    pub d: usize,
    pub message: usize,
    pub processing_threads: usize,
    pub window: usize,
    pub batch_size: usize,
    pub latency_sample_limit: usize,
    pub wait_timeout_ms: u64,
    pub channels: usize,
    pub hold_first_ms: u64,
    pub loss_ppm: u32,
    pub rtt_ms: u64,
    pub rounds: Vec<Round>,
    pub relay: Option<RelayCounters>,
}

#[derive(Clone, Serialize)]
pub struct RelayCounters {
    pub offered: u64,
    pub injected_drops: [u64; 2],
    pub forwarded: u64,
    pub duplicated: u64,
    pub queue_drops: u64,
    pub peak_queue_bytes: u64,
    pub forwarded_wire_bytes: u64,
}

#[derive(Serialize)]
pub struct Report {
    pub schema: u32,
    pub platform: String,
    pub clock: String,
    pub semantics: String,
    pub checksum: String,
    pub verify_payload: bool,
    pub payload_variants: usize,
    pub cells: Vec<Cell>,
}

/// Nearest-rank percentile over a mutable sample slice.
pub fn percentile(values: &mut [f64], quantile: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    if values.is_empty() {
        0.0
    } else {
        values[((values.len() - 1) as f64 * quantile).ceil() as usize]
    }
}
