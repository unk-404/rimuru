//! Versioned JSON report for the saturation diagnostic.
//!
//! Throughput is reported in both GB/s (decimal) and Gbit/s; 1 GB/s = 8 Gbit/s.
//! Raw UDP and L1 rows are paired runs, never concurrent ones.

use serde::Serialize;

/// On-disk report schema for this diagnostic.
pub const SCHEMA: u32 = 2;

#[derive(Clone, Default, Serialize)]
pub struct Sample {
    pub tx_crc_parallel_jobs: u64,
    pub rx_crc_parallel_jobs: u64,
    pub goodput_gbps: f64,
    pub goodput_gbytes_per_second: f64,
    pub elapsed_seconds: f64,
    pub cpu_seconds: Option<f64>,
    pub sent_datagrams: u64,
    pub received_datagrams: u64,
    pub sent_application_bytes: u64,
    pub received_application_bytes: u64,
    pub sent_frames: u64,
    pub received_frames: u64,
    pub sent_wire_bytes: u64,
    pub send_syscalls: u64,
    pub receive_syscalls: u64,
    pub malformed: u64,
    pub integrity_failures: u64,
    pub resource_drops: u64,
    pub orphan_drops: u64,
    pub expired_frames: u64,
    pub memory_peak_bytes: usize,
    pub socket_recv_buffer: usize,
    pub socket_send_buffer: usize,
}

#[derive(Default, Serialize)]
pub struct Latency {
    pub samples: usize,
    pub timed_out: usize,
    pub p50_us: Option<f64>,
    pub p99_us: Option<f64>,
    pub p999_us: Option<f64>,
}

impl Latency {
    pub fn from_samples(mut samples: Vec<f64>, timed_out: usize) -> Self {
        samples.sort_by(f64::total_cmp);
        let quantile = |p: f64| {
            samples
                .get(((samples.len() as f64 * p).ceil() as usize).saturating_sub(1))
                .copied()
        };
        Self {
            samples: samples.len(),
            timed_out,
            p50_us: quantile(0.5),
            p99_us: quantile(0.99),
            p999_us: quantile(0.999),
        }
    }
}

#[derive(Serialize)]
pub struct Cell {
    pub datagram_size: usize,
    pub frame_size: usize,
    pub processing_threads: usize,
    pub io_threads: usize,
    pub rx_dispatchers: usize,
    pub rayon_threads: usize,
    pub raw: Vec<Sample>,
    pub l1: Vec<Sample>,
    pub median_raw_gbps: f64,
    pub median_l1_gbps: f64,
    pub median_raw_gbytes_per_second: f64,
    pub median_l1_gbytes_per_second: f64,
    pub ratio: f64,
    pub raw_relative_mad: f64,
    pub l1_relative_mad: f64,
    pub lossless: bool,
    pub meets_90_percent: bool,
    pub latency: Latency,
}

#[derive(Serialize)]
pub struct Report {
    pub schema: u32,
    pub os: &'static str,
    pub arch: &'static str,
    pub rustc: String,
    pub available_parallelism: usize,
    pub integrity: String,
    pub offered_mbps: f64,
    pub duration_ms: u64,
    pub warmup_ms: u64,
    pub drain_ms: u64,
    pub batch: usize,
    pub socket_buffer_requested: usize,
    pub notes: Vec<String>,
    pub cells: Vec<Cell>,
}

pub fn median(values: impl Iterator<Item = f64>) -> f64 {
    let mut values: Vec<_> = values.collect();
    values.sort_by(f64::total_cmp);
    if values.len() % 2 == 0 {
        (values[values.len() / 2 - 1] + values[values.len() / 2]) / 2.0
    } else {
        values[values.len() / 2]
    }
}

pub fn relative_mad(samples: &[Sample], median: f64) -> f64 {
    if median == 0.0 {
        return 0.0;
    }
    median_of(samples.iter().map(|s| (s.goodput_gbps - median).abs())) / median
}

fn median_of(values: impl Iterator<Item = f64>) -> f64 {
    median(values)
}
