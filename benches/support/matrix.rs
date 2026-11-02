//! The single source of truth for benchmark dimensions and Criterion policy.
//!
//! Changing a matrix value here changes every statistical benchmark. Diagnostic
//! binaries keep their own explicit CLI defaults but validate against these bounds.

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};
use std::{net::SocketAddr, time::Duration};

/// Segment payload sizes (UDP payload limit `D`) covering portable, common MTU,
/// jumbo-ish and the IPv4 loopback boundary. See DOCS 11.5.
pub const DATAGRAM_SIZES: &[usize] = &[1200, 4096, 16384, 65507];
/// Whole-frame/message sizes for L1 Criterion.
pub const FRAME_SIZES: &[usize] = &[65_536, 1_048_576];
/// Message sizes for the L2/L3 matched-window Criterion.
pub const MESSAGE_SIZES: &[usize] = &[1024, 65_536, 1_048_576];
/// `T` is one RX dispatcher plus `T-1` Rayon checksum workers.
pub const PROCESSING_THREADS: &[usize] = &[1, 4];
/// Ordered channels per L3 benchmark group.
pub const CHANNEL_COUNTS: &[usize] = &[1, 4];
/// In-flight window used by the matched L2/L3 fixture.
pub const WINDOW: usize = 4;
/// Largest accepted batch/window/poll parameter.
pub const MAX_BATCH: usize = 256;
/// Criterion: samples per benchmark.
pub const SAMPLE_SIZE: usize = 20;
/// Criterion: warm-up time per benchmark.
pub const WARM_UP: Duration = Duration::from_millis(500);
/// Criterion: measurement time per benchmark.
pub const MEASUREMENT: Duration = Duration::from_secs(2);
/// Isolated CPU-cost profile: more samples, shorter measurement.
pub const COST_SAMPLE_SIZE: usize = 30;
pub const COST_WARM_UP: Duration = Duration::from_millis(300);
pub const COST_MEASUREMENT: Duration = Duration::from_secs(1);
/// Default loopback address; every benchmark uses numeric addresses (DOCS 11.5).
pub fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().expect("literal loopback address")
}
/// Create a Criterion group with the shared statistical policy.
pub fn group<'a>(criterion: &'a mut Criterion, name: String) -> BenchmarkGroup<'a, WallTime> {
    let mut group = criterion.benchmark_group(name);
    group
        .sample_size(SAMPLE_SIZE)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);
    group
}
/// Apply the shared policy to an existing group (cost-oriented profile).
pub fn configure_costs<M: criterion::measurement::Measurement>(group: &mut BenchmarkGroup<'_, M>) {
    group
        .sample_size(COST_SAMPLE_SIZE)
        .warm_up_time(COST_WARM_UP)
        .measurement_time(COST_MEASUREMENT);
}
/// Group names keep `level/topology/threads/datagram/...` in that order.
pub fn group_name(level: &str, threads: usize, datagram: usize) -> String {
    format!("{level}/t{threads}/d{datagram}")
}
