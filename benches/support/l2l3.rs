//! Matched-window L1/L2 or L2/L3 diagnostic with optional external fault relay.
//!
//! This is a saturation diagnostic, not a Criterion statistical benchmark: it runs
//! bounded windows for a wall-clock duration and archives JSON with per-round,
//! per-second and per-counter evidence. Run it through one of the `*_goodput`
//! benchmark targets.

use super::{
    fixture::Fixture,
    matrix,
    relay::{Faults, Relay},
    report::{self, Cell, Counters, RelayCounters, Report, Round, TimeBucket, percentile},
};
use clap::Parser;
use rimuru::benchmarking::Environment;
use std::{
    error::Error as StdError,
    path::PathBuf,
    sync::{Arc, atomic::Ordering::Relaxed},
    time::{Duration, Instant},
};

type BenchResult<T> = Result<T, Box<dyn StdError>>;

const DEFAULT_DATAGRAMS: &str = "1200,4096,16384,65507";
const DEFAULT_MESSAGES: &str = "65536,1048576";
const DEFAULT_THREADS: &str = "1,4";
const MAX_TIMELINE_BUCKETS: usize = 1024;
const DEFAULT_SEED: u64 = 0x5249_4d55_5255;

#[derive(Parser, Debug)]
#[command(about = "Matched L1/L2 or L2/L3 whole-message windows on one local connection")]
struct Args {
    #[arg(long, hide = true)]
    bench: bool,
    #[arg(long)]
    run: bool,
    #[arg(long, default_value_t = 1)]
    channels: usize,
    #[arg(long, default_value_t = 0)]
    hold_first_ms: u64,
    #[arg(long, value_delimiter = ',', default_value = DEFAULT_DATAGRAMS)]
    datagrams: Vec<usize>,
    #[arg(long, alias = "frames", value_delimiter = ',', default_value = DEFAULT_MESSAGES)]
    messages: Vec<usize>,
    /// `T` is one RX dispatcher plus `T-1` Rayon workers.
    #[arg(long, alias = "processing-threads", value_delimiter = ',', default_value = DEFAULT_THREADS)]
    threads: Vec<usize>,
    #[arg(long, default_value_t = matrix::WINDOW)]
    window: usize,
    #[arg(long, alias = "batch-size", default_value_t = 32)]
    batch: usize,
    #[arg(long, default_value_t = 3)]
    rounds: usize,
    #[arg(long, default_value_t = 300)]
    duration_ms: u64,
    #[arg(long, default_value_t = 100)]
    warmup_ms: u64,
    /// Per receive or ticket wait, not a protocol timeout.
    #[arg(long, default_value_t = 10_000)]
    wait_timeout_ms: u64,
    /// Bound exact per-window latency samples; exhausting this limit fails the round.
    #[arg(long, default_value_t = 4_194_304)]
    max_windows: usize,
    #[arg(long, default_value_t = 0)]
    loss_ppm: u32,
    #[arg(long, default_value_t = 0)]
    rtt_ms: u64,
    #[arg(long, default_value_t = DEFAULT_SEED)]
    seed: u64,
    #[arg(long)]
    output: Option<PathBuf>,
    /// Record completed one-second subwindows (at most 1024 per round).
    #[arg(long)]
    timeline: bool,
    /// Compare every delivered payload with the submitted bytes inside the timed window.
    #[arg(long)]
    verify_payload: bool,
}

impl Args {
    fn validate(&self) -> BenchResult<()> {
        let bounded = (1..=16).contains(&self.window)
            && (1..=100).contains(&self.rounds)
            && self.duration_ms > 0
            && (1..=60_000).contains(&self.wait_timeout_ms)
            && (1..=4_194_304).contains(&self.max_windows)
            && (1..=matrix::MAX_BATCH).contains(&self.batch)
            && (1..=16).contains(&self.channels)
            && self.loss_ppm <= 200_000
            && self.rtt_ms <= 500
            && !self.datagrams.is_empty()
            && self.datagrams.iter().all(|d| (128..=65507).contains(d))
            && !self.messages.is_empty()
            && self.messages.iter().all(|&m| m > 0 && m <= 16 << 20)
            && (!self.verify_payload || self.messages.iter().all(|&m| m * self.window <= 64 << 20))
            && !self.threads.is_empty()
            && self.threads.iter().all(|&t| (1..=32).contains(&t));
        if !bounded {
            return Err("invalid benchmark dimensions/limits".into());
        }
        Ok(())
    }

    fn injected(&self) -> bool {
        self.loss_ppm != 0 || self.rtt_ms != 0 || self.hold_first_ms != 0
    }
}

struct Measurement {
    seconds: f64,
    bytes: u64,
    window_times_ms: Vec<f64>,
    error: Option<String>,
    timeline: Vec<TimeBucket>,
    timeline_truncated: bool,
}

async fn measure(fixture: &Fixture, data: &[u8], layer: u8, args: &Args) -> Measurement {
    let start = Instant::now();
    let mut bytes = 0;
    let mut window_times_ms = Vec::new();
    let mut error = None;
    let mut timeline = Vec::new();
    let mut timeline_truncated = false;
    let mut bucket_start = start;
    let mut bucket_bytes = 0u64;
    let mut bucket_windows = 0usize;
    while start.elapsed() < Duration::from_millis(args.duration_ms) {
        if window_times_ms.len() == args.max_windows {
            error = Some("latency sample limit reached before duration".to_owned());
            break;
        }
        let at = Instant::now();
        let result = if layer == 3 {
            fixture.ordered_window(data, false).await
        } else {
            fixture.window(data, layer >= 2, false).await
        };
        match result {
            Ok(count) => {
                bytes += count as u64;
                let end = Instant::now();
                window_times_ms.push(end.duration_since(at).as_secs_f64() * 1000.0);
                if args.timeline {
                    bucket_bytes += count as u64;
                    bucket_windows += 1;
                    let duration = end.duration_since(bucket_start);
                    if duration >= Duration::from_secs(1) {
                        if timeline.len() < MAX_TIMELINE_BUCKETS {
                            timeline.push(TimeBucket {
                                seconds: duration.as_secs_f64(),
                                unique_application_bytes: bucket_bytes,
                                windows: bucket_windows,
                                complete_second: true,
                            });
                        } else {
                            timeline_truncated = true;
                        }
                        bucket_start = end;
                        bucket_bytes = 0;
                        bucket_windows = 0;
                    }
                }
            }
            Err(error_value) => {
                error = Some(error_value.to_string());
                break;
            }
        }
    }
    let end = Instant::now();
    if args.timeline && bucket_windows > 0 {
        if timeline.len() < MAX_TIMELINE_BUCKETS {
            timeline.push(TimeBucket {
                seconds: end.duration_since(bucket_start).as_secs_f64(),
                unique_application_bytes: bucket_bytes,
                windows: bucket_windows,
                complete_second: false,
            });
        } else {
            timeline_truncated = true;
        }
    }
    Measurement {
        seconds: end.duration_since(start).as_secs_f64(),
        bytes,
        window_times_ms,
        error,
        timeline,
        timeline_truncated,
    }
}

fn relay_counters(relay: &Relay) -> RelayCounters {
    let stats = &relay.stats;
    RelayCounters {
        offered: stats.offered.load(Relaxed),
        injected_drops: [
            stats.injected_drops[0].load(Relaxed),
            stats.injected_drops[1].load(Relaxed),
        ],
        forwarded: stats.forwarded.load(Relaxed),
        duplicated: stats.duplicated.load(Relaxed),
        queue_drops: stats.queue_drops.load(Relaxed),
        peak_queue_bytes: stats.peak_queue_bytes.load(Relaxed),
        forwarded_wire_bytes: stats.wire_bytes.load(Relaxed),
    }
}

/// Install the external relay between the fixture endpoints when fault injection
/// is requested. The baseline fixture stays direct for a matched port/socket setup.
fn install_relay(env: &Environment, fixture: &mut Fixture, args: &Args) -> BenchResult<Relay> {
    let relay = Relay::new(
        env,
        fixture.tx.local_addr()?,
        fixture.rx.local_addr()?,
        Faults {
            seed: args.seed,
            loss_ppm: args.loss_ppm,
            delay: Duration::from_micros(args.rtt_ms * 500),
            hold_first_frame: Duration::from_millis(args.hold_first_ms),
            ..Faults::default()
        },
    )?;
    fixture.tx.disconnect(fixture.rx.local_addr()?)?;
    fixture.session = fixture.tx.connect(relay.address)?;
    Ok(relay)
}

pub fn main(ordered: bool) -> ! {
    if std::env::args().any(|arg| arg == "--test") {
        std::process::exit(0);
    }
    let args = Args::parse_from(std::env::args_os().filter(|arg| arg != "--bench"));
    if !args.run {
        if ordered {
            println!(
                "Pass --run to measure matched L2/L3 windows; Criterion: cargo bench --bench l3"
            );
        } else {
            println!(
                "Pass --run to measure matched L1/L2 windows; Criterion: cargo bench --bench l2"
            );
        }
        std::process::exit(0);
    }
    match run(&args, ordered) {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("benchmark failed: {error}");
            std::process::exit(1);
        }
    }
}

fn run(args: &Args, ordered: bool) -> BenchResult<()> {
    args.validate()?;
    let output = args.output.clone().unwrap_or_else(|| {
        PathBuf::from(if ordered {
            "benchmark-results/l3-goodput.json"
        } else {
            "benchmark-results/l2-goodput.json"
        })
    });
    let baseline_layer = if ordered { 2 } else { 1 };
    let injected = args.injected();
    let mut cells = Vec::new();
    for &threads in &args.threads {
        let env = Environment::new(threads)?;
        for &datagram in &args.datagrams {
            for &message in &args.messages {
                let data: Vec<u8> = if args.verify_payload {
                    (0..message)
                        .map(|i| ((i.wrapping_mul(131) ^ (i >> 8)) & 255) as u8)
                        .collect()
                } else {
                    vec![0x5a; message]
                };
                let mut baseline = Fixture::new_with_batch(&env, datagram, args.window, args.batch);
                let mut subject = Fixture::new_with_batch(&env, datagram, args.window, args.batch);
                if args.verify_payload {
                    let payloads = Arc::new(
                        (0..args.window)
                            .map(|index| {
                                data.iter()
                                    .map(|byte| byte.wrapping_add(index as u8))
                                    .collect()
                            })
                            .collect(),
                    );
                    baseline.set_verify_payloads(Arc::clone(&payloads));
                    subject.set_verify_payloads(payloads);
                }
                baseline.set_wait_timeout(Duration::from_millis(args.wait_timeout_ms));
                subject.set_wait_timeout(Duration::from_millis(args.wait_timeout_ms));
                let mut relays = [None, None];
                for (index, slot) in relays.iter_mut().enumerate() {
                    if !injected || (!ordered && index == 0) {
                        continue;
                    }
                    let fixture = if index == 0 {
                        &mut baseline
                    } else {
                        &mut subject
                    };
                    *slot = Some(install_relay(&env, fixture, args)?);
                }
                if ordered {
                    subject.enable_ordered(args.channels)?;
                }
                // Warm up both fixtures with the same window shape as the measurement.
                for index in 0..2 {
                    if injected && !ordered && index == 0 {
                        continue;
                    }
                    let fixture = if index == 0 { &baseline } else { &subject };
                    let layer = baseline_layer + index as u8;
                    env.block_on(async {
                        let start = Instant::now();
                        while start.elapsed() < Duration::from_millis(args.warmup_ms) {
                            if layer == 3 {
                                fixture.ordered_window(&data, false).await.expect("warmup");
                            } else {
                                fixture
                                    .window(&data, layer >= 2, false)
                                    .await
                                    .expect("warmup");
                            }
                        }
                    })
                    .unwrap();
                }
                let mut results = [Vec::new(), Vec::new()];
                for round_index in 0..args.rounds {
                    let order = if round_index % 2 == 0 { [0, 1] } else { [1, 0] };
                    for index in order {
                        if injected && !ordered && index == 0 {
                            continue;
                        }
                        let fixture = if index == 0 { &baseline } else { &subject };
                        let layer = baseline_layer + index as u8;
                        let before_tx = fixture.tx.metrics();
                        let before_rx = fixture.rx.metrics();
                        let measured = env.block_on(measure(fixture, &data, layer, args)).unwrap();
                        let gb_per_second = if measured.seconds > 0.0 {
                            measured.bytes as f64 / measured.seconds / 1e9
                        } else {
                            0.0
                        };
                        let result = Round {
                            seconds: measured.seconds,
                            unique_application_bytes: measured.bytes,
                            gb_per_second,
                            windows: measured.window_times_ms.len(),
                            window_p50_ms: percentile(&mut measured.window_times_ms.clone(), 0.5),
                            window_p99_ms: percentile(&mut measured.window_times_ms.clone(), 0.99),
                            timeline: measured.timeline,
                            timeline_truncated: measured.timeline_truncated,
                            tx: Counters::delta(&before_tx, &fixture.tx.metrics()),
                            rx: Counters::delta(&before_rx, &fixture.rx.metrics()),
                            error: measured.error,
                        };
                        println!(
                            "t={threads} d={datagram} m={message} l{layer} round={} {:.3} GB/s {} windows{}",
                            round_index + 1,
                            gb_per_second,
                            result.windows,
                            if result.error.is_some() { " ERROR" } else { "" }
                        );
                        results[index].push(result);
                    }
                }
                let relay_reports = relays
                    .iter()
                    .map(|relay| {
                        relay.as_ref().map(|relay| {
                            assert!(!relay.blackhole.load(Relaxed));
                            relay_counters(relay)
                        })
                    })
                    .collect::<Vec<_>>();
                for (index, rounds) in results.into_iter().enumerate() {
                    if rounds.is_empty() {
                        continue;
                    }
                    cells.push(Cell {
                        mode: format!("l{}_slice", baseline_layer as usize + index),
                        d: datagram,
                        message,
                        processing_threads: threads,
                        window: args.window,
                        batch_size: args.batch,
                        latency_sample_limit: args.max_windows,
                        wait_timeout_ms: args.wait_timeout_ms,
                        channels: if ordered && index == 1 {
                            args.channels
                        } else {
                            1
                        },
                        hold_first_ms: args.hold_first_ms,
                        loss_ppm: if ordered || index == 1 {
                            args.loss_ppm
                        } else {
                            0
                        },
                        rtt_ms: if ordered || index == 1 {
                            args.rtt_ms
                        } else {
                            0
                        },
                        rounds,
                        relay: relay_reports[index].clone(),
                    });
                }
            }
        }
    }
    let report = Report {
        schema: report::SCHEMA,
        platform: format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
        clock: "std::time::Instant".into(),
        semantics: "Unique Whole bytes; bounded pipeline; reliable tickets wait for remote publication. L3 checks per-channel order and includes head-of-line drain time. Bidirectional external fault relay; no matched raw UDP baseline.".into(),
        checksum: "CRC32C (default)".into(),
        verify_payload: args.verify_payload,
        payload_variants: if args.verify_payload { args.window } else { 1 },
        cells,
    };
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&output, serde_json::to_vec_pretty(&report)?)?;
    if report
        .cells
        .iter()
        .any(|cell| cell.rounds.iter().any(|round| round.error.is_some()))
    {
        return Err("one or more rounds failed (report preserved)".into());
    }
    Ok(())
}
