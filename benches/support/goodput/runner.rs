use super::{
    args::{Args, Integrity},
    report::{self, Cell, Latency, Report, Sample},
};
use rimuru::{Endpoint, EndpointConfig, benchmarking::Environment};
use std::{
    error::Error,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering::{Acquire, Release},
        },
    },
    time::{Duration, Instant},
};
type BenchResult<T> = Result<T, Box<dyn Error>>;

fn cfg(a: &Args, d: usize) -> EndpointConfig {
    EndpointConfig {
        datagram_size: d,
        max_frame_size: 16 << 20,
        memory_budget: 128 << 20,
        frame_capacity: 256,
        max_peers: 16,
        socket_buffer_bytes: a.socket_buffer,
        integrity: match a.integrity {
            Integrity::Crc32c => rimuru::Integrity::Crc32c,
            Integrity::UdpChecksum => rimuru::Integrity::UdpChecksum,
        },
        batch_size: a.batch,
        ..EndpointConfig::default()
    }
}
async fn pace(start: Instant, bytes: u64, mbps: f64) {
    if mbps > 0.0 {
        let target = start + Duration::from_secs_f64(bytes as f64 * 8.0 / (mbps * 1e6));
        if target > Instant::now() {
            tokio::time::sleep_until(target.into()).await;
        }
    }
}
async fn raw(
    r: &Environment,
    a: &Args,
    d: usize,
    addresses: (SocketAddr, SocketAddr),
    duration: u64,
) -> BenchResult<Sample> {
    let tx = r.bind_socket(addresses.0, a.socket_buffer)?;
    let rx = r.bind_socket(addresses.1, a.socket_buffer)?;
    let stop = Arc::new(AtomicBool::new(false));
    let done = stop.clone();
    let socket = rx.clone();
    let drain = Duration::from_millis(a.drain_ms);
    let bytes = vec![0x5a; d];
    let mut buffer = vec![0; 65536];
    let cpu = cpu_time();
    let start = Instant::now();
    let receive = r.spawn(async move {
        let mut received = 0u64;
        loop {
            match tokio::time::timeout(Duration::from_millis(10), socket.recv_from(&mut buffer))
                .await
            {
                Ok(Ok((n, _))) => {
                    received += n as u64;
                }
                Ok(Err(e)) => return Err(e),
                Err(_) if done.load(Acquire) => break,
                Err(_) => {}
            }
        }
        Ok(received)
    });
    let mut sent = 0u64;
    while start.elapsed() < Duration::from_millis(duration) {
        for _ in 0..a.batch {
            sent += tx.send_to(&bytes, addresses.1).await? as u64;
        }
        pace(start, sent, a.offered_mbps).await;
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(drain).await;
    stop.store(true, Release);
    let received = receive.await??;
    let elapsed = start.elapsed().as_secs_f64();
    let t = tx.metrics();
    let q = rx.metrics();
    Ok(Sample {
        goodput_gbps: received as f64 * 8.0 / elapsed / 1e9,
        goodput_gbytes_per_second: received as f64 / elapsed / 1e9,
        elapsed_seconds: elapsed,
        cpu_seconds: cpu_time().zip(cpu).map(|(end, start)| end - start),
        sent_datagrams: t.sent_datagrams,
        received_datagrams: q.received_datagrams,
        sent_application_bytes: sent,
        received_application_bytes: received,
        sent_wire_bytes: t.sent_wire_bytes,
        send_syscalls: t.send_syscalls,
        receive_syscalls: q.receive_syscalls,
        socket_recv_buffer: q.socket_recv_buffer,
        socket_send_buffer: t.socket_send_buffer,
        ..Sample::default()
    })
}
async fn l1(
    r: &Environment,
    a: &Args,
    d: usize,
    m: usize,
    addresses: (SocketAddr, SocketAddr),
    duration: u64,
) -> BenchResult<Sample> {
    let tx = bind_retry(r, addresses.0, cfg(a, d)).await?;
    let rx = bind_retry(r, addresses.1, cfg(a, d)).await?;
    let session = tx.connect_async(addresses.1).await?;
    let before_t = tx.metrics();
    let before_r = rx.metrics();
    let stop = Arc::new(AtomicBool::new(false));
    let done = stop.clone();
    let receive_ep = rx.clone();
    let receive = r.spawn(async move {
        let mut count = 0u64;
        let mut bytes = 0u64;
        loop {
            match tokio::time::timeout(Duration::from_millis(10), receive_ep.recv_async()).await {
                Ok(Ok(frame)) => {
                    bytes += frame.len() as u64;
                    count += 1;
                }
                Err(_) if done.load(Acquire) => break,
                Err(_) => {}
                Ok(Err(e)) => return Err(e),
            }
        }
        Ok((bytes, count))
    });
    let bytes = vec![0x5a; m];
    let cpu = cpu_time();
    let start = Instant::now();
    let mut sent = 0u64;
    let mut count = 0u64;
    while start.elapsed() < Duration::from_millis(duration) {
        match session.send_async(&bytes).await {
            Ok(_) => {
                sent += m as u64;
                count += 1;
            }
            Err(rimuru::Error::NoBuffer) => tokio::task::yield_now().await,
            Err(e) => return Err(e.into()),
        }
        pace(start, sent, a.offered_mbps).await;
    }
    tokio::time::sleep(Duration::from_millis(a.drain_ms)).await;
    stop.store(true, Release);
    let (received, frames) = receive.await??;
    let elapsed = start.elapsed().as_secs_f64();
    let t = tx.metrics();
    let q = rx.metrics();
    let sample = Sample {
        tx_crc_parallel_jobs: t.crc_parallel_jobs - before_t.crc_parallel_jobs,
        rx_crc_parallel_jobs: q.crc_parallel_jobs - before_r.crc_parallel_jobs,
        goodput_gbps: received as f64 * 8.0 / elapsed / 1e9,
        goodput_gbytes_per_second: received as f64 / elapsed / 1e9,
        elapsed_seconds: elapsed,
        cpu_seconds: cpu_time().zip(cpu).map(|(end, start)| end - start),
        sent_datagrams: t.sent_datagrams - before_t.sent_datagrams,
        received_datagrams: q.received_datagrams - before_r.received_datagrams,
        sent_application_bytes: sent,
        received_application_bytes: received,
        sent_frames: count,
        received_frames: frames,
        sent_wire_bytes: t.sent_wire_bytes - before_t.sent_wire_bytes,
        send_syscalls: t.send_syscalls - before_t.send_syscalls,
        receive_syscalls: q.receive_syscalls - before_r.receive_syscalls,
        malformed: q.malformed_datagrams,
        integrity_failures: q.integrity_failures,
        resource_drops: q.resource_drops,
        orphan_drops: q.orphan_drops,
        expired_frames: q.expired_frames,
        memory_peak_bytes: q.memory_peak,
        socket_recv_buffer: q.socket_recv_buffer,
        socket_send_buffer: t.socket_send_buffer,
    };
    tx.close_async().await;
    rx.close_async().await;
    Ok(sample)
}
async fn bind_retry(
    r: &Environment,
    addr: SocketAddr,
    cfg: EndpointConfig,
) -> BenchResult<Endpoint> {
    for _ in 0..100 {
        match r.bind_endpoint(addr, cfg.clone()) {
            Ok(ep) => return Ok(ep),
            Err(rimuru::Error::Io(e)) if e.kind() == std::io::ErrorKind::AddrInUse => {
                tokio::time::sleep(Duration::from_millis(1)).await
            }
            Err(e) => return Err(e.into()),
        }
    }
    Err("previous socket did not release its port".into())
}
async fn latency(
    r: &Environment,
    a: &Args,
    d: usize,
    m: usize,
    addresses: (SocketAddr, SocketAddr),
) -> BenchResult<Latency> {
    let tx = bind_retry(r, addresses.0, cfg(a, d)).await?;
    let rx = bind_retry(r, addresses.1, cfg(a, d)).await?;
    let s = tx.connect_async(addresses.1).await?;
    let data = vec![0x5a; m];
    let mut samples = Vec::new();
    let mut timed_out = 0;
    for _ in 0..a.latency_samples {
        let start = Instant::now();
        s.send_async(&data).await?;
        match tokio::time::timeout(Duration::from_millis(100), rx.recv_async()).await {
            Ok(Ok(_)) => samples.push(start.elapsed().as_secs_f64() * 1e6),
            _ => timed_out += 1,
        }
    }
    tx.close_async().await;
    rx.close_async().await;
    Ok(Latency::from_samples(samples, timed_out))
}
pub fn run(a: Args) -> BenchResult<()> {
    a.validate()?;
    let rustc = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .map(|s| String::from_utf8_lossy(&s.stdout).trim().to_string())
        .unwrap_or_default();
    let mut report = Report {
        schema: report::SCHEMA,
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        rustc,
        available_parallelism: std::thread::available_parallelism().map_or(1, usize::from),
        integrity: format!("{:?}", a.integrity),
        offered_mbps: a.offered_mbps,
        duration_ms: a.duration_ms,
        warmup_ms: a.warmup_ms,
        drain_ms: a.drain_ms,
        batch: a.batch,
        socket_buffer_requested: a.socket_buffer,
        notes: vec![
            "Portable readiness backend; one RX dispatcher; T-1 Rayon workers. No GSO/USO or kernel zero-copy claimed.".into(),
            "Separate raw/L1 runs reuse the same two UDP ports. Alternating order, warmup, median and relative MAD; drain time is included in both rates.".into(),
            "Unpaced is a saturation test: frame loss is reported, never relabeled lossless. Positive offered_mbps is a rate-limited diagnostic, not a capacity proof.".into(),
            "Latency is a separate sequential L1 probe, not per-packet parsing in raw throughput. In-process endpoints, IPv4 loopback, no cross-platform claims.".into(),
        ],
        cells: Vec::new(),
    };
    for &t in &a.threads {
        let runtime = Environment::new(t)?;
        for &d in &a.datagrams {
            for &m in &a.messages {
                let addresses = {
                    let x = runtime.bind_socket("127.0.0.1:0".parse()?, a.socket_buffer)?;
                    let y = runtime.bind_socket("127.0.0.1:0".parse()?, a.socket_buffer)?;
                    (x.local_addr()?, y.local_addr()?)
                };
                let mut raws = Vec::new();
                let mut frames = Vec::new();
                runtime.block_on(async {
                    if a.warmup_ms > 0 {
                        let _ = raw(&runtime, &a, d, addresses, a.warmup_ms).await?;
                        let _ = l1(&runtime, &a, d, m, addresses, a.warmup_ms).await?;
                    }
                    for round in 0..a.rounds {
                        if round % 2 == 0 {
                            raws.push(raw(&runtime, &a, d, addresses, a.duration_ms).await?);
                            frames.push(l1(&runtime, &a, d, m, addresses, a.duration_ms).await?);
                        } else {
                            frames.push(l1(&runtime, &a, d, m, addresses, a.duration_ms).await?);
                            raws.push(raw(&runtime, &a, d, addresses, a.duration_ms).await?);
                        }
                    }
                    Ok::<_, Box<dyn Error>>(())
                })??;
                let med_raw = report::median(raws.iter().map(|s| s.goodput_gbps));
                let med_l1 = report::median(frames.iter().map(|s| s.goodput_gbps));
                let ratio = if med_raw > 0.0 { med_l1 / med_raw } else { 0.0 };
                let lossless = frames.iter().all(|s| {
                    s.sent_frames == s.received_frames
                        && s.integrity_failures == 0
                        && s.resource_drops == 0
                }) && raws
                    .iter()
                    .all(|s| s.sent_datagrams == s.received_datagrams);
                let latency = runtime.block_on(latency(&runtime, &a, d, m, addresses))??;
                println!(
                    "D={d:5} M={m:8} T={t:2} raw={med_raw:7.3} Gbit/s ({:.3} GB/s) L1={med_l1:7.3} Gbit/s ({:.3} GB/s) ratio={ratio:.4} lossless={lossless}",
                    med_raw / 8.0,
                    med_l1 / 8.0
                );
                report.cells.push(Cell {
                    datagram_size: d,
                    frame_size: m,
                    processing_threads: t,
                    io_threads: 2,
                    rx_dispatchers: 1,
                    rayon_threads: t - 1,
                    raw_relative_mad: report::relative_mad(&raws, med_raw),
                    l1_relative_mad: report::relative_mad(&frames, med_l1),
                    raw: raws,
                    l1: frames,
                    median_raw_gbps: med_raw,
                    median_l1_gbps: med_l1,
                    median_raw_gbytes_per_second: med_raw / 8.0,
                    median_l1_gbytes_per_second: med_l1 / 8.0,
                    ratio,
                    lossless,
                    meets_90_percent: ratio >= 0.9 && lossless,
                    latency,
                });
            }
        }
    }
    if let Some(path) = &a.output {
        if let Some(p) = path.parent()
            && !p.as_os_str().is_empty()
        {
            std::fs::create_dir_all(p)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
        println!("Report: {}", path.display());
    }
    if a.strict && report.cells.iter().any(|c| !c.meets_90_percent) {
        return Err("one or more cells missed lossless 90% target (report preserved)".into());
    }
    Ok(())
}

#[cfg(windows)]
fn cpu_time() -> Option<f64> {
    use windows_sys::Win32::{
        Foundation::FILETIME,
        System::Threading::{GetCurrentProcess, GetProcessTimes},
    };
    let mut c = FILETIME::default();
    let mut e = FILETIME::default();
    let mut k = FILETIME::default();
    let mut u = FILETIME::default();
    // SAFETY: valid output pointers, pseudo-handle from GetCurrentProcess.
    if unsafe { GetProcessTimes(GetCurrentProcess(), &mut c, &mut e, &mut k, &mut u) } == 0 {
        return None;
    }
    let ticks = |v: FILETIME| (u64::from(v.dwHighDateTime) << 32) | u64::from(v.dwLowDateTime);
    Some((ticks(k) + ticks(u)) as f64 / 1e7)
}
#[cfg(not(windows))]
fn cpu_time() -> Option<f64> {
    None
}
