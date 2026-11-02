use super::*;
use crate::{
    reassembly::Receiver,
    wire::{self, Begin},
};
use std::{
    sync::atomic::Ordering::{Acquire, Relaxed},
    time::Instant,
};
fn fixture() -> (Runtime, Endpoint, Receiver) {
    let r = Runtime::new(crate::RuntimeConfig {
        io_threads: 1,
        cpu_threads: 0,
        memory_budget: 32 << 20,
    })
    .unwrap();
    let cfg = EndpointConfig {
        memory_budget: 8 << 20,
        max_frame_size: 1 << 20,
        max_peers: 4,
        frame_capacity: 4,
        orphan_bytes: 65536,
        ..EndpointConfig::default()
    };
    let ep = Endpoint::bind_on(&r, "127.0.0.1:0".parse().unwrap(), cfg).unwrap();
    let receiver = Receiver::new(ep.owner.shared.clone(), "127.0.0.1:9".parse().unwrap());
    (r, ep, receiver)
}
fn metadata(id: u32, data: &[u8]) -> Begin {
    let mut m = Begin::new(data.len(), 1200, 1).unwrap();
    m.crc = crc32c::crc32c_append(m.seed(id), data).into();
    m
}
#[test]
fn reordering_orphans_duplicates_and_replay() {
    let (_r, ep, mut rx) = fixture();
    let data = vec![19u8; 3000];
    let m = metadata(7, &data);
    let stride = m.stride.get() as usize;
    rx.data(7, 1, 2, &data[stride..2 * stride]).unwrap();
    rx.begin(7, m, 1 << 20, 1200, 1).unwrap();
    rx.data(7, 0, 2, &data[..stride]).unwrap();
    rx.data(7, 0, 2, &data[..stride]).unwrap();
    assert!(matches!(ep.try_recv(), Err(Error::WouldBlock)));
    rx.data(7, 2, 2, &data[2 * stride..]).unwrap();
    assert_eq!(ep.try_recv().unwrap().as_slice(), data);
    rx.begin(7, m, 1 << 20, 1200, 1).unwrap();
    for (i, p) in data.chunks(stride).enumerate() {
        rx.data(7, i as u32, 2, p).unwrap();
    }
    assert!(matches!(ep.try_recv(), Err(Error::WouldBlock)));
    assert_eq!(ep.metrics().duplicate_segments, 1);
    ep.close().unwrap();
}
#[test]
fn corruption_truncation_and_conflicting_metadata_never_deliver() {
    let (_r, ep, mut rx) = fixture();
    let data = vec![4u8; 2000];
    let m = metadata(1, &data);
    let stride = m.stride.get() as usize;
    rx.begin(1, m, 1 << 20, 1200, 1).unwrap();
    assert!(rx.data(1, 0, 2, &data[..10]).is_err());
    assert!(rx.data(1, u32::MAX, 2, &data).is_err());
    let mut corrupt = data[..stride].to_vec();
    corrupt[0] ^= 1;
    rx.data(1, 0, 2, &corrupt).unwrap();
    rx.data(1, 1, 2, &data[stride..]).unwrap();
    assert_eq!(ep.metrics().integrity_failures, 1);
    assert!(matches!(ep.try_recv(), Err(Error::WouldBlock)));
    let m = metadata(2, &data);
    rx.begin(2, m, 1 << 20, 1200, 1).unwrap();
    let mut different = m;
    different.crc = 123.into();
    assert!(rx.begin(2, different, 1 << 20, 1200, 1).is_err());
    for (i, p) in data.chunks(stride).enumerate() {
        rx.data(2, i as u32, 2, p).unwrap();
    }
    assert!(matches!(ep.try_recv(), Err(Error::WouldBlock)));
    ep.close().unwrap();
}
#[test]
fn expiry_releases_partial_and_orphan_quotas() {
    let (_r, ep, mut rx) = fixture();
    let data = vec![1; 2000];
    let m = metadata(1, &data);
    rx.begin(1, m, 1 << 20, 1200, 1).unwrap();
    rx.data(2, 0, 2, b"orphan").unwrap();
    assert_eq!(ep.owner.shared.frames_in_use.load(Acquire), 1);
    assert!(ep.owner.shared.orphan_bytes.load(Acquire) > 0);
    rx.expire(Instant::now() + Duration::from_secs(2));
    assert_eq!(ep.owner.shared.frames_in_use.load(Acquire), 0);
    assert_eq!(ep.owner.shared.orphan_bytes.load(Acquire), 0);
    assert_eq!(ep.metrics().expired_frames, 1);
    ep.close().unwrap();
}
#[test]
fn malformed_declarations_cannot_allocate_and_unknown_sources_cannot_reassemble() {
    let (_r, ep, mut rx) = fixture();
    let before = ep.metrics().memory_used;
    let mut m = metadata(1, b"x");
    m.total = u32::MAX.into();
    m.count = u32::MAX.into();
    assert!(rx.begin(1, m, 1 << 20, 1200, 1).is_err());
    assert_eq!(ep.metrics().memory_used, before);
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    use zerocopy::IntoBytes;
    let packet = wire::Control::new(wire::BEGIN, 123, 1, m.as_bytes());
    socket
        .send_to(packet.as_bytes(), ep.local_addr().unwrap())
        .unwrap();
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(ep.metrics().memory_used, before);
    assert_eq!(ep.metrics().unknown_sessions, 1);
    ep.close().unwrap();
}

#[test]
fn continuously_readable_udp_keeps_the_single_io_worker_cooperative() {
    use std::sync::atomic::AtomicBool;
    let runtime = Runtime::new(crate::RuntimeConfig {
        io_threads: 1,
        cpu_threads: 0,
        memory_budget: 32 << 20,
    })
    .unwrap();
    let endpoint = Endpoint::bind_on(
        &runtime,
        "127.0.0.1:0".parse().unwrap(),
        EndpointConfig {
            batch_size: 256,
            max_peers: 4,
            ..EndpointConfig::default()
        },
    )
    .unwrap();
    let address = endpoint.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    let flood = std::thread::spawn(move || {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.connect(address).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !flag.load(Relaxed) && Instant::now() < deadline {
            let _ = socket.send(&[0]);
        }
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while endpoint.metrics().received_datagrams < 256 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let (tx, rx) = std::sync::mpsc::channel();
    runtime.spawn(async move {
        let _ = tx.send(());
    });
    let progressed = rx.recv_timeout(Duration::from_secs(1));
    let close = endpoint.close();
    stop.store(true, Relaxed);
    flood.join().unwrap();
    assert!(endpoint.metrics().received_datagrams >= 256);
    progressed.expect("application task starved behind ready UDP");
    close.unwrap();
}

#[cfg(windows)]
#[test]
fn segmented_send_preserves_datagram_geometry_and_single_packet_fallback() {
    let config = EndpointConfig {
        datagram_size: 65507,
        max_peers: 4,
        ..EndpointConfig::default()
    };
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config.clone()).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config).unwrap();
    let session = a.connect(b.local_addr().unwrap()).unwrap();
    let socket = &a.owner.shared.socket;
    let configured = socket.segmentation_size() == 65507;
    // Seven full segments and a short tail exercise segmentation boundaries.
    let data: Vec<u8> = (0..(7 * (65507 - 15) + 83))
        .map(|i| (i % 251) as u8)
        .collect();
    let before = a.metrics();
    let ticket = session.submit_reliable(&data).unwrap();
    assert_eq!(
        b.recv_timeout(Duration::from_secs(5)).unwrap().as_slice(),
        data
    );
    ticket.wait_timeout(Duration::from_secs(5)).unwrap();
    let after = a.metrics();
    if configured && socket.segmentation_size() != 0 {
        assert!(after.reliable_segment_batches > before.reliable_segment_batches);
        assert!(
            after.send_syscalls - before.send_syscalls
                < after.reliable_data_datagrams - before.reliable_data_datagrams
        );
    }
    // Disabling the optimization keeps the installed socket option, but each
    // ordinary datagram must retain its original bytes and boundary.
    socket.disable_segmentation();
    let before = a.metrics();
    let ticket = session.submit_reliable(&data).unwrap();
    assert_eq!(
        b.recv_timeout(Duration::from_secs(5)).unwrap().as_slice(),
        data
    );
    ticket.wait_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        a.metrics().reliable_segment_batches,
        before.reliable_segment_batches
    );
    if configured {
        socket.enable_segmentation(65507);
        if socket.segmentation_size() != 0 {
            socket.fail_next_segment_for_test();
            let before = a.metrics();
            let ticket = session.submit_reliable(&data).unwrap();
            assert_eq!(
                b.recv_timeout(Duration::from_secs(5)).unwrap().as_slice(),
                data
            );
            ticket.wait_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(socket.segmentation_size(), 0);
            assert_eq!(
                a.metrics().reliable_segment_batches,
                before.reliable_segment_batches
            );
        }
    }
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn coarse_parallel_receive_crc_verifies_complete_frame_and_releases_quota() {
    // Inject through the owner deterministically: a cold multi-MiB UDP burst
    // may exceed a CI kernel's socket buffer, which is valid best-effort loss.
    let runtime = Runtime::new(crate::RuntimeConfig {
        io_threads: 2,
        cpu_threads: 3,
        memory_budget: 128 << 20,
    })
    .unwrap();
    let cfg = EndpointConfig {
        max_peers: 4,
        ..EndpointConfig::default()
    };
    let ep = Endpoint::bind_on(&runtime, "127.0.0.1:0".parse().unwrap(), cfg).unwrap();
    let mut receiver = Receiver::new(ep.owner.shared.clone(), "127.0.0.1:9".parse().unwrap());
    let data: Vec<_> = (0..(8 << 20) + 17).map(|i| (i % 251) as u8).collect();
    let meta = metadata(33, &data);
    receiver.begin(33, meta, 16 << 20, 1200, 1).unwrap();
    for (index, payload) in data.chunks(meta.stride.get() as usize).enumerate() {
        receiver.data(33, index as u32, 2, payload).unwrap();
    }
    let frame = ep.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(frame.as_slice(), data);
    assert_eq!(ep.metrics().crc_parallel_jobs, 1);
    assert_eq!(ep.owner.shared.frames_in_use.load(Acquire), 1);
    drop(frame);
    assert_eq!(ep.owner.shared.frames_in_use.load(Acquire), 0);
    ep.close().unwrap();
}

#[test]
fn interleaved_peers_and_malformed_fragments_preserve_valid_frames() {
    use zerocopy::IntoBytes;
    fn data_packet(incarnation: u64, id: u32, index: u32, width: u8, payload: &[u8]) -> Vec<u8> {
        let mut bytes = wire::Header::new(if width == 2 { 0 } else { 1 }, incarnation, id)
            .as_bytes()
            .to_vec();
        if width == 2 {
            bytes.extend_from_slice(&(index as u16).to_le_bytes());
        } else {
            bytes.extend_from_slice(&index.to_le_bytes());
        }
        bytes.extend_from_slice(payload);
        bytes
    }
    fn send(runtime: &Runtime, a: &Endpoint, b: &Endpoint, bytes: &[u8]) {
        let target = b.metrics().received_datagrams + 1;
        runtime
            .block_on(
                a.owner
                    .shared
                    .socket
                    .send_to(bytes, b.local_addr().unwrap()),
            )
            .unwrap()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while b.metrics().received_datagrams < target {
            assert!(Instant::now() < deadline, "receive stalled");
            std::thread::yield_now();
        }
        // The syscall counter precedes parsing. Keep this adversarial sequence
        // deterministic without adding delays to the production transport.
        std::thread::sleep(Duration::from_millis(1));
    }
    let runtime = Runtime::new(crate::RuntimeConfig {
        io_threads: 2,
        cpu_threads: 0,
        memory_budget: 64 << 20,
    })
    .unwrap();
    let cfg = EndpointConfig {
        datagram_size: 16384,
        memory_budget: 8 << 20,
        max_peers: 4,
        frame_capacity: 8,
        orphan_bytes: 65536,
        ..EndpointConfig::default()
    };
    let a = Endpoint::bind_on(&runtime, "127.0.0.1:0".parse().unwrap(), cfg.clone()).unwrap();
    let b = Endpoint::bind_on(&runtime, "127.0.0.1:0".parse().unwrap(), cfg.clone()).unwrap();
    let c = Endpoint::bind_on(&runtime, "127.0.0.1:0".parse().unwrap(), cfg).unwrap();
    let session = a.connect(b.local_addr().unwrap()).unwrap();
    let foreign = c.connect(b.local_addr().unwrap()).unwrap();
    for width in [2, 4] {
        let stride = 16384 - 13 - width as usize;
        let data: Vec<_> = (0..3 * stride + 17).map(|i| (i % 251) as u8).collect();
        let layout = session.allocate_frame(data.len()).unwrap();
        let id = layout.frame_id();
        drop(layout);
        let mut meta = Begin::new(data.len(), 16384, 1).unwrap();
        meta.idx_bytes = width;
        meta.stride = (stride as u32).into();
        meta.count = (data.len().div_ceil(stride) as u32).into();
        meta.crc = crc32c::crc32c_append(meta.seed(id), &data).into();
        let begin = wire::Control::new(wire::BEGIN, session.state.local, id, meta.as_bytes());
        send(&runtime, &a, &b, begin.as_bytes());
        // Wrong prediction: piece 1 arrives before piece 0 and is accepted via fallback.
        let piece1 = data_packet(session.state.local, id, 1, width, &data[stride..2 * stride]);
        send(&runtime, &a, &b, &piece1);
        send(
            &runtime,
            &a,
            &b,
            &data_packet(session.state.local, id, 0, width, &data[..stride]),
        );
        foreign.send(b"cross-peer").unwrap();
        send(&runtime, &a, &b, &piece1);
        let bad = vec![0x91; stride];
        send(
            &runtime,
            &a,
            &b,
            &data_packet(session.state.local, id, 1, width, &bad),
        );
        send(
            &runtime,
            &a,
            &b,
            &data_packet(session.state.local ^ 1, id, 0, width, &bad),
        );
        send(
            &runtime,
            &a,
            &b,
            &data_packet(session.state.local, id, 0, width, &bad[..stride - 1]),
        );
        let mut flags = data_packet(session.state.local, id, 0, width, &bad);
        flags[0] |= 1;
        send(&runtime, &a, &b, &flags);
        let mut oversized = data_packet(session.state.local, id, 0, width, &bad);
        oversized.push(0);
        send(&runtime, &a, &b, &oversized);
        // Reject both configured-limit+1 and +2 packets without accepting them.
        oversized.push(0);
        let rejected = b.metrics().malformed_datagrams;
        runtime
            .block_on(
                a.owner
                    .shared
                    .socket
                    .send_to(&oversized, b.local_addr().unwrap()),
            )
            .unwrap()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while b.metrics().malformed_datagrams == rejected {
            assert!(
                Instant::now() < deadline,
                "oversized receive was not rejected"
            );
            std::thread::yield_now();
        }
        for index in [2, 3] {
            let start = index * stride;
            let end = (start + stride).min(data.len());
            send(
                &runtime,
                &a,
                &b,
                &data_packet(
                    session.state.local,
                    id,
                    index as u32,
                    width,
                    &data[start..end],
                ),
            );
        }
        let frames = [
            b.recv_timeout(Duration::from_secs(2)).unwrap(),
            b.recv_timeout(Duration::from_secs(2)).unwrap(),
        ];
        assert!(
            frames
                .iter()
                .any(|f| f.peer_addr() == a.local_addr().unwrap() && f.as_slice() == data)
        );
        assert!(
            frames
                .iter()
                .any(|f| f.peer_addr() == c.local_addr().unwrap() && f.as_slice() == b"cross-peer")
        );
    }
    a.close().unwrap();
    b.close().unwrap();
    c.close().unwrap();
    let metrics = b.metrics();
    assert!(metrics.duplicate_segments >= 2);
    assert!(metrics.unknown_sessions >= 2);
    assert!(metrics.malformed_datagrams >= 6);
    assert_eq!(metrics.integrity_failures, 0);
}
