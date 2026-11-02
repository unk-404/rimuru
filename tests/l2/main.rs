#[allow(dead_code)]
#[path = "../../benches/support/fixture.rs"]
mod benchmark_fixture;
#[allow(dead_code)]
#[path = "../../benches/support/matrix.rs"]
mod matrix;
#[path = "../../benches/support/relay.rs"]
mod support;
use rimuru::{
    AbortReason, DeliveryOutcome, Endpoint, EndpointConfig, Error, ReliableConfig,
    benchmarking::Environment,
};
use std::{thread, time::Duration};

fn config(d: usize) -> EndpointConfig {
    EndpointConfig {
        datagram_size: d,
        max_peers: 4,
        socket_buffer_bytes: 4 << 20,
        reliable: ReliableConfig {
            initial_rto: Duration::from_millis(30),
            message_timeout: Duration::from_secs(5),
            ..ReliableConfig::default()
        },
        ..EndpointConfig::default()
    }
}
fn pair(d: usize) -> (Endpoint, Endpoint, rimuru::Session) {
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(d)).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(d)).unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    assert!(s.supports_reliable());
    (a, b, s)
}
#[test]
fn reliable_empty_fragmented_large_and_bidirectional() {
    let (a, b, s) = pair(1200);
    let reverse = b.session(a.local_addr().unwrap()).unwrap();
    for size in [0, 1, 65536, 1048576] {
        let data = vec![0x5a; size];
        let ticket = s.submit_reliable(&data).unwrap();
        let received = b.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(received.layer(), 2);
        assert_eq!(received.as_slice(), data);
        assert_eq!(
            ticket
                .wait_timeout(Duration::from_secs(5))
                .unwrap()
                .application_bytes,
            size
        );
    }
    let t1 = s.submit_reliable(b"forward").unwrap();
    let t2 = reverse.submit_reliable(b"reverse").unwrap();
    assert_eq!(
        &*b.recv_timeout(Duration::from_secs(5)).unwrap(),
        b"forward"
    );
    assert_eq!(
        &*a.recv_timeout(Duration::from_secs(5)).unwrap(),
        b"reverse"
    );
    t1.wait().unwrap();
    t2.wait().unwrap();
    assert_eq!(a.metrics().bound_udp_sockets, 1);
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn pipeline_layouts_and_cross_thread_tickets() {
    let (a, b, s) = pair(65507);
    let mut tickets = Vec::new();
    for n in 0..8 {
        let mut layout = s.allocate_frame(65536 + n).unwrap();
        for i in 0..layout.segment_count() {
            layout.payload_mut(i).unwrap().fill(n as u8);
        }
        tickets.push(s.submit_reliable_layout(layout).unwrap());
    }
    let consumer = thread::spawn(move || {
        for _ in 0..8 {
            let f = b.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(f.iter().all(|v| *v == f[0]));
        }
        b.close().unwrap();
    });
    let waiter = thread::spawn(move || {
        for t in tickets {
            t.wait_timeout(Duration::from_secs(5)).unwrap();
        }
    });
    waiter.join().unwrap();
    consumer.join().unwrap();
    a.close().unwrap();
}
#[test]
fn application_held_credit_and_wait_timeout_do_not_cancel() {
    let cfg = EndpointConfig {
        frame_capacity: 1,
        ..config(1200)
    };
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), cfg.clone()).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), cfg).unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    let first = s.submit_reliable(b"held").unwrap();
    let held = b.recv_timeout(Duration::from_secs(2)).unwrap();
    first.wait().unwrap();
    let next = s.submit_reliable(b"next").unwrap();
    assert!(matches!(
        next.wait_timeout(Duration::from_millis(80)),
        Err(Error::TimedOut)
    ));
    assert_eq!(next.outcome(), None);
    drop(held);
    assert_eq!(&*b.recv_timeout(Duration::from_secs(2)).unwrap(), b"next");
    next.wait_timeout(Duration::from_secs(2)).unwrap();
    assert!(b.metrics().reliable_credit_stalls > 0);
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn confirmed_cancel_before_grant_and_close_wakes_ticket() {
    let (a, b, s) = pair(1200);
    let t = s.submit_reliable(b"race").unwrap();
    let outcome = t.cancel().unwrap();
    assert!(matches!(
        outcome,
        DeliveryOutcome::Delivered(_) | DeliveryOutcome::Aborted(AbortReason::Cancelled)
    ));
    if matches!(outcome, DeliveryOutcome::Delivered(_)) {
        b.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn asynchronous_submit_and_large_parallel_crc() {
    let env = Environment::new(4).unwrap();
    let cfg = EndpointConfig {
        memory_budget: 128 << 20,
        ..config(16384)
    };
    let a = env
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg.clone())
        .unwrap();
    let b = env
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg)
        .unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    let bytes = vec![0x37; 8 << 20];
    let t = env
        .block_on(s.submit_reliable_async(&bytes))
        .unwrap()
        .unwrap();
    let f = b.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(f.as_slice(), bytes);
    t.wait_timeout(Duration::from_secs(5)).unwrap();
    assert!(a.metrics().crc_parallel_jobs > 0);
    assert!(b.metrics().crc_parallel_jobs > 0);
    a.close().unwrap();
    b.close().unwrap();
}

fn relayed(
    faults: support::Faults,
    cfg: EndpointConfig,
) -> (
    Environment,
    Endpoint,
    Endpoint,
    rimuru::Session,
    support::Relay,
) {
    let env = Environment::new(1).unwrap();
    let a = env
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg.clone())
        .unwrap();
    let b = env
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg)
        .unwrap();
    let relay = support::Relay::new(
        &env,
        a.local_addr().unwrap(),
        b.local_addr().unwrap(),
        faults,
    )
    .unwrap();
    let session = a.connect(relay.address).unwrap();
    (env, a, b, session, relay)
}
#[test]
fn each_lost_control_and_tail_data_recovers_without_duplicate_delivery() {
    use support::Faults;
    // BEGIN, grant, ACK, COMPLETE request, COMPLETE confirmation, first DATA.
    for rule in [
        (6, 0, 0),
        (7, 0, 1),
        (8, 0, 1),
        (11, 1, 1),
        (11, 0, 0),
        (0, 1, 0),
    ] {
        let (_env, a, b, s, relay) = relayed(
            Faults {
                drop_once: vec![rule],
                duplicate_every: 7,
                reorder_every: 11,
                ..Faults::default()
            },
            config(1200),
        );
        let payload = vec![0x71; 65536];
        let ticket = s.submit_reliable(&payload).unwrap();
        let frame = b.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(frame.as_slice(), payload, "rule {rule:?}");
        ticket.wait_timeout(Duration::from_secs(5)).unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            matches!(b.try_recv(), Err(Error::WouldBlock)),
            "duplicate for {rule:?}"
        );
        assert!(
            relay
                .stats
                .dropped
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0,
            "not injected {rule:?}"
        );
        a.close().unwrap();
        b.close().unwrap();
    }
}

#[test]
fn dropped_large_tail_is_recovered_once() {
    let (_env, a, b, s, relay) = relayed(
        support::Faults {
            drop_data_index_once: Some(16),
            ..support::Faults::default()
        },
        config(65507),
    );
    let data: Vec<u8> = (0..1048576).map(|i| (i % 251) as u8).collect();
    let at = std::time::Instant::now();
    let ticket = s.submit_reliable(&data).unwrap();
    assert_eq!(
        b.recv_timeout(Duration::from_secs(5)).unwrap().as_slice(),
        data
    );
    ticket.wait_timeout(Duration::from_secs(5)).unwrap();
    println!("L2 dropped tail complete in {:?}", at.elapsed());
    assert_eq!(
        relay.stats.injected_drops[0].load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    thread::sleep(Duration::from_millis(100));
    assert!(matches!(b.try_recv(), Err(Error::WouldBlock)));
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn lost_complete_and_first_delayed_ping_still_converge() {
    let (_env, a, b, s, relay) = relayed(
        support::Faults {
            drop_once: vec![(11, 1, 1), (14, 0, 0)],
            ..support::Faults::default()
        },
        config(1200),
    );
    let ticket = s.submit_reliable(b"completion after lost ping").unwrap();
    let frame = b.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(frame.as_slice(), b"completion after lost ping");
    ticket.wait_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        relay
            .stats
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    assert!(matches!(b.try_recv(), Err(Error::WouldBlock)));
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn peers_with_different_negotiated_datagram_sizes_share_the_socket() {
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(65507)).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(65507)).unwrap();
    let c = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(16384)).unwrap();
    let large = a.connect(b.local_addr().unwrap()).unwrap();
    let small = a.connect(c.local_addr().unwrap()).unwrap();
    assert_eq!(large.datagram_size(), 65507);
    assert_eq!(small.datagram_size(), 16384);
    let payload: Vec<u8> = (0..(1 << 20) + 37).map(|n| (n % 251) as u8).collect();
    let t = large.submit_reliable(&payload).unwrap();
    assert_eq!(
        b.recv_timeout(Duration::from_secs(5)).unwrap().as_slice(),
        payload
    );
    t.wait_timeout(Duration::from_secs(5)).unwrap();
    let batches = a.metrics().reliable_segment_batches;
    let t = small.submit_reliable(&payload).unwrap();
    assert_eq!(
        c.recv_timeout(Duration::from_secs(5)).unwrap().as_slice(),
        payload
    );
    t.wait_timeout(Duration::from_secs(5)).unwrap();
    // A different negotiated D must not use segmentation configured for 65507.
    assert_eq!(a.metrics().reliable_segment_batches, batches);
    small.send(b"ordinary L1 frame").unwrap();
    assert_eq!(
        c.recv_timeout(Duration::from_secs(5)).unwrap().as_slice(),
        b"ordinary L1 frame"
    );
    assert_eq!(a.metrics().bound_udp_sockets, 1);
    a.close().unwrap();
    b.close().unwrap();
    c.close().unwrap();
}
#[test]
fn seeded_loss_delay_reorder_and_duplicates_are_atomic() {
    let (_env, a, b, s, relay) = relayed(
        support::Faults {
            seed: 0x52494d555255,
            loss_ppm: 100_000,
            delay: Duration::from_millis(5),
            duplicate_every: 13,
            reorder_every: 7,
            ..support::Faults::default()
        },
        config(4096),
    );
    let payload: Vec<_> = (0..262144).map(|i| (i % 251) as u8).collect();
    let mut tickets = Vec::new();
    for _ in 0..4 {
        tickets.push(s.submit_reliable(&payload).unwrap());
    }
    for _ in 0..4 {
        assert_eq!(
            b.recv_timeout(Duration::from_secs(5)).unwrap().as_slice(),
            payload
        );
    }
    for t in tickets {
        t.wait_timeout(Duration::from_secs(5)).unwrap();
    }
    assert!(a.metrics().reliable_retransmits > 0);
    assert!(
        relay
            .stats
            .duplicated
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn distinct_large_inflight_messages_survive_tail_loss_and_reordering() {
    let (_env, a, b, s, relay) = relayed(
        support::Faults {
            seed: 101,
            delay: Duration::from_millis(2),
            drop_data_index_once: Some(16),
            duplicate_every: 7,
            reorder_every: 11,
            ..support::Faults::default()
        },
        config(65507),
    );
    let payloads: Vec<Vec<u8>> = (0..4)
        .map(|n| {
            (0..1048576)
                .map(|i| ((i * 131 + n * 17) % 251) as u8)
                .collect()
        })
        .collect();
    let tickets: Vec<_> = payloads
        .iter()
        .map(|data| s.submit_reliable(data).unwrap())
        .collect();
    let mut seen = [false; 4];
    for _ in 0..4 {
        let frame = b.recv_timeout(Duration::from_secs(5)).unwrap();
        let index = tickets
            .iter()
            .position(|ticket| ticket.frame_id() == frame.id())
            .unwrap();
        assert!(!seen[index]);
        seen[index] = true;
        assert_eq!(frame.as_slice(), payloads[index]);
    }
    for ticket in tickets {
        ticket.wait_timeout(Duration::from_secs(5)).unwrap();
    }
    assert!(seen.into_iter().all(|value| value));
    assert!(relay.stats.injected_drops[0].load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert!(
        relay
            .stats
            .duplicated
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert_eq!(
        relay
            .stats
            .queue_drops
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(a.metrics().reliable_retransmits > 0);
    assert!(matches!(
        b.recv_timeout(Duration::from_millis(100)),
        Err(Error::TimedOut)
    ));
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn corrupted_whole_message_aborts_and_lost_abort_is_retried() {
    for (d, size) in [(1200, 65536), (65507, 1048576)] {
        let (_env, a, b, s, _relay) = relayed(
            support::Faults {
                corrupt_data_once: true,
                drop_once: vec![(12, 0, 1), (13, 0, 0)],
                ..support::Faults::default()
            },
            config(d),
        );
        let ticket = s.submit_reliable(&vec![0x22; size]).unwrap();
        assert!(matches!(
            ticket.wait_timeout(Duration::from_secs(5)),
            Err(Error::DeliveryAborted(AbortReason::Integrity))
        ));
        assert!(matches!(b.try_recv(), Err(Error::WouldBlock)));
        assert_eq!(b.metrics().integrity_failures, 1);
        // Feedback remains live after the failed message.
        assert_eq!(s.send_reliable(b"next").unwrap().application_bytes, 4);
        assert_eq!(&*b.recv_timeout(Duration::from_secs(2)).unwrap(), b"next");
        a.close().unwrap();
        b.close().unwrap();
    }
}
#[test]
fn permanent_blackhole_is_bounded_unknown_and_close_resolves_queued_work() {
    let cfg = EndpointConfig {
        reliable: ReliableConfig {
            message_timeout: Duration::from_millis(200),
            initial_rto: Duration::from_millis(20),
            max_recovery_rounds: 8,
        },
        ..config(1200)
    };
    let (_env, a, b, s, relay) = relayed(support::Faults::default(), cfg);
    relay
        .blackhole
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let ticket = s.submit_reliable(b"unknown").unwrap();
    assert!(matches!(
        ticket.wait_timeout(Duration::from_secs(2)),
        Err(Error::OutcomeUnknown)
    ));
    assert!(s.is_closed());
    a.close().unwrap();
    b.close().unwrap();
    let (_env, a, b, s, relay) = relayed(support::Faults::default(), config(1200));
    relay
        .blackhole
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let ticket = s.submit_reliable(b"close").unwrap();
    a.close().unwrap();
    assert!(matches!(
        ticket.wait_timeout(Duration::from_secs(1)),
        Err(Error::OutcomeUnknown | Error::DeliveryAborted(AbortReason::Shutdown))
    ));
    b.close().unwrap();
}

#[test]
fn batch_one_and_ipv6_keep_both_directions_progressing() {
    let cfg = EndpointConfig {
        batch_size: 1,
        ..config(1200)
    };
    let a = Endpoint::bind("[::1]:0".parse().unwrap(), cfg.clone()).unwrap();
    let b = Endpoint::bind("[::1]:0".parse().unwrap(), cfg).unwrap();
    let forward = a.connect(b.local_addr().unwrap()).unwrap();
    let reverse = b.session(a.local_addr().unwrap()).unwrap();
    let left = forward.submit_reliable(&vec![0x31; 65536]).unwrap();
    let right = reverse.submit_reliable(&vec![0x32; 65536]).unwrap();
    assert_eq!(left.frame_id(), right.frame_id());
    assert!(
        b.recv_timeout(Duration::from_secs(5))
            .unwrap()
            .iter()
            .all(|v| *v == 0x31)
    );
    assert!(
        a.recv_timeout(Duration::from_secs(5))
            .unwrap()
            .iter()
            .all(|v| *v == 0x32)
    );
    left.wait_timeout(Duration::from_secs(5)).unwrap();
    right.wait_timeout(Duration::from_secs(5)).unwrap();
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn cancellation_waits_for_remote_ack_even_when_abort_packets_are_lost() {
    let cfg = EndpointConfig {
        frame_capacity: 1,
        ..config(1200)
    };
    let (_env, a, b, s, _relay) = relayed(
        support::Faults {
            drop_once: vec![(12, 0, 0), (13, 0, 1)],
            ..support::Faults::default()
        },
        cfg,
    );
    let first = s.submit_reliable(b"held").unwrap();
    let held = b.recv_timeout(Duration::from_secs(2)).unwrap();
    first.wait().unwrap();
    let cancelled = s.submit_reliable(b"cannot be granted").unwrap();
    assert_eq!(
        cancelled.cancel().unwrap(),
        DeliveryOutcome::Aborted(AbortReason::Cancelled)
    );
    drop(held);
    assert!(matches!(b.try_recv(), Err(Error::WouldBlock)));
    let next = s.submit_reliable(b"still usable").unwrap();
    assert_eq!(
        &*b.recv_timeout(Duration::from_secs(2)).unwrap(),
        b"still usable"
    );
    next.wait().unwrap();
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn foreign_executor_needs_no_tokio_context() {
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    foreign(async {
        let s = a.connect_async(b.local_addr().unwrap()).await.unwrap();
        s.send_reliable_async(b"foreign L2").await.unwrap();
        assert_eq!(&*b.recv_async().await.unwrap(), b"foreign L2");
        a.close_async().await;
        b.close_async().await;
    });
}

fn foreign<F: std::future::Future>(future: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Instant;
    struct ThreadWake(std::thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                assert!(Instant::now() < deadline, "foreign executor stalled");
                std::thread::park_timeout(Duration::from_millis(10));
            }
        }
    }
}

#[test]
fn retained_large_slices_survive_source_reuse_loss_reordering_and_tail_sizes() {
    for d in [16384, 65507] {
        let (_env, a, b, s, relay) = relayed(
            support::Faults {
                drop_once: vec![(0, 1, 0)],
                duplicate_every: 7,
                reorder_every: 11,
                ..support::Faults::default()
            },
            config(d),
        );
        for size in [0, 1, d - 15, d - 14, (1 << 20) + 17] {
            let mut data: Vec<_> = (0..size).map(|i| (i % 251) as u8).collect();
            let expected = data.clone();
            let ticket = s.submit_reliable(&data).unwrap();
            data.fill(0xee);
            drop(data);
            let frame = b.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(frame.as_slice(), expected);
            ticket.wait_timeout(Duration::from_secs(5)).unwrap();
        }
        assert!(
            relay
                .stats
                .dropped
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
        );
        assert!(a.metrics().reliable_retransmits > 0);
        a.close().unwrap();
        b.close().unwrap();
    }
}

#[test]
fn empty_message_completes_when_grant_and_terminal_confirmation_are_lost() {
    let (_env, a, b, s, _relay) = relayed(
        support::Faults {
            drop_once: vec![(7, 0, 1), (11, 0, 0)],
            ..support::Faults::default()
        },
        config(65507),
    );
    let ticket = s.submit_reliable(&[]).unwrap();
    assert!(b.recv_timeout(Duration::from_secs(2)).unwrap().is_empty());
    assert_eq!(
        ticket
            .wait_timeout(Duration::from_secs(2))
            .unwrap()
            .application_bytes,
        0
    );
    assert_eq!(
        s.send_reliable(b"after empty").unwrap().application_bytes,
        11
    );
    assert_eq!(
        &*b.recv_timeout(Duration::from_secs(2)).unwrap(),
        b"after empty"
    );
    a.close().unwrap();
    b.close().unwrap();
}
