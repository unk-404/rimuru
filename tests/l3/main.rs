#[path = "../../benches/support/relay.rs"]
mod support;
use rimuru::{Endpoint, EndpointConfig, Error, benchmarking::Environment};
use std::time::Duration;
fn config(d: usize) -> EndpointConfig {
    EndpointConfig {
        datagram_size: d,
        max_peers: 4,
        socket_buffer_bytes: 4 << 20,
        ..EndpointConfig::default()
    }
}
fn pair(d: usize) -> (Endpoint, Endpoint, rimuru::Session) {
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(d)).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(d)).unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    assert!(s.supports_ordered());
    (a, b, s)
}

#[tokio::test]
async fn channel_terminal_state_can_be_awaited() {
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    let session = a.connect_async(b.local_addr().unwrap()).await.unwrap();
    let channel = session.channel(7).unwrap();
    let (closed, observed) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(channel.close_async(), channel.wait_closed_async())
    })
    .await
    .expect("channel close timed out");
    assert_eq!(closed.unwrap(), observed);
    a.close_async().await;
    b.close_async().await;
}

#[test]
fn default_channel_empty_small_large_and_layout_are_ordered() {
    let (a, b, s) = pair(65507);
    let channel = s.channel(0).unwrap();
    for (seq, size) in [0, 1, 65536, 1048576].into_iter().enumerate() {
        let bytes = vec![seq as u8; size];
        let t = s.submit_ordered(&bytes).unwrap();
        assert_eq!(t.channel_sequence(), Some(seq as u32));
        let frame = b.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(frame.layer(), 3);
        assert_eq!(frame.channel_sequence(), Some(seq as u32));
        assert_eq!(frame.as_slice(), bytes);
        t.wait_timeout(Duration::from_secs(3)).unwrap();
    }
    let mut layout = channel.allocate_frame(65539).unwrap();
    assert_eq!(layout.frame_id(), 0);
    for i in 0..layout.segment_count() {
        layout.payload_mut(i).unwrap().fill(0x47);
    }
    let ticket = channel.submit_layout(layout).unwrap();
    let f = b.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(f.iter().all(|b| *b == 0x47));
    assert_eq!(f.channel_sequence(), Some(4));
    ticket.wait().unwrap();
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn pipeline_multiple_channels_and_both_directions() {
    let (a, b, s) = pair(1200);
    let c1 = s.channel(7).unwrap();
    let c2 = s.channel(8).unwrap();
    let reverse = b
        .session(a.local_addr().unwrap())
        .unwrap()
        .channel(7)
        .unwrap();
    let mut tickets = Vec::new();
    for n in 0..8 {
        tickets.push(c1.submit(&vec![n; 65536]).unwrap());
        tickets.push(c2.submit(&[n]).unwrap());
    }
    let mut next = [0, 0];
    for _ in 0..16 {
        let f = b.recv_timeout(Duration::from_secs(5)).unwrap();
        let lane = usize::from(f.channel() == 8);
        assert_eq!(f.channel_sequence(), Some(next[lane]));
        assert_eq!(f[0], next[lane] as u8);
        next[lane] += 1;
    }
    for t in tickets {
        t.wait_timeout(Duration::from_secs(5)).unwrap();
    }
    let t = reverse.submit(b"reverse").unwrap();
    assert_eq!(
        a.recv_timeout(Duration::from_secs(3))
            .unwrap()
            .channel_sequence(),
        Some(0)
    );
    t.wait().unwrap();
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn channel_close_is_confirmed_and_other_channels_remain_usable() {
    let (a, b, s) = pair(1200);
    let c = s.channel(5).unwrap();
    let t = c.submit(b"prefix").unwrap();
    let f = b.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(f.channel_sequence(), Some(0));
    t.wait().unwrap();
    let end = c.close().unwrap();
    assert!(end.peer_confirmed);
    assert_eq!(end.last_delivered, Some(0));
    assert!(matches!(c.submit(b"late"), Err(Error::ChannelClosed)));
    let event = b.channel_event_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(event.channel, 5);
    assert_eq!(event.end.last_delivered, Some(0));
    let other = s.channel(6).unwrap();
    let t = other.submit(b"other").unwrap();
    assert_eq!(&*b.recv_timeout(Duration::from_secs(3)).unwrap(), b"other");
    t.wait().unwrap();
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn concurrent_admission_uses_contiguous_owner_sequence() {
    let (a, b, s) = pair(16384);
    let c = s.channel(3).unwrap();
    let workers: Vec<_> = (0..8)
        .map(|n| {
            let c = c.clone();
            std::thread::spawn(move || c.submit(&[n]).unwrap())
        })
        .collect();
    let tickets: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    let mut seq: Vec<_> = tickets
        .iter()
        .map(|t| t.channel_sequence().unwrap())
        .collect();
    seq.sort_unstable();
    assert_eq!(seq, (0..8).collect::<Vec<_>>());
    for n in 0..8 {
        assert_eq!(
            b.recv_timeout(Duration::from_secs(3))
                .unwrap()
                .channel_sequence(),
            Some(n)
        );
    }
    for t in tickets {
        t.wait().unwrap();
    }
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn async_and_parallel_crc_use_the_implicit_driver() {
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
    let data = vec![0x33; 8 << 20];
    let t = env
        .block_on(s.submit_ordered_async(&data))
        .unwrap()
        .unwrap();
    assert_eq!(
        b.recv_timeout(Duration::from_secs(5)).unwrap().as_slice(),
        data
    );
    t.wait_timeout(Duration::from_secs(5)).unwrap();
    assert!(a.metrics().crc_parallel_jobs > 0);
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
    let s = a.connect(relay.address).unwrap();
    (env, a, b, s, relay)
}
#[test]
fn missing_predecessor_holds_successors_but_another_channel_progresses() {
    let cfg = EndpointConfig {
        reliable: rimuru::ReliableConfig {
            initial_rto: Duration::from_millis(30),
            message_timeout: Duration::from_secs(4),
            ..rimuru::ReliableConfig::default()
        },
        ..config(1200)
    };
    let (_env, a, b, s, _relay) = relayed(
        support::Faults {
            hold_first_frame: Duration::from_millis(180),
            ..support::Faults::default()
        },
        cfg,
    );
    let channel = s.channel(1).unwrap();
    let other = s.channel(2).unwrap();
    let first = channel.submit(&vec![0x31; 65536]).unwrap();
    let later = channel.submit(b"successor").unwrap();
    let independent = other.submit(b"independent").unwrap();
    let frame = b.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(frame.channel(), 2);
    independent.wait_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        later.wait_timeout(Duration::from_millis(30)),
        Err(Error::TimedOut)
    ));
    assert!(b.metrics().ordered_ready_frames > 0);
    for seq in 0..2 {
        let f = b.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(f.channel(), 1);
        assert_eq!(f.channel_sequence(), Some(seq));
    }
    first.wait_timeout(Duration::from_secs(3)).unwrap();
    later.wait_timeout(Duration::from_secs(3)).unwrap();
    assert!(b.metrics().ordered_hol_ns_max > 10_000_000);
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn failed_predecessor_terminates_channel_without_delivering_successor() {
    let (_env, a, b, s, relay) = relayed(
        support::Faults {
            hold_first_frame: Duration::from_secs(20),
            ..support::Faults::default()
        },
        config(1200),
    );
    let c = s.channel(1).unwrap();
    let first = c.submit(&vec![1; 65536]).unwrap();
    let later = c.submit(b"must not publish").unwrap();
    std::thread::sleep(Duration::from_millis(30));
    let result = first.cancel().unwrap();
    assert!(matches!(
        result,
        rimuru::DeliveryOutcome::Aborted(rimuru::AbortReason::Cancelled)
    ));
    assert!(matches!(
        later.wait_timeout(Duration::from_secs(3)),
        Err(Error::DeliveryAborted(rimuru::AbortReason::Cancelled))
    ));
    assert!(matches!(b.try_recv(), Err(Error::WouldBlock)));
    let event = b.channel_event_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(event.channel, 1);
    assert_eq!(event.end.last_delivered, None);
    assert!(event.end.peer_confirmed);
    assert!(
        relay
            .stats
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert!(matches!(c.submit(b"late"), Err(Error::ChannelClosed)));
    let other = s.channel(2).unwrap();
    let t = other.submit(b"unaffected").unwrap();
    assert_eq!(
        &*b.recv_timeout(Duration::from_secs(2)).unwrap(),
        b"unaffected"
    );
    t.wait().unwrap();
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn corruption_closes_the_ordered_channel_and_preserves_other_layers() {
    for (d, size) in [(1200, 65536), (65507, 1048576)] {
        let (_env, a, b, s, _relay) = relayed(
            support::Faults {
                corrupt_data_once: true,
                drop_once: vec![(12, 0, 1), (13, 0, 0)],
                ..support::Faults::default()
            },
            config(d),
        );
        let c = s.channel(3).unwrap();
        let t = c.submit(&vec![0x33; size]).unwrap();
        assert!(matches!(
            t.wait_timeout(Duration::from_secs(4)),
            Err(Error::DeliveryAborted(rimuru::AbortReason::Integrity))
        ));
        assert!(matches!(b.try_recv(), Err(Error::WouldBlock)));
        let event = b.channel_event_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(event.end.reason, rimuru::AbortReason::Integrity);
        s.send_reliable(b"L2 remains").unwrap();
        assert_eq!(
            &*b.recv_timeout(Duration::from_secs(3)).unwrap(),
            b"L2 remains"
        );
        a.close().unwrap();
        b.close().unwrap();
    }
}

#[test]
fn dropped_large_ordered_tail_is_recovered_once() {
    let (_env, a, b, s, relay) = relayed(
        support::Faults {
            drop_data_index_once: Some(16),
            ..support::Faults::default()
        },
        config(65507),
    );
    let data: Vec<u8> = (0..1048576).map(|i| (i % 251) as u8).collect();
    let at = std::time::Instant::now();
    let ticket = s.channel(3).unwrap().submit(&data).unwrap();
    let frame = b.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(frame.as_slice(), data);
    assert_eq!(frame.channel_sequence(), Some(0));
    ticket.wait_timeout(Duration::from_secs(5)).unwrap();
    println!("L3 dropped tail complete in {:?}", at.elapsed());
    assert_eq!(
        relay.stats.injected_drops[0].load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    std::thread::sleep(Duration::from_millis(100));
    assert!(matches!(b.try_recv(), Err(Error::WouldBlock)));
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn seeded_loss_and_control_retries_do_not_break_order_or_duplicate_frames() {
    let cfg = EndpointConfig {
        reliable: rimuru::ReliableConfig {
            initial_rto: Duration::from_millis(30),
            message_timeout: Duration::from_secs(8),
            ..rimuru::ReliableConfig::default()
        },
        ..config(4096)
    };
    let (_env, a, b, s, relay) = relayed(
        support::Faults {
            seed: 0x52494d555255,
            loss_ppm: 100_000,
            delay: Duration::from_millis(3),
            reorder_every: 7,
            duplicate_every: 11,
            ..support::Faults::default()
        },
        cfg,
    );
    let c = s.channel(7).unwrap();
    let mut tickets = Vec::new();
    for seq in 0..8 {
        tickets.push(c.submit(&vec![seq; 65536]).unwrap());
    }
    for seq in 0..8 {
        let f = b.recv_timeout(Duration::from_secs(8)).unwrap();
        assert_eq!(f.channel_sequence(), Some(seq));
        assert!(f.iter().all(|v| *v == seq as u8));
    }
    for t in tickets {
        t.wait_timeout(Duration::from_secs(8)).unwrap();
    }
    assert!(
        relay
            .stats
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert!(!relay.blackhole.load(std::sync::atomic::Ordering::Relaxed));
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn distinct_large_inflight_messages_remain_ordered_after_tail_loss() {
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
    let channel = s.channel(7).unwrap();
    let tickets: Vec<_> = payloads
        .iter()
        .map(|data| channel.submit(data).unwrap())
        .collect();
    for (index, expected) in payloads.iter().enumerate() {
        let frame = b.recv_timeout(Duration::from_secs(8)).unwrap();
        assert_eq!(frame.id(), tickets[index].frame_id());
        assert_eq!(frame.channel(), 7);
        assert_eq!(frame.channel_sequence(), Some(index as u32));
        assert_eq!(frame.as_slice(), expected);
    }
    for ticket in tickets {
        ticket.wait_timeout(Duration::from_secs(8)).unwrap();
    }
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
fn receiver_capacity_one_advances_after_application_releases_the_prefix() {
    let env = Environment::new(1).unwrap();
    let cfg = EndpointConfig {
        batch_size: 1,
        ..config(1200)
    };
    let a = env
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg.clone())
        .unwrap();
    let b = env
        .bind_endpoint(
            "127.0.0.1:0".parse().unwrap(),
            EndpointConfig {
                frame_capacity: 1,
                ..cfg
            },
        )
        .unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    let c = s.channel(0).unwrap();
    let mut tickets = Vec::new();
    for n in 0..4 {
        tickets.push(c.submit(&[n]).unwrap());
    }
    let held = b.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(held.channel_sequence(), Some(0));
    tickets[0].wait().unwrap();
    assert!(matches!(
        tickets[1].wait_timeout(Duration::from_millis(50)),
        Err(Error::TimedOut)
    ));
    drop(held);
    for n in 1..4 {
        assert_eq!(
            b.recv_timeout(Duration::from_secs(4))
                .unwrap()
                .channel_sequence(),
            Some(n)
        );
    }
    for t in tickets {
        t.wait_timeout(Duration::from_secs(4)).unwrap();
    }
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn resource_rejection_does_not_consume_a_channel_sequence() {
    let cfg = EndpointConfig {
        frame_capacity: 1,
        channel_window: 1,
        ..config(1200)
    };
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), cfg.clone()).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), cfg).unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    let c = s.channel(0).unwrap();
    let first = c.submit(&vec![0x55; 1 << 20]).unwrap();
    assert!(matches!(c.submit(b"rejected"), Err(Error::NoBuffer)));
    drop(b.recv_timeout(Duration::from_secs(3)).unwrap());
    first.wait().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let next = loop {
        match c.submit(b"second") {
            Ok(t) => break t,
            Err(Error::NoBuffer) => {
                assert!(std::time::Instant::now() < deadline);
                std::thread::yield_now();
            }
            Err(e) => panic!("{e}"),
        }
    };
    assert_eq!(next.channel_sequence(), Some(1));
    assert_eq!(
        b.recv_timeout(Duration::from_secs(3))
            .unwrap()
            .channel_sequence(),
        Some(1)
    );
    next.wait().unwrap();
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn lost_close_and_ack_preserve_the_published_prefix_and_direction() {
    let cfg = EndpointConfig {
        reliable: rimuru::ReliableConfig {
            initial_rto: Duration::from_millis(30),
            message_timeout: Duration::from_secs(3),
            ..rimuru::ReliableConfig::default()
        },
        ..config(1200)
    };
    let (_env, a, b, s, relay) = relayed(
        support::Faults {
            drop_once: vec![(12, 0, 0), (13, 0, 1)],
            ..support::Faults::default()
        },
        cfg,
    );
    let forward = s.channel(4).unwrap();
    let t = forward.submit(b"prefix").unwrap();
    let frame = b.recv_timeout(Duration::from_secs(2)).unwrap();
    let inc = frame.sender_incarnation();
    let peer = frame.peer_addr();
    t.wait().unwrap();
    let end = forward.close().unwrap();
    assert_eq!(end.last_delivered, Some(0));
    assert!(end.peer_confirmed);
    let event = b.channel_event_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(event.sender_incarnation, inc);
    assert_eq!(event.end.last_delivered, Some(0));
    let peer_session = b.session(peer).unwrap();
    let reverse = peer_session.channel(4).unwrap();
    let t = reverse.submit(b"reverse still open").unwrap();
    assert_eq!(
        &*a.recv_timeout(Duration::from_secs(2)).unwrap(),
        b"reverse still open"
    );
    t.wait().unwrap();
    assert!(
        relay
            .stats
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 2
    );
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn channel_blackhole_and_endpoint_close_resolve_waiters_with_unknown() {
    let cfg = EndpointConfig {
        reliable: rimuru::ReliableConfig {
            message_timeout: Duration::from_millis(120),
            initial_rto: Duration::from_millis(15),
            max_recovery_rounds: 8,
        },
        ..config(1200)
    };
    let (_env, a, b, s, relay) = relayed(support::Faults::default(), cfg);
    let c = s.channel(0).unwrap();
    relay
        .blackhole
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let t = c.submit(b"blackholed").unwrap();
    assert!(matches!(
        t.wait_timeout(Duration::from_secs(2)),
        Err(Error::OutcomeUnknown)
    ));
    let end = c.wait_closed_timeout(Duration::from_secs(1)).unwrap();
    assert!(!end.peer_confirmed);
    assert!(!s.is_closed());
    a.close().unwrap();
    b.close().unwrap();
    let (a, b, s) = pair(65507);
    let c = s.channel(1).unwrap();
    a.close().unwrap();
    assert!(c.end().is_some());
    assert!(c.is_closed());
    b.close().unwrap();
}
#[test]
fn channel_capacity_and_reopen_rules_are_bounded() {
    let cfg = EndpointConfig {
        max_channels: 1,
        ..config(1200)
    };
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), cfg.clone()).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), cfg).unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    let c = s.channel(0).unwrap();
    assert!(matches!(s.channel(1), Err(Error::NoBuffer)));
    c.close().unwrap();
    assert!(s.channel(0).unwrap().is_closed());
    assert!(matches!(s.channel(1), Err(Error::NoBuffer)));
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn ipv6_ordered_api_works_from_a_foreign_executor() {
    let a = Endpoint::bind("[::1]:0".parse().unwrap(), config(65507)).unwrap();
    let b = Endpoint::bind("[::1]:0".parse().unwrap(), config(65507)).unwrap();
    foreign(async {
        let s = a.connect_async(b.local_addr().unwrap()).await.unwrap();
        let c = s.channel_async(9).await.unwrap();
        c.send_async(b"foreign IPv6").await.unwrap();
        let f = b.recv_async().await.unwrap();
        assert_eq!(f.channel_sequence(), Some(0));
        assert_ne!(f.sender_incarnation(), 0);
        c.close_async().await.unwrap();
        assert_eq!(b.channel_event_async().await.unwrap().channel, 9);
        a.close_async().await;
        b.close_async().await;
    });
}
#[test]
fn reconnect_resets_sequence_but_frames_and_events_keep_their_incarnation() {
    let (a, b, s) = pair(1200);
    let old = s.channel(0).unwrap();
    let t = old.submit(b"old").unwrap();
    let held = b.recv_timeout(Duration::from_secs(2)).unwrap();
    t.wait().unwrap();
    let old_inc = held.sender_incarnation();
    a.disconnect(b.local_addr().unwrap()).unwrap();
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while b.session(a.local_addr().unwrap()).is_some() {
        assert!(std::time::Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    let fresh = a.connect(b.local_addr().unwrap()).unwrap();
    let t = fresh.submit_ordered(b"new").unwrap();
    let f = b.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(f.channel_sequence(), Some(0));
    assert_ne!(f.sender_incarnation(), old_inc);
    t.wait().unwrap();
    let event = b.channel_event_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(event.sender_incarnation, old_inc);
    assert!(old.is_closed());
    drop(held);
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn concurrent_receive_consumers_dequeue_each_sequence_exactly_once() {
    let (a, b, s) = pair(65507);
    let c = s.channel(0).unwrap();
    let mut tickets = Vec::new();
    for n in 0..16 {
        tickets.push(c.submit(&[n]).unwrap());
    }
    let consumers: Vec<_> = (0..4)
        .map(|_| {
            let b = b.clone();
            std::thread::spawn(move || {
                (0..4)
                    .map(|_| {
                        let f = b.recv_timeout(Duration::from_secs(3)).unwrap();
                        (f.channel_sequence().unwrap(), f[0])
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut out: Vec<_> = consumers
        .into_iter()
        .flat_map(|w| w.join().unwrap())
        .collect();
    out.sort_unstable();
    assert_eq!(out, (0..16).map(|n| (n, n as u8)).collect::<Vec<_>>());
    for t in tickets {
        t.wait().unwrap();
    }
    a.close().unwrap();
    b.close().unwrap();
}
fn foreign<F: std::future::Future>(future: F) -> F::Output {
    use std::{
        sync::Arc,
        task::{Context, Poll, Wake, Waker},
        time::Instant,
    };
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
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_millis(10));
            }
        }
    }
}
