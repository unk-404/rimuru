use rimuru::{Endpoint, EndpointConfig, Error, Integrity, benchmarking::Environment};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    thread,
    time::Duration,
};

fn runtime() -> Environment {
    Environment::new(3).unwrap()
}

#[test]
fn recv_future_sleeps_until_a_frame_wakes_it() {
    use std::{
        future::Future,
        sync::mpsc,
        task::{Context, Poll, Wake, Waker},
    };

    struct Notify(mpsc::Sender<()>);
    impl Wake for Notify {
        fn wake(self: Arc<Self>) {
            let _ = self.0.send(());
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self.0.send(());
        }
    }

    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    let session = a.connect(b.local_addr().unwrap()).unwrap();
    let (wake_tx, wake_rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(Notify(wake_tx)));
    let mut context = Context::from_waker(&waker);
    let mut receive = std::pin::pin!(b.recv_async());
    assert!(matches!(receive.as_mut().poll(&mut context), Poll::Pending));
    assert!(matches!(
        wake_rx.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    session.send(b"event driven").unwrap();
    wake_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    match receive.as_mut().poll(&mut context) {
        Poll::Ready(Ok(frame)) => assert_eq!(frame.as_slice(), b"event driven"),
        _ => panic!("receive was not ready after notification"),
    }
    a.close().unwrap();
    b.close().unwrap();
}

#[tokio::test]
async fn async_batch_waits_for_data_and_drains_available_frames() {
    let sender = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    let receiver = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    let session = sender
        .connect_async(receiver.local_addr().unwrap())
        .await
        .unwrap();
    let mut frames = Vec::new();
    assert!(matches!(
        receiver.recv_batch_async(&mut frames, 0).await,
        Err(Error::InvalidConfig(_))
    ));
    session.send_async(b"one").await.unwrap();
    session.send_async(b"two").await.unwrap();
    while frames.len() < 2 {
        let count = tokio::time::timeout(
            Duration::from_secs(2),
            receiver.recv_batch_async(&mut frames, 2),
        )
        .await
        .expect("batch wake timed out")
        .unwrap();
        assert!((1..=2).contains(&count));
    }
    let mut received: Vec<_> = frames.iter().map(|f| f.as_slice()).collect();
    received.sort_unstable();
    assert_eq!(received, [b"one".as_slice(), b"two".as_slice()]);
    sender.close_async().await;
    receiver.close_async().await;
}

#[test]
fn async_api_does_not_require_a_callers_tokio_context() {
    use std::{
        future::Future,
        task::{Context, Poll, Wake, Waker},
        time::Instant,
    };
    struct ThreadWake(thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    fn drive<F: Future>(future: F) -> F::Output {
        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => {
                    assert!(Instant::now() < deadline, "foreign executor stalled");
                    thread::park_timeout(Duration::from_millis(10));
                }
            }
        }
    }
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    drive(async {
        let session = a.connect_async(b.local_addr().unwrap()).await.unwrap();
        session.send_async(b"foreign executor").await.unwrap();
        assert_eq!(
            b.recv_async().await.unwrap().as_slice(),
            b"foreign executor"
        );
        a.close_async().await;
        b.close_async().await;
    });
}

#[test]
fn normal_api_needs_no_runtime_and_handles_move_between_threads() {
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config(1200)).unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    thread::spawn(move || s.send(b"implicit runtime").unwrap())
        .join()
        .unwrap();
    assert_eq!(
        b.recv_timeout(Duration::from_secs(2)).unwrap().as_slice(),
        b"implicit runtime"
    );
    a.close().unwrap();
    b.close().unwrap();
}
fn config(d: usize) -> EndpointConfig {
    EndpointConfig {
        datagram_size: d,
        max_frame_size: 2 << 20,
        memory_budget: 32 << 20,
        max_peers: 16,
        socket_buffer_bytes: 4 << 20,
        frame_capacity: 32,
        ..EndpointConfig::default()
    }
}

#[test]
fn large_datagram_vectored_slice_sends_preserve_payload_and_empty_frames() {
    let r = runtime();
    for d in [16384, 65507] {
        let (a, b) = pair(&r, d);
        let s = a.connect(b.local_addr().unwrap()).unwrap();
        for len in [0, 1, 65536, 1048576] {
            let data: Vec<_> = (0..len).map(|i| (i % 251) as u8).collect();
            s.send(&data).unwrap();
            let received = b
                .recv_timeout(Duration::from_secs(3))
                .unwrap_or_else(|error| {
                    panic!(
                        "L1 D={d} length={len}: {error}; tx={:?}; rx={:?}",
                        a.metrics(),
                        b.metrics()
                    )
                });
            assert_eq!(received.as_slice(), data);
        }
        a.close().unwrap();
        b.close().unwrap();
    }
}

fn pair(r: &Environment, d: usize) -> (Endpoint, Endpoint) {
    let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
    (
        r.bind_endpoint(addr, config(d)).unwrap(),
        r.bind_endpoint(addr, config(d)).unwrap(),
    )
}

#[test]
fn handshake_empty_fragmented_large_and_bidirectional_frames() {
    let r = runtime();
    let (a, b) = pair(&r, 1200);
    let session = a.connect(b.local_addr().unwrap()).unwrap();
    for n in [0, 1, 1185, 1186, 65536, 1 << 20] {
        let data: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        session.send(&data).unwrap();
        let got = b
            .recv_timeout(Duration::from_secs(3))
            .unwrap_or_else(|error| {
                panic!(
                    "L1 D=1200 length={n}: {error}; tx={:?}; rx={:?}",
                    a.metrics(),
                    b.metrics()
                )
            });
        assert_eq!(got.as_slice(), data);
        assert_eq!(got.peer_addr(), a.local_addr().unwrap());
    }
    b.session(a.local_addr().unwrap())
        .unwrap()
        .send(b"reply")
        .unwrap();
    assert_eq!(
        a.recv_timeout(Duration::from_secs(2)).unwrap().as_slice(),
        b"reply"
    );
    assert_eq!(a.metrics().bound_udp_sockets, 1);
    assert_eq!(b.metrics().bound_udp_sockets, 1);
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn shared_handles_multiple_peers_and_receive_consumers() {
    let r = runtime();
    let (a, b) = pair(&r, 4096);
    let c = r
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), config(4096))
        .unwrap();
    let sa = a.connect(b.local_addr().unwrap()).unwrap();
    let sc = c.connect(b.local_addr().unwrap()).unwrap();
    let receivers: Vec<_> = (0..2)
        .map(|_| {
            let b = b.clone();
            thread::spawn(move || b.recv_timeout(Duration::from_secs(3)).unwrap())
        })
        .collect();
    let senders = [
        thread::spawn(move || sa.send(b"a").unwrap()),
        thread::spawn(move || sc.send(b"c").unwrap()),
    ];
    for sender in senders {
        sender.join().unwrap();
    }
    let mut bodies: Vec<_> = receivers
        .into_iter()
        .map(|t| t.join().unwrap().as_slice().to_vec())
        .collect();
    bodies.sort();
    assert_eq!(bodies, vec![b"a".to_vec(), b"c".to_vec()]);
    a.close().unwrap();
    b.close().unwrap();
    c.close().unwrap();
}

#[test]
fn application_held_frame_keeps_budget_and_admission_until_final_clone_drop() {
    let r = runtime();
    let mut cfg = config(1200);
    cfg.frame_capacity = 1;
    let a = r
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg.clone())
        .unwrap();
    let b = r
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg)
        .unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    s.send(b"held").unwrap();
    let held = b.recv_timeout(Duration::from_secs(2)).unwrap();
    let clone = held.clone();
    drop(held);
    s.send(b"cannot admit").unwrap();
    thread::sleep(Duration::from_millis(80));
    assert!(matches!(b.try_recv(), Err(Error::WouldBlock)));
    assert!(b.metrics().resource_drops > 0);
    drop(clone);
    s.send(b"released").unwrap();
    assert_eq!(
        b.recv_timeout(Duration::from_secs(2)).unwrap().as_slice(),
        b"released"
    );
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn layout_pool_ownership_and_limits() {
    let r = runtime();
    let (a, b) = pair(&r, 1200);
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    assert!(matches!(
        s.allocate_frame((2 << 20) + 1),
        Err(Error::MessageTooBig)
    ));
    let mut layout = s.allocate_frame(5000).unwrap();
    for i in 0..layout.segment_count() {
        layout.payload_mut(i).unwrap().fill(42);
    }
    s.send_layout(layout).unwrap();
    let frame = b.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(frame.len(), 5000);
    assert!(frame.iter().all(|&b| b == 42));
    b.close().unwrap();
    assert!(frame.iter().all(|&b| b == 42)); // pool outlives endpoint close
    a.close().unwrap();
}

#[test]
fn close_wakes_all_receivers_and_rejects_cross_thread_send() {
    let r = runtime();
    let (a, b) = pair(&r, 1200);
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let a = a.clone();
            thread::spawn(move || a.recv_timeout(Duration::from_secs(2)))
        })
        .collect();
    a.close().unwrap();
    for t in threads {
        assert!(matches!(t.join().unwrap(), Err(Error::Closed)));
    }
    assert!(matches!(
        thread::spawn(move || s.send(b"late")).join().unwrap(),
        Err(Error::Closed)
    ));
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn simultaneous_open_and_multiple_connect_waiters() {
    let r = runtime();
    let (a, b) = pair(&r, 1200);
    let aa = a.local_addr().unwrap();
    let ba = b.local_addr().unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let workers: Vec<_> = [(a.clone(), ba), (b.clone(), aa)]
        .into_iter()
        .map(|(ep, peer)| {
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                ep.connect(peer).unwrap()
            })
        })
        .collect();
    barrier.wait();
    for w in workers {
        w.join().unwrap().send(b"simultaneous").unwrap();
    }
    assert_eq!(
        a.recv_timeout(Duration::from_secs(2)).unwrap().as_slice(),
        b"simultaneous"
    );
    assert_eq!(
        b.recv_timeout(Duration::from_secs(2)).unwrap().as_slice(),
        b"simultaneous"
    );
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn ipv6_and_negotiated_datagram_size() {
    let r = runtime();
    let address = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
    let a = r.bind_endpoint(address, config(16384)).unwrap();
    let b = r.bind_endpoint(address, config(1200)).unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    assert_eq!(s.datagram_size(), 1200);
    s.send(&vec![5; 32000]).unwrap();
    assert_eq!(b.recv_timeout(Duration::from_secs(2)).unwrap().len(), 32000);
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn ipv6_large_frame_preserves_full_fragments_and_tail() {
    let runtime = runtime();
    let address = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
    let a = runtime.bind_endpoint(address, config(65507)).unwrap();
    let b = runtime.bind_endpoint(address, config(65507)).unwrap();
    let session = a.connect(b.local_addr().unwrap()).unwrap();
    let data: Vec<_> = (0..(1 << 20) + 31).map(|i| (i % 251) as u8).collect();
    session.send(&data).unwrap();
    let frame = b
        .recv_timeout(Duration::from_secs(3))
        .unwrap_or_else(|error| {
            panic!(
                "IPv6 L1 D=65507 length={}: {error}; tx={:?}; rx={:?}",
                data.len(),
                a.metrics(),
                b.metrics()
            )
        });
    assert_eq!(frame.as_slice(), data);
    a.close().unwrap();
    b.close().unwrap();
    assert_eq!(frame.as_slice(), data);
}

#[test]
fn explicit_udp_only_and_async_api() {
    let r = runtime();
    let mut cfg = config(1200);
    cfg.integrity = Integrity::UdpChecksum;
    let a = r
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg.clone())
        .unwrap();
    let b = r
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg)
        .unwrap();
    r.block_on(async {
        assert!(matches!(
            a.connect(b.local_addr().unwrap()),
            Err(Error::WouldDeadlock)
        ));
        let s = a.connect_async(b.local_addr().unwrap()).await.unwrap();
        s.send_async(b"async").await.unwrap();
        assert_eq!(b.recv_async().await.unwrap().as_slice(), b"async");
        a.close_async().await;
        b.close_async().await;
    })
    .unwrap();
}
