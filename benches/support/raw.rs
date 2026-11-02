//! Raw-UDP control fixture: the same datagram count a Whole frame needs, without
//! Rimuru framing. It is a control workload, not a protocol-equivalent baseline.

use rimuru::{benchmarking::Environment, udp::DatagramSocket};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};

/// One sender socket and one receiver task. The receiver signals after the exact
/// number of datagrams that carry the benchmark message have been observed.
pub struct RawFixture {
    tx: Arc<DatagramSocket>,
    peer: SocketAddr,
    datagram: usize,
    complete: mpsc::Receiver<usize>,
    stop: Option<oneshot::Sender<()>>,
    worker: tokio::task::JoinHandle<()>,
}

impl RawFixture {
    pub fn new(env: &Environment, datagram: usize, message: usize) -> Self {
        let tx = env.bind_socket(super::matrix::loopback(), 4 << 20).unwrap();
        let rx = env.bind_socket(super::matrix::loopback(), 4 << 20).unwrap();
        let peer = rx.local_addr().unwrap();
        let (send, complete) = mpsc::channel(1);
        let (stop, mut stopping) = oneshot::channel();
        let datagrams = message.div_ceil(datagram);
        let worker = env.spawn(async move {
            let mut buffer = vec![0; 65_536];
            let mut count = 0;
            let mut bytes = 0;
            loop {
                tokio::select! {
                    _ = &mut stopping => break,
                    result = rx.recv_from(&mut buffer) => {
                        let (len, _) = result.unwrap();
                        count += 1;
                        bytes += len;
                        if count == datagrams {
                            if send.send(bytes).await.is_err() {
                                break;
                            }
                            count = 0;
                            bytes = 0;
                        }
                    }
                }
            }
        });
        Self {
            tx,
            peer,
            datagram,
            complete,
            stop: Some(stop),
            worker,
        }
    }

    pub async fn one(&mut self, data: &[u8]) {
        for (index, chunk) in data.chunks(self.datagram).enumerate() {
            self.tx.send_to(chunk, self.peer).await.unwrap();
            if (index + 1) % 32 == 0 {
                tokio::task::yield_now().await;
            }
        }
        let count = tokio::time::timeout(Duration::from_secs(5), self.complete.recv())
            .await
            .expect("raw UDP loss in bounded Criterion fixture")
            .unwrap();
        assert_eq!(count, data.len());
        std::hint::black_box(count);
    }
}

impl Drop for RawFixture {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.worker.abort();
    }
}
