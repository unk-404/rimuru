//! Ordered-message comparison on one local connection: Rimuru L3 vs Quinn stream.
//!
//! Quinn provides encryption and stream semantics; this is an ordered-stream
//! reference, not a claim of transport equivalence. Run order is counterbalanced.

mod support;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rimuru::benchmarking::Environment;
use std::time::{Duration, Instant};
use support::{fixture::Fixture, matrix, quinn};

const MESSAGE_BYTES: usize = 1_048_576;
const CHANNELS: usize = 1;
const WINDOW: usize = 4;
const DATAGRAM: usize = 1200;

fn ordered_comparison(criterion: &mut Criterion) {
    let env = Environment::new(1).unwrap();
    let data = vec![0x5a; MESSAGE_BYTES];
    let mut rimuru = Fixture::new(&env, DATAGRAM, WINDOW);
    rimuru.enable_ordered(CHANNELS).unwrap();
    let mut quinn = env
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(15),
                quinn::Fixture::new(DATAGRAM as u16),
            )
            .await
            .expect("Quinn connection setup timed out")
        })
        .unwrap();
    for (pass, order) in [
        (1, ["rimuru_l3", "quinn_stream"]),
        (2, ["quinn_stream", "rimuru_l3"]),
    ] {
        let mut group = matrix::group(
            criterion,
            format!("ordered_comparison/pass{pass}/t1/d{DATAGRAM}/c{CHANNELS}/w{WINDOW}"),
        );
        group.throughput(Throughput::Bytes((WINDOW * MESSAGE_BYTES) as u64));
        for name in order {
            group.bench_function(name, |b| {
                b.iter_custom(|iterations| {
                    env.block_on(async {
                        let start = Instant::now();
                        for _ in 0..iterations {
                            if name == "rimuru_l3" {
                                rimuru.ordered_window(&data, false).await.unwrap();
                            } else {
                                tokio::time::timeout(Duration::from_secs(10), quinn.window(&data))
                                    .await
                                    .expect("Quinn message window timed out");
                            }
                        }
                        start.elapsed()
                    })
                    .unwrap()
                })
            });
        }
        group.finish();
    }
}

criterion_group!(benches, ordered_comparison);
criterion_main!(benches);
