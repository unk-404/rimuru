//! L1 Criterion: bounded stop-and-wait Whole frames against a raw-UDP control.
//!
//! Saturation capacity and the lossless 90% target are measured by `udp_goodput`.

mod support;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rimuru::benchmarking::Environment;
use std::{hint::black_box, time::Instant};
use support::{fixture::Fixture, matrix, raw::RawFixture};

fn frames(criterion: &mut Criterion) {
    for &threads in matrix::PROCESSING_THREADS {
        let env = Environment::new(threads).unwrap();
        for &datagram in matrix::DATAGRAM_SIZES {
            let mut group = matrix::group(
                criterion,
                matrix::group_name("frame_loopback", threads, datagram),
            );
            for &message in matrix::FRAME_SIZES {
                group.throughput(Throughput::Bytes(message as u64));
                let data = vec![0x5a; message];
                let mut raw = RawFixture::new(&env, datagram, message);
                group.bench_with_input(BenchmarkId::new("raw_udp", message), &message, |b, _| {
                    b.iter_custom(|iterations| {
                        env.block_on(async {
                            let start = Instant::now();
                            for _ in 0..iterations {
                                raw.one(black_box(&data)).await;
                            }
                            start.elapsed()
                        })
                        .unwrap()
                    });
                });
                drop(raw);
                let fixture = Fixture::new(&env, datagram, 1);
                for (name, layout) in [("l1_send", false), ("l1_layout_fill", true)] {
                    group.bench_with_input(BenchmarkId::new(name, message), &message, |b, _| {
                        b.iter_custom(|iterations| {
                            env.block_on(async {
                                let start = Instant::now();
                                for _ in 0..iterations {
                                    fixture
                                        .window(black_box(&data), false, layout)
                                        .await
                                        .unwrap();
                                }
                                start.elapsed()
                            })
                            .unwrap()
                        });
                    });
                }
            }
            group.finish();
        }
    }
}

criterion_group!(benches, frames);
criterion_main!(benches);
