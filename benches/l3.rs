//! Matched L2/L3 whole-message windows. All L3 frames are checked in channel order.

mod support;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rimuru::benchmarking::Environment;
use std::{hint::black_box, time::Instant};
use support::{fixture::Fixture, matrix};

fn messages(criterion: &mut Criterion) {
    for &threads in matrix::PROCESSING_THREADS {
        let env = Environment::new(threads).unwrap();
        for &datagram in matrix::DATAGRAM_SIZES {
            for &channels in matrix::CHANNEL_COUNTS {
                let mut group = matrix::group(
                    criterion,
                    format!(
                        "{}/c{channels}/w{}",
                        matrix::group_name("ordered_loopback", threads, datagram),
                        matrix::WINDOW
                    ),
                );
                for &message in matrix::MESSAGE_SIZES {
                    let data = vec![0x5a; message];
                    group.throughput(Throughput::Bytes((message * matrix::WINDOW) as u64));
                    for (name, ordered, layout) in [
                        ("l2_slice", false, false),
                        ("l3_slice", true, false),
                        ("l2_layout_fill", false, true),
                        ("l3_layout_fill", true, true),
                    ] {
                        let mut fixture = Fixture::new(&env, datagram, matrix::WINDOW);
                        if ordered {
                            fixture.enable_ordered(channels).unwrap();
                        }
                        group.bench_with_input(
                            BenchmarkId::new(name, message),
                            &message,
                            |b, _| {
                                b.iter_custom(|iterations| {
                                    env.block_on(async {
                                        let start = Instant::now();
                                        for _ in 0..iterations {
                                            if ordered {
                                                fixture
                                                    .ordered_window(black_box(&data), layout)
                                                    .await
                                                    .unwrap();
                                            } else {
                                                fixture
                                                    .window(black_box(&data), true, layout)
                                                    .await
                                                    .unwrap();
                                            }
                                        }
                                        start.elapsed()
                                    })
                                    .unwrap()
                                });
                            },
                        );
                    }
                }
                group.finish();
            }
        }
    }
}

criterion_group!(benches, messages);
criterion_main!(benches);
