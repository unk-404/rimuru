//! Matched bounded-window Whole delivery. L2 also waits for remote COMPLETE.

mod support;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rimuru::benchmarking::Environment;
use std::{hint::black_box, time::Instant};
use support::{fixture::Fixture, matrix};

fn messages(criterion: &mut Criterion) {
    for &threads in matrix::PROCESSING_THREADS {
        let env = Environment::new(threads).unwrap();
        for &datagram in matrix::DATAGRAM_SIZES {
            let mut group = matrix::group(
                criterion,
                format!(
                    "{}/w{}",
                    matrix::group_name("reliable_loopback", threads, datagram),
                    matrix::WINDOW
                ),
            );
            for &message in matrix::MESSAGE_SIZES {
                group.throughput(Throughput::Bytes((message * matrix::WINDOW) as u64));
                let data = vec![0x5a; message];
                for (name, reliable, layout) in [
                    ("l1_slice", false, false),
                    ("l2_slice", true, false),
                    ("l1_layout_fill", false, true),
                    ("l2_layout_fill", true, true),
                ] {
                    let fixture = Fixture::new(&env, datagram, matrix::WINDOW);
                    group.bench_with_input(BenchmarkId::new(name, message), &message, |b, _| {
                        b.iter_custom(|iterations| {
                            env.block_on(async {
                                let start = Instant::now();
                                for _ in 0..iterations {
                                    fixture
                                        .window(black_box(&data), reliable, layout)
                                        .await
                                        .expect("bounded Criterion delivery");
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

criterion_group!(benches, messages);
criterion_main!(benches);
