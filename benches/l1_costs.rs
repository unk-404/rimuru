//! Isolated CPU costs. These do not replace end-to-end UDP measurements.

mod support;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rayon::prelude::*;
use std::hint::black_box;
use support::matrix;

/// Sizes that separate cache-resident, frame-sized and parallel-threshold work.
const COST_SIZES: &[usize] = &[65_536, 1_048_576, 8_388_608];
const COST_THREADS: usize = 3;
const PARALLEL_CHUNK: usize = 1 << 20;
const SMALL_CHUNK: usize = 65_536;
const DATAGRAM_STRIDE: usize = 1185;
/// 65507-byte UDP payload minus the 15-byte header used by small frame indices.
const LARGE_DATAGRAM_STRIDE: usize = 65507 - 15;
const SEED: u32 = 0x1234_5678;

fn fast_append(seed: u32, bytes: &[u8]) -> u32 {
    rimuru::benchmarking::checksum(seed, bytes)
}

fn costs(criterion: &mut Criterion) {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(COST_THREADS)
        .build()
        .unwrap();
    let mut group = criterion.benchmark_group("l1_costs");
    matrix::configure_costs(&mut group);
    for &size in COST_SIZES {
        let data = vec![0x5a; size];
        let mut destination = vec![0; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::new("crc32c_contiguous", size),
            &size,
            |b, _| {
                b.iter(|| crc32c::crc32c_append(black_box(SEED), black_box(&data)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("crc_fast_contiguous", size),
            &size,
            |b, _| {
                b.iter(|| fast_append(black_box(SEED), black_box(&data)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("crc_fast_chunked_1185", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let mut digest = crc_fast::Digest::new_with_init_state(
                        crc_fast::CrcAlgorithm::Crc32Iscsi,
                        u64::from(!SEED),
                    );
                    for chunk in black_box(&data).chunks(DATAGRAM_STRIDE) {
                        digest.update(chunk);
                    }
                    digest.finalize()
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("crc_fast_chunked_65492", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let mut digest = crc_fast::Digest::new_with_init_state(
                        crc_fast::CrcAlgorithm::Crc32Iscsi,
                        u64::from(!SEED),
                    );
                    for chunk in black_box(&data).chunks(LARGE_DATAGRAM_STRIDE) {
                        digest.update(chunk);
                    }
                    digest.finalize()
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("crc_fast_append_65492", size),
            &size,
            |b, _| {
                b.iter(|| {
                    black_box(&data)
                        .chunks(LARGE_DATAGRAM_STRIDE)
                        .fold(SEED, fast_append)
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("copy_and_crc_chunks_65492", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let mut crc = SEED;
                    for (input, output) in black_box(&data)
                        .chunks(LARGE_DATAGRAM_STRIDE)
                        .zip(destination.chunks_mut(LARGE_DATAGRAM_STRIDE))
                    {
                        output.copy_from_slice(input);
                        crc = fast_append(crc, input);
                    }
                    black_box((crc, &destination));
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("copy_then_crc_contiguous", size),
            &size,
            |b, _| {
                b.iter(|| {
                    destination.copy_from_slice(black_box(&data));
                    let crc = fast_append(SEED, &destination);
                    black_box((crc, &destination));
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("crc_fast_rayon_1m", size),
            &size,
            |b, _| {
                b.iter(|| {
                    pool.install(|| {
                        let chunks: Vec<_> = black_box(&data)
                            .par_chunks(PARALLEL_CHUNK)
                            .map(|chunk| (fast_append(0, chunk), chunk.len()))
                            .collect();
                        chunks.into_iter().fold(SEED, |crc, (next, len)| {
                            rimuru::benchmarking::combine_checksums(crc, next, len)
                        })
                    })
                });
            },
        );
        group.bench_with_input(BenchmarkId::new("copy", size), &size, |b, _| {
            b.iter(|| {
                destination.copy_from_slice(black_box(&data));
                black_box(&mut destination);
            });
        });
        group.bench_with_input(
            BenchmarkId::new("crc32c_chunked_1185", size),
            &size,
            |b, _| {
                b.iter(|| {
                    black_box(&data)
                        .chunks(DATAGRAM_STRIDE)
                        .fold(SEED, crc32c::crc32c_append)
                });
            },
        );
        group.bench_with_input(BenchmarkId::new("crc32c_rayon_64k", size), &size, |b, _| {
            b.iter(|| {
                pool.install(|| {
                    let chunks: Vec<_> = black_box(&data)
                        .par_chunks(SMALL_CHUNK)
                        .map(|chunk| (crc32c::crc32c(chunk), chunk.len()))
                        .collect();
                    chunks.into_iter().fold(SEED, |crc, (next, len)| {
                        crc32c::crc32c_combine(crc, next, len)
                    })
                })
            });
        });
    }
    group.finish();
}

criterion_group!(benches, costs);
criterion_main!(benches);
