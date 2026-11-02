use super::*;

#[test]
fn accelerated_crc_matches_reference_for_seeds_offsets_and_streams() {
    let data: Vec<_> = (0..(2 << 20) + 64)
        .map(|i| ((i * 131 + i / 13) % 251) as u8)
        .collect();
    assert_eq!(append(0, b"123456789"), 0xe3069283);
    for offset in 0..32 {
        for len in [
            0, 1, 15, 31, 127, 255, 256, 257, 1023, 1185, 4096, 65536, 1048576,
        ] {
            let bytes = &data[offset..offset + len];
            for seed in [0, 1, 0xffffffff, 0x12345678] {
                let expected = crc32c::crc32c_append(seed, bytes);
                assert_eq!(append(seed, bytes), expected, "offset={offset} len={len}");
                assert_eq!(chunks(seed, bytes.chunks(1185)), expected);
            }
        }
    }
}

#[test]
fn cached_combination_matches_reference_including_large_logical_lengths() {
    // Every basis vector of every power is checked, not only sampled CRCs.
    for (power, matrix) in SHIFTS.iter().enumerate() {
        for (bit, &value) in matrix.iter().enumerate() {
            assert_eq!(value, crc32c::crc32c_combine(1 << bit, 0, 1usize << power));
        }
    }
    let mut state = 0x12345678u32;
    for len in [0, 1, 7, 65536, 1048576, 1047310, 8388608, u32::MAX as usize] {
        for _ in 0..64 {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            let first = state;
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            assert_eq!(
                combine(first, state, len),
                crc32c::crc32c_combine(first, state, len)
            );
        }
    }
}
