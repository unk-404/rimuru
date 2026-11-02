use crate::{Endpoint, EndpointConfig, Runtime, RuntimeConfig};

#[test]
fn coarse_train_crc_matches_contiguous_reference_across_headers_and_tail() {
    let runtime = Runtime::new(RuntimeConfig {
        io_threads: 2,
        cpu_threads: 3,
        memory_budget: 128 << 20,
    })
    .unwrap();
    let cfg = EndpointConfig {
        max_peers: 4,
        ..EndpointConfig::default()
    };
    let a = Endpoint::bind_on(&runtime, "127.0.0.1:0".parse().unwrap(), cfg.clone()).unwrap();
    let b = Endpoint::bind_on(&runtime, "127.0.0.1:0".parse().unwrap(), cfg).unwrap();
    let session = a.connect(b.local_addr().unwrap()).unwrap();
    let data: Vec<_> = (0..(8 << 20) + 17).map(|i| (i % 251) as u8).collect();
    let mut layout = session.allocate_frame(data.len()).unwrap();
    let mut offset = 0;
    for i in 0..layout.segment_count() {
        let slot = layout.payload_mut(i).unwrap();
        slot.copy_from_slice(&data[offset..offset + slot.len()]);
        offset += slot.len();
    }
    let expected = crc32c::crc32c_append(layout.meta.seed(layout.id), &data);
    assert_eq!(layout.checksum(false), expected);
    assert_eq!(
        runtime
            .0
            .compute
            .as_ref()
            .unwrap()
            .install(|| layout.checksum(true)),
        expected
    );
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn compact_reliable_storage_preserves_crc_geometry_and_uses_half_a_train_class() {
    let a = Endpoint::bind(
        "127.0.0.1:0".parse().unwrap(),
        EndpointConfig {
            datagram_size: 65507,
            max_peers: 4,
            ..EndpointConfig::default()
        },
    )
    .unwrap();
    let b = Endpoint::bind(
        "127.0.0.1:0".parse().unwrap(),
        EndpointConfig {
            datagram_size: 65507,
            max_peers: 4,
            ..EndpointConfig::default()
        },
    )
    .unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    let data: Vec<_> = (0..1 << 20).map(|i| (i % 251) as u8).collect();
    let before = a.metrics().memory_used;
    let mut packed = s.retain_reliable_slice(&data).unwrap();
    let packed_charge = a.metrics().memory_used - before;
    assert_eq!(packed.bytes.len(), data.len());
    let expected = crc32c::crc32c_append(packed.meta.seed(packed.id), &data);
    assert_eq!(packed.checksum(false), expected);
    assert_eq!(packed.checksum(true), expected);
    let mut offset = 0;
    for i in 0..packed.segment_count() {
        let slot = packed.payload_mut(i).unwrap();
        assert_eq!(slot, &data[offset..offset + slot.len()]);
        offset += slot.len();
    }
    assert_eq!(offset, data.len());
    assert!(packed.payload_mut(packed.segment_count()).is_none());
    let before = a.metrics().memory_used;
    let train = s.allocate_frame(data.len()).unwrap();
    let train_charge = a.metrics().memory_used - before;
    assert!(
        train_charge > packed_charge * 19 / 10,
        "train={train_charge}, compact={packed_charge}"
    );
    drop(train);
    drop(packed);
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn compact_reliable_data32_preserves_header_index_and_tail_without_huge_allocation() {
    let config = EndpointConfig {
        datagram_size: 16384,
        max_peers: 4,
        ..EndpointConfig::default()
    };
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config.clone()).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), config).unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    let data: Vec<_> = (0..(1 << 20) + 37).map(|i| (i % 251) as u8).collect();
    let mut packed = s.retain_reliable_slice(&data).unwrap();
    packed.meta.idx_bytes = 4;
    packed.meta.stride = (16384u32 - 17).into();
    packed.meta.count = (data.len().div_ceil(16384 - 17) as u32).into();
    let ticket = s.submit_reliable_layout(packed).unwrap();
    assert_eq!(
        b.recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
            .as_slice(),
        data
    );
    ticket
        .wait_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    a.close().unwrap();
    b.close().unwrap();
}
