use super::mask::Segments;

#[test]
fn ordered_crc_matches_full_scan_and_falls_back_on_reordering() {
    use super::assembly::Assembly as Partial;
    use crate::{
        buffer::{Budget, Permit, Pool},
        wire::Begin,
    };
    use std::{
        sync::{Arc, atomic::AtomicUsize},
        time::Instant,
    };
    let budget = Budget::new(1 << 20, None);
    let pool = Pool::new(budget.clone(), 0);
    let permits = Arc::new(AtomicUsize::new(0));
    let data: Vec<u8> = (0..150_000).map(|i| (i % 251) as u8).collect();
    let id = 73;
    let mut meta = Begin::new(data.len(), 65507, 1).unwrap();
    meta.crc = crate::checksum::append(meta.seed(id), &data).into();
    let stride = meta.stride.get() as usize;
    for (order, corrupt) in [
        ([0, 1, 2], false),
        ([1, 0, 2], false),
        ([0, 1, 2], true),
        ([1, 0, 2], true),
    ] {
        let mut a = Partial {
            meta,
            bytes: pool.take(data.len()).unwrap(),
            received: Segments::new(3).unwrap(),
            remaining: 3,
            started: Instant::now(),
            charge: budget.reserve(512).unwrap(),
            permit: Permit::take(&permits, 1).unwrap(),
            ordered_crc: Some((0, 0)),
        };
        for index in order {
            let start = index as usize * stride;
            let end = (start + stride).min(data.len());
            let mut payload = data[start..end].to_vec();
            if corrupt && index == 1 {
                payload[0] ^= 1;
            }
            assert!(a.ingest(index, meta.idx_bytes, &payload).unwrap());
            assert!(!a.ingest(index, meta.idx_bytes, &payload).unwrap());
        }
        assert_eq!(a.ordered_crc.is_some(), order[0] == 0);
        assert_eq!(a.verify(id, false), !corrupt);
        assert!(!a.verify(id + 1, false));
    }
}

#[test]
fn ordered_crc_varied_frame_boundaries_and_duplicate_payloads() {
    use super::assembly::Assembly as Partial;
    use crate::{
        buffer::{Budget, Permit, Pool},
        wire::Begin,
    };
    use std::{
        sync::{Arc, atomic::AtomicUsize},
        time::Instant,
    };
    let budget = Budget::new(16 << 20, None);
    let pool = Pool::new(budget.clone(), 0);
    let permits = Arc::new(AtomicUsize::new(0));
    for size in [1, 65491, 65492, 65493, 1048576, 4192250] {
        let data: Vec<u8> = (0..size).map(|i| ((i * 17 + size) % 251) as u8).collect();
        let id = 127;
        let mut meta = Begin::new(size, 65507, 1).unwrap();
        meta.crc = crate::checksum::append(meta.seed(id), &data).into();
        let count = meta.count.get();
        let stride = meta.stride.get() as usize;
        for reverse in [false, true] {
            let mut assembly = Partial {
                meta,
                bytes: pool.take(size).unwrap(),
                received: Segments::new(count as usize).unwrap(),
                remaining: count,
                started: Instant::now(),
                charge: budget.reserve(512).unwrap(),
                permit: Permit::take(&permits, 1).unwrap(),
                ordered_crc: Some((0, 0)),
            };
            let mut order: Vec<_> = (0..count).collect();
            if reverse {
                order.reverse();
            }
            for index in order {
                let start = index as usize * stride;
                let end = (start + stride).min(size);
                let payload = &data[start..end];
                assert!(assembly.ingest(index, meta.idx_bytes, payload).unwrap());
                let mut changed = payload.to_vec();
                changed[0] ^= 1;
                assert!(!assembly.ingest(index, meta.idx_bytes, &changed).unwrap());
            }
            assert_eq!(&*assembly.bytes, data.as_slice());
            assert!(assembly.verify(id, false), "size={size} reverse={reverse}");
            assert_eq!(assembly.ordered_crc.is_some(), !reverse || count == 1);
        }
    }
}

#[test]
fn hot_and_interleaved_contexts_survive_pressure_and_release_storage() {
    use super::{assembly::Assembly as Partial, contexts::Contexts};
    use crate::{
        buffer::{Budget, Permit, Pool},
        wire::Begin,
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering::Relaxed},
        },
        time::{Duration, Instant},
    };
    let budget = Budget::new(65536, None);
    let pool = Pool::new(budget.clone(), 0);
    let permits = Arc::new(AtomicUsize::new(0));
    let partial = |value| {
        let mut bytes = pool.take(1).unwrap();
        bytes[0] = value;
        Partial {
            meta: Begin::new(1, 1200, 2).unwrap(),
            bytes,
            received: Segments::new(1).unwrap(),
            remaining: 1,
            started: Instant::now(),
            charge: budget.reserve(512).unwrap(),
            permit: Permit::take(&permits, 8).unwrap(),
            ordered_crc: None,
        }
    };
    let mut contexts = Contexts::new(budget.clone());
    let one = partial(1);
    let used = budget.used();
    contexts.insert(1, one).unwrap();
    assert_eq!(budget.used(), used, "hot storage must not allocate a table");
    let two = partial(2);
    let used = budget.used();
    contexts.insert(2, two).unwrap();
    assert!(budget.used() > used, "cold table must be charged");
    assert_eq!(contexts.get(1).unwrap().bytes[0], 1);
    let old = contexts.remove(1).unwrap();
    let used = budget.used();
    drop(old);
    assert!(budget.used() < used);
    let three = partial(3);
    let pressure = budget.reserve(65536 - budget.used() - 1).unwrap();
    assert!(contexts.insert(3, three).is_err());
    assert_eq!(contexts.get(2).unwrap().bytes[0], 2);
    assert!(contexts.get(3).is_none());
    drop(pressure);
    contexts.insert(3, partial(3)).unwrap();
    contexts.get_mut(2).unwrap().bytes[0] = 0x55;
    assert_eq!(contexts.get(2).unwrap().bytes[0], 0x55);
    assert_eq!(
        contexts.expire(
            Instant::now() + Duration::from_secs(2),
            Duration::from_secs(1)
        ),
        2
    );
    assert_eq!(permits.load(Relaxed), 0);
    assert_eq!(budget.used(), 0);
}

#[test]
fn inline_and_heap_masks_preserve_every_boundary_bit() {
    let inline = super::mask::INLINE_BITS;
    for count in [0, 1, inline - 1, inline, inline + 1, 885, 65537] {
        let mut bits = Segments::new(count).unwrap();
        assert_eq!(matches!(bits, Segments::Inline(_)), count <= inline);
        for index in 0..count {
            assert!(!bits.contains(index));
        }
        for index in (0..count).step_by(2) {
            bits.insert(index);
        }
        for index in 0..count {
            assert_eq!(bits.contains(index), index % 2 == 0);
        }
        for index in 0..count {
            bits.insert(index);
            assert!(bits.contains(index));
        }
    }
}
