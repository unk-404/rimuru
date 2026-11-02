use super::{
    AbortReason,
    ledger::{Ledger, Terminal},
};
use crate::buffer::Budget;
#[test]
fn terminal_ranges_merge_without_time_based_replay_reset() {
    let mut ledger = Ledger::new(Budget::new(1 << 20, None));
    for id in [3, 1, 2, 5, 4] {
        ledger.admit(0).unwrap();
        ledger.insert(id, Terminal::Complete);
    }
    for id in 1..=5 {
        assert_eq!(ledger.get(id), Some(Terminal::Complete));
    }
    assert_eq!(ledger.get(6), None);
    ledger.admit(0).unwrap();
    ledger.insert(6, Terminal::Abort(AbortReason::Integrity));
    assert_eq!(ledger.get(6), Some(Terminal::Abort(AbortReason::Integrity)));
}

use super::{ReliablePeer, Signal, control, recovery::Recovery, tx::Tx};

#[test]
fn adaptive_loopback_rto_has_scheduling_margin() {
    let mut local = Recovery::new(1200, Duration::from_millis(30), true);
    let mut remote = Recovery::new(1200, Duration::from_millis(30), false);
    local.ack(0, Some(Duration::from_micros(10)));
    remote.ack(0, Some(Duration::from_micros(10)));
    assert_eq!(local.rto, Duration::from_millis(4));
    assert_eq!(remote.rto, Duration::from_millis(1));
}
use crate::{
    Endpoint, EndpointConfig, Error, Session,
    buffer::Permit,
    wire::{self, Begin, Header, Range},
};
use std::{
    sync::atomic::Ordering::Relaxed,
    time::{Duration, Instant},
};
use zerocopy::IntoBytes;
fn context() -> (Endpoint, Endpoint, Session, ReliablePeer) {
    let cfg = EndpointConfig {
        max_peers: 4,
        frame_timeout: Duration::from_millis(1),
        ..EndpointConfig::default()
    };
    let a = Endpoint::bind("127.0.0.1:0".parse().unwrap(), cfg.clone()).unwrap();
    let b = Endpoint::bind("127.0.0.1:0".parse().unwrap(), cfg).unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    let peer = ReliablePeer::new(s.owner.shared.clone(), s.state.clone()).unwrap();
    (a, b, s, peer)
}
fn tx(s: &Session, bytes: usize) -> Tx {
    let layout = s.allocate_frame(bytes).unwrap();
    let signal = Signal::new(s.owner.shared.pool.budget.reserve(256).unwrap());
    let permit = Permit::take(&s.owner.shared.outgoing_reliable, 256).unwrap();
    Tx::new(layout, signal, permit, s.owner.shared.pool.budget.clone()).unwrap()
}
fn begin(id: u32, bytes: &[u8]) -> Begin {
    let mut m = Begin::new(bytes.len(), 1200, 1).unwrap();
    m.layer = 2;
    m.crc = crate::checksum::append(m.seed(id), bytes).into();
    m
}
#[test]
fn ungranted_data_allocates_nothing_and_duplicate_grant_is_idempotent() {
    let (a, b, s, mut p) = context();
    let used = s.owner.shared.pool.budget.used();
    p.data(100, 0, 2, &[0; 1185]).unwrap();
    assert_eq!(s.owner.shared.pool.budget.used(), used);
    let m = begin(100, &[1; 2000]);
    p.begin(100, m).unwrap();
    let used = s.owner.shared.pool.budget.used();
    let grant = p.rx[&100].grant.grant_id.get();
    p.begin(100, m).unwrap();
    assert_eq!(p.rx.len(), 1);
    assert_eq!(p.rx[&100].grant.grant_id.get(), grant);
    assert_eq!(s.owner.shared.pool.budget.used(), used);
    assert_eq!(s.owner.shared.frames_in_use.load(Relaxed), 1);
    p.data(100, 1, 2, &[1; 815]).unwrap();
    let rx = p.rx.get_mut(&100).unwrap();
    let start = rx.started;
    assert!(
        rx.nack(
            s.state.local,
            100,
            start + Duration::from_millis(5),
            Duration::from_millis(500)
        )
        .is_none()
    );
    assert!(
        rx.nack(
            s.state.local,
            100,
            start + Duration::from_millis(10),
            Duration::from_millis(500)
        )
        .is_some()
    );
    assert!(
        rx.nack(
            s.state.local,
            100,
            start + Duration::from_millis(20),
            Duration::from_millis(500)
        )
        .is_none()
    );
    drop(p);
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn acknowledged_storage_outlives_l1_ttl_and_terminal_ids_do_not_resurrect() {
    let (a, b, s, mut p) = context();
    let bytes = vec![0x73; 2000];
    let m = begin(100, &bytes);
    p.begin(100, m).unwrap();
    p.data(100, 0, 2, &bytes[..1185]).unwrap();
    let ack =
        p.rx.get_mut(&100)
            .unwrap()
            .ack(s.state.local, 100, Instant::now(), true)
            .unwrap();
    assert!(matches!(
        wire::decode(ack.as_bytes()),
        Ok(wire::Packet::Control { .. })
    ));
    p.tick(Instant::now() + Duration::from_millis(20));
    assert_eq!(s.owner.shared.frames_in_use.load(Relaxed), 1);
    p.data(100, 1, 2, &bytes[1185..]).unwrap();
    p.finish_ready();
    let frame = a.try_recv().unwrap();
    assert_eq!(frame.as_slice(), bytes);
    drop(frame);
    let used = s.owner.shared.pool.budget.used();
    p.begin(100, m).unwrap();
    p.data(100, 0, 2, &bytes[..1185]).unwrap();
    assert_eq!(s.owner.shared.pool.budget.used(), used);
    assert!(matches!(a.try_recv(), Err(Error::WouldBlock)));
    assert!(!p.rx.contains_key(&100));
    drop(p);
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn malformed_ack_is_transactional_and_known_acks_are_never_rescheduled() {
    let (a, b, s, mut p) = context();
    let mut tx = tx(&s, 2000);
    let id = tx.layout.id;
    tx.grant(7).unwrap();
    let mut cc = Recovery::new(1200, Duration::from_millis(30), true);
    let at = Instant::now();
    tx.sent(0, at, &mut cc);
    tx.sent(1, at, &mut cc);
    p.recovery = cc;
    p.tx.insert(id, tx);
    let ranges = [Range {
        start: 0.into(),
        len: 1.into(),
    }];
    let valid = control::ack(s.state.remote, id, &ranges);
    let Ok(wire::Packet::Control { body, .. }) = wire::decode(valid.as_bytes()) else {
        panic!()
    };
    let mut bad = body.to_vec();
    bad.push(0);
    let header = Header::new(wire::ACK, s.state.remote, 0);
    let inflight = p.recovery.inflight;
    assert!(p.control(header, &bad).is_err());
    assert_eq!(p.recovery.inflight, inflight);
    p.control(header, body).unwrap();
    assert!(p.recovery.inflight < inflight);
    // NACK overlaps the now-known ACK; only fragment 1 may be repaired.
    let tx = p.tx.get_mut(&id).unwrap();
    tx.nack(
        &[Range {
            start: 0.into(),
            len: 2.into(),
        }],
        at,
        &mut p.recovery,
    );
    assert_eq!(tx.next(at, &p.recovery), Some(1));
    assert!(
        tx.validate(&[Range {
            start: 2.into(),
            len: 1.into()
        }])
        .is_err()
    );
    drop(p);
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn completion_and_cancel_choose_one_receive_terminal() {
    let (a, b, s, mut p) = context();
    let bytes = b"already complete";
    p.begin(100, begin(100, bytes)).unwrap();
    p.data(100, 0, 2, bytes).unwrap();
    p.finish_ready();
    let direction = 1 - p.direction();
    let abort = control::abort_body(AbortReason::Cancelled, direction);
    p.control(
        Header::new(wire::ABORT, s.state.remote, 100),
        abort.as_bytes(),
    )
    .unwrap();
    assert_eq!(p.received.get(100), Some(Terminal::Complete));
    assert_eq!(&*a.try_recv().unwrap(), bytes);
    p.control(
        Header::new(wire::ABORT, s.state.remote, 101),
        abort.as_bytes(),
    )
    .unwrap();
    p.begin(101, begin(101, bytes)).unwrap();
    p.data(101, 0, 2, bytes).unwrap();
    p.finish_ready();
    assert_eq!(
        p.received.get(101),
        Some(Terminal::Abort(AbortReason::Cancelled))
    );
    assert!(matches!(a.try_recv(), Err(Error::WouldBlock)));
    drop(p);
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn ledger_exhaustion_is_bounded_and_preserves_every_previous_terminal() {
    let budget = Budget::new(1 << 20, None);
    let mut ledger = Ledger::new(budget.clone());
    for i in 0..super::ledger::LIMIT as u32 {
        ledger.admit(0).unwrap();
        ledger.insert(i * 2 + 1, Terminal::Complete);
    }
    let used = budget.used();
    assert!(matches!(ledger.admit(0), Err(Error::IdExhausted)));
    assert_eq!(budget.used(), used);
    for i in 0..super::ledger::LIMIT as u32 {
        assert_eq!(ledger.get(i * 2 + 1), Some(Terminal::Complete));
    }
    drop(ledger);
    assert_eq!(budget.used(), 0);
}
#[test]
fn karn_sampling_rack_and_congestion_window_have_explicit_bounds() {
    let (a, b, s, _p) = context();
    let mut tx = tx(&s, 2000);
    tx.grant(7).unwrap();
    let mut cc = Recovery::new(1200, Duration::from_millis(100), false);
    let at = Instant::now();
    assert!(cc.can_send(1200, at));
    cc.sent(1200, at);
    assert!(!cc.can_send(1200, at));
    // Recreate an unconstrained local path for deterministic packet timing.
    let mut cc = Recovery::new(1200, Duration::from_millis(100), true);
    tx.sent(0, at, &mut cc);
    tx.sent(1, at + Duration::from_millis(50), &mut cc);
    tx.ack(
        &[Range {
            start: 1.into(),
            len: 1.into(),
        }],
        at + Duration::from_millis(60),
        &mut cc,
    );
    assert_eq!(cc.srtt, Some(Duration::from_millis(10)));
    let (rack, _, _) = tx.tick(at + Duration::from_millis(61), &mut cc);
    assert_eq!(rack, 1);
    tx.sent(0, at + Duration::from_millis(62), &mut cc);
    tx.ack(
        &[Range {
            start: 0.into(),
            len: 1.into(),
        }],
        at + Duration::from_millis(80),
        &mut cc,
    );
    assert_eq!(
        cc.srtt,
        Some(Duration::from_millis(10)),
        "retransmit ACK cannot create an ambiguous RTT sample"
    );
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn opposite_directions_with_identical_ids_have_independent_abort_targets() {
    let (a, b, s, mut p) = context();
    let tx = tx(&s, 100);
    let id = tx.layout.id;
    p.tx.insert(id, tx);
    p.sent.admit(1).unwrap();
    p.begin(id, begin(id, &[3; 100])).unwrap();
    let remote_direction = 1 - p.direction();
    p.control(
        Header::new(wire::ABORT, s.state.remote, id),
        control::abort_body(AbortReason::Cancelled, remote_direction).as_bytes(),
    )
    .unwrap();
    assert!(p.tx.contains_key(&id));
    assert!(!p.rx.contains_key(&id));
    p.control(
        Header::new(wire::ABORT, s.state.remote, id),
        control::abort_body(AbortReason::Integrity, p.direction()).as_bytes(),
    )
    .unwrap();
    assert!(!p.tx.contains_key(&id));
    assert_eq!(
        p.received.get(id),
        Some(Terminal::Abort(AbortReason::Cancelled))
    );
    assert_eq!(
        p.sent.get(id),
        Some(Terminal::Abort(AbortReason::Integrity))
    );
    drop(p);
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn aggregate_send_state_does_not_probe_unsent_packets_or_forget_retry_limits() {
    let (a, b, s, _p) = context();
    let mut tx = tx(&s, 2000);
    tx.grant(7).unwrap();
    let now = Instant::now();
    let mut cc = Recovery::new(1200, Duration::from_millis(30), true);
    assert!(tx.control(now, cc.rto, 0).is_none());
    tx.sent(0, now, &mut cc);
    assert!(tx.control(now, cc.rto, 0).is_none());
    tx.sent(1, now, &mut cc);
    assert!(tx.control(now, cc.rto, 0).is_none());
    assert!(tx.control(now + cc.rto / 2, cc.rto, 0).is_some());
    for _ in 1..32 {
        tx.nack(
            &[Range {
                start: 1.into(),
                len: 1.into(),
            }],
            now,
            &mut cc,
        );
        tx.sent(1, now, &mut cc);
    }
    assert!(tx.exhausted(now, Duration::from_secs(120), 32));
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn first_transmissions_restart_the_feedback_quiet_period() {
    let (a, b, s, _) = context();
    let mut tx = tx(&s, 2000);
    let mut cc = Recovery::new(1200, Duration::from_millis(40), true);
    let at = Instant::now();
    tx.grant(1).unwrap();
    tx.sent(0, at + Duration::from_millis(30), &mut cc);
    tx.sent(1, at + Duration::from_millis(60), &mut cc);
    assert_eq!(
        tx.tick(at + Duration::from_millis(61), &mut cc),
        (0, false, false)
    );
    assert_eq!(tx.next(at + Duration::from_millis(61), &cc), None);
    let (_, round, probe) = tx.tick(at + Duration::from_millis(81), &mut cc);
    assert!(!round && probe, "a truly quiet tail must still be probed");
    assert_eq!(tx.next(at + Duration::from_millis(81), &cc), Some(1));
    tx.sent(1, at + Duration::from_millis(82), &mut cc);
    assert!(
        tx.tick(at + Duration::from_millis(101), &mut cc).1,
        "a repair cannot postpone the recovery round indefinitely"
    );
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn adapted_loopback_rto_keeps_tail_probe_and_recovery_boundaries() {
    let (a, b, s, _) = context();
    let mut tx = tx(&s, 2000);
    let mut cc = Recovery::new(1200, Duration::from_millis(30), true);
    cc.ack(0, Some(Duration::from_micros(10)));
    assert_eq!(cc.rto, Duration::from_millis(4));
    tx.grant(1).unwrap();
    let sent = Instant::now();
    tx.sent(0, sent, &mut cc);
    tx.sent(1, sent, &mut cc);
    assert_eq!(
        tx.tick(sent + Duration::from_micros(1999), &mut cc),
        (0, false, false)
    );
    assert_eq!(
        tx.tick(sent + Duration::from_millis(2), &mut cc),
        (0, false, true)
    );
    assert_eq!(tx.next(sent + Duration::from_millis(2), &cc), Some(1));
    tx.sent(1, sent + Duration::from_millis(2), &mut cc);
    assert!(!tx.tick(sent + Duration::from_micros(3999), &mut cc).1);
    assert!(tx.tick(sent + Duration::from_millis(4), &mut cc).1);
    assert_eq!(tx.next(sent + Duration::from_millis(4), &cc), Some(0));
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn complete_cannot_release_unsent_payload_or_an_empty_message_before_begin() {
    let (a, b, s, mut p) = context();
    let mut tx = tx(&s, 2000);
    let id = tx.layout.id;
    let signal = tx.signal.clone();
    tx.grant(7).unwrap();
    p.sent.admit(1).unwrap();
    p.tx.insert(id, tx);
    let mut complete = Header::new(wire::COMPLETE, s.state.remote, id);
    complete.kind_flags |= 1;
    p.control(complete, &[]).unwrap();
    assert!(signal.outcome().is_none());
    assert!(p.tx.contains_key(&id));
    let now = Instant::now();
    let tx = p.tx.get_mut(&id).unwrap();
    tx.sent(0, now, &mut p.recovery);
    tx.sent(1, now, &mut p.recovery);
    p.control(complete, &[]).unwrap();
    assert!(matches!(
        signal.outcome(),
        Some(super::DeliveryOutcome::Delivered(_))
    ));
    let mut empty = super::test::tx(&s, 0);
    assert!(!empty.can_complete());
    empty.control_sent(now);
    assert!(empty.can_complete());
    drop(p);
    a.close().unwrap();
    b.close().unwrap();
}

fn ordered_meta(id: u32, channel: u16, seq: u32, payload: &[u8]) -> Begin {
    let mut meta = Begin::new(payload.len(), 1200, 1).unwrap();
    meta.layer = 3;
    meta.meta_flags = 1;
    meta.channel = channel.into();
    meta.seq = seq.into();
    meta.crc = crate::checksum::append(meta.seed(id), payload).into();
    meta
}
#[test]
fn ordered_grants_reserve_the_predecessor_before_successors_and_publish_in_order() {
    let (a, b, s, mut p) = context();
    p.begin(101, ordered_meta(101, 9, 1, b"later")).unwrap();
    assert!(!p.rx.contains_key(&101));
    assert_eq!(s.owner.shared.frames_in_use.load(Relaxed), 0);
    assert!(p.begin(102, ordered_meta(102, 9, 1, b"conflict")).is_err());
    let mut conflicting = Begin::new(1, 1200, 1).unwrap();
    conflicting.crc = crate::checksum::append(conflicting.seed(101), b"x").into();
    assert!(p.begin(101, conflicting).is_err());
    p.begin(100, ordered_meta(100, 9, 0, b"first")).unwrap();
    p.promote_ordered();
    assert!(p.rx.contains_key(&100) && p.rx.contains_key(&101));
    p.controls.clear();
    p.data(101, 0, 2, b"later").unwrap();
    p.finish_ready();
    assert_eq!(p.controls.len(), 1);
    assert!(
        matches!(wire::decode(p.controls[0].as_bytes()), Ok(wire::Packet::Control {header, ..}) if header.kind() == wire::ACK)
    );
    assert!(!p.pending.contains_key(&101));
    assert!(matches!(a.try_recv(), Err(Error::WouldBlock)));
    assert_eq!(
        s.owner
            .shared
            .socket
            .metrics
            .ordered_buffered_frames
            .load(Relaxed),
        1
    );
    p.controls.clear();
    p.data(100, 0, 2, b"first").unwrap();
    p.finish_ready();
    assert!(
        p.controls.is_empty(),
        "immediate publication uses COMPLETE, not a redundant SACK"
    );
    assert_eq!(p.pending.len(), 2);
    for (seq, bytes) in [(0, b"first"), (1, b"later")] {
        let f = a.try_recv().unwrap();
        assert_eq!(f.channel_sequence(), Some(seq));
        assert_eq!(f.as_slice(), bytes);
        assert_eq!(f.sender_incarnation(), s.state.remote);
    }
    assert_eq!(
        s.owner
            .shared
            .socket
            .metrics
            .ordered_buffered_frames
            .load(Relaxed),
        0
    );
    drop(p);
    a.close().unwrap();
    b.close().unwrap();
}
#[test]
fn ordered_admission_failures_leave_no_sequence_or_frame_id_hole() {
    let env = crate::benchmarking::Environment::new(1).unwrap();
    let cfg = EndpointConfig {
        max_peers: 4,
        ..EndpointConfig::default()
    };
    let a = env
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg.clone())
        .unwrap();
    let b = env
        .bind_endpoint("127.0.0.1:0".parse().unwrap(), cfg)
        .unwrap();
    let s = a.connect(b.local_addr().unwrap()).unwrap();
    let mut p = ReliablePeer::new(s.owner.shared.clone(), s.state.clone()).unwrap();
    let c = p.open_channel(9).unwrap();
    let submit = |p: &mut ReliablePeer| {
        let frame = s.allocate_ordered_frame(2000).unwrap();
        let crc = frame.payload_crc(false);
        let signal = Signal::new(s.owner.shared.pool.budget.reserve(256).unwrap());
        let permit = Permit::take(&s.owner.shared.outgoing_reliable, 256).unwrap();
        let lane = Permit::take(&c.inflight, 32).unwrap();
        p.submit_ordered(frame, crc, signal, permit, lane, c.clone())
    };
    let (_, seq) = submit(&mut p).unwrap();
    assert_eq!(seq, 0);
    let frame = s.allocate_ordered_frame(2000).unwrap();
    let crc = frame.payload_crc(false);
    let signal = Signal::new(s.owner.shared.pool.budget.reserve(256).unwrap());
    let permit = Permit::take(&s.owner.shared.outgoing_reliable, 256).unwrap();
    let lane = Permit::take(&c.inflight, 32).unwrap();
    let next = s.state.next_id.load(Relaxed);
    let pressure = s
        .owner
        .shared
        .pool
        .budget
        .reserve(s.owner.shared.pool.budget.available())
        .unwrap();
    assert!(matches!(
        p.submit_ordered(frame, crc, signal, permit, lane, c.clone()),
        Err(Error::NoBuffer)
    ));
    assert_eq!(s.state.next_id.load(Relaxed), next);
    assert_eq!(p.ordered.as_ref().unwrap().send[&9].next, 1);
    drop(pressure);
    let (_, seq) = submit(&mut p).unwrap();
    assert_eq!(seq, 1);
    p.ordered.as_mut().unwrap().send.get_mut(&9).unwrap().next = u32::MAX as u64;
    let (_, seq) = submit(&mut p).unwrap();
    assert_eq!(seq, u32::MAX);
    let next = s.state.next_id.load(Relaxed);
    assert!(matches!(submit(&mut p), Err(Error::IdExhausted)));
    assert_eq!(s.state.next_id.load(Relaxed), next);
    drop(p);
    a.close().unwrap();
    b.close().unwrap();
}

#[test]
fn a_full_channel_does_not_block_another_channels_deferred_grant() {
    let (a, b, _s, mut p) = context();
    p.ensure_receive_channel(1).unwrap();
    let counter = p.ordered.as_ref().unwrap().receive[&1].inflight.clone();
    let guards: Vec<_> = (0..32)
        .map(|_| Permit::take(&counter, 32).unwrap())
        .collect();
    p.begin(100, ordered_meta(100, 1, 0, b"blocked")).unwrap();
    assert!(!p.rx.contains_key(&100));
    p.begin(102, ordered_meta(102, 2, 1, b"later")).unwrap();
    p.begin(101, ordered_meta(101, 2, 0, b"first")).unwrap();
    p.promote_ordered();
    assert!(p.rx.contains_key(&102));
    assert!(!p.rx.contains_key(&100));
    drop(guards);
    p.promote_ordered();
    assert!(p.rx.contains_key(&100));
    drop(p);
    a.close().unwrap();
    b.close().unwrap();
}
