//! Control priority, bounded fair DATA bursts and reactor send readiness.
use super::{ReliablePeer, Terminal, control};
use crate::{
    context::SessionState,
    wire::{self, Control},
};
use std::{
    sync::atomic::Ordering::{Acquire, Relaxed, Release},
    time::Instant,
};
use zerocopy::IntoBytes;
impl ReliablePeer {
    /// Nonblocking and burst bounded; socket readiness stays with the endpoint's one owner.
    pub fn drive(&mut self, now: Instant) -> bool {
        self.blocked = false;
        if self.state.closed.load(Acquire) {
            return false;
        }
        self.promote_ordered();
        self.finish_ready();
        self.drive_ordered(now);
        if self.blocked || self.state.closed.load(Acquire) {
            return false;
        }
        let socket = self.shared.socket.clone();
        let local = self.state.local;
        let mut remaining = self.shared.config.batch_size;
        while remaining > 0 {
            let Some(packet) = self.controls.first() else {
                break;
            };
            if !try_send(&socket, &self.state, &mut self.blocked, packet.as_bytes()) {
                return false;
            }
            let packet_len = packet.as_bytes().len();
            self.controls.remove(0);
            remaining -= 1;
            socket
                .metrics
                .reliable_control_datagrams
                .fetch_add(1, Relaxed);
            socket
                .metrics
                .reliable_control_bytes
                .fetch_add(packet_len as u64, Relaxed);
        }
        let direction = self.direction();
        for (&id, p) in &mut self.pending {
            if remaining == 0 {
                return true;
            }
            if p.last.is_none_or(|last| {
                now.duration_since(last)
                    >= self
                        .recovery
                        .rto
                        .saturating_mul(1 << p.attempts.saturating_sub(1).min(7))
                        .min(std::time::Duration::from_secs(60))
            }) {
                let packet = match p.terminal {
                    Terminal::Complete => Control::with_flags(wire::COMPLETE, 1, local, id, &[]),
                    Terminal::Abort(reason) => Control::new(
                        wire::ABORT,
                        local,
                        id,
                        control::abort_body(reason, 1 - direction).as_bytes(),
                    ),
                };
                if !try_send(&socket, &self.state, &mut self.blocked, packet.as_bytes()) {
                    return false;
                }
                p.last = Some(now);
                p.attempts += 1;
                remaining -= 1;
                socket
                    .metrics
                    .reliable_control_datagrams
                    .fetch_add(1, Relaxed);
                socket
                    .metrics
                    .reliable_control_bytes
                    .fetch_add(packet.as_bytes().len() as u64, Relaxed);
            }
        }
        for (&id, rx) in &mut self.rx {
            if remaining == 0 {
                return true;
            }
            if let Some(packet) = rx.ack(local, id, now, false) {
                if !try_send(&socket, &self.state, &mut self.blocked, packet.as_bytes()) {
                    self.controls.push(packet);
                    return false;
                }
                remaining -= 1;
                socket
                    .metrics
                    .reliable_control_datagrams
                    .fetch_add(1, Relaxed);
                socket
                    .metrics
                    .reliable_control_bytes
                    .fetch_add(packet.as_bytes().len() as u64, Relaxed);
            }
            if remaining == 0 {
                return true;
            }
            if let Some(packet) = rx.nack(local, id, now, self.recovery.rto) {
                if !try_send(&socket, &self.state, &mut self.blocked, packet.as_bytes()) {
                    self.controls.push(packet);
                    return false;
                }
                remaining -= 1;
                socket
                    .metrics
                    .reliable_control_datagrams
                    .fetch_add(1, Relaxed);
                socket
                    .metrics
                    .reliable_control_bytes
                    .fetch_add(packet.as_bytes().len() as u64, Relaxed);
            }
        }
        let ordered = &self.ordered;
        for tx in self.tx.values_mut() {
            if tx.layout.meta.layer == 3
                && ordered
                    .as_ref()
                    .and_then(|o| o.send.get(&(tx.layout.meta.channel.get() as u32)))
                    .is_none_or(|s| s.state.closing.load(Acquire))
            {
                continue;
            }
            if remaining == 0 {
                return true;
            }
            if let Some(packet) = tx.control(now, self.recovery.rto, direction) {
                if !try_send(&socket, &self.state, &mut self.blocked, packet.as_bytes()) {
                    return false;
                }
                tx.control_sent(now);
                remaining -= 1;
                socket
                    .metrics
                    .reliable_control_datagrams
                    .fetch_add(1, Relaxed);
                socket
                    .metrics
                    .reliable_control_bytes
                    .fetch_add(packet.as_bytes().len() as u64, Relaxed);
            }
        }
        let quantum = (self.shared.config.batch_size / self.tx.len().max(1)).max(1);
        let mut more = false;
        let len = self.tx.len();
        let start = self.round_robin % len.max(1);
        for pass in 0..2 {
            let skip = if pass == 0 { start } else { 0 };
            let take = if pass == 0 { len - start } else { start };
            for (index, tx) in self.tx.values_mut().enumerate().skip(skip).take(take) {
                if tx.layout.meta.layer == 3
                    && ordered
                        .as_ref()
                        .and_then(|o| o.send.get(&(tx.layout.meta.channel.get() as u32)))
                        .is_none_or(|s| s.state.closing.load(Acquire))
                {
                    continue;
                }
                if remaining == 0 {
                    return true;
                }
                self.round_robin = (index + 1) % len.max(1);
                let mut served = 0;
                while remaining > 0 && served < quantum {
                    let Some(i) = tx.next(now, &self.recovery) else {
                        break;
                    };
                    #[cfg(windows)]
                    if self.state.peer.ip().is_loopback() {
                        let count = tx.initial_segment_run(
                            i,
                            remaining.min(quantum - served),
                            socket.segmentation_size(),
                        );
                        if count >= 2 {
                            match tx.try_send_initial_segments(i, count, &socket) {
                                Ok(_) => {
                                    // A batch is submitted by one syscall; use one
                                    // timestamp so scheduling pauses in this loop
                                    // cannot manufacture ordering between its packets.
                                    let sent_at = Instant::now();
                                    for index in i..i + count {
                                        tx.sent(index, sent_at, &mut self.recovery);
                                    }
                                    socket
                                        .metrics
                                        .reliable_data_datagrams
                                        .fetch_add(count as u64, Relaxed);
                                    socket
                                        .metrics
                                        .reliable_segment_batches
                                        .fetch_add(1, Relaxed);
                                    remaining -= count;
                                    served += count;
                                    continue;
                                }
                                Err(error) if crate::udp::windows::unsupported(&error) => {
                                    socket.disable_segmentation();
                                }
                                Err(error) => {
                                    sent_result(
                                        &socket,
                                        &self.state,
                                        &mut self.blocked,
                                        Err(error),
                                    );
                                    return false;
                                }
                            }
                        }
                    }
                    if !sent_result(
                        &socket,
                        &self.state,
                        &mut self.blocked,
                        tx.try_send(i, &socket),
                    ) {
                        return false;
                    }
                    socket.metrics.reliable_data_datagrams.fetch_add(1, Relaxed);
                    let packet_bytes = tx.packet_len(i);
                    if tx.sent(i, Instant::now(), &mut self.recovery) {
                        socket
                            .metrics
                            .reliable_retransmitted_bytes
                            .fetch_add(packet_bytes as u64, Relaxed);
                        socket.metrics.reliable_retransmits.fetch_add(1, Relaxed);
                    }
                    remaining -= 1;
                    served += 1;
                }
                more |= served == quantum;
            }
        }
        remaining == 0 || more
    }
}

fn try_send(
    socket: &crate::udp::DatagramSocket,
    state: &SessionState,
    blocked: &mut bool,
    packet: &[u8],
) -> bool {
    sent_result(
        socket,
        state,
        blocked,
        socket.try_send_to(packet, state.peer),
    )
}
pub(super) fn sent_result(
    socket: &crate::udp::DatagramSocket,
    state: &SessionState,
    blocked: &mut bool,
    result: std::io::Result<usize>,
) -> bool {
    match result {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            *blocked = true;
            false
        }
        Err(_) => {
            socket.metrics.io_errors.fetch_add(1, Relaxed);
            state.closed.store(true, Release);
            false
        }
    }
}
