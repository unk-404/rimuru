//! Reliable channel termination, preserved delivery prefixes and bounded retries.
use super::{ReliablePeer, Terminal, control};
use crate::{
    AbortReason, ChannelEnd, ChannelEvent, Error, Result,
    ordered::Exchange,
    wire::{self, Control, Header},
};
use std::{
    sync::atomic::Ordering::{Relaxed, Release},
    time::{Duration, Instant},
};
use zerocopy::IntoBytes;
impl ReliablePeer {
    pub fn close_outgoing(&mut self, channel: u16, reason: AbortReason) {
        if let Some(lane) = self
            .ordered
            .as_mut()
            .and_then(|o| o.send.get_mut(&(channel as u32)))
            && lane.state.end().is_none()
            && lane.exchange.is_none()
        {
            lane.state.closing.store(true, Release);
            lane.exchange = Some(Exchange::new(reason, None));
        }
    }
    pub(super) fn close_incoming(&mut self, channel: u16, reason: AbortReason) {
        if let Some(lane) = self
            .ordered
            .as_mut()
            .and_then(|o| o.receive.get_mut(&(channel as u32)))
            && lane.end.is_none()
            && lane.exchange.is_none()
        {
            lane.exchange = Some(Exchange::new(reason, lane.prefix()));
            // Cache the channel stop on each admitted RX once, instead of a
            // second RX lookup and channel-table lookup for every DATA packet.
            for rx in self.rx.values_mut() {
                if rx.meta.layer == 3 && rx.meta.channel.get() == channel {
                    rx.stopped = true;
                }
            }
        }
    }
    fn finish_outgoing(&mut self, channel: u16, end: ChannelEnd) -> Result<()> {
        let Some(lane) = self
            .ordered
            .as_mut()
            .and_then(|o| o.send.get_mut(&(channel as u32)))
        else {
            return Ok(());
        };
        if lane.state.end().is_some() {
            return Ok(());
        }
        if end
            .last_delivered
            .is_some_and(|n| u64::from(n) >= lane.next)
            || lane
                .known_prefix
                .is_some_and(|known| end.last_delivered.is_none_or(|p| p < known))
        {
            return Err(Error::Protocol("channel termination prefix"));
        }
        if self.tx.values().any(|t| {
            t.layout.meta.layer == 3
                && t.layout.meta.channel.get() == channel
                && end
                    .last_delivered
                    .is_some_and(|p| t.layout.meta.seq.get() <= p)
                && !t.can_complete()
        }) {
            return Err(Error::Protocol("unsent ordered prefix"));
        }
        lane.state.finish(end);
        lane.exchange = None;
        loop {
            let id = self.tx.iter().find_map(|(&id, t)| {
                (t.layout.meta.layer == 3 && t.layout.meta.channel.get() == channel).then_some(id)
            });
            let Some(id) = id else {
                break;
            };
            if end.peer_confirmed {
                let delivered = end
                    .last_delivered
                    .is_some_and(|p| self.tx[&id].layout.meta.seq.get() <= p);
                self.finish_tx(
                    id,
                    if delivered {
                        Terminal::Complete
                    } else {
                        Terminal::Abort(end.reason)
                    },
                );
            } else {
                let tx = self.tx.remove(&id).unwrap();
                tx.release_inflight(&mut self.recovery);
                tx.unknown();
                self.sent.insert(id, Terminal::Abort(end.reason));
                self.shared
                    .socket
                    .metrics
                    .reliable_unknown
                    .fetch_add(1, Relaxed);
            }
        }
        Ok(())
    }
    fn finish_incoming(&mut self, channel: u16, end: ChannelEnd) {
        let Some(o) = self.ordered.as_mut() else {
            return;
        };
        let Some(lane) = o.receive.get_mut(&(channel as u32)) else {
            return;
        };
        if lane.end.is_some() {
            return;
        }
        lane.end = Some(end);
        lane.exchange = None;
        let m = &self.shared.socket.metrics;
        for ready in lane.ready.values() {
            m.ordered_buffered_frames.fetch_sub(1, Relaxed);
            m.ordered_buffered_bytes
                .fetch_sub(ready.frame.len() as u64, Relaxed);
        }
        lane.ready.clear();
        lane.sequence_ids.clear();
        o.deferred.retain(|_, d| d.meta.channel.get() != channel);
        loop {
            let id = self.rx.iter().find_map(|(&id, r)| {
                (r.meta.layer == 3 && r.meta.channel.get() == channel).then_some(id)
            });
            let Some(id) = id else {
                break;
            };
            self.rx.remove(&id);
            self.received.insert(id, Terminal::Abort(end.reason));
        }
        self.pending
            .retain(|_, p| !(p.layer == 3 && p.channel == channel));
        if let Some(permit) = lane.event_permit.take() {
            o.events.as_ref().unwrap().publish(
                ChannelEvent {
                    peer: self.state.peer,
                    sender_incarnation: self.state.remote,
                    channel,
                    end,
                },
                permit,
            );
        }
        m.ordered_channels_closed.fetch_add(1, Relaxed);
    }
    pub(super) fn channel_abort(&mut self, h: Header, body: &[u8]) -> Result<()> {
        let (a, reason) = control::read_abort(body)?;
        if a.scope != 1 || h.frame.get() != 0 || !self.state.ordered {
            return Err(Error::Protocol("channel ABORT scope"));
        }
        let channel = a.channel.get();
        let prefix = if a.last_seq.get() == u64::MAX {
            None
        } else {
            Some(
                u32::try_from(a.last_seq.get())
                    .map_err(|_| Error::Protocol("channel prefix width"))?,
            )
        };
        if a.direction == self.direction() {
            if self
                .ordered
                .as_ref()
                .is_none_or(|o| !o.send.contains_key(&(channel as u32)))
            {
                return Ok(());
            }
            if h.kind() == wire::ABORT_ACK
                && self.ordered.as_ref().unwrap().send[&(channel as u32)]
                    .exchange
                    .is_none()
            {
                return Ok(());
            }
            if h.kind() == wire::ABORT_ACK
                && self.ordered.as_ref().unwrap().send[&(channel as u32)]
                    .exchange
                    .is_some_and(|e| e.reason != reason)
            {
                return Err(Error::Protocol("channel ACK reason"));
            }
            self.finish_outgoing(
                channel,
                ChannelEnd {
                    reason,
                    last_delivered: prefix,
                    peer_confirmed: true,
                },
            )?;
            if h.kind() == wire::ABORT {
                self.queue(Control::new(wire::ABORT_ACK, self.state.local, 0, body));
            }
        } else {
            if h.kind() == wire::ABORT_ACK
                && self
                    .ordered
                    .as_ref()
                    .is_none_or(|o| !o.receive.contains_key(&(channel as u32)))
            {
                return Ok(());
            }
            self.ensure_receive_channel(channel)?;
            let lane = &self.ordered.as_ref().unwrap().receive[&(channel as u32)];
            let actual = lane.prefix();
            if h.kind() == wire::ABORT {
                if let Some(x) = lane.exchange
                    && x.reason != reason
                {
                    let p = channel_packet(
                        wire::ABORT,
                        self.state.local,
                        channel,
                        a.direction,
                        x.reason,
                        actual,
                    );
                    self.queue(p);
                    return Ok(());
                }
                let end = lane.end.unwrap_or(ChannelEnd {
                    reason,
                    last_delivered: actual,
                    peer_confirmed: true,
                });
                self.finish_incoming(channel, end);
                self.queue(channel_packet(
                    wire::ABORT_ACK,
                    self.state.local,
                    channel,
                    a.direction,
                    end.reason,
                    end.last_delivered,
                ));
            } else if let Some(x) = lane.exchange
                && x.reason == reason
            {
                if prefix != actual {
                    return Err(Error::Protocol("channel ACK prefix"));
                }
                self.finish_incoming(
                    channel,
                    ChannelEnd {
                        reason,
                        last_delivered: actual,
                        peer_confirmed: true,
                    },
                );
            }
        }
        Ok(())
    }
    pub(super) fn tick_ordered(&mut self, now: Instant) {
        let cfg = &self.shared.config.reliable;
        let deadline = cfg.message_timeout;
        let rounds = cfg.max_recovery_rounds;
        loop {
            let channel = self.ordered.as_ref().and_then(|o| {
                o.deferred.values().find_map(|d| {
                    (now.duration_since(d.started) >= deadline
                        && self.ordered_rx_open(d.meta.channel.get()))
                    .then_some(d.meta.channel.get())
                })
            });
            let Some(channel) = channel else {
                break;
            };
            self.close_incoming(channel, AbortReason::Timeout);
        }
        loop {
            let channel = self.pending.values().find_map(|p| {
                (p.layer == 3
                    && self.ordered_rx_open(p.channel)
                    && (now.duration_since(p.started) >= deadline || p.attempts >= rounds))
                    .then_some(p.channel)
            });
            let Some(channel) = channel else {
                break;
            };
            self.close_incoming(channel, AbortReason::Timeout);
        }

        loop {
            let next = self.ordered.as_ref().and_then(|o| {
                o.send.iter().find_map(|(&id, s)| {
                    s.exchange
                        .filter(|e| {
                            now.duration_since(e.started) >= deadline || e.attempts >= rounds
                        })
                        .map(|e| (id as u16, e))
                })
            });
            let Some((id, e)) = next else {
                break;
            };
            let _ = self.finish_outgoing(
                id,
                ChannelEnd {
                    reason: e.reason,
                    last_delivered: e.prefix,
                    peer_confirmed: false,
                },
            );
        }
        loop {
            let next = self.ordered.as_ref().and_then(|o| {
                o.receive.iter().find_map(|(&id, r)| {
                    r.exchange
                        .filter(|e| {
                            now.duration_since(e.started) >= deadline || e.attempts >= rounds
                        })
                        .map(|e| (id as u16, e))
                })
            });
            let Some((id, e)) = next else {
                break;
            };
            self.finish_incoming(
                id,
                ChannelEnd {
                    reason: e.reason,
                    last_delivered: e.prefix,
                    peer_confirmed: false,
                },
            );
        }
    }
    pub(super) fn drive_ordered(&mut self, now: Instant) {
        let direction = self.direction();
        let Some(o) = self.ordered.as_mut() else {
            return;
        };
        let socket = &self.shared.socket;
        let mut remaining = self.shared.config.batch_size;
        for (&id, lane) in &mut o.send {
            if remaining == 0 {
                break;
            }
            if let Some(e) = &mut lane.exchange
                && exchange_due(e, now, self.recovery.rto)
            {
                let p = channel_packet(
                    wire::ABORT,
                    self.state.local,
                    id as u16,
                    direction,
                    e.reason,
                    e.prefix,
                );
                if super::schedule::sent_result(
                    socket,
                    &self.state,
                    &mut self.blocked,
                    socket.try_send_to(p.as_bytes(), self.state.peer),
                ) {
                    e.last = Some(now);
                    e.attempts += 1;
                    remaining -= 1;
                    socket
                        .metrics
                        .reliable_control_datagrams
                        .fetch_add(1, Relaxed);
                    socket
                        .metrics
                        .reliable_control_bytes
                        .fetch_add(p.as_bytes().len() as u64, Relaxed);
                } else {
                    return;
                }
            }
        }
        for (&id, lane) in &mut o.receive {
            if remaining == 0 {
                break;
            }
            if let Some(e) = &mut lane.exchange
                && exchange_due(e, now, self.recovery.rto)
            {
                let p = channel_packet(
                    wire::ABORT,
                    self.state.local,
                    id as u16,
                    1 - direction,
                    e.reason,
                    e.prefix,
                );
                if super::schedule::sent_result(
                    socket,
                    &self.state,
                    &mut self.blocked,
                    socket.try_send_to(p.as_bytes(), self.state.peer),
                ) {
                    e.last = Some(now);
                    e.attempts += 1;
                    remaining -= 1;
                    socket
                        .metrics
                        .reliable_control_datagrams
                        .fetch_add(1, Relaxed);
                    socket
                        .metrics
                        .reliable_control_bytes
                        .fetch_add(p.as_bytes().len() as u64, Relaxed);
                } else {
                    return;
                }
            }
        }
    }
    pub(super) fn fail_ordered(&mut self) {
        let Some(o) = &self.ordered else {
            return;
        };
        for lane in o.send.values() {
            lane.state.finish(ChannelEnd {
                reason: AbortReason::Shutdown,
                last_delivered: lane.known_prefix,
                peer_confirmed: false,
            });
        }
        loop {
            let id = self
                .ordered
                .as_ref()
                .unwrap()
                .receive
                .iter()
                .find_map(|(&id, r)| r.end.is_none().then_some(id as u16));
            let Some(id) = id else {
                break;
            };
            let prefix = self.ordered.as_ref().unwrap().receive[&(id as u32)].prefix();
            self.finish_incoming(
                id,
                ChannelEnd {
                    reason: AbortReason::Shutdown,
                    last_delivered: prefix,
                    peer_confirmed: false,
                },
            );
        }
    }
}
fn exchange_due(e: &Exchange, now: Instant, rto: Duration) -> bool {
    e.last.is_none_or(|last| {
        now.duration_since(last)
            >= rto
                .saturating_mul(1 << e.attempts.saturating_sub(1).min(7))
                .min(Duration::from_secs(60))
    })
}
pub(super) fn channel_packet(
    kind: u8,
    local: u64,
    channel: u16,
    direction: u8,
    reason: AbortReason,
    prefix: Option<u32>,
) -> Control {
    let body = wire::Abort {
        reason: (reason as u16).into(),
        scope: 1,
        direction,
        channel: channel.into(),
        reserved: 0.into(),
        last_seq: prefix.map_or(u64::MAX, u64::from).into(),
    };
    Control::new(kind, local, 0, body.as_bytes())
}
