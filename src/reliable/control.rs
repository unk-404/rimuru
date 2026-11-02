//! Bounded wire parsing. Validate the whole ACK before changing any sender state.
use super::AbortReason;
use crate::{
    Error, Result,
    wire::{self, Abort, AckHeader, AckRecord, Control, Range},
};
use zerocopy::{FromBytes, IntoBytes};
pub(super) fn visit_ack(
    mut body: &[u8],
    mut visit: impl FnMut(u32, &[Range]) -> Result<()>,
) -> Result<()> {
    let (header, rest) =
        AckHeader::ref_from_prefix(body).map_err(|_| Error::Protocol("ACK header"))?;
    if header.reserved.get() != 0 || header.records.get() == 0 {
        return Err(Error::Protocol("ACK records"));
    }
    body = rest;
    let mut ids = [0u32; 12];
    if header.records.get() as usize > ids.len() {
        return Err(Error::Protocol("ACK count"));
    }
    for n in 0..header.records.get() as usize {
        let (record, rest) =
            AckRecord::ref_from_prefix(body).map_err(|_| Error::Protocol("ACK record"))?;
        let id = record.id.get();
        if id == 0
            || ids[..n].contains(&id)
            || record.reserved.get() != 0
            || record.ranges.get() == 0
        {
            return Err(Error::Protocol("ACK record fields"));
        }
        ids[n] = id;
        let bytes = record.ranges.get() as usize * size_of::<Range>();
        if bytes > rest.len() {
            return Err(Error::Protocol("ACK ranges length"));
        }
        let ranges =
            <[Range]>::ref_from_bytes(&rest[..bytes]).map_err(|_| Error::Protocol("ACK ranges"))?;
        validate_ranges(ranges)?;
        visit(id, ranges)?;
        body = &rest[bytes..];
    }
    if !body.is_empty() {
        return Err(Error::Protocol("ACK trailing bytes"));
    }
    Ok(())
}
pub(super) fn validate_ranges(ranges: &[Range]) -> Result<()> {
    let mut end = 0;
    for r in ranges {
        let start = r.start.get();
        let len = r.len.get();
        if len == 0 || start < end {
            return Err(Error::Protocol("overlapping/empty ranges"));
        }
        end = start
            .checked_add(len)
            .ok_or(Error::Protocol("range overflow"))?;
    }
    Ok(())
}
pub(super) fn ack(local: u64, id: u32, ranges: &[Range]) -> Control {
    let mut body = [0u8; 104];
    body[..8].copy_from_slice(
        AckHeader {
            delay: 0.into(),
            records: 1.into(),
            reserved: 0.into(),
        }
        .as_bytes(),
    );
    body[8..16].copy_from_slice(
        AckRecord {
            id: id.into(),
            ranges: (ranges.len() as u16).into(),
            reserved: 0.into(),
        }
        .as_bytes(),
    );
    body[16..16 + size_of_val(ranges)].copy_from_slice(ranges.as_bytes());
    Control::new(wire::ACK, local, 0, &body[..16 + size_of_val(ranges)])
}
pub(super) fn abort_body(reason: AbortReason, direction: u8) -> Abort {
    Abort {
        reason: (reason as u16).into(),
        scope: 0,
        direction,
        channel: 0.into(),
        reserved: 0.into(),
        last_seq: u64::MAX.into(),
    }
}
pub(super) fn read_abort(body: &[u8]) -> Result<(Abort, AbortReason)> {
    let a = Abort::read_from_bytes(body).map_err(|_| Error::Protocol("ABORT length"))?;
    if a.scope > 1
        || a.direction > 1
        || a.reserved.get() != 0
        || (a.scope == 0 && (a.channel.get() != 0 || a.last_seq.get() != u64::MAX))
        || (a.scope == 1 && a.last_seq.get() != u64::MAX && a.last_seq.get() > u32::MAX as u64)
    {
        return Err(Error::Protocol("ABORT fields"));
    }
    Ok((a, AbortReason::read(a.reason.get())?))
}
pub(super) fn nack(body: &[u8]) -> Result<&[Range]> {
    if body.len() < 4 || body[2..4] != [0, 0] {
        return Err(Error::Protocol("NACK header"));
    }
    let n = u16::from_le_bytes(body[..2].try_into().unwrap()) as usize;
    if n == 0 || body.len() != 4 + n * 8 {
        return Err(Error::Protocol("NACK length"));
    }
    let ranges =
        <[Range]>::ref_from_bytes(&body[4..]).map_err(|_| Error::Protocol("NACK ranges"))?;
    validate_ranges(ranges)?;
    Ok(ranges)
}

use super::{ReliablePeer, Terminal};
use crate::wire::{Credit, Grant, Header};
use std::{sync::atomic::Ordering::Relaxed, time::Instant};
impl ReliablePeer {
    pub fn control(&mut self, h: Header, body: &[u8]) -> Result<()> {
        if body.len() + 17 > 128 {
            return Err(Error::Protocol("L2 control limit"));
        }
        let id = h.frame.get();
        let now = Instant::now();
        match h.kind() {
            wire::BEGIN_ACCEPT => {
                let grant =
                    Grant::read_from_bytes(body).map_err(|_| Error::Protocol("grant length"))?;
                if id == 0 || grant.grant_id.get() == 0 {
                    return Err(Error::Protocol("zero grant"));
                }
                if let Some(tx) = self.tx.get_mut(&id) {
                    if grant.charged_bytes.get() < tx.layout.len() as u64 {
                        return Err(Error::Protocol("undersized grant"));
                    }
                    tx.grant(grant.grant_id.get())?;
                }
            }
            wire::ACK => {
                if id != 0 {
                    return Err(Error::Protocol("ACK frame ID"));
                }
                self::visit_ack(body, |id, ranges| {
                    self.tx
                        .get(&id)
                        .ok_or(Error::Protocol("unknown ACK frame"))?
                        .validate(ranges)
                })?;
                self::visit_ack(body, |id, ranges| {
                    self.tx
                        .get_mut(&id)
                        .unwrap()
                        .ack(ranges, now, &mut self.recovery);
                    Ok(())
                })?;
            }
            wire::NACK => {
                let ranges = self::nack(body)?;
                if let Some(tx) = self.tx.get_mut(&id) {
                    tx.validate(ranges)?;
                    tx.nack(ranges, now, &mut self.recovery);
                }
                self.shared
                    .socket
                    .metrics
                    .reliable_nacks
                    .fetch_add(1, Relaxed);
            }
            wire::CREDIT => {
                let c =
                    Credit::read_from_bytes(body).map_err(|_| Error::Protocol("credit length"))?;
                if id != 0 {
                    return Err(Error::Protocol("credit frame ID"));
                }
                if c.seq.get() > self.remote_credit_seq {
                    self.remote_credit_seq = c.seq.get();
                }
            }
            wire::COMPLETE => {
                if id == 0 || !body.is_empty() {
                    return Err(Error::Protocol("COMPLETE fields"));
                }
                if h.flags() == 1 {
                    // Publication implies all DATA was sent; zero-DATA completion also
                    // converges when the receiver grant was lost.
                    if self.tx.get(&id).is_some_and(|tx| tx.can_complete()) {
                        self.finish_tx(id, Terminal::Complete);
                    }
                    if self.sent.get(id) == Some(Terminal::Complete) {
                        self.queue(Control::new(wire::COMPLETE, self.state.local, id, &[]));
                    }
                } else if self
                    .pending
                    .get(&id)
                    .is_some_and(|p| p.terminal == Terminal::Complete)
                {
                    self.pending.remove(&id);
                }
            }
            wire::PING => {
                if body.len() != 8 {
                    return Err(Error::Protocol("PING length"));
                }
                if !self.replay(id)
                    && let Some(rx) = self.rx.get_mut(&id)
                {
                    let grant = rx.grant_packet(self.state.local, id);
                    let ack = rx.ack(self.state.local, id, now, true);
                    self.queue(grant);
                    if let Some(ack) = ack {
                        self.queue(ack);
                    }
                }
            }
            wire::ABORT | wire::ABORT_ACK => {
                let (a, reason) = self::read_abort(body)?;
                if a.scope == 1 {
                    return self.channel_abort(h, body);
                }
                if id == 0 {
                    return Err(Error::Protocol("ABORT frame ID"));
                }
                let target_tx = a.direction == self.direction();
                if !target_tx
                    && let Some(channel) = self
                        .rx
                        .get(&id)
                        .filter(|r| r.meta.layer == 3)
                        .map(|r| r.meta.channel.get())
                        .or_else(|| {
                            self.ordered
                                .as_ref()
                                .and_then(|o| o.deferred.get(&id))
                                .map(|d| d.meta.channel.get())
                        })
                {
                    if h.kind() == wire::ABORT {
                        self.close_incoming(channel, reason);
                        self.queue(Control::new(wire::ABORT_ACK, self.state.local, id, body));
                    }
                    return Ok(());
                }

                if h.kind() == wire::ABORT {
                    if target_tx {
                        self.finish_tx(id, Terminal::Abort(reason));
                        self.queue(Control::new(wire::ABORT_ACK, self.state.local, id, body));
                    } else if self.received.get(id) == Some(Terminal::Complete)
                        || self
                            .received
                            .get(id)
                            .is_some_and(|old| old != Terminal::Abort(reason))
                    {
                        self.replay(id);
                    } else {
                        if self.received.get(id).is_none() {
                            if !self.rx.contains_key(&id) {
                                self.received.admit(self.rx.len())?;
                            }
                            self.received.insert(id, Terminal::Abort(reason));
                            self.rx.remove(&id);
                        }
                        self.pending.remove(&id);
                        self.queue(Control::new(wire::ABORT_ACK, self.state.local, id, body));
                    }
                } else if target_tx {
                    if self.tx.get(&id).is_some_and(|tx| tx.cancel == Some(reason)) {
                        self.finish_tx(id, Terminal::Abort(reason));
                    }
                } else if self
                    .pending
                    .get(&id)
                    .is_some_and(|p| p.terminal == Terminal::Abort(reason))
                {
                    self.pending.remove(&id);
                }
            }
            _ => return Err(Error::Protocol("unknown reliable control")),
        }
        Ok(())
    }
}
