//! Handshake state machine: challenge, negotiation, confirmation and retries.

use super::{
    Reply,
    cookie::{cookie, identity, nonce, time_bucket},
    pending::Pending,
};
use crate::{
    Error, Result,
    context::Context,
    wire::{self, Confirm, Control, Header, Hello, HelloAck, Retry},
};
use hashbrown::HashMap;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use zerocopy::{FromBytes, IntoBytes};

/// First retry delay; doubles up to the cap (DOCS 13.2).
const RETRY_BASE: u64 = 500;
/// Maximum retry delay in milliseconds.
const RETRY_CAP_MS: u64 = 2000;
/// Metadata charged while a handshake is pending.
const PENDING_CHARGE_BYTES: usize = 4096;
/// Concurrent `connect` waiters per peer.
const MAX_WAITERS: usize = 16;

pub(crate) struct Manager {
    pub(super) shared: Arc<Context>,
    pub(super) pending: HashMap<SocketAddr, Pending, foldhash::fast::RandomState>,
    pub(super) key: [u8; 32],
}

pub(crate) struct Action {
    pub packet: Option<Control>,
    pub established: Option<Arc<crate::context::SessionState>>,
    pub disconnected: bool,
}

impl Action {
    fn none() -> Self {
        Self {
            packet: None,
            established: None,
            disconnected: false,
        }
    }
}

impl Manager {
    pub fn new(shared: Arc<Context>, key: [u8; 32]) -> Self {
        Self {
            shared,
            key,
            pending: HashMap::with_hasher(foldhash::fast::RandomState::default()),
        }
    }

    pub fn start(&mut self, peer: SocketAddr, reply: Reply) -> Result<Option<Control>> {
        if let Some(state) = self.shared.sessions.get(&peer) {
            let _ = reply.send(Ok(state.clone()));
            return Ok(None);
        }
        if let Some(pending) = self.pending.get_mut(&peer) {
            if pending.waiters.len() >= MAX_WAITERS {
                let _ = reply.send(Err(Error::WouldBlock));
            } else {
                pending.waiters.push(reply);
            }
            return Ok(None);
        }
        if self.pending.len() >= self.shared.config.max_peers {
            let _ = reply.send(Err(Error::NoBuffer));
            return Ok(None);
        }
        let prepare = (|| -> Result<_> {
            Ok((
                self.shared.pool.budget.reserve(PENDING_CHARGE_BYTES)?,
                identity()?,
                nonce()?,
            ))
        })();
        let (charge, local, client) = match prepare {
            Ok(values) => values,
            Err(error) => {
                let _ = reply.send(Err(error));
                return Ok(None);
            }
        };
        let pending = Pending::new(&self.shared, local, client, charge, vec![reply]);
        let packet = self.offer(&pending);
        self.pending.insert(peer, pending);
        Ok(Some(packet))
    }

    pub fn control(&mut self, peer: SocketAddr, header: Header, body: &[u8]) -> Result<Action> {
        let mut out = Action::none();
        if header.frame.get() != 0 {
            return Err(Error::Protocol("handshake frame ID"));
        }
        match header.kind() {
            wire::HELLO => {
                let offer =
                    Hello::read_from_bytes(body).map_err(|_| Error::Protocol("HELLO length"))?;
                let integrity = self.shared.config.integrity.code();
                if offer.magic != *b"RMRU"
                    || offer.major.get() != wire::PROTOCOL_MAJOR
                    || offer.minor.get() != wire::PROTOCOL_MINOR
                    || offer.reserved != [0; 3]
                    || header.incarnation.get() == 0
                    || offer.integrity != integrity
                    || !(128..=65507).contains(&offer.max_d.get())
                    || offer.max_control.get() < 128
                {
                    return Err(Error::Protocol("HELLO parameters"));
                }
                let bucket = time_bucket();
                let valid = offer.cookie
                    == cookie(
                        self.key,
                        peer,
                        header.incarnation.get(),
                        &offer.client_nonce,
                        bucket,
                    )
                    || (bucket > 0
                        && offer.cookie
                            == cookie(
                                self.key,
                                peer,
                                header.incarnation.get(),
                                &offer.client_nonce,
                                bucket - 1,
                            ));
                if !valid {
                    let retry = Retry {
                        client_nonce: offer.client_nonce,
                        cookie: cookie(
                            self.key,
                            peer,
                            header.incarnation.get(),
                            &offer.client_nonce,
                            bucket,
                        ),
                    };
                    out.packet = Some(Control::new(wire::RETRY, 0, 0, retry.as_bytes()));
                    return Ok(out);
                }
                // Existing sessions cannot be replaced by replayed old handshakes.
                if self.shared.sessions.contains_key(&peer) {
                    return Ok(out);
                }
                if let Some(pending) = self.pending.get(&peer) {
                    if pending.initiator
                        && (pending.local, pending.client)
                            < (header.incarnation.get(), offer.client_nonce)
                    {
                        return Ok(out);
                    }
                    if !pending.initiator
                        && pending.remote == header.incarnation.get()
                        && pending.client == offer.client_nonce
                    {
                        out.packet = Some(self.offer(pending));
                        return Ok(out);
                    }
                }
                if self.pending.len() >= self.shared.config.max_peers
                    || self.shared.sessions.len() >= self.shared.config.max_peers
                {
                    return Err(Error::NoBuffer);
                }
                let old = self.pending.remove(&peer);
                let (local, waiters, charge) = match old {
                    Some(pending) => (pending.local, pending.waiters, pending.charge),
                    None => (
                        identity()?,
                        Vec::new(),
                        self.shared.pool.budget.reserve(PENDING_CHARGE_BYTES)?,
                    ),
                };
                let pending = Pending {
                    local,
                    remote: header.incarnation.get(),
                    client: offer.client_nonce,
                    server: nonce()?,
                    cookie: offer.cookie,
                    initiator: false,
                    d: self
                        .shared
                        .config
                        .datagram_size
                        .min(offer.max_d.get() as usize),
                    max: self
                        .shared
                        .config
                        .max_frame_size
                        .min(offer.max_message.get() as usize),
                    confirming: false,
                    reliable: offer.caps.get() & wire::RELIABLE_CAP != 0,
                    ordered: offer.caps.get() & (wire::ORDERED_CAP | wire::RELIABLE_CAP)
                        == (wire::ORDERED_CAP | wire::RELIABLE_CAP),
                    started: Instant::now(),
                    last: Instant::now(),
                    attempts: 0,
                    waiters,
                    charge,
                };
                out.packet = Some(self.offer(&pending));
                self.pending.insert(peer, pending);
            }
            wire::RETRY => {
                let retry =
                    Retry::read_from_bytes(body).map_err(|_| Error::Protocol("RETRY length"))?;
                if header.incarnation.get() != 0 {
                    return Err(Error::Protocol("RETRY identity"));
                }
                if let Some(pending) = self.pending.get_mut(&peer)
                    && pending.initiator
                    && pending.client == retry.client_nonce
                    && !pending.confirming
                {
                    pending.cookie = retry.cookie;
                }
                if let Some(pending) = self.pending.get(&peer) {
                    out.packet = Some(self.offer(pending));
                }
            }
            wire::HELLO_ACK => {
                let ack = HelloAck::read_from_bytes(body)
                    .map_err(|_| Error::Protocol("HELLO_ACK length"))?;
                let integrity = self.shared.config.integrity.code();
                if let Some(pending) = self.pending.get_mut(&peer)
                    && pending.initiator
                    && pending.client == ack.client_nonce
                {
                    if header.incarnation.get() == 0
                        || ack.reserved != [0; 3]
                        || ack.integrity != integrity
                        || !(128..=pending.d).contains(&(ack.max_d.get() as usize))
                        || ack.max_message.get() as usize > pending.max
                        || ack.max_control.get() < 128
                        || ack.max_control.get() > ack.max_d.get()
                        || ack.caps.get() & (1u64 << (ack.integrity - 1)) == 0
                    {
                        return Err(Error::Protocol("HELLO_ACK parameters"));
                    }
                    pending.reliable = ack.caps.get() & wire::RELIABLE_CAP != 0;
                    pending.ordered = pending.reliable && ack.caps.get() & wire::ORDERED_CAP != 0;
                    pending.remote = header.incarnation.get();
                    pending.server = ack.server_nonce;
                    pending.d = ack.max_d.get() as usize;
                    pending.max = ack.max_message.get() as usize;
                    pending.confirming = true;
                }
                if let Some(pending) = self.pending.get(&peer) {
                    out.packet = Some(self.offer(pending));
                }
            }
            wire::CONFIRM => {
                let confirm = Confirm::read_from_bytes(body)
                    .map_err(|_| Error::Protocol("CONFIRM length"))?;
                if header.incarnation.get() == 0 {
                    return Err(Error::Protocol("zero CONFIRM identity"));
                }
                if let Some(existing) = self.shared.sessions.get(&peer) {
                    if !existing.initiator
                        && confirm.client_nonce == existing.client_nonce
                        && confirm.server_nonce == existing.server_nonce
                        && confirm.peer_incarnation.get() == existing.local
                        && header.incarnation.get() == existing.remote
                    {
                        out.packet = Some(self.confirm(
                            existing.local,
                            existing.remote,
                            confirm.client_nonce,
                            confirm.server_nonce,
                        ));
                    }
                    return Ok(out);
                }
                let valid = self.pending.get(&peer).is_some_and(|pending| {
                    (!pending.initiator || pending.confirming)
                        && pending.remote == header.incarnation.get()
                        && pending.local == confirm.peer_incarnation.get()
                        && pending.client == confirm.client_nonce
                        && pending.server == confirm.server_nonce
                });
                if !valid {
                    return Ok(out);
                }
                let mut pending = self.pending.remove(&peer).expect("validated above");
                if self.shared.sessions.len() >= self.shared.config.max_peers {
                    for waiter in pending.waiters {
                        let _ = waiter.send(Err(Error::NoBuffer));
                    }
                    return Ok(out);
                }
                if !pending.initiator {
                    out.packet = Some(self.confirm(
                        pending.local,
                        pending.remote,
                        pending.client,
                        pending.server,
                    ));
                }
                let waiters = std::mem::take(&mut pending.waiters);
                let state = self.session_state(peer, pending);
                self.shared.sessions.insert(peer, state.clone());
                for waiter in waiters {
                    let _ = waiter.send(Ok(state.clone()));
                }
                out.established = Some(state);
            }
            wire::EXT => {
                if body.len() != 8
                    || u16::from_le_bytes(body[..2].try_into().expect("two bytes")) != 3
                    || body[2..4] != 4u16.to_le_bytes()
                {
                    return Ok(out);
                }
                if self
                    .shared
                    .sessions
                    .get(&peer)
                    .is_some_and(|state| state.remote == header.incarnation.get())
                {
                    if let Some((_, state)) = self.shared.sessions.remove(&peer) {
                        state
                            .closed
                            .store(true, std::sync::atomic::Ordering::Release);
                    }
                    out.disconnected = true;
                }
            }
            _ => {}
        }
        Ok(out)
    }

    pub fn tick(&mut self, now: Instant) -> Vec<(SocketAddr, Control)> {
        let mut send = Vec::new();
        let mut expired = Vec::new();
        for (&peer, pending) in &mut self.pending {
            if now.duration_since(pending.started) >= self.shared.config.handshake_timeout {
                expired.push(peer);
            } else if now.duration_since(pending.last)
                >= Duration::from_millis((RETRY_BASE << pending.attempts.min(2)).min(RETRY_CAP_MS))
            {
                pending.attempts += 1;
                pending.last = now;
                send.push(peer);
            }
        }
        for peer in expired {
            if let Some(pending) = self.pending.remove(&peer) {
                for waiter in pending.waiters {
                    let _ = waiter.send(Err(Error::TimedOut));
                }
            }
        }
        send.into_iter()
            .filter_map(|peer| {
                self.pending
                    .get(&peer)
                    .map(|pending| (peer, self.offer(pending)))
            })
            .collect()
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        for (_, pending) in self.pending.drain() {
            for waiter in pending.waiters {
                let _ = waiter.send(Err(Error::Closed));
            }
        }
    }
}
