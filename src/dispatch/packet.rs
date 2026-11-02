//! Decode one datagram and route it to the owning peer.
//!
//! Validation errors are counted by class (unknown session, no buffer, malformed)
//! and never panic on network input.

use super::{Peer, ReceiveLoop};
use crate::{
    Error,
    wire::{self, Packet},
};
use std::{
    net::SocketAddr,
    sync::atomic::Ordering::{Acquire, Relaxed, Release},
};

impl ReceiveLoop {
    pub(super) async fn packet(&mut self, peer: SocketAddr, bytes: &[u8]) {
        if bytes.len() > self.shared.config.datagram_size {
            self.shared
                .socket
                .metrics
                .malformed_datagrams
                .fetch_add(1, Relaxed);
            return;
        }
        let result = match wire::decode(bytes) {
            Ok(Packet::Data {
                header,
                index,
                width,
                payload,
            }) => {
                if let Some(state) = self.peers.get_mut(&peer)
                    && state.state.remote == header.incarnation.get()
                    && !state.state.closed.load(Acquire)
                {
                    if header.flags() == 1 {
                        self.reliable_known = true;
                        state.reliable().and_then(|reliable| {
                            reliable.data(header.frame.get(), index, width, payload)
                        })
                    } else {
                        state
                            .receiver
                            .data(header.frame.get(), index, width, payload)
                    }
                } else {
                    self.shared
                        .socket
                        .metrics
                        .unknown_sessions
                        .fetch_add(1, Relaxed);
                    Ok(())
                }
            }
            Ok(Packet::Control { header, body }) if header.kind() == wire::BEGIN => {
                if let Some(state) = self.peers.get_mut(&peer)
                    && state.state.remote == header.incarnation.get()
                    && !state.state.closed.load(Acquire)
                {
                    wire::read_begin(body).and_then(|begin| {
                        if matches!(begin.layer, 2 | 3) {
                            self.reliable_known = true;
                            state
                                .reliable()
                                .and_then(|reliable| reliable.begin(header.frame.get(), begin))
                        } else {
                            state.receiver.begin(
                                header.frame.get(),
                                begin,
                                state.state.max_frame,
                                state.state.d,
                                state.state.integrity.code(),
                            )
                        }
                    })
                } else {
                    self.shared
                        .socket
                        .metrics
                        .unknown_sessions
                        .fetch_add(1, Relaxed);
                    Ok(())
                }
            }
            Ok(Packet::Control { header, body })
                if (wire::BEGIN_ACCEPT..=wire::PING).contains(&header.kind()) =>
            {
                if let Some(state) = self.peers.get_mut(&peer).filter(|state| {
                    state.state.remote == header.incarnation.get()
                        && !state.state.closed.load(Acquire)
                }) {
                    self.reliable_known = true;
                    state
                        .reliable()
                        .and_then(|reliable| reliable.control(header, body))
                } else {
                    self.shared
                        .socket
                        .metrics
                        .unknown_sessions
                        .fetch_add(1, Relaxed);
                    Ok(())
                }
            }
            Ok(Packet::Control { header, body }) => {
                match self.manager.control(peer, header, body) {
                    Ok(action) => {
                        if let Some(session) = action.established {
                            self.peers
                                .insert(peer, Peer::new(self.shared.clone(), session));
                        }
                        if action.disconnected {
                            self.peers.remove(&peer);
                        }
                        if let Some(packet) = action.packet {
                            let _ = self.shared.socket.send_to(packet.as_bytes(), peer).await;
                        }
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        };
        match result {
            Err(Error::IdExhausted) => {
                if let Some(state) = self.peers.get_mut(&peer) {
                    state.state.closed.store(true, Release);
                    if let Some(reliable) = &mut state.reliable {
                        reliable.fail_all();
                    }
                }
            }
            Err(Error::NoBuffer) => {
                self.shared
                    .socket
                    .metrics
                    .resource_drops
                    .fetch_add(1, Relaxed);
            }
            Err(_) => {
                self.shared
                    .socket
                    .metrics
                    .malformed_datagrams
                    .fetch_add(1, Relaxed);
            }
            _ => {}
        }
    }
}
