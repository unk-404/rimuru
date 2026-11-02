//! Owner-side command handling for connects, submits, cancels and disconnects.

use super::{Command, ReceiveLoop};
use crate::{AbortReason, DeliveryOutcome, Error};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

impl ReceiveLoop {
    pub(super) async fn drain_commands(&mut self, first: Command) {
        self.command(first).await;
        for _ in 1..self.shared.config.batch_size {
            if self.shared.closed.load(Acquire) {
                break;
            }
            match self.commands.try_recv() {
                Ok(command) => self.command(command).await,
                Err(_) => break,
            }
        }
    }

    async fn command(&mut self, command: Command) {
        match command {
            Command::OpenChannel {
                peer,
                incarnation,
                id,
                reply,
            } => {
                self.reliable_known = true;
                let result = match self.peers.get_mut(&peer).filter(|state| {
                    state.state.local == incarnation && !state.state.closed.load(Acquire)
                }) {
                    Some(state) => state
                        .reliable()
                        .and_then(|reliable| reliable.open_channel(id)),
                    None => Err(Error::Closed),
                };
                let _ = reply.send(result);
            }
            Command::CloseChannel {
                peer,
                incarnation,
                channel,
            } => {
                if let Some(state) = self
                    .peers
                    .get_mut(&peer)
                    .filter(|state| state.state.local == incarnation)
                    && let Some(reliable) = &mut state.reliable
                {
                    reliable.close_outgoing(channel, AbortReason::Normal);
                }
            }
            Command::OrderedSubmit {
                layout,
                payload_crc,
                signal,
                permit,
                lane_permit,
                channel,
                reply,
            } => {
                if reply.is_closed() {
                    return;
                }
                self.reliable_known = true;
                let peer = layout.state.peer;
                let local = layout.state.local;
                let result =
                    match self.peers.get_mut(&peer).filter(|state| {
                        state.state.local == local && !state.state.closed.load(Acquire)
                    }) {
                        Some(state) => state.reliable().and_then(|reliable| {
                            reliable.submit_ordered(
                                layout,
                                payload_crc,
                                signal.clone(),
                                permit,
                                lane_permit,
                                channel,
                            )
                        }),
                        None => Err(Error::Closed),
                    };
                let _ = reply.send(result);
            }
            Command::Connect { peer, reply } => match self.manager.start(peer, reply) {
                Ok(Some(packet)) => {
                    let _ = self.shared.socket.send_to(packet.as_bytes(), peer).await;
                }
                Ok(None) => {}
                Err(_) => {
                    self.shared
                        .socket
                        .metrics
                        .resource_drops
                        .fetch_add(1, Relaxed);
                }
            },
            Command::Submit {
                layout,
                signal,
                permit,
            } => {
                self.reliable_known = true;
                let peer = layout.state.peer;
                let local = layout.state.local;
                let result =
                    match self.peers.get_mut(&peer).filter(|state| {
                        state.state.local == local && !state.state.closed.load(Acquire)
                    }) {
                        Some(state) => match state.reliable() {
                            Ok(reliable) => reliable.submit(layout, signal.clone(), permit),
                            Err(error) => Err(error),
                        },
                        None => Err(Error::Closed),
                    };
                if let Err(error) = result {
                    if matches!(error, Error::IdExhausted)
                        && let Some(state) = self.peers.get_mut(&peer)
                    {
                        state.state.closed.store(true, Release);
                        if let Some(reliable) = &mut state.reliable {
                            reliable.fail_all();
                        }
                    }
                    signal.finish(DeliveryOutcome::Aborted(abort_reason(error)));
                }
            }
            Command::Cancel {
                peer,
                incarnation,
                id,
            } => {
                if let Some(state) = self
                    .peers
                    .get_mut(&peer)
                    .filter(|state| state.state.local == incarnation)
                    && let Some(reliable) = &mut state.reliable
                {
                    reliable.cancel(id);
                }
            }
            Command::Disconnect { peer, reply } => {
                self.peers.remove(&peer);
                if let Some((_, state)) = self.shared.sessions.remove(&peer) {
                    state.closed.store(true, Release);
                    let body = [3, 0, 4, 0, 0, 0, 0, 0];
                    let packet = crate::wire::Control::new(crate::wire::EXT, state.local, 0, &body);
                    let _ = self.shared.socket.send_to(packet.as_bytes(), peer).await;
                }
                let _ = reply.send(());
            }
        }
    }
}

fn abort_reason(error: Error) -> AbortReason {
    match error {
        Error::IdExhausted => AbortReason::IdExhausted,
        Error::Closed => AbortReason::Shutdown,
        _ => AbortReason::Resource,
    }
}
