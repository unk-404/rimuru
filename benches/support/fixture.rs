//! Matched L1/L2/L3 whole-message window fixture.
//!
//! One window submits `WINDOW` messages, drains the receiver, then waits for every
//! reliable ticket. Producer fill, checksum, submission and delivery confirmation
//! stay inside the timed workload.

use rimuru::{
    Channel, Endpoint, EndpointConfig, Error, Result, Session, benchmarking::Environment,
};
use std::{hint::black_box, sync::Arc, time::Duration};

use super::matrix;

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const SOCKET_BUFFER: usize = 4 << 20;
const MEMORY_BUDGET: usize = 128 << 20;

fn payload_matches(payloads: &[Vec<u8>], index: usize, received: &[u8]) -> bool {
    payloads
        .get(index)
        .is_some_and(|expected| received == expected)
}

pub struct Fixture {
    pub tx: Endpoint,
    pub rx: Endpoint,
    pub session: Session,
    pub window: usize,
    wait_timeout: Duration,
    verify_payloads: Option<Arc<Vec<Vec<u8>>>>,
    channels: Vec<Channel>,
}

impl Fixture {
    pub fn new(env: &Environment, datagram: usize, window: usize) -> Self {
        Self::new_with_batch(env, datagram, window, EndpointConfig::default().batch_size)
    }

    pub fn new_with_batch(
        env: &Environment,
        datagram: usize,
        window: usize,
        batch_size: usize,
    ) -> Self {
        let config = EndpointConfig {
            datagram_size: datagram,
            max_peers: 4,
            memory_budget: MEMORY_BUDGET,
            socket_buffer_bytes: SOCKET_BUFFER,
            batch_size,
            ..EndpointConfig::default()
        };
        let tx = env
            .bind_endpoint(matrix::loopback(), config.clone())
            .unwrap();
        let rx = env.bind_endpoint(matrix::loopback(), config).unwrap();
        let session = tx.connect(rx.local_addr().unwrap()).unwrap();
        Self {
            tx,
            rx,
            session,
            window,
            wait_timeout: WAIT_TIMEOUT,
            verify_payloads: None,
            channels: Vec::new(),
        }
    }

    pub fn set_wait_timeout(&mut self, timeout: Duration) {
        self.wait_timeout = timeout;
    }

    pub fn set_verify_payloads(&mut self, payloads: Arc<Vec<Vec<u8>>>) {
        assert_eq!(payloads.len(), self.window);
        self.verify_payloads = Some(payloads);
    }

    pub fn enable_ordered(&mut self, count: usize) -> Result<()> {
        self.channels = (0..count)
            .map(|id| self.session.channel(id as u16))
            .collect::<Result<_>>()?;
        Ok(())
    }

    pub async fn window(&self, data: &[u8], reliable: bool, layout: bool) -> Result<usize> {
        self.window_layer(data, if reliable { 2 } else { 1 }, layout)
            .await
    }

    pub async fn ordered_window(&self, data: &[u8], layout: bool) -> Result<usize> {
        self.window_layer(data, 3, layout).await
    }

    async fn window_layer(&self, data: &[u8], layer: u8, layout: bool) -> Result<usize> {
        let reliable = layer >= 2;
        let ordered = layer == 3;
        if layout && self.verify_payloads.is_some() {
            return Err(Error::Protocol(
                "payload verification requires slice submissions",
            ));
        }
        let mut tickets = Vec::with_capacity(self.window);
        let mut ids = Vec::with_capacity(self.window);
        for index in 0..self.window {
            let payload = self
                .verify_payloads
                .as_ref()
                .map_or(data, |payloads| payloads[index].as_slice());
            let channel = if ordered {
                Some(&self.channels[index % self.channels.len()])
            } else {
                None
            };
            if layout {
                let mut frame = match channel {
                    Some(c) => c.allocate_frame(data.len())?,
                    None => self.session.allocate_frame(data.len())?,
                };
                for segment in 0..frame.segment_count() {
                    frame.payload_mut(segment).unwrap().fill(0x5a);
                }
                if reliable {
                    let ticket = match channel {
                        Some(c) => c.submit_layout_async(frame).await?,
                        None => self.session.submit_reliable_layout_async(frame).await?,
                    };
                    ids.push((ticket.frame_id(), index));
                    tickets.push(ticket);
                } else {
                    ids.push((self.session.send_layout_async(frame).await?.frame_id, index));
                }
            } else if reliable {
                let ticket = match channel {
                    Some(c) => c.submit_async(black_box(payload)).await?,
                    None => {
                        self.session
                            .submit_reliable_async(black_box(payload))
                            .await?
                    }
                };
                ids.push((ticket.frame_id(), index));
                tickets.push(ticket);
            } else {
                ids.push((
                    self.session.send_async(black_box(payload)).await?.frame_id,
                    index,
                ));
            }
        }
        for _ in 0..self.window {
            let frame = tokio::time::timeout(self.wait_timeout, self.rx.recv_async())
                .await
                .map_err(|_| Error::TimedOut)??;
            let Some(position) = ids.iter().position(|&(id, _)| id == frame.id()) else {
                return Err(Error::Protocol("duplicate/unexpected benchmark frame"));
            };
            if ordered {
                let first = tickets
                    .iter()
                    .filter(|ticket| ticket.channel() == frame.channel())
                    .find(|ticket| ids.iter().any(|&(id, _)| id == ticket.frame_id()))
                    .ok_or(Error::Protocol("ordered benchmark channel"))?;
                if first.frame_id() != frame.id()
                    || first.channel_sequence() != frame.channel_sequence()
                {
                    return Err(Error::Protocol("benchmark order violation"));
                }
            }
            if frame.len() != data.len() {
                return Err(Error::Protocol("benchmark frame length"));
            }
            let payload_index = ids[position].1;
            if self
                .verify_payloads
                .as_ref()
                .is_some_and(|payloads| !payload_matches(payloads, payload_index, frame.as_slice()))
            {
                return Err(Error::Protocol("benchmark payload mismatch"));
            }
            if ordered {
                ids.remove(position);
            } else {
                ids.swap_remove(position);
            }
            black_box(frame.as_slice());
        }
        for ticket in tickets {
            tokio::time::timeout(self.wait_timeout, ticket.wait_async())
                .await
                .map_err(|_| Error::TimedOut)??;
        }
        Ok(self.window * data.len())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.tx.close();
        let _ = self.rx.close();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn payload_verifier_rejects_swapped_and_corrupted_messages() {
        let payloads = vec![vec![1, 2, 3], vec![4, 5, 6]];
        assert!(super::payload_matches(&payloads, 0, &[1, 2, 3]));
        assert!(super::payload_matches(&payloads, 1, &[4, 5, 6]));
        assert!(!super::payload_matches(&payloads, 0, &[4, 5, 6]));
        assert!(!super::payload_matches(&payloads, 1, &[4, 5, 7]));
        assert!(!super::payload_matches(&payloads, 2, &[1, 2, 3]));
    }
}
