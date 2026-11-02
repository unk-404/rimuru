//! FFI endpoint object: default CQ, receive pump, session/channel caches.

use super::{
    abi::{
        RM_EVENT_CHANNEL_CLOSED, RM_EVENT_ENDPOINT_CLOSED, RM_EVENT_MESSAGE, RM_INVALID_ARGUMENT,
        RM_INVALID_STATE, RM_NO_BUFFER, RM_OK, RmEvent, RmStatus, status_from_error,
    },
    cq::Cq,
    handle::{self, Object},
    objects::{ChannelObj, Ids, MessageObj, SessionObj, message_event},
    waiter::Waiter,
};
use crate::{Channel, Endpoint, Error, Frame, Runtime};
use dashmap::{DashMap, mapref::entry::Entry};
use std::{
    net::SocketAddr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering::Release},
    },
};
use tokio::task::JoinHandle;

pub(crate) struct EndpointObj {
    pub endpoint: Endpoint,
    pub default_cq: Arc<Cq>,
    pub default_waiter: OnceLock<Arc<Waiter>>,
    pub closed: AtomicBool,
    pub ids: Ids,
    pub sessions: DashMap<SocketAddr, Arc<SessionObj>, foldhash::fast::RandomState>,
    pub channels: DashMap<(SocketAddr, u16), Arc<ChannelObj>, foldhash::fast::RandomState>,
    pub receive_routes: DashMap<(SocketAddr, u16), Arc<Cq>, foldhash::fast::RandomState>,
    receive_pump: OnceLock<JoinHandle<()>>,
    channel_pump: OnceLock<JoinHandle<()>>,
}

impl EndpointObj {
    pub fn new(endpoint: Endpoint, cq_capacity: usize) -> Result<Arc<Self>, RmStatus> {
        let budget = endpoint.owner().shared.pool.budget.clone();
        let default_cq = Cq::new(budget, cq_capacity).ok_or(RM_NO_BUFFER)?;
        let object = Arc::new(Self {
            endpoint,
            default_cq,
            default_waiter: OnceLock::new(),
            closed: AtomicBool::new(false),
            ids: Ids::new(),
            sessions: DashMap::with_hasher(foldhash::fast::RandomState::default()),
            channels: DashMap::with_hasher(foldhash::fast::RandomState::default()),
            receive_routes: DashMap::with_hasher(foldhash::fast::RandomState::default()),
            receive_pump: OnceLock::new(),
            channel_pump: OnceLock::new(),
        });
        EndpointObj::start_receive_pump(&object);
        Ok(object)
    }

    pub fn runtime(&self) -> &Runtime {
        &self.endpoint.owner().runtime
    }

    fn start_receive_pump(self: &Arc<Self>) {
        let object = self.clone();
        let handle = self.runtime().spawn(async move {
            object.receive_loop().await;
        });
        let _ = self.receive_pump.set(handle);
    }

    async fn receive_loop(self: Arc<Self>) {
        loop {
            match self.endpoint.recv_async().await {
                Ok(frame) => self.deliver(frame).await,
                Err(Error::Closed) => break,
                Err(_) => continue,
            }
        }
    }

    async fn deliver(self: &Arc<Self>, frame: Frame) {
        let peer = frame.peer_addr();
        let layer = frame.layer();
        let channel = frame.channel();
        let channel_seq = frame.channel_sequence().unwrap_or(0);
        let delivery_seq = frame.delivery_sequence().unwrap_or(0);
        let cq = if layer == 3 {
            self.receive_routes
                .get(&(peer, channel))
                .map(|route| route.clone())
                .unwrap_or_else(|| self.default_cq.clone())
        } else {
            self.default_cq.clone()
        };
        let Some(permit) = cq.reserve_async().await else {
            return;
        };
        let session_id = self.session_id(peer);
        let message = match handle::insert(Object::Message(Arc::new(MessageObj { frame }))) {
            Ok(handle) => handle,
            Err(status) => {
                let mut event = message_event(
                    RM_EVENT_MESSAGE,
                    status,
                    0,
                    session_id,
                    delivery_seq,
                    channel_seq,
                    channel,
                    layer,
                );
                event.flags = 1;
                cq.publish(event, permit);
                return;
            }
        };
        cq.publish(
            message_event(
                RM_EVENT_MESSAGE,
                RM_OK,
                message,
                session_id,
                delivery_seq,
                channel_seq,
                channel,
                layer,
            ),
            permit,
        );
    }

    fn session_id(self: &Arc<Self>, peer: SocketAddr) -> u64 {
        if let Some(existing) = self.sessions.get(&peer) {
            return existing.id;
        }
        let Some(session) = self.endpoint.session(peer) else {
            return 0;
        };
        let candidate = Arc::new(SessionObj {
            endpoint: self.endpoint.clone(),
            session,
            peer,
            id: self.ids.next(),
            default_cq: self.default_cq.clone(),
            closed: AtomicBool::new(false),
            owner: Arc::downgrade(self),
        });
        match self.sessions.entry(peer) {
            Entry::Occupied(entry) => entry.get().id,
            Entry::Vacant(entry) => {
                let id = candidate.id;
                entry.insert(candidate);
                id
            }
        }
    }

    pub fn session_for(self: &Arc<Self>, peer: SocketAddr) -> Result<Arc<SessionObj>, RmStatus> {
        if let Some(existing) = self.sessions.get(&peer)
            && !existing.is_closed()
        {
            return Ok(existing.clone());
        }
        let session = flatten(
            self.runtime()
                .block_on_ffi(self.endpoint.connect_async(peer)),
        )?;
        let candidate = Arc::new(SessionObj {
            endpoint: self.endpoint.clone(),
            session,
            peer,
            id: self.ids.next(),
            default_cq: self.default_cq.clone(),
            closed: AtomicBool::new(false),
            owner: Arc::downgrade(self),
        });
        match self.sessions.entry(peer) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                entry.insert(candidate.clone());
                Ok(candidate)
            }
        }
    }

    pub fn session_handle(&self, peer: SocketAddr) -> Option<Arc<SessionObj>> {
        self.sessions.get(&peer).map(|entry| entry.clone())
    }

    pub fn channel_for(
        self: &Arc<Self>,
        session: &Arc<SessionObj>,
        layer: u8,
        id: u16,
        cq: Arc<Cq>,
    ) -> Result<Arc<ChannelObj>, RmStatus> {
        if !(1..=3).contains(&layer) {
            return Err(RM_INVALID_ARGUMENT);
        }
        if let Some(existing) = self.channels.get(&(session.peer, id)) {
            if existing.layer != layer || !Arc::ptr_eq(&existing.cq, &cq) {
                return Err(RM_INVALID_STATE);
            }
            return Ok(existing.clone());
        }
        let channel: Option<Channel> = if layer == 3 {
            Some(
                self.runtime()
                    .block_on_ffi(session.session.channel_async(id))
                    .map_err(status_from_error)
                    .and_then(|inner| inner.map_err(status_from_error))?,
            )
        } else {
            None
        };
        let object = Arc::new(ChannelObj {
            session: session.session.clone(),
            layer,
            id,
            channel,
            cq: cq.clone(),
            endpoint: self.endpoint.clone(),
            closed: AtomicBool::new(false),
            owner: Arc::downgrade(self),
        });
        if layer == 3 {
            self.receive_routes.insert((session.peer, id), cq);
            self.ensure_channel_pump();
        }
        self.channels.insert((session.peer, id), object.clone());
        Ok(object)
    }

    fn ensure_channel_pump(self: &Arc<Self>) {
        if self.channel_pump.get().is_some() {
            return;
        }
        let object = self.clone();
        let runtime = self.runtime().clone();
        let handle = runtime.spawn(async move {
            loop {
                match object.endpoint.channel_event_async().await {
                    Ok(event) => {
                        let cq = object
                            .receive_routes
                            .get(&(event.peer, event.channel))
                            .map(|route| route.clone())
                            .unwrap_or_else(|| object.default_cq.clone());
                        let Some(permit) = cq.reserve_async().await else {
                            return;
                        };
                        let mut record = RmEvent::empty(RM_EVENT_CHANNEL_CLOSED, RM_OK);
                        record.channel_id = event.channel;
                        record.session_id = object.session_id(event.peer);
                        record.layer = 3;
                        cq.publish(record, permit);
                    }
                    Err(Error::Closed) => return,
                    Err(_) => continue,
                }
            }
        });
        let _ = self.channel_pump.set(handle);
    }

    pub fn waiter(self: &Arc<Self>) -> Result<Arc<Waiter>, RmStatus> {
        if let Some(waiter) = self.default_waiter.get() {
            return Ok(waiter.clone());
        }
        let budget = self.endpoint.owner().shared.pool.budget.clone();
        let waiter = Waiter::new(self.default_cq.clone(), budget).ok_or(RM_NO_BUFFER)?;
        let _ = self.default_waiter.set(waiter);
        Ok(self
            .default_waiter
            .get()
            .expect("waiter initialized")
            .clone())
    }

    pub fn close(&self) {
        if self.closed.swap(true, Release) {
            return;
        }
        let endpoint = self.endpoint.clone();
        self.runtime()
            .block_on_ffi(async move { endpoint.close_async().await })
            .ok();
        self.default_cq.close(RM_EVENT_ENDPOINT_CLOSED, RM_OK);
        for entry in &self.sessions {
            entry.value().close();
        }
        if let Some(pump) = self.receive_pump.get() {
            pump.abort();
        }
        if let Some(pump) = self.channel_pump.get() {
            pump.abort();
        }
    }
}

fn flatten<T>(result: crate::Result<Result<T, Error>>) -> Result<T, RmStatus> {
    match result {
        Ok(inner) => inner.map_err(status_from_error),
        Err(error) => Err(status_from_error(error)),
    }
}
