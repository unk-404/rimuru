//! The single endpoint owner: command mailbox, peer table and packet dispatch.
//!
//! Every protocol state transition happens on the receive loop thread. Public
//! handles communicate with it through the bounded `Command` mailbox; they never
//! mutate session or channel state directly.

mod commands;
mod packet;
mod peer;
mod receive;

pub(crate) use peer::Peer;
pub(crate) use receive::ReceiveLoop;

use crate::{
    Error, Result,
    buffer::Permit,
    context::{ChannelState, Context},
    handshake::Reply,
    reliable::Signal,
    runtime::Runtime,
    session::FrameLayout,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::{mpsc, oneshot};

type ChannelReply = oneshot::Sender<Result<Arc<ChannelState>>>;

pub(crate) enum Command {
    OpenChannel {
        peer: SocketAddr,
        incarnation: u64,
        id: u16,
        reply: ChannelReply,
    },
    CloseChannel {
        peer: SocketAddr,
        incarnation: u64,
        channel: u16,
    },
    OrderedSubmit {
        layout: FrameLayout,
        payload_crc: u32,
        signal: Arc<Signal>,
        permit: Permit,
        lane_permit: Permit,
        channel: Arc<ChannelState>,
        reply: oneshot::Sender<Result<(u32, u32)>>,
    },
    Submit {
        layout: FrameLayout,
        signal: Arc<Signal>,
        permit: Permit,
    },
    Cancel {
        peer: SocketAddr,
        incarnation: u64,
        id: u32,
    },
    Connect {
        peer: SocketAddr,
        reply: Reply,
    },
    Disconnect {
        peer: SocketAddr,
        reply: oneshot::Sender<()>,
    },
}

/// Shared handle to one endpoint owner and its implicit runtime.
pub(crate) struct Owner {
    pub runtime: Runtime,
    pub shared: Arc<Context>,
    commands: mpsc::Sender<Command>,
}

impl Owner {
    pub fn new(runtime: Runtime, shared: Arc<Context>, commands: mpsc::Sender<Command>) -> Self {
        Self {
            runtime,
            shared,
            commands,
        }
    }

    /// Enqueue from a synchronous API. Full maps to `WouldBlock`, closed to `Closed`.
    pub fn try_command(&self, command: Command) -> Result<()> {
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::WouldBlock,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })
    }

    /// Enqueue from an async API, waiting for queue capacity.
    pub async fn command(&self, command: Command) -> Result<()> {
        self.commands.send(command).await.map_err(|_| Error::Closed)
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.signal_close();
    }
}
