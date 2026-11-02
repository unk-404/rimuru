//! Whole messages over one UDP socket per endpoint.
//! L1 send reports local transmission; L2 send_reliable waits for remote publication.
//! L3 send_ordered adds reliable per-channel ordered publication.
//! Endpoints, sessions, channels and completion tickets may be used across threads.
//!
//! The C ABI lives in [`ffi`] and is compiled on every supported target with the
//! same export surface.

/// Controlled fixtures for this crate's performance tools, not an application executor API.
#[doc(hidden)]
pub mod benchmarking;
mod buffer;
mod checksum;
mod config;
mod context;
mod dispatch;
mod endpoint;
mod error;
pub mod ffi;
mod handshake;
mod metrics;
mod ordered;
mod reassembly;
mod reliable;
mod runtime;
mod session;
pub mod udp;
mod wire;

pub use buffer::Frame;
pub(crate) use config::RuntimeConfig;
pub use config::{EndpointConfig, Integrity, ReliableConfig};
pub use context::{ChannelEnd, ChannelEvent};
pub use endpoint::Endpoint;
pub use error::{Error, Result};
pub use metrics::MetricsSnapshot;
pub use ordered::Channel;
pub use reliable::{AbortReason, DeliveryOutcome, DeliveryReceipt, SendTicket};
pub(crate) use runtime::Runtime;
pub use session::{FrameLayout, SendReceipt, Session};
