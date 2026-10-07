//! Low-footprint WebSocket fan-out relay.
//!
//! Each URL path under a prefix (default `/rebroadcast`) is a topic. Every text
//! or binary message a client sends is broadcast to all clients on the same
//! topic, the sender included; topics are fully isolated from each other. Messages are reference-counted, so
//! a broadcast stores the payload once no matter how many clients receive it.

pub mod config;
pub mod metrics;
mod relay;
#[cfg(feature = "metrics")]
pub mod stats_http;
mod topics;

pub use relay::{RelaySettings, serve};
