//! The engine: one implementation for both roles.
//!
//! The same `NodeEngine` runs an agent (dials one server, samples, sends) and a
//! server (accepts many agents, routes their data to handlers, sends them
//! commands). A relay tier is simply one that does both. It is generic over the
//! [`Transport`](cs_transport::Transport) and never boxes it, so choosing a
//! transport costs nothing at run time — and everything that would otherwise be
//! written per transport lives here instead, written once.
//!
//! ```no_run
//! use cs_engine::{EngineConfig, NodeEngine};
//! # async fn example<T: cs_transport::Transport>(transport: T) -> cs_util::Result<()> {
//! let engine = NodeEngine::builder(transport)
//!     .config(EngineConfig::new("node-0042"))
//!     .dial("tcp://head01:7777".parse()?)
//!     .build()?;
//!
//! let handle = engine.handle();        // stop it, or read its counters
//! let stopped = engine.run().await?;   // returns when something stops it
//! # let _ = (handle, stopped);
//! # Ok(())
//! # }
//! ```
//!
//! # What it is responsible for
//!
//! - **Two lanes per peer.** Control is drained before data and never dropped;
//!   data is bounded and drops the oldest, because cumulative counters survive a
//!   gap and unbounded memory does not.
//! - **One outbound queue that outlives connections.** An agent keeps buffering
//!   while it is disconnected, and a reconnect finds the buffer intact.
//! - **Chunking.** Payloads above the transport's frame ceiling are split, and
//!   reassembled at the far end under a bound on what a peer can make us hold.
//! - **Commands.** Issued with an id and a deadline, queued per node while it is
//!   away, re-timed on delivery so expiry needs no clock synchronisation, and
//!   answered exactly once.
//! - **Supervision.** A sampler that panics is rebuilt from its factory with
//!   backoff; one that refuses to start is left stopped without taking the engine
//!   with it.
//! - **Shutdown, in order.** Services flush, plugin threads join, handlers close,
//!   `Goodbye` goes out behind everything already queued, writers drain — all
//!   under one deadline.
//!
//! Every deadline goes through the [`Clock`](cs_async_util::Clock), so a test with
//! `start_paused = true` controls the whole engine by advancing time.

mod chunk;
mod command;
mod config;
mod engine;
mod peer;
mod queue;
mod service;
mod stats;
mod worker;

pub use config::{Backoff, EngineConfig};
pub use engine::{EngineBuilder, EngineHandle, NodeEngine, TokioClock};
pub use peer::Stop;
// Re-exported for convenience: an engine is configured with one of these, but the
// trait lives in cs-api so a plugin can implement it without depending on the
// engine.
pub use cs_api::{JobSource, NoJobs};
