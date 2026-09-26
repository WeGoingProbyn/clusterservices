//! The only crate a clusterservices plugin depends on.
//!
//! A plugin author's whole job is business logic: read something, return
//! counters. This crate is the vocabulary for that, and nothing else — no tokio,
//! no sockets, no framing, no transport. The same plugin compiles unchanged
//! whether the agent is talking TCP, gRPC, or RDMA.
//!
//! # Writing a plugin
//!
//! 1. Define the service: a marker type implementing [`ServiceDef`], naming its
//!    data and command types. Both are [`Wire`] types, which every `prost`
//!    message already is.
//! 2. On the agent, implement [`Sampler`] — plus [`ServiceBound`] to say which
//!    service it belongs to, and [`CommandReceiver`] for the commands it accepts
//!    (an empty impl accepts the built-ins).
//! 3. On the server, implement [`Handler`] for the same service.
//!
//! ```
//! use std::time::Duration;
//! use cs_api::{CommandReceiver, JobInfo, NoCommand, Sampler, ServiceBound, ServiceDef};
//! use cs_util::Result;
//!
//! // 1. the service
//! struct Memory;
//! impl ServiceDef for Memory {
//!     const NAME: &'static str = "memory";
//!     type Data = u64; // stands in for a generated prost message
//!     type Command = NoCommand;
//! }
//!
//! // 2. the agent side
//! struct MemorySampler;
//!
//! impl ServiceBound for MemorySampler {
//!     type Service = Memory;
//! }
//! impl CommandReceiver for MemorySampler {}
//!
//! impl Sampler for MemorySampler {
//!     fn interval(&self) -> Duration {
//!         Duration::from_secs(5)
//!     }
//!
//!     fn sample(&mut self, jobs: &[JobInfo]) -> Result<Vec<u64>> {
//!         Ok(jobs.iter().map(|job| job.job_id.into()).collect())
//!     }
//! }
//! ```
//!
//! # What a service sees of the engine
//!
//! [`ServiceCtx`] — an [`Outbox`] for data, a [`WorkerSpawner`] for extra
//! threads, a [`ShutdownSignal`](cs_async_util::ShutdownSignal), a
//! [`Clock`](cs_async_util::Clock), [`EngineStats`], and command dispatch. That
//! is the entire surface. The engine reaches it through the narrow
//! [`runtime`] seam, which plugins never name.
//!
//! # Conventions worth knowing before you write a sampler
//!
//! - **Send cumulative counters, not rates.** The server computes rates and
//!   survives a missed sample; a rate computed on the agent does not.
//! - **Sample every few seconds, batch, and send every 30–60s.** Batches are
//!   columnar: one timestamp array plus one value array per metric.
//! - **Sampling is synchronous and runs on its own thread.** Blocking sysfs reads
//!   are expected and fine; nothing here touches the async runtime.

mod command;
mod ctx;
mod handler;
mod job;
mod sampler;
mod service;
mod stats;
mod wire;

pub mod runtime;

#[cfg(any(test, feature = "test-util"))]
pub mod test_support;

pub use command::{Command, CommandOpts, CommandOutcome, CommandReceiver, Reply};
pub use ctx::{MAX_THREAD_NAME_LEN, Outbox, ServiceCtx, WorkerSpawner, worker_thread_name};
pub use handler::Handler;
pub use job::{JobInfo, StepId};
pub use sampler::{Sampler, SamplerFactory};
pub use service::{
    Cmd, Data, MAX_SERVICE_NAME_LEN, ServiceBound, ServiceDef, ServiceId, is_valid_service_name,
};
pub use stats::{EngineStats, ServiceStats};
pub use wire::{Encodable, NoCommand, Wire};

// Re-exported so a plugin's `Cargo.toml` needs one dependency, not four.
pub use bytes;
pub use cs_async_util::{Clock, CommandHandle, ShutdownSignal};
pub use cs_util::{Error, ErrorKind, Result, ResultExt};
pub use prost;
