//! Per-job snapshots: mergeable summaries of what a step did, sent upstream.
//!
//! A head keeps a running summary of every job step its nodes report, and sends one
//! snapshot per step to the tier above when the step ends. Raw samples stay at the head
//! and are discarded; snapshots are what an aggregator stores. The ratio is what makes
//! the design work: full samples for ten thousand nodes are on the order of a hundred
//! thousand values a second, and snapshots are on the order of a million rows a *day*.
//!
//! Three things follow from that, and they are the whole design:
//!
//! 1. **A snapshot is irreversible.** Once one closes, the samples behind it are gone —
//!    no migration recovers a statistic that was not computed. So it records generously:
//!    a histogram as well as scalars, sums rather than means, and counts that tell a
//!    metric nobody could read from one that read zero.
//! 2. **Every statistic merges.** Merging happens across the nodes of a multi-node job,
//!    across a partial snapshot and its successor, and across periods. A percentile
//!    cannot be merged from summaries, so the histogram is stored and the percentiles
//!    are derived on read.
//! 3. **Counters and gauges are summarised differently**, which is why
//!    [`cs_api::Metrics`] exists: a counter is monotonic, so its own minimum is its
//!    first sample, and what is wanted is statistics of its rate.
//!
//! This crate summarises *any* plugin's batch — it names no plugin, only
//! [`cs_api::Metrics`]. If it ever needs a plugin's message type, the seam has failed.
//!
//! ```no_run
//! # use cs_plugin_snapshot::{Accumulator, StepKey};
//! # use std::time::Instant;
//! # fn example<M: cs_api::Metrics>(batch: &M, now: Instant) {
//! let mut accumulator = Accumulator::new();
//! let step = StepKey::new("node-7", 1234, "0");
//!
//! // On each batch from that node, and again when the node says it was the last.
//! accumulator.fold(&step, 1000, batch, false, now);
//!
//! // Whatever has finished is ready to send upstream.
//! for snapshot in accumulator.take() {
//!     let _ = snapshot.job_id;
//! }
//! # }
//! ```

mod accumulate;
mod hist;
mod merge;
mod service;
mod stats;

/// The wire schema, generated from `proto/snapshot.proto`.
pub mod proto {
    #![allow(missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/cs.snapshot.v1.rs"));
}

pub use accumulate::{Accumulator, AccumulatorConfig, DEFAULT_PERIOD, DEFAULT_SILENCE, StepKey};
pub use merge::{mean, merge, stddev};
pub use proto::{CloseReason, JobSnapshot, Kind, MetricSummary};
pub use service::{SnapshotSampler, SnapshotService, Snapshots};
