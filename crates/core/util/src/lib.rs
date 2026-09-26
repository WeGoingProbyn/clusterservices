//! Error type shared by every clusterservices crate.
//!
//! One error type for the whole framework. [`Error`] is a single pointer wide,
//! carries an [`ErrorKind`] that says *how to react* rather than what happened,
//! and records the source location of every layer of context, so a failure that
//! crossed four crates still reads as one stack:
//!
//! ```text
//! failed to start service "gpu" @ crates/runtime/engine/src/engine.rs:212
//! |- cause 1 - sampler factory failed @ crates/plugins/gpu/src/lib.rs:48
//! |- cause 2 - libnvidia-ml.so.1: cannot open shared object file
//! ```
//!
//! Construct with [`Error::new`], add context with [`ResultExt::context`]:
//!
//! ```
//! use cs_util::{Error, ErrorKind, Result, ResultExt};
//!
//! fn read_interval(raw: &str) -> Result<u64> {
//!     raw.parse()
//!         .map_err(|e| Error::with_source(ErrorKind::Config, "not a number", e))
//!         .context("parsing sample interval")
//! }
//!
//! let err = read_interval("soon").unwrap_err();
//! assert_eq!(err.kind(), ErrorKind::Config); // context inherits the kind
//! assert_eq!(err.chain().count(), 3);
//! ```
//!
//! This crate has no dependencies and must keep it that way.

mod error;

pub use error::{Chain, Error, ErrorKind, Frame, ResultExt};

/// [`Result`](core::result::Result) defaulted to the framework [`Error`].
pub type Result<T, E = Error> = core::result::Result<T, E>;
