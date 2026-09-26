//! Runtime-agnostic async primitives.
//!
//! Everything the engine and the plugin API need to talk about time, shutdown,
//! and in-flight commands without naming a runtime. No tokio here — not even as
//! a dev-dependency — so nothing in `crates/core` can quietly grow a runtime
//! dependency.
//!
//! - [`ShutdownSignal`] — set once, observed by every worker.
//! - [`Clock`] — the only source of time for engine and sampler-loop code, so
//!   tests can drive it. Never call [`Instant::now`](std::time::Instant::now)
//!   or `std::thread::sleep` there directly.
//! - [`command_channel`] / [`CommandHandle`] — a one-shot result the engine
//!   completes when a `CommandResult` comes back.
//! - [`BoxFuture`] — for the few places that need `dyn` dispatch.

mod clock;
mod command;
mod shutdown;

#[cfg(any(test, feature = "test-util"))]
pub mod test_util;

pub use clock::Clock;
pub use command::{CommandHandle, CommandSender, command_channel};
pub use shutdown::{ShutdownSignal, WaitShutdown};

use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard};

/// A boxed, `Send` future. Used where a trait has to be `dyn`-usable.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Lock a mutex, ignoring poisoning.
///
/// Every mutex in this crate guards a small, always-consistent piece of state
/// (a waker slot, a one-shot value). A panic elsewhere cannot leave it half
/// updated, so poisoning carries no information and there is no reason to
/// propagate a panic — or to `unwrap` — here.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
