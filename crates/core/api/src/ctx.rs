use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use cs_async_util::{Clock, CommandHandle, ShutdownSignal};
use cs_util::{Result, ResultExt};

use crate::runtime::{CommandKind, CommandRequest, DataSink, ServiceRuntime, WorkerHost};
use crate::{Command, CommandOpts, CommandOutcome, EngineStats, NoCommand, ServiceDef, ServiceId};

/// Linux truncates a thread name at 16 bytes including the terminator, leaving
/// 15 usable — and per-plugin CPU accounting reads those names back out of
/// `/proc/self/task/<tid>/comm`, so they have to stay distinguishable.
pub const MAX_THREAD_NAME_LEN: usize = 15;

/// Build the `<service>/<worker>` thread name for a worker, within the 15-byte
/// budget.
///
/// The service part is kept whole where possible, since it is what identifies the
/// plugin; the worker part is trimmed first, and only then the service part.
///
/// ```
/// use cs_api::worker_thread_name;
///
/// assert_eq!(worker_thread_name("cgroup", "sample"), "cgroup/sample");
/// // Trimmed to fit, worker first.
/// assert_eq!(worker_thread_name("cgroup", "reconciliation"), "cgroup/reconcil");
/// assert_eq!(worker_thread_name("verylongsvcnm", "worker"), "verylongsvcnm/w");
/// ```
#[must_use]
pub fn worker_thread_name(service: &str, worker: &str) -> String {
    // Leave at least one byte for the worker and one for the separator.
    let service = truncate(service, MAX_THREAD_NAME_LEN - 2);
    let worker = truncate(worker, MAX_THREAD_NAME_LEN - service.len() - 1);
    format!("{service}/{worker}")
}

/// Truncate to at most `max` bytes, on a char boundary.
fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let end = s
        .char_indices()
        .map(|(i, _)| i)
        .take_while(|&i| i <= max)
        .last()
        .unwrap_or(0);
    &s[..end]
}

/// Where a service puts the data it produces.
///
/// Cheap to clone, `Send + Sync`, and usable from any thread, so a service with
/// several workers gives each one a clone. The engine's writer drains the queue
/// during shutdown **until every clone is dropped**, so holding one alive past
/// `on_shutdown` holds up the shutdown deadline.
pub struct Outbox<S: ServiceDef> {
    sink: Arc<dyn DataSink>,
    id: ServiceId,
    _service: PhantomData<fn() -> S>,
}

impl<S: ServiceDef> Outbox<S> {
    /// Wrap a sink. The engine calls this; plugins receive the result.
    #[must_use]
    pub fn new(sink: Arc<dyn DataSink>) -> Self {
        Self {
            sink,
            id: ServiceId::of::<S>(),
            _service: PhantomData,
        }
    }

    /// Queue one message.
    ///
    /// Encodes `msg` on the calling thread and returns — it does not wait for
    /// the network. Overflow drops the oldest queued message rather than
    /// failing, so an error here means the message can never be sent (the engine
    /// is stopping, or the service was deregistered).
    pub fn send(&self, msg: S::Data) -> Result<()> {
        self.sink
            .send(self.id, &msg)
            .with_context(|| format!("sending {} data", self.id))
    }

    /// Which service this outbox belongs to.
    #[must_use]
    pub const fn service(&self) -> ServiceId {
        self.id
    }
}

impl<S: ServiceDef> Clone for Outbox<S> {
    fn clone(&self) -> Self {
        Self {
            sink: Arc::clone(&self.sink),
            id: self.id,
            _service: PhantomData,
        }
    }
}

impl<S: ServiceDef> fmt::Debug for Outbox<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outbox").field("service", &self.id).finish()
    }
}

/// Starts extra threads for a service.
///
/// A [`Sampler`](crate::Sampler) already runs on its own thread and does not
/// need this. It is for services that need more than one — a producer plus a
/// reconciler, say — which they start from
/// [`Sampler::start`](crate::Sampler::start).
#[derive(Clone)]
pub struct WorkerSpawner {
    host: Arc<dyn WorkerHost>,
    service: ServiceId,
}

impl WorkerSpawner {
    /// Wrap a host. The engine calls this; plugins receive the result.
    #[must_use]
    pub const fn new(host: Arc<dyn WorkerHost>, service: ServiceId) -> Self {
        Self { host, service }
    }

    /// Start a worker thread named `<service>/<worker>`.
    ///
    /// The body owns whatever it needs — cloned [`Outbox`], cloned
    /// [`ShutdownSignal`] — and must return when the signal fires; the engine
    /// joins it under the shutdown deadline.
    pub fn spawn_blocking_worker(
        &self,
        worker: &str,
        body: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        let name = worker_thread_name(self.service.name, worker);
        self.host
            .spawn_blocking(&name, Box::new(body))
            .with_context(|| format!("spawning worker thread {name}"))
    }

    /// Which service these workers belong to.
    #[must_use]
    pub const fn service(&self) -> ServiceId {
        self.service
    }
}

impl fmt::Debug for WorkerSpawner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerSpawner")
            .field("service", &self.service)
            .finish()
    }
}

/// Everything a service sees of the engine.
///
/// Note what is *not* here: no transport, no runtime, no sockets, no `tokio`. A
/// plugin written against `ServiceCtx` keeps compiling when the transport
/// changes.
pub struct ServiceCtx<S: ServiceDef> {
    outbox: Outbox<S>,
    workers: WorkerSpawner,
    shutdown: ShutdownSignal,
    runtime: Arc<dyn ServiceRuntime>,
}

impl<S: ServiceDef> ServiceCtx<S> {
    /// Assemble a context. The engine calls this once per registered service.
    #[must_use]
    pub fn new(
        sink: Arc<dyn DataSink>,
        host: Arc<dyn WorkerHost>,
        shutdown: ShutdownSignal,
        runtime: Arc<dyn ServiceRuntime>,
    ) -> Self {
        Self {
            outbox: Outbox::new(sink),
            workers: WorkerSpawner::new(host, ServiceId::of::<S>()),
            shutdown,
            runtime,
        }
    }

    /// Which service this context belongs to.
    #[must_use]
    pub const fn service(&self) -> ServiceId {
        self.outbox.service()
    }

    /// This node's name, as sent in `Hello`.
    #[must_use]
    pub fn node(&self) -> &str {
        self.runtime.node()
    }

    /// Queue one message. Shorthand for `ctx.outbox().send(msg)`.
    pub fn send(&self, msg: S::Data) -> Result<()> {
        self.outbox.send(msg)
    }

    /// The outbox, to clone for extra workers.
    #[must_use]
    pub const fn outbox(&self) -> &Outbox<S> {
        &self.outbox
    }

    /// The worker spawner.
    #[must_use]
    pub const fn workers(&self) -> &WorkerSpawner {
        &self.workers
    }

    /// Start a worker thread. Shorthand for
    /// `ctx.workers().spawn_blocking_worker(..)`.
    pub fn spawn_blocking_worker(
        &self,
        worker: &str,
        body: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        self.workers.spawn_blocking_worker(worker, body)
    }

    /// The shutdown signal, to clone into workers.
    #[must_use]
    pub const fn shutdown_signal(&self) -> &ShutdownSignal {
        &self.shutdown
    }

    /// Whether shutdown has begun. Long loops should check this.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.is_set()
    }

    /// The clock to use for any timing, so tests can control it.
    #[must_use]
    pub fn clock(&self) -> &Arc<dyn Clock> {
        self.runtime.clock()
    }

    /// The engine's own counters, for self-monitoring.
    #[must_use]
    pub fn engine_stats(&self) -> EngineStats {
        self.runtime.engine_stats()
    }

    /// Send a command to one service on one node.
    ///
    /// `T` is the target service, which need not be this one: a server-side
    /// handler for `cgroup` data can retune the `gpu` sampler on the node that
    /// sent it. The returned handle resolves when the node replies, or errors if
    /// the command could not be delivered at all.
    pub fn command<T: ServiceDef>(
        &self,
        node: &str,
        cmd: Command<T::Command>,
        opts: CommandOpts,
    ) -> CommandHandle<CommandOutcome> {
        // `cmd` outlives the borrow taken here, so the engine can encode the
        // payload during `dispatch_command` without it being copied first.
        let kind = match &cmd {
            Command::Shutdown => CommandKind::Shutdown,
            Command::Restart => CommandKind::Restart,
            Command::Custom(payload) => CommandKind::Custom(payload),
        };
        self.runtime.dispatch_command(CommandRequest {
            node,
            service: ServiceId::of::<T>(),
            kind,
            opts,
        })
    }

    /// Send a built-in command to a whole agent.
    ///
    /// Every service is polled first; one rejection aborts the command unless
    /// [`CommandOpts::force`] is set.
    pub fn agent_command(
        &self,
        node: &str,
        cmd: Command<NoCommand>,
        opts: CommandOpts,
    ) -> CommandHandle<CommandOutcome> {
        let kind = match cmd {
            Command::Shutdown => CommandKind::Shutdown,
            Command::Restart => CommandKind::Restart,
            Command::Custom(never) => match never {},
        };
        self.runtime.dispatch_command(CommandRequest {
            node,
            service: ServiceId::AGENT,
            kind,
            opts,
        })
    }
}

impl<S: ServiceDef> Clone for ServiceCtx<S> {
    fn clone(&self) -> Self {
        Self {
            outbox: self.outbox.clone(),
            workers: self.workers.clone(),
            shutdown: self.shutdown.clone(),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

impl<S: ServiceDef> fmt::Debug for ServiceCtx<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceCtx")
            .field("service", &self.service())
            .field("node", &self.node())
            .field("shutting_down", &self.is_shutting_down())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_names_are_left_alone() {
        assert_eq!(worker_thread_name("cgroup", "sample"), "cgroup/sample");
        assert_eq!(worker_thread_name("gpu", "nvml"), "gpu/nvml");
    }

    #[test]
    fn names_fit_the_kernel_budget() {
        for (service, worker) in [
            ("cgroup", "reconciliation"),
            ("verylongsvcnm", "worker"),
            ("selfmon", "procfs-scraper"),
            ("a", "b"),
            ("x", "wwwwwwwwwwwwwwwwwwww"),
        ] {
            let name = worker_thread_name(service, worker);
            assert!(
                name.len() <= MAX_THREAD_NAME_LEN,
                "{name:?} is {} bytes",
                name.len()
            );
            assert!(name.contains('/'), "{name:?} should stay distinguishable");
        }
    }

    #[test]
    fn the_worker_part_is_trimmed_before_the_service_part() {
        // 6 + 1 + 8 = 15: the service name survives whole.
        assert_eq!(
            worker_thread_name("cgroup", "reconciliation"),
            "cgroup/reconcil"
        );
        // A service name too long to fit gives the worker a single byte.
        assert_eq!(
            worker_thread_name("verylongsvcnm", "worker"),
            "verylongsvcnm/w"
        );
    }

    #[test]
    fn a_service_name_over_budget_is_trimmed_too() {
        let name = worker_thread_name("abcdefghijklmnopqrstuv", "worker");
        assert_eq!(name, "abcdefghijklm/w");
        assert_eq!(name.len(), MAX_THREAD_NAME_LEN);
    }

    #[test]
    fn truncation_respects_char_boundaries() {
        // Four-byte chars: cutting mid-char would panic or produce invalid UTF-8.
        let name = worker_thread_name("svc", "𝄞𝄞𝄞𝄞𝄞");
        assert!(name.len() <= MAX_THREAD_NAME_LEN);
        assert_eq!(name, "svc/𝄞𝄞");
    }
}
