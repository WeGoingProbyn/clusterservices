//! A fake engine, for testing plugins without one.
//!
//! Enabled by the `test-util` feature. [`FakeEngine`] implements the whole
//! [`runtime`](crate::runtime) seam by recording what a service does, so a plugin
//! crate can unit-test its own logic — "given these jobs, does my sampler emit
//! the right counters?" — with no transport, no server, and no runtime.
//!
//! It is a recorder, not a simulation: nothing is sent anywhere, dispatched
//! commands resolve to whatever [`FakeEngine::set_command_outcome`] was told to
//! give, and its clock never advances. End-to-end behaviour belongs in
//! `cs-testkit` against the mock transport.

use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use cs_async_util::{BoxFuture, Clock, CommandHandle, ShutdownSignal, command_channel};
use cs_util::{Error, ErrorKind, Result};

use crate::runtime::{CommandKind, CommandRequest, DataSink, ServiceRuntime, WorkerHost};
use crate::{
    CommandOpts, CommandOutcome, Encodable, EngineStats, ServiceCtx, ServiceDef, ServiceId, Wire,
};

/// A clock that reports a fixed instant and never sleeps.
///
/// Enough for code that reads [`Clock::now`]; a test that needs time to pass
/// should use the engine's controllable clock instead.
#[derive(Debug)]
pub struct FrozenClock {
    at: Instant,
}

impl FrozenClock {
    /// A clock stopped at the current instant.
    #[must_use]
    pub fn new() -> Self {
        Self { at: Instant::now() }
    }
}

impl Default for FrozenClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for FrozenClock {
    fn now(&self) -> Instant {
        self.at
    }

    fn sleep(&self, _duration: Duration) -> BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// One recorded command dispatch.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SentCommand {
    /// Node it was addressed to.
    pub node: String,
    /// Service it was addressed to, or [`ServiceId::AGENT`].
    pub service: ServiceId,
    /// Which command, with any custom payload already encoded.
    pub kind: SentCommandKind,
    /// Delivery options it was sent with.
    pub opts: CommandOpts,
}

/// The command variant recorded in a [`SentCommand`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SentCommandKind {
    /// `Command::Shutdown`.
    Shutdown,
    /// `Command::Restart`.
    Restart,
    /// `Command::Custom`, as encoded bytes.
    Custom(Vec<u8>),
}

/// A stand-in engine that records everything a service asks of it.
///
/// ```
/// use cs_api::test_support::FakeEngine;
/// use cs_api::{NoCommand, ServiceDef};
///
/// struct Cpu;
/// impl ServiceDef for Cpu {
///     const NAME: &'static str = "cpu";
///     type Data = String;
///     type Command = NoCommand;
/// }
///
/// let engine = FakeEngine::new("node-0042");
/// let ctx = engine.ctx::<Cpu>();
///
/// ctx.send("batch".to_owned()).unwrap();
/// assert_eq!(engine.sent::<Cpu>().unwrap(), ["batch".to_owned()]);
/// assert_eq!(ctx.node(), "node-0042");
/// ```
pub struct FakeEngine {
    node: String,
    clock: Arc<dyn Clock>,
    shutdown: ShutdownSignal,
    sent: Mutex<Vec<(ServiceId, Vec<u8>)>>,
    commands: Mutex<Vec<SentCommand>>,
    workers: Mutex<Vec<(String, JoinHandle<()>)>>,
    stats: Mutex<EngineStats>,
    outcome: Mutex<CommandOutcome>,
    accepting: Mutex<bool>,
}

impl FakeEngine {
    /// A fake engine for a node named `node`.
    #[must_use]
    pub fn new(node: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            node: node.into(),
            clock: Arc::new(FrozenClock::new()),
            shutdown: ShutdownSignal::new(),
            sent: Mutex::new(Vec::new()),
            commands: Mutex::new(Vec::new()),
            workers: Mutex::new(Vec::new()),
            stats: Mutex::new(EngineStats::default()),
            outcome: Mutex::new(CommandOutcome::Ok),
            accepting: Mutex::new(true),
        })
    }

    /// A context for service `S`, as the engine would hand it to a plugin.
    #[must_use]
    pub fn ctx<S: ServiceDef>(self: &Arc<Self>) -> ServiceCtx<S> {
        ServiceCtx::new(
            Arc::clone(self) as Arc<dyn DataSink>,
            Arc::clone(self) as Arc<dyn WorkerHost>,
            self.shutdown.clone(),
            Arc::clone(self) as Arc<dyn ServiceRuntime>,
        )
    }

    /// Everything sent by `S`, decoded.
    ///
    /// Fails if a recorded message does not decode as `S::Data`, which would mean
    /// the plugin sent something other than what its [`ServiceDef`] promises.
    pub fn sent<S: ServiceDef>(&self) -> Result<Vec<S::Data>> {
        lock(&self.sent)
            .iter()
            .filter(|(id, _)| id.name == S::NAME)
            .map(|(_, bytes)| S::Data::decode(bytes.clone().into()))
            .collect()
    }

    /// Raw recorded messages, in send order, for any service.
    #[must_use]
    pub fn sent_raw(&self) -> Vec<(ServiceId, Vec<u8>)> {
        lock(&self.sent).clone()
    }

    /// How many messages a service has sent.
    #[must_use]
    pub fn sent_count(&self) -> usize {
        lock(&self.sent).len()
    }

    /// Commands dispatched through [`ServiceCtx::command`], in order.
    #[must_use]
    pub fn commands(&self) -> Vec<SentCommand> {
        lock(&self.commands).clone()
    }

    /// Names of the worker threads a service started.
    #[must_use]
    pub fn worker_names(&self) -> Vec<String> {
        lock(&self.workers)
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Wait for every worker thread to finish.
    ///
    /// Fire the [`shutdown_signal`](FakeEngine::shutdown_signal) first, or a
    /// well-behaved worker will never return.
    ///
    /// # Panics
    ///
    /// If a worker thread panicked.
    pub fn join_workers(&self) {
        let workers = std::mem::take(&mut *lock(&self.workers));
        for (name, handle) in workers {
            handle
                .join()
                .unwrap_or_else(|_| panic!("worker thread {name} panicked"));
        }
    }

    /// The shutdown signal shared with every context this engine builds.
    #[must_use]
    pub fn shutdown_signal(&self) -> &ShutdownSignal {
        &self.shutdown
    }

    /// What [`ServiceCtx::command`] should resolve to from now on.
    pub fn set_command_outcome(&self, outcome: CommandOutcome) {
        *lock(&self.outcome) = outcome;
    }

    /// Set the stats [`ServiceCtx::engine_stats`] will return.
    pub fn set_engine_stats(&self, stats: EngineStats) {
        *lock(&self.stats) = stats;
    }

    /// Make every later [`Outbox::send`](crate::Outbox::send) fail, as the engine
    /// does once it has stopped accepting data.
    pub fn stop_accepting(&self) {
        *lock(&self.accepting) = false;
    }
}

impl DataSink for FakeEngine {
    fn send(&self, service: ServiceId, msg: &dyn Encodable) -> Result<()> {
        if !*lock(&self.accepting) {
            return Err(Error::new(
                ErrorKind::Shutdown,
                "fake engine is no longer accepting data",
            ));
        }
        let mut buf = BytesMut::with_capacity(msg.encoded_len());
        msg.encode(&mut buf);
        lock(&self.sent).push((service, buf.to_vec()));
        Ok(())
    }
}

impl WorkerHost for FakeEngine {
    fn spawn_blocking(&self, name: &str, body: Box<dyn FnOnce() + Send + 'static>) -> Result<()> {
        let handle = std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(body)
            .map_err(|e| Error::with_source(ErrorKind::Plugin, "spawning fake worker", e))?;
        lock(&self.workers).push((name.to_owned(), handle));
        Ok(())
    }
}

impl ServiceRuntime for FakeEngine {
    fn node(&self) -> &str {
        &self.node
    }

    fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    fn engine_stats(&self) -> EngineStats {
        lock(&self.stats).clone()
    }

    fn dispatch_command(&self, request: CommandRequest<'_>) -> CommandHandle<CommandOutcome> {
        let kind = match request.kind {
            CommandKind::Shutdown => SentCommandKind::Shutdown,
            CommandKind::Restart => SentCommandKind::Restart,
            CommandKind::Custom(payload) => {
                let mut buf = BytesMut::with_capacity(payload.encoded_len());
                payload.encode(&mut buf);
                SentCommandKind::Custom(buf.to_vec())
            }
        };
        lock(&self.commands).push(SentCommand {
            node: request.node.to_owned(),
            service: request.service,
            kind,
            opts: request.opts,
        });

        let (sender, handle) = command_channel();
        sender.complete(lock(&self.outcome).clone());
        handle
    }
}

impl std::fmt::Debug for FakeEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeEngine")
            .field("node", &self.node)
            .field("sent", &self.sent_count())
            .field("commands", &lock(&self.commands).len())
            .field("workers", &self.worker_names())
            .finish()
    }
}

/// See `cs_async_util::lock` — poisoning carries no information for these
/// recording buffers.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A context wired to a throwaway [`FakeEngine`], when the recording is not
/// needed.
#[must_use]
pub fn fake_ctx<S: ServiceDef>() -> ServiceCtx<S> {
    FakeEngine::new("test-node").ctx::<S>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, NoCommand};
    use cs_async_util::test_util::block_on;

    struct Cpu;

    impl ServiceDef for Cpu {
        const NAME: &'static str = "cpu";
        type Data = String;
        type Command = String;
    }

    struct Gpu;

    impl ServiceDef for Gpu {
        const NAME: &'static str = "gpu";
        type Data = u64;
        type Command = NoCommand;
    }

    #[test]
    fn sent_messages_are_recorded_per_service() {
        let engine = FakeEngine::new("node-1");
        engine.ctx::<Cpu>().send("cpu-batch".to_owned()).unwrap();
        engine.ctx::<Gpu>().send(42).unwrap();

        assert_eq!(engine.sent::<Cpu>().unwrap(), ["cpu-batch".to_owned()]);
        assert_eq!(engine.sent::<Gpu>().unwrap(), [42]);
        assert_eq!(engine.sent_count(), 2);
        assert_eq!(engine.sent_raw()[0].0.name, "cpu");
    }

    #[test]
    fn a_stopped_engine_rejects_sends() {
        let engine = FakeEngine::new("node-1");
        let ctx = engine.ctx::<Cpu>();
        engine.stop_accepting();

        let err = ctx.send("late".to_owned()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Shutdown);
        // The context adds its own frame naming the service.
        assert!(format!("{err:?}").contains("cpu"));
        assert_eq!(engine.sent_count(), 0);
    }

    #[test]
    fn dispatched_commands_are_recorded_and_resolved() {
        let engine = FakeEngine::new("server");
        let ctx = engine.ctx::<Cpu>();

        let outcome = block_on(ctx.command::<Cpu>(
            "node-7",
            Command::Custom("interval=1s".to_owned()),
            CommandOpts::default(),
        ))
        .unwrap();
        assert_eq!(outcome, CommandOutcome::Ok);

        engine.set_command_outcome(CommandOutcome::Rejected("busy".into()));
        let outcome = block_on(ctx.agent_command(
            "node-7",
            Command::Restart,
            CommandOpts::default().forced(),
        ))
        .unwrap();
        assert_eq!(outcome, CommandOutcome::Rejected("busy".into()));

        let sent = engine.commands();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].node, "node-7");
        assert_eq!(sent[0].service, ServiceId::of::<Cpu>());
        assert!(matches!(sent[0].kind, SentCommandKind::Custom(_)));
        assert_eq!(sent[1].service, ServiceId::AGENT);
        assert_eq!(sent[1].kind, SentCommandKind::Restart);
        assert!(sent[1].opts.force);
    }

    #[test]
    fn custom_command_payloads_are_recorded_encoded() {
        let engine = FakeEngine::new("server");
        let ctx = engine.ctx::<Cpu>();
        drop(ctx.command::<Cpu>(
            "node-7",
            Command::Custom("hello".to_owned()),
            CommandOpts::default(),
        ));

        let SentCommandKind::Custom(bytes) = engine.commands()[0].kind.clone() else {
            panic!("expected a custom command");
        };
        assert_eq!(
            <String as Wire>::decode(bytes.into()).unwrap(),
            "hello".to_owned()
        );
    }

    #[test]
    fn workers_get_real_threads_with_budgeted_names() {
        let engine = FakeEngine::new("node-1");
        let ctx = engine.ctx::<Cpu>();
        let shutdown = ctx.shutdown_signal().clone();
        let outbox = ctx.outbox().clone();

        ctx.spawn_blocking_worker("reconciliation", move || {
            block_on(shutdown.wait());
            outbox.send("flushed".to_owned()).expect("send");
        })
        .expect("spawn");

        assert_eq!(engine.worker_names(), ["cpu/reconciliat"]);
        engine.shutdown_signal().set();
        engine.join_workers();
        assert_eq!(engine.sent::<Cpu>().unwrap(), ["flushed".to_owned()]);
    }

    #[test]
    fn engine_stats_are_whatever_the_test_sets() {
        let engine = FakeEngine::new("node-1");
        let ctx = engine.ctx::<Cpu>();
        assert_eq!(ctx.engine_stats(), EngineStats::default());

        engine.set_engine_stats(EngineStats {
            reconnects: 3,
            ..EngineStats::default()
        });
        assert_eq!(ctx.engine_stats().reconnects, 3);
    }

    #[test]
    fn the_context_exposes_node_clock_and_shutdown() {
        let engine = FakeEngine::new("node-42");
        let ctx = engine.ctx::<Cpu>();
        assert_eq!(ctx.node(), "node-42");
        assert_eq!(ctx.service(), ServiceId::of::<Cpu>());
        assert!(!ctx.is_shutting_down());
        let before = ctx.clock().now();
        engine.shutdown_signal().set();
        assert!(ctx.is_shutting_down());
        assert_eq!(ctx.clock().now(), before, "the frozen clock does not move");
    }

    #[test]
    fn contexts_and_outboxes_clone_to_the_same_engine() {
        let engine = FakeEngine::new("node-1");
        let ctx = engine.ctx::<Cpu>();
        let clone = ctx.clone();
        ctx.send("a".to_owned()).unwrap();
        clone.send("b".to_owned()).unwrap();
        clone.outbox().clone().send("c".to_owned()).unwrap();
        assert_eq!(engine.sent::<Cpu>().unwrap().len(), 3);
    }
}
