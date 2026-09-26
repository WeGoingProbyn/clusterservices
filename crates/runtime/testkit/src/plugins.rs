//! Plugins that misbehave on request.
//!
//! One configurable sampler covers every awkward case the engine has to handle —
//! panics, refusals, slow flushes, failing samples — because they differ only in
//! which knob is set, and five near-identical plugins would drift apart. Every
//! knob is off by default, so [`CounterConfig::new`] is a well-behaved sampler.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use cs_api::{
    Command, CommandOpts, CommandOutcome, CommandReceiver, Handler, JobInfo, NoCommand, Origin,
    Reply, Sampler, ServiceBound, ServiceCtx, ServiceDef,
};
use cs_util::{Error, ErrorKind, Result};

/// Sequence numbers, so a test can detect loss, duplication, and reordering.
///
/// Its custom command sets the sampling interval in milliseconds — idempotent, as
/// a custom command should be.
pub struct CounterService;

impl ServiceDef for CounterService {
    const NAME: &'static str = "counter";
    const VERSION: u32 = 2;
    type Data = u64;
    type Command = u64;
}

/// Payloads big enough to need chunking. No commands.
pub struct BulkService;

impl ServiceDef for BulkService {
    const NAME: &'static str = "bulk";
    type Data = String;
    type Command = NoCommand;
}

/// How a [`CounterSampler`] should misbehave. Everything defaults to "correctly".
#[derive(Clone, Debug)]
pub struct CounterConfig {
    interval: Duration,
    panic_on: Option<u64>,
    fail_on: Option<u64>,
    refuse_restart: bool,
    refuse_shutdown: bool,
    ignore_custom: bool,
    fail_start: bool,
    farewell: Option<u64>,
    shutdown_delay: Duration,
    builds: Arc<AtomicU32>,
    samples: Arc<AtomicU32>,
}

impl CounterConfig {
    /// A sampler that behaves, emitting one number every 5ms.
    ///
    /// Fast on purpose: a test should not wait on a plugin.
    #[must_use]
    pub fn new() -> Self {
        Self {
            interval: Duration::from_millis(5),
            panic_on: None,
            fail_on: None,
            refuse_restart: false,
            refuse_shutdown: false,
            ignore_custom: false,
            fail_start: false,
            farewell: None,
            shutdown_delay: Duration::ZERO,
            builds: Arc::new(AtomicU32::new(0)),
            samples: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Sample this often.
    #[must_use]
    pub const fn every(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Panic on the nth sample of each instance, to exercise supervision.
    #[must_use]
    pub const fn panics_on_sample(mut self, nth: u64) -> Self {
        self.panic_on = Some(nth);
        self
    }

    /// Return an error from the nth sample, which the engine should count and
    /// survive without rebuilding anything.
    #[must_use]
    pub const fn fails_on_sample(mut self, nth: u64) -> Self {
        self.fail_on = Some(nth);
        self
    }

    /// Refuse `Restart`, so the engine must leave it running.
    #[must_use]
    pub const fn refuses_restart(mut self) -> Self {
        self.refuse_restart = true;
        self
    }

    /// Refuse `Shutdown`, which must abort an agent-wide shutdown.
    #[must_use]
    pub const fn refuses_shutdown(mut self) -> Self {
        self.refuse_shutdown = true;
        self
    }

    /// Leave custom commands to the default handling, so they come back
    /// [`CommandOutcome::Unsupported`].
    #[must_use]
    pub const fn ignores_custom_commands(mut self) -> Self {
        self.ignore_custom = true;
        self
    }

    /// Fail in `start`, as a plugin does when its hardware is absent.
    #[must_use]
    pub const fn fails_to_start(mut self) -> Self {
        self.fail_start = true;
        self
    }

    /// Emit this value from `on_shutdown`, to prove the final flush arrives.
    #[must_use]
    pub const fn says_farewell(mut self, value: u64) -> Self {
        self.farewell = Some(value);
        self
    }

    /// Block this long in `on_shutdown`, to push against the shutdown deadline.
    #[must_use]
    pub const fn slow_to_shut_down(mut self, delay: Duration) -> Self {
        self.shutdown_delay = delay;
        self
    }

    /// A factory, as [`sampler`](cs_engine::EngineBuilder::sampler) wants it.
    ///
    /// Every instance it builds shares this config's counters, so a test can see
    /// across a rebuild.
    pub fn factory(&self) -> impl FnMut() -> CounterSampler + Send + 'static {
        let config = self.clone();
        move || {
            config.builds.fetch_add(1, Ordering::SeqCst);
            CounterSampler {
                next: 0,
                interval: config.interval,
                config: config.clone(),
            }
        }
    }

    /// How many times the factory has been called — one, plus one per rebuild.
    #[must_use]
    pub fn builds(&self) -> u32 {
        self.builds.load(Ordering::SeqCst)
    }

    /// How many samples every instance has taken between them.
    #[must_use]
    pub fn samples(&self) -> u32 {
        self.samples.load(Ordering::SeqCst)
    }
}

impl Default for CounterConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// A sampler that counts, and misbehaves as its [`CounterConfig`] says.
pub struct CounterSampler {
    /// The last number emitted. Restarts from 0 in a rebuilt instance, which is
    /// how a test tells a rebuild from a continuation.
    next: u64,
    interval: Duration,
    config: CounterConfig,
}

impl ServiceBound for CounterSampler {
    type Service = CounterService;
}

impl CommandReceiver for CounterSampler {
    fn on_command(&mut self, command: Command<u64>) -> Reply {
        match command {
            Command::Custom(_) if self.config.ignore_custom => Reply::Default,
            Command::Custom(millis) => {
                self.interval = Duration::from_millis(millis);
                Reply::Handled
            }
            Command::Restart if self.config.refuse_restart => Reply::rejected("mid-batch"),
            Command::Shutdown if self.config.refuse_shutdown => Reply::rejected("still flushing"),
            _ => Reply::Default,
        }
    }
}

impl Sampler for CounterSampler {
    fn interval(&self) -> Duration {
        self.interval
    }

    fn start(&mut self, _ctx: &ServiceCtx<CounterService>) -> Result<()> {
        if self.config.fail_start {
            return Err(Error::new(
                ErrorKind::Plugin,
                "this node does not have the hardware",
            ));
        }
        Ok(())
    }

    fn sample(&mut self, _jobs: &[JobInfo]) -> Result<Vec<u64>> {
        self.next += 1;
        self.config.samples.fetch_add(1, Ordering::SeqCst);

        if self.config.panic_on == Some(self.next) {
            panic!("counter sampler exploded on sample {}", self.next);
        }
        if self.config.fail_on == Some(self.next) {
            return Err(Error::new(
                ErrorKind::Io,
                format!("cannot read anything on sample {}", self.next),
            ));
        }
        Ok(vec![self.next])
    }

    fn on_shutdown(&mut self, _jobs: &[JobInfo]) -> Result<Vec<u64>> {
        if !self.config.shutdown_delay.is_zero() {
            // Deliberately blocking: this runs on the sampler's own thread, and
            // the point is to make the engine's deadline do its job.
            std::thread::sleep(self.config.shutdown_delay);
        }
        Ok(self.config.farewell.into_iter().collect())
    }
}

/// A sampler that emits one oversized message, then nothing.
pub struct BulkSampler {
    size: usize,
    sent: bool,
}

impl BulkSampler {
    /// A factory emitting one message of `size` bytes.
    pub fn factory(size: usize) -> impl FnMut() -> Self + Send + 'static {
        move || Self { size, sent: false }
    }
}

impl ServiceBound for BulkSampler {
    type Service = BulkService;
}
impl CommandReceiver for BulkSampler {}

impl Sampler for BulkSampler {
    fn interval(&self) -> Duration {
        Duration::from_millis(5)
    }

    fn sample(&mut self, _jobs: &[JobInfo]) -> Result<Vec<String>> {
        if self.sent {
            return Ok(Vec::new());
        }
        self.sent = true;
        Ok(vec!["x".repeat(self.size)])
    }
}

/// A handler that records what it is given, for any service.
///
/// Clone it before registering: the clone the test keeps sees everything the
/// registered one receives.
pub struct Collect<S: ServiceDef> {
    seen: Arc<Mutex<Vec<S::Data>>>,
    senders: Arc<Mutex<Vec<String>>>,
    shutdowns: Arc<AtomicU32>,
    fail_everything: bool,
    _service: PhantomData<fn() -> S>,
}

impl<S: ServiceDef> Collect<S> {
    /// A handler that accepts everything.
    #[must_use]
    pub fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            senders: Arc::new(Mutex::new(Vec::new())),
            shutdowns: Arc::new(AtomicU32::new(0)),
            fail_everything: false,
            _service: PhantomData,
        }
    }

    /// A handler that fails every message, so a test can check the engine logs and
    /// counts it without dropping the connection.
    #[must_use]
    pub fn failing() -> Self {
        Self {
            fail_everything: true,
            ..Self::new()
        }
    }

    /// Everything received so far, in order.
    #[must_use]
    pub fn seen(&self) -> Vec<S::Data>
    where
        S::Data: Clone,
    {
        lock(&self.seen).clone()
    }

    /// How many messages have arrived.
    #[must_use]
    pub fn count(&self) -> usize {
        lock(&self.seen).len()
    }

    /// Which node sent each message, in the same order as
    /// [`seen`](Collect::seen).
    ///
    /// The point of `Origin`: without this a server cannot attribute anything.
    #[must_use]
    pub fn senders(&self) -> Vec<String> {
        lock(&self.senders).clone()
    }

    /// How many times [`Handler::shutdown`] ran.
    #[must_use]
    pub fn shutdowns(&self) -> u32 {
        self.shutdowns.load(Ordering::SeqCst)
    }
}

impl<S: ServiceDef> Default for Collect<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: ServiceDef> Clone for Collect<S> {
    fn clone(&self) -> Self {
        Self {
            seen: Arc::clone(&self.seen),
            senders: Arc::clone(&self.senders),
            shutdowns: Arc::clone(&self.shutdowns),
            fail_everything: self.fail_everything,
            _service: PhantomData,
        }
    }
}

impl<S: ServiceDef> ServiceBound for Collect<S> {
    type Service = S;
}

impl<S: ServiceDef> Handler for Collect<S> {
    async fn handle(&self, _ctx: &ServiceCtx<S>, from: Origin<'_>, message: S::Data) -> Result<()> {
        if self.fail_everything {
            return Err(Error::new(
                ErrorKind::Plugin,
                format!("this handler always fails (message from {})", from.node),
            ));
        }
        lock(&self.senders).push(from.node.to_owned());
        lock(&self.seen).push(message);
        Ok(())
    }

    async fn shutdown(&self) {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
    }
}

/// A handler that commands the node which just sent it data.
///
/// The loop a real server closes when it decides a sampler needs retuning, and the
/// only way to exercise server-to-agent commands through the public API.
pub struct Commander {
    command: Command<u64>,
    opts: CommandOpts,
    target: Option<String>,
    sent: Arc<AtomicU32>,
    outcome: Arc<Mutex<Option<std::result::Result<CommandOutcome, String>>>>,
}

impl Commander {
    /// Send `command` to the node that sent the first message, once.
    #[must_use]
    pub fn new(command: Command<u64>) -> Self {
        Self {
            command,
            opts: CommandOpts::default(),
            target: None,
            sent: Arc::new(AtomicU32::new(0)),
            outcome: Arc::new(Mutex::new(None)),
        }
    }

    /// Send to this node rather than the sender. For "there is no such node".
    #[must_use]
    pub fn addressed_to(mut self, node: impl Into<String>) -> Self {
        self.target = Some(node.into());
        self
    }

    /// Use these delivery options.
    #[must_use]
    pub const fn with_opts(mut self, opts: CommandOpts) -> Self {
        self.opts = opts;
        self
    }

    /// What came back, once it has. `Err` holds the formatted error chain, so a
    /// test can assert on a remote trace.
    #[must_use]
    pub fn outcome(&self) -> Option<std::result::Result<CommandOutcome, String>> {
        lock(&self.outcome).clone()
    }

    /// Whether the command has been sent yet.
    #[must_use]
    pub fn has_sent(&self) -> bool {
        self.sent.load(Ordering::SeqCst) > 0
    }
}

impl Clone for Commander {
    fn clone(&self) -> Self {
        Self {
            command: self.command,
            opts: self.opts,
            target: self.target.clone(),
            sent: Arc::clone(&self.sent),
            outcome: Arc::clone(&self.outcome),
        }
    }
}

impl ServiceBound for Commander {
    type Service = CounterService;
}

impl Handler for Commander {
    async fn handle(
        &self,
        ctx: &ServiceCtx<CounterService>,
        from: Origin<'_>,
        _message: u64,
    ) -> Result<()> {
        // Once only: a handler runs per message, and a test wants one command.
        if self.sent.fetch_add(1, Ordering::SeqCst) > 0 {
            return Ok(());
        }
        // Addressed back to whoever sent this, which is what `Origin` is for.
        let node = self.target.clone().unwrap_or_else(|| from.node.to_owned());
        let answer = ctx
            .command::<CounterService>(&node, self.command, self.opts)
            .await;
        *lock(&self.outcome) = Some(answer.map_err(|err| format!("{err:?}")));
        Ok(())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
