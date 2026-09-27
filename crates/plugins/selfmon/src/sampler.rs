//! The sampler: read the engine's counters and this process, batch, and flush.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use cs_api::Result;
use cs_api::{
    Command, CommandReceiver, EngineStats, Error, ErrorKind, JobInfo, Reply, Sampler, ServiceBound,
    ServiceCtx, ServiceDef, ServiceStats,
};

use crate::proc::{ProcStatus, read_process_cpu, read_status, read_threads, service_of};
use crate::proto::{SelfmonBatch, ServiceSeries, ThreadSeries};

/// Where `/proc/self` normally is.
pub const DEFAULT_PROC_SELF: &str = "/proc/self";

/// The clock tick rate assumed when converting CPU time.
///
/// `USER_HZ`, which is 100 on every mainstream Linux build and has been for
/// decades. Reading it properly means `sysconf(_SC_CLK_TCK)` and therefore libc,
/// and a plugin depends on `cs-api` alone — so it is a constant that can be
/// overridden ([`SelfmonConfig::ticks_per_second`]) rather than a dependency.
pub const DEFAULT_USER_HZ: u64 = 100;

/// The agent's own health and cost.
pub struct SelfmonService;

impl ServiceDef for SelfmonService {
    const NAME: &'static str = "selfmon";
    type Data = SelfmonBatch;
    /// The sample interval in milliseconds. Zero restores the default.
    type Command = u32;
}

/// How often to look, and where.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SelfmonConfig {
    /// How often the engine's counters and this process are read.
    ///
    /// Fifteen seconds by default. Coarser than the cgroup sampler on purpose: this
    /// measures the agent, and an agent that watches itself closely is mostly
    /// watching itself watch itself.
    pub interval: Duration,

    /// How long samples accumulate before a batch is sent.
    pub batch_window: Duration,

    /// A ceiling on samples per batch, whatever the window says.
    pub max_samples_per_batch: usize,

    /// Where `/proc/self` is. A test points this at a fixture.
    pub proc_self: PathBuf,

    /// Clock ticks per second, for turning CPU time into microseconds. See
    /// [`DEFAULT_USER_HZ`].
    pub ticks_per_second: u64,
}

impl SelfmonConfig {
    /// The defaults described on each field.
    #[must_use]
    pub fn new() -> Self {
        Self {
            interval: Duration::from_secs(15),
            batch_window: Duration::from_secs(60),
            max_samples_per_batch: 240,
            proc_self: PathBuf::from(DEFAULT_PROC_SELF),
            ticks_per_second: DEFAULT_USER_HZ,
        }
    }

    /// Sample this often.
    #[must_use]
    pub const fn every(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Batch for this long.
    #[must_use]
    pub const fn batching_for(mut self, window: Duration) -> Self {
        self.batch_window = window;
        self
    }

    /// Read a different proc tree — a fixture, or a container's.
    #[must_use]
    pub fn reading_proc(mut self, proc_self: impl Into<PathBuf>) -> Self {
        self.proc_self = proc_self.into();
        self
    }
}

impl Default for SelfmonConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// One tick's reading of everything.
#[derive(Clone, Debug)]
struct Snapshot {
    at_unix_ms: u64,
    engine: EngineStats,
    process_cpu_usec: Option<u64>,
    status: ProcStatus,
    /// CPU per thread *name*, summed over threads sharing one.
    threads: BTreeMap<String, u64>,
}

/// Reports the engine's counters, this process's cost, and per-plugin CPU.
///
/// ```no_run
/// use cs_engine::NodeEngine;
/// use cs_plugin_selfmon::SelfmonSampler;
///
/// # fn example<T: cs_transport::Transport>(transport: T) -> cs_util::Result<()> {
/// let engine = NodeEngine::builder(transport)
///     .node("node-0042")
///     .sampler(SelfmonSampler::factory())
///     .dial("tcp://head01:7777".parse()?)
///     .build()?;
/// # let _ = engine;
/// # Ok(())
/// # }
/// ```
pub struct SelfmonSampler {
    config: SelfmonConfig,
    interval: Duration,
    /// The context, kept from `start` because this sampler's whole job is asking
    /// the engine about itself.
    ctx: Option<ServiceCtx<SelfmonService>>,
    held: Vec<Snapshot>,
    elapsed: Duration,
    opened: Duration,
}

impl SelfmonSampler {
    /// A sampler with the default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(SelfmonConfig::new())
    }

    /// A sampler with `config`.
    #[must_use]
    pub fn with_config(config: SelfmonConfig) -> Self {
        Self {
            interval: config.interval,
            config,
            ctx: None,
            held: Vec::new(),
            elapsed: Duration::ZERO,
            opened: Duration::ZERO,
        }
    }

    /// A factory, as [`sampler`](cs_engine::EngineBuilder::sampler) wants it.
    pub fn factory() -> impl FnMut() -> Self + Send + 'static {
        Self::new
    }

    /// A factory using `config`.
    pub fn factory_with(config: SelfmonConfig) -> impl FnMut() -> Self + Send + 'static {
        move || Self::with_config(config.clone())
    }

    /// Read everything once.
    fn snapshot(&self, engine: EngineStats, at_unix_ms: u64) -> Result<Snapshot> {
        let mut threads = BTreeMap::new();
        for thread in read_threads(&self.config.proc_self)? {
            // Summed by name: a tid means nothing to a server, and a name means
            // "this plugin". See the note on restarts in the proto.
            let usec = thread.ticks.to_usec(self.config.ticks_per_second);
            *threads.entry(thread.comm).or_insert(0u64) += usec;
        }

        Ok(Snapshot {
            at_unix_ms,
            engine,
            process_cpu_usec: read_process_cpu(&self.config.proc_self)?
                .map(|ticks| ticks.to_usec(self.config.ticks_per_second)),
            status: read_status(&self.config.proc_self)?,
            threads,
        })
    }

    /// Accumulate one snapshot and return a batch if the window has closed.
    fn accumulate(&mut self, snapshot: Snapshot, now: Duration) -> Option<SelfmonBatch> {
        if self.held.is_empty() {
            self.opened = now;
        }
        self.held.push(snapshot);
        self.elapsed = now;

        let full = self.held.len() >= self.config.max_samples_per_batch;
        let elapsed = now.saturating_sub(self.opened);
        if full || elapsed >= self.config.batch_window {
            self.flush()
        } else {
            None
        }
    }

    /// Turn everything held into a batch.
    fn flush(&mut self) -> Option<SelfmonBatch> {
        if self.held.is_empty() {
            return None;
        }
        let held = std::mem::take(&mut self.held);
        Some(build(&held))
    }
}

impl Default for SelfmonSampler {
    fn default() -> Self {
        Self::new()
    }
}

/// Assemble the columnar batch.
fn build(held: &[Snapshot]) -> SelfmonBatch {
    // Every service and thread seen at any point in the window: one appearing
    // halfway through — a plugin that was restarted — still gets a full-length
    // column, so the batch stays readable.
    let mut services: Vec<&str> = held
        .iter()
        .flat_map(|snapshot| snapshot.engine.services.iter().map(|s| s.id.name))
        .collect();
    services.sort_unstable();
    services.dedup();

    let mut threads: Vec<&str> = held
        .iter()
        .flat_map(|snapshot| snapshot.threads.keys().map(String::as_str))
        .collect();
    threads.sort_unstable();
    threads.dedup();

    SelfmonBatch {
        sampled_unix_ms: held.iter().map(|s| s.at_unix_ms).collect(),

        frames_sent: engine_series(held, |e| e.frames_sent),
        frames_received: engine_series(held, |e| e.frames_received),
        bytes_sent: engine_series(held, |e| e.bytes_sent),
        bytes_received: engine_series(held, |e| e.bytes_received),
        reconnects: engine_series(held, |e| e.reconnects),
        peer_timeouts: engine_series(held, |e| e.peer_timeouts),
        peers: engine_series(held, |e| u64::from(e.peers)),
        data_queue_depth: engine_series(held, |e| e.data_queue_depth),
        control_queue_depth: engine_series(held, |e| e.control_queue_depth),
        data_dropped: engine_series(held, |e| e.data_dropped),
        unroutable_frames: engine_series(held, |e| e.unroutable_frames),
        data_forwarded: engine_series(held, |e| e.data_forwarded),
        commands_completed: engine_series(held, |e| e.commands_completed),
        commands_expired: engine_series(held, |e| e.commands_expired),

        process_cpu_usec: optional_series(held, |s| s.process_cpu_usec),
        process_rss_bytes: optional_series(held, |s| s.status.rss_bytes),
        process_threads: optional_series(held, |s| s.status.threads),

        services: services
            .into_iter()
            .map(|name| ServiceSeries {
                service: name.to_owned(),
                messages_sent: service_series(held, name, |s| s.messages_sent),
                messages_received: service_series(held, name, |s| s.messages_received),
                samples: service_series(held, name, |s| s.samples),
                sample_errors: service_series(held, name, |s| s.sample_errors),
                sample_time_total_usec: service_series(held, name, |s| micros(s.sample_time_total)),
                sample_time_max_usec: service_series(held, name, |s| micros(s.sample_time_max)),
                panics: service_series(held, name, |s| s.panics),
                restarts: service_series(held, name, |s| s.restarts),
            })
            .collect(),

        threads: threads
            .into_iter()
            .map(|comm| ThreadSeries {
                thread: comm.to_owned(),
                service: service_of(comm).to_owned(),
                cpu_usec: held
                    .iter()
                    .map(|snapshot| snapshot.threads.get(comm).copied().unwrap_or(0))
                    .collect(),
            })
            .collect(),
    }
}

/// A column from the engine's counters. Always full length.
fn engine_series(held: &[Snapshot], pick: impl Fn(&EngineStats) -> u64) -> Vec<u64> {
    held.iter().map(|snapshot| pick(&snapshot.engine)).collect()
}

/// A column that may be unavailable — empty if it never was, carried forward if it
/// sometimes was. Same rule as the cgroup plugin's series.
fn optional_series(held: &[Snapshot], pick: impl Fn(&Snapshot) -> Option<u64>) -> Vec<u64> {
    if held.iter().all(|snapshot| pick(snapshot).is_none()) {
        return Vec::new();
    }
    let mut column = Vec::with_capacity(held.len());
    let mut last = 0;
    for snapshot in held {
        last = pick(snapshot).unwrap_or(last);
        column.push(last);
    }
    column
}

/// A column from one service's counters, zero where the service was not listed.
fn service_series(
    held: &[Snapshot],
    service: &str,
    pick: impl Fn(&ServiceStats) -> u64,
) -> Vec<u64> {
    held.iter()
        .map(|snapshot| snapshot.engine.service(service).map_or(0, &pick))
        .collect()
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

impl ServiceBound for SelfmonSampler {
    type Service = SelfmonService;
}

impl CommandReceiver for SelfmonSampler {
    fn on_command(&mut self, command: Command<u32>) -> Reply {
        match command {
            Command::Custom(millis) => {
                self.interval = if millis == 0 {
                    self.config.interval
                } else {
                    Duration::from_millis(u64::from(millis))
                };
                Reply::Handled
            }
            _ => Reply::Default,
        }
    }
}

impl Sampler for SelfmonSampler {
    fn interval(&self) -> Duration {
        self.interval
    }

    /// Keep the context: the engine's counters are the point of this sampler, and
    /// [`ServiceCtx::engine_stats`] is the only way to them.
    fn start(&mut self, ctx: &ServiceCtx<SelfmonService>) -> Result<()> {
        self.ctx = Some(ctx.clone());
        Ok(())
    }

    fn sample(&mut self, _jobs: &[JobInfo]) -> Result<Vec<SelfmonBatch>> {
        let Some(ctx) = &self.ctx else {
            // Unreachable through the engine, which always calls `start` first.
            return Err(Error::new(
                ErrorKind::Plugin,
                "selfmon was sampled before it was started",
            ));
        };
        let engine = ctx.engine_stats();
        let snapshot = self.snapshot(engine, unix_millis()?)?;

        let now = self.elapsed + self.interval;
        Ok(self.accumulate(snapshot, now).into_iter().collect())
    }

    fn on_shutdown(&mut self, _jobs: &[JobInfo]) -> Result<Vec<SelfmonBatch>> {
        // One last reading — the counters at the moment of shutdown are the ones
        // that say whether anything was dropped on the way out — then everything
        // held.
        if let Some(ctx) = &self.ctx {
            let engine = ctx.engine_stats();
            if let Ok(snapshot) = self.snapshot(engine, unix_millis()?) {
                self.held.push(snapshot);
            }
        }
        Ok(self.flush().into_iter().collect())
    }
}

/// Wall-clock milliseconds, for stamping samples. See the cgroup plugin: the
/// engine's clock is monotonic by design, and a timestamp is data, not timing.
fn unix_millis() -> Result<u64> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
        .map_err(|err| {
            Error::with_source(
                ErrorKind::Config,
                "this node's clock is before the unix epoch",
                err,
            )
        })
}
