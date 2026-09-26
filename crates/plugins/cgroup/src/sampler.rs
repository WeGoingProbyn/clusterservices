//! The sampler: read every job each tick, batch, and flush.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use cs_api::{
    Command, CommandReceiver, JobInfo, Reply, Sampler, ServiceBound, ServiceCtx, ServiceDef, StepId,
};
use cs_api::{Error, ErrorKind, Result};

use crate::proto::{CgroupBatch, JobRef};
use crate::sample::CgroupSample;

/// Per-job cgroup v2 usage.
pub struct CgroupService;

impl ServiceDef for CgroupService {
    const NAME: &'static str = "cgroup";
    type Data = CgroupBatch;
    /// The sample interval, in milliseconds — idempotent, as a custom command
    /// should be. Zero restores the configured default.
    type Command = u32;
}

/// How often to read, and how long to batch for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CgroupConfig {
    /// How often each job's cgroup is read.
    ///
    /// Three seconds by default: fine enough to see a job's phases, coarse enough
    /// that a thousand-job node is not spending its time in `readdir`.
    pub interval: Duration,

    /// How long samples accumulate before a batch is sent.
    ///
    /// Forty-five seconds by default, so one message carries fifteen samples. The
    /// cost of the window is that the last partial batch of a job is only worth
    /// anything if it is flushed when the job ends — which is why this sampler
    /// watches for jobs disappearing.
    pub batch_window: Duration,

    /// A hard ceiling on samples held per job, whatever the window says.
    ///
    /// Guards the one case the window does not: a node where the clock jumps, or a
    /// sampler whose interval was commanded down to something tiny.
    pub max_samples_per_batch: usize,

    /// How many jobs may be accumulating at once.
    ///
    /// A node churning through short jobs would otherwise grow this map for as long
    /// as the agent runs.
    pub max_jobs: usize,
}

impl CgroupConfig {
    /// The defaults described on each field.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            interval: Duration::from_secs(3),
            batch_window: Duration::from_secs(45),
            max_samples_per_batch: 240,
            max_jobs: 4096,
        }
    }

    /// Read this often.
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
}

impl Default for CgroupConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// What has been read for one job and not yet sent.
#[derive(Debug)]
struct Accumulator {
    job: JobInfo,
    /// Wall-clock sample times, which is what a server needs to line these up
    /// against everything else on the node.
    times: Vec<u64>,
    samples: Vec<CgroupSample>,
    /// When this batch started, on the monotonic clock — because the window must
    /// not be affected by the wall clock being corrected.
    opened: Duration,
}

/// Reads every job's cgroup on a timer and sends columnar batches.
///
/// ```no_run
/// use cs_engine::NodeEngine;
/// use cs_plugin_cgroup::{CgroupJobs, CgroupSampler};
///
/// # fn example<T: cs_transport::Transport>(transport: T) -> cs_util::Result<()> {
/// let engine = NodeEngine::builder(transport)
///     .node("node-0042")
///     .jobs(CgroupJobs::new())
///     .sampler(CgroupSampler::factory())
///     .dial("tcp://head01:7777".parse()?)
///     .build()?;
/// # let _ = engine;
/// # Ok(())
/// # }
/// ```
pub struct CgroupSampler {
    config: CgroupConfig,
    /// The interval actually in use, which a command can change.
    interval: Duration,
    /// One per job step being accumulated, keyed by (job, step).
    open: HashMap<(u32, StepId), Accumulator>,
    /// Monotonic time since the sampler started, advanced by the clock the engine
    /// gives it — never by reading one itself.
    elapsed: Duration,
    /// Samples dropped because [`CgroupConfig::max_jobs`] was reached.
    dropped_samples: u64,
}

impl CgroupSampler {
    /// A sampler with the default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(CgroupConfig::new())
    }

    /// A sampler with `config`.
    #[must_use]
    pub fn with_config(config: CgroupConfig) -> Self {
        Self {
            interval: config.interval,
            config,
            open: HashMap::new(),
            elapsed: Duration::ZERO,
            dropped_samples: 0,
        }
    }

    /// A factory, as [`sampler`](cs_engine::EngineBuilder::sampler) wants it.
    pub fn factory() -> impl FnMut() -> Self + Send + 'static {
        Self::new
    }

    /// A factory using `config`.
    pub fn factory_with(config: CgroupConfig) -> impl FnMut() -> Self + Send + 'static {
        move || Self::with_config(config)
    }

    /// How many samples have been dropped for want of room.
    ///
    /// Counted per tick, not per job: a node stuck over
    /// [`CgroupConfig::max_jobs`] keeps dropping every tick, and that ongoing
    /// pressure is what an operator needs to see — a count of distinct jobs would
    /// stop climbing while the problem continued.
    #[must_use]
    pub fn dropped_samples(&self) -> u64 {
        self.dropped_samples
    }

    /// Read every job, accumulate, and return whatever is ready to send.
    ///
    /// `now` and `wall` are passed in rather than read here so the whole batching
    /// policy is testable without waiting: the engine supplies them from its
    /// [`Clock`](cs_api::Clock).
    fn tick(&mut self, jobs: &[JobInfo], now: Duration, wall: u64) -> Result<Vec<CgroupBatch>> {
        self.elapsed = now;
        let mut ready = Vec::new();
        let mut failure = None;

        // A job that has gone is the important case: whatever was accumulated for
        // it is all anyone will ever get, and for a short job that is everything.
        // So flush the ones that have disappeared *before* reading the rest.
        let present: Vec<(u32, StepId)> = jobs.iter().map(JobInfo::key).collect();
        let vanished: Vec<(u32, StepId)> = self
            .open
            .keys()
            .filter(|key| !present.contains(key))
            .copied()
            .collect();
        for key in vanished {
            if let Some(accumulator) = self.open.remove(&key) {
                ready.extend(finish(accumulator));
            }
        }

        for job in jobs {
            let sample = match CgroupSample::read(job) {
                Ok(sample) => sample,
                Err(err) => {
                    // One unreadable job must not cost the others their samples, so
                    // keep going and report the first failure at the end.
                    failure = failure.or(Some(err));
                    continue;
                }
            };
            // Everything absent means the cgroup went away between the scan and
            // now. Nothing to record, and the flush above will catch it next tick.
            if sample.is_empty() {
                continue;
            }

            let key = job.key();
            if !self.open.contains_key(&key) {
                if self.open.len() >= self.config.max_jobs {
                    self.dropped_samples += 1;
                    continue;
                }
                self.open.insert(
                    key,
                    Accumulator {
                        job: job.clone(),
                        times: Vec::new(),
                        samples: Vec::new(),
                        opened: now,
                    },
                );
            }
            // Just inserted if it was missing.
            let Some(accumulator) = self.open.get_mut(&key) else {
                continue;
            };
            accumulator.times.push(wall);
            accumulator.samples.push(sample);

            let full = accumulator.samples.len() >= self.config.max_samples_per_batch;
            let elapsed = now.saturating_sub(accumulator.opened);
            if full || elapsed >= self.config.batch_window {
                // Taken out and put back empty rather than removed: the job is
                // still running, and dropping the entry would lose the fact that we
                // are already watching it.
                let done = std::mem::replace(
                    accumulator,
                    Accumulator {
                        job: job.clone(),
                        times: Vec::new(),
                        samples: Vec::new(),
                        opened: now,
                    },
                );
                ready.extend(finish(done));
            }
        }

        match failure {
            Some(err) if ready.is_empty() => Err(err),
            // Data beats the report: hand over what was read, and the error would
            // only be worth an engine-level count that costs these samples.
            _ => Ok(ready),
        }
    }

    /// Flush everything, whatever the window says.
    fn flush_all(&mut self) -> Vec<CgroupBatch> {
        self.open
            .drain()
            .filter_map(|(_, accumulator)| finish(accumulator))
            .collect()
    }
}

impl Default for CgroupSampler {
    fn default() -> Self {
        Self::new()
    }
}

/// Turn an accumulator into a batch, or nothing if it holds no samples.
fn finish(accumulator: Accumulator) -> Option<CgroupBatch> {
    if accumulator.samples.is_empty() {
        return None;
    }
    let samples = &accumulator.samples;

    Some(CgroupBatch {
        job: Some(JobRef {
            job_id: accumulator.job.job_id,
            step: accumulator.job.step.to_string(),
            uid: accumulator.job.uid,
        }),
        sampled_unix_ms: accumulator.times,

        cpu_usage_usec: series(samples, |s| s.cpu_usage_usec),
        cpu_user_usec: series(samples, |s| s.cpu_user_usec),
        cpu_system_usec: series(samples, |s| s.cpu_system_usec),
        cpu_nr_periods: series(samples, |s| s.cpu_nr_periods),
        cpu_nr_throttled: series(samples, |s| s.cpu_nr_throttled),
        cpu_throttled_usec: series(samples, |s| s.cpu_throttled_usec),

        memory_current: series(samples, |s| s.memory_current),
        memory_peak: series(samples, |s| s.memory_peak),
        memory_anon: series(samples, |s| s.memory_anon),
        memory_file: series(samples, |s| s.memory_file),
        memory_max_events: series(samples, |s| s.memory_max_events),
        memory_oom_kill: series(samples, |s| s.memory_oom_kill),

        io_rbytes: series(samples, |s| s.io.map(|io| io.rbytes)),
        io_wbytes: series(samples, |s| s.io.map(|io| io.wbytes)),
        io_rios: series(samples, |s| s.io.map(|io| io.rios)),
        io_wios: series(samples, |s| s.io.map(|io| io.wios)),

        pids_current: series(samples, |s| s.pids_current),

        cpu_pressure_some_usec: series(samples, |s| s.cpu_pressure.map(|p| p.some_usec)),
        memory_pressure_some_usec: series(samples, |s| s.memory_pressure.map(|p| p.some_usec)),
        memory_pressure_full_usec: series(samples, |s| s.memory_pressure.map(|p| p.full_usec)),
        io_pressure_some_usec: series(samples, |s| s.io_pressure.map(|p| p.some_usec)),
        io_pressure_full_usec: series(samples, |s| s.io_pressure.map(|p| p.full_usec)),
    })
}

/// One metric's column.
///
/// Empty if no sample had the metric — which is how "this kernel does not expose
/// it" reaches the server, distinct from a column of zeroes.
///
/// If *some* samples had it, the column is filled to full length by carrying the
/// last known value forward (or the first known value backward, at the start).
/// That keeps every column the same length as the timestamps, which is what makes
/// the batch readable at all; a gap mid-window means a read failed while the job
/// still existed, which is rare and worth less than the alignment.
fn series(samples: &[CgroupSample], pick: impl Fn(&CgroupSample) -> Option<u64>) -> Vec<u64> {
    if samples.iter().all(|sample| pick(sample).is_none()) {
        return Vec::new();
    }

    let mut column = Vec::with_capacity(samples.len());
    let mut last = None;
    for sample in samples {
        let value = pick(sample).or(last).unwrap_or(0);
        last = Some(value);
        column.push(value);
    }
    // Anything before the first real reading was filled with 0; backfill it with
    // the first value we did see, so a counter does not appear to jump from zero.
    if let Some(first) = samples.iter().find_map(&pick) {
        let leading = samples
            .iter()
            .take_while(|sample| pick(sample).is_none())
            .count();
        for slot in column.iter_mut().take(leading) {
            *slot = first;
        }
    }
    column
}

impl ServiceBound for CgroupSampler {
    type Service = CgroupService;
}

impl CommandReceiver for CgroupSampler {
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

impl Sampler for CgroupSampler {
    fn interval(&self) -> Duration {
        self.interval
    }

    fn start(&mut self, _ctx: &ServiceCtx<CgroupService>) -> Result<()> {
        // Nothing to check here: an empty or absent cgroup root is a node with no
        // jobs, not a broken one, and refusing to start would take the service down
        // for the life of the agent.
        Ok(())
    }

    fn sample(&mut self, jobs: &[JobInfo]) -> Result<Vec<CgroupBatch>> {
        let now = self.elapsed + self.interval;
        self.tick(jobs, now, unix_millis()?)
    }

    fn on_shutdown(&mut self, jobs: &[JobInfo]) -> Result<Vec<CgroupBatch>> {
        // One last reading, then everything goes: a job's final seconds are the
        // ones an operator asks about.
        let now = self.elapsed + self.interval;
        let mut batches = self.tick(jobs, now, unix_millis()?).unwrap_or_default();
        batches.extend(self.flush_all());
        Ok(batches)
    }
}

/// Wall-clock milliseconds, for stamping samples.
///
/// The engine's [`Clock`](cs_api::Clock) is monotonic and deliberately has no wall
/// clock: it exists so timing can be controlled in tests, and a sample's timestamp
/// is data rather than timing. A clock set before 1970 is a misconfigured node, not
/// something to crash over.
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
