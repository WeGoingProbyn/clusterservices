//! The service a head registers to send snapshots upward.
//!
//! Two halves that share one accumulator: whatever handles a plugin's batches folds
//! them in, and a [`SnapshotSampler`] carries whatever has finished to the tier above.
//!
//! A *sampler* rather than something bespoke, because that is what this framework calls
//! a thing that periodically produces data — and because it means snapshots are
//! batched, chunked, queued and dropped under pressure on exactly the same terms as
//! everything else. A privileged path for them would mean the numbers describing a
//! cluster travelled differently from the cluster's own metrics, which is the same
//! argument that keeps `selfmon` an ordinary plugin.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use cs_api::{
    Clock, CommandReceiver, JobInfo, Metrics, NoCommand, Result, Sampler, ServiceBound, ServiceCtx,
    ServiceDef,
};

use crate::accumulate::{Accumulator, AccumulatorConfig, StepKey};
use crate::proto::JobSnapshot;

/// The snapshot service: per-job summaries, flowing upward.
pub struct SnapshotService;

impl ServiceDef for SnapshotService {
    const NAME: &'static str = "snapshot";
    type Data = JobSnapshot;
    /// Nothing to tell it. What to summarise is decided by which batches arrive, and
    /// how often to close one is configuration rather than a running instruction.
    type Command = NoCommand;
}

/// A shared accumulator: fold batches in from one side, take snapshots from the other.
///
/// Cheap to clone, and the clone sees everything — a handler holds one and the sampler
/// holds another.
#[derive(Clone)]
pub struct Snapshots {
    inner: Arc<Mutex<Accumulator>>,
}

impl Snapshots {
    /// With the default timings.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(AccumulatorConfig::default())
    }

    /// With explicit timings.
    #[must_use]
    pub fn with_config(config: AccumulatorConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Accumulator::with_config(config))),
        }
    }

    /// Fold one batch in.
    ///
    /// `now` is a monotonic reading — `ctx.clock().now()` — used only for the silence
    /// and period rules. The timestamps inside the batch are wall clock and are what
    /// ends up in the snapshot; the two are never compared.
    pub fn fold<M: Metrics>(
        &self,
        step: &StepKey,
        uid: u32,
        batch: &M,
        final_batch: bool,
        now: Instant,
    ) {
        self.lock().fold(step, uid, batch, final_batch, now);
    }

    /// Close whatever has gone quiet or been open too long.
    pub fn tick(&self, now: Instant) -> usize {
        self.lock().tick(now)
    }

    /// Close everything, for a head that is stopping.
    pub fn flush(&self) {
        self.lock().flush();
    }

    /// Take the finished snapshots.
    #[must_use]
    pub fn take(&self) -> Vec<JobSnapshot> {
        self.lock().take()
    }

    /// How many steps are still accumulating.
    #[must_use]
    pub fn open_steps(&self) -> usize {
        self.lock().open_steps()
    }

    /// How many steps were refused because the open table was full.
    #[must_use]
    pub fn refused(&self) -> u64 {
        self.lock().refused()
    }

    /// The accumulator is only ever held for the length of one fold, and none of the
    /// code under this lock can panic while holding it — so a poisoned lock would mean
    /// a bug elsewhere, and carrying on with the data is better than taking the head
    /// down over it.
    fn lock(&self) -> MutexGuard<'_, Accumulator> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for Snapshots {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Snapshots {
    /// Reports the shape and not the contents: printing every open step's statistics
    /// would be megabytes on a busy head, and a lock is held to read any of it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshots")
            .field("open_steps", &self.open_steps())
            .field("refused", &self.refused())
            .finish()
    }
}

/// Sends finished snapshots to the tier above.
///
/// Registered on a head that has an upstream. Its interval is how long a finished
/// snapshot may wait, which is also how often the silence and period rules are
/// checked — a snapshot is not urgent, so this is minutes rather than seconds.
pub struct SnapshotSampler {
    snapshots: Snapshots,
    interval: Duration,
    clock: Option<Arc<dyn Clock>>,
}

impl SnapshotSampler {
    /// How often finished snapshots are swept up, by default.
    pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);

    /// A sampler over `snapshots`.
    #[must_use]
    pub fn new(snapshots: Snapshots) -> Self {
        Self {
            snapshots,
            interval: Self::DEFAULT_INTERVAL,
            clock: None,
        }
    }

    /// Sweep this often instead of every [`DEFAULT_INTERVAL`](Self::DEFAULT_INTERVAL).
    #[must_use]
    pub const fn every(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// A factory, so the engine can rebuild it after a `Restart` or a panic.
    ///
    /// The accumulator lives outside the sampler on purpose: rebuilding the sampler
    /// must not discard every running job's history, and a factory that captured the
    /// shared handle keeps it.
    pub fn factory(snapshots: Snapshots) -> impl FnMut() -> Self + Send + 'static {
        move || Self::new(snapshots.clone())
    }

    /// A factory with a different interval.
    pub fn factory_every(
        snapshots: Snapshots,
        interval: Duration,
    ) -> impl FnMut() -> Self + Send + 'static {
        move || Self::new(snapshots.clone()).every(interval)
    }
}

impl ServiceBound for SnapshotSampler {
    type Service = SnapshotService;
}

impl CommandReceiver for SnapshotSampler {}

impl Sampler for SnapshotSampler {
    fn interval(&self) -> Duration {
        self.interval
    }

    /// Keep the clock. Everything this sampler decides is a matter of elapsed time, and
    /// reading `Instant::now` directly would put it beyond a paused-time test's reach.
    fn start(&mut self, ctx: &ServiceCtx<SnapshotService>) -> Result<()> {
        self.clock = Some(Arc::clone(ctx.clock()));
        Ok(())
    }

    fn sample(&mut self, _jobs: &[JobInfo]) -> Result<Vec<JobSnapshot>> {
        if let Some(clock) = &self.clock {
            self.snapshots.tick(clock.now());
        }
        Ok(self.snapshots.take())
    }

    /// Flush on the way out, so a head being restarted does not throw away what every
    /// running job has done so far. The snapshots are marked incomplete, and the
    /// aggregator merges them with whatever the next head sends.
    fn on_shutdown(&mut self, _jobs: &[JobInfo]) -> Result<Vec<JobSnapshot>> {
        self.snapshots.flush();
        Ok(self.snapshots.take())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cs_api::test_support::FakeEngine;
    use cs_plugin_cgroup::CgroupBatch;

    fn batch() -> CgroupBatch {
        CgroupBatch {
            sampled_unix_ms: vec![1000, 4000],
            cpu_usage_usec: vec![0, 3_000_000],
            ..CgroupBatch::default()
        }
    }

    #[test]
    fn the_service_is_named_and_takes_no_commands() {
        assert_eq!(SnapshotService::NAME, "snapshot");
        // `NoCommand` is uninhabited, so there is nothing to send.
        assert_eq!(std::mem::size_of::<NoCommand>(), 0);
    }

    /// The two halves share one accumulator, which is the whole point of the handle.
    #[test]
    fn what_is_folded_in_one_clone_is_taken_from_another() {
        let folding = Snapshots::new();
        let draining = folding.clone();
        let now = Instant::now();

        folding.fold(&StepKey::new("node-1", 7, "0"), 0, &batch(), true, now);
        let taken = draining.take();

        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].job_id, 7);
        assert!(draining.take().is_empty(), "and taking is destructive");
    }

    #[test]
    fn a_sampler_sweeps_what_has_finished() {
        let snapshots = Snapshots::new();
        let engine = FakeEngine::new("head01");
        let mut sampler = SnapshotSampler::new(snapshots.clone());
        sampler
            .start(&engine.ctx::<SnapshotService>())
            .expect("start");

        assert!(sampler.sample(&[]).expect("sample").is_empty());

        snapshots.fold(
            &StepKey::new("node-1", 7, "0"),
            0,
            &batch(),
            true,
            Instant::now(),
        );
        let swept = sampler.sample(&[]).expect("sample");
        assert_eq!(swept.len(), 1);
        assert_eq!(swept[0].job_id, 7);
    }

    /// A head being restarted must not throw away every running job's history.
    #[test]
    fn shutdown_flushes_open_steps_as_incomplete() {
        let snapshots = Snapshots::new();
        let mut sampler = SnapshotSampler::new(snapshots.clone());
        snapshots.fold(
            &StepKey::new("node-1", 7, "0"),
            0,
            &batch(),
            // Not final: the job is still running.
            false,
            Instant::now(),
        );
        assert_eq!(snapshots.open_steps(), 1);

        let flushed = sampler.on_shutdown(&[]).expect("shutdown");
        assert_eq!(flushed.len(), 1);
        assert!(!flushed[0].complete, "the job outlived the head");
        assert_eq!(snapshots.open_steps(), 0);
    }

    /// Rebuilding the sampler must not lose the accumulator, or a `Restart` would
    /// silently discard every running job.
    #[test]
    fn a_rebuilt_sampler_keeps_the_history() {
        let snapshots = Snapshots::new();
        let mut factory = SnapshotSampler::factory(snapshots.clone());

        let mut first = factory();
        snapshots.fold(
            &StepKey::new("node-1", 7, "0"),
            0,
            &batch(),
            false,
            Instant::now(),
        );
        drop(first);

        let mut rebuilt = factory();
        assert_eq!(snapshots.open_steps(), 1, "survived the rebuild");
        let flushed = rebuilt.on_shutdown(&[]).expect("shutdown");
        assert_eq!(flushed.len(), 1, "and is still reportable");
        first = factory();
        assert_eq!(first.interval(), SnapshotSampler::DEFAULT_INTERVAL);
    }
}
