//! Folding batches into snapshots, and deciding when one is finished.
//!
//! A head keeps one accumulator per `(node, job, step)` and closes it when the node
//! says the step ended, when nothing has been heard about it for long enough, or
//! periodically so a long job's work is not lost if the head restarts.
//!
//! **Per node, deliberately.** A multi-node job produces one snapshot per node and the
//! aggregator merges them. A head knows exactly when its own node's cgroup went away
//! and cannot know whether the job is still running elsewhere, so deciding "the job is
//! over" here would mean inferring it from silence across machines — which is the same
//! mistake as inferring it from silence on one.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use cs_api::{MetricKind, Metrics, Series};

use crate::hist::bounds;
use crate::proto::{CloseReason, JobSnapshot, Kind, MetricSummary};
use crate::stats::Stats;

/// Which step on which node a snapshot describes.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct StepKey {
    /// The node that measured it.
    pub node: String,
    /// Slurm job id.
    pub job_id: u32,
    /// The step, without the `step_` prefix.
    pub step: String,
}

impl StepKey {
    /// A key for one step on one node.
    #[must_use]
    pub fn new(node: impl Into<String>, job_id: u32, step: impl Into<String>) -> Self {
        Self {
            node: node.into(),
            job_id,
            step: step.into(),
        }
    }
}

/// What one step on one node has done so far.
#[derive(Debug)]
struct Open {
    uid: u32,
    /// `None` until the first batch arrives. **Not a zero sentinel**: zero is a
    /// perfectly good unix millisecond, and treating it as "unset" meant the second
    /// batch of a job that started at the epoch overwrote the first one's start.
    first_unix_ms: Option<u64>,
    last_unix_ms: Option<u64>,
    samples: u64,
    /// Per metric, in the order the plugin declared them — so a snapshot's metrics
    /// come out in a stable order rather than a hash order.
    metrics: Vec<(&'static str, MetricKind, Stats)>,
    /// When we last heard anything about this step, on the caller's clock.
    heard: Instant,
    /// When this snapshot started accumulating, for the periodic close.
    opened: Instant,
}

impl Open {
    fn stats_for(&mut self, name: &'static str, kind: MetricKind) -> &mut Stats {
        if let Some(index) = self.metrics.iter().position(|(have, _, _)| *have == name) {
            return &mut self.metrics[index].2;
        }
        self.metrics.push((name, kind, Stats::default()));
        // Just pushed.
        &mut self
            .metrics
            .last_mut()
            .unwrap_or_else(|| unreachable!("just pushed"))
            .2
    }
}

/// How long to wait before giving up on a step nobody has mentioned.
///
/// A batch window is 30–60s, so this has to be comfortably more than two of them or an
/// ordinary gap would close a running job's snapshot. The consequence of it being too
/// long is only that a dead node's snapshot arrives late; too short and a live job is
/// cut in two, which a reader cannot tell from two separate jobs.
pub const DEFAULT_SILENCE: Duration = Duration::from_secs(300);

/// How long a single snapshot may accumulate before a partial one is emitted.
///
/// The insurance against losing a week-long job's history to a head restart. Ten
/// minutes of a long job is a tolerable loss; ten days is not. Set it to zero to send
/// only at the end of a step.
pub const DEFAULT_PERIOD: Duration = Duration::from_secs(600);

/// How a snapshot accumulator is configured.
#[derive(Clone, Copy, Debug)]
pub struct AccumulatorConfig {
    /// See [`DEFAULT_SILENCE`].
    pub silence: Duration,
    /// See [`DEFAULT_PERIOD`]. Zero disables periodic snapshots.
    pub period: Duration,
    /// How many steps may be open at once.
    ///
    /// A bound on memory that a peer cannot talk its way past: without it, a node
    /// inventing job ids would grow this table until the head died. Reaching it drops
    /// the *newest* step rather than evicting an old one, because evicting would throw
    /// away work already done.
    pub max_open: usize,
}

impl Default for AccumulatorConfig {
    fn default() -> Self {
        Self {
            silence: DEFAULT_SILENCE,
            period: DEFAULT_PERIOD,
            max_open: 10_000,
        }
    }
}

/// Folds batches into per-step snapshots.
///
/// Not thread-safe by itself and deliberately not async: it is pure folding, so a
/// handler wraps it in whatever lock it likes and the arithmetic stays testable with no
/// engine, no runtime and no clock but the one passed in.
#[derive(Debug)]
pub struct Accumulator {
    config: AccumulatorConfig,
    open: HashMap<StepKey, Open>,
    /// Snapshots that are finished and not yet taken.
    ready: Vec<JobSnapshot>,
    /// Steps dropped because [`AccumulatorConfig::max_open`] was reached.
    refused: u64,
}

impl Accumulator {
    /// An accumulator with the default timings.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(AccumulatorConfig::default())
    }

    /// An accumulator with explicit timings.
    #[must_use]
    pub fn with_config(config: AccumulatorConfig) -> Self {
        Self {
            config,
            open: HashMap::new(),
            ready: Vec::new(),
            refused: 0,
        }
    }

    /// Fold one batch in.
    ///
    /// `now` is monotonic — the engine's `Clock` — and used only for the silence and
    /// period rules. The timestamps *inside* the batch are wall clock, because they are
    /// data that another machine will read. The two never meet.
    ///
    /// `final_batch` is the node saying the step's cgroup has gone, which closes the
    /// snapshot at once instead of waiting to notice silence.
    pub fn fold<M: Metrics>(
        &mut self,
        key: &StepKey,
        uid: u32,
        batch: &M,
        final_batch: bool,
        now: Instant,
    ) {
        let times = batch.timestamps();
        let series = batch.series();
        // A batch that crossed the network is not to be trusted about its own shape.
        // Its timestamps are still worth having, so the ragged series are dropped
        // rather than the batch.
        let lines_up = batch.columns_line_up();

        if !self.open.contains_key(key) && self.open.len() >= self.config.max_open {
            self.refused += 1;
            return;
        }

        let open = self.open.entry(key.clone()).or_insert_with(|| Open {
            uid,
            first_unix_ms: None,
            last_unix_ms: None,
            samples: 0,
            metrics: Vec::new(),
            heard: now,
            opened: now,
        });
        open.heard = now;
        open.uid = uid;

        if let Some(&first) = times.first() {
            open.first_unix_ms = Some(open.first_unix_ms.map_or(first, |had| had.min(first)));
        }
        if let Some(&last) = times.last() {
            open.last_unix_ms = Some(open.last_unix_ms.map_or(last, |had| had.max(last)));
        }
        open.samples += times.len() as u64;

        for Series { name, kind, values } in series {
            // Absent is not zero: a metric this kernel does not expose is skipped, so
            // its summary keeps `samples: 0` and says "unavailable" rather than
            // averaging in a value nobody measured.
            if values.is_empty() {
                open.stats_for(name, kind);
                continue;
            }
            if !lines_up || values.len() != times.len() {
                let stats = open.stats_for(name, kind);
                stats.discarded = stats.discarded.saturating_add(1);
                continue;
            }
            let stats = open.stats_for(name, kind);
            for (&at, &value) in times.iter().zip(values) {
                stats.push(kind, at, value);
            }
        }

        if final_batch {
            self.close(key, CloseReason::StepEnded);
        }
    }

    /// Close every step that has gone quiet, and emit a partial for any that has been
    /// open too long.
    ///
    /// Called on a timer by whatever owns the accumulator. Returns how many it closed.
    pub fn tick(&mut self, now: Instant) -> usize {
        let silent: Vec<StepKey> = if self.config.silence.is_zero() {
            Vec::new()
        } else {
            self.open
                .iter()
                .filter(|(_, open)| {
                    now.saturating_duration_since(open.heard) >= self.config.silence
                })
                .map(|(key, _)| key.clone())
                .collect()
        };
        for key in &silent {
            self.close(key, CloseReason::Silent);
        }

        let stale: Vec<StepKey> = if self.config.period.is_zero() {
            Vec::new()
        } else {
            self.open
                .iter()
                .filter(|(_, open)| {
                    now.saturating_duration_since(open.opened) >= self.config.period
                })
                .map(|(key, _)| key.clone())
                .collect()
        };
        for key in &stale {
            // Periodic: the step is still running, so this is a piece of it and the
            // aggregator will merge what follows.
            self.close_with(key, CloseReason::Periodic, now);
        }

        silent.len() + stale.len()
    }

    /// Close everything, for a head that is shutting down.
    pub fn flush(&mut self) {
        let keys: Vec<StepKey> = self.open.keys().cloned().collect();
        for key in &keys {
            self.close(key, CloseReason::Shutdown);
        }
    }

    /// Take the finished snapshots, leaving the accumulator empty of them.
    #[must_use]
    pub fn take(&mut self) -> Vec<JobSnapshot> {
        std::mem::take(&mut self.ready)
    }

    /// How many steps are still accumulating.
    #[must_use]
    pub fn open_steps(&self) -> usize {
        self.open.len()
    }

    /// How many steps were refused because [`AccumulatorConfig::max_open`] was reached.
    #[must_use]
    pub fn refused(&self) -> u64 {
        self.refused
    }

    /// Finish a step and queue its snapshot.
    fn close(&mut self, key: &StepKey, why: CloseReason) {
        let Some(open) = self.open.remove(key) else {
            return;
        };
        self.ready.push(snapshot(key, &open, why));
    }

    /// Emit a partial snapshot and keep accumulating from here.
    ///
    /// The statistics start again rather than carrying over, because they are designed
    /// to merge: sending the same interval twice would double-count it, and the
    /// aggregator adding two disjoint pieces gets the same answer as one whole.
    fn close_with(&mut self, key: &StepKey, why: CloseReason, now: Instant) {
        let Some(open) = self.open.get_mut(key) else {
            return;
        };
        let partial = snapshot(key, open, why);
        self.ready.push(partial);

        // Keep the identity and the clock, drop the statistics.
        open.opened = now;
        open.samples = 0;
        open.first_unix_ms = None;
        for (_, _, stats) in &mut open.metrics {
            *stats = Stats::default();
        }
    }
}

impl Default for Accumulator {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the wire snapshot from an accumulator's state.
fn snapshot(key: &StepKey, open: &Open, why: CloseReason) -> JobSnapshot {
    JobSnapshot {
        job_id: key.job_id,
        step: key.step.clone(),
        uid: open.uid,
        node: key.node.clone(),
        first_unix_ms: open.first_unix_ms.unwrap_or_default(),
        last_unix_ms: open.last_unix_ms.unwrap_or_default(),
        samples: open.samples,
        // Only the node saying so makes a snapshot whole. Silence, a periodic piece
        // and a shutdown flush all leave something unsaid.
        complete: why == CloseReason::StepEnded,
        closed_because: why as i32,
        metrics: open
            .metrics
            .iter()
            .map(|(name, kind, stats)| summary(name, *kind, stats))
            .collect(),
        bucket_bounds: bounds(),
    }
}

/// Build one metric's wire summary.
fn summary(name: &str, kind: MetricKind, stats: &Stats) -> MetricSummary {
    let (bucket, bucket_ms) = stats.hist.columns();
    MetricSummary {
        metric: name.to_owned(),
        kind: match kind {
            MetricKind::Counter => Kind::Counter,
            MetricKind::Gauge => Kind::Gauge,
        } as i32,
        samples: stats.samples,
        first: stats.first,
        last: stats.last,
        total: stats.total,
        min: stats.min,
        max: stats.max,
        sum: stats.sum,
        weight: stats.weight,
        sum_squares: stats.sum_squares,
        bucket,
        bucket_ms,
        resets: stats.resets,
        discarded: stats.discarded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cs_plugin_cgroup::CgroupBatch;

    /// A test's clock: one origin, and offsets from it. `Instant::now()` twice would
    /// give two origins microseconds apart, which is the sort of thing that makes a
    /// timing test fail once a month.
    struct Clock(Instant);

    impl Clock {
        fn new() -> Self {
            Self(Instant::now())
        }

        fn at(&self, secs: u64) -> Instant {
            self.0 + Duration::from_secs(secs)
        }
    }

    fn key() -> StepKey {
        StepKey::new("node-1", 1234, "0")
    }

    /// A batch shaped as a real one: samples three seconds apart.
    fn batch(cpu: &[u64], memory: &[u64], start_ms: u64) -> CgroupBatch {
        let rows = cpu.len().max(memory.len()) as u64;
        CgroupBatch {
            sampled_unix_ms: (0..rows).map(|i| start_ms + i * 3000).collect(),
            cpu_usage_usec: cpu.to_vec(),
            memory_current: memory.to_vec(),
            ..CgroupBatch::default()
        }
    }

    fn metric<'a>(snapshot: &'a JobSnapshot, name: &str) -> &'a MetricSummary {
        snapshot
            .metrics
            .iter()
            .find(|m| m.metric == name)
            .unwrap_or_else(|| panic!("{name} is missing from the snapshot"))
    }

    #[test]
    fn a_final_batch_closes_the_snapshot_at_once() {
        let clock = Clock::new();
        let mut acc = Accumulator::new();
        acc.fold(
            &key(),
            1000,
            &batch(&[0, 3_000_000], &[100, 200], 1_000),
            true,
            clock.at(0),
        );

        let snapshots = acc.take();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(acc.open_steps(), 0, "nothing left open");

        let snap = &snapshots[0];
        assert_eq!(snap.job_id, 1234);
        assert_eq!(snap.step, "0");
        assert_eq!(snap.node, "node-1");
        assert_eq!(snap.uid, 1000);
        assert!(snap.complete, "the node said the step ended");
        assert_eq!(snap.closed_because, CloseReason::StepEnded as i32);
        assert_eq!(snap.samples, 2);
        assert_eq!(snap.first_unix_ms, 1_000);
        assert_eq!(snap.last_unix_ms, 4_000);
        assert!(!snap.bucket_bounds.is_empty(), "a reader must not guess");
    }

    /// The one that makes the whole thing worth having: a counter summarised as a rate.
    #[test]
    fn a_counter_is_reported_as_a_rate_and_a_total() {
        let clock = Clock::new();
        let mut acc = Accumulator::new();
        // Three seconds apart, advancing 3s of CPU each time: one core, steadily.
        acc.fold(
            &key(),
            0,
            &batch(&[0, 3_000_000, 6_000_000], &[], 0),
            true,
            clock.at(0),
        );
        let snap = acc.take().remove(0);
        let cpu = metric(&snap, "cpu.usage_usec");

        assert_eq!(cpu.kind, Kind::Counter as i32);
        assert_eq!(cpu.samples, 3);
        assert_eq!(cpu.total, 6_000_000, "the amount consumed");
        assert!((cpu.max - 1_000_000.0).abs() < 1.0, "one core: {}", cpu.max);
        assert!(
            (cpu.weight - 6.0).abs() < 1e-9,
            "two three-second intervals"
        );
        assert_eq!(cpu.resets, 0);
    }

    #[test]
    fn a_gauge_is_reported_as_a_weighted_level() {
        let clock = Clock::new();
        let mut acc = Accumulator::new();
        acc.fold(
            &key(),
            0,
            &batch(&[], &[100, 400, 400], 0),
            true,
            clock.at(0),
        );
        let snap = acc.take().remove(0);
        let memory = metric(&snap, "memory.current");

        assert_eq!(memory.kind, Kind::Gauge as i32);
        assert_eq!(memory.total, 0, "a gauge has no total");
        assert!((memory.min - 100.0).abs() < f64::EPSILON);
        assert!((memory.max - 400.0).abs() < f64::EPSILON);
        assert_eq!(memory.last, 400);
    }

    /// The rule that has to survive all the way into storage.
    #[test]
    fn a_metric_this_kernel_cannot_read_is_reported_as_unavailable_not_zero() {
        let clock = Clock::new();
        let mut acc = Accumulator::new();
        acc.fold(
            &key(),
            0,
            &batch(&[0, 1_000_000], &[], 0),
            true,
            clock.at(0),
        );
        let snap = acc.take().remove(0);

        let psi = metric(&snap, "cpu.pressure_some_usec");
        assert_eq!(psi.samples, 0, "absent, and every other field meaningless");
        assert!((psi.max - 0.0).abs() < f64::EPSILON);
        assert_eq!(psi.total, 0);

        let cpu = metric(&snap, "cpu.usage_usec");
        assert_eq!(cpu.samples, 2, "while the one that was there is full");
    }

    #[test]
    fn several_batches_accumulate_into_one_snapshot() {
        let clock = Clock::new();
        let mut acc = Accumulator::new();
        acc.fold(
            &key(),
            0,
            &batch(&[0, 3_000_000], &[], 0),
            false,
            clock.at(0),
        );
        assert!(acc.take().is_empty(), "not finished yet");
        assert_eq!(acc.open_steps(), 1);

        acc.fold(
            &key(),
            0,
            &batch(&[6_000_000, 9_000_000], &[], 6_000),
            true,
            clock.at(10),
        );
        let snap = acc.take().remove(0);
        assert_eq!(snap.samples, 4);
        assert_eq!(metric(&snap, "cpu.usage_usec").total, 9_000_000);
        assert_eq!(snap.first_unix_ms, 0);
        assert_eq!(snap.last_unix_ms, 9_000);
    }

    /// A node that loses power sends no final batch. The snapshot still has to arrive,
    /// and has to say that its tail is missing.
    #[test]
    fn a_step_nobody_mentions_is_closed_as_incomplete() {
        let clock = Clock::new();
        let mut acc = Accumulator::with_config(AccumulatorConfig {
            silence: Duration::from_secs(100),
            period: Duration::ZERO,
            ..AccumulatorConfig::default()
        });
        acc.fold(
            &key(),
            0,
            &batch(&[0, 1_000_000], &[], 0),
            false,
            clock.at(0),
        );

        assert_eq!(acc.tick(clock.at(50)), 0, "not silent yet");
        assert_eq!(acc.tick(clock.at(100)), 1);

        let snap = acc.take().remove(0);
        assert!(
            !snap.complete,
            "a truncated job must not look like a short one"
        );
        assert_eq!(snap.closed_because, CloseReason::Silent as i32);
        assert_eq!(
            metric(&snap, "cpu.usage_usec").total,
            1_000_000,
            "what was measured is still real"
        );
    }

    /// Insurance against losing a week-long job to a head restart.
    #[test]
    fn a_long_running_step_emits_periodic_pieces_that_merge() {
        let clock = Clock::new();
        let mut acc = Accumulator::with_config(AccumulatorConfig {
            silence: Duration::ZERO,
            period: Duration::from_secs(60),
            ..AccumulatorConfig::default()
        });

        acc.fold(
            &key(),
            0,
            &batch(&[0, 3_000_000], &[], 0),
            false,
            clock.at(0),
        );
        assert_eq!(acc.tick(clock.at(60)), 1, "the period elapsed");

        let first = acc.take().remove(0);
        assert!(!first.complete, "more is coming");
        assert_eq!(first.closed_because, CloseReason::Periodic as i32);
        assert_eq!(metric(&first, "cpu.usage_usec").total, 3_000_000);
        assert_eq!(acc.open_steps(), 1, "and the step keeps going");

        // The next piece counts only what happened after it, so the two add up rather
        // than overlapping.
        acc.fold(
            &key(),
            0,
            &batch(&[6_000_000, 9_000_000], &[], 6_000),
            true,
            clock.at(70),
        );
        let second = acc.take().remove(0);
        assert!(second.complete);
        assert_eq!(
            metric(&second, "cpu.usage_usec").total,
            3_000_000,
            "only this piece's advance, so a reader can add them"
        );
    }

    #[test]
    fn a_shutdown_flushes_what_is_held_and_marks_it_unfinished() {
        let clock = Clock::new();
        let mut acc = Accumulator::new();
        acc.fold(
            &key(),
            0,
            &batch(&[0, 1_000_000], &[], 0),
            false,
            clock.at(0),
        );
        acc.flush();

        let snap = acc.take().remove(0);
        assert!(!snap.complete);
        assert_eq!(snap.closed_because, CloseReason::Shutdown as i32);
        assert_eq!(acc.open_steps(), 0);
    }

    #[test]
    fn steps_are_kept_apart_by_node_job_and_step() {
        let clock = Clock::new();
        let mut acc = Accumulator::new();
        let batch = batch(&[0, 1_000_000], &[], 0);
        let at = clock.at(0);
        acc.fold(&StepKey::new("node-1", 1, "0"), 0, &batch, true, at);
        acc.fold(&StepKey::new("node-2", 1, "0"), 0, &batch, true, at);
        acc.fold(&StepKey::new("node-1", 1, "batch"), 0, &batch, true, at);
        acc.fold(&StepKey::new("node-1", 2, "0"), 0, &batch, true, at);

        assert_eq!(acc.take().len(), 4, "four different things");
    }

    /// A peer inventing job ids must not be able to grow the head until it dies.
    #[test]
    fn the_open_table_is_bounded_and_says_when_it_refuses() {
        let clock = Clock::new();
        let mut acc = Accumulator::with_config(AccumulatorConfig {
            max_open: 2,
            ..AccumulatorConfig::default()
        });
        let batch = batch(&[0, 1_000_000], &[], 0);
        for job_id in 0..5 {
            acc.fold(
                &StepKey::new("node-1", job_id, "0"),
                0,
                &batch,
                false,
                clock.at(0),
            );
        }
        assert_eq!(acc.open_steps(), 2);
        assert_eq!(acc.refused(), 3, "and it is countable, not silent");
    }

    /// A malformed batch loses its ragged series, not the whole snapshot.
    #[test]
    fn a_batch_whose_columns_do_not_line_up_is_partly_salvaged() {
        let clock = Clock::new();
        let mut acc = Accumulator::new();
        let ragged = CgroupBatch {
            sampled_unix_ms: vec![0, 3000, 6000],
            cpu_usage_usec: vec![0, 1_000_000],
            ..CgroupBatch::default()
        };
        acc.fold(&key(), 0, &ragged, true, clock.at(0));

        let snap = acc.take().remove(0);
        assert_eq!(snap.samples, 3, "the timestamps were still real");
        let cpu = metric(&snap, "cpu.usage_usec");
        assert_eq!(cpu.samples, 0, "but its values were not trusted");
        assert_eq!(cpu.discarded, 1, "and somebody can see that happened");
    }

    #[test]
    fn a_snapshot_lists_every_metric_the_plugin_declares() {
        let clock = Clock::new();
        let mut acc = Accumulator::new();
        acc.fold(&key(), 0, &batch(&[0, 1], &[], 0), true, clock.at(0));
        let snap = acc.take().remove(0);
        assert_eq!(
            snap.metrics.len(),
            CgroupBatch::default().series().len(),
            "including the unavailable ones, or an upgrade looks like a gap"
        );
    }
}
