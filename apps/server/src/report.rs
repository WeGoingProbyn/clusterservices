//! Handlers that say out loud what arrived.
//!
//! This is what makes an agent's behaviour visible while it is being written: one
//! line per batch, naming the node it came from, what was in it, and — the part a
//! fixture cannot tell you — which metrics this kernel does not expose.
//!
//! A real head would write to storage instead. The shape would not change: same
//! trait, same `Origin`, same batch.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use cs_api::{Handler, Origin, Result, ServiceBound, ServiceCtx};
use cs_plugin_cgroup::{CgroupBatch, CgroupService};
use cs_plugin_selfmon::{SelfmonBatch, SelfmonService};
use cs_plugin_snapshot::{
    CloseReason, JobSnapshot, Kind, MetricSummary, SnapshotService, Snapshots, StepKey,
};
use tracing::{info, warn};

/// What a handler has seen, for the summary at shutdown.
#[derive(Clone, Debug, Default)]
pub struct Tally {
    batches: Arc<AtomicU64>,
    samples: Arc<AtomicU64>,
}

impl Tally {
    /// A fresh tally.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn record(&self, samples: usize) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.samples.fetch_add(samples as u64, Ordering::Relaxed);
    }

    /// Batches and samples seen so far.
    #[must_use]
    pub fn counts(&self) -> (u64, u64) {
        (
            self.batches.load(Ordering::Relaxed),
            self.samples.load(Ordering::Relaxed),
        )
    }
}

/// Reports per-job cgroup batches.
#[derive(Clone, Debug, Default)]
pub struct CgroupReport {
    tally: Tally,
    /// Where each batch is folded into its step's running summary, when this head is
    /// keeping them. `None` on a head that only logs.
    snapshots: Option<Snapshots>,
}

impl CgroupReport {
    /// A handler sharing `tally`.
    #[must_use]
    pub fn new(tally: Tally) -> Self {
        Self {
            tally,
            snapshots: None,
        }
    }

    /// Also fold every batch into a per-job snapshot.
    ///
    /// The accumulator is shared with a [`SnapshotSampler`](cs_plugin_snapshot::SnapshotSampler),
    /// which sends whatever has finished to the tier above — so this is only worth doing
    /// on a head that has one.
    #[must_use]
    pub fn summarising(mut self, snapshots: Snapshots) -> Self {
        self.snapshots = Some(snapshots);
        self
    }
}

impl ServiceBound for CgroupReport {
    type Service = CgroupService;
}

impl Handler for CgroupReport {
    async fn handle(
        &self,
        ctx: &ServiceCtx<CgroupService>,
        from: Origin<'_>,
        batch: CgroupBatch,
    ) -> Result<()> {
        let samples = batch.sampled_unix_ms.len();
        self.tally.record(samples);

        // Fold before logging, so a head that is summarising keeps the numbers even if
        // the formatting below is changed or removed. `from.node` and not `from.via`:
        // the snapshot belongs to the node that measured it, which is not the relay that
        // handed it over.
        if let (Some(snapshots), Some(job)) = (&self.snapshots, batch.job.as_ref()) {
            snapshots.fold(
                &StepKey::new(from.node, job.job_id, &job.step),
                job.uid,
                &batch,
                batch.r#final,
                ctx.clock().now(),
            );
        }

        let job = batch.job.as_ref().map_or_else(
            || "<no job>".to_owned(),
            |job| format!("{}.{} uid {}", job.job_id, job.step, job.uid),
        );

        // The CPU figure is a *delta across the batch*, computed here rather than
        // sent — which is the whole point of shipping cumulative counters: a
        // missed batch costs resolution, never correctness.
        let cpu = delta(&batch.cpu_usage_usec).map_or_else(
            || "cpu n/a".to_owned(),
            |usec| format!("cpu +{}", duration(usec)),
        );
        let memory = last(&batch.memory_current).map_or_else(
            || "mem n/a".to_owned(),
            |current| match last(&batch.memory_peak) {
                Some(peak) => format!("mem {} (peak {})", bytes(current), bytes(peak)),
                None => format!("mem {}", bytes(current)),
            },
        );
        let io = match (delta(&batch.io_rbytes), delta(&batch.io_wbytes)) {
            (Some(read), Some(written)) => {
                format!("  io +{}r +{}w", bytes(read), bytes(written))
            }
            _ => String::new(),
        };

        info!(
            "{} {job}  {samples} samples over {}  {cpu}  {memory}{io}{}",
            from.node,
            span(&batch.sampled_unix_ms),
            absent(&[
                ("memory.peak", &batch.memory_peak),
                ("io.stat", &batch.io_rbytes),
                ("cpu PSI", &batch.cpu_pressure_some_usec),
                ("memory PSI", &batch.memory_pressure_some_usec),
                ("cpu throttling", &batch.cpu_nr_periods),
            ]),
        );
        Ok(())
    }
}

/// Reports an agent's own health.
#[derive(Clone, Debug, Default)]
pub struct SelfmonReport {
    tally: Tally,
}

impl SelfmonReport {
    /// A handler sharing `tally`.
    #[must_use]
    pub fn new(tally: Tally) -> Self {
        Self { tally }
    }
}

impl ServiceBound for SelfmonReport {
    type Service = SelfmonService;
}

impl Handler for SelfmonReport {
    async fn handle(
        &self,
        _ctx: &ServiceCtx<SelfmonService>,
        from: Origin<'_>,
        batch: SelfmonBatch,
    ) -> Result<()> {
        let samples = batch.sampled_unix_ms.len();
        self.tally.record(samples);

        // Per-plugin CPU: the reason worker threads are named `<service>/<worker>`.
        let mut threads: Vec<String> = batch
            .threads
            .iter()
            .filter_map(|series| {
                let used = delta(&series.cpu_usec)?;
                Some((used, series.thread.clone()))
            })
            .filter(|(used, _)| *used > 0)
            .map(|(used, thread)| format!("{thread} +{}", duration(used)))
            .collect();
        threads.sort_unstable();

        // A relay says so, because "this agent is passing data through" is not a
        // warning but it changes how every other number should be read.
        let relaying = match delta(&batch.data_forwarded).filter(|&n| n > 0) {
            Some(forwarded) => format!("  relayed {forwarded}"),
            None => String::new(),
        };

        info!(
            "{} selfmon  {samples} samples  agent {} rss {} threads {}  queue {}d/{}c  frames {}{}{}",
            from.node,
            delta(&batch.process_cpu_usec).map_or_else(
                || "cpu n/a".to_owned(),
                |usec| format!("cpu +{}", duration(usec))
            ),
            last(&batch.process_rss_bytes).map_or_else(|| "n/a".to_owned(), bytes),
            last(&batch.process_threads).unwrap_or(0),
            last(&batch.data_queue_depth).unwrap_or(0),
            last(&batch.control_queue_depth).unwrap_or(0),
            last(&batch.frames_sent).unwrap_or(0),
            if threads.is_empty() {
                String::new()
            } else {
                format!("  [{}]", threads.join(", "))
            },
            relaying,
        );

        // The two numbers that mean something is wrong, rather than merely
        // interesting. A warning so they are not lost in the stream above.
        if let Some(dropped) = last(&batch.data_dropped).filter(|&n| n > 0) {
            warn!(
                node = from.node,
                dropped, "agent has discarded metrics it had already collected"
            );
        }
        if let Some(timeouts) = last(&batch.peer_timeouts).filter(|&n| n > 0) {
            warn!(
                node = from.node,
                timeouts, "agent has given up on a silent peer"
            );
        }
        Ok(())
    }
}

/// The growth of a cumulative counter across a batch.
fn delta(column: &[u64]) -> Option<u64> {
    let first = column.first()?;
    let last = column.last()?;
    Some(last.saturating_sub(*first))
}

/// The final reading of a gauge.
fn last(column: &[u64]) -> Option<u64> {
    column.last().copied()
}

/// How long a batch covers, from its own timestamps.
fn span(timestamps: &[u64]) -> String {
    match (timestamps.first(), timestamps.last()) {
        (Some(first), Some(last)) => duration(last.saturating_sub(*first) * 1000),
        _ => "0s".to_owned(),
    }
}

/// Which of these columns this kernel did not provide.
///
/// An empty series means unavailable, never zero — so this is the line that tells
/// an operator why a dashboard has a gap, without anyone having to go and read the
/// node's kernel config.
fn absent(columns: &[(&str, &Vec<u64>)]) -> String {
    let missing: Vec<&str> = columns
        .iter()
        .filter(|(_, column)| column.is_empty())
        .map(|(name, _)| *name)
        .collect();
    if missing.is_empty() {
        String::new()
    } else {
        format!("  (absent: {})", missing.join(", "))
    }
}

/// Microseconds as something readable.
fn duration(usec: u64) -> String {
    let secs = usec as f64 / 1_000_000.0;
    if secs >= 1.0 {
        format!("{secs:.3}s")
    } else if usec >= 1000 {
        format!("{:.1}ms", usec as f64 / 1000.0)
    } else {
        format!("{usec}us")
    }
}

/// Bytes as something readable.
fn bytes(count: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = count as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{count}B")
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

/// Reports snapshots as they arrive: what an aggregator will store, logged instead.
///
/// One line per job step, with the figures a person actually asks for — how much CPU,
/// at what rate, how much memory at its peak — and a marker when the snapshot is
/// missing its tail, because a truncated job must not read like a short one.
pub struct SnapshotReport {
    tally: Tally,
}

impl SnapshotReport {
    /// A handler sharing `tally`.
    #[must_use]
    pub fn new(tally: Tally) -> Self {
        Self { tally }
    }
}

impl ServiceBound for SnapshotReport {
    type Service = SnapshotService;
}

impl Handler for SnapshotReport {
    async fn handle(
        &self,
        _ctx: &ServiceCtx<SnapshotService>,
        from: Origin<'_>,
        snapshot: JobSnapshot,
    ) -> Result<()> {
        self.tally.record(snapshot.samples as usize);

        let cpu = metric(&snapshot, "cpu.usage_usec");
        let memory = metric(&snapshot, "memory.current");
        let peak = metric(&snapshot, "memory.peak");

        info!(
            "{}.{} on {} uid {}  {}  cpu {} (mean {}, peak {})  mem {}  {}{}",
            snapshot.job_id,
            snapshot.step,
            snapshot.node,
            snapshot.uid,
            elapsed(snapshot.last_unix_ms.saturating_sub(snapshot.first_unix_ms)),
            cpu.map_or_else(|| "n/a".to_owned(), |m| duration(m.total)),
            // A counter's statistics are rates, in units per second — microseconds of
            // CPU per second is cores, which is the number anyone means.
            cpu.and_then(cores)
                .map_or_else(|| "n/a".to_owned(), |c| format!("{c:.2}")),
            cpu.and_then(peak_cores)
                .map_or_else(|| "n/a".to_owned(), |c| format!("{c:.2}")),
            // Prefer the kernel's own peak: it catches spikes between samples, which
            // is the only reason it is carried alongside `memory.current`.
            peak.filter(|m| m.samples > 0).map_or_else(
                || memory.map_or_else(|| "n/a".to_owned(), |m| bytes(m.max.max(0.0) as u64)),
                |m| format!("{} (kernel peak)", bytes(m.last)),
            ),
            match CloseReason::try_from(snapshot.closed_because) {
                Ok(CloseReason::StepEnded) => "ended",
                Ok(CloseReason::Silent) => "went silent",
                Ok(CloseReason::Periodic) => "partial",
                Ok(CloseReason::Shutdown) => "head stopped",
                _ => "unknown",
            },
            if from.is_direct() {
                String::new()
            } else {
                format!("  via {}", from.via)
            },
        );

        if !snapshot.complete {
            // Worth its own line: a reader joining these into a job record has to know
            // that more may follow, or that a tail is missing for good.
            warn!(
                job = snapshot.job_id,
                step = %snapshot.step,
                node = %snapshot.node,
                "an incomplete snapshot: it will need merging or it lost its tail"
            );
        }
        Ok(())
    }
}

/// One metric out of a snapshot, if the node had it.
fn metric<'a>(snapshot: &'a JobSnapshot, name: &str) -> Option<&'a MetricSummary> {
    snapshot.metrics.iter().find(|m| m.metric == name)
}

/// A counter's mean rate as cores, for a metric measured in microseconds.
fn cores(metric: &MetricSummary) -> Option<f64> {
    (metric.kind == Kind::Counter as i32 && metric.weight > 0.0)
        .then(|| metric.sum / metric.weight / 1_000_000.0)
}

/// The busiest interval, as cores.
fn peak_cores(metric: &MetricSummary) -> Option<f64> {
    (metric.samples > 0).then_some(metric.max / 1_000_000.0)
}

/// A wall-clock span in milliseconds, read at a glance.
fn elapsed(millis: u64) -> String {
    let secs = millis / 1000;
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m{}s", secs / 60, secs % 60),
        _ => format!("{}h{}m", secs / 3600, (secs % 3600) / 60),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_counters_delta_is_its_growth_across_the_batch() {
        assert_eq!(delta(&[1000, 2500, 9000]), Some(8000));
        assert_eq!(delta(&[42]), Some(0), "one sample has not grown");
        assert_eq!(delta(&[]), None, "an absent metric has no delta");
        // A counter that went backwards — a restarted thread — must not wrap.
        assert_eq!(delta(&[9000, 10]), Some(0));
    }

    #[test]
    fn absent_columns_are_named_and_present_ones_are_not() {
        let full = vec![1, 2, 3];
        let empty = Vec::new();
        assert_eq!(absent(&[("a", &full)]), "");
        assert_eq!(
            absent(&[("memory.peak", &empty), ("io.stat", &empty), ("cpu", &full)]),
            "  (absent: memory.peak, io.stat)"
        );
    }

    #[test]
    fn durations_and_sizes_are_scaled_for_reading() {
        assert_eq!(duration(500), "500us");
        assert_eq!(duration(1_500), "1.5ms");
        assert_eq!(duration(1_500_000), "1.500s");
        assert_eq!(bytes(512), "512B");
        assert_eq!(bytes(8 * 1024), "8.0KiB");
        assert_eq!(bytes(7 * 1024 * 1024 * 1024), "7.0GiB");
    }

    #[test]
    fn a_span_comes_from_the_batchs_own_timestamps() {
        assert_eq!(span(&[1_000, 46_000]), "45.000s");
        assert_eq!(span(&[]), "0s");
    }

    /// A batch shaped like one a real agent sends, including a metric this
    /// "kernel" does not expose.
    fn cgroup_batch() -> CgroupBatch {
        CgroupBatch {
            job: Some(cs_plugin_cgroup::JobRef {
                job_id: 1234,
                step: "batch".to_owned(),
                uid: 1000,
            }),
            sampled_unix_ms: vec![1_000, 4_000, 7_000],
            cpu_usage_usec: vec![1_000_000, 2_500_000, 4_000_000],
            memory_current: vec![1024, 2048, 4096],
            // Absent, as it is on a pre-5.19 kernel.
            memory_peak: Vec::new(),
            ..CgroupBatch::default()
        }
    }

    fn direct(node: &str) -> Origin<'_> {
        Origin {
            node,
            via: node,
            service_version: 1,
        }
    }

    #[test]
    fn handling_a_batch_records_it_and_survives_absent_columns() {
        let tally = Tally::new();
        let handler = CgroupReport::new(tally.clone());
        let ctx = cs_api::test_support::fake_ctx::<CgroupService>();

        cs_async_util::test_util::block_on(handler.handle(
            &ctx,
            direct("node-0042"),
            cgroup_batch(),
        ))
        .expect("a batch should be handled");
        assert_eq!(tally.counts(), (1, 3));

        // The awkward one: no job, no timestamps, every column empty. Nothing in
        // the reporting may index past the end of a series.
        cs_async_util::test_util::block_on(handler.handle(
            &ctx,
            direct("node-0042"),
            CgroupBatch::default(),
        ))
        .expect("an empty batch is still a batch");
        assert_eq!(tally.counts(), (2, 3));
    }

    #[test]
    fn a_selfmon_batch_reporting_drops_is_still_handled() {
        let tally = Tally::new();
        let handler = SelfmonReport::new(tally.clone());
        let ctx = cs_api::test_support::fake_ctx::<SelfmonService>();

        let batch = SelfmonBatch {
            sampled_unix_ms: vec![1_000, 2_000],
            process_cpu_usec: vec![100_000, 350_000],
            process_rss_bytes: vec![52_428_800, 52_428_800],
            process_threads: vec![5, 5],
            // The two that get a warning of their own.
            data_dropped: vec![0, 7],
            peer_timeouts: vec![0, 1],
            threads: vec![cs_plugin_selfmon::ThreadSeries {
                thread: "cgroup/sample".to_owned(),
                service: "cgroup".to_owned(),
                cpu_usec: vec![1_000_000, 2_200_000],
            }],
            ..SelfmonBatch::default()
        };

        cs_async_util::test_util::block_on(handler.handle(&ctx, direct("node-0042"), batch))
            .expect("a batch should be handled");
        assert_eq!(tally.counts(), (1, 2));
    }

    #[test]
    fn a_tally_accumulates_across_handlers_sharing_it() {
        let tally = Tally::new();
        let cgroup = CgroupReport::new(tally.clone());
        let selfmon = SelfmonReport::new(tally.clone());
        cgroup.tally.record(15);
        selfmon.tally.record(4);
        assert_eq!(tally.counts(), (2, 19));
    }
}
