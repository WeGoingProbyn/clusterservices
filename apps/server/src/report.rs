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
}

impl CgroupReport {
    /// A handler sharing `tally`.
    #[must_use]
    pub fn new(tally: Tally) -> Self {
        Self { tally }
    }
}

impl ServiceBound for CgroupReport {
    type Service = CgroupService;
}

impl Handler for CgroupReport {
    async fn handle(
        &self,
        _ctx: &ServiceCtx<CgroupService>,
        from: Origin<'_>,
        batch: CgroupBatch,
    ) -> Result<()> {
        let samples = batch.sampled_unix_ms.len();
        self.tally.record(samples);

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

        info!(
            "{} selfmon  {samples} samples  agent {} rss {} threads {}  queue {}d/{}c  frames {}{}",
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
