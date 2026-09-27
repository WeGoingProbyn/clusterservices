//! Merging two snapshots of the same step.
//!
//! The operation the whole statistic set was chosen for, and the aggregator's core:
//! a long job arrives as periodic pieces, a multi-node job as one snapshot per node,
//! and a head that restarted mid-job sends the rest afterwards. All three are this
//! function.
//!
//! It is also the test that matters. "Every statistic merges" is a claim about
//! arithmetic, and the way to check it is to accumulate a series whole, accumulate it
//! in pieces, merge the pieces, and require the same answer — which is what the tests
//! below do.

// Through cs-api's re-export, not a second dependency: rule 2 says a plugin
// depends on cs-api alone, and this is what that re-export is for.
use cs_api::{Error, ErrorKind, Result};

use crate::hist::Histogram;
use crate::proto::{CloseReason, JobSnapshot, MetricSummary};

/// Fold `next` into `base`, which must describe the same step on the same node.
///
/// `next` is taken to cover a **later** period than `base`: `last` and the close reason
/// come from it. Nothing in the numbers can establish that ordering, so the caller owns
/// it — an aggregator merging in arrival order gets it right, since a head only ever
/// sends a step's pieces in order.
///
/// # Errors
///
/// If the two describe different steps, or were built against different histogram
/// boundaries. Both would produce a plausible answer that was wrong, which is the one
/// outcome worth an error.
pub fn merge(base: &mut JobSnapshot, next: &JobSnapshot) -> Result<()> {
    if (base.job_id, &base.step, &base.node) != (next.job_id, &next.step, &next.node) {
        return Err(Error::new(
            ErrorKind::Config,
            format!(
                "cannot merge {}.{} on {} with {}.{} on {}: they are different steps",
                base.job_id, base.step, base.node, next.job_id, next.step, next.node
            ),
        ));
    }
    if base.bucket_bounds != next.bucket_bounds {
        // A future version that rescales the histogram would otherwise have its
        // buckets added to the old scale's, silently.
        return Err(Error::new(
            ErrorKind::Decode,
            "cannot merge snapshots built against different histogram boundaries",
        ));
    }

    // `samples`, not a zero timestamp, decides whether a period is real. Zero is a
    // perfectly good unix millisecond — 1970 — and treating it as "unset" made a job
    // that started at the epoch lose its start. (Twice: the accumulator had the same
    // bug, and both were found by a test whose batch happened to start at zero.)
    match (base.samples, next.samples) {
        (0, 0) => {}
        (0, _) => {
            base.first_unix_ms = next.first_unix_ms;
            base.last_unix_ms = next.last_unix_ms;
        }
        (_, 0) => {}
        (_, _) => {
            base.first_unix_ms = base.first_unix_ms.min(next.first_unix_ms);
            base.last_unix_ms = base.last_unix_ms.max(next.last_unix_ms);
        }
    }
    base.samples += next.samples;
    base.uid = if next.uid == 0 { base.uid } else { next.uid };
    // One piece reaching the end of the step makes the whole thing whole.
    base.complete |= next.complete;
    if next.closed_because != CloseReason::Unspecified as i32 {
        base.closed_because = next.closed_because;
    }

    for summary in &next.metrics {
        match base.metrics.iter_mut().find(|m| m.metric == summary.metric) {
            Some(ours) => merge_metric(ours, summary),
            // A metric one side never reported: a plugin upgraded mid-job, or one node
            // of a job runs a kernel the others do not. Take it as it stands rather
            // than dropping it.
            None => base.metrics.push(summary.clone()),
        }
    }

    Ok(())
}

/// Fold one metric's summary into another's.
fn merge_metric(base: &mut MetricSummary, next: &MetricSummary) {
    if next.samples == 0 {
        // Unavailable on that side. Anything it carries is meaningless, and adding its
        // zeros would drag a mean towards nothing.
        return;
    }
    if base.samples == 0 {
        let metric = std::mem::take(&mut base.metric);
        *base = next.clone();
        base.metric = metric;
        return;
    }

    base.samples += next.samples;
    base.last = next.last;
    base.total += next.total;
    base.resets = base.resets.saturating_add(next.resets);
    base.discarded = base.discarded.saturating_add(next.discarded);

    if next.weight > 0.0 {
        if base.weight > 0.0 {
            base.min = base.min.min(next.min);
            base.max = base.max.max(next.max);
        } else {
            base.min = next.min;
            base.max = next.max;
        }
        base.sum += next.sum;
        base.weight += next.weight;
        base.sum_squares += next.sum_squares;

        // Through `Histogram` rather than by zipping the columns, so the sparse
        // representation is built in exactly one place.
        let mut hist = Histogram::from_columns(&base.bucket, &base.bucket_ms);
        hist.merge(&Histogram::from_columns(&next.bucket, &next.bucket_ms));
        let (bucket, bucket_ms) = hist.columns();
        base.bucket = bucket;
        base.bucket_ms = bucket_ms;
    }
}

/// The time-weighted mean of a merged summary, or `None` when no interval was observed.
///
/// A derived figure, computed on read from the sum and the weight that were stored.
/// Storing the mean instead would have made this function impossible to write: two
/// means cannot be averaged unless they covered equal time, and across nodes they never
/// do.
#[must_use]
pub fn mean(summary: &MetricSummary) -> Option<f64> {
    (summary.weight > 0.0).then(|| summary.sum / summary.weight)
}

/// The standard deviation of a merged summary, from the stored sums.
///
/// Also derived, and for the same reason: a standard deviation does not merge, a sum of
/// squares does.
#[must_use]
pub fn stddev(summary: &MetricSummary) -> Option<f64> {
    let mean = mean(summary)?;
    let variance = (summary.sum_squares / summary.weight) - mean * mean;
    // Floating point can make a variance that should be zero very slightly negative.
    Some(variance.max(0.0).sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accumulate::{Accumulator, StepKey};
    use cs_plugin_cgroup::CgroupBatch;
    use std::time::Instant;

    fn key() -> StepKey {
        StepKey::new("node-1", 1234, "0")
    }

    /// Samples three seconds apart, starting at `start_ms`.
    fn batch(cpu: &[u64], memory: &[u64], start_ms: u64) -> CgroupBatch {
        let rows = cpu.len().max(memory.len()) as u64;
        CgroupBatch {
            sampled_unix_ms: (0..rows).map(|i| start_ms + i * 3000).collect(),
            cpu_usage_usec: cpu.to_vec(),
            memory_current: memory.to_vec(),
            ..CgroupBatch::default()
        }
    }

    /// Accumulate one batch into a finished snapshot.
    fn snapshot_of(batches: &[CgroupBatch]) -> JobSnapshot {
        let mut acc = Accumulator::new();
        let now = Instant::now();
        for (index, batch) in batches.iter().enumerate() {
            acc.fold(&key(), 1000, batch, index + 1 == batches.len(), now);
        }
        acc.take().remove(0)
    }

    fn metric<'a>(snapshot: &'a JobSnapshot, name: &str) -> &'a MetricSummary {
        snapshot
            .metrics
            .iter()
            .find(|m| m.metric == name)
            .unwrap_or_else(|| panic!("{name} is missing"))
    }

    /// The claim the whole statistic set rests on: pieces merged equal the whole
    /// accumulated. If this fails, periodic snapshots and multi-node jobs are both
    /// reporting wrong numbers.
    #[test]
    fn merging_the_pieces_equals_accumulating_the_whole() {
        let whole = snapshot_of(&[batch(
            &[0, 3_000_000, 9_000_000, 10_000_000],
            &[100, 400, 400, 200],
            0,
        )]);

        // The same samples, split — with the boundary sample in both, as a real head
        // would have it: the last reading of one window is the first of the next.
        let mut first = snapshot_of(&[batch(&[0, 3_000_000], &[100, 400], 0)]);
        let second = snapshot_of(&[batch(
            &[3_000_000, 9_000_000, 10_000_000],
            &[400, 400, 200],
            3_000,
        )]);
        merge(&mut first, &second).expect("the same step");

        let cpu_whole = metric(&whole, "cpu.usage_usec");
        let cpu_merged = metric(&first, "cpu.usage_usec");
        assert_eq!(cpu_merged.total, cpu_whole.total, "total CPU");
        assert!(
            (cpu_merged.max - cpu_whole.max).abs() < 1.0,
            "peak rate: {} vs {}",
            cpu_merged.max,
            cpu_whole.max
        );
        assert!(
            (cpu_merged.min - cpu_whole.min).abs() < 1.0,
            "quietest interval"
        );
        assert!(
            (mean(cpu_merged).expect("a mean") - mean(cpu_whole).expect("a mean")).abs() < 1.0,
            "mean rate"
        );
        assert!(
            (cpu_merged.weight - cpu_whole.weight).abs() < 1e-9,
            "weight"
        );

        let mem_whole = metric(&whole, "memory.current");
        let mem_merged = metric(&first, "memory.current");
        assert!((mem_merged.max - mem_whole.max).abs() < f64::EPSILON);
        assert!((mem_merged.min - mem_whole.min).abs() < f64::EPSILON);
        assert!(
            (mean(mem_merged).expect("mean") - mean(mem_whole).expect("mean")).abs() < 1e-9,
            "the time-weighted mean survives being split"
        );

        assert_eq!(first.first_unix_ms, whole.first_unix_ms);
        assert_eq!(first.last_unix_ms, whole.last_unix_ms);
        assert_eq!(
            first.bucket_bounds, whole.bucket_bounds,
            "and the histograms are on one scale"
        );
    }

    /// The histogram is the part that would be easiest to get wrong, since it is the
    /// only merged field with a sparse representation.
    #[test]
    fn merging_adds_the_histograms() {
        let mut first = snapshot_of(&[batch(&[0, 3_000_000], &[], 0)]);
        let second = snapshot_of(&[batch(&[3_000_000, 6_000_000], &[], 3_000)]);

        let before: u64 = metric(&first, "cpu.usage_usec").bucket_ms.iter().sum();
        let theirs: u64 = metric(&second, "cpu.usage_usec").bucket_ms.iter().sum();
        merge(&mut first, &second).expect("same step");
        let after: u64 = metric(&first, "cpu.usage_usec").bucket_ms.iter().sum();

        assert_eq!(after, before + theirs, "time adds up");
        let cpu = metric(&first, "cpu.usage_usec");
        assert!(
            cpu.bucket.windows(2).all(|pair| pair[0] < pair[1]),
            "and the buckets stay ordered"
        );
    }

    /// A step that ended in *either* piece has ended.
    #[test]
    fn completeness_comes_from_whichever_piece_reached_the_end() {
        let mut partial = snapshot_of(&[batch(&[0, 1_000_000], &[], 0)]);
        partial.complete = false;
        partial.closed_because = CloseReason::Periodic as i32;

        let ended = snapshot_of(&[batch(&[1_000_000, 2_000_000], &[], 3_000)]);
        assert!(ended.complete);

        merge(&mut partial, &ended).expect("same step");
        assert!(partial.complete);
        assert_eq!(partial.closed_because, CloseReason::StepEnded as i32);
    }

    /// One node of a job runs an older kernel: its snapshot has a metric the other's
    /// does not, and merging must not lose it.
    #[test]
    fn a_metric_only_one_side_reported_is_kept() {
        let mut bare = snapshot_of(&[batch(&[0, 1_000_000], &[], 0)]);
        let mut richer = snapshot_of(&[batch(&[1_000_000, 2_000_000], &[], 3_000)]);

        // Pretend the other side had PSI, which this one did not.
        let psi = richer
            .metrics
            .iter_mut()
            .find(|m| m.metric == "cpu.pressure_some_usec")
            .expect("declared");
        psi.samples = 2;
        psi.total = 500;
        psi.weight = 3.0;
        psi.sum = 150.0;

        merge(&mut bare, &richer).expect("same step");
        let merged = metric(&bare, "cpu.pressure_some_usec");
        assert_eq!(merged.samples, 2, "taken from the side that had it");
        assert_eq!(merged.total, 500);
    }

    #[test]
    fn merging_an_unavailable_metric_leaves_the_available_one_alone() {
        let mut have = snapshot_of(&[batch(&[0, 3_000_000], &[], 0)]);
        let before = metric(&have, "cpu.usage_usec").clone();

        let mut nothing = snapshot_of(&[batch(&[], &[100, 200], 0)]);
        nothing.samples = 0;
        merge(&mut have, &nothing).expect("same step");

        let after = metric(&have, "cpu.usage_usec");
        assert_eq!(after.samples, before.samples);
        assert_eq!(after.total, before.total);
        assert!((after.sum - before.sum).abs() < f64::EPSILON);
    }

    /// Merging two different steps would produce a plausible, wrong answer — the one
    /// case worth refusing rather than guessing.
    #[test]
    fn merging_different_steps_is_refused() {
        let mut one = snapshot_of(&[batch(&[0, 1], &[], 0)]);
        let mut other = snapshot_of(&[batch(&[0, 1], &[], 0)]);
        other.step = "batch".to_owned();

        let err = merge(&mut one, &other).expect_err("different steps");
        assert_eq!(err.kind(), ErrorKind::Config);
        assert!(err.to_string().contains("different steps"), "{err}");

        let mut elsewhere = snapshot_of(&[batch(&[0, 1], &[], 0)]);
        elsewhere.node = "node-2".to_owned();
        assert!(
            merge(&mut one, &elsewhere).is_err(),
            "the same step on another node is a different measurement"
        );
    }

    #[test]
    fn merging_across_a_rescaled_histogram_is_refused() {
        let mut one = snapshot_of(&[batch(&[0, 1_000_000], &[], 0)]);
        let mut rescaled = snapshot_of(&[batch(&[1_000_000, 2_000_000], &[], 3_000)]);
        rescaled.bucket_bounds = vec![0, 10, 100];

        let err = merge(&mut one, &rescaled).expect_err("different scales");
        assert_eq!(err.kind(), ErrorKind::Decode);
    }

    #[test]
    fn a_standard_deviation_is_derived_from_the_stored_sums() {
        // A steady rate has no spread; a varying one does.
        let steady = snapshot_of(&[batch(&[0, 3_000_000, 6_000_000], &[], 0)]);
        let spread = snapshot_of(&[batch(&[0, 3_000_000, 4_000_000], &[], 0)]);

        let steady = stddev(metric(&steady, "cpu.usage_usec")).expect("a deviation");
        let spread = stddev(metric(&spread, "cpu.usage_usec")).expect("a deviation");
        assert!(steady < 1.0, "a constant rate has no spread: {steady}");
        assert!(spread > 1000.0, "a varying one does: {spread}");
    }

    #[test]
    fn statistics_of_an_unavailable_metric_are_none_rather_than_zero() {
        let snapshot = snapshot_of(&[batch(&[0, 1], &[], 0)]);
        let psi = metric(&snapshot, "cpu.pressure_some_usec");
        assert!(mean(psi).is_none());
        assert!(stddev(psi).is_none());
    }
}
