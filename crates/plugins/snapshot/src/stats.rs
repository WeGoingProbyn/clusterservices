//! One metric's running statistics.
//!
//! Everything in here **merges**, because merging happens in three places: across the
//! nodes of a multi-node job, across a partial snapshot and its successor, and across
//! reporting periods. A median or a percentile cannot be merged from summaries, which
//! is why this keeps sums and a histogram rather than a mean and a p95 — the mean and
//! the quantiles are derived at read time, from things that add.

use cs_api::MetricKind;

use crate::hist::Histogram;

/// A counter that goes *down* has been reset — a rebuilt service, a recreated cgroup.
///
/// The delta across a reset is not a rate but an artefact, and recording it would put
/// an enormous spike in a graph at the moment something restarted. It is discarded and
/// counted instead, so `resets` is the warning that `total` understates the truth.
#[derive(Clone, Debug, Default)]
pub(crate) struct Stats {
    /// How many raw samples have been folded in. Zero means unavailable.
    pub(crate) samples: u64,
    /// First and last raw value seen.
    pub(crate) first: u64,
    pub(crate) last: u64,
    /// How much a counter advanced, resets excluded.
    pub(crate) total: u64,
    /// Of the rate for a counter, of the value for a gauge.
    pub(crate) min: f64,
    pub(crate) max: f64,
    /// Weighted sum, total weight in seconds, weighted sum of squares.
    pub(crate) sum: f64,
    pub(crate) weight: f64,
    pub(crate) sum_squares: f64,
    /// Time in each bucket.
    pub(crate) hist: Histogram,
    /// Counter resets seen.
    pub(crate) resets: u32,
    /// Samples thrown away because a batch's columns did not line up.
    pub(crate) discarded: u32,
    /// The previous sample, for the next delta or interval.
    prev: Option<(u64, u64)>,
}

impl Stats {
    /// Fold one sample in.
    ///
    /// `at` is wall-clock milliseconds. The first sample of a series contributes its
    /// value to `first`/`last` and nothing else: with nothing before it there is no
    /// interval to weight a gauge by and no delta to rate a counter from. That is why
    /// a one-sample snapshot reports `samples: 1` and a zero `weight` — honest, and
    /// distinguishable from a metric nobody could read.
    pub(crate) fn push(&mut self, kind: MetricKind, at: u64, value: u64) {
        if self.samples == 0 {
            self.first = value;
        }
        self.samples += 1;
        self.last = value;

        let Some((prev_at, prev_value)) = self.prev.replace((at, value)) else {
            return;
        };
        // Time cannot run backwards within a series; if it appears to, the batch is
        // out of order and the interval is meaningless.
        let Some(elapsed_ms) = at.checked_sub(prev_at) else {
            self.discarded = self.discarded.saturating_add(1);
            return;
        };
        if elapsed_ms == 0 {
            // Two samples at the same millisecond: no interval to weight, and a rate
            // would be a division by zero.
            return;
        }
        #[expect(
            clippy::cast_precision_loss,
            reason = "milliseconds as seconds; f64 is exact to 2^53 ms, which is 285,000 years"
        )]
        let seconds = elapsed_ms as f64 / 1000.0;

        let observed = match kind {
            MetricKind::Counter => {
                let Some(delta) = value.checked_sub(prev_value) else {
                    self.resets = self.resets.saturating_add(1);
                    return;
                };
                self.total += delta;
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "a counter delta large enough to lose precision here is not a rate"
                )]
                let delta = delta as f64;
                delta / seconds
            }
            // The level that stood *over* this interval is the one at its start: a
            // gauge read at time T described the state until the next reading.
            #[expect(
                clippy::cast_precision_loss,
                reason = "a byte count past 2^53 is not a memory reading"
            )]
            MetricKind::Gauge => prev_value as f64,
        };

        self.observe(observed, seconds);
    }

    /// Record one value that stood for `weight` seconds.
    fn observe(&mut self, value: f64, weight: f64) {
        if self.weight == 0.0 {
            self.min = value;
            self.max = value;
        } else {
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
        self.sum += value * weight;
        self.weight += weight;
        self.sum_squares += value * value * weight;
        self.hist.add(value, weight);
    }

    /// The time-weighted mean, or `None` when no interval has been observed.
    ///
    /// Only the tests use this: a snapshot stores the sum and the weight instead, so
    /// that two of them can be merged, and [`crate::mean`] derives the figure on read.
    #[cfg(test)]
    pub(crate) fn mean(&self) -> Option<f64> {
        (self.weight > 0.0).then(|| self.sum / self.weight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counter(samples: &[(u64, u64)]) -> Stats {
        let mut stats = Stats::default();
        for &(at, value) in samples {
            stats.push(MetricKind::Counter, at, value);
        }
        stats
    }

    fn gauge(samples: &[(u64, u64)]) -> Stats {
        let mut stats = Stats::default();
        for &(at, value) in samples {
            stats.push(MetricKind::Gauge, at, value);
        }
        stats
    }

    /// A counter's statistics describe its rate, not its values.
    #[test]
    fn a_counter_is_summarised_by_its_rate() {
        // One second apart, advancing by 1_000_000 then 2_000_000 µs: one core then
        // two, which is what a job going parallel looks like.
        let stats = counter(&[(1000, 0), (2000, 1_000_000), (3000, 3_000_000)]);

        assert_eq!(stats.samples, 3);
        assert_eq!(stats.first, 0);
        assert_eq!(stats.last, 3_000_000);
        assert_eq!(stats.total, 3_000_000, "the amount, resets excluded");
        assert!((stats.min - 1_000_000.0).abs() < 1.0, "min rate");
        assert!((stats.max - 2_000_000.0).abs() < 1.0, "max rate");
        assert!(
            (stats.mean().expect("a mean") - 1_500_000.0).abs() < 1.0,
            "mean rate over two equal intervals"
        );
        assert!((stats.weight - 2.0).abs() < f64::EPSILON, "two seconds");
    }

    /// The reason counters and gauges cannot share one code path: a counter's own
    /// minimum is its first sample, which says nothing about the job.
    #[test]
    fn a_gauge_is_summarised_by_its_values() {
        let stats = gauge(&[(1000, 100), (2000, 300), (3000, 200)]);

        assert_eq!(stats.samples, 3);
        assert_eq!(stats.total, 0, "a gauge has no total");
        // The value at the start of each interval: 100 for the first second, 300 for
        // the second. The last reading closes no interval, so it does not weigh in.
        assert!((stats.min - 100.0).abs() < f64::EPSILON);
        assert!((stats.max - 300.0).abs() < f64::EPSILON);
        assert!((stats.mean().expect("a mean") - 200.0).abs() < f64::EPSILON);
        assert_eq!(stats.last, 200, "but it is still the last reading");
    }

    /// Time-weighting is the whole point: an interval twice as long counts twice.
    #[test]
    fn a_gauge_mean_is_weighted_by_how_long_each_level_stood() {
        // 100 for one second, then 400 for three.
        let stats = gauge(&[(1000, 100), (2000, 400), (5000, 400)]);
        let mean = stats.mean().expect("a mean");
        // (100*1 + 400*3) / 4
        assert!((mean - 325.0).abs() < f64::EPSILON, "got {mean}");

        // Unweighted it would have been 250, which is the wrong answer a simple
        // average gives when the sampler slips.
        assert!((mean - 250.0).abs() > 1.0);
    }

    /// A restart makes a counter go backwards. Recording that delta would put an
    /// enormous spike in a graph exactly when something was rebuilt.
    #[test]
    fn a_counter_reset_is_excluded_and_counted() {
        let stats = counter(&[
            (1000, 5_000_000),
            (2000, 6_000_000),
            // Restarted: the new instance counts from zero.
            (3000, 0),
            (4000, 1_000_000),
        ]);

        assert_eq!(stats.resets, 1);
        assert_eq!(
            stats.total, 2_000_000,
            "the two real advances, not the negative one"
        );
        assert!(
            (stats.max - 1_000_000.0).abs() < 1.0,
            "no spike from the reset: {}",
            stats.max
        );
    }

    #[test]
    fn a_single_sample_has_no_rate_and_says_so() {
        let stats = counter(&[(1000, 42)]);
        assert_eq!(stats.samples, 1);
        assert_eq!(stats.first, 42);
        assert_eq!(stats.last, 42);
        assert!(stats.mean().is_none(), "one sample closes no interval");
        assert!((stats.weight - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn a_metric_nobody_could_read_stays_empty() {
        let stats = Stats::default();
        assert_eq!(stats.samples, 0, "which is how absence is reported");
        assert!(stats.mean().is_none());
    }

    #[test]
    fn samples_out_of_order_are_discarded_rather_than_believed() {
        let stats = counter(&[(2000, 10), (1000, 20)]);
        assert_eq!(stats.discarded, 1);
        assert!(stats.mean().is_none());
    }

    #[test]
    fn two_samples_at_the_same_instant_do_not_divide_by_zero() {
        let stats = counter(&[(1000, 10), (1000, 20)]);
        assert!(stats.mean().is_none());
        assert!(stats.max.is_finite());
    }
}
