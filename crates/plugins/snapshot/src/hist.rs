//! A sparse, time-weighted histogram.
//!
//! This is what answers the questions scalars cannot: "how long was this job using
//! less than a tenth of a core", "did it sit at its memory limit or touch it once".
//! With samples a few seconds apart what exists is a rate or a level per *interval*,
//! so "time at" is a sum of interval durations — which is a histogram bucket, and why
//! one belongs in a snapshot at all.
//!
//! Three decisions, each for a reason that outlives this file:
//!
//! - **Log-scaled, one boundary set for everything.** A CPU rate spans 0 to a few
//!   hundred cores; memory spans bytes to terabytes. Per-metric boundaries would be
//!   more precise and would have to be declared, versioned and matched on read; powers
//!   of two cover fourteen orders of magnitude in forty-eight numbers and need none of
//!   that.
//! - **Sparse.** A job's CPU rate occupies a handful of buckets out of forty-eight, so
//!   storing only the occupied ones is both smaller and honest: a bucket that is absent
//!   was never entered, which is the same distinction the metrics themselves draw
//!   between absent and zero.
//! - **Merges.** Bucket counts add. That is the whole reason a histogram is here
//!   instead of the percentiles a reader actually wants — those are computed from this
//!   at read time, and cannot be merged from each other at all.

use std::collections::BTreeMap;

/// How many buckets, including the one for zero.
///
/// Bucket 0 is exactly zero. Bucket `i` (1 ≤ i < BUCKETS) covers values up to
/// `2^(i-1)`, so the last covers everything past `2^46` ≈ 7×10^13 — a rate of seventy
/// trillion units per second, or seventy terabytes resident. Anything beyond that is
/// not a measurement.
pub(crate) const BUCKETS: u32 = 48;

/// The upper bound of each bucket, for a reader that must not have to guess.
#[must_use]
pub(crate) fn bounds() -> Vec<u64> {
    (0..BUCKETS)
        .map(|index| match index {
            0 => 0,
            // 1 << 46 is the last finite bound; the top bucket is everything above.
            i if i < BUCKETS - 1 => 1u64 << (i - 1),
            _ => u64::MAX,
        })
        .collect()
}

/// Which bucket a value falls in.
fn bucket_of(value: f64) -> u32 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    // Bucket `i` has upper bound `2^(i-1)`, so it holds values in `(2^(i-2), 2^(i-1)]`
    // and the bucket for `v` is `ceil(log2(v)) + 1`: 1.0 and 0.5 both land in bucket 1
    // (bound 1), 2.0 in bucket 2, and 3.0 and 4.0 in bucket 3 (bound 4).
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "clamped to 1..BUCKETS immediately below"
    )]
    let index = (value.log2().ceil() as i64 + 1).clamp(1, i64::from(BUCKETS) - 1) as u32;
    index
}

/// Time spent in each occupied bucket, in milliseconds.
///
/// A `BTreeMap` rather than an array: sparse, and ordered so the wire form comes out
/// the same every time, which makes a snapshot comparable byte for byte in a test.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Histogram {
    buckets: BTreeMap<u32, u64>,
}

impl Histogram {
    /// Rebuild from the columnar form a snapshot carries.
    ///
    /// Mismatched column lengths take the shorter, because a snapshot that crossed the
    /// network is not to be trusted about its own shape and losing a bucket is better
    /// than pairing a count with the wrong bound.
    pub(crate) fn from_columns(buckets: &[u32], millis: &[u64]) -> Self {
        Self {
            buckets: buckets
                .iter()
                .copied()
                .zip(millis.iter().copied())
                .filter(|&(bucket, _)| bucket < BUCKETS)
                .collect(),
        }
    }

    /// Record `value` as having stood for `weight` seconds.
    pub(crate) fn add(&mut self, value: f64, weight: f64) {
        if weight <= 0.0 || !weight.is_finite() {
            return;
        }
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "a weight in seconds that overflows u64 milliseconds is 500 million years"
        )]
        let millis = (weight * 1000.0).round() as u64;
        if millis == 0 {
            // Sub-millisecond intervals would otherwise vanish silently; they are
            // still worth their bucket, so round up rather than drop.
            *self.buckets.entry(bucket_of(value)).or_default() += 1;
            return;
        }
        *self.buckets.entry(bucket_of(value)).or_default() += millis;
    }

    /// Add another histogram's time to this one.
    pub(crate) fn merge(&mut self, other: &Self) {
        for (&bucket, &millis) in &other.buckets {
            *self.buckets.entry(bucket).or_default() += millis;
        }
    }

    /// The occupied buckets and their time, as two columns.
    ///
    /// Columnar to match every other batch in this project, and because two parallel
    /// varint arrays encode smaller than a repeated message.
    pub(crate) fn columns(&self) -> (Vec<u32>, Vec<u64>) {
        (
            self.buckets.keys().copied().collect(),
            self.buckets.values().copied().collect(),
        )
    }

    /// Total time recorded, in milliseconds.
    #[cfg(test)]
    pub(crate) fn total_ms(&self) -> u64 {
        self.buckets.values().sum()
    }

    /// How many buckets are occupied.
    #[cfg(test)]
    pub(crate) fn occupied(&self) -> usize {
        self.buckets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_are_powers_of_two_with_zero_first_and_everything_last() {
        let bounds = bounds();
        assert_eq!(bounds.len() as u32, BUCKETS);
        assert_eq!(bounds[0], 0, "bucket 0 is exactly zero");
        assert_eq!(bounds[1], 1);
        assert_eq!(bounds[2], 2);
        assert_eq!(bounds[3], 4);
        assert_eq!(*bounds.last().expect("a last bound"), u64::MAX);
        // Strictly increasing, or a reader cannot bisect it.
        assert!(bounds.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn zero_and_negatives_land_in_the_zero_bucket() {
        assert_eq!(bucket_of(0.0), 0);
        assert_eq!(bucket_of(-1.0), 0);
        assert_eq!(bucket_of(f64::NAN), 0);
    }

    #[test]
    fn a_value_lands_in_the_bucket_its_bound_describes() {
        let bounds = bounds();
        for value in [0.5f64, 1.0, 1.5, 2.0, 3.0, 1024.0, 1e9] {
            let bucket = bucket_of(value);
            let upper = bounds[bucket as usize];
            let lower = if bucket <= 1 {
                0
            } else {
                bounds[bucket as usize - 1]
            };
            #[expect(clippy::cast_precision_loss, reason = "bounds are exact well past 1e9")]
            let (lower, upper) = (lower as f64, upper as f64);
            assert!(
                value > lower && value <= upper,
                "{value} is not in ({lower}, {upper}] — bucket {bucket}"
            );
        }
    }

    #[test]
    fn a_huge_value_lands_in_the_top_bucket_rather_than_out_of_range() {
        assert_eq!(bucket_of(1e300), BUCKETS - 1);
        assert_eq!(bucket_of(f64::INFINITY), 0, "infinity is not a measurement");
    }

    /// The question a histogram is here to answer.
    #[test]
    fn time_accumulates_per_bucket() {
        let mut hist = Histogram::default();
        // Three seconds nearly idle, one second at about one core.
        hist.add(0.0, 3.0);
        hist.add(1_000_000.0, 1.0);

        let (buckets, millis) = hist.columns();
        assert_eq!(buckets.len(), 2, "sparse: only what was entered");
        assert_eq!(hist.total_ms(), 4000);
        assert_eq!(millis[0], 3000, "the idle bucket sorts first");
    }

    #[test]
    fn values_in_the_same_bucket_add_up() {
        let mut hist = Histogram::default();
        // Both in (1024, 2048]. 1000 and 1500 would *not* share a band, which is the
        // resolution a log scale buys and the precision it gives up.
        hist.add(1100.0, 1.0);
        hist.add(1500.0, 2.0);
        assert_eq!(hist.occupied(), 1, "both are in (1024, 2048]");
        assert_eq!(hist.total_ms(), 3000);
    }

    /// What the log scale costs: two rates a third apart can land in different buckets.
    /// Worth pinning, so nobody reads more precision into a stored distribution than is
    /// there.
    #[test]
    fn neighbouring_values_across_a_power_of_two_do_not_share_a_bucket() {
        assert_ne!(bucket_of(1000.0), bucket_of(1500.0));
        assert_eq!(bucket_of(1100.0), bucket_of(2048.0));
    }

    #[test]
    fn a_sub_millisecond_interval_is_kept_rather_than_lost() {
        let mut hist = Histogram::default();
        hist.add(5.0, 0.0001);
        assert_eq!(hist.occupied(), 1);
        assert_eq!(
            hist.total_ms(),
            1,
            "rounded up to the smallest unit there is"
        );
    }

    #[test]
    fn a_zero_or_absurd_weight_records_nothing() {
        let mut hist = Histogram::default();
        hist.add(5.0, 0.0);
        hist.add(5.0, -1.0);
        hist.add(5.0, f64::NAN);
        assert_eq!(hist.occupied(), 0);
    }

    /// The property that lets a long job be reported in pieces.
    #[test]
    fn merging_adds_time_bucket_by_bucket() {
        let mut first = Histogram::default();
        first.add(0.0, 1.0);
        first.add(1e6, 1.0);

        let mut second = Histogram::default();
        second.add(0.0, 2.0);
        second.add(1e9, 1.0);

        let mut whole = Histogram::default();
        whole.add(0.0, 1.0);
        whole.add(1e6, 1.0);
        whole.add(0.0, 2.0);
        whole.add(1e9, 1.0);

        first.merge(&second);
        assert_eq!(first, whole, "merged must equal accumulated");
        assert_eq!(first.total_ms(), 5000);
    }

    #[test]
    fn columns_come_out_in_bucket_order_every_time() {
        let mut hist = Histogram::default();
        hist.add(1e9, 1.0);
        hist.add(0.0, 1.0);
        hist.add(1e3, 1.0);
        let (buckets, _) = hist.columns();
        assert!(buckets.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
