//! Reading a plugin's batch as named series, so something downstream can summarise
//! it without knowing the plugin's message type.
//!
//! This is the answer to a question the wire format deliberately does not answer:
//! nothing in `Data` says that `cpu_usage_usec` is a counter and `memory_current` is
//! a gauge, and it should not — that is the plugin's own knowledge, and putting it in
//! the protocol would mean every plugin's semantics had to be versioned alongside the
//! frame format. A plugin declares it here instead, in code, next to the fields it
//! describes.
//!
//! The distinction is not cosmetic. A counter is monotonic, so its minimum is its
//! first sample and its maximum is its last — statistics on the raw values of one are
//! worthless, and what is wanted is statistics of its *rate*. A gauge is the other way
//! round: its value is the interesting thing and its difference means little. A
//! summariser that could not tell them apart would have to guess, and would be wrong
//! about half the fleet's metrics.

/// How a series changes, and therefore how to summarise it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum MetricKind {
    /// Monotonic and cumulative: bytes written, microseconds of CPU, events counted.
    ///
    /// Summarise the **rate** between consecutive samples, never the values. A value
    /// that goes *down* is a reset — a rebuilt service starting from zero, a cgroup
    /// recreated — and the delta across it is not a rate but an artefact, so it is
    /// discarded and counted rather than recorded as a number.
    Counter,
    /// A level that can move either way: bytes resident, processes running.
    ///
    /// Summarise the values, weighted by how long each one stood, because a sample
    /// interval that slips would otherwise let a dense stretch outvote a sparse one.
    Gauge,
}

impl MetricKind {
    /// Stable lowercase name, for logs and for storage that records the kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
        }
    }
}

impl std::fmt::Display for MetricKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One named series out of a batch.
///
/// Borrowed, not copied: a batch is already columnar, so its arrays are handed over
/// as they sit.
#[derive(Clone, Copy, Debug)]
pub struct Series<'a> {
    /// Dotted name, stable across versions of the plugin — it is what storage keys
    /// on, so renaming one silently splits a series in two.
    pub name: &'static str,
    /// How to summarise it.
    pub kind: MetricKind,
    /// The values, one per timestamp — **or empty, meaning the metric is
    /// unavailable on this kernel.** Never partially filled: a plugin with nothing
    /// to report for a metric reports nothing, rather than padding with zeros that
    /// would look like measurements.
    pub values: &'a [u64],
}

impl<'a> Series<'a> {
    /// A cumulative series.
    #[must_use]
    pub const fn counter(name: &'static str, values: &'a [u64]) -> Self {
        Self {
            name,
            kind: MetricKind::Counter,
            values,
        }
    }

    /// A series of levels.
    #[must_use]
    pub const fn gauge(name: &'static str, values: &'a [u64]) -> Self {
        Self {
            name,
            kind: MetricKind::Gauge,
            values,
        }
    }

    /// Whether this kernel exposed the metric at all.
    ///
    /// Empty is the fleet's normal state for something: `memory.peak` needs Linux
    /// 5.19, PSI needs `CONFIG_PSI`, `cpu.stat` reports throttling only once a limit
    /// is set. A summariser must carry the absence through rather than average it
    /// into a zero.
    #[must_use]
    pub const fn is_available(&self) -> bool {
        !self.values.is_empty()
    }
}

/// A batch that can be read as named series.
///
/// Implemented by a plugin on its own message type, which is the only place that
/// knows which field means what. Everything downstream — a summariser, a storage
/// writer, a dashboard — works against this instead of against the plugin.
pub trait Metrics {
    /// When each sample was taken, in milliseconds since the unix epoch.
    ///
    /// Wall clock, because a timestamp is data rather than timing: the engine's
    /// `Clock` is monotonic by design and monotonic time means nothing to a reader on
    /// another machine.
    fn timestamps(&self) -> &[u64];

    /// Every series in the batch, available or not.
    ///
    /// Include the unavailable ones: "this kernel does not expose PSI" is worth
    /// recording, and a series that simply vanished from the list is
    /// indistinguishable from a plugin that was upgraded.
    fn series(&self) -> Vec<Series<'_>>;

    /// Whether every series is either empty or exactly as long as `timestamps`.
    ///
    /// The invariant the columnar shape rests on. Provided rather than required, so a
    /// summariser can check a batch that crossed the network before trusting its
    /// arrays to line up.
    fn columns_line_up(&self) -> bool {
        let rows = self.timestamps().len();
        self.series()
            .iter()
            .all(|series| series.values.is_empty() || series.values.len() == rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A batch shaped like a real one, to exercise the trait's own logic.
    struct Batch {
        times: Vec<u64>,
        cpu: Vec<u64>,
        rss: Vec<u64>,
        psi: Vec<u64>,
    }

    impl Metrics for Batch {
        fn timestamps(&self) -> &[u64] {
            &self.times
        }

        fn series(&self) -> Vec<Series<'_>> {
            vec![
                Series::counter("cpu.usage_usec", &self.cpu),
                Series::gauge("memory.current", &self.rss),
                Series::counter("cpu.pressure_some_usec", &self.psi),
            ]
        }
    }

    fn batch() -> Batch {
        Batch {
            times: vec![1000, 2000, 3000],
            cpu: vec![10, 20, 30],
            rss: vec![100, 200, 150],
            // This kernel has no PSI, which is ordinary.
            psi: Vec::new(),
        }
    }

    #[test]
    fn a_batch_reports_its_series_with_their_kinds() {
        let batch = batch();
        let series = batch.series();
        assert_eq!(series.len(), 3);
        assert_eq!(series[0].kind, MetricKind::Counter);
        assert_eq!(series[1].kind, MetricKind::Gauge);
        assert_eq!(series[1].name, "memory.current");
    }

    /// The distinction the whole trait exists to carry.
    #[test]
    fn an_absent_metric_is_listed_and_marked_unavailable() {
        let batch = batch();
        let series = batch.series();
        let psi = series
            .iter()
            .find(|s| s.name == "cpu.pressure_some_usec")
            .expect("an unavailable metric is still listed");
        assert!(
            !psi.is_available(),
            "absent must be distinguishable from zero"
        );
        assert!(series[0].is_available());
    }

    #[test]
    fn columns_line_up_when_every_series_is_full_or_empty() {
        let batch = batch();
        assert!(batch.columns_line_up());
    }

    /// What a summariser is checking for before it indexes anything: a batch that
    /// crossed the network is not to be trusted about its own shape.
    #[test]
    fn a_short_column_does_not_line_up() {
        let ragged = Batch {
            times: vec![1000, 2000, 3000],
            cpu: vec![10, 20],
            rss: vec![100, 200, 150],
            psi: Vec::new(),
        };
        assert!(!ragged.columns_line_up());
    }

    #[test]
    fn kinds_have_stable_names() {
        assert_eq!(MetricKind::Counter.to_string(), "counter");
        assert_eq!(MetricKind::Gauge.as_str(), "gauge");
    }
}
