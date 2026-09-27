//! Reading a [`CgroupBatch`] as named series.
//!
//! The one place that knows which cgroup file each field came from, and therefore
//! whether it is a counter or a gauge. Everything downstream — the snapshot
//! accumulator, storage, a dashboard — works against [`cs_api::Metrics`] instead of
//! against this crate's message type.

use crate::proto::CgroupBatch;

impl cs_api::Metrics for CgroupBatch {
    fn timestamps(&self) -> &[u64] {
        &self.sampled_unix_ms
    }

    /// Every series, with its kind — the one place that knows which cgroup file a
    /// field came from and therefore how it behaves.
    ///
    /// Names are dotted and mirror the kernel's own (`cpu.stat`'s `usage_usec`
    /// becomes `cpu.usage_usec`), so somebody reading a stored series can find the
    /// file it came from. They are also **stable**: renaming one splits a series in
    /// two in whatever is storing it, with no error anywhere to say so.
    fn series(&self) -> Vec<cs_api::Series<'_>> {
        use cs_api::Series;
        vec![
            // cpu.stat: microseconds and event counts, all cumulative.
            Series::counter("cpu.usage_usec", &self.cpu_usage_usec),
            Series::counter("cpu.user_usec", &self.cpu_user_usec),
            Series::counter("cpu.system_usec", &self.cpu_system_usec),
            Series::counter("cpu.nr_periods", &self.cpu_nr_periods),
            Series::counter("cpu.nr_throttled", &self.cpu_nr_throttled),
            Series::counter("cpu.throttled_usec", &self.cpu_throttled_usec),
            // memory: readings, not totals. A mean over these is a real answer and a
            // difference between two of them is not.
            Series::gauge("memory.current", &self.memory_current),
            Series::gauge("memory.peak", &self.memory_peak),
            Series::gauge("memory.anon", &self.memory_anon),
            Series::gauge("memory.file", &self.memory_file),
            // ...except memory.events, which counts things that happened.
            Series::counter("memory.max_events", &self.memory_max_events),
            Series::counter("memory.oom_kill", &self.memory_oom_kill),
            // io.stat, summed across devices. Cumulative.
            Series::counter("io.rbytes", &self.io_rbytes),
            Series::counter("io.wbytes", &self.io_wbytes),
            Series::counter("io.rios", &self.io_rios),
            Series::counter("io.wios", &self.io_wios),
            // pids.current is how many exist now.
            Series::gauge("pids.current", &self.pids_current),
            // Pressure stall time, cumulative microseconds.
            Series::counter("cpu.pressure_some_usec", &self.cpu_pressure_some_usec),
            Series::counter("memory.pressure_some_usec", &self.memory_pressure_some_usec),
            Series::counter("memory.pressure_full_usec", &self.memory_pressure_full_usec),
            Series::counter("io.pressure_some_usec", &self.io_pressure_some_usec),
            Series::counter("io.pressure_full_usec", &self.io_pressure_full_usec),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cs_api::{MetricKind, Metrics};

    /// Every field of the batch must appear, or a metric is collected on the node and
    /// then silently thrown away by everything downstream.
    #[test]
    fn every_series_in_the_batch_is_declared() {
        let batch = CgroupBatch::default();
        let declared = batch.series().len();
        // Counted from the proto: 6 cpu.stat + 4 memory readings + 2 memory.events
        // + 4 io + 1 pids + 5 pressure.
        assert_eq!(
            declared, 22,
            "a field was added to the proto without being declared here"
        );
    }

    /// The distinction the trait exists for. Getting one of these wrong produces a
    /// plausible number rather than an error, so it is pinned.
    #[test]
    fn memory_is_a_gauge_and_cpu_is_a_counter() {
        let batch = CgroupBatch::default();
        let series = batch.series();
        let kind = |name: &str| {
            series
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("{name} is not declared"))
                .kind
        };

        assert_eq!(kind("cpu.usage_usec"), MetricKind::Counter);
        assert_eq!(kind("memory.current"), MetricKind::Gauge);
        assert_eq!(kind("memory.peak"), MetricKind::Gauge);
        assert_eq!(kind("pids.current"), MetricKind::Gauge);
        // Events are counted even though they live in a memory file.
        assert_eq!(kind("memory.oom_kill"), MetricKind::Counter);
        assert_eq!(kind("io.wbytes"), MetricKind::Counter);
        assert_eq!(kind("io.pressure_full_usec"), MetricKind::Counter);
    }

    #[test]
    fn names_are_dotted_and_unique() {
        let batch = CgroupBatch::default();
        let series = batch.series();
        let mut names: Vec<&str> = series.iter().map(|s| s.name).collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "two series share a name");
        for name in names {
            assert!(name.contains('.'), "{name} is not dotted");
        }
    }

    /// An empty batch declares every metric as unavailable rather than as zero, and a
    /// filled one lines up.
    #[test]
    fn an_empty_batch_declares_everything_unavailable() {
        let batch = CgroupBatch::default();
        assert!(batch.series().iter().all(|s| !s.is_available()));
        assert!(batch.columns_line_up(), "vacuously, but it must not panic");

        let filled = CgroupBatch {
            sampled_unix_ms: vec![1000, 2000],
            cpu_usage_usec: vec![10, 20],
            ..CgroupBatch::default()
        };
        assert!(filled.columns_line_up());
        let series = filled.series();
        let cpu = series
            .iter()
            .find(|s| s.name == "cpu.usage_usec")
            .expect("declared");
        assert!(cpu.is_available());
        assert!(
            !series
                .iter()
                .find(|s| s.name == "memory.current")
                .expect("declared")
                .is_available()
        );
    }
}
