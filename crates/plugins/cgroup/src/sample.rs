//! One tick's reading of one cgroup.

use std::collections::HashMap;
use std::path::Path;

use cs_api::{JobInfo, Result};

use crate::read::{IoTotals, Pressure, read_count, read_io_stat, read_keyed, read_pressure};

/// Everything read from one cgroup at one instant.
///
/// Every field is optional and for one reason: **this kernel may not have it.**
/// `memory.peak` needs 5.19, pressure needs `CONFIG_PSI`, and a cgroup can be
/// removed between listing it and reading it. `None` means "not available", which
/// the batch preserves as an empty series rather than flattening to zero.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub(crate) struct CgroupSample {
    pub(crate) cpu_usage_usec: Option<u64>,
    pub(crate) cpu_user_usec: Option<u64>,
    pub(crate) cpu_system_usec: Option<u64>,
    pub(crate) cpu_nr_periods: Option<u64>,
    pub(crate) cpu_nr_throttled: Option<u64>,
    pub(crate) cpu_throttled_usec: Option<u64>,

    pub(crate) memory_current: Option<u64>,
    pub(crate) memory_peak: Option<u64>,
    pub(crate) memory_anon: Option<u64>,
    pub(crate) memory_file: Option<u64>,
    pub(crate) memory_max_events: Option<u64>,
    pub(crate) memory_oom_kill: Option<u64>,

    pub(crate) io: Option<IoTotals>,
    pub(crate) pids_current: Option<u64>,

    pub(crate) cpu_pressure: Option<Pressure>,
    pub(crate) memory_pressure: Option<Pressure>,
    pub(crate) io_pressure: Option<Pressure>,
}

impl CgroupSample {
    /// Read every metric from a job step's cgroup.
    ///
    /// An `Err` means a file was there and unreadable *as a cgroup file* — a wrong
    /// assumption about the kernel's format, worth surfacing. A job that simply
    /// ended produces a sample full of `None`, which
    /// [`is_empty`](CgroupSample::is_empty) reports, because every read returns
    /// "absent" once the directory is gone.
    pub(crate) fn read(job: &JobInfo) -> Result<Self> {
        Self::read_dir(&job.cgroup)
    }

    fn read_dir(dir: &Path) -> Result<Self> {
        let cpu = read_keyed(&dir.join("cpu.stat"))?.unwrap_or_default();
        let memory = read_keyed(&dir.join("memory.stat"))?.unwrap_or_default();
        let events = read_keyed(&dir.join("memory.events"))?.unwrap_or_default();

        Ok(Self {
            cpu_usage_usec: pick(&cpu, "usage_usec"),
            cpu_user_usec: pick(&cpu, "user_usec"),
            cpu_system_usec: pick(&cpu, "system_usec"),
            cpu_nr_periods: pick(&cpu, "nr_periods"),
            cpu_nr_throttled: pick(&cpu, "nr_throttled"),
            cpu_throttled_usec: pick(&cpu, "throttled_usec"),

            memory_current: read_count(&dir.join("memory.current"))?,
            memory_peak: read_count(&dir.join("memory.peak"))?,
            memory_anon: pick(&memory, "anon"),
            memory_file: pick(&memory, "file"),
            memory_max_events: pick(&events, "max"),
            memory_oom_kill: pick(&events, "oom_kill"),

            io: read_io_stat(&dir.join("io.stat"))?,
            pids_current: read_count(&dir.join("pids.current"))?,

            cpu_pressure: read_pressure(&dir.join("cpu.pressure"))?,
            memory_pressure: read_pressure(&dir.join("memory.pressure"))?,
            io_pressure: read_pressure(&dir.join("io.pressure"))?,
        })
    }

    /// Whether nothing at all could be read — which is what a job that has just
    /// ended looks like.
    pub(crate) fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// A key from a `key value` file, if it was there.
fn pick(values: &HashMap<String, u64>, key: &str) -> Option<u64> {
    values.get(key).copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeCgroups;
    use cs_api::{ErrorKind, StepId};

    #[test]
    fn a_full_cgroup_reads_every_metric() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Batch, 1000);
        tree.write_typical(&job);

        let sample = CgroupSample::read(&job).expect("read");
        assert_eq!(sample.cpu_usage_usec, Some(123_456));
        assert_eq!(sample.cpu_nr_throttled, Some(2));
        assert_eq!(sample.memory_current, Some(8_388_608));
        assert_eq!(sample.memory_peak, Some(9_000_000));
        assert_eq!(sample.memory_anon, Some(4096));
        assert_eq!(sample.memory_oom_kill, Some(0));
        assert_eq!(sample.io.expect("io").rbytes, 1536);
        assert_eq!(sample.pids_current, Some(17));
        assert_eq!(sample.cpu_pressure.expect("psi").some_usec, 42);
        assert_eq!(sample.memory_pressure.expect("psi").full_usec, 7890);
        assert!(!sample.is_empty());
    }

    #[test]
    fn an_older_kernel_without_peak_or_psi_still_samples() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Index(0), 1000);
        // Only what every cgroup v2 kernel has.
        tree.write(
            &job,
            "cpu.stat",
            "usage_usec 500\nuser_usec 400\nsystem_usec 100\n",
        );
        tree.write(&job, "memory.current", "1024\n");
        tree.write(&job, "pids.current", "3\n");

        let sample = CgroupSample::read(&job).expect("read");
        assert_eq!(sample.cpu_usage_usec, Some(500));
        assert_eq!(sample.memory_current, Some(1024));
        assert_eq!(
            sample.memory_peak, None,
            "absent before 5.19, and that is not a failure"
        );
        assert_eq!(sample.cpu_pressure, None, "absent without CONFIG_PSI");
        assert_eq!(sample.io, None, "absent without io controller");
        assert_eq!(
            sample.cpu_nr_periods, None,
            "absent until a cpu limit is set"
        );
        assert!(!sample.is_empty());
    }

    #[test]
    fn a_job_that_ended_reads_as_nothing_rather_than_failing() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Batch, 1000);
        tree.write_typical(&job);
        // The job ends and slurmstepd removes the cgroup.
        tree.remove_job(&job);

        let sample = CgroupSample::read(&job).expect("a vanished cgroup is not an error");
        assert!(
            sample.is_empty(),
            "every read should have come back absent: {sample:?}"
        );
    }

    #[test]
    fn a_file_in_the_wrong_format_is_reported_rather_than_read_as_zero() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Batch, 1000);
        tree.write_typical(&job);
        tree.write(&job, "cpu.stat", "usage_usec what\n");

        let err = CgroupSample::read(&job).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.to_string().contains("cpu.stat"));
    }
}
