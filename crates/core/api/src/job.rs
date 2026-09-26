use std::fmt;
use std::path::{Path, PathBuf};

/// Which step of a job a cgroup belongs to.
///
/// Slurm names step cgroups `step_<id>`, where the id is either a number or one
/// of three special names.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum StepId {
    /// `step_batch` — the batch script itself.
    Batch,
    /// `step_extern` — the container for processes adopted into the job, e.g. by
    /// `pam_slurm_adopt` for an interactive ssh session.
    Extern,
    /// `step_interactive` — `salloc`-style interactive steps.
    Interactive,
    /// `step_<n>` — the n-th `srun` within the job.
    Index(u32),
}

impl StepId {
    /// Parse a step cgroup directory name, e.g. `step_batch` or `step_0`.
    ///
    /// ```
    /// use cs_api::StepId;
    ///
    /// assert_eq!(StepId::parse_dir("step_batch"), Some(StepId::Batch));
    /// assert_eq!(StepId::parse_dir("step_12"), Some(StepId::Index(12)));
    /// assert_eq!(StepId::parse_dir("job_9"), None);
    /// ```
    #[must_use]
    pub fn parse_dir(dir: &str) -> Option<Self> {
        let id = dir.strip_prefix("step_")?;
        match id {
            "batch" => Some(Self::Batch),
            "extern" => Some(Self::Extern),
            "interactive" => Some(Self::Interactive),
            // Reject `+0`, `0x1`, and leading zeroes so the mapping back to a
            // directory name is exact.
            n if n.bytes().all(|b| b.is_ascii_digit()) && !n.starts_with('0') || n == "0" => {
                n.parse().ok().map(Self::Index)
            }
            _ => None,
        }
    }

    /// The cgroup directory name for this step — the inverse of
    /// [`parse_dir`](StepId::parse_dir).
    #[must_use]
    pub fn dir_name(&self) -> String {
        format!("step_{self}")
    }
}

impl fmt::Display for StepId {
    /// The step id without the `step_` prefix: `batch`, `extern`,
    /// `interactive`, or the number.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Batch => f.write_str("batch"),
            Self::Extern => f.write_str("extern"),
            Self::Interactive => f.write_str("interactive"),
            Self::Index(n) => write!(f, "{n}"),
        }
    }
}

/// One job step a sampler should report on, as discovered by the engine.
///
/// Samplers receive `&[JobInfo]` rather than scanning themselves, so the cgroup
/// hierarchy is walked once per tick no matter how many plugins are loaded, and
/// every plugin agrees on which jobs exist.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct JobInfo {
    /// Slurm job id.
    pub job_id: u32,
    /// Which step within the job.
    pub step: StepId,
    /// Owner of the job, for attributing usage to a user without a lookup.
    pub uid: u32,
    /// Absolute path to the step's cgroup directory, e.g.
    /// `/sys/fs/cgroup/system.slice/slurmstepd.scope/job_1234/step_0`. Read
    /// `cpu.stat`, `memory.current`, and friends from here.
    pub cgroup: PathBuf,
}

impl JobInfo {
    /// A job step.
    #[must_use]
    pub fn new(job_id: u32, step: StepId, uid: u32, cgroup: impl Into<PathBuf>) -> Self {
        Self {
            job_id,
            step,
            uid,
            cgroup: cgroup.into(),
        }
    }

    /// Identity of this step, for use as a map key.
    #[must_use]
    pub const fn key(&self) -> (u32, StepId) {
        (self.job_id, self.step)
    }

    /// Path to a file inside this step's cgroup.
    ///
    /// ```
    /// use cs_api::{JobInfo, StepId};
    ///
    /// let job = JobInfo::new(1234, StepId::Index(0), 1000, "/sys/fs/cgroup/job_1234/step_0");
    /// assert!(job.cgroup_file("cpu.stat").ends_with("step_0/cpu.stat"));
    /// ```
    #[must_use]
    pub fn cgroup_file(&self, name: impl AsRef<Path>) -> PathBuf {
        self.cgroup.join(name)
    }
}

impl fmt::Display for JobInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.job_id, self.step)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_dir_names_round_trip() {
        for step in [
            StepId::Batch,
            StepId::Extern,
            StepId::Interactive,
            StepId::Index(0),
            StepId::Index(7),
            StepId::Index(u32::MAX),
        ] {
            let dir = step.dir_name();
            assert_eq!(StepId::parse_dir(&dir), Some(step), "{dir}");
        }
    }

    #[test]
    fn step_dir_names_match_slurms_layout() {
        assert_eq!(StepId::Batch.dir_name(), "step_batch");
        assert_eq!(StepId::Extern.dir_name(), "step_extern");
        assert_eq!(StepId::Interactive.dir_name(), "step_interactive");
        assert_eq!(StepId::Index(3).dir_name(), "step_3");
    }

    #[test]
    fn unparseable_step_dirs_are_rejected() {
        for bad in [
            "",
            "step_",
            "job_1234",
            "step_batch2",
            "step_-1",
            "step_+0",
            "step_007",
            "step_99999999999999999999",
            "cpu.stat",
        ] {
            assert_eq!(StepId::parse_dir(bad), None, "{bad} should not parse");
        }
    }

    #[test]
    fn jobs_are_displayed_as_job_dot_step() {
        let job = JobInfo::new(1234, StepId::Index(0), 1000, "/sys/fs/cgroup/x");
        assert_eq!(job.to_string(), "1234.0");
        assert_eq!(
            JobInfo::new(9, StepId::Batch, 0, "/x").to_string(),
            "9.batch"
        );
    }

    #[test]
    fn key_identifies_a_step_within_a_job() {
        let a = JobInfo::new(1, StepId::Index(0), 1000, "/a");
        let b = JobInfo::new(1, StepId::Index(1), 1000, "/b");
        assert_ne!(a.key(), b.key());
        assert_eq!(a.key(), (1, StepId::Index(0)));
    }

    #[test]
    fn cgroup_file_joins_onto_the_step_directory() {
        let job = JobInfo::new(
            1234,
            StepId::Batch,
            1000,
            "/sys/fs/cgroup/job_1234/step_batch",
        );
        assert_eq!(
            job.cgroup_file("memory.current"),
            PathBuf::from("/sys/fs/cgroup/job_1234/step_batch/memory.current")
        );
    }
}
