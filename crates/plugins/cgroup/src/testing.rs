//! A fake cgroup tree on disk.
//!
//! Enabled by the `test-util` feature, and used by this crate's own tests. The
//! parsers read files, so the honest way to test them is with files — a mocked
//! filesystem would test the mock.

// Scoped to this module, not the crate: the rest is real library code and obeys
// the workspace ban. A fixture that cannot make a directory has nothing useful to
// return, and saying so on the spot beats threading a Result through every test.
#![allow(
    clippy::expect_used,
    reason = "a test fixture reports a broken assumption by panicking"
)]

use std::fs;
use std::path::{Path, PathBuf};

use cs_api::{JobInfo, StepId};

use crate::CgroupJobs;

/// A throwaway `slurmstepd.scope` directory.
///
/// Cleaned up when dropped.
#[derive(Debug)]
pub struct FakeCgroups {
    dir: tempfile::TempDir,
}

impl FakeCgroups {
    /// An empty scope directory.
    ///
    /// # Panics
    ///
    /// If a temporary directory cannot be made, which in a test is fatal anyway.
    #[must_use]
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("a temporary directory"),
        }
    }

    /// The root to point [`CgroupJobs`] at.
    #[must_use]
    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    /// A job source that will find what this fixture holds.
    ///
    /// The uid is fixed, because a test cannot `chown` to another user.
    #[must_use]
    pub fn jobs(&self) -> CgroupJobs {
        CgroupJobs::under(self.root())
    }

    /// Create `job_<id>/step_<step>` and return what discovery would report for it.
    ///
    /// Creating the directory is the point, so the return value is often ignored.
    ///
    /// # Panics
    ///
    /// If the directory cannot be created.
    pub fn add_job(&self, job_id: u32, step: StepId, uid: u32) -> JobInfo {
        let path = self
            .root()
            .join(format!("job_{job_id}"))
            .join(step.dir_name());
        fs::create_dir_all(&path).expect("create the step directory");
        JobInfo::new(job_id, step, uid, path)
    }

    /// Create a directory that is not a job step, to check it is ignored.
    ///
    /// # Panics
    ///
    /// If the directory cannot be created.
    pub fn add_raw_dir(&self, relative: &str) {
        fs::create_dir_all(self.root().join(relative)).expect("create the directory");
    }

    /// Write one control file into a job's cgroup.
    ///
    /// # Panics
    ///
    /// If the file cannot be written.
    pub fn write(&self, job: &JobInfo, name: &str, contents: &str) {
        fs::create_dir_all(&job.cgroup).expect("the cgroup directory");
        fs::write(job.cgroup_file(name), contents).expect("write the control file");
    }

    /// Fill in what a busy job on a recent kernel looks like.
    ///
    /// The values are arbitrary but fixed, so a test can assert on them.
    pub fn write_typical(&self, job: &JobInfo) {
        self.write(
            job,
            "cpu.stat",
            "usage_usec 123456\nuser_usec 100000\nsystem_usec 23456\n\
             nr_periods 10\nnr_throttled 2\nthrottled_usec 500\n",
        );
        self.write(job, "memory.current", "8388608\n");
        self.write(job, "memory.peak", "9000000\n");
        self.write(
            job,
            "memory.stat",
            "anon 4096\nfile 8192\nkernel_stack 0\nslab 128\n",
        );
        self.write(
            job,
            "memory.events",
            "low 0\nhigh 0\nmax 3\noom 0\noom_kill 0\n",
        );
        self.write(
            job,
            "io.stat",
            "8:0 rbytes=1024 wbytes=2048 rios=4 wios=8 dbytes=0 dios=0\n\
             8:16 rbytes=512 wbytes=0 rios=2 wios=0 dbytes=0 dios=0\n",
        );
        self.write(job, "pids.current", "17\n");
        self.write(job, "cpu.pressure", "some avg10=0.00 avg60=0.00 total=42\n");
        self.write(
            job,
            "memory.pressure",
            "some avg10=0.00 avg60=0.00 total=123456\nfull avg10=0.00 total=7890\n",
        );
        self.write(
            job,
            "io.pressure",
            "some avg10=0.00 total=11\nfull avg10=0.00 total=5\n",
        );
    }

    /// Advance a job's cumulative counters, as the kernel would between samples.
    ///
    /// # Panics
    ///
    /// If the file cannot be written.
    pub fn advance_cpu(&self, job: &JobInfo, usage_usec: u64) {
        self.write(
            job,
            "cpu.stat",
            &format!(
                "usage_usec {usage_usec}\nuser_usec {}\nsystem_usec {}\n\
                 nr_periods 10\nnr_throttled 2\nthrottled_usec 500\n",
                usage_usec * 4 / 5,
                usage_usec / 5,
            ),
        );
    }

    /// Set the memory reading, as the kernel would between samples.
    ///
    /// # Panics
    ///
    /// If the file cannot be written.
    pub fn set_memory(&self, job: &JobInfo, bytes: u64) {
        self.write(job, "memory.current", &format!("{bytes}\n"));
    }

    /// Remove a job's cgroup, as slurmstepd does the moment the step ends.
    ///
    /// # Panics
    ///
    /// If the directory exists and cannot be removed.
    pub fn remove_job(&self, job: &JobInfo) {
        if job.cgroup.exists() {
            fs::remove_dir_all(&job.cgroup).expect("remove the step directory");
        }
    }

    /// The path of a job's directory, for a test that wants to poke at it.
    #[must_use]
    pub fn path_of(&self, job: &JobInfo) -> PathBuf {
        job.cgroup.clone()
    }
}

impl Default for FakeCgroups {
    fn default() -> Self {
        Self::new()
    }
}
