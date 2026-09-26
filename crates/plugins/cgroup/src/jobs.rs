//! Finding the jobs on a node.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use cs_api::{Error, ErrorKind, JobInfo, JobSource, Result, StepId};

/// Where Slurm puts job cgroups under a default systemd setup.
pub const DEFAULT_ROOT: &str = "/sys/fs/cgroup/system.slice/slurmstepd.scope";

/// Walks the cgroup tree to find job steps.
///
/// The layout is `<root>/job_<id>/step_<step>`, so discovery is two levels of
/// `readdir` and no more: no `/proc` scan, no Slurm RPC, nothing that needs
/// privileges beyond reading the hierarchy the agent is already in.
///
/// One of these serves every sampler — the engine calls it once per tick and hands
/// the same list to all of them, so ten plugins do not mean ten walks.
///
/// ```no_run
/// use cs_plugin_cgroup::CgroupJobs;
///
/// // What an agent uses.
/// let jobs = CgroupJobs::new();
///
/// // What a test uses.
/// let fixture = CgroupJobs::under("/tmp/fake-cgroups");
/// # let _ = (jobs, fixture);
/// ```
#[derive(Clone, Debug)]
pub struct CgroupJobs {
    root: PathBuf,
    uid_of: UidSource,
}

/// How a job's owner is determined.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum UidSource {
    /// From the owner of the job's cgroup directory, which slurmstepd chowns to
    /// the job's user so the user can read its own accounting.
    DirectoryOwner,
    /// A fixed value, for a test or a site where the above does not hold.
    Fixed(u32),
}

impl CgroupJobs {
    /// Discovery under the standard Slurm location.
    #[must_use]
    pub fn new() -> Self {
        Self::under(DEFAULT_ROOT)
    }

    /// Discovery under some other root — a test fixture, or a site with a
    /// non-default cgroup layout.
    #[must_use]
    pub fn under(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            uid_of: UidSource::DirectoryOwner,
        }
    }

    /// Report every job as owned by `uid` instead of asking the filesystem.
    #[must_use]
    pub const fn with_fixed_uid(mut self, uid: u32) -> Self {
        self.uid_of = UidSource::Fixed(uid);
        self
    }

    /// Where this is looking.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Every step of every job, as one flat list.
    fn scan(&self) -> Result<Vec<JobInfo>> {
        let mut found = Vec::new();

        for job_dir in list_dir(&self.root)? {
            let Some(job_id) = job_id_of(&job_dir) else {
                continue;
            };
            // A job with no step directories is mid-setup or mid-teardown. Not an
            // error, and nothing to sample yet.
            for step_dir in list_dir(&job_dir)? {
                let Some(step) = step_dir
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(StepId::parse_dir)
                else {
                    continue;
                };
                let uid = self.uid_of(&step_dir);
                found.push(JobInfo::new(job_id, step, uid, step_dir));
            }
        }

        // Sorted so the list is stable between ticks: readdir order is not, and a
        // sampler comparing this tick to the last should not see churn that is not
        // there.
        found.sort_unstable_by_key(JobInfo::key);
        Ok(found)
    }

    fn uid_of(&self, dir: &Path) -> u32 {
        match self.uid_of {
            UidSource::Fixed(uid) => uid,
            UidSource::DirectoryOwner => owner_of(dir).unwrap_or(0),
        }
    }
}

impl Default for CgroupJobs {
    fn default() -> Self {
        Self::new()
    }
}

impl JobSource for CgroupJobs {
    fn jobs(&self) -> Result<Vec<JobInfo>> {
        self.scan()
    }
}

/// Directory entries that are themselves directories.
///
/// A root that does not exist is an empty list, not an error: a node with no jobs
/// may not have the scope directory at all, and an agent must not spend its life
/// complaining about that.
fn list_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(Error::with_source(
                ErrorKind::Io,
                format!("cannot list {}", dir.display()),
                err,
            ));
        }
    };

    let mut dirs = Vec::new();
    for entry in entries {
        // One unreadable entry must not lose the whole node's job list: a cgroup
        // can vanish between `read_dir` and `stat`.
        let Ok(entry) = entry else { continue };
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            dirs.push(entry.path());
        }
    }
    dirs.sort_unstable();
    Ok(dirs)
}

/// `job_1234` → `1234`.
fn job_id_of(dir: &Path) -> Option<u32> {
    dir.file_name()?
        .to_str()?
        .strip_prefix("job_")?
        .parse()
        .ok()
}

/// The uid owning a directory.
#[cfg(unix)]
fn owner_of(dir: &Path) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(dir).ok().map(|meta| meta.uid())
}

#[cfg(not(unix))]
fn owner_of(_dir: &Path) -> Option<u32> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeCgroups;

    #[test]
    fn every_step_of_every_job_is_found() {
        let tree = FakeCgroups::new();
        tree.add_job(1234, StepId::Batch, 1000);
        tree.add_job(1234, StepId::Index(0), 1000);
        tree.add_job(1234, StepId::Extern, 1000);
        tree.add_job(99, StepId::Interactive, 1001);

        let found = tree.jobs().jobs().expect("scan");
        let described: Vec<String> = found.iter().map(ToString::to_string).collect();
        // Ordered by job id, then by `StepId`'s own ordering — which is the order
        // its variants are declared in, not how they print. Stable is the promise;
        // alphabetical was never one.
        assert_eq!(
            described,
            ["99.interactive", "1234.batch", "1234.extern", "1234.0"],
            "every step, in a stable order"
        );
        assert!(found[0].cgroup.ends_with("job_99/step_interactive"));
    }

    #[test]
    fn the_order_is_stable_between_scans() {
        let tree = FakeCgroups::new();
        for id in [7u32, 3, 11, 5] {
            tree.add_job(id, StepId::Batch, 1000);
        }
        let jobs = tree.jobs();
        let first: Vec<_> = jobs
            .jobs()
            .expect("scan")
            .iter()
            .map(JobInfo::key)
            .collect();
        let second: Vec<_> = jobs
            .jobs()
            .expect("scan")
            .iter()
            .map(JobInfo::key)
            .collect();
        assert_eq!(first, second);
        assert_eq!(
            first.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            [3, 5, 7, 11],
            "sorted, because readdir order is not"
        );
    }

    #[test]
    fn a_node_with_no_jobs_reports_none_rather_than_failing() {
        let tree = FakeCgroups::new();
        assert!(tree.jobs().jobs().expect("scan").is_empty());
    }

    #[test]
    fn a_missing_root_is_an_empty_list_not_an_error() {
        // A node that has never run a job may not have the scope directory.
        let jobs = CgroupJobs::under("/nonexistent/slurmstepd.scope");
        assert!(jobs.jobs().expect("a missing root is ordinary").is_empty());
    }

    #[test]
    fn directories_that_are_not_jobs_are_ignored() {
        let tree = FakeCgroups::new();
        tree.add_job(1234, StepId::Batch, 1000);
        // Things a real scope directory contains alongside jobs.
        tree.add_raw_dir("cgroup.procs.d");
        tree.add_raw_dir("job_notanumber");
        tree.add_raw_dir("job_1234/notastep");
        tree.add_raw_dir("job_1234/step_");

        let found = tree.jobs().jobs().expect("scan");
        assert_eq!(found.len(), 1, "only the real step: {found:?}");
        assert_eq!(found[0].step, StepId::Batch);
    }

    #[test]
    fn a_job_still_being_set_up_has_no_steps_and_is_skipped() {
        let tree = FakeCgroups::new();
        tree.add_raw_dir("job_5000");
        tree.add_job(1234, StepId::Batch, 1000);

        let found = tree.jobs().jobs().expect("scan");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].job_id, 1234);
    }

    #[test]
    fn a_fixed_uid_overrides_the_directory_owner() {
        let tree = FakeCgroups::new();
        tree.add_job(1234, StepId::Batch, 1000);
        let found = CgroupJobs::under(tree.root())
            .with_fixed_uid(4242)
            .jobs()
            .expect("scan");
        assert_eq!(found[0].uid, 4242);
    }

    #[test]
    fn the_owner_of_the_step_directory_is_the_jobs_user() {
        // The fixture cannot chown, so this only checks that *something* plausible
        // is reported — on a real node it is the job's user.
        let tree = FakeCgroups::new();
        tree.add_job(1234, StepId::Batch, 1000);
        let found = CgroupJobs::under(tree.root()).jobs().expect("scan");
        assert_eq!(
            found[0].uid,
            owner_of(&found[0].cgroup).expect("the fixture dir exists")
        );
    }
}
