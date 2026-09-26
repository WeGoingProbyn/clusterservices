//! The parsers against genuine kernel output.
//!
//! Every other test in this crate reads files this crate wrote, which proves the
//! parsers handle what I *think* the kernel produces. This one points them at the
//! test process's own cgroup, so it fails if that belief is wrong — real `cpu.stat`
//! has keys my fixture never had (`nice_usec`), real `memory.stat` has forty
//! lines, and real `io.stat` is often absent entirely.
//!
//! Self-skipping: a machine without cgroup v2, or a container without a readable
//! cgroup of its own, is not a failure.

// A test reports a broken assumption by panicking.
#![allow(
    clippy::expect_used,
    reason = "a test helper should fail loudly and name what went wrong"
)]

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use cs_api::{JobInfo, JobSource, Sampler, StepId};
use cs_plugin_cgroup::{CgroupConfig, CgroupJobs, CgroupSampler};

/// This process's own cgroup directory, if it has a readable one.
fn own_cgroup() -> Option<PathBuf> {
    // cgroup v2 puts the unified hierarchy on the `0::` line.
    let relative = fs::read_to_string("/proc/self/cgroup")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("0::").map(str::to_owned))?;
    let dir = PathBuf::from(format!("/sys/fs/cgroup{relative}"));
    // A cgroup without cpu.stat has the controller disabled, which tells us
    // nothing either way.
    dir.join("cpu.stat").is_file().then_some(dir)
}

#[test]
fn the_parsers_read_this_machines_real_cgroup() {
    let Some(dir) = own_cgroup() else {
        eprintln!("skipping: no readable cgroup v2 directory for this process");
        return;
    };

    // Pretend our own cgroup is a job step; the sampler does not care how the
    // directory came to exist.
    let job = JobInfo::new(1, StepId::Batch, 1000, &dir);
    let mut sampler = CgroupSampler::with_config(
        CgroupConfig::new()
            .every(Duration::from_millis(1))
            .batching_for(Duration::from_millis(1)),
    );

    // Twice: a window opens on the first sample, so it takes a second tick for any
    // time to have elapsed within it.
    let mut batches = Vec::new();
    for _ in 0..2 {
        batches.extend(
            sampler
                .sample(std::slice::from_ref(&job))
                .unwrap_or_else(|err| {
                    panic!(
                        "real kernel output at {} did not parse: {err:?}",
                        dir.display()
                    )
                }),
        );
    }
    let batch = batches.first().expect("the window should have closed");
    assert_eq!(batch.sampled_unix_ms.len(), 2);

    // cpu.stat and memory.current exist on any cgroup v2 kernel with the
    // controllers enabled, and a process that has run at all has used some CPU.
    let usage = batch
        .cpu_usage_usec
        .first()
        .copied()
        .expect("cpu.stat should have been readable");
    assert!(usage > 0, "this process has used some CPU");
    assert!(
        batch.memory_current.first().copied().unwrap_or(0) > 0,
        "memory.current should be non-zero for a live cgroup"
    );

    // Whatever this kernel does not expose must arrive as an empty column, never as
    // a zero. This is the assertion a fixture cannot make honestly, because a
    // fixture only omits what I remembered to omit.
    for (name, column) in [
        ("memory_peak", &batch.memory_peak),
        ("io_rbytes", &batch.io_rbytes),
        ("cpu_pressure_some_usec", &batch.cpu_pressure_some_usec),
    ] {
        assert!(
            column.is_empty() || column.len() == batch.sampled_unix_ms.len(),
            "{name} is neither absent nor aligned: {column:?}"
        );
    }

    eprintln!(
        "read {}: cpu {usage}us, memory {}B, peak {}, io {}, psi {}",
        dir.display(),
        batch.memory_current.first().copied().unwrap_or(0),
        presence(&batch.memory_peak),
        presence(&batch.io_rbytes),
        presence(&batch.cpu_pressure_some_usec),
    );
}

#[test]
fn discovery_survives_a_real_cgroup_hierarchy() {
    if !PathBuf::from("/sys/fs/cgroup").is_dir() {
        eprintln!("skipping: no /sys/fs/cgroup");
        return;
    }
    // Pointed at the real root, which has no `job_*` directories — so this checks
    // the walk copes with a large real tree and reports nothing, rather than
    // tripping over something it did not expect.
    let found = CgroupJobs::under("/sys/fs/cgroup")
        .jobs()
        .expect("a real hierarchy should scan cleanly");
    assert!(
        found.is_empty(),
        "this machine is not running Slurm jobs, so nothing should match: {found:?}"
    );
}

fn presence(column: &[u64]) -> &'static str {
    if column.is_empty() {
        "absent"
    } else {
        "present"
    }
}
