//! Per-job CPU, memory, I/O and pressure from cgroup v2.
//!
//! The reason this project exists, in one crate: on a node running six jobs, the
//! kernel already knows exactly what each one used — it is in
//! `/sys/fs/cgroup/.../job_<id>/step_<n>/` — and nothing was collecting it per
//! job. This reads it, batches it, and hands it to the engine.
//!
//! ```no_run
//! use cs_engine::NodeEngine;
//! use cs_plugin_cgroup::{CgroupJobs, CgroupSampler};
//!
//! # fn example<T: cs_transport::Transport>(transport: T) -> cs_util::Result<()> {
//! let engine = NodeEngine::builder(transport)
//!     .node("node-0042")
//!     // Finds the jobs, once per tick, for every sampler on the agent.
//!     .jobs(CgroupJobs::new())
//!     .sampler(CgroupSampler::factory())
//!     .dial("tcp://head01:7777".parse()?)
//!     .build()?;
//! # let _ = engine;
//! # Ok(())
//! # }
//! ```
//!
//! # What it sends
//!
//! [`CgroupBatch`](proto::CgroupBatch), columnar: one timestamp array and one array
//! per metric, fifteen samples to a message at the defaults. Counters are
//! cumulative, exactly as the kernel reports them, and the server computes rates —
//! so a missed batch costs resolution but never correctness. Memory is the
//! exception and cannot be otherwise: it is a gauge, which is why `memory.peak` is
//! carried alongside it.
//!
//! **An empty series means the metric is not available**, not that it is zero.
//! `memory.peak` needs Linux 5.19, pressure needs `CONFIG_PSI`, and `cpu.stat`
//! reports throttling only once a limit is set — a fleet is rarely uniform, and
//! flattening "absent" to zero would quietly invent data.
//!
//! # What it does about jobs ending
//!
//! A cgroup disappears the moment its step ends, and with the defaults a job's last
//! forty-five seconds would go with it. So each tick notices which jobs have gone
//! and flushes their partial batches first. For a job that ran for ten seconds that
//! flush is the entire record of it.

mod jobs;
mod read;
mod sample;
mod sampler;

#[cfg(any(test, feature = "test-util"))]
pub mod testing;

mod metrics;

/// The wire schema, generated from `proto/cgroup.proto`.
pub mod proto {
    #![allow(missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/cs.cgroup.v1.rs"));
}

pub use jobs::{CgroupJobs, DEFAULT_ROOT};
pub use proto::{CgroupBatch, JobRef};
pub use sampler::{CgroupConfig, CgroupSampler, CgroupService};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeCgroups;
    use cs_api::test_support::FakeEngine;
    use cs_api::{Command, CommandReceiver, JobInfo, Reply, Sampler, ServiceDef, StepId};
    use std::time::Duration;

    /// Every sample the sampler would take, driven by hand so no test waits.
    fn sample_n(sampler: &mut CgroupSampler, jobs: &[JobInfo], ticks: usize) -> Vec<CgroupBatch> {
        let mut batches = Vec::new();
        for _ in 0..ticks {
            batches.extend(sampler.sample(jobs).expect("sample"));
        }
        batches
    }

    fn config() -> CgroupConfig {
        // Three samples to a batch, so a test can see two batches quickly.
        CgroupConfig::new()
            .every(Duration::from_secs(1))
            .batching_for(Duration::from_secs(3))
    }

    #[test]
    fn a_batch_carries_one_column_per_metric_and_one_timestamp_each() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Batch, 1000);
        tree.write_typical(&job);

        let mut sampler = CgroupSampler::with_config(config());
        let batches = sample_n(&mut sampler, std::slice::from_ref(&job), 4);

        assert_eq!(batches.len(), 1, "one window closed");
        let batch = &batches[0];
        let reference = batch.job.as_ref().expect("the job is named");
        assert_eq!(reference.job_id, 1234);
        assert_eq!(reference.step, "batch");
        assert_eq!(reference.uid, 1000);

        let samples = batch.sampled_unix_ms.len();
        assert!(samples >= 3, "got {samples} samples");
        for (name, column) in [
            ("cpu_usage_usec", &batch.cpu_usage_usec),
            ("memory_current", &batch.memory_current),
            ("io_rbytes", &batch.io_rbytes),
            ("pids_current", &batch.pids_current),
            (
                "memory_pressure_full_usec",
                &batch.memory_pressure_full_usec,
            ),
        ] {
            assert_eq!(
                column.len(),
                samples,
                "{name} should have one value per timestamp"
            );
        }
        assert!(
            batch
                .sampled_unix_ms
                .windows(2)
                .all(|pair| pair[0] <= pair[1])
        );
    }

    #[test]
    fn counters_are_sent_as_the_kernel_reports_them() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Batch, 1000);
        tree.write_typical(&job);

        let mut sampler = CgroupSampler::with_config(config());
        // The kernel's counter climbs between samples; the agent must not turn it
        // into a rate.
        tree.advance_cpu(&job, 1000);
        sampler.sample(std::slice::from_ref(&job)).expect("sample");
        tree.advance_cpu(&job, 2500);
        sampler.sample(std::slice::from_ref(&job)).expect("sample");
        tree.advance_cpu(&job, 9000);
        let batches = sample_n(&mut sampler, std::slice::from_ref(&job), 2);

        let batch = batches.first().expect("a batch");
        assert_eq!(
            &batch.cpu_usage_usec[..3],
            &[1000, 2500, 9000],
            "absolute counters, for the server to differentiate"
        );
    }

    #[test]
    fn a_metric_this_kernel_lacks_is_an_empty_column_not_zeroes() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Index(0), 1000);
        // An older kernel: no memory.peak, no pressure.
        tree.write(&job, "cpu.stat", "usage_usec 500\n");
        tree.write(&job, "memory.current", "1024\n");

        let mut sampler = CgroupSampler::with_config(config());
        let batches = sample_n(&mut sampler, std::slice::from_ref(&job), 4);
        let batch = batches.first().expect("a batch");

        assert!(!batch.cpu_usage_usec.is_empty());
        assert!(!batch.memory_current.is_empty());
        assert!(
            batch.memory_peak.is_empty(),
            "absent must not arrive as zero"
        );
        assert!(batch.memory_pressure_full_usec.is_empty());
        assert!(batch.io_rbytes.is_empty());
    }

    #[test]
    fn a_job_that_ends_has_its_partial_batch_flushed_at_once() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Batch, 1000);
        tree.write_typical(&job);

        // A short job: two samples, nowhere near the window.
        let mut sampler = CgroupSampler::with_config(config());
        assert!(
            sampler
                .sample(std::slice::from_ref(&job))
                .expect("sample")
                .is_empty()
        );
        assert!(
            sampler
                .sample(std::slice::from_ref(&job))
                .expect("sample")
                .is_empty()
        );

        // It ends, so the engine's next job list no longer has it.
        tree.remove_job(&job);
        let batches = sampler.sample(&[]).expect("sample");

        assert_eq!(batches.len(), 1, "the last seconds of a short job");
        assert_eq!(batches[0].sampled_unix_ms.len(), 2);
        assert_eq!(
            batches[0].job.as_ref().expect("job").job_id,
            1234,
            "and it still knows whose they were"
        );
    }

    #[test]
    fn shutdown_flushes_every_partial_batch() {
        let tree = FakeCgroups::new();
        let first = tree.add_job(1, StepId::Batch, 1000);
        let second = tree.add_job(2, StepId::Index(0), 1001);
        tree.write_typical(&first);
        tree.write_typical(&second);
        let jobs = [first, second];

        let mut sampler = CgroupSampler::with_config(config());
        assert!(sampler.sample(&jobs).expect("sample").is_empty());

        let batches = sampler.on_shutdown(&jobs).expect("flush");
        assert_eq!(batches.len(), 2, "one per job, not one per node");
        let mut ids: Vec<u32> = batches
            .iter()
            .map(|batch| batch.job.as_ref().expect("job").job_id)
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, [1, 2]);
    }

    #[test]
    fn every_step_of_a_job_is_batched_separately() {
        let tree = FakeCgroups::new();
        let batch_step = tree.add_job(1234, StepId::Batch, 1000);
        let srun_step = tree.add_job(1234, StepId::Index(0), 1000);
        tree.write_typical(&batch_step);
        tree.write_typical(&srun_step);
        tree.set_memory(&srun_step, 999);
        let jobs = [batch_step, srun_step];

        let mut sampler = CgroupSampler::with_config(config());
        sampler.sample(&jobs).expect("sample");
        let batches = sampler.on_shutdown(&jobs).expect("flush");

        assert_eq!(batches.len(), 2);
        let steps: std::collections::HashSet<String> = batches
            .iter()
            .map(|batch| batch.job.as_ref().expect("job").step.clone())
            .collect();
        assert_eq!(
            steps,
            ["batch".to_owned(), "0".to_owned()].into_iter().collect(),
            "a job's batch script and its srun are different workloads"
        );
    }

    #[test]
    fn one_unreadable_job_does_not_cost_the_others_their_samples() {
        let tree = FakeCgroups::new();
        let good = tree.add_job(1, StepId::Batch, 1000);
        let bad = tree.add_job(2, StepId::Batch, 1000);
        tree.write_typical(&good);
        tree.write_typical(&bad);
        tree.write(&bad, "cpu.stat", "usage_usec what\n");
        let jobs = [good, bad];

        let mut sampler = CgroupSampler::with_config(config());
        // No batch is ready yet, so the failure is what surfaces.
        let err = sampler.sample(&jobs).unwrap_err();
        assert_eq!(err.kind(), cs_api::ErrorKind::Decode);

        // But the good job was still sampled, and its data comes out.
        let batches = sampler.on_shutdown(&jobs).expect("flush");
        assert_eq!(batches.len(), 1, "the readable job's data survives");
        assert_eq!(batches[0].job.as_ref().expect("job").job_id, 1);
    }

    #[test]
    fn a_job_that_vanishes_mid_tick_is_skipped_rather_than_recorded_empty() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Batch, 1000);
        // Discovered, then gone before the read — the race a busy node runs
        // constantly.
        tree.remove_job(&job);

        let mut sampler = CgroupSampler::with_config(config());
        assert!(
            sampler
                .sample(std::slice::from_ref(&job))
                .expect("sample")
                .is_empty()
        );
        assert!(
            sampler.on_shutdown(&[job]).expect("flush").is_empty(),
            "nothing was read, so there is nothing to send"
        );
    }

    #[test]
    fn a_command_retunes_the_interval_and_zero_restores_the_default() {
        let mut sampler = CgroupSampler::with_config(config());
        assert_eq!(sampler.interval(), Duration::from_secs(1));

        assert_eq!(sampler.on_command(Command::Custom(250)), Reply::Handled);
        assert_eq!(sampler.interval(), Duration::from_millis(250));

        assert_eq!(sampler.on_command(Command::Custom(0)), Reply::Handled);
        assert_eq!(
            sampler.interval(),
            Duration::from_secs(1),
            "zero means the configured default"
        );

        // Built-ins are the engine's business.
        assert_eq!(sampler.on_command(Command::Restart), Reply::Default);
    }

    #[test]
    fn the_number_of_accumulating_jobs_is_bounded() {
        let tree = FakeCgroups::new();
        let mut jobs = Vec::new();
        for id in 0..10u32 {
            let job = tree.add_job(id, StepId::Batch, 1000);
            tree.write_typical(&job);
            jobs.push(job);
        }

        let mut sampler = CgroupSampler::with_config(CgroupConfig {
            max_jobs: 4,
            ..config()
        });
        sampler.sample(&jobs).expect("sample");
        assert_eq!(sampler.dropped_samples(), 6, "the six that did not fit");

        let batches = sampler.on_shutdown(&jobs).expect("flush");
        assert_eq!(batches.len(), 4, "held to the cap");
        // Counted per tick: `on_shutdown` reads again and drops the same six, which
        // is what makes the counter show ongoing pressure rather than plateauing.
        assert_eq!(sampler.dropped_samples(), 12);
    }

    #[test]
    fn a_batch_is_capped_by_sample_count_as_well_as_by_time() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Batch, 1000);
        tree.write_typical(&job);

        let mut sampler = CgroupSampler::with_config(CgroupConfig {
            max_samples_per_batch: 2,
            // A window long enough that only the count can close a batch.
            ..CgroupConfig::new()
                .every(Duration::from_millis(1))
                .batching_for(Duration::from_secs(3600))
        });
        let batches = sample_n(&mut sampler, &[job], 6);
        assert_eq!(batches.len(), 3, "closed by count, not by the clock");
        assert!(batches.iter().all(|batch| batch.sampled_unix_ms.len() == 2));
    }

    /// The whole plugin against the fake engine: no transport, no server, no
    /// runtime — just "do these jobs produce these counters".
    #[test]
    fn the_sampler_works_against_the_fake_engine() {
        let tree = FakeCgroups::new();
        let job = tree.add_job(1234, StepId::Batch, 1000);
        tree.write_typical(&job);

        let engine = FakeEngine::new("node-0042");
        let ctx = engine.ctx::<CgroupService>();
        let mut sampler = CgroupSampler::with_config(config());
        sampler.start(&ctx).expect("start");

        for batch in sample_n(&mut sampler, std::slice::from_ref(&job), 4) {
            ctx.send(batch).expect("send");
        }

        let sent = engine
            .sent::<CgroupService>()
            .expect("decode what was sent");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].job.as_ref().expect("job").job_id, 1234);
        assert!(!sent[0].cpu_usage_usec.is_empty());
        // And it really went through the wire encoding.
        assert_eq!(engine.sent_raw()[0].0.name, "cgroup");
    }

    #[test]
    fn the_service_name_is_short_enough_for_a_thread_name() {
        assert_eq!(CgroupService::NAME, "cgroup");
        assert_eq!(
            cs_api::worker_thread_name(CgroupService::NAME, "sample"),
            "cgroup/sample",
            "per-plugin CPU is attributed by this name"
        );
    }
}
