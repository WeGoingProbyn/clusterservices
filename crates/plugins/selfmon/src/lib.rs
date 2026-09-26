//! The agent watching itself.
//!
//! A monitoring agent has to be accountable for its own overhead. This one reports
//! three things a cluster operator will eventually want to argue about:
//!
//! - **Is it keeping up?** The engine's counters — queue depths, reconnects, and
//!   above all `data_dropped`, which is non-zero exactly when this agent threw away
//!   metrics it had already collected.
//! - **What does it cost?** This process's CPU and resident memory, from
//!   `/proc/self`.
//! - **Which plugin costs it?** CPU per thread name. This is the payoff for naming
//!   worker threads `<service>/<worker>`: the kernel accounts CPU per thread, so
//!   that convention is what makes "the gpu plugin is eating a core" answerable
//!   rather than a guess.
//!
//! ```no_run
//! use cs_engine::NodeEngine;
//! use cs_plugin_selfmon::SelfmonSampler;
//!
//! # fn example<T: cs_transport::Transport>(transport: T) -> cs_util::Result<()> {
//! let engine = NodeEngine::builder(transport)
//!     .node("node-0042")
//!     .sampler(SelfmonSampler::factory())
//!     .dial("tcp://head01:7777".parse()?)
//!     .build()?;
//! # let _ = engine;
//! # Ok(())
//! # }
//! ```
//!
//! # It is an ordinary plugin
//!
//! Deliberately: no special path into the engine, no privileged hook. It reaches
//! the counters through [`ServiceCtx::engine_stats`](cs_api::ServiceCtx::engine_stats)
//! like any plugin could, and it is batched, chunked, queued, and dropped under
//! pressure on exactly the same terms as the metrics it reports on. Self-monitoring
//! that took a shortcut would be measuring a different system from the one running.
//!
//! # Two numbers to read carefully
//!
//! **Thread CPU is summed by name, not by thread id.** A tid means nothing to a
//! server. The consequence is that a rebuilt service gets a fresh thread whose
//! counter starts at zero, so a thread's CPU can step *down* across a restart — a
//! decrease there means a restart, not a bad reading, and `panics`/`restarts` in the
//! same batch say which.
//!
//! **CPU time assumes `USER_HZ` is 100.** Reading it properly needs
//! `sysconf(_SC_CLK_TCK)` and therefore libc, and a plugin depends on `cs-api`
//! alone — so it is [`DEFAULT_USER_HZ`], overridable on
//! [`SelfmonConfig`](SelfmonConfig::ticks_per_second), rather than a dependency. It
//! has been 100 on every mainstream Linux build for decades.

mod proc;
mod sampler;

#[cfg(any(test, feature = "test-util"))]
pub mod testing;

/// The wire schema, generated from `proto/selfmon.proto`.
pub mod proto {
    #![allow(missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/cs.selfmon.v1.rs"));
}

pub use proto::{SelfmonBatch, ServiceSeries, ThreadSeries};
pub use sampler::{
    DEFAULT_PROC_SELF, DEFAULT_USER_HZ, SelfmonConfig, SelfmonSampler, SelfmonService,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeProc;
    use cs_api::test_support::FakeEngine;
    use cs_api::{
        Command, CommandReceiver, EngineStats, NoCommand, Reply, Sampler, ServiceDef, ServiceId,
        ServiceStats,
    };
    use std::time::Duration;

    struct Cgroup;
    impl ServiceDef for Cgroup {
        const NAME: &'static str = "cgroup";
        type Data = u64;
        type Command = NoCommand;
    }

    /// Stats that look like a busy agent.
    fn stats() -> EngineStats {
        EngineStats {
            connected: true,
            peers: 1,
            frames_sent: 100,
            bytes_sent: 4096,
            data_queue_depth: 7,
            data_dropped: 3,
            reconnects: 2,
            services: vec![ServiceStats {
                samples: 20,
                sample_errors: 1,
                sample_time_total: Duration::from_millis(40),
                sample_time_max: Duration::from_millis(9),
                panics: 1,
                ..ServiceStats::new(ServiceId::of::<Cgroup>())
            }],
            ..EngineStats::default()
        }
    }

    /// A sampler pointed at a fixture, with a short window.
    fn sampler(proc: &FakeProc) -> SelfmonSampler {
        SelfmonSampler::with_config(
            SelfmonConfig::new()
                .every(Duration::from_secs(1))
                .batching_for(Duration::from_secs(3))
                .reading_proc(proc.path()),
        )
    }

    /// A fixture that looks like a running agent.
    fn running_agent() -> FakeProc {
        let proc = FakeProc::new();
        proc.write_process_cpu(300, 50);
        proc.write_status(51_200, 5);
        proc.add_thread(101, "cs-agent", 5, 1);
        proc.add_thread(102, "cgroup/sample", 200, 20);
        proc.add_thread(103, "selfmon/sample", 2, 0);
        proc.add_thread(104, "tokio-runtime-w", 40, 8);
        proc
    }

    #[test]
    fn a_batch_reports_the_engines_counters_this_process_and_every_thread() {
        let proc = running_agent();
        let engine = FakeEngine::new("node-0042");
        engine.set_engine_stats(stats());
        let ctx = engine.ctx::<SelfmonService>();

        let mut sampler = sampler(&proc);
        sampler.start(&ctx).expect("start");
        let mut batches = Vec::new();
        for _ in 0..4 {
            batches.extend(sampler.sample(&[]).expect("sample"));
        }

        assert_eq!(batches.len(), 1, "one window closed");
        let batch = &batches[0];
        let samples = batch.sampled_unix_ms.len();
        assert!(samples >= 3, "got {samples}");

        // The engine's counters, one value per sample.
        assert_eq!(batch.frames_sent.len(), samples);
        assert!(batch.frames_sent.iter().all(|&value| value == 100));
        assert!(
            batch.data_dropped.iter().all(|&value| value == 3),
            "the number that says metrics were lost"
        );
        assert_eq!(batch.data_queue_depth[0], 7);
        assert_eq!(batch.peers[0], 1);

        // This process.
        assert_eq!(
            batch.process_cpu_usec[0], 3_500_000,
            "350 ticks at 100Hz is 3.5 seconds"
        );
        assert_eq!(batch.process_rss_bytes[0], 51_200 * 1024);
        assert_eq!(batch.process_threads[0], 5);
    }

    #[test]
    fn per_plugin_cpu_is_attributed_by_thread_name() {
        let proc = running_agent();
        let engine = FakeEngine::new("node-0042");
        let ctx = engine.ctx::<SelfmonService>();

        let mut sampler = sampler(&proc);
        sampler.start(&ctx).expect("start");
        let batch = sampler
            .on_shutdown(&[])
            .expect("flush")
            .pop()
            .expect("a batch");

        let by_thread: std::collections::HashMap<&str, &ThreadSeries> = batch
            .threads
            .iter()
            .map(|series| (series.thread.as_str(), series))
            .collect();

        // The whole point: the cgroup plugin's cost, separated from everything else.
        let cgroup = by_thread["cgroup/sample"];
        assert_eq!(cgroup.service, "cgroup");
        assert_eq!(cgroup.cpu_usec[0], 2_200_000, "220 ticks at 100Hz");

        let selfmon = by_thread["selfmon/sample"];
        assert_eq!(selfmon.service, "selfmon");
        assert_eq!(selfmon.cpu_usec[0], 20_000);

        // And the framework's own overhead, visibly not belonging to a plugin.
        assert_eq!(by_thread["tokio-runtime-w"].service, "");
        assert_eq!(by_thread["cs-agent"].service, "");
        assert_eq!(by_thread["tokio-runtime-w"].cpu_usec[0], 480_000);
    }

    #[test]
    fn threads_sharing_a_name_are_summed() {
        let proc = FakeProc::new();
        proc.write_process_cpu(0, 0);
        // Two runtime threads, as a multi-threaded runtime has.
        proc.add_thread(201, "tokio-runtime-w", 10, 0);
        proc.add_thread(202, "tokio-runtime-w", 30, 0);

        let engine = FakeEngine::new("node-0042");
        let ctx = engine.ctx::<SelfmonService>();
        let mut sampler = sampler(&proc);
        sampler.start(&ctx).expect("start");
        let batch = sampler
            .on_shutdown(&[])
            .expect("flush")
            .pop()
            .expect("a batch");

        let runtime = batch
            .threads
            .iter()
            .find(|series| series.thread == "tokio-runtime-w")
            .expect("the runtime threads");
        assert_eq!(
            runtime.cpu_usec[0], 400_000,
            "40 ticks between them, reported once"
        );
        assert_eq!(batch.threads.len(), 1, "one entry per name, not per tid");
    }

    #[test]
    fn a_services_counters_arrive_as_their_own_series() {
        let proc = running_agent();
        let engine = FakeEngine::new("node-0042");
        engine.set_engine_stats(stats());
        let ctx = engine.ctx::<SelfmonService>();

        let mut sampler = sampler(&proc);
        sampler.start(&ctx).expect("start");
        sampler.sample(&[]).expect("sample");
        let batch = sampler
            .on_shutdown(&[])
            .expect("flush")
            .pop()
            .expect("a batch");

        let cgroup = batch
            .services
            .iter()
            .find(|series| series.service == "cgroup")
            .expect("the cgroup service");
        assert_eq!(cgroup.samples[0], 20);
        assert_eq!(cgroup.sample_errors[0], 1);
        assert_eq!(cgroup.panics[0], 1, "a crash is worth reporting home");
        assert_eq!(cgroup.sample_time_total_usec[0], 40_000);
        assert_eq!(
            cgroup.sample_time_max_usec[0], 9_000,
            "the worst case, which an average would hide"
        );
    }

    #[test]
    fn a_service_that_appears_mid_window_still_gets_a_full_length_column() {
        let proc = running_agent();
        let engine = FakeEngine::new("node-0042");
        let ctx = engine.ctx::<SelfmonService>();

        let mut sampler = sampler(&proc);
        sampler.start(&ctx).expect("start");
        // First sample: no services registered yet.
        sampler.sample(&[]).expect("sample");
        // Then one appears — a plugin that was restarted, say.
        engine.set_engine_stats(stats());
        sampler.sample(&[]).expect("sample");

        let batch = sampler
            .on_shutdown(&[])
            .expect("flush")
            .pop()
            .expect("a batch");
        let samples = batch.sampled_unix_ms.len();
        let cgroup = batch
            .services
            .iter()
            .find(|series| series.service == "cgroup")
            .expect("the service that appeared");
        assert_eq!(
            cgroup.samples.len(),
            samples,
            "a short column would not line up with the timestamps"
        );
        assert_eq!(cgroup.samples[0], 0, "zero before it existed");
        assert_eq!(cgroup.samples[1], 20);
    }

    #[test]
    fn a_thread_that_disappears_is_reported_as_zero_for_the_rest_of_the_window() {
        let proc = FakeProc::new();
        proc.write_process_cpu(0, 0);
        proc.add_thread(301, "gpu/sample", 100, 0);

        let engine = FakeEngine::new("node-0042");
        let ctx = engine.ctx::<SelfmonService>();
        let mut sampler = sampler(&proc);
        sampler.start(&ctx).expect("start");
        sampler.sample(&[]).expect("sample");

        // The service is stopped and its thread goes.
        proc.remove_thread(301);
        sampler.sample(&[]).expect("sample");

        let batch = sampler
            .on_shutdown(&[])
            .expect("flush")
            .pop()
            .expect("a batch");
        let gpu = batch
            .threads
            .iter()
            .find(|series| series.thread == "gpu/sample")
            .expect("it was there at the start");
        assert_eq!(gpu.cpu_usec[0], 1_000_000);
        assert_eq!(gpu.cpu_usec[1], 0, "gone, and the column still lines up");
    }

    #[test]
    fn an_unavailable_process_figure_is_an_empty_column() {
        // A proc tree with threads but no status file: unusual, but a container
        // might do it, and it must not fail the sample.
        let proc = FakeProc::new();
        proc.write_process_cpu(10, 0);
        proc.add_thread(401, "cs-agent", 1, 0);

        let engine = FakeEngine::new("node-0042");
        let ctx = engine.ctx::<SelfmonService>();
        let mut sampler = sampler(&proc);
        sampler.start(&ctx).expect("start");
        let batch = sampler
            .on_shutdown(&[])
            .expect("flush")
            .pop()
            .expect("a batch");

        assert!(!batch.process_cpu_usec.is_empty());
        assert!(
            batch.process_rss_bytes.is_empty(),
            "absent must not arrive as zero"
        );
        assert!(batch.process_threads.is_empty());
    }

    #[test]
    fn a_missing_proc_tree_does_not_stop_the_engine_counters_being_reported() {
        let engine = FakeEngine::new("node-0042");
        engine.set_engine_stats(stats());
        let ctx = engine.ctx::<SelfmonService>();

        let mut sampler = SelfmonSampler::with_config(
            SelfmonConfig::new()
                .every(Duration::from_secs(1))
                .batching_for(Duration::from_secs(3))
                .reading_proc("/nonexistent/proc/self"),
        );
        sampler.start(&ctx).expect("start");
        let batch = sampler
            .on_shutdown(&[])
            .expect("flush")
            .pop()
            .expect("a batch");

        assert_eq!(
            batch.frames_sent[0], 100,
            "the engine's counters still come"
        );
        assert!(batch.threads.is_empty());
        assert!(batch.process_cpu_usec.is_empty());
    }

    #[test]
    fn shutdown_takes_a_final_reading_and_flushes() {
        let proc = running_agent();
        let engine = FakeEngine::new("node-0042");
        engine.set_engine_stats(stats());
        let ctx = engine.ctx::<SelfmonService>();

        let mut sampler = sampler(&proc);
        sampler.start(&ctx).expect("start");
        assert!(
            sampler.sample(&[]).expect("sample").is_empty(),
            "window open"
        );

        let batches = sampler.on_shutdown(&[]).expect("flush");
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].sampled_unix_ms.len(),
            2,
            "the held sample plus one final reading, because the counters at \
             shutdown are the ones that say whether anything was dropped"
        );
    }

    #[test]
    fn a_command_retunes_the_interval_and_zero_restores_the_default() {
        let mut sampler = SelfmonSampler::new();
        assert_eq!(sampler.interval(), Duration::from_secs(15));
        assert_eq!(sampler.on_command(Command::Custom(500)), Reply::Handled);
        assert_eq!(sampler.interval(), Duration::from_millis(500));
        assert_eq!(sampler.on_command(Command::Custom(0)), Reply::Handled);
        assert_eq!(sampler.interval(), Duration::from_secs(15));
        assert_eq!(sampler.on_command(Command::Shutdown), Reply::Default);
    }

    #[test]
    fn the_whole_plugin_works_against_the_fake_engine() {
        let proc = running_agent();
        let engine = FakeEngine::new("node-0042");
        engine.set_engine_stats(stats());
        let ctx = engine.ctx::<SelfmonService>();

        let mut sampler = sampler(&proc);
        sampler.start(&ctx).expect("start");
        for _ in 0..4 {
            for batch in sampler.sample(&[]).expect("sample") {
                ctx.send(batch).expect("send");
            }
        }

        let sent = engine
            .sent::<SelfmonService>()
            .expect("decode what was sent");
        assert_eq!(sent.len(), 1);
        assert!(!sent[0].threads.is_empty());
        assert_eq!(engine.sent_raw()[0].0.name, "selfmon");
    }

    #[test]
    fn the_service_name_fits_a_thread_name() {
        assert_eq!(SelfmonService::NAME, "selfmon");
        let thread = cs_api::worker_thread_name(SelfmonService::NAME, "sample");
        assert_eq!(thread, "selfmon/sample");
        assert!(thread.len() <= cs_api::MAX_THREAD_NAME_LEN);
        // And it round-trips: this is what makes the report self-consistent.
        assert_eq!(crate::proc::service_of(&thread), "selfmon");
    }
}
