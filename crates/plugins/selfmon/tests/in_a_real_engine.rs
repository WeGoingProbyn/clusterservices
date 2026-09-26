//! Selfmon inside a running engine, reading this process's real `/proc`.
//!
//! The docs claim that naming worker threads `<service>/<worker>` is what makes
//! per-plugin CPU attributable. That claim spans three crates — the engine names
//! the thread, the kernel accounts to it, this plugin reads it back — so it is
//! worth proving rather than asserting.

// A test harness reports a broken assumption by panicking.
#![allow(
    clippy::expect_used,
    reason = "a test helper should fail loudly and name what went wrong"
)]

use std::time::Duration;

use cs_plugin_selfmon::{SelfmonBatch, SelfmonConfig, SelfmonSampler, SelfmonService};
use cs_testkit::{Collect, CounterConfig, TestCluster};

/// Fast enough that the test does not wait, and a window short enough to close.
fn config() -> SelfmonConfig {
    SelfmonConfig::new()
        .every(Duration::from_millis(20))
        .batching_for(Duration::from_millis(60))
}

#[tokio::test]
async fn the_thread_naming_convention_makes_per_plugin_cpu_attributable() {
    let reports = Collect::<SelfmonService>::new();
    let counter = CounterConfig::new();

    // An agent running two samplers: a workload to be measured, and selfmon.
    let cluster = TestCluster::builder()
        .server({
            let reports = reports.clone();
            move |server| server.handler(reports)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| {
                agent
                    .sampler(counter.factory())
                    .sampler(SelfmonSampler::factory_with(config()))
            }
        })
        .start()
        .await;

    cluster
        .wait_for("a selfmon batch", || reports.count() >= 1)
        .await;
    let batch: SelfmonBatch = reports.seen().remove(0);

    // Both samplers' threads exist, named for their services by the engine and
    // read back out of /proc by this plugin.
    let named: Vec<(&str, &str)> = batch
        .threads
        .iter()
        .map(|series| (series.thread.as_str(), series.service.as_str()))
        .collect();
    assert!(
        named.contains(&("counter/sample", "counter")),
        "the workload sampler's thread should be attributed to it: {named:?}"
    );
    assert!(
        named.contains(&("selfmon/sample", "selfmon")),
        "and so should this plugin's own: {named:?}"
    );

    // Threads that belong to no plugin are reported with no service, which is what
    // puts the framework's overhead beside the plugins' cost.
    assert!(
        named.iter().any(|(_, service)| service.is_empty()),
        "the runtime's own threads should be visible too: {named:?}"
    );

    // Every column lines up with the timestamps, and the engine's counters are
    // real: the agent has sent this batch's predecessors over a live connection.
    let samples = batch.sampled_unix_ms.len();
    assert!(samples >= 1);
    for series in &batch.threads {
        assert_eq!(series.cpu_usec.len(), samples, "{} column", series.thread);
    }
    assert_eq!(batch.frames_sent.len(), samples);
    assert!(
        batch.process_threads[0] >= 2,
        "a running agent has several threads"
    );

    cluster.stop().await;
}

#[tokio::test]
async fn a_services_own_counters_come_back_through_selfmon() {
    let reports = Collect::<SelfmonService>::new();
    // A sampler that fails every other read, so there is something to report.
    let counter = CounterConfig::new()
        .every(Duration::from_millis(5))
        .fails_on_sample(2);

    let cluster = TestCluster::builder()
        .server({
            let reports = reports.clone();
            move |server| server.handler(reports)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| {
                agent
                    .sampler(counter.factory())
                    .sampler(SelfmonSampler::factory_with(config()))
            }
        })
        .start()
        .await;

    cluster
        .wait_for("selfmon to notice the failure", || {
            reports.seen().iter().any(|batch| {
                batch
                    .services
                    .iter()
                    .any(|series| series.service == "counter" && series.sample_errors.contains(&1))
            })
        })
        .await;

    cluster.stop().await;
}
