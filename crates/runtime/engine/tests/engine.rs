//! The engine, end to end, over the hostile mock transport.
//!
//! Driven entirely through `cs-testkit`: the plugins, the cluster, and the waiting
//! all come from there, so these tests say what the engine should *do* and nothing
//! about how to set one up.

use std::time::Duration;

use cs_api::{Command, CommandOpts, CommandOutcome};
use cs_engine::{EngineConfig, NodeEngine, Stop};
use cs_testkit::{
    BulkSampler, BulkService, Collect, Commander, CounterConfig, CounterService, TestCluster,
};
use cs_transport_mock::{Direction, MockNetwork};
use cs_util::ErrorKind;

/// A cluster with one well-behaved agent and a recording server.
async fn simple() -> (TestCluster, Collect<CounterService>, CounterConfig) {
    let collected = Collect::<CounterService>::new();
    let counter = CounterConfig::new();

    let cluster = TestCluster::builder()
        .server({
            let collected = collected.clone();
            move |server| server.handler(collected)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    (cluster, collected, counter)
}

// --- the data path ------------------------------------------------------------

#[tokio::test]
async fn data_flows_from_a_sampler_to_a_handler_on_another_engine() {
    let (cluster, collected, _) = simple().await;
    cluster
        .wait_for("the first batches", || collected.count() >= 3)
        .await;

    // Sequence numbers, in order, with nothing lost or repeated.
    let seen = collected.seen();
    assert_eq!(seen[..3], [1, 2, 3], "got {seen:?}");
    // And every one is attributed to the node that sent it.
    assert!(collected.senders().iter().all(|node| node == "node-1"));

    let stats = cluster.agent().stats();
    assert!(stats.connected);
    assert_eq!(stats.peers, 1);
    let counter = stats.service("counter").expect("the service is listed");
    assert!(counter.samples >= 3);
    assert_eq!(counter.sample_errors, 0);
    assert_eq!(counter.panics, 0);

    cluster.stop().await;
}

#[tokio::test]
async fn several_agents_are_kept_apart() {
    let collected = Collect::<CounterService>::new();
    let counter = CounterConfig::new();

    let cluster = TestCluster::builder()
        .server({
            let collected = collected.clone();
            move |server| server.handler(collected)
        })
        .agents(3, {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    cluster
        .wait_for("all three agents", || {
            cluster.server().expect("server").stats().peers == 3
        })
        .await;
    cluster
        .wait_for("data from every agent", || {
            let senders = collected.senders();
            ["node-1", "node-2", "node-3"]
                .iter()
                .all(|node| senders.iter().any(|seen| seen == node))
        })
        .await;

    cluster.stop().await;
}

#[tokio::test]
async fn a_message_too_big_for_one_frame_is_chunked_and_reassembled() {
    let collected = Collect::<BulkService>::new();
    let size = 40 * 1024;

    let cluster = TestCluster::builder()
        // Small enough that 40 KiB needs many frames.
        .max_frame(512)
        .server({
            let collected = collected.clone();
            move |server| server.handler(collected)
        })
        .agent("node-1", move |agent| {
            agent.sampler(BulkSampler::factory(size))
        })
        .start()
        .await;

    cluster
        .wait_for("the chunked message", || collected.count() >= 1)
        .await;
    let seen = collected.seen();
    assert_eq!(
        seen[0].len(),
        size,
        "the message came back a different size"
    );
    assert!(seen[0].bytes().all(|byte| byte == b'x'));

    // It really was split, rather than squeezed through whole.
    let sent = cluster.link().frames_sent(Direction::ToServer);
    assert!(
        sent > 50,
        "only {sent} frames for {size} bytes over 512-byte frames"
    );

    cluster.stop().await;
}

// --- commands -----------------------------------------------------------------

#[tokio::test]
async fn a_handler_can_command_the_node_that_sent_it_data() {
    let commander = Commander::new(Command::Custom(7));
    let counter = CounterConfig::new();

    let cluster = TestCluster::builder()
        .server({
            let commander = commander.clone();
            move |server| server.handler(commander)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    cluster
        .wait_for("the command to be answered", || {
            commander.outcome().is_some()
        })
        .await;

    // The sampler answered `Handled`, which is `Ok` to whoever asked.
    assert_eq!(commander.outcome(), Some(Ok(CommandOutcome::Ok)));
    let stats = cluster.server().expect("server").stats();
    assert_eq!(stats.commands_completed, 1);
    assert_eq!(stats.commands_expired, 0);

    cluster.stop().await;
}

#[tokio::test]
async fn a_custom_command_nobody_implements_comes_back_unsupported() {
    let commander = Commander::new(Command::Custom(7));
    let counter = CounterConfig::new().ignores_custom_commands();

    let cluster = TestCluster::builder()
        .server({
            let commander = commander.clone();
            move |server| server.handler(commander)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    cluster
        .wait_for("an answer", || commander.outcome().is_some())
        .await;
    assert_eq!(
        commander.outcome(),
        Some(Ok(CommandOutcome::Unsupported)),
        "a service that ignores a command must not report success"
    );

    cluster.stop().await;
}

#[tokio::test]
async fn a_refused_restart_leaves_the_service_running_and_says_why() {
    let commander = Commander::new(Command::Restart);
    let counter = CounterConfig::new().refuses_restart();

    let cluster = TestCluster::builder()
        .server({
            let commander = commander.clone();
            move |server| server.handler(commander)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    cluster
        .wait_for("the refusal", || commander.outcome().is_some())
        .await;
    assert_eq!(
        commander.outcome(),
        Some(Ok(CommandOutcome::Rejected("mid-batch".into()))),
        "the reason must reach the operator"
    );

    // And the service was neither stopped nor rebuilt.
    let built_once = counter.builds();
    let samples = counter.samples();
    cluster
        .wait_for("more samples", || counter.samples() > samples + 2)
        .await;
    assert_eq!(counter.builds(), built_once, "it should not be rebuilt");

    cluster.stop().await;
}

#[tokio::test]
async fn a_command_for_a_node_that_is_not_connected_expires() {
    let commander = Commander::new(Command::Restart)
        .addressed_to("node-that-does-not-exist")
        .with_opts(CommandOpts::expiring_in(Duration::from_millis(50)));
    let counter = CounterConfig::new();

    let cluster = TestCluster::builder()
        .server({
            let commander = commander.clone();
            move |server| server.handler(commander)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    cluster
        .wait_for("the command to expire", || commander.outcome().is_some())
        .await;
    assert_eq!(
        commander.outcome(),
        Some(Ok(CommandOutcome::Expired)),
        "a command for an absent node must resolve, not hang"
    );
    assert_eq!(
        cluster.server().expect("server").stats().commands_expired,
        1
    );

    cluster.stop().await;
}

// --- supervision --------------------------------------------------------------

#[tokio::test]
async fn a_panicking_sampler_is_rebuilt_and_keeps_reporting() {
    let collected = Collect::<CounterService>::new();
    let counter = CounterConfig::new().panics_on_sample(3);

    let cluster = TestCluster::builder()
        .server({
            let collected = collected.clone();
            move |server| server.handler(collected)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    // It panics on its third sample, is rebuilt, and counts again from one — so a
    // repeated `1` is the proof that a fresh instance took over.
    cluster
        .wait_for("a rebuild", || counter.builds() >= 2)
        .await;
    cluster
        .wait_for("samples after the rebuild", || {
            collected.seen().iter().filter(|&&n| n == 1).count() >= 2
        })
        .await;

    let stats = cluster.agent().stats();
    assert!(
        stats.service("counter").expect("counter").panics >= 1,
        "the panic should be counted"
    );

    cluster.stop().await;
}

#[tokio::test]
async fn a_failing_sample_is_counted_but_does_not_rebuild_anything() {
    let collected = Collect::<CounterService>::new();
    let counter = CounterConfig::new().fails_on_sample(2);

    let cluster = TestCluster::builder()
        .server({
            let collected = collected.clone();
            move |server| server.handler(collected)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    cluster
        .wait_for("samples either side of the failure", || {
            counter.samples() >= 4
        })
        .await;

    let stats = cluster.agent().stats();
    let service = stats.service("counter").expect("counter");
    assert_eq!(service.sample_errors, 1, "the failure should be counted");
    assert_eq!(service.panics, 0, "an error is not a panic");
    assert_eq!(counter.builds(), 1, "an error must not rebuild the sampler");
    // The samples either side of the failure still arrived.
    assert!(collected.seen().contains(&1));
    assert!(collected.seen().contains(&3));

    cluster.stop().await;
}

#[tokio::test]
async fn a_sampler_that_refuses_to_start_stops_without_taking_the_engine_down() {
    let counter = CounterConfig::new().fails_to_start();

    let cluster = TestCluster::builder()
        .server(|server| server.handler(Collect::<CounterService>::new()))
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    // The engine still connects and stays up; only the service is gone.
    cluster
        .wait_for("the agent to connect anyway", || {
            cluster.server().expect("server").stats().peers == 1
        })
        .await;
    assert!(!cluster.agent().handle().is_stopping());
    assert_eq!(counter.builds(), 1, "a refusal to start is not retried");

    cluster.stop().await;
}

// --- outages ------------------------------------------------------------------

#[tokio::test]
async fn data_produced_during_an_outage_is_delivered_on_reconnect() {
    let (cluster, collected, _) = simple().await;
    cluster
        .wait_for("the first batches", || collected.count() >= 2)
        .await;
    let before_outage = collected.count();

    // Break the link. The sampler carries on filling the buffer.
    cluster.link().kill();
    cluster
        .wait_for("a reconnect", || cluster.network().link_count() >= 2)
        .await;
    cluster
        .wait_for("data after the reconnect", || {
            collected.count() > before_outage + 2
        })
        .await;

    // Nothing went backwards across the gap: the sequence kept climbing.
    let seen = collected.seen();
    let highest = seen.iter().copied().max().expect("some data");
    assert!(
        highest as usize >= before_outage,
        "the sequence went backwards: {seen:?}"
    );
    assert!(cluster.agent().stats().reconnects >= 1);

    cluster.stop().await;
}

#[tokio::test]
async fn an_agent_keeps_dialling_until_the_server_appears() {
    let collected = Collect::<CounterService>::new();
    let counter = CounterConfig::new();

    // No server at all to begin with.
    let cluster = TestCluster::builder()
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    cluster
        .wait_for("some failed attempts", || {
            cluster.network().connect_attempts(cs_testkit::SERVER) >= 3
        })
        .await;
    assert!(!cluster.agent().stats().connected);

    // Bring a server up on the same network; the agent finds it unprompted.
    let server = NodeEngine::builder(cluster.network().transport())
        .config(EngineConfig::new("head01"))
        .handler(collected.clone())
        .listen(TestCluster::server_endpoint())
        .build()
        .expect("server");
    let server_handle = server.handle();
    let server_task = tokio::spawn(server.run());

    cluster
        .wait_for("data once the server exists", || collected.count() >= 1)
        .await;

    cluster.stop().await;
    server_handle.shutdown();
    server_task.await.expect("task").expect("clean stop");
}

#[tokio::test]
async fn garbage_on_the_wire_is_survived_rather_than_fatal() {
    let (cluster, collected, _) = simple().await;
    cluster
        .wait_for("a connection and some data", || collected.count() >= 1)
        .await;
    let link = cluster.link();

    // Something that is not a frame at all, then a frame for a service nobody
    // handles.
    link.inject_garbage(Direction::ToServer).expect("inject");
    let stray = cs_transport::Frame::Data(cs_transport::DataFrame::new(
        "nosuchservice",
        1,
        bytes::Bytes::from_static(b"\x01"),
    ))
    .encode();
    link.inject(Direction::ToServer, stray).expect("inject");

    // The connection survives both, and data keeps flowing on it.
    let before = collected.count();
    cluster
        .wait_for("data after the garbage", || collected.count() > before + 2)
        .await;
    assert!(link.is_alive(), "the connection should have survived");
    assert_eq!(
        cluster.network().link_count(),
        1,
        "and should not have been re-established"
    );
    assert!(
        cluster.server().expect("server").stats().unroutable_frames >= 2,
        "both should be counted"
    );

    cluster.stop().await;
}

#[tokio::test]
async fn a_handler_that_always_fails_does_not_cost_the_connection() {
    let failing = Collect::<CounterService>::failing();
    let counter = CounterConfig::new();

    let cluster = TestCluster::builder()
        .server({
            let failing = failing.clone();
            move |server| server.handler(failing)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    cluster
        .wait_for("several rejected messages", || {
            cluster
                .server()
                .expect("server")
                .stats()
                .service("counter")
                .is_some_and(|service| service.messages_received >= 3)
        })
        .await;
    assert_eq!(failing.count(), 0, "it rejected everything");
    assert_eq!(cluster.network().live_link_count(), 1, "still connected");

    cluster.stop().await;
}

// --- shutdown -----------------------------------------------------------------

#[tokio::test]
async fn on_shutdown_data_is_flushed_before_the_connection_closes() {
    let collected = Collect::<CounterService>::new();
    let farewell = 999_999;
    // Slow enough that the farewell cannot be mistaken for an ordinary sample.
    let counter = CounterConfig::new()
        .every(Duration::from_millis(50))
        .says_farewell(farewell);

    let mut cluster = TestCluster::builder()
        .server({
            let collected = collected.clone();
            move |server| server.handler(collected)
        })
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    cluster
        .wait_for("the agent to connect", || {
            cluster.server().expect("server").stats().peers == 1
        })
        .await;

    // Stopping the agent must get the final flush out ahead of the goodbye.
    let agent = cluster.take_agent();
    assert_eq!(agent.stop().await, Stop::Shutdown);
    cluster
        .wait_for("the farewell batch", || {
            collected.seen().contains(&farewell)
        })
        .await;

    cluster.stop().await;
}

#[tokio::test]
async fn a_slow_flush_does_not_hang_shutdown_past_its_deadline() {
    let counter = CounterConfig::new()
        // Far longer than the deadline below, so the engine must give up on it.
        .slow_to_shut_down(Duration::from_secs(30))
        .says_farewell(1);

    let mut cluster = TestCluster::builder()
        .server(|server| server.handler(Collect::<CounterService>::new()))
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| {
                agent
                    .config(EngineConfig {
                        shutdown_deadline: Duration::from_millis(200),
                        ..cs_testkit::test_config("node-1")
                    })
                    .sampler(counter.factory())
            }
        })
        .start()
        .await;

    cluster
        .wait_for("the agent to be sampling", || counter.samples() >= 1)
        .await;

    let agent = cluster.take_agent();
    let began = std::time::Instant::now();
    agent.stop().await;
    let took = began.elapsed();
    assert!(
        took < Duration::from_secs(5),
        "shutdown took {took:?}; the deadline should have cut it short"
    );

    cluster.stop().await;
}

#[tokio::test]
async fn a_server_shutdown_tells_its_agents_to_come_back() {
    let (cluster, collected, _) = simple().await;
    cluster
        .wait_for("a connection", || {
            cluster.server().expect("server").stats().peers == 1
        })
        .await;

    let mut cluster = cluster;
    let server = cluster.take_server();
    server.stop().await;
    assert_eq!(
        collected.shutdowns(),
        1,
        "Handler::shutdown should have run"
    );

    // The agent is told `restart`, so it keeps dialling rather than giving up.
    cluster
        .wait_for("the agent to try again", || {
            cluster.network().connect_attempts(cs_testkit::SERVER) >= 2
        })
        .await;

    cluster.stop().await;
}

#[tokio::test]
async fn restarting_reports_the_reason_the_supervisor_needs() {
    let counter = CounterConfig::new();
    let mut cluster = TestCluster::builder()
        .agent("node-1", {
            let counter = counter.clone();
            move |agent| agent.sampler(counter.factory())
        })
        .start()
        .await;

    let agent = cluster.take_agent();
    agent.handle().restart();
    assert_eq!(
        agent.finish().await,
        Stop::Restart,
        "the agent must be able to tell systemd to start it again"
    );
}

// --- what the builder refuses -------------------------------------------------

#[test]
fn an_engine_with_no_endpoints_is_rejected() {
    let err = NodeEngine::builder(MockNetwork::new().transport())
        .config(EngineConfig::new("node-1"))
        .build()
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Config);
    assert!(err.to_string().contains("dial an upstream"));
}

#[test]
fn a_sampler_with_nowhere_to_send_is_rejected() {
    let err = NodeEngine::builder(MockNetwork::new().transport())
        .config(EngineConfig::new("node-1"))
        .sampler(CounterConfig::new().factory())
        .listen(TestCluster::server_endpoint())
        .build()
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Config);
    assert!(err.to_string().contains("upstream to dial"));
}

#[test]
fn two_services_with_one_name_are_rejected() {
    let err = NodeEngine::builder(MockNetwork::new().transport())
        .config(EngineConfig::new("head01"))
        .handler(Collect::<CounterService>::new())
        .handler(Collect::<CounterService>::new())
        .listen(TestCluster::server_endpoint())
        .build()
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Config);
    assert!(err.to_string().contains("registered twice"));
}

#[test]
fn a_nameless_engine_is_rejected() {
    let err = NodeEngine::builder(MockNetwork::new().transport())
        .listen(TestCluster::server_endpoint())
        .build()
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Config);
    assert!(format!("{err:?}").contains("node name"));
}
