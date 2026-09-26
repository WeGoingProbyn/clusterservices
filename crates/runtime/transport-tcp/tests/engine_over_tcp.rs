//! The engine over real sockets.
//!
//! The contract suite says this transport keeps the promises the engine is built
//! on; this says the engine actually works when it does. Same plugins as the mock
//! tests use, so what differs is only the transport underneath.

// Test code reports a broken assumption by panicking; the lint exemption in
// `clippy.toml` only reaches inside test functions, not the helpers beside them.
#![allow(
    clippy::expect_used,
    reason = "a test helper should fail loudly and name what went wrong"
)]

use std::time::Duration;

use cs_api::{Command, CommandOutcome};
use cs_engine::{Backoff, EngineConfig, EngineHandle, NodeEngine, Stop};
use cs_testkit::{Collect, Commander, CounterConfig, CounterService, wait_for};
use cs_transport::Endpoint;
use cs_transport_tcp::TcpTransport;

/// Ask the kernel for any free port.
///
/// Deliberately *not* "find a free port and then bind it": that races every other
/// test in the workspace between the finding and the binding. The engine reports
/// what it actually bound, so nothing here has to guess.
fn any_port() -> Endpoint {
    Endpoint::from_parts("tcp", "127.0.0.1:0")
}

/// Wait until an engine has bound, and say where.
async fn bound(handle: &EngineHandle) -> Endpoint {
    wait_for("the server to bind", &mut || handle.listening().is_some()).await;
    handle.listening().expect("just checked")
}

fn config(node: &str) -> EngineConfig {
    EngineConfig {
        heartbeat_interval: Duration::from_millis(50),
        job_refresh: Duration::from_millis(20),
        shutdown_deadline: Duration::from_secs(5),
        reconnect: Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(40),
            factor: 2,
        },
        ..EngineConfig::new(node)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_and_attribution_survive_a_real_socket() {
    let collected = Collect::<CounterService>::new();
    let counter = CounterConfig::new();

    let server = NodeEngine::builder(TcpTransport::new())
        .config(config("head01"))
        .handler(collected.clone())
        .listen(any_port())
        .build()
        .expect("server");
    let server_handle = server.handle();
    let server_task = tokio::spawn(server.run());
    let at = bound(&server_handle).await;

    let agent = NodeEngine::builder(TcpTransport::new())
        .config(config("node-1"))
        .sampler(counter.factory())
        .dial(at)
        .build()
        .expect("agent");
    let agent_handle = agent.handle();
    let agent_task = tokio::spawn(agent.run());

    wait_for("batches over TCP", &mut || collected.count() >= 3).await;
    let seen = collected.seen();
    assert_eq!(seen[..3], [1, 2, 3], "got {seen:?}");
    assert!(
        collected.senders().iter().all(|node| node == "node-1"),
        "every batch should be attributed to the node that sent it"
    );

    agent_handle.shutdown();
    assert_eq!(
        agent_task.await.expect("agent task").expect("clean stop"),
        Stop::Shutdown
    );
    server_handle.shutdown();
    server_task.await.expect("server task").expect("clean stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_command_round_trips_over_a_real_socket() {
    let commander = Commander::new(Command::Custom(7));
    let counter = CounterConfig::new();

    let server = NodeEngine::builder(TcpTransport::new())
        .config(config("head01"))
        .handler(commander.clone())
        .listen(any_port())
        .build()
        .expect("server");
    let server_handle = server.handle();
    let server_task = tokio::spawn(server.run());
    let at = bound(&server_handle).await;

    let agent = NodeEngine::builder(TcpTransport::new())
        .config(config("node-1"))
        .sampler(counter.factory())
        .dial(at)
        .build()
        .expect("agent");
    let agent_handle = agent.handle();
    let agent_task = tokio::spawn(agent.run());

    wait_for("the command to be answered", &mut || {
        commander.outcome().is_some()
    })
    .await;
    assert_eq!(commander.outcome(), Some(Ok(CommandOutcome::Ok)));
    assert_eq!(server_handle.stats().commands_completed, 1);

    agent_handle.shutdown();
    agent_task.await.expect("agent task").expect("clean stop");
    server_handle.shutdown();
    server_task.await.expect("server task").expect("clean stop");
}

/// The case a cluster actually lives through: the server is restarted under the
/// agents, on real sockets, with a real "connection refused" in between.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_survives_the_server_being_restarted() {
    let counter = CounterConfig::new();

    let first_batch = Collect::<CounterService>::new();
    let first_server = NodeEngine::builder(TcpTransport::new())
        .config(config("head01"))
        .handler(first_batch.clone())
        .listen(any_port())
        .build()
        .expect("server");
    let first_handle = first_server.handle();
    let first_task = tokio::spawn(first_server.run());
    // The port the replacement has to come back on.
    let at = bound(&first_handle).await;

    let agent = NodeEngine::builder(TcpTransport::new())
        .config(config("node-1"))
        .sampler(counter.factory())
        .dial(at.clone())
        .build()
        .expect("agent");
    let agent_handle = agent.handle();
    let agent_task = tokio::spawn(agent.run());

    wait_for("data before the restart", &mut || first_batch.count() >= 2).await;

    // Take the server away entirely, so the agent's dials are genuinely refused.
    first_handle.shutdown();
    first_task.await.expect("server task").expect("clean stop");
    wait_for("the agent to notice", &mut || {
        !agent_handle.stats().connected
    })
    .await;

    // Bring a new one up on the *same* address — which needs SO_REUSEADDR, since
    // the connection the first server just closed is sitting in TIME_WAIT on that
    // very port. The agent then finds it unprompted.
    let second_batch = Collect::<CounterService>::new();
    let second_server = NodeEngine::builder(TcpTransport::new())
        .config(config("head01"))
        .handler(second_batch.clone())
        .listen(at)
        .build()
        .expect("second server");
    let second_handle = second_server.handle();
    let second_task = tokio::spawn(second_server.run());

    wait_for("data after the restart", &mut || second_batch.count() >= 2).await;
    assert!(
        agent_handle.stats().reconnects >= 1,
        "the reconnection should be counted"
    );

    agent_handle.shutdown();
    agent_task.await.expect("agent task").expect("clean stop");
    second_handle.shutdown();
    second_task.await.expect("server task").expect("clean stop");
}
