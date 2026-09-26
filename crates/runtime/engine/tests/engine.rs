//! The engine, end to end, over the hostile mock transport.
//!
//! These drive the public API only — build an engine, register plugins, run it —
//! so they exercise the same path an agent and a server will.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cs_api::{
    CommandOpts, CommandOutcome, CommandReceiver, Handler, JobInfo, NoCommand, Reply, Sampler,
    ServiceBound, ServiceCtx, ServiceDef,
};
use cs_engine::{EngineConfig, EngineHandle, NodeEngine, Stop};
use cs_transport_mock::{Direction, MockNetwork};
use cs_util::{Error, ErrorKind, Result};

// --- the service under test ---------------------------------------------------

/// Counters, with a command to retune the interval.
struct Counter;

impl ServiceDef for Counter {
    const NAME: &'static str = "counter";
    const VERSION: u32 = 2;
    /// Sequence numbers, so a test can detect loss, duplication, and reordering.
    type Data = u64;
    /// A new interval, in milliseconds.
    type Command = u64;
}

/// A sampler that emits consecutive numbers.
struct CountSampler {
    next: u64,
    interval: Duration,
    /// Panics on this sample, to exercise supervision. `None` never panics.
    panic_on: Option<u64>,
    /// Refuses `Restart`, to exercise the rejection path.
    refuse_restart: bool,
    /// What `on_shutdown` emits.
    farewell: Option<u64>,
    /// Shared so a test can see how many times the factory was called.
    builds: Arc<AtomicU32>,
}

#[derive(Clone, Default)]
struct CounterSetup {
    panic_on: Option<u64>,
    refuse_restart: bool,
    farewell: Option<u64>,
    interval: Option<Duration>,
    builds: Arc<AtomicU32>,
}

impl CounterSetup {
    /// A factory, as the engine wants it.
    fn factory(&self) -> impl FnMut() -> CountSampler + Send + 'static {
        let setup = self.clone();
        move || {
            setup.builds.fetch_add(1, Ordering::SeqCst);
            CountSampler {
                next: 0,
                interval: setup.interval.unwrap_or(Duration::from_millis(5)),
                panic_on: setup.panic_on,
                refuse_restart: setup.refuse_restart,
                farewell: setup.farewell,
                builds: Arc::clone(&setup.builds),
            }
        }
    }

    fn builds(&self) -> u32 {
        self.builds.load(Ordering::SeqCst)
    }
}

impl ServiceBound for CountSampler {
    type Service = Counter;
}

impl CommandReceiver for CountSampler {
    fn on_command(&mut self, command: cs_api::Command<u64>) -> Reply {
        match command {
            cs_api::Command::Custom(millis) => {
                self.interval = Duration::from_millis(millis);
                Reply::Handled
            }
            cs_api::Command::Restart if self.refuse_restart => Reply::rejected("mid-batch"),
            _ => Reply::Default,
        }
    }
}

impl Sampler for CountSampler {
    fn interval(&self) -> Duration {
        self.interval
    }

    fn sample(&mut self, _jobs: &[JobInfo]) -> Result<Vec<u64>> {
        self.next += 1;
        if self.panic_on == Some(self.next) {
            panic!("sampler exploded on sample {}", self.next);
        }
        Ok(vec![self.next])
    }

    fn on_shutdown(&mut self, _jobs: &[JobInfo]) -> Result<Vec<u64>> {
        let _ = &self.builds;
        Ok(self.farewell.into_iter().collect())
    }
}

/// A sampler that refuses to start, for the "NVML is absent" case.
struct Unstartable;

impl ServiceBound for Unstartable {
    type Service = Counter;
}
impl CommandReceiver for Unstartable {}

impl Sampler for Unstartable {
    fn interval(&self) -> Duration {
        Duration::from_millis(5)
    }

    fn sample(&mut self, _jobs: &[JobInfo]) -> Result<Vec<u64>> {
        Ok(Vec::new())
    }

    fn start(&mut self, _ctx: &ServiceCtx<Counter>) -> Result<()> {
        Err(Error::new(ErrorKind::Plugin, "this node has no hardware"))
    }
}

/// A service whose payloads are big enough to need chunking.
struct Bulk;

impl ServiceDef for Bulk {
    const NAME: &'static str = "bulk";
    type Data = String;
    type Command = NoCommand;
}

struct BulkSampler {
    size: usize,
    sent: bool,
}

impl ServiceBound for BulkSampler {
    type Service = Bulk;
}
impl CommandReceiver for BulkSampler {}

impl Sampler for BulkSampler {
    fn interval(&self) -> Duration {
        Duration::from_millis(5)
    }

    fn sample(&mut self, _jobs: &[JobInfo]) -> Result<Vec<String>> {
        if self.sent {
            return Ok(Vec::new());
        }
        self.sent = true;
        Ok(vec!["x".repeat(self.size)])
    }
}

// --- handlers -----------------------------------------------------------------

/// Records everything it is given.
#[derive(Clone, Default)]
struct Collect<T> {
    seen: Arc<Mutex<Vec<T>>>,
    closed: Arc<AtomicU32>,
}

impl<T> Collect<T> {
    fn seen(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.seen.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

impl ServiceBound for Collect<u64> {
    type Service = Counter;
}

impl Handler for Collect<u64> {
    async fn handle(&self, _ctx: &ServiceCtx<Counter>, message: u64) -> Result<()> {
        self.seen
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(message);
        Ok(())
    }

    async fn shutdown(&self) {
        self.closed.fetch_add(1, Ordering::SeqCst);
    }
}

impl ServiceBound for Collect<String> {
    type Service = Bulk;
}

impl Handler for Collect<String> {
    async fn handle(&self, _ctx: &ServiceCtx<Bulk>, message: String) -> Result<()> {
        self.seen
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(message);
        Ok(())
    }
}

/// Sends a command back to the node that just sent it data — the loop a real
/// server closes when it decides a sampler is too chatty.
struct Commander {
    seen: Arc<AtomicU64>,
    outcome: Arc<Mutex<Option<std::result::Result<CommandOutcome, String>>>>,
    interval_ms: u64,
}

impl ServiceBound for Commander {
    type Service = Counter;
}

impl Handler for Commander {
    async fn handle(&self, ctx: &ServiceCtx<Counter>, message: u64) -> Result<()> {
        if self.seen.fetch_add(1, Ordering::SeqCst) > 0 {
            return Ok(());
        }
        let _ = message;
        let answer = ctx
            .command::<Counter>(
                "node-1",
                cs_api::Command::Custom(self.interval_ms),
                CommandOpts::default(),
            )
            .await;
        *self.outcome.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(answer.map_err(|err| format!("{err:?}")));
        Ok(())
    }
}

// --- harness ------------------------------------------------------------------

/// Wait for `condition`, or fail the test.
async fn wait_for(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("timed out waiting for {what}");
}

fn config(node: &str) -> EngineConfig {
    EngineConfig {
        // Short enough that tests do not wait, long enough to be deterministic.
        heartbeat_interval: Duration::from_millis(50),
        job_refresh: Duration::from_millis(20),
        shutdown_deadline: Duration::from_secs(5),
        reconnect: cs_engine::Backoff {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(20),
            factor: 2,
        },
        restart_backoff: cs_engine::Backoff {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(20),
            factor: 2,
        },
        ..EngineConfig::new(node)
    }
}

/// A running engine and the handle that stops it.
struct Running {
    handle: EngineHandle,
    task: tokio::task::JoinHandle<Result<Stop>>,
}

impl Running {
    #[allow(clippy::expect_used, reason = "a test harness should fail loudly")]
    async fn stop(self) -> Stop {
        self.handle.shutdown();
        self.task.await.expect("engine task").expect("clean stop")
    }

    #[allow(clippy::expect_used, reason = "a test harness should fail loudly")]
    async fn finish(self) -> Stop {
        self.task.await.expect("engine task").expect("clean stop")
    }
}

fn start<T: cs_transport::Transport>(engine: NodeEngine<T>) -> Running {
    let handle = engine.handle();
    Running {
        handle,
        task: tokio::spawn(engine.run()),
    }
}

// --- tests --------------------------------------------------------------------

#[tokio::test]
async fn data_flows_from_a_sampler_to_a_handler_on_another_engine() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");
    let collected = Collect::<u64>::default();

    let server = start(
        NodeEngine::builder(network.transport())
            .config(config("head01"))
            .handler(collected.clone())
            .listen(at.clone())
            .build()
            .expect("server"),
    );
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(CounterSetup::default().factory())
            .dial(at)
            .build()
            .expect("agent"),
    );

    wait_for("the first batches to arrive", || {
        collected.seen().len() >= 3
    })
    .await;

    // Sequence numbers, in order, with nothing lost or repeated.
    let seen = collected.seen();
    assert_eq!(seen[..3], [1, 2, 3], "got {seen:?}");

    let stats = agent.handle.stats();
    assert!(stats.connected);
    assert_eq!(stats.peers, 1);
    let counter = stats.service("counter").expect("the service is listed");
    assert!(counter.samples >= 3);
    assert_eq!(counter.sample_errors, 0);
    assert_eq!(counter.panics, 0);

    agent.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn a_message_too_big_for_one_frame_is_chunked_and_reassembled() {
    // Small enough that a 40 KiB message needs many frames.
    let network = MockNetwork::new().with_max_frame(512);
    let at = MockNetwork::endpoint("server");
    let collected = Collect::<String>::default();
    let size = 40 * 1024;

    let server = start(
        NodeEngine::builder(network.transport())
            .config(config("head01"))
            .handler(collected.clone())
            .listen(at.clone())
            .build()
            .expect("server"),
    );
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(move || BulkSampler { size, sent: false })
            .dial(at)
            .build()
            .expect("agent"),
    );

    wait_for("the chunked message", || !collected.seen().is_empty()).await;
    let seen = collected.seen();
    assert_eq!(
        seen[0].len(),
        size,
        "the message came back a different size"
    );
    assert!(seen[0].bytes().all(|b| b == b'x'));

    // It really was split: far more frames than messages.
    let link = network.last_link().expect("a link");
    assert!(
        link.frames_sent(Direction::ToServer) > 50,
        "only {} frames for {size} bytes over 512-byte frames",
        link.frames_sent(Direction::ToServer)
    );

    agent.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn a_server_handler_can_command_the_node_that_sent_it_data() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");
    let outcome = Arc::new(Mutex::new(None));
    let commander = Commander {
        seen: Arc::new(AtomicU64::new(0)),
        outcome: Arc::clone(&outcome),
        interval_ms: 7,
    };

    let server = start(
        NodeEngine::builder(network.transport())
            .config(config("head01"))
            .handler(commander)
            .listen(at.clone())
            .build()
            .expect("server"),
    );
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(CounterSetup::default().factory())
            .dial(at)
            .build()
            .expect("agent"),
    );

    wait_for("the command to be answered", || {
        outcome.lock().unwrap_or_else(|p| p.into_inner()).is_some()
    })
    .await;

    let answered = outcome
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
        .expect("an answer");
    // The sampler said `Handled`, which is `Ok` to the operator.
    assert_eq!(answered, Ok(CommandOutcome::Ok));

    let stats = server.handle.stats();
    assert_eq!(stats.commands_completed, 1);
    assert_eq!(stats.commands_expired, 0);

    agent.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn a_rejected_restart_leaves_the_service_running_and_says_why() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");
    let collected = Collect::<u64>::default();
    let setup = CounterSetup {
        refuse_restart: true,
        ..CounterSetup::default()
    };

    let server_engine = NodeEngine::builder(network.transport())
        .config(config("head01"))
        .handler(collected.clone())
        .listen(at.clone())
        .build()
        .expect("server");
    let server = start(server_engine);
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(setup.factory())
            .dial(at)
            .build()
            .expect("agent"),
    );

    wait_for("the agent to connect", || {
        server.handle.stats().peers == 1 && !collected.seen().is_empty()
    })
    .await;

    // There is no public command API on the handle, so the command goes through a
    // handler's context — which is the path an operator's request takes anyway.
    // Here we assert the sampler kept running and was never rebuilt.
    let before = setup.builds();
    wait_for("more samples", || collected.seen().len() > 3).await;
    assert_eq!(setup.builds(), before, "the sampler should not be rebuilt");

    agent.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn a_panicking_sampler_is_rebuilt_and_keeps_reporting() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");
    let collected = Collect::<u64>::default();
    let setup = CounterSetup {
        panic_on: Some(3),
        ..CounterSetup::default()
    };

    let server = start(
        NodeEngine::builder(network.transport())
            .config(config("head01"))
            .handler(collected.clone())
            .listen(at.clone())
            .build()
            .expect("server"),
    );
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(setup.factory())
            .dial(at)
            .build()
            .expect("agent"),
    );

    // It panics on its third sample, is rebuilt, and starts counting again — so
    // the sequence restarts rather than stopping.
    wait_for("a rebuild", || setup.builds() >= 2).await;
    wait_for("samples after the rebuild", || {
        let seen = collected.seen();
        seen.iter().filter(|&&n| n == 1).count() >= 2
    })
    .await;

    let stats = agent.handle.stats();
    let counter = stats.service("counter").expect("counter");
    assert!(counter.panics >= 1, "the panic should be counted");

    agent.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn a_sampler_that_refuses_to_start_stops_without_taking_the_engine_down() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");

    let server = start(
        NodeEngine::builder(network.transport())
            .config(config("head01"))
            .handler(Collect::<u64>::default())
            .listen(at.clone())
            .build()
            .expect("server"),
    );
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(|| Unstartable)
            .dial(at)
            .build()
            .expect("agent"),
    );

    // The engine still connects and stays up; only the service is gone.
    wait_for("the agent to connect anyway", || {
        server.handle.stats().peers == 1
    })
    .await;
    assert!(!agent.handle.is_stopping());

    agent.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn data_produced_during_an_outage_is_delivered_on_reconnect() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");
    let collected = Collect::<u64>::default();

    let server = start(
        NodeEngine::builder(network.transport())
            .config(config("head01"))
            .handler(collected.clone())
            .listen(at.clone())
            .build()
            .expect("server"),
    );
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(CounterSetup::default().factory())
            .dial(at)
            .build()
            .expect("agent"),
    );

    wait_for("the first batches", || collected.seen().len() >= 2).await;
    let before_outage = collected.seen().len();

    // Break the link. The sampler carries on filling the buffer.
    network.last_link().expect("a link").kill();
    wait_for("a reconnect", || network.link_count() >= 2).await;
    wait_for("data after the reconnect", || {
        collected.seen().len() > before_outage + 2
    })
    .await;

    // Nothing was lost across the gap: the sequence continues.
    let seen = collected.seen();
    let highest = seen.iter().copied().max().expect("some data");
    assert!(
        highest as usize >= before_outage,
        "the sequence went backwards: {seen:?}"
    );
    assert!(agent.handle.stats().reconnects >= 1);

    agent.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn an_agent_keeps_dialling_until_the_server_appears() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");
    let collected = Collect::<u64>::default();

    // The agent starts first, with nothing to connect to.
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(CounterSetup::default().factory())
            .dial(at.clone())
            .build()
            .expect("agent"),
    );
    wait_for("some failed attempts", || {
        network.connect_attempts("server") >= 3
    })
    .await;
    assert!(!agent.handle.stats().connected);

    // Now bring the server up; the agent finds it without being told.
    let server = start(
        NodeEngine::builder(network.transport())
            .config(config("head01"))
            .handler(collected.clone())
            .listen(at)
            .build()
            .expect("server"),
    );
    wait_for("data once the server exists", || {
        !collected.seen().is_empty()
    })
    .await;

    agent.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn on_shutdown_data_is_flushed_before_the_connection_closes() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");
    let collected = Collect::<u64>::default();
    let farewell = 999_999;
    let setup = CounterSetup {
        farewell: Some(farewell),
        interval: Some(Duration::from_millis(50)),
        ..CounterSetup::default()
    };

    let server = start(
        NodeEngine::builder(network.transport())
            .config(config("head01"))
            .handler(collected.clone())
            .listen(at.clone())
            .build()
            .expect("server"),
    );
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(setup.factory())
            .dial(at)
            .build()
            .expect("agent"),
    );

    wait_for("the agent to connect", || server.handle.stats().peers == 1).await;

    assert_eq!(agent.stop().await, Stop::Shutdown);
    wait_for("the farewell batch", || {
        collected.seen().contains(&farewell)
    })
    .await;

    server.stop().await;
}

#[tokio::test]
async fn garbage_on_the_wire_is_survived_rather_than_fatal() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");
    let collected = Collect::<u64>::default();

    let server = start(
        NodeEngine::builder(network.transport())
            .config(config("head01"))
            .handler(collected.clone())
            .listen(at.clone())
            .build()
            .expect("server"),
    );
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(CounterSetup::default().factory())
            .dial(at)
            .build()
            .expect("agent"),
    );

    wait_for("a connection", || network.link_count() >= 1).await;
    let link = network.last_link().expect("a link");
    wait_for("some data", || !collected.seen().is_empty()).await;

    // Something that is not a frame at all, then something that is a frame but
    // nobody's business.
    link.inject_garbage(Direction::ToServer).expect("inject");
    let stray = cs_transport::Frame::Data(cs_transport::DataFrame::new(
        "nosuchservice",
        1,
        bytes::Bytes::from_static(b"\x01"),
    ))
    .encode();
    link.inject(Direction::ToServer, stray).expect("inject");

    // The link survives both, and data keeps flowing on it.
    let before = collected.seen().len();
    wait_for("data after the garbage", || {
        collected.seen().len() > before + 2
    })
    .await;
    assert!(link.is_alive(), "the connection should have survived");
    assert_eq!(network.link_count(), 1, "and not been re-established");

    let stats = server.handle.stats();
    assert!(
        stats.unroutable_frames >= 2,
        "both should be counted, got {}",
        stats.unroutable_frames
    );

    agent.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn a_server_shutdown_tells_its_agents_and_closes_its_handlers() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");
    let collected = Collect::<u64>::default();
    let closed = Arc::clone(&collected.closed);

    let server = start(
        NodeEngine::builder(network.transport())
            .config(config("head01"))
            .handler(collected.clone())
            .listen(at.clone())
            .build()
            .expect("server"),
    );
    let agent = start(
        NodeEngine::builder(network.transport())
            .config(config("node-1"))
            .sampler(CounterSetup::default().factory())
            .dial(at)
            .build()
            .expect("agent"),
    );
    wait_for("a connection", || server.handle.stats().peers == 1).await;

    server.stop().await;
    assert_eq!(closed.load(Ordering::SeqCst), 1, "Handler::shutdown ran");

    // The agent notices and starts dialling again rather than giving up.
    wait_for("the agent to try again", || {
        network.connect_attempts("server") >= 2
    })
    .await;
    agent.stop().await;
}

#[tokio::test]
async fn restarting_reports_the_reason_the_supervisor_needs() {
    let network = MockNetwork::new();
    let at = MockNetwork::endpoint("server");

    let agent_engine = NodeEngine::builder(network.transport())
        .config(config("node-1"))
        .sampler(CounterSetup::default().factory())
        .dial(at)
        .build()
        .expect("agent");
    let handle = agent_engine.handle();
    let agent = Running {
        handle: handle.clone(),
        task: tokio::spawn(agent_engine.run()),
    };

    handle.restart();
    assert_eq!(
        agent.finish().await,
        Stop::Restart,
        "the agent must be able to tell systemd to start it again"
    );
}

// --- what the builder refuses -------------------------------------------------

#[test]
fn an_engine_with_no_endpoints_is_rejected() {
    let network = MockNetwork::new();
    let err = NodeEngine::builder(network.transport())
        .config(config("node-1"))
        .build()
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Config);
    assert!(err.to_string().contains("dial an upstream"));
}

#[test]
fn a_sampler_with_nowhere_to_send_is_rejected() {
    let network = MockNetwork::new();
    let err = NodeEngine::builder(network.transport())
        .config(config("node-1"))
        .sampler(CounterSetup::default().factory())
        .listen(MockNetwork::endpoint("server"))
        .build()
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Config);
    assert!(err.to_string().contains("upstream to dial"));
}

#[test]
fn two_services_with_one_name_are_rejected() {
    let network = MockNetwork::new();
    let err = NodeEngine::builder(network.transport())
        .config(config("head01"))
        .handler(Collect::<u64>::default())
        .handler(Collect::<u64>::default())
        .listen(MockNetwork::endpoint("server"))
        .build()
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Config);
    assert!(err.to_string().contains("registered twice"));
}

#[test]
fn a_nameless_engine_is_rejected() {
    let network = MockNetwork::new();
    let err = NodeEngine::builder(network.transport())
        .listen(MockNetwork::endpoint("server"))
        .build()
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Config);
    assert!(format!("{err:?}").contains("node name"));
}
