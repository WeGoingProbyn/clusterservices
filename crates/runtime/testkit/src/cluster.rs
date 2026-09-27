//! A whole cluster in one process.

use std::sync::Arc;
use std::time::{Duration, Instant};

use cs_api::EngineStats;
use cs_engine::{Backoff, EngineBuilder, EngineConfig, EngineHandle, NodeEngine, Stop};
use cs_operator::Operator;
use cs_transport::Endpoint;
use cs_transport_mock::{Link, MockNetwork, MockRx, MockTransport, MockTx};
use cs_util::Result;

/// The name the cluster's server listens under.
pub const SERVER: &str = "server";

/// The name the cluster's server accepts operators under.
pub const ADMIN: &str = "admin";

/// An operator connected to the test cluster's admin endpoint.
pub type TestOperator = Operator<MockTx, MockRx>;

/// How long [`TestCluster::wait_for`] waits before giving up.
const PATIENCE: Duration = Duration::from_secs(10);

/// Timings that keep a test fast without making it flaky.
///
/// Everything is milliseconds rather than seconds, so a test never waits on a
/// timer; the engine's behaviour does not depend on the scale, only the ordering.
#[must_use]
pub fn test_config(node: &str) -> EngineConfig {
    EngineConfig {
        heartbeat_interval: Duration::from_millis(20),
        // Three heartbeats, as in production — short enough that a test can watch a
        // peer be given up on without waiting.
        peer_timeout: Duration::from_millis(60),
        job_refresh: Duration::from_millis(20),
        shutdown_deadline: Duration::from_secs(5),
        reconnect: Backoff {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(20),
            factor: 2,
        },
        restart_backoff: Backoff {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(20),
            factor: 2,
        },
        ..EngineConfig::new(node)
    }
}

/// One running engine.
pub struct Node {
    name: String,
    handle: EngineHandle,
    task: tokio::task::JoinHandle<Result<Stop>>,
}

impl Node {
    /// This node's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Stop it and read its counters from outside.
    #[must_use]
    pub fn handle(&self) -> &EngineHandle {
        &self.handle
    }

    /// Its counters right now.
    #[must_use]
    pub fn stats(&self) -> EngineStats {
        self.handle.stats()
    }

    /// Ask it to stop, and wait for it.
    ///
    /// # Panics
    ///
    /// If the engine task panicked or returned an error.
    pub async fn stop(self) -> Stop {
        self.handle.shutdown();
        self.finish().await
    }

    /// Wait for it to stop on its own — after a command told it to, say.
    ///
    /// # Panics
    ///
    /// If the engine task panicked or returned an error.
    pub async fn finish(self) -> Stop {
        match self.task.await {
            Ok(Ok(stopped)) => stopped,
            Ok(Err(err)) => panic!("engine {} failed: {err:?}", self.name),
            Err(err) => panic!("engine {} panicked: {err}", self.name),
        }
    }
}

/// One server plus any number of agents, all in this process, over the mock
/// transport.
///
/// The links between them can be broken, delayed, or fed nonsense through
/// [`network`](TestCluster::network), so a test can do to the engine anything the
/// real world could.
///
/// ```no_run
/// use cs_testkit::{Collect, CounterConfig, CounterService, TestCluster};
///
/// # async fn example() {
/// let collected = Collect::<CounterService>::new();
/// let counter = CounterConfig::new();
///
/// let cluster = TestCluster::builder()
///     .server({
///         let collected = collected.clone();
///         move |server| server.handler(collected)
///     })
///     .agent("node-1", {
///         let counter = counter.clone();
///         move |agent| agent.sampler(counter.factory())
///     })
///     .start()
///     .await;
///
/// cluster.wait_for("three batches", || collected.count() >= 3).await;
/// cluster.stop().await;
/// # }
/// ```
pub struct TestCluster {
    network: MockNetwork,
    server: Option<Node>,
    agents: Vec<Node>,
}

/// Configures a [`TestCluster`] before it starts.
pub struct ClusterBuilder {
    network: MockNetwork,
    server: Option<Configure>,
    agents: Vec<(String, Configure)>,
    admin: bool,
}

/// Adds plugins to one engine under construction.
type Configure = Box<dyn FnOnce(EngineBuilder<MockTransport>) -> EngineBuilder<MockTransport>>;

impl TestCluster {
    /// Start configuring a cluster.
    #[must_use]
    pub fn builder() -> ClusterBuilder {
        ClusterBuilder {
            network: MockNetwork::new(),
            server: None,
            agents: Vec::new(),
            admin: false,
        }
    }

    /// Where the cluster's server listens.
    #[must_use]
    pub fn server_endpoint() -> Endpoint {
        MockNetwork::endpoint(SERVER)
    }

    /// Where the cluster's server accepts operators.
    #[must_use]
    pub fn admin_endpoint() -> Endpoint {
        MockNetwork::endpoint(ADMIN)
    }

    /// Connect an operator to the cluster's admin endpoint.
    ///
    /// Its heartbeat is set to match [`test_config`]'s timings: a head there gives
    /// up on a peer that has been silent for 60ms, and an operator waiting for an
    /// answer is silent.
    ///
    /// # Panics
    ///
    /// If the cluster was not built with [`ClusterBuilder::admin`], or the
    /// connection fails — either way, a mistake in the test.
    pub async fn operator(&self, name: &str) -> TestOperator {
        self.connect_as(name, &Self::admin_endpoint()).await
    }

    /// Connect to any of the cluster's endpoints as `name`.
    ///
    /// For the test that checks an *agent* cannot command another node: same client,
    /// wrong door.
    ///
    /// Retries while the endpoint refuses, because the engines are tasks and a
    /// listener binds when its task first runs — a connect immediately after
    /// [`ClusterBuilder::start`] can beat it there.
    ///
    /// # Panics
    ///
    /// If nothing is listening within [`PATIENCE`].
    pub async fn connect_as(&self, name: &str, at: &Endpoint) -> TestOperator {
        let give_up = Instant::now() + PATIENCE;
        loop {
            match cs_operator::connect(&self.network.transport(), at, name).await {
                Ok(operator) => return operator.with_heartbeat(Duration::from_millis(10)),
                Err(err) => {
                    assert!(
                        Instant::now() < give_up,
                        "nothing accepted a connection at {at}: {err:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }

    /// The network under the cluster, for breaking things.
    #[must_use]
    pub fn network(&self) -> &MockNetwork {
        &self.network
    }

    /// The server, if this cluster has one.
    #[must_use]
    pub fn server(&self) -> Option<&Node> {
        self.server.as_ref()
    }

    /// The agents, in the order they were added.
    #[must_use]
    pub fn agents(&self) -> &[Node] {
        &self.agents
    }

    /// The only agent.
    ///
    /// # Panics
    ///
    /// If the cluster has no agents, or more than one.
    #[must_use]
    pub fn agent(&self) -> &Node {
        match self.agents.as_slice() {
            [only] => only,
            other => panic!(
                "expected exactly one agent, this cluster has {}",
                other.len()
            ),
        }
    }

    /// The link the only agent is currently using.
    ///
    /// A reconnect makes a new link, so take this again after breaking one.
    ///
    /// # Panics
    ///
    /// If nothing has connected yet.
    #[must_use]
    pub fn link(&self) -> Link {
        self.network
            .last_link()
            .expect("nothing has connected to the cluster yet")
    }

    /// Take the only agent out of the cluster, to stop it by itself.
    ///
    /// For a test about what happens *to the rest* when one engine stops.
    ///
    /// # Panics
    ///
    /// If the cluster does not have exactly one agent.
    #[must_use]
    pub fn take_agent(&mut self) -> Node {
        assert_eq!(
            self.agents.len(),
            1,
            "take_agent needs exactly one agent to take"
        );
        self.agents.remove(0)
    }

    /// Take the server out of the cluster, to stop it while the agents run on.
    ///
    /// # Panics
    ///
    /// If the cluster has no server.
    #[must_use]
    pub fn take_server(&mut self) -> Node {
        self.server.take().expect("this cluster has no server")
    }

    /// Break every link, as a switch going down would.
    pub fn partition(&self) {
        self.network.kill_all();
    }

    /// Wait until `condition` holds.
    ///
    /// # Panics
    ///
    /// If it has not held within ten seconds, naming `what` — which is why `what`
    /// should read as the thing being waited for.
    pub async fn wait_for(&self, what: &str, mut condition: impl FnMut() -> bool) {
        wait_for(what, condition_fn(&mut condition)).await;
    }

    /// Stop the agents, then the server, and check every engine stopped cleanly.
    ///
    /// Agents first on purpose: a server that goes first would make its agents
    /// reconnect, and the test would be racing its own teardown.
    pub async fn stop(self) {
        for agent in self.agents {
            agent.stop().await;
        }
        if let Some(server) = self.server {
            server.stop().await;
        }
    }
}

impl ClusterBuilder {
    /// Also accept operators, so [`TestCluster::operator`] can connect.
    #[must_use]
    pub fn admin(mut self) -> Self {
        self.admin = true;
        self
    }

    /// Add the server, configuring its handlers.
    #[must_use]
    pub fn server(
        mut self,
        configure: impl FnOnce(EngineBuilder<MockTransport>) -> EngineBuilder<MockTransport> + 'static,
    ) -> Self {
        self.server = Some(Box::new(configure));
        self
    }

    /// Add an agent called `node`, configuring its samplers.
    #[must_use]
    pub fn agent(
        mut self,
        node: &str,
        configure: impl FnOnce(EngineBuilder<MockTransport>) -> EngineBuilder<MockTransport> + 'static,
    ) -> Self {
        self.agents.push((node.to_owned(), Box::new(configure)));
        self
    }

    /// Add `count` agents named `node-1`, `node-2`, … all configured the same way.
    #[must_use]
    pub fn agents(
        mut self,
        count: usize,
        configure: impl Fn(EngineBuilder<MockTransport>) -> EngineBuilder<MockTransport>
        + Clone
        + 'static,
    ) -> Self {
        for index in 1..=count {
            self = self.agent(&format!("node-{index}"), configure.clone());
        }
        self
    }

    /// Cap the frame size, to make the engine chunk.
    #[must_use]
    pub fn max_frame(mut self, bytes: usize) -> Self {
        self.network = MockNetwork::with_capacity(8).with_max_frame(bytes);
        self
    }

    /// Hold this many frames per link direction before a sender blocks.
    #[must_use]
    pub fn link_capacity(mut self, frames: usize) -> Self {
        self.network = MockNetwork::with_capacity(frames);
        self
    }

    /// Build and start everything.
    ///
    /// The server goes up first so the agents connect immediately — a test about
    /// reconnection should leave the server out and add it later.
    ///
    /// # Panics
    ///
    /// If any engine fails to build, which in a test is a mistake in the test.
    pub async fn start(self) -> TestCluster {
        let at = TestCluster::server_endpoint();

        let admin = self.admin;
        let server = self.server.map(|configure| {
            let mut builder = NodeEngine::builder(self.network.transport())
                .config(test_config("head01"))
                .listen(at.clone());
            if admin {
                builder = builder.admin(TestCluster::admin_endpoint());
            }
            let engine = configure(builder).build().expect("the server should build");
            spawn("head01", engine)
        });

        let agents = self
            .agents
            .into_iter()
            .map(|(node, configure)| {
                let builder = NodeEngine::builder(self.network.transport())
                    .config(test_config(&node))
                    .dial(at.clone());
                let engine = configure(builder)
                    .build()
                    .unwrap_or_else(|err| panic!("agent {node} should build: {err:?}"));
                spawn(&node, engine)
            })
            .collect();

        TestCluster {
            network: self.network,
            server,
            agents,
        }
    }
}

/// Start an engine as a task.
fn spawn(name: &str, engine: NodeEngine<MockTransport>) -> Node {
    let handle = engine.handle();
    Node {
        name: name.to_owned(),
        handle,
        task: tokio::spawn(engine.run()),
    }
}

/// Reborrow a closure so `wait_for` can take `&mut dyn FnMut`.
fn condition_fn(condition: &mut impl FnMut() -> bool) -> &mut dyn FnMut() -> bool {
    condition
}

/// Poll `condition` until it holds, or panic.
///
/// Polling rather than signalling on purpose: a test asserts on what the system
/// *did*, and wiring notifications into the engine for a test's benefit would mean
/// testing something other than production code.
///
/// # Panics
///
/// If `condition` has not held within ten seconds.
pub async fn wait_for(what: &str, condition: &mut dyn FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("timed out after {PATIENCE:?} waiting for {what}");
}

/// A shared counter, for a test that needs one.
pub type Shared<T> = Arc<std::sync::Mutex<T>>;
