//! Three tiers: agent → relay → head.
//!
//! A relay is an engine in both roles, which the engine has always supported. What
//! these tests cover is the part that did not exist: the relay telling the head which
//! nodes are behind it, and the head sending a command down through it to one of them.
//!
//! Built by hand rather than through `TestCluster`, because that harness points every
//! agent at one endpoint and the whole point here is that they do not.

use std::time::Duration;

use cs_api::ServiceDef;
use cs_engine::{EngineHandle, NodeEngine, Stop};
use cs_operator::Request;
use cs_testkit::{CounterConfig, CounterService, TestOperator, test_config, wait_for};
use cs_transport::{Endpoint, Outcome, StatusReport};
use cs_transport_mock::{MockNetwork, MockTransport};
use cs_util::ErrorKind;

/// How long to keep asking the head before deciding it will never know.
const PATIENCE: Duration = Duration::from_secs(5);

/// One running engine, with the handle to stop it.
struct Tier {
    handle: EngineHandle,
    task: tokio::task::JoinHandle<cs_util::Result<Stop>>,
}

impl Tier {
    fn spawn(engine: NodeEngine<MockTransport>) -> Self {
        Self {
            handle: engine.handle(),
            task: tokio::spawn(engine.run()),
        }
    }

    async fn stop(self) {
        self.handle.shutdown();
        let _ = self.task.await;
    }
}

fn at(name: &str) -> Endpoint {
    MockNetwork::endpoint(name)
}

/// An operator on the head's admin endpoint.
///
/// Retries while the endpoint refuses: each engine is a task, and a listener binds
/// when its task first runs, so connecting straight after `Tiers::start` can beat it.
async fn operator(network: &MockNetwork) -> TestOperator {
    let give_up = tokio::time::Instant::now() + PATIENCE;
    loop {
        match cs_operator::connect(&network.transport(), &at("head-admin"), "ctl@test").await {
            Ok(operator) => return operator.with_heartbeat(Duration::from_millis(10)),
            Err(err) => {
                assert!(
                    tokio::time::Instant::now() < give_up,
                    "the head never accepted an operator: {err:?}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

/// Ask the head what it knows, until `settled` is happy with the answer.
///
/// Reachability is announced rather than requested, so it arrives when it arrives —
/// this is the one thing in these tests that has to be waited for rather than
/// asserted outright.
#[expect(
    clippy::expect_used,
    reason = "harness code: a broken assumption here is a mistake in the test, and \
              panicking is how it should say so — the same exemption cs-testkit has"
)]
async fn status_until(
    network: &MockNetwork,
    what: &str,
    settled: impl Fn(&StatusReport) -> bool,
) -> StatusReport {
    let mut operator = operator(network).await;
    let give_up = tokio::time::Instant::now() + PATIENCE;
    loop {
        let report = operator
            .status(Duration::from_secs(2))
            .await
            .expect("the head should answer a status request");
        if settled(&report) {
            operator.close().await;
            return report;
        }
        assert!(
            tokio::time::Instant::now() < give_up,
            "waiting for {what}; the head last said {:?}",
            report.nodes
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// head ← relay ← agent, with an admin endpoint on the head.
///
/// Timings come from `test_config`, so nothing here waits on a real clock for long.
struct Tiers {
    network: MockNetwork,
    head: Tier,
    relay: Tier,
    agent: Tier,
    counter: CounterConfig,
}

impl Tiers {
    #[expect(
        clippy::expect_used,
        reason = "harness code: a broken assumption here is a mistake in the test, and \
                  panicking is how it should say so — the same exemption cs-testkit has"
    )]
    fn start() -> Self {
        let network = MockNetwork::new();
        let counter = CounterConfig::new();

        let head = Tier::spawn(
            NodeEngine::builder(network.transport())
                .config(test_config("head01"))
                .listen(at("head"))
                .admin(at("head-admin"))
                .build()
                .expect("the head should build"),
        );

        // Both roles at once, which is the whole of what makes a relay.
        let relay = Tier::spawn(
            NodeEngine::builder(network.transport())
                .config(test_config("relay-a"))
                .listen(at("relay-a"))
                .dial(at("head"))
                .build()
                .expect("the relay should build"),
        );

        let agent = Tier::spawn(
            NodeEngine::builder(network.transport())
                .config(test_config("node-1"))
                .dial(at("relay-a"))
                .sampler(counter.factory())
                .build()
                .expect("the agent should build"),
        );

        Self {
            network,
            head,
            relay,
            agent,
            counter,
        }
    }

    async fn stop(self) {
        self.agent.stop().await;
        self.relay.stop().await;
        self.head.stop().await;
    }
}

/// The announcement itself: the head learns about a node it has never spoken to.
#[tokio::test]
async fn a_head_learns_which_nodes_are_behind_a_relay() {
    let tiers = Tiers::start();

    let report = status_until(&tiers.network, "node-1 to be announced", |report| {
        report.nodes.iter().any(|node| node.node == "node-1")
    })
    .await;

    assert_eq!(report.node, "head01", "the head answers for itself");

    let relay = report
        .nodes
        .iter()
        .find(|node| node.node == "relay-a")
        .expect("the relay is a direct peer");
    assert!(relay.is_direct());
    assert_eq!(relay.hops, 1);
    assert!(
        relay.connected.is_some(),
        "the head knows how long its own peer has been there"
    );

    let agent = report
        .nodes
        .iter()
        .find(|node| node.node == "node-1")
        .expect("checked above");
    assert!(!agent.is_direct());
    assert_eq!(agent.via, "relay-a", "and which child it is behind");
    assert_eq!(agent.hops, 2);
    assert_eq!(
        agent.connected, None,
        "only the tier a node is attached to knows how long it has been there"
    );
    assert!(
        agent
            .services
            .iter()
            .any(|s| s.name == CounterService::NAME),
        "what a node runs travels with it: {:?}",
        agent.services
    );

    tiers.stop().await;
}

/// The point of the table: a command addressed to a node two tiers down arrives, and
/// its answer comes back up both hops.
#[tokio::test]
async fn a_command_is_routed_down_through_a_relay() {
    let tiers = Tiers::start();
    status_until(&tiers.network, "node-1 to be announced", |report| {
        report.nodes.iter().any(|node| node.node == "node-1")
    })
    .await;
    let built_once = tiers.counter.builds();

    let mut operator = operator(&tiers.network).await;
    let answers = operator
        .run(
            vec![Request::restart("node-1", Some(CounterService::NAME))],
            Duration::from_secs(5),
        )
        .await
        .expect("the head should answer");
    operator.close().await;

    assert_eq!(answers[0].outcome, Some(Outcome::Ok));

    let counter = tiers.counter.clone();
    wait_for("the rebuild on the agent", &mut || {
        counter.builds() > built_once
    })
    .await;

    tiers.stop().await;
}

/// A node nobody announced is refused at once, rather than queued until a time to
/// live runs out.
#[tokio::test]
async fn a_node_behind_nobody_has_no_route() {
    let tiers = Tiers::start();

    let mut operator = operator(&tiers.network).await;
    let answers = operator
        .run(
            vec![Request::restart("node-404", Some(CounterService::NAME))],
            Duration::from_secs(5),
        )
        .await
        .expect("the head should answer");
    operator.close().await;

    let Some(Outcome::Failed(trace)) = &answers[0].outcome else {
        panic!("expected a failure, got {:?}", answers[0].outcome);
    };
    let err = trace.to_error();
    assert_eq!(err.kind(), ErrorKind::Rejected);
    assert!(err.to_string().contains("no route"), "{err}");

    tiers.stop().await;
}

/// When a relay goes, so does everything behind it. A route that outlived its child
/// would have the head answering for nodes it cannot reach.
#[tokio::test]
async fn losing_a_relay_retires_the_nodes_behind_it() {
    let Tiers {
        network,
        head,
        relay,
        agent,
        counter: _counter,
    } = Tiers::start();

    status_until(&network, "node-1 to be announced", |report| {
        report.nodes.iter().any(|node| node.node == "node-1")
    })
    .await;

    // Stop the relay, leaving the agent dialling something that is not there.
    relay.stop().await;

    status_until(&network, "node-1 to be forgotten", |report| {
        report.nodes.is_empty()
    })
    .await;

    // And a command for it is refused rather than sent into a route that is gone.
    let mut operator = operator(&network).await;
    let answers = operator
        .run(
            vec![Request::restart("node-1", Some(CounterService::NAME))],
            Duration::from_secs(5),
        )
        .await
        .expect("the head should answer");
    operator.close().await;

    assert!(
        matches!(answers[0].outcome, Some(Outcome::Failed(_))),
        "got {:?}",
        answers[0].outcome
    );

    agent.stop().await;
    head.stop().await;
}
