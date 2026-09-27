//! Test infrastructure for everything below the plugin layer.
//!
//! Three things, in increasing order of scope:
//!
//! - **[Plugins that misbehave](plugins)** — a sampler that panics, refuses a
//!   restart, fails a sample, or dawdles in `on_shutdown`, plus handlers that
//!   record what arrives or command the node that sent it. One configurable
//!   sampler rather than five near-identical ones, because they differ only in
//!   which knob is set.
//! - **[`TestCluster`]** — a server and any number of agents in this process, over
//!   the mock transport, with the links between them under the test's control.
//! - **[The transport contract suite](contract)** — the guarantees the engine is
//!   written against. A new transport is done when
//!   [`transport_contract!`] passes.
//!
//! ```no_run
//! use cs_testkit::{Collect, CounterConfig, CounterService, TestCluster};
//!
//! # async fn example() {
//! let collected = Collect::<CounterService>::new();
//! let counter = CounterConfig::new().panics_on_sample(3);
//!
//! let cluster = TestCluster::builder()
//!     .server({
//!         let collected = collected.clone();
//!         move |server| server.handler(collected)
//!     })
//!     .agent("node-1", {
//!         let counter = counter.clone();
//!         move |agent| agent.sampler(counter.factory())
//!     })
//!     .start()
//!     .await;
//!
//! // It panics, is rebuilt from its factory, and carries on.
//! cluster.wait_for("a rebuild", || counter.builds() >= 2).await;
//! cluster.stop().await;
//! # }
//! ```
//!
//! Everything here panics rather than returning errors: in a test a broken
//! assumption is a bug in the test, and a panic names it on the spot — which is
//! why this one crate is exempt from the workspace's ban on `unwrap`/`expect`.

// A harness exists to fail loudly. Nothing outside this crate may do this.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "a test harness reports a broken assumption by panicking"
)]

pub mod cluster;
pub mod contract;
pub mod plugins;

pub use cluster::{ADMIN, Node, SERVER, TestCluster, TestOperator, test_config, wait_for};
pub use contract::TestTransport;
pub use plugins::{
    BulkSampler, BulkService, Collect, Commander, CounterConfig, CounterSampler, CounterService,
};

/// A [`TestTransport`] fixture for the mock transport.
///
/// Each fixture owns its own [`MockNetwork`](cs_transport_mock::MockNetwork), so
/// contract tests cannot collide on an endpoint name.
#[derive(Debug)]
pub struct MockFixture {
    network: cs_transport_mock::MockNetwork,
    next: std::sync::atomic::AtomicU32,
}

impl MockFixture {
    /// A fixture on a fresh network.
    #[must_use]
    pub fn new() -> Self {
        Self {
            // Small, so the backpressure check has something to observe.
            network: cs_transport_mock::MockNetwork::with_capacity(4),
            next: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// The network under the fixture, for a test that wants to break a link.
    #[must_use]
    pub fn network(&self) -> &cs_transport_mock::MockNetwork {
        &self.network
    }
}

impl Default for MockFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl TestTransport for MockFixture {
    type Transport = cs_transport_mock::MockTransport;

    fn transport(&self) -> Self::Transport {
        self.network.transport()
    }

    fn fresh_endpoint(&self) -> cs_transport::Endpoint {
        let index = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        cs_transport_mock::MockNetwork::endpoint(&format!("peer-{index}"))
    }

    fn shows_backpressure(&self) -> bool {
        true
    }
}
