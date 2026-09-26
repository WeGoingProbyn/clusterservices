//! The shared transport contract, run against TCP.
//!
//! The same checks the mock transport passes, over real sockets. That is the whole
//! point of the suite: the engine is written against these guarantees, so if both
//! transports satisfy them, swapping one for the other needs no engine change.

// Test code reports a broken assumption by panicking; the lint exemption in
// `clippy.toml` only reaches inside test functions, not the helpers beside them.
#![allow(
    clippy::expect_used,
    reason = "a test helper should fail loudly and name what went wrong"
)]

use std::net::TcpListener as StdListener;

use cs_testkit::TestTransport;
use cs_transport::Endpoint;
use cs_transport_tcp::TcpTransport;

/// Supplies TCP endpoints for the contract suite.
struct TcpFixture {
    /// Small enough that `a_frame_at_the_ceiling_is_carried` sends something
    /// reasonable rather than four megabytes.
    max_frame: usize,
}

impl TcpFixture {
    fn new() -> Self {
        Self {
            max_frame: 64 * 1024,
        }
    }
}

impl TestTransport for TcpFixture {
    type Transport = TcpTransport;

    fn transport(&self) -> Self::Transport {
        TcpTransport::new().with_max_frame(self.max_frame)
    }

    /// A port the kernel has just confirmed is free.
    ///
    /// Binding to port 0 and asking what we got would not do: the suite also dials
    /// a fresh endpoint expecting nothing to be there, and port 0 cannot be
    /// dialled. So take a real port, let it go, and hand it over — briefly racy in
    /// principle, reliable in practice on a test machine.
    fn fresh_endpoint(&self) -> Endpoint {
        let probe = StdListener::bind("127.0.0.1:0").expect("the kernel should spare a port");
        let address = probe.local_addr().expect("a bound socket has an address");
        drop(probe);
        Endpoint::from_parts("tcp", &address.to_string())
    }

    /// TCP's own scheme is not foreign, so use the mock's.
    fn foreign_endpoint(&self) -> Endpoint {
        Endpoint::from_parts("mock", "server")
    }

    /// Left at the default `false`: the kernel's socket buffers will absorb far
    /// more than a test wants to send, so "the sender blocks" cannot be shown in a
    /// handful of frames. The check skips itself rather than pretend.
    fn shows_backpressure(&self) -> bool {
        false
    }
}

cs_testkit::transport_contract!(TcpFixture::new());
