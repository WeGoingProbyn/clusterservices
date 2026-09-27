//! An in-process [`Transport`](cs_transport::Transport) built to misbehave.
//!
//! Everything below the plugin layer is verified against this before it ever sees
//! a socket, so it is deliberately hostile — a mock that only works is a mock that
//! proves nothing. It can:
//!
//! - **Apply real backpressure.** Links are bounded queues, so a sender whose peer
//!   stops reading blocks, exactly as it would on a socket.
//! - **Break a connection at any moment.** [`Link::kill`] fails both halves with a
//!   retryable error, losing whatever was in flight.
//! - **Refuse connections**, once, a set number of times, or until told otherwise
//!   — and [`MockNetwork::kill_on_connect`] hands back a connection that is
//!   already dead, which is the case code usually gets wrong.
//! - **Be slow.** [`MockNetwork::set_send_delay`] delays every send on the
//!   runtime's timer, so a paused-time test controls it exactly.
//! - **Speak nonsense.** [`Link::inject_garbage`] pushes bytes that are not a
//!   frame; the reader gets a [`Decode`](cs_util::ErrorKind::Decode) error and must
//!   carry on rather than reconnect.
//! - **Enforce its frame ceiling.** An oversized frame fails to send, so an engine
//!   that forgets to chunk fails here rather than in production.
//!
//! Frames are **serialised to bytes** on the way through, like a real transport,
//! so every test exercises the encode/decode path rather than passing Rust values
//! around.
//!
//! # Scoping
//!
//! A [`MockNetwork`] is a value, not a global. Each test makes its own, so two
//! tests can both bind `mock://server` and nothing leaks between them.
//!
//! ```
//! use cs_transport::{Connection, Frame, FrameRx, FrameTx, Heartbeat, Listener, Transport};
//! use cs_transport_mock::{Direction, MockNetwork};
//! use std::time::SystemTime;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), cs_util::Error> {
//! let network = MockNetwork::new();
//! let transport = network.transport();
//! let at = MockNetwork::endpoint("server");
//!
//! let listener = transport.listen(&at).await?;
//! let (client, server) = (transport.connect(&at).await?, listener.accept().await?);
//! let (mut tx, _) = client.split();
//! let (_, mut rx) = server.split();
//!
//! tx.send(Frame::Heartbeat(Heartbeat::at(SystemTime::UNIX_EPOCH))).await?;
//! rx.recv().await?.expect("a frame");
//!
//! let link = network.last_link().expect("a link");
//! assert_eq!(link.frames_sent(Direction::ToServer), 1);
//!
//! // Anything the real world can do to a connection, a test can do here.
//! link.inject_garbage(Direction::ToServer)?;
//! assert!(rx.recv().await.is_err());
//! # Ok(())
//! # }
//! ```

mod conn;
mod link;
mod network;

pub use conn::{MockConnection, MockRx, MockTx};
pub use link::{Direction, Link};
pub use network::{MockListener, MockNetwork, MockTransport, SCHEME};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use cs_transport::{
        Connection, DataFrame, Frame, FrameRx, FrameTx, Hello, Listener, Transport,
    };
    use cs_util::ErrorKind;
    use std::time::Duration;

    /// A server listening at `mock://server`, plus one connected client.
    async fn pair(network: &MockNetwork) -> (MockListener, MockConnection, MockConnection) {
        let transport = network.transport();
        let at = MockNetwork::endpoint("server");
        let listener = transport.listen(&at).await.expect("listen");
        let client = transport.connect(&at).await.expect("connect");
        let server = listener.accept().await.expect("accept");
        (listener, client, server)
    }

    fn data(payload: &'static [u8]) -> Frame {
        Frame::Data(DataFrame::new("cgroup", 1, Bytes::from_static(payload)))
    }

    #[tokio::test]
    async fn frames_cross_in_both_directions_and_arrive_whole() {
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (mut client_tx, mut client_rx) = client.split();
        let (mut server_tx, mut server_rx) = server.split();

        client_tx
            .send(Frame::Hello(Hello::new("node-1")))
            .await
            .expect("send hello");
        let Some(Frame::Hello(hello)) = server_rx.recv().await.expect("recv") else {
            panic!("expected hello");
        };
        assert_eq!(hello.node, "node-1");

        server_tx.send(data(b"counters")).await.expect("send data");
        let Some(Frame::Data(got)) = client_rx.recv().await.expect("recv") else {
            panic!("expected data");
        };
        assert_eq!(got.payload, Bytes::from_static(b"counters"));
    }

    #[tokio::test]
    async fn frames_keep_their_order() {
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (mut tx, _) = client.split();
        let (_, mut rx) = server.split();

        for i in 0..8u32 {
            tx.send(Frame::Data(DataFrame::new(
                "cgroup",
                i,
                Bytes::from_static(b"x"),
            )))
            .await
            .expect("send");
        }
        for i in 0..8u32 {
            let Some(Frame::Data(got)) = rx.recv().await.expect("recv") else {
                panic!("expected data");
            };
            assert_eq!(got.service_version, i, "frames arrived out of order");
        }
    }

    #[tokio::test]
    async fn the_endpoints_report_each_other() {
        let network = MockNetwork::new();
        let (listener, client, server) = pair(&network).await;
        assert_eq!(
            listener.local_endpoint().expect("local").as_str(),
            "mock://server"
        );
        assert_eq!(client.peer().as_str(), "mock://server");
        assert!(
            server.peer().as_str().starts_with("mock://client-"),
            "got {}",
            server.peer()
        );
    }

    // --- clean close vs. broken close ---

    #[tokio::test]
    async fn a_closed_half_reads_as_end_of_stream() {
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (mut tx, _) = client.split();
        let (_, mut rx) = server.split();

        tx.send(data(b"last")).await.expect("send");
        tx.close().await.expect("close");

        assert!(
            rx.recv().await.expect("recv").is_some(),
            "queued frame first"
        );
        assert!(rx.recv().await.expect("recv").is_none(), "then clean close");
        assert!(
            rx.recv().await.expect("recv").is_none(),
            "and it stays closed"
        );
    }

    #[tokio::test]
    async fn a_dropped_half_is_a_broken_connection_not_a_clean_close() {
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (tx, _) = client.split();
        let (_, mut rx) = server.split();

        drop(tx);

        let err = rx.recv().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Transport);
        assert!(
            err.is_retryable(),
            "a dropped peer should be reconnected to"
        );
        assert!(err.to_string().contains("without closing"));
    }

    #[tokio::test]
    async fn sending_after_close_fails() {
        let network = MockNetwork::new();
        let (_listener, client, _server) = pair(&network).await;
        let (mut tx, _) = client.split();
        tx.close().await.expect("close");
        assert_eq!(
            tx.send(data(b"too late")).await.unwrap_err().kind(),
            ErrorKind::Transport
        );
    }

    // --- killing a link ---

    #[tokio::test]
    async fn killing_a_link_fails_both_halves_retryably() {
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (mut client_tx, mut client_rx) = client.split();
        let (mut server_tx, mut server_rx) = server.split();

        let link = network.last_link().expect("link");
        assert!(link.is_alive());
        link.kill();
        assert!(!link.is_alive());

        for err in [
            client_tx.send(data(b"x")).await.unwrap_err(),
            server_tx.send(data(b"x")).await.unwrap_err(),
            client_rx.recv().await.unwrap_err(),
            server_rx.recv().await.unwrap_err(),
        ] {
            assert_eq!(err.kind(), ErrorKind::Transport);
            assert!(err.is_retryable());
        }
        // Killing twice is harmless.
        link.kill();
    }

    #[tokio::test]
    async fn a_kill_loses_whatever_was_in_flight() {
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (mut tx, _) = client.split();
        let (_, mut rx) = server.split();

        tx.send(data(b"never arrives")).await.expect("send");
        network.last_link().expect("link").kill();

        // The frame was queued, but a broken connection does not deliver it.
        assert!(rx.recv().await.is_err());
    }

    #[tokio::test]
    async fn a_reader_already_waiting_is_woken_by_a_kill() {
        let network = MockNetwork::new();
        let (_listener, _client, server) = pair(&network).await;
        let (_, mut rx) = server.split();

        let link = network.last_link().expect("link");
        let waiter = tokio::spawn(async move { rx.recv().await });

        // Let it block on an empty link before breaking it.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!waiter.is_finished(), "should still be waiting");

        link.kill();
        let err = waiter.await.expect("task").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Transport);
    }

    #[tokio::test]
    async fn kill_all_takes_down_every_link() {
        let network = MockNetwork::new();
        let transport = network.transport();
        let at = MockNetwork::endpoint("server");
        let listener = transport.listen(&at).await.expect("listen");
        // Held, not dropped: a dropped connection is a broken one, which would
        // muddy what `kill_all` is being asked to do here.
        let _connections: Vec<_> = {
            let mut held = Vec::new();
            for _ in 0..3 {
                held.push((
                    transport.connect(&at).await.expect("connect"),
                    listener.accept().await.expect("accept"),
                ));
            }
            held
        };
        assert_eq!(network.link_count(), 3);
        assert_eq!(network.live_link_count(), 3);

        network.kill_all();
        assert_eq!(network.live_link_count(), 0);
        assert_eq!(network.link_count(), 3, "dead links are still listed");
    }

    // --- refusing connections ---

    #[tokio::test]
    async fn connecting_to_nothing_is_refused_retryably() {
        let network = MockNetwork::new();
        let err = network
            .transport()
            .connect(&MockNetwork::endpoint("absent"))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Transport);
        assert!(
            err.is_retryable(),
            "an agent dialling before the server is up must retry"
        );
        assert!(err.to_string().contains("nothing is listening"));
    }

    #[tokio::test]
    async fn a_foreign_endpoint_is_a_configuration_error() {
        let network = MockNetwork::new();
        let tcp = "tcp://head01:7777".parse().expect("parse");
        assert_eq!(
            network.transport().connect(&tcp).await.unwrap_err().kind(),
            ErrorKind::Config
        );
        assert_eq!(
            network.transport().listen(&tcp).await.unwrap_err().kind(),
            ErrorKind::Config
        );
    }

    #[tokio::test]
    async fn refusals_can_be_made_to_stop() {
        let network = MockNetwork::new();
        let transport = network.transport();
        let at = MockNetwork::endpoint("server");
        let _listener = transport.listen(&at).await.expect("listen");

        network.refuse_connects_times("server", 3);
        for attempt in 1..=3 {
            assert!(
                transport.connect(&at).await.is_err(),
                "attempt {attempt} should be refused"
            );
        }
        assert!(transport.connect(&at).await.is_ok(), "then it lets us in");
        assert_eq!(network.connect_attempts("server"), 4);

        network.refuse_connects("server");
        assert!(transport.connect(&at).await.is_err());
        assert!(transport.connect(&at).await.is_err());
        network.allow_connects("server");
        assert!(transport.connect(&at).await.is_ok());
    }

    #[tokio::test]
    async fn kill_on_connect_hands_back_a_dead_connection() {
        let network = MockNetwork::new();
        let transport = network.transport();
        let at = MockNetwork::endpoint("server");
        let _listener = transport.listen(&at).await.expect("listen");

        network.kill_on_connect("server");
        // `connect` itself succeeds — that is the trap.
        let client = transport.connect(&at).await.expect("connect");
        let (mut tx, _) = client.split();
        assert!(tx.send(data(b"x")).await.is_err());
    }

    #[tokio::test]
    async fn binding_the_same_endpoint_twice_is_rejected() {
        let network = MockNetwork::new();
        let transport = network.transport();
        let at = MockNetwork::endpoint("server");
        let first = transport.listen(&at).await.expect("first listen");
        assert_eq!(
            transport.listen(&at).await.unwrap_err().kind(),
            ErrorKind::Config
        );

        // Dropping the listener frees the address, like a real socket.
        drop(first);
        assert!(transport.listen(&at).await.is_ok());
    }

    #[tokio::test]
    async fn a_dropped_listener_refuses_new_connections() {
        let network = MockNetwork::new();
        let transport = network.transport();
        let at = MockNetwork::endpoint("server");
        let listener = transport.listen(&at).await.expect("listen");
        drop(listener);
        assert!(transport.connect(&at).await.is_err());
    }

    // --- backpressure ---

    #[tokio::test]
    async fn a_sender_blocks_once_the_queue_is_full() {
        let network = MockNetwork::with_capacity(2);
        let (_listener, client, server) = pair(&network).await;
        let (mut tx, _) = client.split();
        let (_, mut rx) = server.split();
        let link = network.last_link().expect("link");

        let sender = tokio::spawn(async move {
            for _ in 0..5 {
                tx.send(data(b"x")).await.expect("send");
            }
        });

        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(
            !sender.is_finished(),
            "the sender should be blocked on a full queue"
        );
        assert_eq!(link.queued(Direction::ToServer), 2, "queue is at capacity");
        assert_eq!(
            link.frames_sent(Direction::ToServer),
            2,
            "only what fit has been sent"
        );

        for _ in 0..5 {
            rx.recv().await.expect("recv").expect("frame");
        }
        sender.await.expect("sender task");
        assert_eq!(link.frames_sent(Direction::ToServer), 5);
        assert_eq!(link.frames_received(Direction::ToServer), 5);
        assert_eq!(link.queued(Direction::ToServer), 0);
    }

    #[tokio::test]
    async fn the_two_directions_do_not_share_a_queue() {
        let network = MockNetwork::with_capacity(1);
        let (_listener, client, server) = pair(&network).await;
        let (mut client_tx, mut client_rx) = client.split();
        let (mut server_tx, mut server_rx) = server.split();

        // Fill each direction; neither should stop the other.
        client_tx.send(data(b"a")).await.expect("to server");
        server_tx.send(data(b"b")).await.expect("to client");

        assert!(server_rx.recv().await.expect("recv").is_some());
        assert!(client_rx.recv().await.expect("recv").is_some());
    }

    // --- nonsense on the wire ---

    #[tokio::test]
    async fn injected_garbage_is_a_decode_error_that_does_not_kill_the_link() {
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (mut tx, _) = client.split();
        let (_, mut rx) = server.split();
        let link = network.last_link().expect("link");

        link.inject_garbage(Direction::ToServer).expect("inject");
        let err = rx.recv().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(
            !err.is_retryable(),
            "reading the same bytes again cannot help"
        );

        // The connection itself is fine, and the engine must be able to carry on.
        assert!(link.is_alive());
        tx.send(data(b"after garbage")).await.expect("send");
        assert!(rx.recv().await.expect("recv").is_some());
    }

    #[tokio::test]
    async fn injected_bytes_can_be_a_frame_the_peer_never_sent() {
        let network = MockNetwork::new();
        let (_listener, _client, server) = pair(&network).await;
        let (_, mut rx) = server.split();
        let link = network.last_link().expect("link");

        let forged = Frame::Hello(Hello::new("not-really-this-node")).encode();
        link.inject(Direction::ToServer, forged).expect("inject");

        let Some(Frame::Hello(hello)) = rx.recv().await.expect("recv") else {
            panic!("expected hello");
        };
        assert_eq!(hello.node, "not-really-this-node");
    }

    #[tokio::test]
    async fn a_blackholed_direction_swallows_frames_while_looking_healthy() {
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (mut tx, _) = client.split();
        let (_, mut rx) = server.split();
        let link = network.last_link().expect("link");

        link.blackhole(Direction::ToServer);
        assert!(link.is_blackholed(Direction::ToServer));

        // The send succeeds — that is the trap. A half-open connection accepts
        // writes and delivers nothing, and the sender cannot tell.
        tx.send(data(b"into the void")).await.expect("send");
        tx.send(data(b"also lost")).await.expect("send");
        assert!(link.is_alive(), "nothing broke");
        assert_eq!(
            link.frames_sent(Direction::ToServer),
            2,
            "the sender believes both went"
        );
        assert_eq!(
            link.frames_received(Direction::ToServer),
            0,
            "and neither did"
        );

        // The reader gets nothing at all, rather than an error.
        let waiting = tokio::spawn(async move { rx.recv().await });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(
            !waiting.is_finished(),
            "a blackholed link must be silent, not broken"
        );
        waiting.abort();
    }

    #[tokio::test]
    async fn a_blackhole_can_be_lifted_and_the_other_direction_is_unaffected() {
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (mut client_tx, mut client_rx) = client.split();
        let (mut server_tx, mut server_rx) = server.split();
        let link = network.last_link().expect("link");

        link.blackhole(Direction::ToServer);
        client_tx.send(data(b"lost")).await.expect("send");
        // The reverse direction still works, which is what makes it *half* open.
        server_tx.send(data(b"arrives")).await.expect("send");
        assert!(client_rx.recv().await.expect("recv").is_some());

        link.restore(Direction::ToServer);
        assert!(!link.is_blackholed(Direction::ToServer));
        client_tx.send(data(b"gets through")).await.expect("send");
        let Some(Frame::Data(got)) = server_rx.recv().await.expect("recv") else {
            panic!("expected data");
        };
        assert_eq!(got.payload, Bytes::from_static(b"gets through"));
    }

    #[tokio::test]
    async fn injecting_into_a_dead_link_fails() {
        let network = MockNetwork::new();
        let (_listener, _client, _server) = pair(&network).await;
        let link = network.last_link().expect("link");
        link.kill();
        assert!(link.inject_garbage(Direction::ToServer).is_err());
    }

    // --- the frame ceiling ---

    #[tokio::test]
    async fn an_oversized_frame_is_refused() {
        let network = MockNetwork::new().with_max_frame(256);
        let (_listener, client, _server) = pair(&network).await;
        let (mut tx, _) = client.split();

        let limit = DataFrame::max_payload(256, "cgroup", "", None);
        tx.send(Frame::Data(DataFrame::new(
            "cgroup",
            u32::MAX,
            Bytes::from(vec![0u8; limit]),
        )))
        .await
        .expect("a frame at the limit must fit");

        let err = tx
            .send(Frame::Data(DataFrame::new(
                "cgroup",
                u32::MAX,
                Bytes::from(vec![0u8; limit + 1]),
            )))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Transport);
        assert!(err.to_string().contains("exceeds"));
    }

    #[tokio::test]
    async fn capabilities_report_the_configured_ceiling() {
        let network = MockNetwork::new().with_max_frame(4096);
        assert_eq!(network.capabilities().name, "mock");
        assert_eq!(network.capabilities().max_frame, 4096);
        assert_eq!(network.transport().capabilities().max_frame, 4096);
        assert!(!network.capabilities().native_lanes);
    }

    // --- slowness ---

    #[tokio::test(start_paused = true)]
    async fn a_send_delay_is_paid_on_every_frame() {
        let network = MockNetwork::new();
        network.set_send_delay(Duration::from_millis(250));
        let (_listener, client, server) = pair(&network).await;
        let (mut tx, _) = client.split();
        let (_, mut rx) = server.split();

        let sender = tokio::spawn(async move {
            tx.send(data(b"slow")).await.expect("send");
        });
        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(!sender.is_finished(), "should still be in the delay");

        tokio::time::advance(Duration::from_millis(200)).await;
        sender.await.expect("sender task");
        assert!(rx.recv().await.expect("recv").is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_link_killed_mid_delay_never_delivers() {
        let network = MockNetwork::new();
        network.set_send_delay(Duration::from_secs(5));
        let (_listener, client, server) = pair(&network).await;
        let (mut tx, _) = client.split();
        let (_, mut rx) = server.split();
        let link = network.last_link().expect("link");

        let sender = tokio::spawn(async move { tx.send(data(b"in flight")).await });
        tokio::time::advance(Duration::from_secs(1)).await;
        link.kill();
        tokio::time::advance(Duration::from_secs(10)).await;

        assert!(sender.await.expect("task").is_err(), "the send should fail");
        assert!(rx.recv().await.is_err());
        assert_eq!(link.frames_sent(Direction::ToServer), 0);
    }

    // --- counters ---

    #[tokio::test]
    async fn counters_track_each_direction_separately() {
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (mut client_tx, mut client_rx) = client.split();
        let (mut server_tx, mut server_rx) = server.split();
        let link = network.last_link().expect("link");

        client_tx.send(data(b"12345")).await.expect("send");
        server_tx.send(data(b"1")).await.expect("send");
        server_tx.send(data(b"2")).await.expect("send");
        server_rx.recv().await.expect("recv").expect("frame");

        assert_eq!(link.frames_sent(Direction::ToServer), 1);
        assert_eq!(link.frames_sent(Direction::ToClient), 2);
        assert_eq!(link.frames_received(Direction::ToServer), 1);
        assert_eq!(link.frames_received(Direction::ToClient), 0);
        assert!(
            link.bytes_sent(Direction::ToServer) > 5,
            "payload plus envelope"
        );
        assert_eq!(link.id(), 1);
        assert_eq!(link.server().as_str(), "mock://server");

        client_rx.recv().await.expect("recv").expect("frame");
        assert_eq!(link.frames_received(Direction::ToClient), 1);
        assert_eq!(Direction::ToServer.flip(), Direction::ToClient);
    }

    #[tokio::test]
    async fn several_clients_get_separate_links() {
        let network = MockNetwork::new();
        let transport = network.transport();
        let at = MockNetwork::endpoint("server");
        let listener = transport.listen(&at).await.expect("listen");

        let mut connections = Vec::new();
        for _ in 0..3 {
            connections.push((
                transport.connect(&at).await.expect("connect"),
                listener.accept().await.expect("accept"),
            ));
        }
        assert_eq!(network.link_count(), 3);
        let ids: Vec<_> = network.links().iter().map(Link::id).collect();
        assert_eq!(ids, [1, 2, 3]);

        // Breaking one leaves the others alone.
        network.links()[1].kill();
        assert!(network.links()[0].is_alive());
        assert!(!network.links()[1].is_alive());
        assert!(network.links()[2].is_alive());
        assert_eq!(network.live_link_count(), 2);
    }

    #[tokio::test]
    async fn two_networks_can_use_the_same_endpoint_name() {
        let (first, second) = (MockNetwork::new(), MockNetwork::new());
        let at = MockNetwork::endpoint("server");
        let _a = first.transport().listen(&at).await.expect("first");
        let _b = second.transport().listen(&at).await.expect("second");

        first.transport().connect(&at).await.expect("connect");
        assert_eq!(first.link_count(), 1);
        assert_eq!(second.link_count(), 0, "networks are isolated");
    }

    #[tokio::test]
    async fn accepting_can_be_shared_because_it_takes_a_shared_reference() {
        let network = MockNetwork::new();
        let transport = network.transport();
        let at = MockNetwork::endpoint("server");
        let listener = std::sync::Arc::new(transport.listen(&at).await.expect("listen"));

        let accepting = {
            let listener = std::sync::Arc::clone(&listener);
            tokio::spawn(async move { listener.accept().await.map(|c| c.peer()) })
        };
        transport.connect(&at).await.expect("connect");
        let peer = accepting.await.expect("task").expect("accept");
        assert!(peer.as_str().starts_with("mock://client-"));
    }

    #[tokio::test]
    async fn the_mock_serialises_frames_rather_than_passing_them_through() {
        // Proof that the encoding is exercised: a frame arrives byte-identical but
        // as a distinct allocation, and its payload points into the received
        // buffer rather than the sent one.
        let network = MockNetwork::new();
        let (_listener, client, server) = pair(&network).await;
        let (mut tx, _) = client.split();
        let (_, mut rx) = server.split();

        let payload = Bytes::from(vec![9u8; 512]);
        let sent_ptr = payload.as_ptr() as usize;
        tx.send(Frame::Data(DataFrame::new("cgroup", 1, payload)))
            .await
            .expect("send");

        let Some(Frame::Data(got)) = rx.recv().await.expect("recv") else {
            panic!("expected data");
        };
        assert_eq!(got.payload.len(), 512);
        assert!(got.payload.iter().all(|&b| b == 9));
        assert_ne!(
            got.payload.as_ptr() as usize,
            sent_ptr,
            "the payload should have been through an encode/decode round trip"
        );
    }
}
