//! The contract every transport has to satisfy.
//!
//! A new transport is **done when this suite passes**. The engine is written
//! against these guarantees and nothing else, so a transport that satisfies them
//! needs no engine changes, and one that quietly does not will fail here rather
//! than in a cluster at 3am.
//!
//! Implement [`TestTransport`] for the thing under test and invoke
//! [`transport_contract!`](crate::transport_contract) to generate the tests:
//!
//! ```ignore
//! cs_testkit::transport_contract!(MyTransportFixture::new());
//! ```
//!
//! Each check is a plain async function here, so a transport can also run one on
//! its own while being debugged.

use std::time::Duration;

use bytes::Bytes;
use cs_transport::{
    Connection, DataFrame, Frame, FrameRx, FrameTx, Goodbye, Heartbeat, Hello, Listener, Outcome,
    ServiceInfo, Transport,
};
use cs_util::ErrorKind;

/// How to make the transport under test, and what it can be asked to do.
///
/// The awkward part of testing a transport generically is that endpoints are not
/// portable: `mock://server` and `tcp://127.0.0.1:0` have nothing in common. So the
/// fixture supplies them, and says which optional behaviours it can demonstrate.
pub trait TestTransport: Send + Sync + 'static {
    /// The transport under test.
    type Transport: Transport;

    /// A transport instance. Two instances must be able to talk to each other, so
    /// that a test can keep a "client" and a "server" side apart.
    fn transport(&self) -> Self::Transport;

    /// An endpoint nothing is listening on yet.
    ///
    /// Called more than once per test run, and must give a fresh one each time so
    /// tests do not collide.
    fn fresh_endpoint(&self) -> cs_transport::Endpoint;

    /// An endpoint for a different transport, to check that a foreign scheme is
    /// refused. Defaults to a TCP one, which suits everything except TCP itself.
    fn foreign_endpoint(&self) -> cs_transport::Endpoint {
        cs_transport::Endpoint::from_parts("notatransport", "somewhere:1")
    }

    /// Whether `connect` to an endpoint nobody is listening on fails.
    ///
    /// True for everything real; a transport that queues connections might differ.
    fn refuses_unlistened(&self) -> bool {
        true
    }

    /// Whether the transport applies backpressure a test can observe within a few
    /// frames.
    ///
    /// False for anything with large kernel buffers — TCP will happily absorb far
    /// more than a test wants to send — so the check is skipped rather than made
    /// meaningless.
    fn shows_backpressure(&self) -> bool {
        false
    }
}

/// One of everything, so no frame kind is accidentally unsupported.
fn every_frame_kind() -> Vec<Frame> {
    vec![
        Frame::Hello(
            Hello::new("node-0042")
                .with_build("cs-agent test")
                .with_services([ServiceInfo::new("counter", 2), ServiceInfo::new("bulk", 1)]),
        ),
        Frame::Data(DataFrame::new(
            "counter",
            2,
            Bytes::from_static(b"some counters"),
        )),
        Frame::Data(DataFrame::chunked(
            "bulk",
            1,
            cs_transport::Chunk::new(7, 1, 3),
            Bytes::from_static(b"middle"),
        )),
        Frame::Command(cs_transport::CommandFrame::for_service(
            cs_transport::CommandId(9),
            "counter",
            cs_transport::CommandKind::Custom(Bytes::from_static(b"interval")),
        )),
        Frame::CommandResult(cs_transport::CommandResult::new(
            cs_transport::CommandId(9),
            Outcome::Rejected("busy".into()),
        )),
        Frame::Heartbeat(Heartbeat::at(std::time::SystemTime::UNIX_EPOCH)),
        Frame::Goodbye(Goodbye::restart("upgrading")),
    ]
}

/// A connected pair: the dialler's halves and the accepter's halves.
async fn pair<T: TestTransport>(
    fixture: &T,
) -> (
    (
        <<T::Transport as Transport>::Conn as Connection>::Tx,
        <<T::Transport as Transport>::Conn as Connection>::Rx,
    ),
    (
        <<T::Transport as Transport>::Conn as Connection>::Tx,
        <<T::Transport as Transport>::Conn as Connection>::Rx,
    ),
) {
    let transport = fixture.transport();
    let requested = fixture.fresh_endpoint();
    let listener = transport
        .listen(&requested)
        .await
        .expect("a transport should be able to listen");
    // Not necessarily what we asked for: a test binding port 0 has to be told what
    // it actually got.
    let at = listener
        .local_endpoint()
        .expect("a listener should know where it is");

    let dialler = fixture.transport();
    let (client, server) = tokio::join!(dialler.connect(&at), listener.accept());
    let client = client.expect("connect");
    let server = server.expect("accept");
    (client.split(), server.split())
}

/// Frames cross in both directions, arrive whole, and keep their order.
///
/// Sends and receives concurrently on purpose. A transport is entitled to any
/// buffer size it likes — and *should* block a sender whose peer is not reading —
/// so a check that queued a fixed number of frames before reading one would be
/// asserting the buffer's size rather than the ordering.
pub async fn frames_round_trip_in_order<T: TestTransport>(fixture: T) {
    const COUNT: u32 = 32;
    let ((mut client_tx, mut client_rx), (mut server_tx, mut server_rx)) = pair(&fixture).await;

    let sending = tokio::spawn(async move {
        for index in 0..COUNT {
            client_tx
                .send(Frame::Data(DataFrame::new(
                    "counter",
                    index,
                    Bytes::from_static(b"x"),
                )))
                .await
                .expect("send to server");
        }
        client_tx
    });

    for index in 0..COUNT {
        let Some(Frame::Data(data)) = server_rx.recv().await.expect("recv") else {
            panic!("expected data");
        };
        assert_eq!(
            data.service_version, index,
            "frame {index} arrived out of order"
        );
        assert_eq!(data.payload, Bytes::from_static(b"x"), "payload changed");
    }
    let _client_tx = sending.await.expect("sending task");

    // And the other way, so neither direction is special.
    server_tx
        .send(Frame::Heartbeat(Heartbeat::at(
            std::time::SystemTime::UNIX_EPOCH,
        )))
        .await
        .expect("send to client");
    assert!(matches!(
        client_rx.recv().await.expect("recv"),
        Some(Frame::Heartbeat(_))
    ));
}

/// Every frame kind survives the journey unchanged.
pub async fn every_frame_kind_survives<T: TestTransport>(fixture: T) {
    let ((mut tx, _), (_, mut rx)) = pair(&fixture).await;

    for expected in every_frame_kind() {
        tx.send(expected.clone()).await.expect("send");
        let got = rx.recv().await.expect("recv").expect("a frame");
        assert_eq!(
            got,
            expected,
            "a {} frame changed in transit",
            expected.kind_str()
        );
    }
}

/// A payload right at the transport's frame ceiling still goes through.
pub async fn a_frame_at_the_ceiling_is_carried<T: TestTransport>(fixture: T) {
    let max_frame = fixture.transport().capabilities().max_frame;
    let limit = DataFrame::max_payload(max_frame, "counter", "", None);
    assert!(limit > 0, "a transport must carry at least some payload");

    let ((mut tx, _), (_, mut rx)) = pair(&fixture).await;
    let payload = Bytes::from(vec![7u8; limit]);
    tx.send(Frame::Data(DataFrame::new("counter", u32::MAX, payload)))
        .await
        .expect("a frame at the ceiling should be carried");

    let Some(Frame::Data(data)) = rx.recv().await.expect("recv") else {
        panic!("expected data");
    };
    assert_eq!(data.payload.len(), limit);
    assert!(data.payload.iter().all(|&byte| byte == 7));
}

/// A closed half reads as end of stream, and stays closed.
pub async fn a_clean_close_is_end_of_stream<T: TestTransport>(fixture: T) {
    let ((mut tx, _), (_, mut rx)) = pair(&fixture).await;

    tx.send(Frame::Goodbye(Goodbye::shutdown("done")))
        .await
        .expect("send");
    tx.flush().await.expect("flush");
    tx.close().await.expect("close");

    assert!(
        matches!(rx.recv().await.expect("recv"), Some(Frame::Goodbye(_))),
        "what was sent before the close must still arrive"
    );
    assert!(
        rx.recv().await.expect("recv").is_none(),
        "a clean close must read as end of stream"
    );
    assert!(
        rx.recv().await.expect("recv").is_none(),
        "and must stay closed"
    );
}

/// A half that is dropped rather than closed is *noticed*, one way or the other.
///
/// Deliberately not "is an error". Over TCP a dropped socket sends a FIN, which is
/// byte-for-byte what `close` sends — the two are indistinguishable at the
/// protocol level, and no amount of care in the transport can separate them. A
/// transport with an in-band close marker (the mock) can report a failure and may;
/// one without must report end of stream.
///
/// Which is exactly why the protocol carries `Goodbye`: *intent* is an application
/// question, answered by a frame, never by the shape of a disconnect. The engine
/// therefore treats an unannounced end of stream as a reason to reconnect, and
/// depends on nothing here beyond "the reader is told something".
pub async fn a_dropped_half_is_noticed<T: TestTransport>(fixture: T) {
    let ((tx, _), (_, mut rx)) = pair(&fixture).await;
    drop(tx);

    match rx.recv().await {
        // A transport that cannot tell a drop from a close. The engine reconnects
        // because no `Goodbye` preceded it.
        Ok(None) => {}
        Err(err) => {
            assert_eq!(err.kind(), ErrorKind::Transport);
            assert!(
                err.is_retryable(),
                "a broken connection must be retryable, or the engine gives up on it"
            );
        }
        Ok(Some(frame)) => panic!(
            "a dropped half must not keep producing frames, got a {}",
            frame.kind_str()
        ),
    }
}

/// Dialling an endpoint nobody is listening on fails, retryably.
///
/// An agent that starts before its server must treat this as ordinary.
pub async fn connecting_to_nothing_is_retryable<T: TestTransport>(fixture: T) {
    if !fixture.refuses_unlistened() {
        return;
    }
    let transport = fixture.transport();
    let nowhere = fixture.fresh_endpoint();

    let err = match transport.connect(&nowhere).await {
        Err(err) => err,
        Ok(_) => panic!("connecting to {nowhere} should have failed"),
    };
    assert_eq!(err.kind(), ErrorKind::Transport);
    assert!(
        err.is_retryable(),
        "an agent dialling before its server is up must keep trying"
    );
}

/// An endpoint for a different transport is a configuration error, not a
/// connection failure.
pub async fn a_foreign_endpoint_is_a_configuration_error<T: TestTransport>(fixture: T) {
    let transport = fixture.transport();
    let foreign = fixture.foreign_endpoint();

    for kind in [
        transport.connect(&foreign).await.err().map(|e| e.kind()),
        transport.listen(&foreign).await.err().map(|e| e.kind()),
    ] {
        assert_eq!(
            kind,
            Some(ErrorKind::Config),
            "a foreign endpoint is a mistake in configuration, and retrying will not fix it"
        );
    }
}

/// A listener reports somewhere that can actually be dialled.
pub async fn a_listener_reports_a_usable_endpoint<T: TestTransport>(fixture: T) {
    let transport = fixture.transport();
    let listener = transport
        .listen(&fixture.fresh_endpoint())
        .await
        .expect("listen");
    let at = listener.local_endpoint().expect("local endpoint");

    let dialler = fixture.transport();
    let (client, server) = tokio::join!(dialler.connect(&at), listener.accept());
    let client = client.expect("the reported endpoint should be dialable");
    let server = server.expect("accept");
    assert_eq!(client.peer().scheme(), at.scheme());
    assert!(!server.peer().authority().is_empty(), "who connected?");
}

/// One listener serves many peers, and they do not interfere.
pub async fn many_connections_stay_independent<T: TestTransport>(fixture: T) {
    let transport = fixture.transport();
    let listener = transport
        .listen(&fixture.fresh_endpoint())
        .await
        .expect("listen");
    let at = listener.local_endpoint().expect("local endpoint");

    let mut pairs = Vec::new();
    let dialler = fixture.transport();
    for index in 0..4u32 {
        let (client, server) = tokio::join!(dialler.connect(&at), listener.accept());
        let (client_tx, _) = client.expect("connect").split();
        let (_, server_rx) = server.expect("accept").split();
        pairs.push((index, client_tx, server_rx));
    }

    // Each peer sends its own number; each must read back exactly its own.
    for (index, tx, _) in &mut pairs {
        tx.send(Frame::Data(DataFrame::new(
            "counter",
            *index,
            Bytes::from_static(b"x"),
        )))
        .await
        .expect("send");
    }
    for (index, _, rx) in &mut pairs {
        let Some(Frame::Data(data)) = rx.recv().await.expect("recv") else {
            panic!("expected data");
        };
        assert_eq!(
            data.service_version, *index,
            "a frame arrived on the wrong connection"
        );
    }
}

/// Capabilities describe something usable.
pub async fn capabilities_are_sane<T: TestTransport>(fixture: T) {
    let capabilities = fixture.transport().capabilities();
    assert!(!capabilities.name.is_empty(), "a transport needs a name");
    assert!(
        capabilities.max_frame > 64,
        "a frame ceiling of {} leaves no room for a message",
        capabilities.max_frame
    );
    // The engine sizes chunks against this, so it has to admit a payload.
    assert!(DataFrame::max_payload(capabilities.max_frame, "counter", "", None) > 0);
}

/// A sender stops accepting frames once the peer stops reading.
///
/// Skipped unless the fixture says it can show this within a few frames.
pub async fn a_sender_feels_backpressure<T: TestTransport>(fixture: T) {
    if !fixture.shows_backpressure() {
        return;
    }
    let ((mut tx, _), (_, mut rx)) = pair(&fixture).await;

    let sending = tokio::spawn(async move {
        for _ in 0..64u32 {
            if tx
                .send(Frame::Data(DataFrame::new(
                    "counter",
                    1,
                    Bytes::from_static(b"x"),
                )))
                .await
                .is_err()
            {
                return;
            }
        }
    });

    for _ in 0..40 {
        tokio::task::yield_now().await;
    }
    assert!(
        !sending.is_finished(),
        "a sender whose peer never reads must eventually block"
    );

    // Draining lets it finish, which proves it was blocked and not broken. Each
    // read is itself bounded: once the sender is done the queue runs dry, and
    // waiting on an empty one would hang instead of finishing the test.
    let drained = tokio::time::timeout(Duration::from_secs(5), async {
        while !sending.is_finished() {
            let _ = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
        }
    })
    .await;
    assert!(drained.is_ok(), "draining should have unblocked the sender");
    sending.await.expect("sender task");
}

/// Generate the contract tests for a transport.
///
/// Takes an expression producing a fresh [`TestTransport`] fixture — a fresh one
/// per test, so nothing leaks between them.
///
/// ```ignore
/// cs_testkit::transport_contract!(MockFixture::new());
/// ```
#[macro_export]
macro_rules! transport_contract {
    ($fixture:expr) => {
        mod transport_contract {
            use super::*;

            macro_rules! contract_test {
                ($name:ident) => {
                    #[tokio::test]
                    async fn $name() {
                        $crate::contract::$name($fixture).await;
                    }
                };
            }

            contract_test!(frames_round_trip_in_order);
            contract_test!(every_frame_kind_survives);
            contract_test!(a_frame_at_the_ceiling_is_carried);
            contract_test!(a_clean_close_is_end_of_stream);
            contract_test!(a_dropped_half_is_noticed);
            contract_test!(connecting_to_nothing_is_retryable);
            contract_test!(a_foreign_endpoint_is_a_configuration_error);
            contract_test!(a_listener_reports_a_usable_endpoint);
            contract_test!(many_connections_stay_independent);
            contract_test!(capabilities_are_sane);
            contract_test!(a_sender_feels_backpressure);
        }
    };
}
