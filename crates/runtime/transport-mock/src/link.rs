use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use cs_transport::Endpoint;
use cs_util::{Error, ErrorKind, Result};
use tokio::sync::{mpsc, watch};

/// Which way along a link bytes are travelling.
///
/// A link is two independent byte streams; faults and counters apply to one
/// direction at a time, so a test can break or observe exactly one.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Direction {
    /// From the peer that dialled to the peer that accepted — agent to server.
    ToServer,
    /// From the peer that accepted to the peer that dialled — server to agent.
    ToClient,
}

impl Direction {
    /// The other direction.
    #[must_use]
    pub const fn flip(self) -> Self {
        match self {
            Self::ToServer => Self::ToClient,
            Self::ToClient => Self::ToServer,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::ToServer => 0,
            Self::ToClient => 1,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::ToServer => "to_server",
            Self::ToClient => "to_client",
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What travels down a mock link.
///
/// Bytes, not frames: the mock serialises every frame exactly as a real transport
/// would, so the encode/decode path is exercised by every test and a test can
/// inject bytes that are not a frame at all.
pub(crate) enum Wire {
    /// One encoded frame.
    Bytes(Bytes),
    /// An explicit clean close, which becomes `Ok(None)` for the reader.
    ///
    /// Distinct from the channel simply closing: that is what happens when a peer
    /// drops its sending half without closing it, and the reader is told so.
    Close,
}

/// One end of a link's plumbing, as handed to a connection.
pub(crate) struct PipeEnds {
    pub(crate) sending: Direction,
    pub(crate) tx: mpsc::Sender<Wire>,
    pub(crate) rx: mpsc::Receiver<Wire>,
    pub(crate) killed: watch::Receiver<bool>,
}

/// A handle to one established connection, from outside both of its ends.
///
/// This is how a test breaks things: it holds a `Link` while the engine holds the
/// two halves, and can kill the connection, inject bytes the engine never asked
/// for, or read the counters — none of which the engine can tell apart from the
/// real world misbehaving.
#[derive(Clone)]
pub struct Link {
    inner: Arc<LinkInner>,
}

struct LinkInner {
    id: u64,
    client: Endpoint,
    server: Endpoint,
    killed: watch::Sender<bool>,
    /// Weak so that holding a `Link` does not keep a direction's channel open:
    /// a dropped sending half must still look dropped to the reader.
    senders: [mpsc::WeakSender<Wire>; 2],
    frames_sent: [AtomicU64; 2],
    frames_received: [AtomicU64; 2],
    bytes_sent: [AtomicU64; 2],
}

impl Link {
    /// Build a link and the two ends of its plumbing.
    pub(crate) fn new(
        id: u64,
        client: Endpoint,
        server: Endpoint,
        capacity: usize,
    ) -> (Self, PipeEnds, PipeEnds) {
        let (to_server_tx, to_server_rx) = mpsc::channel(capacity);
        let (to_client_tx, to_client_rx) = mpsc::channel(capacity);
        let (killed, killed_rx) = watch::channel(false);

        let inner = Arc::new(LinkInner {
            id,
            client,
            server,
            killed,
            senders: [to_server_tx.downgrade(), to_client_tx.downgrade()],
            frames_sent: [AtomicU64::new(0), AtomicU64::new(0)],
            frames_received: [AtomicU64::new(0), AtomicU64::new(0)],
            bytes_sent: [AtomicU64::new(0), AtomicU64::new(0)],
        });

        let client_end = PipeEnds {
            sending: Direction::ToServer,
            tx: to_server_tx,
            rx: to_client_rx,
            killed: killed_rx.clone(),
        };
        let server_end = PipeEnds {
            sending: Direction::ToClient,
            tx: to_client_tx,
            rx: to_server_rx,
            killed: killed_rx,
        };
        (Self { inner }, client_end, server_end)
    }

    /// Sequence number of this link within its network, from 1.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    /// The endpoint of the peer that dialled.
    #[must_use]
    pub fn client(&self) -> &Endpoint {
        &self.inner.client
    }

    /// The endpoint of the peer that accepted.
    #[must_use]
    pub fn server(&self) -> &Endpoint {
        &self.inner.server
    }

    /// Break the connection now, in both directions.
    ///
    /// Everything in flight is lost, and both peers' next `send` or `recv` fails
    /// with a retryable [`ErrorKind::Transport`] error — the same thing they would
    /// see if a cable were pulled. Idempotent.
    pub fn kill(&self) {
        // The only receiver-side failure is "all receivers dropped", which means
        // nothing is left to notice the kill.
        let _ = self.inner.killed.send(true);
    }

    /// Whether the link is still usable.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        !*self.inner.killed.borrow()
    }

    /// Push arbitrary bytes into one direction, as if the peer had sent them.
    ///
    /// The receiver will try to decode them as a frame. Use this to make a peer
    /// look like it is speaking a different protocol.
    ///
    /// Fails if the link is dead, the direction's queue is full, or the sending
    /// peer has already gone away.
    pub fn inject(&self, direction: Direction, bytes: Bytes) -> Result<()> {
        if !self.is_alive() {
            return Err(dead_link_error());
        }
        let sender = self.inner.senders[direction.index()]
            .upgrade()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Transport,
                    format!("nothing is sending {direction} on link {}", self.inner.id),
                )
            })?;
        sender.try_send(Wire::Bytes(bytes)).map_err(|e| {
            Error::new(
                ErrorKind::Transport,
                format!("cannot inject {direction} on link {}: {e}", self.inner.id),
            )
        })
    }

    /// Push bytes that are not a decodable frame.
    ///
    /// The receiving peer gets an [`ErrorKind::Decode`] error, which the engine
    /// must survive without dropping the connection.
    pub fn inject_garbage(&self, direction: Direction) -> Result<()> {
        // Field number 0 with wire type 7: unrepresentable in protobuf.
        self.inject(direction, Bytes::from_static(&[0x07, 0xff, 0xff, 0xff]))
    }

    /// Frames handed to this direction by its sender.
    #[must_use]
    pub fn frames_sent(&self, direction: Direction) -> u64 {
        self.inner.frames_sent[direction.index()].load(Ordering::Relaxed)
    }

    /// Frames taken off this direction by its reader.
    ///
    /// The gap between this and [`frames_sent`](Link::frames_sent) is what is
    /// still in flight.
    #[must_use]
    pub fn frames_received(&self, direction: Direction) -> u64 {
        self.inner.frames_received[direction.index()].load(Ordering::Relaxed)
    }

    /// Encoded bytes handed to this direction.
    #[must_use]
    pub fn bytes_sent(&self, direction: Direction) -> u64 {
        self.inner.bytes_sent[direction.index()].load(Ordering::Relaxed)
    }

    /// How many frames are queued in this direction right now.
    ///
    /// What a backpressure assertion looks at: it cannot exceed the network's
    /// capacity, and a sender that reaches it blocks.
    #[must_use]
    pub fn queued(&self, direction: Direction) -> usize {
        self.inner.senders[direction.index()]
            .upgrade()
            .map_or(0, |tx| tx.max_capacity() - tx.capacity())
    }

    pub(crate) fn count_sent(&self, direction: Direction, bytes: usize) {
        self.inner.frames_sent[direction.index()].fetch_add(1, Ordering::Relaxed);
        self.inner.bytes_sent[direction.index()].fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn count_received(&self, direction: Direction) {
        self.inner.frames_received[direction.index()].fetch_add(1, Ordering::Relaxed);
    }
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Link")
            .field("id", &self.inner.id)
            .field("client", &self.inner.client)
            .field("server", &self.inner.server)
            .field("alive", &self.is_alive())
            .field("to_server", &self.frames_sent(Direction::ToServer))
            .field("to_client", &self.frames_sent(Direction::ToClient))
            .finish()
    }
}

/// The error both halves report once a link has been killed.
#[track_caller]
pub(crate) fn dead_link_error() -> Error {
    // Retryable on purpose: a killed link is a broken connection, and the engine
    // is supposed to reconnect rather than give up.
    Error::new(ErrorKind::Transport, "mock link was killed")
}
