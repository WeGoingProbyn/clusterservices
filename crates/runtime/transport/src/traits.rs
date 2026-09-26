use std::future::Future;

use cs_util::Result;

use crate::{Capabilities, Endpoint, Frame};

/// A way of moving [`Frame`]s between two peers.
///
/// This is the whole contract. A transport dials, listens, sends frames, receives
/// frames, and reports when a connection dies. It knows nothing about services,
/// routing, commands, retries, or shutdown — those are the engine's, and putting
/// any of them here is what would make a second transport expensive.
///
/// Chosen once at startup and never boxed: the engine is
/// `NodeEngine<T: Transport>`, so every call here is a static one.
///
/// # What an implementation must guarantee
///
/// - **Frames arrive whole, in order, exactly once, per lane.** A partial frame
///   is a dead connection, not a short read.
/// - **A close is reported, not inferred.** [`FrameRx::recv`] returns `Ok(None)`
///   once the peer has finished. Whether it *meant* to is not a transport
///   question — see [`FrameTx::close`].
/// - **Errors are classified.** Anything worth retrying —
///   refused connection, reset, timeout — must carry a retryable kind, or the
///   engine will treat a transient outage as fatal.
/// - **Backpressure is real.** [`FrameTx::send`] must not buffer without bound;
///   when the peer stops reading it must eventually stop returning immediately.
pub trait Transport: Send + Sync + 'static {
    /// An established connection.
    type Conn: Connection;

    /// An accepting socket.
    type Listener: Listener<Conn = Self::Conn>;

    /// What this transport can do. Read once, at startup.
    fn capabilities(&self) -> Capabilities;

    /// Dial `endpoint`.
    ///
    /// Fails with a retryable error if the peer is merely absent, so the agent's
    /// reconnect loop can tell "not up yet" from "misconfigured".
    fn connect(&self, endpoint: &Endpoint) -> impl Future<Output = Result<Self::Conn>> + Send;

    /// Start accepting at `endpoint`.
    fn listen(&self, endpoint: &Endpoint) -> impl Future<Output = Result<Self::Listener>> + Send;
}

/// Accepts inbound connections.
pub trait Listener: Send + Sync + 'static {
    /// The connections this produces.
    type Conn: Connection;

    /// Wait for the next peer.
    ///
    /// Takes `&self` so the accept loop can share it. A failure to accept one
    /// connection must not be reported as a failure of the listener itself unless
    /// the listener really is dead.
    fn accept(&self) -> impl Future<Output = Result<Self::Conn>> + Send;

    /// Where this ended up listening.
    ///
    /// Not necessarily what was passed to [`Transport::listen`]: a test binding
    /// `tcp://127.0.0.1:0` needs to discover the port it actually got.
    fn local_endpoint(&self) -> Result<Endpoint>;
}

/// One live connection, before it is split for use.
///
/// Reading and writing happen on separate tasks, so the first thing the engine
/// does with a connection is [`split`](Connection::split) it.
pub trait Connection: Send + 'static {
    /// The sending half.
    type Tx: FrameTx;

    /// The receiving half.
    type Rx: FrameRx;

    /// Who is on the other end, as best the transport can say. For logs.
    fn peer(&self) -> Endpoint;

    /// Split into halves that can move to different tasks.
    fn split(self) -> (Self::Tx, Self::Rx);
}

/// The sending half of a connection.
pub trait FrameTx: Send + 'static {
    /// Send one frame.
    ///
    /// Takes the frame by value so nothing has to be cloned to write it. Returns
    /// once the frame is handed to the transport — not once the peer has it.
    fn send(&mut self, frame: Frame) -> impl Future<Output = Result<()>> + Send;

    /// Push anything buffered out to the peer.
    ///
    /// The default does nothing, which is correct for a transport that does not
    /// buffer. One that coalesces small frames must implement it, because the
    /// engine calls it before it starts waiting for a reply.
    fn flush(&mut self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Close, so the peer's [`FrameRx::recv`] returns `Ok(None)`.
    ///
    /// Sent after `Goodbye` — and the ordering is what carries the meaning, because
    /// **a transport cannot be asked to distinguish an intended close from a
    /// dropped connection.** Over TCP both send a FIN and the peer sees exactly the
    /// same thing. A transport with an in-band close marker may report a dropped
    /// half as a failure instead, and the mock does, but nothing above may depend
    /// on it: an end of stream with no `Goodbye` before it means the peer went
    /// away, and the engine reconnects.
    fn close(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// The receiving half of a connection.
pub trait FrameRx: Send + 'static {
    /// Wait for the next frame.
    ///
    /// - `Ok(Some(frame))` — a whole frame.
    /// - `Ok(None)` — the peer has finished. Nothing more will arrive.
    /// - `Err(_)` — the connection broke. Retryable kinds mean "reconnect".
    ///
    /// A frame that fails to decode is
    /// [`ErrorKind::Decode`](cs_util::ErrorKind::Decode), not
    /// [`Transport`](cs_util::ErrorKind::Transport): retrying will not fix a peer
    /// that is speaking nonsense.
    ///
    /// **`Decode` may only be returned when exactly one frame's bytes have been
    /// consumed and the stream is still framed.** The engine responds to it by
    /// reading the next frame, so a transport that has lost frame alignment — a
    /// length prefix it could not trust, say — must report
    /// [`Transport`](cs_util::ErrorKind::Transport) instead and let the connection
    /// be rebuilt.
    fn recv(&mut self) -> impl Future<Output = Result<Option<Frame>>> + Send;
}
