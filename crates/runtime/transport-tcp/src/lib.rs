//! The TCP transport: tokio sockets plus length-delimited framing.
//!
//! Frames go out as a four-byte big-endian length followed by that many bytes.
//! The framing is hand-written rather than taken from `tokio_util::codec` for one
//! reason: **how a failure is classified matters more here than how it is read.**
//! The engine reacts to [`ErrorKind`](cs_util::ErrorKind) — retry, reconnect, or
//! give up — so every `io::Error` on this path is deliberately mapped rather than
//! passed through whatever a codec chose.
//!
//! ```no_run
//! use cs_transport::{Endpoint, Transport};
//! use cs_transport_tcp::TcpTransport;
//!
//! # async fn example() -> Result<(), cs_util::Error> {
//! let transport = TcpTransport::new();
//! let connection = transport.connect(&"tcp://head01:7777".parse()?).await?;
//! # let _ = connection;
//! # Ok(())
//! # }
//! ```
//!
//! # The one rule that is easy to get wrong
//!
//! A transport may return [`ErrorKind::Decode`](cs_util::ErrorKind::Decode) **only
//! when it has consumed exactly one frame's bytes and the stream is still
//! framed.** The engine treats `Decode` as "that frame was nonsense, read the next
//! one", so returning it after losing frame alignment would have the engine read
//! the middle of a message as a header. Here that means a bad *payload* is
//! `Decode`, while a bad *length prefix* is
//! [`Transport`](cs_util::ErrorKind::Transport) — the connection is no longer
//! trustworthy, and reconnecting is the only way back.

use std::io;
use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};
use cs_transport::{
    Capabilities, Connection, Endpoint, Frame, FrameRx, FrameTx, Listener, Transport,
};
use cs_util::{Error, ErrorKind, Result, ResultExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpSocket, TcpStream, lookup_host};
use tracing::debug;

/// The scheme this transport answers to.
pub const SCHEME: &str = "tcp";

/// How many bytes the length prefix takes.
const HEADER: usize = 4;

/// Connections the kernel may hold for us before we accept them.
///
/// Generous because a server restart has every agent in the cluster dialling at
/// once, and a refused connection costs one of them a backoff.
const BACKLOG: u32 = 1024;

/// TCP, with length-delimited frames.
///
/// Cheap to clone; the configuration is all there is.
#[derive(Clone, Copy, Debug)]
pub struct TcpTransport {
    max_frame: usize,
    nodelay: bool,
}

impl TcpTransport {
    /// A transport with the default frame ceiling and Nagle disabled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_frame: Capabilities::DEFAULT_MAX_FRAME,
            // Off by default: this carries a control plane as well as data, and
            // Nagle would sit on a small command frame waiting for company.
            nodelay: true,
        }
    }

    /// Set the largest frame this transport will carry, in either direction.
    ///
    /// Both a limit on what it will send and a guard on what it will read: a
    /// corrupt or hostile length prefix cannot make this process allocate more
    /// than this.
    #[must_use]
    pub const fn with_max_frame(mut self, max_frame: usize) -> Self {
        self.max_frame = max_frame;
        self
    }

    /// Leave Nagle's algorithm on, coalescing small writes.
    ///
    /// Only worth it for a link that carries nothing but bulk data.
    #[must_use]
    pub const fn with_nagle(mut self) -> Self {
        self.nodelay = false;
        self
    }

    fn prepare(&self, stream: &TcpStream) {
        if self.nodelay {
            if let Err(err) = stream.set_nodelay(true) {
                // Not fatal: it costs latency, not correctness.
                debug!(error = %err, "cannot disable Nagle on this socket");
            }
        }
    }
}

impl Default for TcpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for TcpTransport {
    type Conn = TcpConnection;
    type Listener = TcpAcceptor;

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(SCHEME).with_max_frame(self.max_frame)
    }

    async fn connect(&self, endpoint: &Endpoint) -> Result<TcpConnection> {
        let authority = endpoint.require_scheme(SCHEME)?;
        // `connect` resolves the name, so a DNS failure lands here too — and is
        // retryable for the same reason a refused connection is: a name that does
        // not resolve now may well resolve in a minute.
        let stream = TcpStream::connect(authority)
            .await
            .map_err(|err| dial_error(endpoint, err))?;
        self.prepare(&stream);

        let peer = peer_endpoint(&stream);
        debug!(%endpoint, %peer, "connected");
        Ok(TcpConnection {
            stream,
            peer,
            max_frame: self.max_frame,
        })
    }

    async fn listen(&self, endpoint: &Endpoint) -> Result<TcpAcceptor> {
        let authority = endpoint.require_scheme(SCHEME)?;
        // Every failure on this path is fatal at startup — a taken port, a denied
        // privilege, a name that does not resolve — so all of it is `Config`.
        let bind_error = |err: io::Error| {
            Error::with_source(
                ErrorKind::Config,
                format!("cannot listen on {endpoint}"),
                err,
            )
        };

        let address = lookup_host(authority)
            .await
            .map_err(bind_error)?
            .next()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Config,
                    format!("{endpoint} does not resolve to an address"),
                )
            })?;

        // Built by hand rather than with `TcpListener::bind` for one reason:
        // SO_REUSEADDR. Without it a restarted server cannot rebind its own port
        // while a connection from the previous incarnation sits in TIME_WAIT — so
        // the restart the protocol is designed around (`Goodbye { RESTART }`, every
        // agent reconnecting) would fail at the first step and keep failing for a
        // minute. It does *not* permit two live listeners on one port: that is
        // SO_REUSEPORT, which is deliberately not set, so a genuine clash is still
        // reported.
        let socket = match address {
            SocketAddr::V4(_) => TcpSocket::new_v4(),
            SocketAddr::V6(_) => TcpSocket::new_v6(),
        }
        .map_err(bind_error)?;
        socket.set_reuseaddr(true).map_err(bind_error)?;
        socket.bind(address).map_err(bind_error)?;
        let listener = socket.listen(BACKLOG).map_err(bind_error)?;

        Ok(TcpAcceptor {
            listener,
            max_frame: self.max_frame,
            nodelay: self.nodelay,
        })
    }
}

/// A bound TCP listener.
#[derive(Debug)]
pub struct TcpAcceptor {
    listener: TcpListener,
    max_frame: usize,
    nodelay: bool,
}

impl Listener for TcpAcceptor {
    type Conn = TcpConnection;

    async fn accept(&self) -> Result<TcpConnection> {
        let (stream, from) = self
            .listener
            .accept()
            .await
            .map_err(|err| transport_error("accepting a connection", err))?;
        if self.nodelay {
            let _ = stream.set_nodelay(true);
        }
        debug!(%from, "accepted");
        Ok(TcpConnection {
            stream,
            peer: Endpoint::from_parts(SCHEME, &from.to_string()),
            max_frame: self.max_frame,
        })
    }

    fn local_endpoint(&self) -> Result<Endpoint> {
        let address = self
            .listener
            .local_addr()
            .map_err(|err| transport_error("asking where we are listening", err))?;
        Ok(Endpoint::from_parts(SCHEME, &address.to_string()))
    }
}

/// One established TCP connection.
#[derive(Debug)]
pub struct TcpConnection {
    stream: TcpStream,
    peer: Endpoint,
    max_frame: usize,
}

impl Connection for TcpConnection {
    type Tx = TcpTx;
    type Rx = TcpRx;

    fn peer(&self) -> Endpoint {
        self.peer.clone()
    }

    fn split(self) -> (TcpTx, TcpRx) {
        let (read, write) = self.stream.into_split();
        (
            TcpTx {
                writer: write,
                staging: BytesMut::with_capacity(8 * 1024),
                max_frame: self.max_frame,
                peer: self.peer.clone(),
            },
            TcpRx {
                // Buffered so a frame costs one syscall rather than one per read:
                // the header and the payload usually arrive together.
                reader: BufReader::with_capacity(16 * 1024, read),
                max_frame: self.max_frame,
                peer: self.peer,
                finished: false,
            },
        )
    }
}

/// The sending half.
#[derive(Debug)]
pub struct TcpTx {
    writer: OwnedWriteHalf,
    /// Reused, so a frame costs no allocation and exactly one write: length and
    /// payload go out together or a reader could see a header with no body behind
    /// it for a whole round trip.
    staging: BytesMut,
    max_frame: usize,
    peer: Endpoint,
}

impl FrameTx for TcpTx {
    async fn send(&mut self, frame: Frame) -> Result<()> {
        let kind = frame.kind_str();
        let bytes = frame.encode();
        if bytes.len() > self.max_frame {
            return Err(Error::new(
                ErrorKind::Transport,
                format!(
                    "a {kind} frame of {} bytes exceeds this transport's max_frame of {}",
                    bytes.len(),
                    self.max_frame
                ),
            ));
        }

        self.staging.clear();
        self.staging.reserve(HEADER + bytes.len());
        // Cast is safe: the length was just checked against `max_frame`, which is
        // a `usize` that cannot exceed `u32::MAX` in any supported configuration.
        self.staging.put_u32(bytes.len() as u32);
        self.staging.extend_from_slice(&bytes);

        self.writer
            .write_all(&self.staging)
            .await
            .map_err(|err| transport_error("writing a frame", err))
            .with_context(|| format!("sending a {kind} frame to {}", self.peer))
    }

    async fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .await
            .map_err(|err| transport_error("flushing", err))
    }

    async fn close(&mut self) -> Result<()> {
        // A TCP FIN is what the peer reads as end of stream.
        self.writer
            .shutdown()
            .await
            .map_err(|err| transport_error("closing the connection", err))
    }
}

/// The receiving half.
#[derive(Debug)]
pub struct TcpRx {
    reader: BufReader<OwnedReadHalf>,
    max_frame: usize,
    peer: Endpoint,
    finished: bool,
}

impl FrameRx for TcpRx {
    async fn recv(&mut self) -> Result<Option<Frame>> {
        if self.finished {
            return Ok(None);
        }

        let mut header = [0u8; HEADER];
        match self.fill(&mut header).await? {
            // Nothing at all: the peer closed between frames, which is the clean
            // close the engine is waiting for.
            0 => {
                self.finished = true;
                return Ok(None);
            }
            HEADER => {}
            // Part of a header and then nothing: the connection broke mid-frame.
            partial => {
                self.finished = true;
                return Err(self.truncated(format!(
                    "a frame header ended after {partial} of {HEADER} bytes"
                )));
            }
        }

        let length = u32::from_be_bytes(header) as usize;
        if length == 0 {
            // Not a decode failure: without a payload we cannot tell where the
            // next frame starts, so the stream is no longer framed.
            self.finished = true;
            return Err(self.unframed("a frame claimed to be zero bytes long"));
        }
        if length > self.max_frame {
            // Likewise fatal, and the reason the ceiling exists: believing this
            // would mean allocating whatever a peer asked for.
            self.finished = true;
            return Err(self.unframed(format!(
                "a frame claimed to be {length} bytes, over the {} byte ceiling",
                self.max_frame
            )));
        }

        let mut payload = vec![0u8; length];
        let filled = self.fill(&mut payload).await?;
        if filled != length {
            self.finished = true;
            return Err(self.truncated(format!("a frame ended after {filled} of {length} bytes")));
        }

        // Safe to report a decode failure from here: exactly one frame's bytes
        // have been consumed, so the stream is still framed and the engine can
        // skip this frame and carry on.
        Frame::decode(payload.into())
            .map(Some)
            .with_context(|| format!("reading a frame from {}", self.peer))
    }
}

impl TcpRx {
    /// Read until `buf` is full or the peer stops talking.
    ///
    /// Returns how much was read, so the caller can tell "closed between frames"
    /// from "closed in the middle of one" — a distinction `read_exact` throws away
    /// by reporting both as `UnexpectedEof`.
    async fn fill(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            let read = self
                .reader
                .read(&mut buf[filled..])
                .await
                .map_err(|err| transport_error("reading from the connection", err))?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        Ok(filled)
    }

    #[track_caller]
    fn truncated(&self, what: String) -> Error {
        Error::new(
            ErrorKind::Transport,
            format!("{} broke mid-frame: {what}", self.peer),
        )
    }

    #[track_caller]
    fn unframed(&self, what: impl Into<String>) -> Error {
        // Transport rather than Decode on purpose: frame alignment is lost, so
        // the only safe thing is a new connection.
        Error::new(
            ErrorKind::Transport,
            format!(
                "{} is not speaking this protocol: {}",
                self.peer,
                what.into()
            ),
        )
    }
}

/// `tcp://<addr>` for the far end of a stream.
fn peer_endpoint(stream: &TcpStream) -> Endpoint {
    let address = stream
        .peer_addr()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
    Endpoint::from_parts(SCHEME, &address.to_string())
}

/// A failure to reach a peer.
///
/// Always retryable: an agent that starts before its server, a server being
/// restarted, and a name that does not resolve yet are all ordinary, and the
/// engine's job is to keep trying.
#[track_caller]
fn dial_error(endpoint: &Endpoint, err: io::Error) -> Error {
    Error::with_source(
        ErrorKind::Transport,
        format!("cannot connect to {endpoint}"),
        err,
    )
}

/// Anything that goes wrong on an established connection.
#[track_caller]
fn transport_error(doing: &str, err: io::Error) -> Error {
    Error::with_source(ErrorKind::Transport, format!("{doing} failed"), err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_name_the_transport_and_its_ceiling() {
        let capabilities = TcpTransport::new().capabilities();
        assert_eq!(capabilities.name, "tcp");
        assert_eq!(capabilities.max_frame, Capabilities::DEFAULT_MAX_FRAME);
        assert!(
            !capabilities.native_lanes,
            "one stream, so the engine interleaves"
        );
        assert!(!capabilities.zero_copy);

        let small = TcpTransport::new().with_max_frame(1024).capabilities();
        assert_eq!(small.max_frame, 1024);
    }

    #[tokio::test]
    async fn a_foreign_endpoint_is_refused_before_any_syscall() {
        let transport = TcpTransport::new();
        let mock: Endpoint = "mock://server".parse().expect("parse");
        assert_eq!(
            transport.connect(&mock).await.unwrap_err().kind(),
            ErrorKind::Config
        );
        assert_eq!(
            transport.listen(&mock).await.unwrap_err().kind(),
            ErrorKind::Config
        );
    }

    /// SO_REUSEADDR must not become SO_REUSEPORT: a real clash still has to be
    /// reported, or two servers would silently share a port.
    #[tokio::test]
    async fn binding_a_taken_port_is_a_configuration_error() {
        let transport = TcpTransport::new();
        let first = transport
            .listen(&"tcp://127.0.0.1:0".parse().expect("parse"))
            .await
            .expect("bind");
        let taken = first.local_endpoint().expect("local endpoint");

        let err = transport.listen(&taken).await.unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::Config,
            "a taken port is fatal, not transient"
        );
        assert!(format!("{err:?}").contains("cannot listen"));
    }

    #[tokio::test]
    async fn an_unresolvable_host_is_retryable() {
        let transport = TcpTransport::new();
        let err = transport
            .connect(&"tcp://no-such-host.invalid:7777".parse().expect("parse"))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Transport);
        assert!(
            err.is_retryable(),
            "a name that does not resolve yet may resolve later"
        );
    }

    #[tokio::test]
    async fn an_oversized_frame_is_refused_rather_than_written() {
        use bytes::Bytes;
        use cs_transport::DataFrame;

        let transport = TcpTransport::new().with_max_frame(256);
        let listener = transport
            .listen(&"tcp://127.0.0.1:0".parse().expect("parse"))
            .await
            .expect("bind");
        let at = listener.local_endpoint().expect("local endpoint");
        let (client, _server) = tokio::join!(transport.connect(&at), listener.accept());
        let (mut tx, _) = client.expect("connect").split();

        let err = tx
            .send(Frame::Data(DataFrame::new(
                "counter",
                1,
                Bytes::from(vec![0u8; 512]),
            )))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Transport);
        assert!(err.to_string().contains("exceeds"));
    }

    /// A length prefix bigger than the ceiling must kill the connection, not be
    /// reported as a skippable decode failure: frame alignment is gone.
    #[tokio::test]
    async fn a_lying_length_prefix_ends_the_connection() {
        let transport = TcpTransport::new().with_max_frame(1024);
        let listener = transport
            .listen(&"tcp://127.0.0.1:0".parse().expect("parse"))
            .await
            .expect("bind");
        let at = listener.local_endpoint().expect("local endpoint");

        let (rogue, accepted) = tokio::join!(
            TcpStream::connect(at.authority()),
            Listener::accept(&listener)
        );
        let mut rogue = rogue.expect("connect");
        let (_, mut rx) = accepted.expect("accept").split();

        // Claim a gigabyte, send nothing.
        rogue
            .write_all(&u32::to_be_bytes(1_000_000_000))
            .await
            .expect("write");
        rogue.flush().await.expect("flush");

        let err = rx.recv().await.unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::Transport,
            "must not be Decode: the engine would skip it and read a payload as a header"
        );
        assert!(err.is_retryable());
        assert!(format!("{err:?}").contains("ceiling"));
    }

    #[tokio::test]
    async fn a_frame_cut_in_half_is_a_broken_connection() {
        let transport = TcpTransport::new();
        let listener = transport
            .listen(&"tcp://127.0.0.1:0".parse().expect("parse"))
            .await
            .expect("bind");
        let at = listener.local_endpoint().expect("local endpoint");

        let (rogue, accepted) = tokio::join!(
            TcpStream::connect(at.authority()),
            Listener::accept(&listener)
        );
        let mut rogue = rogue.expect("connect");
        let (_, mut rx) = accepted.expect("accept").split();

        // A header promising 64 bytes, then 4 bytes and a hang-up.
        rogue.write_all(&u32::to_be_bytes(64)).await.expect("write");
        rogue.write_all(b"only").await.expect("write");
        rogue.shutdown().await.expect("shutdown");
        drop(rogue);

        let err = rx.recv().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Transport);
        assert!(format!("{err:?}").contains("mid-frame"));
    }

    #[tokio::test]
    async fn a_header_cut_in_half_is_not_mistaken_for_a_clean_close() {
        let transport = TcpTransport::new();
        let listener = transport
            .listen(&"tcp://127.0.0.1:0".parse().expect("parse"))
            .await
            .expect("bind");
        let at = listener.local_endpoint().expect("local endpoint");

        let (rogue, accepted) = tokio::join!(
            TcpStream::connect(at.authority()),
            Listener::accept(&listener)
        );
        let mut rogue = rogue.expect("connect");
        let (_, mut rx) = accepted.expect("accept").split();

        // Two of the four header bytes, then gone. `read_exact` would call this
        // `UnexpectedEof`, indistinguishable from a clean close.
        rogue.write_all(&[0u8, 1u8]).await.expect("write");
        rogue.shutdown().await.expect("shutdown");
        drop(rogue);

        let err = rx.recv().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Transport);
        assert!(format!("{err:?}").contains("header ended after 2"));
    }

    #[tokio::test]
    async fn a_malformed_payload_is_a_decode_error_and_the_stream_survives() {
        use bytes::Bytes;
        use cs_transport::DataFrame;

        let transport = TcpTransport::new();
        let listener = transport
            .listen(&"tcp://127.0.0.1:0".parse().expect("parse"))
            .await
            .expect("bind");
        let at = listener.local_endpoint().expect("local endpoint");

        let (rogue, accepted) = tokio::join!(
            TcpStream::connect(at.authority()),
            Listener::accept(&listener)
        );
        let mut rogue = rogue.expect("connect");
        let (_, mut rx) = accepted.expect("accept").split();

        // A correctly framed payload that is not a frame.
        let garbage = [0x07u8, 0xff, 0xff, 0xff];
        rogue
            .write_all(&u32::to_be_bytes(garbage.len() as u32))
            .await
            .expect("write");
        rogue.write_all(&garbage).await.expect("write");
        // Then a real frame right behind it.
        let good = Frame::Data(DataFrame::new("counter", 1, Bytes::from_static(b"ok"))).encode();
        rogue
            .write_all(&u32::to_be_bytes(good.len() as u32))
            .await
            .expect("write");
        rogue.write_all(&good).await.expect("write");
        rogue.flush().await.expect("flush");

        let err = rx.recv().await.unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::Decode,
            "the frame was nonsense but the stream is still framed"
        );

        // And that is the point: the next frame is readable, so the engine can
        // log the bad one and carry on.
        let Some(Frame::Data(data)) = rx.recv().await.expect("recv") else {
            panic!("expected the frame behind the garbage");
        };
        assert_eq!(data.payload, Bytes::from_static(b"ok"));
    }
}
