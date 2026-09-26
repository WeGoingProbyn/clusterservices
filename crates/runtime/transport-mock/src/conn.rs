use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use cs_transport::{Connection, Endpoint, Frame, FrameRx, FrameTx};
use cs_util::{Error, ErrorKind, Result, ResultExt};
use tokio::sync::{mpsc, watch};

use crate::link::{Direction, Link, PipeEnds, Wire, dead_link_error};
use crate::network::Network;

/// One end of an established mock connection.
pub struct MockConnection {
    peer: Endpoint,
    link: Link,
    ends: PipeEnds,
    network: Arc<Network>,
}

impl MockConnection {
    pub(crate) fn new(peer: Endpoint, link: Link, ends: PipeEnds, network: Arc<Network>) -> Self {
        Self {
            peer,
            link,
            ends,
            network,
        }
    }

    /// The link this connection belongs to, for a test that accepted it and wants
    /// to break it later.
    #[must_use]
    pub fn link(&self) -> &Link {
        &self.link
    }
}

impl fmt::Debug for MockConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockConnection")
            .field("peer", &self.peer)
            .field("link", &self.link.id())
            .field("sending", &self.ends.sending)
            .finish()
    }
}

impl Connection for MockConnection {
    type Tx = MockTx;
    type Rx = MockRx;

    fn peer(&self) -> Endpoint {
        self.peer.clone()
    }

    fn split(self) -> (MockTx, MockRx) {
        let PipeEnds {
            sending,
            tx,
            rx,
            killed,
        } = self.ends;
        (
            MockTx {
                link: self.link.clone(),
                direction: sending,
                tx,
                killed: killed.clone(),
                network: Arc::clone(&self.network),
                closed: false,
            },
            MockRx {
                link: self.link,
                direction: sending.flip(),
                rx,
                killed,
                finished: false,
            },
        )
    }
}

/// The sending half of a mock connection.
pub struct MockTx {
    link: Link,
    direction: Direction,
    tx: mpsc::Sender<Wire>,
    killed: watch::Receiver<bool>,
    network: Arc<Network>,
    closed: bool,
}

impl MockTx {
    /// Fails if the link is dead or this half has already been closed.
    fn check_usable(&mut self) -> Result<()> {
        if *self.killed.borrow_and_update() {
            return Err(dead_link_error());
        }
        if self.closed {
            return Err(Error::new(
                ErrorKind::Transport,
                "this half of the mock connection is already closed",
            ));
        }
        Ok(())
    }

    /// Push one item, losing the race to a kill if there is one.
    async fn push(&mut self, item: Wire) -> Result<()> {
        let delay = Duration::from_millis(self.network.send_delay_ms.load(Ordering::Relaxed));
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
            // The link may have died while we were asleep.
            if *self.killed.borrow_and_update() {
                return Err(dead_link_error());
            }
        }

        tokio::select! {
            // A kill beats a queued send: a real connection does not deliver what
            // was in flight when it broke.
            biased;
            _ = self.killed.changed() => Err(dead_link_error()),
            result = self.tx.send(item) => result.map_err(|_| {
                Error::new(
                    ErrorKind::Transport,
                    "mock peer dropped its receiving half",
                )
            }),
        }
    }
}

impl FrameTx for MockTx {
    async fn send(&mut self, frame: Frame) -> Result<()> {
        self.check_usable()?;

        let kind = frame.kind_str();
        let bytes = frame.encode();

        // A real transport cannot carry an oversized frame, so neither does this
        // one: an engine that forgets to chunk fails here rather than in
        // production.
        let max_frame = self.network.capabilities.max_frame;
        if bytes.len() > max_frame {
            return Err(Error::new(
                ErrorKind::Transport,
                format!(
                    "{kind} frame of {} bytes exceeds the mock transport's max_frame of {max_frame}",
                    bytes.len()
                ),
            ));
        }

        let len = bytes.len();
        self.push(Wire::Bytes(bytes))
            .await
            .with_context(|| format!("sending {kind} frame {}", self.direction))?;
        self.link.count_sent(self.direction, len);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.check_usable()?;
        self.push(Wire::Close).await.context("closing mock link")?;
        self.closed = true;
        Ok(())
    }
}

impl fmt::Debug for MockTx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockTx")
            .field("link", &self.link.id())
            .field("direction", &self.direction)
            .field("closed", &self.closed)
            .finish()
    }
}

/// The receiving half of a mock connection.
pub struct MockRx {
    link: Link,
    direction: Direction,
    rx: mpsc::Receiver<Wire>,
    killed: watch::Receiver<bool>,
    finished: bool,
}

impl FrameRx for MockRx {
    async fn recv(&mut self) -> Result<Option<Frame>> {
        if self.finished {
            return Ok(None);
        }
        if *self.killed.borrow_and_update() {
            return Err(dead_link_error());
        }

        let item = tokio::select! {
            biased;
            _ = self.killed.changed() => return Err(dead_link_error()),
            item = self.rx.recv() => item,
        };

        match item {
            Some(Wire::Bytes(bytes)) => {
                self.link.count_received(self.direction);
                // Decode failures propagate as they are: `ErrorKind::Decode`, not
                // `Transport`. The engine must log and carry on rather than
                // reconnect, since the same bytes would fail again.
                Frame::decode(bytes)
                    .map(Some)
                    .with_context(|| format!("reading a frame {}", self.direction))
            }
            Some(Wire::Close) => {
                self.finished = true;
                Ok(None)
            }
            // Every sender gone, with no `Close`: the peer dropped its half
            // instead of closing it, which is a broken connection.
            None => {
                self.finished = true;
                Err(Error::new(
                    ErrorKind::Transport,
                    "mock peer dropped its sending half without closing it",
                ))
            }
        }
    }
}

impl fmt::Debug for MockRx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockRx")
            .field("link", &self.link.id())
            .field("direction", &self.direction)
            .field("finished", &self.finished)
            .finish()
    }
}
