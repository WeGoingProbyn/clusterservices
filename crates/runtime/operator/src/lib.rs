//! The operator side of the command protocol.
//!
//! An operator is a peer that connects to a head's **admin endpoint** and sends
//! commands naming the nodes they are for. The head forwards each one to that node
//! and sends back the node's own answer, so what arrives here is what the service
//! said — including, when something failed, the remote error chain.
//!
//! This is deliberately not an engine. There are no services to run, no data to
//! queue, nothing to reconnect to: an operator connects, asks, is answered, and
//! goes. What it needs is the transport traits, which is all this crate depends on
//! — so the same client is driven by `cs-ctl` over TCP and by the engine's own
//! tests over the in-process mock, and the thing under test is the thing that ships.
//!
//! ```no_run
//! # async fn example<T: cs_transport::Transport>(transport: &T) -> cs_util::Result<()> {
//! use cs_operator::{connect, Request};
//! use cs_transport::{CommandKind, Endpoint};
//! use std::time::Duration;
//!
//! let at = Endpoint::parse("tcp://head01:7788")?;
//! let mut operator = connect(transport, &at, "ctl@my-desk").await?;
//! let answers = operator
//!     .run(
//!         vec![Request::restart("node-7", Some("cgroup"))],
//!         Duration::from_secs(30),
//!     )
//!     .await?;
//! for answer in answers {
//!     println!("{}: {:?}", answer.node, answer.outcome);
//! }
//! operator.close().await;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use cs_transport::{
    CommandFrame, CommandId, CommandKind, Connection, Endpoint, Frame, FrameRx, FrameTx, Goodbye,
    Heartbeat, Hello, Outcome, StatusReport, StatusRequest, Transport,
};
use cs_util::{Error, ErrorKind, Result, ResultExt};
use tracing::{debug, warn};

/// How often an operator proves it is still there while waiting.
///
/// A head drops a peer that has said nothing for `peer_timeout` — 45 seconds by
/// default — and an operator waiting on a command with a sixty-second time to live
/// says nothing at all. Well under any sane timeout, and cheap: one frame.
const HEARTBEAT: Duration = Duration::from_secs(5);

/// One command for one node.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Request {
    /// Which node. Never empty — the head serves many and has to be told.
    pub node: String,
    /// Which service, or `None` for the whole agent.
    ///
    /// A whole-agent command can only be a built-in: `Custom` carries a payload
    /// only a service can interpret.
    pub service: Option<String>,
    /// What to ask for.
    pub kind: CommandKind,
    /// Skip asking the service whether it consents.
    pub force: bool,
    /// How much longer this is worth delivering. `None` leaves it to the head.
    pub ttl: Option<Duration>,
}

impl Request {
    /// Restart a service, or the whole agent.
    #[must_use]
    pub fn restart(node: impl Into<String>, service: Option<&str>) -> Self {
        Self::new(node, service, CommandKind::Restart)
    }

    /// Stop a service, or the whole agent.
    #[must_use]
    pub fn shutdown(node: impl Into<String>, service: Option<&str>) -> Self {
        Self::new(node, service, CommandKind::Shutdown)
    }

    /// Any command.
    #[must_use]
    pub fn new(node: impl Into<String>, service: Option<&str>, kind: CommandKind) -> Self {
        Self {
            node: node.into(),
            service: service.map(str::to_owned),
            kind,
            force: false,
            ttl: None,
        }
    }

    /// Deliver without asking the service's consent.
    #[must_use]
    pub const fn forced(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    /// Give up on delivering after this long.
    #[must_use]
    pub const fn with_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.ttl = ttl;
        self
    }

    /// What this command is aimed at, for output: the service or `<agent>`.
    #[must_use]
    pub fn target(&self) -> &str {
        self.service.as_deref().unwrap_or("<agent>")
    }
}

/// What came back for one [`Request`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Answer {
    /// The node the command was for.
    pub node: String,
    /// The service it was for, or `None` for a whole-agent command.
    pub service: Option<String>,
    /// How it went, or `None` if nothing came back in time.
    ///
    /// `None` is not a failure to carry out the command — it is not knowing, which
    /// is a different thing and has to read differently to whoever is looking.
    pub outcome: Option<Outcome>,
}

impl Answer {
    /// Whether this is an unambiguous success.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        matches!(self.outcome, Some(Outcome::Ok))
    }

    /// What this command was aimed at, for output.
    #[must_use]
    pub fn target(&self) -> &str {
        self.service.as_deref().unwrap_or("<agent>")
    }
}

/// A connection to a head's admin endpoint.
pub struct Operator<Tx, Rx> {
    tx: Tx,
    rx: Rx,
    next_id: u64,
    heartbeat: Duration,
}

/// Connect to a head's admin endpoint and introduce ourselves as `name`.
///
/// `name` goes in the `Hello`, and is what the head's log will blame for whatever
/// follows, so it should say who and from where.
///
/// A free function rather than `Operator::connect`, because the halves an operator
/// is made of are the transport's associated types: nothing in `Operator<Tx, Rx>`
/// could tell the compiler which `T` they came from.
pub async fn connect<T: Transport>(
    transport: &T,
    at: &Endpoint,
    name: &str,
) -> Result<Operator<<T::Conn as Connection>::Tx, <T::Conn as Connection>::Rx>> {
    let connection = transport
        .connect(at)
        .await
        .with_context(|| format!("connecting to {at}"))?;
    let (mut tx, rx) = connection.split();

    // Every peer says hello first; the head refuses a connection that does not.
    tx.send(Frame::Hello(
        Hello::new(name).with_build(concat!("cs-operator ", env!("CARGO_PKG_VERSION"))),
    ))
    .await
    .context("introducing ourselves")?;
    tx.flush().await.context("introducing ourselves")?;

    Ok(Operator {
        tx,
        rx,
        next_id: 1,
        heartbeat: HEARTBEAT,
    })
}

impl<Tx: FrameTx, Rx: FrameRx> Operator<Tx, Rx> {
    /// Prove we are still here this often instead of every [`HEARTBEAT`].
    ///
    /// It has to be comfortably shorter than the head's `peer_timeout`, which an
    /// operator has no way to read — hence a default that suits the default
    /// configuration and a setter for the rest.
    #[must_use]
    pub const fn with_heartbeat(mut self, every: Duration) -> Self {
        self.heartbeat = every;
        self
    }

    /// Send every request, then collect the answers until `timeout` elapses.
    ///
    /// Answers come back in the order the nodes produce them, not the order asked,
    /// so the result is sorted the way the requests were: an operator reading a
    /// hostlist's worth of output wants it to line up with what they typed.
    ///
    /// A request whose answer does not arrive in time is returned with
    /// `outcome: None` rather than dropped or made up.
    pub async fn run(&mut self, requests: Vec<Request>, timeout: Duration) -> Result<Vec<Answer>> {
        let mut answers: Vec<Answer> = requests
            .iter()
            .map(|request| Answer {
                node: request.node.clone(),
                service: request.service.clone(),
                outcome: None,
            })
            .collect();
        if requests.is_empty() {
            return Ok(answers);
        }

        // Which request each id belongs to, so an answer can be put back in place.
        let mut waiting: HashMap<CommandId, usize> = HashMap::with_capacity(requests.len());
        for (index, request) in requests.iter().enumerate() {
            let id = self.send(request).await?;
            waiting.insert(id, index);
        }
        self.tx.flush().await.context("sending commands")?;

        let deadline = tokio::time::Instant::now() + timeout;
        while !waiting.is_empty() {
            let frame = tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => {
                    warn!(outstanding = waiting.len(), "giving up waiting for answers");
                    break;
                }
                () = tokio::time::sleep(self.heartbeat) => {
                    self.beat().await?;
                    continue;
                }
                frame = self.rx.recv() => frame.context("waiting for answers")?,
            };

            let Some(frame) = frame else {
                return Err(Error::new(
                    ErrorKind::Transport,
                    "the head closed the connection before answering",
                ));
            };

            match frame {
                Frame::CommandResult(result) => match waiting.remove(&result.id) {
                    Some(index) => answers[index].outcome = Some(result.outcome),
                    None => debug!(id = %result.id, "an answer to a command we did not send"),
                },
                Frame::Goodbye(goodbye) => {
                    return Err(Error::new(
                        ErrorKind::Transport,
                        format!(
                            "the head is going away ({}): {}",
                            goodbye.reason, goodbye.detail
                        ),
                    ));
                }
                Frame::Heartbeat(_) => {}
                other => debug!(frame = %other, "ignoring a frame an operator has no use for"),
            }
        }

        Ok(answers)
    }

    /// Ask what the head knows: who is connected, and what do they run.
    ///
    /// Answered from the head's own tables — direct peers, plus every node its
    /// children announced — so this is also how you see what is behind a relay.
    pub async fn status(&mut self, timeout: Duration) -> Result<StatusReport> {
        let id = self.next_command_id();
        self.tx
            .send(Frame::Status(StatusRequest::new(id)))
            .await
            .context("asking for status")?;
        self.tx.flush().await.context("asking for status")?;

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let frame = tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => {
                    return Err(Error::new(
                        ErrorKind::Timeout,
                        "the head did not answer a status request in time",
                    ));
                }
                // Waiting is not an excuse to go quiet: a head drops a peer that has
                // said nothing for `peer_timeout`, and it has no way to tell a
                // thinking operator from a dead one.
                () = tokio::time::sleep(self.heartbeat) => {
                    self.beat().await?;
                    continue;
                }
                frame = self.rx.recv() => frame.context("waiting for status")?,
            };

            let Some(frame) = frame else {
                return Err(Error::new(
                    ErrorKind::Transport,
                    "the head closed the connection before answering",
                ));
            };

            match frame {
                Frame::StatusReport(report) if report.id == id => return Ok(report),
                Frame::Goodbye(goodbye) => {
                    return Err(Error::new(
                        ErrorKind::Transport,
                        format!(
                            "the head is going away ({}): {}",
                            goodbye.reason, goodbye.detail
                        ),
                    ));
                }
                other => debug!(frame = %other, "ignoring a frame while waiting for status"),
            }
        }
    }

    /// Proof of life, so a head does not drop us while a node thinks.
    async fn beat(&mut self) -> Result<()> {
        self.tx
            .send(Frame::Heartbeat(Heartbeat::at(SystemTime::now())))
            .await
            .context("sending a heartbeat")?;
        self.tx.flush().await.context("sending a heartbeat")
    }

    /// The next id, unique on this connection. Ids are shared between commands and
    /// status requests, which is what keeps an answer unambiguous.
    fn next_command_id(&mut self) -> CommandId {
        let id = CommandId(self.next_id);
        self.next_id += 1;
        id
    }

    /// Send one command and return the id its answer will carry.
    async fn send(&mut self, request: &Request) -> Result<CommandId> {
        let id = self.next_command_id();

        let frame = match &request.service {
            Some(service) => CommandFrame::for_service(id, service, request.kind.clone()),
            None => CommandFrame::for_agent(id, request.kind.clone()),
        }
        .for_node(&request.node)
        .with_ttl(request.ttl);
        let frame = if request.force { frame.forced() } else { frame };

        self.tx
            .send(Frame::Command(frame))
            .await
            .with_context(|| format!("sending a command for {}", request.node))?;
        Ok(id)
    }

    /// Say goodbye and close.
    ///
    /// Failures are logged rather than returned: whatever the operator came to do
    /// is already done, and a rude disconnect is not worth an error message that
    /// looks like the command failed.
    pub async fn close(mut self) {
        if let Err(err) = self
            .tx
            .send(Frame::Goodbye(Goodbye::shutdown("operator finished")))
            .await
        {
            debug!(error = ?err, "could not say goodbye");
        }
        if let Err(err) = self.tx.close().await {
            debug!(error = ?err, "could not close cleanly");
        }
    }
}
