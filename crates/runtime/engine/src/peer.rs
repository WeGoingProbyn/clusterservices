use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cs_api::{CommandOpts, CommandOutcome, ServiceId};
use cs_async_util::{Clock, ShutdownSignal};
use cs_transport::{
    Chunk, CommandFrame, CommandKind, CommandResult, DataFrame, Endpoint, ErrorTrace, Frame,
    FrameRx, FrameTx, GoodbyeReason, Heartbeat, Hello, Outcome, PROTOCOL_VERSION, ServiceInfo,
    StatusReport,
};
use cs_util::{Error, ErrorKind, Result};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::chunk::{Reassembled, Reassembler, split};
use crate::command::Commands;
use crate::config::EngineConfig;
use crate::engine::{Forwarded, Routing};
use crate::queue::{Outgoing, PeerQueue};
use crate::service::{AskKind, Builtin, ErasedHandler, Sender, ServiceHandle};
use crate::stats::SharedCounters;

/// What a connected peer is allowed to ask for.
///
/// Set by *how the connection came to exist* — which endpoint it arrived on, or
/// that we dialled it — never by anything the peer says about itself. A flag in
/// `Hello` would be a request to be trusted; this is a decision already made by
/// whoever configured the engine.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Role {
    /// Something below us: a compute node, or a relay serving them. Its commands
    /// are for our own services and may not name anyone else.
    Node,
    /// The peer we dialled — our own parent in the tier above.
    ///
    /// May name any node we serve, which is how a command reaches a node behind a
    /// relay. Trusted by construction: we chose to dial it.
    Upstream,
    /// An operator on the admin endpoint. Same routing rights, for the same reason.
    Operator,
}

impl Role {
    /// Whether a command from this peer may name a node that is not us.
    pub(crate) const fn may_route(self) -> bool {
        matches!(self, Self::Upstream | Self::Operator)
    }

    /// Whether this peer may ask what we know.
    ///
    /// The same answer, and deliberately so: the two are the same privilege. A
    /// compute node that could enumerate the cluster has learnt something it has no
    /// use for, and a peer that can route commands can discover the same list by
    /// trying them.
    pub(crate) const fn may_query(self) -> bool {
        self.may_route()
    }

    /// Whether this peer is a node below us, and so belongs in what we announce
    /// upward and in what an operator is shown.
    pub(crate) const fn is_below(self) -> bool {
        matches!(self, Self::Node)
    }
}

/// One connected peer.
pub(crate) struct Peer {
    /// The peer's node name: from its `Hello` if it sent one, otherwise the
    /// endpoint we dialled.
    pub(crate) node: String,
    /// Where it is: for logs, and for the admin view `cs-ctl status` prints.
    pub(crate) endpoint: Endpoint,
    /// What to send it.
    pub(crate) queue: Arc<PeerQueue>,
    /// What it says it runs, so a command for a service it does not have can be
    /// refused without a round trip.
    pub(crate) services: Vec<ServiceInfo>,
    /// Per-peer sequence for chunked messages.
    message_ids: AtomicU64,
    /// What this peer may ask for.
    pub(crate) role: Role,
    /// When the connection was established, for an operator asking how long this
    /// node has been here. Monotonic, so it is a duration and never a timestamp
    /// two peers could disagree about.
    pub(crate) connected_at: Instant,
}

impl Peer {
    pub(crate) fn new(
        node: String,
        endpoint: Endpoint,
        queue: Arc<PeerQueue>,
        services: Vec<ServiceInfo>,
        role: Role,
        connected_at: Instant,
    ) -> Self {
        Self {
            node,
            endpoint,
            queue,
            services,
            message_ids: AtomicU64::new(1),
            role,
            connected_at,
        }
    }

    /// Whether the peer advertised `service`. An agent that sent no service list
    /// is given the benefit of the doubt.
    pub(crate) fn runs(&self, service: &str) -> bool {
        self.services.is_empty() || self.services.iter().any(|s| s.name == service)
    }

    fn next_message_id(&self) -> u64 {
        self.message_ids.fetch_add(1, Ordering::Relaxed)
    }
}

/// Everything a connection's tasks need from the engine, with no transport in
/// sight.
#[derive(Clone)]
pub(crate) struct Wiring {
    pub(crate) config: Arc<EngineConfig>,
    pub(crate) counters: SharedCounters,
    pub(crate) commands: Arc<Commands>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) shutdown: ShutdownSignal,
    /// Agent-side samplers, by service name.
    pub(crate) services: Arc<HashMap<&'static str, ServiceHandle>>,
    /// Server-side handlers, by service name.
    pub(crate) handlers: Arc<HashMap<&'static str, Arc<dyn ErasedHandler>>>,
    /// Set when a command asks the whole agent to stop or restart.
    pub(crate) stop: Arc<StopRequest>,
    pub(crate) max_frame: usize,
    /// The engine's routing knowledge: where a named node is, what we can reach,
    /// and what a child tells us it can reach.
    pub(crate) routing: Arc<dyn Routing>,
}

/// How the engine was asked to stop.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Stop {
    /// Stop and stay stopped.
    Shutdown,
    /// Stop, and let the supervisor start us again. The agent exits with a
    /// dedicated status for systemd's `RestartForceExitStatus=` rather than
    /// re-executing itself.
    Restart,
}

/// Records why the engine is stopping. First writer wins.
#[derive(Debug, Default)]
pub struct StopRequest {
    reason: std::sync::Mutex<Option<Stop>>,
}

impl StopRequest {
    pub(crate) fn request(&self, reason: Stop) {
        let mut slot = self
            .reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        slot.get_or_insert(reason);
    }

    pub(crate) fn reason(&self) -> Stop {
        self.reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .unwrap_or(Stop::Shutdown)
    }
}

/// Why a connection ended.
#[derive(Debug)]
pub(crate) enum Ended {
    /// The peer closed cleanly.
    PeerClosed,
    /// The peer said goodbye first.
    Goodbye(GoodbyeReason),
    /// We are shutting down.
    LocalShutdown,
    /// The peer stopped talking without saying so.
    Silent(Duration),
    /// It broke. The error is carried for the log line that reports it.
    Broken(#[allow(dead_code, reason = "shown by the Debug used when logging")] Error),
}

impl Ended {
    /// Whether to dial again.
    pub(crate) fn should_reconnect(&self) -> bool {
        match self {
            Self::LocalShutdown => false,
            Self::Goodbye(reason) => reason.should_reconnect(),
            // A clean close we did not ask for still means the server went away,
            // and an agent's job is to come back. A peer that went quiet is the
            // same: whatever happened to it, this connection is finished.
            Self::PeerClosed | Self::Broken(_) | Self::Silent(_) => true,
        }
    }
}

/// Drain a peer's queue onto its connection until the link dies or the queue is
/// closed and empty.
///
/// The queue decides *what* goes next — control before data, always — and this
/// decides how it is framed, which is where chunking happens: only the writer
/// knows the transport's ceiling.
pub(crate) async fn write_loop<Tx: FrameTx>(
    mut tx: Tx,
    peer: Arc<Peer>,
    link_dead: ShutdownSignal,
    wiring: Wiring,
    last_write: Arc<LastWrite>,
) {
    loop {
        let next = tokio::select! {
            biased;
            () = link_dead.wait() => break,
            item = peer.queue.next() => item,
        };

        let Some(item) = next else {
            // The queue is closed and drained: this is the clean close the peer's
            // reader is waiting to see.
            if let Err(err) = tx.close().await {
                debug!(node = %peer.node, error = ?err, "closing the connection failed");
            }
            return;
        };

        let result = match item {
            Outgoing::Control(frame) => send(&mut tx, frame, &wiring).await,
            Outgoing::Data { service, payload } => {
                write_data(&mut tx, &peer, service, payload, &wiring).await
            }
        };
        match result {
            Ok(()) => last_write.mark(wiring.clock.now()),
            Err(err) => {
                warn!(node = %peer.node, error = ?err, "write failed; the connection is gone");
                link_dead.set();
                return;
            }
        }
    }
}

/// Frame one message, splitting it if the transport cannot carry it whole.
///
/// The message id is allocated per peer and only when it is actually needed, so an
/// unchunked message costs nothing extra.
async fn write_data<Tx: FrameTx>(
    tx: &mut Tx,
    peer: &Peer,
    service: ServiceId,
    payload: bytes::Bytes,
    wiring: &Wiring,
) -> Result<()> {
    let pieces = split(service.name, payload, wiring.max_frame)?;
    let message_id = if pieces.len() > 1 {
        peer.next_message_id()
    } else {
        0
    };

    for (chunk, piece) in pieces {
        let frame = match chunk {
            Some(chunk) => Frame::Data(DataFrame::chunked(
                service.name,
                service.version,
                Chunk::new(message_id, chunk.index, chunk.count),
                piece,
            )),
            None => Frame::Data(DataFrame::new(service.name, service.version, piece)),
        };
        send(tx, frame, wiring).await?;
    }
    Ok(())
}

/// Send one frame and count it.
async fn send<Tx: FrameTx>(tx: &mut Tx, frame: Frame, wiring: &Wiring) -> Result<()> {
    let bytes = frame.payload_len();
    tx.send(frame).await?;
    wiring.counters.frames_sent.fetch_add(1, Ordering::Relaxed);
    wiring
        .counters
        .bytes_sent
        .fetch_add(bytes as u64, Ordering::Relaxed);
    Ok(())
}

/// Read frames until the connection ends, routing each one.
pub(crate) async fn read_loop<Rx: FrameRx>(
    mut rx: Rx,
    peer: Arc<Peer>,
    link_dead: ShutdownSignal,
    wiring: Wiring,
) -> Ended {
    // Commands are processed one at a time, off the reader, so a service that is
    // busy sampling cannot stall the connection — and so per-service ordering is
    // preserved, which spawning a task per command would not do.
    let (to_commands, commands_rx) = mpsc::channel::<CommandFrame>(wiring.config.control_queue);
    let command_task = tokio::spawn(command_loop(commands_rx, Arc::clone(&peer), wiring.clone()));

    let mut handlers = JoinSet::new();
    let mut reassembler = Reassembler::new(wiring.config.max_partial_messages);
    // Any frame at all is proof of life, not just a heartbeat, so a busy connection
    // never pays for the timeout below.
    let mut last_heard = wiring.clock.now();

    let ended = loop {
        // A half-open connection produces no error and no close — reads simply
        // never return — so silence has to be a timeout rather than something to
        // wait for. Rebuilt each pass because it is an absolute deadline that moves
        // forward every time the peer says anything.
        let silence: cs_async_util::BoxFuture<'static, ()> = if wiring.config.peer_timeout.is_zero()
        {
            Box::pin(std::future::pending())
        } else {
            wiring
                .clock
                .sleep_until(last_heard + wiring.config.peer_timeout)
        };

        let frame = tokio::select! {
            biased;
            () = wiring.shutdown.wait() => break Ended::LocalShutdown,
            () = link_dead.wait() => break Ended::Broken(Error::new(
                ErrorKind::Transport,
                "the connection was closed locally",
            )),
            () = silence => {
                wiring
                    .counters
                    .peer_timeouts
                    .fetch_add(1, Ordering::Relaxed);
                break Ended::Silent(wiring.config.peer_timeout);
            }
            Some(finished) = handlers.join_next(), if !handlers.is_empty() => {
                match finished {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => warn!(node = %peer.node, error = ?err, "a handler failed"),
                    Err(err) => warn!(node = %peer.node, error = %err, "a handler panicked"),
                }
                continue;
            }
            frame = rx.recv() => frame,
        };

        match frame {
            Ok(None) => break Ended::PeerClosed,
            Ok(Some(frame)) => {
                last_heard = wiring.clock.now();
                wiring
                    .counters
                    .frames_received
                    .fetch_add(1, Ordering::Relaxed);
                wiring
                    .counters
                    .bytes_received
                    .fetch_add(frame.payload_len() as u64, Ordering::Relaxed);

                if let Some(reason) = route(
                    frame,
                    &peer,
                    &wiring,
                    &mut reassembler,
                    &mut handlers,
                    &to_commands,
                )
                .await
                {
                    break Ended::Goodbye(reason);
                }
            }
            Err(err) if err.kind() == ErrorKind::Decode => {
                // A peer speaking nonsense is not a reason to drop a connection:
                // the frame is counted and skipped, and the next one may be fine.
                wiring
                    .counters
                    .unroutable_frames
                    .fetch_add(1, Ordering::Relaxed);
                warn!(node = %peer.node, error = ?err, "undecodable frame; skipping it");
            }
            Err(err) => break Ended::Broken(err),
        }
    };

    // Let in-flight handlers finish before the connection goes; the shutdown
    // deadline above us bounds how long that can take.
    while let Some(finished) = handlers.join_next().await {
        if let Ok(Err(err)) = finished {
            warn!(node = %peer.node, error = ?err, "a handler failed while draining");
        }
    }
    drop(to_commands);
    let _ = command_task.await;
    ended
}

/// Route one frame. Returns a goodbye reason if the peer is leaving.
async fn route(
    frame: Frame,
    peer: &Arc<Peer>,
    wiring: &Wiring,
    reassembler: &mut Reassembler,
    handlers: &mut JoinSet<Result<()>>,
    to_commands: &mpsc::Sender<CommandFrame>,
) -> Option<GoodbyeReason> {
    match frame {
        Frame::Data(data) => {
            let payload = match data.chunk {
                None => Some(data.payload),
                Some(chunk) => match reassembler.push(&data.service, chunk, data.payload) {
                    Reassembled::Complete(whole) => Some(whole),
                    Reassembled::Partial => None,
                    Reassembled::Rejected(why) => {
                        wiring
                            .counters
                            .unroutable_frames
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(node = %peer.node, service = %data.service, why, "bad chunk");
                        None
                    }
                },
            };
            if let Some(payload) = payload {
                dispatch_data(
                    &data.service,
                    data.service_version,
                    payload,
                    peer,
                    wiring,
                    handlers,
                );
            }
            None
        }
        Frame::Command(command) => {
            if to_commands.send(command).await.is_err() {
                warn!(node = %peer.node, "dropping a command: the connection is closing");
            }
            None
        }
        Frame::CommandResult(result) => {
            wiring
                .commands
                .complete(result.id, result.outcome, wiring.clock.now());
            None
        }
        Frame::Goodbye(goodbye) => {
            info!(node = %peer.node, reason = %goodbye.reason, detail = %goodbye.detail, "peer said goodbye");
            Some(goodbye.reason)
        }
        Frame::Heartbeat(_) => None,
        // A second Hello on an established connection is meaningless but harmless.
        Frame::Hello(hello) => {
            debug!(node = %peer.node, repeated = %hello.node, "ignoring a repeated hello");
            None
        }
        Frame::Reachable(reach) => {
            if peer.role.is_below() {
                debug!(node = %peer.node, nodes = reach.nodes.len(), "reachability announced");
                wiring.routing.record_reachable(&peer.node, reach.nodes);
            } else {
                // Only a peer *below* us can tell us what it serves. An announcement
                // from above would have us route a command back the way it came.
                wiring
                    .counters
                    .unroutable_frames
                    .fetch_add(1, Ordering::Relaxed);
                warn!(node = %peer.node, "ignoring reachability from a peer that is not below us");
            }
            None
        }
        Frame::Status(request) => {
            if peer.role.may_query() {
                let report = StatusReport::new(request.id, &wiring.config.node)
                    .with_build(&wiring.config.build)
                    .up_for(wiring.routing.uptime())
                    .reaching(wiring.routing.known_nodes());
                if let Err(err) = peer.queue.push_control(Frame::StatusReport(report)) {
                    warn!(node = %peer.node, error = ?err, "cannot answer a status request");
                }
            } else {
                // Dropped rather than refused: the only peers that should ask are an
                // operator and the tier above, and a compute node has no business
                // enumerating the cluster.
                wiring
                    .counters
                    .unroutable_frames
                    .fetch_add(1, Ordering::Relaxed);
                warn!(node = %peer.node, "ignoring a status request from a peer not entitled to ask");
            }
            None
        }
        // Nothing in the engine asks for status; `cs-ctl` does, and it is not one.
        Frame::StatusReport(report) => {
            debug!(node = %peer.node, id = %report.id, "ignoring an unsolicited status report");
            None
        }
    }
}

/// Hand a complete message to its handler, or count it as unroutable.
fn dispatch_data(
    service: &str,
    service_version: u32,
    payload: bytes::Bytes,
    peer: &Arc<Peer>,
    wiring: &Wiring,
    handlers: &mut JoinSet<Result<()>>,
) {
    let Some(handler) = wiring.handlers.get(service) else {
        // Log, count, drop — never kill the connection. A node running a plugin
        // this server has never heard of is a rollout in progress, not an error.
        wiring
            .counters
            .unroutable_frames
            .fetch_add(1, Ordering::Relaxed);
        debug!(node = %peer.node, service, "no handler for this service; dropping");
        return;
    };
    wiring.counters.message_received(handler.id().name);
    let from = Sender {
        // One and the same for now: nothing forwards yet, so the peer that sent
        // this is the node that measured it.
        node: peer.node.clone(),
        via: peer.node.clone(),
        service_version,
    };
    let handler = Arc::clone(handler);
    handlers.spawn(handler.handle(from, payload));
}

/// Process one connection's commands.
///
/// A command **for us** — one that names no node, or names this engine — is carried
/// out one at a time, right here, which is what preserves per-service ordering. A
/// command for somebody else is passed on by a task of its own, because an operator
/// addressing a hostlist would otherwise wait for each node's answer, or each TTL,
/// before the next command left: a thousand nodes at sixty seconds is a fortnight.
/// Ordering survives that, because each one joins the target's own ordered control
/// queue; what is *not* guaranteed is the order of two commands sent at once for the
/// same node **and** service, which is why custom commands are specified to be
/// idempotent.
async fn command_loop(mut commands: mpsc::Receiver<CommandFrame>, peer: Arc<Peer>, wiring: Wiring) {
    let mut forwarding: JoinSet<()> = JoinSet::new();

    loop {
        // Bounded by the same figure as the control queue: many commands may be in
        // flight at once, but not unboundedly many.
        if forwarding.len() >= wiring.config.control_queue {
            forwarding.join_next().await;
            continue;
        }
        let Some(command) = commands.recv().await else {
            break;
        };

        if !is_for_us(&command, &wiring) {
            forwarding.spawn(forward(command, Arc::clone(&peer), wiring.clone()));
            continue;
        }

        let id = command.id;
        let (outcome, stop) = execute(command, &wiring).await;

        // The result is queued *before* any stop is acted on, so it leaves ahead
        // of the `Goodbye` the shutdown sequence will queue behind it.
        if let Err(err) = peer
            .queue
            .push_control(Frame::CommandResult(CommandResult::new(id, outcome)))
        {
            warn!(node = %peer.node, %id, error = ?err, "cannot answer a command");
        }
        if let Some(stop) = stop {
            wiring.stop.request(stop);
            wiring.shutdown.set();
        }
    }

    // Answers still owed to a peer that is still connected.
    while forwarding.join_next().await.is_some() {}
}

/// Whether a command is ours to carry out rather than to pass on.
///
/// Empty means "whoever this frame was addressed to", which is every command on a
/// server → agent hop. A command naming us explicitly is ours as well: a tier
/// addressing its own child by name should not have it bounce.
fn is_for_us(command: &CommandFrame, wiring: &Wiring) -> bool {
    command.node.is_empty() || command.node == wiring.config.node
}

/// Pass one command on, and answer with what the node it was for said.
async fn forward(command: CommandFrame, peer: Arc<Peer>, wiring: Wiring) {
    let id = command.id;
    let outcome = match forwarded_outcome(command, &peer, &wiring).await {
        Ok(outcome) => outcome,
        // Never delivered at all: not a node we serve, or the engine is stopping.
        // Flattened into a trace so the operator sees this side's error chain under
        // its own, which is the whole reason `ErrorTrace` exists.
        Err(err) => Outcome::Failed(ErrorTrace::from_error(&wiring.config.node, &err)),
    };
    if let Err(err) = peer
        .queue
        .push_control(Frame::CommandResult(CommandResult::new(id, outcome)))
    {
        warn!(peer = %peer.node, %id, error = ?err, "cannot answer a forwarded command");
    }
}

/// Send one command onward and wait for the node's answer.
async fn forwarded_outcome(command: CommandFrame, peer: &Peer, wiring: &Wiring) -> Result<Outcome> {
    // The whole of the authorization, and it cannot be talked around from the wire:
    // a compute node may not reach its neighbours through the head they share.
    if !peer.role.may_route() {
        return Err(Error::new(
            ErrorKind::Rejected,
            format!(
                "{:?} may not send commands for other nodes, and named {:?}",
                peer.node, command.node
            ),
        ));
    }
    // A whole-agent command is a built-in by construction: `Custom` needs a service
    // to interpret its payload.
    if command.is_agent_wide() && !command.kind.is_builtin() {
        return Err(Error::new(
            ErrorKind::Rejected,
            "a custom command needs a service to interpret it",
        ));
    }

    let node = command.node.clone();
    let service = command.service.clone();
    info!(
        from = %peer.node,
        node = %node,
        target = command.target(),
        kind = %command.kind,
        "passing on a command"
    );

    let handle = wiring.routing.dispatch(Forwarded {
        node: &node,
        service: &service,
        kind: command.kind,
        force: command.force,
        // The operator's remaining time to live, carried across the hop. Relative
        // both times, so no clock anywhere is compared with another.
        expiry: command.ttl.unwrap_or(CommandOpts::DEFAULT_EXPIRY),
        queue: false,
    })?;

    Ok(match handle.await? {
        CommandOutcome::Ok => Outcome::Ok,
        CommandOutcome::Unsupported => Outcome::Unsupported,
        CommandOutcome::Rejected(why) => Outcome::Rejected(why),
        CommandOutcome::Expired => Outcome::Expired,
        CommandOutcome::UnknownService => Outcome::UnknownService,
    })
}

/// Carry out one command and say how it went.
async fn execute(command: CommandFrame, wiring: &Wiring) -> (Outcome, Option<Stop>) {
    if command.is_agent_wide() {
        return execute_agent_wide(command, wiring).await;
    }

    let Some(service) = wiring.services.get(command.service.as_str()) else {
        return (Outcome::UnknownService, None);
    };
    let (kind, builtin) = match &command.kind {
        CommandKind::Shutdown => (AskKind::Builtin(Builtin::Shutdown), Some(Builtin::Shutdown)),
        CommandKind::Restart => (AskKind::Builtin(Builtin::Restart), Some(Builtin::Restart)),
        CommandKind::Custom(payload) => (AskKind::Custom(payload.clone()), None),
    };

    let asked = service.ask(kind).await;
    if asked.apply {
        if let Some(builtin) = builtin {
            service.apply(builtin).await;
        }
    }
    (asked.outcome, None)
}

/// Poll every service, then act — or abandon the whole thing if one refuses.
async fn execute_agent_wide(command: CommandFrame, wiring: &Wiring) -> (Outcome, Option<Stop>) {
    let (builtin, stop) = match command.kind {
        CommandKind::Shutdown => (Builtin::Shutdown, Stop::Shutdown),
        CommandKind::Restart => (Builtin::Restart, Stop::Restart),
        // There is no such thing as a custom command for a whole agent: nothing
        // could know how to decode it.
        CommandKind::Custom(_) => return (Outcome::Unsupported, None),
    };

    let mut refusals = Vec::new();
    let mut to_apply = Vec::new();

    for service in wiring.services.values() {
        if command.force {
            // `force` skips asking, but the service still gets to flush in
            // `on_shutdown` under the deadline.
            to_apply.push(service);
            continue;
        }
        let asked = service.ask(AskKind::Builtin(builtin)).await;
        match asked.outcome {
            Outcome::Rejected(why) => refusals.push(format!("{}: {why}", service.id().name)),
            _ if asked.apply => to_apply.push(service),
            _ => {}
        }
    }

    if !refusals.is_empty() {
        return (Outcome::Rejected(refusals.join("; ")), None);
    }
    for service in to_apply {
        service.apply(builtin).await;
    }
    (Outcome::Ok, Some(stop))
}

/// When something last went out on a connection.
///
/// Read by the heartbeat loop so a busy connection is never given redundant proof
/// of life.
#[derive(Debug)]
pub(crate) struct LastWrite {
    at: std::sync::Mutex<Instant>,
}

impl LastWrite {
    pub(crate) fn new(at: Instant) -> Self {
        Self {
            at: std::sync::Mutex::new(at),
        }
    }

    fn mark(&self, at: Instant) {
        *self
            .at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = at;
    }

    fn at(&self) -> Instant {
        *self
            .at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Send a heartbeat whenever the connection has been quiet for too long.
pub(crate) async fn heartbeat_loop(
    peer: Arc<Peer>,
    link_dead: ShutdownSignal,
    wiring: Wiring,
    last_write: Arc<LastWrite>,
) {
    let interval = wiring.config.heartbeat_interval;
    if interval.is_zero() {
        return;
    }
    loop {
        let sleep = wiring.clock.sleep_until(last_write.at() + interval);
        tokio::select! {
            biased;
            () = link_dead.wait() => return,
            () = wiring.shutdown.wait() => return,
            () = sleep => {}
        }

        let now = wiring.clock.now();
        if now < last_write.at() + interval {
            // Something else went out while we slept; that is proof of life
            // already, so there is nothing to send.
            continue;
        }
        if peer
            .queue
            .push_control(Frame::Heartbeat(
                Heartbeat::at(std::time::SystemTime::now()),
            ))
            .is_err()
        {
            return;
        }
        last_write.mark(now);
    }
}

/// Read the `Hello` an agent must send first, and check we can talk to it.
pub(crate) async fn accept_hello<Rx: FrameRx>(rx: &mut Rx) -> Result<Hello> {
    let frame = rx
        .recv()
        .await?
        .ok_or_else(|| Error::new(ErrorKind::Transport, "the peer closed before saying hello"))?;

    let Frame::Hello(hello) = frame else {
        return Err(Error::new(
            ErrorKind::Decode,
            format!("expected hello, got a {} frame", frame.kind_str()),
        ));
    };
    if !hello.protocol_matches() {
        return Err(Error::new(
            ErrorKind::Decode,
            format!(
                "node {} speaks frame protocol {}, this build speaks {PROTOCOL_VERSION}",
                hello.node, hello.protocol
            ),
        ));
    }
    Ok(hello)
}

/// The `Hello` this engine introduces itself with.
pub(crate) fn our_hello(config: &EngineConfig, services: &[ServiceId]) -> Hello {
    Hello::new(&config.node)
        .with_build(&config.build)
        .with_services(
            services
                .iter()
                .map(|id| ServiceInfo::new(id.name, id.version)),
        )
}
