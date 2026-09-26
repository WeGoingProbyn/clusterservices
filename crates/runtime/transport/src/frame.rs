use std::fmt;
use std::time::{Duration, SystemTime};

use bytes::{BufMut, Bytes, BytesMut};
use cs_util::{Error, ErrorKind, Result};

use crate::ErrorTrace;
use crate::proto;

/// Version of the frame protocol this build speaks.
///
/// Sent in [`Hello`] and checked by the peer. Bumped only for a change that an
/// older peer cannot decode — adding a field or an enum member is not one.
pub const PROTOCOL_VERSION: u32 = 1;

/// Which of a connection's two lanes a frame travels on.
///
/// Ordered so that sorting or comparing puts [`Control`](Lane::Control) first,
/// matching the rule that the control lane is always drained before the data
/// lane. A transport with native multiplexing may map these onto separate
/// streams; one without simply interleaves them in this priority order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Lane {
    /// Hello, Goodbye, commands, command results, heartbeats.
    Control,
    /// Service data. Everything here is droppable under pressure; nothing on the
    /// control lane is.
    Data,
}

impl Lane {
    /// Stable lowercase name, for logs and metric labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Data => "data",
        }
    }
}

impl fmt::Display for Lane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One unit of transfer between two peers.
///
/// This is what a [`Transport`](crate::Transport) moves, and the only thing it
/// knows about: it never looks inside a [`Data`](Frame::Data) payload, and it has
/// no opinion about services, retries, or commands.
///
/// A frame's [`lane`](Frame::lane) follows from its variant rather than being
/// carried alongside it, so the two can never contradict each other.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Frame {
    /// Metrics from one service.
    Data(DataFrame),
    /// An agent introducing itself, once, before any data.
    Hello(Hello),
    /// A clean close, with the reason.
    Goodbye(Goodbye),
    /// An instruction for a service or a whole agent.
    Command(CommandFrame),
    /// The answer to exactly one command.
    CommandResult(CommandResult),
    /// Proof of life.
    Heartbeat(Heartbeat),
}

impl Frame {
    /// Which lane this frame belongs on.
    #[must_use]
    pub const fn lane(&self) -> Lane {
        match self {
            Self::Data(_) => Lane::Data,
            _ => Lane::Control,
        }
    }

    /// Stable lowercase name of the variant, for logs and per-kind counters.
    #[must_use]
    pub const fn kind_str(&self) -> &'static str {
        match self {
            Self::Data(_) => "data",
            Self::Hello(_) => "hello",
            Self::Goodbye(_) => "goodbye",
            Self::Command(_) => "command",
            Self::CommandResult(_) => "command_result",
            Self::Heartbeat(_) => "heartbeat",
        }
    }

    /// Bytes of service payload carried, for byte counters. Zero for every
    /// control frame except a custom command.
    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            Self::Data(data) => data.payload.len(),
            Self::Command(cmd) => match &cmd.kind {
                CommandKind::Custom(payload) => payload.len(),
                _ => 0,
            },
            _ => 0,
        }
    }

    /// Encode to bytes, allocating exactly once.
    ///
    /// Consumes the frame so no payload or name has to be cloned.
    #[must_use]
    pub fn encode(self) -> Bytes {
        let message = self.into_proto();
        let mut buf = BytesMut::with_capacity(prost::Message::encoded_len(&message));
        // Infallible: the buffer above has exactly the required capacity, and
        // `BytesMut` grows rather than failing regardless.
        prost::Message::encode_raw(&message, &mut buf);
        buf.freeze()
    }

    /// Encode into a caller-provided buffer, which must have
    /// [`encoded_len`](Frame::encoded_len) bytes of capacity.
    ///
    /// For a transport handing out registered memory. Otherwise prefer
    /// [`encode`](Frame::encode), which builds the wire message once instead of
    /// twice.
    pub fn encode_into(self, buf: &mut dyn BufMut) {
        prost::Message::encode_raw(&self.into_proto(), &mut { buf });
    }

    /// Exact encoded size.
    ///
    /// Builds the wire message to measure it, so do not pair this with
    /// [`encode`](Frame::encode) on a hot path — use
    /// [`DataFrame::max_payload`] to make chunking decisions instead.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        prost::Message::encoded_len(&self.clone().into_proto())
    }

    /// Decode one frame.
    ///
    /// Rejects anything structurally impossible — no body, an unspecified enum
    /// member, an empty node or service name, a chunk index past its count — as
    /// [`ErrorKind::Decode`], which the engine treats as "this peer is wrong",
    /// not "retry".
    pub fn decode(buf: Bytes) -> Result<Self> {
        let message = <proto::Frame as prost::Message>::decode(buf)
            .map_err(|e| Error::with_source(ErrorKind::Decode, "malformed frame", e))?;
        Self::try_from_proto(message)
    }

    /// The wire form of this frame.
    ///
    /// Public because a transport that speaks protobuf natively — the gRPC one —
    /// streams these instead of encoding them itself.
    #[must_use]
    pub fn into_proto(self) -> proto::Frame {
        let body = match self {
            Self::Data(data) => proto::frame::Body::Data(data.into_proto()),
            Self::Hello(hello) => proto::frame::Body::Hello(hello.into_proto()),
            Self::Goodbye(goodbye) => proto::frame::Body::Goodbye(goodbye.into_proto()),
            Self::Command(cmd) => proto::frame::Body::Command(cmd.into_proto()),
            Self::CommandResult(res) => proto::frame::Body::CommandResult(res.into_proto()),
            Self::Heartbeat(hb) => proto::frame::Body::Heartbeat(proto::Heartbeat {
                sent_unix_ms: hb.sent_unix_ms,
            }),
        };
        proto::Frame { body: Some(body) }
    }

    /// Validate a decoded wire frame into its Rust form.
    pub fn try_from_proto(message: proto::Frame) -> Result<Self> {
        let body = message
            .body
            .ok_or_else(|| decode_error("frame carries no body"))?;
        Ok(match body {
            proto::frame::Body::Data(data) => Self::Data(DataFrame::try_from_proto(data)?),
            proto::frame::Body::Hello(hello) => Self::Hello(Hello::try_from_proto(hello)?),
            proto::frame::Body::Goodbye(goodbye) => {
                Self::Goodbye(Goodbye::try_from_proto(goodbye)?)
            }
            proto::frame::Body::Command(cmd) => Self::Command(CommandFrame::try_from_proto(cmd)?),
            proto::frame::Body::CommandResult(res) => {
                Self::CommandResult(CommandResult::try_from_proto(res)?)
            }
            proto::frame::Body::Heartbeat(hb) => Self::Heartbeat(Heartbeat {
                sent_unix_ms: hb.sent_unix_ms,
            }),
        })
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data(data) => write!(
                f,
                "data {}/v{} {}B{}",
                data.service,
                data.service_version,
                data.payload.len(),
                match data.chunk {
                    Some(chunk) => format!(" chunk {}/{}", chunk.index + 1, chunk.count),
                    None => String::new(),
                }
            ),
            Self::Hello(hello) => write!(
                f,
                "hello {} protocol {} ({} services)",
                hello.node,
                hello.protocol,
                hello.services.len()
            ),
            Self::Goodbye(goodbye) => write!(f, "goodbye {}", goodbye.reason),
            Self::Command(cmd) => write!(f, "command {} {} -> {}", cmd.id, cmd.kind, cmd.target()),
            Self::CommandResult(res) => write!(f, "result {} {}", res.id, res.outcome),
            Self::Heartbeat(_) => f.write_str("heartbeat"),
        }
    }
}

/// Metrics from one service, already encoded by the plugin.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataFrame {
    /// The producing service's name — the routing key. Never empty.
    pub service: String,
    /// The producing service's version.
    pub service_version: u32,
    /// Set when the engine split a message too large for one frame.
    pub chunk: Option<Chunk>,
    /// The plugin's encoded message. Opaque here.
    pub payload: Bytes,
}

impl DataFrame {
    /// A whole, unchunked message.
    #[must_use]
    pub fn new(service: impl Into<String>, service_version: u32, payload: Bytes) -> Self {
        Self {
            service: service.into(),
            service_version,
            chunk: None,
            payload,
        }
    }

    /// One chunk of a split message.
    #[must_use]
    pub fn chunked(
        service: impl Into<String>,
        service_version: u32,
        chunk: Chunk,
        payload: Bytes,
    ) -> Self {
        Self {
            service: service.into(),
            service_version,
            chunk: Some(chunk),
            payload,
        }
    }

    /// The largest payload that still fits in `max_frame` bytes, given this
    /// service name and chunk metadata.
    ///
    /// This is what the engine chunks against: framing overhead depends on the
    /// length of the service name and on the varint width of the chunk numbers,
    /// so it cannot be a constant. Returns 0 if the envelope alone already
    /// exceeds `max_frame`, which means the transport's limit is unusably small
    /// for this service.
    #[must_use]
    pub fn max_payload(max_frame: usize, service: &str, chunk: Option<Chunk>) -> usize {
        // Everything except the payload field. Worst case on the version, since a
        // varint is widest at its largest value and the engine may use any.
        let base = prost::Message::encoded_len(
            &Self {
                service: service.to_owned(),
                service_version: u32::MAX,
                chunk,
                payload: Bytes::new(),
            }
            .into_proto(),
        );

        if frame_len_for_payload(base, 0) > max_frame {
            return 0;
        }
        // Not closed-form: a longer payload widens the `Data` message, which can
        // widen the length delimiter the `Frame` wraps it in. Search instead of
        // deriving, so the answer is exact at every size.
        let (mut lo, mut hi) = (0, max_frame);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            if frame_len_for_payload(base, mid) <= max_frame {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        lo
    }

    fn into_proto(self) -> proto::Data {
        let (message_id, chunk_index, chunk_count) = match self.chunk {
            Some(chunk) => (chunk.message_id, chunk.index, chunk.count),
            None => (0, 0, 0),
        };
        proto::Data {
            service: self.service,
            service_version: self.service_version,
            payload: self.payload,
            message_id,
            chunk_index,
            chunk_count,
        }
    }

    fn try_from_proto(data: proto::Data) -> Result<Self> {
        if data.service.is_empty() {
            return Err(decode_error("data frame has no service name"));
        }
        let chunk = if data.chunk_count == 0 {
            None
        } else {
            if data.chunk_index >= data.chunk_count {
                return Err(decode_error(format!(
                    "chunk index {} is past its count {}",
                    data.chunk_index, data.chunk_count
                )));
            }
            Some(Chunk {
                message_id: data.message_id,
                index: data.chunk_index,
                count: data.chunk_count,
            })
        };
        Ok(Self {
            service: data.service,
            service_version: data.service_version,
            chunk,
            payload: data.payload,
        })
    }
}

/// Encoded size of a whole data frame whose `Data` message costs `base` bytes
/// before its payload, carrying a payload of `payload` bytes.
///
/// Three nested costs: the payload's own field (tag, length delimiter, bytes),
/// the `Data` message's length delimiter inside the `Frame`, and the `Frame`'s
/// oneof tag. Field numbers 1..=6 keep every tag to a single byte.
fn frame_len_for_payload(base: usize, payload: usize) -> usize {
    let data_len = if payload == 0 {
        // prost omits an empty `bytes` field entirely: it is the proto3 default.
        base
    } else {
        base + 1 + prost::length_delimiter_len(payload) + payload
    };
    1 + prost::length_delimiter_len(data_len) + data_len
}

/// Where one chunk sits within a split message.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Chunk {
    /// Identifies the message being reassembled, unique per (service, peer).
    pub message_id: u64,
    /// 0-based position. Always less than [`count`](Chunk::count).
    pub index: u32,
    /// How many chunks the message was split into. At least 1.
    pub count: u32,
}

impl Chunk {
    /// One chunk of a message split into `count` pieces.
    #[must_use]
    pub const fn new(message_id: u64, index: u32, count: u32) -> Self {
        Self {
            message_id,
            index,
            count,
        }
    }

    /// Whether this is the last chunk, and reassembly can finish.
    #[must_use]
    pub const fn is_last(&self) -> bool {
        self.index + 1 >= self.count
    }
}

/// An agent introducing itself.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Hello {
    /// Node name as the cluster knows it. Never empty: it is how commands get
    /// addressed back.
    pub node: String,
    /// Frame protocol version the sender speaks.
    pub protocol: u32,
    /// Build identifier, for operators. Advisory.
    pub build: String,
    /// Every service the agent runs, so the server can reject a command for a
    /// service this node does not have without a round trip.
    pub services: Vec<ServiceInfo>,
}

impl Hello {
    /// A hello for `node`, speaking this build's [`PROTOCOL_VERSION`].
    #[must_use]
    pub fn new(node: impl Into<String>) -> Self {
        Self {
            node: node.into(),
            protocol: PROTOCOL_VERSION,
            build: String::new(),
            services: Vec::new(),
        }
    }

    /// Add the services this agent runs.
    #[must_use]
    pub fn with_services(mut self, services: impl IntoIterator<Item = ServiceInfo>) -> Self {
        self.services = services.into_iter().collect();
        self
    }

    /// Set the build identifier.
    #[must_use]
    pub fn with_build(mut self, build: impl Into<String>) -> Self {
        self.build = build.into();
        self
    }

    /// Whether the peer's protocol version is one this build can talk to.
    #[must_use]
    pub const fn protocol_matches(&self) -> bool {
        self.protocol == PROTOCOL_VERSION
    }

    /// Look up one of the advertised services.
    #[must_use]
    pub fn service(&self, name: &str) -> Option<&ServiceInfo> {
        self.services.iter().find(|s| s.name == name)
    }

    fn into_proto(self) -> proto::Hello {
        proto::Hello {
            node: self.node,
            protocol: self.protocol,
            build: self.build,
            services: self
                .services
                .into_iter()
                .map(|s| proto::ServiceInfo {
                    name: s.name,
                    version: s.version,
                })
                .collect(),
        }
    }

    fn try_from_proto(hello: proto::Hello) -> Result<Self> {
        if hello.node.is_empty() {
            return Err(decode_error("hello has no node name"));
        }
        let services = hello
            .services
            .into_iter()
            .map(|s| {
                if s.name.is_empty() {
                    Err(decode_error("hello advertises a service with no name"))
                } else {
                    Ok(ServiceInfo {
                        name: s.name,
                        version: s.version,
                    })
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            node: hello.node,
            protocol: hello.protocol,
            build: hello.build,
            services,
        })
    }
}

/// One service advertised in a [`Hello`].
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ServiceInfo {
    /// The service's name.
    pub name: String,
    /// The service's version.
    pub version: u32,
}

impl ServiceInfo {
    /// A service advertisement.
    #[must_use]
    pub fn new(name: impl Into<String>, version: u32) -> Self {
        Self {
            name: name.into(),
            version,
        }
    }
}

impl fmt::Display for ServiceInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/v{}", self.name, self.version)
    }
}

/// A clean close.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Goodbye {
    /// Why the sender is going away — which decides whether to reconnect.
    pub reason: GoodbyeReason,
    /// Detail for the log. Advisory.
    pub detail: String,
}

impl Goodbye {
    /// Going away for good.
    #[must_use]
    pub fn shutdown(detail: impl Into<String>) -> Self {
        Self {
            reason: GoodbyeReason::Shutdown,
            detail: detail.into(),
        }
    }

    /// Going away briefly; the peer should reconnect.
    #[must_use]
    pub fn restart(detail: impl Into<String>) -> Self {
        Self {
            reason: GoodbyeReason::Restart,
            detail: detail.into(),
        }
    }

    /// Closing because of an error.
    #[must_use]
    pub fn error(detail: impl Into<String>) -> Self {
        Self {
            reason: GoodbyeReason::Error,
            detail: detail.into(),
        }
    }

    fn into_proto(self) -> proto::Goodbye {
        proto::Goodbye {
            reason: proto::GoodbyeReason::from(self.reason) as i32,
            detail: self.detail,
        }
    }

    fn try_from_proto(goodbye: proto::Goodbye) -> Result<Self> {
        let reason = proto::GoodbyeReason::try_from(goodbye.reason)
            .map_err(|_| decode_error(format!("unknown goodbye reason {}", goodbye.reason)))?;
        Ok(Self {
            reason: GoodbyeReason::try_from_proto(reason)?,
            detail: goodbye.detail,
        })
    }
}

/// Why a peer is closing.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum GoodbyeReason {
    /// Not coming back. Do not reconnect.
    Shutdown,
    /// Coming back shortly. Reconnect with the usual backoff.
    Restart,
    /// Closing because of an error.
    Error,
}

impl GoodbyeReason {
    /// Whether the peer expects to be reconnected to.
    #[must_use]
    pub const fn should_reconnect(self) -> bool {
        matches!(self, Self::Restart | Self::Error)
    }

    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shutdown => "shutdown",
            Self::Restart => "restart",
            Self::Error => "error",
        }
    }

    fn try_from_proto(reason: proto::GoodbyeReason) -> Result<Self> {
        Ok(match reason {
            proto::GoodbyeReason::Shutdown => Self::Shutdown,
            proto::GoodbyeReason::Restart => Self::Restart,
            proto::GoodbyeReason::Error => Self::Error,
            proto::GoodbyeReason::Unspecified => {
                return Err(decode_error("goodbye has no reason"));
            }
        })
    }
}

impl From<GoodbyeReason> for proto::GoodbyeReason {
    fn from(reason: GoodbyeReason) -> Self {
        match reason {
            GoodbyeReason::Shutdown => Self::Shutdown,
            GoodbyeReason::Restart => Self::Restart,
            GoodbyeReason::Error => Self::Error,
        }
    }
}

impl fmt::Display for GoodbyeReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Identifies one in-flight command on one connection.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct CommandId(pub u64);

impl fmt::Display for CommandId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// An instruction for one service, or for a whole agent.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CommandFrame {
    /// Unique per connection; the matching [`CommandResult`] carries it back.
    /// Never 0.
    pub id: CommandId,
    /// Target service name, or empty for the whole agent.
    pub service: String,
    /// Which command, with any custom payload.
    pub kind: CommandKind,
    /// Deliver without asking the service's consent.
    pub force: bool,
    /// How much longer this is worth delivering, from when it was sent.
    ///
    /// Relative rather than absolute, and recomputed whenever the command leaves
    /// a queue, so expiry needs no clock synchronisation between the two peers.
    /// `None` means it never expires.
    pub ttl: Option<Duration>,
}

impl CommandFrame {
    /// A command for one service.
    #[must_use]
    pub fn for_service(id: CommandId, service: impl Into<String>, kind: CommandKind) -> Self {
        Self {
            id,
            service: service.into(),
            kind,
            force: false,
            ttl: None,
        }
    }

    /// A command for the whole agent.
    #[must_use]
    pub fn for_agent(id: CommandId, kind: CommandKind) -> Self {
        Self {
            id,
            service: String::new(),
            kind,
            force: false,
            ttl: None,
        }
    }

    /// Set the remaining time to live.
    #[must_use]
    pub const fn with_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.ttl = ttl;
        self
    }

    /// Deliver without asking the service's consent.
    #[must_use]
    pub const fn forced(mut self) -> Self {
        self.force = true;
        self
    }

    /// Whether this targets the whole agent rather than one service.
    #[must_use]
    pub fn is_agent_wide(&self) -> bool {
        self.service.is_empty()
    }

    /// The target, for logs: the service name or `<agent>`.
    #[must_use]
    pub fn target(&self) -> &str {
        if self.is_agent_wide() {
            "<agent>"
        } else {
            &self.service
        }
    }

    fn into_proto(self) -> proto::Command {
        let (kind, payload) = match self.kind {
            CommandKind::Shutdown => (proto::CommandKind::Shutdown, Bytes::new()),
            CommandKind::Restart => (proto::CommandKind::Restart, Bytes::new()),
            CommandKind::Custom(payload) => (proto::CommandKind::Custom, payload),
        };
        proto::Command {
            id: self.id.0,
            service: self.service,
            kind: kind as i32,
            force: self.force,
            ttl_ms: self
                .ttl
                .map_or(0, |ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
            payload,
        }
    }

    fn try_from_proto(cmd: proto::Command) -> Result<Self> {
        if cmd.id == 0 {
            return Err(decode_error("command has no id"));
        }
        let kind = proto::CommandKind::try_from(cmd.kind)
            .map_err(|_| decode_error(format!("unknown command kind {}", cmd.kind)))?;
        let kind = match kind {
            proto::CommandKind::Shutdown => CommandKind::Shutdown,
            proto::CommandKind::Restart => CommandKind::Restart,
            proto::CommandKind::Custom => CommandKind::Custom(cmd.payload),
            proto::CommandKind::Unspecified => {
                return Err(decode_error("command has no kind"));
            }
        };
        Ok(Self {
            id: CommandId(cmd.id),
            service: cmd.service,
            kind,
            force: cmd.force,
            ttl: match cmd.ttl_ms {
                0 => None,
                ms => Some(Duration::from_millis(ms)),
            },
        })
    }
}

/// Which command is being issued.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CommandKind {
    /// Stop the target and leave it stopped.
    Shutdown,
    /// Stop the target and rebuild it from its factory.
    Restart,
    /// A command the service defined, still encoded — the transport and engine
    /// do not know its type.
    Custom(Bytes),
}

impl CommandKind {
    /// Whether this is one of the two built-ins the engine acts on itself.
    #[must_use]
    pub const fn is_builtin(&self) -> bool {
        matches!(self, Self::Shutdown | Self::Restart)
    }

    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Shutdown => "shutdown",
            Self::Restart => "restart",
            Self::Custom(_) => "custom",
        }
    }
}

impl fmt::Display for CommandKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The answer to exactly one command.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CommandResult {
    /// The command being answered.
    pub id: CommandId,
    /// How it turned out.
    pub outcome: Outcome,
}

impl CommandResult {
    /// An answer for command `id`.
    #[must_use]
    pub const fn new(id: CommandId, outcome: Outcome) -> Self {
        Self { id, outcome }
    }

    fn into_proto(self) -> proto::CommandResult {
        let (outcome, reason, error) = match self.outcome {
            Outcome::Ok => (proto::Outcome::Ok, String::new(), None),
            Outcome::Unsupported => (proto::Outcome::Unsupported, String::new(), None),
            Outcome::Rejected(reason) => (proto::Outcome::Rejected, reason, None),
            Outcome::Expired => (proto::Outcome::Expired, String::new(), None),
            Outcome::UnknownService => (proto::Outcome::UnknownService, String::new(), None),
            Outcome::Failed(trace) => (
                proto::Outcome::Failed,
                String::new(),
                Some(trace.into_proto()),
            ),
        };
        proto::CommandResult {
            id: self.id.0,
            outcome: outcome as i32,
            reason,
            error,
        }
    }

    fn try_from_proto(res: proto::CommandResult) -> Result<Self> {
        if res.id == 0 {
            return Err(decode_error("command result has no id"));
        }
        let outcome = proto::Outcome::try_from(res.outcome)
            .map_err(|_| decode_error(format!("unknown outcome {}", res.outcome)))?;
        let outcome = match outcome {
            proto::Outcome::Ok => Outcome::Ok,
            proto::Outcome::Unsupported => Outcome::Unsupported,
            proto::Outcome::Rejected => Outcome::Rejected(res.reason),
            proto::Outcome::Expired => Outcome::Expired,
            proto::Outcome::UnknownService => Outcome::UnknownService,
            proto::Outcome::Failed => {
                let trace = res
                    .error
                    .ok_or_else(|| decode_error("failed outcome carries no error"))?;
                Outcome::Failed(ErrorTrace::try_from_proto(trace)?)
            }
            proto::Outcome::Unspecified => {
                return Err(decode_error("command result has no outcome"));
            }
        };
        Ok(Self {
            id: CommandId(res.id),
            outcome,
        })
    }
}

/// How a command turned out.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Outcome {
    /// Accepted and acted on.
    Ok,
    /// The service does not implement this custom command.
    Unsupported,
    /// The service refused, with a reason.
    Rejected(String),
    /// Never delivered: it outlived its ttl while the node was disconnected.
    Expired,
    /// The target node does not run the named service.
    UnknownService,
    /// The far side tried and failed, with the remote error chain.
    Failed(ErrorTrace),
}

impl Outcome {
    /// Whether the command was accepted.
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Unsupported => "unsupported",
            Self::Rejected(_) => "rejected",
            Self::Expired => "expired",
            Self::UnknownService => "unknown_service",
            Self::Failed(_) => "failed",
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(reason) => write!(f, "rejected: {reason}"),
            Self::Failed(trace) => write!(f, "failed: {}", trace.summary()),
            other => f.write_str(other.as_str()),
        }
    }
}

/// Proof of life, sent when a connection would otherwise be idle.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Heartbeat {
    /// Sender's wall clock in milliseconds. Diagnostics only — never used for
    /// expiry or ordering, both of which would need clock synchronisation.
    pub sent_unix_ms: u64,
}

impl Heartbeat {
    /// A heartbeat stamped with `at`.
    ///
    /// Takes the time rather than reading a clock, so nothing in this crate has
    /// an ambient notion of "now".
    #[must_use]
    pub fn at(at: SystemTime) -> Self {
        let millis = at
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |since| since.as_millis());
        Self {
            sent_unix_ms: u64::try_from(millis).unwrap_or(u64::MAX),
        }
    }
}

/// A frame the peer should not have sent.
#[track_caller]
pub(crate) fn decode_error(msg: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(frame: Frame) -> Frame {
        let encoded = frame.clone().encode();
        assert_eq!(
            encoded.len(),
            frame.encoded_len(),
            "encoded_len disagrees with encode"
        );
        Frame::decode(encoded).expect("decode")
    }

    #[test]
    fn data_frames_round_trip() {
        let frame = Frame::Data(DataFrame::new("cgroup", 2, Bytes::from_static(b"counters")));
        assert_eq!(round_trip(frame.clone()), frame);
        assert_eq!(frame.lane(), Lane::Data);
        assert_eq!(frame.payload_len(), 8);
        assert_eq!(frame.to_string(), "data cgroup/v2 8B");
    }

    #[test]
    fn chunked_data_frames_round_trip() {
        let frame = Frame::Data(DataFrame::chunked(
            "gpu",
            1,
            Chunk::new(77, 2, 5),
            Bytes::from_static(b"part"),
        ));
        let decoded = round_trip(frame.clone());
        assert_eq!(decoded, frame);
        let Frame::Data(data) = decoded else {
            panic!("expected data");
        };
        let chunk = data.chunk.expect("chunk");
        assert_eq!(chunk, Chunk::new(77, 2, 5));
        assert!(!chunk.is_last());
        assert!(Chunk::new(77, 4, 5).is_last());
        assert_eq!(frame.to_string(), "data gpu/v1 4B chunk 3/5");
    }

    #[test]
    fn hello_round_trips_with_its_service_list() {
        let frame = Frame::Hello(
            Hello::new("node-0042")
                .with_build("cs-agent 0.1.0")
                .with_services([ServiceInfo::new("cgroup", 2), ServiceInfo::new("gpu", 1)]),
        );
        let decoded = round_trip(frame.clone());
        assert_eq!(decoded, frame);

        let Frame::Hello(hello) = decoded else {
            panic!("expected hello");
        };
        assert_eq!(hello.node, "node-0042");
        assert!(hello.protocol_matches());
        assert_eq!(hello.service("gpu"), Some(&ServiceInfo::new("gpu", 1)));
        assert_eq!(hello.service("nope"), None);
        assert_eq!(hello.services[0].to_string(), "cgroup/v2");
        assert_eq!(frame.lane(), Lane::Control);
    }

    #[test]
    fn goodbye_round_trips_every_reason() {
        for goodbye in [
            Goodbye::shutdown("operator asked"),
            Goodbye::restart("server upgrade"),
            Goodbye::error("frame too large"),
        ] {
            let reason = goodbye.reason;
            let frame = Frame::Goodbye(goodbye);
            assert_eq!(round_trip(frame.clone()), frame);
            assert_eq!(
                reason.should_reconnect(),
                reason != GoodbyeReason::Shutdown,
                "{reason} reconnect rule"
            );
        }
    }

    #[test]
    fn commands_round_trip_including_custom_payloads() {
        let frame = Frame::Command(
            CommandFrame::for_service(
                CommandId(9),
                "cgroup",
                CommandKind::Custom(Bytes::from_static(b"interval=1s")),
            )
            .with_ttl(Some(Duration::from_secs(30)))
            .forced(),
        );
        let decoded = round_trip(frame.clone());
        assert_eq!(decoded, frame);

        let Frame::Command(cmd) = decoded else {
            panic!("expected command");
        };
        assert_eq!(cmd.id, CommandId(9));
        assert!(!cmd.is_agent_wide());
        assert_eq!(cmd.target(), "cgroup");
        assert!(cmd.force);
        assert_eq!(cmd.ttl, Some(Duration::from_secs(30)));
        assert!(!cmd.kind.is_builtin());
        assert_eq!(frame.payload_len(), 11);
    }

    #[test]
    fn agent_wide_commands_have_no_service() {
        let frame = Frame::Command(CommandFrame::for_agent(CommandId(1), CommandKind::Restart));
        let decoded = round_trip(frame.clone());
        let Frame::Command(cmd) = decoded else {
            panic!("expected command");
        };
        assert!(cmd.is_agent_wide());
        assert_eq!(cmd.target(), "<agent>");
        assert_eq!(cmd.ttl, None);
        assert!(cmd.kind.is_builtin());
        assert_eq!(frame.to_string(), "command #1 restart -> <agent>");
    }

    #[test]
    fn command_results_round_trip_every_outcome() {
        let outcomes = [
            Outcome::Ok,
            Outcome::Unsupported,
            Outcome::Rejected("mid-flush".into()),
            Outcome::Expired,
            Outcome::UnknownService,
            Outcome::Failed(ErrorTrace::new(
                "node-7",
                ErrorKind::Plugin,
                vec![crate::TraceFrame::located("nvml missing", "gpu.rs", 19)],
            )),
        ];
        for outcome in outcomes {
            let frame = Frame::CommandResult(CommandResult::new(CommandId(4), outcome.clone()));
            assert_eq!(round_trip(frame.clone()), frame);
            assert_eq!(frame.lane(), Lane::Control);
        }
        assert!(Outcome::Ok.is_ok());
        assert!(!Outcome::Expired.is_ok());
        assert_eq!(
            Outcome::Rejected("busy".into()).to_string(),
            "rejected: busy"
        );
    }

    #[test]
    fn heartbeats_round_trip() {
        let at = SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
        let frame = Frame::Heartbeat(Heartbeat::at(at));
        assert_eq!(round_trip(frame.clone()), frame);
        let Frame::Heartbeat(hb) = frame else {
            panic!("expected heartbeat");
        };
        assert_eq!(hb.sent_unix_ms, 1_700_000_000_000);
        // A clock before the epoch must not panic.
        assert_eq!(
            Heartbeat::at(SystemTime::UNIX_EPOCH - Duration::from_secs(1)).sent_unix_ms,
            0
        );
    }

    #[test]
    fn lanes_follow_from_the_body_and_sort_control_first() {
        assert!(Lane::Control < Lane::Data);
        let mut lanes = [Lane::Data, Lane::Control];
        lanes.sort_unstable();
        assert_eq!(lanes, [Lane::Control, Lane::Data]);
        assert_eq!(Lane::Control.to_string(), "control");
    }

    #[test]
    fn every_kind_has_a_stable_name() {
        let frames = [
            Frame::Data(DataFrame::new("s", 1, Bytes::new())),
            Frame::Hello(Hello::new("n")),
            Frame::Goodbye(Goodbye::shutdown("")),
            Frame::Command(CommandFrame::for_agent(CommandId(1), CommandKind::Shutdown)),
            Frame::CommandResult(CommandResult::new(CommandId(1), Outcome::Ok)),
            Frame::Heartbeat(Heartbeat { sent_unix_ms: 0 }),
        ];
        let names: Vec<_> = frames.iter().map(Frame::kind_str).collect();
        assert_eq!(
            names,
            [
                "data",
                "hello",
                "goodbye",
                "command",
                "command_result",
                "heartbeat"
            ]
        );
    }

    // --- rejection of frames a correct peer would never send ---

    fn decode_proto(message: proto::Frame) -> Error {
        let mut buf = BytesMut::new();
        prost::Message::encode_raw(&message, &mut buf);
        Frame::decode(buf.freeze()).expect_err("should not decode")
    }

    #[test]
    fn a_frame_with_no_body_is_rejected() {
        let err = decode_proto(proto::Frame { body: None });
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.to_string().contains("no body"));
    }

    #[test]
    fn garbage_bytes_are_a_decode_error_and_keep_the_prost_cause() {
        let err = Frame::decode(Bytes::from_static(&[0xff, 0xff, 0xff, 0xff])).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(!err.is_retryable(), "a bad frame must not trigger a retry");
        assert!(err.chain().count() >= 2);
    }

    #[test]
    fn unspecified_enum_members_are_rejected() {
        let cases: Vec<(proto::frame::Body, &str)> = vec![
            (
                proto::frame::Body::Goodbye(proto::Goodbye::default()),
                "goodbye has no reason",
            ),
            (
                proto::frame::Body::Command(proto::Command {
                    id: 1,
                    ..proto::Command::default()
                }),
                "command has no kind",
            ),
            (
                proto::frame::Body::CommandResult(proto::CommandResult {
                    id: 1,
                    ..proto::CommandResult::default()
                }),
                "command result has no outcome",
            ),
        ];
        for (body, expected) in cases {
            let err = decode_proto(proto::Frame { body: Some(body) });
            assert_eq!(err.kind(), ErrorKind::Decode);
            assert!(
                err.to_string().contains(expected),
                "expected {expected:?}, got {err}"
            );
        }
    }

    #[test]
    fn enum_values_from_a_newer_peer_are_rejected_not_guessed() {
        let err = decode_proto(proto::Frame {
            body: Some(proto::frame::Body::Goodbye(proto::Goodbye {
                reason: 99,
                detail: String::new(),
            })),
        });
        assert!(err.to_string().contains("unknown goodbye reason 99"));
    }

    #[test]
    fn nameless_and_idless_frames_are_rejected() {
        let cases: Vec<(proto::frame::Body, &str)> = vec![
            (
                proto::frame::Body::Data(proto::Data {
                    service: String::new(),
                    ..proto::Data::default()
                }),
                "no service name",
            ),
            (
                proto::frame::Body::Hello(proto::Hello::default()),
                "no node name",
            ),
            (
                proto::frame::Body::Hello(proto::Hello {
                    node: "n".into(),
                    services: vec![proto::ServiceInfo::default()],
                    ..proto::Hello::default()
                }),
                "service with no name",
            ),
            (
                proto::frame::Body::Command(proto::Command {
                    id: 0,
                    kind: proto::CommandKind::Restart as i32,
                    ..proto::Command::default()
                }),
                "command has no id",
            ),
            (
                proto::frame::Body::CommandResult(proto::CommandResult {
                    id: 0,
                    outcome: proto::Outcome::Ok as i32,
                    ..proto::CommandResult::default()
                }),
                "command result has no id",
            ),
        ];
        for (body, expected) in cases {
            let err = decode_proto(proto::Frame { body: Some(body) });
            assert!(
                err.to_string().contains(expected),
                "expected {expected:?}, got {err}"
            );
        }
    }

    #[test]
    fn a_chunk_index_past_its_count_is_rejected() {
        let err = decode_proto(proto::Frame {
            body: Some(proto::frame::Body::Data(proto::Data {
                service: "s".into(),
                chunk_index: 5,
                chunk_count: 5,
                ..proto::Data::default()
            })),
        });
        assert!(err.to_string().contains("past its count"));
    }

    #[test]
    fn a_failed_outcome_without_a_trace_is_rejected() {
        let err = decode_proto(proto::Frame {
            body: Some(proto::frame::Body::CommandResult(proto::CommandResult {
                id: 1,
                outcome: proto::Outcome::Failed as i32,
                ..proto::CommandResult::default()
            })),
        });
        assert!(err.to_string().contains("carries no error"));
    }

    // --- chunking arithmetic ---

    #[test]
    fn max_payload_leaves_room_for_the_envelope() {
        for max_frame in [64usize, 256, 1024, 64 * 1024, 4 * 1024 * 1024] {
            for chunk in [None, Some(Chunk::new(u64::MAX, 1, u32::MAX))] {
                let limit = DataFrame::max_payload(max_frame, "cgroup", chunk);
                assert!(limit > 0, "no room at all in {max_frame}");

                let exact = Frame::Data(DataFrame {
                    service: "cgroup".into(),
                    service_version: u32::MAX,
                    chunk,
                    payload: Bytes::from(vec![0u8; limit]),
                })
                .encoded_len();
                assert!(
                    exact <= max_frame,
                    "a max-size payload encoded to {exact} > {max_frame}"
                );

                let over = Frame::Data(DataFrame {
                    service: "cgroup".into(),
                    service_version: u32::MAX,
                    chunk,
                    payload: Bytes::from(vec![0u8; limit + 1]),
                })
                .encoded_len();
                assert!(
                    over > max_frame,
                    "one more byte still fit: {over} <= {max_frame}, so the limit is not tight"
                );
            }
        }
    }

    #[test]
    fn max_payload_is_zero_when_the_envelope_alone_does_not_fit() {
        assert_eq!(DataFrame::max_payload(2, "a-long-service-name", None), 0);
        assert_eq!(DataFrame::max_payload(0, "s", None), 0);
    }

    #[test]
    fn a_longer_service_name_leaves_less_room() {
        let short = DataFrame::max_payload(1024, "cpu", None);
        let long = DataFrame::max_payload(1024, "cgroup-memory", None);
        assert!(long < short);
        assert_eq!(short - long, "cgroup-memory".len() - "cpu".len());
    }

    #[test]
    fn decoding_slices_the_input_rather_than_copying_the_payload() {
        let payload = Bytes::from(vec![7u8; 4096]);
        let encoded = Frame::Data(DataFrame::new("cgroup", 1, payload)).encode();

        let Frame::Data(data) = Frame::decode(encoded.clone()).expect("decode") else {
            panic!("expected data");
        };
        // A `Bytes` payload that shares the frame's allocation points into it;
        // a copied one would have its own.
        let base = encoded.as_ptr() as usize;
        let payload_ptr = data.payload.as_ptr() as usize;
        assert!(
            payload_ptr >= base && payload_ptr < base + encoded.len(),
            "payload was copied out of the frame buffer instead of sliced"
        );
    }
}
