# clusterservices

A Rust framework for per-job resource monitoring on HPC clusters, focused on
**non-exclusive (shared-node) jobs**. Slurm accounting stores coarse per-job
summaries and LDMS collects node-level metrics without job attribution; this
project fills the gap by sampling per-job usage from cgroup v2 (plus NVML for
shared GPUs, perf counters, eBPF later) and streaming it to a central server.

The framework is built so that **plugin authors write only business logic**.
Plugins never touch tokio, prost, sockets, or the transport in use. Adding a
plugin or a transport must never require rewriting the engine.

> Status: early scaffolding. Most of what's below is design intent, not code
> yet. When implementing, follow this document; if a design point turns out to
> be wrong, update this file in the same change.

## Environment assumptions

- Slurm with **cgroup v2**; job cgroups live under
  `/sys/fs/cgroup/system.slice/slurmstepd.scope/job_<id>/step_<n>/`.
- GPUs are **shared between jobs**; attribution via NVML per-process data,
  mapping PIDs to jobs through `/proc/<pid>/cgroup`.
- Agent runs as a systemd service in its own cgroup (so it can monitor itself).
- Rust edition 2024. Uses native `async fn` / return-position `impl Future` in
  traits (no `async-trait` crate unless `dyn` dispatch is truly required).

## Architecture (bottom up)

```
 plugins       Sampler / Handler / CommandReceiver      business logic only
 ServiceCtx    Outbox<S>, ShutdownSignal, workers       handles defined in core
 NodeEngine    routing, peers, lanes, command queues,   written once, generic
               TTLs, reconnect, supervision, shutdown   over the transport
 Transport     connect / listen / send & recv Frame     mock, TCP, gRPC, RDMA
```

- **Core vocabulary** (`crates/core/*`): error type, runtime-agnostic async
  primitives, plugin-facing traits. **No tokio, no transport code.**
- **Transport**: moves runtime-owned `Frame`s between two peers and reports
  when a connection dies. Knows nothing about services, retries, or commands.
- **NodeEngine**: same engine for agent (dials one server) and server (listens,
  accepts many agents). Owns everything shared across transports. A relay /
  aggregation tier is just an engine in both roles.
- **ServiceCtx**: the only thing a service sees of the engine.
- **Plugins**: samplers on the agent, handlers on the server.

### Data flow

Agent worker → `Outbox::send(msg)` → encoded into a transport-provided buffer →
data lane of the peer queue → writer task wraps in `Frame` → transport → server
reader → engine routes by service name → decodes → `Handler::handle(ctx, msg)`.

### Command flow

Operator / server handler → `ctx.command::<S>(target, cmd)` → per-node control
queue (with expiry) → agent engine → service's `CommandReceiver::on_command` →
`Reply` → `CommandResult` frame (same ID) → server matches to waiting
`CommandHandle`.

## Crate map

Directory names are short and match the package name minus the `cs-` prefix.

```
crates/
  core/
    util/          cs-util        Error, ErrorKind, ResultExt, Frame/Chain.
                                  Zero deps. DONE.
    async-util/    cs-async-util  ShutdownSignal, Clock, BoxFuture,
                                  CommandHandle. Depends on cs-util only — no
                                  runtime, not even as a dev-dependency (its
                                  own futures are tested with the park/unpark
                                  `block_on` in `src/lib.rs`). DONE.
    api/           cs-api         Wire/Encodable, ServiceDef/ServiceId,
                                  ServiceBound, Sampler/SamplerFactory, Handler,
                                  CommandReceiver, Command/Reply/CommandOutcome,
                                  ServiceCtx + handles (Outbox, WorkerSpawner),
                                  JobInfo/StepId, EngineStats, NoCommand, the
                                  `runtime` seam, and `test_support::FakeEngine`
                                  behind the `test-util` feature. The ONLY crate
                                  plugins depend on — it re-exports bytes, prost,
                                  and the cs-util / cs-async-util items they
                                  need, so a plugin manifest has one dependency.
                                  DONE.
  runtime/
    transport/     cs-transport   Transport/Listener/Connection/FrameTx/FrameRx
                                  traits, Frame + Lane, Capabilities, Endpoint,
                                  ErrorTrace, and `proto/frame.proto` (Frame,
                                  Data, Hello, Goodbye, Command, CommandResult,
                                  ErrorTrace, Heartbeat) + encoding. Depends on
                                  cs-util/bytes/prost only — NOT on cs-api: the
                                  transport layer knows bytes and names, not
                                  plugin semantics. DONE.
    transport-mock/ cs-transport-mock  In-process transport with fault
                                  injection. Serializes frames to bytes.
                                  `MockNetwork` is a scoped value, not a global,
                                  so tests can't collide on an endpoint name.
                                  DONE.
    transport-tcp/ cs-transport-tcp   tokio + length-delimited framing
                                  (4-byte big-endian length). Sets SO_REUSEADDR
                                  so a restarted server can rebind at once, and
                                  TCP_NODELAY so a command frame is not held
                                  waiting for company. Passes the contract suite.
                                  DONE.
    engine/        cs-engine      NodeEngine, peer table, lanes, command queues,
                                  service registry + supervision, shutdown,
                                  TokioClock, JobSource. DONE.
    operator/      cs-operator    The operator side of the command protocol:
                                  connect to a head's admin endpoint, send commands
                                  naming nodes, collect the answers. Not an engine —
                                  no services, no queues, no reconnect. Depends on
                                  the cs-transport traits only, so `cs-ctl` drives it
                                  over TCP and the engine's own tests drive the same
                                  code over the mock. DONE.
    testkit/       cs-testkit     TestCluster harness, misbehaving test plugins,
                                  and the transport contract suite generic over
                                  TestTransport. The one crate exempt from the
                                  no-unwrap rule: a harness reports a broken
                                  assumption by panicking. DONE.
  plugins/
    cgroup/        cs-plugin-cgroup  Per-job cgroup v2 sampler: cpu.stat,
                                  memory.{current,peak,stat,events}, io.stat,
                                  pids.current, PSI. Also `CgroupJobs`, the
                                  `JobSource` an agent uses, and `FakeCgroups`
                                  behind `test-util`. DONE.
    selfmon/       cs-plugin-selfmon  The agent watching itself: engine counters,
                                  this process's CPU/RSS from /proc/self, and
                                  **CPU per thread name** — which is what the
                                  `<service>/<worker>` convention was for. Plus
                                  `FakeProc` behind `test-util`. DONE.
    gpu/           cs-plugin-gpu      (later)
apps/
  agent/           cs-agent       binary: dials a head and registers samplers.
                                  Runs `selfmon` always, `cgroup` only when
                                  `--cgroups` says where to look (nothing to read
                                  off a Slurm node), and `burn` — a load generator
                                  that lives in the binary, not in `crates/`,
                                  because it is a development aid. DONE.
  server/          cs-server      binary: accepts agents and logs what they send,
                                  one line per batch naming the node, the job, the
                                  deltas, and which metrics the kernel lacked.
                                  Enough to develop an agent against. DONE
                                  (no storage, no admin port).
  ctl/             cs-ctl         operator CLI: `restart` / `shutdown`, hostlist
                                  `--nodes node-[1-4,7]` with padding preserved,
                                  one line per node and an exit status that says
                                  whether every one of them said ok. DONE.
```

Later transports: `transport_grpc` (tonic bidi stream
`rpc Session(stream Frame) returns (stream Frame)`), `transport_rdma` (UCX or
libfabric via FFI, progress thread bridged to async).

### Dependency rules (enforce in review)

1. `crates/core/*` never depends on tokio or any runtime/transport crate.
2. Plugins depend on `cs-api` only — including their generated protobuf code,
   which `prost_build::Config::prost_path("cs_api::prost")` points at cs-api's
   re-export so the plugin needs no direct prost dependency. `JobSource` lives in
   cs-api for the same reason: the crate that knows how to find jobs is a plugin.
3. Transport impls depend on `cs-transport` (+ tokio), never on `cs-engine`.
4. `cs-engine` depends on the `cs-transport` traits, never on an impl.
5. Only `apps/*` and `cs-testkit` depend on concrete transports; each non-mock
   transport sits behind a Cargo feature. `cs-operator` is on the same footing as a
   transport impl for this purpose: it takes a `T: Transport` and never names one.
6. Transport chosen once at startup, by a `match` whose arms each call a generic
   `run<T: Transport>(..)`. `Box<dyn Transport>` is not merely discouraged, it is
   **impossible**: `Transport::connect` returns `impl Future`, so the trait is not
   dyn-compatible. The engine stays generic (`NodeEngine<T: Transport>`).
   **Dispatch on the endpoint's own scheme**, as `cs-server` does — `tcp://…`
   selects the TCP transport, and a later `grpc://…` is one more arm with no
   separate setting to keep in step. A scheme with no feature compiled in is an
   `ErrorKind::Config` naming it.

## Key types (reference)

```rust
// cs-api — as built
pub trait Wire: Sized + Send + 'static {
    fn encoded_len(&self) -> usize;
    fn encode(&self, buf: &mut dyn BufMut);   // lets transports supply registered memory
    fn decode(buf: Bytes) -> Result<Self, Error>;
}
// blanket impl for T: prost::Message + Default + 'static.
// `&mut dyn BufMut` satisfies prost's `impl BufMut` parameter directly, so this
// path needs no adapter and no unsafe.

// Wire is Sized (decode), so the engine cannot hold `dyn Wire`. This is the
// object-safe half it holds instead; blanket-implemented for every Wire type.
pub trait Encodable: Send {
    fn encoded_len(&self) -> usize;
    fn encode(&self, buf: &mut dyn BufMut);
}

pub trait ServiceDef: 'static {
    const NAME: &'static str;         // 1..=12 bytes of [a-z0-9_-]
    const VERSION: u32 = 1;
    type Data: Wire;
    type Command: Wire;               // NoCommand (uninhabited) if none

    // Never overridden. Anything building a ServiceId reads it, which turns an
    // invalid NAME into a build failure instead of a startup failure on 1000 nodes.
    const CHECK_NAME: () = assert!(is_valid_service_name(Self::NAME));
}

// Name + version with the type erased: what the engine routes, queues and logs on.
pub struct ServiceId { pub name: &'static str, pub version: u32 }
impl ServiceId {
    pub const AGENT: Self;                        // empty name = whole-agent target
    pub const fn of<S: ServiceDef>() -> Self;
}

// Supertrait of both CommandReceiver and Handler. It exists so `Data<Self>` and
// `Cmd<Self>` can name a plugin's message types through one path regardless of
// which trait it is implementing.
pub trait ServiceBound: 'static { type Service: ServiceDef; }
pub type Data<T> = <<T as ServiceBound>::Service as ServiceDef>::Data;
pub type Cmd<T>  = <<T as ServiceBound>::Service as ServiceDef>::Command;

// A message type with no values, aliased to say which kind is absent:
// `NoCommand` for a service taking no custom commands, `NoData` for one that
// sends nothing. `Vec<NoData>` cannot be non-empty, so `Sampler::sample`
// returning it is silence the compiler checks — which is what `burn` needs, a
// service whose entire effect is local.
pub enum Never {}
pub type NoCommand = Never;
pub type NoData = Never;

pub enum Command<C> { Shutdown, Restart, Custom(C) }
pub enum Reply { Default, Handled, Rejected(String) }

pub trait CommandReceiver: ServiceBound {
    fn on_command(&mut self, _cmd: Command<Cmd<Self>>) -> Reply { Reply::Default }
}

pub trait Sampler: CommandReceiver + Send + 'static {
    fn interval(&self) -> Duration;
    fn sample(&mut self, jobs: &[JobInfo]) -> Result<Vec<Data<Self>>>;
    // Runs once on the sampler's thread before the first sample, and after every
    // rebuild. The hook for anything needing the engine: cloning the Outbox for
    // extra workers, reading engine_stats, failing startup when NVML is absent.
    fn start(&mut self, _ctx: &ServiceCtx<Self::Service>) -> Result<()> { Ok(()) }
    fn on_shutdown(&mut self, _jobs: &[JobInfo]) -> Result<Vec<Data<Self>>> { Ok(Vec::new()) }
}

// Registered as a factory so Restart and panic-recovery can rebuild. Any
// `FnMut() -> S` qualifies. Fallible construction belongs in Sampler::start.
pub trait SamplerFactory: Send + 'static { type Sampler: Sampler; fn build(&mut self) -> Self::Sampler; }

// `Origin` is not in the original sketch and has to be: a server that cannot tell
// which node sent a batch cannot attribute anything — the entire point of this
// project — nor address a command back to the sender.
pub struct Origin<'a> { pub node: &'a str, pub service_version: u32 }

pub trait Handler: ServiceBound + Send + Sync + 'static {
    fn handle(&self, ctx: &ServiceCtx<Self::Service>, from: Origin<'_>, msg: Data<Self>)
        -> impl Future<Output = Result<()>> + Send;
    fn shutdown(&self) -> impl Future<Output = ()> + Send { async {} }
}
// RPITIT makes Handler non-dyn-compatible; the engine boxes the future once at
// the point where it erases the handler for its routing table. No async-trait.

// cs-api::runtime — the seam the engine implements and plugins never name.
// It is what lets ServiceCtx reach a `NodeEngine<T: Transport>` without any
// plugin type mentioning T.
pub trait DataSink: Send + Sync + 'static {
    fn send(&self, service: ServiceId, msg: &dyn Encodable) -> Result<()>;
}
pub trait WorkerHost: Send + Sync + 'static {
    fn spawn_blocking(&self, name: &str, body: Box<dyn FnOnce() + Send + 'static>) -> Result<()>;
}
pub trait ServiceRuntime: Send + Sync + 'static {
    fn node(&self) -> &str;
    fn clock(&self) -> &Arc<dyn Clock>;
    fn engine_stats(&self) -> EngineStats;
    fn dispatch_command(&self, req: CommandRequest<'_>) -> CommandHandle<CommandOutcome>;
}
pub enum CommandKind<'a> { Shutdown, Restart, Custom(&'a dyn Encodable) }

// cs-transport — as built
pub trait Transport: Send + Sync + 'static {
    type Conn: Connection;
    type Listener: Listener<Conn = Self::Conn>;
    fn capabilities(&self) -> Capabilities;   // max frame size, native lanes, zero-copy
    fn connect(&self, ep: &Endpoint) -> impl Future<Output = Result<Self::Conn>> + Send;
    fn listen(&self, ep: &Endpoint) -> impl Future<Output = Result<Self::Listener>> + Send;
}
pub trait Listener: Send + Sync + 'static {
    type Conn: Connection;
    fn accept(&self) -> impl Future<Output = Result<Self::Conn>> + Send;  // &self: shared accept loop
    fn local_endpoint(&self) -> Result<Endpoint>;   // tests bind :0 and ask what they got
}
pub trait Connection: Send + 'static {
    type Tx: FrameTx;
    type Rx: FrameRx;
    fn peer(&self) -> Endpoint;                     // for logs
    fn split(self) -> (Self::Tx, Self::Rx);
}
// FrameTx: send(Frame) by value, flush() (default no-op), close() for a clean close.
// FrameRx::recv() -> Ok(None) on clean close, Err(retryable) on a broken one.

// The frame itself. No lane field: the lane follows from the variant, so the two
// can never disagree. Payload/reason/error are folded into the variants that
// carry them, so a nonsensical frame cannot be constructed.
pub enum Frame {
    Data(DataFrame), Hello(Hello), Goodbye(Goodbye),
    Command(CommandFrame), CommandResult(CommandResult), Heartbeat(Heartbeat),
}
pub enum Lane { Control, Data }            // Ord: Control sorts first, matching drain order

pub enum Frame {
    Data, Hello, Goodbye, Command, CommandResult, Heartbeat,
    Reachable(Reachable),          // what a tier below can reach; sent upward
    Status(StatusRequest),         // what do you know?
    StatusReport(StatusReport),    // this, and how long I have been up
}

// `CommandFrame.node`: which node this is for, when that is not the peer being
// sent to. Empty on the server -> agent hop (the connection already says which
// node, and a second answer could only disagree); set by an operator, whose one
// connection reaches every node the head serves, and by a tier forwarding down.
pub enum CommandKind { Shutdown, Restart, Custom(Bytes) }
pub enum Outcome { Ok, Unsupported, Rejected(String), Expired, UnknownService, Failed(ErrorTrace) }

// Endpoint is `scheme://authority` (tcp://, mock://, ucx://), not an enum — the
// set of transports is open. A transport calls `ep.require_scheme("tcp")?` and
// parses the authority itself. A bad endpoint is ErrorKind::Config.
```

`Sampler` is a convenience adapter: a service with one worker running a
sleep/sample/command loop. Plugins needing several workers use `ServiceCtx`
directly (`spawn_blocking_worker`, cloned `Outbox`, `ShutdownSignal`), reached
from `Sampler::start`.

`ServiceCtx<S>` is the whole surface: `service()`, `node()`, `send()`,
`outbox()`, `workers()`, `spawn_blocking_worker()`, `shutdown_signal()`,
`is_shutting_down()`, `clock()`, `engine_stats()`, `command::<T>(node, cmd,
opts)`, `agent_command(node, cmd, opts)`. It is `Clone`, `Send + Sync`, and
names no transport.

**Why `sample` returns `Result`** (deviation from the original sketch, which
returned a bare `Vec`): a job whose cgroup vanished mid-sample is normal — skip
it and return the rest. `Err` means "this sampler could not do its job at all",
which the engine logs with the full error chain, counts in
`ServiceStats::sample_errors`, and recovers from. Without it a total failure is
indistinguishable from "no jobs to report", and the error type earns nothing.
`on_shutdown` matches it for symmetry.

## Behavioural contracts

**Envelope / routing**
- Services are routed by `ServiceDef::NAME` string. Unknown service on receipt:
  log, count, drop. Never kill the connection.
- Node name is sent once in `Hello`, along with the agent's service list (lets
  the server reject commands for services a node doesn't run).
- Two lanes: **control** (commands, results, heartbeats, hello/goodbye) is
  always drained before **data**. Transports may map lanes natively
  (`Capabilities::native_lanes`); otherwise the engine interleaves in `Lane`
  order.
- Engine chunks payloads above `Capabilities::max_frame`, sizing each chunk with
  `DataFrame::max_payload(max_frame, service, chunk)`. That figure is not a
  constant and must not be hand-derived: the service name's length and the
  varint widths of the chunk numbers both move it, and a larger payload can
  widen the length delimiter of the `Data` message enclosing it.
- The frame protocol is validated on decode, not trusted: a missing body, an
  `_UNSPECIFIED` or unrecognised enum member, an empty node/service name, a zero
  command id, a chunk index past its count, and a `Failed` outcome with no
  trace are all `ErrorKind::Decode`. Decode is *not* retryable — re-reading the
  same bytes cannot help — so a peer speaking nonsense never triggers a
  reconnect storm.
- Wire compatibility: fields are only added, never renumbered; every enum keeps
  `_UNSPECIFIED = 0`; `PROTOCOL_VERSION` in `Hello` is bumped only for a change
  an older peer cannot decode.
- **A transport may return `Decode` only when it has consumed exactly one frame's
  bytes and the stream is still framed.** The engine answers `Decode` by reading
  the *next* frame, so a transport that has lost frame alignment must report
  `Transport` and let the connection be rebuilt. In the TCP transport that is the
  difference between a bad payload (`Decode`, skip it) and an untrustworthy length
  prefix (`Transport`, reconnect).
- `proto/frame.proto` is compiled by **protox**, not protoc, so building the
  workspace needs nothing but a Rust toolchain — no system protobuf on cluster
  build hosts. `bytes` fields decode as `Bytes` slices of the input buffer, so
  receiving a frame does not copy its payload.

**Commands**
- Built-ins `Shutdown`, `Restart`; everything else is `Custom(S::Command)`.
- Target a single service or the whole agent (`ServiceId::AGENT`, the empty name).
  An agent-wide command can only be a built-in, which the type system enforces:
  `agent_command` takes `Command<NoCommand>`, whose `Custom` arm is uninhabited.
- Every command has an ID and an expiry (`CommandOpts { expiry, force }`, default
  60s). Queued per node while disconnected; delivered on reconnect if unexpired,
  else resolved as `Expired`.
- On the wire the expiry is a **relative `ttl_ms`, recomputed each time the
  command leaves a queue**, never an absolute deadline — so expiry needs no clock
  synchronisation between server and node. Nothing in the protocol compares two
  peers' clocks; `Heartbeat.sent_unix_ms` is diagnostic only.
- `CommandOutcome` is the *answer*: `Ok | Unsupported | Rejected(reason) |
  Expired | UnknownService`. Failure to deliver at all — no such node, transport
  dead, shutdown — is the `Err` arm of the `CommandHandle` instead, never an
  outcome.
- At-most-once with ack. Per-service ordering only. Custom commands should be
  designed idempotent ("set interval to 1s", not "halve interval").
- `Reply` semantics:
  | Command  | Default                               | Handled                  | Rejected |
  |----------|---------------------------------------|--------------------------|----------|
  | Shutdown | engine calls on_shutdown, stops it    | service stopped itself   | keeps running |
  | Restart  | on_shutdown, drop, rebuild via factory | service reset in place  | keeps running |
  | Custom   | reply Unsupported                     | reply Ok                 | reply with reason |
- Agent-wide commands poll every service first; any rejection aborts unless
  `force` is set. `force` skips `on_command` but still runs `on_shutdown`
  under the deadline.
- Samplers are registered as **factories** (`.sampler(|| X::new())`) so
  restart and panic recovery can rebuild them (panic → rebuild with backoff).
  The factory stays on the engine's task and is **never moved onto the sampler's
  worker thread**: a factory lost to a panicking job could not rebuild anything.
  So `build()` runs under `catch_unwind` on the async side, and everything else —
  `start`, `sample`, `on_command`, `on_shutdown` — runs on the worker thread,
  where a panic costs only the instance.
- A service that returns `Err` from `start` is left stopped, not retried: NVML
  being absent will not change while the agent runs. Only panics retry.
- Commands are delivered in **two steps**, `Ask` then `Apply`, so an agent-wide
  command collects every service's vote before any of them is torn down. `apply`
  is set only for a built-in answered `Reply::Default`; `Reply::Handled` means the
  service did it itself and the engine must not also act.
- Command processing is serialised per connection on its own task, which both
  keeps the reader free and preserves per-service ordering — spawning a task per
  command would not.
- Agent restart = graceful shutdown then exit with a dedicated exit code;
  systemd (`RestartForceExitStatus=`) restarts it. No self re-exec.

**Operators**
- An operator is a peer on a **second listen endpoint** (`EngineBuilder::admin`),
  and that is the whole of the authorization: a command arriving there may name any
  node the head serves and is forwarded to it; the same command on the agents'
  endpoint is resolved against the head's own services, which is to say refused.
  **Nothing in this protocol authenticates anybody**, so the bind address is the only
  lock on a fleet-wide restart button — hence a default of `127.0.0.1:7788` and a
  note in `--help` saying why.
- The role comes from **how the connection came to exist** — which endpoint it
  arrived on, or that we dialled it — never from anything the peer claims about
  itself. A flag in `Hello` would be a request to be trusted. There are three:
  `Node` (accepted below us: its commands are for our own services and may name
  nobody else), `Upstream` (the peer we dialled, trusted by construction, and the one
  that sends commands for nodes we serve), and `Operator` (the admin endpoint, same
  routing rights).
- An operator is **not in the peer table**. It is not a node: nothing is addressed to
  it by name, it produces no data, and two operators on one host would otherwise
  collide on a name and evict each other.
- **An operator's command is not queued for a node that is away.** A handler's is —
  that is automation, and it can wait — but an operator is a person watching a
  terminal, and "node-7 is not connected" now beats the same answer in sixty seconds.
  Undeliverable comes back as `Failed(ErrorTrace)`, so the reason reads as an error
  chain rather than an outcome that has to be guessed at.
- A command **for us** — naming no node, or naming this engine — is carried out one
  at a time on the connection's own task, which is what preserves per-service
  ordering. Only commands for somebody else are passed on concurrently.
- Forwarded commands are **not serialised** against each other, unlike a local one.
  The invariant serialisation protects is per-service ordering, and that survives
  because each one goes onto the target node's own ordered control queue; a hostlist
  of a thousand nodes cannot wait for a thousand TTLs in turn.
- The TTL is recomputed on each hop, as everywhere else, so two hops need no more
  clock agreement than one.
- **An operator has to heartbeat.** It is a peer like any other, so `peer_timeout`
  applies to it, and an operator waiting on a slow node is silent. `cs-operator` sends
  one every 5s, which suits the default 45s timeout; a head configured tighter needs
  `with_heartbeat`.

**Tiers**
- A **relay is an engine in both roles** — `listen` for the tier below, `dial` for
  the tier above — which the engine always supported. What was missing, and now
  exists, is everything that crosses a tier for *commands*.
- **Reachability is announced, not asked for.** Any engine that both listens and
  dials sends `Reachable` upward on connecting and whenever what it serves changes:
  its own children at one hop, plus everything they announced one hop further.
  A leaf agent sends none — its name is in its `Hello` and it serves nobody.
- **Always the full set, never a delta**, for the same reason command TTLs are
  relative: state that can drift between two peers is state that will. A parent
  replaces everything it knew about that child, so a lost announcement costs a moment
  of staleness instead of a permanently wrong table. The empty announcement is
  meaningful — it retires the last node behind a relay.
- Announcements are **coalesced** over `reachability_interval` (5s), because five
  hundred agents reconnecting together is one change, not five hundred.
- The parent keeps `announced` **per child** and derives a flat `node → route` table
  from it, rebuilt on every announcement: dispatching a command is then one lookup
  rather than a scan. Keeping it per child is what makes a child's contribution
  removable at all.
- Two children claiming one node is a real misconfiguration (a node dialling two
  relays). The nearer wins and the collision is logged rather than resolved silently.
  `MAX_HOPS` (8) bounds a loop or a peer talking nonsense, and an announcement
  claiming to reach *us* is dropped.
- **Routing a command is the same code as an operator's command**, which is why the
  two arrived together: `Command.node` names the target, the frame goes to the child
  that serves it, and the in-flight entry is keyed on **the child** — so a child that
  disconnects abandons it rather than leaving it waiting for a node it can no longer
  reach.
- What is **not** built: a relay cannot forward *data* upward. A batch for a service
  it has no handler for is counted and dropped, as before. That needs `Data.origin`
  (additive) and the chunk-size arithmetic to account for it, plus a decision about
  whether a middle tier forwards or aggregates. Until then a relay carries commands
  and reachability only, and `Origin { node, via }` in cs-api is ready for the day it
  carries data.

**Status**
- `Status`/`StatusReport` answer "who is connected, and what do they run", from the
  same tables routing uses — so it shows what is behind a relay, with `via` and
  `hops`, and `cs-ctl status` at the global tier lists nodes it has never spoken to.
- `connected` is `None` for an indirect node and set for a direct one: **only the tier
  a node is attached to knows how long it has been there**, and inventing it would
  make a guess look like a measurement.
- Everything is a **duration**, never a timestamp. Nothing in this protocol compares
  two peers' clocks.
- Answered only for a peer that `may_query()`, which is the same set that may route:
  an operator and the tier above. From a compute node it is counted and dropped — it
  has no use for the cluster's inventory, and a peer that can route commands could
  discover the same list by trying them.

**Liveness**
- Heartbeats go out on the control lane when a connection has been idle for
  `heartbeat_interval`; **any** frame resets the idle timer, so a busy connection
  never pays for them.
- `peer_timeout` (default 3× the heartbeat) drops a peer that has said nothing at
  all. This is the only defence against a **half-open connection**: a node that
  loses power sends no `FIN` and no `RST`, so writes keep succeeding into nothing
  and reads never return. Without it a server believes that agent is connected
  until TCP's keepalive notices — hours — and every command queued for it is
  delivered into the void.
- The check lives in the read loop, which already knows when it last heard
  anything; no watchdog task, no shared state. `Ended::Silent` is retryable, so an
  agent redials and a server simply drops the peer.
- `validate()` requires `peer_timeout >= 2 * heartbeat_interval`, so one lost
  heartbeat cannot drop a healthy connection. It cannot check the thing that
  actually matters, though: a **server's timeout must exceed its agents'
  heartbeat interval**, and that is a cross-node configuration question no single
  engine can see.
- `EngineStats::peer_timeouts` is separate from `reconnects` on purpose: a
  reconnect after a clean close is ordinary, a timeout means a peer stopped
  answering without saying so.
- The mock models this with `Link::blackhole(direction)` — frames are counted as
  sent and then discarded, leaving the link up and silent.

**Shutdown ordering**
- One sequence for both roles, in this order, all under one deadline (default
  10s): services stop (each flushing `on_shutdown` into the queue) → plugin
  worker threads join → `Handler::shutdown()` → `Goodbye` queued *behind*
  everything already there → writers drain and close → in-flight commands
  resolved.
- **The deadline has to bound cleanup, not just the wait.** Aborting a service's
  task does not stop the OS thread its `on_shutdown` is blocking on, so
  `NamedWorker` *detaches* rather than joins on drop, and the plugin-worker join
  is itself under the deadline. Joining either would let one plugin hold the
  process open past the deadline that exists for exactly that reason; a detached
  thread owns everything it touches and dies with the process.
- **Service tasks and connection tasks are tracked in two separate `JoinSet`s**,
  and the order above is why: waiting for the connections before closing the
  queues deadlocks until the deadline (the writer cannot finish until the queue
  is closed; the queue must not close until the services have flushed). A single
  set made every shutdown take the full deadline.
- The `Goodbye` reason depends on direction: upstream it reports our own stop
  reason; **downstream it is always `RESTART`**, because an agent's job is to
  keep reaching its server and only an operator stops an agent.
- Agent outbound data queue is bounded; on long outages drop oldest
  (disk spill is a possible later runtime-only feature). The queue belongs to the
  engine, **not to a connection**, so a reconnect finds the buffer intact; only
  queued *control* frames are dropped on reconnect, since a result for a dead
  connection can never be delivered.
- Restart is reported as `Stop::Restart` from `run()`, for the app to turn into
  the exit status systemd matches with `RestartForceExitStatus=`.

**Metrics conventions**
- Send cumulative counters, not rates; server computes rates.
- Sample every few seconds, batch and send every 30–60s.
- Batches are columnar (one timestamp array, one value array per metric).
- **An empty series means the metric is unavailable, never that it is zero.** A
  fleet is not uniform: `memory.peak` needs Linux 5.19, PSI needs `CONFIG_PSI`,
  `cpu.stat` reports throttling only once a limit is set. Flattening absent to
  zero invents data that looks real.
- Memory is a **gauge**, not a counter, and cannot be otherwise — which is why
  `memory.peak` is carried alongside `memory.current`: it is the only memory
  figure a sample gap cannot hide.
- **Flush a job's partial batch the tick its cgroup disappears.** With a 45s
  window, a job that ran for ten seconds would otherwise leave no record at all.
  The sampler compares the engine's job list against what it is accumulating and
  flushes what has gone *before* reading the rest.
- A plugin reads the world through files, so test it with files. `FakeCgroups`
  (feature `test-util`) builds a throwaway tree; mocking the filesystem would test
  the mock.
- A *missing* control file is ordinary (`Ok(None)`); a file that is **present and
  in an unexpected format** is `ErrorKind::Decode` naming the file and line,
  because reporting zero there would turn a wrong assumption about the kernel into
  plausible-looking data.

**Self-monitoring**
- Engine keeps atomic counters (queue depths, frames/bytes, drops,
  reconnects, command RTT, per-service `sample()` timings) exposed read-only
  via `ServiceCtx::engine_stats()` and `EngineHandle::stats()`.
- `cs-plugin-selfmon` reports them, and is **an ordinary plugin on purpose**: no
  privileged hook, no special path into the engine. It is batched, chunked, queued
  and dropped under pressure on the same terms as the metrics it reports on — a
  shortcut would mean measuring a different system from the one running.
- Thread CPU is summed **by name, not by tid**: a tid means nothing to a server.
  The consequence is that a rebuilt service gets a fresh thread starting from zero,
  so a thread's CPU can step *down* across a restart; `panics`/`restarts` in the
  same batch say why.
- `USER_HZ` is assumed to be 100 (`DEFAULT_USER_HZ`, overridable). Reading it
  properly needs `sysconf(_SC_CLK_TCK)` and therefore libc, which rule 2 forbids a
  plugin.
- `/proc/<pid>/stat` **cannot be parsed by splitting on whitespace.** Field 2 is a
  thread name in parentheses, and a thread name may contain spaces *and*
  parentheses — so the parse starts at the **last** `)`. Getting this wrong yields
  a plausible-looking wrong number rather than an error. There is a test with a
  thread called `we (are) evil`.
- Worker threads are named `<service>/<worker>` (≤15 chars, Linux limit) so
  per-plugin CPU can be read from `/proc/self/task/<tid>/{comm,stat}`.
  `cs_api::worker_thread_name` does the trimming — worker part first, service
  part only if it still does not fit — and this budget is *why*
  `MAX_SERVICE_NAME_LEN` is 12.
- **How named threads and a controllable clock coexist:** a sampler's loop is an
  async task (so every interval goes through `Clock` and a paused-time test drives
  it), while `sample()` itself runs on a dedicated thread named `<service>/sample`
  (so CPU attribution works). `spawn_blocking` would give neither a stable name
  nor a private thread. The sampler instance travels into each job and back out,
  which costs one channel round trip per sample and makes a panic lose exactly the
  instance — nothing half-updated survives to rebuild around. Engine code must
  keep using `Clock`, never `Instant::now`/`thread::sleep`.
- Server may route a service's output to a same-process handler via a local
  peer that skips the transport.

## Design notes (not built)

### Tiers: agent → relay → cluster head → global
<!-- Mostly built now; what is left is marked below. -->

The intended shape is more than one cluster, each with a head, and a tier above
them all:

```
                         global
                  /                  \
          cluster-a head        cluster-b head
           /        \                 |
       relay      relay             relay
       /  \        /  \            /   \
    agents      agents           agents
```

**This works now, for commands.** Every middle tier is an engine in both roles —
`listen` for the tier below, `dial` for the tier above — and two of the three things
that had to exist for a tier to be crossed have been built:

1. ~~**`Hello` advertises only the peer's own services.**~~ Done: `Reachable`
   announcements, the full set each time, coalesced, with a `node → child` table
   derived from them. See **Tiers** under the behavioural contracts.
2. ~~**A relay must forward a command addressed to a node it serves.**~~ Done: the
   same code that carries an operator's command, which is why the two arrived
   together. `cs-ctl status` at a global head lists nodes it has never spoken to, and
   `cs-ctl restart -n node-1` from there reaches a node two tiers down.
3. **Data carries no origin.** Still true, and now the only gap. `Data` has
   `service`, `service_version` and `payload`; who produced it is implicit in which
   connection it arrived on, which is correct with two tiers and wrong with three — a
   relay forwarding upward would have the head attribute every agent's metrics to the
   relay. Needs a `Data.origin` field (additive), set by the first tier that forwards;
   `Origin { node, via }` in cs-api is already the shape a handler sees.

   Two things make it more than adding a field. `DataFrame::max_payload` decides the
   chunk size and would have to account for the origin's length — that figure must not
   be hand-derived. And there is a policy question a field does not answer: does a
   middle tier forward a batch it has no handler for, or aggregate it? Today it counts
   and drops it, which is at least visible rather than wrong. **The one thing that was not additive has been done**: `Origin` in `cs-api`
now distinguishes `node` (who produced this) from `via` (who delivered it). That
is a plugin-facing *semantic*, and a handler written against "`node` is who I am
talking to" would silently attribute a whole rack to a relay the day one appeared.
They are equal in every two-tier deployment and there is a test pinning it.

Two further consequences, recorded so they are not rediscovered late:

- **Node names must be qualified above a cluster.** `node-1` exists in every
  cluster. Storage at the global tier keys on (cluster head, origin) rather than
  origin alone; commands address a path — `clusterA/node-1` — which needs no
  protocol change, because `CommandRequest.node` is already a string and each tier
  strips its own prefix as it forwards.
- ~~**A tier that forwards commands needs a `node → child` table**~~, built from the
  reachability announcements above. Built: `Inner::routes`, derived from what each
  child announced. This is where the relay tier wins over independent servers — that
  table is local knowledge, refreshed by the peers themselves, where N independent
  servers would need a distributed directory that stays correct while nodes move.

### Several servers sharing the load

The agent takes one `dial` endpoint today. Spreading a cluster across several
servers needs three things, and only the first is easy.

1. **An ordered list with failover.** `dial(endpoint)` becomes
   `dial([endpoints])` plus a policy, and the dial loop advances on failure and
   resets on success. Backoff must apply per *cycle* through the list, not per
   endpoint, or an agent with five servers hammers all five before it ever waits.
   Useful under every design below, including a redundant relay pair.

2. **Command routing, which is the actual problem.** Server-side state is not
   stateless: commands are queued *per node* on the server that issued them. If an
   operator tells server A to restart a service on node-7 while node-7 is
   connected to server B, A's queue never delivers it. That needs either a shared
   node→server directory, an operator-facing layer that routes to the right
   server, or servers forwarding to each other.

3. **Metric continuity across a move.** The server differences cumulative counters
   to get rates. When node-7 moves from A to B, B has no previous value, so the
   first batch after the move yields no rate — recoverable, and a real payoff of
   the cumulative-counters rule, since pre-computed rates would have produced a
   *wrong* number instead of a missing one. The storage layer must treat "first
   sighting of this series" as "no rate yet", never as "counter jumped from zero".

**A relay tier is probably the better answer than N independent servers**, and
this document already anticipated it ("a relay is just an engine in both roles").
The
deciding argument is (2): a relay knows exactly which agents are connected to it,
so forwarding a command downward is a **local** table lookup, whereas independent
servers need a distributed directory that has to stay correct while nodes move.
The relay also collapses the central server's peer count from ten thousand to
tens, which is the scaling win that matters. What it needs that does not exist
yet: a relay must *forward* a command addressed to a node it serves, rather than
only resolving commands against its own services.

**Topology belongs in whatever writes the agent's config, not in the agent.** An
agent that queried Slurm for the switch hierarchy would need a Slurm dependency,
credentials, and a reason to be trusted with them. An ordered server list *is* the
topology decision, already made by whatever provisions the node — so the framework
only ever needs the list and a failover policy, and stays free of Slurm entirely.

### A Slurm API wrapper

Worth building; worth building **outside this repo**. Slurm's OpenAPI spec changes
shape between versions, so a wrapper that presents one stable API across them is
real work with its own release cadence, and exporting it to Python through pyo3
serves scripting users who will never run an agent. Nothing in `clusterservices`
needs it: the agent reads cgroups, and the server is told node names by the agents
themselves. The one place the two meet is generating agent configuration —
including the topology-ordered server list above — which is a provisioning
concern, not a framework one.

## Error handling

All crates use `cs_util::Error` (boxed, one pointer wide).

- Construct with `Error::new(kind, msg)`; add context with
  `.context("...")` / `.with_context(|| ...)`. All three and every
  `From<ForeignError>` impl are `#[track_caller]`, so `?` records the line it
  was used on.
- Context inherits the inner error's `ErrorKind`. `.with_kind(k)` reclassifies
  the outermost frame only — that is how a socket `Io` failure becomes a
  retryable `Transport` one at the transport boundary.
- Foreign errors enter through `Error::with_source(kind, msg, e)` (message plus
  cause) or `Error::foreign(kind, e)` (cause only, no message of our own), both
  of which keep `e` whole so `err.downcast_ref::<E>()` still works. Only
  `From<std::io::Error>` (→ `Io`) exists as an implicit conversion; every other
  foreign error must have its kind chosen explicitly at the boundary. Add a
  `From` impl only where the kind is unambiguous for every call site, and keep
  it `#[track_caller]`.
- `err.chain()` walks the frames outermost-first, descending into a foreign
  error's own `source()` chain, and yields `Frame { message, location, kind }`
  — this is exactly what gets flattened into `ErrorTrace` for the wire.
  Framework frames carry a location and kind; frames borrowed from inside a
  foreign error's `source()` chain carry neither, and so do frames rebuilt from a
  remote trace — `Error::remote` / `remote_context` deliberately are **not**
  `#[track_caller]`, because the caller is whatever is decoding the frame and the
  real `file:line` is already folded into the message. Claiming both printed two
  locations for one failure, which `cs-ctl` made obvious the first time it showed a
  remote error.
- `ErrorKind` is small and about **how to react**, not what happened:
  `Io, Transport, Decode, Config, Timeout, Rejected, Shutdown, Plugin`.
  Engine uses `is_retryable()` (Transport, Timeout → reconnect/backoff).
  Do not add a kind per failure.
- `{}` = one line with location; `{:#}` and `Debug` = full tree:
  ```
  Error: failed to start service "gpu" @ crates/runtime/engine/src/engine.rs:212
  |- cause 1 - sampler factory failed @ crates/plugins/gpu/src/lib.rs:48
  |- cause 2 - NVML initialization failed @ crates/plugins/gpu/src/nvml.rs:19
  |- cause 3 - libnvidia-ml.so.1: cannot open shared object file
  ```
- Errors crossing the network are flattened into `ErrorTrace`
  (node, kind, frames of message/file/line) inside `CommandResult`, and
  rebuilt server-side so `cs-ctl` shows the remote trace under the local one.
  `ErrorTrace::from_error` / `to_error` do this. A remote location cannot become
  a real `&'static Location` on this side, so `to_error` folds each `file:line`
  into its frame's message text — the rebuilt chain prints identically to the
  original. The remote `kind` is preserved, so a remote `Timeout` is still
  retryable here; an unrecognised kind from a newer peer falls back to `Plugin`
  and is kept verbatim in the message rather than becoming a new kind.
- No `unwrap`/`expect` in library code except for true invariants, with a
  comment saying why.

## Testing strategy

Build and verify everything below the plugin layer against the **mock
transport** first, then run the **same suite** against TCP.

- Plugin business logic does not need any of the below: `cs-api`'s
  `test_support::FakeEngine` (feature `test-util`) implements the whole `runtime`
  seam as a recorder, so a plugin crate can assert "these jobs produce these
  counters" with no engine, no transport, and no runtime. Use it for plugin unit
  tests; use the harness below for anything crossing a connection.
- `cs-async-util` exposes a park/unpark `block_on` under its own `test-util`
  feature, for crates that must not depend on a runtime but need to await their
  own futures.
- `cs-transport-mock` is hostile, as it must be. What it can do, and the method
  names for each:
  - real backpressure — `MockNetwork::with_capacity(n)`, bounded per direction;
    `Link::queued(dir)` to assert depth
  - kill a connection at any instant — `Link::kill()`, `MockNetwork::kill_all()`;
    both halves then fail retryably and **whatever was in flight is lost**
  - refuse connections — `refuse_connects`, `refuse_connects_times(n)`,
    `allow_connects`, and `kill_on_connect` (which is the nastier one: `connect`
    *succeeds* and hands back an already-dead connection)
  - be slow — `set_send_delay(d)`, on the runtime timer so `start_paused` tests
    control it exactly
  - speak nonsense — `Link::inject_garbage(dir)` and `Link::inject(dir, bytes)`,
    which can also forge a frame the peer never sent
  - enforce its frame ceiling — `with_max_frame(n)`; an oversized frame fails to
    send, so an engine that forgets to chunk fails here, not in production
  - counters per direction — `frames_sent`, `frames_received`, `bytes_sent`
- The mock distinguishes a `close()` from a *dropped* half (the latter is a
  retryable `Transport` error), which is useful for driving the engine's
  broken-connection path — **but nothing may depend on that distinction.** TCP
  sends a FIN either way, so the two are identical on the wire. Intent is carried
  by `Goodbye`, not by the shape of a disconnect: an end of stream with no
  `Goodbye` before it means the peer went away, and the engine reconnects. Engine
  code must handle both shapes.
- The mock serialises frames to bytes on the way through, so every test exercises
  encode/decode rather than passing Rust values around.
- The **operator client is shared, not duplicated**: `cs-operator` is what `cs-ctl`
  runs over TCP and what `TestCluster::operator` drives over the mock, so the six
  forwarding tests in `crates/runtime/engine/tests/engine.rs` exercise the code that
  ships. `connect_as` opens the same client on the *agents'* endpoint, which is how
  "an agent cannot command another node" is tested at all.
- `cs-testkit` provides `TestCluster` (one server + N agents in one process,
  controllable links) and test plugins: counter (sequence numbers for
  loss/dup detection), echo with custom command, rejects-restart,
  panics-on-3rd-sample, slow-on_shutdown.
- Transport contract suite is generic over a `TestTransport` trait; a new
  transport is **done when the suite passes**. Invoke it with
  `cs_testkit::transport_contract!(fixture)`, which generates one `#[tokio::test]`
  per check. The fixture supplies endpoints (they are not portable between
  transports) and declares which optional behaviours it can demonstrate —
  `shows_backpressure` is false for TCP, whose kernel buffers absorb more than a
  test wants to send, so that check is skipped rather than made meaningless.
- **A contract check must never assume a buffer size.** Queueing N frames before
  reading one asserts the buffer, not the ordering, and correctly deadlocks
  against a transport that applies backpressure — which is what a hostile mock
  does. Send and receive concurrently instead.
- `cs-testkit`'s plugins are one configurable sampler
  (`CounterConfig::panics_on_sample`, `fails_on_sample`, `refuses_restart`,
  `refuses_shutdown`, `ignores_custom_commands`, `fails_to_start`,
  `says_farewell`, `slow_to_shut_down`) rather than five near-identical ones,
  because they differ only in which knob is set and would otherwise drift apart.
- **Time**: all engine timing (TTLs, backoff, heartbeats, sampler intervals)
  goes through the `Clock` abstraction in `cs-async-util` so tests can run
  with a controlled clock (`#[tokio::test(start_paused = true)]` for async
  paths). Never call `std::thread::sleep` / `Instant::now` directly in engine
  or sampler-loop code.
- Consider `turmoil` for deterministic network simulation of the TCP transport.
- Real-world edges (cgroup files, NVML, signals, systemd) are covered by a
  small integration run on real nodes, not simulated.

## Build order / roadmap

1. ~~`cs-util` (error type) and `cs-async-util` (ShutdownSignal, Clock).~~ Done.
2. ~~`cs-api` traits and types.~~ Done.
3. ~~`cs-transport` traits + `Frame` + frame proto.~~ Done.
4. ~~`cs-transport-mock`.~~ Done.
5. ~~`cs-engine` against the mock, driven by `cs-testkit` test plugins.~~ Done.
6. ~~`cs-transport-tcp`; contract suite passes on both transports.~~ Done — the
   same 11 checks pass over the mock and over real sockets, plus an engine-level
   run over TCP (`tests/engine_over_tcp.rs`) covering data, a command round trip,
   and an agent surviving a server restart.
7. Plugins: ~~cgroup~~, ~~selfmon~~, then gpu. ← gpu next
8. Apps: ~~server~~, ~~agent~~, ~~ctl~~ (`status`, `restart`, `shutdown`). Done.
   Not built: custom commands from the CLI — a plugin's payload is its own type, so a
   generic tool can only carry bytes it cannot construct; that wants a per-plugin
   subcommand or a `--payload @file` escape hatch.
9. ~~Reachability announcements and the routing table~~ — a relay carries commands and
   answers `status` for nodes behind it. ← **`Data.origin` is what is left** before a
   relay can carry metrics too.
10. Later: gRPC transport, eBPF network plugin, RDMA transport.

## What a binary owns

The engine does none of this, and `apps/server` is the worked example:

1. **Config.** `EngineConfig::validate()` runs inside `build()`, so a bad value
   fails at startup with a named field rather than misbehaving later.
2. **Logging.** The libraries emit `tracing` events; nothing installs a
   subscriber, so until a binary does, every `info!` and `warn!` goes nowhere.
3. **The transport match** — see rule 6.
4. **Registration.** Samplers and a `JobSource` on an agent; handlers on a head.
   Registering no `JobSource` is not the same as registering an empty one: the
   engine then never starts the `jobs/scan` thread, so an agent running only
   plugins that ignore the job list costs nothing for one. Found by reading the
   agent's own thread list on its first run — which is what selfmon is for.
5. **Signals.** SIGTERM/SIGINT → `EngineHandle::shutdown()`; a *second* signal
   exits without waiting (130). Take the handle **before** `run()`, which consumes
   the engine.
6. **Exit status.** `Stop::Restart` → **75** (`EX_TEMPFAIL`), which a unit file
   names in `RestartForceExitStatus=`. `Stop::Shutdown` → 0.

An agent should use a `current_thread` runtime and a head a multi-threaded one:
every blocking read already happens on its own named thread, so an agent's async
side only shuffles frames, and one runtime thread instead of N both costs less and
reads better in its own selfmon output.

A binary is not exempt from the `unwrap`/`expect` ban.

### Running the two of them

```sh
cargo run -p cs-server                                       # tcp://0.0.0.0:7777
cargo run -p cs-agent -- --burn-threads 2 --burn-percent 60   # in another terminal
```

`burn` (`apps/agent/src/burn.rs`) is why that shows anything on a workstation: it
spins threads on a duty cycle and sawtooths a ballast allocation, and because its
workers are named `burn/spin0` the head prints their CPU **by name** next to the
agent's own — the `<service>/<worker>` convention working end to end, which is
otherwise only visible on a busy cluster. It is a plugin in every respect (depends
on `cs-api` alone, registered as a factory, restartable) and so doubles as the
smallest worked example of one; it lives in the binary rather than `crates/plugins/`
because shipping a CPU waster as a library invites someone to enable it in
production.

Verified on this machine: `burn/spin0 +5.970s, burn/spin1 +5.990s` over a ten-second
window at 60% duty on two threads, RSS following the ballast, and the batch flushed
by `on_shutdown` arriving **before** the `Goodbye` — the shutdown ordering above,
observed rather than asserted.

### Installing them

`packaging/` holds the two unit files and their environment files, checked in as
static text — a `build.rs` cannot write outside `OUT_DIR`, runs before anyone has
chosen an install path, and would have to guess `ExecStart=`. The units encode the
contracts above rather than preferences: `Restart=on-failure` with
`RestartForceExitStatus=75` (75 *is* the restart mechanism; exit 0 means an operator
said shutdown and meant it, so `Restart=always` would make the two verbs
indistinguishable), `TimeoutStopSec=20s` against the engine's own 10s deadline,
`Slice=system.slice` so an agent is never under `slurmstepd.scope` looking like a
job, and `After=slurmd.service` without `Requires=`, because an agent that starts
early finds no jobs and says so while one that refuses to start is simply absent.

### Operating a running cluster

```sh
cs-ctl status                                       # every node, direct or behind a relay
cs-ctl status --nodes node-[1-4]                    # narrowed
cs-ctl restart  --nodes node-[1-4,7] --service cgroup
cs-ctl shutdown --nodes node-7                      # the whole agent
cs-ctl restart  --nodes node-[1-100] --timeout 30 --head tcp://head01:7788
```

Exit status is the contract a script depends on: **0** every node said ok, **1** the
head answered but some node did not, **2** no answer at all (no head, bad arguments,
connection lost). A whole-agent `restart` makes the agent exit **75**, which its unit
file turns back into a running agent; that is the only restart mechanism, and there
is deliberately no self re-exec.

## Commands

```sh
cargo build --workspace
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all
```

Use `--all-features`: `test_support` and the doctests inside it are behind
`test-util`, and later transports sit behind features of their own.

Also run **`cargo build -p <crate>` for each crate**: a workspace build unifies
features and so hides a crate that uses a tokio feature it never declared.
`cs-transport-mock` shipped that way for weeks — `tokio::select!` without
`features = ["macros"]` — and only failed when built alone.

## Conventions

- Serialization is synchronous; async lives only at the I/O boundary.
- Sampling code (sysfs reads, NVML) is sync and runs on blocking threads,
  never on the tokio worker pool.
- Use bounded channels everywhere between tasks.
- Every new public trait method gets a default impl where one is sensible, so
  existing plugins keep compiling.
- Keep `cs-api` small; anything a plugin doesn't strictly need belongs in the
  engine.
- Shared metadata (version, edition) and every dependency version live in the
  root `[workspace.package]` / `[workspace.dependencies]`; crates use
  `foo.workspace = true`. Every crate opts into the shared lint set with
  `[lints] workspace = true`.
- `unwrap_used` / `expect_used` / `todo` are clippy lints on the whole
  workspace, so `-D warnings` rejects them in library code. `clippy.toml`
  allows them in tests. The invariant-with-a-comment exception means writing
  the comment *and* an `#[allow]`.
- New public items are documented: `missing_docs` is on (warn + `-D warnings`).
