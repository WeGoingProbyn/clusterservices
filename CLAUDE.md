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
apps/              (later)
  agent/           cs-agent       binary: registers samplers, picks transport
  server/          cs-server      binary: registers handlers, admin port
  ctl/             cs-ctl         operator CLI (hostlist-style --nodes)
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
   transport sits behind a Cargo feature.
6. Transport chosen once at startup:
   `match cfg.transport { Tcp => run(NodeEngine::new(TcpTransport::new(..)?)) , .. }`.
   The engine stays generic (`NodeEngine<T: Transport>`); no `Box<dyn Transport>`.

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
  foreign error's `source()` chain carry neither.
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
8. Apps: agent, server, ctl.
9. Later: gRPC transport, relay/aggregation tier, eBPF network plugin,
   RDMA transport.

## Commands

```sh
cargo build --workspace
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all
```

Use `--all-features`: `test_support` and the doctests inside it are behind
`test-util`, and later transports sit behind features of their own.

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
