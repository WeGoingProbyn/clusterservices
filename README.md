# clusterservices

Per-job resource monitoring for HPC clusters, aimed at the case that existing
tools handle worst: **non-exclusive nodes, where several jobs share the hardware.**

Slurm's accounting keeps coarse per-job summaries. LDMS collects detailed
node-level metrics but cannot say which job caused them. When a node runs one job
that distinction does not matter; when it runs six, it is the only thing that
matters. This project fills the gap by sampling usage per job from cgroup v2 —
plus NVML for shared GPUs, perf counters and eBPF later — and streaming it to a
central server.

It is a **framework**, not a single daemon. The point of the layering below is
that a plugin author writes business logic ("read these files, return these
counters") and nothing else: no tokio, no sockets, no framing, no retries. Adding
a plugin or a transport must never mean rewriting the engine.

> **Status: it runs.** An agent samples, batches, chunks, and streams to a server
> that routes to handlers; commands flow back with replies; outages are buffered and
> survived; shutdown is ordered. All of that is exercised end to end over a
> deliberately hostile in-process transport **and** over real TCP sockets. The cgroup
> sampler that reads per-job usage is tested against a fake tree *and* against this
> machine's real one, and `cs-agent` and `cs-server` are two processes you can start
> today — see [Try it](#try-it), and `cs-ctl` can restart a service on a hostlist of
> them. What is missing: the GPU plugin, storage behind the server, and a run on real
> Slurm. See [Roadmap](#roadmap).

## Architecture

Four layers, each ignorant of the one above it:

```
  plugins       Sampler / Handler / CommandReceiver     business logic only
  ServiceCtx    Outbox<S>, ShutdownSignal, workers      the only view of the engine
  NodeEngine    routing, peers, lanes, command queues,  written once, generic
                TTLs, reconnect, supervision, shutdown  over the transport
  Transport     connect / listen / send & recv Frame    mock, TCP, gRPC, RDMA
```

The load-bearing idea is the seam between the middle two. `NodeEngine<T:
Transport>` is generic over its transport and never boxes it, but a plugin's types
must never mention `T` — so the engine reaches plugins through three narrow
`dyn` traits (`DataSink`, `WorkerHost`, `ServiceRuntime`) that hand out bytes,
names, and counters. Everything a transport would otherwise have to re-implement
lives above that seam, once.

**One engine serves both roles.** The agent dials one server; the server accepts
many agents. A relay/aggregation tier is simply an engine that does both.

### Data flow

```
sampler thread ──encode──▶ Outbox ──▶ data lane ──▶ writer ──chunk──▶ Frame ──▶ transport
                                                                                    │
handler ◀──decode── route by service name ◀── reassemble ◀── reader ◀───────────────┘
```

### Command flow

```
server handler ──▶ ctx.command::<S>(node, cmd) ──▶ per-node queue (with TTL) ──▶ agent
                                                                                  │
CommandHandle ◀── match by id ◀── CommandResult ◀── Reply ◀── service's on_command ┘
```

## Crate layout

Directory names match the package name minus the `cs-` prefix. ✅ is built and
tested; 🚧 is in progress; ⬜ is planned and mostly not on disk yet.

```
crates/
  core/                   no tokio, no transport, no I/O — ever
    util/          cs-util         Error, ErrorKind, ResultExt.        791 loc  ✅
                                   Zero dependencies.
    async-util/    cs-async-util   ShutdownSignal, Clock, BoxFuture,   815 loc  ✅
                                   CommandHandle. Depends on cs-util
                                   only — no runtime, not even to test.
    api/           cs-api          Wire, ServiceDef, Sampler, Handler, 2618 loc ✅
                                   ServiceCtx, JobInfo, EngineStats,
                                   the `runtime` seam, FakeEngine.
                                   The only crate a plugin depends on.
  runtime/                may use tokio
    transport/     cs-transport    Transport/Connection/FrameTx/       2408 loc ✅
                                   FrameRx traits, Frame, Lane,
                                   Capabilities, Endpoint, ErrorTrace,
                                   and proto/frame.proto.
    transport-mock/ cs-transport-mock  In-process transport built to   1623 loc ✅
                                   misbehave: backpressure, kills,
                                   refusals, delays, garbage frames.
    transport-tcp/ cs-transport-tcp   tokio + length-delimited         948 loc ✅
                                   framing, SO_REUSEADDR, NODELAY.
    engine/        cs-engine       NodeEngine, peer table, lanes,      5563 loc ✅
                                   command queues, chunking,
                                   supervision, shutdown, TokioClock.
    operator/      cs-operator     The operator side of the command     ~380 loc ✅
                                   protocol: connect, ask, collect,
                                   and `status`. Shared by cs-ctl and
                                   the engine's own tier tests.
    testkit/       cs-testkit      TestCluster, misbehaving plugins,   1431 loc ✅
                                   the transport contract suite.
  plugins/
    cgroup/        cs-plugin-cgroup   Per-job cgroup v2 sampler,      2035 loc ✅
                                   plus the JobSource that finds them.
    snapshot/      cs-plugin-snapshot Per-job summaries that merge:   ~1500 loc ✅
                                   one per (node, job, step), sent
                                   upward when the step ends.
    gpu/           cs-plugin-gpu      Shared-GPU attribution via NVML.          ⬜
    selfmon/       cs-plugin-selfmon  Engine counters, process cost,  1571 loc ✅
                                   and CPU per plugin thread.
apps/
  agent/           cs-agent        dials a head, registers samplers,    794 loc ✅
                                   plus `burn` to give it something
                                   to report on a non-cluster machine
  server/          cs-server       accepts agents and operators,        774 loc ✅
                                   logs every batch; no storage yet
  ctl/             cs-ctl          operator CLI: status, restart or     ~870 loc ✅
                                   shutdown a hostlist of nodes
```

### Dependency rules

Enforced in review, and the reason the layering holds:

1. `crates/core/*` never depends on tokio or any runtime/transport crate.
2. Plugins depend on `cs-api` only — it re-exports `bytes`, `prost`, and the
   `cs-util` / `cs-async-util` items they need, so a plugin manifest has one line.
3. Transport implementations depend on `cs-transport` (+ tokio), never on
   `cs-engine`.
4. `cs-engine` depends on the `cs-transport` *traits*, never on an implementation.
5. Only `apps/*` and `cs-testkit` depend on a concrete transport; each non-mock
   transport sits behind a Cargo feature.
6. The transport is chosen once, at startup, and the engine stays generic. No
   `Box<dyn Transport>`.

`cs-transport` deliberately does **not** depend on `cs-api`: the transport layer
knows bytes and names, not plugin semantics.

## Writing a plugin

Three impls and a marker type. Nothing here names a runtime or a transport.

```rust
use std::time::Duration;
use cs_api::{CommandReceiver, JobInfo, NoCommand, Sampler, ServiceBound, ServiceDef};
use cs_util::Result;

// 1. the service: a name, a version, and its two message types
struct Memory;
impl ServiceDef for Memory {
    const NAME: &'static str = "memory";
    type Data = MemoryBatch;    // any prost message is a Wire type for free
    type Command = NoCommand;   // uninhabited: this service takes no commands
}

// 2. the agent side
struct MemorySampler;

impl ServiceBound for MemorySampler { type Service = Memory; }
impl CommandReceiver for MemorySampler {}   // built-ins only

impl Sampler for MemorySampler {
    fn interval(&self) -> Duration { Duration::from_secs(5) }

    fn sample(&mut self, jobs: &[JobInfo]) -> Result<Vec<MemoryBatch>> {
        jobs.iter().map(read_memory_current).collect()
    }
}
```

Register it as a **factory**, so the engine can rebuild it after a `Restart`
command or a panic:

```rust
NodeEngine::builder(transport)
    .node("node-0042")
    .sampler(|| MemorySampler)
    .dial("tcp://head01:7777".parse()?)
    .build()?
    .run()
    .await?;
```

A few conventions matter more than they look:

- **Ship cumulative counters, not rates.** The server computes rates, and can then
  survive a missed sample. A rate computed on the agent cannot.
- **Sample every few seconds; batch and send every 30–60s.** Batches are columnar:
  one timestamp array, one value array per metric.
- **`sample()` is synchronous and runs on its own named thread.** Blocking sysfs
  reads are expected. A job whose cgroup vanished mid-sample is normal — skip it
  and return the rest. Reserve `Err` for "this sampler could not work at all".

Plugin crates can unit-test against `cs_api::test_support::FakeEngine` (feature
`test-util`), which implements the whole engine seam as a recorder — no engine, no
transport, no runtime required to assert "these jobs produce these counters". For
anything crossing a connection, `cs-testkit`'s `TestCluster` runs a server and any
number of agents in one process, with the links between them under the test's
control.

## Building

```sh
cargo build --workspace
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all
```

Rust 1.85+ (edition 2024). **No system dependencies**: the frame protocol is
compiled by `protox`, a pure-Rust protobuf compiler, so a Rust toolchain is all a
cluster build host needs — no `protoc`.

Use `--all-features`: `test_support` and its doctests sit behind `test-util`, and
later transports have features of their own.

External dependencies are few and deliberate: `bytes`, `prost`, `tokio`,
`tracing`, and `protox` at build time. 534 tests, no `unsafe`, `unwrap`/`expect`
denied in library code by lint — with one documented exemption for `cs-testkit`,
whose job is to panic loudly.

## Try it

Two terminals, no cluster required:

```sh
cargo run -p cs-server                                      # listens on tcp://0.0.0.0:7777
cargo run -p cs-agent -- --burn-threads 2 --burn-percent 60  # dials 127.0.0.1:7777
```

The head prints one line per batch it receives:

```
node-7 selfmon  6 samples  agent cpu +11.990s rss 40.2MiB threads 6  queue 0d/0c
  frames 2  [burn/sample +10.0ms, burn/spin0 +5.980s, burn/spin1 +6.000s]
```

`burn` is a load generator built into the agent, there so a machine that is not a
compute node has something to report: it spins threads on a duty cycle and sawtooths
a ballast allocation. Its workers are named `burn/spin0`, which is why their CPU
appears **by name** above — thread naming, per-plugin attribution, batching,
chunking and the wire format all in one line. `--no-burn` turns it off; `--cgroups`
adds the real per-job sampler on a node that has job cgroups.

Both binaries stop on SIGTERM, flush what they were holding, and exit 0 — or exit
75, which is what systemd's `RestartForceExitStatus=` is for.

In a third terminal, ask what it can see and tell it to do something:

```
$ cs-ctl status
head01 up 4m12s  (cs-server 0.1.0)
node-7  direct              3m50s  selfmon/v1
1 node
```


```
$ cs-ctl restart --nodes node-[6-7] --service selfmon
node-6  selfmon     failed:
    no node named "node-6" is connected @ crates/runtime/engine/src/engine.rs:166
node-7  selfmon     ok
1 of 2 ok
```

### Three tiers

A relay is an engine in both roles, so it is the same binary with an upstream:

```sh
cs-server -l tcp://0.0.0.0:7777 -n global                         # the top
cs-server -l tcp://0.0.0.0:7777 -n relay-a --upstream tcp://global:7777 --relay
cs-agent  --server tcp://relay-a:7777                             # a node behind it
```

The relay announces what it serves, so the global head can see and command nodes it
has never spoken to:

```
$ cs-ctl -H tcp://global:7788 status
global up 43s  (cs-server 0.1.0)
relay-a  direct             42s  -
node-1   via relay-a +1         -  selfmon/v1
2 nodes

$ cs-ctl -H tcp://global:7788 restart -n node-1 -s selfmon
node-1  selfmon     ok
1 of 1 ok
```

It carries metrics too. `--relay` on the middle tier registers no handlers, and the
engine passes on what it cannot read with the producer's name attached:

```
$ # at the global head, which has never spoken to node-1
node-1  selfmon  4 samples  agent cpu +1.810s rss 26.7MiB  …  [burn/spin0 +1.800s]
relay-a selfmon  3 samples  agent cpu +10.0ms  rss 8.7MiB   …  relayed 3
```

The first line is node-1's own metrics, attributed to node-1 rather than to the relay
in front of it. The second is the relay reporting on itself through the same plugin an
agent uses, including how much it has passed on.

`cs-ctl` talks to a **second** port (`tcp://127.0.0.1:7788` by default, loopback on
purpose: reaching it is the only authorization there is). The head forwards each
command to the node named and hands back that node's own answer — including, as
above, the error chain from wherever it went wrong. Exit status is 0 when every node
said ok, 1 when one did not, 2 when there was no answer to be had.

## Installing it

`packaging/` has systemd units for both, with the contracts spelled out in
[`packaging/README.md`](packaging/README.md) — chiefly that **exit 75 is the restart
mechanism** (`RestartForceExitStatus=75`) while exit 0 means an operator asked for a
shutdown and meant it, so `Restart=always` is wrong here.

## Design decisions worth knowing

[`CLAUDE.md`](CLAUDE.md) is the design document and the source of truth for
behavioural contracts. The decisions most likely to surprise a reader of the code:

- **A frame's lane is derived from its variant**, not carried beside it, so the two
  can never contradict each other. Control is drained before data and never
  dropped; data is bounded and drops the *oldest*, because cumulative counters
  survive a gap and unbounded memory does not.
- **The outbound queue belongs to the engine, not to a connection.** That is what
  makes an outage survivable: samplers keep filling it while disconnected, and a
  reconnect finds the buffer intact.
- **Command expiry is a relative TTL, recomputed each time the command leaves a
  queue** — never an absolute deadline. Nothing in the protocol compares two
  peers' clocks, because a cluster will not reliably give you synchronised ones.
- **A sampler's loop is async but its `sample()` runs on a dedicated named
  thread.** The loop goes through the `Clock` so a paused-time test drives every
  interval; the thread is named `<service>/sample` so per-plugin CPU can be read
  from `/proc/self/task/<tid>/`. `spawn_blocking` would give neither. The `selfmon`
  plugin is what collects it, and there is an end-to-end test proving the naming
  survives all three layers.
- **Errors keep their shape across the network.** A command that fails on a node
  comes back as a rebuilt `cs_util::Error` chain, each remote frame still showing
  its own `file:line`, printed under the local trace.
- **Decode failures are not retryable.** A peer speaking nonsense gets its frame
  logged, counted, and skipped — never a dropped connection, and never a reconnect
  storm. A transport may only say `Decode` while the stream is still framed; one
  that has lost alignment must report a broken connection instead.
- **Intent is carried by `Goodbye`, not by the shape of a disconnect.** A dropped
  socket and a deliberate close both send a FIN, so no transport can tell them
  apart — an end of stream with no `Goodbye` before it means the peer went away,
  and the agent reconnects.
- **Silence is a failure mode of its own.** A node that loses power sends no FIN
  and no RST: writes keep succeeding into nothing and reads never return. So a peer
  that says nothing for `peer_timeout` is dropped, and the mock can reproduce it
  exactly (`Link::blackhole`).

## Roadmap

| # | Step | State |
|---|------|-------|
| 1 | `cs-util`, `cs-async-util` | ✅ done |
| 2 | `cs-api` traits and types | ✅ done |
| 3 | `cs-transport` traits, `Frame`, frame proto | ✅ done |
| 4 | `cs-transport-mock` | ✅ done |
| 5 | `cs-engine` against the mock, driven by `cs-testkit` | ✅ done |
| 6 | `cs-transport-tcp`; contract suite on both transports | ✅ done |
| 7 | Plugins: cgroup ✅, selfmon ✅, then gpu | |
| 8 | Apps: server ✅, agent ✅, ctl ✅ | |
| 9 | The relay tier: reachability, routing, `status`, `Data.origin` ✅ | |
| 10 | Per-job snapshots ✅ | |
| 11 | The aggregator: storage, a Slurm-joined job record, opt-in raw streaming | ← next |
| 9 | Later: gRPC transport, relay tier, eBPF, RDMA | |

Adding a transport is: implement the traits, write a `TestTransport` fixture, and
call `cs_testkit::transport_contract!(fixture)`. TCP was ~300 lines of transport
and one line of test invocation.

## Environment assumptions

- Slurm with **cgroup v2**; job cgroups under
  `/sys/fs/cgroup/system.slice/slurmstepd.scope/job_<id>/step_<n>/`.
- GPUs **shared between jobs**, attributed via NVML per-process data mapped back
  to jobs through `/proc/<pid>/cgroup`.
- The agent runs as a systemd service in its own cgroup, so it can monitor itself.
- Restart means graceful shutdown then a dedicated exit status, which systemd
  matches with `RestartForceExitStatus=`. No self re-exec.
