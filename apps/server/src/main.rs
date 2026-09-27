//! The cluster head: accepts agents and reports what they send.
//!
//! Deliberately thin, and a working example of the six things a binary in this
//! project owns — config, logging, choosing a transport, registration, signals,
//! and the exit status. Everything else is the engine's.
//!
//! ```sh
//! cargo run -p cs-server                              # tcp://0.0.0.0:7777
//! cargo run -p cs-server -- --listen tcp://127.0.0.1:0  # any free port
//! RUST_LOG=debug cargo run -p cs-server
//! ```

mod report;

use std::process::ExitCode;
use std::time::Duration;

use cs_engine::{EngineConfig, EngineHandle, NodeEngine, Stop};
use cs_plugin_selfmon::{SelfmonConfig, SelfmonSampler};
use cs_plugin_snapshot::{SnapshotSampler, Snapshots};
use cs_transport::{Endpoint, Transport};
use cs_util::{Error, ErrorKind, Result, ResultExt};
use tracing::{error, info, warn};

use crate::report::{CgroupReport, SelfmonReport, SnapshotReport, Tally};

/// Where to listen when nothing says otherwise.
const DEFAULT_LISTEN: &str = "tcp://0.0.0.0:7777";

/// Where to accept operators when nothing says otherwise.
///
/// **Loopback on purpose.** A peer that reaches this endpoint may restart a service
/// on any node this head serves, and nothing in the protocol authenticates it yet —
/// so the bind address is the only lock on the door. Widen it deliberately, or
/// reach it over ssh.
const DEFAULT_ADMIN: &str = "tcp://127.0.0.1:7788";

/// Exit status that means "start me again".
///
/// `EX_TEMPFAIL` from `sysexits.h`, chosen because it is distinctive and not a
/// signal code. A unit file turns it into a restart:
///
/// ```ini
/// [Service]
/// Restart=on-failure
/// RestartForceExitStatus=75
/// ```
const RESTART_EXIT_CODE: u8 = 75;

/// Exit status after a second signal, matching the shell's convention for SIGINT.
const FORCED_EXIT_CODE: i32 = 130;

fn main() -> ExitCode {
    match start() {
        Ok(code) => code,
        Err(err) => {
            // Before logging is up this is all there is; afterwards the `Debug`
            // form prints the whole error chain with each frame's location.
            eprintln!("cs-server: {err:?}");
            ExitCode::FAILURE
        }
    }
}

fn start() -> Result<ExitCode> {
    let args = match Args::parse(std::env::args().skip(1))? {
        Parsed::Run(args) => args,
        Parsed::Usage => {
            println!("{USAGE}");
            return Ok(ExitCode::SUCCESS);
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("cs-server")
        .build()
        .map_err(|err| Error::with_source(ErrorKind::Config, "cannot start the runtime", err))?;

    let stopped = runtime.block_on(serve(&args))?;
    info!(?stopped, "cs-server stopped");
    Ok(match stopped {
        Stop::Shutdown => ExitCode::SUCCESS,
        Stop::Restart => ExitCode::from(RESTART_EXIT_CODE),
    })
}

/// Pick the transport the endpoint asks for, and run on it.
///
/// A `match`, not a `Box<dyn Transport>` — which is not merely discouraged but
/// impossible, because `Transport::connect` returns `impl Future` and so the trait
/// is not dyn-compatible. Each arm monomorphises [`run`] separately, which is how
/// the choice costs nothing on the frame path.
///
/// Dispatching on the endpoint's own scheme means the address decides: `tcp://…`
/// selects TCP, and a later `grpc://…` or `ucx://…` becomes one more arm with no
/// separate configuration to keep in step.
async fn serve(args: &Args) -> Result<Stop> {
    match args.listen.scheme() {
        #[cfg(feature = "tcp")]
        cs_transport_tcp::SCHEME => run(cs_transport_tcp::TcpTransport::new(), args).await,
        other => Err(Error::new(
            ErrorKind::Config,
            format!(
                "no transport compiled in for {other:?} endpoints; \
                 build with the matching feature"
            ),
        )),
    }
}

/// Build the engine, wire up signals, and run until something stops it.
async fn run<T: Transport>(transport: T, args: &Args) -> Result<Stop> {
    let tally = Tally::new();

    let mut builder = NodeEngine::builder(transport)
        .config(EngineConfig::new(&args.node))
        .build_id(concat!("cs-server ", env!("CARGO_PKG_VERSION")))
        // No samplers and no job source: a head produces nothing of its own, it
        // only receives.
        .listen(args.listen.clone());

    // Per-job summaries, kept while a step runs and sent up when it ends. Only worth
    // keeping when there is a tier above to send them to: a head with nothing upstream
    // would accumulate them and then have nowhere to put them.
    let snapshots = args
        .upstream
        .as_ref()
        .filter(|_| !args.relay)
        .map(|_| Snapshots::new());

    // One handler per service it knows how to read — unless it is a relay, whose job
    // is to pass data on rather than read it. That is the whole difference: the engine
    // forwards what it has no handler for, so a relay is configured by *not*
    // registering them, and there is no "forward" switch anywhere in the engine.
    if !args.relay {
        let cgroup = match &snapshots {
            Some(snapshots) => CgroupReport::new(tally.clone()).summarising(snapshots.clone()),
            None => CgroupReport::new(tally.clone()),
        };
        builder = builder
            .handler(cgroup)
            .handler(SelfmonReport::new(tally.clone()))
            // A head also reads the snapshots *below* it, which is what makes a cluster
            // head under a global tier work: it summarises its own nodes and passes on
            // what its children already summarised.
            .handler(SnapshotReport::new(tally.clone()));
    }

    if let Some(snapshots) = &snapshots {
        info!("keeping per-job snapshots and sending them upstream");
        builder = builder.sampler(SnapshotSampler::factory(snapshots.clone()));
    }

    // A second endpoint, for `cs-ctl`. Commands arriving there name a node and are
    // forwarded to it; commands arriving on the agents' endpoint are not.
    if let Some(admin) = &args.admin {
        builder = builder.admin(admin.clone());
    }

    // With an upstream as well as a listener, this *is* a relay — the engine has
    // always supported both roles, and what was missing was the routing that now
    // exists. It announces the nodes below it, and passes down commands addressed to
    // them. What it cannot yet do is forward their *metrics*: a batch for a service
    // it has no handler for is counted and dropped, which is at least visible.
    if let Some(upstream) = &args.upstream {
        info!(
            upstream = upstream.as_str(),
            relay = args.relay,
            "reporting to a tier above"
        );
        builder = builder
            .dial(upstream.clone())
            // A tier in the middle reports on itself to the tier above, through the
            // same plugin an agent uses and on the same terms — batched, chunked,
            // queued, dropped under pressure. That is how `data_forwarded` becomes
            // visible at all: a relay with no sampler is a relay nobody can see.
            .sampler(SelfmonSampler::factory_with(
                SelfmonConfig::new().batching_for(Duration::from_secs(30)),
            ));
    }

    let engine = builder.build().context("building the engine")?;

    // Before `run`, which consumes the engine.
    let handle = engine.handle();
    tokio::spawn(watch_signals(handle.clone()));

    let stopped = engine.run().await;

    let (batches, samples) = tally.counts();
    info!(batches, samples, "received in total");
    stopped
}

/// First signal asks for a graceful stop; a second one stops waiting.
///
/// The engine's shutdown is bounded by `shutdown_deadline` already, so the second
/// signal is for the case where something outside it is wedged.
async fn watch_signals(handle: EngineHandle) {
    use tokio::signal::unix::{SignalKind, signal};

    let (mut terminate, mut interrupt) = match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(terminate), Ok(interrupt)) => (terminate, interrupt),
        _ => {
            // Not fatal: the process can still be stopped, just not politely.
            error!("cannot install signal handlers; shutdown will not be graceful");
            return;
        }
    };

    tokio::select! {
        _ = terminate.recv() => {}
        _ = interrupt.recv() => {}
    }
    info!("signal received; stopping (send another to force it)");
    handle.shutdown();

    tokio::select! {
        _ = terminate.recv() => {}
        _ = interrupt.recv() => {}
    }
    warn!("second signal; exiting without finishing shutdown");
    std::process::exit(FORCED_EXIT_CODE);
}

const USAGE: &str = "\
cs-server — accepts agents and reports what they send

Usage: cs-server [options]

Options:
  -l, --listen <endpoint>  where to accept agents, as scheme://authority
                           (default: $CS_LISTEN, else tcp://0.0.0.0:7777)
  -u, --upstream <endpoint>
                           a tier above to report to: this head announces the
                           nodes below it and passes their commands down
                           (default: $CS_UPSTREAM, else none)
      --relay              register no handlers, so every batch received is
                           passed upward with its producer's name attached
                           rather than read here. Needs --upstream.
  -a, --admin <endpoint>   where to accept operators running cs-ctl
                           (default: $CS_ADMIN, else tcp://127.0.0.1:7788)
                           Reaching this endpoint is the only authorization
                           there is; keep it on loopback or a management net.
      --no-admin           accept no operators at all
  -n, --node <name>        this head's name, used in logs and Goodbye
                           (default: $CS_NODE, else the hostname)
  -h, --help               print this

Environment:
  RUST_LOG                 tracing filter, e.g. `debug` or `cs_engine=debug`
                           (default: info)

Exit status:
  0    stopped cleanly
  75   asked to restart; name this in systemd's RestartForceExitStatus=
  130  a second signal arrived before shutdown finished
";

/// What the command line asked for.
#[derive(Clone, Debug)]
struct Args {
    listen: Endpoint,
    /// Where operators connect, or `None` for a head nobody can command.
    admin: Option<Endpoint>,
    /// The tier above, if this is not the top.
    upstream: Option<Endpoint>,
    /// Register no handlers, so everything received is passed upward untouched.
    relay: bool,
    node: String,
}

/// Either run, or just print the usage.
enum Parsed {
    Run(Args),
    Usage,
}

impl Args {
    /// Parse arguments, falling back to the environment and then to defaults.
    ///
    /// Hand-rolled rather than pulling in an argument parser: there are two
    /// options, and the agent has not yet decided how it wants to be configured.
    /// When it does, both binaries should share that instead.
    fn parse(args: impl Iterator<Item = String>) -> Result<Parsed> {
        let mut listen = std::env::var("CS_LISTEN").unwrap_or_else(|_| DEFAULT_LISTEN.to_owned());
        let mut admin = std::env::var("CS_ADMIN").unwrap_or_else(|_| DEFAULT_ADMIN.to_owned());
        let mut upstream = std::env::var("CS_UPSTREAM").ok();
        let mut relay = false;
        let mut serving_operators = true;
        let mut node = std::env::var("CS_NODE").ok();

        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => return Ok(Parsed::Usage),
                "-l" | "--listen" => listen = value(&mut args, &arg)?,
                "-a" | "--admin" => admin = value(&mut args, &arg)?,
                "-u" | "--upstream" => upstream = Some(value(&mut args, &arg)?),
                "--relay" => relay = true,
                "--no-admin" => serving_operators = false,
                "-n" | "--node" => node = Some(value(&mut args, &arg)?),
                other => {
                    return Err(Error::new(
                        ErrorKind::Config,
                        format!("unrecognised argument {other:?}; try --help"),
                    ));
                }
            }
        }

        if relay && upstream.is_none() {
            return Err(Error::new(
                ErrorKind::Config,
                "--relay needs --upstream: a relay with nowhere to pass data would \
                 register no handlers and drop everything",
            ));
        }

        Ok(Parsed::Run(Self {
            listen: Endpoint::parse(&listen).with_context(|| format!("--listen {listen:?}"))?,
            admin: serving_operators
                .then(|| Endpoint::parse(&admin).with_context(|| format!("--admin {admin:?}")))
                .transpose()?,
            upstream: upstream
                .as_deref()
                .map(|up| Endpoint::parse(up).with_context(|| format!("--upstream {up:?}")))
                .transpose()?,
            relay,
            node: node.unwrap_or_else(hostname),
        }))
    }
}

/// The value following an option.
fn value(args: &mut impl Iterator<Item = String>, option: &str) -> Result<String> {
    args.next().ok_or_else(|| {
        Error::new(
            ErrorKind::Config,
            format!("{option} needs a value; try --help"),
        )
    })
}

/// This machine's hostname.
///
/// Read from procfs rather than through libc, which keeps the binary's dependency
/// list to what it actually needs. An agent wants the same — but note that its node
/// name has to be what *Slurm* calls the node, which is not always the hostname.
fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|name| name.trim().to_owned())
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "head".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args> {
        match Args::parse(args.iter().map(|arg| (*arg).to_owned()))? {
            Parsed::Run(args) => Ok(args),
            Parsed::Usage => Err(Error::new(ErrorKind::Config, "asked for usage")),
        }
    }

    #[test]
    fn the_default_endpoint_is_parseable() {
        // Guards the one constant that would otherwise fail at startup rather than
        // at build time.
        let endpoint = Endpoint::parse(DEFAULT_LISTEN).expect("the default must parse");
        assert_eq!(endpoint.scheme(), "tcp");
    }

    #[test]
    fn operators_are_accepted_on_loopback_by_default() {
        let args = parse(&[]).expect("defaults");
        let admin = args.admin.expect("an admin endpoint by default");
        assert!(
            admin.authority().starts_with("127.0.0.1"),
            "the default must not be reachable from the network: {admin}"
        );
        assert_ne!(
            admin.authority(),
            args.listen.authority(),
            "operators and agents must not share a door"
        );
    }

    #[test]
    fn the_admin_endpoint_can_be_moved_or_refused() {
        let moved = parse(&["--admin", "tcp://10.0.0.1:9999"]).expect("moved");
        assert_eq!(
            moved.admin.map(|e| e.authority().to_owned()).as_deref(),
            Some("10.0.0.1:9999")
        );
        assert!(
            parse(&["--no-admin"]).expect("refused").admin.is_none(),
            "--no-admin should leave nothing to connect to"
        );
    }

    /// A relay is configured by having no handlers, so `--relay` without anywhere to
    /// pass data would silently drop every batch it received.
    #[test]
    fn relaying_without_an_upstream_is_rejected() {
        let err = parse(&["--relay"]).expect_err("nowhere to relay to");
        assert_eq!(err.kind(), ErrorKind::Config);
        assert!(err.to_string().contains("--upstream"), "{err}");
        assert!(
            parse(&["--relay", "--upstream", "tcp://global:7777"])
                .expect("a relay")
                .relay
        );
    }

    #[test]
    fn an_upstream_makes_it_a_relay_and_is_off_by_default() {
        assert!(
            parse(&[]).expect("defaults").upstream.is_none(),
            "a head reports to nobody unless told to"
        );
        let relay = parse(&["--upstream", "tcp://global:7777"]).expect("relay");
        assert_eq!(
            relay.upstream.map(|e| e.authority().to_owned()).as_deref(),
            Some("global:7777")
        );
    }

    #[test]
    fn listen_and_node_can_be_given() {
        let args =
            parse(&["--listen", "tcp://127.0.0.1:9000", "--node", "head01"]).expect("should parse");
        assert_eq!(args.listen.as_str(), "tcp://127.0.0.1:9000");
        assert_eq!(args.node, "head01");
    }

    #[test]
    fn short_options_work_too() {
        let args = parse(&["-l", "tcp://[::1]:1", "-n", "h"]).expect("should parse");
        assert_eq!(args.listen.authority(), "[::1]:1");
        assert_eq!(args.node, "h");
    }

    #[test]
    fn help_is_not_a_run() {
        assert!(matches!(
            Args::parse(["--help".to_owned()].into_iter()),
            Ok(Parsed::Usage)
        ));
    }

    #[test]
    fn a_bad_endpoint_is_rejected_with_the_argument_named() {
        let err = parse(&["--listen", "127.0.0.1:9000"]).expect_err("not an endpoint");
        assert_eq!(err.kind(), ErrorKind::Config);
        let shown = format!("{err:?}");
        assert!(shown.contains("--listen"), "{shown}");
    }

    #[test]
    fn an_option_without_a_value_is_rejected() {
        let err = parse(&["--listen"]).expect_err("no value");
        assert!(err.to_string().contains("needs a value"));
    }

    #[test]
    fn an_unknown_argument_is_rejected_rather_than_ignored() {
        let err = parse(&["--verbose"]).expect_err("unknown");
        assert!(err.to_string().contains("--verbose"));
    }

    #[test]
    fn a_node_name_is_always_produced() {
        // Whatever the machine says, a head must have a name: the engine refuses to
        // build without one.
        assert!(!hostname().is_empty());
        assert!(
            EngineConfig::new(hostname()).validate().is_ok(),
            "the default name should satisfy the engine"
        );
    }
}
