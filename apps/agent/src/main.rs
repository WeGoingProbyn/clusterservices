//! The compute-node agent: samples this node and streams it to a head.
//!
//! Registers two plugins by default — `selfmon`, which reports the agent and the
//! engine it is running in, and `burn`, which exists so there is something to
//! report on a machine that is not a compute node. The cgroup sampler, the reason
//! the project exists, is opt-in with `--cgroups`, because there are no job cgroups
//! to read anywhere but a Slurm node.
//!
//! ```sh
//! cargo run -p cs-server                                  # in one terminal
//! cargo run -p cs-agent                                    # in another
//! cargo run -p cs-agent -- --burn-threads 4 --burn-percent 80
//! cargo run -p cs-agent -- --cgroups            # on a real node
//! RUST_LOG=cs_engine=debug cargo run -p cs-agent
//! ```
//!
//! See `apps/server/src/main.rs`: the two binaries have the same shape, because the
//! six things a binary owns are the same in both roles. The differences are that
//! this one `dial`s where the head `listen`s, that it registers samplers rather than
//! handlers, and that its runtime is `current_thread`.

mod burn;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use cs_engine::{EngineConfig, EngineHandle, NodeEngine, Stop};
use cs_plugin_cgroup::{CgroupConfig, CgroupJobs, CgroupSampler, DEFAULT_ROOT};
use cs_plugin_selfmon::{SelfmonConfig, SelfmonSampler};
use cs_transport::{Endpoint, Transport};
use cs_util::{Error, ErrorKind, Result, ResultExt};
use tracing::{error, info, warn};

use crate::burn::{Burn, BurnConfig};

/// Which head to dial when nothing says otherwise.
const DEFAULT_SERVER: &str = "tcp://127.0.0.1:7777";

/// How often the agent looks at itself.
///
/// Five seconds rather than the plugin's own fifteen: this is a development
/// default, and waiting a quarter of a minute to see whether anything works is
/// longer than anyone waits before assuming it does not.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);

/// How long samples accumulate before a batch goes out.
///
/// The convention is 30–60s; twenty is for watching it happen.
const DEFAULT_WINDOW: Duration = Duration::from_secs(20);

/// Exit status that means "start me again", which is how `Restart` is carried out:
/// the agent shuts down gracefully and systemd brings it back.
///
/// ```ini
/// [Service]
/// Restart=always
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
            eprintln!("cs-agent: {err:?}");
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

    // One runtime thread is enough and is the better choice: every blocking read
    // already happens on its own named thread, so the async side only shuffles
    // frames — and an agent's own CPU is a number someone will look at.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Error::with_source(ErrorKind::Config, "cannot start the runtime", err))?;

    let stopped = runtime.block_on(sample(&args))?;
    info!(?stopped, "cs-agent stopped");
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
async fn sample(args: &Args) -> Result<Stop> {
    match args.server.scheme() {
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

/// Register the plugins, wire up signals, and run until something stops it.
async fn run<T: Transport>(transport: T, args: &Args) -> Result<Stop> {
    let mut builder = NodeEngine::builder(transport)
        .config(EngineConfig::new(&args.node))
        .build_id(concat!("cs-agent ", env!("CARGO_PKG_VERSION")))
        // The agent watching itself. First, so that whatever a later plugin does to
        // this process, the plugin that reports on it is already running.
        .sampler(SelfmonSampler::factory_with(
            SelfmonConfig::new()
                .every(args.interval)
                .batching_for(args.window),
        ))
        // One endpoint. An ordered list with failover is a design note in CLAUDE.md
        // and needs a policy, not just a `Vec`.
        .dial(args.server.clone());

    // The reason the project exists, and the one plugin that cannot be demonstrated
    // here: without slurmstepd there is nothing under the root to find, and a
    // sampler reporting an empty fleet forever is worse than one that was never
    // registered. `CgroupJobs` is also the engine's `JobSource` — the list of jobs
    // every sampler is handed — so the two are registered together or not at all.
    if let Some(root) = &args.cgroups {
        info!(root = %root.display(), "reading job cgroups");
        builder = builder
            .jobs(CgroupJobs::under(root))
            .sampler(CgroupSampler::factory_with(
                CgroupConfig::new().batching_for(args.window),
            ));
    }

    if let Some(config) = &args.burn {
        builder = builder.sampler(Burn::factory(config.clone()));
    }

    let engine = builder.build().context("building the engine")?;

    // Before `run`, which consumes the engine.
    let handle = engine.handle();
    tokio::spawn(watch_signals(handle));

    info!(
        node = args.node,
        server = args.server.as_str(),
        "cs-agent starting"
    );
    engine.run().await
}

/// First signal asks for a graceful stop; a second one stops waiting.
///
/// The engine's shutdown is bounded by `shutdown_deadline` already — including the
/// join of plugin worker threads, which is why a spinning `burn` worker cannot hold
/// the process open — so the second signal is for something outside it being wedged.
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
cs-agent — samples this node and streams it to a head

Usage: cs-agent [options]

Options:
  -s, --server <endpoint>  the head to dial, as scheme://authority
                           (default: $CS_SERVER, else tcp://127.0.0.1:7777)
  -n, --node <name>        this node's name, as the head will know it
                           (default: $CS_NODE, else the hostname)
  -i, --interval <secs>    how often to sample (default: 5)
  -w, --window <secs>      how long to batch before sending (default: 20)
      --cgroups [<root>]   also sample job cgroups under <root>
                           (default: /sys/fs/cgroup/system.slice/slurmstepd.scope)
                           Off unless given: only a Slurm node has any.
      --burn-threads <n>   threads generating CPU load (default: 1)
      --burn-percent <n>   how much of each thread's time to spin (default: 25)
      --burn-memory <mib>  peak ballast held and released (default: 64)
      --no-burn            generate no load at all
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
    server: Endpoint,
    node: String,
    interval: Duration,
    window: Duration,
    /// Where job cgroups are, if this node has any.
    cgroups: Option<PathBuf>,
    /// How much load to generate, or `None` for an honest idle agent.
    burn: Option<BurnConfig>,
}

/// Either run, or just print the usage.
enum Parsed {
    Run(Args),
    Usage,
}

impl Args {
    /// Parse arguments, falling back to the environment and then to defaults.
    ///
    /// Hand-rolled rather than pulling in an argument parser. That is a decision
    /// with a shelf life: the moment either binary needs a config *file* — which is
    /// how a thousand nodes are actually configured — `serde` and `toml` are already
    /// in the workspace manifest, and this should become the overrides for it.
    fn parse(args: impl Iterator<Item = String>) -> Result<Parsed> {
        let mut server = std::env::var("CS_SERVER").unwrap_or_else(|_| DEFAULT_SERVER.to_owned());
        let mut node = std::env::var("CS_NODE").ok();
        let mut interval = DEFAULT_INTERVAL;
        let mut window = DEFAULT_WINDOW;
        let mut cgroups = None;
        let mut burn = BurnConfig::default();
        let mut burning = true;

        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => return Ok(Parsed::Usage),
                "-s" | "--server" => server = value(&mut args, &arg)?,
                "-n" | "--node" => node = Some(value(&mut args, &arg)?),
                "-i" | "--interval" => interval = seconds(&value(&mut args, &arg)?, &arg)?,
                "-w" | "--window" => window = seconds(&value(&mut args, &arg)?, &arg)?,
                "--cgroups" => {
                    // The only option whose value is optional, because the default
                    // root is long enough that nobody would type it.
                    let root = match args.peek() {
                        Some(next) if !next.starts_with('-') => args.next().unwrap_or_default(),
                        _ => DEFAULT_ROOT.to_owned(),
                    };
                    cgroups = Some(PathBuf::from(root));
                }
                "--burn-threads" => burn.threads = number(&value(&mut args, &arg)?, &arg)?,
                "--burn-percent" => {
                    let percent: u32 = number(&value(&mut args, &arg)?, &arg)?;
                    if percent > 100 {
                        return Err(Error::new(
                            ErrorKind::Config,
                            format!("{arg} is a percentage, so at most 100, not {percent}"),
                        ));
                    }
                    burn.duty = f64::from(percent) / 100.0;
                }
                "--burn-memory" => {
                    let mib: usize = number(&value(&mut args, &arg)?, &arg)?;
                    burn.ballast = mib * 1024 * 1024;
                }
                "--no-burn" => burning = false,
                other => {
                    return Err(Error::new(
                        ErrorKind::Config,
                        format!("unrecognised argument {other:?}; try --help"),
                    ));
                }
            }
        }

        burn.interval = interval;

        Ok(Parsed::Run(Self {
            server: Endpoint::parse(&server).with_context(|| format!("--server {server:?}"))?,
            node: node.unwrap_or_else(hostname),
            interval,
            window,
            cgroups,
            burn: burning.then_some(burn),
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

/// A whole number, named after the option it came from so a typo says which.
fn number<T: std::str::FromStr>(raw: &str, option: &str) -> Result<T> {
    raw.trim().parse().map_err(|_| {
        Error::new(
            ErrorKind::Config,
            format!("{option} needs a whole number, not {raw:?}"),
        )
    })
}

/// A duration in seconds. Zero is rejected here rather than by the engine, which
/// would only see a sampler asking to run in a tight loop.
fn seconds(raw: &str, option: &str) -> Result<Duration> {
    let secs: u64 = number(raw, option)?;
    if secs == 0 {
        return Err(Error::new(
            ErrorKind::Config,
            format!("{option} must be at least one second"),
        ));
    }
    Ok(Duration::from_secs(secs))
}

/// This machine's hostname.
///
/// Read from procfs rather than through libc, which keeps this binary's dependency
/// list to what it needs. Note that a node's name has to be what *Slurm* calls it,
/// which is not always its hostname — so on a real cluster this is a fallback and
/// `$CS_NODE` is the answer.
fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|name| name.trim().to_owned())
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "node".to_owned())
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
        let endpoint = Endpoint::parse(DEFAULT_SERVER).expect("the default must parse");
        assert_eq!(endpoint.scheme(), "tcp");
    }

    #[test]
    fn the_defaults_burn_a_little_and_read_no_cgroups() {
        let args = parse(&[]).expect("no arguments is valid");
        assert!(args.cgroups.is_none(), "not a Slurm node unless told");
        let burn = args.burn.expect("burning by default");
        assert_eq!(burn.threads, 1);
        assert_eq!(
            burn.interval, args.interval,
            "churn on the sampling interval"
        );
    }

    #[test]
    fn the_engine_accepts_what_the_defaults_produce() {
        // `validate` is the same check `build` runs, so this fails here rather than
        // on a node.
        let args = parse(&[]).expect("defaults");
        EngineConfig::new(args.node)
            .validate()
            .expect("the defaults should satisfy the engine");
    }

    #[test]
    fn everything_can_be_given() {
        let args = parse(&[
            "--server",
            "tcp://head01:7777",
            "--node",
            "node-7",
            "--interval",
            "2",
            "--window",
            "45",
            "--burn-threads",
            "4",
            "--burn-percent",
            "80",
            "--burn-memory",
            "8",
        ])
        .expect("should parse");

        assert_eq!(args.server.authority(), "head01:7777");
        assert_eq!(args.node, "node-7");
        assert_eq!(args.interval, Duration::from_secs(2));
        assert_eq!(args.window, Duration::from_secs(45));
        let burn = args.burn.expect("burning");
        assert_eq!(burn.threads, 4);
        assert!((burn.duty - 0.8).abs() < f64::EPSILON);
        assert_eq!(burn.ballast, 8 * 1024 * 1024);
    }

    #[test]
    fn burning_can_be_turned_off() {
        assert!(
            parse(&["--no-burn"]).expect("should parse").burn.is_none(),
            "--no-burn should leave nothing to run"
        );
    }

    #[test]
    fn cgroups_defaults_to_the_slurm_root_but_takes_a_path() {
        let default = parse(&["--cgroups"]).expect("bare");
        assert_eq!(default.cgroups.as_deref(), Some(DEFAULT_ROOT.as_ref()));

        let given = parse(&["--cgroups", "/tmp/fake", "--node", "n"]).expect("with a root");
        assert_eq!(given.cgroups.as_deref(), Some("/tmp/fake".as_ref()));
        assert_eq!(given.node, "n", "the option after it is still parsed");
    }

    #[test]
    fn a_percentage_over_a_hundred_is_rejected() {
        let err = parse(&["--burn-percent", "101"]).expect_err("not a percentage");
        assert_eq!(err.kind(), ErrorKind::Config);
        assert!(err.to_string().contains("at most 100"), "{err}");
    }

    #[test]
    fn a_zero_interval_is_rejected_rather_than_spun_on() {
        let err = parse(&["--interval", "0"]).expect_err("zero");
        assert!(err.to_string().contains("at least one second"), "{err}");
    }

    #[test]
    fn a_non_numeric_value_names_its_option() {
        let err = parse(&["--burn-threads", "lots"]).expect_err("not a number");
        let shown = err.to_string();
        assert!(shown.contains("--burn-threads"), "{shown}");
        assert!(shown.contains("lots"), "{shown}");
    }

    #[test]
    fn a_bad_endpoint_is_rejected_with_the_argument_named() {
        let err = parse(&["--server", "127.0.0.1:7777"]).expect_err("not an endpoint");
        assert_eq!(err.kind(), ErrorKind::Config);
        assert!(format!("{err:?}").contains("--server"), "{err:?}");
    }

    #[test]
    fn an_unknown_argument_is_rejected_rather_than_ignored() {
        let err = parse(&["--burn-everything"]).expect_err("unknown");
        assert!(err.to_string().contains("--burn-everything"));
    }

    #[test]
    fn help_is_not_a_run() {
        assert!(matches!(
            Args::parse(["--help".to_owned()].into_iter()),
            Ok(Parsed::Usage)
        ));
    }
}
