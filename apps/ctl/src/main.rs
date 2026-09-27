//! The operator's tool: tell nodes to do things, and print what they said.
//!
//! ```sh
//! cs-ctl restart --nodes node-[1-4] --service cgroup
//! cs-ctl shutdown --nodes node-7                      # the whole agent
//! cs-ctl restart --nodes node-[1-100] --service selfmon --timeout 30
//! ```
//!
//! It connects to a head's **admin endpoint** — a second port, separate from the
//! one agents use, because reaching it is the entire authorization for restarting a
//! thousand nodes. The head forwards each command to the node named and sends back
//! that node's own answer, so what prints here is what the service said, including
//! the remote error chain when something failed.
//!
//! The protocol work lives in `cs-operator`, which the engine's own tests drive over
//! the mock transport. This binary is the hostlist, the table, and the exit status.

mod hostlist;

use std::process::ExitCode;
use std::time::Duration;

use cs_operator::{Answer, Request};
use cs_transport::{CommandKind, Endpoint, KnownNode, Outcome, StatusReport, Transport};
use cs_util::{Error, ErrorKind, Result, ResultExt};

/// Which head to ask when nothing says otherwise.
///
/// Loopback, matching `cs-server`'s default: an admin endpoint reachable from the
/// network is a fleet-wide restart button with no lock on it.
const DEFAULT_ADMIN: &str = "tcp://127.0.0.1:7788";

/// How long to wait for answers.
///
/// Longer than the default time to live of a command (60s) would be waiting for
/// something that can no longer happen; shorter risks reporting "no answer" for a
/// node that was about to reply. Sixty-five seconds is one plus a margin.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(65);

/// Every node answered, and answered `ok`.
const EXIT_OK: u8 = 0;
/// Reached the head, but at least one node did not say `ok`.
const EXIT_NOT_OK: u8 = 1;
/// Never got as far as an answer: no head, bad arguments, connection lost.
const EXIT_FAILED: u8 = 2;

fn main() -> ExitCode {
    match start() {
        Ok(code) => code,
        Err(err) => {
            // The `Debug` form, which prints the whole chain with each frame's
            // location — including, for a failure on a node, the remote trace under
            // the local one.
            eprintln!("cs-ctl: {err:?}");
            ExitCode::from(EXIT_FAILED)
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

    // No subscriber by default: this is a command-line tool, and `tracing` output
    // interleaved with the table would be noise. `RUST_LOG` turns it on for anyone
    // debugging the protocol.
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Error::with_source(ErrorKind::Config, "cannot start the runtime", err))?;

    runtime.block_on(ask(&args))
}

/// Pick the transport the endpoint asks for, and run on it.
async fn ask(args: &Args) -> Result<ExitCode> {
    match args.admin.scheme() {
        #[cfg(feature = "tcp")]
        cs_transport_tcp::SCHEME => run(&cs_transport_tcp::TcpTransport::new(), args).await,
        other => Err(Error::new(
            ErrorKind::Config,
            format!(
                "no transport compiled in for {other:?} endpoints; \
                 build with the matching feature"
            ),
        )),
    }
}

/// Connect, ask, print, and decide the exit status.
async fn run<T: Transport>(transport: &T, args: &Args) -> Result<ExitCode> {
    let mut operator = cs_operator::connect(transport, &args.admin, &args.name)
        .await
        .with_context(|| format!("asking {}", args.admin))?;

    let printed = match &args.verb {
        Verb::Status => {
            let report = operator.status(args.timeout).await;
            report.map(|report| show_status(&report, &args.nodes))
        }
        Verb::Command(kind) => {
            let requests: Vec<Request> = args
                .nodes
                .iter()
                .map(|node| {
                    Request::new(node, args.service.as_deref(), kind.clone())
                        .forced(args.force)
                        .with_ttl(args.ttl)
                })
                .collect();
            operator
                .run(requests, args.timeout)
                .await
                .map(|answers| report(&answers))
        }
    };

    operator.close().await;
    printed
}

/// Print what a head can reach, optionally narrowed to a hostlist.
fn show_status(report: &StatusReport, only: &[String]) -> ExitCode {
    println!(
        "{} up {}{}",
        report.node,
        duration(report.uptime),
        if report.build.is_empty() {
            String::new()
        } else {
            format!("  ({})", report.build)
        }
    );

    let shown: Vec<&KnownNode> = report
        .nodes
        .iter()
        .filter(|node| only.is_empty() || only.contains(&node.node))
        .collect();

    if shown.is_empty() {
        println!("no nodes connected");
        // Not an error: a head with no agents is a head that has just started. The
        // exit status says "nothing matched", which a script can act on.
        return ExitCode::from(EXIT_NOT_OK);
    }

    let width = shown.iter().map(|node| node.node.len()).max().unwrap_or(0);
    for node in &shown {
        let services = if node.services.is_empty() {
            "-".to_owned()
        } else {
            node.services
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" ")
        };
        println!(
            "{name:width$}  {via:<12}  {connected:>8}  {services}",
            name = node.node,
            // Where it is from here: attached to this head, or behind a child.
            via = if node.is_direct() {
                "direct".to_owned()
            } else {
                format!("via {} +{}", node.via, node.hops - 1)
            },
            connected = node.connected.map_or_else(|| "-".to_owned(), duration),
        );
    }
    println!(
        "{} node{}",
        shown.len(),
        if shown.len() == 1 { "" } else { "s" }
    );
    ExitCode::from(EXIT_OK)
}

/// A duration an operator reads at a glance.
fn duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m{}s", secs / 60, secs % 60),
        3600..86_400 => format!("{}h{}m", secs / 3600, (secs % 3600) / 60),
        _ => format!("{}d{}h", secs / 86_400, (secs % 86_400) / 3600),
    }
}

/// Print one line per node, and return the exit status that summarises them.
fn report(answers: &[Answer]) -> ExitCode {
    let width = answers
        .iter()
        .map(|answer| answer.node.len())
        .max()
        .unwrap_or(0);

    let mut ok = 0usize;
    for answer in answers {
        println!(
            "{node:width$}  {target:<10}  {outcome}",
            node = answer.node,
            target = answer.target(),
            outcome = describe(answer.outcome.as_ref()),
        );
        // The remote chain, indented under the line it belongs to. This is what
        // `ErrorTrace` was for: a failure on node-7 reads here as it read there.
        if let Some(Outcome::Failed(trace)) = &answer.outcome {
            for line in format!("{:#}", trace.to_error()).lines() {
                println!("    {line}");
            }
        }
        if answer.is_ok() {
            ok += 1;
        }
    }

    let total = answers.len();
    if ok == total {
        println!("{total} of {total} ok");
        ExitCode::from(EXIT_OK)
    } else {
        println!("{ok} of {total} ok");
        ExitCode::from(EXIT_NOT_OK)
    }
}

/// One outcome in a few words.
fn describe(outcome: Option<&Outcome>) -> String {
    match outcome {
        Some(Outcome::Ok) => "ok".to_owned(),
        Some(Outcome::Unsupported) => "unsupported: the service has no such command".to_owned(),
        Some(Outcome::Rejected(why)) => format!("refused: {why}"),
        Some(Outcome::Expired) => "expired: never delivered in time".to_owned(),
        Some(Outcome::UnknownService) => "no such service on this node".to_owned(),
        Some(Outcome::Failed(_)) => "failed:".to_owned(),
        // Not a failure — not knowing, which reads differently on purpose.
        None => "no answer before the timeout".to_owned(),
    }
}

const USAGE: &str = "\
cs-ctl — tell nodes to do things

Usage: cs-ctl <command> [--nodes <hostlist>] [options]

Commands:
  status                   what the head can reach: every node, whether it is
                           attached here or behind a relay, how long it has been
                           connected and what it runs
  restart                  stop the target and build it again
  shutdown                 stop the target and leave it stopped

Options:
  -n, --nodes <hostlist>   which nodes, Slurm-style: node-[1-4,7]
                           Required for restart and shutdown; narrows status.
                           May be given more than once. Zero padding is kept:
                           node-[08-11] is node-08, not node-8.
  -s, --service <name>     which service on those nodes
                           Omitted: the whole agent, which only accepts the
                           built-in commands above.
  -H, --head <endpoint>    the head's admin endpoint
                           (default: $CS_ADMIN, else tcp://127.0.0.1:7788)
      --force              do not ask the service whether it consents
                           It still gets to flush on the way out.
      --ttl <secs>         stop trying to deliver after this long (default: 60)
      --timeout <secs>     stop waiting for answers after this long (default: 65)
      --name <name>        how to identify ourselves to the head, which logs it
                           (default: ctl@<hostname>/<pid>)
  -h, --help               print this

Environment:
  RUST_LOG                 turn on protocol logging, to stderr

Exit status:
  0    every node answered ok; or, for status, at least one node matched
  1    reached the head, but a node did not answer ok — or none matched
  2    never got an answer: no head, bad arguments, connection lost

Notes:
  A whole-agent `shutdown` stops the process. Whether it comes back is up to its
  unit file — with Restart=always it will, promptly.
";

/// What to do.
#[derive(Clone, Debug)]
enum Verb {
    /// Send a command to every named node.
    Command(CommandKind),
    /// Ask the head what it can reach.
    Status,
}

/// What the command line asked for.
#[derive(Clone, Debug)]
struct Args {
    verb: Verb,
    nodes: Vec<String>,
    service: Option<String>,
    admin: Endpoint,
    force: bool,
    ttl: Option<Duration>,
    timeout: Duration,
    name: String,
}

/// Either run, or just print the usage.
enum Parsed {
    Run(Args),
    Usage,
}

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Result<Parsed> {
        let mut verb = None;
        let mut lists: Vec<String> = Vec::new();
        let mut service = None;
        let mut admin = std::env::var("CS_ADMIN").unwrap_or_else(|_| DEFAULT_ADMIN.to_owned());
        let mut force = false;
        let mut ttl = None;
        let mut timeout = DEFAULT_TIMEOUT;
        let mut name = None;

        let mut args = args;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => return Ok(Parsed::Usage),
                "restart" => verb = Some(Verb::Command(CommandKind::Restart)),
                "shutdown" => verb = Some(Verb::Command(CommandKind::Shutdown)),
                "status" => verb = Some(Verb::Status),
                "-n" | "--nodes" => lists.push(value(&mut args, &arg)?),
                "-s" | "--service" => service = Some(value(&mut args, &arg)?),
                "-H" | "--head" => admin = value(&mut args, &arg)?,
                "--force" => force = true,
                "--ttl" => ttl = Some(seconds(&value(&mut args, &arg)?, &arg)?),
                "--timeout" => timeout = seconds(&value(&mut args, &arg)?, &arg)?,
                "--name" => name = Some(value(&mut args, &arg)?),
                other => {
                    return Err(Error::new(
                        ErrorKind::Config,
                        format!("unrecognised argument {other:?}; try --help"),
                    ));
                }
            }
        }

        let Some(verb) = verb else {
            return Err(Error::new(
                ErrorKind::Config,
                "say what to do: status, restart or shutdown; try --help",
            ));
        };
        // `status` asks about everything; a command has to say who it is for. There
        // is no default node list and there should not be: a tool that restarts the
        // whole cluster when an argument is forgotten is a tool that will.
        if lists.is_empty() && !matches!(verb, Verb::Status) {
            return Err(Error::new(
                ErrorKind::Config,
                "say which nodes with --nodes; there is no default and there should not be",
            ));
        }

        let mut nodes = Vec::new();
        for list in &lists {
            nodes.extend(hostlist::expand(list)?);
        }
        // Two `--nodes` may overlap; a command must still go once.
        let mut seen = std::collections::HashSet::new();
        nodes.retain(|node| seen.insert(node.clone()));

        Ok(Parsed::Run(Self {
            verb,
            nodes,
            service,
            admin: Endpoint::parse(&admin).with_context(|| format!("--head {admin:?}"))?,
            force,
            ttl,
            timeout,
            name: name.unwrap_or_else(identity),
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

/// A duration in seconds.
fn seconds(raw: &str, option: &str) -> Result<Duration> {
    let secs: u64 = raw.trim().parse().map_err(|_| {
        Error::new(
            ErrorKind::Config,
            format!("{option} needs a whole number of seconds, not {raw:?}"),
        )
    })?;
    if secs == 0 {
        return Err(Error::new(
            ErrorKind::Config,
            format!("{option} must be at least one second"),
        ));
    }
    Ok(Duration::from_secs(secs))
}

/// Who the head should blame for this.
fn identity() -> String {
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|name| name.trim().to_owned())
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "somewhere".to_owned());
    format!("ctl@{host}/{}", std::process::id())
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
    fn the_default_endpoint_is_parseable_and_local() {
        let endpoint = Endpoint::parse(DEFAULT_ADMIN).expect("the default must parse");
        assert_eq!(endpoint.scheme(), "tcp");
        assert!(
            endpoint.authority().starts_with("127.0.0.1"),
            "an admin endpoint should default to loopback, not the network"
        );
    }

    #[test]
    fn a_verb_and_a_hostlist_are_enough() {
        let args = parse(&["restart", "--nodes", "node-[1-3]"]).expect("should parse");
        assert!(matches!(args.verb, Verb::Command(CommandKind::Restart)));
        assert_eq!(args.nodes, vec!["node-1", "node-2", "node-3"]);
        assert!(args.service.is_none(), "no --service is the whole agent");
        assert!(!args.force);
        assert!(args.name.starts_with("ctl@"), "{}", args.name);
    }

    #[test]
    fn several_node_options_accumulate_without_repeating_a_node() {
        let args = parse(&[
            "shutdown",
            "-n",
            "node-[1-2]",
            "-n",
            "node-2,node-5",
            "-s",
            "cgroup",
        ])
        .expect("should parse");
        assert!(matches!(args.verb, Verb::Command(CommandKind::Shutdown)));
        assert_eq!(args.nodes, vec!["node-1", "node-2", "node-5"]);
        assert_eq!(args.service.as_deref(), Some("cgroup"));
    }

    #[test]
    fn everything_else_can_be_given_too() {
        let args = parse(&[
            "restart",
            "-n",
            "n1",
            "--force",
            "--ttl",
            "10",
            "--timeout",
            "15",
            "--head",
            "tcp://head01:9999",
            "--name",
            "me",
        ])
        .expect("should parse");
        assert!(args.force);
        assert_eq!(args.ttl, Some(Duration::from_secs(10)));
        assert_eq!(args.timeout, Duration::from_secs(15));
        assert_eq!(args.admin.authority(), "head01:9999");
        assert_eq!(args.name, "me");
    }

    #[test]
    fn status_needs_no_hostlist_but_accepts_one_as_a_filter() {
        let all = parse(&["status"]).expect("status alone is valid");
        assert!(matches!(all.verb, Verb::Status));
        assert!(all.nodes.is_empty(), "no filter means every node");

        let some = parse(&["status", "-n", "node-[1-2]"]).expect("status with a filter");
        assert_eq!(some.nodes, vec!["node-1", "node-2"]);
    }

    #[test]
    fn a_status_report_prints_direct_and_indirect_nodes() {
        let report = StatusReport::new(cs_transport::CommandId(1), "head01")
            .up_for(Duration::from_secs(3700))
            .reaching(vec![
                KnownNode::direct("relay-a", "tcp://10.0.0.9:5", Duration::from_secs(90)),
                KnownNode::behind("node-1", "relay-a", 2),
            ]);
        assert_eq!(show_status(&report, &[]), ExitCode::from(EXIT_OK));
        // Narrowed to something that is not there: nothing matched, which is not the
        // same as ok.
        assert_eq!(
            show_status(&report, &["node-9".to_owned()]),
            ExitCode::from(EXIT_NOT_OK)
        );
    }

    #[test]
    fn a_head_with_no_agents_is_not_an_error_but_is_not_ok_either() {
        let empty = StatusReport::new(cs_transport::CommandId(1), "head01");
        assert_eq!(show_status(&empty, &[]), ExitCode::from(EXIT_NOT_OK));
    }

    #[test]
    fn durations_read_at_a_glance() {
        assert_eq!(duration(Duration::from_secs(9)), "9s");
        assert_eq!(duration(Duration::from_secs(90)), "1m30s");
        assert_eq!(duration(Duration::from_secs(3700)), "1h1m");
        assert_eq!(duration(Duration::from_secs(90_000)), "1d1h");
    }

    #[test]
    fn a_missing_verb_is_rejected() {
        let err = parse(&["--nodes", "node-1"]).expect_err("no verb");
        assert!(
            err.to_string().contains("status, restart or shutdown"),
            "{err}"
        );
    }

    /// No default node list, ever: a tool that restarts the whole cluster when an
    /// argument is forgotten is a tool that will.
    #[test]
    fn a_missing_hostlist_is_rejected_rather_than_meaning_everything() {
        let err = parse(&["restart"]).expect_err("no nodes");
        assert_eq!(err.kind(), ErrorKind::Config);
        assert!(err.to_string().contains("--nodes"), "{err}");
    }

    #[test]
    fn a_bad_hostlist_is_rejected_with_the_reason() {
        let err = parse(&["restart", "-n", "node-[4-1]"]).expect_err("backwards");
        assert!(err.to_string().contains("backwards"), "{err}");
    }

    #[test]
    fn a_bad_endpoint_names_the_option() {
        let err = parse(&["restart", "-n", "n1", "--head", "head01:1"]).expect_err("no scheme");
        assert!(format!("{err:?}").contains("--head"), "{err:?}");
    }

    #[test]
    fn an_unknown_argument_is_rejected() {
        let err = parse(&["restart", "-n", "n1", "--now"]).expect_err("unknown");
        assert!(err.to_string().contains("--now"));
    }

    #[test]
    fn help_is_not_a_run() {
        assert!(matches!(
            Args::parse(["--help".to_owned()].into_iter()),
            Ok(Parsed::Usage)
        ));
    }

    #[test]
    fn every_outcome_reads_as_something_an_operator_can_act_on() {
        let outcomes = [
            Some(Outcome::Ok),
            Some(Outcome::Unsupported),
            Some(Outcome::Rejected("mid-batch".into())),
            Some(Outcome::Expired),
            Some(Outcome::UnknownService),
            None,
        ];
        for outcome in outcomes {
            let shown = describe(outcome.as_ref());
            assert!(!shown.is_empty());
            assert!(
                shown.chars().next().is_some_and(char::is_lowercase),
                "{shown:?} should read as part of a line"
            );
        }
    }

    /// `report`'s exit status is the contract a script depends on.
    #[test]
    fn the_exit_status_says_whether_every_node_was_ok() {
        let ok = |node: &str| Answer {
            node: node.to_owned(),
            service: Some("cgroup".to_owned()),
            outcome: Some(Outcome::Ok),
        };
        assert_eq!(
            report(&[ok("node-1"), ok("node-2")]),
            ExitCode::from(EXIT_OK)
        );

        let mixed = [
            ok("node-1"),
            Answer {
                node: "node-2".to_owned(),
                service: Some("cgroup".to_owned()),
                outcome: None,
            },
        ];
        assert_eq!(report(&mixed), ExitCode::from(EXIT_NOT_OK));
    }
}
