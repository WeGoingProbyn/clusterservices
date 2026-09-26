use std::fmt;
use std::time::Duration;

use crate::{Cmd, ServiceBound};

/// A command delivered to a service.
///
/// Two built-ins every service understands, plus whatever the service defines
/// for itself. Custom commands should be **idempotent** — "set the interval to
/// 1s", not "halve the interval" — because delivery is at-most-once with an ack
/// and an operator cannot always tell which happened.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Command<C> {
    /// Stop the service. It does not come back without an operator.
    Shutdown,
    /// Stop and rebuild the service from its factory.
    Restart,
    /// A command the service defined.
    Custom(C),
}

impl<C> Command<C> {
    /// Whether this is [`Shutdown`](Command::Shutdown) or
    /// [`Restart`](Command::Restart) — the two the engine acts on itself.
    #[must_use]
    pub const fn is_builtin(&self) -> bool {
        matches!(self, Self::Shutdown | Self::Restart)
    }

    /// The custom payload, if this is a custom command.
    #[must_use]
    pub const fn as_custom(&self) -> Option<&C> {
        match self {
            Self::Custom(cmd) => Some(cmd),
            _ => None,
        }
    }

    /// Rebuild with a different custom payload, keeping the variant.
    pub fn map_custom<D>(self, f: impl FnOnce(C) -> D) -> Command<D> {
        match self {
            Self::Shutdown => Command::Shutdown,
            Self::Restart => Command::Restart,
            Self::Custom(cmd) => Command::Custom(f(cmd)),
        }
    }
}

impl<C> fmt::Display for Command<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shutdown => f.write_str("shutdown"),
            Self::Restart => f.write_str("restart"),
            Self::Custom(_) => f.write_str("custom"),
        }
    }
}

/// A service's answer to a command.
///
/// What each answer means depends on the command:
///
/// | Command  | `Default`                                | `Handled`              | `Rejected`    |
/// |----------|------------------------------------------|------------------------|---------------|
/// | Shutdown | engine calls `on_shutdown`, stops it     | service stopped itself | keeps running |
/// | Restart  | `on_shutdown`, drop, rebuild via factory | service reset in place | keeps running |
/// | Custom   | reply `Unsupported`                      | reply `Ok`             | reply with the reason |
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Reply {
    /// "Do whatever you would have done." The engine's default handling applies.
    Default,
    /// "I dealt with it." The engine does nothing further.
    Handled,
    /// "No, because…" — the reason travels back to the operator.
    Rejected(String),
}

impl Reply {
    /// A rejection with a reason.
    #[must_use]
    pub fn rejected(reason: impl Into<String>) -> Self {
        Self::Rejected(reason.into())
    }

    /// The reason, if this is a rejection.
    #[must_use]
    pub fn rejection(&self) -> Option<&str> {
        match self {
            Self::Rejected(reason) => Some(reason),
            _ => None,
        }
    }
}

impl fmt::Display for Reply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("default"),
            Self::Handled => f.write_str("handled"),
            Self::Rejected(reason) => write!(f, "rejected: {reason}"),
        }
    }
}

/// A service that can be commanded.
///
/// Implemented by every [`Sampler`](crate::Sampler); the default
/// [`on_command`](CommandReceiver::on_command) accepts the built-ins and reports
/// custom commands as unsupported, so a service with no commands of its own
/// writes nothing but the empty impl.
pub trait CommandReceiver: ServiceBound {
    /// React to a command.
    ///
    /// Runs on the service's own thread, between samples — never concurrently
    /// with [`sample`](crate::Sampler::sample), so `&mut self` is free to change
    /// whatever it likes. Keep it quick: the operator is waiting for the reply
    /// and the next sample is not taken until this returns.
    fn on_command(&mut self, _cmd: Command<Cmd<Self>>) -> Reply {
        Reply::Default
    }
}

/// How a dispatched command turned out.
///
/// This is the *answer*, not the delivery: failures to deliver at all (no such
/// node, transport dead, shutdown) come back as an `Err` from the
/// [`CommandHandle`](cs_async_util::CommandHandle) instead.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CommandOutcome {
    /// Accepted and acted on.
    Ok,
    /// The service does not implement this custom command.
    Unsupported,
    /// The service refused, with a reason.
    Rejected(String),
    /// Never delivered: it sat in the node's queue past its expiry while the
    /// node was disconnected.
    Expired,
    /// The target node does not run the named service, per its `Hello`.
    UnknownService,
}

impl CommandOutcome {
    /// Whether the command was accepted.
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }
}

impl fmt::Display for CommandOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => f.write_str("ok"),
            Self::Unsupported => f.write_str("unsupported"),
            Self::Rejected(reason) => write!(f, "rejected: {reason}"),
            Self::Expired => f.write_str("expired"),
            Self::UnknownService => f.write_str("unknown service"),
        }
    }
}

/// Delivery options for a dispatched command.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CommandOpts {
    /// How long the command stays worth delivering. A command queued for a
    /// disconnected node is resolved as [`CommandOutcome::Expired`] once this
    /// elapses.
    pub expiry: Duration,
    /// Skip asking: deliver even if the service would reject it.
    ///
    /// For agent-wide [`Shutdown`](Command::Shutdown) and
    /// [`Restart`](Command::Restart), `force` skips
    /// [`on_command`](CommandReceiver::on_command) entirely but still runs
    /// `on_shutdown` under the shutdown deadline.
    pub force: bool,
}

impl CommandOpts {
    /// How long a command waits for a disconnected node by default.
    pub const DEFAULT_EXPIRY: Duration = Duration::from_secs(60);

    /// Default options with a different expiry.
    #[must_use]
    pub const fn expiring_in(expiry: Duration) -> Self {
        Self {
            expiry,
            force: false,
        }
    }

    /// These options, but forced.
    #[must_use]
    pub const fn forced(mut self) -> Self {
        self.force = true;
        self
    }
}

impl Default for CommandOpts {
    fn default() -> Self {
        Self {
            expiry: Self::DEFAULT_EXPIRY,
            force: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoCommand, ServiceDef};

    struct Echo;

    impl ServiceDef for Echo {
        const NAME: &'static str = "echo";
        type Data = String;
        type Command = String;
    }

    struct Plugin;

    impl ServiceBound for Plugin {
        type Service = Echo;
    }

    /// Takes the default `on_command`.
    impl CommandReceiver for Plugin {}

    struct Picky;

    impl ServiceBound for Picky {
        type Service = Echo;
    }

    impl CommandReceiver for Picky {
        fn on_command(&mut self, cmd: Command<Cmd<Self>>) -> Reply {
            match cmd {
                Command::Restart => Reply::rejected("mid-flush"),
                Command::Custom(ref arg) if arg == "ping" => Reply::Handled,
                _ => Reply::Default,
            }
        }
    }

    #[test]
    fn the_default_receiver_defers_everything_to_the_engine() {
        let mut plugin = Plugin;
        assert_eq!(plugin.on_command(Command::Shutdown), Reply::Default);
        assert_eq!(
            plugin.on_command(Command::Custom("anything".into())),
            Reply::Default
        );
    }

    #[test]
    fn a_receiver_can_reject_with_a_reason() {
        let mut picky = Picky;
        let reply = picky.on_command(Command::Restart);
        assert_eq!(reply.rejection(), Some("mid-flush"));
        assert_eq!(reply.to_string(), "rejected: mid-flush");
        assert_eq!(
            picky.on_command(Command::Custom("ping".into())),
            Reply::Handled
        );
        assert_eq!(picky.on_command(Command::Shutdown), Reply::Default);
    }

    #[test]
    fn builtins_are_distinguishable_from_custom() {
        let shutdown: Command<u32> = Command::Shutdown;
        let custom = Command::Custom(7);
        assert!(shutdown.is_builtin());
        assert!(!custom.is_builtin());
        assert_eq!(shutdown.as_custom(), None);
        assert_eq!(custom.as_custom(), Some(&7));
        assert_eq!(shutdown.to_string(), "shutdown");
        assert_eq!(custom.to_string(), "custom");
    }

    #[test]
    fn map_custom_rewrites_only_the_payload() {
        assert_eq!(
            Command::Custom(2u32).map_custom(|n| n.to_string()),
            Command::Custom("2".to_owned())
        );
        assert_eq!(
            Command::<u32>::Shutdown.map_custom(|n| n.to_string()),
            Command::Shutdown
        );
    }

    #[test]
    fn a_service_without_commands_can_only_be_sent_builtins() {
        // `Command::Custom(NoCommand)` is uninhabited, so this match is total.
        let cmd: Command<NoCommand> = Command::Restart;
        let described = match cmd {
            Command::Shutdown => "shutdown",
            Command::Restart => "restart",
            Command::Custom(never) => match never {},
        };
        assert_eq!(described, "restart");
    }

    #[test]
    fn default_opts_expire_and_do_not_force() {
        let opts = CommandOpts::default();
        assert_eq!(opts.expiry, CommandOpts::DEFAULT_EXPIRY);
        assert!(!opts.force);
        assert!(CommandOpts::default().forced().force);
        assert_eq!(
            CommandOpts::expiring_in(Duration::from_secs(5)).expiry,
            Duration::from_secs(5)
        );
    }

    #[test]
    fn outcomes_describe_themselves() {
        assert!(CommandOutcome::Ok.is_ok());
        assert!(!CommandOutcome::Expired.is_ok());
        assert_eq!(
            CommandOutcome::Rejected("busy".into()).to_string(),
            "rejected: busy"
        );
        assert_eq!(
            CommandOutcome::UnknownService.to_string(),
            "unknown service"
        );
    }
}
