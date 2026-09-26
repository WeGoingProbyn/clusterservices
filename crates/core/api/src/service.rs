use std::fmt;

use crate::Wire;

/// What a service is: a name, a version, and the two message types it uses.
///
/// A `ServiceDef` is a marker type — it is never instantiated. It is the shared
/// vocabulary between the sampler on the agent and the handler on the server,
/// and the name is what the engine routes on.
///
/// ```
/// use cs_api::{NoCommand, ServiceDef};
///
/// struct CpuService;
///
/// impl ServiceDef for CpuService {
///     const NAME: &'static str = "cpu";
///     // `String` stands in for a generated prost message here.
///     type Data = String;
///     type Command = NoCommand;
/// }
/// ```
pub trait ServiceDef: 'static {
    /// Routing key on the wire. Must satisfy [`is_valid_service_name`].
    ///
    /// Keep it short: worker threads are named `<service>/<worker>` and Linux
    /// truncates thread names at 15 characters.
    const NAME: &'static str;

    /// Bumped when [`Data`](ServiceDef::Data) or
    /// [`Command`](ServiceDef::Command) changes incompatibly. Sent in `Hello` so
    /// a mismatched pair can be reported rather than silently mis-parsed.
    const VERSION: u32 = 1;

    /// Metrics flowing agent → server.
    type Data: Wire;

    /// Commands flowing server → agent. [`NoCommand`](crate::NoCommand) if the
    /// service takes none.
    type Command: Wire;

    /// Compile-time check that [`NAME`](ServiceDef::NAME) is valid.
    ///
    /// Never override this. Anything that builds a [`ServiceId`] reads it, which
    /// forces evaluation, so an invalid name fails the build rather than
    /// failing at startup on a thousand nodes.
    const CHECK_NAME: () = assert!(
        is_valid_service_name(Self::NAME),
        "ServiceDef::NAME must be 1..=12 bytes of [a-z0-9_-] (see is_valid_service_name)"
    );
}

/// Longest permitted service name.
///
/// Bounded by the thread-naming budget rather than the wire format: Linux caps
/// thread names at 15 bytes and worker threads are named `<service>/<worker>`,
/// which is how per-plugin CPU time is attributed from
/// `/proc/self/task/<tid>/stat`.
pub const MAX_SERVICE_NAME_LEN: usize = 12;

/// Whether `name` is a usable service name: 1 to [`MAX_SERVICE_NAME_LEN`] bytes
/// of `[a-z0-9_-]`.
///
/// The empty name is reserved: it means "the whole agent" when used as a command
/// target.
///
/// ```
/// use cs_api::is_valid_service_name;
///
/// assert!(is_valid_service_name("cgroup"));
/// assert!(!is_valid_service_name(""));          // reserved for agent-wide
/// assert!(!is_valid_service_name("GPU"));       // lowercase only
/// assert!(!is_valid_service_name("net flows")); // no spaces
/// ```
#[must_use]
pub const fn is_valid_service_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_SERVICE_NAME_LEN {
        return false;
    }
    let mut i = 0;
    while i < bytes.len() {
        if !matches!(bytes[i], b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-') {
            return false;
        }
        i += 1;
    }
    true
}

/// A service's name and version, with the type erased.
///
/// This is how the engine talks about a service once it no longer knows the
/// [`ServiceDef`] — in routing tables, queues, logs, and stats.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ServiceId {
    /// The routing key. Empty means "the whole agent" (see [`ServiceId::AGENT`]).
    pub name: &'static str,
    /// [`ServiceDef::VERSION`].
    pub version: u32,
}

impl ServiceId {
    /// The agent itself, as a command target: an empty service name.
    ///
    /// Commands sent here are polled to every service (see the `Reply` table in
    /// `CLAUDE.md`).
    pub const AGENT: Self = Self {
        name: "",
        version: 0,
    };

    /// The id of a service, checking its name at compile time.
    #[must_use]
    pub const fn of<S: ServiceDef>() -> Self {
        // Forces `CHECK_NAME` to be evaluated, turning a bad name into a build
        // failure at the first use of the service.
        let () = S::CHECK_NAME;
        Self {
            name: S::NAME,
            version: S::VERSION,
        }
    }

    /// Whether this targets the whole agent rather than one service.
    #[must_use]
    pub const fn is_agent_wide(&self) -> bool {
        self.name.is_empty()
    }
}

impl fmt::Display for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_agent_wide() {
            f.write_str("<agent>")
        } else {
            write!(f, "{}/v{}", self.name, self.version)
        }
    }
}

impl fmt::Debug for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

/// Implemented by every plugin type that belongs to exactly one service.
///
/// Supertrait of both [`CommandReceiver`](crate::CommandReceiver) and
/// [`Handler`](crate::Handler), which is what lets [`Data<T>`] and [`Cmd<T>`]
/// name a plugin's message types no matter which of those it is.
pub trait ServiceBound: 'static {
    /// The service this type implements a piece of.
    type Service: ServiceDef;
}

/// The data type of whatever service `T` belongs to.
pub type Data<T> = <<T as ServiceBound>::Service as ServiceDef>::Data;

/// The custom command type of whatever service `T` belongs to.
pub type Cmd<T> = <<T as ServiceBound>::Service as ServiceDef>::Command;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoCommand;

    struct Cpu;

    impl ServiceDef for Cpu {
        const NAME: &'static str = "cpu";
        const VERSION: u32 = 3;
        type Data = String;
        type Command = NoCommand;
    }

    #[test]
    fn service_id_carries_name_and_version() {
        const ID: ServiceId = ServiceId::of::<Cpu>();
        assert_eq!(ID.name, "cpu");
        assert_eq!(ID.version, 3);
        assert!(!ID.is_agent_wide());
        assert_eq!(ID.to_string(), "cpu/v3");
    }

    #[test]
    fn version_defaults_to_one() {
        struct Bare;
        impl ServiceDef for Bare {
            const NAME: &'static str = "bare";
            type Data = String;
            type Command = NoCommand;
        }
        assert_eq!(ServiceId::of::<Bare>().version, 1);
    }

    #[test]
    fn the_agent_target_is_the_empty_name() {
        assert!(ServiceId::AGENT.is_agent_wide());
        assert_eq!(ServiceId::AGENT.to_string(), "<agent>");
        assert!(!is_valid_service_name(ServiceId::AGENT.name));
    }

    #[test]
    fn valid_names_are_short_lowercase_and_unpunctuated() {
        for good in ["cpu", "cgroup", "gpu", "selfmon", "net-flows", "io_2", "a"] {
            assert!(is_valid_service_name(good), "{good} should be valid");
        }
        for bad in [
            "",
            "GPU",
            "net flows",
            "cpu.stat",
            "cpu/step",
            "thirteenchars",
            "naïve",
        ] {
            assert!(!is_valid_service_name(bad), "{bad} should be invalid");
        }
    }

    #[test]
    fn the_length_cap_is_exactly_max_service_name_len() {
        let at_cap = "x".repeat(MAX_SERVICE_NAME_LEN);
        assert!(is_valid_service_name(&at_cap));
        assert!(!is_valid_service_name(&format!("{at_cap}x")));
    }

    #[test]
    fn data_and_cmd_resolve_through_service_bound() {
        struct Plugin;
        impl ServiceBound for Plugin {
            type Service = Cpu;
        }

        // Compiles only if the aliases resolve to `Cpu`'s associated types.
        let data: Data<Plugin> = String::from("sample");
        assert_eq!(data, "sample");
        assert_eq!(size_of::<Cmd<Plugin>>(), 0);
    }
}
