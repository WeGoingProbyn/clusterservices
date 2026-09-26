//! The seam between the engine and the plugin API.
//!
//! **Plugins never name anything in this module.** It exists because
//! [`ServiceCtx`](crate::ServiceCtx) has to reach into an engine that is generic
//! over its transport, while a plugin's types must not mention the transport at
//! all. So the engine implements these three traits and hands the resulting trait
//! objects to each service's context; the transport stays behind them.
//!
//! Everything here is deliberately narrow — bytes, names, and `dyn` — so that
//! `NodeEngine<T: Transport>` can satisfy it without leaking `T`.

use std::sync::Arc;

use cs_async_util::{Clock, CommandHandle};
use cs_util::Result;

use crate::{CommandOpts, CommandOutcome, Encodable, EngineStats, ServiceId};

/// Where an [`Outbox`](crate::Outbox) puts messages.
///
/// The engine encodes `msg` straight into a queue buffer on the calling thread —
/// serialization is synchronous, by design — and returns without waiting for the
/// network.
pub trait DataSink: Send + Sync + 'static {
    /// Queue one message for `service`.
    ///
    /// Returns an error only when the message can never be sent: the engine is
    /// shutting down, or the service is no longer registered. A full queue is
    /// *not* an error — the oldest message is dropped and counted in
    /// [`EngineStats::data_dropped`].
    fn send(&self, service: ServiceId, msg: &dyn Encodable) -> Result<()>;
}

/// Where a [`WorkerSpawner`](crate::WorkerSpawner) puts threads.
///
/// Workers are OS threads, not tasks: plugin sampling code is synchronous and
/// must never run on the async worker pool.
pub trait WorkerHost: Send + Sync + 'static {
    /// Start a named thread and track it for shutdown.
    ///
    /// `name` is already trimmed to the 15-byte thread-name budget by
    /// [`WorkerSpawner`](crate::WorkerSpawner).
    fn spawn_blocking(&self, name: &str, body: Box<dyn FnOnce() + Send + 'static>) -> Result<()>;
}

/// Everything else a service can ask of the engine.
pub trait ServiceRuntime: Send + Sync + 'static {
    /// This node's name, as sent in `Hello`.
    fn node(&self) -> &str;

    /// The clock all timing goes through, so tests can control it.
    fn clock(&self) -> &Arc<dyn Clock>;

    /// Copy out the engine's counters.
    fn engine_stats(&self) -> EngineStats;

    /// Queue a command for a node and return a handle to its result.
    fn dispatch_command(&self, request: CommandRequest<'_>) -> CommandHandle<CommandOutcome>;
}

/// A command on its way out, with the payload type already erased.
#[derive(Debug)]
pub struct CommandRequest<'a> {
    /// Name of the node to deliver to.
    pub node: &'a str,
    /// Target service, or [`ServiceId::AGENT`] for the whole agent.
    pub service: ServiceId,
    /// Which command.
    pub kind: CommandKind<'a>,
    /// Delivery options.
    pub opts: CommandOpts,
}

/// [`Command`](crate::Command) with its custom payload erased to
/// [`Encodable`], so the engine can frame it without knowing the type.
pub enum CommandKind<'a> {
    /// [`Command::Shutdown`](crate::Command::Shutdown).
    Shutdown,
    /// [`Command::Restart`](crate::Command::Restart).
    Restart,
    /// [`Command::Custom`](crate::Command::Custom), ready to encode.
    Custom(&'a dyn Encodable),
}

impl std::fmt::Debug for CommandKind<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shutdown => f.write_str("Shutdown"),
            Self::Restart => f.write_str("Restart"),
            Self::Custom(payload) => f
                .debug_struct("Custom")
                .field("encoded_len", &payload.encoded_len())
                .finish(),
        }
    }
}
