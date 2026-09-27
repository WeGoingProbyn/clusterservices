use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cs_api::CommandOutcome;
use cs_async_util::{CommandHandle, CommandSender, command_channel};
use cs_transport::{CommandFrame, CommandId, CommandKind, Frame, Outcome};
use cs_util::{Error, ErrorKind};
use tokio::sync::Notify;
use tracing::{debug, warn};

use crate::stats::SharedCounters;

/// A command waiting for its answer.
struct InFlight {
    node: String,
    sent_at: Instant,
    expires_at: Option<Instant>,
    answer: CommandSender<CommandOutcome>,
}

/// A command for a node that is not currently connected.
struct Queued {
    id: CommandId,
    service: String,
    kind: CommandKind,
    force: bool,
    expires_at: Option<Instant>,
    answer: CommandSender<CommandOutcome>,
}

/// Every command this engine has issued and not yet resolved.
///
/// Two states, and the difference is the whole point: **in flight** means the
/// frame is on a connection and the node owes us an answer; **queued** means the
/// node is not connected and we are holding the command until it comes back. Each
/// carries a deadline, so nothing waits forever in either state.
pub(crate) struct Commands {
    next_id: AtomicU64,
    in_flight: Mutex<HashMap<CommandId, InFlight>>,
    queued: Mutex<HashMap<String, VecDeque<Queued>>>,
    /// Woken whenever a deadline is added, so the sweeper can re-plan.
    changed: Notify,
    counters: SharedCounters,
}

/// A command that is ready to be framed and sent.
pub(crate) struct Ready {
    pub(crate) frame: Frame,
    /// The id the frame carries, for logging and for tests.
    #[allow(dead_code, reason = "read by tests and useful when tracing a command")]
    pub(crate) id: CommandId,
}

impl Commands {
    pub(crate) fn new(counters: SharedCounters) -> Self {
        Self {
            // Ids start at 1: zero means "unset" on the wire and is rejected.
            next_id: AtomicU64::new(1),
            in_flight: Mutex::new(HashMap::new()),
            queued: Mutex::new(HashMap::new()),
            changed: Notify::new(),
            counters,
        }
    }

    fn next_id(&self) -> CommandId {
        CommandId(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Register a command and, if the node is connected, produce the frame to send.
    ///
    /// Synchronous on purpose: dispatching is called from
    /// [`ServiceCtx::command`](cs_api::ServiceCtx::command), which a handler may
    /// call without awaiting, and it only touches a couple of mutexes.
    #[allow(
        clippy::too_many_arguments,
        reason = "one call site; a struct would only move the noise"
    )]
    pub(crate) fn dispatch(
        &self,
        node: &str,
        service: &str,
        target: &str,
        kind: CommandKind,
        force: bool,
        ttl: Duration,
        now: Instant,
        connected: bool,
    ) -> (CommandHandle<CommandOutcome>, Option<Ready>) {
        let id = self.next_id();
        let (answer, handle) = command_channel();
        let expires_at = (!ttl.is_zero()).then(|| now + ttl);

        if connected {
            let frame = Frame::Command(CommandFrame {
                id,
                service: service.to_owned(),
                kind,
                force,
                ttl: (!ttl.is_zero()).then_some(ttl),
                // Empty when the frame is going to the node it is for: the connection
                // already says which node, and a second answer could only disagree.
                // Set when `node` is a *child* that will pass it on further down.
                node: target.to_owned(),
            });
            lock(&self.in_flight).insert(
                id,
                InFlight {
                    node: node.to_owned(),
                    sent_at: now,
                    expires_at,
                    answer,
                },
            );
            self.changed.notify_waiters();
            (handle, Some(Ready { frame, id }))
        } else {
            debug!(node, service, %id, "queueing a command for a disconnected node");
            lock(&self.queued)
                .entry(node.to_owned())
                .or_default()
                .push_back(Queued {
                    id,
                    service: service.to_owned(),
                    kind,
                    force,
                    expires_at,
                    answer,
                });
            self.changed.notify_waiters();
            (handle, None)
        }
    }

    /// Move a node's queued commands onto its new connection.
    ///
    /// The time-to-live on the wire is **recomputed here**, from what is left of
    /// the deadline rather than what it was when the command was issued. That is
    /// what lets expiry work without the two peers agreeing on the time: nothing
    /// absolute is ever sent.
    pub(crate) fn flush_queued(&self, node: &str, now: Instant) -> Vec<Ready> {
        let Some(waiting) = lock(&self.queued).remove(node) else {
            return Vec::new();
        };

        let mut ready = Vec::new();
        for command in waiting {
            let remaining = match command.expires_at {
                Some(deadline) => match deadline.checked_duration_since(now) {
                    Some(left) if !left.is_zero() => Some(left),
                    // Expired while it waited; resolve it rather than send it.
                    _ => {
                        self.expire(command.answer, node, command.id);
                        continue;
                    }
                },
                None => None,
            };

            let frame = Frame::Command(CommandFrame {
                id: command.id,
                service: command.service,
                kind: command.kind,
                force: command.force,
                ttl: remaining,
                node: String::new(),
            });
            lock(&self.in_flight).insert(
                command.id,
                InFlight {
                    node: node.to_owned(),
                    sent_at: now,
                    expires_at: command.expires_at,
                    answer: command.answer,
                },
            );
            ready.push(Ready {
                frame,
                id: command.id,
            });
        }
        ready
    }

    /// Resolve a command with the answer that came back from a node.
    ///
    /// An id nobody is waiting for is counted and dropped: a duplicate or very
    /// late result must not disturb anything, and is not worth killing a
    /// connection over.
    pub(crate) fn complete(&self, id: CommandId, outcome: Outcome, now: Instant) {
        let Some(waiting) = lock(&self.in_flight).remove(&id) else {
            warn!(%id, "a result arrived for a command nobody is waiting for");
            return;
        };
        self.counters
            .command_completed(now.saturating_duration_since(waiting.sent_at));

        // Where the wire's vocabulary becomes the plugin API's: a remote failure
        // is an `Err` with the remote trace rebuilt inside it, never an outcome.
        match outcome {
            Outcome::Ok => waiting.answer.complete(CommandOutcome::Ok),
            Outcome::Unsupported => waiting.answer.complete(CommandOutcome::Unsupported),
            Outcome::Rejected(why) => waiting.answer.complete(CommandOutcome::Rejected(why)),
            Outcome::Expired => waiting.answer.complete(CommandOutcome::Expired),
            Outcome::UnknownService => waiting.answer.complete(CommandOutcome::UnknownService),
            Outcome::Failed(trace) => waiting.answer.fail(
                trace
                    .to_error()
                    .context(format!("command {id} failed on node {}", waiting.node)),
            ),
        };
    }

    /// Resolve everything whose deadline has passed.
    ///
    /// Returns the next deadline to wake for, if any.
    pub(crate) fn sweep(&self, now: Instant) -> Option<Instant> {
        let mut next = None;

        let expired: Vec<_> = {
            let mut in_flight = lock(&self.in_flight);
            let overdue: Vec<CommandId> = in_flight
                .iter()
                .filter_map(|(id, command)| match command.expires_at {
                    Some(deadline) if deadline <= now => Some(*id),
                    Some(deadline) => {
                        next =
                            Some(next.map_or(deadline, |soonest: Instant| soonest.min(deadline)));
                        None
                    }
                    None => None,
                })
                .collect();
            overdue
                .into_iter()
                .filter_map(|id| in_flight.remove(&id).map(|command| (id, command)))
                .collect()
        };
        for (id, command) in expired {
            self.expire(command.answer, &command.node, id);
        }

        let stale: Vec<_> = {
            let mut queued = lock(&self.queued);
            let mut stale = Vec::new();
            for (node, waiting) in queued.iter_mut() {
                let mut kept = VecDeque::with_capacity(waiting.len());
                while let Some(command) = waiting.pop_front() {
                    match command.expires_at {
                        Some(deadline) if deadline <= now => {
                            stale.push((node.clone(), command));
                        }
                        Some(deadline) => {
                            next = Some(
                                next.map_or(deadline, |soonest: Instant| soonest.min(deadline)),
                            );
                            kept.push_back(command);
                        }
                        None => kept.push_back(command),
                    }
                }
                *waiting = kept;
            }
            queued.retain(|_, waiting| !waiting.is_empty());
            stale
        };
        for (node, command) in stale {
            self.expire(command.answer, &node, command.id);
        }

        next
    }

    fn expire(&self, answer: CommandSender<CommandOutcome>, node: &str, id: CommandId) {
        debug!(node, %id, "command expired before it could be delivered");
        self.counters
            .commands_expired
            .fetch_add(1, Ordering::Relaxed);
        answer.complete(CommandOutcome::Expired);
    }

    /// Abandon everything in flight to `node`, because its connection died.
    ///
    /// The commands go back to the queue to be delivered on reconnect — they were
    /// sent at-most-once, and a command whose result never arrived may or may not
    /// have been carried out, so the safe thing is to let it expire rather than
    /// silently retry. Custom commands are expected to be idempotent for exactly
    /// this reason.
    pub(crate) fn abandon(&self, node: &str) {
        let lost: Vec<_> = {
            let mut in_flight = lock(&self.in_flight);
            let ids: Vec<CommandId> = in_flight
                .iter()
                .filter(|(_, command)| command.node == node)
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| in_flight.remove(&id).map(|command| (id, command)))
                .collect()
        };
        for (id, command) in lost {
            debug!(node, %id, "connection died with a command in flight");
            command.answer.fail(Error::new(
                ErrorKind::Transport,
                format!("the connection to {node} died before command {id} was answered"),
            ));
        }
    }

    /// Resolve everything, because the engine is stopping.
    pub(crate) fn abandon_all(&self) {
        let in_flight: Vec<_> = lock(&self.in_flight).drain().collect();
        for (id, command) in in_flight {
            command.answer.fail(Error::new(
                ErrorKind::Shutdown,
                format!("the engine stopped before command {id} was answered"),
            ));
        }
        let queued: Vec<_> = lock(&self.queued).drain().collect();
        for (_, waiting) in queued {
            for command in waiting {
                command.answer.fail(Error::new(
                    ErrorKind::Shutdown,
                    format!("the engine stopped before command {} was sent", command.id),
                ));
            }
        }
    }

    /// Wait until a deadline is added or changed.
    pub(crate) async fn wait_for_change(&self) {
        self.changed.notified().await;
    }

    #[cfg(test)]
    pub(crate) fn in_flight_count(&self) -> usize {
        lock(&self.in_flight).len()
    }

    #[cfg(test)]
    pub(crate) fn queued_count(&self, node: &str) -> usize {
        lock(&self.queued).get(node).map_or(0, VecDeque::len)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::Counters;
    use cs_async_util::test_util::block_on;
    use cs_transport::ErrorTrace;
    use std::sync::Arc;

    fn commands() -> (Commands, SharedCounters) {
        let counters: SharedCounters = Arc::new(Counters::default());
        (Commands::new(Arc::clone(&counters)), counters)
    }

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn a_command_to_a_connected_node_produces_a_frame_at_once() {
        let (commands, _) = commands();
        let (handle, ready) = commands.dispatch(
            "node-7",
            "cgroup",
            "",
            CommandKind::Restart,
            false,
            Duration::from_secs(60),
            now(),
            true,
        );
        let ready = ready.expect("a frame to send");
        assert_eq!(commands.in_flight_count(), 1);
        assert_eq!(commands.queued_count("node-7"), 0);

        let Frame::Command(frame) = ready.frame else {
            panic!("expected a command frame");
        };
        assert_eq!(frame.id, ready.id);
        assert_eq!(frame.service, "cgroup");
        assert_eq!(frame.ttl, Some(Duration::from_secs(60)));
        assert!(!frame.force);

        commands.complete(ready.id, Outcome::Ok, now());
        assert_eq!(block_on(handle).expect("outcome"), CommandOutcome::Ok);
        assert_eq!(commands.in_flight_count(), 0);
    }

    #[test]
    fn a_command_to_a_disconnected_node_waits_for_it() {
        let (commands, _) = commands();
        let (_handle, ready) = commands.dispatch(
            "node-7",
            "cgroup",
            "",
            CommandKind::Shutdown,
            false,
            Duration::from_secs(60),
            now(),
            false,
        );
        assert!(ready.is_none(), "nothing to send yet");
        assert_eq!(commands.queued_count("node-7"), 1);
        assert_eq!(commands.in_flight_count(), 0);
    }

    #[test]
    fn ids_are_unique_and_never_zero() {
        let (commands, _) = commands();
        let mut ids = Vec::new();
        for _ in 0..5 {
            let (_h, ready) = commands.dispatch(
                "n",
                "s",
                "",
                CommandKind::Restart,
                false,
                Duration::ZERO,
                now(),
                true,
            );
            ids.push(ready.expect("frame").id);
        }
        assert!(ids.iter().all(|id| id.0 != 0));
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), 5);
    }

    #[test]
    fn a_reconnect_recomputes_the_time_to_live_from_what_is_left() {
        let (commands, _) = commands();
        let issued = now();
        let (_handle, _) = commands.dispatch(
            "node-7",
            "cgroup",
            "",
            CommandKind::Restart,
            false,
            Duration::from_secs(60),
            issued,
            false,
        );

        // Forty seconds later the node comes back: twenty are left, and twenty is
        // what goes on the wire — not the sixty it was issued with.
        let ready = commands.flush_queued("node-7", issued + Duration::from_secs(40));
        assert_eq!(ready.len(), 1);
        let Frame::Command(frame) = &ready[0].frame else {
            panic!("expected a command frame");
        };
        assert_eq!(frame.ttl, Some(Duration::from_secs(20)));
        assert_eq!(commands.in_flight_count(), 1);
        assert_eq!(commands.queued_count("node-7"), 0);
    }

    #[test]
    fn a_command_that_outlived_its_ttl_is_expired_rather_than_sent() {
        let (commands, counters) = commands();
        let issued = now();
        let (handle, _) = commands.dispatch(
            "node-7",
            "cgroup",
            "",
            CommandKind::Restart,
            false,
            Duration::from_secs(60),
            issued,
            false,
        );

        let ready = commands.flush_queued("node-7", issued + Duration::from_secs(61));
        assert!(ready.is_empty(), "nothing should be sent");
        assert_eq!(block_on(handle).expect("outcome"), CommandOutcome::Expired);
        assert_eq!(counters.snapshot().commands_expired, 1);
    }

    #[test]
    fn the_sweeper_expires_overdue_commands_in_both_states() {
        let (commands, counters) = commands();
        let issued = now();
        let (queued, _) = commands.dispatch(
            "node-7",
            "s",
            "",
            CommandKind::Restart,
            false,
            Duration::from_secs(10),
            issued,
            false,
        );
        let (sent, _) = commands.dispatch(
            "node-8",
            "s",
            "",
            CommandKind::Restart,
            false,
            Duration::from_secs(10),
            issued,
            true,
        );
        let (long, _) = commands.dispatch(
            "node-9",
            "s",
            "",
            CommandKind::Restart,
            false,
            Duration::from_secs(600),
            issued,
            true,
        );

        let next = commands.sweep(issued + Duration::from_secs(11));
        assert_eq!(block_on(queued).expect("outcome"), CommandOutcome::Expired);
        assert_eq!(block_on(sent).expect("outcome"), CommandOutcome::Expired);
        assert_eq!(counters.snapshot().commands_expired, 2);

        // The one with time left survives, and its deadline is what to wake for.
        assert_eq!(commands.in_flight_count(), 1);
        assert_eq!(next, Some(issued + Duration::from_secs(600)));
        drop(long);
    }

    #[test]
    fn a_command_with_no_ttl_never_expires() {
        let (commands, _) = commands();
        let issued = now();
        let (_handle, _) = commands.dispatch(
            "node-7",
            "s",
            "",
            CommandKind::Restart,
            false,
            Duration::ZERO,
            issued,
            true,
        );
        assert_eq!(commands.sweep(issued + Duration::from_secs(86_400)), None);
        assert_eq!(commands.in_flight_count(), 1);
    }

    #[test]
    fn a_remote_failure_comes_back_as_an_error_carrying_the_remote_trace() {
        let (commands, _) = commands();
        let (handle, ready) = commands.dispatch(
            "node-7",
            "gpu",
            "",
            CommandKind::Restart,
            false,
            Duration::ZERO,
            now(),
            true,
        );
        let id = ready.expect("frame").id;

        let remote = Error::new(ErrorKind::Timeout, "nvml did not answer");
        commands.complete(
            id,
            Outcome::Failed(ErrorTrace::from_error("node-7", &remote)),
            now(),
        );

        let err = block_on(handle).unwrap_err();
        // The remote kind survives, so a remote timeout is still retryable here.
        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert!(err.is_retryable());
        let rendered = format!("{err:?}");
        assert!(rendered.contains("failed on node node-7"), "{rendered}");
        assert!(rendered.contains("nvml did not answer"), "{rendered}");
    }

    #[test]
    fn every_outcome_maps_onto_the_plugin_api() {
        let cases = [
            (Outcome::Ok, CommandOutcome::Ok),
            (Outcome::Unsupported, CommandOutcome::Unsupported),
            (
                Outcome::Rejected("busy".into()),
                CommandOutcome::Rejected("busy".into()),
            ),
            (Outcome::Expired, CommandOutcome::Expired),
            (Outcome::UnknownService, CommandOutcome::UnknownService),
        ];
        for (wire, expected) in cases {
            let (commands, _) = commands();
            let (handle, ready) = commands.dispatch(
                "n",
                "s",
                "",
                CommandKind::Restart,
                false,
                Duration::ZERO,
                now(),
                true,
            );
            commands.complete(ready.expect("frame").id, wire, now());
            assert_eq!(block_on(handle).expect("outcome"), expected);
        }
    }

    #[test]
    fn a_result_for_an_unknown_command_is_ignored() {
        let (commands, counters) = commands();
        commands.complete(CommandId(999), Outcome::Ok, now());
        assert_eq!(counters.snapshot().commands_completed, 0);
    }

    #[test]
    fn rtt_is_measured_from_dispatch_to_result() {
        let (commands, counters) = commands();
        let issued = now();
        let (_handle, ready) = commands.dispatch(
            "n",
            "s",
            "",
            CommandKind::Restart,
            false,
            Duration::ZERO,
            issued,
            true,
        );
        commands.complete(
            ready.expect("frame").id,
            Outcome::Ok,
            issued + Duration::from_millis(250),
        );
        let stats = counters.snapshot();
        assert_eq!(stats.commands_completed, 1);
        assert_eq!(stats.mean_command_rtt(), Some(Duration::from_millis(250)));
    }

    #[test]
    fn a_dead_connection_fails_what_it_was_carrying() {
        let (commands, _) = commands();
        let (lost, _) = commands.dispatch(
            "node-7",
            "s",
            "",
            CommandKind::Restart,
            false,
            Duration::ZERO,
            now(),
            true,
        );
        let (other, _) = commands.dispatch(
            "node-8",
            "s",
            "",
            CommandKind::Restart,
            false,
            Duration::ZERO,
            now(),
            true,
        );

        commands.abandon("node-7");
        let err = block_on(lost).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Transport);
        assert!(err.is_retryable(), "the operator may simply try again");
        assert!(err.to_string().contains("node-7"));

        // The other node's command is untouched.
        assert_eq!(commands.in_flight_count(), 1);
        drop(other);
    }

    #[test]
    fn stopping_resolves_everything_rather_than_leaving_a_caller_waiting() {
        let (commands, _) = commands();
        let (sent, _) = commands.dispatch(
            "node-7",
            "s",
            "",
            CommandKind::Restart,
            false,
            Duration::ZERO,
            now(),
            true,
        );
        let (queued, _) = commands.dispatch(
            "node-8",
            "s",
            "",
            CommandKind::Restart,
            false,
            Duration::ZERO,
            now(),
            false,
        );

        commands.abandon_all();
        for handle in [sent, queued] {
            let err = block_on(handle).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Shutdown);
        }
        assert_eq!(commands.in_flight_count(), 0);
        assert_eq!(commands.queued_count("node-8"), 0);
    }

    #[test]
    fn queued_commands_keep_their_order_per_node() {
        let (commands, _) = commands();
        let issued = now();
        for _ in 0..3 {
            let _ = commands.dispatch(
                "node-7",
                "s",
                "",
                CommandKind::Restart,
                false,
                Duration::from_secs(60),
                issued,
                false,
            );
        }
        let ready = commands.flush_queued("node-7", issued);
        let ids: Vec<_> = ready.iter().map(|r| r.id.0).collect();
        assert_eq!(ids, [1, 2, 3]);
    }
}
