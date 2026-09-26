use std::time::Duration;

use crate::ServiceId;

/// A point-in-time read of the engine's own counters.
///
/// Returned by [`ServiceCtx::engine_stats`](crate::ServiceCtx::engine_stats).
/// The engine keeps these as atomics and copies them out on demand, so reading
/// them is cheap and never blocks the data path — the point is that the agent can
/// monitor itself with an ordinary sampler instead of special-casing telemetry.
///
/// All counters are **cumulative since process start**, following the same rule
/// as plugin metrics: ship counters, let the server compute rates.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct EngineStats {
    /// Whether a peer connection is currently up. On an agent there is one; on a
    /// server this is true when at least one agent is connected.
    pub connected: bool,
    /// Peers currently connected. Always 0 or 1 on an agent.
    pub peers: u32,
    /// Times a connection was established after a failure or a drop.
    pub reconnects: u64,
    /// Frames handed to the transport.
    pub frames_sent: u64,
    /// Frames read from the transport.
    pub frames_received: u64,
    /// Payload bytes handed to the transport, excluding framing.
    pub bytes_sent: u64,
    /// Payload bytes read from the transport, excluding framing.
    pub bytes_received: u64,
    /// Messages waiting in the data lane right now.
    pub data_queue_depth: u64,
    /// Messages waiting in the control lane right now. Drained ahead of data.
    pub control_queue_depth: u64,
    /// Messages dropped because the bounded data queue was full — the oldest go
    /// first. Non-zero means the outage outlasted the buffer.
    pub data_dropped: u64,
    /// Frames discarded because no service claimed the name on them.
    pub unroutable_frames: u64,
    /// Commands whose results came back.
    pub commands_completed: u64,
    /// Commands that expired in a queue without being delivered.
    pub commands_expired: u64,
    /// Total round-trip time of completed commands, for an average against
    /// [`commands_completed`](EngineStats::commands_completed).
    pub command_rtt_total: Duration,
    /// One entry per registered service.
    pub services: Vec<ServiceStats>,
}

impl EngineStats {
    /// The stats of one service by name.
    #[must_use]
    pub fn service(&self, name: &str) -> Option<&ServiceStats> {
        self.services.iter().find(|s| s.id.name == name)
    }

    /// Mean command round-trip time, or `None` if nothing has completed.
    #[must_use]
    pub fn mean_command_rtt(&self) -> Option<Duration> {
        let n = u32::try_from(self.commands_completed).ok()?;
        self.command_rtt_total.checked_div(n)
    }
}

/// Per-service counters within [`EngineStats`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ServiceStats {
    /// Which service.
    pub id: ServiceId,
    /// Messages this service handed to its [`Outbox`](crate::Outbox).
    pub messages_sent: u64,
    /// Messages routed to this service's [`Handler`](crate::Handler).
    pub messages_received: u64,
    /// Completed calls to [`Sampler::sample`](crate::Sampler::sample).
    pub samples: u64,
    /// Calls to `sample` that returned an error.
    pub sample_errors: u64,
    /// Total time spent inside `sample`, for an average against
    /// [`samples`](ServiceStats::samples).
    pub sample_time_total: Duration,
    /// Longest single `sample` call. A sampler overrunning its interval shows up
    /// here first.
    pub sample_time_max: Duration,
    /// Times this service panicked and was rebuilt from its factory.
    pub panics: u64,
    /// Times this service was restarted by command.
    pub restarts: u64,
}

impl ServiceStats {
    /// Zeroed counters for `id`.
    #[must_use]
    pub const fn new(id: ServiceId) -> Self {
        Self {
            id,
            messages_sent: 0,
            messages_received: 0,
            samples: 0,
            sample_errors: 0,
            sample_time_total: Duration::ZERO,
            sample_time_max: Duration::ZERO,
            panics: 0,
            restarts: 0,
        }
    }

    /// Mean time spent in `sample`, or `None` if it has never completed.
    #[must_use]
    pub fn mean_sample_time(&self) -> Option<Duration> {
        let n = u32::try_from(self.samples).ok()?;
        self.sample_time_total.checked_div(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoCommand, ServiceDef};

    struct Cpu;
    impl ServiceDef for Cpu {
        const NAME: &'static str = "cpu";
        type Data = String;
        type Command = NoCommand;
    }

    fn stats() -> EngineStats {
        EngineStats {
            connected: true,
            peers: 1,
            commands_completed: 4,
            command_rtt_total: Duration::from_millis(200),
            services: vec![ServiceStats {
                samples: 8,
                sample_time_total: Duration::from_millis(80),
                sample_time_max: Duration::from_millis(30),
                ..ServiceStats::new(ServiceId::of::<Cpu>())
            }],
            ..EngineStats::default()
        }
    }

    #[test]
    fn a_fresh_snapshot_is_all_zeroes() {
        let empty = EngineStats::default();
        assert!(!empty.connected);
        assert_eq!(empty.frames_sent, 0);
        assert_eq!(empty.command_rtt_total, Duration::ZERO);
        assert!(empty.services.is_empty());
        assert_eq!(empty.mean_command_rtt(), None);
    }

    #[test]
    fn services_are_addressable_by_name() {
        let stats = stats();
        assert_eq!(stats.service("cpu").map(|s| s.samples), Some(8));
        assert!(stats.service("gpu").is_none());
    }

    #[test]
    fn averages_come_from_the_cumulative_totals() {
        let stats = stats();
        assert_eq!(stats.mean_command_rtt(), Some(Duration::from_millis(50)));
        let cpu = stats.service("cpu").expect("cpu");
        assert_eq!(cpu.mean_sample_time(), Some(Duration::from_millis(10)));
        assert_eq!(cpu.sample_time_max, Duration::from_millis(30));
    }

    #[test]
    fn averages_are_none_before_the_first_completion() {
        let fresh = ServiceStats::new(ServiceId::of::<Cpu>());
        assert_eq!(fresh.mean_sample_time(), None);
        assert_eq!(fresh.samples, 0);
    }
}
