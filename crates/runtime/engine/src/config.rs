use std::time::Duration;

use cs_util::{Error, ErrorKind, Result};

/// How long to wait between retries of something that keeps failing.
///
/// Exponential with a ceiling, and no jitter: every delay goes through the
/// [`Clock`](cs_async_util::Clock), and a test that advances time by a known
/// amount has to be able to predict what it is waiting for. Jitter belongs in the
/// dial loop's caller if a cluster ever needs it to avoid a thundering herd.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Backoff {
    /// Delay before the first retry.
    pub initial: Duration,
    /// Ceiling, however many attempts have failed.
    pub max: Duration,
    /// Multiplier applied per failed attempt.
    pub factor: u32,
}

impl Backoff {
    /// The delay after `failures` consecutive failures. `0` means "no failures
    /// yet", and yields no delay at all.
    #[must_use]
    pub fn delay(&self, failures: u32) -> Duration {
        if failures == 0 {
            return Duration::ZERO;
        }
        let Some(exponent) = failures.checked_sub(1) else {
            return self.initial;
        };
        let scale = self
            .factor
            .checked_pow(exponent.min(32))
            .unwrap_or(u32::MAX);
        self.initial
            .checked_mul(scale)
            .unwrap_or(self.max)
            .min(self.max)
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            factor: 2,
        }
    }
}

/// Everything about an engine that an operator might want to change.
///
/// [`EngineConfig::default`] is usable as-is except for
/// [`node`](EngineConfig::node), which has no sensible default and is validated
/// at build time.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// This node's name, as the cluster knows it. Sent once in `Hello` and used
    /// to address commands. Required.
    pub node: String,

    /// Build identifier reported in `Hello`. Advisory.
    pub build: String,

    /// Frames a peer's data lane holds before the oldest are dropped.
    ///
    /// This is the buffer that covers a server outage. Dropping the oldest is
    /// deliberate: fresh counters are worth more than stale ones, and cumulative
    /// counters mean the server can still compute a correct rate across the gap.
    pub data_queue: usize,

    /// Frames a peer's control lane holds.
    ///
    /// Control frames are never dropped, so a peer that fills this is not
    /// keeping up with its own commands and the connection is dropped instead.
    pub control_queue: usize,

    /// How long the whole shutdown sequence may take before it is abandoned.
    pub shutdown_deadline: Duration,

    /// How long a connection may be idle before a heartbeat is sent. Zero
    /// disables heartbeats.
    pub heartbeat_interval: Duration,

    /// How long a peer may say nothing at all before its connection is dropped.
    ///
    /// The only defence against a **half-open connection**: a node that loses power
    /// sends no `FIN` and no `RST`, so the socket stays open and writes keep
    /// succeeding into nothing. Without this, a server believes that agent is
    /// connected until TCP's keepalive notices — which defaults to a couple of
    /// hours — and every command queued for it is delivered into the void.
    ///
    /// Any frame counts as proof of life, not just a heartbeat, so a busy
    /// connection never pays for this. Zero disables it.
    ///
    /// Must be at least twice [`heartbeat_interval`](EngineConfig::heartbeat_interval)
    /// so that one lost heartbeat cannot drop a healthy connection —
    /// [`validate`](EngineConfig::validate) enforces it. Note that it is compared
    /// against the *peer's* heartbeat interval, which this engine cannot see: a
    /// server's timeout has to exceed what its agents are configured to send.
    pub peer_timeout: Duration,

    /// How often the job list is refreshed, and therefore the freshest a
    /// sampler's view of the node can be.
    pub job_refresh: Duration,

    /// How long a change in what this engine can reach may settle before it is
    /// announced to the tier above.
    ///
    /// Only an engine that both listens and dials sends these at all. A relay whose
    /// five hundred agents reconnect together would otherwise send five hundred
    /// announcements where one, a moment later, says the same thing. Nothing depends
    /// on the delay for correctness — every announcement carries the full current set
    /// — so this trades a little staleness for a lot less traffic.
    pub reachability_interval: Duration,

    /// Delay between reconnection attempts.
    pub reconnect: Backoff,

    /// Delay before rebuilding a service that panicked.
    pub restart_backoff: Backoff,

    /// How many partially reassembled messages a peer may have in flight before
    /// the oldest is abandoned.
    ///
    /// A bound on what a peer can make this process hold: it can never be more
    /// than this many times the transport's frame size.
    pub max_partial_messages: usize,
}

impl EngineConfig {
    /// A configuration for `node`, with defaults elsewhere.
    #[must_use]
    pub fn new(node: impl Into<String>) -> Self {
        Self {
            node: node.into(),
            ..Self::default()
        }
    }

    /// Check anything that would otherwise fail confusingly much later.
    pub fn validate(&self) -> Result<()> {
        if self.node.trim().is_empty() {
            return Err(config_error("the engine needs a node name"));
        }
        if self.data_queue == 0 {
            return Err(config_error("data_queue must hold at least one frame"));
        }
        if self.control_queue == 0 {
            return Err(config_error("control_queue must hold at least one frame"));
        }
        if self.shutdown_deadline.is_zero() {
            return Err(config_error("shutdown_deadline must be non-zero"));
        }
        if self.job_refresh.is_zero() {
            return Err(config_error("job_refresh must be non-zero"));
        }
        if self.reachability_interval.is_zero() {
            return Err(config_error("reachability_interval must be non-zero"));
        }
        if self.max_partial_messages == 0 {
            return Err(config_error(
                "max_partial_messages must allow at least one message",
            ));
        }
        if !self.peer_timeout.is_zero() {
            if self.heartbeat_interval.is_zero() {
                return Err(config_error(
                    "peer_timeout needs heartbeats to be enabled, or an idle but \
                     healthy connection would be dropped",
                ));
            }
            if self.peer_timeout < self.heartbeat_interval * 2 {
                return Err(config_error(
                    "peer_timeout must be at least twice heartbeat_interval, so one \
                     lost heartbeat cannot drop a healthy connection",
                ));
            }
        }
        Ok(())
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            node: String::new(),
            build: String::new(),
            // At a batch every 30–60s, 1024 frames is several hours of outage.
            data_queue: 1024,
            control_queue: 256,
            shutdown_deadline: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(15),
            // Three heartbeats: two may be lost before a peer is given up on.
            peer_timeout: Duration::from_secs(45),
            job_refresh: Duration::from_secs(5),
            reachability_interval: Duration::from_secs(5),
            reconnect: Backoff::default(),
            restart_backoff: Backoff {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(60),
                factor: 2,
            },
            max_partial_messages: 16,
        }
    }
}

#[track_caller]
fn config_error(msg: &str) -> Error {
    Error::new(ErrorKind::Config, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeats_may_be_disabled_along_with_the_timeout() {
        // Both off is a coherent choice — no liveness checking at all.
        let config = EngineConfig {
            heartbeat_interval: Duration::ZERO,
            peer_timeout: Duration::ZERO,
            ..EngineConfig::new("node-1")
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn the_default_timeout_is_three_heartbeats() {
        let config = EngineConfig::new("node-1");
        assert_eq!(config.peer_timeout, config.heartbeat_interval * 3);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn backoff_grows_then_stops_at_the_ceiling() {
        let backoff = Backoff {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(1),
            factor: 2,
        };
        assert_eq!(backoff.delay(0), Duration::ZERO, "nothing has failed yet");
        assert_eq!(backoff.delay(1), Duration::from_millis(100));
        assert_eq!(backoff.delay(2), Duration::from_millis(200));
        assert_eq!(backoff.delay(3), Duration::from_millis(400));
        assert_eq!(backoff.delay(4), Duration::from_millis(800));
        assert_eq!(backoff.delay(5), Duration::from_secs(1), "capped");
        assert_eq!(backoff.delay(u32::MAX), Duration::from_secs(1));
    }

    #[test]
    fn backoff_is_deterministic_so_tests_can_predict_it() {
        let backoff = Backoff::default();
        assert_eq!(backoff.delay(3), backoff.delay(3));
    }

    #[test]
    fn a_nameless_engine_is_rejected() {
        for bad in ["", "   "] {
            let err = EngineConfig::new(bad).validate().unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Config);
            assert!(err.to_string().contains("node name"));
        }
        assert!(EngineConfig::new("node-1").validate().is_ok());
    }

    #[test]
    fn zero_sized_queues_and_deadlines_are_rejected() {
        let base = EngineConfig::new("node-1");
        let cases = [
            EngineConfig {
                data_queue: 0,
                ..base.clone()
            },
            EngineConfig {
                control_queue: 0,
                ..base.clone()
            },
            EngineConfig {
                shutdown_deadline: Duration::ZERO,
                ..base.clone()
            },
            EngineConfig {
                job_refresh: Duration::ZERO,
                ..base.clone()
            },
            EngineConfig {
                max_partial_messages: 0,
                ..base.clone()
            },
            // A timeout with nothing to keep the connection alive.
            EngineConfig {
                heartbeat_interval: Duration::ZERO,
                peer_timeout: Duration::from_secs(45),
                ..base.clone()
            },
            // A timeout so tight that one lost heartbeat would trip it.
            EngineConfig {
                heartbeat_interval: Duration::from_secs(15),
                peer_timeout: Duration::from_secs(20),
                ..base
            },
        ];
        for config in cases {
            assert_eq!(config.validate().unwrap_err().kind(), ErrorKind::Config);
        }
    }
}
