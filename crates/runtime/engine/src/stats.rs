use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use cs_api::{EngineStats, ServiceId, ServiceStats};

/// The engine's counters, live.
///
/// Atomics rather than a lock, and incremented on the paths they measure, so
/// self-monitoring costs the data path an atomic add rather than a wait. Read out
/// with [`Counters::snapshot`], which is what
/// [`ServiceCtx::engine_stats`](cs_api::ServiceCtx::engine_stats) returns.
///
/// Everything is cumulative since start, following the same rule the plugins
/// follow: ship counters, let the server compute rates.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub(crate) connected: AtomicBool,
    pub(crate) peers: AtomicU64,
    pub(crate) reconnects: AtomicU64,
    pub(crate) peer_timeouts: AtomicU64,
    pub(crate) frames_sent: AtomicU64,
    pub(crate) frames_received: AtomicU64,
    pub(crate) bytes_sent: AtomicU64,
    pub(crate) bytes_received: AtomicU64,
    pub(crate) data_queue_depth: AtomicU64,
    pub(crate) control_queue_depth: AtomicU64,
    pub(crate) data_dropped: AtomicU64,
    pub(crate) unroutable_frames: AtomicU64,
    pub(crate) commands_completed: AtomicU64,
    pub(crate) commands_expired: AtomicU64,
    pub(crate) command_rtt_total_ms: AtomicU64,
    services: Mutex<HashMap<&'static str, ServiceCounters>>,
}

/// Per-service counters. Registered once per service at startup, so a snapshot
/// lists every service whether or not it has done anything yet.
#[derive(Debug, Default)]
struct ServiceCounters {
    id: Option<ServiceId>,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    samples: AtomicU64,
    sample_errors: AtomicU64,
    sample_time_total_us: AtomicU64,
    sample_time_max_us: AtomicU64,
    panics: AtomicU64,
    restarts: AtomicU64,
}

impl Counters {
    /// Make a service appear in snapshots from now on.
    pub(crate) fn register_service(&self, id: ServiceId) {
        lock(&self.services).entry(id.name).or_default().id = Some(id);
    }

    fn with<R>(&self, service: &'static str, f: impl FnOnce(&ServiceCounters) -> R) -> R {
        f(lock(&self.services).entry(service).or_default())
    }

    pub(crate) fn message_sent(&self, service: &'static str) {
        self.with(service, |s| {
            s.messages_sent.fetch_add(1, Ordering::Relaxed);
        });
    }

    pub(crate) fn message_received(&self, service: &'static str) {
        self.with(service, |s| {
            s.messages_received.fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Record one completed `sample` call and how long it took.
    pub(crate) fn sampled(&self, service: &'static str, took: Duration, failed: bool) {
        let micros = u64::try_from(took.as_micros()).unwrap_or(u64::MAX);
        self.with(service, |s| {
            s.samples.fetch_add(1, Ordering::Relaxed);
            if failed {
                s.sample_errors.fetch_add(1, Ordering::Relaxed);
            }
            s.sample_time_total_us.fetch_add(micros, Ordering::Relaxed);
            s.sample_time_max_us.fetch_max(micros, Ordering::Relaxed);
        });
    }

    pub(crate) fn panicked(&self, service: &'static str) {
        self.with(service, |s| {
            s.panics.fetch_add(1, Ordering::Relaxed);
        });
    }

    pub(crate) fn restarted(&self, service: &'static str) {
        self.with(service, |s| {
            s.restarts.fetch_add(1, Ordering::Relaxed);
        });
    }

    pub(crate) fn command_completed(&self, rtt: Duration) {
        self.commands_completed.fetch_add(1, Ordering::Relaxed);
        self.command_rtt_total_ms.fetch_add(
            u64::try_from(rtt.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// Copy everything out, for a plugin to report.
    pub(crate) fn snapshot(&self) -> EngineStats {
        let mut services: Vec<ServiceStats> = lock(&self.services)
            .values()
            .filter_map(|counters| {
                let id = counters.id?;
                Some(ServiceStats {
                    id,
                    messages_sent: counters.messages_sent.load(Ordering::Relaxed),
                    messages_received: counters.messages_received.load(Ordering::Relaxed),
                    samples: counters.samples.load(Ordering::Relaxed),
                    sample_errors: counters.sample_errors.load(Ordering::Relaxed),
                    sample_time_total: Duration::from_micros(
                        counters.sample_time_total_us.load(Ordering::Relaxed),
                    ),
                    sample_time_max: Duration::from_micros(
                        counters.sample_time_max_us.load(Ordering::Relaxed),
                    ),
                    panics: counters.panics.load(Ordering::Relaxed),
                    restarts: counters.restarts.load(Ordering::Relaxed),
                })
            })
            .collect();
        // Stable order, so a snapshot is comparable to the last one.
        services.sort_unstable_by_key(|s| s.id.name);

        EngineStats {
            connected: self.connected.load(Ordering::Relaxed),
            peers: u32::try_from(self.peers.load(Ordering::Relaxed)).unwrap_or(u32::MAX),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            peer_timeouts: self.peer_timeouts.load(Ordering::Relaxed),
            frames_sent: self.frames_sent.load(Ordering::Relaxed),
            frames_received: self.frames_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            data_queue_depth: self.data_queue_depth.load(Ordering::Relaxed),
            control_queue_depth: self.control_queue_depth.load(Ordering::Relaxed),
            data_dropped: self.data_dropped.load(Ordering::Relaxed),
            unroutable_frames: self.unroutable_frames.load(Ordering::Relaxed),
            commands_completed: self.commands_completed.load(Ordering::Relaxed),
            commands_expired: self.commands_expired.load(Ordering::Relaxed),
            command_rtt_total: Duration::from_millis(
                self.command_rtt_total_ms.load(Ordering::Relaxed),
            ),
            services,
        }
    }
}

/// See `cs_async_util::lock` — this map is always consistent.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Shared handle to the counters.
pub(crate) type SharedCounters = Arc<Counters>;

#[cfg(test)]
mod tests {
    use super::*;
    use cs_api::{NoCommand, ServiceDef};

    struct Cpu;
    impl ServiceDef for Cpu {
        const NAME: &'static str = "cpu";
        type Data = u64;
        type Command = NoCommand;
    }

    struct Gpu;
    impl ServiceDef for Gpu {
        const NAME: &'static str = "gpu";
        type Data = u64;
        type Command = NoCommand;
    }

    #[test]
    fn a_registered_service_appears_even_before_it_does_anything() {
        let counters = Counters::default();
        counters.register_service(ServiceId::of::<Cpu>());

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.services.len(), 1);
        let cpu = snapshot.service("cpu").expect("cpu");
        assert_eq!(cpu.samples, 0);
        assert_eq!(cpu.id.name, "cpu");
    }

    #[test]
    fn sample_timings_accumulate_a_total_and_a_maximum() {
        let counters = Counters::default();
        counters.register_service(ServiceId::of::<Cpu>());
        counters.sampled("cpu", Duration::from_millis(10), false);
        counters.sampled("cpu", Duration::from_millis(30), false);
        counters.sampled("cpu", Duration::from_millis(20), true);

        let cpu = counters.snapshot().service("cpu").expect("cpu").clone();
        assert_eq!(cpu.samples, 3);
        assert_eq!(cpu.sample_errors, 1, "a failed sample still counts as one");
        assert_eq!(cpu.sample_time_total, Duration::from_millis(60));
        assert_eq!(cpu.sample_time_max, Duration::from_millis(30));
        assert_eq!(cpu.mean_sample_time(), Some(Duration::from_millis(20)));
    }

    #[test]
    fn services_are_listed_in_a_stable_order() {
        let counters = Counters::default();
        counters.register_service(ServiceId::of::<Gpu>());
        counters.register_service(ServiceId::of::<Cpu>());
        let names: Vec<_> = counters
            .snapshot()
            .services
            .iter()
            .map(|s| s.id.name)
            .collect();
        assert_eq!(names, ["cpu", "gpu"]);
    }

    #[test]
    fn engine_wide_counters_survive_the_round_trip_to_a_snapshot() {
        let counters = Counters::default();
        counters.connected.store(true, Ordering::Relaxed);
        counters.peers.store(3, Ordering::Relaxed);
        counters.frames_sent.fetch_add(7, Ordering::Relaxed);
        counters.data_dropped.fetch_add(2, Ordering::Relaxed);
        counters.command_completed(Duration::from_millis(40));
        counters.command_completed(Duration::from_millis(60));

        let snapshot = counters.snapshot();
        assert!(snapshot.connected);
        assert_eq!(snapshot.peers, 3);
        assert_eq!(snapshot.frames_sent, 7);
        assert_eq!(snapshot.data_dropped, 2);
        assert_eq!(snapshot.commands_completed, 2);
        assert_eq!(snapshot.mean_command_rtt(), Some(Duration::from_millis(50)));
    }

    #[test]
    fn counters_for_an_unregistered_service_are_kept_but_not_reported() {
        // The data path must never have to check whether a service is known.
        let counters = Counters::default();
        counters.message_sent("ghost");
        counters.panicked("ghost");
        assert!(counters.snapshot().services.is_empty());

        counters.register_service(ServiceId::of::<Cpu>());
        counters.message_sent("cpu");
        counters.restarted("cpu");
        let cpu = counters.snapshot().service("cpu").expect("cpu").clone();
        assert_eq!(cpu.messages_sent, 1);
        assert_eq!(cpu.restarts, 1);
    }
}
