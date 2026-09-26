use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::BoxFuture;

/// The only source of time for engine and sampler-loop code.
///
/// TTLs, backoff, heartbeats, and sampler intervals all go through a `Clock` so
/// tests can control them. Engine code must not call
/// [`Instant::now`] or `std::thread::sleep` directly.
///
/// Implementations live with the runtime (a tokio-backed one in the engine's
/// crate), which is why the returned futures are boxed: a `Clock` is normally
/// held as `Arc<dyn Clock>`.
pub trait Clock: Send + Sync + 'static {
    /// The current monotonic instant.
    fn now(&self) -> Instant;

    /// Sleep for `duration`. A zero duration must still be a valid future.
    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()>;

    /// Sleep until `deadline`, returning immediately if it has passed.
    fn sleep_until(&self, deadline: Instant) -> BoxFuture<'static, ()> {
        self.sleep(deadline.saturating_duration_since(self.now()))
    }
}

/// So `Arc<dyn Clock>` (the usual way to hold one) satisfies `C: Clock` in
/// generic code.
impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now(&self) -> Instant {
        (**self).now()
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()> {
        (**self).sleep(duration)
    }

    fn sleep_until(&self, deadline: Instant) -> BoxFuture<'static, ()> {
        (**self).sleep_until(deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lock;
    use std::sync::Mutex;

    /// Records what it was asked to sleep for and never actually sleeps.
    struct RecordingClock {
        base: Instant,
        slept: Mutex<Vec<Duration>>,
    }

    impl RecordingClock {
        fn new() -> Self {
            Self {
                base: Instant::now(),
                slept: Mutex::new(Vec::new()),
            }
        }

        fn slept(&self) -> Vec<Duration> {
            lock(&self.slept).clone()
        }
    }

    impl Clock for RecordingClock {
        fn now(&self) -> Instant {
            self.base
        }

        fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()> {
            lock(&self.slept).push(duration);
            Box::pin(std::future::ready(()))
        }
    }

    #[test]
    fn sleep_until_converts_a_deadline_to_a_duration() {
        let clock = RecordingClock::new();
        drop(clock.sleep_until(clock.now() + Duration::from_secs(5)));
        assert_eq!(clock.slept(), [Duration::from_secs(5)]);
    }

    #[test]
    fn sleep_until_a_past_deadline_does_not_wait() {
        let clock = RecordingClock::new();
        drop(clock.sleep_until(clock.now() - Duration::from_secs(5)));
        assert_eq!(clock.slept(), [Duration::ZERO]);
    }

    #[test]
    fn arc_forwards_every_method() {
        let recorder = Arc::new(RecordingClock::new());
        let clock: Arc<dyn Clock> = recorder.clone();

        let deadline = clock.now() + Duration::from_millis(30);
        drop(Clock::sleep(&clock, Duration::from_millis(10)));
        drop(Clock::sleep_until(&clock, deadline));
        assert_eq!(
            recorder.slept(),
            [Duration::from_millis(10), Duration::from_millis(30)]
        );

        // The blanket impl is what lets an `Arc<dyn Clock>` be passed where the
        // engine is generic over `C: Clock`.
        fn takes_generic<C: Clock>(clock: &C) -> Instant {
            clock.now()
        }
        assert_eq!(takes_generic(&clock), recorder.now());
    }
}
