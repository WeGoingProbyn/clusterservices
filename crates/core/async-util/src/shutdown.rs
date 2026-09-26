use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use crate::lock;

/// A flag that is set once and never cleared, with an awaitable edge.
///
/// The engine sets it; every worker, sampler loop, and writer task watches it.
/// Cloning is cheap and all clones share one flag, so a service can hand copies
/// to its own threads.
///
/// ```
/// # use cs_async_util::ShutdownSignal;
/// let signal = ShutdownSignal::new();
/// assert!(!signal.is_set());
///
/// let worker_copy = signal.clone();
/// signal.set();
/// assert!(worker_copy.is_set());
/// ```
///
/// [`wait`](ShutdownSignal::wait) returns an owned `'static` future, so it can
/// be selected on or moved into a spawned task:
///
/// ```
/// # use cs_async_util::ShutdownSignal;
/// # async fn example(signal: ShutdownSignal) {
/// // Both arms may be taken; `wait` is cancel-safe and may be re-awaited.
/// signal.wait().await;
/// # }
/// ```
#[derive(Clone)]
pub struct ShutdownSignal {
    inner: Arc<Inner>,
}

struct Inner {
    set: AtomicBool,
    /// Wakers of the currently pending [`WaitShutdown`] futures. Slots are
    /// reused so a loop that repeatedly creates and drops waiters cannot grow
    /// this without bound.
    waiters: Mutex<Vec<Option<Waker>>>,
}

impl ShutdownSignal {
    /// A signal that has not fired.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                set: AtomicBool::new(false),
                waiters: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Fire the signal, waking every waiter. Idempotent.
    pub fn set(&self) {
        if self.inner.set.swap(true, Ordering::AcqRel) {
            return;
        }
        // The guard is dropped at the end of this statement, so the wakers below
        // are invoked without the lock held.
        let waiters = std::mem::take(&mut *lock(&self.inner.waiters));
        for waker in waiters.into_iter().flatten() {
            waker.wake();
        }
    }

    /// Whether the signal has fired.
    #[must_use]
    pub fn is_set(&self) -> bool {
        self.inner.set.load(Ordering::Acquire)
    }

    /// A future that resolves when the signal fires — immediately if it already
    /// has.
    pub fn wait(&self) -> WaitShutdown {
        WaitShutdown {
            inner: Arc::clone(&self.inner),
            slot: None,
        }
    }

    #[cfg(test)]
    fn slot_count(&self) -> usize {
        lock(&self.inner.waiters).len()
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownSignal")
            .field("set", &self.is_set())
            .finish()
    }
}

/// Future returned by [`ShutdownSignal::wait`].
#[must_use = "futures do nothing unless awaited"]
pub struct WaitShutdown {
    inner: Arc<Inner>,
    slot: Option<usize>,
}

impl Future for WaitShutdown {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        if this.inner.set.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        let mut waiters = lock(&this.inner.waiters);
        // Re-check under the lock. `set` swaps the flag *before* taking this
        // lock, so seeing `false` here means our waker is registered in time.
        if this.inner.set.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        match this.slot.and_then(|slot| waiters.get_mut(slot)) {
            Some(entry) => {
                if !entry.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                    *entry = Some(cx.waker().clone());
                }
            }
            None => {
                let waker = Some(cx.waker().clone());
                let slot = match waiters.iter().position(Option::is_none) {
                    Some(free) => {
                        waiters[free] = waker;
                        free
                    }
                    None => {
                        waiters.push(waker);
                        waiters.len() - 1
                    }
                };
                this.slot = Some(slot);
            }
        }
        Poll::Pending
    }
}

impl Drop for WaitShutdown {
    fn drop(&mut self) {
        let Some(slot) = self.slot.take() else {
            return;
        };
        if self.inner.set.load(Ordering::Acquire) {
            // `set` already drained every waker; the slot vector is empty or
            // belongs to futures that will resolve on their next poll anyway.
            return;
        }
        if let Some(entry) = lock(&self.inner.waiters).get_mut(slot) {
            *entry = None;
        }
    }
}

impl std::fmt::Debug for WaitShutdown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitShutdown")
            .field("set", &self.inner.set.load(Ordering::Acquire))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{block_on, poll_once};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn set_is_visible_to_clones_and_idempotent() {
        let signal = ShutdownSignal::new();
        let clone = signal.clone();
        assert!(!clone.is_set());
        signal.set();
        signal.set();
        assert!(clone.is_set());
    }

    #[test]
    fn wait_resolves_immediately_when_already_set() {
        let signal = ShutdownSignal::new();
        signal.set();
        block_on(signal.wait());
    }

    #[test]
    fn wait_is_pending_until_set() {
        let signal = ShutdownSignal::new();
        let mut fut = std::pin::pin!(signal.wait());
        assert!(poll_once(&mut fut).is_pending());
        signal.set();
        assert!(poll_once(&mut fut).is_ready());
    }

    #[test]
    fn every_waiter_is_woken() {
        let signal = ShutdownSignal::new();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let signal = signal.clone();
                thread::spawn(move || block_on(signal.wait()))
            })
            .collect();

        // Give the waiters a chance to register before firing.
        thread::sleep(Duration::from_millis(20));
        signal.set();
        for handle in threads {
            handle.join().expect("waiter thread");
        }
    }

    #[test]
    fn a_waiter_set_between_check_and_registration_is_still_woken() {
        // Hammer the registration race: the setter runs concurrently with a
        // waiter's first poll.
        for _ in 0..200 {
            let signal = ShutdownSignal::new();
            let setter = signal.clone();
            let handle = thread::spawn(move || setter.set());
            block_on(signal.wait());
            handle.join().expect("setter thread");
        }
    }

    #[test]
    fn dropped_waiters_release_their_slots() {
        let signal = ShutdownSignal::new();
        for _ in 0..100 {
            let mut fut = std::pin::pin!(signal.wait());
            assert!(poll_once(&mut fut).is_pending());
        }
        assert_eq!(
            signal.slot_count(),
            1,
            "each dropped waiter should free its slot for the next one"
        );
    }

    #[test]
    fn repolling_one_waiter_does_not_allocate_more_slots() {
        let signal = ShutdownSignal::new();
        let mut fut = std::pin::pin!(signal.wait());
        for _ in 0..10 {
            assert!(poll_once(&mut fut).is_pending());
        }
        assert_eq!(signal.slot_count(), 1);
    }

    #[test]
    fn wait_future_is_static_and_send() {
        fn assert_static_send<F: Future + Send + 'static>(_: F) {}
        assert_static_send(ShutdownSignal::new().wait());
    }
}
