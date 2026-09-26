//! A one-future executor for tests, so crates that must not depend on a runtime
//! can still test their own futures.
//!
//! Enabled by the `test-util` feature. This is not a runtime: it drives exactly
//! one future on the calling thread and parks in between. Use it in tests, never
//! in the agent or server.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

struct ThreadWaker {
    thread: Thread,
    awake: AtomicBool,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.awake.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

/// Drive `fut` to completion on the current thread.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = Box::pin(fut);
    let state = Arc::new(ThreadWaker {
        thread: thread::current(),
        awake: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&state));
    let mut cx = Context::from_waker(&waker);

    loop {
        if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return out;
        }
        // `park` may return spuriously, hence the flag.
        while !state.awake.swap(false, Ordering::AcqRel) {
            thread::park();
        }
    }
}

/// Poll `fut` once with a no-op waker, discarding any wake-up.
///
/// For asserting that something is still pending.
pub fn poll_once<F: Future>(fut: &mut Pin<&mut F>) -> Poll<F::Output> {
    fut.as_mut().poll(&mut Context::from_waker(Waker::noop()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_on_resolves_a_future_woken_from_another_thread() {
        let signal = crate::ShutdownSignal::new();
        let setter = signal.clone();
        let handle = thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(20));
            setter.set();
        });
        block_on(signal.wait());
        handle.join().expect("setter thread");
        assert!(signal.is_set());
    }

    #[test]
    fn poll_once_reports_pending_without_blocking() {
        let signal = crate::ShutdownSignal::new();
        let mut fut = std::pin::pin!(signal.wait());
        assert!(poll_once(&mut fut).is_pending());
        signal.set();
        assert!(poll_once(&mut fut).is_ready());
    }
}
