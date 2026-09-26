use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use cs_util::{Error, ErrorKind};

use crate::lock;

/// Create a one-shot pair for a command's result.
///
/// The engine keeps the [`CommandSender`] in its in-flight table, keyed by
/// command ID, and hands the [`CommandHandle`] to whoever issued the command.
/// When the matching `CommandResult` frame arrives the sender is completed; if
/// the engine gives up on the command instead — the peer went away, the command
/// expired, shutdown won the race — dropping the sender resolves the handle as
/// an error, so a caller can never wait forever.
///
/// ```
/// use cs_async_util::{CommandHandle, command_channel};
/// use cs_util::{Error, ErrorKind};
///
/// # async fn example() {
/// let (sender, handle) = command_channel::<&str>();
/// assert!(sender.complete("ok"));
/// assert_eq!(handle.await.unwrap(), "ok");
///
/// // Abandoned commands resolve rather than hanging.
/// let (sender, handle) = command_channel::<&str>();
/// drop(sender);
/// assert_eq!(handle.await.unwrap_err().kind(), ErrorKind::Shutdown);
///
/// // And a failure can be delivered deliberately, keeping its cause.
/// let (sender, handle) = command_channel::<&str>();
/// sender.fail(Error::new(ErrorKind::Timeout, "the node never answered"));
/// assert_eq!(handle.await.unwrap_err().kind(), ErrorKind::Timeout);
/// # }
/// ```
#[allow(clippy::type_complexity)]
pub fn command_channel<T>() -> (CommandSender<T>, CommandHandle<T>) {
    let shared = Arc::new(Mutex::new(State::<T> {
        value: None,
        waker: None,
        sender_gone: false,
        receiver_gone: false,
    }));
    (
        CommandSender {
            shared: Some(Arc::clone(&shared)),
        },
        CommandHandle { shared },
    )
}

struct State<T> {
    value: Option<Result<T, Error>>,
    waker: Option<Waker>,
    sender_gone: bool,
    receiver_gone: bool,
}

/// Completes a [`CommandHandle`] exactly once.
///
/// Dropping without completing resolves the handle as
/// [`ErrorKind::Shutdown`].
pub struct CommandSender<T> {
    // `None` once completed, so `Drop` knows not to report abandonment.
    shared: Option<Arc<Mutex<State<T>>>>,
}

impl<T> CommandSender<T> {
    /// Deliver the result. Returns `false` if the handle was already dropped,
    /// in which case `value` is discarded — a late result for a caller that
    /// stopped caring is not an error.
    pub fn complete(self, value: T) -> bool {
        self.resolve(Ok(value))
    }

    /// Deliver a failure instead of a result.
    ///
    /// The handle's caller sees this exact error, so the reason a command could
    /// not be answered — including a failure rebuilt from a remote node's error
    /// trace — reaches them intact rather than as "abandoned".
    pub fn fail(self, error: Error) -> bool {
        self.resolve(Err(error))
    }

    fn resolve(mut self, outcome: Result<T, Error>) -> bool {
        let Some(shared) = self.shared.take() else {
            // Unreachable: both callers consume `self`, and the field is only
            // taken here.
            return false;
        };
        let waker = {
            let mut state = lock(&shared);
            if state.receiver_gone {
                return false;
            }
            state.value = Some(outcome);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    /// Whether the handle has been dropped. A caller that stopped waiting can
    /// be checked for before doing expensive work to produce the result.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared
            .as_ref()
            .is_some_and(|shared| lock(shared).receiver_gone)
    }
}

impl<T> Drop for CommandSender<T> {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };
        let waker = {
            let mut state = lock(&shared);
            state.sender_gone = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> std::fmt::Debug for CommandSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandSender")
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Awaits the result of one in-flight command.
///
/// Resolves to `Err` with [`ErrorKind::Shutdown`] if the engine dropped the
/// command without a result. Dropping the handle cancels interest in the result;
/// the engine's [`CommandSender::complete`] then reports `false`.
#[must_use = "futures do nothing unless awaited"]
pub struct CommandHandle<T> {
    shared: Arc<Mutex<State<T>>>,
}

impl<T> Future for CommandHandle<T> {
    type Output = Result<T, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = lock(&self.shared);
        if let Some(outcome) = state.value.take() {
            return Poll::Ready(outcome);
        }
        if state.sender_gone {
            return Poll::Ready(Err(Error::new(
                ErrorKind::Shutdown,
                "command was abandoned before a result arrived",
            )));
        }
        if !state
            .waker
            .as_ref()
            .is_some_and(|w| w.will_wake(cx.waker()))
        {
            state.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl<T> Drop for CommandHandle<T> {
    fn drop(&mut self) {
        lock(&self.shared).receiver_gone = true;
    }
}

impl<T> std::fmt::Debug for CommandHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = lock(&self.shared);
        f.debug_struct("CommandHandle")
            .field("ready", &state.value.is_some())
            .field("abandoned", &state.sender_gone)
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
    fn result_delivered_before_the_handle_is_polled() {
        let (sender, handle) = command_channel::<u32>();
        assert!(sender.complete(7));
        assert_eq!(block_on(handle).unwrap(), 7);
    }

    #[test]
    fn handle_is_pending_until_completed() {
        let (sender, handle) = command_channel::<u32>();
        let mut handle = std::pin::pin!(handle);
        assert!(poll_once(&mut handle).is_pending());
        assert!(sender.complete(1));
        assert!(poll_once(&mut handle).is_ready());
    }

    #[test]
    fn result_delivered_from_another_thread_wakes_the_waiter() {
        let (sender, handle) = command_channel::<&'static str>();
        let setter = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            assert!(sender.complete("handled"));
        });
        assert_eq!(block_on(handle).unwrap(), "handled");
        setter.join().expect("sender thread");
    }

    #[test]
    fn a_deliberate_failure_reaches_the_caller_with_its_cause() {
        let (sender, handle) = command_channel::<u32>();
        assert!(sender.fail(Error::new(ErrorKind::Rejected, "node said no")));
        let err = block_on(handle).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Rejected);
        assert!(err.to_string().contains("node said no"));
    }

    #[test]
    fn a_failure_can_carry_a_whole_remote_chain() {
        use cs_util::ResultExt;
        let (sender, handle) = command_channel::<u32>();
        let remote = Err::<(), _>(Error::new(ErrorKind::Plugin, "nvml missing"))
            .context("sampler failed")
            .unwrap_err();
        sender.fail(remote);
        let err = block_on(handle).unwrap_err();
        assert_eq!(err.chain().count(), 2);
        assert_eq!(err.kind(), ErrorKind::Plugin);
    }

    #[test]
    fn dropping_the_sender_fails_the_handle() {
        let (sender, handle) = command_channel::<u32>();
        drop(sender);
        let err = block_on(handle).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Shutdown);
        assert!(!err.is_retryable());
    }

    #[test]
    fn dropping_the_sender_wakes_a_pending_waiter() {
        let (sender, handle) = command_channel::<u32>();
        let dropper = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            drop(sender);
        });
        assert!(block_on(handle).is_err());
        dropper.join().expect("dropper thread");
    }

    #[test]
    fn dropping_the_handle_makes_completion_report_false() {
        let (sender, handle) = command_channel::<u32>();
        assert!(!sender.is_closed());
        drop(handle);
        assert!(sender.is_closed());
        assert!(
            !sender.complete(9),
            "late result is discarded, not an error"
        );
    }

    #[test]
    fn a_result_racing_a_dropped_handle_is_never_lost_silently() {
        for _ in 0..200 {
            let (sender, handle) = command_channel::<u32>();
            let completer = thread::spawn(move || sender.complete(1));
            drop(handle);
            // Either it landed before the drop or it was reported as discarded;
            // both are fine, neither may panic or hang.
            let _ = completer.join().expect("completer thread");
        }
    }

    #[test]
    fn handle_is_send_and_static_for_send_payloads() {
        fn assert_send_static<F: Future + Send + 'static>(_: F) {}
        let (_sender, handle) = command_channel::<u32>();
        assert_send_static(handle);
    }
}
