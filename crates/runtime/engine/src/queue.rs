use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Mutex, MutexGuard};

use bytes::Bytes;
use cs_api::ServiceId;
use cs_transport::Frame;
use cs_util::{Error, ErrorKind, Result};
use tokio::sync::Notify;

use crate::stats::SharedCounters;

/// One thing waiting to go out to a peer.
#[derive(Debug)]
pub(crate) enum Outgoing {
    /// A control frame, already built. Never dropped.
    Control(Frame),
    /// Encoded service data, not yet framed — the writer decides how many frames
    /// it becomes, since only it knows the transport's ceiling.
    Data {
        /// Which service produced it.
        service: ServiceId,
        /// The plugin's encoded message.
        payload: Bytes,
    },
}

#[cfg(test)]
impl Outgoing {
    /// Which lane this went out on, for assertions about drain order.
    pub(crate) const fn lane(&self) -> cs_transport::Lane {
        match self {
            Self::Control(_) => cs_transport::Lane::Control,
            Self::Data { .. } => cs_transport::Lane::Data,
        }
    }
}

/// A peer's two outbound lanes.
///
/// The whole point is the asymmetry between them:
///
/// - **Control is never dropped.** Commands, results, hello, goodbye and
///   heartbeats all change state on the far side; silently losing one would leave
///   the two peers disagreeing. If this lane fills, the peer is not keeping up
///   with its own control traffic and the connection is dropped instead, which is
///   recoverable.
/// - **Data drops the oldest.** A long outage must not grow memory without bound,
///   and because plugins send *cumulative* counters, the server can still compute
///   a correct rate across a gap. Fresh samples are worth more than stale ones.
///
/// Pushed from both sync sampler threads and async tasks, so the lock is a
/// `std::sync::Mutex` held for a few instructions and never across an await.
#[derive(Debug)]
pub(crate) struct PeerQueue {
    inner: Mutex<Inner>,
    ready: Notify,
    data_capacity: usize,
    control_capacity: usize,
    counters: SharedCounters,
}

#[derive(Debug)]
struct Inner {
    control: VecDeque<Frame>,
    data: VecDeque<Outgoing>,
    /// Set once nothing new may be queued. The writer keeps going until what is
    /// already queued has gone out.
    closed: bool,
    /// Set when the connection is gone; whatever is queued is lost.
    aborted: bool,
}

impl PeerQueue {
    pub(crate) fn new(
        data_capacity: usize,
        control_capacity: usize,
        counters: SharedCounters,
    ) -> Self {
        Self {
            inner: Mutex::new(Inner {
                control: VecDeque::new(),
                data: VecDeque::new(),
                closed: false,
                aborted: false,
            }),
            ready: Notify::new(),
            data_capacity,
            control_capacity,
            counters,
        }
    }

    /// Queue a control frame.
    ///
    /// Fails if the queue is closed, or if the control lane is full — which means
    /// the peer is not draining its own control traffic and the connection should
    /// be dropped.
    pub(crate) fn push_control(&self, frame: Frame) -> Result<()> {
        let mut inner = lock(&self.inner);
        if inner.closed || inner.aborted {
            return Err(closed_error());
        }
        if inner.control.len() >= self.control_capacity {
            return Err(Error::new(
                ErrorKind::Transport,
                format!(
                    "peer is not draining its control lane ({} frames queued)",
                    inner.control.len()
                ),
            ));
        }
        inner.control.push_back(frame);
        self.counters
            .control_queue_depth
            .fetch_add(1, Ordering::Relaxed);
        drop(inner);
        self.ready.notify_one();
        Ok(())
    }

    /// Queue service data, dropping the oldest if the lane is full.
    ///
    /// Only fails once the queue is closed: a full data lane is an expected
    /// condition, not an error, and the drop is counted in
    /// [`EngineStats::data_dropped`](cs_api::EngineStats::data_dropped).
    pub(crate) fn push_data(&self, service: ServiceId, payload: Bytes) -> Result<()> {
        let mut inner = lock(&self.inner);
        if inner.closed || inner.aborted {
            return Err(closed_error());
        }
        while inner.data.len() >= self.data_capacity {
            inner.data.pop_front();
            self.counters.data_dropped.fetch_add(1, Ordering::Relaxed);
            self.counters
                .data_queue_depth
                .fetch_sub(1, Ordering::Relaxed);
        }
        inner.data.push_back(Outgoing::Data { service, payload });
        self.counters
            .data_queue_depth
            .fetch_add(1, Ordering::Relaxed);
        drop(inner);
        self.ready.notify_one();
        Ok(())
    }

    /// Put a control frame at the *front* of its lane.
    ///
    /// For `Hello`, which has to be the first thing a new connection carries even
    /// though batches may already be queued behind it from before the reconnect.
    pub(crate) fn push_front_control(&self, frame: Frame) -> Result<()> {
        let mut inner = lock(&self.inner);
        if inner.closed || inner.aborted {
            return Err(closed_error());
        }
        inner.control.push_front(frame);
        self.counters
            .control_queue_depth
            .fetch_add(1, Ordering::Relaxed);
        drop(inner);
        self.ready.notify_one();
        Ok(())
    }

    /// Throw away queued control frames, keeping the data.
    ///
    /// What a reconnect needs: a command result for a connection that no longer
    /// exists can never be delivered, but the buffered metrics behind it are still
    /// worth sending — that buffer is the whole point of surviving an outage.
    pub(crate) fn drop_control(&self) {
        let mut inner = lock(&self.inner);
        let dropped = inner.control.len();
        inner.control.clear();
        self.counters
            .control_queue_depth
            .fetch_sub(dropped as u64, Ordering::Relaxed);
    }

    /// Take the next thing to send, control lane first, without waiting.
    pub(crate) fn take(&self) -> Option<Outgoing> {
        let mut inner = lock(&self.inner);
        if inner.aborted {
            return None;
        }
        if let Some(frame) = inner.control.pop_front() {
            self.counters
                .control_queue_depth
                .fetch_sub(1, Ordering::Relaxed);
            return Some(Outgoing::Control(frame));
        }
        let data = inner.data.pop_front();
        if data.is_some() {
            self.counters
                .data_queue_depth
                .fetch_sub(1, Ordering::Relaxed);
        }
        data
    }

    /// Wait for the next thing to send.
    ///
    /// `None` means the queue is closed and empty, which is the writer's signal to
    /// send `Goodbye` and finish, or aborted, which is its signal to stop at once.
    pub(crate) async fn next(&self) -> Option<Outgoing> {
        loop {
            // Registered *before* looking, so a push between the look and the wait
            // cannot be missed.
            let mut ready = std::pin::pin!(self.ready.notified());
            ready.as_mut().enable();

            if let Some(item) = self.take() {
                return Some(item);
            }
            {
                let inner = lock(&self.inner);
                if inner.aborted
                    || (inner.closed && inner.control.is_empty() && inner.data.is_empty())
                {
                    return None;
                }
            }
            ready.await;
        }
    }

    /// Stop accepting new work; let the writer finish what is queued.
    pub(crate) fn close(&self) {
        lock(&self.inner).closed = true;
        self.ready.notify_waiters();
        self.ready.notify_one();
    }

    /// Throw away everything queued — the connection is gone.
    pub(crate) fn abort(&self) {
        let mut inner = lock(&self.inner);
        inner.aborted = true;
        inner.closed = true;
        self.counters
            .control_queue_depth
            .fetch_sub(inner.control.len() as u64, Ordering::Relaxed);
        self.counters
            .data_queue_depth
            .fetch_sub(inner.data.len() as u64, Ordering::Relaxed);
        inner.control.clear();
        inner.data.clear();
        drop(inner);
        self.ready.notify_waiters();
        self.ready.notify_one();
    }

    /// Frames queued on each lane, as (control, data).
    pub(crate) fn depths(&self) -> (usize, usize) {
        let inner = lock(&self.inner);
        (inner.control.len(), inner.data.len())
    }

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        lock(&self.inner).closed
    }
}

#[track_caller]
fn closed_error() -> Error {
    Error::new(
        ErrorKind::Shutdown,
        "the engine is no longer accepting frames for this peer",
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::Counters;
    use cs_transport::{Goodbye, Heartbeat, Hello, Lane};
    use std::sync::Arc;
    use std::time::SystemTime;

    fn queue(data: usize, control: usize) -> (PeerQueue, SharedCounters) {
        let counters: SharedCounters = Arc::new(Counters::default());
        (
            PeerQueue::new(data, control, Arc::clone(&counters)),
            counters,
        )
    }

    fn service() -> ServiceId {
        struct Cpu;
        impl cs_api::ServiceDef for Cpu {
            const NAME: &'static str = "cpu";
            type Data = u64;
            type Command = cs_api::NoCommand;
        }
        ServiceId::of::<Cpu>()
    }

    fn heartbeat() -> Frame {
        Frame::Heartbeat(Heartbeat::at(SystemTime::UNIX_EPOCH))
    }

    #[test]
    fn control_frames_go_before_data_however_they_were_queued() {
        let (queue, _) = queue(8, 8);
        queue
            .push_data(service(), Bytes::from_static(b"first"))
            .unwrap();
        queue
            .push_data(service(), Bytes::from_static(b"second"))
            .unwrap();
        queue.push_control(heartbeat()).unwrap();

        // The control frame was queued last and comes out first.
        assert_eq!(queue.take().expect("item").lane(), Lane::Control);
        assert_eq!(queue.take().expect("item").lane(), Lane::Data);
        assert_eq!(queue.take().expect("item").lane(), Lane::Data);
        assert!(queue.take().is_none());
    }

    #[test]
    fn data_keeps_its_order_among_itself() {
        let (queue, _) = queue(8, 8);
        for i in 0..4u8 {
            queue.push_data(service(), Bytes::from(vec![i])).unwrap();
        }
        for i in 0..4u8 {
            let Some(Outgoing::Data { payload, .. }) = queue.take() else {
                panic!("expected data");
            };
            assert_eq!(payload, Bytes::from(vec![i]));
        }
    }

    #[test]
    fn a_full_data_lane_drops_the_oldest_and_counts_it() {
        let (queue, counters) = queue(3, 8);
        for i in 0..5u8 {
            queue.push_data(service(), Bytes::from(vec![i])).unwrap();
        }
        assert_eq!(queue.depths(), (0, 3), "never grows past its capacity");
        assert_eq!(counters.snapshot().data_dropped, 2);

        // What survives is the newest, because stale counters are worth less.
        let mut kept = Vec::new();
        while let Some(Outgoing::Data { payload, .. }) = queue.take() {
            kept.push(payload[0]);
        }
        assert_eq!(kept, [2, 3, 4]);
    }

    #[test]
    fn a_full_control_lane_is_an_error_rather_than_a_silent_loss() {
        let (queue, _) = queue(8, 2);
        queue.push_control(heartbeat()).unwrap();
        queue.push_control(heartbeat()).unwrap();

        let err = queue.push_control(heartbeat()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Transport);
        assert!(err.to_string().contains("not draining its control lane"));
        assert_eq!(queue.depths(), (2, 0), "nothing was dropped");
    }

    #[test]
    fn queue_depths_are_reported_engine_wide() {
        let (queue, counters) = queue(8, 8);
        queue.push_control(heartbeat()).unwrap();
        queue.push_data(service(), Bytes::new()).unwrap();
        queue.push_data(service(), Bytes::new()).unwrap();

        let stats = counters.snapshot();
        assert_eq!(stats.control_queue_depth, 1);
        assert_eq!(stats.data_queue_depth, 2);

        queue.take();
        queue.take();
        let stats = counters.snapshot();
        assert_eq!(stats.control_queue_depth, 0);
        assert_eq!(stats.data_queue_depth, 1);
    }

    #[tokio::test]
    async fn next_waits_and_is_woken_by_a_push() {
        let (queue, _) = queue(8, 8);
        let queue = Arc::new(queue);

        let waiter = {
            let queue = Arc::clone(&queue);
            tokio::spawn(async move { queue.next().await.map(|item| item.lane()) })
        };
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!waiter.is_finished(), "should be waiting on an empty queue");

        queue.push_control(heartbeat()).unwrap();
        assert_eq!(waiter.await.expect("task"), Some(Lane::Control));
    }

    #[tokio::test]
    async fn a_push_racing_the_wait_is_never_missed() {
        // The registration race: notify before the waiter has started waiting.
        for _ in 0..200 {
            let (queue, _) = queue(8, 8);
            let queue = Arc::new(queue);
            let pusher = {
                let queue = Arc::clone(&queue);
                tokio::spawn(async move { queue.push_control(heartbeat()) })
            };
            assert!(queue.next().await.is_some(), "the waiter hung");
            pusher.await.expect("task").expect("push");
        }
    }

    #[tokio::test]
    async fn closing_lets_the_writer_finish_what_is_queued() {
        let (queue, _) = queue(8, 8);
        queue
            .push_data(service(), Bytes::from_static(b"last"))
            .unwrap();
        queue
            .push_control(Frame::Goodbye(Goodbye::shutdown("done")))
            .unwrap();
        queue.close();

        assert!(queue.is_closed());
        assert_eq!(
            queue.push_data(service(), Bytes::new()).unwrap_err().kind(),
            ErrorKind::Shutdown,
            "nothing new is accepted"
        );

        // But what was already queued still goes out, in lane order.
        assert_eq!(queue.next().await.expect("item").lane(), Lane::Control);
        assert_eq!(queue.next().await.expect("item").lane(), Lane::Data);
        assert!(queue.next().await.is_none(), "then the writer is done");
    }

    #[tokio::test]
    async fn aborting_throws_away_what_was_in_flight() {
        let (queue, counters) = queue(8, 8);
        queue.push_control(heartbeat()).unwrap();
        queue.push_data(service(), Bytes::new()).unwrap();
        queue.abort();

        assert!(queue.next().await.is_none());
        assert_eq!(queue.depths(), (0, 0));
        let stats = counters.snapshot();
        assert_eq!(stats.control_queue_depth, 0, "depths are corrected");
        assert_eq!(stats.data_queue_depth, 0);
    }

    #[tokio::test]
    async fn a_waiting_writer_is_woken_by_a_close() {
        let (queue, _) = queue(8, 8);
        let queue = Arc::new(queue);
        let waiter = {
            let queue = Arc::clone(&queue);
            tokio::spawn(async move { queue.next().await })
        };
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        queue.close();
        assert!(waiter.await.expect("task").is_none());
    }

    #[tokio::test]
    async fn a_waiting_writer_is_woken_by_an_abort() {
        let (queue, _) = queue(8, 8);
        let queue = Arc::new(queue);
        let waiter = {
            let queue = Arc::clone(&queue);
            tokio::spawn(async move { queue.next().await })
        };
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        queue.abort();
        assert!(waiter.await.expect("task").is_none());
    }

    #[test]
    fn hello_can_jump_the_control_queue() {
        let (queue, _) = queue(8, 8);
        queue.push_control(heartbeat()).unwrap();
        queue
            .push_front_control(Frame::Hello(Hello::new("node-1")))
            .unwrap();

        let Some(Outgoing::Control(Frame::Hello(_))) = queue.take() else {
            panic!("hello should go first on a new connection");
        };
    }

    #[test]
    fn dropping_control_keeps_the_buffered_data() {
        let (queue, counters) = queue(8, 8);
        queue.push_control(heartbeat()).unwrap();
        queue.push_control(heartbeat()).unwrap();
        queue
            .push_data(service(), Bytes::from_static(b"batch"))
            .unwrap();

        queue.drop_control();
        assert_eq!(queue.depths(), (0, 1), "the outage buffer survives");
        assert_eq!(counters.snapshot().control_queue_depth, 0);

        let Some(Outgoing::Data { payload, .. }) = queue.take() else {
            panic!("expected the data to still be there");
        };
        assert_eq!(payload, Bytes::from_static(b"batch"));
    }

    #[test]
    fn hello_is_a_control_frame_like_any_other() {
        let (queue, _) = queue(8, 8);
        queue
            .push_control(Frame::Hello(Hello::new("node-1")))
            .unwrap();
        assert_eq!(queue.depths(), (1, 0));
    }
}
