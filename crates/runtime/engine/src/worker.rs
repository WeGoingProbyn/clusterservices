use std::sync::mpsc;
use std::thread;

use cs_api::runtime::WorkerHost;
use cs_api::{ServiceId, worker_thread_name};
use cs_util::{Error, ErrorKind, Result};
use tokio::sync::oneshot;

/// A dedicated OS thread that runs closures, one at a time, under a name of our
/// choosing.
///
/// Why not `spawn_blocking`? Two requirements pull in opposite directions:
///
/// - Sampling must happen on a **named** thread, because per-plugin CPU time is
///   read back from `/proc/self/task/<tid>/{comm,stat}`. A pooled
///   `spawn_blocking` thread is called `tokio-runtime-worker` and is shared, so
///   that attribution is impossible.
/// - Interval timing must go through the [`Clock`](cs_async_util::Clock), so a
///   test can drive it — which means the *loop* has to be async, since a `Clock`
///   hands out futures.
///
/// So the loop lives in an async task and the work lives here: the task sends a
/// closure to this thread and awaits the result. The sampler itself travels into
/// each closure and back out again, which costs one channel round trip per sample
/// — nothing, at seconds-scale intervals — and means a panic simply loses the
/// instance, leaving nothing half-updated behind to rebuild around.
pub(crate) struct NamedWorker {
    name: String,
    jobs: Option<mpsc::Sender<Job>>,
    thread: Option<thread::JoinHandle<()>>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

impl NamedWorker {
    /// Start a worker thread called `name`, truncated to the kernel's limit.
    pub(crate) fn spawn(name: &str) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<Job>();
        let thread = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                // Ends when the sender is dropped, or when a job panics — in
                // which case the pending result sender drops too, and the caller
                // sees the panic.
                while let Ok(job) = rx.recv() {
                    job();
                }
            })
            .map_err(|e| {
                Error::with_source(
                    ErrorKind::Plugin,
                    format!("cannot start worker thread {name}"),
                    e,
                )
            })?;

        Ok(Self {
            name: name.to_owned(),
            jobs: Some(tx),
            thread: Some(thread),
        })
    }

    /// Run `job` on the worker thread and wait for its result.
    ///
    /// An `Err` here means the worker is gone — which in practice means `job`, or
    /// an earlier one, panicked. The caller is expected to treat that as a failed
    /// service rather than propagate it.
    pub(crate) async fn run<R: Send + 'static>(
        &self,
        job: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R> {
        let (done, wait) = oneshot::channel();
        let Some(jobs) = &self.jobs else {
            return Err(self.gone());
        };
        jobs.send(Box::new(move || {
            // The receiver going away means the caller stopped waiting; the work
            // was still done, so there is nothing to report.
            let _ = done.send(job());
        }))
        .map_err(|_| self.gone())?;

        wait.await.map_err(|_| self.gone())
    }

    #[track_caller]
    fn gone(&self) -> Error {
        Error::new(
            ErrorKind::Plugin,
            format!("worker thread {} stopped unexpectedly", self.name),
        )
    }
}

impl Drop for NamedWorker {
    fn drop(&mut self) {
        // Closing the channel ends the loop; then wait, so a thread never
        // outlives the engine that started it.
        self.jobs = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl std::fmt::Debug for NamedWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamedWorker")
            .field("name", &self.name)
            .finish()
    }
}

/// Starts the long-running threads a plugin asks for through
/// [`WorkerSpawner`](cs_api::WorkerSpawner).
///
/// Unlike [`NamedWorker`], these run one closure until it returns — a plugin's own
/// loop, which is expected to watch the
/// [`ShutdownSignal`](cs_async_util::ShutdownSignal) and return when it fires. The
/// engine joins them during shutdown, under the deadline.
pub(crate) struct Workers {
    threads: std::sync::Mutex<Vec<(String, thread::JoinHandle<()>)>>,
}

impl Workers {
    pub(crate) fn new() -> Self {
        Self {
            threads: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Join every plugin worker thread, in the order they were started.
    ///
    /// Blocking, so it runs inside `spawn_blocking` during shutdown. A worker
    /// that ignores the shutdown signal blocks here, which is what the shutdown
    /// deadline is for.
    pub(crate) fn join_all(&self) -> Vec<String> {
        let threads = std::mem::take(&mut *lock(&self.threads));
        let mut panicked = Vec::new();
        for (name, thread) in threads {
            if thread.join().is_err() {
                panicked.push(name);
            }
        }
        panicked
    }

    pub(crate) fn names(&self) -> Vec<String> {
        lock(&self.threads)
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }
}

impl WorkerHost for Workers {
    fn spawn_blocking(&self, name: &str, body: Box<dyn FnOnce() + Send + 'static>) -> Result<()> {
        let thread = thread::Builder::new()
            .name(name.to_owned())
            .spawn(body)
            .map_err(|e| {
                Error::with_source(
                    ErrorKind::Plugin,
                    format!("cannot start worker thread {name}"),
                    e,
                )
            })?;
        lock(&self.threads).push((name.to_owned(), thread));
        Ok(())
    }
}

/// The thread name for a service's own sampler loop.
pub(crate) fn sampler_thread_name(service: ServiceId) -> String {
    worker_thread_name(service.name, "sample")
}

fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cs_api::{NoCommand, ServiceDef};

    struct Cgroup;
    impl ServiceDef for Cgroup {
        const NAME: &'static str = "cgroup";
        type Data = u64;
        type Command = NoCommand;
    }

    #[tokio::test]
    async fn a_job_runs_on_the_worker_thread_and_returns_its_value() {
        let worker = NamedWorker::spawn("cgroup/sample").expect("spawn");
        let name = worker
            .run(|| thread::current().name().map(str::to_owned))
            .await
            .expect("run");
        assert_eq!(name.as_deref(), Some("cgroup/sample"));
        assert_eq!(worker.run(|| 2 + 2).await.expect("run"), 4);
    }

    #[tokio::test]
    async fn jobs_run_one_at_a_time_in_order() {
        let worker = NamedWorker::spawn("t/order").expect("spawn");
        let mut seen = Vec::new();
        for i in 0..5 {
            seen.push(worker.run(move || i).await.expect("run"));
        }
        assert_eq!(seen, [0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn state_can_travel_into_a_job_and_back_out() {
        // This is how a sampler reaches its own thread: moved in, moved out.
        let worker = NamedWorker::spawn("t/state").expect("spawn");
        let mut counter = 0u32;
        for _ in 0..3 {
            counter = worker
                .run(move || {
                    counter += 1;
                    counter
                })
                .await
                .expect("run");
        }
        assert_eq!(counter, 3);
    }

    #[tokio::test]
    async fn a_panicking_job_is_reported_and_does_not_poison_the_caller() {
        let worker = NamedWorker::spawn("t/panic").expect("spawn");
        let err = worker
            .run(|| panic!("sampler exploded"))
            .await
            .expect_err("a panic should surface as an error");
        assert_eq!(err.kind(), ErrorKind::Plugin);
        assert!(err.to_string().contains("stopped unexpectedly"));

        // The thread is gone, so later jobs fail too: the caller is expected to
        // rebuild the whole worker.
        assert!(worker.run(|| 1).await.is_err());
    }

    #[tokio::test]
    async fn dropping_a_worker_joins_its_thread() {
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker = NamedWorker::spawn("t/drop").expect("spawn");
        {
            let flag = std::sync::Arc::clone(&flag);
            worker
                .run(move || flag.store(true, std::sync::atomic::Ordering::SeqCst))
                .await
                .expect("run");
        }
        drop(worker);
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn sampler_threads_are_named_for_their_service_within_the_kernel_limit() {
        let name = sampler_thread_name(ServiceId::of::<Cgroup>());
        assert_eq!(name, "cgroup/sample");
        assert!(name.len() <= cs_api::MAX_THREAD_NAME_LEN);
    }

    #[test]
    fn plugin_workers_are_named_and_joined() {
        let workers = Workers::new();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        for i in 0..3 {
            let seen = std::sync::Arc::clone(&seen);
            workers
                .spawn_blocking(
                    &format!("svc/w{i}"),
                    Box::new(move || {
                        let name = thread::current().name().map(str::to_owned);
                        lock(&seen).push(name);
                    }),
                )
                .expect("spawn");
        }
        assert_eq!(workers.names(), ["svc/w0", "svc/w1", "svc/w2"]);
        assert!(workers.join_all().is_empty(), "none should have panicked");

        let mut names = lock(&seen).clone();
        names.sort();
        assert_eq!(
            names,
            [
                Some("svc/w0".to_owned()),
                Some("svc/w1".to_owned()),
                Some("svc/w2".to_owned())
            ]
        );
    }

    #[test]
    fn a_panicking_plugin_worker_is_named_in_the_join_result() {
        let workers = Workers::new();
        workers
            .spawn_blocking("svc/boom", Box::new(|| panic!("worker exploded")))
            .expect("spawn");
        workers
            .spawn_blocking("svc/fine", Box::new(|| {}))
            .expect("spawn");
        assert_eq!(workers.join_all(), ["svc/boom"]);
    }
}
