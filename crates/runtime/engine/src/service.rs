use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use cs_api::runtime::{DataSink, ServiceRuntime, WorkerHost};
use cs_api::{
    Command, Data, Handler, JobInfo, Origin, Outbox, Reply, Sampler, SamplerFactory, ServiceBound,
    ServiceCtx, ServiceDef, ServiceId, Wire,
};
use cs_async_util::{BoxFuture, Clock, ShutdownSignal};
use cs_transport::{ErrorTrace, Outcome};
use cs_util::{Error, ErrorKind, Result};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::config::Backoff;
use crate::stats::SharedCounters;
use crate::worker::{NamedWorker, sampler_thread_name};

/// The engine's current view of which jobs are on this node.
///
/// Refreshed on a timer and shared by every sampler, so the cgroup hierarchy is
/// walked once per tick however many plugins are loaded, and all of them agree
/// about what exists.
#[derive(Clone, Debug, Default)]
pub(crate) struct Jobs {
    inner: Arc<Mutex<Arc<Vec<JobInfo>>>>,
}

impl Jobs {
    pub(crate) fn snapshot(&self) -> Arc<Vec<JobInfo>> {
        Arc::clone(&lock(&self.inner))
    }

    pub(crate) fn replace(&self, jobs: Vec<JobInfo>) {
        *lock(&self.inner) = Arc::new(jobs);
    }
}

/// What the engine hands a service when it starts it.
pub(crate) struct ServiceDeps {
    pub(crate) sink: Arc<dyn DataSink>,
    pub(crate) host: Arc<dyn WorkerHost>,
    pub(crate) runtime: Arc<dyn ServiceRuntime>,
    pub(crate) shutdown: ShutdownSignal,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) counters: SharedCounters,
    pub(crate) jobs: Jobs,
    pub(crate) restart_backoff: Backoff,
}

impl ServiceDeps {
    fn ctx<S: ServiceDef>(&self) -> ServiceCtx<S> {
        ServiceCtx::new(
            Arc::clone(&self.sink),
            Arc::clone(&self.host),
            self.shutdown.clone(),
            Arc::clone(&self.runtime),
        )
    }
}

/// What the engine asks a running service to do.
pub(crate) enum ServiceMessage {
    /// "Would you accept this?" — delivers the command to
    /// [`CommandReceiver::on_command`](cs_api::CommandReceiver::on_command) and
    /// reports what it said, **without** the engine acting on it yet.
    ///
    /// The two steps exist for agent-wide commands: every service votes before any
    /// of them is torn down, so one service's refusal cannot leave the others
    /// already stopped.
    Ask {
        kind: AskKind,
        reply: oneshot::Sender<Asked>,
    },
    /// "Now do it." The engine-side half of a built-in: run `on_shutdown`, then
    /// stop or rebuild.
    Apply {
        builtin: Builtin,
        done: oneshot::Sender<()>,
    },
}

/// A command as it reaches a service, with any custom payload still encoded.
#[derive(Clone, Debug)]
pub(crate) enum AskKind {
    Builtin(Builtin),
    Custom(Bytes),
}

/// The two commands the engine acts on itself.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Builtin {
    Shutdown,
    Restart,
}

/// What a service said about a command.
#[derive(Clone, Debug)]
pub(crate) struct Asked {
    /// What to report to whoever issued the command.
    pub(crate) outcome: Outcome,
    /// Whether the engine still has to do its half.
    ///
    /// True only for a built-in the service answered with
    /// [`Reply::Default`]. [`Reply::Handled`] means the service dealt with it
    /// itself, so the engine must not also stop or rebuild it.
    pub(crate) apply: bool,
}

impl Asked {
    fn report(outcome: Outcome) -> Self {
        Self {
            outcome,
            apply: false,
        }
    }
}

/// A running service, as the rest of the engine sees it.
#[derive(Clone)]
pub(crate) struct ServiceHandle {
    id: ServiceId,
    to_service: mpsc::Sender<ServiceMessage>,
}

impl ServiceHandle {
    pub(crate) fn id(&self) -> ServiceId {
        self.id
    }

    /// Deliver a command and wait for the service's answer.
    ///
    /// A service that has already stopped answers [`Outcome::UnknownService`]: to
    /// an operator, a service that is gone is indistinguishable from one that was
    /// never there.
    pub(crate) async fn ask(&self, kind: AskKind) -> Asked {
        let (reply, answer) = oneshot::channel();
        if self
            .to_service
            .send(ServiceMessage::Ask { kind, reply })
            .await
            .is_err()
        {
            return Asked::report(Outcome::UnknownService);
        }
        answer
            .await
            .unwrap_or_else(|_| Asked::report(Outcome::UnknownService))
    }

    /// Tell the service to carry out a built-in it already consented to.
    pub(crate) async fn apply(&self, builtin: Builtin) {
        let (done, finished) = oneshot::channel();
        if self
            .to_service
            .send(ServiceMessage::Apply { builtin, done })
            .await
            .is_ok()
        {
            let _ = finished.await;
        }
    }
}

/// A sampler registered but not yet started.
///
/// This closure is the erasure point: it captures the plugin's concrete types and
/// returns an opaque handle plus a future to spawn, so nothing downstream of
/// registration is generic over the plugin.
pub(crate) type SamplerRegistration =
    Box<dyn FnOnce(&ServiceDeps) -> (ServiceHandle, BoxFuture<'static, ()>) + Send>;

/// Build the registration for one sampler factory.
pub(crate) fn register_sampler<F>(factory: F) -> (ServiceId, SamplerRegistration)
where
    F: SamplerFactory,
{
    let id = ServiceId::of::<<F::Sampler as ServiceBound>::Service>();
    let registration: SamplerRegistration = Box::new(move |deps: &ServiceDeps| {
        let (to_service, from_engine) = mpsc::channel(4);
        let driver = SamplerDriver {
            factory,
            shared: Shared {
                id,
                ctx: deps.ctx(),
                outbox: Outbox::new(Arc::clone(&deps.sink)),
                clock: Arc::clone(&deps.clock),
                counters: Arc::clone(&deps.counters),
                jobs: deps.jobs.clone(),
            },
            from_engine,
            shutdown: deps.shutdown.clone(),
            restart_backoff: deps.restart_backoff,
        };
        (
            ServiceHandle { id, to_service },
            Box::pin(driver.run()) as BoxFuture<'static, ()>,
        )
    });
    (id, registration)
}

/// Everything a driver needs that is **not** the factory.
///
/// Split out for one reason: the driver's helpers await while borrowing this, and
/// a shared borrow of the factory would force `F: Sync` on every plugin author.
/// Keeping the factory behind `&mut self` and everything else behind `&Shared`
/// leaves the driver future `Send` with no extra bound.
struct Shared<S: ServiceDef> {
    id: ServiceId,
    ctx: ServiceCtx<S>,
    outbox: Outbox<S>,
    clock: Arc<dyn Clock>,
    counters: SharedCounters,
    jobs: Jobs,
}

impl<S: ServiceDef> Shared<S> {
    /// One `sample` call, with its data encoded and queued from the sampler's own
    /// thread.
    async fn sample_once<P>(&self, worker: &NamedWorker, sampler: P) -> (Option<P>, Duration)
    where
        P: Sampler + ServiceBound<Service = S>,
    {
        let service = self.id.name;
        let jobs = self.jobs.snapshot();
        let outbox = self.outbox.clone();

        let began = self.clock.now();
        let sampled = worker
            .run(move || {
                let mut sampler = sampler;
                // Encoding happens here too: it is synchronous work, and this is
                // the thread that is allowed to block.
                let outcome = sampler.sample(&jobs).map(|messages| {
                    let produced = messages.len();
                    let mut queued = 0;
                    for message in messages {
                        match outbox.send(message) {
                            Ok(()) => queued += 1,
                            Err(err) => warn!(service, error = ?err, "cannot queue a sample"),
                        }
                    }
                    (produced, queued)
                });
                let interval = sampler.interval();
                (sampler, outcome, interval)
            })
            .await;
        let took = self.clock.now().saturating_duration_since(began);

        match sampled {
            Ok((sampler, outcome, interval)) => {
                self.counters.sampled(service, took, outcome.is_err());
                match outcome {
                    Ok((produced, queued)) => {
                        for _ in 0..queued {
                            self.counters.message_sent(service);
                        }
                        debug!(service, produced, queued, ?took, "sampled");
                    }
                    // The whole chain, so an operator sees where it failed.
                    Err(err) => warn!(service, error = ?err, "sample failed"),
                }
                (Some(sampler), interval)
            }
            Err(err) => {
                self.counters.panicked(service);
                error!(service, error = ?err, "sampler panicked; rebuilding it");
                (None, Duration::ZERO)
            }
        }
    }

    /// Emit whatever the sampler has left. Its instance is consumed either way.
    async fn flush<P>(&self, worker: &NamedWorker, sampler: P)
    where
        P: Sampler + ServiceBound<Service = S>,
    {
        let service = self.id.name;
        let jobs = self.jobs.snapshot();
        let outbox = self.outbox.clone();

        let flushed = worker
            .run(move || {
                let mut sampler = sampler;
                sampler.on_shutdown(&jobs).map(|messages| {
                    messages
                        .into_iter()
                        .filter_map(|message| outbox.send(message).ok())
                        .count()
                })
            })
            .await;

        match flushed {
            Ok(Ok(queued)) => {
                for _ in 0..queued {
                    self.counters.message_sent(service);
                }
                debug!(service, queued, "flushed on shutdown");
            }
            Ok(Err(err)) => warn!(service, error = ?err, "on_shutdown failed"),
            Err(err) => {
                self.counters.panicked(service);
                error!(service, error = ?err, "sampler panicked during shutdown");
            }
        }
    }

    /// Put a command to the sampler and turn its [`Reply`] into a wire outcome.
    async fn ask<P>(&self, worker: &NamedWorker, sampler: P, kind: AskKind) -> (Option<P>, Asked)
    where
        P: Sampler + ServiceBound<Service = S>,
    {
        let service = self.id.name;
        let command = match kind {
            AskKind::Builtin(Builtin::Shutdown) => Command::Shutdown,
            AskKind::Builtin(Builtin::Restart) => Command::Restart,
            AskKind::Custom(bytes) => match <S::Command as Wire>::decode(bytes) {
                Ok(custom) => Command::Custom(custom),
                Err(err) => {
                    // The service's own command type could not be read: the sender
                    // is wrong, and the operator needs to see exactly why.
                    let err = err.context(format!("decoding a {service} command"));
                    warn!(service, error = ?err, "undecodable command");
                    return (
                        Some(sampler),
                        Asked::report(Outcome::Failed(ErrorTrace::from_error(
                            self.ctx.node(),
                            &err,
                        ))),
                    );
                }
            },
        };
        let is_builtin = command.is_builtin();

        let asked = worker
            .run(move || {
                let mut sampler = sampler;
                let reply = sampler.on_command(command);
                (sampler, reply)
            })
            .await;

        let (sampler, reply) = match asked {
            Ok(pair) => pair,
            Err(err) => {
                self.counters.panicked(service);
                error!(service, error = ?err, "sampler panicked handling a command");
                let err = Error::new(
                    ErrorKind::Plugin,
                    format!("{service} panicked while handling a command"),
                );
                return (
                    None,
                    Asked::report(Outcome::Failed(ErrorTrace::from_error(
                        self.ctx.node(),
                        &err,
                    ))),
                );
            }
        };

        // The `Reply` table, in one place:
        //
        // | command  | Default            | Handled           | Rejected         |
        // | shutdown | engine stops it    | it stopped itself | keeps running    |
        // | restart  | engine rebuilds it | it reset itself   | keeps running    |
        // | custom   | reply Unsupported  | reply Ok          | reply the reason |
        let asked = match (reply, is_builtin) {
            (Reply::Rejected(why), _) => Asked::report(Outcome::Rejected(why)),
            (Reply::Default, true) => Asked {
                outcome: Outcome::Ok,
                apply: true,
            },
            (Reply::Handled, true | false) => Asked::report(Outcome::Ok),
            (Reply::Default, false) => Asked::report(Outcome::Unsupported),
        };
        (Some(sampler), asked)
    }
}

/// Drives one sampler: its interval, its commands, its panics, its shutdown.
///
/// The factory stays here and never travels to the worker thread. That matters: if
/// it were moved into a job that panicked it would be lost, and the service could
/// never be rebuilt — which is the one thing panic recovery needs. So `build` runs
/// here under `catch_unwind`, and everything else, where a panic only costs the
/// instance, runs on the worker thread.
struct SamplerDriver<F: SamplerFactory> {
    factory: F,
    shared: Shared<<F::Sampler as ServiceBound>::Service>,
    from_engine: mpsc::Receiver<ServiceMessage>,
    shutdown: ShutdownSignal,
    restart_backoff: Backoff,
}

/// Why one life of a sampler ended.
enum Stopped {
    /// Stop for good.
    Done,
    /// Build a fresh instance and carry on.
    Rebuild,
    /// As `Rebuild`, but because it crashed — so back off first.
    Crashed,
}

impl<F: SamplerFactory> SamplerDriver<F> {
    /// The service's whole life: build, run, rebuild on panic, stop on command or
    /// shutdown.
    async fn run(mut self) {
        let mut consecutive_crashes = 0u32;

        loop {
            let delay = self.restart_backoff.delay(consecutive_crashes);
            if !delay.is_zero() {
                debug!(
                    service = self.shared.id.name,
                    ?delay,
                    "waiting before rebuilding"
                );
                let sleep = self.shared.clock.sleep(delay);
                tokio::select! {
                    biased;
                    () = self.shutdown.wait() => return,
                    () = sleep => {}
                }
            }

            match self.attempt().await {
                Stopped::Done => return,
                Stopped::Rebuild => consecutive_crashes = 0,
                Stopped::Crashed => consecutive_crashes = consecutive_crashes.saturating_add(1),
            }
            if self.shutdown.is_set() {
                return;
            }
        }
    }

    /// One life of one sampler instance.
    async fn attempt(&mut self) -> Stopped {
        let service = self.shared.id.name;
        let thread_name = sampler_thread_name(self.shared.id);
        let worker = match NamedWorker::spawn(&thread_name) {
            Ok(worker) => worker,
            Err(err) => {
                error!(service, error = ?err, "cannot start sampler thread");
                return Stopped::Done;
            }
        };

        // The factory is trivial by contract, so building here rather than on the
        // worker thread costs nothing — and keeps the factory safe from a panic.
        let built = std::panic::catch_unwind(AssertUnwindSafe(|| self.factory.build()));
        let sampler = match built {
            Ok(sampler) => sampler,
            Err(_) => {
                self.shared.counters.panicked(service);
                error!(service, "sampler factory panicked");
                return Stopped::Crashed;
            }
        };

        let ctx = self.shared.ctx.clone();
        let started = worker
            .run(move || {
                let mut sampler = sampler;
                let result = sampler.start(&ctx);
                (sampler, result)
            })
            .await;
        let mut sampler = match started {
            Ok((sampler, Ok(()))) => sampler,
            Ok((_, Err(err))) => {
                // A service that cannot start is not a crash worth retrying: NVML
                // being absent will not change while the agent runs.
                error!(service, error = ?err, "service refused to start");
                return Stopped::Done;
            }
            Err(err) => {
                self.shared.counters.panicked(service);
                error!(service, error = ?err, "sampler panicked while starting");
                return Stopped::Crashed;
            }
        };
        info!(service, thread = %thread_name, "service started");

        loop {
            let (survivor, interval) = self.shared.sample_once(&worker, sampler).await;
            let Some(next) = survivor else {
                return Stopped::Crashed;
            };
            sampler = next;

            let deadline = self.shared.clock.now() + interval;
            loop {
                let tick = self.shared.clock.sleep_until(deadline);
                tokio::select! {
                    biased;
                    () = self.shutdown.wait() => {
                        self.shared.flush(&worker, sampler).await;
                        return Stopped::Done;
                    }
                    message = self.from_engine.recv() => {
                        // `None` means the engine dropped our handle: no command
                        // will ever arrive again, but sampling carries on.
                        let Some(message) = message else { continue };
                        match self.handle_message(&worker, sampler, message).await {
                            Handled::Alive(next) => {
                                sampler = next;
                                continue;
                            }
                            Handled::Finished(stopped) => return stopped,
                        }
                    }
                    () = tick => break,
                }
            }
        }
    }

    /// Answer one engine message.
    async fn handle_message(
        &mut self,
        worker: &NamedWorker,
        sampler: F::Sampler,
        message: ServiceMessage,
    ) -> Handled<F::Sampler> {
        match message {
            ServiceMessage::Ask { kind, reply } => {
                let (survivor, asked) = self.shared.ask(worker, sampler, kind).await;
                let _ = reply.send(asked);
                match survivor {
                    Some(sampler) => Handled::Alive(sampler),
                    None => Handled::Finished(Stopped::Crashed),
                }
            }
            ServiceMessage::Apply { builtin, done } => {
                self.shared.flush(worker, sampler).await;
                let _ = done.send(());
                match builtin {
                    Builtin::Shutdown => {
                        info!(service = self.shared.id.name, "stopped by command");
                        Handled::Finished(Stopped::Done)
                    }
                    Builtin::Restart => {
                        self.shared.counters.restarted(self.shared.id.name);
                        info!(service = self.shared.id.name, "restarting by command");
                        Handled::Finished(Stopped::Rebuild)
                    }
                }
            }
        }
    }
}

/// What became of a sampler after handling a message.
enum Handled<S> {
    Alive(S),
    Finished(Stopped),
}

/// A registered handler, with its type erased.
///
/// [`Handler::handle`] returns an opaque future, so the trait is not
/// dyn-compatible. This is the one place that future is boxed, and also where the
/// payload is decoded — which keeps decoding off the reader task and lets a
/// malformed message fail one message rather than a connection.
pub(crate) trait ErasedHandler: Send + Sync + 'static {
    fn id(&self) -> ServiceId;

    /// Decode and handle one message from `from`.
    ///
    /// The origin is owned rather than borrowed because the future outlives the
    /// frame it came from: the engine spawns it and moves on to the next read.
    fn handle(self: Arc<Self>, from: Sender, payload: Bytes) -> BoxFuture<'static, Result<()>>;

    /// Release resources, during server shutdown.
    fn shutdown(self: Arc<Self>) -> BoxFuture<'static, ()>;
}

struct HandlerEntry<H: Handler> {
    handler: H,
    ctx: ServiceCtx<H::Service>,
}

impl<H: Handler> ErasedHandler for HandlerEntry<H> {
    fn id(&self) -> ServiceId {
        ServiceId::of::<H::Service>()
    }

    fn handle(self: Arc<Self>, from: Sender, payload: Bytes) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let message = <Data<H> as Wire>::decode(payload).map_err(|err| {
                err.context(format!(
                    "decoding {} data from {}",
                    H::Service::NAME,
                    from.node
                ))
            })?;
            let origin = Origin {
                node: &from.node,
                service_version: from.service_version,
            };
            self.handler.handle(&self.ctx, origin, message).await
        })
    }

    fn shutdown(self: Arc<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move { self.handler.shutdown().await })
    }
}

/// An owned [`Origin`], for a handler future that outlives the frame.
#[derive(Clone, Debug)]
pub(crate) struct Sender {
    pub(crate) node: String,
    pub(crate) service_version: u32,
}

/// How a handler is built once the engine's internals exist.
pub(crate) type HandlerRegistration =
    Box<dyn FnOnce(&ServiceDeps) -> Arc<dyn ErasedHandler> + Send>;

/// Build the registration for one handler.
pub(crate) fn register_handler<H: Handler>(handler: H) -> (ServiceId, HandlerRegistration) {
    let id = ServiceId::of::<H::Service>();
    let registration: HandlerRegistration = Box::new(move |deps: &ServiceDeps| {
        Arc::new(HandlerEntry {
            handler,
            ctx: deps.ctx(),
        }) as Arc<dyn ErasedHandler>
    });
    (id, registration)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
