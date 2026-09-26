use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use cs_api::runtime::{
    CommandKind as ApiCommandKind, CommandRequest, DataSink, ServiceRuntime, WorkerHost,
};
use cs_api::{
    CommandOutcome, Encodable, EngineStats, Handler, JobSource, NoJobs, Sampler, SamplerFactory,
    ServiceBound, ServiceId,
};
use cs_async_util::{BoxFuture, Clock, CommandHandle, ShutdownSignal, command_channel};
use cs_transport::{
    Capabilities, CommandKind as WireCommandKind, Connection, Endpoint, Frame, Goodbye, Listener,
    Transport,
};
use cs_util::{Error, ErrorKind, Result, ResultExt};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::command::Commands;
use crate::config::EngineConfig;
use crate::peer::{
    Ended, LastWrite, Peer, Stop, StopRequest, Wiring, accept_hello, heartbeat_loop, our_hello,
    read_loop, write_loop,
};
use crate::queue::PeerQueue;
use crate::service::{
    ErasedHandler, HandlerRegistration, Jobs, SamplerRegistration, ServiceDeps, ServiceHandle,
    register_handler, register_sampler,
};
use crate::stats::{Counters, SharedCounters};
use crate::worker::{NamedWorker, Workers};

/// A [`Clock`] backed by the tokio runtime.
///
/// The only clock the agent and server use in production. Because it goes through
/// tokio's timer, a test with `start_paused = true` controls every deadline in the
/// engine just by advancing time.
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioClock;

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn sleep_until(&self, deadline: Instant) -> BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep_until(deadline.into()))
    }
}

/// Everything shared across the engine, with the transport factored out.
///
/// This is what implements the [`cs_api::runtime`] seam, which is why it is not
/// generic over `T`: a plugin's [`ServiceCtx`](cs_api::ServiceCtx) reaches the
/// engine through here without ever naming the transport.
pub(crate) struct Inner {
    config: Arc<EngineConfig>,
    clock: Arc<dyn Clock>,
    counters: SharedCounters,
    commands: Arc<Commands>,
    workers: Arc<Workers>,
    jobs: Jobs,
    /// The outbound queue to the upstream server.
    ///
    /// One per engine and **outliving every connection**, because that is what
    /// makes an outage survivable: samplers keep filling it while the agent is
    /// disconnected, and a reconnect finds the buffer intact.
    uplink: Option<Arc<PeerQueue>>,
    /// Connected peers by node name. An agent has at most one; a server has many.
    peers: Mutex<HashMap<String, Arc<Peer>>>,
    /// Filled in once the registrations have been run.
    ///
    /// A `OnceLock` rather than a field, because of a genuine circularity: a
    /// service's [`ServiceCtx`](cs_api::ServiceCtx) is built from this `Inner`, so
    /// `Inner` has to exist first — but the engine also has to be able to find a
    /// service by name. Building a second `Inner` around the finished tables would
    /// leave every plugin holding a context wired to the first one, whose peer
    /// table nothing ever updates.
    services: OnceLock<Arc<HashMap<&'static str, ServiceHandle>>>,
    handlers: OnceLock<Arc<HashMap<&'static str, Arc<dyn ErasedHandler>>>>,
}

impl Inner {
    fn peer(&self, node: &str) -> Option<Arc<Peer>> {
        lock(&self.peers).get(node).cloned()
    }

    fn services(&self) -> Arc<HashMap<&'static str, ServiceHandle>> {
        self.services
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(HashMap::new()))
    }

    fn handlers(&self) -> Arc<HashMap<&'static str, Arc<dyn ErasedHandler>>> {
        self.handlers
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(HashMap::new()))
    }
}

impl DataSink for Inner {
    fn send(&self, service: ServiceId, msg: &dyn Encodable) -> Result<()> {
        let Some(uplink) = &self.uplink else {
            return Err(Error::new(
                ErrorKind::Config,
                format!("{service} produced data but this engine has no upstream to send it to"),
            ));
        };
        // Encoded here, on the caller's thread — a sampler's own worker thread.
        let mut buffer = BytesMut::with_capacity(msg.encoded_len());
        msg.encode(&mut buffer);
        uplink.push_data(service, buffer.freeze())
    }
}

impl WorkerHost for Inner {
    fn spawn_blocking(&self, name: &str, body: Box<dyn FnOnce() + Send + 'static>) -> Result<()> {
        self.workers.spawn_blocking(name, body)
    }
}

impl ServiceRuntime for Inner {
    fn node(&self) -> &str {
        &self.config.node
    }

    fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    fn engine_stats(&self) -> EngineStats {
        self.counters.snapshot()
    }

    fn dispatch_command(&self, request: CommandRequest<'_>) -> CommandHandle<CommandOutcome> {
        let kind = match request.kind {
            ApiCommandKind::Shutdown => WireCommandKind::Shutdown,
            ApiCommandKind::Restart => WireCommandKind::Restart,
            ApiCommandKind::Custom(payload) => {
                let mut buffer = BytesMut::with_capacity(payload.encoded_len());
                payload.encode(&mut buffer);
                WireCommandKind::Custom(buffer.freeze())
            }
        };
        let service = if request.service.is_agent_wide() {
            ""
        } else {
            request.service.name
        };
        let peer = self.peer(request.node);

        // The node told us what it runs in its `Hello`, so a command for a service
        // it does not have is refused here rather than after a round trip.
        if let Some(connected) = &peer {
            if !service.is_empty() && !connected.runs(service) {
                let (answer, handle) = command_channel();
                answer.complete(CommandOutcome::UnknownService);
                return handle;
            }
        }

        let (handle, ready) = self.commands.dispatch(
            request.node,
            service,
            kind,
            request.opts.force,
            request.opts.expiry,
            self.clock.now(),
            peer.is_some(),
        );
        if let (Some(peer), Some(ready)) = (peer, ready) {
            if let Err(err) = peer.queue.push_control(ready.frame) {
                warn!(node = request.node, error = ?err, "cannot queue a command");
            }
        }
        handle
    }
}

/// Collects registrations, then builds an engine.
///
/// See [`NodeEngine::builder`].
pub struct EngineBuilder<T: Transport> {
    transport: T,
    config: EngineConfig,
    clock: Arc<dyn Clock>,
    job_source: Arc<dyn JobSource>,
    samplers: Vec<(ServiceId, SamplerRegistration)>,
    handlers: Vec<(ServiceId, HandlerRegistration)>,
    dial: Option<Endpoint>,
    listen: Option<Endpoint>,
}

impl<T: Transport> EngineBuilder<T> {
    /// Replace the whole configuration.
    #[must_use]
    pub fn config(mut self, config: EngineConfig) -> Self {
        self.config = config;
        self
    }

    /// Set this node's name.
    #[must_use]
    pub fn node(mut self, node: impl Into<String>) -> Self {
        self.config.node = node.into();
        self
    }

    /// Set the build identifier reported in `Hello`.
    #[must_use]
    pub fn build_id(mut self, build: impl Into<String>) -> Self {
        self.config.build = build.into();
        self
    }

    /// Use a different clock. Tests substitute one here; production uses
    /// [`TokioClock`].
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Where the job list comes from. Defaults to [`NoJobs`].
    #[must_use]
    pub fn jobs(mut self, source: impl JobSource) -> Self {
        self.job_source = Arc::new(source);
        self
    }

    /// Register a sampler, as a factory so it can be rebuilt after a `Restart` or
    /// a panic.
    ///
    /// ```ignore
    /// builder.sampler(|| CpuSampler::new())
    /// ```
    #[must_use]
    pub fn sampler<F>(mut self, factory: F) -> Self
    where
        F: SamplerFactory,
        F::Sampler: Sampler + ServiceBound,
    {
        self.samplers.push(register_sampler(factory));
        self
    }

    /// Register a handler for data arriving from agents.
    #[must_use]
    pub fn handler<H: Handler>(mut self, handler: H) -> Self {
        self.handlers.push(register_handler(handler));
        self
    }

    /// Dial this endpoint, and keep dialling it. The agent role.
    #[must_use]
    pub fn dial(mut self, endpoint: Endpoint) -> Self {
        self.dial = Some(endpoint);
        self
    }

    /// Accept connections here. The server role.
    ///
    /// An engine may do both, which is what a relay tier is.
    #[must_use]
    pub fn listen(mut self, endpoint: Endpoint) -> Self {
        self.listen = Some(endpoint);
        self
    }

    /// Check everything and build.
    ///
    /// Fails for anything that would otherwise go wrong much later and less
    /// clearly: no node name, two services with one name, a sampler with nowhere
    /// to send, or no endpoint at all.
    pub fn build(self) -> Result<NodeEngine<T>> {
        self.config.validate().context("engine configuration")?;

        let mut seen = HashMap::new();
        for id in self
            .samplers
            .iter()
            .map(|(id, _)| *id)
            .chain(self.handlers.iter().map(|(id, _)| *id))
        {
            if seen.insert(id.name, id).is_some() {
                return Err(Error::new(
                    ErrorKind::Config,
                    format!("service {:?} is registered twice", id.name),
                ));
            }
        }

        if self.dial.is_none() && self.listen.is_none() {
            return Err(Error::new(
                ErrorKind::Config,
                "an engine must dial an upstream, listen for agents, or both",
            ));
        }
        if !self.samplers.is_empty() && self.dial.is_none() {
            return Err(Error::new(
                ErrorKind::Config,
                "samplers produce data, so the engine needs an upstream to dial",
            ));
        }

        let counters: SharedCounters = Arc::new(Counters::default());
        for (id, _) in &self.samplers {
            counters.register_service(*id);
        }
        for (id, _) in &self.handlers {
            counters.register_service(*id);
        }

        let config = Arc::new(self.config);
        let capabilities = self.transport.capabilities();
        let uplink = self.dial.as_ref().map(|_| {
            Arc::new(PeerQueue::new(
                config.data_queue,
                config.control_queue,
                Arc::clone(&counters),
            ))
        });

        Ok(NodeEngine {
            transport: Arc::new(self.transport),
            listening: Arc::new(Mutex::new(None)),
            config: Arc::clone(&config),
            clock: self.clock,
            job_source: self.job_source,
            samplers: self.samplers,
            handlers: self.handlers,
            dial: self.dial,
            listen: self.listen,
            counters,
            uplink,
            capabilities,
            shutdown: ShutdownSignal::new(),
            stop: Arc::new(StopRequest::default()),
        })
    }
}

/// The engine, for either role or both.
///
/// One implementation serves the agent (dials one server) and the server (accepts
/// many agents); a relay is simply one that does both. Generic over the transport
/// and never boxed, so the choice of transport costs nothing at run time.
pub struct NodeEngine<T: Transport> {
    transport: Arc<T>,
    listening: Arc<Mutex<Option<Endpoint>>>,
    config: Arc<EngineConfig>,
    clock: Arc<dyn Clock>,
    job_source: Arc<dyn JobSource>,
    samplers: Vec<(ServiceId, SamplerRegistration)>,
    handlers: Vec<(ServiceId, HandlerRegistration)>,
    dial: Option<Endpoint>,
    listen: Option<Endpoint>,
    counters: SharedCounters,
    uplink: Option<Arc<PeerQueue>>,
    capabilities: Capabilities,
    shutdown: ShutdownSignal,
    stop: Arc<StopRequest>,
}

impl<T: Transport> NodeEngine<T> {
    /// Start building an engine on `transport`.
    #[must_use]
    pub fn builder(transport: T) -> EngineBuilder<T> {
        EngineBuilder {
            transport,
            config: EngineConfig::default(),
            clock: Arc::new(TokioClock),
            job_source: Arc::new(NoJobs),
            samplers: Vec::new(),
            handlers: Vec::new(),
            dial: None,
            listen: None,
        }
    }

    /// A handle for stopping the engine and reading its counters from outside.
    ///
    /// Take this before [`run`](NodeEngine::run), which consumes the engine.
    #[must_use]
    pub fn handle(&self) -> EngineHandle {
        EngineHandle {
            shutdown: self.shutdown.clone(),
            stop: Arc::clone(&self.stop),
            counters: Arc::clone(&self.counters),
            listening: Arc::clone(&self.listening),
        }
    }

    /// Run until something stops the engine, then shut down in order.
    ///
    /// Returns how it was stopped, which is what tells an agent whether to exit
    /// with the status that asks systemd to start it again.
    pub async fn run(mut self) -> Result<Stop> {
        let inner = Arc::new(Inner {
            config: Arc::clone(&self.config),
            clock: Arc::clone(&self.clock),
            counters: Arc::clone(&self.counters),
            commands: Arc::new(Commands::new(Arc::clone(&self.counters))),
            workers: Arc::new(Workers::new()),
            jobs: Jobs::default(),
            uplink: self.uplink.clone(),
            peers: Mutex::new(HashMap::new()),
            services: OnceLock::new(),
            handlers: OnceLock::new(),
        });

        let deps = ServiceDeps {
            sink: Arc::clone(&inner) as Arc<dyn DataSink>,
            host: Arc::clone(&inner) as Arc<dyn WorkerHost>,
            runtime: Arc::clone(&inner) as Arc<dyn ServiceRuntime>,
            shutdown: self.shutdown.clone(),
            clock: Arc::clone(&self.clock),
            counters: Arc::clone(&self.counters),
            jobs: inner.jobs.clone(),
            restart_backoff: self.config.restart_backoff,
        };

        let mut services = HashMap::new();
        let mut drivers = Vec::new();
        for (id, registration) in std::mem::take(&mut self.samplers) {
            let (handle, driver) = registration(&deps);
            services.insert(id.name, handle);
            drivers.push(driver);
        }
        let mut handlers: HashMap<&'static str, Arc<dyn ErasedHandler>> = HashMap::new();
        for (id, registration) in std::mem::take(&mut self.handlers) {
            handlers.insert(id.name, registration(&deps));
        }
        // Every context handed out above points at this same `Inner`, so a handler
        // that dispatches a command finds the peer table the connections update.
        let _ = inner.services.set(Arc::new(services));
        let _ = inner.handlers.set(Arc::new(handlers));

        let wiring = Wiring {
            config: Arc::clone(&self.config),
            counters: Arc::clone(&self.counters),
            commands: Arc::clone(&inner.commands),
            clock: Arc::clone(&self.clock),
            shutdown: self.shutdown.clone(),
            services: inner.services(),
            handlers: inner.handlers(),
            stop: Arc::clone(&self.stop),
            max_frame: self.capabilities.max_frame,
        };

        // Two sets, because shutdown has to wait for them in order: the services
        // must finish flushing *before* the queues are closed, and the connections
        // cannot finish until after — a single set would deadlock until the
        // deadline.
        let mut service_tasks: JoinSet<()> = JoinSet::new();
        let mut link_tasks: JoinSet<()> = JoinSet::new();
        for driver in drivers {
            service_tasks.spawn(driver);
        }
        if !wiring.services.is_empty() {
            service_tasks.spawn(refresh_jobs(
                Arc::clone(&self.job_source),
                inner.jobs.clone(),
                Arc::clone(&self.clock),
                self.shutdown.clone(),
                self.config.job_refresh,
            ));
        }
        service_tasks.spawn(sweep_commands(
            Arc::clone(&inner.commands),
            Arc::clone(&self.clock),
            self.shutdown.clone(),
        ));

        if let Some(endpoint) = self.dial.clone() {
            link_tasks.spawn(dial_loop(
                Arc::clone(&self.transport),
                endpoint,
                Arc::clone(&inner),
                wiring.clone(),
            ));
        }
        if let Some(endpoint) = self.listen.clone() {
            let listener = self
                .transport
                .listen(&endpoint)
                .await
                .with_context(|| format!("listening on {endpoint}"))?;
            // Where we *actually* bound, which is not what was asked for when the
            // request was port 0 — so record it where an operator, or a test, can
            // see it.
            let bound = listener.local_endpoint().unwrap_or(endpoint);
            info!(endpoint = %bound, transport = self.capabilities.name, "listening");
            *lock(&self.listening) = Some(bound);
            link_tasks.spawn(accept_loop(listener, Arc::clone(&inner), wiring.clone()));
        }

        info!(
            node = %self.config.node,
            services = wiring.services.len(),
            handlers = wiring.handlers.len(),
            "engine running"
        );

        self.shutdown.wait().await;
        let reason = self.stop.reason();
        info!(node = %self.config.node, ?reason, "shutting down");
        // Not `&self`: holding a shared borrow of the engine across an await
        // would make the whole future require `Sync`, and the registrations it
        // still owns are only `Send`.
        shut_down(
            &inner,
            service_tasks,
            link_tasks,
            Arc::clone(&self.config),
            Arc::clone(&self.clock),
            Arc::clone(&self.counters),
            reason,
        )
        .await;
        Ok(reason)
    }
}

impl<T: Transport> std::fmt::Debug for NodeEngine<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeEngine")
            .field("node", &self.config.node)
            .field("transport", &self.capabilities.name)
            .field("samplers", &self.samplers.len())
            .field("handlers", &self.handlers.len())
            .field("dial", &self.dial)
            .field("listen", &self.listen)
            .finish()
    }
}

/// Stop everything, in the order that loses the least data.
///
/// Services first, so `on_shutdown` data is queued; then the plugins' own
/// threads; then `Goodbye` behind everything already queued; then the writers
/// drain and close. The whole sequence is bounded by
/// [`EngineConfig::shutdown_deadline`].
async fn shut_down(
    inner: &Arc<Inner>,
    mut service_tasks: JoinSet<()>,
    mut link_tasks: JoinSet<()>,
    config: Arc<EngineConfig>,
    clock: Arc<dyn Clock>,
    counters: SharedCounters,
    reason: Stop,
) {
    let deadline = clock.now() + config.shutdown_deadline;

    // 1. Services have already seen the signal; wait for them to flush and stop.
    //    Their `on_shutdown` data lands in the queue behind whatever is already
    //    there, which is why the writer must not be stopped yet.
    let stopping = async { while service_tasks.join_next().await.is_some() {} };
    let overran = tokio::select! {
        biased;
        () = stopping => false,
        () = clock.sleep_until(deadline) => true,
    };
    if overran {
        warn!("shutdown deadline reached with services still running; abandoning them");
        service_tasks.abort_all();
    }

    // 2. The plugins' own worker threads, which may be blocked in a read.
    //
    //    Also under the deadline: a worker that ignores the shutdown signal must
    //    not be able to stop the process from exiting. Abandoning a join handle
    //    detaches the thread, which then dies with the process.
    let workers = Arc::clone(&inner.workers);
    let names = workers.names();
    if !names.is_empty() {
        debug!(?names, "joining plugin worker threads");
        let joining = tokio::task::spawn_blocking(move || workers.join_all());
        tokio::select! {
            biased;
            joined = joining => match joined {
                Ok(panicked) if !panicked.is_empty() => warn!(?panicked, "worker threads panicked"),
                Ok(_) => {}
                Err(err) => warn!(error = %err, "joining worker threads failed"),
            },
            () = clock.sleep_until(deadline) => {
                warn!(?names, "deadline reached; abandoning plugin worker threads");
            }
        }
    }

    // 3. Handlers release their resources once nothing more will be delivered.
    for handler in inner.handlers().values() {
        Arc::clone(handler).shutdown().await;
    }

    // 4. Say goodbye on every connection, behind everything already queued, and
    //    let the writers finish.
    // The reason depends on which way the connection points. Upstream, we report
    // why *we* are stopping. Downstream, always `Restart`: an agent's job is to
    // keep reaching its server, and only an operator stops an agent — so a server
    // going away must never be read as "do not come back".
    let upstream_goodbye = match reason {
        Stop::Shutdown => Goodbye::shutdown("engine stopping"),
        Stop::Restart => Goodbye::restart("engine restarting"),
    };
    let uplink = inner.uplink.clone();
    let downstream: Vec<Arc<PeerQueue>> = lock(&inner.peers)
        .values()
        .map(|peer| Arc::clone(&peer.queue))
        .filter(|queue| {
            uplink
                .as_ref()
                .is_none_or(|uplink| !Arc::ptr_eq(queue, uplink))
        })
        .collect();

    for queue in &downstream {
        let _ = queue.push_control(Frame::Goodbye(Goodbye::restart("server stopping")));
        queue.close();
    }
    let mut queues = downstream;
    if let Some(uplink) = uplink {
        let _ = uplink.push_control(Frame::Goodbye(upstream_goodbye));
        uplink.close();
        queues.push(uplink);
    }

    // 5. The writers can finish now that the queues are closed, and the readers
    //    with them. Waiting on the connections here — not in step 1 — is what
    //    lets a `Goodbye` be the last thing on the wire.
    let closing = async { while link_tasks.join_next().await.is_some() {} };
    tokio::select! {
        biased;
        () = closing => debug!("every connection closed"),
        () = clock.sleep_until(deadline) => {
            let left: usize = queues
                .iter()
                .map(|queue| {
                    let (control, data) = queue.depths();
                    control + data
                })
                .sum();
            warn!(frames = left, "shutdown deadline reached with frames still queued");
            link_tasks.abort_all();
        }
    }

    inner.commands.abandon_all();
    counters.connected.store(false, Ordering::Relaxed);
    info!(node = %config.node, "stopped");
}

/// Stop the engine, and read its counters, from outside `run`.
#[derive(Clone, Debug)]
pub struct EngineHandle {
    shutdown: ShutdownSignal,
    stop: Arc<StopRequest>,
    counters: SharedCounters,
    listening: Arc<Mutex<Option<Endpoint>>>,
}

impl EngineHandle {
    /// Stop the engine and stay stopped.
    pub fn shutdown(&self) {
        self.stop.request(Stop::Shutdown);
        self.shutdown.set();
    }

    /// Stop the engine so a supervisor can start it again.
    pub fn restart(&self) {
        self.stop.request(Stop::Restart);
        self.shutdown.set();
    }

    /// Whether shutdown has begun.
    #[must_use]
    pub fn is_stopping(&self) -> bool {
        self.shutdown.is_set()
    }

    /// The engine's counters, as a plugin would see them.
    #[must_use]
    pub fn stats(&self) -> EngineStats {
        self.counters.snapshot()
    }

    /// Where this engine is listening, once it is.
    ///
    /// `None` before the listener binds, and for an engine that only dials. Not
    /// necessarily the endpoint it was given: an engine told to listen on port 0
    /// learns its real port only from here.
    #[must_use]
    pub fn listening(&self) -> Option<Endpoint> {
        lock(&self.listening).clone()
    }
}

/// Keep the shared job list fresh.
async fn refresh_jobs(
    source: Arc<dyn JobSource>,
    jobs: Jobs,
    clock: Arc<dyn Clock>,
    shutdown: ShutdownSignal,
    interval: Duration,
) {
    // Enumeration is a blocking filesystem walk, so it gets its own named thread
    // like any other sampling work.
    let worker = match NamedWorker::spawn("jobs/scan") {
        Ok(worker) => worker,
        Err(err) => {
            error!(error = ?err, "cannot start the job scanner");
            return;
        }
    };

    loop {
        let scanner = Arc::clone(&source);
        match worker.run(move || scanner.jobs()).await {
            Ok(Ok(found)) => {
                debug!(count = found.len(), "refreshed the job list");
                jobs.replace(found);
            }
            Ok(Err(err)) => warn!(error = ?err, "cannot list jobs"),
            Err(err) => {
                error!(error = ?err, "the job scanner panicked");
                return;
            }
        }

        let sleep = clock.sleep(interval);
        tokio::select! {
            biased;
            () = shutdown.wait() => return,
            () = sleep => {}
        }
    }
}

/// Expire commands whose time has run out.
async fn sweep_commands(commands: Arc<Commands>, clock: Arc<dyn Clock>, shutdown: ShutdownSignal) {
    loop {
        let next = commands.sweep(clock.now());
        // With nothing to expire there is nothing to wake for: wait for a
        // dispatch instead of polling.
        let wait: BoxFuture<'static, ()> = match next {
            Some(deadline) => clock.sleep_until(deadline),
            None => {
                let commands = Arc::clone(&commands);
                Box::pin(async move { commands.wait_for_change().await })
            }
        };
        tokio::select! {
            biased;
            () = shutdown.wait() => return,
            () = wait => {}
        }
    }
}

/// Dial upstream, and keep dialling.
async fn dial_loop<T: Transport>(
    transport: Arc<T>,
    endpoint: Endpoint,
    inner: Arc<Inner>,
    wiring: Wiring,
) {
    let mut failures = 0u32;
    let mut connections = 0u64;

    loop {
        if wiring.shutdown.is_set() {
            return;
        }
        let delay = wiring.config.reconnect.delay(failures);
        if !delay.is_zero() {
            debug!(%endpoint, ?delay, "waiting before dialling again");
            let sleep = wiring.clock.sleep(delay);
            tokio::select! {
                biased;
                () = wiring.shutdown.wait() => return,
                () = sleep => {}
            }
        }

        match transport.connect(&endpoint).await {
            Err(err) => {
                failures = failures.saturating_add(1);
                // Retryable by contract, which is what makes "the server is not up
                // yet" ordinary rather than fatal.
                warn!(%endpoint, attempt = failures, error = ?err, "cannot connect");
                continue;
            }
            Ok(connection) => {
                failures = 0;
                connections += 1;
                if connections > 1 {
                    wiring.counters.reconnects.fetch_add(1, Ordering::Relaxed);
                }
                info!(%endpoint, "connected upstream");

                let ended = serve_upstream(connection, &inner, &wiring, &endpoint).await;
                info!(%endpoint, ?ended, "upstream connection ended");
                if !ended.should_reconnect() {
                    return;
                }
                failures = failures.saturating_add(1);
            }
        }
    }
}

/// Run one upstream connection to its end.
async fn serve_upstream<C: Connection>(
    connection: C,
    inner: &Arc<Inner>,
    wiring: &Wiring,
    endpoint: &Endpoint,
) -> Ended {
    let Some(uplink) = inner.uplink.clone() else {
        return Ended::LocalShutdown;
    };
    // Control frames queued for the previous connection are meaningless now: a
    // command result nobody can receive, or a stale hello. Buffered *data* stays.
    uplink.drop_control();

    let services: Vec<ServiceId> = inner.services().values().map(ServiceHandle::id).collect();
    let peer = Arc::new(Peer::new(
        endpoint.to_string(),
        endpoint.clone(),
        Arc::clone(&uplink),
        Vec::new(),
    ));

    // Hello goes first, ahead of every batch that was waiting.
    if let Err(err) = uplink.push_front_control(Frame::Hello(our_hello(&wiring.config, &services)))
    {
        return Ended::Broken(err);
    }

    lock(&inner.peers).insert(peer.node.clone(), Arc::clone(&peer));
    inner.counters.connected.store(true, Ordering::Relaxed);
    inner.counters.peers.store(1, Ordering::Relaxed);

    let ended = run_connection(connection, Arc::clone(&peer), wiring.clone()).await;

    lock(&inner.peers).remove(&peer.node);
    inner.counters.connected.store(false, Ordering::Relaxed);
    inner.counters.peers.store(0, Ordering::Relaxed);
    ended
}

/// Accept agents until the engine stops.
async fn accept_loop<L: Listener>(listener: L, inner: Arc<Inner>, wiring: Wiring) {
    let mut connections: JoinSet<()> = JoinSet::new();

    loop {
        let accepted = tokio::select! {
            biased;
            () = wiring.shutdown.wait() => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => continue,
            accepted = listener.accept() => accepted,
        };

        match accepted {
            Ok(connection) => {
                connections.spawn(serve_agent(connection, Arc::clone(&inner), wiring.clone()));
            }
            Err(err) => {
                warn!(error = ?err, "cannot accept a connection");
                if !err.is_retryable() {
                    break;
                }
            }
        }
    }

    // Let the connections we accepted finish; the shutdown deadline bounds this.
    while connections.join_next().await.is_some() {}
}

/// Run one agent's connection to its end.
async fn serve_agent<C: Connection>(connection: C, inner: Arc<Inner>, wiring: Wiring) {
    let remote = connection.peer();
    let (tx, mut rx) = connection.split();

    // An agent must introduce itself before anything else, so the server knows
    // which node it is talking to before it accepts any data.
    let hello = match accept_hello(&mut rx).await {
        Ok(hello) => hello,
        Err(err) => {
            warn!(%remote, error = ?err, "rejecting a connection that did not say hello");
            let mut tx = tx;
            let _ = FrameTxExt::say_goodbye(&mut tx, Goodbye::error(err.to_string())).await;
            return;
        }
    };
    let node = hello.node.clone();
    info!(%remote, node = %node, services = hello.services.len(), "agent connected");

    let queue = Arc::new(PeerQueue::new(
        wiring.config.data_queue,
        wiring.config.control_queue,
        Arc::clone(&wiring.counters),
    ));
    let peer = Arc::new(Peer::new(
        node.clone(),
        remote,
        Arc::clone(&queue),
        hello.services,
    ));

    // Anything queued while this node was away goes out now, with its remaining
    // time to live recomputed.
    for ready in wiring.commands.flush_queued(&node, wiring.clock.now()) {
        if let Err(err) = queue.push_control(ready.frame) {
            warn!(node = %node, error = ?err, "cannot deliver a queued command");
        }
    }

    {
        let mut peers = lock(&inner.peers);
        peers.insert(node.clone(), Arc::clone(&peer));
        let count = peers.len();
        inner.counters.peers.store(count as u64, Ordering::Relaxed);
        inner.counters.connected.store(count > 0, Ordering::Relaxed);
    }

    let ended = run_connection_halves(tx, rx, Arc::clone(&peer), wiring.clone()).await;
    info!(node = %node, ?ended, "agent disconnected");

    {
        let mut peers = lock(&inner.peers);
        peers.remove(&node);
        let count = peers.len();
        inner.counters.peers.store(count as u64, Ordering::Relaxed);
        inner.counters.connected.store(count > 0, Ordering::Relaxed);
    }
    queue.abort();
    // A command whose result never arrived may or may not have been carried out,
    // so it is failed rather than silently retried.
    wiring.commands.abandon(&node);
}

/// Split a connection and run its halves.
async fn run_connection<C: Connection>(connection: C, peer: Arc<Peer>, wiring: Wiring) -> Ended {
    let (tx, rx) = connection.split();
    run_connection_halves(tx, rx, peer, wiring).await
}

/// Run a reader, a writer and a heartbeat together until the connection ends.
async fn run_connection_halves<Tx, Rx>(tx: Tx, rx: Rx, peer: Arc<Peer>, wiring: Wiring) -> Ended
where
    Tx: cs_transport::FrameTx,
    Rx: cs_transport::FrameRx,
{
    let link_dead = ShutdownSignal::new();
    let last_write = Arc::new(LastWrite::new(wiring.clock.now()));

    let writer = tokio::spawn(write_loop(
        tx,
        Arc::clone(&peer),
        link_dead.clone(),
        wiring.clone(),
        Arc::clone(&last_write),
    ));
    let heartbeat = tokio::spawn(heartbeat_loop(
        Arc::clone(&peer),
        link_dead.clone(),
        wiring.clone(),
        last_write,
    ));

    let ended = read_loop(rx, Arc::clone(&peer), link_dead.clone(), wiring).await;

    // On a local shutdown the writer must stay alive to drain what the services
    // just flushed — the shutdown sequence closes the queue, which is what ends
    // it. Any other ending means the connection is gone, so stop writing at once.
    if !matches!(ended, Ended::LocalShutdown) {
        link_dead.set();
    }
    let _ = writer.await;
    let _ = heartbeat.await;
    ended
}

/// Send one last frame on a connection that is being refused.
trait FrameTxExt {
    async fn say_goodbye(&mut self, goodbye: Goodbye) -> Result<()>;
}

impl<Tx: cs_transport::FrameTx> FrameTxExt for Tx {
    async fn say_goodbye(&mut self, goodbye: Goodbye) -> Result<()> {
        self.send(Frame::Goodbye(goodbye)).await?;
        self.close().await
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
