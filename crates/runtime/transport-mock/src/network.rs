use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use cs_transport::{Capabilities, Endpoint, Listener, Transport};
use cs_util::{Error, ErrorKind, Result};
use tokio::sync::mpsc;

use crate::conn::MockConnection;
use crate::link::Link;

/// How many connections may wait to be accepted before a dialler blocks.
const BACKLOG: usize = 16;

/// The scheme mock endpoints use.
pub const SCHEME: &str = "mock";

/// What [`MockNetwork::refuse_connects`] and friends make `connect` do.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fault {
    /// Refuse every attempt until cleared.
    Refuse,
    /// Refuse the next `n` attempts, then behave normally.
    RefuseTimes(usize),
    /// Complete the connection, then break it immediately — the case an engine
    /// usually gets wrong, because `connect` succeeded.
    AcceptThenKill,
}

/// An in-process network that mock transports talk over.
///
/// Scoped rather than global: each test makes its own, so two tests can both use
/// `mock://server` without colliding and nothing leaks between them.
///
/// ```
/// use cs_transport::{Connection, Frame, FrameRx, FrameTx, Hello, Listener, Transport};
/// use cs_transport_mock::MockNetwork;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), cs_util::Error> {
/// let network = MockNetwork::new();
/// let transport = network.transport();
/// let server_at = MockNetwork::endpoint("server");
///
/// let listener = transport.listen(&server_at).await?;
/// let client = transport.connect(&server_at).await?;
/// let accepted = listener.accept().await?;
///
/// let (mut tx, _) = client.split();
/// let (_, mut rx) = accepted.split();
/// tx.send(Frame::Hello(Hello::new("node-1"))).await?;
/// assert!(matches!(rx.recv().await?, Some(Frame::Hello(_))));
///
/// // And the test can break it from outside, whenever it likes.
/// network.links()[0].kill();
/// assert!(rx.recv().await.is_err());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MockNetwork {
    inner: Arc<Network>,
}

pub(crate) struct Network {
    pub(crate) capabilities: Capabilities,
    pub(crate) send_delay_ms: AtomicU64,
    capacity: usize,
    next_id: AtomicU64,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Accept queues, by endpoint authority.
    listeners: HashMap<String, mpsc::Sender<MockConnection>>,
    faults: HashMap<String, Fault>,
    attempts: HashMap<String, u64>,
    links: Vec<Link>,
}

impl MockNetwork {
    /// A network with room for 8 frames per direction and the default frame
    /// ceiling.
    ///
    /// The small queue is deliberate: backpressure that never happens is
    /// backpressure that was never tested.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(8)
    }

    /// A network whose links hold `capacity` frames per direction before a sender
    /// blocks.
    ///
    /// # Panics
    ///
    /// If `capacity` is zero, which `tokio::sync::mpsc` does not allow.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "a mock link needs room for at least one frame"
        );
        Self {
            inner: Arc::new(Network {
                capabilities: Capabilities::new(SCHEME),
                send_delay_ms: AtomicU64::new(0),
                capacity,
                next_id: AtomicU64::new(1),
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// Set the largest frame this network will carry.
    ///
    /// Anything bigger fails to send, which is how a test proves the engine
    /// chunks.
    #[must_use]
    pub fn with_max_frame(self, max_frame: usize) -> Self {
        // Before any transport exists, so replacing the whole `Arc` is simplest
        // and keeps `Capabilities` immutable afterwards.
        Self {
            inner: Arc::new(Network {
                capabilities: self.inner.capabilities.with_max_frame(max_frame),
                send_delay_ms: AtomicU64::new(self.inner.send_delay_ms.load(Ordering::Relaxed)),
                capacity: self.inner.capacity,
                next_id: AtomicU64::new(1),
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// A transport on this network, to hand to an engine.
    #[must_use]
    pub fn transport(&self) -> MockTransport {
        MockTransport {
            network: Arc::clone(&self.inner),
        }
    }

    /// `mock://<name>` — the endpoint a listener binds and a dialler dials.
    #[must_use]
    pub fn endpoint(name: &str) -> Endpoint {
        Endpoint::from_parts(SCHEME, name)
    }

    /// Delay every send by `delay`, to model a slow network.
    ///
    /// Uses the runtime's timer, so a test with `start_paused = true` controls it.
    pub fn set_send_delay(&self, delay: Duration) {
        self.inner.send_delay_ms.store(
            u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// Refuse every connection to `name` until [`allow_connects`](MockNetwork::allow_connects).
    ///
    /// The refusal is a retryable error, exactly as a real "connection refused"
    /// is, so an agent's reconnect loop treats it the same way.
    pub fn refuse_connects(&self, name: &str) {
        self.set_fault(name, Fault::Refuse);
    }

    /// Refuse the next `times` connections to `name`, then behave normally.
    ///
    /// For asserting that an agent retries with backoff and eventually gets
    /// through.
    pub fn refuse_connects_times(&self, name: &str, times: usize) {
        self.set_fault(name, Fault::RefuseTimes(times));
    }

    /// Let connections to `name` succeed, then kill them immediately.
    ///
    /// The nastier failure: `connect` returns a connection that is already dead,
    /// so code which only handles errors from `connect` gets caught.
    pub fn kill_on_connect(&self, name: &str) {
        self.set_fault(name, Fault::AcceptThenKill);
    }

    /// Stop interfering with connections to `name`.
    pub fn allow_connects(&self, name: &str) {
        lock(&self.inner.state).faults.remove(name);
    }

    fn set_fault(&self, name: &str, fault: Fault) {
        lock(&self.inner.state)
            .faults
            .insert(name.to_owned(), fault);
    }

    /// How many times anything has tried to connect to `name`, including refused
    /// attempts.
    #[must_use]
    pub fn connect_attempts(&self, name: &str) -> u64 {
        lock(&self.inner.state)
            .attempts
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    /// Every link established on this network, oldest first, including dead ones.
    #[must_use]
    pub fn links(&self) -> Vec<Link> {
        lock(&self.inner.state).links.clone()
    }

    /// The most recently established link.
    #[must_use]
    pub fn last_link(&self) -> Option<Link> {
        lock(&self.inner.state).links.last().cloned()
    }

    /// How many links have ever been established.
    #[must_use]
    pub fn link_count(&self) -> usize {
        lock(&self.inner.state).links.len()
    }

    /// How many links are still alive.
    #[must_use]
    pub fn live_link_count(&self) -> usize {
        lock(&self.inner.state)
            .links
            .iter()
            .filter(|link| link.is_alive())
            .count()
    }

    /// Break every link on the network — a switch going down.
    pub fn kill_all(&self) {
        for link in lock(&self.inner.state).links.iter() {
            link.kill();
        }
    }

    /// What a transport on this network reports.
    #[must_use]
    pub fn capabilities(&self) -> Capabilities {
        self.inner.capabilities
    }
}

impl Default for MockNetwork {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MockNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock(&self.inner.state);
        f.debug_struct("MockNetwork")
            .field("capacity", &self.inner.capacity)
            .field("max_frame", &self.inner.capabilities.max_frame)
            .field("listeners", &state.listeners.keys().collect::<Vec<_>>())
            .field("links", &state.links.len())
            .finish()
    }
}

/// A [`Transport`] onto a [`MockNetwork`].
///
/// Serialises every frame to bytes just as a real transport does, so tests
/// exercise the encoding and can inject bytes that are not frames.
#[derive(Clone)]
pub struct MockTransport {
    network: Arc<Network>,
}

impl fmt::Debug for MockTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockTransport")
            .field("max_frame", &self.network.capabilities.max_frame)
            .finish()
    }
}

impl Transport for MockTransport {
    type Conn = MockConnection;
    type Listener = MockListener;

    fn capabilities(&self) -> Capabilities {
        self.network.capabilities
    }

    async fn connect(&self, endpoint: &Endpoint) -> Result<MockConnection> {
        let name = endpoint.require_scheme(SCHEME)?.to_owned();

        let (accept_queue, fault) = {
            let mut state = lock(&self.network.state);
            *state.attempts.entry(name.clone()).or_default() += 1;

            let fault = match state.faults.get_mut(&name) {
                Some(Fault::RefuseTimes(remaining)) => {
                    *remaining -= 1;
                    let exhausted = *remaining == 0;
                    if exhausted {
                        state.faults.remove(&name);
                    }
                    Some(Fault::Refuse)
                }
                Some(other) => Some(*other),
                None => None,
            };
            (state.listeners.get(&name).cloned(), fault)
        };

        if fault == Some(Fault::Refuse) {
            return Err(refused(endpoint, "refused by the mock network"));
        }

        let Some(accept_queue) = accept_queue else {
            return Err(refused(endpoint, "nothing is listening"));
        };

        let id = self.network.next_id.fetch_add(1, Ordering::Relaxed);
        let client_at = Endpoint::from_parts(SCHEME, &format!("client-{id}"));
        let (link, client_end, server_end) = Link::new(
            id,
            client_at.clone(),
            endpoint.clone(),
            self.network.capacity,
        );
        lock(&self.network.state).links.push(link.clone());

        let accepted = MockConnection::new(
            client_at,
            link.clone(),
            server_end,
            Arc::clone(&self.network),
        );
        accept_queue
            .send(accepted)
            .await
            .map_err(|_| refused(endpoint, "the listener went away"))?;

        if fault == Some(Fault::AcceptThenKill) {
            link.kill();
        }

        Ok(MockConnection::new(
            endpoint.clone(),
            link,
            client_end,
            Arc::clone(&self.network),
        ))
    }

    async fn listen(&self, endpoint: &Endpoint) -> Result<MockListener> {
        let name = endpoint.require_scheme(SCHEME)?.to_owned();
        let (tx, rx) = mpsc::channel(BACKLOG);

        let mut state = lock(&self.network.state);
        if let Some(existing) = state.listeners.get(&name) {
            // A closed channel means the previous listener was dropped, so the
            // address is free again — the same rule as a real socket.
            if !existing.is_closed() {
                return Err(Error::new(
                    ErrorKind::Config,
                    format!("{endpoint} is already bound on this mock network"),
                ));
            }
        }
        state.listeners.insert(name, tx);

        Ok(MockListener {
            endpoint: endpoint.clone(),
            incoming: tokio::sync::Mutex::new(rx),
        })
    }
}

/// A bound mock listener.
pub struct MockListener {
    endpoint: Endpoint,
    /// Behind a mutex because [`Listener::accept`] takes `&self`, so an accept
    /// loop can be shared.
    incoming: tokio::sync::Mutex<mpsc::Receiver<MockConnection>>,
}

impl Listener for MockListener {
    type Conn = MockConnection;

    async fn accept(&self) -> Result<MockConnection> {
        let mut incoming = self.incoming.lock().await;
        incoming.recv().await.ok_or_else(|| {
            Error::new(
                ErrorKind::Transport,
                "the mock network dropped this listener",
            )
        })
    }

    fn local_endpoint(&self) -> Result<Endpoint> {
        Ok(self.endpoint.clone())
    }
}

impl fmt::Debug for MockListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockListener")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// A refused connection, retryable so a reconnect loop keeps trying.
#[track_caller]
fn refused(endpoint: &Endpoint, why: &str) -> Error {
    Error::new(
        ErrorKind::Transport,
        format!("connection to {endpoint} refused: {why}"),
    )
}

/// See `cs_async_util::lock` — this state is always consistent, so poisoning
/// carries no information.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
