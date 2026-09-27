use std::future::Future;

use cs_util::Result;

use crate::{Data, ServiceBound, ServiceCtx};

/// Where a message came from.
///
/// A handler needs this for the obvious reason — metrics are worthless without
/// knowing which node they describe — and for the less obvious one: a command is
/// addressed to a node by name, so this is what makes `ctx.command(origin.node,
/// ..)` possible from inside a handler.
///
/// # `node` and `via` are the same thing today, and will not always be
///
/// In a two-tier deployment — agents dialling one server — the node that produced
/// a batch *is* the peer that delivered it, and these two fields always match.
/// Once a relay or a cluster head forwards data upward they diverge: `node` stays
/// the agent that measured something, while `via` becomes the tier that passed it
/// on.
///
/// The distinction is drawn now, before any relay exists, because it is a
/// *semantic* change to a plugin-facing type rather than an additive one to the
/// wire format. A handler written today against "`node` is where this came from"
/// keeps working when a tier is inserted beneath it; one written against "`node`
/// is who I am talking to" would silently start attributing every node's metrics
/// to a relay. Use `node` to attribute data and to address a command; use `via`
/// only for diagnostics about the path it took.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Origin<'a> {
    /// The node that **produced** this message, as it named itself in its
    /// `Hello`. What data should be attributed to, and what a command should be
    /// addressed to.
    pub node: &'a str,
    /// The peer this message was **delivered by** — the far end of the connection
    /// it arrived on. Equal to [`node`](Origin::node) unless a tier forwarded it.
    pub via: &'a str,
    /// The [`ServiceDef::VERSION`](crate::ServiceDef::VERSION) the producer declared
    /// for this service, so a handler can notice a version skew rather than
    /// misread the message.
    pub service_version: u32,
}

impl Origin<'_> {
    /// Whether this arrived straight from the node that produced it.
    ///
    /// False once a relay or cluster head is in the path.
    #[must_use]
    pub fn is_direct(&self) -> bool {
        self.node == self.via
    }
}

/// The server side of a service: what happens to data when it arrives.
///
/// Handlers are shared (`&self`, `Sync`) and run concurrently — the engine drives
/// many at once on the async runtime, so per-message state goes in the message
/// and shared state goes behind whatever lock or channel the handler chooses.
///
/// [`handle`](Handler::handle) is an ordinary `async fn` from the implementor's
/// side. The engine boxes the future once, at the point where it erases the
/// handler's type for its routing table, so plugins pay no `async_trait`-style
/// allocation in their own code.
///
/// ```
/// use cs_api::{Handler, NoCommand, Origin, ServiceBound, ServiceCtx, ServiceDef};
/// use cs_util::Result;
/// use std::sync::atomic::{AtomicU64, Ordering};
///
/// struct Cpu;
/// impl ServiceDef for Cpu {
///     const NAME: &'static str = "cpu";
///     type Data = String; // stands in for a generated prost message
///     type Command = NoCommand;
/// }
///
/// #[derive(Default)]
/// struct CpuWriter {
///     rows: AtomicU64,
/// }
///
/// impl ServiceBound for CpuWriter {
///     type Service = Cpu;
/// }
///
/// impl Handler for CpuWriter {
///     async fn handle(
///         &self,
///         ctx: &ServiceCtx<Cpu>,
///         from: Origin<'_>,
///         msg: String,
///     ) -> Result<()> {
///         // `from.node` is which node measured this; `ctx.node()` is us.
///         let _ = (from.node, ctx.node(), msg);
///         self.rows.fetch_add(1, Ordering::Relaxed);
///         Ok(())
///     }
/// }
/// ```
pub trait Handler: ServiceBound + Send + Sync + 'static {
    /// Deal with one message.
    ///
    /// An error is logged and counted; it does not kill the connection or the
    /// service. If the work can fail transiently, decide inside the handler
    /// whether to retry — the message will not be redelivered.
    fn handle(
        &self,
        ctx: &ServiceCtx<Self::Service>,
        from: Origin<'_>,
        msg: Data<Self>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Release resources during server shutdown.
    ///
    /// Called after in-flight [`handle`](Handler::handle) calls have drained,
    /// under the shutdown deadline. Flush and close things here; the process is
    /// about to exit.
    fn shutdown(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoCommand, ServiceDef};
    use cs_async_util::test_util::block_on;
    use cs_util::{Error, ErrorKind};
    use std::sync::Mutex;

    fn origin() -> Origin<'static> {
        Origin {
            node: "node-0042",
            via: "node-0042",
            service_version: 1,
        }
    }

    struct Cpu;

    impl ServiceDef for Cpu {
        const NAME: &'static str = "cpu";
        type Data = String;
        type Command = NoCommand;
    }

    #[derive(Default)]
    struct Collect {
        seen: Mutex<Vec<String>>,
        closed: Mutex<bool>,
    }

    impl ServiceBound for Collect {
        type Service = Cpu;
    }

    impl Handler for Collect {
        async fn handle(
            &self,
            _ctx: &ServiceCtx<Cpu>,
            from: Origin<'_>,
            msg: String,
        ) -> Result<()> {
            assert_eq!(from.node, "node-0042", "the sender should be named");
            if msg.is_empty() {
                return Err(Error::new(ErrorKind::Decode, "empty batch"));
            }
            self.seen.lock().expect("lock").push(msg);
            Ok(())
        }

        async fn shutdown(&self) {
            *self.closed.lock().expect("lock") = true;
        }
    }

    #[test]
    fn the_sender_is_named_and_the_handler_is_an_ordinary_async_fn() {
        let ctx = crate::test_support::fake_ctx::<Cpu>();
        let handler = Collect::default();

        block_on(handler.handle(&ctx, origin(), "batch-1".into())).expect("handled");
        block_on(handler.handle(&ctx, origin(), "batch-2".into())).expect("handled");
        assert_eq!(
            *handler.seen.lock().expect("lock"),
            ["batch-1".to_owned(), "batch-2".to_owned()]
        );
    }

    #[test]
    fn handler_errors_are_returned_not_panicked() {
        let ctx = crate::test_support::fake_ctx::<Cpu>();
        let handler = Collect::default();
        let err = block_on(handler.handle(&ctx, origin(), String::new())).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(handler.seen.lock().expect("lock").is_empty());
    }

    #[test]
    fn shutdown_defaults_to_doing_nothing() {
        struct Bare;
        impl ServiceBound for Bare {
            type Service = Cpu;
        }
        impl Handler for Bare {
            async fn handle(
                &self,
                _ctx: &ServiceCtx<Cpu>,
                _from: Origin<'_>,
                _msg: String,
            ) -> Result<()> {
                Ok(())
            }
        }

        block_on(Bare.shutdown());

        let handler = Collect::default();
        block_on(handler.shutdown());
        assert!(*handler.closed.lock().expect("lock"));
    }

    #[test]
    fn a_direct_connection_reports_the_same_node_twice() {
        assert!(origin().is_direct(), "no tier in between");

        // What a relay will look like: the agent still owns the data, the relay
        // merely carried it.
        let forwarded = Origin {
            node: "node-0042",
            via: "rack3-relay",
            service_version: 1,
        };
        assert!(!forwarded.is_direct());
        assert_eq!(
            forwarded.node, "node-0042",
            "attribution follows the producer, never the carrier"
        );
    }

    #[test]
    fn handlers_are_shared_across_tasks() {
        fn assert_shareable<H: Handler>() {}
        assert_shareable::<Collect>();
    }
}
