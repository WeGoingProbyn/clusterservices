use std::future::Future;

use cs_util::Result;

use crate::{Data, ServiceBound, ServiceCtx};

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
/// use cs_api::{Handler, NoCommand, ServiceBound, ServiceCtx, ServiceDef};
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
///     async fn handle(&self, ctx: &ServiceCtx<Cpu>, msg: String) -> Result<()> {
///         let _ = (ctx.node(), msg);
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
        async fn handle(&self, _ctx: &ServiceCtx<Cpu>, msg: String) -> Result<()> {
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
    fn a_handler_is_an_ordinary_async_fn() {
        let ctx = crate::test_support::fake_ctx::<Cpu>();
        let handler = Collect::default();

        block_on(handler.handle(&ctx, "batch-1".into())).expect("handled");
        block_on(handler.handle(&ctx, "batch-2".into())).expect("handled");
        assert_eq!(
            *handler.seen.lock().expect("lock"),
            ["batch-1".to_owned(), "batch-2".to_owned()]
        );
    }

    #[test]
    fn handler_errors_are_returned_not_panicked() {
        let ctx = crate::test_support::fake_ctx::<Cpu>();
        let handler = Collect::default();
        let err = block_on(handler.handle(&ctx, String::new())).unwrap_err();
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
            async fn handle(&self, _ctx: &ServiceCtx<Cpu>, _msg: String) -> Result<()> {
                Ok(())
            }
        }

        block_on(Bare.shutdown());

        let handler = Collect::default();
        block_on(handler.shutdown());
        assert!(*handler.closed.lock().expect("lock"));
    }

    #[test]
    fn handlers_are_shared_across_tasks() {
        fn assert_shareable<H: Handler>() {}
        assert_shareable::<Collect>();
    }
}
