use std::error::Error as StdError;
use std::fmt;
use std::panic::Location;

/// How the caller should react to a failure.
///
/// Deliberately small: a kind exists because some layer of the framework
/// branches on it, not to describe what went wrong. Put the description in the
/// message. In particular, do not add a kind per failure mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// Filesystem or syscall failure (sysfs reads, procfs, config files).
    Io,
    /// A connection failed, died, or produced an unusable frame. Retryable.
    Transport,
    /// A frame or message could not be decoded. The peer or the wire is wrong,
    /// retrying the same bytes will not help.
    Decode,
    /// Invalid configuration or invalid registration; fatal at startup.
    Config,
    /// An operation exceeded its deadline. Retryable.
    Timeout,
    /// A peer or a plugin refused the request and said why.
    Rejected,
    /// The operation lost its race with shutdown, or was abandoned because the
    /// engine is stopping.
    Shutdown,
    /// A plugin misbehaved: panicked, failed to build, or returned an error
    /// from its own logic.
    Plugin,
}

impl ErrorKind {
    /// Whether the engine should back off and retry rather than give up.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Transport | Self::Timeout)
    }

    /// Lowercase, stable name — used in logs, metric labels, and on the wire.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Io => "io",
            Self::Transport => "transport",
            Self::Decode => "decode",
            Self::Config => "config",
            Self::Timeout => "timeout",
            Self::Rejected => "rejected",
            Self::Shutdown => "shutdown",
            Self::Plugin => "plugin",
        }
    }

    /// The inverse of [`as_str`](ErrorKind::as_str), for a kind that arrived over
    /// the network.
    ///
    /// `None` for anything unrecognised — a peer running a newer build. The
    /// caller decides what to do with that; it must not become a new kind.
    ///
    /// ```
    /// # use cs_util::ErrorKind;
    /// assert_eq!(ErrorKind::parse("timeout"), Some(ErrorKind::Timeout));
    /// assert_eq!(ErrorKind::parse("sideways"), None);
    /// ```
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "io" => Self::Io,
            "transport" => Self::Transport,
            "decode" => Self::Decode,
            "config" => Self::Config,
            "timeout" => Self::Timeout,
            "rejected" => Self::Rejected,
            "shutdown" => Self::Shutdown,
            "plugin" => Self::Plugin,
            _ => return None,
        })
    }
}

impl std::str::FromStr for ErrorKind {
    type Err = Error;

    #[track_caller]
    fn from_str(name: &str) -> Result<Self, Error> {
        Self::parse(name)
            .ok_or_else(|| Error::new(ErrorKind::Decode, format!("unknown error kind {name:?}")))
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The framework error: an [`ErrorKind`] plus a chain of located messages.
///
/// Every constructor and every `From` impl is `#[track_caller]`, so `?` records
/// the line it was written on and no layer needs to repeat where it is.
///
/// Formatting has two modes:
/// - `{}` — the outermost frame only, with its location.
/// - `{:#}` and `{:?}` — the whole chain, one cause per line.
///
/// `Debug` is the multi-line form on purpose: returning `Result<_, Error>` from
/// `main` or a test prints the full chain.
pub struct Error(Box<Inner>);

struct Inner {
    kind: ErrorKind,
    msg: Message,
    /// Where this frame was created, or `None` for one that happened somewhere
    /// this process cannot point at — see [`Error::remote`].
    location: Option<&'static Location<'static>>,
    /// The next frame of *our* chain, added by [`Error::context`].
    source: Option<Error>,
}

/// One frame's message: either text we were given, or a foreign error we are
/// carrying (kept whole so it stays downcastable and its own `source()` chain
/// can be walked).
enum Message {
    Text(String),
    Foreign(Box<dyn StdError + Send + Sync + 'static>),
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(s) => f.write_str(s),
            Self::Foreign(e) => e.fmt(f),
        }
    }
}

impl Error {
    /// A new error with no cause.
    #[track_caller]
    #[must_use]
    pub fn new(kind: ErrorKind, msg: impl Into<String>) -> Self {
        Self(Box::new(Inner {
            kind,
            msg: Message::Text(msg.into()),
            location: Some(Location::caller()),
            source: None,
        }))
    }

    /// A frame for something that went wrong somewhere else.
    ///
    /// Deliberately **not** `#[track_caller]`: the caller is whatever is rebuilding
    /// a chain that arrived over the network, and recording that line would point
    /// at the messenger. The original `file:line` travels in the message instead,
    /// which is what `ErrorTrace::to_error` puts there — so a remote chain prints
    /// exactly as it did on the node it happened on, once, rather than with this
    /// build's line appended to every frame.
    #[must_use]
    pub fn remote(kind: ErrorKind, msg: impl Into<String>) -> Self {
        Self(Box::new(Inner {
            kind,
            msg: Message::Text(msg.into()),
            location: None,
            source: None,
        }))
    }

    /// Add an outer frame that also happened somewhere else. See [`Error::remote`].
    #[must_use]
    pub fn remote_context(self, msg: impl Into<String>) -> Self {
        let kind = self.0.kind;
        Self(Box::new(Inner {
            kind,
            msg: Message::Text(msg.into()),
            location: None,
            source: Some(self),
        }))
    }

    /// A new error caused by a foreign error, which becomes the next frame.
    ///
    /// Use this at the boundary where a third-party error enters the framework
    /// and you have to choose its kind:
    ///
    /// ```
    /// # use cs_util::{Error, ErrorKind};
    /// let raw = "1x";
    /// let err = raw
    ///     .parse::<u32>()
    ///     .map_err(|e| Error::with_source(ErrorKind::Config, "bad port", e))
    ///     .unwrap_err();
    /// assert_eq!(err.kind(), ErrorKind::Config);
    /// ```
    #[track_caller]
    #[must_use]
    pub fn with_source(
        kind: ErrorKind,
        msg: impl Into<String>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        let location = Some(Location::caller());
        Self(Box::new(Inner {
            kind,
            msg: Message::Text(msg.into()),
            location,
            source: Some(Self(Box::new(Inner {
                kind,
                msg: Message::Foreign(Box::new(source)),
                location,
                source: None,
            }))),
        }))
    }

    /// Wrap a foreign error with a kind but no message of its own.
    #[track_caller]
    #[must_use]
    pub fn foreign(kind: ErrorKind, source: impl StdError + Send + Sync + 'static) -> Self {
        Self(Box::new(Inner {
            kind,
            msg: Message::Foreign(Box::new(source)),
            location: Some(Location::caller()),
            source: None,
        }))
    }

    /// Add a layer of context. The new frame inherits this error's kind and
    /// records the caller's location.
    #[track_caller]
    #[must_use]
    pub fn context(self, msg: impl Into<String>) -> Self {
        Self(Box::new(Inner {
            kind: self.kind(),
            msg: Message::Text(msg.into()),
            location: Some(Location::caller()),
            source: Some(self),
        }))
    }

    /// Like [`Error::context`], but the message is only built on failure.
    #[track_caller]
    #[must_use]
    pub fn with_context<S: Into<String>>(self, msg: impl FnOnce() -> S) -> Self {
        // Not delegating to `context`: that would report this line as the
        // location instead of the caller's.
        Self(Box::new(Inner {
            kind: self.kind(),
            msg: Message::Text(msg().into()),
            location: Some(Location::caller()),
            source: Some(self),
        }))
    }

    /// Replace the kind of the outermost frame.
    ///
    /// For the case where a lower layer's classification is wrong for the
    /// caller — e.g. an [`Io`](ErrorKind::Io) failure on a socket is
    /// [`Transport`](ErrorKind::Transport) to the engine, and therefore
    /// retryable.
    #[must_use]
    pub fn with_kind(mut self, kind: ErrorKind) -> Self {
        self.0.kind = kind;
        self
    }

    /// How to react to this error.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.0.kind
    }

    /// Shorthand for `self.kind().is_retryable()`.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.0.kind.is_retryable()
    }

    /// Where the outermost frame was created.
    ///
    /// `None` for a frame rebuilt from another node's error, which happened at a
    /// line in *that* build — see [`Error::remote`].
    #[must_use]
    pub fn location(&self) -> Option<&'static Location<'static>> {
        self.0.location
    }

    /// Every frame, outermost first, including this one.
    ///
    /// The chain continues into the `source()` chain of any foreign error it
    /// carries, so nothing is hidden.
    #[must_use]
    pub fn chain(&self) -> Chain<'_> {
        Chain {
            state: Some(Link::Ours(self)),
        }
    }

    /// The innermost frame — the thing that actually failed.
    #[must_use]
    pub fn root_cause(&self) -> Frame<'_> {
        // The chain always yields at least this error's own frame.
        self.chain().last().unwrap_or_else(|| self.frame())
    }

    /// Downcast the foreign error carried by any frame of the chain.
    ///
    /// ```
    /// # use cs_util::{Error, ErrorKind, ResultExt};
    /// let io = std::io::Error::from(std::io::ErrorKind::NotFound);
    /// let err = Error::from(io).context("reading cpu.stat");
    /// let found = err.downcast_ref::<std::io::Error>().expect("io error kept");
    /// assert_eq!(found.kind(), std::io::ErrorKind::NotFound);
    /// ```
    #[must_use]
    pub fn downcast_ref<E: StdError + 'static>(&self) -> Option<&E> {
        let mut next = Some(self);
        while let Some(err) = next {
            if let Message::Foreign(foreign) = &err.0.msg {
                if let Some(hit) = foreign.downcast_ref::<E>() {
                    return Some(hit);
                }
                let mut deeper = foreign.source();
                while let Some(std_err) = deeper {
                    if let Some(hit) = std_err.downcast_ref::<E>() {
                        return Some(hit);
                    }
                    deeper = std_err.source();
                }
            }
            next = err.0.source.as_ref();
        }
        None
    }

    fn frame(&self) -> Frame<'_> {
        Frame {
            message: &self.0.msg,
            location: self.0.location,
            kind: Some(self.0.kind),
        }
    }

    /// The next frame down, whether it is ours or a foreign error's.
    fn next_link(&self) -> Option<Link<'_>> {
        if let Some(ours) = &self.0.source {
            return Some(Link::Ours(ours));
        }
        match &self.0.msg {
            // A foreign frame's own causes continue the chain.
            Message::Foreign(e) => e.source().map(Link::Std),
            Message::Text(_) => None,
        }
    }
}

impl fmt::Display for Error {
    /// `{}` is one line; `{:#}` is the whole chain.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write_chain(self, f)
        } else {
            write!(f, "{}", self.frame())
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_chain(self, f)
    }
}

fn write_chain(err: &Error, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut frames = err.chain();
    match frames.next() {
        Some(head) => write!(f, "{head}")?,
        None => return Ok(()),
    }
    for (i, frame) in frames.enumerate() {
        write!(f, "\n|- cause {} - {frame}", i + 1)?;
    }
    Ok(())
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self.next_link()? {
            Link::Ours(err) => Some(err),
            Link::Std(err) => Some(err),
        }
    }
}

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(err: std::io::Error) -> Self {
        Self(Box::new(Inner {
            kind: ErrorKind::Io,
            msg: Message::Foreign(Box::new(err)),
            location: Some(Location::caller()),
            source: None,
        }))
    }
}

/// One frame of an [`Error`] chain.
///
/// Frames the framework created have a location and a kind — including the ones
/// whose message is borrowed from a foreign error, such as those produced by
/// `From<std::io::Error>`. Frames reached by walking a foreign error's own
/// [`source`](StdError::source) chain have neither. This is also the shape the
/// engine flattens into `ErrorTrace` when an error crosses the network.
#[derive(Clone, Copy)]
pub struct Frame<'a> {
    message: &'a (dyn fmt::Display + 'a),
    location: Option<&'static Location<'static>>,
    kind: Option<ErrorKind>,
}

impl<'a> Frame<'a> {
    /// This frame's message, without location.
    #[must_use]
    pub fn message(&self) -> &'a (dyn fmt::Display + 'a) {
        self.message
    }

    /// Where the frame was created, if the framework created it.
    #[must_use]
    pub fn location(&self) -> Option<&'static Location<'static>> {
        self.location
    }

    /// The frame's kind, if the framework created it.
    #[must_use]
    pub fn kind(&self) -> Option<ErrorKind> {
        self.kind
    }
}

impl fmt::Display for Frame<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)?;
        if let Some(loc) = self.location {
            write!(f, " @ {}:{}", loc.file(), loc.line())?;
        }
        Ok(())
    }
}

impl fmt::Debug for Frame<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

/// Iterator over the frames of an [`Error`], outermost first.
///
/// Created by [`Error::chain`].
pub struct Chain<'a> {
    state: Option<Link<'a>>,
}

#[derive(Clone, Copy)]
enum Link<'a> {
    Ours(&'a Error),
    Std(&'a (dyn StdError + 'static)),
}

impl<'a> Iterator for Chain<'a> {
    type Item = Frame<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state.take()? {
            Link::Ours(err) => {
                self.state = err.next_link();
                Some(err.frame())
            }
            Link::Std(err) => {
                self.state = err.source().map(Link::Std);
                Some(Frame {
                    message: err,
                    location: None,
                    kind: None,
                })
            }
        }
    }
}

impl std::iter::FusedIterator for Chain<'_> {}

/// Add context to a `Result` whose error can become an [`Error`].
///
/// ```
/// # use cs_util::{Error, ErrorKind, ResultExt};
/// fn read() -> Result<String, std::io::Error> {
///     Err(std::io::ErrorKind::PermissionDenied.into())
/// }
///
/// let err = read().context("reading /sys/fs/cgroup/.../cpu.stat").unwrap_err();
/// assert_eq!(err.kind(), ErrorKind::Io);
/// ```
pub trait ResultExt<T>: Sized {
    /// Wrap the error with a message and the caller's location.
    fn context(self, msg: impl Into<String>) -> Result<T, Error>;

    /// Like [`context`](ResultExt::context), but only builds the message on the
    /// error path.
    fn with_context<S: Into<String>>(self, msg: impl FnOnce() -> S) -> Result<T, Error>;
}

impl<T, E: Into<Error>> ResultExt<T> for Result<T, E> {
    #[track_caller]
    fn context(self, msg: impl Into<String>) -> Result<T, Error> {
        match self {
            Ok(v) => Ok(v),
            // Inlined rather than calling `Error::context` so the location is
            // the caller's, not this line.
            Err(e) => {
                let inner = e.into();
                Err(Error(Box::new(Inner {
                    kind: inner.kind(),
                    msg: Message::Text(msg.into()),
                    location: Some(Location::caller()),
                    source: Some(inner),
                })))
            }
        }
    }

    #[track_caller]
    fn with_context<S: Into<String>>(self, msg: impl FnOnce() -> S) -> Result<T, Error> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => {
                let inner = e.into();
                Err(Error(Box::new(Inner {
                    kind: inner.kind(),
                    msg: Message::Text(msg().into()),
                    location: Some(Location::caller()),
                    source: Some(inner),
                })))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const _: () = assert!(size_of::<Error>() == size_of::<usize>());
    const _: () = assert!(size_of::<Result<(), Error>>() == size_of::<usize>());

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn error_is_send_sync_and_static() {
        assert_send_sync::<Error>();
    }

    #[test]
    fn display_is_one_line_with_location() {
        let err = Error::new(ErrorKind::Config, "no node name");
        let line = line!() - 1;
        let shown = err.to_string();
        assert_eq!(
            shown,
            format!("no node name @ crates/core/util/src/error.rs:{line}")
        );
    }

    #[test]
    fn context_inherits_kind_and_records_caller() {
        let err = Error::new(ErrorKind::Transport, "connection reset");
        let inner_line = line!() - 1;
        let err = err.context("sending batch");
        let outer_line = line!() - 1;

        assert_eq!(err.kind(), ErrorKind::Transport);
        assert!(err.is_retryable());
        assert_eq!(
            err.location().map(std::panic::Location::line),
            Some(outer_line)
        );

        let frames: Vec<_> = err.chain().collect();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].message().to_string(), "sending batch");
        assert_eq!(frames[1].message().to_string(), "connection reset");
        assert_eq!(frames[1].location().map(Location::line), Some(inner_line));
    }

    #[test]
    fn with_kind_only_changes_the_outer_frame() {
        let err = Error::new(ErrorKind::Io, "broken pipe")
            .context("writing frame")
            .with_kind(ErrorKind::Transport);
        assert!(err.is_retryable());
        let frames: Vec<_> = err.chain().collect();
        assert_eq!(frames[0].kind(), Some(ErrorKind::Transport));
        assert_eq!(frames[1].kind(), Some(ErrorKind::Io));
    }

    #[test]
    fn debug_and_alternate_display_render_the_tree() {
        let err = Error::new(ErrorKind::Plugin, "NVML initialization failed")
            .context("sampler factory failed")
            .context("failed to start service \"gpu\"");

        let rendered = format!("{err:?}");
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("failed to start service \"gpu\" @ crates/core/util/src/"));
        assert!(lines[1].starts_with("|- cause 1 - sampler factory failed @ "));
        assert!(lines[2].starts_with("|- cause 2 - NVML initialization failed @ "));
        assert_eq!(rendered, format!("{err:#}"));
        assert_ne!(rendered, format!("{err}"));
    }

    #[test]
    fn io_errors_convert_without_duplicating_their_message() {
        fn read() -> Result<(), Error> {
            Err(std::io::Error::from(std::io::ErrorKind::NotFound))?;
            Ok(())
        }
        let err = read().context("reading cpu.stat").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Io);
        assert!(!err.is_retryable());

        let frames: Vec<_> = err.chain().collect();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].message().to_string(), "reading cpu.stat");
        // The converted frame is still ours: it is located at the `?` and keeps
        // the kind `From` chose. Only the io error's text is borrowed.
        assert_eq!(frames[1].kind(), Some(ErrorKind::Io));
        assert!(frames[1].location().is_some());
        assert!(
            frames[1].message().to_string().contains("not found"),
            "unexpected: {}",
            frames[1].message()
        );
    }

    #[test]
    fn with_source_keeps_the_foreign_error_as_a_cause() {
        let err = "1x"
            .parse::<u32>()
            .map_err(|e| Error::with_source(ErrorKind::Config, "bad port", e))
            .unwrap_err();

        let frames: Vec<_> = err.chain().collect();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].message().to_string(), "bad port");
        assert_eq!(
            frames[1].message().to_string(),
            "invalid digit found in string"
        );
        assert_eq!(
            err.root_cause().message().to_string(),
            frames[1].message().to_string()
        );
    }

    #[test]
    fn chain_descends_into_nested_foreign_sources() {
        #[derive(Debug)]
        struct Outer(std::io::Error);

        impl fmt::Display for Outer {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("outer foreign")
            }
        }

        impl StdError for Outer {
            fn source(&self) -> Option<&(dyn StdError + 'static)> {
                Some(&self.0)
            }
        }

        let err = Error::foreign(
            ErrorKind::Plugin,
            Outer(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        )
        .context("loading plugin");

        let frames: Vec<_> = err.chain().collect();
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[1].message().to_string(), "outer foreign");
        assert!(
            frames[2]
                .message()
                .to_string()
                .contains("permission denied")
        );
        assert!(err.downcast_ref::<std::io::Error>().is_some());
        assert!(err.downcast_ref::<Outer>().is_some());
        assert!(err.downcast_ref::<std::fmt::Error>().is_none());
    }

    #[test]
    fn source_walks_the_same_chain_as_std() {
        let err = Error::new(ErrorKind::Decode, "short frame").context("reading hello");
        let mut count = 1;
        let mut cursor: Option<&(dyn StdError + 'static)> = err.source();
        while let Some(e) = cursor {
            count += 1;
            cursor = e.source();
        }
        assert_eq!(count, err.chain().count());
    }

    #[test]
    fn with_context_is_lazy() {
        let mut built = 0;
        let ok: Result<(), Error> = Ok(());
        let _ = ok.with_context(|| {
            built += 1;
            "unused"
        });
        assert_eq!(built, 0);

        let err: Result<(), Error> = Err(Error::new(ErrorKind::Timeout, "deadline"));
        let _ = err.with_context(|| {
            built += 1;
            "used"
        });
        assert_eq!(built, 1);
    }

    #[test]
    fn every_kind_round_trips_through_its_wire_name() {
        for kind in [
            ErrorKind::Io,
            ErrorKind::Transport,
            ErrorKind::Decode,
            ErrorKind::Config,
            ErrorKind::Timeout,
            ErrorKind::Rejected,
            ErrorKind::Shutdown,
            ErrorKind::Plugin,
        ] {
            assert_eq!(ErrorKind::parse(kind.as_str()), Some(kind));
            assert_eq!(kind.as_str().parse::<ErrorKind>().unwrap(), kind);
        }
    }

    /// A frame from another node claims no line in this build: it already carries
    /// the remote `file:line` in its message, and two locations for one failure is
    /// how a rebuilt chain stops reading like the original.
    #[test]
    fn a_remote_frame_has_no_local_location() {
        let err = Error::remote(
            ErrorKind::Rejected,
            "no node named \"node-6\" @ engine.rs:166",
        )
        .remote_context("forwarding a command @ peer.rs:512");
        assert!(err.location().is_none());
        assert!(err.chain().all(|frame| frame.location().is_none()));

        let shown = format!("{err:#}");
        assert_eq!(
            shown.matches(" @ ").count(),
            2,
            "one location per frame, the remote one: {shown}"
        );
        assert_eq!(
            err.kind(),
            ErrorKind::Rejected,
            "and the remote kind stands"
        );
    }

    #[test]
    fn an_unknown_kind_is_a_decode_error_not_a_new_kind() {
        assert_eq!(ErrorKind::parse("from-the-future"), None);
        let err = "from-the-future".parse::<ErrorKind>().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.to_string().contains("from-the-future"));
    }

    #[test]
    fn kind_strings_are_stable() {
        for (kind, name) in [
            (ErrorKind::Io, "io"),
            (ErrorKind::Transport, "transport"),
            (ErrorKind::Decode, "decode"),
            (ErrorKind::Config, "config"),
            (ErrorKind::Timeout, "timeout"),
            (ErrorKind::Rejected, "rejected"),
            (ErrorKind::Shutdown, "shutdown"),
            (ErrorKind::Plugin, "plugin"),
        ] {
            assert_eq!(kind.to_string(), name);
            assert_eq!(kind.as_str(), name);
        }
    }

    #[test]
    fn only_transport_and_timeout_retry() {
        let retryable: Vec<_> = [
            ErrorKind::Io,
            ErrorKind::Transport,
            ErrorKind::Decode,
            ErrorKind::Config,
            ErrorKind::Timeout,
            ErrorKind::Rejected,
            ErrorKind::Shutdown,
            ErrorKind::Plugin,
        ]
        .into_iter()
        .filter(|kind| kind.is_retryable())
        .collect();
        assert_eq!(retryable, [ErrorKind::Transport, ErrorKind::Timeout]);
    }
}
