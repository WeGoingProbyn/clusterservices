use std::fmt;

use cs_util::{Error, ErrorKind, Result};

use crate::frame::decode_error;
use crate::proto;

/// A [`cs_util::Error`] chain flattened for the wire.
///
/// A command that fails on a node fails somewhere specific, several layers deep,
/// and an operator running `cs-ctl` needs to see that — not "command failed".
/// Locations cannot survive a round trip as real
/// [`Location`](std::panic::Location)s, since those must be `&'static`, so
/// [`to_error`](ErrorTrace::to_error) folds each remote `file:line` into its
/// message text. The rebuilt chain then prints under the local one, and the
/// remote trace reads exactly as it did on the node that produced it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ErrorTrace {
    /// Node the error happened on.
    pub node: String,
    /// How to react, from the outermost frame of the original error.
    pub kind: ErrorKind,
    /// Outermost frame first, matching [`Error::chain`] order.
    pub frames: Vec<TraceFrame>,
}

impl ErrorTrace {
    /// A trace with explicit frames. Mostly useful in tests; prefer
    /// [`from_error`](ErrorTrace::from_error).
    #[must_use]
    pub fn new(node: impl Into<String>, kind: ErrorKind, frames: Vec<TraceFrame>) -> Self {
        Self {
            node: node.into(),
            kind,
            frames,
        }
    }

    /// Flatten an error that happened on `node`.
    #[must_use]
    pub fn from_error(node: impl Into<String>, error: &Error) -> Self {
        Self {
            node: node.into(),
            kind: error.kind(),
            frames: error
                .chain()
                .map(|frame| TraceFrame {
                    message: frame.message().to_string(),
                    file: frame
                        .location()
                        .map(|l| l.file().to_owned())
                        .unwrap_or_default(),
                    line: frame.location().map_or(0, std::panic::Location::line),
                })
                .collect(),
        }
    }

    /// Rebuild an [`Error`] with the remote chain intact.
    ///
    /// The result carries the remote [`kind`](ErrorTrace::kind), so a remote
    /// [`Timeout`](ErrorKind::Timeout) is still retryable here. Each frame's
    /// original `file:line` is appended to its message, since it cannot be a real
    /// location on this side.
    #[must_use]
    pub fn to_error(&self) -> Error {
        let mut frames = self.frames.iter().rev();
        let Some(innermost) = frames.next() else {
            return Error::remote(
                self.kind,
                format!("remote error on {} with no detail", self.node),
            );
        };

        // `Error::remote`, not `Error::new`: these frames happened on another node
        // and each already carries its own `file:line` in its message, so claiming
        // this line as well would print two locations for one failure.
        let mut error = Error::remote(self.kind, innermost.to_string());
        for frame in frames {
            error = error.remote_context(frame.to_string());
        }
        error
    }

    /// The outermost message, for a one-line log.
    #[must_use]
    pub fn summary(&self) -> String {
        match self.frames.first() {
            Some(frame) => format!("{} on {}", frame.message, self.node),
            None => format!("remote error on {}", self.node),
        }
    }

    pub(crate) fn into_proto(self) -> proto::ErrorTrace {
        proto::ErrorTrace {
            node: self.node,
            kind: self.kind.as_str().to_owned(),
            frames: self
                .frames
                .into_iter()
                .map(|frame| proto::TraceFrame {
                    message: frame.message,
                    file: frame.file,
                    line: frame.line,
                })
                .collect(),
        }
    }

    pub(crate) fn try_from_proto(trace: proto::ErrorTrace) -> Result<Self> {
        if trace.node.is_empty() {
            return Err(decode_error("error trace has no node"));
        }
        // An unrecognised kind means the peer is newer than we are. Keep the
        // message intact and fall back to a kind that will not make the engine
        // retry something it does not understand.
        let kind = ErrorKind::parse(&trace.kind).unwrap_or(ErrorKind::Plugin);
        let mut frames: Vec<_> = trace
            .frames
            .into_iter()
            .map(|frame| TraceFrame {
                message: frame.message,
                file: frame.file,
                line: frame.line,
            })
            .collect();
        if ErrorKind::parse(&trace.kind).is_none() && !trace.kind.is_empty() {
            frames.insert(
                0,
                TraceFrame::bare(format!("remote error kind {:?}", trace.kind)),
            );
        }
        Ok(Self {
            node: trace.node,
            kind,
            frames,
        })
    }
}

impl fmt::Display for ErrorTrace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.summary())
    }
}

/// One frame of a flattened error chain.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TraceFrame {
    /// The frame's message.
    pub message: String,
    /// Source file, or empty for a frame that came from inside a foreign error's
    /// own source chain.
    pub file: String,
    /// Line in `file`, or 0 when `file` is empty.
    pub line: u32,
}

impl TraceFrame {
    /// A frame with a source location.
    #[must_use]
    pub fn located(message: impl Into<String>, file: impl Into<String>, line: u32) -> Self {
        Self {
            message: message.into(),
            file: file.into(),
            line,
        }
    }

    /// A frame with no location.
    #[must_use]
    pub fn bare(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            file: String::new(),
            line: 0,
        }
    }

    /// Whether this frame knows where it came from.
    #[must_use]
    pub fn has_location(&self) -> bool {
        !self.file.is_empty()
    }
}

impl fmt::Display for TraceFrame {
    /// `message @ file:line`, matching how a local error prints one frame.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)?;
        if self.has_location() {
            write!(f, " @ {}:{}", self.file, self.line)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cs_util::ResultExt;

    fn remote_failure() -> Error {
        let io = std::io::Error::from(std::io::ErrorKind::NotFound);
        Err::<(), _>(io)
            .context("NVML initialization failed")
            .context("sampler factory failed")
            .unwrap_err()
            .with_kind(ErrorKind::Plugin)
    }

    #[test]
    fn a_trace_keeps_every_frame_of_the_original_chain() {
        let error = remote_failure();
        let trace = ErrorTrace::from_error("node-7", &error);

        assert_eq!(trace.node, "node-7");
        assert_eq!(trace.kind, ErrorKind::Plugin);
        assert_eq!(trace.frames.len(), error.chain().count());
        assert_eq!(trace.frames[0].message, "sampler factory failed");
        assert!(trace.frames[0].has_location());
        assert!(trace.frames[0].file.ends_with("trace.rs"));
    }

    #[test]
    fn a_trace_survives_the_wire_and_rebuilds_as_an_error() {
        let trace = ErrorTrace::from_error("node-7", &remote_failure());
        let decoded = ErrorTrace::try_from_proto(trace.clone().into_proto()).expect("decode trace");
        assert_eq!(decoded, trace);

        let rebuilt = decoded.to_error();
        assert_eq!(rebuilt.kind(), ErrorKind::Plugin);
        assert_eq!(rebuilt.chain().count(), trace.frames.len());

        // The remote locations are still readable, folded into the messages.
        let rendered = format!("{rebuilt:?}");
        assert!(rendered.starts_with("sampler factory failed @ "));
        assert!(rendered.contains("trace.rs:"));
        assert!(rendered.contains("|- cause 1 - NVML initialization failed @ "));
        assert!(rendered.contains("entity not found"));
    }

    #[test]
    fn the_remote_kind_decides_whether_the_engine_retries() {
        let timeout = Error::new(ErrorKind::Timeout, "deadline on the far side");
        let rebuilt = ErrorTrace::from_error("node-7", &timeout).to_error();
        assert!(rebuilt.is_retryable());

        let config = Error::new(ErrorKind::Config, "bad interval");
        let rebuilt = ErrorTrace::from_error("node-7", &config).to_error();
        assert!(!rebuilt.is_retryable());
    }

    #[test]
    fn an_unknown_remote_kind_is_kept_verbatim_instead_of_guessed() {
        let mut wire = ErrorTrace::from_error("node-7", &remote_failure()).into_proto();
        wire.kind = "quantum".into();

        let decoded = ErrorTrace::try_from_proto(wire).expect("decode");
        assert_eq!(
            decoded.kind,
            ErrorKind::Plugin,
            "falls back, does not retry"
        );
        assert_eq!(decoded.frames[0].message, "remote error kind \"quantum\"");
        assert!(decoded.to_error().to_string().contains("quantum"));
    }

    #[test]
    fn an_empty_trace_still_produces_a_usable_error() {
        let trace = ErrorTrace::new("node-7", ErrorKind::Transport, Vec::new());
        let error = trace.to_error();
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(error.to_string().contains("node-7"));
        assert_eq!(trace.summary(), "remote error on node-7");
    }

    #[test]
    fn a_trace_without_a_node_is_rejected() {
        let mut wire = ErrorTrace::from_error("node-7", &remote_failure()).into_proto();
        wire.node = String::new();
        let err = ErrorTrace::try_from_proto(wire).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
    }

    #[test]
    fn summary_names_the_node_and_the_outermost_message() {
        let trace = ErrorTrace::from_error("node-7", &remote_failure());
        assert_eq!(trace.summary(), "sampler factory failed on node-7");
        assert_eq!(trace.to_string(), trace.summary());
    }

    #[test]
    fn frames_without_a_location_print_without_one() {
        let bare = TraceFrame::bare("libnvidia-ml.so.1: cannot open shared object file");
        assert!(!bare.has_location());
        assert_eq!(
            bare.to_string(),
            "libnvidia-ml.so.1: cannot open shared object file"
        );
        assert_eq!(
            TraceFrame::located("boom", "gpu.rs", 19).to_string(),
            "boom @ gpu.rs:19"
        );
    }
}
