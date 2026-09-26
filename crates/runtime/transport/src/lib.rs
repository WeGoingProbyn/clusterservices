//! The transport layer: what crosses a connection, and how.
//!
//! Two halves, kept apart on purpose:
//!
//! - **[`Frame`] and the frame protocol** — the vocabulary two peers share. A
//!   [`Frame`] is one unit of transfer: service [`Data`](Frame::Data), a
//!   [`Hello`], a [`Command`](Frame::Command), a [`CommandResult`], a
//!   [`Goodbye`], a [`Heartbeat`]. Which of the two [`Lane`]s a frame travels on
//!   follows from its variant, so the lane and the body can never disagree.
//! - **The [`Transport`] trait family** — [`Transport`] dials and listens,
//!   [`Connection`] splits into [`FrameTx`] and [`FrameRx`]. That is all. No
//!   service routing, no retries, no command matching, no shutdown ordering:
//!   those live in the engine, written once, so adding a transport is a day's
//!   work rather than a rewrite.
//!
//! ```
//! use bytes::Bytes;
//! use cs_transport::{DataFrame, Frame, Lane};
//!
//! let frame = Frame::Data(DataFrame::new("cgroup", 2, Bytes::from_static(b"...")));
//! assert_eq!(frame.lane(), Lane::Data);
//!
//! let bytes = frame.clone().encode();
//! assert_eq!(Frame::decode(bytes)?, frame);
//! # Ok::<(), cs_util::Error>(())
//! ```
//!
//! # Wire compatibility
//!
//! [`PROTOCOL_VERSION`] is sent in every [`Hello`] and bumped only for a change an
//! older peer cannot decode. Adding a field or an enum member is not such a
//! change: every enum keeps an `_UNSPECIFIED = 0` member, and the decoder rejects
//! both that and any value it does not recognise, so a newer peer's frame is
//! refused explicitly rather than half-understood. All validation that a
//! structurally impossible frame can fail — no body, no node name, a chunk index
//! past its count — raises [`ErrorKind::Decode`](cs_util::ErrorKind::Decode),
//! which tells the engine "this peer is wrong", not "retry".
//!
//! # Errors on the wire
//!
//! A command that fails on a node comes back as
//! [`Outcome::Failed`] carrying an [`ErrorTrace`] — the node's whole
//! [`cs_util::Error`] chain, flattened. The server rebuilds it with
//! [`ErrorTrace::to_error`], so `cs-ctl` prints the remote trace underneath the
//! local one.

mod caps;
mod endpoint;
mod frame;
mod trace;
mod traits;

/// The wire schema, generated from `proto/frame.proto`.
///
/// Public because a transport that speaks protobuf natively streams these types
/// directly — the gRPC transport's `rpc Session(stream Frame)`. Everything else
/// should use the hand-written types at the crate root, which validate on decode
/// and cannot represent a frame that makes no sense.
pub mod proto {
    #![allow(missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/cs.frame.v1.rs"));
}

pub use caps::Capabilities;
pub use endpoint::Endpoint;
pub use frame::{
    Chunk, CommandFrame, CommandId, CommandKind, CommandResult, DataFrame, Frame, Goodbye,
    GoodbyeReason, Heartbeat, Hello, Lane, Outcome, PROTOCOL_VERSION, ServiceInfo,
};
pub use trace::{ErrorTrace, TraceFrame};
pub use traits::{Connection, FrameRx, FrameTx, Listener, Transport};
