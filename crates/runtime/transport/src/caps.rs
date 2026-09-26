use std::fmt;

/// What a transport can and cannot do, so the engine can adapt instead of
/// assuming.
///
/// Read once at startup. Everything here is a property of the transport
/// implementation, not of an individual connection.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Capabilities {
    /// Short stable name — `"mock"`, `"tcp"`, `"grpc"`, `"rdma"`. Appears in logs
    /// and metric labels, so an operator can tell what a node is talking over.
    pub name: &'static str,

    /// Largest encoded frame this transport will carry, in bytes.
    ///
    /// The engine chunks data payloads to fit, using
    /// [`DataFrame::max_payload`](crate::DataFrame::max_payload) to account for
    /// envelope overhead.
    pub max_frame: usize,

    /// Whether the transport multiplexes the two lanes itself — separate streams,
    /// separate queue pairs.
    ///
    /// When false, the engine interleaves them into one stream and is responsible
    /// for draining control ahead of data. When true it may hand both to the
    /// transport and let it keep them apart.
    pub native_lanes: bool,

    /// Whether the transport can carry a payload without copying it, given a
    /// buffer it provided.
    ///
    /// Advisory: it tells the engine whether asking for a transport-supplied
    /// buffer is worth the trouble. Correctness never depends on it.
    pub zero_copy: bool,
}

impl Capabilities {
    /// Frame ceiling assumed when a transport has no opinion: 4 MiB.
    ///
    /// Large enough that ordinary metric batches are never chunked, small enough
    /// that a corrupt length prefix cannot make a peer allocate unboundedly.
    pub const DEFAULT_MAX_FRAME: usize = 4 * 1024 * 1024;

    /// Capabilities for a plain, single-stream, copying transport — the common
    /// case, and the least the engine can rely on.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            max_frame: Self::DEFAULT_MAX_FRAME,
            native_lanes: false,
            zero_copy: false,
        }
    }

    /// Set the maximum frame size.
    #[must_use]
    pub const fn with_max_frame(mut self, max_frame: usize) -> Self {
        self.max_frame = max_frame;
        self
    }

    /// Declare that the transport keeps the lanes apart itself.
    #[must_use]
    pub const fn with_native_lanes(mut self) -> Self {
        self.native_lanes = true;
        self
    }

    /// Declare that payloads can avoid a copy.
    #[must_use]
    pub const fn with_zero_copy(mut self) -> Self {
        self.zero_copy = true;
        self
    }
}

impl fmt::Display for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (max_frame {}", self.name, self.max_frame)?;
        if self.native_lanes {
            f.write_str(", native lanes")?;
        }
        if self.zero_copy {
            f.write_str(", zero copy")?;
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_is_the_least_capable_transport() {
        let caps = Capabilities::new("tcp");
        assert_eq!(caps.name, "tcp");
        assert_eq!(caps.max_frame, Capabilities::DEFAULT_MAX_FRAME);
        assert!(!caps.native_lanes, "assume nothing about multiplexing");
        assert!(!caps.zero_copy, "assume a copy");
        assert_eq!(caps.to_string(), "tcp (max_frame 4194304)");
    }

    #[test]
    fn a_more_capable_transport_declares_it() {
        let caps = Capabilities::new("rdma")
            .with_max_frame(64 * 1024)
            .with_native_lanes()
            .with_zero_copy();
        assert_eq!(caps.max_frame, 65536);
        assert!(caps.native_lanes);
        assert!(caps.zero_copy);
        assert_eq!(
            caps.to_string(),
            "rdma (max_frame 65536, native lanes, zero copy)"
        );
    }

    #[test]
    fn capabilities_are_const_constructible() {
        const MOCK: Capabilities = Capabilities::new("mock").with_max_frame(1024);
        assert_eq!(MOCK.max_frame, 1024);
    }
}
