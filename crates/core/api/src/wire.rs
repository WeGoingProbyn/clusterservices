use bytes::{BufMut, Bytes};
use cs_util::{Error, ErrorKind, Result};

/// A message that can cross the wire.
///
/// Implemented automatically for every `prost` message, so a plugin whose
/// `build.rs` runs `prost-build` gets it for free on the generated types:
///
/// ```
/// use cs_api::Wire;
/// use bytes::BytesMut;
///
/// #[derive(Clone, PartialEq, prost::Message)]
/// struct CpuBatch {
///     #[prost(uint32, tag = "1")]
///     job_id: u32,
///     #[prost(uint64, repeated, tag = "2")]
///     usage_usec: Vec<u64>,
/// }
///
/// let batch = CpuBatch { job_id: 1234, usage_usec: vec![10, 21, 33] };
///
/// // What the engine does with it: reserve, encode, send the bytes.
/// let mut buf = BytesMut::with_capacity(batch.encoded_len());
/// batch.encode(&mut buf);
/// assert_eq!(CpuBatch::decode(buf.freeze()).unwrap(), batch);
/// ```
///
/// Encoding is deliberately synchronous and infallible: it happens on the
/// worker thread that produced the message, into a buffer the transport
/// supplied (possibly registered memory for RDMA).
pub trait Wire: Sized + Send + 'static {
    /// Exact number of bytes [`encode`](Wire::encode) will write.
    fn encoded_len(&self) -> usize;

    /// Write the message into `buf`.
    ///
    /// The caller must have reserved at least [`encoded_len`](Wire::encoded_len)
    /// bytes. Writing more than that, or being handed less, is an engine bug and
    /// may panic.
    fn encode(&self, buf: &mut dyn BufMut);

    /// Read a message back. `buf` holds exactly one message's bytes.
    fn decode(buf: Bytes) -> Result<Self>;
}

impl<T: prost::Message + Default + 'static> Wire for T {
    fn encoded_len(&self) -> usize {
        prost::Message::encoded_len(self)
    }

    fn encode(&self, buf: &mut dyn BufMut) {
        // `bytes` implements `BufMut` for `&mut T where T: BufMut + ?Sized`, so
        // the trait object satisfies prost's generic parameter as-is — no
        // adapter, and nothing `unsafe`, needed on this path.
        //
        // `encode_raw` skips the capacity check that `encode` does; the contract
        // above puts that check on the caller, which has already asked for
        // `encoded_len()` bytes.
        prost::Message::encode_raw(self, &mut { buf });
    }

    fn decode(buf: Bytes) -> Result<Self> {
        <T as prost::Message>::decode(buf)
            .map_err(|e| Error::with_source(ErrorKind::Decode, "malformed message", e))
    }
}

/// The object-safe half of [`Wire`].
///
/// [`Wire`] is `Sized` (it has to be, for `decode`), so the engine cannot hold a
/// `dyn Wire`. This is what it holds instead: enough to ask a plugin's message
/// how big it is and to write it into a transport buffer, without naming its
/// type. Blanket-implemented for every [`Wire`] type; plugins never implement it
/// and rarely name it.
pub trait Encodable: Send {
    /// See [`Wire::encoded_len`].
    fn encoded_len(&self) -> usize;

    /// See [`Wire::encode`].
    fn encode(&self, buf: &mut dyn BufMut);
}

impl<T: Wire> Encodable for T {
    fn encoded_len(&self) -> usize {
        Wire::encoded_len(self)
    }

    fn encode(&self, buf: &mut dyn BufMut) {
        Wire::encode(self, buf);
    }
}

/// The command type of a service that takes no custom commands.
///
/// Uninhabited, so `Command::Custom(..)` cannot be constructed for such a
/// service and its `on_command` only ever sees the built-ins:
///
/// ```
/// use cs_api::{Command, NoCommand};
///
/// let cmd: Command<NoCommand> = Command::Shutdown;
/// match cmd {
///     Command::Shutdown | Command::Restart => {}
///     // Unreachable, and the compiler knows it.
///     Command::Custom(never) => match never {},
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NoCommand {}

impl Wire for NoCommand {
    fn encoded_len(&self) -> usize {
        match *self {}
    }

    fn encode(&self, _buf: &mut dyn BufMut) {
        match *self {}
    }

    fn decode(_buf: Bytes) -> Result<Self> {
        Err(Error::new(
            ErrorKind::Decode,
            "service accepts no custom commands",
        ))
    }
}

impl std::fmt::Display for NoCommand {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    /// Round-trip through the `&mut dyn BufMut` path a transport would use.
    fn round_trip<T: Wire + PartialEq + std::fmt::Debug>(msg: &T) -> T {
        let mut buf = BytesMut::with_capacity(msg.encoded_len());
        {
            let sink: &mut dyn BufMut = &mut buf;
            msg.encode(sink);
        }
        assert_eq!(
            buf.len(),
            msg.encoded_len(),
            "encoded_len must match what encode writes"
        );
        T::decode(buf.freeze()).expect("decode")
    }

    #[test]
    fn prost_messages_get_wire_for_free() {
        // `String` is a prost message (a scalar-only message in prost's model),
        // which keeps this test free of a build.rs.
        let msg = String::from("node-0042");
        assert_eq!(round_trip(&msg), msg);
    }

    #[test]
    fn encoding_through_a_trait_object_matches_prost() {
        let msg = String::from("cpu.stat");
        let mut via_dyn = BytesMut::new();
        Wire::encode(&msg, &mut via_dyn as &mut dyn BufMut);
        let direct = prost::Message::encode_to_vec(&msg);
        assert_eq!(via_dyn.as_ref(), direct.as_slice());
    }

    #[test]
    fn decode_failure_is_a_decode_error() {
        // Field 1 with wire type 7, which does not exist.
        let err = <u32 as Wire>::decode(Bytes::from_static(&[0x0f, 0x01])).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(!err.is_retryable());
        assert!(
            err.chain().count() >= 2,
            "the prost error should be kept as a cause"
        );
    }

    #[test]
    fn encodable_erases_the_message_type() {
        let msg = String::from("gpu");
        let erased: &dyn Encodable = &msg;
        let mut buf = BytesMut::with_capacity(erased.encoded_len());
        erased.encode(&mut buf);
        assert_eq!(buf.len(), Wire::encoded_len(&msg));
        assert_eq!(<String as Wire>::decode(buf.freeze()).unwrap(), msg);
    }

    #[test]
    fn no_command_never_decodes() {
        let err = NoCommand::decode(Bytes::new()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Decode);
    }

    #[test]
    fn no_command_is_uninhabited() {
        assert_eq!(size_of::<NoCommand>(), 0);
        assert_eq!(size_of::<Option<NoCommand>>(), 0);
    }
}
