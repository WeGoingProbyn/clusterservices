use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use cs_transport::{Chunk, DataFrame};
use cs_util::{Error, ErrorKind, Result};

/// Split a payload into as many frames as the transport's frame ceiling requires.
///
/// Returns the chunk metadata to attach to each piece, alongside the piece. A
/// payload that fits in one frame produces a single entry with no metadata, which
/// is the overwhelmingly common case and costs nothing extra on the wire.
///
/// The per-chunk limit is computed with **worst-case** chunk numbers rather than
/// the real ones: a chunk's metadata is varint-encoded, so its size depends on its
/// own values, and sizing against the real ones is circular — the count depends on
/// the limit, which depends on the count. Assuming the widest possible numbers
/// wastes a handful of bytes per frame and removes the circularity.
pub(crate) fn split(
    service: &str,
    payload: Bytes,
    max_frame: usize,
) -> Result<Vec<(Option<Chunk>, Bytes)>> {
    if payload.len() <= DataFrame::max_payload(max_frame, service, None) {
        return Ok(vec![(None, payload)]);
    }

    let widest = Chunk::new(u64::MAX, u32::MAX - 1, u32::MAX);
    let limit = DataFrame::max_payload(max_frame, service, Some(widest));
    if limit == 0 {
        return Err(Error::new(
            ErrorKind::Config,
            format!(
                "the transport's max_frame of {max_frame} leaves no room for {service} data, \
                 even one byte of it"
            ),
        ));
    }

    let count = payload.len().div_ceil(limit);
    let count = u32::try_from(count).map_err(|_| {
        Error::new(
            ErrorKind::Config,
            format!(
                "a {} byte {service} message needs more than {} chunks",
                payload.len(),
                u32::MAX
            ),
        )
    })?;

    let mut pieces = Vec::with_capacity(count as usize);
    let mut rest = payload;
    // `message_id` is filled in by the caller, which owns the per-peer sequence.
    for index in 0..count {
        let take = limit.min(rest.len());
        let piece = rest.split_to(take);
        pieces.push((Some(Chunk::new(0, index, count)), piece));
    }
    Ok(pieces)
}

/// Rebuilds messages that arrived in pieces.
///
/// One of these per peer. Chunks of a message arrive in order on one lane, so
/// anything out of order means the peer is wrong and the partial message is
/// abandoned rather than patched up.
///
/// Bounded twice over: at most `max_partial` messages at a time, and each is at
/// most the transport's frame size per chunk — so a peer can never make this
/// process hold an unbounded amount.
#[derive(Debug)]
pub(crate) struct Reassembler {
    partials: HashMap<(String, u64), Partial>,
    max_partial: usize,
    next_seq: u64,
}

#[derive(Debug)]
struct Partial {
    /// Insertion order, so the oldest can be evicted without keeping a second
    /// structure.
    seq: u64,
    next_index: u32,
    count: u32,
    bytes: BytesMut,
}

/// What a chunk did to the reassembler.
#[derive(Debug)]
pub(crate) enum Reassembled {
    /// The message is complete.
    Complete(Bytes),
    /// More chunks are needed.
    Partial,
    /// The chunk did not belong to anything: the peer skipped, repeated, or
    /// contradicted itself. The partial message it referred to is gone.
    Rejected(&'static str),
}

impl Reassembler {
    pub(crate) fn new(max_partial: usize) -> Self {
        Self {
            partials: HashMap::new(),
            max_partial: max_partial.max(1),
            next_seq: 0,
        }
    }

    /// Feed in one chunk.
    pub(crate) fn push(&mut self, service: &str, chunk: Chunk, payload: Bytes) -> Reassembled {
        if chunk.count == 0 || chunk.index >= chunk.count {
            return Reassembled::Rejected("chunk index is not within its count");
        }
        let key = (service.to_owned(), chunk.message_id);

        if chunk.index == 0 {
            // A restart of the same message id replaces whatever was there.
            self.evict_if_full();
            let seq = self.next_seq;
            self.next_seq += 1;
            self.partials.insert(
                key.clone(),
                Partial {
                    seq,
                    next_index: 0,
                    count: chunk.count,
                    bytes: BytesMut::new(),
                },
            );
        }

        let Some(partial) = self.partials.get_mut(&key) else {
            return Reassembled::Rejected("chunk arrived without a first chunk");
        };
        if partial.count != chunk.count {
            self.partials.remove(&key);
            return Reassembled::Rejected("chunk count changed mid-message");
        }
        if partial.next_index != chunk.index {
            self.partials.remove(&key);
            return Reassembled::Rejected("chunk arrived out of order");
        }

        partial.bytes.extend_from_slice(&payload);
        partial.next_index += 1;

        if partial.next_index == partial.count {
            // `expect` is unreachable: we hold a mutable borrow of this entry.
            let partial = self
                .partials
                .remove(&key)
                .unwrap_or_else(|| unreachable!("entry was just borrowed"));
            Reassembled::Complete(partial.bytes.freeze())
        } else {
            Reassembled::Partial
        }
    }

    /// How many messages are half-arrived.
    #[cfg(test)]
    pub(crate) fn pending(&self) -> usize {
        self.partials.len()
    }

    fn evict_if_full(&mut self) {
        while self.partials.len() >= self.max_partial {
            let Some(oldest) = self
                .partials
                .iter()
                .min_by_key(|(_, partial)| partial.seq)
                .map(|(key, _)| key.clone())
            else {
                return;
            };
            self.partials.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cs_transport::{Frame, Lane};

    fn payload(len: usize) -> Bytes {
        Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<_>>())
    }

    /// Split, frame, encode, decode, reassemble — the whole path a big message
    /// takes.
    fn round_trip(service: &str, payload: Bytes, max_frame: usize) -> (Bytes, usize) {
        let pieces = split(service, payload, max_frame).expect("split");
        let mut reassembler = Reassembler::new(8);
        let mut frames = 0;
        let mut complete = None;

        for (chunk, piece) in pieces {
            let frame = match chunk {
                Some(chunk) => Frame::Data(DataFrame::chunked(
                    service,
                    1,
                    Chunk::new(77, chunk.index, chunk.count),
                    piece,
                )),
                None => Frame::Data(DataFrame::new(service, 1, piece)),
            };
            assert_eq!(frame.lane(), Lane::Data);

            let encoded = frame.encode();
            assert!(
                encoded.len() <= max_frame,
                "a chunk encoded to {} bytes, over the {max_frame} ceiling",
                encoded.len()
            );
            frames += 1;

            let Frame::Data(data) = Frame::decode(encoded).expect("decode") else {
                panic!("expected data");
            };
            match data.chunk {
                None => complete = Some(data.payload),
                Some(chunk) => match reassembler.push(service, chunk, data.payload) {
                    Reassembled::Complete(bytes) => complete = Some(bytes),
                    Reassembled::Partial => {}
                    Reassembled::Rejected(why) => panic!("rejected: {why}"),
                },
            }
        }
        (complete.expect("a complete message"), frames)
    }

    #[test]
    fn a_payload_that_fits_is_sent_whole() {
        let pieces = split("cgroup", payload(100), 4096).expect("split");
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].0, None, "no chunk metadata when none is needed");
    }

    #[test]
    fn an_oversized_payload_survives_the_round_trip() {
        for (size, max_frame) in [
            (10_000, 1024),
            (10_000, 256),
            (1_000_000, 64 * 1024),
            (300, 128),
        ] {
            let original = payload(size);
            let (rebuilt, frames) = round_trip("cgroup", original.clone(), max_frame);
            assert_eq!(rebuilt, original, "{size} bytes over {max_frame} frames");
            assert!(frames > 1, "{size} over {max_frame} should have chunked");
        }
    }

    #[test]
    fn a_payload_one_byte_over_the_limit_chunks_into_exactly_two() {
        let limit = DataFrame::max_payload(1024, "cgroup", None);
        let pieces = split("cgroup", payload(limit + 1), 1024).expect("split");
        assert_eq!(pieces.len(), 2);
        assert_eq!(pieces[0].0.expect("chunk").count, 2);
        assert_eq!(pieces[1].0.expect("chunk").index, 1);
        assert!(pieces[1].0.expect("chunk").is_last());
    }

    #[test]
    fn every_chunk_is_full_except_the_last() {
        let pieces = split("cgroup", payload(5000), 1024).expect("split");
        let sizes: Vec<_> = pieces.iter().map(|(_, piece)| piece.len()).collect();
        let (last, full) = sizes.split_last().expect("at least one");
        assert!(
            full.iter().all(|&len| len == full[0]),
            "leading chunks should be the same size: {sizes:?}"
        );
        assert!(*last <= full[0]);
        assert_eq!(sizes.iter().sum::<usize>(), 5000);
    }

    #[test]
    fn a_frame_ceiling_too_small_for_any_payload_is_a_config_error() {
        let err = split("a-very-long-service-name", payload(1000), 8).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Config);
        assert!(!err.is_retryable(), "the transport will never grow");
    }

    #[test]
    fn chunks_out_of_order_are_rejected_and_the_partial_is_dropped() {
        let mut reassembler = Reassembler::new(8);
        assert!(matches!(
            reassembler.push("cgroup", Chunk::new(1, 0, 3), payload(10)),
            Reassembled::Partial
        ));
        assert_eq!(reassembler.pending(), 1);

        // Skipping index 1 abandons the whole message rather than guessing.
        let rejected = reassembler.push("cgroup", Chunk::new(1, 2, 3), payload(10));
        assert!(matches!(rejected, Reassembled::Rejected(_)));
        assert_eq!(reassembler.pending(), 0);
    }

    #[test]
    fn a_chunk_with_no_beginning_is_rejected() {
        let mut reassembler = Reassembler::new(8);
        let rejected = reassembler.push("cgroup", Chunk::new(1, 1, 2), payload(10));
        assert!(matches!(rejected, Reassembled::Rejected(_)));
        assert_eq!(reassembler.pending(), 0);
    }

    #[test]
    fn a_count_that_changes_mid_message_is_rejected() {
        let mut reassembler = Reassembler::new(8);
        reassembler.push("cgroup", Chunk::new(1, 0, 3), payload(10));
        let rejected = reassembler.push("cgroup", Chunk::new(1, 1, 9), payload(10));
        assert!(matches!(rejected, Reassembled::Rejected(_)));
    }

    #[test]
    fn nonsensical_chunk_numbers_are_rejected() {
        let mut reassembler = Reassembler::new(8);
        for bad in [Chunk::new(1, 0, 0), Chunk::new(1, 5, 5)] {
            assert!(matches!(
                reassembler.push("cgroup", bad, payload(10)),
                Reassembled::Rejected(_)
            ));
        }
    }

    #[test]
    fn messages_from_different_services_reassemble_independently() {
        let mut reassembler = Reassembler::new(8);
        reassembler.push("cgroup", Chunk::new(1, 0, 2), Bytes::from_static(b"aa"));
        reassembler.push("gpu", Chunk::new(1, 0, 2), Bytes::from_static(b"bb"));
        assert_eq!(reassembler.pending(), 2, "same id, different services");

        let cgroup = reassembler.push("cgroup", Chunk::new(1, 1, 2), Bytes::from_static(b"cc"));
        let Reassembled::Complete(bytes) = cgroup else {
            panic!("expected a complete message");
        };
        assert_eq!(bytes, Bytes::from_static(b"aacc"));
        assert_eq!(reassembler.pending(), 1);
    }

    #[test]
    fn a_peer_cannot_make_us_hold_more_than_the_configured_partials() {
        let mut reassembler = Reassembler::new(4);
        // Start 100 messages and finish none of them.
        for id in 0..100 {
            reassembler.push("cgroup", Chunk::new(id, 0, 2), payload(64));
            assert!(
                reassembler.pending() <= 4,
                "pending grew to {}",
                reassembler.pending()
            );
        }
        assert_eq!(reassembler.pending(), 4);

        // The most recent ones are the ones kept.
        let recent = reassembler.push("cgroup", Chunk::new(99, 1, 2), payload(64));
        assert!(matches!(recent, Reassembled::Complete(_)));
        let evicted = reassembler.push("cgroup", Chunk::new(0, 1, 2), payload(64));
        assert!(matches!(evicted, Reassembled::Rejected(_)));
    }

    #[test]
    fn restarting_a_message_id_replaces_the_abandoned_attempt() {
        let mut reassembler = Reassembler::new(4);
        reassembler.push("cgroup", Chunk::new(1, 0, 3), Bytes::from_static(b"old"));
        reassembler.push("cgroup", Chunk::new(1, 0, 2), Bytes::from_static(b"new"));
        assert_eq!(reassembler.pending(), 1);

        let done = reassembler.push("cgroup", Chunk::new(1, 1, 2), Bytes::from_static(b"er"));
        let Reassembled::Complete(bytes) = done else {
            panic!("expected a complete message");
        };
        assert_eq!(bytes, Bytes::from_static(b"newer"), "the retry won");
    }
}
