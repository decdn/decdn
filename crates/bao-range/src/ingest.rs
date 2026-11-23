//! The pull-side store contract: what a gap-driven paid pull needs from a
//! store beyond [`RangedStore`]'s queries. It ingests a leg's raw bao while it
//! verifies it, and keeps a peer's typed fault ahead of the decoder's own
//! complaint.
//!
//! The contract lives here, beside [`RangedStore`], so a store and the pull
//! that fills it need not depend on each other: `decdn-client` drives these
//! traits, and both `decdn-client` (a download's store) and `decdn-cache` (the
//! node's cache) implement them.

use std::pin::Pin;
use std::sync::atomic::AtomicU64;

use crate::{AlignedRange, RangedStore};

/// A boxed, `Send` future returning an [`anyhow::Result`]: the async-trait
/// shape of [`IngestStore::flush_present_record`], and of the pull's sources
/// and funders that `decdn-client` defines.
pub type SourceFuture<'a, T> =
    Pin<Box<dyn std::future::Future<Output = anyhow::Result<T>> + Send + 'a>>;

/// A reader that may park a typed fault while it feeds a decoder.
///
/// When a pull's bytes stop arriving, the decoder sees only a truncated or
/// failed stream. If the reader knows WHY (a stalled peer, a mid-stream
/// refusal, a rejected voucher), that reason is what the caller needs in order
/// to score the peer, so it wins.
pub trait StashedFault {
    /// Take the parked typed fault, if any. Returns `None` on a source whose
    /// bytes carry the whole story (an in-memory buffer, a scripted double).
    fn take_fault(&mut self) -> Option<anyhow::Error>;

    /// The decoder failed on this source's bytes: they were corrupt, or the
    /// stream ended short of them. A paid pull books it as the node failing
    /// its delivery (ADR 003 § Credit Window); a source with no payment behind
    /// it has nothing to book.
    fn note_failed_delivery(&mut self) {}
}

/// A plain in-memory wire buffer has no out-of-band failure mode: whatever the
/// decoder says about it is the whole story.
///
/// Test-only, and gated to say so: it lets a streaming ingest loop be driven
/// from a fixed wire buffer. A test using it exercises the `None` branch, where
/// the decoder's error is the whole story, so it must not be the only coverage
/// of the fault-precedence rule.
#[cfg(any(test, feature = "test-util"))]
impl StashedFault for bytes::Bytes {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        None
    }
}

/// A reader over the raw interleaved bao encoding of one range.
///
/// The store's ingest decoder pulls bytes on demand through
/// [`iroh_io::AsyncStreamReader`]; a typed peer fault (stall, refusal, voucher
/// rejection) is preserved through [`StashedFault`] so it beats the decoder's
/// generic "bytes stopped arriving". Blanket-implemented, so any reader that is
/// both is a `BaoRangeReader` with no extra code.
pub trait BaoRangeReader: iroh_io::AsyncStreamReader + StashedFault + Send {}

impl<T> BaoRangeReader for T where T: iroh_io::AsyncStreamReader + StashedFault + Send {}

/// The future [`IngestStore::ingest_stream`] returns: the reader back, and how
/// the ingest ended.
pub type IngestFuture<'a, R> =
    Pin<Box<dyn std::future::Future<Output = anyhow::Result<(R, IngestEnd)>> + 'a>>;

/// How an [`IngestStore::ingest_stream`] ended without a fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestEnd {
    /// The decoder reached the end of the range: the source finishes the pull.
    Drained,
    /// The verified prefix reached the lowered `stop_at` before the end of the
    /// range: the source stops the pull.
    Stopped,
}

/// The store capability a gap-driven pull needs beyond [`RangedStore`]'s
/// queries: ingest one gap's raw bao.
///
/// Kept a generic method (not `dyn`) so an impl can stream any
/// [`BaoRangeReader`].
///
/// The returned future is intentionally NOT `Send`-bounded.
/// [`iroh_io::AsyncStreamReader`]'s methods are return-position-impl-trait-in-
/// trait with no `Send` bound on the trait itself, so a method generic over
/// `R: BaoRangeReader` can never prove its decode-loop future is `Send` for an
/// arbitrary `R`; only a concrete, non-generic instantiation could. A pull
/// only ever `.await`s this future in place, never spawns it across a task
/// boundary, so dropping `Send` here is behavior-preserving.
pub trait IngestStore: RangedStore {
    /// Decode-and-admit the raw bao bytes in `reader` as `range`'s content,
    /// verifying against the store's rooted hash as it streams. Returns the
    /// drained `reader` (its typed fault, if any, surfaces via
    /// [`StashedFault`] on the caller's copy) so the source can finish the
    /// pull.
    ///
    /// `claimed_total` is the blob size the leg's sender signs in its header.
    /// The sender serves `range` clamped to that size and encodes it under
    /// that size's tree, so a store keyed by offset verifies the leg under
    /// it.
    ///
    /// `stop_at` is an end the caller can lower while the leg streams. Once
    /// the verified prefix reaches it, the ingest checkpoints that prefix and
    /// returns [`IngestEnd::Stopped`] without reading further. After each
    /// verified group it reports the prefix through `on_progress` first, then
    /// reads `stop_at` with `SeqCst`. A caller that records the prefix with
    /// `SeqCst`, lowers `stop_at`, and then reads the recorded prefix with
    /// `SeqCst` knows where the leg stops: at the lowered end when the prefix
    /// read is short of it, and within one group past that prefix otherwise.
    /// A steal lowers it to the split, so the leg stops there on its open
    /// stream. A chunk-group-aligned end cuts the pre-order bao encoding of
    /// `range` between two items, so the stopped prefix verifies on its own.
    ///
    /// `on_progress` sees the content bytes verified on THIS range after each
    /// verified group.
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        claimed_total: u64,
        stop_at: Option<&'a AtomicU64>,
    ) -> IngestFuture<'a, R>
    where
        R: BaoRangeReader + 'a;

    /// Persist the store's current in-memory present-range snapshot to its
    /// durable record. The single-writer flush point: several
    /// `ingest_stream` calls can run concurrently on one store, so no
    /// checkpoint writes the record; callers flush it explicitly instead. A
    /// single-source pull calls this once after its gap loop, before
    /// `finalize`, so it keeps its resume durability without per-checkpoint
    /// fsyncs.
    ///
    /// The snapshot is taken when this is called; the returned future writes
    /// it. An implementation that touches the disk does so off the runtime
    /// workers, because the paid pulls that share the runtime pay from their
    /// decode loops. A store whose presence is its own bookkeeping has no
    /// record and returns at once.
    ///
    /// # Errors
    ///
    /// Any I/O failure persisting the record.
    fn flush_present_record(&self) -> SourceFuture<'_, ()>;

    /// The size a verified final chunk proved, if any leg has proved one. A
    /// store keyed by a fixed size knows that size from the start, so the
    /// default is [`total_bytes`](RangedStore::total_bytes). A pull completes
    /// once a size is proven and every byte below it is present.
    fn proven(&self) -> Option<u64> {
        Some(self.total_bytes())
    }

    /// Move the planner's bound ([`total_bytes`](RangedStore::total_bytes))
    /// to `bound`. A pull clamps a first size to the cap's reach this way
    /// while no size is proven. A store keyed by a fixed size keeps it, so
    /// the default does nothing.
    fn set_bound(&self, bound: u64) {
        let _ = bound;
    }

    /// Record `size` as the blob's proven size: a node's size proof verified
    /// it against the root before the leg that carried it opened. The
    /// planner's bound becomes it at once. A store keyed by a fixed size knows
    /// its size already, so the default does nothing.
    fn prove_size(&self, size: crate::ProvenSize) {
        let _ = size;
    }
}

/// A fill store a streaming reader reads back while the pull fills it: an
/// [`IngestStore`] that also wakes a waiting reader when an ingest checkpoint
/// makes more verified bytes present.
pub trait StreamStore: IngestStore + 'static {
    /// Woken each time an ingest checkpoint makes more verified bytes
    /// present. The reader registers on it before it reads the present
    /// frontier, so a checkpoint that lands between the read and the wait
    /// still wakes it.
    fn present_grew(&self) -> &tokio::sync::Notify;
}

/// The received bytes do not verify against the requested hash: the sender
/// delivered corrupt or wrong bytes. A typed marker, so fault classification
/// tells corruption from a truncated stream by type, not by message.
#[derive(Debug)]
pub struct HashMismatch;

impl std::fmt::Display for HashMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("received bytes do not match requested hash")
    }
}

impl std::error::Error for HashMismatch {}
