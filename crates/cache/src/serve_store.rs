//! [`ServeStore`]: node-only extension of [`RangedStore`] for progressive
//! serve-while-filling (#1621 Task 4, B1 of the node serve-miss driver).
//!
//! `ServeStore` extends `bao_range::RangedStore` but is defined here, in
//! `cache`, rather than in the `bao-range` leaf crate: its two methods speak
//! iroh-blobs / `futures_util::Stream` types directly (a live present-ranges
//! watch and a re-encoded wire-bao stream), and `bao-range` stays free of an
//! iroh-blobs dependency (ADR 038). No routing calls this yet — the future
//! node serve-miss driver (B2) adopts it; its value today is the typed seam
//! plus the tests in Task 5.

use bytes::Bytes;
use decdn_bao_range::{RangedFuture, RangedStore, RangedStoreError};
use std::pin::Pin;

/// A live watch of which chunk ranges of a blob are present. The first item
/// is the current bitfield; further items arrive as the blob fills. Mirrors
/// `iroh_blobs::api::blobs::ObserveProgress::stream()` — NEVER
/// `await_completion`, which hangs on a partial blob.
pub type PresentRangeWatch =
    Pin<Box<dyn futures_util::Stream<Item = bao_tree::ChunkRanges> + Send>>;

/// A stream of wire-format bao chunks (header + proof + leaf bytes) for a
/// re-encoded byte range, each item fallible with the store's
/// [`RangedStoreError`].
pub type EncodeStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, RangedStoreError>> + Send>>;

/// Node-only extension of [`RangedStore`]: progressive serve-while-filling
/// primitives over an already-open per-blob store.
///
/// `observe` watches which ranges are present as a partial blob fills;
/// `encode_range` re-encodes whatever is currently held in a byte span into
/// `cdn/client/v1` wire bao (header + interleaved proof/leaves), the same
/// wire shape [`RangedStore::read`] does not produce.
pub trait ServeStore: RangedStore {
    /// Watch which chunk ranges are present, from now until the blob is
    /// complete.
    fn observe(&self) -> RangedFuture<'_, PresentRangeWatch>;

    /// Re-encode the currently-present bytes of
    /// `[byte_offset, byte_offset + byte_len)` into wire-format bao.
    fn encode_range(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, EncodeStream>;
}
