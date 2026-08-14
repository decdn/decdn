//! [`ServeStore`]: node-only extension of [`RangedStore`] for progressive
//! serve-while-filling (#1621).
//!
//! `ServeStore` extends `bao_range::RangedStore` but is defined here, in
//! `cache`, rather than in the `bao-range` leaf crate: its two methods speak
//! iroh-blobs / `futures_util::Stream` types directly (a live present-ranges
//! watch and a re-encoded wire-bao stream), and `bao-range` stays free of an
//! iroh-blobs dependency (ADR 038). The node serve-miss serve path
//! (`crates/node/src/handlers/client/serve_encoder.rs`) consumes it, observing
//! the store's present ranges as the blob fills; `ServeStore` is the typed seam
//! that keeps those iroh-blobs types out of `bao-range`.

use bytes::Bytes;
use decdn_bao_range::{RangedFuture, RangedStore, RangedStoreError};
use std::pin::Pin;

/// A live watch of which chunk ranges of a blob are present. The first item
/// is the current bitfield; further items arrive as the blob fills. Mirrors
/// `iroh_blobs::api::blobs::ObserveProgress::stream()` — NEVER
/// `await_completion`, which hangs on a partial blob.
pub type PresentRangeWatch =
    Pin<Box<dyn futures_util::Stream<Item = bao_tree::ChunkRanges> + Send>>;

/// A stream of wire-format bao chunks for a re-encoded byte range —
/// interleaved proof pairs and chunk-group data in tree order, **header-less**
/// (no 8-byte size prefix; the signed `StreamResponse.total_bytes` is the
/// authoritative size, ADR 038). Each item is fallible with the store's
/// [`RangedStoreError`].
pub type EncodeStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, RangedStoreError>> + Send>>;

/// Node-only extension of [`RangedStore`]: progressive serve-while-filling
/// primitives over an already-open per-blob store.
///
/// `observe` watches which ranges are present as a partial blob fills;
/// `encode_range` re-encodes whatever is currently held in a byte span into
/// the `cdn/client/v1` header-less interleaved wire bao (ADR 038), the same
/// wire shape [`RangedStore::read`] does not produce.
pub trait ServeStore: RangedStore {
    /// Watch which chunk ranges are present, from now until the blob is
    /// complete.
    ///
    /// **Precondition:** the blob must already be materialized (at least its
    /// first chunk group admitted, so the store reports `Partial`). A blob the
    /// store has never seen has no defined current bitfield to watch, so this
    /// returns `Err` rather than an empty watch. A serve driver must therefore
    /// open the watch AFTER the first admit lands (or tolerate a transient
    /// `Err` and retry), not the instant it kicks off an upstream pull.
    fn observe(&self) -> RangedFuture<'_, PresentRangeWatch>;

    /// Re-encode the currently-present bytes of
    /// `[byte_offset, byte_offset + byte_len)` into header-less wire bao.
    fn encode_range(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, EncodeStream>;
}
