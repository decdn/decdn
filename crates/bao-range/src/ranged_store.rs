//! The `(hash, total_bytes)` blob view every range-aware backend implements.
//!
//! [`RangedStore`] is what keeps this crate iroh-blobs-free: it speaks byte
//! offsets and `ChunkRanges` only, so the node's iroh-blobs cache and the
//! CLI's `.partial` sidecar can both satisfy it (#578).

/// A boxed future returned by a [`RangedStore`] method, borrowing the store
/// for `'a`.
pub type RangedFuture<'a, T> =
    core::pin::Pin<Box<dyn core::future::Future<Output = Result<T, RangedStoreError>> + Send + 'a>>;

/// What a [`RangedStore`] operation can fail with.
#[derive(Debug, thiserror::Error)]
pub enum RangedStoreError {
    /// The requested range is not chunk-group aligned, or does not verify
    /// against the outboard.
    #[error("range alignment: {0}")]
    Alignment(#[from] crate::RangeVerifyError),
    /// Finalization was asked for while ranges are still missing.
    #[error("blob is incomplete: cannot finalize")]
    Incomplete,
    /// The underlying store (file, blob store) failed.
    #[error("backend: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// A source-agnostic, range-aware view of ONE blob `(hash, total_bytes)`.
/// Speaks `ChunkRanges` + byte offsets only — no hash type — so `bao-range`
/// stays iroh-blobs-free (#578). Both the node (iroh-blobs cache) and the
/// client (`.partial` sidecar) backends implement it and pass one shared
/// conformance suite.
pub trait RangedStore: Send + Sync {
    /// Total blob length in bytes (known at construction).
    fn total_bytes(&self) -> u64;
    /// Chunk ranges that verify right now.
    fn present_ranges(&self) -> RangedFuture<'_, bao_tree::ChunkRanges>;
    /// `[byte_offset, byte_offset+byte_len)` (`byte_len == 0` ⇒ to end) minus what is present.
    fn missing_ranges(
        &self,
        byte_offset: u64,
        byte_len: u64,
    ) -> RangedFuture<'_, bao_tree::ChunkRanges>;
    /// Verify the interleaved `bao_bytes` for `range` against the root in
    /// transit, write data + proof, record the range. Idempotent per range.
    fn admit(&self, range: crate::AlignedRange, bao_bytes: bytes::Bytes) -> RangedFuture<'_, ()>;
    /// Plaintext bytes for the held `[byte_offset, byte_offset+byte_len)` span.
    /// The returned span is collected into `Bytes`, so callers (e.g. a
    /// future gap-driven driver) should request gap-sized spans rather than
    /// whole blobs to stay O(gap) in memory.
    fn read(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, bytes::Bytes>;
    /// The whole blob is present.
    fn is_complete(&self) -> RangedFuture<'_, bool>;
    /// Whole-blob wrap-up: verify complete (+ promote/materialize on backends
    /// that need it). Errors `Incomplete` if any range is still missing.
    fn finalize(&self) -> RangedFuture<'_, ()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_from_range_verify() {
        // Any RangeVerifyError becomes RangedStoreError::Alignment via `?`.
        fn coerce(e: crate::RangeVerifyError) -> RangedStoreError {
            e.into()
        }
        // The trait is dyn-compatible: this type-checks only if object-safe.
        fn _assert_dyn(_: &dyn RangedStore) {}
        // Construct a representative RangeVerifyError to exercise the From.
        let err = coerce(crate::RangeVerifyError::RangeOutOfBounds {
            offset: 0,
            len: 0,
            blob_size: 0,
        });
        assert!(matches!(err, RangedStoreError::Alignment(_)));
    }
}
