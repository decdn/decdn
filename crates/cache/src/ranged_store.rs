//! [`NodeRangedStore`]: the node-side [`RangedStore`] backend over the
//! iroh-blobs cache (#1621 Task 4).
//!
//! A thin, per-blob adapter over [`CacheEngine`]'s already-shipped
//! present/missing/admit/export API — behavior-preserving, no new storage
//! logic. `finalize` relies on the node's own auto-promotion: once every
//! chunk of a blob is present the store already reports it complete, so
//! finalize is just that check, erroring [`RangedStoreError::Incomplete`]
//! otherwise.

use bytes::Bytes;
use decdn_bao_range::{AlignedRange, RangedFuture, RangedStore, RangedStoreError};
use iroh_blobs::Hash;

use crate::engine::CacheEngine;
use crate::serve_store::{EncodeStream, PresentRangeWatch, ServeStore};

/// Node-side [`RangedStore`]: a per-blob view over the iroh-blobs cache.
#[derive(Debug)]
pub struct NodeRangedStore {
    engine: CacheEngine,
    hash: Hash,
    total_bytes: u64,
}

impl NodeRangedStore {
    /// Wrap `engine`'s view of `hash` (a `total_bytes`-byte blob) as a
    /// [`RangedStore`]. `engine` is cheap to clone (`Arc`-backed), so this
    /// holds it by value rather than behind an extra `Arc`.
    #[must_use]
    pub const fn new(engine: CacheEngine, hash: Hash, total_bytes: u64) -> Self {
        Self {
            engine,
            hash,
            total_bytes,
        }
    }

    /// The blob hash this store is scoped to.
    #[must_use]
    pub const fn hash(&self) -> Hash {
        self.hash
    }

    /// The wrapped [`CacheEngine`] handle. Lets a node-crate wrapper (#1621 Task
    /// 10, `NodeAdmitStore`) reach the engine's `admit_bao_stream` without
    /// duplicating the `(engine, hash, total_bytes)` triple it already holds.
    #[must_use]
    pub const fn engine(&self) -> &CacheEngine {
        &self.engine
    }
}

fn backend<E: std::error::Error + Send + Sync + 'static>(e: E) -> RangedStoreError {
    RangedStoreError::Backend(Box::new(e))
}

impl RangedStore for NodeRangedStore {
    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    fn present_ranges(&self) -> RangedFuture<'_, bao_tree::ChunkRanges> {
        Box::pin(async move {
            let p = self
                .engine
                .present_ranges(self.hash)
                .await
                .map_err(backend)?;
            Ok(p.chunk_ranges().clone())
        })
    }

    fn missing_ranges(
        &self,
        byte_offset: u64,
        byte_len: u64,
    ) -> RangedFuture<'_, bao_tree::ChunkRanges> {
        Box::pin(async move {
            // Validate + align locally so an out-of-bounds request is a typed
            // `Alignment` error, not an opaque backend fault (a bad range is an
            // argument error, and the client backend classifies it the same way).
            let aligned = decdn_bao_range::align_range(byte_offset, byte_len, self.total_bytes)?;
            let present = self
                .engine
                .present_ranges(self.hash)
                .await
                .map_err(backend)?;
            Ok(aligned.chunk_ranges().clone() - present.chunk_ranges())
        })
    }

    fn admit(&self, range: AlignedRange, bao_bytes: Bytes) -> RangedFuture<'_, ()> {
        Box::pin(async move {
            self.engine
                .admit_bao(self.hash, range.chunk_ranges().clone(), bao_bytes)
                .await
                .map_err(backend)
        })
    }

    fn read(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, Bytes> {
        Box::pin(async move {
            // Reject an out-of-bounds span as a typed `Alignment` error before
            // touching the store; the aligned widening is discarded — `read` is
            // byte-exact and forwards the original offset/len.
            decdn_bao_range::align_range(byte_offset, byte_len, self.total_bytes)?;
            self.engine
                .export_range(self.hash, byte_offset, byte_len)
                .await
                .map_err(backend)
        })
    }

    fn is_complete(&self) -> RangedFuture<'_, bool> {
        Box::pin(async move {
            Ok(self
                .engine
                .present_ranges(self.hash)
                .await
                .map_err(backend)?
                .is_complete())
        })
    }

    fn finalize(&self) -> RangedFuture<'_, ()> {
        Box::pin(async move {
            if self
                .engine
                .present_ranges(self.hash)
                .await
                .map_err(backend)?
                .is_complete()
            {
                Ok(())
            } else {
                Err(RangedStoreError::Incomplete)
            }
        })
    }
}

impl ServeStore for NodeRangedStore {
    fn observe(&self) -> RangedFuture<'_, PresentRangeWatch> {
        Box::pin(async move {
            self.engine
                .observe_present_ranges(self.hash)
                .await
                .map_err(backend)
        })
    }

    fn encode_range(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, EncodeStream> {
        Box::pin(async move {
            let stream = self
                .engine
                .export_bao_range_stream(self.hash, byte_offset, byte_len, self.total_bytes)
                .await
                .map_err(backend)?;
            Ok(Box::pin(futures_util::StreamExt::map(stream, |item| {
                item.map_err(backend)
            })) as EncodeStream)
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests
mod tests {
    use super::NodeRangedStore;
    use crate::engine::CacheEngine;
    use iroh_blobs::Hash;

    /// The `hash`/`engine` accessors (#1621 Task 10) round-trip the values
    /// `new` was constructed with — the node-crate `NodeAdmitStore` wrapper
    /// reaches `admit_bao_stream` through these rather than duplicating the
    /// `(engine, hash, total_bytes)` triple.
    #[tokio::test]
    async fn node_ranged_store_accessors_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
        let hash = Hash::from([7u8; 32]);
        let store = NodeRangedStore::new(engine, hash, 4096);
        assert_eq!(store.hash(), hash);
        // Round-trip a query through the accessor's engine handle to prove it
        // is a live, queryable handle rather than just a structural copy.
        assert!(
            store
                .engine()
                .present_ranges(hash)
                .await
                .unwrap()
                .is_empty(),
            "the accessor's engine handle is live and queryable"
        );
    }
}
