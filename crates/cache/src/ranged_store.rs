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
            self.engine
                .missing_ranges(self.hash, byte_offset, byte_len, self.total_bytes)
                .await
                .map_err(backend)
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
