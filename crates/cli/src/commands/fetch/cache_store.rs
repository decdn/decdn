//! The `decdn gateway` cache as a [`Streamer`](decdn_client::Streamer)'s fill
//! store (#2881): a [`StreamStore`] over a [`CacheEngine`], so the bytes one
//! request pays for serve the next request for the same blob.
//!
//! Each ingest streams a leg's bao into the cache through
//! [`CacheEngine::admit_bao_stream_until`], which verifies every chunk group
//! against the blob's root and keeps the admitted ranges as a partial held
//! against GC by its protecting tag. The store reads presence and bytes back
//! through [`NodeRangedStore`], so the reader hands out only verified bytes.
//!
//! This lives in the CLI, beside the fetch that opens it: it names both
//! `decdn_cache`'s engine and `decdn_client`'s [`IngestStore`] trait, and
//! neither of those crates may depend on the other (#578).

use std::sync::atomic::AtomicU64;

use decdn_bao_range::{AlignedRange, RangedFuture, RangedStore};
use decdn_cache::{AdmitControl, AdmitEnd, CacheEngine, Hash, NodeRangedStore};
use decdn_client::{
    BaoRangeReader, IngestEnd, IngestFuture, IngestStore, SourceFuture, StreamStore, StreamStores,
};
use tokio::sync::Notify;

/// Opens a [`CacheStore`] over one [`CacheEngine`] for each stream.
///
/// Every call for one hash opens a view of the same cache entry, and two
/// streams may fill it at the same time: each admit verifies its bytes
/// against the root before the store keeps them, and writing a chunk group
/// twice leaves it as it was. Each stream fetches what is missing when it
/// plans a range, so two concurrent streams of one blob can both pay for a
/// range neither had yet.
#[derive(Clone)]
pub(crate) struct CacheStores {
    engine: CacheEngine,
}

impl CacheStores {
    /// Stores over `engine`.
    pub(crate) const fn new(engine: CacheEngine) -> Self {
        Self { engine }
    }
}

impl StreamStores for CacheStores {
    type Store = CacheStore;

    async fn open(&self, hash: [u8; 32], total_bytes: u64) -> anyhow::Result<CacheStore> {
        Ok(CacheStore::new(
            self.engine.clone(),
            Hash::from_bytes(hash),
            total_bytes,
        ))
    }
}

/// One stream's view of one cache entry: [`RangedStore`] queries delegate to
/// a [`NodeRangedStore`], and [`IngestStore::ingest_stream`] admits each leg
/// through [`CacheEngine::admit_bao_stream_until`].
///
/// The store is keyed by the size it opens with, a size a holder proved, so
/// [`IngestStore::proven`] is that size from the start.
pub(crate) struct CacheStore {
    inner: NodeRangedStore,
    /// Woken as each verified chunk group goes to the store, and once more
    /// when a leg's admit has written all of its groups.
    grew: Notify,
}

impl CacheStore {
    /// `engine`'s view of `hash`, a `total_bytes`-byte blob.
    pub(crate) const fn new(engine: CacheEngine, hash: Hash, total_bytes: u64) -> Self {
        Self {
            inner: NodeRangedStore::new(engine, hash, total_bytes),
            grew: Notify::const_new(),
        }
    }
}

impl RangedStore for CacheStore {
    fn total_bytes(&self) -> u64 {
        self.inner.total_bytes()
    }

    fn present_ranges(&self) -> RangedFuture<'_, bao_tree::ChunkRanges> {
        self.inner.present_ranges()
    }

    fn missing_ranges(
        &self,
        byte_offset: u64,
        byte_len: u64,
    ) -> RangedFuture<'_, bao_tree::ChunkRanges> {
        self.inner.missing_ranges(byte_offset, byte_len)
    }

    fn admit(&self, range: AlignedRange, bao_bytes: bytes::Bytes) -> RangedFuture<'_, ()> {
        self.inner.admit(range, bao_bytes)
    }

    fn read(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, bytes::Bytes> {
        self.inner.read(byte_offset, byte_len)
    }

    fn is_complete(&self) -> RangedFuture<'_, bool> {
        self.inner.is_complete()
    }

    fn finalize(&self) -> RangedFuture<'_, ()> {
        self.inner.finalize()
    }
}

impl IngestStore for CacheStore {
    /// Streams `reader`'s bao for `range` into the cache, verifying each
    /// chunk group as it lands. `on_progress` sees the content bytes verified
    /// on this range after each group, and the reader waiting on
    /// [`StreamStore::present_grew`] wakes with it. A lowered `stop_at` stops
    /// the admit at the first group boundary past it.
    ///
    /// The cache verifies under the size this store opened with, a size a
    /// holder proved, so `claimed_total` plays no part: a leg encoded under
    /// another size fails to verify.
    ///
    /// A typed fault the reader parked (a stalled, refusing or rejected leg)
    /// outranks the cache's own error, which sees only a truncated feed, so
    /// fault classification, reputation and the funding recovery step see
    /// what the peer did.
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        _claimed_total: u64,
        stop_at: Option<&'a AtomicU64>,
    ) -> IngestFuture<'a, R>
    where
        R: BaoRangeReader + 'a,
    {
        Box::pin(async move {
            let start = range.fetch_start();
            let progress = |verified_end: u64| {
                if let Some(cb) = on_progress {
                    cb(verified_end.saturating_sub(start));
                }
                self.grew.notify_waiters();
            };
            let admitted = self
                .inner
                .engine()
                .admit_bao_stream_until(
                    self.inner.hash(),
                    range.chunk_ranges().clone(),
                    self.inner.total_bytes(),
                    reader,
                    None,
                    AdmitControl {
                        on_progress: Some(&progress),
                        stop_at,
                    },
                )
                .await;
            // Every group the admit verified is in the store now.
            self.grew.notify_waiters();
            match admitted {
                Ok((mut reader, end)) => {
                    // A fault parked after the last byte (a voucher rejected
                    // at the closing interval) leaves a clean feed behind it.
                    if let Some(fault) = reader.take_fault() {
                        return Err(fault);
                    }
                    let end = match end {
                        AdmitEnd::Drained => IngestEnd::Drained,
                        AdmitEnd::Stopped => IngestEnd::Stopped,
                    };
                    Ok((reader, end))
                }
                Err((mut reader, cache_err)) => {
                    if let Some(fault) = reader.take_fault() {
                        return Err(fault);
                    }
                    // No parked fault: the bytes themselves failed to verify.
                    if matches!(
                        cache_err,
                        decdn_cache::CacheError::VerifyFailed { .. }
                            | decdn_cache::CacheError::HashMismatch { .. }
                    ) {
                        return Err(anyhow::Error::new(decdn_client::HashMismatch));
                    }
                    Err(anyhow::Error::from(cache_err))
                }
            }
        })
    }

    /// Presence lives in the cache's own bookkeeping, so there is no record
    /// to flush.
    fn flush_present_record(&self) -> SourceFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl StreamStore for CacheStore {
    fn present_grew(&self) -> &Notify {
        &self.grew
    }
}

#[cfg(test)]
mod tests;
