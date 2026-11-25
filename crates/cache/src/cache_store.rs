//! [`CacheStore`]: the store a paid pull fills when its bytes land in the
//! cache. The node's miss pull and the gateway's streams both fill one.
//!
//! It answers [`RangedStore`] queries through [`NodeRangedStore`] and ingests
//! each leg's raw bao through [`CacheEngine::admit_bao_stream_until`], which
//! verifies every chunk group against the blob's root and keeps the admitted
//! ranges as a partial held against GC by its protecting tag, without
//! buffering the leg in memory. It implements the pull-side contract
//! `decdn-bao-range` defines ([`IngestStore`], [`StreamStore`]), so the pull
//! that drives it needs no edge into this crate.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use decdn_bao_range::{
    AlignedRange, BaoRangeReader, HashMismatch, IngestEnd, IngestFuture, IngestStore, RangedFuture,
    RangedStore, SourceFuture, StreamStore,
};
use tokio::sync::Notify;

use crate::{
    AdmitControl, AdmitEnd, CacheEngine, CacheError, FillSession, Hash, InUse, NodeRangedStore,
};

/// A callback that sees the content bytes each leg admitted.
pub type OnAdmitted = Arc<dyn Fn(u64) + Send + Sync>;

/// One pull's view of one cache entry, keyed by the size it opens with: a
/// size a holder proved, so [`IngestStore::proven`] is that size from the
/// start.
///
/// Two stores over one entry may fill it at the same time: each admit
/// verifies its bytes against the root before the store keeps them, and
/// writing a chunk group twice leaves it as it was.
///
/// The store holds its entry against capacity eviction for as long as it
/// lives ([`CacheEngine::hold_in_use`]), so GC never sweeps an entry a pull
/// is writing.
pub struct CacheStore {
    inner: NodeRangedStore,
    /// Keeps the entry out of eviction while this store lives.
    _in_use: InUse,
    /// The serving leg's fill session, when a serve reads beside this pull:
    /// the admit captures each range's proof nodes into it as they verify.
    session: Option<Arc<FillSession>>,
    /// Sees the content bytes each leg admitted.
    on_admitted: Option<OnAdmitted>,
    /// Woken as each verified chunk group goes to the store, and once more
    /// when a leg's admit has written all of its groups.
    grew: Notify,
}

impl CacheStore {
    /// `engine`'s view of `hash`, a `total_bytes`-byte blob.
    #[must_use]
    pub fn new(engine: CacheEngine, hash: Hash, total_bytes: u64) -> Self {
        Self {
            _in_use: engine.hold_in_use(hash),
            inner: NodeRangedStore::new(engine, hash, total_bytes),
            session: None,
            on_admitted: None,
            grew: Notify::const_new(),
        }
    }

    /// Capture each admitted range's proof nodes into `session`, the shared
    /// outboard of a serving leg that reads beside this pull (#1621, ADR 038).
    #[must_use]
    pub fn with_session(mut self, session: Option<Arc<FillSession>>) -> Self {
        self.session = session;
        self
    }

    /// Hand `on_admitted` the content bytes each leg admitted, once the leg's
    /// admit ends without a fault.
    #[must_use]
    pub fn on_admitted(mut self, on_admitted: OnAdmitted) -> Self {
        self.on_admitted = Some(on_admitted);
        self
    }

    /// The cache engine this store admits into.
    #[must_use]
    pub const fn engine(&self) -> &CacheEngine {
        self.inner.engine()
    }

    /// The blob this store is scoped to.
    #[must_use]
    pub const fn hash(&self) -> Hash {
        self.inner.hash()
    }

    /// The session when it is exclusive ([`FillSession::mark_exclusive`]). Such
    /// a store keeps the blob up to the node's cap on bytes held and hands every
    /// later verified leaf to the session's serve leg. Its present ranges
    /// include the leaves it handed over, so the pull counts them as landed and
    /// does not fetch them again.
    fn exclusive_session(&self) -> Option<&Arc<FillSession>> {
        self.session.as_ref().filter(|s| s.is_exclusive())
    }
}

impl std::fmt::Debug for CacheStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheStore")
            .field("hash", &self.inner.hash())
            .field("total_bytes", &self.inner.total_bytes())
            .field("session", &self.session.is_some())
            .finish_non_exhaustive()
    }
}

impl RangedStore for CacheStore {
    fn total_bytes(&self) -> u64 {
        self.inner.total_bytes()
    }

    /// The ranges the store holds, plus those an exclusive fill handed over.
    fn present_ranges(&self) -> RangedFuture<'_, bao_tree::ChunkRanges> {
        let Some(session) = self.exclusive_session() else {
            return self.inner.present_ranges();
        };
        Box::pin(async move { Ok(self.inner.present_ranges().await? | session.passed_ranges()) })
    }

    /// The ranges the store lacks, less those an exclusive fill handed over.
    fn missing_ranges(
        &self,
        byte_offset: u64,
        byte_len: u64,
    ) -> RangedFuture<'_, bao_tree::ChunkRanges> {
        let Some(session) = self.exclusive_session() else {
            return self.inner.missing_ranges(byte_offset, byte_len);
        };
        Box::pin(async move {
            Ok(self.inner.missing_ranges(byte_offset, byte_len).await? - session.passed_ranges())
        })
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
    /// on this range after each group, and a reader waiting on
    /// [`StreamStore::present_grew`] wakes with it. A lowered `stop_at` stops
    /// the admit at the first group boundary past it.
    ///
    /// The cache verifies under the size this store opened with, a size a
    /// holder proved, so `claimed_total` plays no part: a leg encoded under
    /// another size fails to verify.
    ///
    /// A typed fault the reader parked (a stalled, refusing or rejected leg)
    /// outranks the cache's own error, which sees only a truncated feed, so
    /// fault classification, reputation, lane remedies and the funding
    /// recovery step see what the peer did. A fault parked after the last byte
    /// (a voucher rejected at the closing interval) outranks a clean admit
    /// too. With no parked fault, bytes that fail to verify are a
    /// [`HashMismatch`].
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
            let verified = AtomicU64::new(start);
            let progress = |verified_end: u64| {
                verified.fetch_max(verified_end, Ordering::Relaxed);
                if let Some(cb) = on_progress {
                    cb(verified_end.saturating_sub(start));
                }
                self.grew.notify_waiters();
            };
            // An exclusive fill stores the blob up to the node's cap on bytes held
            // and hands the rest to its serve leg.
            let store_budget = match self.exclusive_session() {
                Some(_) => self.inner.engine().held_room(self.inner.hash()).await,
                None => None,
            };
            let admitted = self
                .inner
                .engine()
                .admit_bao_stream_until(
                    self.inner.hash(),
                    range.chunk_ranges().clone(),
                    self.inner.total_bytes(),
                    reader,
                    self.session.as_ref(),
                    AdmitControl {
                        on_progress: Some(&progress),
                        stop_at,
                        store_budget,
                    },
                )
                .await;
            // Every group the admit verified is in the store now.
            self.grew.notify_waiters();
            match admitted {
                Ok((mut reader, end)) => {
                    if let Some(fault) = reader.take_fault() {
                        return Err(fault);
                    }
                    if let Some(on_admitted) = &self.on_admitted {
                        on_admitted(verified.load(Ordering::Relaxed).saturating_sub(start));
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
                    if matches!(
                        cache_err,
                        CacheError::VerifyFailed { .. } | CacheError::HashMismatch { .. }
                    ) {
                        reader.note_failed_delivery();
                        return Err(anyhow::Error::new(HashMismatch));
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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod tests;
