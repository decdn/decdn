use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use decdn_bao_range::{AlignedRange, RangedFuture, RangedStore};

use super::{BaoRangeReader, IngestStore, SourceFuture};
use crate::ClientRangedStore;

/// An [`IngestStore`] wrapper that counts `flush_present_record` calls and
/// delegates every real operation to an inner [`ClientRangedStore`]. It lets
/// a test observe the interval flush firing during a still-running fetch.
///
/// Each flush is numbered from 1 at its call, the snapshot it takes. The
/// first `slow_flushes` land `flush_delay` after their call, to model a
/// record fsync under writeback pressure; the rest land at once. A flush
/// lands on its own task, so it lands even when its caller drops the
/// returned future, as a real store's blocking write does; `last_landed`
/// then holds the number of the last flush to land.
pub(crate) struct FlushCountingStore {
    pub(crate) inner: ClientRangedStore,
    pub(crate) flushes: Arc<AtomicUsize>,
    pub(crate) flush_delay: Duration,
    pub(crate) slow_flushes: usize,
    pub(crate) last_landed: Arc<AtomicUsize>,
}

impl FlushCountingStore {
    /// Wrap `inner`, every flush taking `flush_delay`.
    pub(crate) fn new(inner: ClientRangedStore, flush_delay: Duration) -> Self {
        Self {
            inner,
            flushes: Arc::default(),
            flush_delay,
            slow_flushes: usize::MAX,
            last_landed: Arc::default(),
        }
    }
}

impl RangedStore for FlushCountingStore {
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
    fn admit(&self, range: AlignedRange, bao_bytes: Bytes) -> RangedFuture<'_, ()> {
        self.inner.admit(range, bao_bytes)
    }
    fn read(&self, byte_offset: u64, byte_len: u64) -> RangedFuture<'_, Bytes> {
        self.inner.read(byte_offset, byte_len)
    }
    fn is_complete(&self) -> RangedFuture<'_, bool> {
        self.inner.is_complete()
    }
    fn finalize(&self) -> RangedFuture<'_, ()> {
        self.inner.finalize()
    }
}

impl IngestStore for FlushCountingStore {
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        claimed_total: u64,
        stop_at: Option<&'a std::sync::atomic::AtomicU64>,
    ) -> super::IngestFuture<'a, R>
    where
        R: BaoRangeReader + 'a,
    {
        IngestStore::ingest_stream(
            &self.inner,
            range,
            reader,
            on_progress,
            claimed_total,
            stop_at,
        )
    }

    fn flush_present_record(&self) -> SourceFuture<'_, ()> {
        let seq = self.flushes.fetch_add(1, Ordering::SeqCst) + 1;
        let write = IngestStore::flush_present_record(&self.inner);
        let delay = if seq <= self.slow_flushes {
            self.flush_delay
        } else {
            Duration::ZERO
        };
        let last_landed = Arc::clone(&self.last_landed);
        let landing = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            last_landed.store(seq, Ordering::SeqCst);
        });
        Box::pin(async move {
            write.await?;
            landing
                .await
                .map_err(|e| anyhow::anyhow!("flush landing task: {e}"))
        })
    }

    fn proven(&self) -> Option<u64> {
        IngestStore::proven(&self.inner)
    }

    fn set_bound(&self, bound: u64) {
        IngestStore::set_bound(&self.inner, bound);
    }
}
