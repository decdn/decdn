//! `NodeAdmitStore` — the node's pull-leg [`decdn_client::IngestStore`]
//! over [`decdn_cache::CacheEngine::admit_bao_stream`] (#1621).
//!
//! `drive()`'s gap-driven pull loop needs a store that both answers
//! [`decdn_bao_range::RangedStore`] queries (present/missing ranges, read,
//! finalize) and can ingest a gap's raw bao wire. [`NodeRangedStore`]
//! already answers the queries over the cache; this type adds the ingest half
//! by wrapping a `NodeRangedStore` and streaming each gap straight into
//! [`decdn_cache::CacheEngine::admit_bao_stream`], which admits the bytes as a
//! partial held against GC by its `decdn-partial-` protecting tag, without
//! buffering the whole gap in memory.
//!
//! This has to live in `node`, not `cache` or `decdn-client`: it names both
//! `decdn_cache`'s store and `decdn_client`'s [`IngestStore`] trait, and
//! #578 forbids either of those crates depending on the other. `node` is the
//! one crate that already depends on both.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use decdn_bao_range::{AlignedRange, RangedFuture, RangedStore};
use decdn_cache::{CacheEngine, FillSession, Hash, NodeRangedStore};
use decdn_client::{BaoRangeReader, IngestEnd, IngestStore};

use crate::metrics::Metrics;

/// The node's pull-leg store: [`RangedStore`] queries delegate to a
/// [`NodeRangedStore`], and [`IngestStore::ingest_stream`] admits each gap via
/// [`CacheEngine::admit_bao_stream`]. When a [`FillSession`] is present, the cache's
/// admit path captures each admitted range's proof nodes into the serve leg's shared
/// outboard (#1621, ADR 038) so the serve leg can drive a coherent whole-range
/// encode while the pull fills incrementally — the node just threads the session in.
pub(crate) struct NodeAdmitStore {
    inner: NodeRangedStore,
    /// Serve-leg fill session. `None` when no serve leg reads beside this pull (e.g.
    /// the admit-only unit tests). Passed to [`CacheEngine::admit_bao_stream`], which
    /// captures the admitted range's outboard proof nodes into it cache-side.
    session: Option<Arc<FillSession>>,
    /// Counts each admitted range's payload into `decdn_bytes_received_total`.
    /// Set only on the paid node-to-node pulls, so an operator's own-origin fill
    /// through the same store is not counted as bytes received from peers.
    received: Option<Arc<Metrics>>,
}

impl NodeAdmitStore {
    /// Wrap `engine`'s view of `hash` (a `total_bytes`-byte blob) as the node's
    /// pull-leg store. When `session` is wired, the cache's admit path captures each
    /// admitted range's outboard proof nodes into it (the serve leg's shared outboard).
    pub(crate) const fn new(
        engine: CacheEngine,
        hash: Hash,
        total_bytes: u64,
        session: Option<Arc<FillSession>>,
    ) -> Self {
        Self {
            inner: NodeRangedStore::new(engine, hash, total_bytes),
            session,
            received: None,
        }
    }

    /// Count every admitted range into `decdn_bytes_received_total`. For the
    /// paid node-to-node pulls only.
    pub(crate) fn counting_received(mut self, metrics: Arc<Metrics>) -> Self {
        self.received = Some(metrics);
        self
    }
}

impl RangedStore for NodeAdmitStore {
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

impl IngestStore for NodeAdmitStore {
    /// Streams `reader`'s raw bao wire for `range` straight into the cache via
    /// [`CacheEngine::admit_bao_stream`], which verifies each chunk group
    /// against the store's rooted hash as it lands and admits the range as a
    /// partial held against GC by its `decdn-partial-` protecting tag.
    ///
    /// `on_progress` is accepted for the trait but unused on this path: the
    /// node's progress metering happens at the SERVE leg, which
    /// meters what it forwards to the downstream client — not at this shared
    /// upstream ingest. If `admit_bao_stream` grows a progress hook later, it
    /// plugs in here.
    ///
    /// The `R: BaoRangeReader` bound (`AsyncStreamReader + StashedFault +
    /// Send`) satisfies `admit_bao_stream`'s `R: AsyncStreamReader + Send`, so
    /// `reader` passes straight through — the drained reader `admit_bao_stream`
    /// returns is handed back as-is, preserving its `StashedFault` for the
    /// caller's [`decdn_client::BlobSource::finish`].
    ///
    /// On the admit ERROR path this recovers the reader's parked typed peer
    /// fault. `admit_bao_stream` hands the reader back on both arms; when it
    /// errors, the reason the stream stopped is almost always a peer fault the
    /// pull reader parked mid-fill (`PullStalled`/`PullTimeout`/`UpstreamRefused`/
    /// `UpstreamVoucherRejected`/buyer-side `LocalPullFault`) — the cache decoder
    /// only sees the resulting truncation as a generic `CacheError`. Surfacing
    /// the parked fault verbatim keeps `pull_verdict` fault classification,
    /// reputation, channel remedies, and the funding recovery step working.
    /// No-op: unlike [`decdn_client::ClientRangedStore`]'s `.partial` +
    /// `.ranges` sidecar, this store's presence is derived live from
    /// [`CacheEngine`]'s own admitted-range bookkeeping — there is no
    /// separate present-range record to flush.
    fn flush_present_record(&self) -> decdn_client::SourceFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        _on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        _claimed_total: u64,
        stop_at: Option<&'a AtomicU64>,
    ) -> decdn_client::IngestFuture<'a, R>
    where
        R: BaoRangeReader + 'a,
    {
        Box::pin(async move {
            // Only a steal lowers an end, and the node's pull leg drives one
            // source with no steal, so the cache admit reads every range to
            // its end. A caller that asks otherwise is this process's bug,
            // never the upstream's.
            if stop_at.is_some() {
                return Err(
                    anyhow::anyhow!("the node's admit store reads every range to its end")
                        .context(decdn_client::LocalPullFault),
                );
            }
            // The cache verifies under the size this store was opened with,
            // which is the size the serve leg signs downstream, so the
            // upstream's claim plays no part here.
            //
            // `admit_bao_stream` verifies + admits the range and, when a
            // [`FillSession`] is wired, captures its outboard proof nodes into it
            // cache-side (no-op when no serve leg reads beside this pull).
            // Front-to-back admits union to the whole tree.
            match self
                .inner
                .engine()
                .admit_bao_stream(
                    self.inner.hash(),
                    range.chunk_ranges().clone(),
                    self.inner.total_bytes(),
                    reader,
                    self.session.as_ref(),
                )
                .await
            {
                Ok(mut reader) => {
                    // A parked fault can outlive a fully-decoded byte stream: a
                    // voucher rejected at the closing interval (buyer-side
                    // `LocalPullFault`/`UpstreamVoucherRejected`) parks the fault
                    // AFTER every requested byte already landed, so admit sees a
                    // clean EOF and returns `Ok`. Mirror the client store's `Done`
                    // arm — surface the parked fault over the apparent success.
                    if let Some(fault) = reader.take_fault() {
                        return Err(fault);
                    }
                    if let Some(metrics) = &self.received {
                        metrics.bytes_received(range.fetch_len());
                    }
                    Ok((reader, IngestEnd::Drained))
                }
                Err((mut reader, cache_err)) => {
                    // The parked typed peer fault is the real reason the stream
                    // stopped; it beats the generic truncation the cache decoder
                    // sees, so it wins.
                    if let Some(fault) = reader.take_fault() {
                        return Err(fault);
                    }
                    // No parked fault means the bytes themselves failed to verify
                    // (a dishonest upstream). Surface the typed corruption sentinel
                    // `pull_verdict` / `is_bao_corruption` understand.
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
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation
)] // tests
mod tests;
