//! `BackendSource` — the node's UNPAID [`decdn_client_pull::BlobSource`] over its
//! own configured origin (fs/http/s3), for the own-origin serve-miss.
//!
//! The peer serve-miss path pulls a cold blob from another node over a paid
//! `cdn/client/v1` channel ([`decdn_client_pull::PeerSource`]). The own-origin
//! serve-miss flow is the LOCAL twin: the bytes are already reachable through this
//! node's own origin, so
//! there is no counterparty, no channel, and no payment — but the driver, sink,
//! and serve leg are reused verbatim. This source is what lets that happen: it
//! streams the header-less interleaved bao wire for one [`AlignedRange`] straight
//! out of [`CacheEngine::origin_range_wire`], so the existing `NodeAdmitStore`
//! sink verifies-and-stores it against the root `H` exactly as it does a peer
//! pull's wire. The wire is produced window by window, so a leg holds
//! `O(window + outboard)` bytes whatever its range (#2065).
//!
//! # Why the origin bytes are a LOCAL fault, not upstream corruption
//!
//! By the time this source runs, the node has already signed a `StreamResponse`
//! committing to serve the blob under `H` to a paying client.
//! [`CacheEngine::origin_range_wire`] therefore treats a range that fails bao
//! verification as a HARD [`decdn_cache::CacheError::VerifyFailed`] — a
//! corrupt/misconfigured OWN origin — rather than degrading it (there is no
//! upstream to blame and no fallback that still honours `H`). This source
//! propagates that fault out of [`BlobSource::open`] when it is found up front,
//! and parks it on the reader ([`StashedFault`]) when it is found mid-stream;
//! nothing here scores a provider, because there is no provider.
//!
//! # The self-payment counter (THE CRUX)
//!
//! [`decdn_client_pull::drive`]'s per-gap completion is PAID-frontier gated: a gap
//! is `Done` only once the ledger's committed `bytes` reach the gap end. An unpaid
//! source whose `finish` never advanced a ledger would leave that frontier at zero
//! and the gap loop would re-draw forever. So this source carries a fresh, LOCAL
//! bookkeeping [`PoolLedger`] (`self_pay`) and, on [`BlobSource::finish`],
//! advances it by exactly the leg's drained WIRE bytes at rate 0 — `committed.bytes`
//! moves so the frontier reaches the gap end, while `committed.amount` stays 0 so
//! nothing resembling a payment or a deposit-exhaustion is ever computed. This is
//! NOT payment: no channel, no voucher, no chain, no counterparty — only the
//! driver's internal completion counter, exactly the `ScriptedSource::paying`
//! precedent with a zero rate.

use std::sync::Arc;

use alloy::primitives::U256;
use bytes::Bytes;
use decdn_bao_range::AlignedRange;
use decdn_cache::{CacheEngine, CacheError, CacheResult, Hash, OriginRangeWire};
use decdn_client_pull::sink::StashedFault;
use decdn_client_pull::source::SourceFuture;
use decdn_client_pull::{BlobSource, PoolLedger, UpstreamPullHeader, VoucherProgress};
use iroh_io::AsyncStreamReader;

/// The node's unpaid [`BlobSource`] for the own-origin serve-miss flow.
///
/// One `BackendSource` serves one gap-driven fetch of the blob `root` (a
/// `total_bytes`-byte blob) out of `engine`'s configured origins. `self_pay` is a
/// LOCAL completion counter only — see the module docs and
/// [`BlobSource::finish`].
pub(crate) struct BackendSource {
    engine: CacheEngine,
    root: [u8; 32],
    total_bytes: u64,
    /// Local completion bookkeeping ONLY — no channel, no chain, no counterparty.
    self_pay: Arc<PoolLedger>,
}

impl BackendSource {
    /// Build an unpaid own-origin source for `root` (a `total_bytes`-byte blob)
    /// over `engine`, whose `self_pay` ledger the caller also drives the
    /// completion frontier off (the SAME `Arc` the driver is handed). See the
    /// module docs for why the ledger is local bookkeeping, not payment.
    pub(crate) const fn new(
        engine: CacheEngine,
        root: [u8; 32],
        total_bytes: u64,
        self_pay: Arc<PoolLedger>,
    ) -> Self {
        Self {
            engine,
            root,
            total_bytes,
            self_pay,
        }
    }

    /// The LOCAL completion-counter ledger this source advances on
    /// [`BlobSource::finish`]. The pull leg MUST hand this SAME `Arc` to
    /// [`decdn_client_pull::drive`] as its `ledger`, so the paid-frontier the gap
    /// loop reads for completion is the one `finish` moves — the whole point of THE
    /// CRUX in the module docs. Returned as a fresh handle onto the shared ledger,
    /// never a second ledger.
    pub(crate) fn ledger(&self) -> Arc<PoolLedger> {
        Arc::clone(&self.self_pay)
    }
}

impl BlobSource for BackendSource {
    type Reader = BackendReader;

    fn open(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
    ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
        Box::pin(async move {
            // This source is pinned to one blob: a mismatched `hash` is a
            // driver/orchestration bug, not an origin condition.
            if hash != self.root {
                anyhow::bail!(
                    "backend source for {} opened for a foreign hash {}",
                    Hash::from(self.root),
                    Hash::from(hash),
                );
            }
            // Stream + verify + encode the range out of our own origin. A fault
            // found up front (a wrong-length outboard or first window, a transport
            // fault, a read past its time budget) surfaces here via `?`; a clean
            // decline (no origin serves the outboard or the range) is `None`.
            let Some(wire) = self
                .engine
                .origin_range_wire(Hash::from(hash), &range)
                .await?
            else {
                anyhow::bail!(
                    "no configured origin serves the outboard or the range for {}",
                    Hash::from(hash)
                );
            };
            let header = UpstreamPullHeader {
                total_bytes: self.total_bytes,
                rate_per_mb: 0,
                interval_bytes: 0,
                // A local origin re-encode, not a network round trip.
                ttfb_ms: 0.0,
            };
            Ok((header, BackendReader::new(wire, range.wire_len())))
        })
    }

    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async move {
            // Advance the LOCAL completion counter by this leg's wire length at
            // rate 0, so `committed.bytes` reaches the gap end (the driver's
            // paid-frontier completion) while `committed.amount` stays 0 — the
            // `ScriptedSource::finish` paying arm, with a zero rate. This is not
            // payment; see the module docs. The driver calls `finish` only after
            // the sink decoded the whole range, so the reader drained exactly
            // `expected_wire_len` bytes.
            debug_assert!(
                reader.pending.is_empty(),
                "finish on a backend reader with undrained wire"
            );
            if reader.expected_wire_len > 0 {
                self.self_pay
                    .issue(
                        reader.expected_wire_len,
                        0,
                        // No chain on the unpaid leg: `Keep` on a lane that has
                        // opened none commits a sealed section and opens nothing.
                        decdn_client_pull::EpochAction::Keep,
                        |_next, _chain| async { Ok(()) },
                    )
                    .await?;
            }
            Ok(VoucherProgress::from_cumulative(
                self.self_pay.committed(),
                U256::ZERO,
            ))
        })
    }
}

/// The chunk source a [`BackendReader`] drains: in production the
/// [`OriginRangeWire`], whose chunks end in one terminal `Err` on a fault.
pub(crate) trait WireChunks: Send {
    /// The next chunk, a terminal `Err`, or `None` at a clean end.
    fn next_chunk(&mut self) -> impl Future<Output = Option<CacheResult<Bytes>>> + Send;
}

impl WireChunks for OriginRangeWire {
    fn next_chunk(&mut self) -> impl Future<Output = Option<CacheResult<Bytes>>> + Send {
        Self::next_chunk(self)
    }
}

/// The reader a [`BackendSource`] yields: the header-less bao wire streamed out
/// of an [`OriginRangeWire`]. A fault the encode stopped on — a window that fails
/// verification against `H`, or an origin that stops serving mid-stream — fails
/// the read and is kept for [`StashedFault::take_fault`], so the sink reports
/// that typed [`CacheError`] rather than a bare truncation.
pub(crate) struct BackendReader<W = OriginRangeWire> {
    wire: W,
    /// The unread rest of the last chunk the wire yielded.
    pending: Bytes,
    /// The terminal fault the wire ended on, until the sink takes it.
    fault: Option<CacheError>,
    /// The range's exact header-less wire byte count
    /// ([`AlignedRange::wire_len`]), used by [`BlobSource::finish`] to advance
    /// the local completion counter.
    expected_wire_len: u64,
}

impl<W: WireChunks> BackendReader<W> {
    const fn new(wire: W, expected_wire_len: u64) -> Self {
        Self {
            wire,
            pending: Bytes::new(),
            fault: None,
            expected_wire_len,
        }
    }

    /// Refill `pending` from the wire when it is empty. `Ok(false)` at a clean
    /// end; `Err` once the wire has ended on a fault (kept for `take_fault`).
    async fn refill(&mut self) -> std::io::Result<bool> {
        while self.pending.is_empty() {
            match self.wire.next_chunk().await {
                Some(Ok(chunk)) => self.pending = chunk,
                Some(Err(fault)) => {
                    let msg = fault.to_string();
                    self.fault = Some(fault);
                    return Err(std::io::Error::other(msg));
                }
                None if self.fault.is_some() => {
                    return Err(std::io::Error::other(
                        "own origin range wire ended on a fault",
                    ));
                }
                None => return Ok(false),
            }
        }
        Ok(true)
    }
}

impl<W: WireChunks> AsyncStreamReader for BackendReader<W> {
    /// Reads `len` bytes, or fewer only at the wire's end: the bao decoder
    /// reads each leaf with one `read_bytes_exact`, so a short read here would
    /// fail it even though the rest of the leaf is in the next chunk. A read
    /// inside one chunk is a zero-copy slice; only a read that spans chunks is
    /// copied.
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        if len == 0 || !self.refill().await? {
            return Ok(Bytes::new());
        }
        if self.pending.len() >= len {
            return Ok(self.pending.split_to(len));
        }
        let mut out = bytes::BytesMut::new();
        while out.len() < len {
            if !self.refill().await? {
                break;
            }
            let take = self.pending.len().min(len - out.len());
            out.extend_from_slice(&self.pending.split_to(take));
        }
        Ok(out.freeze())
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        let mut out = [0u8; L];
        let mut filled = 0usize;
        while filled < L {
            if !self.refill().await? {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "backend reader exhausted before a fixed-size bao read",
                ));
            }
            let take = self.pending.len().min(L - filled);
            let got = self.pending.split_to(take);
            let dst = out
                .get_mut(filled..filled.saturating_add(take))
                .ok_or_else(|| std::io::Error::other("backend reader fixed-size read overran"))?;
            dst.copy_from_slice(&got);
            filled = filled.saturating_add(take);
        }
        Ok(out)
    }
}

impl<W: WireChunks> StashedFault for BackendReader<W> {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        self.fault.take().map(anyhow::Error::from)
    }
}

#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "tests"
)]
#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use alloy::primitives::U256;
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use decdn_bao_range::{IROH_BLOCK_SIZE, RangedStore, align_range};
    use decdn_cache::{
        CacheEngine, Hash, Origin, OriginFetch, OriginKind, OriginPullError, OriginRangeFetch,
        OriginRangeRequest, OutboardFetch,
    };
    use decdn_client_pull::{BlobSource, Cumulative, IngestStore, PoolLedger, VoucherProgress};
    use iroh_io::AsyncStreamReader;

    use super::{BackendReader, BackendSource, WireChunks};
    use crate::node_origin::NodeAdmitStore;

    /// A minimal own-origin double: serves one blob's aligned ranges plus its
    /// `{H}.obao4` outboard. `data` is held separately from `hash` so a test can
    /// serve bytes that do NOT hash to `H` (a corrupt/misconfigured own origin).
    #[derive(Debug)]
    struct FakeOrigin {
        hash: Hash,
        data: Bytes,
        outboard: Bytes,
        size: u64,
    }

    impl FakeOrigin {
        fn new(hash: Hash, data: &[u8], outboard: Bytes) -> Self {
            Self {
                hash,
                data: Bytes::from(data.to_vec()),
                outboard,
                size: data.len() as u64,
            }
        }
    }

    impl Origin for FakeOrigin {
        fn kind(&self) -> OriginKind {
            OriginKind::Http
        }

        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>>
        {
            let result = if hash == self.hash {
                Ok(OriginFetch::found_one_shot(self.data.clone()))
            } else {
                Ok(OriginFetch::NotFound)
            };
            Box::pin(async move { result })
        }

        fn size(
            &self,
            hash: Hash,
        ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, OriginPullError>> + Send + '_>>
        {
            let out = (hash == self.hash).then_some(self.size);
            Box::pin(async move { Ok(out) })
        }

        fn fetch_outboard(
            &self,
            hash: Hash,
            _outboard_max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>>
        {
            let result = if hash == self.hash {
                OutboardFetch::Found(self.outboard.clone())
            } else {
                OutboardFetch::NotFound
            };
            Box::pin(async move { Ok(result) })
        }

        fn fetch_range_data(
            &self,
            hash: Hash,
            req: OriginRangeRequest,
        ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>>
        {
            let result = if hash == self.hash {
                let s = req.fetch_start as usize;
                let e = req.fetch_end as usize;
                match self.data.get(s..e) {
                    Some(span) => OriginRangeFetch::Ranged {
                        data: Bytes::copy_from_slice(span),
                    },
                    None => OriginRangeFetch::NotFound,
                }
            } else {
                OriginRangeFetch::Unsupported
            };
            Box::pin(async move { Ok(result) })
        }
    }

    /// A blob spanning several chunk groups plus a partial final group, so the
    /// bao tree has real interior nodes.
    fn test_blob() -> Vec<u8> {
        let size = 5 * decdn_cache::CHUNK_GROUP_BYTES as usize + 123;
        (0..size).map(|i| (i % 251) as u8).collect()
    }

    fn fresh_ledger() -> Arc<PoolLedger> {
        Arc::new(PoolLedger::new(Cumulative::default()))
    }

    /// (a) Full-miss whole-blob: `BackendSource::open` yields wire that a fresh
    /// `NodeAdmitStore` ingests to a complete, byte-exact blob under `H`.
    #[tokio::test]
    async fn backend_source_full_miss_admits_complete_blob() -> anyhow::Result<()> {
        let data = test_blob();
        let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data.clone());
        let hash = Hash::from(root);
        let total = data.len() as u64;

        let origin = FakeOrigin::new(hash, &data, outboard);
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

        let source = BackendSource::new(engine, root, total, fresh_ledger());
        let aligned = align_range(0, 0, total).map_err(|e| anyhow::anyhow!("align: {e}"))?;
        let (header, reader) = source.open(root, aligned.clone()).await?;
        assert_eq!(header.total_bytes, total);
        assert_eq!(header.rate_per_mb, 0, "unpaid source quotes rate 0");
        assert_eq!(header.interval_bytes, 0, "unpaid source quotes interval 0");

        // Ingest into a FRESH engine's NodeAdmitStore — the driver's sink — which
        // verifies the wire against `H` as it stores it.
        let tmp2 = tempfile::tempdir()?;
        let engine2 = CacheEngine::open(tmp2.path(), vec![], 64).await?;
        let store = NodeAdmitStore::new(engine2.clone(), hash, total, None);
        let mut drained = IngestStore::ingest_stream(&store, &aligned, reader, None).await?;
        assert_eq!(
            drained.read_bytes(1).await?.len(),
            0,
            "the reader is fully drained by admit"
        );

        assert!(
            RangedStore::is_complete(&store).await?,
            "the whole-blob wire must complete the blob under H"
        );
        assert_eq!(
            engine2.get(hash).await?.as_ref(),
            data.as_slice(),
            "the reconstructed content must be byte-exact"
        );
        Ok(())
    }

    /// (b) Mismatched origin blob, corrupt past the first window: the wire ends
    /// mid-stream on a LOCAL-origin `VerifyFailed`, and the driver's sink
    /// reports that parked fault — no provider/upstream scoring is reachable
    /// from this source.
    #[tokio::test]
    async fn backend_source_mismatch_is_local_verify_fault() -> anyhow::Result<()> {
        let window = decdn_cache::RANGE_PULL_WINDOW_BYTES as usize;
        let genuine: Vec<u8> = (0..window + 5 * decdn_cache::CHUNK_GROUP_BYTES as usize + 123)
            .map(|i| (i % 251) as u8)
            .collect();
        let ob = PreOrderMemOutboard::create(&genuine, IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data.clone());
        let hash = Hash::from(root);
        let total = genuine.len() as u64;

        // Same length; the second window's bytes differ, so it will not verify
        // against H after the first window has already streamed.
        let mut corrupt = genuine.clone();
        for b in &mut corrupt[window..window + 1024] {
            *b ^= 0xFF;
        }
        assert_ne!(Hash::new(&corrupt), hash, "fixtures must differ");
        let origin = FakeOrigin::new(hash, &corrupt, outboard);
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

        let source = BackendSource::new(engine, root, total, fresh_ledger());
        let aligned = align_range(0, 0, total).map_err(|e| anyhow::anyhow!("align: {e}"))?;
        let (_header, reader) = source.open(root, aligned.clone()).await?;

        let tmp2 = tempfile::tempdir()?;
        let engine2 = CacheEngine::open(tmp2.path(), vec![], 64).await?;
        let store = NodeAdmitStore::new(engine2, hash, total, None);
        let err = IngestStore::ingest_stream(&store, &aligned, reader, None)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected VerifyFailed, got Ok"))?;
        let cache_err = err
            .downcast_ref::<decdn_cache::CacheError>()
            .ok_or_else(|| anyhow::anyhow!("expected a CacheError, got {err:?}"))?;
        assert!(
            matches!(cache_err, decdn_cache::CacheError::VerifyFailed { expected } if *expected == hash),
            "a corrupt own origin must surface as a local VerifyFailed, got {cache_err:?}"
        );
        Ok(())
    }

    /// (c) `finish` advances the shared ledger's committed `bytes` by the leg's
    /// wire (at amount 0), so a `drive` over this source reaches completion — the
    /// driver reads `ledger.committed().bytes` for the paid frontier and discards
    /// `finish`'s returned progress. That progress is amount-keyed for buyer
    /// voucher persistence, so a rate-0 self-pay reports no advance (`None`):
    /// there is nothing to pay yourself, and nothing to persist.
    #[tokio::test]
    async fn backend_source_finish_advances_completion_counter() -> anyhow::Result<()> {
        let data = test_blob();
        let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data.clone());
        let hash = Hash::from(root);
        let total = data.len() as u64;

        let origin = FakeOrigin::new(hash, &data, outboard);
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

        let ledger = fresh_ledger();
        let source = BackendSource::new(engine, root, total, Arc::clone(&ledger));
        let aligned = align_range(0, 0, total).map_err(|e| anyhow::anyhow!("align: {e}"))?;
        let (_header, reader) = source.open(root, aligned).await?;
        let expected_wire = reader.expected_wire_len;
        assert!(expected_wire > 0, "a non-empty blob has non-zero wire");

        let progress: VoucherProgress = source.finish(reader).await?;
        // A rate-0 self-pay is not a payment: amount did not rise past the seed,
        // so there is nothing to persist as a buyer voucher.
        assert!(
            progress.advanced().is_none(),
            "own-origin self-pay never advances the payment watermark"
        );
        // The completion frontier the driver actually reads: the shared ledger's
        // committed bytes advanced by the leg's wire, at amount 0.
        assert_eq!(ledger.committed().bytes, U256::from(expected_wire));
        assert_eq!(ledger.committed().amount, U256::ZERO);
        Ok(())
    }

    /// A scripted chunk source: yields `items` in order, then `None`.
    struct VecWire(std::collections::VecDeque<decdn_cache::CacheResult<Bytes>>);

    impl WireChunks for VecWire {
        fn next_chunk(
            &mut self,
        ) -> impl Future<Output = Option<decdn_cache::CacheResult<Bytes>>> + Send {
            let next = self.0.pop_front();
            async move { next }
        }
    }

    fn reader_over(items: Vec<decdn_cache::CacheResult<Bytes>>) -> BackendReader<VecWire> {
        BackendReader::new(VecWire(items.into()), 0)
    }

    /// Fixed-size reads fill across chunk boundaries: a wire cut into 1- and
    /// 7-byte chunks still ingests to the complete, byte-exact blob.
    #[tokio::test]
    async fn backend_reader_reassembles_a_finely_chunked_wire() -> anyhow::Result<()> {
        let data = test_blob();
        let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let hash = Hash::from(root);
        let total = data.len() as u64;
        let aligned = align_range(0, 0, total).map_err(|e| anyhow::anyhow!("align: {e}"))?;
        let wire = decdn_bao_range::encode_verified_range(
            root,
            &aligned,
            &data,
            Bytes::from(ob.data.clone()),
        )?
        .slice(8..);

        let mut items = Vec::new();
        let mut rest = wire;
        let mut step = 1;
        while !rest.is_empty() {
            let take = step.min(rest.len());
            items.push(Ok(rest.split_to(take)));
            step = if step == 1 { 7 } else { 1 };
        }

        let tmp = tempfile::tempdir()?;
        let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
        let store = NodeAdmitStore::new(engine.clone(), hash, total, None);
        IngestStore::ingest_stream(&store, &aligned, reader_over(items), None).await?;
        assert!(RangedStore::is_complete(&store).await?);
        assert_eq!(engine.get(hash).await?.as_ref(), data.as_slice());
        Ok(())
    }

    /// A zero-length read consumes nothing, and a fixed-size read past a clean
    /// end is `UnexpectedEof`.
    #[tokio::test]
    async fn backend_reader_zero_read_and_short_fixed_read() {
        let mut reader = reader_over(vec![Ok(Bytes::from_static(b"abc"))]);
        assert!(reader.read_bytes(0).await.unwrap().is_empty());
        let err = reader.read::<8>().await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    /// A terminal fault fails the read and is handed to the sink once through
    /// `take_fault`.
    #[tokio::test]
    async fn backend_reader_keeps_the_terminal_fault() {
        use decdn_client_pull::sink::StashedFault;

        let hash = Hash::new(b"fault");
        let mut reader = reader_over(vec![
            Ok(Bytes::from_static(b"ab")),
            Err(decdn_cache::CacheError::VerifyFailed { expected: hash }),
        ]);
        assert_eq!(reader.read_bytes(2).await.unwrap().as_ref(), b"ab");
        assert!(
            reader.read_bytes(16).await.is_err(),
            "the fault fails the read"
        );
        assert!(
            reader.read_bytes(16).await.is_err(),
            "and every read after it"
        );
        let fault = reader.take_fault().expect("the fault is kept");
        assert!(matches!(
            fault.downcast_ref::<decdn_cache::CacheError>(),
            Some(decdn_cache::CacheError::VerifyFailed { .. })
        ));
        assert!(reader.take_fault().is_none(), "taken once");
    }
}
