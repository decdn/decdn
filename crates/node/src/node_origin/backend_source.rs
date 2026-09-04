//! `BackendSource` — the node's UNPAID [`decdn_client_pull::BlobSource`] over its
//! own configured origin (fs/http/s3), for the own-origin serve-miss.
//!
//! The peer serve-miss path pulls a cold blob from another node over a paid
//! `cdn/client/v1` channel ([`decdn_client_pull::PeerSource`]). The own-origin
//! serve-miss flow is the LOCAL twin: the bytes are already reachable through this
//! node's own origin, so
//! there is no counterparty, no channel, and no payment — but the driver, sink,
//! and serve leg are reused verbatim. This source is what lets that happen: it
//! produces the header-less interleaved bao wire for one [`AlignedRange`] straight
//! out of [`CacheEngine::origin_encode_range`], so the existing `NodeAdmitStore`
//! sink verifies-and-stores it against the root `H` exactly as it does a peer
//! pull's wire.
//!
//! # Why the origin bytes are a LOCAL fault, not upstream corruption
//!
//! By the time this source runs, the node has already signed a `StreamResponse`
//! committing to serve the blob under `H` to a paying client.
//! [`CacheEngine::origin_encode_range`] therefore treats a range that fails bao
//! verification as a HARD [`decdn_cache::CacheError::VerifyFailed`] — a
//! corrupt/misconfigured OWN origin — rather than degrading it (there is no
//! upstream to blame and no fallback that still honours `H`). This source
//! propagates that fault out of [`BlobSource::open`]; nothing here scores a
//! provider, because there is no provider.
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
use decdn_cache::{CacheEngine, Hash};
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
#[allow(dead_code, reason = "wired by FA.2/FA.3 orchestration")]
pub(crate) struct BackendSource {
    engine: CacheEngine,
    root: [u8; 32],
    total_bytes: u64,
    /// Local completion bookkeeping ONLY — no channel, no chain, no counterparty.
    self_pay: Arc<PoolLedger>,
}

#[allow(dead_code, reason = "wired by FA.2/FA.3 orchestration")]
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
            // Fetch + verify + encode the range out of our own origin. A verify
            // failure surfaces here as `CacheError::VerifyFailed` (a local-origin
            // fault) via `?`; a decline (no origin serves it any more) is `None`.
            let Some(combined) = self
                .engine
                .origin_encode_range(Hash::from(hash), &range)
                .await?
            else {
                anyhow::bail!(
                    "backend origin no longer serves range for {}",
                    Hash::from(hash)
                );
            };
            // `origin_encode_range` returns the header-full wire (leading 8-byte LE
            // size header); the driver's sink wants the header-less body (ADR 038,
            // the size comes from the signed `total_bytes` instead). Guard the
            // length with `get` — never index — then take the zero-copy slice.
            let wire_len = combined
                .get(8..)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "backend origin wire for {} is shorter than its 8-byte size header",
                        Hash::from(hash),
                    )
                })?
                .len();
            let wire = combined.slice(8..);
            let header = UpstreamPullHeader {
                total_bytes: self.total_bytes,
                rate_per_mb: 0,
                interval_bytes: 0,
                // A local origin re-encode, not a network round trip.
                ttfb_ms: 0.0,
            };
            Ok((
                header,
                BackendReader {
                    wire,
                    wire_len: wire_len as u64,
                },
            ))
        })
    }

    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async move {
            // Advance the LOCAL completion counter by this leg's drained wire bytes
            // at rate 0, so `committed.bytes` reaches the gap end (the driver's
            // paid-frontier completion) while `committed.amount` stays 0 — the
            // `ScriptedSource::finish` paying arm, with a zero rate. This is not
            // payment; see the module docs.
            if reader.wire_len > 0 {
                self.self_pay
                    .issue(
                        reader.wire_len,
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

/// The reader a [`BackendSource`] yields: a fixed header-less bao wire buffer over
/// [`Bytes`] with a no-op [`StashedFault`] (an own-origin leg parks no typed
/// upstream fault — a bad origin already failed the encode in
/// [`BlobSource::open`]). Mirrors the shape of `client-pull`'s `ScriptedReader` /
/// the node's `admit_store` test `MemReader`, but as a real (non-test) type.
#[allow(dead_code, reason = "wired by FA.2/FA.3 orchestration")]
pub(crate) struct BackendReader {
    wire: Bytes,
    /// Wire byte count handed to this reader (before consumption), used by
    /// [`BlobSource::finish`] to advance the local completion counter.
    wire_len: u64,
}

impl AsyncStreamReader for BackendReader {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        let take = self.wire.len().min(len);
        Ok(self.wire.split_to(take))
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        if self.wire.len() < L {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "backend reader exhausted before a fixed-size bao read",
            ));
        }
        let got = self.wire.split_to(L);
        let mut out = [0u8; L];
        out.copy_from_slice(&got);
        Ok(out)
    }
}

impl StashedFault for BackendReader {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        None
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

    use super::BackendSource;
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

        fn fetch_range(
            &self,
            hash: Hash,
            req: OriginRangeRequest,
            _outboard_max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>>
        {
            let result = if hash == self.hash {
                let s = req.fetch_start as usize;
                let e = req.fetch_end as usize;
                match self.data.get(s..e) {
                    Some(span) => OriginRangeFetch::Ranged {
                        data: Bytes::copy_from_slice(span),
                        outboard: self.outboard.clone(),
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

    /// (b) Mismatched origin blob: `open` fails with a LOCAL-origin
    /// `VerifyFailed` — no provider/upstream scoring is reachable from this
    /// source.
    #[tokio::test]
    async fn backend_source_mismatch_is_local_verify_fault() -> anyhow::Result<()> {
        let genuine = test_blob();
        let ob = PreOrderMemOutboard::create(&genuine, IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data.clone());
        let hash = Hash::from(root);
        let total = genuine.len() as u64;

        // Same length, different bytes: the served span will not verify against H.
        let corrupt: Vec<u8> = genuine.iter().map(|b| b ^ 0xFF).collect();
        assert_ne!(Hash::new(&corrupt), hash, "fixtures must differ");
        let origin = FakeOrigin::new(hash, &corrupt, outboard);
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

        let source = BackendSource::new(engine, root, total, fresh_ledger());
        let aligned = align_range(0, 0, total).map_err(|e| anyhow::anyhow!("align: {e}"))?;
        let err = source
            .open(root, aligned)
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
        let expected_wire = reader.wire_len;
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
}
