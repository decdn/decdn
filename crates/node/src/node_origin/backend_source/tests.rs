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
use decdn_client::{BlobSource, Cumulative, IngestStore, PoolLedger, VoucherProgress};
use iroh_io::AsyncStreamReader;

use super::{BackendReader, BackendSource, WireChunks};
use crate::node_origin::NodeAdmitStore;
use std::assert_matches;

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
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
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
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, OriginPullError>> + Send + '_>> {
        let out = (hash == self.hash).then_some(self.size);
        Box::pin(async move { Ok(out) })
    }

    fn fetch_outboard(
        &self,
        hash: Hash,
        _outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>> {
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
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
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
    let (mut drained, _) =
        IngestStore::ingest_stream(&store, &aligned, reader, None, aligned.blob_size(), None)
            .await?;
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
    let err = IngestStore::ingest_stream(&store, &aligned, reader, None, aligned.blob_size(), None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected VerifyFailed, got Ok"))?;
    let cache_err = err
        .downcast_ref::<decdn_cache::CacheError>()
        .ok_or_else(|| anyhow::anyhow!("expected a CacheError, got {err:?}"))?;
    assert_matches!(
        cache_err,
        decdn_cache::CacheError::VerifyFailed { expected } if *expected == hash,
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
    IngestStore::ingest_stream(
        &store,
        &aligned,
        reader_over(items),
        None,
        aligned.blob_size(),
        None,
    )
    .await?;
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
    use decdn_client::sink::StashedFault;

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
    assert_matches!(
        fault.downcast_ref::<decdn_cache::CacheError>(),
        Some(decdn_cache::CacheError::VerifyFailed { .. })
    );
    assert!(reader.take_fault().is_none(), "taken once");
}
