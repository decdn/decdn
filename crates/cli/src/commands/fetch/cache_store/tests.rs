use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_client::{
    Cumulative, FakeFunder, NoCache, PoolContext, PoolLedger, ProgressClock, ProvenSize,
    PullConfig, Recovery, ScriptedSource, StaticSources, StopPolicy, StreamCandidate, Streamer,
};
use tokio::io::AsyncReadExt as _;

use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;
use decdn_bao_range::{
    CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE, RangedStore, align_range, encode_verified_range,
};
use decdn_cache::CacheEngine;
use decdn_client::sink::StashedFault;
use decdn_client::{IngestEnd, IngestStore, StreamStore, StreamStores};
use iroh_io::AsyncStreamReader;

use super::CacheStores;

/// A leg's wire in memory, with no fault to park.
struct MemReader(Bytes);

impl AsyncStreamReader for MemReader {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        let take = self.0.len().min(len);
        Ok(self.0.split_to(take))
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        let mut out = [0u8; L];
        if self.0.len() < L {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        out.copy_from_slice(&self.0.split_to(L));
        Ok(out)
    }
}

impl StashedFault for MemReader {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        None
    }
}

/// A blob of `groups` chunk groups, its root, and the header-less bao wire
/// for the whole of it.
fn blob(groups: u64) -> anyhow::Result<([u8; 32], Vec<u8>, decdn_bao_range::AlignedRange, Bytes)> {
    let len = usize::try_from(groups * CHUNK_GROUP_BYTES)?;
    let data: Vec<u8> = (0..len)
        .map(|i| u8::try_from(i % 251).unwrap_or_default())
        .collect();
    let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    let root = *outboard.root.as_bytes();
    let total = u64::try_from(len)?;
    let range = align_range(0, total, total)?;
    let wire = encode_verified_range(root, &range, &data, Bytes::from(outboard.data))?;
    // The cache admits the header-less wire: the size comes from the store.
    Ok((root, data, range, wire.slice(8..)))
}

#[tokio::test]
async fn a_drained_leg_lands_in_the_cache_for_the_next_stream() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let engine = CacheEngine::open(dir.path(), Vec::new(), 0).await?;
    let stores = CacheStores::new(engine);
    let (root, data, range, wire) = blob(4)?;
    let total = range.blob_size();

    let store = stores.open(root, total).await?;
    let grew = store.present_grew().notified();
    tokio::pin!(grew);
    grew.as_mut().enable();
    let seen = Mutex::new(Vec::new());
    let on_progress = |received: u64| {
        if let Ok(mut seen) = seen.lock() {
            seen.push(received);
        }
    };
    let reader = MemReader(wire);
    let (_, end) =
        IngestStore::ingest_stream(&store, &range, reader, Some(&on_progress), total, None).await?;

    anyhow::ensure!(end == IngestEnd::Drained);
    anyhow::ensure!(
        *seen.lock().map_err(|_| anyhow::anyhow!("poisoned"))?
            == vec![
                CHUNK_GROUP_BYTES,
                2 * CHUNK_GROUP_BYTES,
                3 * CHUNK_GROUP_BYTES,
                4 * CHUNK_GROUP_BYTES
            ],
        "progress counts the bytes verified on the range, one report per group"
    );
    anyhow::ensure!(
        tokio::time::timeout(std::time::Duration::from_secs(1), grew)
            .await
            .is_ok(),
        "a waiting reader wakes as the bytes land"
    );
    // A later stream of the blob opens the same entry, already whole.
    let again = stores.open(root, total).await?;
    anyhow::ensure!(again.is_complete().await?);
    anyhow::ensure!(again.read(0, total).await? == data);
    Ok(())
}

#[tokio::test]
async fn a_lowered_end_stops_the_leg_at_a_group_boundary() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let engine = CacheEngine::open(dir.path(), Vec::new(), 0).await?;
    let stores = CacheStores::new(engine);
    let (root, data, range, wire) = blob(8)?;
    let total = range.blob_size();

    let store = stores.open(root, total).await?;
    let stop_at = AtomicU64::new(CHUNK_GROUP_BYTES + 1);
    let reader = MemReader(wire);
    let (_, end) =
        IngestStore::ingest_stream(&store, &range, reader, None, total, Some(&stop_at)).await?;

    anyhow::ensure!(end == IngestEnd::Stopped);
    let held = 2 * CHUNK_GROUP_BYTES;
    let missing = store.missing_ranges(0, total).await?;
    anyhow::ensure!(
        missing
            == bao_tree::ChunkRanges::from(
                bao_tree::ChunkNum(held / 1024)..bao_tree::ChunkNum(total / 1024)
            ),
        "the two groups up to the stop are present, the rest missing: {missing:?}"
    );
    let prefix = data
        .get(..usize::try_from(held)?)
        .ok_or_else(|| anyhow::anyhow!("short blob"))?;
    anyhow::ensure!(store.read(0, held).await? == prefix);
    Ok(())
}

/// A [`Streamer`] filling the cache hands its reader every byte of the blob,
/// and the cache holds the blob whole after it.
#[tokio::test]
async fn a_stream_reads_back_what_it_fills_into_the_cache() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let engine = CacheEngine::open(dir.path(), Vec::new(), 0).await?;
    let (root, data, range, _) = blob(96)?;
    let total = range.blob_size();

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
    anyhow::ensure!(source.root() == root);
    let ctx = PoolContext {
        pool_id: B256::ZERO,
        provider: Address::repeat_byte(0xAB),
        deposit: U256::from(u128::MAX),
        client_signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
        capability: None,
    };
    let candidate = StreamCandidate::new(source, Arc::new(Mutex::new(ctx)), ledger);
    let sources = StaticSources::new(vec![candidate])?;
    let holders = sources.holders();
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(u128::MAX)));
    let streamer =
        Streamer::with_stores(sources, funder, CacheStores::new(engine.clone())).holders(holders);
    let stop = StopPolicy::new(false, None, Arc::new(ProgressClock::new()));
    let (mut reader, mut drive) = streamer
        .open(
            root,
            ProvenSize::assume_for_tests(total),
            &PullConfig::default(),
            Arc::new(NoCache),
            stop,
        )
        .await?;
    let mut out = Vec::new();
    let read = drive.alongside(reader.read_to_end(&mut out)).await;
    drive.finish().await?;
    read?;
    anyhow::ensure!(
        out.len() == data.len(),
        "{} of {} bytes",
        out.len(),
        data.len()
    );
    anyhow::ensure!(out == data, "the stream is the blob");
    anyhow::ensure!(
        engine
            .present_ranges(decdn_cache::Hash::from_bytes(root))
            .await?
            .is_complete()
    );
    Ok(())
}
