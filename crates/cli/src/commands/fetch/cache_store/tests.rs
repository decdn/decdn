use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use bao_tree::io::outboard::PreOrderMemOutboard;
use decdn_bao_range::{CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE};
use decdn_cache::CacheEngine;
use decdn_client::{
    Cumulative, FakeFunder, NoCache, PoolContext, PoolLedger, ProgressClock, ProvenSize,
    PullConfig, Recovery, ScriptedSource, StaticSources, StopPolicy, StreamCandidate, Streamer,
};
use tokio::io::AsyncReadExt as _;

use super::CacheStores;

/// A [`Streamer`] filling the cache hands its reader every byte of the blob,
/// and the cache holds the blob whole after it.
#[tokio::test]
async fn a_stream_reads_back_what_it_fills_into_the_cache() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let engine = CacheEngine::open(dir.path(), Vec::new(), 0).await?;
    let len = usize::try_from(96 * CHUNK_GROUP_BYTES)?;
    let data: Vec<u8> = (0..len)
        .map(|i| u8::try_from(i % 251).unwrap_or_default())
        .collect();
    let root = *PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE)
        .root
        .as_bytes();
    let total = u64::try_from(len)?;

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
