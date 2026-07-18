//! G-ORIGIN-04: migrate a cached hash from default-open to an assigned origin.

#![cfg(feature = "anvil-e2e")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]

use std::time::Duration;

use alloy::primitives::{B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::bindings::FeeRouter;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;

const MIB: usize = 1024 * 1024;
const EPOCH_LENGTH_SECS: u64 = 7 * 24 * 60 * 60;
const OVERALL_TIMEOUT: Duration = Duration::from_secs(900);

#[tokio::test(flavor = "multi_thread")]
async fn default_open_origin_migrates_without_revoking_cached_serving() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("G-ORIGIN-04 exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0x43; 2 * MIB];

    // A holds the canonical bytes and begins in the default-open origin set.
    let (origin_a, hash) = NodeFixture::launch(&chain, "US", &payload).await?;
    let hash_b256 = B256::from_slice(hash.as_bytes());

    chain
        .add_default_open_operator(origin_a.operator_addr())
        .await?;
    assert!(chain.content_namespaces(hash_b256).await?.is_empty());
    assert_eq!(
        chain.origins(U256::ZERO).await?,
        vec![origin_a.operator_addr()]
    );
    assert!(
        chain
            .is_authorized_origin(U256::ZERO, origin_a.operator_addr())
            .await?
    );
    // A separate bonded node starts without H and has no matching local-origin
    // object. Its paid cache miss must therefore resolve A through the on-chain
    // default-open directory, acquire verified bytes, and retain them locally.
    let cache = NodeFixture::launch_pull_through_cache(&chain, "US", &[&origin_a]).await?;
    chain
        .mint_usdc(cache.operator_addr(), U256::from(100_000_000u64))
        .await?;
    let client = ClientFixture::new(&chain).await?;
    let first = client.fetch(&chain, &cache, hash).await?;
    assert_eq!(first.bytes, payload);
    assert!(client.probe(&cache, hash).await?.body.has_blob);

    wait_for_origin_settlement(&chain, &origin_a).await?;

    // Active operator B now holds the same H but remains outside namespace 0's
    // allowlist, so chain truth does not recognize it as an origin for H.
    let (origin_b, hash_b) = NodeFixture::launch(&chain, "US", &payload).await?;
    assert_eq!(hash_b, hash);
    assert!(
        !chain
            .is_authorized_origin(U256::ZERO, origin_b.operator_addr())
            .await?
    );
    assert_eq!(
        chain.served_bytes(origin_b.operator_addr()).await?,
        U256::ZERO,
        "the non-allowlisted operator must not be selected as a default-open origin"
    );

    // Migrate H to a registered namespace assigned to B. A deliberately
    // remains on namespace 0: the contract resolution seam must switch to B
    // because the claim removes H from default-open treatment.
    let publisher = PrivateKeySigner::random();
    let namespace = chain.create_namespace(&publisher).await?;
    chain
        .claim_content(&publisher, namespace, hash_b256)
        .await?;
    chain
        .propose_assignment(&publisher, namespace, &[origin_b.operator_addr()])
        .await?;
    chain.activate_assignment_after_timelock(namespace).await?;
    assert_eq!(chain.content_namespaces(hash_b256).await?, vec![namespace]);
    assert_eq!(
        chain.origins(namespace).await?,
        vec![origin_b.operator_addr()]
    );
    assert_eq!(
        chain.origins(U256::ZERO).await?,
        vec![origin_a.operator_addr()]
    );
    assert!(
        chain
            .is_authorized_origin(U256::ZERO, origin_a.operator_addr())
            .await?,
        "A stays on namespace 0 so the hash migration, not allowlist removal, drives the switch"
    );
    assert!(
        chain
            .is_authorized_origin(namespace, origin_b.operator_addr())
            .await?
    );

    let baseline_epoch = chain.head_timestamp().await? / EPOCH_LENGTH_SECS;
    let fee = FeeRouter::new(chain.addrs().fee_router, chain.admin());
    let default_open_before = fee
        .bytesPerEpoch(origin_a.operator_addr(), baseline_epoch)
        .call()
        .await?;
    let assigned_before = fee
        .bytesPerEpoch(origin_b.operator_addr(), baseline_epoch)
        .call()
        .await?;

    let cached = client.fetch(&chain, &cache, hash).await?;
    assert_eq!(cached.bytes, payload);
    assert_eq!(
        fee.bytesPerEpoch(origin_a.operator_addr(), baseline_epoch)
            .call()
            .await?,
        default_open_before,
        "cached re-serve must not add a delivery for default-open origin A"
    );
    assert_eq!(
        fee.bytesPerEpoch(origin_b.operator_addr(), baseline_epoch)
            .call()
            .await?,
        assigned_before,
        "cached re-serve must not add a delivery for assigned origin B"
    );

    Ok(())
}

async fn wait_for_origin_settlement(
    chain: &ChainFixture,
    origin: &NodeFixture,
) -> anyhow::Result<()> {
    let admin = origin.admin_client()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let channels = admin.channels().await.context("origin admin channels")?;
        let redeem_threshold = U256::from(channels.redeem_threshold_micro_usdc);
        for snapshot in channels.channels {
            if snapshot.outstanding_micro_usdc == 0 {
                continue;
            }
            let channel_id = snapshot
                .channel_id
                .parse::<B256>()
                .context("parse origin channel id")?;
            let on_chain = decdn_e2e::assert::read_channel(
                chain.admin(),
                chain.addrs().payment_channel,
                channel_id,
            )
            .await?;
            let outstanding = U256::from(snapshot.outstanding_micro_usdc);
            if !on_chain.withdrawnAmount.is_zero()
                && outstanding.saturating_sub(on_chain.withdrawnAmount) < redeem_threshold
            {
                return Ok(());
            }
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "origin's latest voucher did not settle"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
