//! G-NODE-04 — blacklist compliance loop (issue #1031).
//!
//! Drives the full cross-layer loop on a real anvil deployment + in-process
//! `decdn-node` daemon:
//!
//! - **Global compliance (`global_blacklist_compliance`):** a node holds and
//!   serves blob `H`; governance adds `H` to `ContentBlacklist`; the node's
//!   blacklist watcher evicts `H` (asserted via admin `evict` dry-run flipping
//!   `was_present` true→false) and then refuses paid delivery (the client fetch
//!   fails). Eviction is durable across a daemon restart. Finally, a signed
//!   post-entry `ProbeResponse` for `H` is driven through the `SlashJudge`
//!   commit-reveal to prove serving-after-blacklist is on-chain slashable.
//! - **Regional scope (`regional_blacklist_scope`, ties GOV-05):** a US and a DE
//!   node both hold `H2`; a US-regional entry is evicted by the US node and
//!   ignored by the DE node, which keeps serving it.
//!
//! **Coverage boundary.** Eviction is the single production lever, and DHT
//! announce-suppression + probe `has_blob:false` are *mechanically downstream*
//! of it (the republisher's and probe handler's `is_evicted` gates, unit-tested
//! in `crates/node/src/dht/publish.rs` and `handlers/probe.rs`). This e2e
//! asserts the two reliably-observable end states — the blob left the store and
//! delivery is refused — rather than re-testing those gates over the network.
//! The as-built `SlashJudge` enforces slashability from an entry's `addedAt`
//! (no on-chain compliance-window grace — that window is the node's *reaction*
//! budget, ADR 011 "target spec vs as-built"), so "after the window" is modeled
//! by stamping evidence after `addedAt`. Appeal-driven un-eviction and the
//! explicit probe-hold interplay are follow-ups (see `blacklist_watcher` docs).

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units
)]

use std::time::Duration;

use alloy::primitives::{B256, U256, keccak256};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolValue;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_common::admin::{AdminRpcClient, EvictRequest};
use decdn_e2e::bindings::{Erc20, ProbeMsg, SlashJudge};
use decdn_e2e::chain::{ChainFixture, region_key};
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_e2e::time;
use decdn_incentive::{ProbeSlashData, slash_judge_domain};
use jsonrpsee::http_client::HttpClient;

const MIB: usize = 1024 * 1024;

/// Overall ceiling so an unbounded await fails fast with a clear message.
/// Cleanup (anvil kill, daemon kill) runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(900);

/// EIP-712 typehash string for `ProbeResponse` — must byte-match
/// `SlashJudge.PROBE_TYPEHASH`.
const PROBE_TYPE: &[u8] =
    b"ProbeResponse(bytes32 hash,bool hasBlob,uint64 ratePerMb,uint64 timestampUs)";
/// `OffenseType.Blacklist` discriminant (`ISlashJudge` enum: Phantom, Rate, Blacklist).
const OFFENSE_BLACKLIST: u8 = 2;
/// The daemon config's quoted rate; echoed into the probe evidence.
const RATE_PER_MB: u64 = 10;

#[tokio::test(flavor = "multi_thread")]
async fn global_blacklist_compliance() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_global()))
        .await
        .context("global blacklist e2e exceeded the overall timeout")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn regional_blacklist_scope() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_regional()))
        .await
        .context("regional blacklist e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run_global() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0x5Au8; 2 * MIB];
    let (node, hash) = NodeFixture::launch(&chain, "US", &payload).await?;
    let admin = node.admin_client()?;
    let hash_key = to_b256(hash);

    // Baseline: the node holds and serves `H`.
    assert!(
        evict_was_present(&admin, hash).await?,
        "node must hold H before blacklisting"
    );
    let client = ClientFixture::new(&chain).await?;
    let outcome = client.fetch(&chain, &node, hash).await?;
    assert_eq!(outcome.bytes, payload, "delivered bytes must match H");

    // Governance blacklists H globally; advance a while to model the compliance
    // window passing.
    chain.add_hash_global(hash_key).await?;
    time::increase_time(&chain.admin, 3600).await?;

    // The watcher evicts H — assert the blob left the store.
    let evicted = poll(Duration::from_secs(60), || async {
        Ok((!evict_was_present(&admin, hash).await?).then_some(()))
    })
    .await?;
    assert!(
        evicted.is_some(),
        "node never evicted the blacklisted blob H"
    );

    // ...and the paid client path now refuses delivery.
    let refused = ClientFixture::new(&chain).await?;
    let fetch = refused.fetch(&chain, &node, hash).await;
    assert!(
        fetch.is_err(),
        "node must refuse to deliver a blacklisted, evicted blob"
    );

    // Eviction is durable across a daemon restart.
    node.restart().await?;
    assert!(
        !evict_was_present(&admin, hash).await?,
        "eviction must survive a restart (evicted.log)"
    );
    let refused2 = ClientFixture::new(&chain).await?;
    assert!(
        refused2.fetch(&chain, &node, hash).await.is_err(),
        "delivery must stay refused after restart"
    );

    // Full signed-evidence drive: serving H after the entry is on-chain
    // slashable via SlashJudge.
    drive_blacklist_slash(&chain, &node, hash).await?;

    Ok(())
}

async fn run_regional() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0xC3u8; 2 * MIB];
    // Same bytes → same hash on both nodes.
    let (us_node, hash) = NodeFixture::launch(&chain, "US", &payload).await?;
    let (de_node, hash_de) = NodeFixture::launch(&chain, "DE", &payload).await?;
    assert_eq!(hash, hash_de, "identical payloads must share a hash");
    let us_admin = us_node.admin_client()?;
    let de_admin = de_node.admin_client()?;

    assert!(
        evict_was_present(&us_admin, hash).await?,
        "US node must hold H2"
    );
    assert!(
        evict_was_present(&de_admin, hash).await?,
        "DE node must hold H2"
    );

    // A US-regional entry: in scope for the US node, out of scope for DE.
    chain
        .add_hash_regional(region_key("US"), to_b256(hash))
        .await?;
    time::increase_time(&chain.admin, 3600).await?;

    // US node evicts.
    let evicted = poll(Duration::from_secs(60), || async {
        Ok((!evict_was_present(&us_admin, hash).await?).then_some(()))
    })
    .await?;
    assert!(
        evicted.is_some(),
        "US node never evicted the in-region blacklisted blob"
    );

    // DE node keeps serving it: still present, and paid delivery still works.
    assert!(
        evict_was_present(&de_admin, hash).await?,
        "DE (out-of-region) node must NOT evict a US-regional entry"
    );
    let client = ClientFixture::new(&chain).await?;
    let outcome = client.fetch(&chain, &de_node, hash).await?;
    assert_eq!(
        outcome.bytes, payload,
        "out-of-region node must keep delivering the blob"
    );

    Ok(())
}

/// Dry-run `admin_v1_evict` — `was_present` is true iff the node currently holds
/// (and has not evicted) the blob. Dry-run never mutates cache state.
async fn evict_was_present(admin: &HttpClient, hash: Hash) -> anyhow::Result<bool> {
    let resp = admin
        .evict(EvictRequest {
            hash: alloy::hex::encode(hash.as_bytes()),
            dry_run: true,
        })
        .await
        .context("admin evict dry-run")?;
    Ok(resp.was_present)
}

/// Construct a signed post-entry `ProbeResponse` for `hash` and drive it through
/// `SlashJudge.submitBlacklistChallenge` (commit-reveal + challenge bond),
/// asserting the slash lands. Proves serving a blacklisted hash is slashable.
async fn drive_blacklist_slash(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: Hash,
) -> anyhow::Result<()> {
    let hash_key = to_b256(hash);
    // Evidence timestamp: current chain time (µs), strictly after the entry's
    // `addedAt` and fresh enough to pass the staleness check.
    let response_ts_us = chain.block_timestamp().await? * 1_000_000;

    // The signed `slash_sig` — the production probe signer produces exactly the
    // EIP-712 digest `SlashJudge` verifies.
    let probe = ProbeSlashData {
        hash: hash_key,
        has_blob: true,
        rate_per_mb: RATE_PER_MB,
        timestamp_us: response_ts_us,
    };
    let domain = slash_judge_domain(chain.chain_id, chain.addrs.slash_judge);
    let slash_sig = probe.sign(&node.operator, &domain)?.as_bytes().to_vec();

    // evidenceHash = keccak(abi.encode(uint8(Blacklist), structHash, isStream)),
    // structHash = keccak(abi.encode(PROBE_TYPEHASH, fields)) — mirrors SlashJudge.
    let struct_hash = keccak256(
        (
            keccak256(PROBE_TYPE),
            hash_key,
            true,
            RATE_PER_MB,
            response_ts_us,
        )
            .abi_encode(),
    );
    // `uint8(Blacklist)` and `uint256(2)` abi-encode to the same 32-byte word.
    let evidence_hash = keccak256((U256::from(OFFENSE_BLACKLIST), struct_hash, false).abi_encode());

    // A funded challenger holding the challenge bond (refunded on success).
    let challenger = PrivateKeySigner::random();
    let challenger_addr = challenger.address();
    chain.fund_eth(challenger_addr, 100).await?;
    let judge = SlashJudge::new(chain.addrs.slash_judge, &chain.admin);
    let bond = judge
        .challengeBond()
        .call()
        .await
        .context("read challengeBond")?;
    chain.transfer_token(challenger_addr, bond).await?;

    let cp = chain.provider_for(&challenger);
    let approve = Erc20::new(chain.addrs.token, &cp)
        .approve(chain.addrs.slash_judge, bond)
        .send()
        .await
        .context("challenger approve TOKEN")?
        .get_receipt()
        .await
        .context("approve receipt")?;
    assert!(approve.status(), "challenger TOKEN approve reverted");

    // Commit, mature past MIN_REVEAL_DELAY (1 min), then reveal.
    let salt = B256::ZERO;
    let commitment = keccak256((evidence_hash, salt, challenger_addr).abi_encode());
    let judge_c = SlashJudge::new(chain.addrs.slash_judge, &cp);
    let commit = judge_c
        .commitChallenge(commitment)
        .send()
        .await
        .context("commitChallenge send")?
        .get_receipt()
        .await
        .context("commitChallenge receipt")?;
    assert!(commit.status(), "commitChallenge reverted");
    time::increase_time(&chain.admin, 61).await?;

    let response_data = ProbeMsg {
        hash: hash_key,
        hasBlob: true,
        ratePerMb: RATE_PER_MB,
        timestampUs: response_ts_us,
    }
    .abi_encode();
    let node_id = B256::from_slice(node.node_id.as_bytes());
    let receipt = judge_c
        .submitBlacklistChallenge(
            node.operator_addr,
            node_id,
            hash_key,
            response_data.into(),
            slash_sig.into(),
            false,
            salt,
        )
        .send()
        .await
        .context("submitBlacklistChallenge send")?
        .get_receipt()
        .await
        .context("submitBlacklistChallenge receipt")?;
    assert!(
        receipt.status(),
        "submitBlacklistChallenge reverted — serving a blacklisted hash was not slashable"
    );
    Ok(())
}

fn to_b256(hash: Hash) -> B256 {
    B256::from(*hash.as_bytes())
}

/// Poll `f` until it yields `Some`, or `timeout` elapses. A closure error aborts
/// immediately with that error rather than a generic timeout.
async fn poll<T, F, Fut>(timeout: Duration, mut f: F) -> anyhow::Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Option<T>>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await? {
            return Ok(Some(v));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
