//! G-NODE-04 — blacklist compliance loop (issue #1031).
//!
//! Drives the full cross-layer loop on a real anvil deployment + in-process
//! `decdn-node` daemon:
//!
//! - **Global compliance (`global_blacklist_compliance`):** a node holds and
//!   serves blob `H`; governance adds `H` to `ContentBlacklist`; the node's
//!   blacklist watcher evicts `H` (asserted via admin `evict` dry-run flipping
//!   `was_present` true→false), then refuses paid delivery *with the blacklist
//!   reason* (`HashBlacklisted`, not a bare error), and its probe handler
//!   reports `has_blob: false`. Eviction is durable across a daemon restart.
//!   Finally, a signed post-entry `ProbeResponse` for `H` is driven through the
//!   `SlashJudge` commit-reveal to prove serving-after-blacklist is slashable.
//! - **Regional scope (`regional_blacklist_scope`, ties GOV-05):** a US and a DE
//!   node both hold `H2`; a US-regional entry is evicted by the US node and
//!   ignored by the DE node. A global sentinel blob (held only by DE, blacklisted
//!   *after* the regional entry) is the ordering barrier: DE evicting it proves
//!   its watcher processed past the regional entry before we assert non-eviction.
//! - **Scope transition (`regional_scope_transition`):** a US-regional entry is
//!   retained-but-not-evicted by a DE operator, then evicted after the operator
//!   `updateRegion`s to US — a scope change that emits no `ContentBlacklist`
//!   event, so only the watcher's periodic re-scope of its retained deny-set
//!   catches it.
//!
//! **Coverage.** The journey exercises all three serving seams end-to-end
//! against the daemon — delivery refusal (matched to `HashBlacklisted`), the
//! probe handler (`has_blob: false`), and on-chain slashability. DHT
//! announce-suppression stays delegated to `crates/node/src/dht/publish.rs`
//! unit tests (its `refuses` gate). `SlashJudge` enforces slashability from
//! an entry's `effectiveAt` (`addedAt + complianceWindow`, ADR 011 § Compliance
//! Window, #1169), so the slash drive advances chain time past that window
//! before stamping evidence — the node's *reaction* budget is now an on-chain
//! grace, not just a prose target. Appeal-driven un-eviction and the explicit
//! probe-hold interplay are follow-ups (see `blacklist_watcher` docs).

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
use decdn_e2e::bindings::{Erc20, SlashJudge, SlashJudgeBlacklist};
use decdn_e2e::chain::{ChainFixture, region_key};
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_e2e::time;
use decdn_incentive::{ProbeSlashData, slash_judge_domain};
use jsonrpsee::http_client::HttpClient;

const MIB: usize = 1024 * 1024;

/// Overall ceiling so an unbounded await fails fast with a clear message.
/// Cleanup (anvil kill, daemon kill) runs on drop even on timeout. Kept
/// comfortably below the `anvil-e2e` job timeout so this per-test message wins
/// over the opaque job kill; ~3-4× the observed runtime.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(600);

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

#[tokio::test(flavor = "multi_thread")]
async fn regional_scope_transition() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_scope_transition()))
        .await
        .context("scope-transition e2e exceeded the overall timeout")??;
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
    time::increase_time(chain.admin(), 3600).await?;

    // The watcher evicts H — assert the blob left the store.
    let evicted = poll(Duration::from_secs(60), || async {
        Ok((!evict_was_present(&admin, hash).await?).then_some(()))
    })
    .await?;
    assert!(
        evicted.is_some(),
        "node never evicted the blacklisted blob H"
    );

    // ...the paid client path now refuses delivery for the blacklist reason
    // specifically (not some unrelated channel/connect/payment failure).
    assert_refused_as_blacklisted(&chain, &node, hash).await?;

    // ...and the probe handler stops signing `has_blob: true` (the phantom-blob
    // slash seam — distinct from the delivery path above).
    let probe = ClientFixture::new(&chain).await?.probe(&node, hash).await?;
    assert!(
        !probe.body.has_blob,
        "daemon must report has_blob:false for an evicted blacklisted blob"
    );

    // Eviction is durable across a daemon restart — and so is its *cause*. The
    // refusal assertion below is the cross-layer proof of the governance deny
    // projection: `evicted.log` alone would bring the node back up answering
    // `EvictedSinceProbe`, silently re-opening the wire-code fingerprint that
    // distinguishes a governance takedown from this operator's private denylist.
    node.restart().await?;
    assert!(
        !evict_was_present(&admin, hash).await?,
        "eviction must survive a restart (evicted.log)"
    );
    assert_refused_as_blacklisted(&chain, &node, hash).await?;

    // Full signed-evidence drive: serving H after the entry is on-chain
    // slashable via SlashJudge.
    drive_blacklist_slash(&chain, &node, hash).await?;

    Ok(())
}

/// Assert a paid fetch of `hash` from `node` is refused *for the blacklist
/// reason* — matching `HashBlacklisted` so a channel/connect/payment
/// regression that also fails the fetch cannot green this check.
async fn assert_refused_as_blacklisted(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: Hash,
) -> anyhow::Result<()> {
    let err = ClientFixture::new(chain)
        .await?
        .fetch(chain, node, hash)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("fetch of an evicted blacklisted blob must fail"))?;
    let msg = format!("{err:#}");
    // `HashBlacklisted`, NOT `EvictedSinceProbe`. A governance takedown and this
    // operator's own `[content] denied_hashes` must be one wire code (ADR 011
    // §`StreamRequest` Response) — answering governance from the eviction arm
    // made the local-denylist code a unique fingerprint for an operator's
    // private legal exposure. This assertion is the cross-layer half of the unit
    // test `local_and_governance_hash_denials_share_one_wire_code`: it proves the
    // watcher's deny actually reaches the wire on a real deployment, which is
    // where the earlier version of this feature broke.
    anyhow::ensure!(
        msg.contains("HashBlacklisted"),
        "refusal must be the blacklist reason, got: {msg}"
    );
    Ok(())
}

async fn run_regional() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0xC3u8; 2 * MIB];
    // A distinct blob only the DE node holds, used as an event-ordering barrier.
    let sentinel = vec![0xD4u8; MIB];
    // US holds H2; DE holds H2 + the sentinel (same H2 bytes → same hash).
    let (us_node, us_hashes) = NodeFixture::launch_with_blobs(&chain, "US", &[&payload]).await?;
    let (de_node, de_hashes) =
        NodeFixture::launch_with_blobs(&chain, "DE", &[&payload, &sentinel]).await?;
    let hash = us_hashes[0];
    let sentinel_hash = de_hashes[1];
    assert_eq!(de_hashes[0], hash, "shared payload must share a hash");
    let us_admin = us_node.admin_client()?;
    let de_admin = de_node.admin_client()?;

    assert!(evict_was_present(&us_admin, hash).await?, "US must hold H2");
    assert!(evict_was_present(&de_admin, hash).await?, "DE must hold H2");
    assert!(
        evict_was_present(&de_admin, sentinel_hash).await?,
        "DE must hold the sentinel"
    );

    // A US-regional entry (in scope for US, out of scope for DE), then a GLOBAL
    // sentinel that DE holds. Emitting the sentinel *after* the regional entry
    // makes DE's eviction of the sentinel a positive signal that its watcher has
    // processed events at/after the regional entry — so a scope bug that evicts
    // the regional entry a cycle late would already have fired before we assert
    // non-eviction.
    chain
        .add_hash_regional(region_key("US"), to_b256(hash))
        .await?;
    chain.add_hash_global(to_b256(sentinel_hash)).await?;
    time::increase_time(chain.admin(), 3600).await?;

    // US evicts the in-region entry.
    let us_evicted = poll(Duration::from_secs(60), || async {
        Ok((!evict_was_present(&us_admin, hash).await?).then_some(()))
    })
    .await?;
    assert!(
        us_evicted.is_some(),
        "US node never evicted the in-region blacklisted blob"
    );

    // Barrier: DE evicting the global sentinel proves its watcher processed past
    // the regional entry.
    let de_barrier = poll(Duration::from_secs(60), || async {
        Ok((!evict_was_present(&de_admin, sentinel_hash).await?).then_some(()))
    })
    .await?;
    assert!(
        de_barrier.is_some(),
        "DE node never evicted the global sentinel (watcher not processing events)"
    );

    // DE ignored the US-regional entry: still holds H2 and still delivers it.
    assert!(
        evict_was_present(&de_admin, hash).await?,
        "DE (out-of-region) node must NOT evict a US-regional entry"
    );
    let outcome = ClientFixture::new(&chain)
        .await?
        .fetch(&chain, &de_node, hash)
        .await?;
    assert_eq!(
        outcome.bytes, payload,
        "out-of-region node must keep delivering the blob"
    );

    Ok(())
}

/// A blacklist entry that is out of scope when first seen must still be evicted
/// once a later transition brings it into scope — even though that transition
/// (here `updateRegion`) emits no `ContentBlacklist` event, so it is only caught
/// by the watcher's periodic re-scope of its retained deny-set.
async fn run_scope_transition() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0xE7u8; 2 * MIB];
    // The operator self-attests region DE and holds H.
    let (node, hash) = NodeFixture::launch(&chain, "DE", &payload).await?;
    let admin = node.admin_client()?;
    assert!(evict_was_present(&admin, hash).await?, "node must hold H");

    // Blacklist H regionally for US — out of scope for the DE operator. The
    // watcher retains it in its deny-set but must NOT evict.
    chain
        .add_hash_regional(region_key("US"), to_b256(hash))
        .await?;
    // Several 2s poll cycles to confirm the out-of-scope entry is not evicted.
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(
        evict_was_present(&admin, hash).await?,
        "out-of-region entry must NOT be evicted before the region changes"
    );

    // Operator moves to US (no ContentBlacklist event fires) — H is now in scope.
    chain.update_region(node.operator(), "US").await?;

    // The periodic re-scope of the retained deny-set now finds H in scope and evicts.
    let evicted = poll(Duration::from_secs(60), || async {
        Ok((!evict_was_present(&admin, hash).await?).then_some(()))
    })
    .await?;
    assert!(
        evicted.is_some(),
        "node must evict once the region transition brings H into scope"
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
    // ADR 011 § Compliance Window: the entry is not slashable until
    // `effectiveAt = addedAt + complianceWindow`, so step past it before
    // stamping evidence. Without this the challenge reverts with
    // `BlacklistAfterResponse` — which is the correct behaviour and exactly what
    // #1169 added, so the drive has to respect it rather than route around it.
    // The window is read from the contract, not hardcoded, so a governance
    // change to the default can't turn this into a no-op.
    let window = chain.compliance_window().await?;
    chain.advance_time(window + 60).await?;

    // Evidence timestamp: current chain time (µs), now strictly after the
    // entry's `effectiveAt` and fresh enough to pass the staleness check.
    let response_ts_us = chain.head_timestamp().await? * 1_000_000;

    // The signed `slash_sig` — the production probe signer produces exactly the
    // EIP-712 digest `SlashJudge` verifies.
    let probe = ProbeSlashData {
        hash: hash_key,
        has_blob: true,
        rate_per_mb: RATE_PER_MB,
        timestamp_us: response_ts_us,
    };
    let domain = slash_judge_domain(chain.chain_id(), chain.addrs().slash_judge);
    let slash_sig = probe.sign(node.operator(), &domain)?.as_bytes().to_vec();

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
    let judge = SlashJudge::new(chain.addrs().slash_judge, chain.admin());
    let bond = judge
        .challengeBond()
        .call()
        .await
        .context("read challengeBond")?;
    chain.transfer_token(challenger_addr, bond).await?;

    let cp = chain.provider_for(&challenger);
    let approve = Erc20::new(chain.addrs().token, &cp)
        .approve(chain.addrs().slash_judge, bond)
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
    let judge_c = SlashJudge::new(chain.addrs().slash_judge, &cp);
    let commit = judge_c
        .commitChallenge(commitment)
        .send()
        .await
        .context("commitChallenge send")?
        .get_receipt()
        .await
        .context("commitChallenge receipt")?;
    assert!(commit.status(), "commitChallenge reverted");
    time::increase_time(chain.admin(), 61).await?;

    let response_data = SlashJudge::ProbeMsg {
        hash: hash_key,
        hasBlob: true,
        ratePerMb: RATE_PER_MB,
        timestampUs: response_ts_us,
    }
    .abi_encode();
    let node_id = B256::from_slice(node.node_id().as_bytes());
    let receipt = SlashJudgeBlacklist::new(chain.addrs().slash_judge, &cp)
        .submitBlacklistChallenge(
            node.operator_addr(),
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
