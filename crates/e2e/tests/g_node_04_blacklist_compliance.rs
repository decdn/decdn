//! G-NODE-04 — blacklist compliance loop (issue #1031).
//!
//! Drives the full cross-layer loop on a real anvil deployment + in-process
//! `decdn-node` daemon:
//!
//! - **Global compliance (`global_blacklist_compliance`):** a node holds and
//!   serves blob `H`; governance adds `H` to `ContentBlacklist`; the node's
//!   blacklist watcher evicts `H` (asserted via admin `evict` dry-run flipping
//!   `was_present` true→false), then refuses paid delivery `Declined` *for the
//!   blacklist reason* (its `chain_hash_denied` counter), and refuses a
//!   voucher-less discovery open. Eviction is durable across a daemon restart.
//!   Finally, a signed post-entry `StreamResponse { ok: true }` for `H` is
//!   driven through the `SlashJudge` commit-reveal to prove
//!   serving-after-blacklist is slashable.
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
//! - **Removal reversal (`removal_lifts_the_deny_but_eviction_is_sticky`):** the
//!   `removeHashGlobal` slow path (an appeal / wrongful-entry reversal) lifts the
//!   node's governance deny, so the refusal *reason* moves from `chain_hash_denied`
//!   to the sticky eviction — but the wire code stays `Declined`, the blob stays
//!   evicted, and a discovery open is still refused. Content un-eviction is not a
//!   watcher action (eviction is durable and one-way, `evicted.log`); appeal
//!   restitution is financial, through `SlashAppeal` (ADR 028).
//! - **Open-stream interplay (`takedown_evicts_a_blob_pinned_by_an_open_stream`):**
//!   a discovery open pins `H` for the life of its stream (the node answers
//!   `ok`), then governance blacklists `H`; the watcher evicts it anyway. The
//!   pin is LRU-exemption only — a governance takedown always wins (ADR 040
//!   §Pinning).
//!
//! **Coverage.** The journey exercises all three serving seams end-to-end
//! against the daemon — delivery refusal (`Declined`, pinned to the governance
//! deny by the node's per-reason counter), the
//! discovery open (a signed refusal), and on-chain slashability. DHT
//! announce-suppression stays delegated to `crates/node/src/dht/publish.rs`
//! unit tests (its `refuses` gate). `SlashJudge` enforces slashability from
//! an entry's `effectiveAt` (`addedAt + complianceWindow`, ADR 011 § Compliance
//! Window, #1169), so the slash drive advances chain time past that window
//! before stamping evidence — the node's *reaction* budget is now an on-chain
//! grace, not just a prose target.

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

use alloy::primitives::{B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_common::admin::{AdminRpcClient, EvictRequest};
use decdn_e2e::chain::{ChainFixture, region_key};
use decdn_e2e::client::{ClientFixture, PoolSession};
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;
use decdn_e2e::time;
use jsonrpsee::http_client::HttpClient;

const MIB: usize = 1024 * 1024;

/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule). The
/// longest internal poll ladder among the journeys that take it is 60+60s (the
/// regional leg), which stays under the ~150s threshold for the heavy tier.
/// Cleanup runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;
/// `removal_lifts_the_deny_but_eviction_is_sticky` needs the heavy tier: its
/// internal ladder is `60s` evict + `30s` blacklist + `180s` removal convergence
/// (`60+30+180=270s`), which exceeds the `~150s` heavy threshold and would crowd
/// the `2×120s=240s` deploy ladder inside `STANDARD 300s` under
/// `--test-threads 2` contention.
const REMOVAL_OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::HEAVY;

/// The node's refusal counter for a governance hash deny.
const CHAIN_HASH_DENIED: &str = "decdn_serve_stream_rejected_chain_hash_denied_total";
/// The node's refusal counter for a blob it no longer holds after an open.
const WITHDRAWN: &str = "decdn_serve_stream_rejected_withdrawn_total";

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

#[tokio::test(flavor = "multi_thread")]
async fn removal_lifts_the_deny_but_eviction_is_sticky() -> anyhow::Result<()> {
    tokio::time::timeout(REMOVAL_OVERALL_TIMEOUT, Box::pin(run_removal_reversal()))
        .await
        .context("removal-reversal e2e exceeded the overall timeout")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn takedown_evicts_a_blob_pinned_by_an_open_stream() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_open_stream_interplay()))
        .await
        .context("open-stream interplay e2e exceeded the overall timeout")??;
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
    let outcome = client
        .fetch(&chain, &node, hash, alloy::primitives::U256::ZERO)
        .await?;
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
    // specifically (not some unrelated pool/connect/payment failure).
    assert_refused_as_blacklisted(&chain, &node, hash).await?;

    // ...and a discovery open is refused too (the blacklist-violation
    // compliance seam — an open that sends no voucher, distinct from the
    // delivery path above).
    assert_open_refused(&chain, &node, hash, "an evicted blacklisted blob").await?;

    // Eviction is durable across a daemon restart — and so is its *cause*. The
    // refusal assertion below is the cross-layer proof of the governance deny
    // projection: `evicted.log` alone would bring the node back up refusing for
    // the plain eviction, with the governance deny lost.
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

/// Assert a paid fetch of `hash` from `node` is refused `Declined` *for the
/// blacklist reason*. The node's `chain_hash_denied` counter pins the cause, so
/// a pool/connect/payment regression that also fails the fetch cannot green
/// this check.
async fn assert_refused_as_blacklisted(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: Hash,
) -> anyhow::Result<()> {
    let before = node.scrape_metric(CHAIN_HASH_DENIED).await?;
    let err = ClientFixture::new(chain)
        .await?
        .fetch(chain, node, hash, alloy::primitives::U256::ZERO)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("fetch of an evicted blacklisted blob must fail"))?;
    let msg = format!("{err:#}");
    // A governance takedown and this operator's own `[content] denied_hashes`
    // share the one wire code `Declined` (ADR 011 §`StreamRequest` Response), so
    // a client cannot fingerprint an operator's private legal exposure. The
    // counter proves the watcher's governance deny reaches the serve path on a
    // real deployment, not only the eviction.
    anyhow::ensure!(
        msg.contains("Declined"),
        "refusal must be Declined, got: {msg}"
    );
    let after = node.scrape_metric(CHAIN_HASH_DENIED).await?;
    anyhow::ensure!(
        after > before,
        "the refusal must count on {CHAIN_HASH_DENIED} ({before} -> {after}); got: {msg}"
    );
    Ok(())
}

/// Poll a reused pool session until a single-attempt fetch of `hash` is refused
/// `Declined` and the attempt bumps the node's `metric` refusal counter, or
/// `budget` elapses. Returns the last refusal message on convergence, `None`
/// otherwise. `each_round` runs before every attempt.
///
/// Uses [`ClientFixture::fetch_once`], not [`ClientFixture::fetch`]: `fetch`
/// opens a fresh on-chain pool each call, so polling it would burn the budget
/// before a reason transition could ever be observed. `fetch_once` is one attempt
/// on the existing session — an open-time refusal advances no watermark, so the
/// session stays reusable across attempts.
async fn poll_refusal_reason<F, Fut>(
    client: &ClientFixture,
    session: &mut PoolSession,
    node: &NodeFixture,
    hash: Hash,
    metric: &str,
    budget: Duration,
    mut each_round: F,
) -> anyhow::Result<Result<String, String>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        each_round().await?;
        let before = node.scrape_metric(metric).await?;
        let msg = match client
            .fetch_once(session, hash, 0, alloy::primitives::U256::ZERO)
            .await
        {
            Ok(_) => anyhow::bail!("fetch of a refused blob unexpectedly succeeded"),
            Err(e) => format!("{e:#}"),
        };
        anyhow::ensure!(
            msg.contains("Declined"),
            "refusal must be Declined, got: {msg}"
        );
        if node.scrape_metric(metric).await? > before {
            return Ok(Ok(msg));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(Err(format!(
                "{msg}; refusal counters: {}",
                declined_counters(node).await?
            )));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The node's per-reason counters behind a `Declined` refusal, for a failure
/// message that names the reason the node actually refused for.
async fn declined_counters(node: &NodeFixture) -> anyhow::Result<String> {
    let mut out = Vec::new();
    for reason in [
        "chain_hash_denied",
        "hash_denied",
        "withdrawn",
        "foreign_declined",
        "internal_error",
        "range_not_satisfiable",
        "blob_too_large",
        "origin_denied",
    ] {
        let name = format!("decdn_serve_stream_rejected_{reason}_total");
        out.push(format!("{reason}={}", node.scrape_metric(&name).await?));
    }
    Ok(out.join(" "))
}

/// The `removeHashGlobal` reversal (an appeal / wrongful-entry override) lifts the
/// governance deny but not the eviction: the refusal stays `Declined` but its
/// reason moves to the sticky eviction, the blob stays evicted, and the
/// node still refuses a discovery open. Content un-eviction is never a watcher
/// action — `evicted.log` is durable and one-way, and appeal restitution is
/// financial (`SlashAppeal`, ADR 028), not a re-serve.
async fn run_removal_reversal() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0x8Bu8; 2 * MIB]; // H — blacklisted, then removed.
    // W — never blacklisted; keeps the pool session live so every H fetch below is
    // a cheap `fetch_once` reusing one pool rather than opening a fresh one.
    let warmup = vec![0x8Cu8; MIB];
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &[&payload, &warmup]).await?;
    let hash = hashes[0];
    let warmup_hash = hashes[1];
    let admin = node.admin_client()?;
    let hash_key = to_b256(hash);

    // Baseline: the node holds H.
    assert!(
        evict_was_present(&admin, hash).await?,
        "node must hold H before blacklisting"
    );

    // One session, proven live by the warm-up fetch of the never-blacklisted W,
    // reused for every H fetch below.
    let client = ClientFixture::new(&chain).await?;
    let (mut session, _warm) = client.open_session(&chain, &node, warmup_hash).await?;

    // Blacklist H globally and let the watcher evict it.
    chain.add_hash_global(hash_key).await?;
    time::increase_time(chain.admin(), 3600).await?;
    let evicted = poll(Duration::from_secs(60), || async {
        Ok((!evict_was_present(&admin, hash).await?).then_some(()))
    })
    .await?;
    assert!(
        evicted.is_some(),
        "node never evicted the blacklisted blob H"
    );

    // While the entry stands, delivery is refused for the governance deny.
    let denied = poll_refusal_reason(
        &client,
        &mut session,
        &node,
        hash,
        CHAIN_HASH_DENIED,
        Duration::from_secs(30),
        || async { Ok(()) },
    )
    .await?;
    if let Err(last) = denied {
        panic!("delivery must be refused for the governance deny while the entry stands: {last}");
    }

    // Governance removes the entry (the appeal / wrongful-entry slow path). The
    // watcher lifts the governance deny on the `HashRemoved` tail event, moving
    // the refusal reason to the sticky eviction (`undeny_hash`).
    chain.remove_hash_global(hash_key).await?;

    // Mine across the poll. The log poller only sees the `HashRemoved` event once
    // the chain head has advanced past its block (the add→evict path got that for
    // free from its `increase_time`; a lone removal tx does not), and mining each
    // round also keeps the watcher's periodic re-scope ticking. `180s` covers the
    // `10s` `isHashBlacklistedForOperator` RPC timeout plus `eth_getLogs` poll
    // jitter under `--test-threads 2` contention (#1826).
    let reverted = poll_refusal_reason(
        &client,
        &mut session,
        &node,
        hash,
        WITHDRAWN,
        Duration::from_secs(180),
        || time::mine(chain.admin()),
    )
    .await?;
    if let Err(last) = reverted {
        panic!(
            "removal must lift the governance deny — the refusal must move to the \
             sticky eviction once the entry is gone; last refusal was: {last}"
        );
    }

    // The eviction itself is sticky and one-way: the blob is still gone and a
    // discovery open still declines it, even though the hash is no longer
    // blacklisted.
    assert!(
        !evict_was_present(&admin, hash).await?,
        "removal must NOT un-evict the blob (eviction is durable, one-way)"
    );
    assert_open_refused(
        &chain,
        &node,
        hash,
        "an evicted blob after the blacklist entry is removed",
    )
    .await?;

    Ok(())
}

/// An open stream's pin does not shield a blob from a governance takedown. A
/// discovery open makes the node answer `ok` and pin H for the life of the
/// stream; governance then blacklists H while the stream is still open, and the
/// watcher evicts it anyway — the pin is LRU-exemption only, and a takedown
/// always wins (ADR 040 §Pinning).
async fn run_open_stream_interplay() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0x9Du8; 2 * MIB];
    let (node, hash) = NodeFixture::launch(&chain, "US", &payload).await?;
    let admin = node.admin_client()?;
    let hash_key = to_b256(hash);

    // The node holds H and answers it: this open signs `ok: true` and pins H
    // (eviction-exempt from LRU) until the stream ends.
    let client = ClientFixture::new(&chain).await?;
    let held = client.discovery_open(&chain, &node, hash).await?;
    assert!(
        held.served(),
        "node must answer H (and so pin it for the open stream) before the takedown"
    );

    // Blacklist H while the stream is open. The watcher evicts within a couple
    // of its 2s poll cycles, so the eviction is observed against a pin the
    // takedown had to override.
    chain.add_hash_global(hash_key).await?;
    time::increase_time(chain.admin(), 3600).await?;
    let evicted = poll(Duration::from_secs(60), || async {
        Ok((!evict_was_present(&admin, hash).await?).then_some(()))
    })
    .await?;
    assert!(
        evicted.is_some(),
        "a governance takedown must evict a blob an open stream pins (the pin does not \
         shield it)"
    );
    held.cancel();

    // The open seam agrees: a fresh open of the evicted blob is refused, so no
    // new stream can pin it back into service.
    assert_open_refused(
        &chain,
        &node,
        hash,
        "a taken-down blob an open stream pinned",
    )
    .await?;
    assert_refused_as_blacklisted(&chain, &node, hash).await?;

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
        .fetch(&chain, &de_node, hash, alloy::primitives::U256::ZERO)
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

    // The operator registered as DE, which stamps `regionLastChanged`; the
    // `updateRegion` cooldown (= REGION_STABILITY_WINDOW) runs from that stamp,
    // so step past it before the region change or the tx reverts
    // `RegionCooldownActive`.
    let window = chain.region_stability_window().await?;
    chain.advance_time(window + 60).await?;

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
/// A voucher-less discovery open of `hash` on `node` answers a signed refusal:
/// the node verifies, signs `ok: false`, and pins nothing. `what` names the blob
/// for the failure message.
async fn assert_open_refused(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: Hash,
    what: &str,
) -> anyhow::Result<()> {
    let opened = ClientFixture::new(chain)
        .await?
        .discovery_open(chain, node, hash)
        .await?;
    let served = opened.served();
    opened.cancel();
    anyhow::ensure!(!served, "the daemon must refuse an open of {what}");
    Ok(())
}

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

/// Drive a signed post-entry `StreamResponse { ok: true }` for `hash` through
/// `SlashJudge.submitBlacklistChallenge` (commit-reveal + challenge bond),
/// asserting the slash lands. Proves serving a blacklisted hash is slashable.
///
/// ADR 011 § Compliance Window: the entry is not slashable until
/// `effectiveAt = addedAt + complianceWindow`, so the fixture steps chain time
/// past it before stamping evidence. Without that the challenge reverts with
/// `BlacklistAfterResponse` — the correct behaviour (#1169), so the drive
/// respects it rather than routing around it.
async fn drive_blacklist_slash(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: Hash,
) -> anyhow::Result<()> {
    let challenger = PrivateKeySigner::random();
    let node_id = B256::from_slice(node.node_id().as_bytes());
    let slash_id = chain
        .slash_operator_via_judge(&challenger, node.operator(), node_id, to_b256(hash))
        .await
        .context("serving a blacklisted hash must be slashable")?;
    let (operator, _slashed_at, amount) = chain.slash_record(slash_id).await?;
    anyhow::ensure!(
        operator == node.operator_addr(),
        "the slash record must name the serving operator, got {operator}"
    );
    anyhow::ensure!(amount > U256::ZERO, "the slash must take a non-zero amount");
    Ok(())
}

fn to_b256(hash: Hash) -> B256 {
    B256::from(*hash.as_bytes())
}
