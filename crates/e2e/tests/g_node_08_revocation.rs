//! Authorized-origin gate — the *closing* direction (#1373).
//!
//! G-NODE-08 (`g_node_08_authorized_origin.rs`) covers the gate opening: before
//! ratification a backend-only hash refuses, after ratification the same node
//! fills and serves it. This journey covers the dual — the gate closing again on
//! `revokeAssignment` — and the invariant that closing must NOT retroactively
//! pull already-held content.
//!
//! Two behaviors are pinned, in value order:
//!
//! 1. **The gate re-closes on revoke.** After `revokeAssignment(namespace,
//!    operator)`, a *fresh* backend-only hash in that namespace goes back to
//!    refusing, and the refusal is the authorized-origin gate (pinned by the
//!    `unauthorized_origin` reject counter), not some incidental miss.
//! 2. **Already-held content survives revocation.** ADR 037 § Seed-leech caps is explicit that
//!    the gate covers pull *initiation* only, never a range already in the store —
//!    refusing already-held content is `ContentBlacklist`'s job. So a blob served
//!    (and thereby cached) before the revoke must still serve after it, even with
//!    the gate closed. This is the invariant most likely to regress if someone
//!    later "tightens" the gate, so it is asserted with the gate provably shut.
//!
//! Scope, precisely as in G-NODE-08: `pull_origin_gate_blocks` is namespace-scoped
//! (does this namespace have any active authorized origin?), not per-operator; see
//! #1368. This journey holds under either reading.
//!
//! **Not covered here — staker deactivation (#1373 leg 3).** A revocation event is
//! not the only way the gate re-closes: if the assigned operator unbonds below
//! `minBond`, `active_node_for` drops it with no `OriginAssignment` event at all.
//! That path goes through the `ChainStakerSet` projection (not
//! `ChainOriginDirectory`) and the full `decdn node unbond` window
//! (`g_node_06_unbond.rs`), so it is tracked as a remaining follow-up rather than
//! folded in here.

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

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_client_pull::UpstreamRefused;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::{ChannelSession, ClientFixture};
use decdn_e2e::node::NodeFixture;
use decdn_protocol::client::StreamError;

const KIB: usize = 1024;
const CACHED_LEN: usize = 96 * KIB;
const HELD_LEN: usize = 160 * KIB;
const FRESH_LEN: usize = 48 * KIB;
const OVERALL_TIMEOUT: Duration = Duration::from_secs(900);
/// Budget for the node's `ChainOriginDirectory` watcher to observe
/// `AssignmentRevoked` (event poll cadence is 500ms in the fixture config).
const RECLOSE_TIMEOUT: Duration = Duration::from_secs(60);
/// The per-reason reject counter the gate bumps *before* the wire write, which
/// discriminates a gated refusal from the plain `NotFound` seven reasons collapse
/// onto (#1371). Same constant as the G-NODE-08 journey.
const UNAUTHORIZED_ORIGIN_METRIC: &str = "decdn_serve_stream_rejected_unauthorized_origin_total";

#[tokio::test(flavor = "multi_thread")]
async fn revoking_an_assignment_recloses_the_gate_without_dropping_held_content()
-> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("G-NODE-08 revocation exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    let cached_payload = payload(0xC1, CACHED_LEN);
    let held_payload = payload(0x93, HELD_LEN);

    // The node under test: opaque `fs` backend + the authorized-origin gate, no
    // discovery peers (so a served backend-only blob can only be its own origin).
    let (node, cached) =
        NodeFixture::launch_authorized_origin(&chain, "US", &[&cached_payload]).await?;
    let _cached_hash = *cached.first().context("launch returned no cached hash")?;

    // `held_hash` starts ONLY in the backend — never warmed — so serving it later
    // requires the gate to be open, and after we cache it, keeping it served
    // requires the gate to be *irrelevant* to already-held content.
    let held_hash = node.seed_origin_blob(&held_payload)?;

    let client = ClientFixture::new(&chain).await?;

    // -------------------------------------------------------------- ratification
    let publisher = PrivateKeySigner::random();
    let namespace = chain.create_namespace(&publisher).await?;
    chain
        .propose_assignment(&publisher, namespace, &[node.operator_addr()])
        .await?;
    chain.activate_assignment_after_timelock(namespace).await?;
    assert_eq!(
        chain.origins(namespace).await?,
        vec![node.operator_addr()],
        "the DAO-ratified assignment must list this operator as the namespace's origin"
    );

    // ------------------------------------------------------- serve + cache held
    // `fetch` retries: the directory watcher has to observe `AssignmentActivated`
    // before the gate opens, and there is no readiness signal for that.
    let served = client.fetch(&chain, &node, held_hash, namespace).await?;
    assert_eq!(
        served.bytes, held_payload,
        "a recognized origin must serve the backend's bytes verbatim"
    );
    assert!(
        client.probe(&node, held_hash).await?.body.has_blob,
        "the reactive backend fill must land the held blob in the local store"
    );

    // A warmed session whose channel the node's serve path already recognizes, so
    // the single-shot fetches below don't race channel registration.
    let (mut session, _) = client.open_session(&chain, &node, held_hash).await?;

    // -------------------------------------------------------------------- revoke
    // Immediate (no timelock), emits `AssignmentRevoked`.
    chain
        .revoke_assignment(&publisher, namespace, node.operator_addr())
        .await?;
    assert!(
        chain.origins(namespace).await?.is_empty(),
        "revokeAssignment must clear the namespace's active origin set on-chain"
    );
    assert!(
        !chain
            .is_authorized_origin(namespace, node.operator_addr())
            .await?,
        "the operator must no longer be an authorized origin after revoke"
    );

    // ------------------------------------------------- 1. the gate re-closes
    // A *fresh* backend-only hash in the namespace is refused again, once the node
    // has observed `AssignmentRevoked`. A distinct hash per attempt is essential:
    // a fetch that lands while the watcher is still catching up would SERVE and
    // cache that hash, and a reused hash would then serve from cache forever,
    // masking the re-close. See `wait_for_gate_to_reclose`.
    wait_for_gate_to_reclose(&client, &node, &mut session, namespace, RECLOSE_TIMEOUT).await?;

    // ------------------------------------------ 2. already-held content survives
    // With the gate provably shut, the blob cached before the revoke STILL serves:
    // the gate is a pull-*initiation* check, and a held range is served from the
    // availability arm above it (ADR 037 § Seed-leech caps). This is the dual of G-NODE-08's
    // cached-blob control and the invariant most likely to regress on a "tighten
    // the gate" change.
    let after_revoke = client
        .fetch_once(&mut session, held_hash, 0, namespace)
        .await
        .context("already-held content must still serve after revoke")?;
    assert_eq!(
        after_revoke, held_payload,
        "revocation must not drop or corrupt content the node already holds"
    );

    Ok(())
}

/// Poll fresh backend-only hashes until one is refused by the authorized-origin
/// gate, proving the node has observed `AssignmentRevoked` and re-closed. Each
/// attempt uses a **distinct** hash: an attempt that lands before the watcher
/// catches up serves and caches that hash, so reusing one would let the cache
/// mask the re-close. The refusal is pinned to the gate (not an incidental miss)
/// by the `unauthorized_origin` counter stepping across it.
///
/// Each *served* attempt is a real paid delivery on the shared `session`
/// channel. In practice the watcher (500ms cadence) catches up within a few
/// attempts, so the deposit exposure is small; even if it didn't, an exhausted
/// channel would refuse with `InsufficientDeposit` — a *different* reject
/// counter — so the `unauthorized_origin` delta below would fail rather than
/// pass falsely. The failure mode is a confusing message, never a false green.
async fn wait_for_gate_to_reclose(
    client: &ClientFixture,
    node: &NodeFixture,
    session: &mut ChannelSession,
    namespace: U256,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempt: usize = 0;
    loop {
        // Distinct backend-only payload per attempt (seed varies the bytes, len
        // varies the hash further), each a true pull-initiation probe.
        let seed = 0xD0u8.wrapping_add(u8::try_from(attempt % 200).unwrap_or(0));
        let fresh_payload = payload(seed, FRESH_LEN + attempt);
        let fresh_hash = node.seed_origin_blob(&fresh_payload)?;

        let before = node.scrape_metric(UNAUTHORIZED_ORIGIN_METRIC).await?;
        match client.fetch_once(session, fresh_hash, 0, namespace).await {
            // Gate not yet re-closed: this hash was served (and cached). Discard
            // it and try a fresh one.
            Ok(_served) => {}
            Err(e) => {
                let code = e
                    .downcast_ref::<UpstreamRefused>()
                    .map(|r| r.error.clone())
                    .with_context(|| {
                        format!("re-close probe failed for a non-refusal reason: {e:#}")
                    })?;
                anyhow::ensure!(
                    code == StreamError::NotFound,
                    "re-close refusal code must be NotFound, got {code:?}"
                );
                let after = node.scrape_metric(UNAUTHORIZED_ORIGIN_METRIC).await?;
                anyhow::ensure!(
                    after == before + 1,
                    "the re-closed gate must bump the unauthorized-origin reject counter \
                     (0→1), pinning the refusal to the gate rather than a plain miss"
                );
                return Ok(());
            }
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the gate never re-closed within {timeout:?} after revokeAssignment"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
        attempt += 1;
    }
}

/// A deterministic payload whose every byte is >= 0x80 (mirrors the G-NODE-08
/// journey; keeps content bytes from spelling ASCII by accident, though this
/// journey does not run the wire-opacity scan).
fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            let step = u8::try_from(i % 251).expect("i % 251 fits in u8");
            0x80 | (seed ^ step).wrapping_mul(7) >> 1
        })
        .collect()
}
