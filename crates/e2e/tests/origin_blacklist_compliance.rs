//! Origin-blacklist compliance loop (#1398 item 8).
//!
//! The ORIGIN deny-set (ADR 011 § Hash Evasion and Origin Blacklisting) is the
//! address-keyed twin of the hash deny-set exercised by G-NODE-04. Governance
//! calls `ContentBlacklist.setOriginBlacklist(origin, true)`; the node's
//! blacklist watcher projects the `OriginBlacklistUpdated` event into its
//! `ContentDenylist`, and the delivery path then refuses any paid stream whose
//! channel is funded by that address — with the `OriginBlacklisted` wire reason,
//! not a bare channel/connect failure. Un-blacklisting re-opens the gate.
//!
//! Two further legs guard the parts of that path with the WEAKEST primitives,
//! and both were previously untested:
//!
//! - **The operator list.** `addOperator` writes `isOperatorBlacklisted` and
//!   emits `OperatorBlacklisted` — it never touches `_isOriginBlacklisted` or
//!   `OriginBlacklistUpdated`. A consumer watching only the origin event misses
//!   it entirely, and it is the *primary* governance route (it also ejects from
//!   `CapacityBond`). `OriginAssignment` unions the two; so must the node.
//! - **Restart.** The origin half has no version counter to detect a missed
//!   update against, so the deny-set must survive a process restart. A
//!   regression here comes up as an EMPTY deny-set, which is indistinguishable
//!   from "nothing is blacklisted" while the node serves what it must refuse.
//!
//! This is also the ABI-drift guard for the hand-written origin declarations the
//! node watcher decodes: it is the only test that drives the on-chain write →
//! `OriginBlacklistUpdated` event → watcher-projection path end to end, so a
//! signature drift in `setOriginBlacklist` / `OriginBlacklistUpdated` surfaces
//! here rather than silently mis-decoding in production (`sol!` bindings are only
//! checked against a live chain by the `anvil-e2e` job).

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

use anyhow::Context;
use decdn_client_pull::UpstreamRefused;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;
use decdn_protocol::client::StreamError;

const MIB: usize = 1024 * 1024;

/// Heavy journey tier (see [`decdn_e2e::timeout`] for the tier rule): the legs
/// chain repeated 180s `CATCHUP_BUDGET` polls, above the ~150s threshold for the
/// standard tier. Cleanup (anvil kill, daemon kill) runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::HEAVY;

/// Budget for each catch-up poll. Generous because a *refused* `fetch` retries
/// its (transient-classified) `OriginBlacklisted` reject to its own ~45s internal
/// deadline before surfacing the error, so a single projected attempt already
/// costs that much; the watcher itself catches up in ~1s.
const CATCHUP_BUDGET: Duration = Duration::from_secs(180);

#[tokio::test(flavor = "multi_thread")]
async fn origin_blacklist_refuses_delivery_to_the_blacklisted_funder() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("origin-blacklist e2e exceeded the overall timeout")??;
    Ok(())
}

/// The operator-level list is a SEPARATE mapping and a separate event. A node
/// that projects only `OriginBlacklistUpdated` passes the test above and still
/// serves an operator governance has blacklisted.
#[tokio::test(flavor = "multi_thread")]
async fn operator_blacklist_refuses_delivery_to_the_blacklisted_funder() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_operator_leg()))
        .await
        .context("operator-blacklist e2e exceeded the overall timeout")??;
    Ok(())
}

/// The origin deny-set must survive a restart. It has no version counter to
/// detect a missed update against, so a node that loses it comes up serving
/// content it is obliged to refuse — and reports nothing wrong.
#[tokio::test(flavor = "multi_thread")]
async fn origin_deny_set_survives_a_restart() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_restart_leg()))
        .await
        .context("origin-blacklist restart e2e exceeded the overall timeout")??;
    Ok(())
}

/// Poll until a fetch by `client` is refused specifically for `expected`.
///
/// Asserts on the TYPED refusal rather than a formatted string: a
/// channel/connect/payment regression that also fails the fetch must not green
/// the check. Before the watcher catches up the fetch may still succeed — that
/// is "not yet projected", so retry.
async fn poll_until_refused(
    chain: &ChainFixture,
    node: &NodeFixture,
    client: &ClientFixture,
    hash: decdn_cache::Hash,
    expected: StreamError,
) -> anyhow::Result<()> {
    let refused = poll(CATCHUP_BUDGET, || async {
        match client
            .fetch(chain, node, hash, alloy::primitives::U256::ZERO)
            .await
        {
            Ok(_) => Ok(None),
            Err(err) => {
                let code = err
                    .downcast_ref::<UpstreamRefused>()
                    .map(|r| r.error().clone())
                    .with_context(|| format!("expected a signed refusal, got: {err:#}"))?;
                anyhow::ensure!(
                    code == expected,
                    "refusal must be {expected:?}, got: {code:?}"
                );
                Ok(Some(()))
            }
        }
    })
    .await?;
    anyhow::ensure!(
        refused.is_some(),
        "node never refused the blacklisted funder"
    );
    Ok(())
}

async fn run_operator_leg() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0x5Au8; 2 * MIB];
    let (node, hash) = NodeFixture::launch(&chain, "US", &payload).await?;

    let client = ClientFixture::new(&chain).await?;
    let funder = client.address();

    let outcome = client
        .fetch(&chain, &node, hash, alloy::primitives::U256::ZERO)
        .await?;
    assert_eq!(outcome.bytes, payload, "baseline delivery must match H");

    // `addOperator` only — the origin mapping stays clear, so a node projecting
    // just `OriginBlacklistUpdated` sees nothing at all.
    chain.set_operator_blacklist(funder, true).await?;
    assert!(
        chain.is_operator_blacklisted(funder).await?,
        "addOperator must land on-chain"
    );
    assert!(
        !chain.is_origin_blacklisted(funder).await?,
        "addOperator must NOT set the origin mapping — that separation is the point"
    );

    poll_until_refused(&chain, &node, &client, hash, StreamError::OriginBlacklisted).await?;

    // `removeOperator` re-opens the gate.
    chain.set_operator_blacklist(funder, false).await?;
    let restored = poll(CATCHUP_BUDGET, || async {
        Ok(client
            .fetch(&chain, &node, hash, alloy::primitives::U256::ZERO)
            .await
            .ok()
            .map(|o| o.bytes))
    })
    .await?;
    assert_eq!(
        restored.as_deref(),
        Some(payload.as_slice()),
        "removeOperator must restore delivery"
    );

    Ok(())
}

async fn run_restart_leg() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0x3Cu8; 2 * MIB];
    let (node, hash) = NodeFixture::launch(&chain, "US", &payload).await?;

    let client = ClientFixture::new(&chain).await?;
    let funder = client.address();

    chain.set_origin_blacklist(funder, true).await?;
    poll_until_refused(&chain, &node, &client, hash, StreamError::OriginBlacklisted).await?;

    // Restart. From the new process's view the deny-set must be re-established
    // from scratch — whether by replaying the event tail or by reading chain
    // state, the observable contract is the same: it must still refuse.
    node.restart().await?;

    poll_until_refused(&chain, &node, &client, hash, StreamError::OriginBlacklisted).await?;

    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0x7Eu8; 2 * MIB];
    let (node, hash) = NodeFixture::launch(&chain, "US", &payload).await?;

    // A single buyer reused across the whole journey: the origin deny-set is
    // keyed by the channel FUNDER, so the same signer must fund the baseline
    // (allowed) fetch and the post-blacklist (refused) one.
    let client = ClientFixture::new(&chain).await?;
    let funder = client.address();

    // Baseline: an un-blacklisted funder is served H in full.
    let outcome = client
        .fetch(&chain, &node, hash, alloy::primitives::U256::ZERO)
        .await?;
    assert_eq!(outcome.bytes, payload, "baseline delivery must match H");

    // Governance blacklists the funder address on-chain.
    chain.set_origin_blacklist(funder, true).await?;
    assert!(
        chain.is_origin_blacklisted(funder).await?,
        "setOriginBlacklist must land on-chain"
    );

    // The watcher projects `OriginBlacklistUpdated` into the delivery gate; a
    // fresh fetch from the SAME funder is then refused for the origin-blacklist
    // reason specifically (`OriginBlacklisted`), not a bare channel/connect/
    // payment failure that a regression could also produce. During the brief
    // window before the watcher catches up the fetch may still succeed — treat
    // that as "not yet projected" and retry.
    let refused = poll(CATCHUP_BUDGET, || async {
        match client
            .fetch(&chain, &node, hash, alloy::primitives::U256::ZERO)
            .await
        {
            Ok(_) => Ok(None),
            Err(err) => {
                // Assert on the TYPED refusal, not a formatted-string match: a
                // channel/connect/payment regression that also fails the fetch
                // must not green this check. `client.fetch` wraps the error in
                // `.context(..)`, but anyhow preserves `downcast_ref` to the inner
                // `UpstreamRefused` (same shape g_node_08_namespace_scope uses).
                let code = err
                    .downcast_ref::<UpstreamRefused>()
                    .map(|r| r.error().clone())
                    .with_context(|| format!("expected a signed refusal, got: {err:#}"))?;
                anyhow::ensure!(
                    code == StreamError::OriginBlacklisted,
                    "refusal must be the origin-blacklist reason, got: {code:?}"
                );
                Ok(Some(()))
            }
        }
    })
    .await?;
    assert!(
        refused.is_some(),
        "node never refused delivery to the blacklisted funder"
    );

    // Un-blacklisting restores service — the clear direction
    // (`setOriginBlacklist(funder, false)`) must also flow through the watcher
    // and re-open the gate.
    chain.set_origin_blacklist(funder, false).await?;
    let restored = poll(CATCHUP_BUDGET, || async {
        Ok(client
            .fetch(&chain, &node, hash, alloy::primitives::U256::ZERO)
            .await
            .ok()
            .map(|o| o.bytes))
    })
    .await?;
    assert_eq!(
        restored.as_deref(),
        Some(payload.as_slice()),
        "un-blacklisting the funder must restore delivery"
    );

    Ok(())
}
