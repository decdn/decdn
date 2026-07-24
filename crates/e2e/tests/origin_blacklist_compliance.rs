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

/// Overall ceiling so an unbounded await fails fast with a clear message.
/// Cleanup (anvil kill, daemon kill) runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(600);

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
