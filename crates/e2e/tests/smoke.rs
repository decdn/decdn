//! Smoke test proving the `crates/e2e` fixtures compose end-to-end: a real
//! anvil deployment, an in-process `decdn-node` daemon onboarded on-chain, and
//! the paid client path delivering a blob — asserting across all three layers
//! (delivered bytes, daemon admin RPC, on-chain state).
//!
//! Each full CUJ journey is its own sibling file on top of these fixtures:
//! `g_node_04_blacklist_compliance.rs` and `origin_blacklist_compliance.rs`
//! (hash and operator deny-sets), `slash_appeal.rs` (G-NODE-05 detection +
//! appeal), `g_node_06_unbond.rs` (capacity reduction and the unbonding
//! window), `g_gov_02_rate_bounds.rs` and `g_gov_03_real_evidence.rs`
//! (governance → daemon, and daemon signatures as on-chain evidence),
//! `g_origin_01_publish.rs` (namespace lifecycle), the `cli_*` files (publish,
//! setup, fetch resume / top-up, bundle pull), and `anvil_pool_redeem.rs` (the
//! capability-registration + voucher redemption path). Feature-scoped journeys
//! sit beside them on the same fixtures — e.g. `origin_stream_while_store.rs` —
//! so the directory listing, not this paragraph, is the authority on coverage.
//!
//! The gap is G-NODE-08 — content published to a namespace becoming servable
//! *as an origin*, which needs `OriginAssignment` publisher vetting, a seated
//! origin, and a bonded operator delivering it, plus the privacy assertion that
//! no origin location reaches the wire. `ClientFixture`'s raw frame capture is
//! the tap that journey needs; nothing drives it end to end yet.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a built `decdn-node` binary:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e
//! ```

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
use anyhow::Context;
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::assert as e2e_assert;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::{ClientFixture, DEPOSIT_MICRO_USDC};
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;
use decdn_e2e::time;

const MIB: usize = 1024 * 1024;

/// Overall ceiling: the standard journey tier (see [`decdn_e2e::timeout`] for
/// the rule that picks it). Defense-in-depth so an unbounded await fails fast
/// with a clear message rather than squatting the runner; cleanup (anvil kill,
/// daemon kill) runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn smoke_compose_fixtures() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("e2e smoke exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    // Surface daemon/background-task logs on failure (nextest captures stderr).
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // ---- Chain: deploy the full protocol on a fresh anvil.
    let chain = ChainFixture::launch().await?;

    // ---- Node: onboard one operator and bring its daemon up serving a 2 MiB
    // blob. 2 MiB @ 10 µUSDC/MiB → a 20 µUSDC claim, above the node's 10 µUSDC
    // redeem threshold, so the seller redeems on-chain and served-bytes advance.
    let payload = vec![0xABu8; 2 * MIB];
    let (node, hash) = NodeFixture::launch(&chain, "US", &payload).await?;

    // ---- Layer 1 (daemon): admin RPC is healthy.
    let admin = node.admin_client()?;
    admin.health().await.context("admin health")?;

    // ---- Layer 2 (chain): the operator is active (bonded + registered).
    assert!(
        e2e_assert::operator_active(
            chain.admin(),
            chain.addrs().capacity_bond,
            node.operator_addr()
        )
        .await?,
        "operator must be on-chain active"
    );

    // ---- Layer 3 (delivery): the paid client path delivers the exact blob.
    let client = ClientFixture::new(&chain).await?;
    let outcome = client
        .fetch(&chain, &node, hash, alloy::primitives::U256::ZERO)
        .await?;
    assert_eq!(
        outcome.bytes, payload,
        "delivered bytes must match the blob"
    );

    // ---- Cross-layer: the daemon reports a lane on *the* pool we paid through —
    // not merely that some lane exists. Match on the exact on-chain poolId, then
    // check the signer (reported as the snapshot counterparty) it names. The poll
    // closure propagates the real admin error rather than swallowing it into a
    // generic timeout. The admin surface keys each snapshot by the lane's pool id.
    let expected_pid = outcome.pool_id;
    let snapshot = poll(Duration::from_secs(30), || async {
        let c = admin.lanes().await.context("admin lanes")?;
        Ok(c.lanes
            .into_iter()
            .find(|s| s.pool_id.parse::<B256>().is_ok_and(|id| id == expected_pid)))
    })
    .await?
    .with_context(|| {
        format!("daemon admin RPC never reported a lane on the paid pool {expected_pid}")
    })?;
    assert_eq!(
        snapshot.counterparty,
        client.address().to_string(),
        "reported lane signer must be the buyer"
    );

    // The pool deposit lives on-chain, not per-lane in the node's store, so the
    // admin snapshot reports 0 for it; read the pool record directly to confirm
    // the opened deposit landed.
    let pool =
        e2e_assert::read_pool(chain.admin(), chain.addrs().payment_pool, expected_pid).await?;
    assert_eq!(
        pool.deposit,
        U256::from(DEPOSIT_MICRO_USDC),
        "on-chain pool deposit must match the opened deposit"
    );

    // ---- Cross-layer: delivery landed on-chain — the seller redeemed the
    // voucher(s), so FeeRouter accumulates the paid byte count (ADR 036 makes
    // this the canonical vote-weight source). Paid bytes are WIRE bytes — content
    // plus interleaved bao proof nodes (ADR 038 §Payment metering) — so a fully
    // settled 2 MiB delivery lands `align_range(0, 0, len).wire_len()`: the
    // content total plus ~0.4% proof overhead.
    //
    // The figure observable at read time sits in `[content_bytes, wire_bytes]`.
    // The client pays a cumulative voucher at each `voucher_interval_mb` boundary
    // (each ≥ the node's `redeem_threshold_micro_usdc`, so each redeems) plus a
    // final closing voucher for the last partial group. That closing delta is the
    // ~8 KiB of trailing proof — far below the redeem threshold — so whether it
    // has settled on-chain by the time we read is a race: not yet → the content
    // total, settled → the full wire total. Both are correct transient states of
    // the *same* delivery, so asserting either exact value is the #1381 flake
    // (observed content-total locally, wire-total on CI). Bound it on both sides
    // instead — `< content` means redemption never landed; `> wire` means the
    // accounting over-reported past the paid wire bytes.
    let content_bytes = U256::from(payload.len());
    let wire_bytes = U256::from(
        decdn_bao_range::align_range(0, 0, u64::try_from(payload.len())?)
            .context("align whole-blob payload for its wire length")?
            .wire_len(),
    );
    // Poll until the full content has settled (proving redemption landed), not
    // merely past zero — that races the first interval's redemption.
    let served = poll(Duration::from_secs(90), || async {
        let b = chain
            .served_bytes(node.operator_addr())
            .await
            .context("read served bytes")?;
        Ok((b >= content_bytes).then_some(b))
    })
    .await?
    .context(
        "on-chain served-bytes never reached the content total (seller redeem did not land)",
    )?;
    assert!(
        served >= content_bytes && served <= wire_bytes,
        "on-chain served-bytes {served} must land in [{content_bytes}, {wire_bytes}] \
         (content total .. full wire total incl. bao proof) — outside means under- or over-accounting"
    );

    // ---- Time control: advancing the chain clock works (used by window
    // journeys: dispute / timelock / unbond).
    time::increase_time(chain.admin(), 60).await?;

    Ok(())
}
