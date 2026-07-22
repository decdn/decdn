//! Smoke test proving the `crates/e2e` fixtures compose end-to-end: a real
//! anvil deployment, an in-process `decdn-node` daemon onboarded on-chain, and
//! the paid client path delivering a blob — asserting across all three layers
//! (delivered bytes, daemon admin RPC, on-chain state).
//!
//! Full CUJ journeys (onboarding edge cases, blacklist compliance, slash +
//! appeal, origin recognition, governance → daemon) are follow-up issues that
//! each become a test file on top of these fixtures.
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

/// Defense-in-depth overall ceiling so an unbounded await fails fast with a
/// clear message rather than squatting the runner. Cleanup (anvil kill, daemon
/// kill) runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);

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

    // ---- Cross-layer: the daemon reports *the* channel we paid through — not
    // merely that some channel exists. Match on the exact on-chain channelId,
    // then check the counterparty + deposit it reports. The poll closure
    // propagates the real admin error rather than swallowing it into a generic
    // timeout.
    let expected_cid = outcome.channel_id;
    let snapshot = poll(Duration::from_secs(30), || async {
        let c = admin.channels().await.context("admin channels")?;
        Ok(c.channels.into_iter().find(|s| {
            s.channel_id
                .parse::<B256>()
                .is_ok_and(|id| id == expected_cid)
        }))
    })
    .await?
    .with_context(|| format!("daemon admin RPC never reported the paid channel {expected_cid}"))?;
    assert_eq!(
        snapshot.counterparty,
        client.address().to_string(),
        "reported channel counterparty must be the buyer"
    );
    assert_eq!(
        snapshot.deposit_micro_usdc, DEPOSIT_MICRO_USDC,
        "reported channel deposit must match the opened deposit"
    );

    // ---- Cross-layer: delivery landed on-chain — the seller redeemed the
    // voucher(s), so FeeRouter accumulates the delivered-bytes (ADR 036). The
    // client pays a cumulative voucher at each `voucher_interval_mb` boundary
    // plus a closing voucher, so a payload spanning multiple intervals is
    // redeemed on-chain in more than one step: served-bytes climbs to the exact
    // payload total but is briefly observable at an intermediate boundary. Poll
    // until it *reaches* the expected total (not merely past zero — that races
    // the first interval's redemption), then assert exact equality to still
    // catch accounting drift that would overshoot the payload size.
    let expected_served = U256::from(payload.len());
    let served = poll(Duration::from_secs(90), || async {
        let b = chain
            .served_bytes(node.operator_addr())
            .await
            .context("read served bytes")?;
        Ok(if b >= expected_served { Some(b) } else { None })
    })
    .await?
    .context("on-chain served-bytes never reached the payload size (seller redeem did not land)")?;
    assert_eq!(
        served, expected_served,
        "on-chain served-bytes must equal the delivered payload size"
    );

    // ---- Time control: advancing the chain clock works (used by window
    // journeys: dispute / timelock / unbond).
    time::increase_time(chain.admin(), 60).await?;

    Ok(())
}
