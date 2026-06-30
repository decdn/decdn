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

use alloy::primitives::U256;
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::assert as e2e_assert;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
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
        .expect("e2e smoke exceeded the overall timeout")
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
    admin.health().await.expect("admin health");

    // ---- Layer 2 (chain): the operator is active (bonded + registered).
    assert!(
        e2e_assert::operator_active(&chain.admin, chain.addrs.capacity_bond, node.operator_addr)
            .await?,
        "operator must be on-chain active"
    );

    // ---- Layer 3 (delivery): the paid client path delivers the exact blob.
    let client = ClientFixture::new(&chain).await?;
    let outcome = client.fetch(&chain, &node, hash).await?;
    assert_eq!(
        outcome.bytes, payload,
        "delivered bytes must match the blob"
    );

    // ---- Cross-layer: the daemon now reports the open channel over admin RPC.
    let channels = poll(Duration::from_secs(30), || async {
        admin
            .channels()
            .await
            .ok()
            .filter(|c| !c.channels.is_empty())
    })
    .await;
    assert!(
        channels.is_some(),
        "daemon admin RPC never reported the open channel"
    );

    // ---- Cross-layer: delivery landed on-chain — the seller redeemed the
    // voucher, so FeeRouter served-bytes for the operator advanced past zero.
    let served = poll(Duration::from_secs(90), || async {
        chain
            .served_bytes(node.operator_addr)
            .await
            .ok()
            .filter(|b| *b > U256::ZERO)
    })
    .await;
    assert!(
        served.is_some(),
        "on-chain served-bytes never advanced (seller redeem did not land)"
    );

    // ---- Time control: advancing the chain clock works (used by window
    // journeys: dispute / timelock / unbond).
    time::increase_time(&chain.admin, 60).await?;

    Ok(())
}

/// Poll `f` until it yields `Some`, or `timeout` elapses.
async fn poll<T, F, Fut>(timeout: Duration, mut f: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return Some(v);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
