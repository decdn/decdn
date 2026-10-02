//! Cross-layer proof that the deployment preflight aborts bring-up **before the
//! seller lane store is touched**.
//!
//! The lane store binds to the configured `PaymentPool` deployment when it
//! opens. On a stamp mismatch it drops the seller lane state, the pending
//! settles and the watcher checkpoints, and the unredeemed vouchers in them are
//! lost. A typo'd `blockchain.payment_pool_address` must therefore fail the
//! preflight and stop the daemon while the store is still bound to the real
//! deployment. Unit tests cover each preflight verdict against a mocked
//! provider. Only a real daemon shows the call order: a preflight that ran
//! after the store open would still exit nonzero, but only after the drop.
//!
//! The journey seeds a seller lane, then repoints the node at a codeless
//! address, at every other contract the deploy manifest lists, and at the
//! settlement USDC. Each boot exits nonzero with the matching verdict, and no
//! boot logs a lane drop. The sweep
//! also pins that the identity probe tells `PaymentPool` apart from every
//! sibling, including those that share one of its views: the `FeeRouter`
//! answers `usdc()` and the `DecdnGovernor` answers `feeRouter()`. With the
//! config reverted, the node boots on the original deployment and the same
//! pool session pays for and redeems another blob.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a built `decdn-node` binary:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e --test deployment_preflight_guard
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

use alloy::primitives::{Address, U256};
use anyhow::Context;
use decdn_e2e::assert as e2e_assert;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::{FOREIGN_LANE_DROPPED, FOREIGN_LANES_DROPPED, NodeFixture};
use decdn_e2e::poll;

const MIB: usize = 1024 * 1024;

/// Overall ceiling: the standard journey tier. Cleanup (anvil kill, daemon kill)
/// runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// How long a boot with a bad deployment gets to exit. The preflight verdict is
/// terminal, so the expected exit is near-immediate. The ceiling sits above the
/// daemon's preflight retry budget (`DEPLOYMENT_PREFLIGHT_BUDGET`, one minute),
/// so a verdict that is retried by mistake still exits here, and then fails the
/// "not retried" needle.
const PREFLIGHT_EXIT: Duration = Duration::from_secs(90);

/// How long the node gets to redeem a voucher on chain.
const REDEEM: Duration = Duration::from_secs(90);

#[tokio::test(flavor = "multi_thread")]
async fn deployment_preflight_aborts_before_the_lane_store_opens() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("deployment-preflight e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payment_pool = chain.addrs().payment_pool;

    // Two held blobs, each large enough (2 MiB @ 10 µUSDC/MiB → ~20 µUSDC) that
    // one delivery clears the node's 10 µUSDC redeem threshold. Blob A seeds the
    // lane before the bad boots; blob B proves the same lane after them.
    let payload_a = vec![0xA5u8; 2 * MIB];
    let payload_b = vec![0x5Au8; 2 * MIB];
    let (node, hashes) =
        NodeFixture::launch_with_blobs(&chain, "US", &[payload_a.as_slice(), payload_b.as_slice()])
            .await?;
    let hash_a = hashes.first().copied().context("no hash for blob A")?;
    let hash_b = hashes.get(1).copied().context("no hash for blob B")?;

    let client = ClientFixture::new(&chain).await?;
    let signer = client.address();
    let provider = node.operator_addr();

    // ---- Seed a seller lane bound to the snapshot deployment, and wait until
    // the node has redeemed it on chain, so the lane store holds a lane with a
    // nonzero watermark.
    let (mut session, warm) = client.open_session(&chain, &node, hash_a).await?;
    anyhow::ensure!(
        warm == payload_a,
        "warm-up delivery must be blob A byte-exact"
    );
    let pool_id = session.pool_id();
    let paid_before = poll(REDEEM, || async {
        let paid =
            e2e_assert::read_watermark(chain.admin(), payment_pool, pool_id, signer, provider)
                .await?
                .amount;
        Ok((paid > 0).then_some(paid))
    })
    .await?
    .context("the seeded lane was never redeemed on chain")?;

    // ---- A codeless address: the preflight finds no code and aborts. The
    // address is nonzero, so it passes config validation and reaches the
    // preflight.
    node.set_payment_pool_address(Address::repeat_byte(0xC0))?;
    let status = node
        .respawn_expecting_exit(PREFLIGHT_EXIT)
        .await
        .context("boot against a codeless PaymentPool address")?;
    anyhow::ensure!(
        !status.success(),
        "a boot against a codeless PaymentPool address must exit nonzero (got {status})"
    );
    node.log_line(&["has no code on chain"])
        .context("the codeless boot never logged the no-code verdict")?;

    // ---- Every other contract in the deploy manifest, read from the manifest
    // itself so a new contract joins the sweep without an edit here, plus the
    // settlement USDC. Each has code but does not answer
    // `PaymentPool.getRateBounds()`, so the preflight aborts on the identity
    // probe without retrying it. A zero entry is a contract the deploy left
    // undeployed (the dormant `BuybackBurner`): there is no code to probe, and
    // config validation rejects a zero address before the preflight runs.
    let mut siblings: Vec<(String, Address)> = chain
        .manifest_contracts()
        .iter()
        .filter(|(_, addr)| *addr != payment_pool && !addr.is_zero())
        .cloned()
        .collect();
    siblings.push(("USDC".to_string(), chain.usdc()));
    // The siblings that share a `PaymentPool` view are the cases a weaker probe
    // lets through; a renamed manifest key must not drop them silently.
    for must_cover in ["FeeRouter", "DecdnGovernor"] {
        anyhow::ensure!(
            siblings.iter().any(|(name, _)| name == must_cover),
            "the deploy manifest lists no {must_cover}; the sweep would not cover it: \
             {siblings:?}"
        );
    }
    for (name, sibling) in siblings {
        node.set_payment_pool_address(sibling)?;
        let status = node
            .respawn_expecting_exit(PREFLIGHT_EXIT)
            .await
            .with_context(|| format!("boot against the {name} as a PaymentPool"))?;
        anyhow::ensure!(
            !status.success(),
            "a boot against the {name} as a PaymentPool must exit nonzero (got {status})"
        );
        // The address in the needle ties the verdict to this boot.
        let verdict = format!("{sibling} does not answer PaymentPool.getRateBounds()");
        node.log_line(&[&verdict, "not retried"]).with_context(|| {
            format!("the {name} boot never logged the unretried getRateBounds() verdict")
        })?;
    }

    // ---- No bad boot opened the lane store, so none dropped a lane.
    assert_no_lane_dropped(&node, "a boot that failed the deployment preflight")?;

    // ---- Revert the config. The node boots on the original deployment, and the
    // same pool session pays for blob B and redeems past its earlier
    // watermark. A dropped lane would pass this too, because the buyer's
    // cumulative voucher re-creates it: the drop WARNs' absence is the proof
    // that the lane survived, and this is the proof the node still serves.
    node.set_payment_pool_address(payment_pool)?;
    node.restart()
        .await
        .context("restart on the original PaymentPool")?;
    let bytes_b = client
        .fetch_once(&mut session, hash_b, 0, U256::ZERO)
        .await
        .context("paid fetch on the original lane after the reverted config")?;
    anyhow::ensure!(
        bytes_b == payload_b,
        "the delivery after the revert must be blob B byte-exact"
    );
    poll(REDEEM, || async {
        let paid =
            e2e_assert::read_watermark(chain.admin(), payment_pool, pool_id, signer, provider)
                .await?
                .amount;
        Ok((paid > paid_before).then_some(paid))
    })
    .await?
    .context("the original lane never redeemed past its watermark after the revert")?;

    assert_no_lane_dropped(&node, "a boot of this node")
}

/// Fail if any spawn of `node` logged either seller-lane drop WARN.
/// `node_pull_pool_redeploy.rs` asserts that a real redeploy logs both, so
/// their absence here is a real signal.
fn assert_no_lane_dropped(node: &NodeFixture, who: &str) -> anyhow::Result<()> {
    for needle in [FOREIGN_LANES_DROPPED, FOREIGN_LANE_DROPPED] {
        if let Some(line) = node.log_line(&[needle]) {
            anyhow::bail!("{who} dropped the seller lane state: {line}");
        }
    }
    Ok(())
}
