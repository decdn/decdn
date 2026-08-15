//! Cross-layer proof of the pool redemption path: a paid delivery accrues
//! vouchers, and the node's on-chain `redeem`/`redeemMany` **registers the
//! buyer's capability signer on its first redemption** (the `getAuthorization`
//! cap goes non-zero) and then **redeems voucher-only** on every later one (the
//! lane's cumulative-paid watermark advances while the registered cap stays put).
//!
//! This is the end-to-end validation of ADR 003 §Capability delegation as the
//! settlement path actually runs it: the buyer opens a pool, self-issues an
//! owner capability delegating spend to its own key, and pays over
//! `cdn/client/v1`; the node intakes that capability, accrues a lane, and — once
//! the accrued claim crosses its redeem threshold — cashes it against the
//! `PaymentPool`, registering the signer exactly once and paying down the lane
//! against the on-chain deposit.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a built `decdn-node` binary:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e anvil_pool_redeem
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
use anyhow::Context;
use decdn_e2e::assert as e2e_assert;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;

const MIB: usize = 1024 * 1024;

/// Overall ceiling: the standard journey tier. Cleanup (anvil kill, daemon kill)
/// runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn pool_redeem_registers_signer_once_then_redeems_voucher_only() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("pool-redeem e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // Two held blobs, each large enough (2 MiB @ 10 µUSDC/MiB → ~20 µUSDC) that a
    // single delivery's accrued claim clears the node's 10 µUSDC redeem threshold
    // and triggers an on-chain redemption. The first proves registration, the
    // second proves the later redemption registers nothing new.
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
    let payment_pool = chain.addrs().payment_pool;

    // Open ONE pool and pay the first delivery on it. `open_session` returns only
    // once the node's serve path has delivered `hash_a` and taken its voucher, so
    // the lane exists and the first redemption is in flight.
    let (mut session, warm) = client.open_session(&chain, &node, hash_a).await?;
    anyhow::ensure!(
        warm == payload_a,
        "warm-up delivery must be blob A byte-exact"
    );
    let pool_id = session.pool_id();

    // ---- First redemption: registers the capability signer. The node cashes the
    // accrued voucher once it crosses the redeem threshold; the redemption carries
    // the owner-signed capability as a `CapabilityReg`, so `getAuthorization.cap`
    // flips from the unregistered zero sentinel to the granted spending cap.
    let auth_after_first = poll(Duration::from_secs(90), || async {
        let auth =
            e2e_assert::read_authorization(chain.admin(), payment_pool, pool_id, signer).await?;
        Ok((auth.cap > 0).then_some(auth))
    })
    .await?
    .context("signer was never registered on-chain (getAuthorization.cap stayed zero)")?;
    // The self-issued capability sits at the widest cap the pool's `uint64`
    // field carries: a self-owned capability delegates spend to the owner's own
    // key, so the cap bounds nothing — the pool deposit is the real spending
    // bound (redemption pays `min(desired, cap - spent, remaining)`), and
    // leaving it at the ceiling keeps a later `topUp` beyond the opening
    // deposit redeemable.
    assert_eq!(
        auth_after_first.cap,
        u64::MAX,
        "registered cap must be the self-capability's uncapped spending cap (u64::MAX)"
    );

    // The lane's cumulative-paid watermark advanced past zero — the on-chain
    // `PoolRedeemed.newPaidCumulative` the redeemer wrote.
    let paid_after_first =
        e2e_assert::read_watermark(chain.admin(), payment_pool, pool_id, signer, provider)
            .await?
            .amount;
    anyhow::ensure!(
        paid_after_first > 0,
        "lane watermark must advance after the first redemption (got {paid_after_first})"
    );

    // ---- Second delivery on the SAME pool/lane: pay another blob, so the lane's
    // cumulative claim rises and the node redeems again.
    let bytes_b = client
        .fetch_once(&mut session, hash_b, 0, U256::ZERO)
        .await
        .context("second paid fetch on the same pool")?;
    anyhow::ensure!(
        bytes_b == payload_b,
        "second delivery must be blob B byte-exact"
    );

    // ---- Later redemption is voucher-only: the lane watermark advances further,
    // but the signer is already registered, so `getAuthorization` is untouched
    // (same cap, `spent` never exceeding the cap).
    let paid_after_second = poll(Duration::from_secs(90), || async {
        let paid =
            e2e_assert::read_watermark(chain.admin(), payment_pool, pool_id, signer, provider)
                .await?
                .amount;
        Ok((paid > paid_after_first).then_some(paid))
    })
    .await?
    .context("lane watermark never advanced past the first redemption's cumulative")?;

    let auth_after_second =
        e2e_assert::read_authorization(chain.admin(), payment_pool, pool_id, signer).await?;
    assert_eq!(
        auth_after_second.cap, auth_after_first.cap,
        "the later redemption must register nothing new — the cap must be unchanged"
    );
    anyhow::ensure!(
        auth_after_second.spent >= paid_after_second,
        "the registered signer's spent-so-far must cover the lane's paid cumulative \
         (spent={}, paid={paid_after_second})",
        auth_after_second.spent
    );
    anyhow::ensure!(
        auth_after_second.spent <= auth_after_second.cap,
        "spent must never exceed the granted cap (spent={}, cap={})",
        auth_after_second.spent,
        auth_after_second.cap
    );

    Ok(())
}
