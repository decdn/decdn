//! Live anvil-backed e2e for the CLI `decdn fetch` buyer auto-`topUp` path
//! (issue #1103). The mirror of `crates/node/tests/anvil_settlement_e2e.rs:937`
//! (which asserts the *node* buyer's `top_up` raises the on-chain deposit and
//! persists it) but for the CLI fetch buyer's stack: the shared
//! [`decdn_client_pull::buyer_channel::top_up`] kernel driving the same
//! persistent [`RedbBuyerChannelStore`] the CLI fetch path uses.
//!
//! Shape: deploy the protocol, onboard a provider operator (so `openChannel`'s
//! `isActive(provider)` gate passes), have a buyer open a channel and record it
//! in a redb store, advance the persisted voucher watermark so the channel's
//! remaining deposit runs low (the sustained-fetch scenario that strands a
//! small channel today), then `top_up` and assert BOTH the on-chain
//! `getChannel().deposit` and the persisted record's deposit rose by the added
//! amount.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_fetch_topup
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units,
    // One sequential on-chain journey reads more clearly unsplit.
    clippy::too_many_lines
)]

use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_client_pull::buyer_channel::{ensure_allowance, open_channel, top_up};
use decdn_e2e::chain::ChainFixture;
use decdn_incentive::buyer_channel::BuyerChannelStore;
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::voucher_domain;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);

#[tokio::test(flavor = "multi_thread")]
async fn cli_fetch_auto_topup_raises_and_persists_deposit() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli fetch top-up e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // Provider operator — must be on-chain `isActive` for `openChannel` to pass.
    let provider_signer =
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).context("build provider signer")?;
    let provider_addr = provider_signer.address();
    let node_secret = iroh::SecretKey::from_bytes(&[0x33u8; 32]);
    chain
        .onboard_operator(
            &provider_signer,
            &node_secret,
            "US",
            "/ip4/127.0.0.1/udp/1/quic-v1",
        )
        .await
        .context("onboard provider operator")?;

    // Buyer/client account: gas + enough USDC for the deposit and a full refill.
    let buyer_signer =
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x22)).context("build buyer signer")?;
    let buyer_addr = buyer_signer.address();
    chain.fund_eth(buyer_addr, 100).await?;
    chain
        .mint_usdc(
            buyer_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await?;

    let buyer_provider = chain.provider_for(&buyer_signer);
    let pc = PaymentChannel::new(chain.addrs().payment_channel, buyer_provider.clone());
    // Unlimited standing allowance: covers both the initial `openChannel`
    // deposit and the later refill `topUp` without re-approving (a `--max-approve`
    // buyer). Passing `None` selects the max-approval path.
    ensure_allowance(
        &buyer_provider,
        chain.usdc(),
        buyer_addr,
        chain.addrs().payment_channel,
        None,
    )
    .await
    .context("approve PaymentChannel")?;

    let deposit = U256::from(DEPOSIT_MICRO_USDC);
    let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_channel);

    let opened = open_channel(
        &pc,
        Arc::new(buyer_signer.clone()),
        &voucher_dom,
        chain.usdc(),
        buyer_addr,
        provider_addr,
        deposit,
        // ZERO => self-signing (the funder signs its own vouchers); this test
        // funds and signs with the same buyer key.
        Address::ZERO,
    )
    .await
    .context("open buyer channel")?;
    let channel_id = opened.state.channel_id;

    // The persistent store the CLI fetch path uses. The store enforces a
    // `0o700` data dir; `tempdir()` defaults to `0o755`, so tighten it first.
    // Unix-only (the `0o700` mode and the store's enforcement are POSIX); the
    // rest of the journey is platform-independent.
    let dir = tempfile::tempdir().context("tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod data dir 0o700")?;
    let store = RedbBuyerChannelStore::open(dir.path()).context("open redb buyer store")?;
    store.record(&opened.state).context("record channel")?;

    // Simulate a sustained series of fetches that spends the channel down to a
    // remaining deposit of 1 µUSDC — well below the CLI's low-water mark — by
    // advancing the persisted voucher watermark.
    let prior_amount = deposit - U256::from(1u64);
    let outcome = store
        .advance_progress(
            provider_addr,
            channel_id,
            U256::from(1u64),
            U256::from(1u64),
            prior_amount,
        )
        .context("advance watermark")?;
    anyhow::ensure!(
        matches!(
            outcome,
            decdn_incentive::buyer_channel::AdvanceOutcome::Advanced
        ),
        "watermark advance failed: {outcome:?}"
    );

    // The CLI's refill policy tops up by the shortfall that restores the
    // remaining deposit to the configured target (here: `deposit - remaining`,
    // remaining == 1). Drive the shared kernel with that amount.
    let remaining = deposit - prior_amount; // == 1
    let additional = deposit - remaining; // restore to a full `deposit`
    let _ = top_up(&pc, &store, provider_addr, additional)
        .await
        .context("top_up")?;

    let expected = deposit + additional;
    let onchain = pc
        .getChannel(channel_id)
        .call()
        .await
        .context("getChannel")?
        .deposit;
    anyhow::ensure!(
        onchain == expected,
        "topUp must raise the on-chain deposit to {expected}, got {onchain}"
    );
    let persisted = store
        .get_by_provider(provider_addr)
        .context("re-read persisted channel")?
        .ok_or_else(|| anyhow::anyhow!("buyer channel vanished after top_up"))?
        .deposit;
    anyhow::ensure!(
        persisted == expected,
        "top_up must persist the raised deposit ({expected} µUSDC), got {persisted}"
    );

    // The watermark must be untouched by the top-up (a reused channel resumes
    // from the same nonce/bytes/amount, only with more headroom).
    let after = store
        .get_by_provider(provider_addr)
        .context("re-read watermark")?
        .ok_or_else(|| anyhow::anyhow!("buyer channel vanished"))?;
    anyhow::ensure!(
        after.last_amount == prior_amount && after.last_nonce == U256::from(1u64),
        "top_up must not disturb the voucher watermark"
    );

    Ok(())
}
