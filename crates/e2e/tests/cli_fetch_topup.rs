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
use decdn_cache::Hash;
use decdn_client_pull::buyer_channel::{ensure_allowance, open_channel, top_up};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_channel::BuyerChannelStore;
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::voucher_domain;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (>= deploy minDeposit)
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

    // Clamp the deposit up to the on-chain floor (as the CLI open path does).
    let min_deposit = pc.minDeposit().call().await.context("read minDeposit")?;
    let deposit = U256::from(DEPOSIT_MICRO_USDC).max(min_deposit);
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

// ---- Reactive graduation (#1497): a genuine MID-FETCH `InsufficientDeposit` ----
//
// The test above drives the shared `top_up` kernel directly (the auto-refill-at-
// reuse-time leg, #1103). This one drives the shipped `decdn` binary through a
// REAL paid pull that outgrows its `initial_deposit` partway through — the
// reactive leg added in `fetch_blob_streaming` (`crates/cli/src/commands/fetch.rs`)
// — and asserts the fetch still SUCCEEDS, having topped the channel up on-chain
// toward `working_deposit` rather than surfacing the exhaustion as a terminal
// error.
//
// The channel opens at the on-chain `minDeposit` floor (1 USDC) and the node's
// rate is set high enough that the very FIRST voucher interval (1 MB, the
// protocol default) already costs more than that — so the node rejects the
// channel's first-ever voucher with `InsufficientDeposit`. A fresh channel has
// no prior accepted voucher, so the node's reject carries no `WatermarkBundle`
// (`watermark_bundle_for_reject` requires one to echo back) — exactly the
// "genuine exhaustion, not a healable desync" case `genuine_exhaustion` exists
// to recognize. The CLI should top up to `working_deposit` and retry the same
// blob from scratch, landing a channel deposit of exactly `working_deposit` (no
// bytes were ever committed before the top-up).

const INITIAL_DEPOSIT_MICRO_USDC: u64 = 1_000_000; // 1 USDC == on-chain minDeposit
const WORKING_DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC — plenty to finish the blob
const HIGH_RATE_PER_MB: u64 = 2_000_000; // 2 USDC/MB — exceeds the initial deposit in <1 MB
const TOPUP_KEYSTORE_PASSWORD: &str = "topup-e2e-password";

#[tokio::test(flavor = "multi_thread")]
async fn fetch_larger_than_initial_deposit_tops_up_and_completes() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_reactive_topup()))
        .await
        .context("reactive top-up e2e exceeded the overall timeout")??;
    Ok(())
}

/// A deterministic multi-MB blob — big enough that, at [`HIGH_RATE_PER_MB`], its
/// total cost spans several 1 MB voucher intervals and comfortably exceeds
/// [`INITIAL_DEPOSIT_MICRO_USDC`] while staying well under
/// [`WORKING_DEPOSIT_MICRO_USDC`].
fn make_blob() -> Vec<u8> {
    let mut v = vec![0u8; 2 * 1024 * 1024 + 777];
    let mut x: u32 = 0x2468_ace0;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

async fn run_reactive_topup() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    let blob = make_blob();
    let blob_hash = Hash::new(&blob);
    let (node, hash) = NodeFixture::launch(&chain, "US", &blob).await?;
    anyhow::ensure!(
        hash == blob_hash,
        "seeded blob hash mismatch: {hash} vs {blob_hash}"
    );
    // Bait the very first voucher interval into `InsufficientDeposit`: at the
    // default cost of 10 µUSDC/MB the 1 USDC initial deposit is plenty, so the
    // rate must be raised before the buyer ever opens the channel.
    node.set_rate_per_mb(HIGH_RATE_PER_MB).await?;

    // Funded buyer with an on-disk keystore under a `0o700` client data dir (the
    // `RedbBuyerChannelStore` the CLI opens enforces the mode; `tempdir` is
    // `0o755`).
    let client_dir = tempfile::tempdir().context("client tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        client_dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod client dir 0o700")?;
    eth_identity::generate_and_persist(client_dir.path(), TOPUP_KEYSTORE_PASSWORD, false)
        .context("generate buyer keystore")?;
    let keystore = eth_identity::keystore_path(client_dir.path());
    let buyer =
        eth_identity::load_signer(&keystore, TOPUP_KEYSTORE_PASSWORD).context("load buyer")?;
    let buyer_addr = buyer.address();
    chain.fund_eth(buyer_addr, 100).await?;
    chain
        .mint_usdc(
            buyer_addr,
            U256::from(WORKING_DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint buyer USDC")?;

    let out = client_dir.path().join("blob.bin");
    let args = topup_fetch_argv(
        &chain,
        &node,
        &blob_hash,
        client_dir.path(),
        &keystore,
        &out,
    );

    let before = billed_bytes(client_dir.path(), node.operator_addr())?;
    anyhow::ensure!(
        before == 0,
        "no bytes should be billed before the first fetch"
    );

    run_topup_fetch_until_ready(client_dir.path(), &args).await?;

    let got = std::fs::read(&out).context("read output")?;
    anyhow::ensure!(
        got == blob,
        "the fetch must still complete successfully after the reactive top-up: got {} bytes, \
         expected {}",
        got.len(),
        blob.len()
    );
    let partial = client_dir.path().join("blob.bin.partial");
    anyhow::ensure!(
        !partial.exists(),
        "the .partial scratch file must be promoted away, not left beside --output: {}",
        partial.display()
    );

    // The graduating assertion: the channel's on-chain deposit must have grown
    // from the 1 USDC initial open to the full working deposit. No bytes were
    // ever committed before the reactive top-up fired (the very first voucher
    // was the one rejected), so the topped-up deposit lands at EXACTLY
    // `working_deposit` — not merely "more than initial".
    let store = RedbBuyerChannelStore::open(client_dir.path()).context("open buyer store")?;
    let persisted = store
        .get_by_provider(node.operator_addr())
        .context("read persisted channel")?
        .ok_or_else(|| anyhow::anyhow!("buyer channel not recorded after fetch"))?;
    let channel_id = persisted.channel_id;
    anyhow::ensure!(
        persisted.deposit == U256::from(WORKING_DEPOSIT_MICRO_USDC),
        "the persisted deposit must reflect the graduated top-up: got {}, expected {}",
        persisted.deposit,
        WORKING_DEPOSIT_MICRO_USDC
    );

    let buyer_provider = chain.provider_for(&buyer);
    let pc = PaymentChannel::new(chain.addrs().payment_channel, buyer_provider);
    let onchain = pc
        .getChannel(channel_id)
        .call()
        .await
        .context("getChannel")?
        .deposit;
    anyhow::ensure!(
        onchain == U256::from(WORKING_DEPOSIT_MICRO_USDC),
        "the on-chain channel deposit must have grown to the working deposit: got {onchain}, \
         expected {WORKING_DEPOSIT_MICRO_USDC}"
    );

    drop(node);
    Ok(())
}

/// Cumulative bytes billed on the persisted channel for `provider`, or `0` before
/// any channel has been recorded. Mirrors `cli_fetch_resume.rs`'s helper of the
/// same shape.
fn billed_bytes(data_dir: &std::path::Path, provider: Address) -> anyhow::Result<u64> {
    let Ok(store) = RedbBuyerChannelStore::open(data_dir) else {
        return Ok(0);
    };
    let Some(state) = store.get_by_provider(provider).context("read channel")? else {
        return Ok(0);
    };
    Ok(u64::try_from(state.last_bytes_delivered).unwrap_or(u64::MAX))
}

/// Run `decdn fetch`, retrying until the node's chain watcher has observed the
/// freshly-opened channel (`decdn fetch` has no internal retry for that race).
async fn run_topup_fetch_until_ready(
    data_dir: &std::path::Path,
    args: &[String],
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output =
            tokio::process::Command::from(decdn_command(data_dir, TOPUP_KEYSTORE_PASSWORD)?)
                .arg("fetch")
                .args(args)
                .output()
                .await
                .context("spawn decdn fetch")?;
        if output.status.success() {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn fetch never succeeded; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "fetch not ready; retrying after watcher catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The `decdn fetch` argv (after the `fetch` subcommand) for the reactive
/// top-up journey: a small `--initial-deposit-micro-usdc` (clamped to the
/// on-chain floor) and a `--working-deposit-micro-usdc` large enough to finish
/// the blob once the reactive leg tops up. `--capacity-bond-address` is omitted
/// deliberately: the node holds the blob, so the fetch never needs reactive
/// origin pull-through.
fn topup_fetch_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: &Hash,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
    out: &std::path::Path,
) -> Vec<String> {
    vec![
        "--hash".into(),
        hash.to_hex(),
        "-o".into(),
        out.display().to_string(),
        "--node-id".into(),
        node.node_id().to_string(),
        "--addr".into(),
        format!("127.0.0.1:{}", node.bind_port()),
        "--provider-address".into(),
        format!("{}", node.operator_addr()),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-channel-address".into(),
        format!("{}", chain.addrs().payment_channel),
        "--slash-judge-address".into(),
        format!("{}", chain.addrs().slash_judge),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
        "--initial-deposit-micro-usdc".into(),
        INITIAL_DEPOSIT_MICRO_USDC.to_string(),
        "--working-deposit-micro-usdc".into(),
        WORKING_DEPOSIT_MICRO_USDC.to_string(),
    ]
}
