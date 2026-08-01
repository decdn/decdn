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
use decdn_bao_range::align_range;
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
// The channel opens at a small configured initial deposit (1 USDC; escrowed as
// configured — no on-chain floor) and the node's rate is set high enough that
// the very FIRST voucher interval (1 MB, the protocol default) already costs
// more than that — so the node rejects the channel's first-ever voucher with
// `InsufficientDeposit`. A fresh channel has no prior accepted voucher, so the
// node's reject carries no `WatermarkBundle` (`watermark_bundle_for_reject`
// requires one to echo back) — exactly the "genuine exhaustion, not a healable
// desync" case `genuine_exhaustion` exists to recognize. The CLI should top up
// to `working_deposit` and retry the same blob from scratch, landing a channel
// deposit of exactly `working_deposit` (no bytes were ever committed before
// the top-up).

const INITIAL_DEPOSIT_MICRO_USDC: u64 = 1_000_000; // 1 USDC initial deposit
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
    topup_fetch_argv_with_deposits(
        chain,
        node,
        hash,
        data_dir,
        keystore,
        out,
        INITIAL_DEPOSIT_MICRO_USDC,
        WORKING_DEPOSIT_MICRO_USDC,
    )
}

/// Same argv shape as [`topup_fetch_argv`], but with the initial/working deposit
/// as parameters rather than the single-voucher-exhaustion test's fixed
/// constants — used by the multi-interval regression below, which needs a
/// bigger initial deposit so several intervals are delivered before exhaustion.
#[allow(clippy::too_many_arguments)]
fn topup_fetch_argv_with_deposits(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: &Hash,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
    out: &std::path::Path,
    initial_deposit_micro_usdc: u64,
    working_deposit_micro_usdc: u64,
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
        initial_deposit_micro_usdc.to_string(),
        "--working-deposit-micro-usdc".into(),
        working_deposit_micro_usdc.to_string(),
    ]
}

// ---- Multi-interval reactive top-up: no double-pay, no under-pay ----
//
// The test above bakes the channel's very FIRST voucher into `InsufficientDeposit`
// on a fresh channel — no prior accepted voucher exists, so this is the shallowest
// possible exercise of the reactive branch. This test drives the case that
// actually motivated the fix: several whole voucher intervals get delivered AND
// ACCEPTED first, and only the NEXT one exhausts the deposit.
//
// Before the `genuine_exhaustion` fix (client-pull's advancement-based bundle
// check), this scenario was unreachable end-to-end: once any voucher has been
// accepted, the node attaches a `WatermarkBundle` to every subsequent
// watermark-gated rejection it can (`watermark_bundle_for_reject`), including a
// perfectly ordinary exhaustion — and the old `genuine_exhaustion` treated ANY
// authenticated bundle as proof of a healable desync, regardless of whether it
// told the client anything new. That routed real exhaustion into the resync path
// (which cannot fix a genuinely short deposit) instead of the top-up path, and the
// fetch failed outright after burning `MAX_RESUME_ATTEMPTS`. The fix distinguishes
// "bundle reports something AHEAD of what we already hold" (desync — reseed) from
// "bundle just echoes our own already-committed watermark" (not a desync — the
// exhaustion is real), by comparing the bundle's nonce against `ledger.committed()`.
//
// This also exercises the OTHER half of the fix: resuming at the CONTENT paid
// frontier — `content_paid_frontier(fetch_start_offset, total_bytes,
// committed.bytes_now - committed.bytes_at_start)` — rather than the raw on-disk
// length OR the naive `fetch_start + wire_delta`. Vouchers pay for WIRE bytes
// (bao content plus interleaved proof, ADR 038), so `committed.bytes` is a WIRE
// watermark; mapping it back through the bao tree lands the resume on the largest
// content chunk-group boundary provably inside the paid wire. The fix must
// re-fetch and pay for exactly the delivered-but-unpaid tail — no more
// (double-pay), no less (under-pay). The old `fetch_start + wire_delta` resume
// treated the wire watermark as a content offset and overshot by the proof
// overhead, silently skipping ~one proof's worth of delivered content from
// billing — a sub-1% under-pay that a content-only cost floor cannot see (the
// paid proof inflates any honest settle above it), which is why the floor below
// is the whole-blob WIRE cost.

const MULTI_WORKING_DEPOSIT_MICRO_USDC: u64 = 40_000_000; // plenty to finish the blob
// Must be >= working_deposit / LOW_WATER_DIVISOR (8_000_000 at the current
// divisor of 5): this test pre-opens and pre-records the channel itself, so
// `open_or_reuse`'s reuse-time auto-refill (#1103) sees an EXISTING channel on
// the CLI's one and only invocation. Below that threshold, `open_or_reuse`
// tops it up to the working deposit before the stream even opens — pre-empting
// the REACTIVE (mid-stream) top-up this test means to exercise.
//
// Covers exactly two whole voucher intervals at the daemon's UNCONFIGURED
// default voucher interval — 4 MiB
// (`decdn_common::config::DEFAULT_VOUCHER_INTERVAL_MB`, distinct from the
// client-side wire fallback of the same name in `decdn_protocol`, which is 1)
// — and `MULTI_RATE_PER_MB` (2 * 8_000_000 = 16_000_000), so the third — a
// partial tail, since the blob is just over two intervals — is the one that
// genuinely exhausts the deposit.
//
// The interval is left at the daemon's default deliberately, rather than
// configured down to something smaller: overriding it requires a daemon
// RESTART (`voucher_interval_mb` is read once at bring-up), and restarting
// between the channel's on-chain open and this test's later on-chain `topUp`
// was observed to make the daemon's settlement watcher stop applying
// `ChannelToppedUp` events to its tracked channel state — the admin API kept
// reporting the pre-top-up deposit indefinitely, well past any poll interval,
// causing the resumed voucher to be rejected forever. That looks like a real,
// separate bug in the watcher/restart interaction, out of scope for this fix;
// avoiding any daemon restart in this test sidesteps it entirely (mirroring
// the single-voucher test above, which also never restarts the daemon and
// reliably sees its own top-up applied).
const MULTI_INITIAL_DEPOSIT_MICRO_USDC: u64 = 16_000_000;
const MULTI_RATE_PER_MB: u64 = 2_000_000; // 2 USDC/MB, same as the single-voucher test
/// The megabyte the per-MB rates are quoted against — the denominator of
/// `next_voucher`'s `ceil(bytes * rate / MB)` voucher-pricing formula.
const MB_BYTES: u64 = 1024 * 1024;

#[tokio::test(flavor = "multi_thread")]
async fn fetch_topup_after_several_delivered_intervals_does_not_double_pay() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_multi_interval_topup()))
        .await
        .context("multi-interval reactive top-up e2e exceeded the overall timeout")??;
    Ok(())
}

/// A deterministic blob spanning just over two whole 4 MiB voucher intervals
/// (the daemon's default cadence), so two full intervals are delivered and
/// accepted before a small partial third exhausts
/// `MULTI_INITIAL_DEPOSIT_MICRO_USDC`.
fn make_multi_interval_blob() -> Vec<u8> {
    let mut v = vec![0u8; 9 * 1024 * 1024 + 777];
    let mut x: u32 = 0x2468_ac13;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

async fn run_multi_interval_topup() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // A blob spanning just over two whole 4 MiB voucher intervals, so two full
    // intervals get delivered and accepted before a small partial tail
    // exhausts the deposit.
    let blob = make_multi_interval_blob();
    let blob_hash = Hash::new(&blob);
    let (node, hash) = NodeFixture::launch(&chain, "US", &blob).await?;
    anyhow::ensure!(
        hash == blob_hash,
        "seeded blob hash mismatch: {hash} vs {blob_hash}"
    );
    node.set_rate_per_mb(MULTI_RATE_PER_MB).await?;

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
            U256::from(MULTI_WORKING_DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint buyer USDC")?;

    // Pre-open the channel ourselves and wait for the node to observe it,
    // rather than letting the CLI's own `open_or_reuse` race the node's chain
    // watcher the way `run_topup_fetch_until_ready` exists to ride out for the
    // OTHER test in this file. That retry loop re-invokes the WHOLE `decdn
    // fetch` process on ANY failure, including ones unrelated to the race —
    // and a second invocation resumes from whatever the first one flushed via
    // the function-ENTRY `resume_offset(existing_partial_len(...))` path
    // (unrelated to this fix, and pre-existing), which would corrupt the very
    // cost measurement this test exists to take. Pre-clearing the race keeps
    // this test to exactly ONE `decdn fetch` invocation, so the reactive
    // top-up branch is the only thing that can move the byte offset.
    let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_channel);
    ensure_allowance(
        &chain.provider_for(&buyer),
        chain.usdc(),
        buyer_addr,
        chain.addrs().payment_channel,
        None,
    )
    .await
    .context("approve PaymentChannel")?;
    let pc = PaymentChannel::new(chain.addrs().payment_channel, chain.provider_for(&buyer));
    // Escrowed as configured — no on-chain floor to clamp up to, only a
    // non-zero requirement (`openChannel` reverts `ZeroAmount`).
    let initial_deposit = U256::from(MULTI_INITIAL_DEPOSIT_MICRO_USDC);
    let opened = open_channel(
        &pc,
        Arc::new(buyer.clone()),
        &voucher_dom,
        chain.usdc(),
        buyer_addr,
        node.operator_addr(),
        initial_deposit,
        // ZERO => self-signing (the funder signs its own vouchers).
        Address::ZERO,
    )
    .await
    .context("open buyer channel")?;
    {
        // Scoped: the CLI subprocess below opens its OWN handle on the same
        // redb file, and the store enforces single-writer access — this
        // handle must be dropped before spawning `decdn fetch`.
        let store = RedbBuyerChannelStore::open(client_dir.path()).context("open buyer store")?;
        store.record(&opened.state).context("record channel")?;
    }
    node.wait_for_channel(opened.state.channel_id, Duration::from_secs(60))
        .await
        .context("wait for node to observe the pre-opened channel")?;

    let out = client_dir.path().join("blob.bin");
    let args = topup_fetch_argv_with_deposits(
        &chain,
        &node,
        &blob_hash,
        client_dir.path(),
        &keystore,
        &out,
        MULTI_INITIAL_DEPOSIT_MICRO_USDC,
        MULTI_WORKING_DEPOSIT_MICRO_USDC,
    );

    let before = billed_bytes(client_dir.path(), node.operator_addr())?;
    anyhow::ensure!(
        before == 0,
        "no bytes should be billed before the first fetch"
    );

    // A single invocation — see the comment above on why this test avoids
    // `run_topup_fetch_until_ready`'s blind cross-invocation retry.
    let output =
        tokio::process::Command::from(decdn_command(client_dir.path(), TOPUP_KEYSTORE_PASSWORD)?)
            .arg("fetch")
            .args(&args)
            .output()
            .await
            .context("spawn decdn fetch")?;
    anyhow::ensure!(
        output.status.success(),
        "decdn fetch failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

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

    // The money-correctness assertion: the channel's persisted cumulative
    // voucher watermark (`last_amount` — the off-chain figure a `settleChannel`
    // would later claim on-chain; the channel is never closed in this test, so
    // there is no on-chain `claimedAmount` to read yet) must be the blob's real
    // WIRE cost — delivered ONCE — never `blob_cost + already_delivered_prefix`
    // (double-pay) and never below the whole-blob wire cost (under-pay).
    //
    // Two reference costs, both via the node's/`next_voucher`'s `ceil(bytes *
    // rate / MB)` voucher-pricing formula:
    //
    //  * `true_cost` — over the blob's CONTENT bytes. This is the spec figure and
    //    an absolute floor, but NOT a tight one: vouchers actually pay for WIRE
    //    bytes (content + interleaved bao proof, ADR 038), so every honest settle
    //    sits ABOVE `true_cost` by the paid proof. A content-only floor therefore
    //    cannot see the wire-vs-content under-pay this test guards — the skipped
    //    sliver (~one proof's worth) is smaller than the proof overhead that
    //    inflates the settle above `true_cost`. It is kept only for context.
    //  * `wire_floor` — over the blob's exact bao WIRE bytes (`bao_encoded_size`,
    //    the identical tree walk the serve encoder and the pull's
    //    `expected_wire_bytes` use). A correct fetch pays for every one of these
    //    bytes at least once, so `wire_floor` is the TIGHT no-under-pay gate: the
    //    old `fetch_start + wire_delta` resume skipped delivered content and
    //    settles strictly below it.
    let blob_len = u64::try_from(blob.len()).context("blob length as u64")?;
    let ceil_cost = |bytes: u64| {
        U256::from(bytes)
            .saturating_mul(U256::from(MULTI_RATE_PER_MB))
            .div_ceil(U256::from(MB_BYTES))
    };
    let true_cost = ceil_cost(blob_len);
    // Exact whole-blob wire size: content + every 64-byte bao proof node, in
    // pre-order (ADR 038). `align_range(0, 0, blob_len)` is the whole-blob range;
    // its `wire_len()` is `bao_encoded_size` over the real block-size tree.
    let whole_wire = align_range(0, 0, blob_len)
        .context("align whole blob")?
        .wire_len();
    let wire_floor = ceil_cost(whole_wire);
    // One 16 KiB chunk group's cost. The conservative resume snaps the paid
    // frontier DOWN to a group boundary, so the resumed leg re-fetches STRICTLY
    // LESS than one group of already-paid content; two groups of headroom above
    // `wire_floor` also covers the resumed leg's own left-boundary proof hashes
    // (~log2(groups) × 64 B) and the handful of per-voucher `ceil` roundings —
    // and is still ~250× below a single re-paid 4 MiB voucher interval, which is
    // what a genuine double-pay would add.
    let one_group_cost = ceil_cost(decdn_bao_range::CHUNK_GROUP_BYTES);
    let ceiling = wire_floor.saturating_add(one_group_cost.saturating_mul(U256::from(2u64)));

    let store = RedbBuyerChannelStore::open(client_dir.path()).context("open buyer store")?;
    let persisted = store
        .get_by_provider(node.operator_addr())
        .context("read persisted channel")?
        .ok_or_else(|| anyhow::anyhow!("buyer channel not recorded after fetch"))?;

    // Upper bound: no double-pay. At most one chunk group of re-fetched paid
    // content plus small proof/rounding slack above the whole-blob wire cost.
    anyhow::ensure!(
        persisted.last_amount <= ceiling,
        "the channel must not have double-paid the prefix delivered before the top-up: \
         settled {} µUSDC, but the whole-blob WIRE cost at {MULTI_RATE_PER_MB} µUSDC/MB is \
         {wire_floor} µUSDC (content-only cost {true_cost}); tight ceiling {ceiling} allows \
         under one re-fetched chunk group — a double-pay would settle a full interval higher",
        persisted.last_amount
    );
    // Lower bound: no under-pay. The whole blob's wire bytes were paid at least
    // once. The wire-vs-content bug skipped ~one proof's worth of delivered
    // content and would settle BELOW `wire_floor` (yet still above the coarse
    // content-only `true_cost`, which is why the floor must be `wire_floor`).
    anyhow::ensure!(
        persisted.last_amount >= wire_floor,
        "the channel under-paid: settled {} µUSDC, below the whole-blob WIRE cost of \
         {wire_floor} µUSDC (content-only {true_cost}) — resuming past the true content paid \
         frontier would skip billing the delivered-but-unpaid tail exactly like this",
        persisted.last_amount
    );

    drop(node);
    Ok(())
}

// ---- Two reactive top-ups in ONE fetch: the paid-frontier baselines are per-leg ----
//
// The multi-interval test above drives exactly ONE top-up, sized so the working
// deposit finishes the blob. That leaves the loop's most fragile invariant
// untested: `content_paid_frontier` inverts the wire cost of ONE contiguous
// delivery starting at `fetch_start_offset`, but `ledger.committed().bytes` is
// CHANNEL-cumulative and keeps climbing across every leg of the fetch.
//
// If the two baselines (`fetch_start_offset` / `fetch_start_committed_bytes`) are
// captured once per FETCH rather than re-anchored per LEG, the second top-up feeds
// the helper the SUM of two independent bao range encodings — which re-bills the
// first leg's span and its re-sent root->offset proof path — against a start offset
// that is still the fetch's original one. The inflated budget maps to a frontier
// PAST the true paid one: content skipped unbilled, `byte_offset` beyond the
// verified on-disk prefix, `set_len` zero-extending the partial, and the whole-file
// hash check failing a fetch that was already paid for.
//
// Sizing, at `TWO_TOPUP_RATE_PER_MB` and the daemon's default 4 MiB voucher
// interval, so exactly two reactive top-ups are needed:
//
//   * The node's pre-serve deposit gate (#1518) refuses to serve unless headroom
//     covers one credit window — `DEFAULT_CREDIT_WINDOW_BYTES` (8 MiB) at the
//     quoted rate = 16_000_000 µUSDC here. So BOTH the initial deposit and the
//     working target must be >= that, or the resumed open after a top-up is
//     refused instead of served. This is what puts a hard floor under the blob
//     size: two top-ups need a blob costing more than `initial + working`.
//   * Vouchers accumulate 8_000_000 µUSDC per whole 4 MiB interval. Starting at a
//     16_000_000 deposit: vouchers 1-2 are accepted (cumulative 8M, then exactly
//     16M — the gate is `>`, so an exact match still clears), and voucher 3 (24M)
//     exhausts it. Each top-up restores headroom to the full 16_000_000 working
//     target, buying two more intervals. A ~20 MiB blob costs ~40_300_000 µUSDC of
//     wire, which lands strictly between `initial + working` (32M — so a second
//     top-up IS required) and `initial + 2*working` (48M — so two suffice, inside
//     the `MAX_TOPUP_ATTEMPTS` budget of 3).
//
// The money bound is the same two-sided WIRE band the single-top-up test uses, and
// it is the point of the test: across two top-ups the blob's wire bytes must be
// paid for exactly once. The ceiling allows a little more slack here than the
// single-top-up case because there are two conservative group-snapped resumes, each
// re-fetching strictly under one chunk group, plus each resumed leg's own
// left-boundary proof hashes.

const TWO_TOPUP_RATE_PER_MB: u64 = 2_000_000; // 2 USDC/MB, as above
// Both must clear the 8 MiB credit window at the rate above (16_000_000 µUSDC).
const TWO_TOPUP_INITIAL_MICRO_USDC: u64 = 16_000_000;
const TWO_TOPUP_WORKING_MICRO_USDC: u64 = 16_000_000;
/// Just over 20 MiB: costs more than `initial + working` (forcing a SECOND
/// top-up) and less than `initial + 2*working` (so two are enough).
const TWO_TOPUP_BLOB_BYTES: usize = 20 * 1024 * 1024 + 4113;

/// IGNORED — a live reproducer for a SEPARATE, pre-existing defect, not a
/// regression in the per-leg baseline fix this test was written for.
///
/// As soon as the exhausting blob is large enough that the node still has bytes
/// to send when the deposit runs out, `decdn fetch` dies with
///
/// ```text
/// Error: write failed: frame I/O error: sending stopped by peer: error 0
/// ```
///
/// and the reactive branch never runs at all — the CLI prints no `note: …topped
/// up…` line, and no `topUp` reaches the chain. The mid-stream rejection reaches
/// the client as an ambiguous WRITE failure instead of the typed
/// `UpstreamVoucherRejected` that `genuine_exhaustion` keys on, so the whole
/// reactive top-up path is bypassed.
///
/// Both existing reactive tests avoid this by construction: their blobs are small
/// enough that the node has finished sending before the exhausting voucher is
/// refused (2 MiB and 9 MiB, against a deposit that funds ~8 MiB plus the credit
/// window). Empirically the boundary sits between the 9 MiB that passes and 14 MiB;
/// at 14 MiB and 20 MiB this test fails on the FIRST leg.
///
/// Verified pre-existing: stashing every source change from the #1497 review pass
/// and re-running reproduces the identical failure, so it is not caused by the
/// per-leg re-anchor, the `reseed` monotonicity guard, or the proof-of-service
/// refill gate.
///
/// This matters beyond the test: it is exactly the case the two-tier deposit
/// exists to serve — a single blob far larger than the small initial deposit —
/// and it also means a SECOND reactive top-up has never been exercised end to
/// end. Un-ignore once the mid-stream rejection surfaces as a typed rejection;
/// the sizing below is already correct for driving two top-ups.
#[ignore = "reproduces a pre-existing mid-stream rejection defect; see the doc comment"]
#[tokio::test(flavor = "multi_thread")]
async fn fetch_across_two_reactive_topups_pays_each_wire_byte_exactly_once() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_two_topup_fetch()))
        .await
        .context("two-top-up reactive e2e exceeded the overall timeout")??;
    Ok(())
}

fn make_two_topup_blob() -> Vec<u8> {
    let mut v = vec![0u8; TWO_TOPUP_BLOB_BYTES];
    let mut x: u32 = 0x1357_9bdf;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

async fn run_two_topup_fetch() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    let blob = make_two_topup_blob();
    let blob_hash = Hash::new(&blob);
    let (node, hash) = NodeFixture::launch(&chain, "US", &blob).await?;
    anyhow::ensure!(
        hash == blob_hash,
        "seeded blob hash mismatch: {hash} vs {blob_hash}"
    );
    node.set_rate_per_mb(TWO_TOPUP_RATE_PER_MB).await?;

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
    // Enough for the open plus both top-ups, with headroom.
    chain
        .mint_usdc(
            buyer_addr,
            U256::from(TWO_TOPUP_INITIAL_MICRO_USDC + 4 * TWO_TOPUP_WORKING_MICRO_USDC),
        )
        .await
        .context("mint buyer USDC")?;

    // Pre-open and pre-record the channel, and never restart the daemon — same
    // reasoning as the multi-interval test above: this keeps the journey to
    // exactly ONE `decdn fetch` invocation, so the reactive top-up branch is the
    // only thing that can move the byte offset, and the settle measurement below
    // is not corrupted by a cross-invocation resume.
    let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_channel);
    ensure_allowance(
        &chain.provider_for(&buyer),
        chain.usdc(),
        buyer_addr,
        chain.addrs().payment_channel,
        None,
    )
    .await
    .context("approve PaymentChannel")?;
    let pc = PaymentChannel::new(chain.addrs().payment_channel, chain.provider_for(&buyer));
    let opened = open_channel(
        &pc,
        Arc::new(buyer.clone()),
        &voucher_dom,
        chain.usdc(),
        buyer_addr,
        node.operator_addr(),
        U256::from(TWO_TOPUP_INITIAL_MICRO_USDC),
        // ZERO => self-signing (the funder signs its own vouchers).
        Address::ZERO,
    )
    .await
    .context("open buyer channel")?;
    {
        // Scoped: the CLI subprocess opens its own handle on the same redb file
        // and the store is single-writer.
        let store = RedbBuyerChannelStore::open(client_dir.path()).context("open buyer store")?;
        store.record(&opened.state).context("record channel")?;
    }
    node.wait_for_channel(opened.state.channel_id, Duration::from_secs(60))
        .await
        .context("wait for node to observe the pre-opened channel")?;

    let out = client_dir.path().join("blob.bin");
    let args = topup_fetch_argv_with_deposits(
        &chain,
        &node,
        &blob_hash,
        client_dir.path(),
        &keystore,
        &out,
        TWO_TOPUP_INITIAL_MICRO_USDC,
        TWO_TOPUP_WORKING_MICRO_USDC,
    );

    let output =
        tokio::process::Command::from(decdn_command(client_dir.path(), TOPUP_KEYSTORE_PASSWORD)?)
            .arg("fetch")
            .args(&args)
            .output()
            .await
            .context("spawn decdn fetch")?;
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    anyhow::ensure!(output.status.success(), "decdn fetch failed: {stderr}");

    // The fetch must have taken the reactive branch TWICE. Without this the test
    // could pass having driven the single-top-up path the test above already
    // covers, and the per-leg baseline invariant would go unexercised.
    let topups = stderr.matches("topped up").count();
    anyhow::ensure!(
        topups == 2,
        "the sizing must force exactly two reactive top-ups (saw {topups}); \
         re-check the deposit/rate/blob arithmetic against the pre-serve credit-window \
         gate. stderr:\n{stderr}"
    );

    // Byte-exact delivery. This is where a frontier that ran PAST the true paid
    // one shows up: `set_len` would have zero-extended the partial over the
    // skipped span, and the whole-file BLAKE3 check would reject the result.
    let got = std::fs::read(&out).context("read output")?;
    anyhow::ensure!(
        got == blob,
        "the blob must be byte-exact after two reactive top-ups: got {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    let partial = client_dir.path().join("blob.bin.partial");
    anyhow::ensure!(
        !partial.exists(),
        "the .partial scratch file must be promoted away, not left beside --output: {}",
        partial.display()
    );

    // The money bound, two-sided over WIRE bytes — see the multi-interval test
    // above for why the floor must be the wire cost and not the content-only one.
    let blob_len = u64::try_from(blob.len()).context("blob length as u64")?;
    let ceil_cost = |bytes: u64| {
        U256::from(bytes)
            .saturating_mul(U256::from(TWO_TOPUP_RATE_PER_MB))
            .div_ceil(U256::from(1024u64 * 1024))
    };
    let whole_wire = align_range(0, 0, blob_len)
        .context("align whole blob")?
        .wire_len();
    let wire_floor = ceil_cost(whole_wire);
    // Two conservative resumes, each re-fetching strictly under one 16 KiB chunk
    // group, plus each resumed leg's left-boundary proof path and the per-voucher
    // `ceil` roundings. Four groups of headroom covers all of it and is still far
    // below the 8_000_000 a single re-paid 4 MiB interval would add.
    let one_group_cost = ceil_cost(decdn_bao_range::CHUNK_GROUP_BYTES);
    let ceiling = wire_floor.saturating_add(one_group_cost.saturating_mul(U256::from(4u64)));

    let store = RedbBuyerChannelStore::open(client_dir.path()).context("open buyer store")?;
    let persisted = store
        .get_by_provider(node.operator_addr())
        .context("read persisted channel")?
        .ok_or_else(|| anyhow::anyhow!("buyer channel not recorded after fetch"))?;

    anyhow::ensure!(
        persisted.last_amount >= wire_floor,
        "under-pay across two top-ups: settled {} µUSDC, below the whole-blob WIRE cost of \
         {wire_floor} µUSDC. A second-leg paid frontier derived from a stale baseline \
         overshoots the true one and skips billing exactly this way",
        persisted.last_amount
    );
    anyhow::ensure!(
        persisted.last_amount <= ceiling,
        "double-pay across two top-ups: settled {} µUSDC against a whole-blob WIRE cost of \
         {wire_floor} µUSDC (ceiling {ceiling}); a re-paid 4 MiB interval would add 8000000",
        persisted.last_amount
    );

    // Both top-ups landed on-chain and are reflected locally. Each one restores
    // headroom to the working target, so the escrow ends at
    // `settled + working` — and in particular strictly above what one top-up
    // alone could have reached (`initial + working`).
    let one_topup_ceiling = U256::from(TWO_TOPUP_INITIAL_MICRO_USDC + TWO_TOPUP_WORKING_MICRO_USDC);
    anyhow::ensure!(
        persisted.deposit > one_topup_ceiling,
        "two top-ups must escrow more than a single top-up could ({one_topup_ceiling}); got {}",
        persisted.deposit
    );
    // The escrow must still cover everything vouchered — the fetch completed, so
    // the final voucher was within deposit.
    anyhow::ensure!(
        persisted.deposit >= persisted.last_amount,
        "escrow {} must cover the settled amount {}",
        persisted.deposit,
        persisted.last_amount
    );
    let onchain = pc
        .getChannel(opened.state.channel_id)
        .call()
        .await
        .context("read on-chain channel")?
        .deposit;
    anyhow::ensure!(
        onchain == persisted.deposit,
        "the persisted deposit must match the chain after two top-ups: local {} vs chain \
         {onchain}",
        persisted.deposit
    );

    drop(node);
    Ok(())
}
