//! Live anvil-backed e2e for `decdn fetch -o -` (#1848 4b): the CLI's STDOUT
//! streaming path over the `decdn_client::Streamer` consumption face,
//! driving the shipped `decdn` binary against a real node over a real paid
//! `cdn/client/v1` stream.
//!
//! Why the whole binary rather than a unit test: the `Streamer` /
//! `VerifiedReader` unit tests in `decdn-client` already prove verified,
//! consumption-paced streaming against a `ScriptedSource`. What only exists on
//! the real wire is that the CLI routes `-o -` to `stream_to_stdout`, opens a
//! real `(signer, provider)` payment lane, streams the bao-verified bytes to the
//! process's stdout (nothing but verified bytes — safe to pipe), and persists
//! the lane's voucher watermark afterward exactly like the file path.
//!
//! What this test asserts:
//!   1. The captured stdout is byte-identical to the source blob.
//!   2. The client's persisted buyer-pool store shows a non-zero cumulative
//!      watermark against the holder — the stream was paid for and settled.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of both `decdn` and `decdn-node`:
//!
//! ```bash
//! cargo build -p decdn-cli -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_fetch_stdout
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
use decdn_cache::Hash;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "stdout-stream-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`]): one daemon plus a chain.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`).
const CHUNK_GROUP: usize = 16 * 1024;

/// Deterministic pseudo-random blob spanning many chunk groups plus a ragged
/// final group, so the streamed output exercises multi-group bao verification.
fn make_blob() -> Vec<u8> {
    let mut v = vec![0u8; 20 * CHUNK_GROUP + 321];
    let mut x: u32 = 0x5EED_1848;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_fetch_dash_streams_verified_bytes_to_stdout() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli fetch -o - e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    let blob = make_blob();
    let blob_hash = Hash::new(&blob);
    let (holder, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice()]).await?;
    anyhow::ensure!(
        hashes.first() == Some(&blob_hash),
        "holder seeded blob hash mismatch: {:?} vs {blob_hash}",
        hashes.first()
    );

    // Funded buyer with an on-disk keystore under a `0o700` client data dir (the
    // `RedbBuyerPoolStore` the CLI opens enforces the mode; `tempdir` is `0o755`).
    let client_dir = tempfile::tempdir().context("client tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        client_dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod client dir 0o700")?;
    eth_identity::generate_and_persist(client_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate buyer keystore")?;
    let keystore = eth_identity::keystore_path(client_dir.path());
    let buyer = eth_identity::load_signer(&keystore, KEYSTORE_PASSWORD).context("load buyer")?;
    let buyer_addr = buyer.address();
    chain.fund_eth(buyer_addr, 100).await?;
    chain
        .mint_usdc(
            buyer_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(8u64),
        )
        .await
        .context("mint buyer USDC")?;

    let args = fetch_argv(&chain, &blob_hash, client_dir.path(), &keystore);

    // The node's chain watcher observes the freshly-opened pool asynchronously; a
    // pre-serve refusal before it catches up is retryable, so retry until one
    // invocation streams the whole blob to stdout.
    let stdout = run_fetch_until_streamed(client_dir.path(), &args, blob.len()).await?;

    // (1) stdout is byte-identical to the source blob — nothing but verified
    // bytes reached the pipe.
    anyhow::ensure!(
        stdout == blob,
        "streamed {} bytes to stdout, expected {}",
        stdout.len(),
        blob.len()
    );

    // (2) The client's persisted store shows a non-zero watermark against the
    // holder — the stream was paid for and settled, exactly like the file path.
    let billed = billed_bytes(client_dir.path(), holder.operator_addr())?;
    anyhow::ensure!(
        billed > 0,
        "the client's persisted buyer-pool store must show bytes billed to the holder"
    );

    drop(holder);
    Ok(())
}

/// Run `decdn fetch ... -o -` until one invocation streams the whole blob to
/// stdout, returning the captured stdout bytes.
async fn run_fetch_until_streamed(
    data_dir: &std::path::Path,
    args: &[String],
    expected_len: usize,
) -> anyhow::Result<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
            .arg("fetch")
            .args(args)
            .output()
            .await
            .context("spawn decdn fetch -o -")?;
        if output.status.success() && output.stdout.len() == expected_len {
            return Ok(output.stdout);
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "no `decdn fetch -o -` streamed the whole blob; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "fetch -o - not ready (status {:?}, {} stdout bytes); retrying:\n{}",
            output.status.code(),
            output.stdout.len(),
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// Cumulative bytes the buyer has paid `provider` for, from the persisted pool's
/// `(signer, provider)` lane watermark. Reopened per call because the CLI
/// subprocess owns the store between calls.
fn billed_bytes(
    data_dir: &std::path::Path,
    provider: alloy::primitives::Address,
) -> anyhow::Result<u64> {
    let Ok(store) = RedbBuyerPoolStore::open(data_dir) else {
        return Ok(0);
    };
    let mut billed = U256::ZERO;
    for pool in store.load_all().context("load buyer pools")?.pools {
        for (lane, progress) in pool.lanes() {
            if lane.provider == provider {
                billed = billed.max(progress.last_bytes);
            }
        }
    }
    Ok(u64::try_from(billed).unwrap_or(u64::MAX))
}

/// The `decdn fetch` argv (after the `fetch` subcommand) to stream `hash` to
/// stdout via auto-discovery, writing to `-o -`.
fn fetch_argv(
    chain: &ChainFixture,
    hash: &Hash,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
) -> Vec<String> {
    vec![
        "--hash".into(),
        hash.to_hex(),
        "-o".into(),
        "-".into(),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--slash-judge-address".into(),
        format!("{}", chain.addrs().slash_judge),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
        "--working-deposit-micro-usdc".into(),
        DEPOSIT_MICRO_USDC.to_string(),
    ]
}
