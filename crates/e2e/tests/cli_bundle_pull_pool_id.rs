//! Live anvil-backed e2e for `decdn bundle pull` over a shared payment pool: one
//! pool funds every entry in the bundle.
//!
//! Shape: deploy the protocol, launch a provider node serving two small blobs,
//! then `bundle pull` a two-entry manifest with the buyer's chain coordinates
//! and NO pool pre-recorded in the client store. The CLI opens one pool on the
//! first entry, reuses it for the second, and lands both files — proving the
//! whole bundle pays every provider it touches from the caller's single shared
//! pool (ADR 003), with no per-provider open.
//!
//! `--provider-address` is passed alongside `--node-id` to pin every entry to
//! this one node, so both entries share the same `(signer, provider)` lane.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_bundle_pull_pool_id
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
use decdn_incentive::lane::LaneKey;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "bundle-pool-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn cli_bundle_pull_uses_one_pool_for_the_whole_bundle() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli bundle pull pool e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's pool \
              state, so decomposing it would thread state through helpers without reducing the \
              journey's length or making it easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // Two distinct served blobs → a two-entry bundle. Fetching both on ONE pool is
    // the point: the second entry reuses the pool the first opened, serialized by
    // the per-provider voucher lane lock.
    let blob_a = b"decdn bundle-pull pool e2e blob A".to_vec();
    let blob_b = b"decdn bundle-pull pool e2e blob B - a second entry".to_vec();
    let hash_a = Hash::new(&blob_a);
    let hash_b = Hash::new(&blob_b);
    let (node, hashes) =
        NodeFixture::launch_with_blobs(&chain, "US", &[blob_a.as_slice(), blob_b.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes == vec![hash_a, hash_b],
        "seeded blob hashes mismatch: {hashes:?} vs [{hash_a}, {hash_b}]"
    );
    let provider_addr = node.operator_addr();

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
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint buyer USDC")?;

    // A local v1 manifest naming both served blobs. `-i` means no manifest blob
    // is fetched first; the entries are pulled directly.
    let out_dir = client_dir.path().join("out");
    let manifest_path = client_dir.path().join("bundle.json");
    let manifest = format!(
        r#"{{"version":1,"entries":[{{"path":"a.bin","hash":"{}"}},{{"path":"b.bin","hash":"{}"}}]}}"#,
        hash_a.to_hex(),
        hash_b.to_hex(),
    );
    std::fs::write(&manifest_path, manifest).context("write local manifest")?;

    let args = bundle_pull_argv(
        &chain,
        &node,
        &manifest_path,
        &out_dir,
        client_dir.path(),
        &keystore,
    );

    // The node serves the freshly-opened pool once its `getPool` view resolves it
    // (right after `openPool` mines), so the first run can race that resolution.
    // Retry until it lands, exactly as the fetch e2e does.
    run_bundle_pull_until_ready(client_dir.path(), &args).await?;

    // Both entries landed with the served bytes.
    let got_a = std::fs::read(out_dir.join("a.bin")).context("read a.bin")?;
    let got_b = std::fs::read(out_dir.join("b.bin")).context("read b.bin")?;
    anyhow::ensure!(got_a == blob_a, "a.bin mismatch: {} bytes", got_a.len());
    anyhow::ensure!(got_b == blob_b, "b.bin mismatch: {} bytes", got_b.len());

    // The CLI opened exactly one pool for this owner, recorded it in the client
    // store, and paid the bundle from it: the pool's `(signer, provider)` lane
    // watermark advanced past zero for the one provider both entries share.
    let store = RedbBuyerPoolStore::open(client_dir.path()).context("reopen client store")?;
    let state = store
        .get_by_owner(buyer_addr)
        .context("read buyer pool")?
        .ok_or_else(|| anyhow::anyhow!("no pool was recorded for owner {buyer_addr}"))?;
    anyhow::ensure!(
        state.owner == buyer_addr,
        "recorded pool owner {} != buyer {buyer_addr}",
        state.owner
    );
    let lane = LaneKey {
        pool_id: state.pool_id,
        signer: buyer_addr,
        provider: provider_addr,
    };
    let progress = state
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("no lane progress recorded for provider {provider_addr}"))?;
    anyhow::ensure!(
        progress.last_bytes > U256::ZERO && progress.last_amount > U256::ZERO,
        "the pool must have paid for the bundle; lane watermark did not advance \
         (bytes={}, amount={})",
        progress.last_bytes,
        progress.last_amount
    );

    // `NodeFixture` tears the daemon down on drop; there is nothing to await.
    drop(node);
    Ok(())
}

/// Run `decdn bundle pull`, retrying until the node's serve path resolves the
/// freshly-opened pool (the CLI has no internal retry for that race).
async fn run_bundle_pull_until_ready(
    data_dir: &std::path::Path,
    args: &[String],
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
            .arg("bundle")
            .arg("pull")
            .args(args)
            .output()
            .await
            .context("spawn decdn bundle pull")?;
        if output.status.success() {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn bundle pull never succeeded; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "bundle pull not ready; retrying after serve-path catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The `decdn bundle pull` argv (after the `bundle pull` subcommand) to pull a
/// local manifest over the explicit-node path, with chain coordinates as flags so
/// no config file is needed. `--provider-address` pins every entry to this one
/// node (and one lane). `--capacity-bond-address` is required even on the
/// explicit-node path: it is the EIP-712 `verifyingContract` the buyer signs its
/// ADR 005 client identity binding against, and the node refuses to serve a paid
/// request that carries no verified binding.
fn bundle_pull_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    manifest: &std::path::Path,
    out_dir: &std::path::Path,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
) -> Vec<String> {
    vec![
        "-i".into(),
        manifest.display().to_string(),
        "-o".into(),
        out_dir.display().to_string(),
        "--node-id".into(),
        node.node_id().to_string(),
        "--addr".into(),
        format!("127.0.0.1:{}", node.bind_port()),
        "--provider-address".into(),
        format!("{}", node.operator_addr()),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--slash-judge-address".into(),
        format!("{}", chain.addrs().slash_judge),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
    ]
}
