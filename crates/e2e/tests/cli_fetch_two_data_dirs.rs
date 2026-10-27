//! Live anvil-backed e2e for two `decdn get` data dirs on one wallet (#2468).
//!
//! Shape: deploy the protocol and launch a node serving one blob. Data dir A
//! fetches it and opens the wallet's pool. Data dir B, a separate `--data-dir`
//! on the same keystore, fetches it and adopts that pool from chain. Then A and
//! B fetch it again at the same time.
//!
//! Each data dir signs with a key derived from the wallet key and its own
//! store's salt, so the two share one pool but never a `(pool, signer,
//! provider)` lane. The proof is in each store's lane watermark: it is exactly
//! the wire bytes of that data dir's own fetches. On a shared lane, B starts
//! from the chain watermark (zero, nothing is redeemed yet) while the node holds
//! A's bytes on the lane, so B heals to A's watermark and records A's bytes as
//! well as its own.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e -E 'binary(cli_fetch_two_data_dirs)'
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

use std::path::Path;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_cache::range_pull::{align_range, bao_encoded_size};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity;
use decdn_incentive::lane::LaneKey;
use decdn_incentive::lane_signer::derive_lane_signer;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "two-data-dirs-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn two_data_dirs_on_one_wallet_pay_on_their_own_lanes() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("two data dirs e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    let blob = vec![0x5au8; 1536 * 1024];
    let hash = Hash::new(&blob);
    let wire = U256::from(whole_blob_wire_bytes(blob.len() as u64));
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice()]).await?;
    anyhow::ensure!(
        hashes == vec![hash],
        "seeded blob hash mismatch: {hashes:?}"
    );
    let provider = node.operator_addr();

    // One wallet, two client data dirs. The keystore lives in A; B names it
    // with `--keystore`. Both dirs are `0o700`, which the buyer store enforces
    // (`tempdir` is `0o755`).
    let dir_a = private_tempdir()?;
    let dir_b = private_tempdir()?;
    eth_identity::generate_and_persist(dir_a.path(), KEYSTORE_PASSWORD, false)
        .context("generate wallet keystore")?;
    let keystore = eth_identity::keystore_path(dir_a.path());
    let wallet = eth_identity::load_signer(&keystore, KEYSTORE_PASSWORD).context("load wallet")?;
    let wallet_addr = wallet.address();
    chain.fund_eth(wallet_addr, 100).await?;
    chain
        .mint_usdc(
            wallet_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint wallet USDC")?;

    let get = |dir: &Path, out: &str| -> Vec<String> {
        get_argv(&chain, &node, &hash, &dir.join(out), dir, &keystore)
    };

    // A opens the wallet's pool. The first run can race the node's view of the
    // fresh pool, so it retries until it lands.
    run_get_until_ready(dir_a.path(), &get(dir_a.path(), "a1.bin")).await?;
    let (pool_a, lane_a) = recorded_lane(dir_a.path(), &wallet, provider)?;
    anyhow::ensure!(
        lane_a.progress == wire,
        "A's first fetch must record exactly its own {wire} wire bytes, got {}",
        lane_a.progress
    );
    check_pull_record(dir_a.path(), blob.len() as u64, wire, provider)?;

    // B has no pool row, so it adopts A's pool from chain and signs on its own
    // lane, which starts at zero.
    run_get_until_ready(dir_b.path(), &get(dir_b.path(), "b1.bin")).await?;
    let (pool_b, lane_b) = recorded_lane(dir_b.path(), &wallet, provider)?;
    anyhow::ensure!(
        pool_b == pool_a,
        "B opened pool {pool_b} instead of adopting {pool_a}"
    );
    anyhow::ensure!(
        lane_a.signer != lane_b.signer,
        "both data dirs drew one salt and sign as {}",
        lane_a.signer
    );
    anyhow::ensure!(
        lane_b.progress == wire,
        "B's first fetch must record exactly its own {wire} wire bytes, got {} (a shared lane \
         heals to A's watermark)",
        lane_b.progress
    );

    // Both data dirs fetch at the same time. Each pool is live and known to
    // the node, so neither needs a retry.
    let (args_a, args_b) = (get(dir_a.path(), "a2.bin"), get(dir_b.path(), "b2.bin"));
    let (a, b) = tokio::join!(
        run_get_once(dir_a.path(), &args_a),
        run_get_once(dir_b.path(), &args_b),
    );
    a.context("A's concurrent fetch")?;
    b.context("B's concurrent fetch")?;

    for (dir, name) in [
        (&dir_a, "a1.bin"),
        (&dir_a, "a2.bin"),
        (&dir_b, "b1.bin"),
        (&dir_b, "b2.bin"),
    ] {
        let got = std::fs::read(dir.path().join(name)).with_context(|| format!("read {name}"))?;
        anyhow::ensure!(got == blob, "{name} mismatch: {} bytes", got.len());
    }
    let two_fetches = wire * U256::from(2u64);
    for (label, dir, earlier) in [("A", &dir_a, lane_a), ("B", &dir_b, lane_b)] {
        let (pool, lane) = recorded_lane(dir.path(), &wallet, provider)?;
        anyhow::ensure!(pool == pool_a, "{label} moved to pool {pool}");
        anyhow::ensure!(
            lane.signer == earlier.signer,
            "{label}'s lane signer changed"
        );
        anyhow::ensure!(
            lane.progress == two_fetches,
            "{label} must record exactly its own two fetches ({two_fetches} wire bytes), got {}",
            lane.progress
        );
    }

    // `NodeFixture` tears the daemon down on drop; there is nothing to await.
    drop(node);
    Ok(())
}

/// One data dir's lane to the provider, as its buyer store records it.
#[derive(Debug, Clone, Copy)]
struct RecordedLane {
    signer: Address,
    progress: U256,
}

/// The pool `dir`'s store records for `wallet`, and the cumulative wire bytes
/// of the lane `dir` signs to `provider` on. The lane's signer is the key
/// derived from the wallet and the store's salt. The store handle drops on
/// return, so a later `decdn get` can open the file.
fn recorded_lane(
    dir: &Path,
    wallet: &PrivateKeySigner,
    provider: Address,
) -> anyhow::Result<(B256, RecordedLane)> {
    let store = RedbBuyerPoolStore::open(dir).context("open buyer store")?;
    let state = store
        .get_by_owner(wallet.address())
        .context("read buyer pool")?
        .ok_or_else(|| anyhow::anyhow!("no pool recorded in {}", dir.display()))?;
    let signer = derive_lane_signer(wallet, &store.lane_salt()?)?.address();
    let lane = LaneKey {
        pool_id: state.pool_id,
        signer,
        provider,
    };
    let progress = state
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("no lane recorded for signer {signer}"))?;
    let wallet_lane = LaneKey {
        signer: wallet.address(),
        ..lane
    };
    anyhow::ensure!(
        state.lane_progress(wallet_lane).is_none(),
        "{} records a lane signed by the wallet key",
        dir.display()
    );
    Ok((
        state.pool_id,
        RecordedLane {
            signer,
            progress: progress.last_bytes,
        },
    ))
}

/// A `0o700` temporary data dir.
fn private_tempdir() -> anyhow::Result<tempfile::TempDir> {
    let dir = tempfile::tempdir().context("tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod data dir 0o700")?;
    Ok(dir)
}

/// The exact wire bytes a whole-blob paid pull vouchers for: the bao-encoded
/// size of the 16 KiB chunk-group-aligned whole range (ADR 038), the quantity
/// the buyer records as the lane watermark.
fn whole_blob_wire_bytes(total: u64) -> u64 {
    match align_range(0, 0, total) {
        Ok(aligned) => bao_encoded_size(total, aligned.chunk_ranges()),
        Err(_) => total,
    }
}

/// The run that landed left a `done` record for `decdn dashboard` under its
/// `HOME`, `home`: the blob's `len` bytes verified, and `wire` bytes paid to
/// `provider` alone.
fn check_pull_record(home: &Path, len: u64, wire: U256, provider: Address) -> anyhow::Result<()> {
    let dir = home.join(".decdn/activity/pulls");
    let records: Vec<serde_json::Value> = std::fs::read_dir(&dir)
        .with_context(|| format!("read {}", dir.display()))?
        .filter_map(Result::ok)
        .filter_map(|e| std::fs::read(e.path()).ok())
        .filter_map(|bytes| serde_json::from_slice(&bytes).ok())
        .collect();
    let done = records
        .iter()
        .find(|r| r["state"] == "done")
        .with_context(|| format!("no done record among {records:?}"))?;
    anyhow::ensure!(done["kind"] == "blob", "{done}");
    anyhow::ensure!(done["received_bytes"] == len, "{done}");
    anyhow::ensure!(done["finished_unix_ms"].is_u64(), "{done}");
    anyhow::ensure!(
        U256::from(done["paid"]["bytes"].as_u64().unwrap_or(0)) == wire,
        "the record pays {wire} wire bytes: {done}"
    );
    anyhow::ensure!(
        done["paid"]["amount_micro_usdc"]
            .as_u64()
            .is_some_and(|a| a > 0),
        "{done}"
    );
    anyhow::ensure!(
        done["nodes"].as_array().map(Vec::len) == Some(1)
            && done["nodes"][0]["provider"] == provider.to_checksum(None),
        "one node, {provider}: {done}"
    );
    Ok(())
}

/// Run `decdn get` once in `data_dir`, failing with its stderr.
async fn run_get_once(data_dir: &Path, args: &[String]) -> anyhow::Result<()> {
    let output = spawn_get(data_dir, args).await?;
    anyhow::ensure!(
        output.status.success(),
        "decdn get failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// Run `decdn get`, retrying until the node's serve path resolves the
/// freshly-opened pool (the CLI has no internal retry for that race).
async fn run_get_until_ready(data_dir: &Path, args: &[String]) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = spawn_get(data_dir, args).await?;
        if output.status.success() {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn get never succeeded; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "get not ready; retrying after serve-path catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

async fn spawn_get(data_dir: &Path, args: &[String]) -> anyhow::Result<std::process::Output> {
    tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
        .arg("get")
        .args(args)
        .output()
        .await
        .context("spawn decdn get")
}

/// The `decdn get` argv (after the `get` subcommand) for blob `hash` into
/// `out`, pinned to this one node and provider so every fetch of a data dir
/// lands on one lane. Chain coordinates are flags, so no config file is
/// needed.
fn get_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: &Hash,
    out: &Path,
    data_dir: &Path,
    keystore: &Path,
) -> Vec<String> {
    vec![
        format!("decdn:{}", hash.to_hex()),
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
