//! Live anvil-backed e2e for a stored buyer pool the chain holds no record of
//! (issue #2678).
//!
//! Shape: deploy the protocol, launch a node that holds one blob, and seed the
//! client's `RedbBuyerPoolStore` with a buyer row for a pool id this deployment
//! never opened. `getPool` reads that pool with a zero owner, so the node
//! refuses every lane on it `NotFound` and the fetch can pay for no byte. The
//! first `decdn get` runs with a short give-up window and fails. Its stderr
//! names the stale pool on a `note:` line, and the row is gone from the store.
//! The second `decdn get` finds no row, opens a pool, and lands the blob
//! byte-identical.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_fetch_stale_pool
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
use decdn_cache::Hash;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::Deployment;
use decdn_incentive::buyer_pool::{BuyerPoolState, BuyerPoolStore};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "stale-pool-e2e-password";
/// The no-progress limit of the failing run. Nodes refuse every lane on the
/// stale pool, so the run makes no progress and gives up after this long.
const GIVE_UP_AFTER_SECS: u64 = 5;
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn cli_fetch_drops_a_stale_stored_pool_and_recovers() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli fetch stale-pool e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: the second fetch depends on the row the first \
              one dropped, so decomposing it would thread state through helpers without making \
              it easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    let blob = vec![0x5Au8; 256 * 1024];
    let hash = Hash::new(&blob);
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice()]).await?;
    anyhow::ensure!(
        hashes == vec![hash],
        "seeded blob hash mismatch: {hashes:?}"
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
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint buyer USDC")?;

    // The stale row: a pool id this deployment never opened, recorded for the
    // buyer as if a run had opened it.
    let deployment = Deployment {
        chain_id: chain.chain_id(),
        payment_pool: chain.addrs().payment_pool,
    };
    let stale_pool = B256::repeat_byte(0xAB);
    {
        let store = RedbBuyerPoolStore::open(client_dir.path()).context("open client store")?;
        store
            .record(&BuyerPoolState::new(
                stale_pool,
                deployment,
                buyer_addr,
                chain.usdc(),
                U256::from(DEPOSIT_MICRO_USDC),
            ))
            .context("seed the stale buyer row")?;
    }

    let out = client_dir.path().join("blob.bin");
    let mut args = fetch_argv(&chain, &node, &hash, client_dir.path(), &keystore, &out);

    // The first run pays from the stale row. Every node refuses its lanes, so
    // the run fails, and on the way out it reads the pool once more, finds no
    // record, and drops the row.
    let mut first_args = args.clone();
    first_args.extend([
        "--give-up-after-secs".into(),
        GIVE_UP_AFTER_SECS.to_string(),
    ]);
    let first = run_get(client_dir.path(), &first_args).await?;
    let stderr = String::from_utf8_lossy(&first.stderr);
    anyhow::ensure!(
        !first.status.success(),
        "a fetch on a stale pool must fail; stderr:\n{stderr}"
    );
    // A give-up prints its stop line and exits `EX_TEMPFAIL` (75); the stale
    // pool rides along as a note under it.
    anyhow::ensure!(
        first.status.code() == Some(75),
        "a fetch on a stale pool must end as a give-up (exit 75), got {:?}; stderr:\n{stderr}",
        first.status.code()
    );
    let note = stderr
        .lines()
        .find(|line| line.starts_with("note: ") && line.contains("no record of stored buyer pool"))
        .ok_or_else(|| {
            anyhow::anyhow!("stderr names no stale pool on a `note:` line:\n{stderr}")
        })?;
    anyhow::ensure!(
        note.contains(&stale_pool.to_string()) && note.contains("its row is dropped"),
        "the note must name the dropped pool {stale_pool}: {note}"
    );
    anyhow::ensure!(!out.exists(), "a failed fetch must land no output");
    {
        let store = RedbBuyerPoolStore::open(client_dir.path()).context("reopen client store")?;
        let row = store
            .get_by_owner(deployment, buyer_addr)
            .context("read buyer row")?;
        anyhow::ensure!(
            row.is_none(),
            "the stale row must be gone after the failed fetch, found {row:?}"
        );
    }

    // The second run finds no row, so it opens a pool and lands the blob. The
    // node serves a freshly opened pool once its `getPool` view resolves it,
    // so the run can race that resolution; retry until it lands.
    args.extend(["--give-up-after-secs".into(), "30".into()]);
    run_get_until_ready(client_dir.path(), &args).await?;
    let got = std::fs::read(&out).context("read fetched blob")?;
    anyhow::ensure!(got == blob, "fetched blob mismatch: {} bytes", got.len());

    let store = RedbBuyerPoolStore::open(client_dir.path()).context("reopen client store")?;
    let state = store
        .get_by_owner(deployment, buyer_addr)
        .context("read buyer row")?
        .ok_or_else(|| anyhow::anyhow!("the recovering fetch recorded no pool"))?;
    anyhow::ensure!(
        state.pool_id != stale_pool,
        "the recovering fetch must pay from a new pool, not the stale {stale_pool}"
    );

    // `NodeFixture` tears the daemon down on drop; there is nothing to await.
    drop(node);
    Ok(())
}

/// Run `decdn get` with `args` once and return its output.
async fn run_get(
    data_dir: &std::path::Path,
    args: &[String],
) -> anyhow::Result<std::process::Output> {
    tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
        .arg("get")
        .args(args)
        .output()
        .await
        .context("spawn decdn get")
}

/// Run `decdn get`, retrying until the node's serve path resolves the
/// freshly-opened pool (the CLI has no internal retry for that race).
async fn run_get_until_ready(data_dir: &std::path::Path, args: &[String]) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = run_get(data_dir, args).await?;
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

/// The `decdn get` argv (after the `get` subcommand) to pull `hash` from
/// `node` over the explicit-node path, with chain coordinates as flags so no
/// config file is needed. `--capacity-bond-address` is the EIP-712
/// `verifyingContract` the buyer signs its ADR 005 client identity binding
/// against; the node refuses a paid request with no verified binding.
fn fetch_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: &Hash,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
    out: &std::path::Path,
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
        "--governance-rpc-url".into(),
        chain.rpc_url(),
        "--governance-chain-id".into(),
        chain.chain_id().to_string(),
        "--pool".into(),
        format!(
            "eip155:{}:{:#x}",
            chain.chain_id(),
            chain.addrs().payment_pool
        ),
        "--pool-rpc-url".into(),
        chain.rpc_url(),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
    ]
}
