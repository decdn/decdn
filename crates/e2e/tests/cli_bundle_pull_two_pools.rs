//! Live anvil-backed e2e for a bundle pull across two `PaymentPool` contracts
//! (ADR 005 § Payment pool routing): a bundle pull runs one pass per pool
//! that a holder is routed to, and a later pass skips what an earlier pass
//! landed.
//!
//! Shape: the fixture chain runs two `PaymentPool` contracts, the snapshot's
//! (pool 1) and a second one (pool 2). Holder A accepts pool 1 and carries the
//! manifest and `a.bin`. Holder B accepts pool 2 only and carries `b.bin`. The
//! client's config lists both pools, pool 1 first. `decdn get` pulls the
//! bundle by its manifest hash with no `--node-id`, so every entry discovers
//! its holders.
//!
//! The first pass pays through pool 1: it lands the manifest and `a.bin`, and
//! routes holder B to pool 2. The second pass pays through pool 2 and lands
//! `b.bin`. What this test asserts, for one `decdn get` that starts from an
//! empty output directory:
//!   1. It exits zero, and both outputs are byte-identical to their blobs.
//!   2. The client's store holds a pool of the buyer on each contract.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of both `decdn` and `decdn-node`:
//!
//! ```bash
//! cargo build -p decdn-cli -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e -E 'binary(cli_bundle_pull_two_pools)'
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

use alloy::primitives::{Address, U256};
use anyhow::Context;
use decdn_cache::Hash;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::Deployment;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "bundle-two-pools-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule): two
/// daemons plus a chain, but no long internal poll ladder.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// A deterministic blob of a few chunk groups.
fn make_blob(seed: u8) -> Vec<u8> {
    let mut v = vec![seed; 6 * 16 * 1024 + 111];
    let mut x: u32 = u32::from(seed) << 16 | 0x5EED;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bundle_pull_pays_each_holder_through_the_pool_it_accepts() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("two-pool bundle pull e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;
    let pool1 = chain.addrs().payment_pool;
    let pool2 = chain.redeploy_payment_pool().await?;
    anyhow::ensure!(pool2 != pool1, "the second pool must land at a new address");

    let a = make_blob(0xA1);
    let b = make_blob(0xB2);
    let (a_hash, b_hash) = (Hash::new(&a), Hash::new(&b));
    let manifest = format!(
        r#"{{"version":1,"entries":[{{"path":"a.bin","hash":"{}"}},{{"path":"b.bin","hash":"{}"}}]}}"#,
        a_hash.to_hex(),
        b_hash.to_hex(),
    )
    .into_bytes();
    let manifest_hash = Hash::new(&manifest);

    let (holder_a, hashes_a) =
        NodeFixture::launch_with_blobs(&chain, "US", &[a.as_slice(), manifest.as_slice()]).await?;
    anyhow::ensure!(
        hashes_a == vec![a_hash, manifest_hash],
        "holder A seeded blob hashes mismatch: {hashes_a:?}"
    );
    let (holder_b, hashes_b) =
        NodeFixture::launch_with_blobs_on_payment_pool(&chain, pool2, "US", &[b.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes_b == vec![b_hash],
        "holder B seeded blob hashes mismatch: {hashes_b:?}"
    );

    // Funded buyer with an on-disk keystore under a `0o700` client data dir (the
    // `RedbBuyerPoolStore` the CLI opens enforces the mode; `tempdir` is
    // `0o755`). It opens one pool on each contract.
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

    let config = client_dir.path().join("client.toml");
    std::fs::write(&config, client_config(&chain, &keystore, [pool1, pool2]))
        .context("write client config")?;
    let out_dir = client_dir.path().join("out");
    let args = bundle_get_argv(&manifest_hash, &out_dir, client_dir.path());
    pull_from_empty_until_done(client_dir.path(), &config, &args, &out_dir).await?;

    // (1) Byte-identical outputs.
    for (name, want) in [("a.bin", &a), ("b.bin", &b)] {
        let got = std::fs::read(out_dir.join(name)).with_context(|| format!("read {name}"))?;
        anyhow::ensure!(
            got == *want,
            "{name} mismatch: got {} bytes, expected {}",
            got.len(),
            want.len()
        );
    }

    // (2) The buyer holds a pool on each contract: each holder was paid
    // through the pool it accepts.
    let store = RedbBuyerPoolStore::open(client_dir.path()).context("open buyer store")?;
    for payment_pool in [pool1, pool2] {
        let deployment = Deployment {
            chain_id: chain.chain_id(),
            payment_pool,
        };
        anyhow::ensure!(
            store
                .get_by_owner(deployment, buyer_addr)
                .context("read buyer pool")?
                .is_some(),
            "the buyer holds no pool on {payment_pool}"
        );
    }

    // `NodeFixture` tears each daemon down on drop; there is nothing to await.
    drop(holder_a);
    drop(holder_b);
    Ok(())
}

/// Run `decdn --config <config> get <args>` from an empty `out_dir` until one
/// invocation exits zero. The nodes catch up with a freshly opened pool
/// asynchronously, so an early attempt can fail. Each attempt starts from an
/// empty output directory, so the run that succeeds landed every entry itself.
async fn pull_from_empty_until_done(
    data_dir: &Path,
    config: &Path,
    args: &[String],
    out_dir: &Path,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        match std::fs::remove_dir_all(out_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).with_context(|| format!("remove {}", out_dir.display()));
            }
        }
        let output = tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
            .arg("--config")
            .arg(config)
            .arg("get")
            .args(args)
            .output()
            .await
            .context("spawn decdn get")?;
        if output.status.success() {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "no `decdn get` landed the bundle; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "bundle get not done yet; retrying:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The client config: the fixture chain as the governance chain, and one
/// payment chain (the same chain) that lists `pools` in that order.
fn client_config(chain: &ChainFixture, keystore: &Path, pools: [Address; 2]) -> String {
    let rpc_url = chain.rpc_url();
    let chain_id = chain.chain_id();
    let addrs = chain.addrs();
    format!(
        r#"[blockchain]
rpc_url = "{rpc_url}"
chain_id = {chain_id}
eth_keystore = '{keystore}'
capacity_bond_address = "{capacity_bond}"
slash_judge_address = "{slash_judge}"
buyer_working_deposit_micro_usdc = {DEPOSIT_MICRO_USDC}

[[blockchain.payment_chains]]
rpc_url = "{rpc_url}"
chain_id = {chain_id}
payment_pools = ["{first}", "{second}"]
"#,
        keystore = keystore.display(),
        capacity_bond = addrs.capacity_bond,
        slash_judge = addrs.slash_judge,
        first = pools[0],
        second = pools[1],
    )
}

/// The `decdn get` argv (after the `get` subcommand) to pull the bundle whose
/// manifest has hash `manifest` via auto-discovery, with every chain
/// coordinate from the config file.
fn bundle_get_argv(manifest: &Hash, out_dir: &Path, data_dir: &Path) -> Vec<String> {
    vec![
        format!("decdn:{}/", manifest.to_hex()),
        "-o".into(),
        out_dir.display().to_string(),
        "--jobs".into(),
        "2".into(),
        "--data-dir".into(),
        data_dir.display().to_string(),
    ]
}
