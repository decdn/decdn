//! Live anvil-backed e2e for `decdn pool list --all` (#2077).
//!
//! The gap this closes is not "a second way to list pools" — it is that the
//! store-backed listing reads the one file a reset `identity.data_dir` loses,
//! so the operator whose deposit went missing gets `pools=0` from the command
//! they reach for first (#2072). So the journey deliberately destroys the local
//! record and then asks again:
//!
//! 1. open a real pool on chain with `decdn pool open`;
//! 2. `pool list` and `pool list --all` agree, and `--all` marks it `TRACKED`;
//! 3. delete the client store — the reset, in one line;
//! 4. `pool list` now shows nothing, which is the defect;
//! 5. `pool list --all` still shows the pool, its deposit, and `tracked=false`.
//!
//! No node is involved: this is a chain read and a store read, nothing paid.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo build -p decdn-cli
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_pool_list_all
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

use alloy::primitives::U256;
use anyhow::Context;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "pool-list-all-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn pool_list_all_shows_a_pool_the_local_store_has_lost() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("pool list --all e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's chain \
              and store state, so decomposing it would thread state through helpers without \
              making the journey easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

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
    chain.fund_eth(buyer.address(), 100).await?;
    chain
        .mint_usdc(buyer.address(), U256::from(DEPOSIT_MICRO_USDC))
        .await
        .context("mint buyer USDC")?;

    let chain_argv = chain_argv(&chain, client_dir.path(), &keystore);

    // ---- 1. A real pool, opened on chain and recorded locally.
    let deposit = DEPOSIT_MICRO_USDC.to_string();
    let opened = run_pool(
        client_dir.path(),
        &["open", "--deposit-micro-usdc", &deposit],
        &chain_argv,
    )?;
    let pool_id: String = serde_json::from_str::<serde_json::Value>(&opened)
        .ok()
        .and_then(|v| v["poolId"].as_str().map(str::to_owned))
        .or_else(|| {
            opened
                .split_whitespace()
                .find(|t| t.starts_with("0x") && t.len() == 66)
                .map(str::to_owned)
        })
        .with_context(|| format!("no poolId in `pool open` output: {opened}"))?;

    // ---- 2. Both views agree while the store is intact.
    let local = run_pool(client_dir.path(), &["list"], &chain_argv)?;
    anyhow::ensure!(
        local.contains("pools=1"),
        "the store-backed listing must see the pool it just recorded: {local}"
    );

    let all: serde_json::Value = serde_json::from_str(&run_pool(
        client_dir.path(),
        &["list", "--all", "--json"],
        &chain_argv,
    )?)
    .context("parse pool list --all --json")?;
    anyhow::ensure!(all["source"] == "chain", "wrong source: {all}");
    anyhow::ensure!(
        all["pools"].as_array().map(Vec::len) == Some(1),
        "the chain must report exactly the one opened pool: {all}"
    );
    let row = &all["pools"][0];
    anyhow::ensure!(row["pool_id"] == pool_id.as_str(), "wrong pool: {row}");
    anyhow::ensure!(row["status"] == "open", "a fresh pool is Open: {row}");
    anyhow::ensure!(
        row["deposit_micro_usdc"] == DEPOSIT_MICRO_USDC.to_string().as_str(),
        "the chain's deposit must be what was escrowed: {row}"
    );
    anyhow::ensure!(
        row["tracked"] == serde_json::Value::Bool(true),
        "an intact store tracks the pool it recorded: {row}"
    );

    // ---- 3. The reset: the store is exactly what a moved volume loses.
    let store_file = client_dir
        .path()
        .join(decdn_common::data_dir::CLIENT_BUYER_DB_FILE);
    std::fs::remove_file(&store_file).context("delete the client buyer store")?;

    // ---- 4. The defect, reproduced: the deposit is still escrowed on chain,
    // and the command an operator reaches for first reports nothing.
    let after_reset = run_pool(client_dir.path(), &["list"], &chain_argv)?;
    anyhow::ensure!(
        after_reset.contains("pools=0"),
        "a lost store must list nothing — that is the situation --all exists \
         for: {after_reset}"
    );

    // ---- 5. `--all` still has the answer, and says the store does not.
    // The store-backed listing above re-created the file it reads (that is the
    // writable client path, unchanged); drop it again so what follows is about
    // `--all` alone, which must never need the file to exist.
    std::fs::remove_file(&store_file).context("delete the re-created client store")?;
    let all: serde_json::Value = serde_json::from_str(&run_pool(
        client_dir.path(),
        &["list", "--all", "--json"],
        &chain_argv,
    )?)
    .context("parse pool list --all --json after reset")?;
    let row = &all["pools"][0];
    anyhow::ensure!(row["pool_id"] == pool_id.as_str(), "wrong pool: {row}");
    anyhow::ensure!(
        row["deposit_micro_usdc"] == DEPOSIT_MICRO_USDC.to_string().as_str(),
        "the escrowed deposit survives the reset: {row}"
    );
    anyhow::ensure!(
        row["tracked"] == serde_json::Value::Bool(false),
        "no store file means nothing is tracked, and --all must say so: {row}"
    );
    anyhow::ensure!(
        all["local_store_read"] == serde_json::Value::Bool(true),
        "an absent store is a read answer — nothing is tracked — not an \
         unreadable one: {all}"
    );
    anyhow::ensure!(
        !store_file.exists(),
        "--all must not re-create the store it reports on"
    );

    // And the human-readable table carries the same facts.
    let table = run_pool(client_dir.path(), &["list", "--all"], &chain_argv)?;
    anyhow::ensure!(table.contains("pools=1"), "{table}");
    anyhow::ensure!(
        table.contains("local_store=read"),
        "the table must say whether the local record answered: {table}"
    );
    anyhow::ensure!(
        table.contains("open") && table.contains("10.000000"),
        "the table must show the state and the deposit: {table}"
    );

    // ---- 6. A store that exists but will not open is a different answer from
    // one that is absent, and the two must not render alike: the first is
    // "unknown, go look", the second is "nothing is tracked".
    std::fs::write(&store_file, b"not a redb file").context("write a broken store")?;
    let broken: serde_json::Value = serde_json::from_str(&run_pool(
        client_dir.path(),
        &["list", "--all", "--json"],
        &chain_argv,
    )?)
    .context("parse pool list --all --json over a broken store")?;
    anyhow::ensure!(
        broken["local_store_read"] == serde_json::Value::Bool(false),
        "an unreadable store must not report as read: {broken}"
    );
    anyhow::ensure!(
        broken["pools"][0]["tracked"] == serde_json::Value::Null,
        "an unreadable store answers for no pool: {broken}"
    );
    let broken_table = run_pool(client_dir.path(), &["list", "--all"], &chain_argv)?;
    anyhow::ensure!(
        broken_table.contains("local_store=unreadable"),
        "the table must carry the distinction, not only --json: {broken_table}"
    );
    Ok(())
}

/// Run `decdn pool <sub...> <chain flags>` and return stdout, failing loudly.
fn run_pool(home: &Path, sub: &[&str], chain_argv: &[String]) -> anyhow::Result<String> {
    let out = decdn_command(home, KEYSTORE_PASSWORD)?
        .arg("pool")
        .args(sub)
        .args(chain_argv)
        .output()
        .context("run decdn pool")?;
    anyhow::ensure!(
        out.status.success(),
        "`decdn pool {}` failed: {}",
        sub.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The chain + store coordinates every `pool` subcommand here shares.
fn chain_argv(chain: &ChainFixture, data_dir: &Path, keystore: &Path) -> Vec<String> {
    vec![
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
    ]
}
