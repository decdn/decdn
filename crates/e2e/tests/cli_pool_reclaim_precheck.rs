//! Live anvil-backed e2e for the `decdn pool reclaim --pool` pre-check (#2237).
//!
//! `reclaim --pool` reads the pool and the chain's head time before it unlocks
//! the keystore, and refuses a pool that `reclaim` would revert on with its own
//! cause. The journey runs every refusal with a *wrong* keystore password: a
//! refusal that names its cause, not a keystore error, proves the check ran
//! before the unlock.
//!
//! 1. an id that was never opened → "does not exist";
//! 2. open a real pool → still Open, "run `decdn pool close`";
//! 3. close it → in its dispute window, with the deadline;
//! 4. advance chain time (not the wall clock) past the window → the reclaim
//!    lands, which proves the check compares against chain time;
//! 5. reclaim again → already Closed.
//!
//! No node is involved: these are chain reads and one `reclaim` transaction.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo build -p decdn-cli
//! cargo nextest run -p decdn-e2e --features anvil-e2e -E 'binary(cli_pool_reclaim_precheck)'
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
const KEYSTORE_PASSWORD: &str = "pool-reclaim-precheck-e2e-password";
/// A password that cannot decrypt the keystore: any step that reaches the
/// unlock with it fails on the keystore, not on the pre-check.
const WRONG_PASSWORD: &str = "not-the-keystore-password";
/// Past `PAYMENT_DISPUTE_WINDOW` (48 h in `BaseProtocolDeploy.s.sol`). A longer
/// governed window makes step 4 fail loudly, never pass vacuously.
const PAST_DISPUTE_WINDOW_SECS: u64 = 49 * 3_600;
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn pool_reclaim_refuses_with_its_cause_before_the_keystore_unlock() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("pool reclaim pre-check e2e exceeded the overall timeout")??;
    Ok(())
}

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

    let home = client_dir.path();
    let chain_argv = chain_argv(&chain, home, &keystore);

    // ---- 1. A pool id that was never opened.
    let missing = format!("0x{}", "22".repeat(32));
    let err = reclaim_refused(home, &missing, &chain_argv)?;
    anyhow::ensure!(
        err.contains("does not exist on this PaymentPool contract"),
        "a never-opened pool reads as a zero owner and must be named as missing: {err}"
    );

    // ---- 2. A real pool, still Open.
    let deposit = DEPOSIT_MICRO_USDC.to_string();
    let opened = run_pool(
        home,
        KEYSTORE_PASSWORD,
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
    let err = reclaim_refused(home, &pool_id, &chain_argv)?;
    anyhow::ensure!(
        err.contains("still Open") && err.contains("decdn pool close --pool"),
        "an Open pool must point at `pool close`: {err}"
    );

    // ---- 3. Closed, inside its dispute window.
    run_pool(
        home,
        KEYSTORE_PASSWORD,
        &["close", "--pool", &pool_id],
        &chain_argv,
    )?;
    let err = reclaim_refused(home, &pool_id, &chain_argv)?;
    anyhow::ensure!(
        err.contains("still in its dispute window; reclaimable after Unix"),
        "an in-window pool must name the window and its deadline: {err}"
    );

    // ---- 4. Past the window on chain time only. The wall clock has not moved,
    // so this lands only if the check reads the chain's head time.
    chain.advance_time(PAST_DISPUTE_WINDOW_SECS).await?;
    let reclaimed = run_pool(
        home,
        KEYSTORE_PASSWORD,
        &["reclaim", "--pool", &pool_id],
        &chain_argv,
    )?;
    anyhow::ensure!(
        reclaimed.contains("reclaimed pool"),
        "a pool past its window on chain time must reclaim: {reclaimed}"
    );

    // ---- 5. Already reclaimed.
    let err = reclaim_refused(home, &pool_id, &chain_argv)?;
    anyhow::ensure!(
        err.contains("already Closed"),
        "a reclaimed pool must say so: {err}"
    );
    Ok(())
}

/// Run `decdn pool reclaim --pool <id>` with [`WRONG_PASSWORD`], require it to
/// fail before the keystore unlock, and return its stderr.
fn reclaim_refused(home: &Path, pool_id: &str, chain_argv: &[String]) -> anyhow::Result<String> {
    let out = decdn_command(home, WRONG_PASSWORD)?
        .args(["pool", "reclaim", "--pool", pool_id])
        .args(chain_argv)
        .output()
        .context("run decdn pool reclaim")?;
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    anyhow::ensure!(
        !out.status.success(),
        "`pool reclaim --pool {pool_id}` must be refused: {stderr}"
    );
    anyhow::ensure!(
        !stderr.contains("keystore"),
        "the refusal must come from the pre-check, before the keystore unlock: {stderr}"
    );
    Ok(stderr)
}

/// Run `decdn pool <sub...> <chain flags>` and return stdout, failing loudly.
fn run_pool(
    home: &Path,
    password: &str,
    sub: &[&str],
    chain_argv: &[String],
) -> anyhow::Result<String> {
    let out = decdn_command(home, password)?
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
