//! Live anvil-backed e2e for the unmatched-pattern warning of a real bundle
//! pull (#2672).
//!
//! A real (non-dry-run) `decdn get decdn:<h>/` prints each `--include` or
//! `--exclude` pattern that matched no manifest entry on stderr. The warning
//! goes out through `eprintln!`, so only the real binary shows that it prints
//! at all, and on stderr rather than stdout.
//!
//! The run reaches the filter without a node or a payment:
//!
//! - the chain serves the one read the puller makes while it builds
//!   (`PaymentPool.usdc()`);
//! - `--node-id` and `--provider-address` pin a node that does not exist, so
//!   node selection reads no registry and dials nothing;
//! - a manifest written by hand into the output root's manifest cache stands
//!   in for one a paid pull cached, so the manifest fetch reads the cache;
//! - `--exclude 'metal/*' --exclude '**'` keeps no entry, so the run reports
//!   nothing to fetch and exits 0 before it dials a node or pays.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo build -p decdn-cli
//! cargo nextest run -p decdn-e2e --features anvil-e2e -E 'binary(cli_bundle_pull_unmatched_warning)'
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

use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_client::bundle::BundleManifest;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_incentive::eth_identity;

const KEYSTORE_PASSWORD: &str = "bundle-unmatched-warning-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn a_real_bundle_pull_warns_on_stderr_for_an_unmatched_exclude() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("bundle pull unmatched-warning e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // Buyer keystore under a `0o700` client data dir (the `RedbBuyerPoolStore`
    // the CLI opens enforces the mode; `tempdir` is `0o755`). The run sends no
    // transaction, so the buyer holds no funds.
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
    let home = client_dir.path();

    let out_dir = tempfile::tempdir().context("output tempdir")?;
    let out = out_dir.path();
    let hex = cache_manifest(out, &manifest_json())?;

    let run = decdn_command(home, KEYSTORE_PASSWORD)?
        .arg("get")
        .args(bundle_get_argv(&chain, &hex, out, home, &keystore))
        .args(["--exclude", "metal/*", "--exclude", "**"])
        .output()
        .context("run decdn get")?;
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    anyhow::ensure!(
        run.status.success(),
        "a pull whose filters keep no entry must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );

    // `metal/*` matches neither entry, because a pattern matches the whole
    // path from the bundle root; `**/metal/*` matches the second, so the
    // warning carries the `**/` hint. `**` matches every entry, so it gets no
    // warning.
    anyhow::ensure!(
        stderr.contains("warning: --exclude 'metal/*' matched no entries"),
        "the unmatched exclude must be named on stderr: {stderr}"
    );
    anyhow::ensure!(
        stderr.contains("did you mean '**/metal/*'"),
        "the warning must carry the `**/` hint: {stderr}"
    );
    anyhow::ensure!(
        stderr.matches("matched no entries").count() == 1,
        "only the unmatched pattern gets a warning: {stderr}"
    );
    anyhow::ensure!(
        stdout.contains("no bundle entries match the include/exclude filters; nothing to fetch"),
        "the run must report that the filters keep nothing: {stdout}"
    );

    // The output root holds only the manifest cache: no entry was fetched.
    let names = names_in(out)?;
    anyhow::ensure!(
        names == [".decdn-bundles"],
        "the output root must gain nothing but the manifest cache: {names:?}"
    );
    Ok(())
}

/// A v1 bundle manifest with two entries below `openai-gpt-oss-20b/`, one in
/// its `metal/` directory. The hashes and sizes are placeholders: the run
/// fetches no entry.
fn manifest_json() -> Vec<u8> {
    let entry_hash = format!("b3:{}", blake3::hash(b"entry").to_hex());
    serde_json::json!({
        "version": 1,
        "entries": [
            {"path": "openai-gpt-oss-20b/config.json", "hash": entry_hash, "size": 10},
            {"path": "openai-gpt-oss-20b/metal/model.bin", "hash": entry_hash, "size": 20},
        ],
    })
    .to_string()
    .into_bytes()
}

/// Cache `bytes` as the manifest a pull into `out` fetched, and return their
/// BLAKE3 hex, the hash a `decdn:<hex>/` URI names. A load re-hashes the
/// file, so it must hold `bytes` exactly.
fn cache_manifest(out: &Path, bytes: &[u8]) -> anyhow::Result<String> {
    let hash = blake3::hash(bytes);
    let path = BundleManifest::cache_path(out, *hash.as_bytes());
    let parent = path.parent().context("manifest cache path has a parent")?;
    std::fs::create_dir_all(parent).context("create manifest cache dir")?;
    std::fs::write(&path, bytes).context("write cached manifest")?;
    Ok(hash.to_hex().to_string())
}

/// The names in `dir`, sorted.
fn names_in(dir: &Path) -> anyhow::Result<Vec<String>> {
    let mut names = std::fs::read_dir(dir)
        .with_context(|| format!("read {}", dir.display()))?
        .map(|e| Ok(e?.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

/// The `decdn get` argv (after the `get` subcommand) to pull the bundle whose
/// manifest has hash `hex` into `out`, with the chain coordinates as flags, so
/// no config file is needed. The pinned node is a fresh iroh key with no
/// `--addr`, and the provider a fresh address: neither exists, and the run
/// never reaches either.
fn bundle_get_argv(
    chain: &ChainFixture,
    hex: &str,
    out: &Path,
    data_dir: &Path,
    keystore: &Path,
) -> Vec<String> {
    vec![
        format!("decdn:{hex}/"),
        "-o".into(),
        out.display().to_string(),
        "--node-id".into(),
        iroh::SecretKey::generate().public().to_string(),
        "--provider-address".into(),
        format!("{}", PrivateKeySigner::random().address()),
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
