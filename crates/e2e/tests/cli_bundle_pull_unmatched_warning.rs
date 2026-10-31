//! Live anvil-backed e2e for the unmatched-pattern warning of a real bundle
//! pull (#2672).
//!
//! A real (non-dry-run) `decdn get decdn:<hash>/` prints a `warning:` line on
//! stderr for each `--include` or `--exclude` pattern that matches no manifest
//! entry. The warning goes out through `eprintln!`, which an in-process test
//! cannot capture, so only the real binary shows that it prints at all, and on
//! stderr rather than stdout. `crates/cli/tests/bundle_pull_dryrun.rs` covers
//! the dry-run path.
//!
//! Each run reaches the filter without a node or a payment. In call order:
//!
//! - the run opens the buyer pool store and binds its iroh endpoint, as every
//!   real pull does;
//! - `--node-id` and `--provider-address` pin a node, so node selection reads
//!   no `CapacityBond` registry, which lists no node here. No node runs under
//!   the pinned key, so a dial cannot succeed;
//! - the run unlocks the buyer keystore, but sends no transaction;
//! - the puller's build reads `PaymentPool.usdc()`, the run's one chain read,
//!   so the test needs a chain;
//! - a manifest written by hand into the output root's manifest cache stands
//!   in for one a paid pull cached, so the manifest fetch reads the cache.
//!
//! The filter then runs, and each run ends without dialing a node or paying:
//!
//! 1. `--exclude 'metal/*' --exclude '**'` keeps no entry, so the run reports
//!    nothing to fetch and exits 0.
//! 2. `--exclude 'metal/*'` alone keeps both entries: this is the run that
//!    goes on to pay. `--receipt` names the path one entry lands at, so the
//!    pre-pull guard refuses the run after the filter.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo build -p decdn-cli
//! cargo nextest run -p decdn-e2e --features anvil-e2e -E 'binary(cli_bundle_pull_unmatched_warning)'
//! ```

#![cfg(feature = "anvil-e2e")]

use std::path::Path;
use std::process::Output;
use std::time::Duration;

use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_client::bundle::BundleManifest;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_incentive::eth_identity;

const KEYSTORE_PASSWORD: &str = "bundle-unmatched-warning-e2e-password";
/// The warning for `--exclude 'metal/*'`, which matches no entry.
const UNMATCHED: &str = "warning: --exclude 'metal/*' matched no entries";
/// The hint the warning carries: `**/metal/*` matches an entry.
const HINT: &str = "did you mean '**/metal/*'";
/// What a run whose filters keep no entry prints on stdout.
const NOTHING_TO_FETCH: &str =
    "no bundle entries match the include/exclude filters; nothing to fetch";
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
    // the CLI opens enforces the mode; `tempdir` is `0o755`). The test funds
    // the buyer with nothing: no run sends a transaction.
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
    let argv = bundle_get_argv(&chain, &hex, out, home, &keystore);

    // ---- 1. `**` keeps no entry: the run reports nothing to fetch.
    let emptied = get(home, &argv, &["--exclude", "metal/*", "--exclude", "**"]).await?;
    let stdout = String::from_utf8_lossy(&emptied.stdout);
    let stderr = String::from_utf8_lossy(&emptied.stderr);
    anyhow::ensure!(
        emptied.status.success(),
        "a pull whose filters keep no entry must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );
    ensure_warned_once(&emptied)?;
    anyhow::ensure!(
        stdout.contains(NOTHING_TO_FETCH),
        "the run must report that the filters keep nothing: {stdout}"
    );
    // The run adds nothing to the output root: the manifest cache the test
    // wrote is its one name.
    let names = names_in(out)?;
    anyhow::ensure!(
        names == [".decdn-bundles"],
        "the run must add nothing to the output root: {names:?}"
    );

    // ---- 2. Both entries stay. A `--receipt` at the path `config.json` lands
    // at makes the pre-pull guard refuse the run, after the filter.
    let entry_dir = out.join("openai-gpt-oss-20b");
    // The CLI refuses a `--receipt` whose directory cannot take a file.
    std::fs::create_dir(&entry_dir).context("create the entry directory")?;
    let receipt = entry_dir.join("config.json").display().to_string();
    let kept = get(
        home,
        &argv,
        &["--exclude", "metal/*", "--receipt", receipt.as_str()],
    )
    .await?;
    let stdout = String::from_utf8_lossy(&kept.stdout);
    let stderr = String::from_utf8_lossy(&kept.stderr);
    anyhow::ensure!(
        !kept.status.success(),
        "a `--receipt` at an entry's landed path must be refused\nstdout: {stdout}\nstderr: {stderr}"
    );
    anyhow::ensure!(
        stderr.contains(r#"is where entry "openai-gpt-oss-20b/config.json" lands"#),
        "the run must stop at the receipt guard: {stderr}"
    );
    ensure_warned_once(&kept)?;
    anyhow::ensure!(
        !stdout.contains(NOTHING_TO_FETCH),
        "a run that keeps entries has something to fetch: {stdout}"
    );
    let landed = names_in(&entry_dir)?;
    anyhow::ensure!(landed.is_empty(), "no entry may land: {landed:?}");
    Ok(())
}

/// Run `decdn get <argv> <extra>` as the buyer whose keystore is under `home`.
async fn get(home: &Path, argv: &[String], extra: &[&str]) -> anyhow::Result<Output> {
    let mut cmd = decdn_command(home, KEYSTORE_PASSWORD)?;
    cmd.arg("get").args(argv).args(extra);
    tokio::process::Command::from(cmd)
        .kill_on_drop(true)
        .output()
        .await
        .context("spawn decdn get")
}

/// Require `run` to print the warning for `metal/*`, with its `**/` hint,
/// once on stderr and never on stdout. `metal/*` matches neither entry,
/// because a pattern matches the whole path from the bundle root;
/// `**/metal/*` matches the second, so the warning carries the hint. `**`
/// matches every entry, so it gets no warning.
fn ensure_warned_once(run: &Output) -> anyhow::Result<()> {
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    anyhow::ensure!(
        stderr.contains(UNMATCHED),
        "the unmatched exclude must be named on stderr: {stderr}"
    );
    anyhow::ensure!(
        stderr.contains(HINT),
        "the warning must carry the `**/` hint: {stderr}"
    );
    anyhow::ensure!(
        stderr.matches("matched no entries").count() == 1,
        "only the unmatched pattern gets a warning, and only once: {stderr}"
    );
    anyhow::ensure!(
        !stdout.contains("matched no entries"),
        "the warning must not reach stdout: {stdout}"
    );
    Ok(())
}

/// A v1 bundle manifest with two entries below `openai-gpt-oss-20b/`, one in
/// its `metal/` directory. The hashes and sizes are placeholders: no run
/// fetches an entry.
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
/// BLAKE3 hex, the hash a `decdn:<hash>/` URI names. A load re-hashes the
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
/// `--addr`, and the provider a fresh address: no node runs under that key,
/// and no node registered that provider address.
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
