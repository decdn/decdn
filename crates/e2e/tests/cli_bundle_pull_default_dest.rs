//! Live anvil-backed e2e for a whole-bundle `decdn get decdn:<hash>/` with no
//! `-o`: the bundle's default destination is its own `./<hash>/` directory,
//! and a pull into it refuses a hidden entry path before any entry lands.
//!
//! Shape: deploy the protocol, launch a provider node serving `model.bin`,
//! `a/.git/config` and the manifest that names both, then run the real `decdn`
//! binary from a fresh current directory:
//!
//! 1. `decdn get decdn:<h>/` with no `-o` pays for the manifest, then refuses
//!    `a/.git/config`. It exits non-zero, and the current directory holds only
//!    `<h>/`, with neither entry in it.
//! 2. The same pull with `--overwrite` is refused the same way: `--overwrite`
//!    lifts only the overwrite guard.
//! 3. `-o <dir>` names the directory, so the pull lands both entries there,
//!    the hidden one included.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`, and `cargo build -p decdn-node -p decdn-cli` first:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e -E 'binary(cli_bundle_pull_default_dest)'
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

use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use alloy::primitives::U256;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "bundle-default-dest-e2e-password";
/// What the refusal names: the hidden entry, quoted.
const REFUSAL: &str = r#"refusing to pull hidden path "a/.git/config""#;
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn a_default_bundle_pull_refuses_a_hidden_path_before_any_entry_lands() -> anyhow::Result<()>
{
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("default-destination bundle pull e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step runs against the directory and pool \
              the previous step left"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    let model = vec![0x4du8; 64 * 1024];
    let git_config = b"[core]\n\thooksPath = /tmp/not-yours\n".to_vec();
    let hash_model = Hash::new(&model);
    let hash_config = Hash::new(&git_config);
    let manifest = format!(
        r#"{{"version":1,"entries":[{{"path":"model.bin","hash":"{}"}},{{"path":"a/.git/config","hash":"{}"}}]}}"#,
        hash_model.to_hex(),
        hash_config.to_hex(),
    )
    .into_bytes();
    let hash_manifest = Hash::new(&manifest);
    let hex = hash_manifest.to_hex();
    let (node, hashes) = NodeFixture::launch_with_blobs(
        &chain,
        "US",
        &[model.as_slice(), git_config.as_slice(), manifest.as_slice()],
    )
    .await?;
    anyhow::ensure!(
        hashes == vec![hash_model, hash_config, hash_manifest],
        "seeded blob hashes mismatch: {hashes:?}"
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
    chain.fund_eth(buyer.address(), 100).await?;
    chain
        .mint_usdc(
            buyer.address(),
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint buyer USDC")?;

    let cwd = tempfile::tempdir().context("current-directory tempdir")?;
    let args = bundle_get_argv(&chain, &node, &hex, client_dir.path(), &keystore);
    let bundle_dir = cwd.path().join(&hex);

    // 1. No `-o`: refused once the manifest names the hidden entry. The first
    // run can race the node's resolution of the pool it opens, so retry until
    // the refusal shows.
    let refused = run_until(client_dir.path(), cwd.path(), &args, |out| {
        String::from_utf8_lossy(&out.stderr).contains(REFUSAL)
    })
    .await?;
    anyhow::ensure!(
        !refused.status.success(),
        "a refused pull must exit non-zero"
    );
    assert_nothing_landed(cwd.path(), &bundle_dir, &hex)?;

    // 2. `--overwrite` does not lift the refusal.
    let mut overwrite = args.clone();
    overwrite.push("--overwrite".into());
    let out = get(client_dir.path(), cwd.path(), &overwrite).await?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    anyhow::ensure!(
        !out.status.success() && stderr.contains(REFUSAL),
        "--overwrite must not lift the hidden-path refusal; stderr:\n{stderr}"
    );
    assert_nothing_landed(cwd.path(), &bundle_dir, &hex)?;

    // 3. An explicit `-o` names the directory, so the hidden entry lands.
    let named = cwd.path().join("x");
    let mut explicit = args.clone();
    explicit.extend(["-o".to_owned(), named.display().to_string()]);
    run_until(client_dir.path(), cwd.path(), &explicit, |out| {
        out.status.success()
    })
    .await?;
    let landed_config =
        std::fs::read(named.join("a/.git/config")).context("read x/a/.git/config")?;
    let landed_model = std::fs::read(named.join("model.bin")).context("read x/model.bin")?;
    anyhow::ensure!(landed_config == git_config, "x/a/.git/config mismatch");
    anyhow::ensure!(landed_model == model, "x/model.bin mismatch");

    // `NodeFixture` tears the daemon down on drop; there is nothing to await.
    drop(node);
    Ok(())
}

/// The refused pulls left the current directory holding only the bundle's
/// own `<hex>/` directory, and no entry inside it.
fn assert_nothing_landed(cwd: &Path, bundle_dir: &Path, hex: &str) -> anyhow::Result<()> {
    let names: Vec<String> = std::fs::read_dir(cwd)
        .context("read the current directory")?
        .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()?;
    anyhow::ensure!(
        names == [hex],
        "the current directory must hold only {hex}/, got {names:?}"
    );
    for entry in ["model.bin", "a"] {
        let path: PathBuf = bundle_dir.join(entry);
        anyhow::ensure!(
            std::fs::symlink_metadata(&path).is_err(),
            "{} landed before the refusal",
            path.display()
        );
    }
    Ok(())
}

/// Run `decdn get <args>` from `cwd`.
async fn get(data_dir: &Path, cwd: &Path, args: &[String]) -> anyhow::Result<Output> {
    let mut cmd = decdn_command(data_dir, KEYSTORE_PASSWORD)?;
    cmd.current_dir(cwd).arg("get").args(args);
    tokio::process::Command::from(cmd)
        .output()
        .await
        .context("spawn decdn get")
}

/// Run `decdn get <args>` from `cwd` until `done` holds for its output,
/// retrying while the node's serve path catches up with the freshly opened
/// pool (the CLI has no internal retry for that race).
async fn run_until(
    data_dir: &Path,
    cwd: &Path,
    args: &[String],
    done: impl Fn(&Output) -> bool,
) -> anyhow::Result<Output> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = get(data_dir, cwd, args).await?;
        if done(&output) {
            return Ok(output);
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn get never reached the expected outcome; last status {}, stderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "bundle get not there yet; retrying after serve-path catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The `decdn get` argv (after the `get` subcommand) to pull the bundle whose
/// manifest has hash `hex` from this one node, with no `-o` and the chain
/// coordinates as flags, so no config file is needed. `--capacity-bond-address`
/// is the EIP-712 `verifyingContract` of the buyer's ADR 005 identity binding,
/// which the node requires on every paid request.
fn bundle_get_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    hex: &str,
    data_dir: &Path,
    keystore: &Path,
) -> Vec<String> {
    vec![
        format!("decdn:{hex}/"),
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
