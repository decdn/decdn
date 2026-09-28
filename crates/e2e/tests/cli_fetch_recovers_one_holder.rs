//! Live anvil-backed e2e: the sole holder of a blob restarts mid-transfer and
//! `decdn fetch` still finishes, without a rerun (#1174).
//!
//! The rewritten fetch path runs one `acquire` loop over a `SourceSet`: a
//! source that stops answering cools down (2s doubling to 60s) rather than
//! being dropped, so a holder that comes back mid-fetch is simply usable again
//! on its next poll. This journey drives the real `decdn` binary against a
//! real node to prove that end to end: kill the node the CLI is mid-stream
//! against, bring it back up a few seconds later (same data dir, so it still
//! holds the blob), and the in-flight `decdn fetch` process completes with the
//! correct bytes rather than erroring out or hanging past its give-up bound.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of the `decdn` binary:
//!
//! ```bash
//! cargo build -p decdn-cli
//! cargo nextest run -p decdn-e2e --features anvil-e2e a_restarted_sole_holder_finishes_the_fetch
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
use decdn_cache::Hash;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "restart-e2e-password";
/// The node's restart happens well inside a single cooldown cycle (2s doubling
/// to 60s), so the fetch never has to wait out a long backoff. This is
/// generous but still well under the [`decdn_e2e::timeout::HEAVY`] ceiling
/// below.
const GIVE_UP_AFTER_SECS: u64 = 240;
/// Standard journey tier is 300s; a 96 MiB transfer plus a node restart and a
/// bounded cooldown wait can crowd that under CI contention, so this journey
/// takes the heavier tier. See [`decdn_e2e::timeout`] for the tier rule.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::HEAVY;

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_sole_holder_finishes_the_fetch() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("restarted-sole-holder fetch e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // Large enough that stopping the node partway through a stream lands
    // solidly mid-transfer rather than racing completion.
    let blob = vec![0x5Au8; 96 * 1024 * 1024];
    let blob_hash = Hash::new(&blob);
    let (node, hash) = NodeFixture::launch(&chain, "DE", &blob).await?;
    anyhow::ensure!(
        hash == blob_hash,
        "seeded blob hash mismatch: {hash} vs {blob_hash}"
    );

    // Funded buyer with an on-disk keystore under a `0o700` client data dir
    // (the `RedbBuyerPoolStore` the CLI opens enforces the mode; `tempdir` is
    // `0o755`).
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

    let out = client_dir.path().join("blob.bin");
    let partial = client_dir.path().join("blob.bin.partial");
    let mut args = fetch_argv(
        &chain,
        &node,
        &blob_hash,
        client_dir.path(),
        &keystore,
        &out,
    );
    args.push("--give-up-after-secs".into());
    args.push(GIVE_UP_AFTER_SECS.to_string());

    let mut cmd =
        tokio::process::Command::from(decdn_command(client_dir.path(), KEYSTORE_PASSWORD)?);
    cmd.arg("fetch").args(&args).kill_on_drop(true);
    let fetch = tokio::spawn(async move { cmd.output().await });

    // Let the transfer get solidly underway before pulling the rug: wait for
    // the `.partial` scratch file to pass 8 MiB (well short of the 96 MiB
    // total), so the restart lands mid-stream rather than racing the open.
    wait_for_partial_bytes(&partial, 8 * 1024 * 1024, Duration::from_secs(60)).await?;

    node.stop().context("stop the sole holder mid-transfer")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    node.restart()
        .await
        .context("restart the sole holder (same data dir, still holds the blob)")?;

    let output = tokio::time::timeout(Duration::from_secs(300), fetch)
        .await
        .context("decdn fetch did not finish within 300s of the node coming back")?
        .context("decdn fetch task panicked")?
        .context("spawn/await decdn fetch")?;
    anyhow::ensure!(
        output.status.success(),
        "decdn fetch did not recover from the node restart; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let got = std::fs::read(&out).context("read fetch output")?;
    anyhow::ensure!(
        got == blob,
        "fetch output did not match the served blob: got {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        !partial.exists(),
        "the .partial scratch file must be promoted away, not left beside --output: {}",
        partial.display()
    );

    drop(node);
    Ok(())
}

/// Poll `partial`'s size every 200ms until it is at least `min_bytes`, or fail
/// once `deadline` elapses. The `.partial` scratch file is the gap-driven
/// fetch's on-disk progress marker (ADR 038's `ClientRangedStore`), so watching
/// it grow is how a test observes "the transfer is underway" without adding
/// any observability to the client itself.
async fn wait_for_partial_bytes(
    partial: &Path,
    min_bytes: u64,
    deadline: Duration,
) -> anyhow::Result<()> {
    let started = tokio::time::Instant::now();
    loop {
        if std::fs::metadata(partial).is_ok_and(|meta| meta.len() >= min_bytes) {
            return Ok(());
        }
        anyhow::ensure!(
            started.elapsed() < deadline,
            "{} never reached {min_bytes} bytes within {deadline:?}",
            partial.display()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The `decdn fetch` argv (after the `fetch` subcommand) to pull `hash` from
/// `node` over the explicit-node path, with chain coordinates as flags so no
/// config file is needed. `--capacity-bond-address` is required: it is the
/// EIP-712 `verifyingContract` the buyer signs its ADR 005 client identity
/// binding against, and the node refuses to serve a paid request that carries no
/// verified binding — even for a blob it already holds.
fn fetch_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: &Hash,
    data_dir: &Path,
    keystore: &Path,
    out: &Path,
) -> Vec<String> {
    vec![
        "--hash".into(),
        hash.to_hex(),
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
        "--working-deposit-micro-usdc".into(),
        DEPOSIT_MICRO_USDC.to_string(),
    ]
}
