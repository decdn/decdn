//! Live anvil-backed e2e for `decdn bundle pull --channel-id` (issue #1481): the
//! publisher-pays *adopt-by-id* path, extended from `decdn fetch` to bundle pull.
//!
//! Shape: deploy the protocol, launch a provider node serving two small blobs,
//! have a buyer open one payment channel on-chain, then `bundle pull` a
//! two-entry manifest with `--channel-id` and NO channel pre-recorded in the
//! client store. The whole bundle must adopt that single channel (hydrating it
//! from chain on the first entry, reusing it for the second) and land both
//! files — proving the `Payment::Adopt` branch pins every entry to the channel's
//! provider and pays them all from it.
//!
//! `--provider-address` is passed too, standing alone as the mismatch guard
//! against the channel's on-chain provider (the #1492 decouple): it is accepted
//! without being the thing that locates the node.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_bundle_pull_channel_id
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

use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use anyhow::Context;
use decdn_cache::Hash;
use decdn_client_pull::buyer_channel::{ensure_allowance, open_channel};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_channel::BuyerChannelStore;
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::voucher_domain;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "bundle-adopt-e2e-password";
const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);

#[tokio::test(flavor = "multi_thread")]
async fn cli_bundle_pull_adopts_one_channel_for_the_whole_bundle() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli bundle pull --channel-id e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's channel \
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

    // Two distinct served blobs → a two-entry bundle. Fetching both on ONE
    // adopted channel is the point: the second entry must reuse the channel the
    // first hydrated, serialized by the per-provider voucher lock.
    let blob_a = b"decdn bundle-pull adopt e2e blob A (#1481)".to_vec();
    let blob_b = b"decdn bundle-pull adopt e2e blob B (#1481) - a second entry".to_vec();
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
    // `RedbBuyerChannelStore` the CLI opens enforces the mode; `tempdir` is
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

    // Open ONE channel on-chain against the node's operator. voucherSigner ZERO =
    // self-signing (the funder signs), and the funder is our buyer whose key is in
    // the keystore — so the channel is adoptable. Crucially we do NOT record it in
    // the client store: `bundle pull --channel-id` must hydrate it from chain.
    let buyer_provider = chain.provider_for(&buyer);
    let pc = PaymentChannel::new(chain.addrs().payment_channel, buyer_provider.clone());
    ensure_allowance(
        &buyer_provider,
        chain.usdc(),
        buyer_addr,
        chain.addrs().payment_channel,
        None,
    )
    .await
    .context("approve PaymentChannel")?;
    let deposit = U256::from(DEPOSIT_MICRO_USDC);
    let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_channel);
    let opened = open_channel(
        &pc,
        Arc::new(buyer.clone()),
        &voucher_dom,
        chain.usdc(),
        buyer_addr,
        provider_addr,
        deposit,
        Address::ZERO,
    )
    .await
    .context("open buyer channel")?;
    let channel_id = opened.state.channel_id;

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
        channel_id,
        &manifest_path,
        &out_dir,
        client_dir.path(),
        &keystore,
    );

    // The node accepts vouchers only once its chain watcher has decoded the
    // `ChannelOpened` event (~500ms poll), so the first run races it. Retry until
    // observation lands, exactly as the fetch e2e does.
    run_bundle_pull_until_ready(client_dir.path(), &args).await?;

    // Both entries landed with the served bytes.
    let got_a = std::fs::read(out_dir.join("a.bin")).context("read a.bin")?;
    let got_b = std::fs::read(out_dir.join("b.bin")).context("read b.bin")?;
    anyhow::ensure!(got_a == blob_a, "a.bin mismatch: {} bytes", got_a.len());
    anyhow::ensure!(got_b == blob_b, "b.bin mismatch: {} bytes", got_b.len());

    // The adopted channel was hydrated into the client store and actually paid:
    // its persisted watermark advanced past zero, and it is the SAME channel id we
    // opened (adopt, not a fresh auto-open).
    let store = RedbBuyerChannelStore::open(client_dir.path()).context("reopen client store")?;
    let state = store
        .get_by_channel_id(channel_id)
        .context("read adopted channel")?
        .ok_or_else(|| anyhow::anyhow!("adopted channel {channel_id} was not persisted"))?;
    anyhow::ensure!(
        state.provider == provider_addr,
        "adopted channel provider {} != node operator {provider_addr}",
        state.provider
    );
    anyhow::ensure!(
        state.last_bytes_delivered > U256::ZERO,
        "the adopted channel must have paid for the bundle; watermark did not advance"
    );

    // `NodeFixture` tears the daemon down on drop; there is nothing to await.
    drop(node);
    Ok(())
}

/// Run `decdn bundle pull`, retrying until the node's chain watcher has observed
/// the freshly-opened channel (the CLI has no internal retry for that race).
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
            "bundle pull not ready; retrying after watcher catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The `decdn bundle pull` argv (after the `bundle pull` subcommand) to adopt
/// `channel_id` and pull a local manifest over the explicit-node path, with chain
/// coordinates as flags so no config file is needed. `--capacity-bond-address` is
/// omitted deliberately: `--node-id` locates the node, so the adopt path never
/// reads the registry. `--provider-address` is passed standing alone as the
/// channel-provider mismatch guard (#1492).
fn bundle_pull_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    channel_id: B256,
    manifest: &std::path::Path,
    out_dir: &std::path::Path,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
) -> Vec<String> {
    vec![
        "--channel-id".into(),
        format!("{channel_id:#x}"),
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
        "--payment-channel-address".into(),
        format!("{}", chain.addrs().payment_channel),
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
