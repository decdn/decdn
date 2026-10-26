//! Live anvil-backed e2e for pull receipts (#2599): `decdn get --receipt`,
//! `decdn receipt verify` and `decdn receipt settle`.
//!
//! Shape: deploy the protocol and launch a node serving one blob and a
//! two-entry bundle. `decdn get` pulls the blob and then the bundle, each with
//! `--receipt`, and each run ends with the receipt block on stdout. Both files
//! verify offline; a copy with one node signature bit flipped does not. With
//! `-o -` the block goes to stderr and stdout carries only the bytes, for a
//! blob and for one bundle entry, whose manifest and entry fetches build two
//! ledgers on one lane that the receipt merges. A repeat of the bundle pull
//! pays nothing, writes no receipt and removes the earlier file. The node
//! redeems the lane in its own sweep, and `decdn receipt settle` (scanning from
//! the block at the receipt's start) finds the redemption that covers the
//! blob's claim.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`, and a fresh `decdn-node` (nextest does not rebuild it):
//!
//! ```bash
//! cargo build --bin decdn-node --bin decdn
//! cargo nextest run -p decdn-e2e --features anvil-e2e -E 'binary(cli_receipt)'
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
use decdn_client::receipt::{ContentKind, PullReceipt};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "receipt-e2e-password";
const MIB: usize = 1024 * 1024;
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn get_writes_a_receipt_that_verifies_and_settles() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("receipt e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential journey: every pull pays on the one lane, and settle reads the \
              blob's receipt after all of them"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if the `decdn` binary is not built.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // Long enough to meter several chunks of the hash chain and end on a
    // residual voucher.
    let blob = vec![0x5au8; 3 * MIB + 4321];
    let entry_a = vec![0x41u8; 768 * 1024];
    let entry_b = vec![0x42u8; 512 * 1024];
    let (hash, hash_a, hash_b) = (Hash::new(&blob), Hash::new(&entry_a), Hash::new(&entry_b));
    let manifest = format!(
        r#"{{"version":1,"entries":[{{"path":"a.bin","hash":"{}"}},{{"path":"b.bin","hash":"{}"}}]}}"#,
        hash_a.to_hex(),
        hash_b.to_hex(),
    )
    .into_bytes();
    let hash_manifest = Hash::new(&manifest);
    let (node, hashes) = NodeFixture::launch_with_blobs(
        &chain,
        "US",
        &[
            blob.as_slice(),
            entry_a.as_slice(),
            entry_b.as_slice(),
            manifest.as_slice(),
        ],
    )
    .await?;
    anyhow::ensure!(
        hashes == vec![hash, hash_a, hash_b, hash_manifest],
        "seeded blob hashes mismatch: {hashes:?}"
    );

    let dir = private_tempdir()?;
    let home = dir.path();
    eth_identity::generate_and_persist(home, KEYSTORE_PASSWORD, false)
        .context("generate wallet keystore")?;
    let keystore = eth_identity::keystore_path(home);
    let wallet = eth_identity::load_signer(&keystore, KEYSTORE_PASSWORD).context("load wallet")?;
    chain.fund_eth(wallet.address(), 100).await?;
    chain
        .mint_usdc(
            wallet.address(),
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(2u64),
        )
        .await
        .context("mint wallet USDC")?;

    // ---- The blob: the run ends with the receipt block, and the file verifies.
    let blob_receipt = home.join("blob.json");
    let mut args = vec![
        format!("decdn:{}", hash.to_hex()),
        "-o".into(),
        home.join("blob.bin").display().to_string(),
        "--receipt".into(),
        blob_receipt.display().to_string(),
    ];
    args.extend(chain_args(&chain, &node, home, &keystore));
    let stdout = text(&run_until_ok(home, "get", &args).await?.stdout)?;
    anyhow::ensure!(
        std::fs::read(home.join("blob.bin"))? == blob,
        "the blob landed wrong"
    );
    anyhow::ensure!(
        stdout.contains("decdn receipt v1")
            && stdout.contains(&format!("decdn receipt verify {}", blob_receipt.display())),
        "the run did not end with the receipt block:\n{stdout}"
    );
    let receipt = load(&blob_receipt)?;
    anyhow::ensure!(
        receipt.content.complete && receipt.content.kind == ContentKind::Blob,
        "blob receipt content: {:?}",
        receipt.content
    );
    let lane = receipt
        .lanes
        .first()
        .context("the blob receipt has no lane")?;
    anyhow::ensure!(
        receipt.lanes.len() == 1 && lane.provider == node.operator_addr(),
        "one lane to the node, got {:?}",
        receipt.lanes
    );
    anyhow::ensure!(
        lane.proof.is_some() && !lane.responses.is_empty(),
        "the lane carries its node signature and its proof: {lane:?}"
    );
    let blob_claim = lane.proof.as_ref().map_or(0, |p| p.claim.amount);
    anyhow::ensure!(
        receipt.summary().paid.bytes >= blob.len() as u64,
        "the claim pays at least the blob's bytes: {:?}",
        receipt.summary()
    );
    run_ok(
        home,
        "receipt",
        &["verify".into(), blob_receipt.display().to_string()],
    )
    .await?;

    // ---- A flipped bit in the node's signature fails verification, by name.
    let mut tampered = receipt.clone();
    let sig = &mut tampered.lanes[0].responses[0].slash_sig;
    let mut bytes = sig.to_vec();
    bytes[5] ^= 0x01;
    *sig = bytes.into();
    let tampered_path = home.join("tampered.json");
    std::fs::write(&tampered_path, serde_json::to_vec(&tampered)?)?;
    let output = spawn(
        home,
        "receipt",
        &["verify".into(), tampered_path.display().to_string()],
    )
    .await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    anyhow::ensure!(
        !output.status.success() && stderr.contains("is not signed by the node"),
        "a tampered receipt must fail by name; status {}, stderr:\n{stderr}",
        output.status
    );

    // ---- The bundle: the manifest and both entries land on the one lane.
    let bundle_receipt = home.join("bundle.json");
    let mut args = vec![
        format!("decdn:{}/", hash_manifest.to_hex()),
        "-o".into(),
        home.join("bundle").display().to_string(),
        "--receipt".into(),
        bundle_receipt.display().to_string(),
    ];
    args.extend(chain_args(&chain, &node, home, &keystore));
    let stdout = text(&run_until_ok(home, "get", &args).await?.stdout)?;
    anyhow::ensure!(
        stdout.contains("decdn receipt v1") && stdout.contains("(bundle,"),
        "the bundle run did not end with the receipt block:\n{stdout}"
    );
    let bundle = load(&bundle_receipt)?;
    let mut landed: Vec<String> = bundle
        .content
        .entries
        .iter()
        .map(|e| e.hash.to_string())
        .collect();
    landed.sort();
    let mut expected: Vec<String> = [hash_manifest, hash_a, hash_b]
        .iter()
        .map(|h| format!("0x{}", h.to_hex()))
        .collect();
    expected.sort();
    anyhow::ensure!(
        bundle.content.kind == ContentKind::Bundle && landed == expected,
        "the bundle receipt lists the manifest and both entries: {:?}",
        bundle.content
    );
    anyhow::ensure!(
        bundle.lanes.len() == 1 && bundle.lanes[0].prior.amount >= blob_claim,
        "the bundle pays on from the blob's claim: {:?}",
        bundle.lanes
    );
    run_ok(
        home,
        "receipt",
        &["verify".into(), bundle_receipt.display().to_string()],
    )
    .await?;

    // ---- A dry run previews and writes nothing: the receipt stays.
    let mut dry = args.clone();
    dry.push("--dry-run".into());
    run_ok(home, "get", &dry).await?;
    anyhow::ensure!(
        load(&bundle_receipt)? == bundle,
        "a dry run must leave the receipt as it is"
    );

    // ---- A receipt path an entry lands at is refused before any entry is
    // fetched, and the entry stays.
    let entry_path = home.join("bundle").join("a.bin");
    let mut colliding = args.clone();
    let at = colliding
        .iter()
        .position(|a| a == "--receipt")
        .context("no --receipt")?;
    colliding[at + 1] = entry_path.display().to_string();
    let output = spawn(home, "get", &colliding).await?;
    let stderr = text(&output.stderr)?;
    anyhow::ensure!(
        !output.status.success() && stderr.contains("is where entry"),
        "a receipt over an entry must be refused; stderr:\n{stderr}"
    );
    anyhow::ensure!(
        std::fs::read(&entry_path)? == entry_a,
        "the refused run left the entry alone"
    );

    // ---- A repeat of the bundle pull pays nothing: no receipt, and the
    // earlier file is gone so nothing verifies it by mistake.
    let output = run_until_ok(home, "get", &args).await?;
    let stderr = text(&output.stderr)?;
    anyhow::ensure!(
        !bundle_receipt.exists() && stderr.contains("no receipt: this run paid nothing"),
        "a run that paid nothing must say so and remove the earlier receipt; stderr:\n{stderr}"
    );

    // ---- A receipt path that names the blob's own destination is refused
    // before anything is paid.
    let mut colliding = vec![
        format!("decdn:{}", hash.to_hex()),
        "-o".into(),
        home.join("blob.bin").display().to_string(),
        "--receipt".into(),
        home.join(".").join("blob.bin").display().to_string(),
    ];
    colliding.extend(chain_args(&chain, &node, home, &keystore));
    let output = spawn(home, "get", &colliding).await?;
    anyhow::ensure!(
        !output.status.success() && text(&output.stderr)?.contains("is where the blob lands"),
        "a receipt over the blob must be refused"
    );
    anyhow::ensure!(
        std::fs::read(home.join("blob.bin"))? == blob,
        "the refused run left the blob alone"
    );

    // ---- `-o -`: stdout carries exactly the bytes; the block goes to stderr.
    let stream_receipt = home.join("stream.json");
    let mut args = vec![
        format!("decdn:{}", hash.to_hex()),
        "-o".into(),
        "-".into(),
        "--receipt".into(),
        stream_receipt.display().to_string(),
    ];
    args.extend(chain_args(&chain, &node, home, &keystore));
    let output = run_until_ok(home, "get", &args).await?;
    anyhow::ensure!(output.stdout == blob, "stdout must be the blob alone");
    anyhow::ensure!(
        text(&output.stderr)?.contains("decdn receipt v1"),
        "the block goes to stderr under `-o -`"
    );
    run_ok(
        home,
        "receipt",
        &["verify".into(), stream_receipt.display().to_string()],
    )
    .await?;

    // ---- One entry to stdout: the manifest fetch and the entry fetch each
    // build a ledger on the one lane, and the receipt merges them.
    let entry_receipt = home.join("entry.json");
    let mut args = vec![
        format!("decdn:{}/a.bin", hash_manifest.to_hex()),
        "-o".into(),
        "-".into(),
        "--receipt".into(),
        entry_receipt.display().to_string(),
    ];
    args.extend(chain_args(&chain, &node, home, &keystore));
    let output = run_until_ok(home, "get", &args).await?;
    anyhow::ensure!(output.stdout == entry_a, "stdout must be the entry alone");
    let entry = load(&entry_receipt)?;
    let mut landed: Vec<String> = entry
        .content
        .entries
        .iter()
        .map(|e| e.hash.to_string())
        .collect();
    landed.sort();
    let mut expected: Vec<String> = [hash_manifest, hash_a]
        .iter()
        .map(|h| format!("0x{}", h.to_hex()))
        .collect();
    expected.sort();
    anyhow::ensure!(
        entry.content.kind == ContentKind::Bundle
            && entry.content.hash.to_string() == format!("0x{}", hash_manifest.to_hex())
            && landed == expected
            && entry.lanes.len() == 1
            && entry.lanes[0].responses.len() == 2,
        "the entry receipt merges both fetches into one lane: {entry:?}"
    );
    run_ok(
        home,
        "receipt",
        &["verify".into(), entry_receipt.display().to_string()],
    )
    .await?;

    // ---- Settlement: the node redeems in its own sweep; settle finds it,
    // scanning from the block at the receipt's start.
    let settle_args = vec![
        "settle".into(),
        blob_receipt.display().to_string(),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--json".into(),
    ];
    let settled = poll(Duration::from_secs(120), || async {
        let stdout = run_ok(home, "receipt", &settle_args).await?;
        let receipt: PullReceipt = serde_json::from_str(stdout.trim())?;
        Ok((receipt.summary().settled_lanes == 1).then_some(receipt))
    })
    .await?
    .context("the blob's lane was never redeemed up to its claim")?;
    let settlement = settled.lanes[0].settlement.context("no settlement")?;
    anyhow::ensure!(
        settlement.new_paid_cumulative >= blob_claim && settled.lanes[0].is_settled(),
        "the settlement covers the claim: {settlement:?}"
    );
    anyhow::ensure!(
        load(&blob_receipt)? == settled,
        "settle rewrites the file with what it prints"
    );
    // A settled receipt still verifies: settlement is not a signed field.
    run_ok(
        home,
        "receipt",
        &["verify".into(), blob_receipt.display().to_string()],
    )
    .await?;

    drop(node);
    Ok(())
}

fn load(path: &Path) -> anyhow::Result<PullReceipt> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))
}

/// The chain and node flags every `decdn get` here takes: pinned to the one
/// node and provider, so every pull pays on one lane, with chain coordinates as
/// flags so no config file is needed.
fn chain_args(
    chain: &ChainFixture,
    node: &NodeFixture,
    data_dir: &Path,
    keystore: &Path,
) -> Vec<String> {
    vec![
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

fn text(bytes: &[u8]) -> anyhow::Result<String> {
    Ok(String::from_utf8(bytes.to_vec())?)
}

/// Run `decdn <command> <args>` once, failing with its stderr; returns stdout.
async fn run_ok(home: &Path, command: &str, args: &[String]) -> anyhow::Result<String> {
    let output = spawn(home, command, args).await?;
    anyhow::ensure!(
        output.status.success(),
        "decdn {command} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    text(&output.stdout)
}

/// Run `decdn <command> <args>`, retrying until the node's serve path resolves
/// the freshly-opened pool (the CLI has no internal retry for that race).
async fn run_until_ok(
    home: &Path,
    command: &str,
    args: &[String],
) -> anyhow::Result<std::process::Output> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = spawn(home, command, args).await?;
        if output.status.success() {
            return Ok(output);
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn {command} never succeeded; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "{command} not ready; retrying after serve-path catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

async fn spawn(
    home: &Path,
    command: &str,
    args: &[String],
) -> anyhow::Result<std::process::Output> {
    tokio::process::Command::from(decdn_command(home, KEYSTORE_PASSWORD)?)
        .arg(command)
        .args(args)
        .output()
        .await
        .with_context(|| format!("spawn decdn {command}"))
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
