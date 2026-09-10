//! Live anvil-backed e2e for `decdn bundle pull` of **chunked** bundle entries
//! that share a chunk (`appendix-bundles.md` § Chunked files).
//!
//! Shape: two files share their first 1 MiB and differ only in a 1 KiB tail, so
//! each file is a two-chunk concatenation `[shared, tail]` and the `shared` chunk
//! is one blob named by both entries. The node serves three distinct blobs
//! (`shared`, `tail_a`, `tail_b`). `bundle pull` fetches each distinct chunk once,
//! concatenates each file in order, and verifies the whole-file BLAKE3.
//!
//! Two things are proven:
//! 1. **Correct assembly** — each output file's bytes (and BLAKE3) match the
//!    original, so the concatenation + whole-file verification is sound end to end.
//! 2. **No double-pay for the shared chunk** — the buyer's `(signer, provider)`
//!    lane watermark equals the wire bytes of `shared` + `tail_a` + `tail_b` with
//!    `shared` counted **once**. Had the pull re-fetched the shared chunk for the
//!    second file, the watermark would carry `shared` twice.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_bundle_pull_shared_chunk
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

use alloy::primitives::U256;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_cache::range_pull::{align_range, bao_encoded_size};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity;
use decdn_incentive::lane::LaneKey;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "bundle-shared-chunk-e2e-password";
/// The two files' shared prefix and their differing tails.
const SHARED_BYTES: usize = 1024 * 1024;
const TAIL_BYTES: usize = 1024;
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn cli_bundle_pull_shares_a_chunk_across_two_files() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli bundle pull shared-chunk e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's chain \
              and node state, so decomposing it would thread state through helpers without \
              reducing the journey's length or making it easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // Two files sharing their first 1 MiB, differing only in a 1 KiB tail. Each
    // file is the concatenation `[shared, tail]`; the `shared` chunk is one blob
    // referenced by both entries.
    let shared = vec![0x53u8; SHARED_BYTES];
    let tail_a = vec![0x41u8; TAIL_BYTES];
    let tail_b = vec![0x42u8; TAIL_BYTES];
    let mut file_a = shared.clone();
    file_a.extend_from_slice(&tail_a);
    let mut file_b = shared.clone();
    file_b.extend_from_slice(&tail_b);

    let hash_shared = Hash::new(&shared);
    let hash_tail_a = Hash::new(&tail_a);
    let hash_tail_b = Hash::new(&tail_b);
    let whole_a = Hash::new(&file_a);
    let whole_b = Hash::new(&file_b);

    // The exact wire bytes each whole-blob pull vouchers for: bao content plus its
    // interleaved proof over the 16 KiB chunk-group-aligned whole range (ADR 038).
    // The three distinct chunk blobs are each served once, so the shared lane
    // watermark must equal their sum with `shared` counted a single time.
    let wire_shared = whole_blob_wire_bytes(shared.len() as u64);
    let wire_tail_a = whole_blob_wire_bytes(tail_a.len() as u64);
    let wire_tail_b = whole_blob_wire_bytes(tail_b.len() as u64);
    let expected_wire = wire_shared
        .checked_add(wire_tail_a)
        .and_then(|s| s.checked_add(wire_tail_b))
        .context("aggregate wire-byte overflow")?;
    // The watermark had the shared chunk been (wrongly) fetched twice — the value
    // this test exists to rule out.
    let double_paid_wire = expected_wire
        .checked_add(wire_shared)
        .context("double-paid wire overflow")?;

    // Seed the node with the three distinct chunk blobs, in a fixed order.
    let (node, hashes) = NodeFixture::launch_with_blobs(
        &chain,
        "US",
        &[shared.as_slice(), tail_a.as_slice(), tail_b.as_slice()],
    )
    .await?;
    anyhow::ensure!(
        hashes == vec![hash_shared, hash_tail_a, hash_tail_b],
        "seeded blob hashes mismatch: {hashes:?}"
    );
    let provider_addr = node.operator_addr();

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

    // A local v1 manifest whose two entries are chunked and share the `shared`
    // chunk hash. `-i` means no manifest blob is fetched first.
    let out_dir = client_dir.path().join("out");
    let manifest_path = client_dir.path().join("bundle.json");
    let manifest = format!(
        r#"{{"version":1,"entries":[{{"path":"a.bin","hash":"b3:{whole_a}","size":{size_a},"chunks":[{{"hash":"b3:{shared}","size":{shared_sz}}},{{"hash":"b3:{tail_a}","size":{tail_sz}}}]}},{{"path":"b.bin","hash":"b3:{whole_b}","size":{size_b},"chunks":[{{"hash":"b3:{shared}","size":{shared_sz}}},{{"hash":"b3:{tail_b}","size":{tail_sz}}}]}}]}}"#,
        whole_a = whole_a.to_hex(),
        whole_b = whole_b.to_hex(),
        shared = hash_shared.to_hex(),
        tail_a = hash_tail_a.to_hex(),
        tail_b = hash_tail_b.to_hex(),
        size_a = file_a.len(),
        size_b = file_b.len(),
        shared_sz = SHARED_BYTES,
        tail_sz = TAIL_BYTES,
    );
    std::fs::write(&manifest_path, manifest).context("write local manifest")?;

    let args = bundle_pull_argv(
        &chain,
        &node,
        &manifest_path,
        &out_dir,
        client_dir.path(),
        &keystore,
    );

    // The node serves the freshly-opened pool once its `getPool` view resolves it;
    // retry until it lands, exactly as the pool-id bundle e2e does.
    run_bundle_pull_until_ready(client_dir.path(), &args).await?;

    // Both files assembled from their chunks, byte-exact and BLAKE3-exact.
    let got_a = std::fs::read(out_dir.join("a.bin")).context("read a.bin")?;
    let got_b = std::fs::read(out_dir.join("b.bin")).context("read b.bin")?;
    anyhow::ensure!(got_a == file_a, "a.bin mismatch: {} bytes", got_a.len());
    anyhow::ensure!(got_b == file_b, "b.bin mismatch: {} bytes", got_b.len());
    anyhow::ensure!(Hash::new(&got_a) == whole_a, "a.bin BLAKE3 mismatch");
    anyhow::ensure!(Hash::new(&got_b) == whole_b, "b.bin BLAKE3 mismatch");

    // The shared chunk was fetched — and paid for — exactly once: the lane
    // watermark is the aggregate wire bytes with `shared` counted a single time,
    // not the double-paid value.
    let store = RedbBuyerPoolStore::open(client_dir.path()).context("reopen client store")?;
    let state = store
        .get_by_owner(buyer_addr)
        .context("read buyer pool")?
        .ok_or_else(|| anyhow::anyhow!("no pool was recorded for owner {buyer_addr}"))?;
    let lane = LaneKey {
        pool_id: state.pool_id,
        signer: buyer_addr,
        provider: provider_addr,
    };
    let progress = state
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("no lane progress recorded for provider {provider_addr}"))?;
    anyhow::ensure!(
        progress.last_bytes == U256::from(expected_wire),
        "lane watermark must count the shared chunk once ({expected_wire} bytes), got {} \
         (the double-paid value would be {double_paid_wire})",
        progress.last_bytes,
    );

    // `NodeFixture` tears the daemon down on drop; there is nothing to await.
    drop(node);
    Ok(())
}

/// The exact wire bytes a whole-blob paid pull vouchers for: the bao-encoded size
/// (content plus interleaved proof) of the 16 KiB chunk-group-aligned whole range
/// (ADR 038). This is the quantity the node meters and persists as the lane
/// watermark.
fn whole_blob_wire_bytes(total: u64) -> u64 {
    // `byte_len == 0` means "to the blob end"; the whole range always aligns.
    match align_range(0, 0, total) {
        Ok(aligned) => bao_encoded_size(total, aligned.chunk_ranges()),
        Err(_) => total,
    }
}

/// Run `decdn bundle pull`, retrying until the node's serve path resolves the
/// freshly-opened pool (the CLI has no internal retry for that race).
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
            "bundle pull not ready; retrying after serve-path catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The `decdn bundle pull` argv (after the `bundle pull` subcommand) to pull a
/// local manifest over the explicit-node path, with chain coordinates as flags so
/// no config file is needed. `--provider-address` pins every entry (and every
/// chunk) to this one node and one lane. `--capacity-bond-address` is required
/// even on the explicit-node path: it is the EIP-712 `verifyingContract` the buyer
/// signs its ADR 005 client identity binding against.
fn bundle_pull_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    manifest: &std::path::Path,
    out_dir: &std::path::Path,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
) -> Vec<String> {
    vec![
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
