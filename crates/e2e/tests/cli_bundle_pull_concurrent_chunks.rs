//! Live anvil-backed e2e for `decdn bundle pull` of a **single chunked file**
//! with several DISTINCT chunks, fetched concurrently under the `--jobs`
//! semaphore (default 16) from one node (`appendix-bundles.md` § Chunked
//! files).
//!
//! Shape: one file is the concatenation of four distinct 1 MiB chunks
//! (`c0..c3`, each a different fill byte). The manifest entry lists all four
//! chunk hashes in order; the whole-file `hash` is the BLAKE3 of the
//! concatenation. The node serves all four distinct chunk blobs. `bundle
//! pull`'s per-file fan-out (Task 6) dispatches all four chunk fetches
//! concurrently, bounded by the global `--jobs` semaphore (Task 5) and
//! sharing one voucher ledger per lane (Tasks 1-4).
//!
//! Two things are proven end to end:
//! 1. **Correct concurrent assembly** — the output file's bytes (and BLAKE3)
//!    match the original concatenation, so concurrent chunk fetch +
//!    in-order reassembly + whole-file verification is sound.
//! 2. **Exactly-once payment under concurrency** — the buyer's
//!    `(signer, provider)` lane watermark equals the SUM of the wire bytes of
//!    all four distinct chunks, each counted exactly once. Had concurrent
//!    fetches raced onto the shared ledger incorrectly (double-counting a
//!    chunk, or dropping one), the watermark would not match this sum.
//!
//! **On proving concurrency itself:** this test does not instrument peak
//! in-flight streams. The node's only exposed concurrency observable —
//! `admin_v1_health.in_flight_streams`, backed by the `dispatch_in_flight`
//! gauge — counts *accepted QUIC connections* under the per-connection
//! [`ConnectionLimiter`], not per-stream fetches multiplexed over one
//! connection (`crates/node/src/handlers/client/dispatch.rs`). A single
//! buyer pulling one file's chunks from one node runs them as concurrent
//! streams on what may be a single reused connection, so that gauge would
//! read 1 regardless of whether the chunk fetches overlapped — it cannot
//! distinguish this test's happy path from a serialized one. Adding a
//! per-stream counter to prove it here would mean adding production
//! instrumentation for a test, which the task brief rules out. Concurrency
//! is instead exercised structurally (4 distinct chunks fanned out under
//! `--jobs 16`, the real default), and the deterministic proof that the
//! shared-ledger gate is race-safe under concurrent same-lane pulls lives in
//! the Task 2 unit test `pool_wide_spent_gates_on_other_run_lanes`
//! (`crates/client-pull/src/scheduler.rs`). This test is the end-to-end
//! guard that concurrent chunk delivery assembles correctly and pays exactly
//! once; it does not re-prove the concurrency gate itself.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_bundle_pull_concurrent_chunks
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
const KEYSTORE_PASSWORD: &str = "bundle-concurrent-chunks-e2e-password";
/// Four distinct chunks, ~1 MiB each, concatenated into one file.
const CHUNK_COUNT: usize = 4;
const CHUNK_BYTES: usize = 1024 * 1024;
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn cli_bundle_pull_fetches_chunked_file_chunks_concurrently_and_pays_once()
-> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli bundle pull concurrent-chunks e2e exceeded the overall timeout")??;
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

    // One file, four distinct 1 MiB chunks (different fill bytes so their
    // hashes are all distinct). The whole file is their concatenation.
    let chunks: Vec<Vec<u8>> = (0..CHUNK_COUNT)
        .map(|i| {
            #[allow(
                clippy::cast_possible_truncation,
                reason = "CHUNK_COUNT is a small compile-time constant, well under u8::MAX"
            )]
            let fill = 0x10u8 * (i as u8 + 1);
            vec![fill; CHUNK_BYTES]
        })
        .collect();
    let mut whole_file = Vec::with_capacity(CHUNK_COUNT * CHUNK_BYTES);
    for chunk in &chunks {
        whole_file.extend_from_slice(chunk);
    }

    let chunk_hashes: Vec<Hash> = chunks.iter().map(Hash::new).collect();
    anyhow::ensure!(
        chunk_hashes
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            == CHUNK_COUNT,
        "chunk hashes must all be distinct: {chunk_hashes:?}"
    );
    let whole_hash = Hash::new(&whole_file);

    // The exact wire bytes each whole-blob pull vouchers for: bao content plus
    // its interleaved proof over the 16 KiB chunk-group-aligned whole range
    // (ADR 038). All four chunks are distinct and each served once, so the
    // lane watermark must equal their sum — no chunk counted twice, none
    // skipped, regardless of fetch order or overlap.
    let expected_wire = chunks
        .iter()
        .try_fold(0u64, |acc, c| {
            acc.checked_add(whole_blob_wire_bytes(c.len() as u64))
        })
        .context("aggregate wire-byte overflow")?;

    // Seed the node with the four distinct chunk blobs, in a fixed order.
    let chunk_refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &chunk_refs).await?;
    anyhow::ensure!(
        hashes == chunk_hashes,
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

    // A local v1 manifest with one chunked entry listing all four chunk
    // hashes in order. `-i` means no manifest blob is fetched first.
    let out_dir = client_dir.path().join("out");
    let manifest_path = client_dir.path().join("bundle.json");
    let chunk_entries: String = chunk_hashes
        .iter()
        .map(|h| format!(r#"{{"hash":"b3:{}","size":{CHUNK_BYTES}}}"#, h.to_hex()))
        .collect::<Vec<_>>()
        .join(",");
    let manifest = format!(
        r#"{{"version":1,"entries":[{{"path":"file.bin","hash":"b3:{whole}","size":{size},"chunks":[{chunk_entries}]}}]}}"#,
        whole = whole_hash.to_hex(),
        size = whole_file.len(),
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
    // retry until it lands, exactly as the shared-chunk bundle e2e does.
    let stdout = run_bundle_pull_until_ready(client_dir.path(), &args).await?;
    anyhow::ensure!(
        stdout.contains("downloaded "),
        "expected a `downloaded X` summary line, got:\n{stdout}"
    );

    // The file assembled from its four concurrently-fetched chunks,
    // byte-exact and BLAKE3-exact.
    let got = std::fs::read(out_dir.join("file.bin")).context("read file.bin")?;
    anyhow::ensure!(got == whole_file, "file.bin mismatch: {} bytes", got.len());
    anyhow::ensure!(Hash::new(&got) == whole_hash, "file.bin BLAKE3 mismatch");

    // Exactly-once payment under concurrency: the lane watermark equals the
    // sum of all four distinct chunks' wire bytes. Had any chunk been
    // double-fetched (or double-vouchered) by a race in the shared ledger,
    // the watermark would exceed this sum; had one been skipped, it would
    // fall short.
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
        "lane watermark must equal the sum of all four distinct chunks' wire bytes \
         ({expected_wire}), got {}",
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
) -> anyhow::Result<String> {
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
            return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
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
/// signs its ADR 005 client identity binding against. `--jobs 16` is the real
/// CLI default (unset explicitly here to exercise the actual default path).
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
