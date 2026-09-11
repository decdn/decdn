//! Live anvil-backed e2e for `decdn bundle pull`'s **range-dedup** path
//! (bundle-chunk-hints plan, task 7): an optimized bundle whose two files
//! share a large byte run dedups the shared range — splices it locally from
//! the first file's already-materialized blob — instead of re-downloading
//! (and re-paying for) it from the node.
//!
//! Shape: two files share their first 4 MiB, chunk-group-aligned, and differ
//! only in a distinct tail. Each file is stored as ONE whole-file blob (the
//! current `origin import --optimize` model: chunks are manifest-only dedup
//! hints, never separately stored blobs), so the node here is seeded with the
//! two whole files directly via [`NodeFixture::launch_with_blobs`] — exactly
//! what an `--optimize` import would have produced on disk. The manifest is
//! hand-built (mirrors the deleted `cli_bundle_pull_shared_chunk.rs`'s
//! approach) with each entry's `chunks` hints naming the shared run and each
//! file's distinct tail as separate BLAKE3-addressed spans.
//!
//! What this test proves:
//! 1. **Correct assembly** — both output files are byte-exact and whole-file
//!    BLAKE3-exact against the originals, so splice-then-verify is sound.
//! 2. **Dedup happened** — pulling `[a.bin, b.bin]` in one `--jobs 1`
//!    (sequential) invocation bills the shared lane exactly `a`'s whole-file
//!    wire bytes plus `b`'s COMPLEMENT wire bytes (the tail only) — strictly
//!    less than the sum of both files' whole-file wire sizes. `--jobs 1`
//!    guarantees `a` finalizes (and registers its chunks) before `b` starts,
//!    so `b` is the one whose shared run gets spliced rather than paid for.
//! 3. **No-overlap parity** — a lone, non-overlapping file `c.bin` (no
//!    `chunks` hints) bills exactly its whole-file wire size on the same
//!    lane, matching a plain (non-dedup) pull.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_bundle_pull_range_dedup
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units,
    clippy::similar_names
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

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "bundle-range-dedup-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// The two files' shared prefix: several MiB, an exact multiple of the 16 KiB
/// bao chunk-group size (`decdn_bao_range::IROH_BLOCK_SIZE`) so it survives
/// inward alignment intact (no partial group shaved off either edge).
const SHARED_BYTES: u64 = 4 * 1024 * 1024;
/// Each file's distinct tail. Deliberately NOT chunk-group-aligned, so this
/// journey also proves a ragged complement is handled correctly.
const TAIL_BYTES: u64 = 200_000 + 123;
/// The lone no-overlap file used for the parity assertion.
const LONE_BYTES: u64 = 300_000;

#[tokio::test(flavor = "multi_thread")]
async fn cli_bundle_pull_dedups_an_overlapping_range() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli bundle pull range-dedup e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's chain \
              and lane state, so decomposing it would thread state through helpers without \
              reducing the journey's length or making it easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // The shared run and each file's distinct tail, all deterministic
    // pseudo-random bytes (never a constant fill — a real BLAKE3 exercise,
    // not an accidental all-zeros/all-same-byte degenerate).
    let shared = deterministic_bytes(SHARED_BYTES, 0x5EED_0001);
    let tail_a = deterministic_bytes(TAIL_BYTES, 0x5EED_00A1);
    let tail_b = deterministic_bytes(TAIL_BYTES, 0x5EED_00B2);
    let lone = deterministic_bytes(LONE_BYTES, 0x5EED_0C3C);

    let mut file_a = shared.clone();
    file_a.extend_from_slice(&tail_a);
    let mut file_b = shared.clone();
    file_b.extend_from_slice(&tail_b);

    let hash_shared = Hash::new(&shared);
    let hash_tail_a = Hash::new(&tail_a);
    let hash_tail_b = Hash::new(&tail_b);
    let whole_a = Hash::new(&file_a);
    let whole_b = Hash::new(&file_b);
    let hash_lone = Hash::new(&lone);

    // The node is seeded with each file as ONE whole-file blob — exactly what
    // `origin import --optimize` stores; chunk hints never name separately
    // stored blobs. `lone` is a third, non-overlapping blob for the parity
    // assertion.
    let (node, hashes) = NodeFixture::launch_with_blobs(
        &chain,
        "US",
        &[file_a.as_slice(), file_b.as_slice(), lone.as_slice()],
    )
    .await?;
    anyhow::ensure!(
        hashes == vec![whole_a, whole_b, hash_lone],
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
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(6u64),
        )
        .await
        .context("mint buyer USDC")?;

    let out_dir = client_dir.path().join("out");

    // --- Step 1: no-overlap parity -----------------------------------------
    // A lone file with no `chunks` hints pulled by itself must bill EXACTLY its
    // whole-file wire size on the lane — the same quantity a plain (non-dedup)
    // pull always bills. This is the baseline the dedup step below is measured
    // against.
    let lone_manifest_path = client_dir.path().join("lone.json");
    std::fs::write(
        &lone_manifest_path,
        format!(
            r#"{{"version":1,"entries":[{{"path":"lone.bin","hash":"{}"}}]}}"#,
            hash_lone.to_hex(),
        ),
    )
    .context("write lone manifest")?;
    let lone_args = bundle_pull_argv(
        &chain,
        &node,
        &lone_manifest_path,
        &out_dir,
        client_dir.path(),
        &keystore,
        1,
    );

    let before_lone = billed_bytes(client_dir.path(), provider_addr)?;
    anyhow::ensure!(
        before_lone == 0,
        "lane must be unbilled before the first pull"
    );
    run_bundle_pull_until_ready(client_dir.path(), &lone_args).await?;
    let paid_lone = billed_bytes(client_dir.path(), provider_addr)?.saturating_sub(before_lone);
    let wire_lone = whole_blob_wire_bytes(LONE_BYTES);
    anyhow::ensure!(
        paid_lone == wire_lone,
        "a lone no-overlap file must bill exactly its whole-file wire size: got {paid_lone}, \
         expected {wire_lone}"
    );
    let got_lone = std::fs::read(out_dir.join("lone.bin")).context("read lone.bin")?;
    anyhow::ensure!(
        got_lone == lone,
        "lone.bin mismatch: {} bytes",
        got_lone.len()
    );

    // --- Step 2: dedup across two hint-carrying entries ---------------------
    // `a.bin` and `b.bin` share the `shared` chunk hash (identical bytes); each
    // also carries a distinct-tail chunk. `--jobs 1` forces sequential
    // processing so `a` fully finalizes (and registers its chunks) before `b`
    // starts — `b` is then the one whose shared run gets spliced.
    let ab_manifest_path = client_dir.path().join("ab.json");
    let ab_manifest = format!(
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
    std::fs::write(&ab_manifest_path, ab_manifest).context("write a/b manifest")?;
    let ab_args = bundle_pull_argv(
        &chain,
        &node,
        &ab_manifest_path,
        &out_dir,
        client_dir.path(),
        &keystore,
        1,
    );

    let before_ab = billed_bytes(client_dir.path(), provider_addr)?;
    run_bundle_pull_until_ready(client_dir.path(), &ab_args).await?;
    let paid_ab = billed_bytes(client_dir.path(), provider_addr)?.saturating_sub(before_ab);

    // (1) Byte-exact + whole-file-BLAKE3-exact outputs.
    let got_a = std::fs::read(out_dir.join("a.bin")).context("read a.bin")?;
    let got_b = std::fs::read(out_dir.join("b.bin")).context("read b.bin")?;
    anyhow::ensure!(got_a == file_a, "a.bin mismatch: {} bytes", got_a.len());
    anyhow::ensure!(got_b == file_b, "b.bin mismatch: {} bytes", got_b.len());
    anyhow::ensure!(Hash::new(&got_a) == whole_a, "a.bin BLAKE3 mismatch");
    anyhow::ensure!(Hash::new(&got_b) == whole_b, "b.bin BLAKE3 mismatch");

    // (2) Dedup happened: the shared lane paid `a`'s whole file plus only `b`'s
    // COMPLEMENT (the tail past the shared, chunk-group-aligned run) — not both
    // files' whole-file wire sizes.
    let wire_a_whole = whole_blob_wire_bytes(file_a.len() as u64);
    let wire_b_whole = whole_blob_wire_bytes(file_b.len() as u64);
    let wire_b_complement = range_wire_bytes(SHARED_BYTES, TAIL_BYTES, file_b.len() as u64)?;
    let expected_paid = wire_a_whole
        .checked_add(wire_b_complement)
        .context("expected-paid overflow")?;
    let no_dedup_total = wire_a_whole
        .checked_add(wire_b_whole)
        .context("no-dedup total overflow")?;
    anyhow::ensure!(
        paid_ab == expected_paid,
        "shared lane must bill exactly a's whole file ({wire_a_whole}) plus b's complement \
         ({wire_b_complement}) = {expected_paid}, got {paid_ab}"
    );
    anyhow::ensure!(
        paid_ab < no_dedup_total,
        "dedup must strictly undercut re-downloading both whole files: paid {paid_ab} is not \
         less than the no-dedup total {no_dedup_total}"
    );

    // `NodeFixture` tears the daemon down on drop; there is nothing to await.
    drop(node);
    Ok(())
}

/// Deterministic pseudo-random bytes (xorshift32), seeded distinctly per
/// caller so every blob in this journey is content-distinct except where the
/// test explicitly shares bytes (the `shared` run reused verbatim in both
/// files).
fn deterministic_bytes(len: u64, seed: u32) -> Vec<u8> {
    let len = usize::try_from(len).unwrap_or(0);
    let mut v = vec![0u8; len];
    let mut x = seed;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

/// The exact wire bytes a whole-blob paid pull vouchers for: the bao-encoded
/// size (content plus interleaved proof) of the 16 KiB chunk-group-aligned
/// whole range (ADR 038) — the quantity the node meters and persists as the
/// lane watermark.
fn whole_blob_wire_bytes(total: u64) -> u64 {
    // `byte_len == 0` means "to the blob end"; the whole range always aligns.
    match align_range(0, 0, total) {
        Ok(aligned) => bao_encoded_size(total, aligned.chunk_ranges()),
        Err(_) => total,
    }
}

/// The exact wire bytes a driven `[offset, offset + len)` range of a
/// `total`-byte blob vouchers for — the same quantity `bundle_pull`'s
/// `drive_ranges_failover` pays for one complement run. Mirrors
/// `plan_dedup`/`drive`'s per-run accounting: each `(offset, len)` complement
/// run is driven (and billed) independently via its own `align_range`.
fn range_wire_bytes(offset: u64, len: u64, total: u64) -> anyhow::Result<u64> {
    let aligned = align_range(offset, len, total)
        .map_err(|e| anyhow::anyhow!("align_range({offset}, {len}, {total}): {e}"))?;
    Ok(bao_encoded_size(total, aligned.chunk_ranges()))
}

/// Cumulative wire bytes the buyer has paid `provider` for, read from the
/// persisted pool's `(signer, provider)` lane watermark. The store is
/// reopened per call because the CLI subprocess owns it between calls.
fn billed_bytes(
    data_dir: &std::path::Path,
    provider: alloy::primitives::Address,
) -> anyhow::Result<u64> {
    // Before the first pull there is no store yet — nothing has been billed.
    let Ok(store) = RedbBuyerPoolStore::open(data_dir) else {
        return Ok(0);
    };
    let states = store.load_all().context("load buyer pools")?.pools;
    let mut billed = U256::ZERO;
    for state in states {
        for (lane, progress) in state.lanes() {
            if lane.provider == provider {
                billed = billed.max(progress.last_bytes);
            }
        }
    }
    Ok(u64::try_from(billed).unwrap_or(u64::MAX))
}

/// Run `decdn bundle pull`, retrying until the node's serve path resolves the
/// (possibly freshly-opened) pool — the CLI has no internal retry for that
/// race, matching every sibling bundle-pull e2e.
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
/// local manifest over the explicit-node path, with chain coordinates as
/// flags so no config file is needed. `--provider-address` pins every entry
/// to this one node (and one lane) — required so both `a.bin` and `b.bin`
/// land on the SAME lane the dedup assertion reads. `--jobs` is explicit
/// (rather than relying on the default) so the sequential-donor-before-
/// recipient ordering this journey depends on is visible in the argv.
fn bundle_pull_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    manifest: &std::path::Path,
    out_dir: &std::path::Path,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
    jobs: u32,
) -> Vec<String> {
    vec![
        "-i".into(),
        manifest.display().to_string(),
        "-o".into(),
        out_dir.display().to_string(),
        "--jobs".into(),
        jobs.to_string(),
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
