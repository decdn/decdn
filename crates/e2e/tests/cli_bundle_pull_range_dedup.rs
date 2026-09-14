//! Live anvil-backed e2e for `decdn bundle pull`'s **range-dedup** path: an
//! optimized bundle whose two files share a large byte run dedups the shared
//! range — splices it locally from the first file's already-materialized
//! blob — instead of re-downloading (and re-paying for) it from the node.
//!
//! Shape: two files share their first 4 MiB, chunk-group-aligned, and differ
//! only in a distinct tail. Each file is stored as ONE whole-file blob (the
//! `origin import --optimize` model: chunks are manifest-only dedup
//! hints, never separately stored blobs), so the node here is seeded with the
//! two whole files directly via [`NodeFixture::launch_with_blobs`] — exactly
//! what an `--optimize` import would have produced on disk. The manifest is
//! hand-built, with each entry's `chunks` hints naming the shared run and
//! each file's distinct tail as separate BLAKE3-addressed spans.
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

/// The fixed chunk size the real-import journey (below) pins
/// `--chunk-avg`/`--chunk-min`/`--chunk-max` to. This is `fastcdc` v2020's own
/// `MINIMUM_MAX` — the largest value it accepts for `--chunk-min` — so it is
/// the biggest fixed chunk size reachable via `min == avg == max`.
/// `SHARED_BYTES` is an exact multiple of it, so the forced fixed-size cuts
/// land exactly on the shared/tail seam.
const IMPORT_CHUNK_BYTES: u64 = 1024 * 1024;

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

/// Self-heal regression. A *lying recipient hint* names a real donor chunk (a
/// chunk the donor genuinely holds, so the donor splice VERIFIES) at an offset
/// where the recipient's actual bytes differ, so the reassembled whole-file
/// BLAKE3 mismatches. `pull_entry` must then re-drive the whole blob, let
/// `drive` finalize+promote it, and SUCCEED — the recovered file byte- and
/// BLAKE3-exact.
///
/// This exercises the finalize-aware guard on that self-heal path: the
/// whole-file re-drive completes the blob (`drive` renames `<hex>.partial` to
/// `<hex>`), so the guard must recognize completion and skip calling
/// `hash_partial` on the now-absent `.partial` file, rather than surfacing an
/// `Err(open: No such file)` that would fail the entry spuriously. With the
/// guard the pull succeeds; a regression here makes `bundle pull` exit
/// non-zero and `run_bundle_pull_until_ready` never succeed (the test times
/// out).
#[tokio::test(flavor = "multi_thread")]
async fn cli_bundle_pull_self_heals_a_lying_recipient_hint() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_self_heal()))
        .await
        .context("cli bundle pull self-heal e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey mirroring the dedup test's shape"
)]
async fn run_self_heal() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // `donor` is a real chunk file A genuinely holds. File B's REAL first
    // `SHARED_BYTES` (`prefix_b`) are DIFFERENT bytes, but B's manifest LIES that
    // its first chunk is `donor`'s hash. So the donor splice verifies (A really
    // has `donor`) yet placing `donor` at B's head makes B's whole-file BLAKE3
    // mismatch — the exact trigger for the whole-file re-drive self-heal.
    let donor = deterministic_bytes(SHARED_BYTES, 0x5EED_D010);
    let tail_a = deterministic_bytes(TAIL_BYTES, 0x5EED_D0A1);
    let prefix_b = deterministic_bytes(SHARED_BYTES, 0x5EED_D0B0);
    let tail_b = deterministic_bytes(TAIL_BYTES, 0x5EED_D0B2);

    let mut file_a = donor.clone();
    file_a.extend_from_slice(&tail_a);
    // B's real content — its head is `prefix_b`, NOT `donor`.
    let mut file_b = prefix_b.clone();
    file_b.extend_from_slice(&tail_b);

    let hash_donor = Hash::new(&donor);
    let hash_tail_a = Hash::new(&tail_a);
    // B's chunk[1] hash — B's real tail, a hash no entry registers, so it
    // contributes no donor (only the lying chunk[0] does).
    let hash_tail_b = Hash::new(&tail_b);
    let whole_a = Hash::new(&file_a);
    let whole_b = Hash::new(&file_b);

    // The node holds each file as ONE whole-file blob (the `--optimize` model);
    // it serves B's REAL bytes when the self-heal re-drive asks for the whole blob.
    let (node, hashes) =
        NodeFixture::launch_with_blobs(&chain, "US", &[file_a.as_slice(), file_b.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes == vec![whole_a, whole_b],
        "seeded blob hashes mismatch: {hashes:?}"
    );
    let provider_addr = node.operator_addr();

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

    // `--jobs 1` forces `a.bin` to finalize (and register `hash_donor`) before
    // `b.bin` starts, so `b` is the recipient whose lying hint splices `donor`.
    let manifest_path = client_dir.path().join("self_heal.json");
    let manifest = format!(
        r#"{{"version":1,"entries":[{{"path":"a.bin","hash":"b3:{whole_a}","size":{size_a},"chunks":[{{"hash":"b3:{donor}","size":{shared_sz}}},{{"hash":"b3:{tail_a}","size":{tail_sz}}}]}},{{"path":"b.bin","hash":"b3:{whole_b}","size":{size_b},"chunks":[{{"hash":"b3:{donor}","size":{shared_sz}}},{{"hash":"b3:{tail_b}","size":{tail_sz}}}]}}]}}"#,
        whole_a = whole_a.to_hex(),
        whole_b = whole_b.to_hex(),
        donor = hash_donor.to_hex(),
        tail_a = hash_tail_a.to_hex(),
        tail_b = hash_tail_b.to_hex(),
        size_a = file_a.len(),
        size_b = file_b.len(),
        shared_sz = SHARED_BYTES,
        tail_sz = TAIL_BYTES,
    );
    std::fs::write(&manifest_path, manifest).context("write self-heal manifest")?;

    // Warm-up into a throwaway dir, via the retrying runner, ONLY to open the
    // on-chain pool and let the node's serve path resolve it — the same serve
    // race every sibling bundle-pull e2e absorbs. This is not the assertion: the
    // retrying runner would MASK the self-heal bug (a spurious first-attempt
    // failure leaves the blob finalized at staging, so the retry recovers). The
    // measured run below is the SINGLE-shot pull that must not fail at all.
    let warm_out = client_dir.path().join("warm");
    let warm_args = bundle_pull_argv(
        &chain,
        &node,
        &manifest_path,
        &warm_out,
        client_dir.path(),
        &keystore,
        1,
    );
    run_bundle_pull_until_ready(client_dir.path(), &warm_args).await?;

    // Snapshot the lane watermark AFTER the warm-up so the assertion below measures
    // only the measured run's spend — the warm-up's own self-heal spend (and any
    // serve-race retry it absorbed) is folded into `before` and excluded.
    let before = billed_bytes(client_dir.path(), provider_addr)?;

    // The measured run: a fresh output dir (so `a.bin` is re-pulled and
    // re-registers `hash_donor`, arming `b.bin`'s self-heal) and EXACTLY ONE
    // `bundle pull` invocation. The pool is already open, so the only way this
    // exits non-zero is the self-heal bug — a `.partial` `drive` already renamed
    // to `staging` after the whole-file re-drive. Without the finalize-aware
    // guard the entry fails here; the retry loop is deliberately not used.
    let out_dir = client_dir.path().join("out");
    let out_args = bundle_pull_argv(
        &chain,
        &node,
        &manifest_path,
        &out_dir,
        client_dir.path(),
        &keystore,
        1,
    );
    let output =
        tokio::process::Command::from(decdn_command(client_dir.path(), KEYSTORE_PASSWORD)?)
            .arg("bundle")
            .arg("pull")
            .args(&out_args)
            .output()
            .await
            .context("spawn single-shot self-heal bundle pull")?;
    anyhow::ensure!(
        output.status.success(),
        "self-heal bundle pull must succeed in one attempt; without the finalize-aware guard the \
         re-driven entry fails spuriously. stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Both outputs byte-exact and whole-file BLAKE3-exact — self-heal is sound.
    let got_a = std::fs::read(out_dir.join("a.bin")).context("read a.bin")?;
    let got_b = std::fs::read(out_dir.join("b.bin")).context("read b.bin")?;
    anyhow::ensure!(got_a == file_a, "a.bin mismatch: {} bytes", got_a.len());
    anyhow::ensure!(got_b == file_b, "b.bin mismatch: {} bytes", got_b.len());
    anyhow::ensure!(Hash::new(&got_a) == whole_a, "a.bin BLAKE3 mismatch");
    anyhow::ensure!(Hash::new(&got_b) == whole_b, "b.bin BLAKE3 mismatch");

    // Money assertion — EXACT delta, no `>=` slack (the whole point of Finding
    // Critical-4: a cumulative `>=` was already satisfied by the warm-up, so it
    // could not tell the self-heal path from any other outcome). The measured run
    // re-fetches every byte of the recipient: `a.bin` is re-driven WHOLE (fresh out
    // dir, first entry, no donor yet), then `b.bin` drives its complement (the tail
    // past the shared run), splices the lying donor at its head, fails the
    // whole-file BLAKE3, and re-drives the head range `[0, SHARED)`. So `b` costs
    // its complement wire PLUS its head wire — two separately-metered ranges, not a
    // single whole-file fetch and not a dedup discount. If `plan_dedup` stopped
    // producing the donor, `b` would be fetched plain (one whole-file range) and
    // this exact figure would not match; the delta is the falsifiable witness that
    // the self-heal (drive complement -> splice -> mismatch -> re-drive head) ran.
    let total_a = file_a.len() as u64;
    let total_b = file_b.len() as u64;
    let complement_wire = range_wire_bytes(SHARED_BYTES, TAIL_BYTES, total_b)?;
    let head_wire = range_wire_bytes(0, SHARED_BYTES, total_b)?;
    let expected_delta = whole_blob_wire_bytes(total_a)
        .checked_add(complement_wire)
        .and_then(|v| v.checked_add(head_wire))
        .context("self-heal expected-delta overflow")?;
    let paid = billed_bytes(client_dir.path(), provider_addr)?.saturating_sub(before);
    anyhow::ensure!(
        paid == expected_delta,
        "self-heal must pay exactly a's whole file plus b's complement plus b's \
         re-driven head = {expected_delta}, got {paid}"
    );

    drop(node);
    Ok(())
}

/// End-to-end proof that raising `--max-lane-streams` above 1 is safe: two
/// multi-chunk entries pinned to ONE provider (so they share ONE
/// `(pool, signer, provider)` lane) are pulled CONCURRENTLY, and both complete
/// with exactly-correct billing.
///
/// Both entries draw on one shared voucher watermark. With concurrent streams
/// their per-chunk `PayWord` reveals interleave on the lane's shared chain
/// index, so a slower stream's reveal for its own delivered chunk routinely
/// lands at or below the frontier a faster sibling already advanced. The node
/// must credit that below-frontier reveal from lane headroom (the same
/// fungible-credit rule the benign-voucher path uses) rather than crediting
/// nothing and throttling the slow stream into a `ClientPaymentFault`. Before
/// that node-side fix, `--max-lane-streams 2` could starve one stream; this
/// journey exercises the fixed path end to end.
///
/// Asserts: (1) both files byte- and BLAKE3-exact, and (2) the shared lane bills
/// EXACTLY both whole files' wire bytes — no double-pay, no under-pay — the same
/// total a sequential pull of the two would bill.
#[tokio::test(flavor = "multi_thread")]
async fn cli_bundle_pull_concurrent_same_lane_streams_bill_correctly() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_concurrent_same_lane()))
        .await
        .context("cli bundle pull concurrent same-lane e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey mirroring the dedup test's shape"
)]
async fn run_concurrent_same_lane() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // Two content-distinct multi-chunk files (no shared bytes, no chunk hints):
    // each pulls its WHOLE file, and each spans several 1 MiB `PayWord` chunks so
    // the concurrent streams genuinely interleave reveals on the shared chain.
    let file_a = deterministic_bytes(4 * 1024 * 1024 + 4_321, 0x5EED_5A1A);
    let file_b = deterministic_bytes(4 * 1024 * 1024 + 8_765, 0x5EED_5B2B);
    let whole_a = Hash::new(&file_a);
    let whole_b = Hash::new(&file_b);

    let (node, hashes) =
        NodeFixture::launch_with_blobs(&chain, "US", &[file_a.as_slice(), file_b.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes == vec![whole_a, whole_b],
        "seeded blob hashes mismatch: {hashes:?}"
    );
    let provider_addr = node.operator_addr();

    // Funded buyer with an on-disk keystore under a `0o700` client data dir.
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
    let manifest_path = client_dir.path().join("concurrent.json");
    std::fs::write(
        &manifest_path,
        format!(
            r#"{{"version":1,"entries":[{{"path":"a.bin","hash":"b3:{}"}},{{"path":"b.bin","hash":"b3:{}"}}]}}"#,
            whole_a.to_hex(),
            whole_b.to_hex(),
        ),
    )
    .context("write concurrent manifest")?;

    // `--jobs 2` runs both entries at once; `--max-lane-streams 2` lets both
    // share the one lane concurrently instead of serializing on it.
    let mut args = bundle_pull_argv(
        &chain,
        &node,
        &manifest_path,
        &out_dir,
        client_dir.path(),
        &keystore,
        2,
    );
    args.push("--max-lane-streams".into());
    args.push("2".into());

    let before = billed_bytes(client_dir.path(), provider_addr)?;
    anyhow::ensure!(before == 0, "lane must be unbilled before the pull");
    run_bundle_pull_until_ready(client_dir.path(), &args).await?;
    let paid = billed_bytes(client_dir.path(), provider_addr)?.saturating_sub(before);

    // (1) Both files byte- and BLAKE3-exact.
    let got_a = std::fs::read(out_dir.join("a.bin")).context("read a.bin")?;
    let got_b = std::fs::read(out_dir.join("b.bin")).context("read b.bin")?;
    anyhow::ensure!(got_a == file_a, "a.bin mismatch: {} bytes", got_a.len());
    anyhow::ensure!(got_b == file_b, "b.bin mismatch: {} bytes", got_b.len());
    anyhow::ensure!(Hash::new(&got_a) == whole_a, "a.bin BLAKE3 mismatch");
    anyhow::ensure!(Hash::new(&got_b) == whole_b, "b.bin BLAKE3 mismatch");

    // (2) The shared lane bills EXACTLY both whole files' wire bytes — the same
    // total a sequential pull bills, so concurrent same-lane delivery neither
    // double-pays nor under-pays.
    let wire_a = whole_blob_wire_bytes(file_a.len() as u64);
    let wire_b = whole_blob_wire_bytes(file_b.len() as u64);
    let expected = wire_a
        .checked_add(wire_b)
        .context("expected-paid overflow")?;
    anyhow::ensure!(
        paid == expected,
        "concurrent same-lane pull must bill exactly a's whole file ({wire_a}) plus b's whole \
         file ({wire_b}) = {expected}, got {paid}"
    );

    drop(node);
    Ok(())
}

/// Live seam test: the two journeys above seed whole-file blobs directly and
/// hand-build the manifest, so the `origin import --optimize` -> chunk-hint ->
/// `bundle pull` seam is never exercised end-to-end. This journey runs the
/// REAL `decdn origin import --optimize` over an on-disk source tree, serves
/// the manifest + whole-file blobs it actually wrote (read back from the
/// import store, not the in-memory originals), and `bundle pull --hash`es the
/// import-emitted bundle hash — asserting the same dedup money property as
/// [`cli_bundle_pull_dedups_an_overlapping_range`].
///
/// `--chunk-avg`/`--chunk-min`/`--chunk-max` are all pinned to
/// [`IMPORT_CHUNK_BYTES`] (fastcdc v2020's own `MINIMUM_MAX`, the largest
/// value its `--chunk-min` accepts): fastcdc can never cut before `min` and is
/// forced to cut at `max`, so with `min == avg == max` the real chunker
/// degenerates to fixed-size cuts — while still being the genuine `fastcdc`
/// code path, not a stand-in. `SHARED_BYTES` is an exact multiple of
/// `IMPORT_CHUNK_BYTES`, so the fixed cuts land exactly on the shared/tail
/// seam: each file gets `SHARED_BYTES / IMPORT_CHUNK_BYTES` identical leading
/// chunks (byte-identical prefix -> identical fastcdc cut decisions -> equal
/// hashes) plus one distinct final chunk covering its own tail.
#[tokio::test(flavor = "multi_thread")]
async fn cli_bundle_pull_from_optimize_import_dedups() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_from_optimize_import()))
        .await
        .context("cli bundle pull from-optimize-import e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey mirroring the hand-built dedup test's shape"
)]
async fn run_from_optimize_import() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // Real files on disk: `a.bin`/`b.bin` share the same chunk-group-aligned
    // `SHARED_BYTES` prefix as the hand-built dedup journey, each with a
    // distinct, ragged `TAIL_BYTES` tail.
    let shared = deterministic_bytes(SHARED_BYTES, 0x0F71_5EED);
    let tail_a = deterministic_bytes(TAIL_BYTES, 0x0F71_00A1);
    let tail_b = deterministic_bytes(TAIL_BYTES, 0x0F71_00B2);
    let mut file_a = shared.clone();
    file_a.extend_from_slice(&tail_a);
    let mut file_b = shared;
    file_b.extend_from_slice(&tail_b);

    let src_dir = tempfile::tempdir().context("source tempdir")?;
    std::fs::write(src_dir.path().join("a.bin"), &file_a).context("write a.bin source")?;
    std::fs::write(src_dir.path().join("b.bin"), &file_b).context("write b.bin source")?;

    // A HOME for the offline `origin import` invocation. It touches no
    // keystore/chain, but `decdn_command` requires an absolute HOME regardless.
    let import_home = tempfile::tempdir().context("import home tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        import_home.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod import home 0o700")?;
    let import_store = import_home.path().join("import-store");
    let bundle_out = import_home.path().join("bundle.json");
    let chunk_bytes = IMPORT_CHUNK_BYTES.to_string();

    let output =
        tokio::process::Command::from(decdn_command(import_home.path(), KEYSTORE_PASSWORD)?)
            .arg("origin")
            .arg("import")
            .arg("-i")
            .arg(src_dir.path())
            .arg("--to")
            .arg(format!("fs:{}", import_store.display()))
            .arg("--optimize")
            .arg("--chunk-avg")
            .arg(&chunk_bytes)
            .arg("--chunk-min")
            .arg(&chunk_bytes)
            .arg("--chunk-max")
            .arg(&chunk_bytes)
            .arg("--bundle")
            .arg(&bundle_out)
            .arg("--json")
            .output()
            .await
            .context("spawn decdn origin import --optimize")?;
    anyhow::ensure!(
        output.status.success(),
        "decdn origin import --optimize failed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // --- Import-half assertions: the real `--optimize` output shape ---------
    let report: serde_json::Value = serde_json::from_slice(output.stdout.trim_ascii_end())
        .context("parse origin import --json report")?;
    anyhow::ensure!(
        report["optimized"].as_bool() == Some(true),
        "report: {report}"
    );
    let chunks_per_file = SHARED_BYTES / IMPORT_CHUNK_BYTES + 1;
    anyhow::ensure!(
        report["chunks_total"].as_u64() == Some(chunks_per_file * 2),
        "expected {} chunk hints across both files, report: {report}",
        chunks_per_file * 2
    );
    let bundle_hex = report["bundle_hash"]
        .as_str()
        .and_then(|s| s.strip_prefix("b3:"))
        .context("report missing bundle_hash")?
        .to_string();
    let bundle_hash: Hash = bundle_hex.parse().context("parse bundle_hash hex")?;

    let manifest_bytes = std::fs::read(import_object_path(&import_store, &bundle_hex))
        .context("read manifest blob from import store")?;
    let manifest: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).context("parse manifest json")?;
    let entries = manifest["entries"].as_array().context("manifest entries")?;
    anyhow::ensure!(
        entries.len() == 2,
        "expected 2 entries, manifest: {manifest}"
    );

    // Every entry's WHOLE-FILE hash (+ its `.obao4` outboard) is a stored data
    // object, exactly like a plain import; a chunk-hint hash that is not also
    // some entry's whole-file hash is checked NOT stored — chunks are
    // manifest-only dedup hints, never separately stored blobs.
    let mut whole_hex_by_path: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut all_hint_hexes: Vec<String> = Vec::new();
    for e in entries {
        let path = e["path"].as_str().context("entry path")?.to_string();
        let whole_hex = e["hash"]
            .as_str()
            .and_then(|s| s.strip_prefix("b3:"))
            .context("entry hash")?
            .to_string();
        anyhow::ensure!(
            import_object_path(&import_store, &whole_hex).is_file(),
            "whole-file blob {whole_hex} must be stored"
        );
        anyhow::ensure!(
            import_obao4_path(&import_store, &whole_hex).is_file(),
            "whole-file outboard for {whole_hex} must be stored"
        );
        let chunks = e["chunks"].as_array().context("entry chunks")?;
        anyhow::ensure!(
            chunks.len() as u64 == chunks_per_file,
            "expected exactly {chunks_per_file} chunk hints, entry: {e}"
        );
        for c in chunks {
            let chex = c["hash"]
                .as_str()
                .and_then(|s| s.strip_prefix("b3:"))
                .context("chunk hash")?
                .to_string();
            all_hint_hexes.push(chex);
        }
        whole_hex_by_path.insert(path, whole_hex);
    }
    for chex in &all_hint_hexes {
        if whole_hex_by_path.values().any(|w| w == chex) {
            continue;
        }
        anyhow::ensure!(
            !import_object_path(&import_store, chex).is_file(),
            "chunk hint {chex} must NOT be stored as its own data object"
        );
    }
    let whole_a_hex = whole_hex_by_path
        .get("a.bin")
        .context("manifest missing a.bin entry")?
        .clone();
    let whole_b_hex = whole_hex_by_path
        .get("b.bin")
        .context("manifest missing b.bin entry")?
        .clone();
    let whole_a: Hash = whole_a_hex.parse().context("parse a.bin whole hash")?;
    let whole_b: Hash = whole_b_hex.parse().context("parse b.bin whole hash")?;
    anyhow::ensure!(whole_a == Hash::new(&file_a), "a.bin whole hash mismatch");
    anyhow::ensure!(whole_b == Hash::new(&file_b), "b.bin whole hash mismatch");

    // --- Serve the import's ACTUAL bytes -------------------------------------
    // The manifest and both whole-file blobs the node serves are read back
    // from the import store `--optimize` actually wrote, not the in-memory
    // source bytes — the seeded node is provably driven by the real import
    // output, not a reconstruction of it.
    let stored_a = std::fs::read(import_object_path(&import_store, &whole_a_hex))
        .context("read a.bin blob from import store")?;
    let stored_b = std::fs::read(import_object_path(&import_store, &whole_b_hex))
        .context("read b.bin blob from import store")?;
    anyhow::ensure!(stored_a == file_a, "stored a.bin bytes differ from source");
    anyhow::ensure!(stored_b == file_b, "stored b.bin bytes differ from source");

    let (node, hashes) = NodeFixture::launch_with_blobs(
        &chain,
        "US",
        &[
            manifest_bytes.as_slice(),
            stored_a.as_slice(),
            stored_b.as_slice(),
        ],
    )
    .await?;
    anyhow::ensure!(
        hashes == vec![bundle_hash, whole_a, whole_b],
        "seeded blob hashes mismatch: {hashes:?}"
    );
    let provider_addr = node.operator_addr();

    // --- Buyer setup (mirrors the hand-built dedup journey) ------------------
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
    let args = bundle_pull_hash_argv(
        &chain,
        &node,
        &bundle_hex,
        &out_dir,
        client_dir.path(),
        &keystore,
        1,
    );

    let before = billed_bytes(client_dir.path(), provider_addr)?;
    anyhow::ensure!(before == 0, "lane must be unbilled before the first pull");
    run_bundle_pull_until_ready(client_dir.path(), &args).await?;
    let paid = billed_bytes(client_dir.path(), provider_addr)?.saturating_sub(before);

    // (1) Byte-exact + whole-file-BLAKE3-exact outputs.
    let got_a = std::fs::read(out_dir.join("a.bin")).context("read a.bin")?;
    let got_b = std::fs::read(out_dir.join("b.bin")).context("read b.bin")?;
    anyhow::ensure!(got_a == file_a, "a.bin mismatch: {} bytes", got_a.len());
    anyhow::ensure!(got_b == file_b, "b.bin mismatch: {} bytes", got_b.len());
    anyhow::ensure!(Hash::new(&got_a) == whole_a, "a.bin BLAKE3 mismatch");
    anyhow::ensure!(Hash::new(&got_b) == whole_b, "b.bin BLAKE3 mismatch");

    // (2) Dedup happened. `--hash` fetches the manifest blob itself over the
    // SAME paid lane before any entry (`fetch_to_memory`), so the exact
    // watermark is the manifest's own whole-file wire cost PLUS the same money
    // property the hand-built manifest journey asserts: `a`'s whole file plus
    // only `b`'s chunk-group-aligned COMPLEMENT past the shared run.
    let wire_manifest = whole_blob_wire_bytes(manifest_bytes.len() as u64);
    let wire_a_whole = whole_blob_wire_bytes(file_a.len() as u64);
    let wire_b_whole = whole_blob_wire_bytes(file_b.len() as u64);
    let wire_b_complement = range_wire_bytes(SHARED_BYTES, TAIL_BYTES, file_b.len() as u64)?;
    let expected_paid = wire_manifest
        .checked_add(wire_a_whole)
        .and_then(|v| v.checked_add(wire_b_complement))
        .context("expected-paid overflow")?;
    let no_dedup_total = wire_manifest
        .checked_add(wire_a_whole)
        .and_then(|v| v.checked_add(wire_b_whole))
        .context("no-dedup total overflow")?;
    anyhow::ensure!(
        paid == expected_paid,
        "shared lane must bill exactly the manifest ({wire_manifest}) plus a's whole file \
         ({wire_a_whole}) plus b's complement ({wire_b_complement}) = {expected_paid}, got {paid}"
    );
    anyhow::ensure!(
        paid < no_dedup_total,
        "dedup must strictly undercut re-downloading both whole files (plus the manifest): \
         paid {paid} is not less than {no_dedup_total}"
    );

    drop(node);
    Ok(())
}

/// End-to-end proof of the incremental-sync loop: publish a bundle, pull it,
/// mutate one file, re-publish, and re-pull into the SAME output directory.
///
/// `a.bin` never changes across the two publishes; `b.bin`'s tail changes (so
/// its whole-file hash changes) while its leading chunk keeps the exact bytes
/// (and hash) it had before. The second pull must: skip `a.bin` entirely (the
/// `.decdn-manifest.json` skip-cache fast-skip from task 3), and for `b.bin`
/// splice its shared leading chunk from the OLD on-disk copy still sitting at
/// `out_dir/b.bin` (the on-disk chunk-donor seeding from task 5) rather than
/// re-downloading and re-paying for it — paying only for the changed tail.
///
/// `b.bin`'s v2 bytes are seeded into the SAME node's filesystem origin
/// (`seed_origin_blob_with_outboard`, ADR-038 range tier) after the mutation,
/// mirroring a real operator re-importing updated content: the client-visible
/// picture is a single node whose held content changed between the two pulls.
#[tokio::test(flavor = "multi_thread")]
async fn bundle_pull_reuses_unchanged_and_splices_changed() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_delta_update()))
        .await
        .context("bundle pull delta-update e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey mirroring the sibling dedup tests' shape"
)]
async fn run_delta_update() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // `a.bin`: unchanged across both publishes, no chunk hints (plain entry).
    let file_a = deterministic_bytes(300_000, 0x0DE7_7A0A);
    let whole_a = Hash::new(&file_a);

    // `b.bin` v1: a chunk-group-aligned shared prefix plus a distinct, ragged
    // tail — the same shape the hand-built dedup journeys above use.
    let shared = deterministic_bytes(SHARED_BYTES, 0x0DE7_5EED);
    let tail_v1 = deterministic_bytes(TAIL_BYTES, 0x0DE7_00B1);
    let mut file_b_v1 = shared.clone();
    file_b_v1.extend_from_slice(&tail_v1);
    let hash_shared = Hash::new(&shared);
    let hash_tail_v1 = Hash::new(&tail_v1);
    let whole_b_v1 = Hash::new(&file_b_v1);

    // The node is seeded with `a.bin` and `b.bin` v1 as whole-file blobs — the
    // `--optimize` import model; chunk hints are manifest-only, never
    // separately stored.
    let (node, hashes) =
        NodeFixture::launch_with_blobs(&chain, "US", &[file_a.as_slice(), file_b_v1.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes == vec![whole_a, whole_b_v1],
        "seeded blob hashes mismatch: {hashes:?}"
    );
    let provider_addr = node.operator_addr();

    // Funded buyer with an on-disk keystore under a `0o700` client data dir.
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

    // --- First pull: v1 manifest, fresh out_dir -----------------------------
    let manifest_v1_path = client_dir.path().join("bundle_v1.json");
    let manifest_v1 = format!(
        r#"{{"version":1,"entries":[{{"path":"a.bin","hash":"b3:{whole_a}","size":{size_a}}},{{"path":"b.bin","hash":"b3:{whole_b_v1}","size":{size_b_v1},"chunks":[{{"hash":"b3:{shared}","size":{shared_sz}}},{{"hash":"b3:{tail_v1}","size":{tail_sz}}}]}}]}}"#,
        whole_a = whole_a.to_hex(),
        whole_b_v1 = whole_b_v1.to_hex(),
        shared = hash_shared.to_hex(),
        tail_v1 = hash_tail_v1.to_hex(),
        size_a = file_a.len(),
        size_b_v1 = file_b_v1.len(),
        shared_sz = SHARED_BYTES,
        tail_sz = TAIL_BYTES,
    );
    std::fs::write(&manifest_v1_path, manifest_v1).context("write v1 manifest")?;
    let mut args_v1 = bundle_pull_argv(
        &chain,
        &node,
        &manifest_v1_path,
        &out_dir,
        client_dir.path(),
        &keystore,
        1,
    );
    args_v1.push("--json".into());

    let before_v1 = billed_bytes(client_dir.path(), provider_addr)?;
    anyhow::ensure!(
        before_v1 == 0,
        "lane must be unbilled before the first pull"
    );
    let report1 = run_bundle_pull_json_until_ready(client_dir.path(), &args_v1).await?;

    // (1) Both files landed, byte-exact, and the skip-cache exists.
    let got_a = std::fs::read(out_dir.join("a.bin")).context("read a.bin (first pull)")?;
    let got_b = std::fs::read(out_dir.join("b.bin")).context("read b.bin (first pull)")?;
    anyhow::ensure!(got_a == file_a, "a.bin mismatch on first pull");
    anyhow::ensure!(got_b == file_b_v1, "b.bin mismatch on first pull");
    anyhow::ensure!(
        report1["fetched"].as_u64() == Some(2),
        "first pull must fetch both entries fresh: {report1}"
    );
    anyhow::ensure!(
        report1["failed"].as_u64() == Some(0),
        "first pull must have no failures: {report1}"
    );
    let saved_manifest_path = out_dir.join(".decdn-manifest.json");
    anyhow::ensure!(
        saved_manifest_path.is_file(),
        "first pull must write {}",
        saved_manifest_path.display()
    );
    let saved_manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&saved_manifest_path).context("read skip-cache")?)
            .context("parse skip-cache json")?;
    anyhow::ensure!(
        saved_manifest["files"]["a.bin"].is_object()
            && saved_manifest["files"]["b.bin"].is_object(),
        "skip-cache must record both a.bin and b.bin: {saved_manifest}"
    );

    // --- Mutate b.bin: new tail, same leading chunk -------------------------
    let tail_v2 = deterministic_bytes(TAIL_BYTES, 0x0DE7_00B2);
    anyhow::ensure!(tail_v2 != tail_v1, "tail must actually change");
    let mut file_b_v2 = shared.clone();
    file_b_v2.extend_from_slice(&tail_v2);
    let hash_tail_v2 = Hash::new(&tail_v2);
    let whole_b_v2 = Hash::new(&file_b_v2);
    anyhow::ensure!(
        whole_b_v2 != whole_b_v1,
        "mutating the tail must change the whole-file hash"
    );

    // Seed the node with `b.bin` v2's bytes via its filesystem origin, WITH the
    // sibling `.obao4` outboard so the reactive pull-through can serve the
    // ranged complement fetch below (a missing outboard falls back to a
    // whole-blob fill and would never exercise the range tier, #1372).
    node.seed_origin_blob_with_outboard(&file_b_v2)
        .context("seed b.bin v2 into node origin")?;

    // Re-publish: `a.bin` entry is byte-identical to the v1 manifest; `b.bin`
    // carries the NEW whole-file hash/size but its leading chunk hash is
    // UNCHANGED (same `shared` bytes), only the tail chunk hash differs.
    let manifest_v2_path = client_dir.path().join("bundle_v2.json");
    let manifest_v2 = format!(
        r#"{{"version":1,"entries":[{{"path":"a.bin","hash":"b3:{whole_a}","size":{size_a}}},{{"path":"b.bin","hash":"b3:{whole_b_v2}","size":{size_b_v2},"chunks":[{{"hash":"b3:{shared}","size":{shared_sz}}},{{"hash":"b3:{tail_v2}","size":{tail_sz}}}]}}]}}"#,
        whole_a = whole_a.to_hex(),
        whole_b_v2 = whole_b_v2.to_hex(),
        shared = hash_shared.to_hex(),
        tail_v2 = hash_tail_v2.to_hex(),
        size_a = file_a.len(),
        size_b_v2 = file_b_v2.len(),
        shared_sz = SHARED_BYTES,
        tail_sz = TAIL_BYTES,
    );
    std::fs::write(&manifest_v2_path, manifest_v2).context("write v2 manifest")?;
    let mut args_v2 = bundle_pull_argv(
        &chain,
        &node,
        &manifest_v2_path,
        &out_dir,
        client_dir.path(),
        &keystore,
        1,
    );
    args_v2.push("--json".into());

    // --- Second pull: v2 manifest, SAME out_dir -----------------------------
    let before_v2 = billed_bytes(client_dir.path(), provider_addr)?;
    let report2 = run_bundle_pull_json_until_ready(client_dir.path(), &args_v2).await?;
    let paid_v2 = billed_bytes(client_dir.path(), provider_addr)?.saturating_sub(before_v2);

    // (2) `a.bin` is skipped (the unchanged file); `b.bin` is (re-)fetched.
    anyhow::ensure!(
        report2["skipped"].as_u64().unwrap_or(0) >= 1,
        "second pull must skip at least the unchanged a.bin: {report2}"
    );
    anyhow::ensure!(
        report2["failed"].as_u64() == Some(0),
        "second pull must have no failures: {report2}"
    );

    // (3) `b.bin`'s shared leading chunk was spliced from the OLD on-disk
    // copy, never downloaded or paid for again.
    let spliced_bytes = report2["spliced_bytes"].as_u64().unwrap_or(0);
    anyhow::ensure!(
        spliced_bytes > 0,
        "second pull must splice b.bin's unchanged leading chunk from disk: {report2}"
    );

    // (4) Only the complement (b.bin's changed tail) was paid for — `a.bin`
    // contributes nothing (skipped, never billed) and `b.bin` bills exactly
    // its chunk-group-aligned complement past the shared run, not its whole
    // v2 size.
    let full_bundle_content_size = file_a
        .len()
        .checked_add(file_b_v2.len())
        .and_then(|v| u64::try_from(v).ok())
        .context("full bundle size overflow")?;
    let downloaded = report2["downloaded"].as_u64().unwrap_or(u64::MAX);
    anyhow::ensure!(
        downloaded < full_bundle_content_size,
        "second pull must download less than the full bundle content size ({full_bundle_content_size}): {report2}"
    );
    let wire_b_complement = range_wire_bytes(SHARED_BYTES, TAIL_BYTES, file_b_v2.len() as u64)?;
    anyhow::ensure!(
        paid_v2 == wire_b_complement,
        "second pull must bill EXACTLY b.bin's complement ({wire_b_complement}) — a.bin is \
         skipped (unbilled) and b.bin's shared leading chunk is spliced from disk, not \
         re-downloaded: got {paid_v2}"
    );

    // (5) The changed file on disk now hashes to the NEW manifest hash.
    let got_a2 = std::fs::read(out_dir.join("a.bin")).context("read a.bin (second pull)")?;
    let got_b2 = std::fs::read(out_dir.join("b.bin")).context("read b.bin (second pull)")?;
    anyhow::ensure!(
        got_a2 == file_a,
        "a.bin must be untouched by the second pull"
    );
    anyhow::ensure!(
        got_b2 == file_b_v2,
        "b.bin must be byte-exact to the NEW content after the second pull"
    );
    anyhow::ensure!(
        Hash::new(&got_b2) == whole_b_v2,
        "b.bin must hash to the NEW manifest hash after the second pull"
    );

    drop(node);
    Ok(())
}

/// Like [`run_bundle_pull_until_ready`], but for a `--json`-flagged invocation:
/// retries on the same transient serve-path race, and returns the parsed
/// [`serde_json::Value`] of the single `PullReport` line on success.
async fn run_bundle_pull_json_until_ready(
    data_dir: &std::path::Path,
    args: &[String],
) -> anyhow::Result<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
            .arg("bundle")
            .arg("pull")
            .args(args)
            .output()
            .await
            .context("spawn decdn bundle pull --json")?;
        if output.status.success() {
            let line = output
                .stdout
                .trim_ascii_end()
                .split(|&b| b == b'\n')
                .next_back()
                .context("empty --json output")?;
            return serde_json::from_slice(line).context("parse PullReport json");
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::ensure!(
            !stderr.contains("does not match its whole-file hash")
                && !stderr.contains("No such file")
                && !stderr.contains(".partial"),
            "decdn bundle pull --json failed with a non-transient reconstruction fault; \
             stderr:\n{stderr}"
        );
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn bundle pull --json never succeeded; last stderr:\n{stderr}"
        );
        tracing::debug!(
            "bundle pull --json not ready; retrying after serve-path catch-up:\n{stderr}"
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The sharded data-object path `{store}/{hex[..2]}/{hex}` `origin import`
/// writes for content addressed by `hex` — mirrors the private `object_paths`
/// helper in `crates/cli/src/commands/origin.rs`, not visible from here.
fn import_object_path(store: &std::path::Path, hex: &str) -> std::path::PathBuf {
    store.join(&hex[..2]).join(hex)
}

/// The sibling `{hex}.obao4` outboard path next to [`import_object_path`].
fn import_obao4_path(store: &std::path::Path, hex: &str) -> std::path::PathBuf {
    store.join(&hex[..2]).join(format!("{hex}.obao4"))
}

/// `decdn bundle pull --hash <bundle_hex>` argv — the `--hash` twin of
/// [`bundle_pull_argv`], fetching the manifest blob itself first instead of
/// reading it from a local file.
fn bundle_pull_hash_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    bundle_hex: &str,
    out_dir: &std::path::Path,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
    jobs: u32,
) -> Vec<String> {
    vec![
        "--hash".into(),
        bundle_hex.to_string(),
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
/// `drive_ranges_ordered` pays for one complement run. Mirrors
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
        let stderr = String::from_utf8_lossy(&output.stderr);
        // A reconstruction/self-heal fault is never transient — retrying would
        // absorb a real bug (and silently re-pay for it), the exact hazard
        // Finding Critical-4 flags. Only the serve-path pool-resolution race is
        // retryable; any `failed:` line naming a whole-file-hash mismatch or a
        // missing `.partial` fails the test immediately.
        anyhow::ensure!(
            !stderr.contains("does not match its whole-file hash")
                && !stderr.contains("No such file")
                && !stderr.contains(".partial"),
            "decdn bundle pull failed with a non-transient reconstruction fault; \
             stderr:\n{stderr}"
        );
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn bundle pull never succeeded; last stderr:\n{stderr}"
        );
        tracing::debug!("bundle pull not ready; retrying after serve-path catch-up:\n{stderr}");
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
