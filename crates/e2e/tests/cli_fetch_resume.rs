//! Live anvil-backed e2e for the CLI `decdn fetch` **streaming + resumable
//! download** path (#1120, #1122), driving the shipped `decdn` binary against a
//! real node over a real paid `cdn/client/v1` stream.
//!
//! Why the whole binary rather than a unit test: the driver/store unit tests in
//! `decdn-client-pull` already prove the gap-driven fetch verifies every byte,
//! checkpoints as it goes, and re-opens only the missing gap. What they cannot
//! reach is the part that actually costs money — that a resumed fetch pulls only
//! `missing_ranges` and therefore **pays only for the gap**. That is the whole
//! point of the feature (#1122's repro was a 708 MB blob re-fetched from byte 0
//! after a drop), and it is only observable end to end, as the delta in the
//! persisted voucher watermark across two runs.
//!
//! **New-model on-disk state (#1621).** The gap-driven fetch resumes from a
//! [`ClientRangedStore`]'s three sidecars beside `--output`, NOT from a raw
//! `.partial`: a positioned `<stem>.partial` data file, a whole-blob
//! `<stem>.partial.obao4` pre-order outboard, and a `<stem>.partial.ranges` JSON
//! present-range record. The `.ranges` record is the resume signal — a raw
//! `.partial` with no record is truncated and re-fetched from scratch. So these
//! journeys construct a real *checkpointed* on-disk state with
//! [`ClientRangedStore::seed_checkpointed_prefix`] (the `test-util` fixture seam)
//! rather than writing bytes into a bare `.partial`, which the store would ignore.
//!
//! Five journeys, sharing one node and one funded buyer:
//!
//! 1. **Clean fetch.** Output matches the blob and the `.partial` scratch file is
//!    gone — a fetch that leaves litter beside `--output` would be a regression
//!    users notice immediately.
//! 2. **Resume.** Seeded with a genuine checkpointed prefix (data + outboard +
//!    record), the fetch completes correctly AND bills strictly fewer bytes than
//!    the clean run did: `drive` reads the recorded prefix locally and pulls only
//!    the still-missing suffix. The inequality is the assertion that matters;
//!    without it the test would pass on an implementation that silently
//!    re-downloaded everything.
//! 3. **Corrupt checkpoint.** Seeded with a valid checkpoint whose `.partial` data
//!    is then flipped a byte — the record still claims the group present.
//!    `finalize`'s whole-blob `valid_ranges` sweep is the guarantee: a
//!    corrupt group can never be silently promoted. The first run pulls the
//!    missing suffix, the sweep catches the bad group, shrinks the present set to
//!    exclude it, and returns `Incomplete` — so the run FAILS and writes no
//!    `--output`. The store keeps the (now-shrunk) sidecars, so the very next run
//!    re-pulls exactly the dropped group and promotes the correct blob. Never a
//!    silently wrong output; never a wedge.
//! 4. **Checkpoint from a different blob.** Seeded from a FOREIGN blob's bytes and
//!    outboard at `--output`'s location. On open against the real blob's root the
//!    foreign groups fail the `finalize` sweep, are dropped, and re-fetched. The
//!    fetch recovers to the correct blob rather than wedging or promoting foreign
//!    bytes. Adjacent to journey 3 and driven by the same sweep, which is exactly
//!    why both are here.
//! 5. **Complete checkpoint, exact-multiple-of-16-KiB blob.** A complete
//!    checkpointed partial whose blob size is an exact multiple of the chunk-group
//!    size. `drive` sees an empty `missing_ranges`, pulls NOTHING, and `finalize`
//!    promotes the already-complete `.partial` — the fetch finishes for FREE
//!    (#1622). Any positive billed cost means the store re-downloaded a blob
//!    already fully on disk.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of the `decdn` binary:
//!
//! ```bash
//! cargo build -p decdn-cli
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_fetch_resume
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
use decdn_client_pull::ClientRangedStore;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "resume-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Bytes per bao chunk group — the granularity a resume offset must snap to.
const CHUNK_GROUP: usize = 16 * 1024;
/// Groups to pre-seed into the `.partial` for the resume journey. Enough that
/// skipping them is unambiguous in the billed-bytes comparison.
const SEEDED_GROUPS: usize = 4;

#[tokio::test(flavor = "multi_thread")]
async fn cli_fetch_streams_to_disk_and_resumes_an_interrupted_download() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli fetch resume e2e exceeded the overall timeout")??;
    Ok(())
}

/// Deterministic multi-group blob with a partial final group, so the journey
/// exercises both interior groups and the ragged right edge.
fn make_blob() -> Vec<u8> {
    let mut v = vec![0u8; 12 * CHUNK_GROUP + 1234];
    let mut x: u32 = 0x1234_5678;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

/// A blob whose total size is an EXACT multiple of the chunk-group size, so a
/// COMPLETE `.partial` snaps to `byte_offset == total_bytes` — the aligned-size
/// resume trap journey 5 guards. Distinct byte pattern from [`make_blob`] so a
/// crossed-up hash can never pass by coincidence.
fn make_aligned_blob() -> Vec<u8> {
    let mut v = vec![0u8; 8 * CHUNK_GROUP];
    let mut x: u32 = 0x8bad_f00d;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

/// Flip the first byte of a checkpoint's `.partial` data file in place, leaving
/// its `.obao4` outboard and `.ranges` record untouched — so the record still
/// claims the (now-corrupt) group present. This is the "bit rot after a durable
/// checkpoint" state `finalize`'s `valid_ranges` sweep exists to catch.
fn corrupt_partial_byte(partial: &std::path::Path) -> anyhow::Result<()> {
    let mut bytes = std::fs::read(partial).context("read .partial to corrupt")?;
    let first = bytes
        .first_mut()
        .context("checkpoint .partial is empty; nothing to corrupt")?;
    *first ^= 0xff;
    std::fs::write(partial, &bytes).context("write corrupted .partial")?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's fetch \
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

    let blob = make_blob();
    let blob_hash = Hash::new(&blob);
    // A second blob whose total size is an EXACT multiple of the chunk-group size,
    // for the aligned-complete-partial journey (journey 5).
    let aligned = make_aligned_blob();
    let aligned_hash = Hash::new(&aligned);
    let (node, hashes) =
        NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice(), aligned.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes.first() == Some(&blob_hash),
        "seeded blob hash mismatch: {:?} vs {blob_hash}",
        hashes.first()
    );
    anyhow::ensure!(
        hashes.get(1) == Some(&aligned_hash),
        "seeded aligned blob hash mismatch: {:?} vs {aligned_hash}",
        hashes.get(1)
    );

    // Funded buyer with an on-disk keystore under a `0o700` client data dir (the
    // `RedbBuyerPoolStore` the CLI opens enforces the mode; `tempdir` is
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
    let args = fetch_argv(
        &chain,
        &node,
        &blob_hash,
        client_dir.path(),
        &keystore,
        &out,
    );
    let out_aligned = client_dir.path().join("aligned.bin");
    let partial_aligned = client_dir.path().join("aligned.bin.partial");
    let aligned_args = fetch_argv(
        &chain,
        &node,
        &aligned_hash,
        client_dir.path(),
        &keystore,
        &out_aligned,
    );

    // ---- Journey 1: a clean fetch ----
    //
    // The node accepts vouchers only once its chain watcher has decoded the
    // `PoolOpened` event (~500ms poll), so the first run races it. Retry until
    // observation lands, exactly as the manifest e2e does.
    let before_clean = billed_bytes(client_dir.path(), node.operator_addr())?;
    run_fetch_until_ready(client_dir.path(), &args).await?;

    let got = std::fs::read(&out).context("read output")?;
    anyhow::ensure!(
        got == blob,
        "clean fetch produced {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        !partial.exists(),
        "the .partial scratch file must be promoted away, not left beside --output: {}",
        partial.display()
    );
    let clean_cost =
        billed_bytes(client_dir.path(), node.operator_addr())?.saturating_sub(before_clean);
    anyhow::ensure!(
        clean_cost > 0,
        "a clean fetch must bill some bytes; the watermark did not advance"
    );

    // ---- Journey 2: resume from a genuine checkpointed partial ----
    //
    // Seed a real group-aligned CHECKPOINT (positioned `.partial` + `.obao4`
    // outboard + `.ranges` record) and remove the output, so the fetch has to
    // resume from it. This stands in for "a previous run was interrupted after
    // durably checkpointing this prefix": the on-disk state is byte-identical to
    // what `ingest_stream`'s checkpoint leaves, and constructing it directly makes
    // the billed-bytes comparison exact. `drive` reads the recorded prefix locally
    // and pulls only the still-missing suffix.
    std::fs::remove_file(&out).context("remove output before resume")?;
    let seeded = SEEDED_GROUPS * CHUNK_GROUP;
    ClientRangedStore::seed_checkpointed_prefix(
        client_dir.path(),
        "blob.bin",
        &blob,
        seeded as u64,
    )
    .context("seed genuine checkpointed prefix")?;

    let before_resume = billed_bytes(client_dir.path(), node.operator_addr())?;
    run_fetch_until_ready(client_dir.path(), &args).await?;

    let got = std::fs::read(&out).context("read resumed output")?;
    anyhow::ensure!(
        got == blob,
        "resumed fetch produced {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        !partial.exists(),
        "the .partial must be promoted away after a resume too"
    );
    let resume_cost =
        billed_bytes(client_dir.path(), node.operator_addr())?.saturating_sub(before_resume);

    // THE assertion. Equality here would mean the resume offset never reached the
    // wire and the client silently re-paid for bytes it already had — the exact
    // waste #1122 reports, passing every other check in this file.
    anyhow::ensure!(
        resume_cost < clean_cost,
        "a resumed fetch must bill fewer bytes than a full one: resume billed {resume_cost}, \
         clean billed {clean_cost} — the {seeded}-byte prefix on disk was re-fetched"
    );

    // ---- Journey 3: a corrupt checkpoint is caught by the finalize sweep ----
    //
    // Seed a valid checkpoint, then flip a byte in its `.partial` data while the
    // `.ranges` record still claims the group present. The client
    // trusts the record for what to SKIP, so the corrupt group is never re-pulled
    // on its own — only `finalize`'s whole-blob `valid_ranges` sweep can catch it.
    //
    // First run: `drive` pulls the missing suffix, `finalize` sweeps, the flipped
    // group fails, `present` is shrunk to drop exactly it, and `finalize` returns
    // `Incomplete` — so the run FAILS and writes no `--output` (a silently wrong
    // output file is the exact bug this guards). The store keeps its now-shrunk
    // sidecars, so the corruption is not promoted and not left to poison presence.
    std::fs::remove_file(&out).context("remove output before corrupt run")?;
    ClientRangedStore::seed_checkpointed_prefix(
        client_dir.path(),
        "blob.bin",
        &blob,
        seeded as u64,
    )
    .context("seed valid checkpoint before corrupting it")?;
    corrupt_partial_byte(&partial).context("corrupt one byte of the checkpoint's .partial data")?;

    let output =
        tokio::process::Command::from(decdn_command(client_dir.path(), KEYSTORE_PASSWORD)?)
            .arg("fetch")
            .args(&args)
            .output()
            .await
            .context("spawn decdn fetch over a corrupt checkpoint")?;
    anyhow::ensure!(
        !output.status.success(),
        "a fetch whose finalize sweep catches a corrupt group must fail, not write a wrong \
         output file"
    );
    anyhow::ensure!(
        !out.exists(),
        "a failed finalize sweep must not leave a (wrong) output file behind"
    );

    // Second run: the sweep already dropped the bad group from the record, so the
    // resume re-pulls exactly it and promotes the correct blob — the self-heal the
    // client guarantees without ever emitting a wrong output.
    run_fetch_until_ready(client_dir.path(), &args).await?;
    let got = std::fs::read(&out).context("read output after corruption recovery")?;
    anyhow::ensure!(
        got == blob,
        "the run after a caught corruption must re-pull the dropped group and promote the correct \
         blob: got {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        !partial.exists(),
        "the recovered blob must be promoted away, not left as a .partial"
    );

    // ---- Journey 4: a checkpoint belonging to a DIFFERENT blob ----
    //
    // The trap journey 3 does not cover, and the two are driven by the same sweep.
    // Seed a checkpoint from a FOREIGN blob's bytes and outboard at `--output`'s
    // location. On open against the real blob's `(root, total_bytes)` the record
    // claims a prefix present, so `drive` pulls only the (real) suffix — but the
    // foreign prefix groups fail `finalize`'s `valid_ranges` sweep and are dropped.
    // A subsequent run re-pulls them from the node (overwriting the foreign bytes)
    // and promotes the correct blob. The command must recover, never wedge and
    // never promote foreign bytes.
    //
    // Mundane trigger: fetch one blob to `-o out.bin`, interrupt it, then fetch a
    // different one to the same `-o`.
    std::fs::remove_file(&out).context("remove output before journey 4")?;
    ClientRangedStore::seed_checkpointed_prefix(
        client_dir.path(),
        "blob.bin",
        &aligned,
        (SEEDED_GROUPS * CHUNK_GROUP) as u64,
    )
    .context("seed a foreign-blob checkpoint at blob.bin's location")?;

    run_fetch_until_ready(client_dir.path(), &args).await?;

    let got = std::fs::read(&out).context("read output after a foreign checkpoint")?;
    anyhow::ensure!(
        got == blob,
        "a checkpoint belonging to another blob must be discarded and the fetch restarted clean, \
         not wedged or promoted with foreign bytes: got {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        !partial.exists(),
        "the .partial must be promoted away after recovering from a foreign checkpoint"
    );

    // ---- Journey 5: a COMPLETE checkpoint for an exact-multiple-of-16-KiB blob --
    //
    // Seed a COMPLETE checkpoint (record claims the whole blob) whose total size is
    // an exact multiple of the chunk-group size. The store is
    // simply complete: `drive` computes an EMPTY `missing_ranges`, opens NOTHING,
    // and `finalize` promotes the already-complete `.partial` in place. The
    // aligned-size trap of an offset-derived resume — reading a
    // complete partial as `byte_offset == total_bytes`, drawing a `NotFound`, and
    // re-paying from zero — cannot arise: presence is record-driven, not
    // offset-derived. A ragged final group (journeys 1-4) exercises the same free
    // finish; this pins it for the exact-multiple size an offset-derived resume would re-pay.
    //
    // A complete checkpoint with no `--output` is exactly the on-disk state a crash
    // between the last checkpoint and the atomic promote leaves behind.
    ClientRangedStore::seed_checkpointed_prefix(
        client_dir.path(),
        "aligned.bin",
        &aligned,
        aligned.len() as u64,
    )
    .context("seed complete aligned checkpoint")?;
    anyhow::ensure!(
        !out_aligned.exists(),
        "aligned output should not exist before journey 5"
    );

    let before_complete = billed_bytes(client_dir.path(), node.operator_addr())?;
    run_fetch_until_ready(client_dir.path(), &aligned_args).await?;

    let got = std::fs::read(&out_aligned).context("read aligned output")?;
    anyhow::ensure!(
        got == aligned,
        "a complete aligned partial must be promoted as-is: got {} bytes, expected {}",
        got.len(),
        aligned.len()
    );
    anyhow::ensure!(
        !partial_aligned.exists(),
        "the complete .partial must be promoted away, not left beside --output"
    );
    let complete_cost =
        billed_bytes(client_dir.path(), node.operator_addr())?.saturating_sub(before_complete);
    // THE assertion for this journey: a partial that is already the whole blob is
    // finished with ZERO further payment. Any positive cost means the file was
    // truncated and the whole blob re-fetched — the aligned-size re-pay bug.
    anyhow::ensure!(
        complete_cost == 0,
        "a complete partial must cost nothing to finish: billed {complete_cost} extra bytes for a \
         blob already fully on disk (the aligned-size re-pay bug)"
    );

    // `NodeFixture` tears the daemon down on drop; there is nothing to await.
    drop(node);
    Ok(())
}

/// Cumulative bytes the buyer has paid `provider` for, read from the persisted
/// pool's `(signer, provider)` lane watermark. The store is reopened per call
/// because the CLI subprocess owns it between calls. One buyer signs one pool, so
/// the highest lane watermark naming `provider` across the tracked pools is the
/// cumulative bytes billed to it.
fn billed_bytes(
    data_dir: &std::path::Path,
    provider: alloy::primitives::Address,
) -> anyhow::Result<u64> {
    // Before the first fetch there is no store yet — nothing has been billed.
    let Ok(store) = RedbBuyerPoolStore::open(data_dir) else {
        return Ok(0);
    };
    let mut billed = U256::ZERO;
    for pool in store.load_all().context("load buyer pools")?.pools {
        for (lane, progress) in pool.lanes() {
            if lane.provider == provider {
                billed = billed.max(progress.last_bytes);
            }
        }
    }
    Ok(u64::try_from(billed).unwrap_or(u64::MAX))
}

/// Run `decdn fetch`, retrying until the node's chain watcher has observed the
/// freshly-opened pool (`decdn fetch` has no internal retry for that race).
async fn run_fetch_until_ready(data_dir: &std::path::Path, args: &[String]) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
            .arg("fetch")
            .args(args)
            .output()
            .await
            .context("spawn decdn fetch")?;
        if output.status.success() {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn fetch never succeeded; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "fetch not ready; retrying after watcher catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
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
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
    out: &std::path::Path,
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
