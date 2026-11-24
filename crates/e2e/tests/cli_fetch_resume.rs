//! Live anvil-backed e2e for the CLI `decdn get` **streaming + resumable
//! download** path (#1120, #1122), driving the shipped `decdn` binary against a
//! real node over a real paid `cdn/client/v1` stream.
//!
//! Why the whole binary rather than a unit test: the driver/store unit tests in
//! `decdn-client` already prove the gap-driven fetch verifies every byte,
//! checkpoints as it goes, and re-opens only the missing gap. What they cannot
//! reach is the part that actually costs money — that a resumed fetch pulls only
//! `missing_ranges` and therefore **pays only for the gap**. That is the whole
//! point of the feature (#1122's repro was a 708 MB blob re-fetched from byte 0
//! after a drop), and it is only observable end to end, as the delta in the
//! persisted voucher watermark across two runs.
//!
//! **On-disk state (#2881).** The gap-driven fetch resumes from the store a
//! download keeps beside `--output`, in `<output>.decdn`: an iroh-blobs file
//! store holding the verified bytes, their outboard and the present ranges.
//! These journeys build a real resumable state in it with
//! [`ClientRangedStore::seed_download_prefix`] (the `test-util` fixture seam).
//!
//! Five journeys, sharing one node and one funded buyer:
//!
//! 1. **Clean fetch.** Output matches the blob and the store is gone — a fetch
//!    that leaves litter beside `--output` would be a regression users notice
//!    immediately.
//! 2. **Resume.** Seeded with a verified prefix, the fetch completes correctly
//!    AND bills strictly fewer bytes than the clean run did: `drive` reads the
//!    held prefix locally and pulls only the still-missing suffix. The
//!    inequality is the assertion that matters; without it the test would pass
//!    on an implementation that silently re-downloaded everything.
//! 3. **Corrupt store data.** Seeded with a verified prefix whose data file is
//!    then flipped a byte. The exported file's whole-file BLAKE3 is the
//!    guarantee: a corrupt byte can never become the output. The run pulls the
//!    missing suffix, exports, the hash catches the drift, and the file is
//!    removed. The same run then fetches the blob again into an empty store and
//!    exports the correct blob. Never a silently wrong output; never a wedge.
//! 4. **A store holding a different blob.** Seeded with a FOREIGN blob's prefix
//!    at `--output`'s location. The store keys entries by hash, so the real
//!    blob's entry starts empty, the blob is fetched whole, and the foreign
//!    bytes never reach the output.
//! 5. **Complete store, exact-multiple-of-16-KiB blob.** A store that holds a
//!    whole blob whose size is an exact multiple of the chunk-group size.
//!    `drive` sees an empty `missing_ranges`, pulls NOTHING, and `finalize`
//!    exports the blob — the fetch finishes for FREE (#1622). Any positive
//!    billed cost means the store re-downloaded a blob already fully on disk.
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
use decdn_client::ClientRangedStore;
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
/// Groups to pre-seed into the store for the resume journey. Enough that
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
/// COMPLETE store snaps to `byte_offset == total_bytes` — the aligned-size
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

/// Flip the first byte of the data file in the store `store` (a download's
/// `<output>.decdn`), leaving the store's own record untouched, so it still
/// holds the (now-corrupt) group present. This is the "bit rot after a
/// verified write" state the exported file's hash exists to catch.
fn corrupt_store_byte(store: &std::path::Path) -> anyhow::Result<()> {
    let data = std::fs::read_dir(store.join("data"))
        .context("list the store's data dir")?
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "data"))
        .context("the store holds no data file to corrupt")?;
    let mut bytes = std::fs::read(&data).context("read the data file to corrupt")?;
    let first = bytes
        .first_mut()
        .context("the data file is empty; nothing to corrupt")?;
    *first ^= 0xff;
    std::fs::write(&data, &bytes).context("write the corrupted data file")?;
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
    let partial = client_dir.path().join("blob.bin.decdn");
    let args = fetch_argv(
        &chain,
        &node,
        &blob_hash,
        client_dir.path(),
        &keystore,
        &out,
    );
    let out_aligned = client_dir.path().join("aligned.bin");
    let partial_aligned = client_dir.path().join("aligned.bin.decdn");
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
        "the store must be removed, not left beside --output: {}",
        partial.display()
    );
    let clean_cost =
        billed_bytes(client_dir.path(), node.operator_addr())?.saturating_sub(before_clean);
    anyhow::ensure!(
        clean_cost > 0,
        "a clean fetch must bill some bytes; the watermark did not advance"
    );

    // ---- Journey 2: resume from a verified prefix in the store ----
    //
    // Seed a real group-aligned prefix into the store and remove the output,
    // so the fetch has to resume from it. This stands in for "a previous run
    // was interrupted after this prefix landed": the store holds what an
    // ingest leaves, and constructing it directly makes the billed-bytes
    // comparison exact. `drive` reads the held prefix locally and pulls only
    // the still-missing suffix.
    std::fs::remove_file(&out).context("remove output before resume")?;
    let seeded = SEEDED_GROUPS * CHUNK_GROUP;
    ClientRangedStore::seed_download_prefix(&out, &blob, seeded as u64)
        .await
        .context("seed a verified prefix")?;

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
        "the store must be removed after a resume too"
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

    // ---- Journey 3: corrupt store data is caught by the export hash ----
    //
    // Seed a verified prefix, then flip a byte in the store's data file while
    // the store still holds the group present. The client trusts the store for
    // what to SKIP, so the corrupt group is never re-pulled on its own: only
    // the exported file's whole-file hash can catch it.
    //
    // `drive` pulls the missing suffix, the store exports, the hash does not
    // match, and the file is removed; the corruption never stays as the
    // output. The same command then fetches the blob once more into an empty
    // store and exports the correct one: the self-heal needs no second run.
    std::fs::remove_file(&out).context("remove output before corrupt run")?;
    ClientRangedStore::seed_download_prefix(&out, &blob, seeded as u64)
        .await
        .context("seed a verified prefix before corrupting it")?;
    corrupt_store_byte(&partial).context("corrupt one byte of the store's data")?;

    let output =
        tokio::process::Command::from(decdn_command(client_dir.path(), KEYSTORE_PASSWORD)?)
            .arg("get")
            .args(&args)
            .output()
            .await
            .context("spawn decdn get over a corrupt checkpoint")?;
    anyhow::ensure!(
        output.status.success(),
        "one fetch over a corrupt checkpoint must heal and succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let got = std::fs::read(&out).context("read output after corruption recovery")?;
    anyhow::ensure!(
        got == blob,
        "a fetch whose export hash catches a corrupt group must re-pull the blob and \
         export the correct blob: got {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        !partial.exists(),
        "the store must be removed after the recovered export"
    );

    // ---- Journey 4: a store holding a DIFFERENT blob ----
    //
    // Seed a FOREIGN blob's prefix into the store at `--output`'s location. The
    // store keys entries by hash, so the real blob's entry starts empty and
    // the blob is fetched whole. The command must finish with the correct
    // blob and never let the foreign bytes reach the output.
    //
    // Mundane trigger: fetch one blob to `-o out.bin`, interrupt it, then fetch a
    // different one to the same `-o`.
    std::fs::remove_file(&out).context("remove output before journey 4")?;
    ClientRangedStore::seed_download_prefix(&out, &aligned, (SEEDED_GROUPS * CHUNK_GROUP) as u64)
        .await
        .context("seed a foreign blob's prefix at blob.bin's location")?;

    run_fetch_until_ready(client_dir.path(), &args).await?;

    let got = std::fs::read(&out).context("read output after a foreign checkpoint")?;
    anyhow::ensure!(
        got == blob,
        "a store holding another blob must not stop or poison the fetch: got {} bytes, \
         expected {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        !partial.exists(),
        "the store must be removed after a fetch beside a foreign blob"
    );

    // ---- Journey 5: a COMPLETE store for an exact-multiple-of-16-KiB blob --
    //
    // Seed a store that holds the whole blob, whose total size is an exact
    // multiple of the chunk-group size. The store is simply complete: `drive`
    // computes an EMPTY `missing_ranges`, opens NOTHING, and `finalize` exports
    // the blob. The aligned-size trap of an offset-derived resume — reading a
    // complete store as `byte_offset == total_bytes`, drawing a `NotFound`, and
    // re-paying from zero — cannot arise: presence comes from the store, not
    // from an offset. A ragged final group (journeys 1-4) exercises the same free
    // finish; this pins it for the exact-multiple size an offset-derived resume would re-pay.
    //
    // A complete store with no `--output` is exactly the on-disk state a crash
    // between the last ingest and the export leaves behind.
    ClientRangedStore::seed_download_prefix(&out_aligned, &aligned, aligned.len() as u64)
        .await
        .context("seed a complete aligned store")?;
    anyhow::ensure!(
        !out_aligned.exists(),
        "aligned output should not exist before journey 5"
    );

    let before_complete = billed_bytes(client_dir.path(), node.operator_addr())?;
    run_fetch_until_ready(client_dir.path(), &aligned_args).await?;

    let got = std::fs::read(&out_aligned).context("read aligned output")?;
    anyhow::ensure!(
        got == aligned,
        "a complete aligned store must be exported as-is: got {} bytes, expected {}",
        got.len(),
        aligned.len()
    );
    anyhow::ensure!(
        !partial_aligned.exists(),
        "the complete store must be removed, not left beside --output"
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

/// Run `decdn get`, retrying until the node's chain watcher has observed the
/// freshly-opened pool (`decdn get` has no internal retry for that race).
async fn run_fetch_until_ready(data_dir: &std::path::Path, args: &[String]) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
            .arg("get")
            .args(args)
            .output()
            .await
            .context("spawn decdn get")?;
        if output.status.success() {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn get never succeeded; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "fetch not ready; retrying after watcher catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The `decdn get` argv (after the `get` subcommand) to pull `hash` from
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
        format!("decdn:{}", hash.to_hex()),
        "-o".into(),
        out.display().to_string(),
        "--node-id".into(),
        node.node_id().to_string(),
        "--addr".into(),
        format!("127.0.0.1:{}", node.bind_port()),
        "--provider-address".into(),
        format!("{}", node.operator_addr()),
        "--governance-rpc-url".into(),
        chain.rpc_url(),
        "--governance-chain-id".into(),
        chain.chain_id().to_string(),
        "--pool".into(),
        format!(
            "eip155:{}:{:#x}",
            chain.chain_id(),
            chain.addrs().payment_pool
        ),
        "--pool-rpc-url".into(),
        chain.rpc_url(),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
        "--working-deposit-micro-usdc".into(),
        DEPOSIT_MICRO_USDC.to_string(),
    ]
}
