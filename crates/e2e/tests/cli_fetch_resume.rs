//! Live anvil-backed e2e for the CLI `decdn fetch` **streaming + resumable
//! download** path (#1120, #1122), driving the shipped `decdn` binary against a
//! real node over a real paid `cdn/client/v1` channel.
//!
//! Why the whole binary rather than a unit test: the unit tests in
//! `decdn-client-pull::sink` already prove the decoder verifies, trims, and
//! flushes correctly against a fixed wire buffer. What they cannot reach is the
//! part that actually costs money — that a resumed fetch asks the node for a
//! `byte_offset > 0` and therefore **pays only for the tail**. That is the whole
//! point of the feature (#1122's repro was a 708 MB blob re-fetched from byte 0
//! after a drop), and it is only observable end to end, as the delta in the
//! persisted voucher watermark across two runs.
//!
//! Three journeys, sharing one node and one funded buyer:
//!
//! 1. **Clean fetch.** Output matches the blob and the `.partial` scratch file is
//!    gone — a fetch that leaves litter beside `--output` would be a regression
//!    users notice immediately.
//! 2. **Resume.** Seeded with a genuine group-aligned prefix, the fetch completes
//!    correctly AND bills strictly fewer bytes than the clean run did. The
//!    inequality is the assertion that matters; without it the test would pass on
//!    an implementation that silently re-downloaded everything.
//! 3. **Corrupt partial.** Seeded with a prefix of the right length but the wrong
//!    bytes, the fetch must FAIL and discard the partial. A prefix off disk is
//!    never verified on the wire (the CLI persists no outboard sidecar to check it
//!    against — see `sink::resume_offset`), so this whole-file check is the only
//!    thing standing between a bad `.partial` and a silently wrong output file. It
//!    must also not leave the bad prefix behind to poison every retry.
//! 4. **Partial from a different blob.** Seeded LONGER than the blob, so the node
//!    refuses the resume offset outright rather than serving bytes that fail a
//!    hash. That refusal carries no watermark bundle, so nothing reseeds — and
//!    leaving the partial in place would wedge the command permanently. Adjacent
//!    to journey 3 and behaves oppositely, which is exactly why both are here.
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
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_channel::BuyerChannelStore;
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "resume-e2e-password";
const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);

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
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice()]).await?;
    anyhow::ensure!(
        hashes.first() == Some(&blob_hash),
        "seeded blob hash mismatch: {:?} vs {blob_hash}",
        hashes.first()
    );

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

    // ---- Journey 1: a clean fetch ----
    //
    // The node accepts vouchers only once its chain watcher has decoded the
    // `ChannelOpened` event (~500ms poll), so the first run races it. Retry until
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

    // ---- Journey 2: resume from a genuine partial ----
    //
    // Seed a real group-aligned prefix and remove the output, so the fetch has to
    // pick the partial up. This stands in for "a previous run was interrupted":
    // the on-disk state is byte-identical either way, and constructing it
    // directly makes the billed-bytes comparison exact.
    std::fs::remove_file(&out).context("remove output before resume")?;
    let seeded = SEEDED_GROUPS * CHUNK_GROUP;
    std::fs::write(&partial, &blob[..seeded]).context("seed genuine partial")?;

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

    // ---- Journey 3: a corrupt partial is caught and discarded ----
    //
    // Right length, wrong bytes: the tail still verifies group-by-group on the
    // wire, so ONLY the whole-file check can catch this.
    std::fs::remove_file(&out).context("remove output before corrupt run")?;
    let mut bad = blob[..seeded].to_vec();
    bad[0] ^= 0xff;
    std::fs::write(&partial, &bad).context("seed corrupt partial")?;

    let output =
        tokio::process::Command::from(decdn_command(client_dir.path(), KEYSTORE_PASSWORD)?)
            .arg("fetch")
            .args(&args)
            .output()
            .await
            .context("spawn decdn fetch over a corrupt partial")?;
    anyhow::ensure!(
        !output.status.success(),
        "a fetch resuming onto a corrupt prefix must fail, not write a wrong output file"
    );
    anyhow::ensure!(
        !out.exists(),
        "a failed verification must not leave an output file behind"
    );
    anyhow::ensure!(
        !partial.exists(),
        "a corrupt .partial must be discarded, or every retry resumes onto the same corruption"
    );

    // ---- Journey 4: a partial from a DIFFERENT (larger) blob ----
    //
    // The trap journey 3 does not cover, and the two look adjacent while behaving
    // oppositely. A same-length corrupt prefix passes the response floor, reaches
    // the whole-file hash, and self-heals. A prefix LONGER than the blob never
    // gets that far: the node cannot serve a resume past its own `total_bytes`, so
    // the pull is refused before a byte moves. That refusal is not a voucher
    // rejection, so nothing reseeds — and if the partial is then left in place,
    // every subsequent run recomputes the same impossible offset and fails
    // identically, forever, with an error blaming the node for a local stale file.
    //
    // Mundane trigger: fetch a big blob to `-o out.bin`, interrupt it, then fetch a
    // smaller one to the same `-o`.
    std::fs::write(&partial, vec![0xABu8; blob.len() * 2]).context("seed oversized partial")?;
    anyhow::ensure!(!out.exists(), "output should not exist before journey 4");

    run_fetch_until_ready(client_dir.path(), &args).await?;

    let got = std::fs::read(&out).context("read output after an oversized partial")?;
    anyhow::ensure!(
        got == blob,
        "a partial belonging to another blob must be discarded and the fetch restarted clean, \
         not wedged: got {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        !partial.exists(),
        "the .partial must be promoted away after recovering from a stale one"
    );

    // `NodeFixture` tears the daemon down on drop; there is nothing to await.
    drop(node);
    Ok(())
}

/// Cumulative bytes the buyer has paid the provider for, read from the persisted
/// channel watermark. The store is reopened per call because the CLI subprocess
/// owns it between calls.
fn billed_bytes(
    data_dir: &std::path::Path,
    provider: alloy::primitives::Address,
) -> anyhow::Result<u64> {
    // Before the first fetch there is no store yet — nothing has been billed.
    let Ok(store) = RedbBuyerChannelStore::open(data_dir) else {
        return Ok(0);
    };
    let Some(state) = store.get_by_provider(provider).context("read channel")? else {
        return Ok(0);
    };
    Ok(u64::try_from(state.last_bytes_delivered).unwrap_or(u64::MAX))
}

/// Run `decdn fetch`, retrying until the node's chain watcher has observed the
/// freshly-opened channel (`decdn fetch` has no internal retry for that race).
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
/// config file is needed. `--capacity-bond-address` is omitted deliberately: the
/// node holds the blob, so the (unbound) fetch never needs reactive origin
/// pull-through.
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
        "--initial-deposit-micro-usdc".into(),
        DEPOSIT_MICRO_USDC.to_string(),
    ]
}
