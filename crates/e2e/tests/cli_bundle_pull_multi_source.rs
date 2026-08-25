//! Live anvil-backed e2e for `decdn bundle pull`'s **multi-source parallel
//! fetch** path (ADR 039, #1774): a bundle whose large blob is held by TWO
//! nodes pulls that blob fanned out across both, while small entries in the
//! same bundle stay single-source.
//!
//! Shape: two independently-bonded nodes ("holder A" and "holder B") are each
//! pre-warmed with the SAME large blob; A additionally holds `small-a.bin` and
//! B `small-b.bin`. A local three-entry manifest is pulled with `--jobs 3` and
//! no `--node-id`, so every entry auto-discovers its holders. The large blob
//! clears the (test-lowered) `--multi-source-min-bytes` floor with two
//! admissible holders, so it engages the ordered lock-set + per-lane fan-out;
//! each small blob has exactly ONE holder, so its gate declines and it pulls
//! single-source.
//!
//! What this test asserts:
//!   1. All three outputs are byte-identical to their source blobs.
//!   2. ONE pull invocation bills holder A MORE than `small-a.bin`'s wire bytes
//!      AND holder B more than `small-b.bin`'s — since each small blob can only
//!      be served by its one holder, the excess on each lane is large-blob
//!      delivery: proof the large blob was genuinely split across both.
//!   3. Both nodes' admin `lanes()` report a non-zero settled claim from the
//!      client.
//!   4. The client's persisted buyer-pool store shows ONE pool id — the whole
//!      bundle, fan-out included, draws on the one shared pool (ADR 003).
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of both `decdn` and `decdn-node`:
//!
//! ```bash
//! cargo build -p decdn-cli -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_bundle_pull_multi_source
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
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "bundle-multi-source-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule): two
/// daemons plus a chain, but no long internal poll ladder.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`,
/// chunk-log 4 == 16 KiB chunk groups).
const CHUNK_GROUP: usize = 16 * 1024;

/// Multi-source engagement floor for this journey (`--multi-source-min-bytes`),
/// lowered far below the production 64 MiB default so the large blob clears it
/// while the small blobs stay below it. The gate logic (`should_multi_source`)
/// is size-relative, not size-absolute, so a lowered floor exercises the exact
/// same gate the production default guards.
const MULTI_SOURCE_MIN_BYTES: u64 = 200_000;

/// Deterministic pseudo-random blob comfortably above
/// [`MULTI_SOURCE_MIN_BYTES`] (~640 KiB), spanning many chunk groups plus a
/// ragged final group.
fn make_large_blob() -> Vec<u8> {
    let mut v = vec![0u8; 40 * CHUNK_GROUP + 777];
    let mut x: u32 = 0xC0FF_EE11;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

/// A small blob comfortably BELOW [`MULTI_SOURCE_MIN_BYTES`], so its entry's
/// engagement gate declines on size (it also has only one admissible holder,
/// which declines independently).
fn make_small_blob(seed: u8) -> Vec<u8> {
    let mut v = vec![seed; 6 * CHUNK_GROUP + 111];
    let mut x: u32 = u32::from(seed) << 16 | 0x5EED;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_bundle_pull_engages_multi_source_for_the_large_entry() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli bundle pull multi-source e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's \
              fixtures, so decomposing it would thread state through helpers without reducing \
              the journey's length or making it easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    let large = make_large_blob();
    let small_a = make_small_blob(0xA1);
    let small_b = make_small_blob(0xB2);
    let large_hash = Hash::new(&large);
    let small_a_hash = Hash::new(&small_a);
    let small_b_hash = Hash::new(&small_b);
    anyhow::ensure!(
        (small_a.len() as u64) < MULTI_SOURCE_MIN_BYTES
            && (small_b.len() as u64) < MULTI_SOURCE_MIN_BYTES,
        "small blobs must stay below the engagement floor"
    );

    // BOTH holders carry the large blob (two admissible sources for it); each
    // small blob lives on exactly one, so its entry cannot fan out.
    let (holder_a, hashes_a) =
        NodeFixture::launch_with_blobs(&chain, "US", &[large.as_slice(), small_a.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes_a == vec![large_hash, small_a_hash],
        "holder A seeded blob hashes mismatch: {hashes_a:?}"
    );
    let (holder_b, hashes_b) =
        NodeFixture::launch_with_blobs(&chain, "US", &[large.as_slice(), small_b.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes_b == vec![large_hash, small_b_hash],
        "holder B seeded blob hashes mismatch: {hashes_b:?}"
    );
    anyhow::ensure!(
        holder_a.operator_addr() != holder_b.operator_addr(),
        "the two holders must be distinct operators for admit_sources to admit both"
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
    // One pool fans out to every provider (ADR 003) — a single deposit backs
    // BOTH lanes the large entry opens plus both small entries, so it must
    // cover the whole bundle across the two operators, plus headroom for the
    // reactive-topup ramp.
    chain
        .mint_usdc(
            buyer_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(8u64),
        )
        .await
        .context("mint buyer USDC")?;

    // A local v1 manifest naming all three blobs. `-i` means no manifest blob
    // is fetched first; the entries are pulled directly.
    let out_dir = client_dir.path().join("out");
    let manifest_path = client_dir.path().join("bundle.json");
    let manifest = format!(
        r#"{{"version":1,"entries":[{{"path":"large.bin","hash":"{}"}},{{"path":"small-a.bin","hash":"{}"}},{{"path":"small-b.bin","hash":"{}"}}]}}"#,
        large_hash.to_hex(),
        small_a_hash.to_hex(),
        small_b_hash.to_hex(),
    );
    std::fs::write(&manifest_path, manifest).context("write local manifest")?;

    let args = bundle_pull_argv(
        &chain,
        &manifest_path,
        &out_dir,
        client_dir.path(),
        &keystore,
    );

    // The wire bytes each small blob bills its one holder. A single pull
    // invocation must bill EACH operator MORE than its exclusive small blob:
    // the excess can only be large-blob delivery, which proves the large entry
    // fanned out across both lanes.
    let wire_small_a = whole_blob_wire_bytes(small_a.len() as u64);
    let wire_small_b = whole_blob_wire_bytes(small_b.len() as u64);
    run_pull_until_large_blob_splits(
        client_dir.path(),
        &args,
        &out_dir,
        holder_a.operator_addr(),
        holder_b.operator_addr(),
        wire_small_a,
        wire_small_b,
    )
    .await?;

    // (1) Byte-identical outputs.
    for (name, want) in [
        ("large.bin", &large),
        ("small-a.bin", &small_a),
        ("small-b.bin", &small_b),
    ] {
        let got = std::fs::read(out_dir.join(name)).with_context(|| format!("read {name}"))?;
        anyhow::ensure!(
            got == *want,
            "{name} mismatch: got {} bytes, expected {}",
            got.len(),
            want.len()
        );
    }

    // (2) was the retry-loop condition (see run_pull_until_large_blob_splits).

    // (3) BOTH nodes' admin lanes report a non-zero settled claim funded by
    // the client.
    let paid_a = settled_outstanding(&holder_a, buyer_addr).await?;
    let paid_b = settled_outstanding(&holder_b, buyer_addr).await?;
    anyhow::ensure!(
        paid_a > 0 && paid_b > 0,
        "both holders must report a non-zero settled claim from the client (A={paid_a}, \
         B={paid_b})"
    );

    // (4) ONE pool id in the client's persisted store: the whole bundle —
    // fan-out included — drew on the one shared pool (ADR 003).
    let store = RedbBuyerPoolStore::open(client_dir.path()).context("reopen client store")?;
    let pool_ids: std::collections::HashSet<_> = store
        .load_all()
        .context("load buyer pools")?
        .pools
        .into_iter()
        .map(|pool| pool.pool_id)
        .collect();
    anyhow::ensure!(
        pool_ids.len() == 1,
        "the bundle must fan out on ONE shared pool, found {} distinct pool id(s): \
         {pool_ids:?}",
        pool_ids.len()
    );

    // `NodeFixture` tears each daemon down on drop; there is nothing to await.
    drop(holder_a);
    drop(holder_b);
    Ok(())
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

/// Poll a node's admin `lanes()` for the lane funded by `payer` (the client's
/// address) until it reports a non-zero settled claim, or the budget expires.
/// Mirrors `cli_fetch_multi_source`'s `settled_outstanding`.
async fn settled_outstanding(
    node: &NodeFixture,
    payer: alloy::primitives::Address,
) -> anyhow::Result<u64> {
    let admin = node.admin_client()?;
    let payer_hex = payer.to_string();
    let snapshot = poll(Duration::from_secs(30), || async {
        let resp = admin.lanes().await.context("admin lanes")?;
        Ok(resp
            .lanes
            .into_iter()
            .find(|s| s.counterparty.eq_ignore_ascii_case(&payer_hex))
            .filter(|s| s.outstanding_micro_usdc > 0))
    })
    .await?
    .with_context(|| {
        format!("node never reported a non-zero settled claim funded by {payer_hex}")
    })?;
    Ok(snapshot.outstanding_micro_usdc)
}

/// Cumulative bytes the buyer has paid `provider` for, read from the persisted
/// pool's `(signer, provider)` lane watermark. The store is reopened per call
/// because the CLI subprocess owns it between calls.
fn billed_bytes(
    data_dir: &std::path::Path,
    provider: alloy::primitives::Address,
) -> anyhow::Result<u64> {
    // Before the first pull there is no store yet — nothing has been billed.
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

/// Run `decdn bundle pull` until ONE invocation bills each holder MORE than
/// its exclusive small blob's wire bytes — the excess is large-blob delivery,
/// so that invocation genuinely fanned the large entry out across both lanes.
///
/// The nodes' chain watchers observe the freshly-opened pool asynchronously,
/// and a holder whose watcher has not caught up refuses the stream pre-serve;
/// exit status is therefore not a readiness signal for this journey.
///
/// The deltas are measured PER INVOCATION (before/after baselines around each
/// attempt): a loop that accumulated payments across attempts would pass on two
/// single-source pulls that each paid a different holder, which is exactly the
/// non-parallelized case this journey exists to rule out. Each attempt starts
/// from a clean slate (the whole output dir, staging sidecars included, is
/// removed) so a resumed pull cannot bill one lane for a remainder the
/// previous attempt left.
async fn run_pull_until_large_blob_splits(
    data_dir: &std::path::Path,
    args: &[String],
    out_dir: &std::path::Path,
    op_a: alloy::primitives::Address,
    op_b: alloy::primitives::Address,
    wire_small_a: u64,
    wire_small_b: u64,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        match std::fs::remove_dir_all(out_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).with_context(|| format!("remove {}", out_dir.display()));
            }
        }
        let before_a = billed_bytes(data_dir, op_a)?;
        let before_b = billed_bytes(data_dir, op_b)?;

        let output = tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
            .arg("bundle")
            .arg("pull")
            .args(args)
            .output()
            .await
            .context("spawn decdn bundle pull")?;
        if output.status.success() {
            let billed_a = billed_bytes(data_dir, op_a)?.saturating_sub(before_a);
            let billed_b = billed_bytes(data_dir, op_b)?.saturating_sub(before_b);
            if billed_a > wire_small_a && billed_b > wire_small_b {
                return Ok(());
            }
            tracing::debug!(
                billed_a,
                billed_b,
                wire_small_a,
                wire_small_b,
                "pull succeeded without splitting the large blob; retrying after watcher \
                 catch-up:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        } else {
            tracing::debug!(
                "bundle pull not ready; retrying after watcher catch-up:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "no single `decdn bundle pull` split the large blob across both holders; last \
             stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The `decdn bundle pull` argv (after the `bundle pull` subcommand) to pull a
/// local manifest via AUTO-DISCOVERY — deliberately no
/// `--node-id`/`--addr`/`--provider-address`, since multi-source only ever runs
/// on the auto-discovered candidate set. `--capacity-bond-address` is what
/// discovery enumerates the active registry from (and the EIP-712
/// `verifyingContract` the buyer signs its ADR 005 client identity binding
/// against). Carries the lowered `--multi-source-min-bytes` floor and an
/// explicit `--max-sources`/`--jobs` so the journey's intent is visible in the
/// argv rather than relying on defaults.
fn bundle_pull_argv(
    chain: &ChainFixture,
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
        "--jobs".into(),
        "3".into(),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
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
        "--multi-source".into(),
        "--max-sources".into(),
        "4".into(),
        "--multi-source-min-bytes".into(),
        MULTI_SOURCE_MIN_BYTES.to_string(),
    ]
}
