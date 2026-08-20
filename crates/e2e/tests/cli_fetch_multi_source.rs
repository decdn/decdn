//! Live anvil-backed e2e for the CLI `decdn fetch` **multi-source parallel
//! fetch** path (ADR 039), driving the shipped `decdn` binary against TWO real
//! nodes over real paid `cdn/client/v1` streams.
//!
//! Why the whole binary rather than a unit test: the scheduler/segmentation
//! unit tests in `decdn-client-pull` already prove tail-stealing, reassembly,
//! and per-lane pacing work against `ScriptedSource` — a source built to
//! deterministically prove the payee binding BY CONSTRUCTION. What they cannot
//! reach is the part that only exists on the real wire: that the CLI's
//! auto-discovered candidate set, probed and admitted via `admit_sources`,
//! actually opens one `(signer, provider)` payment lane PER holder against the
//! real `PaymentPool` contract, and that BOTH lanes settle a non-zero claim —
//! i.e. the work was genuinely split across two independent daemons, not
//! served by one while the other sat idle.
//!
//! Two independently-bonded nodes ("holder A" and "holder B") are launched,
//! each pre-warmed with the SAME blob (so they share a hash and are both
//! admissible sources). The client never pins `--node-id`, so `decdn fetch`
//! auto-discovers both via the on-chain `CapacityBond` registry, probes them,
//! and — the blob cleared past a (test-lowered) `--multi-source-min-bytes`
//! floor and two admissible holders found — engages
//! `decdn_client_pull::multi_source_fetch` (`try_multi_source_fetch` in
//! `crates/cli/src/commands/fetch.rs`) instead of the single-source
//! failover loop.
//!
//! What this test asserts:
//!   1. The fetched output is byte-identical to the source blob.
//!   2. BOTH nodes' admin `lanes()` report a non-zero settled claim from the
//!      client's address — proof neither lane was a no-op fallback and the
//!      transfer was genuinely parallelized across the two holders.
//!   3. The client's own persisted buyer-pool store shows a non-zero
//!      cumulative watermark against BOTH node operators, on the SAME pool id
//!      — the ADR 039 payment model (one pool, one lane per provider), not two
//!      separate pools.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of both `decdn` and `decdn-node`:
//!
//! ```bash
//! cargo build -p decdn-cli -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_fetch_multi_source
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
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "multi-source-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule): two
/// daemons plus a chain, but no long internal poll ladder.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`,
/// chunk-log 4 == 16 KiB chunk groups).
const CHUNK_GROUP: usize = 16 * 1024;

/// Multi-source engagement floor for this journey (`--multi-source-min-bytes`),
/// lowered far below the production 64 MiB default so the test blob clears it
/// in well under a second of transfer. The gate logic (`should_multi_source`)
/// is size-relative, not size-absolute, so a lowered floor with a blob
/// comfortably above it still exercises the exact same gate the production
/// default guards.
const MULTI_SOURCE_MIN_BYTES: u64 = 200_000;

/// Deterministic pseudo-random blob comfortably above [`MULTI_SOURCE_MIN_BYTES`]
/// (~640 KiB), spanning many chunk groups plus a ragged final group.
fn make_blob() -> Vec<u8> {
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

#[tokio::test(flavor = "multi_thread")]
async fn cli_fetch_engages_multi_source_across_two_holders() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli fetch multi-source e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    // Fail fast (before anvil starts) if `cargo build -p decdn-cli` hasn't run.
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // BOTH nodes are pre-warmed with the SAME blob bytes, so they share a hash
    // and are both admissible sources — the gate `try_multi_source_fetch`
    // checks (`admit_sources` + `should_multi_source`) needs at least two
    // distinct-operator holders of the exact hash requested.
    let blob = make_blob();
    let blob_hash = Hash::new(&blob);
    let (holder_a, hashes_a) =
        NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice()]).await?;
    anyhow::ensure!(
        hashes_a.first() == Some(&blob_hash),
        "holder A seeded blob hash mismatch: {:?} vs {blob_hash}",
        hashes_a.first()
    );
    let (holder_b, hashes_b) =
        NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice()]).await?;
    anyhow::ensure!(
        hashes_b.first() == Some(&blob_hash),
        "holder B seeded blob hash mismatch: {:?} vs {blob_hash}",
        hashes_b.first()
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
    // BOTH lanes this fetch opens, so it must cover the whole blob across the
    // two of them, plus headroom for the reactive-topup ramp.
    chain
        .mint_usdc(
            buyer_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(8u64),
        )
        .await
        .context("mint buyer USDC")?;

    let out = client_dir.path().join("blob.bin");
    let args = fetch_argv(&chain, &blob_hash, client_dir.path(), &keystore, &out);

    let before_a = billed_bytes(client_dir.path(), holder_a.operator_addr())?;
    let before_b = billed_bytes(client_dir.path(), holder_b.operator_addr())?;
    run_fetch_until_ready(client_dir.path(), &args).await?;

    // (1) Byte-identical output.
    let got = std::fs::read(&out).context("read output")?;
    anyhow::ensure!(
        got == blob,
        "multi-source fetch produced {} bytes, expected {}",
        got.len(),
        blob.len()
    );

    // (2) BOTH nodes' admin lanes report a non-zero settled claim funded by the
    // client, proving neither lane was a dead fallback.
    let paid_a = settled_outstanding(&holder_a, buyer_addr).await?;
    let paid_b = settled_outstanding(&holder_b, buyer_addr).await?;
    anyhow::ensure!(
        paid_a > 0 && paid_b > 0,
        "both holders must report a non-zero settled claim from the client — the transfer was \
         not parallelized across both (A={paid_a}, B={paid_b})"
    );

    // (3) The client's own persisted store shows a non-zero cumulative
    // watermark against BOTH operators, on the SAME pool id — one shared pool,
    // two per-provider lanes (ADR 039 § Payment model), not two separate pools.
    let billed_a =
        billed_bytes(client_dir.path(), holder_a.operator_addr())?.saturating_sub(before_a);
    let billed_b =
        billed_bytes(client_dir.path(), holder_b.operator_addr())?.saturating_sub(before_b);
    anyhow::ensure!(
        billed_a > 0 && billed_b > 0,
        "the client's persisted buyer-pool store must show bytes billed to BOTH providers \
         (A={billed_a}, B={billed_b})"
    );
    let pool_ids = distinct_pool_ids(client_dir.path())?;
    anyhow::ensure!(
        pool_ids.len() == 1,
        "multi-source fetch must fan out across providers on ONE shared pool, found {} \
         distinct pool id(s): {pool_ids:?}",
        pool_ids.len()
    );

    // `NodeFixture` tears each daemon down on drop; there is nothing to await.
    drop(holder_a);
    drop(holder_b);
    Ok(())
}

/// Poll a node's admin `lanes()` for the lane funded by `payer` (the client's
/// address) until it reports a non-zero settled claim, or the budget expires.
/// Mirrors `node_to_node_coalesce`'s `settled_outstanding`: the admin surface
/// reads the persisted lane store, so a short poll rides out the fsync gap
/// between `fetch` returning and the node durably recording the voucher.
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

/// Every distinct pool id present in the client's persisted store, across all
/// tracked pools. Used to assert the multi-source fetch fanned out on ONE
/// shared pool rather than opening a separate pool per provider.
fn distinct_pool_ids(
    data_dir: &std::path::Path,
) -> anyhow::Result<std::collections::HashSet<decdn_incentive::PoolId>> {
    let Ok(store) = RedbBuyerPoolStore::open(data_dir) else {
        return Ok(std::collections::HashSet::new());
    };
    Ok(store
        .load_all()
        .context("load buyer pools")?
        .pools
        .into_iter()
        .map(|pool| pool.pool_id)
        .collect())
}

/// Run `decdn fetch`, retrying until the nodes' chain watchers have observed
/// the freshly-opened pool (`decdn fetch` has no internal retry for that
/// race).
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

/// The `decdn fetch` argv (after the `fetch` subcommand) to pull `hash` via
/// AUTO-DISCOVERY — deliberately no `--node-id`/`--addr`/`--provider-address`,
/// since multi-source only ever runs on the auto-discovered candidate set
/// (`crates/cli/src/commands/fetch.rs`'s `multi_source_target` doc comment).
/// `--capacity-bond-address` is what `resolve_target_node` reads to enumerate
/// the active registry instead. Carries the lowered `--multi-source-min-bytes`
/// floor and an explicit `--max-sources` so the journey's intent is visible in
/// the argv rather than relying on the (already-on) defaults alone.
fn fetch_argv(
    chain: &ChainFixture,
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
