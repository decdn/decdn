//! B3.5 money proof — two real paying clients coalesce onto ONE serve-miss fill,
//! each paying its OWN egress on its OWN pool lane (#1656).
//!
//! Two independently-funded `cdn/client/v1` buyers fetch the SAME missing blob
//! from ONE pull-through node CONCURRENTLY. The node runs the range-aware
//! coalescing serve-miss (`CacheEngine::claim_fill`): the first miss OWNS a single
//! upstream fill for the hash, the overlapping second ATTACHES as an observer and
//! streams the same filling cache — each over its OWN send stream, metering its
//! OWN egress against its OWN pool lane and voucher collection. The single fill
//! promotes the blob to the node's cache exactly once.
//!
//! This is an **own-origin** coalescing proof: the node's single upstream fill is
//! its own configured fs origin (the DECISION-A own-origin case — the node eats
//! the origin egress once, a real dollar saving), not a paid upstream peer. The
//! node-to-node *paid* upstream variant is not expressible in this harness: a
//! `NodeFixture` is funded only for its seller/operator role (a TOKEN capacity
//! bond via `ChainFixture::onboard_operator`), never with USDC or a
//! `PaymentPool` approval for a *buyer* role, and there is no fixture to fund a
//! node as an upstream-paying buyer, nor cross-node DHT / on-chain
//! `OriginAssignment` discovery wiring between two `NodeFixture`s. Building either
//! would be net-new harness infrastructure. The own-origin path exercises the
//! IDENTICAL `claim_fill` / `FillSession` coalescing machinery
//! (`serve_via_backend_origin`, the peer twin's own-origin sibling), so the
//! downstream money property this test proves — N independent per-pool ledgers,
//! no cross-client leakage — is exactly the same on both paths.
//!
//! What this test asserts (executable proof of incentive sign-off point (b)):
//!   1. Both concurrent same-hash misses enter the own-origin coalescing serve
//!      tier (`decdn_local_outboard_serves_total` advances by exactly 2 across the
//!      two concurrent fetches — neither was a cache hit).
//!   2. Both clients receive the full blob, byte-exact.
//!   3. The two clients settle on DISTINCT pools, each accruing its OWN egress
//!      payment (`outstanding_micro_usdc > 0`), the two amounts near-equal — so
//!      neither pool was billed the other's bytes (no cross-client leakage: a
//!      leak would show one pool at ~2x and the other at ~0, or one carrying
//!      the sum).
//!   4. Removing the origin and fetching once more from a third client still
//!      delivers the blob byte-exact — proving the concurrent misses promoted a
//!      complete, byte-exact, non-torn cache entry retrievable without the
//!      origin. (The one-vs-two-fills count itself is the node-level test cited
//!      below, not this assertion.)
//!
//! What it does NOT assert, and why (incentive sign-off points (a) and (c)):
//!   - The strict "exactly ONE upstream pull" count and the abandoned-pull leech
//!     bound are discharged at the layer that actually spends by the node-level
//!     test `window_pull_through_concurrent_same_hash_single_upstream_pull`
//!     (asserts a single upstream watermark via a gated stub upstream) and by the
//!     `crates/cache/src/fill_session.rs` unit tests
//!     (`same_range_coalesces_to_one_pull`, `claim_first_owns_second_attaches_same_range`,
//!     `last_observer_leaving_cancels`, `last_observer_leaving_removes_session`).
//!     The own-origin e2e cannot assert the count directly: entering the serve
//!     tier is metered per request (owner AND observer both bump it), and there is
//!     no owner-vs-observer counter, so `decdn_local_outboard_serves_total` alone
//!     cannot distinguish one coalesced fill from two independent ones. This e2e
//!     therefore stays focused on the two-independent-settlements proof, per the
//!     B3.5 brief's sanctioned scope.
//!
//! Gated behind `anvil-e2e` (off by default). Requires `anvil` + `forge` on
//! `PATH` and a prior build of the `decdn-node` binary:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e two_clients_coalesce
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

use alloy::primitives::{B256, U256};
use anyhow::Context;
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;

/// Overall ceiling so an unbounded await fails fast with a clear message.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(300);

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`,
/// chunk-log 4 == 16 KiB chunk groups).
const CHUNK_GROUP: usize = 16 * 1024;

/// Deterministic pseudo-random blob spanning several chunk groups plus a ragged
/// final group, so a delivery exercises interior groups and the right edge.
fn make_blob(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = 0x9E37_79B9;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

/// Read the `outstanding_micro_usdc` (cumulative accrued voucher claim) the node
/// reports for the lane on `pool_id`, polling until a non-zero claim is visible
/// or the budget expires. A settled full delivery has a non-zero cumulative
/// claim; the node records it before the client's `fetch_once` returns, but the
/// admin surface reads the persisted store, so a short poll rides out the fsync
/// gap.
async fn settled_outstanding(node: &NodeFixture, pool_id: B256) -> anyhow::Result<u64> {
    let admin = node.admin_client()?;
    let wanted = pool_id;
    let snapshot = poll(Duration::from_secs(30), || async {
        let resp = admin.lanes().await.context("admin lanes")?;
        Ok(resp
            .lanes
            .into_iter()
            .find(|s| s.pool_id.parse::<B256>().is_ok_and(|id| id == wanted))
            .filter(|s| s.outstanding_micro_usdc > 0))
    })
    .await?
    .with_context(|| format!("node never reported a non-zero settled claim for pool {wanted}"))?;
    Ok(snapshot.outstanding_micro_usdc)
}

#[tokio::test(flavor = "multi_thread")]
async fn two_clients_coalesce_one_upstream_pull_each_pays() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_two_clients_coalesce()))
        .await
        .context("two-clients-coalesce e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(clippy::similar_names)] // pid_a/pid_b, paid_a/paid_b — per-client a/b pairs read clearly
async fn run_two_clients_coalesce() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let node = NodeFixture::launch_pull_through_cache(&chain, "US", &[]).await?;

    // A small warm-up blob (own-origin, with outboard) both clients fetch first
    // during session set-up. Fetching it creates each pool lane in the node's
    // live serve map, so the later concurrent fetches race the miss path from a
    // clean start with NO per-fetch pool-readiness delay — the two misses
    // therefore overlap deterministically rather than one racing ahead while the
    // other is still waiting for the serve path to accept its pool.
    let warm = make_blob(3 * CHUNK_GROUP + 7);
    let warm_hash = node.seed_origin_blob_with_outboard(&warm)?;

    // The multi-MB blob under test: seeded into the fs origin (with its `{H}.obao4`
    // outboard) but NOT the cache, so the first fetch is a real own-origin miss and
    // the streaming/coalescing path is exercised over many voucher intervals.
    let blob = make_blob(256 * CHUNK_GROUP + 123);
    let hash = node.seed_origin_blob_with_outboard(&blob)?;

    let client_a = ClientFixture::new(&chain).await?;
    let client_b = ClientFixture::new(&chain).await?;

    // Open + register both pools (sequentially — this is set-up), each proven
    // live by its warm-up delivery. `open_session` returns only once the node's
    // serve path has actually served this pool, so both are ready to stream.
    let (mut sess_a, _) = client_a
        .open_session(&chain, &node, warm_hash)
        .await
        .context("client A session set-up")?;
    let (mut sess_b, _) = client_b
        .open_session(&chain, &node, warm_hash)
        .await
        .context("client B session set-up")?;
    let pid_a = sess_a.pool_id();
    let pid_b = sess_b.pool_id();
    anyhow::ensure!(
        pid_a != pid_b,
        "the two clients must fund DISTINCT pools (got {pid_a} twice)"
    );

    // Baseline the own-origin serve-tier counter AFTER warm-up: client A's warm-up
    // was an own-origin miss (one tier entry), client B's was a cache hit (none).
    let tier_before = node
        .scrape_metric("decdn_local_outboard_serves_total")
        .await?;

    // The proof: both clients fetch the SAME missing blob CONCURRENTLY. The first
    // miss owns the single own-origin fill; the overlapping second attaches as an
    // observer and streams the same filling cache. Each pays its OWN egress on its
    // OWN pool lane.
    let fa = client_a.fetch_once(&mut sess_a, hash, 0, U256::ZERO);
    let fb = client_b.fetch_once(&mut sess_b, hash, 0, U256::ZERO);
    let (bytes_a, bytes_b) = tokio::try_join!(fa, fb).context("concurrent coalesced fetch")?;

    // (2) Both clients received the full blob, byte-exact.
    anyhow::ensure!(
        bytes_a == blob,
        "client A delivered bytes must hash-match the blob: got {} bytes, expected {}",
        bytes_a.len(),
        blob.len()
    );
    anyhow::ensure!(
        bytes_b == blob,
        "client B delivered bytes must hash-match the blob: got {} bytes, expected {}",
        bytes_b.len(),
        blob.len()
    );

    // (1) Both concurrent misses entered the own-origin coalescing serve tier —
    // neither was served from cache. A delta of exactly 2 proves the two misses
    // were live at the same time (the necessary condition for coalescing); the
    // single-pull guarantee itself is the node-level + unit tests' job (see the
    // module docs).
    let tier_after = node
        .scrape_metric("decdn_local_outboard_serves_total")
        .await?;
    anyhow::ensure!(
        tier_after == tier_before + 2,
        "both concurrent same-hash misses must enter the own-origin serve-miss tier \
         (expected +2, got {tier_before} -> {tier_after})"
    );

    // (3) Two INDEPENDENT per-pool settlements, no cross-client leakage. Each
    // lane accrued its own egress payment; the two amounts are near-equal because
    // each client received the same warm-up + blob bytes. A leak would show one
    // lane carrying ~both deliveries (the sum) and the other near zero.
    let paid_a = settled_outstanding(&node, pid_a).await?;
    let paid_b = settled_outstanding(&node, pid_b).await?;
    anyhow::ensure!(
        paid_a > 0 && paid_b > 0,
        "each client must settle its OWN non-zero egress claim (A={paid_a}, B={paid_b})"
    );
    let (lo, hi) = (paid_a.min(paid_b), paid_a.max(paid_b));
    // Within 25%: each meter counts only THAT client's received bytes, so the two
    // full-blob deliveries settle to near-equal claims. The bound is generous
    // enough for a not-yet-settled trailing closing voucher on one side, but far
    // tighter than the ~2x / ~0 split a cross-client leak would produce.
    anyhow::ensure!(
        hi <= lo + lo / 4,
        "the two per-pool settlements must be near-equal — a large gap means one \
         pool was billed the other's bytes (A={paid_a}, B={paid_b})"
    );

    // (4) The concurrent misses promoted a complete, byte-exact cache entry.
    // Take the origin away and fetch once more from a fresh client: it can only
    // succeed if the coalesced fill committed the blob to the node's cache. This
    // proves the entry is non-torn and complete, not that only one fill ran —
    // that count is the node-level test cited above.
    let hex = hash.to_hex();
    let shard = hex.as_str().get(..2).context("blob hex too short")?;
    let data_path = node.origin_root().join(shard).join(hex.as_str());
    let outboard_path = node
        .origin_root()
        .join(shard)
        .join(format!("{}.obao4", hex.as_str()));
    std::fs::remove_file(&data_path).context("remove seeded origin data object")?;
    std::fs::remove_file(&outboard_path).context("remove seeded origin outboard")?;

    let client_c = ClientFixture::new(&chain).await?;
    let out_c = client_c.fetch(&chain, &node, hash, U256::ZERO).await?;
    anyhow::ensure!(
        out_c.bytes == blob,
        "a post-coalescing fetch with the origin removed must be served byte-exact from the \
         node's cache — the concurrent misses must have promoted a complete, non-torn cache \
         entry"
    );

    Ok(())
}
