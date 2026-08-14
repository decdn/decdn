//! Money proof — the NODE-TO-NODE PAID variant of `coalesce_two_clients`.
//!
//! Two independently-funded `cdn/client/v1` buyers fetch the SAME missing blob
//! from ONE pull-through node (the SERVER) CONCURRENTLY. The server holds nothing
//! itself — no cached copy, no own fs origin — so its serve-miss can only be
//! satisfied by a PAID node-to-node pull from a second node (the PROVIDER) that
//! does hold the blob. The server runs the range-aware coalescing serve-miss
//! (`serve_via_window_pull_through` → `CacheEngine::claim_fill`): the first miss
//! OWNS the single upstream pull for the hash, the overlapping second ATTACHES as
//! an observer and streams the same filling cache — each downstream client over
//! its OWN send stream, metering its OWN egress against its OWN lane.
//!
//! This closes the gap `coalesce_two_clients` calls out in its header: that proof
//! coalesces on the OWN-ORIGIN path only (the server eats its own fs-origin egress
//! once, no upstream payment), because the harness could not express a node paying
//! an upstream. Two pieces of net-new harness make it expressible here:
//!   * `ChainFixture::fund_node_as_buyer` — mints the server operator the USDC a
//!     buyer pool escrows (the daemon self-approves the `PaymentPool` at
//!     buyer bootstrap, so minting is the fixture's whole job).
//!   * cross-node discovery — the provider is seated as an authorized origin for a
//!     namespace via `OriginAssignment.addOrigin`, and the server is given the
//!     provider as an iroh discovery peer, so the server's directory fallback
//!     resolves the provider to a dialable address on its own cache miss.
//!
//! What this test asserts (the node-to-node money property):
//!   1. Both concurrent same-hash misses coalesce onto EXACTLY ONE completed paid
//!      upstream pull: `decdn_node_pull_success_total` on the SERVER advances by
//!      exactly 1 across the two concurrent fetches. (The observer's attach drops
//!      its own handshake unused — only the owner's `run_pull_leg` scores a
//!      `Delivered` outcome, which is what bumps this counter.)
//!   2. Both clients receive the full blob, byte-exact.
//!   3. The two clients settle on DISTINCT downstream lanes, each accruing its
//!      OWN egress payment, the two amounts near-equal — so neither lane was
//!      billed the other's bytes (no cross-client leakage).
//!   4. The upstream pull was genuinely PAID: the PROVIDER reports exactly one
//!      buyer lane — funded by the server's operator — with a non-zero settled
//!      claim. A single such lane (not two) is the upstream face of the
//!      coalescing: one pull, one lane, paid once.
//!
//! Gated behind `anvil-e2e` (off by default). Requires `anvil` + `forge` on
//! `PATH` and a prior build of the `decdn-node` binary:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e node_to_node_coalesce
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

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_e2e::poll;

/// Overall ceiling so an unbounded await fails fast with a clear message. This
/// journey stands up TWO daemons plus a chain, so it needs the full standard
/// tier rather than a tighter budget.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(300);

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`,
/// chunk-log 4 == 16 KiB chunk groups).
const CHUNK_GROUP: usize = 16 * 1024;

/// USDC (base units) minted to the server operator so its buyer pool to the
/// provider never starves: far above the 0.5 USDC initial + 10 USDC working
/// deposit a single pull can escrow. Mirrors the client fixture's own mint.
const SERVER_BUYER_USDC: u64 = 1_000_000_000;

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

/// Read the `outstanding_micro_usdc` (cumulative accrued voucher claim) a node
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

/// Time to keep observing the provider's lane store AFTER the first paid lane
/// appears, so a second one lagging through the persisted-store fsync window is
/// caught rather than raced past. Sized as several `wait_for_pool` / redeem poll
/// cadences (~500ms), comfortably inside the overall budget.
const UPSTREAM_LANE_SETTLE_WINDOW: Duration = Duration::from_secs(3);

/// Poll the PROVIDER's lane store for the buyer lanes funded by `payer` (the
/// server operator, the pool owner) that have accrued a non-zero settled claim,
/// returning `(count, total_outstanding_micro_usdc)`. Proves the upstream pull
/// moved real money server → provider, and that it did so over exactly ONE lane.
///
/// Returning on the FIRST sighting of any paid lane would undercount: a SECOND
/// paid lane — the signature of a coalescing regression that opened two upstream
/// pulls — can surface a beat later through the same fsync window, and a single
/// read would race past it and let a caller's `== 1` assertion pass falsely. So
/// this waits for at least one paid lane, then keeps observing for
/// [`UPSTREAM_LANE_SETTLE_WINDOW`] and returns the MAXIMUM paid-lane count seen —
/// a lagging second lane then correctly fails the assertion. The total is the sum
/// at that maximum.
async fn provider_lanes_from(
    provider: &NodeFixture,
    payer: Address,
) -> anyhow::Result<(usize, u64)> {
    let admin = provider.admin_client()?;
    let payer_hex = payer.to_string();

    // (1) Ride out the fsync window: wait until at least one paid lane funded by
    // `payer` is visible before starting to count.
    poll(Duration::from_secs(30), || async {
        let resp = admin.lanes().await.context("admin lanes")?;
        let any_paid = resp.lanes.iter().any(|s| {
            s.counterparty.eq_ignore_ascii_case(&payer_hex) && s.outstanding_micro_usdc > 0
        });
        Ok(any_paid.then_some(()))
    })
    .await?
    .with_context(|| {
        format!("provider never reported a non-zero settled claim funded by {payer_hex}")
    })?;

    // (2) Observe a stability window and take the max count seen, so a second
    // paid lane that surfaces late is not missed.
    let mut max_count = 0usize;
    let mut total_at_max = 0u64;
    let deadline = tokio::time::Instant::now() + UPSTREAM_LANE_SETTLE_WINDOW;
    loop {
        let resp = admin.lanes().await.context("admin lanes")?;
        let paid: Vec<u64> = resp
            .lanes
            .into_iter()
            .filter(|s| {
                s.counterparty.eq_ignore_ascii_case(&payer_hex) && s.outstanding_micro_usdc > 0
            })
            .map(|s| s.outstanding_micro_usdc)
            .collect();
        // `>=` so a growing cumulative claim on a stable count still refreshes the
        // reported total to the latest reading.
        if paid.len() >= max_count {
            max_count = paid.len();
            total_at_max = paid.iter().sum();
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Ok((max_count, total_at_max))
}

#[tokio::test(flavor = "multi_thread")]
async fn two_clients_coalesce_one_paid_upstream_pull() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_node_to_node_coalesce()))
        .await
        .context("node-to-node coalesce e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(clippy::similar_names)] // pid_a/pid_b, paid_a/paid_b — per-client a/b pairs read clearly
async fn run_node_to_node_coalesce() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // A small warm-up blob both clients fetch during session set-up, and the
    // multi-MB blob under test. BOTH live only on the PROVIDER (cache-warmed at
    // launch); the server holds neither, so every client fetch is a server miss
    // that can only be satisfied by a paid pull from the provider.
    let warm = make_blob(3 * CHUNK_GROUP + 7);
    let blob = make_blob(256 * CHUNK_GROUP + 123);

    let provider = NodeFixture::launch_with_blobs(&chain, "US", &[&warm, &blob]).await?;
    let (warm_hash, hash) = {
        let mut hashes = provider.1.into_iter();
        let warm_hash = hashes.next().context("provider missing warm hash")?;
        let hash = hashes.next().context("provider missing blob hash")?;
        (warm_hash, hash)
    };
    let provider = provider.0;

    // Seat the provider as an authorized origin for a fresh namespace, so the
    // server's on-chain `OriginAssignment` directory fallback resolves it on a
    // cache miss. The namespace owner must be a vetted publisher. Seated BEFORE
    // the server launches so the server enumerates it at directory bring-up.
    let publisher = PrivateKeySigner::random();
    chain.vet_publisher(publisher.address()).await?;
    let namespace = chain.create_namespace(&publisher).await?;
    chain
        .add_origin(&publisher, namespace, provider.operator_addr())
        .await?;

    // The SERVER: an empty bonded cache node whose misses use paid node-to-node
    // pull-through. It is given the provider as an iroh discovery peer (so it can
    // dial the directory-resolved provider) and funded as a buyer (so it can pay).
    let server = NodeFixture::launch_pull_through_cache(&chain, "US", &[&provider]).await?;
    chain
        .fund_node_as_buyer(server.operator_addr(), U256::from(SERVER_BUYER_USDC))
        .await?;

    let client_a = ClientFixture::new(&chain).await?;
    let client_b = ClientFixture::new(&chain).await?;

    // Open + register both downstream pools (sequentially — this is set-up),
    // each proven live by its warm-up delivery. The warm-up must carry the
    // provider's namespace: the server holds the warm blob no more than the main
    // one, so its warm-up is itself a node-to-node miss that only discovers the
    // provider under that namespace. `open_session_in_namespace` returns once the
    // server's serve path has actually served this pool.
    let (mut sess_a, _) = client_a
        .open_session_in_namespace(&chain, &server, warm_hash, namespace)
        .await
        .context("client A session set-up")?;
    let (mut sess_b, _) = client_b
        .open_session_in_namespace(&chain, &server, warm_hash, namespace)
        .await
        .context("client B session set-up")?;
    let pid_a = sess_a.pool_id();
    let pid_b = sess_b.pool_id();
    anyhow::ensure!(
        pid_a != pid_b,
        "the two clients must fund DISTINCT pools (got {pid_a} twice)"
    );

    // Baseline the server's upstream-pull-success counter AFTER warm-up: client
    // A's warm-up was a server miss that pulled once from the provider; client
    // B's warm-up was then a server cache HIT (no pull). So exactly one pull has
    // succeeded so far; the concurrent main fetch must add exactly one more.
    let pull_before = server
        .scrape_metric("decdn_node_pull_success_total")
        .await?;

    // The proof: both clients fetch the SAME missing blob CONCURRENTLY. The first
    // miss owns the single upstream pull from the provider; the overlapping second
    // attaches as an observer and streams the same filling cache. Each pays its
    // OWN egress on its OWN downstream lane.
    let fa = client_a.fetch_once(&mut sess_a, hash, 0, namespace);
    let fb = client_b.fetch_once(&mut sess_b, hash, 0, namespace);
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

    // (1) EXACTLY ONE paid upstream pull delivered across the two concurrent
    // misses. Only the coalescing owner's `run_pull_leg` reaches a `Delivered`
    // outcome; the observer attaches and drops its own handshake unused. A delta
    // of 2 would mean the two misses each opened and completed their own paid
    // pull — the double-spend coalescing exists to prevent.
    let pull_after = server
        .scrape_metric("decdn_node_pull_success_total")
        .await?;
    anyhow::ensure!(
        pull_after == pull_before + 1,
        "two concurrent same-hash misses must coalesce onto exactly ONE paid upstream pull \
         (expected +1, got {pull_before} -> {pull_after})"
    );

    // (3) Two INDEPENDENT downstream per-lane settlements, no cross-client
    // leakage. Each lane accrued its own egress payment; the two amounts are
    // near-equal because each client received the same warm-up + blob bytes. A
    // leak would show one lane carrying ~both deliveries and the other ~zero.
    let paid_a = settled_outstanding(&server, pid_a).await?;
    let paid_b = settled_outstanding(&server, pid_b).await?;
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
        "the two downstream settlements must be near-equal — a large gap means one lane was \
         billed the other's bytes (A={paid_a}, B={paid_b})"
    );

    // (4) The upstream pull was genuinely PAID, and over exactly ONE lane. The
    // provider reports a single buyer lane funded by the server operator with a
    // non-zero settled claim — the coalesced pull's single voucher stream. Two
    // lanes here would mean the server opened a second upstream pull; zero would
    // mean it never paid at all (an own-origin fill, not the node-to-node path).
    let (upstream_lanes, upstream_paid) =
        provider_lanes_from(&provider, server.operator_addr()).await?;
    anyhow::ensure!(
        upstream_lanes == 1,
        "the coalesced pull must settle over EXACTLY ONE server->provider lane, got \
         {upstream_lanes}"
    );
    anyhow::ensure!(
        upstream_paid > 0,
        "the provider must have been PAID for the upstream pull (settled claim was {upstream_paid})"
    );

    Ok(())
}
