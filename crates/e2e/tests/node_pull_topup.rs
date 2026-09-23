//! Live anvil-backed e2e for the daemon's node-to-node cache-miss buyer leg
//! REACTIVE MID-PULL TOP-UP (#1598, follow-up to #1530).
//!
//! The loopback suite (`crates/node/tests/node_origin_pull.rs`,
//! `a_pull_larger_than_the_working_deposit_tops_up_once_and_completes` and
//! siblings) drives the reactive top-up through a `FundingOpener` double whose
//! `top_up_pool_by` just adds to an in-memory cell. That proves the DECISION —
//! detect exhaustion, resume at the paid frontier, do not re-pay delivered
//! bytes — but by construction it cannot exercise three things that only exist
//! against a real chain and a real upstream daemon:
//!
//!   1. A real on-chain `topUp` receipt. `decdn_client::buyer_pool::top_up`
//!      submits and awaits `get_receipt()`; here that transaction actually mines
//!      and raises the pool's escrow.
//!   2. The upstream's chain-watcher lag. The serving seeder refuses to serve the
//!      resumed leg until ITS watcher observes the raised deposit
//!      (`event_poll_interval_ms`, 500ms in this fixture), so the resume budget
//!      is spent against a real watcher rather than an immediately-accepting
//!      double.
//!   3. The seller-side pre-serve deposit gate (#1518), which is what the whole
//!      buyer↔seeder handshake rides through as the deposit is topped up.
//!
//! This is the FIRST node-to-node paid-pull top-up e2e; `cli_fetch_topup.rs` is
//! its CLI-buyer twin. The topology mirrors `node_to_node_coalesce.rs` (a cold
//! pull-through SERVER that holds nothing, satisfying a client miss only by a
//! paid pull from a SEEDER that holds the blob), with two differences that force
//! the reactive branch:
//!
//!   * the SERVER's `buyer_working_deposit_micro_usdc` is shrunk BELOW the blob's
//!     wire cost, so its upstream pull exhausts the deposit mid-stream; and
//!   * both hops price at the same rate, because the ADR 041 buy-margin gate
//!     (`serve_economics.policy = "margin"`, the default) caps the SERVER's
//!     upstream buy at its own sell rate — a higher seeder rate is refused as
//!     below-margin, so the CLI test's `HIGH_RATE` trick does not transfer to the
//!     node-to-node leg. The blob is made expensive by size, not by an
//!     asymmetric rate.
//!
//! Gated behind `anvil-e2e` (off by default). Requires `anvil` + `forge` on
//! `PATH` and a prior build of the `decdn-node` binary:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e node_pull_topup
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
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_incentive::payment_pool::PaymentPool;

/// Overall ceiling so an unbounded await fails fast with a clear message. This
/// journey stands up two daemons plus a chain and drives a mid-pull on-chain
/// `topUp`, so it needs the full standard tier rather than a tighter budget.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(300);

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`,
/// chunk-log 4 == 16 KiB chunk groups).
const CHUNK_GROUP: usize = 16 * 1024;

/// 0.001 USDC/MB (1000 µUSDC/MB, at the wire cap `MAX_RATE_PER_MB`), priced
/// identically on BOTH hops. The ADR 041 buy-margin gate
/// (`serve_economics.policy = "margin"`, the default) caps the SERVER's upstream
/// buy at its own sell rate, so an equal seeder rate clears the gate in the
/// market regime while a higher one would be refused as below-margin.
///
/// The rate, the seeder's refundable floor `M` ([`SEEDER_MIN_REMAINING`]), and
/// the SERVER's working deposit ([`SERVER_WORKING_DEPOSIT`]) are all a factor of
/// 500 below the pre-#2036 sizing (rate `500_000`, M `1_000_000`, deposit
/// `4_000_000`) so this whole node-to-node exhaust-mid-pull scenario is preserved
/// unchanged under the lowered `MAX_RATE_PER_MB` (1000): every `µUSDC` quantity in
/// the open-gate and exhaust-mid-blob inequalities scales by the same factor, and
/// the blob is byte-identical.
const RATE_PER_MB: u64 = 1000;

/// The SERVER's node-to-node buyer working deposit, shrunk to 8000 `µUSDC` — far
/// below the blob's ~9040 `µUSDC` wire cost, so the upstream pull exhausts it
/// mid-stream and the reactive top-up fires exactly once (the node's
/// `MAX_REACTIVE_TOPUPS` is 1). Sized above the seeder's pre-serve floor with room
/// to spare: the seeder admits the SERVER's pool only while `remaining − M` covers
/// the reserved credit window (#1518), and this journey sets `M` to
/// [`POOL_MIN_REMAINING`] (2000 `µUSDC`), so the 6000 `µUSDC` of headroom
/// (8000 − 2000) here also absorbs the several concurrent one-chunk floor
/// reservations the gap-driven pull holds on the seeder side at once.
const SERVER_WORKING_DEPOSIT: u64 = 8_000;

/// The refundable floor `M` for this journey, scaled down by the same factor of
/// 500 as the rate and deposit so the open-gate headroom (`working − M ≥ one
/// chunk`) is preserved: 8000 − 2000 = 6000 ≥ 1000. It is set on BOTH nodes,
/// because `M` has two roles here that must agree at the same value:
///
///   * On the SEEDER it is the seller-side pre-serve floor: the seeder admits the
///     SERVER's buyer pool only while `remaining − M` covers the reserved credit
///     window (#1518).
///   * On the SERVER it is the buyer-side `seller_reserve` estimate: the SERVER's
///     pull pacer holds back its estimate of the upstream's `M` (taken from its
///     own `pool_min_remaining_deposit`, since an upstream running this software
///     keeps the same floor) before funding a voucher.
///
/// At the default `M` (1 USDC) the SERVER's pacer would reserve `1_000_000` `µUSDC`
/// out of the scaled 8000 `µUSDC` deposit and could never fund the first voucher, so both
/// nodes are scaled together.
const POOL_MIN_REMAINING: u64 = 2_000;

/// USDC (base units) minted to the SERVER operator so its buyer pool to the
/// seeder can open at [`SERVER_WORKING_DEPOSIT`] and fund a full reactive top-up
/// with headroom to spare. Mirrors `node_to_node_coalesce.rs`.
const SERVER_BUYER_USDC: u64 = 1_000_000_000;

/// The blob under test: 9 MiB of content. At [`RATE_PER_MB`] its whole-blob wire
/// cost (content + interleaved bao proof, ADR 038) is ~9040 `µUSDC` — strictly above
/// the 8000 `µUSDC` working deposit, so the pull must top up. The buy clears the ADR
/// 041 margin gate in the market regime: the SERVER checks the seeder's warming
/// allowance (`serve_economics.warming_budget`, 5 USDC, starting full) before each
/// pull run and debits it only after the run, and the warm-up's 1 MB debit
/// (1000 `µUSDC`) leaves it positive when the blob's pull starts.
const BLOB_BYTES: usize = 9 * 1024 * 1024;

/// Deterministic pseudo-random blob spanning many chunk groups, so a delivery
/// exercises interior groups and a ragged right edge.
fn make_blob(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = 0x51ED_270B;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

/// Poll the SEEDER's lane store for the buyer pool funded by the SERVER operator
/// and return its `pool_id`, so the journey can read the SERVER's upstream pool
/// escrow on-chain. A lane appears only once the seeder has intaken the SERVER's
/// first voucher (the warm-up delivery), so this resolves promptly after set-up.
async fn server_upstream_pool_id(
    seeder: &NodeFixture,
    server_operator: alloy::primitives::Address,
) -> anyhow::Result<B256> {
    let admin = seeder.admin_client()?;
    let payer_hex = server_operator.to_string();
    let pool_hex = decdn_e2e::poll(Duration::from_secs(30), || async {
        let resp = admin.lanes().await.context("admin lanes")?;
        Ok(resp
            .lanes
            .into_iter()
            .find(|s| s.counterparty.eq_ignore_ascii_case(&payer_hex))
            .map(|s| s.pool_id))
    })
    .await?
    .with_context(|| format!("seeder never reported a buyer lane funded by {payer_hex}"))?;
    pool_hex
        .parse::<B256>()
        .with_context(|| format!("seeder lane pool_id is not a B256: {pool_hex}"))
}

/// A client fetch whose node-to-node pull costs more than the SERVER's working
/// deposit completes byte-exact, after exactly one on-chain reactive top-up.
///
/// The blob is large enough that the fused serve-miss runs well past the ramped
/// credit window's floor, so the journey also rides the serve-demand path: the
/// SERVER's pull fetches the span its serve encoder waits on even when its own
/// window has closed.
///
/// Run the daemons with `DECDN_NODE_LOG="warn,decdn_node=debug"` to see their
/// debug logs through the harness.
#[tokio::test(flavor = "multi_thread")]
async fn node_pull_larger_than_working_deposit_tops_up_once_and_completes() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("node-to-node reactive top-up e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's \
              on-chain/daemon state, so decomposing it would thread state through helpers \
              without reducing the journey's length or making it easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // A small warm-up blob and the multi-MB blob under test. BOTH live only on
    // the SEEDER (cache-warmed at launch); the SERVER holds neither, so every
    // client fetch is a server miss that only a paid pull from the seeder can
    // satisfy. The warm-up is what resolves the seeder's on-chain view of the
    // SERVER's freshly-opened buyer pool BEFORE the deposit-exhausting fetch, so
    // that fetch is not racing the seeder's pool-view catch-up.
    let warm = make_blob(3 * CHUNK_GROUP + 7);
    let blob = make_blob(BLOB_BYTES);

    let (seeder, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &[&warm, &blob]).await?;
    let (warm_hash, blob_hash) = {
        let mut it = hashes.into_iter();
        let warm_hash = it.next().context("seeder missing warm hash")?;
        let blob_hash = it.next().context("seeder missing blob hash")?;
        (warm_hash, blob_hash)
    };
    seeder.set_rate_per_mb(RATE_PER_MB).await?;
    // The seeder admits the SERVER's buyer pull pool only while its remaining
    // deposit minus the refundable floor `M` covers the reserved credit window
    // (#1518). Scale `M` down by the same factor as the rate and deposit so the
    // scaled 8000 µUSDC deposit still clears the open gate. (The SERVER's own `M`
    // is scaled to match below, since its pacer uses it as the upstream-reserve
    // estimate it must not spend into.)
    seeder
        .set_pool_min_remaining_deposit(POOL_MIN_REMAINING)
        .await?;

    // Seat the seeder as an authorized origin for a fresh namespace so the
    // SERVER's on-chain `OriginAssignment` directory fallback resolves it on a
    // cache miss. Seated BEFORE the server launches so the server enumerates it
    // at directory bring-up.
    let publisher = PrivateKeySigner::random();
    chain.vet_publisher(publisher.address()).await?;
    let namespace = chain.create_namespace(&publisher).await?;
    chain
        .add_origin(&publisher, namespace, seeder.operator_addr())
        .await?;

    // The SERVER: an empty bonded cache node whose misses use paid node-to-node
    // pull-through. It is given the seeder as an iroh discovery peer, priced at
    // the same rate as the seeder (so its buy clears the ADR 041 margin gate),
    // and its buyer working deposit is shrunk below the blob's wire cost so the
    // upstream pull must reactively top up. Finally it is funded as a buyer so
    // that top-up can actually escrow USDC.
    let server = NodeFixture::launch_pull_through_cache(&chain, "US", &[&seeder]).await?;
    server.set_rate_per_mb(RATE_PER_MB).await?;
    server
        .set_buyer_working_deposit(SERVER_WORKING_DEPOSIT)
        .await?;
    // The SERVER's pull pacer holds back its estimate of the upstream's refundable
    // floor `M` before funding each voucher, and takes that estimate from its own
    // `pool_min_remaining_deposit`. Scale it to match the seeder's `M`: at the
    // default (1 USDC) the pacer would reserve 1_000_000 µUSDC out of the 8000 µUSDC
    // deposit and never fund the first voucher.
    server
        .set_pool_min_remaining_deposit(POOL_MIN_REMAINING)
        .await?;
    chain
        .fund_node_as_buyer(server.operator_addr(), U256::from(SERVER_BUYER_USDC))
        .await?;

    let client = ClientFixture::new(&chain).await?;

    // Warm-up: a node-to-node miss for the small blob. `open_session_in_namespace`
    // retries until the server's serve path delivers, which requires the server's
    // own upstream pull from the seeder to succeed — so on return, the server's
    // buyer pool is open AND the seeder has served it at least once (its pool-view
    // is warm). The warm blob is tiny relative to the working deposit, so this
    // costs a rounding error and never itself trips the reactive top-up.
    let (mut session, warm_got) = client
        .open_session_in_namespace(&chain, &server, warm_hash, namespace)
        .await
        .context("client session set-up (node-to-node warm-up)")?;
    anyhow::ensure!(warm_got == warm, "warm-up delivered the wrong bytes");

    // Baseline the SERVER's reactive-top-up counter AFTER the warm-up, so the
    // assertion isolates the ONE top-up the deposit-exhausting fetch below must
    // drive. The warm-up cannot have topped up (its cost is far under the working
    // deposit), so this reads 0 — but assert on the DELTA regardless, per the
    // `scrape_metric` contract.
    let topup_before = server
        .scrape_metric("decdn_node_pull_reactive_topup_total")
        .await?;
    let refused_before = server
        .scrape_metric("decdn_node_pull_reactive_topup_refused_total")
        .await?;

    // The proof: fetch the blob whose wire cost exceeds the server's working
    // deposit. The server misses, pulls from the seeder, exhausts its deposit
    // mid-stream, tops up on-chain, and resumes at the paid frontier to complete
    // — all inside this one client fetch. The seeder's pool-view is already warm
    // (from the warm-up), so a single `fetch_once` suffices: any error here is a
    // real verdict, not a readiness race.
    let got = client
        .fetch_once(&mut session, blob_hash, 0, namespace)
        .await
        .context("deposit-exhausting node-to-node fetch")?;

    // (1) The client received the full blob, byte-exact — the resumed pull
    // delivered every byte across the top-up.
    anyhow::ensure!(
        got == blob,
        "the fetch must complete byte-exact after the reactive top-up: got {} bytes, expected {}",
        got.len(),
        blob.len()
    );

    // (2) EXACTLY ONE reactive top-up funded the pull, and none was refused. A
    // delta of 0 would mean the deposit was never exhausted (mis-sized blob); a
    // delta of 2 would mean the node's `MAX_REACTIVE_TOPUPS` bound broke.
    let topup_after = server
        .scrape_metric("decdn_node_pull_reactive_topup_total")
        .await?;
    anyhow::ensure!(
        topup_after == topup_before + 1,
        "the deposit-exhausting pull must reactively top up exactly once \
         (expected {} -> {}, got {topup_after})",
        topup_before,
        topup_before + 1
    );
    let refused_after = server
        .scrape_metric("decdn_node_pull_reactive_topup_refused_total")
        .await?;
    anyhow::ensure!(
        refused_after == refused_before,
        "a healthy top-up must not tick the refused counter (before {refused_before}, after \
         {refused_after})"
    );

    // (3) The SERVER ends up holding the blob it pulled — the miss was filled
    // into its cache, not just streamed through. Its probe now answers
    // `has_blob: true` for the hash.
    let probe = client
        .probe(&server, blob_hash)
        .await
        .context("probe server for the pulled blob")?;
    anyhow::ensure!(
        probe.body.has_blob,
        "the server must hold the blob it pulled through and topped up for"
    );

    // (4) The top-up was a REAL on-chain transaction: the SERVER's upstream buyer
    // pool escrow rose above the working deposit it opened at. `deposit` is
    // monotonic (openPool sets it, topUp adds to it; redemptions track a separate
    // `totalRedeemed`), so a value above the opening deposit can only mean a
    // `topUp` mined — the property the loopback double, which raises an in-memory
    // cell, can never establish.
    let pool_id = server_upstream_pool_id(&seeder, server.operator_addr()).await?;
    // Any signer builds a valid read-only provider; `getPool` needs no funds or
    // gas, so reuse the publisher rather than the client's `Arc`-wrapped key.
    let pool = PaymentPool::new(chain.addrs().payment_pool, chain.provider_for(&publisher));
    let onchain_deposit = pool
        .getPool(pool_id)
        .call()
        .await
        .context("read the server's upstream pool on-chain")?
        .deposit;
    anyhow::ensure!(
        U256::from(onchain_deposit) > U256::from(SERVER_WORKING_DEPOSIT),
        "the reactive top-up must have raised the server's upstream pool escrow on-chain above \
         its opening working deposit ({SERVER_WORKING_DEPOSIT} µUSDC), got {onchain_deposit}"
    );

    drop(server);
    drop(seeder);
    Ok(())
}
