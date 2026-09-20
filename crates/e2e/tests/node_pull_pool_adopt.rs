//! Live anvil-backed e2e for the daemon's recovery of a buyer pool its local
//! store has lost (#2072).
//!
//! The buyer store (`buyer.redb`, under `identity.data_dir`) is the node's only
//! local record that it owns a funded `PaymentPool` deposit. A reset data dir —
//! a moved volume, a re-provisioned host — loses it. A node that cannot recover
//! from that opens a *second* deposit beside the first, forgets that one too,
//! and once its wallet is drained every cache-miss pull fails
//! `ERC20: transfer amount exceeds balance` with its own escrow sitting idle
//! on-chain — against ADR 003 §node→node, which says a pool is opened once and
//! reused and that owner funds are never stranded.
//!
//! The unit tests in `crates/node/src/buyer_channel.rs` cover the adoption
//! DECISION (newest `Open` wins, a `Closing` pool is skipped, an unreachable
//! chain adopts nothing) against a mocked transport. Three things only exist
//! against a real chain and a real upstream daemon, and this is what pins them:
//!
//!   1. **The `getPools` / `getPool` / `watermark` bindings decode.** They are
//!      hand-written `sol!` declarations; a wrong type or field order
//!      mis-decodes silently everywhere except here.
//!   2. **No second `openPool` lands.** The owner's on-chain pool count is read
//!      back after the restart, which is the only place the "escrow a second
//!      deposit" bug was ever observable.
//!   3. **A reseeded lane can still buy bytes.** `PoolLedger` signs
//!      `prior + accrued`, so a lane resumed from zero signs cumulatives at or
//!      below the contract's watermark, which `_applyVoucher` treats as
//!      transient-empty. The node would stream real bytes and buy none of them;
//!      only a real seeder redeeming real vouchers shows that.
//!
//! Topology mirrors `node_pull_topup.rs`: a cold pull-through SERVER that holds
//! nothing, satisfying a client miss only by a paid pull from a SEEDER that
//! holds the blob.
//!
//! Run the daemons with `DECDN_NODE_LOG="warn,decdn_node=debug"` to see their
//! debug logs through the harness.

#![cfg(feature = "anvil-e2e")]

use std::time::Duration;

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_incentive::payment_pool::{PaymentPool, enumerate_owned_pools};

/// Overall ceiling so an unbounded await fails fast. This journey stands up two
/// daemons plus a chain, drives two paid pulls and restarts a daemon in between,
/// so it needs the full standard tier.
const OVERALL_TIMEOUT: Duration = Duration::from_mins(5);

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`).
const CHUNK_GROUP: usize = 16 * 1024;

/// 0.001 USDC/MB, at the wire cap `MAX_RATE_PER_MB`, priced identically on both
/// hops so the SERVER's upstream buy clears the ADR 041 margin gate.
const RATE_PER_MB: u64 = 1000;

/// The SERVER's buyer working deposit. Sized generously relative to both blobs
/// so neither pull exhausts it: this journey is about losing the record of the
/// pool, not about running it down, and a reactive top-up would add a second
/// on-chain write to reason about.
const SERVER_WORKING_DEPOSIT: u64 = 1_000_000;

/// The refundable floor `M`, set on both nodes so the seller-side pre-serve gate
/// (#1518) and the buyer's pacer estimate agree. Well under the working deposit
/// so the open gate clears with room.
const POOL_MIN_REMAINING: u64 = 2_000;

/// USDC minted to the SERVER operator. Deliberately generous: an under-funded
/// wallet is its own failure mode, and this journey must fail only if adoption
/// fails.
const SERVER_BUYER_USDC: u64 = 1_000_000_000;

/// Deterministic pseudo-random blob spanning many chunk groups.
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

/// A node that has lost its buyer store adopts the pool it already owns on
/// chain, resumes its lanes from the contract's watermark, and keeps buying —
/// without escrowing a second deposit.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_lost_its_buyer_store_adopts_its_pool_instead_of_opening_a_second()
-> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("buyer-pool adoption e2e exceeded the overall timeout")??;
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

    // Two blobs, both only on the SEEDER. The first drives the pull that opens
    // the SERVER's pool and advances a lane; the second is fetched after the
    // store loss, so it can only be served if the adopted pool still buys.
    // `second` is deliberately SMALLER than `first`. The reseed is what makes the
    // second pull resume ABOVE the watermark the first one left; without it the
    // lane restarts at zero and signs a cumulative priced on `second` alone,
    // which is below that watermark — a stale cumulative `_applyVoucher` pays
    // nothing for. Sizing `second` larger would let a from-zero cumulative clear
    // the watermark by accident and the journey would pass with the reseed gone.
    let first = make_blob(6 * CHUNK_GROUP + 37);
    let second = make_blob(2 * CHUNK_GROUP + 11);

    let (seeder, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &[&first, &second]).await?;
    let (first_hash, second_hash) = {
        let mut it = hashes.into_iter();
        let a = it.next().context("seeder missing first hash")?;
        let b = it.next().context("seeder missing second hash")?;
        (a, b)
    };
    seeder.set_rate_per_mb(RATE_PER_MB).await?;
    seeder
        .set_pool_min_remaining_deposit(POOL_MIN_REMAINING)
        .await?;

    // Seat the seeder as an authorized origin so the SERVER's on-chain
    // `OriginAssignment` directory resolves it on a miss. Before the server
    // launches, so the server enumerates it at directory bring-up.
    let publisher = PrivateKeySigner::random();
    chain.vet_publisher(publisher.address()).await?;
    let namespace = chain.create_namespace(&publisher).await?;
    chain
        .add_origin(&publisher, namespace, seeder.operator_addr())
        .await?;

    let server = NodeFixture::launch_pull_through_cache(&chain, "US", &[&seeder]).await?;
    server.set_rate_per_mb(RATE_PER_MB).await?;
    server
        .set_buyer_working_deposit(SERVER_WORKING_DEPOSIT)
        .await?;
    server
        .set_pool_min_remaining_deposit(POOL_MIN_REMAINING)
        .await?;
    chain
        .fund_node_as_buyer(server.operator_addr(), U256::from(SERVER_BUYER_USDC))
        .await?;

    let client = ClientFixture::new(&chain).await?;

    // First fetch: the SERVER misses, opens its buyer pool, pulls from the
    // seeder and pays it. On return the pool exists on chain and its lane to the
    // seeder carries a non-zero cumulative.
    let (mut session, got_first) = client
        .open_session_in_namespace(&chain, &server, first_hash, namespace)
        .await
        .context("client session set-up (first node-to-node pull)")?;
    anyhow::ensure!(
        got_first == first,
        "the first pull delivered the wrong bytes"
    );
    anyhow::ensure!(
        second.len() < first.len(),
        "this journey needs `second` smaller than `first`, so a lane resumed from zero \
         regresses below the watermark the first pull left (got {} vs {})",
        second.len(),
        first.len()
    );

    let pool_contract = PaymentPool::new(chain.addrs().payment_pool, chain.admin().clone());
    let owner = server.operator_addr();

    let before = enumerate_owned_pools(&pool_contract, owner).await?;
    anyhow::ensure!(
        before.len() == 1,
        "the first pull must open exactly one pool, found {}",
        before.len()
    );
    let original_pool = *before.first().context("pool id")?;

    // The lane the SERVER paid on must have a non-zero on-chain watermark before
    // the store is destroyed — otherwise the reseed below proves nothing, and a
    // zero here would mean the seeder never redeemed and the journey is not
    // actually exercising a paid lane.
    let watermark = decdn_e2e::poll(Duration::from_mins(1), || async {
        let w = pool_contract
            .getWatermark(original_pool, owner, seeder.operator_addr())
            .call()
            .await
            .context("read the SERVER's lane watermark")?;
        Ok((w.amount > 0).then_some(w))
    })
    .await?
    .context("the seeder never redeemed the SERVER's first voucher")?;

    // Destroy the buyer store, exactly as a reset data dir would, and restart.
    // Everything else — keystore, on-chain registration, cache — survives, which
    // is what makes the pool recoverable at all.
    let buyer_store = server.data_dir().join("buyer.redb");
    anyhow::ensure!(
        buyer_store.exists(),
        "expected a buyer store at {}",
        buyer_store.display()
    );
    // Unlinked while the daemon still holds its descriptor: the old process
    // writes on into an inode with no name until `restart` kills it, and the new
    // one finds no store at all — the same view a moved data dir gives it.
    std::fs::remove_file(&buyer_store).context("remove the SERVER's buyer pool store")?;
    server.restart().await?;

    // (1) Nothing was opened at bootstrap. The node opens lazily on a miss, so
    // this alone cannot catch a reverted adoption — assertion (4) is where a
    // second deposit would appear.
    let at_boot = enumerate_owned_pools(&pool_contract, owner).await?;
    anyhow::ensure!(
        at_boot == before,
        "bootstrap must not open a pool (before {before:?}, at_boot {at_boot:?})"
    );

    // (2) The second fetch completes. It can only do so if the adopted pool pays
    // the seeder, which requires the lane to have resumed above the contract's
    // watermark: a lane resumed from zero signs cumulatives `_applyVoucher`
    // treats as transient-empty, so the seeder would be served nothing it could
    // cash and would stop serving.
    let got_second = client
        .fetch_once(&mut session, second_hash, 0, namespace)
        .await
        .context("node-to-node fetch after the buyer store was lost")?;
    anyhow::ensure!(
        got_second == second,
        "the fetch after adoption must complete byte-exact: got {} bytes, expected {}",
        got_second.len(),
        second.len()
    );

    // (3) The lane's on-chain watermark advanced past where it stood before the
    // store was lost — the node really bought those bytes rather than delivering
    // them against vouchers that pay nothing.
    let advanced = decdn_e2e::poll(Duration::from_mins(1), || async {
        let w = pool_contract
            .getWatermark(original_pool, owner, seeder.operator_addr())
            .call()
            .await
            .context("re-read the SERVER's lane watermark")?;
        Ok((w.amount > watermark.amount).then_some(w))
    })
    .await?
    .with_context(|| {
        format!(
            "the lane watermark never advanced past {} after adoption — the resumed lane \
             signed cumulatives the contract pays nothing for",
            watermark.amount
        )
    })?;
    anyhow::ensure!(
        advanced.bytesDelivered > watermark.bytesDelivered,
        "the adopted lane must pay for new bytes (before {}, after {})",
        watermark.bytesDelivered,
        advanced.bytesDelivered
    );
    // The delta is the SECOND blob's own price, not some smaller remainder. A
    // lane resumed from zero could only ever move the watermark by less, because
    // the contract pays the increment over what it already holds.
    let paid = advanced.amount - watermark.amount;
    let expected = price_micro_usdc(second.len());
    anyhow::ensure!(
        paid == expected,
        "the adopted lane must pay the second blob's full price: expected {expected}, \
         paid {paid} (watermark {} -> {})",
        watermark.amount,
        advanced.amount
    );

    // (4) Still exactly one pool. A node that failed to adopt would have opened a
    // second one to serve the fetch above, which is the bug's only on-chain trace.
    let after = enumerate_owned_pools(&pool_contract, owner).await?;
    anyhow::ensure!(
        after == before,
        "a node that lost its buyer store must adopt the pool it owns, not open a second \
         (before {before:?}, after {after:?})"
    );

    Ok(())
}

/// The whole-blob wire price at [`RATE_PER_MB`], rounded up the way the contract
/// and the ledger both round: a partial megabyte is charged.
fn price_micro_usdc(bytes: usize) -> u64 {
    const MB: u128 = 1024 * 1024;
    let cost = (bytes as u128 * u128::from(RATE_PER_MB)).div_ceil(MB);
    u64::try_from(cost).unwrap_or(u64::MAX)
}
