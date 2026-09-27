//! Live anvil-backed e2e for a node repointed at a redeployed `PaymentPool`
//! (#2088).
//!
//! `PaymentPool` derives `poolId = keccak256(owner, ownerPoolNonce)`. Neither
//! the contract address nor the chain id is an input, and a fresh deployment
//! restarts every owner's nonce at zero. The same owner's Nth pool therefore has
//! a byte-identical id on every deployment, so a buyer row the node wrote
//! against deployment #1 names a pool that deployment #2 also mints.
//!
//! The node tags each buyer row with the `PaymentPool` it was written against.
//! At bootstrap it drops a row from another deployment before it enumerates the
//! configured one, and it logs that drop at WARN:
//!
//! > the tracked buyer pool belongs to a different PaymentPool deployment;
//! > dropping the stale row
//!
//! with `pool_id`, `foreign_payment_pool` and `configured_payment_pool` fields.
//! The unit tests in `crates/node/src/buyer_channel.rs` cover that decision
//! against a mocked transport. This file runs it against two real deployments of
//! the same contract, which is the only place the hand-written `sol!` bindings
//! meet two same-shaped contracts that differ by address alone:
//!
//!   1. **An empty redeploy.** The node drops the #1 row, opens nothing at boot,
//!      and its first cache-miss pull opens a fresh pool on #2. That pool has the
//!      #1 id, but its lane starts at zero.
//!   2. **A redeploy that reissued the tracked id.** The node's key already owns
//!      an open pool on #2 at the tracked id. The node adopts it with the #2
//!      deposit and no lanes, and the next pull pays #2 only for the bytes it
//!      delivers — not the #1 cumulative on top.
//!
//! Topology: a pull-through SERVER that holds nothing buys each client miss from
//! a SEEDER. Leg 1 runs on #1 through SEEDER #1. Leg 2 runs on #2 through a
//! fresh SEEDER #2 that has only ever known #2. The fresh seeder is deliberate:
//! the seller-side lane store is keyed without the deployment too (#2181), and
//! a repointed SEEDER #1 would reject the SERVER's first #2 voucher against its
//! #1 frontier. Each leg's blob lives in its own namespace, seated on its own
//! seeder, so the directory never routes a leg-2 miss to SEEDER #1.
//!
//! The drop assertion reads the SERVER's captured log, so `DECDN_NODE_LOG` must
//! keep `decdn_node` at `warn` or more verbose. Run with
//! `DECDN_NODE_LOG="warn,decdn_node=debug"` to see the daemons' debug logs
//! through the harness.

#![cfg(feature = "anvil-e2e")]

use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::DynProvider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_common::admin::{AdminRpcClient, BuyerPoolSnapshot};
use decdn_e2e::bindings::Erc20;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::ensure_decdn_cli_built;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_incentive::payment_pool::{PaymentPool, enumerate_owned_pools};

/// Overall ceiling so an unbounded await fails fast. Each journey stands up
/// three daemons plus a chain and deploys a second `PaymentPool`, then drives
/// two paid pulls around a repoint. Its sequential poll budgets — two redemption
/// waits, the log wait and the admin-store waits — run past the standard tier's
/// ~150s rule, so it takes the heavy tier.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::HEAVY;

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`).
const CHUNK_GROUP: usize = 16 * 1024;

/// 0.001 USDC/MB, at the wire cap `MAX_RATE_PER_MB`, priced identically on every
/// hop so the SERVER's upstream buy clears the ADR 041 margin gate.
const RATE_PER_MB: u64 = 1000;

/// The SERVER's buyer working deposit on #1: what its leg-1 pool escrows.
const FIRST_WORKING_DEPOSIT: u64 = 1_000_000;

/// The SERVER's buyer working deposit from the repoint on: what a fresh open on
/// #2 escrows. Distinct from [`FIRST_WORKING_DEPOSIT`] so a #2 deposit that
/// reads as the #1 figure is the stale row, not a coincidence.
const REDEPLOY_WORKING_DEPOSIT: u64 = 1_500_000;

/// The deposit of the pool the test opens on #2 under the SERVER's key. Distinct
/// from both working deposits so the adopted row's deposit names its source.
/// Above [`REDEPLOY_WORKING_DEPOSIT`], so reusing it never triggers a refill that
/// would move the figure.
const PREOPENED_DEPOSIT: u64 = 2_000_000;

/// The refundable floor `M`, set on every node so the seller-side pre-serve gate
/// (#1518) and the buyer's pacer estimate agree.
const POOL_MIN_REMAINING: u64 = 2_000;

/// USDC minted to the SERVER operator. Generous, so an under-funded wallet
/// never stands in for the failure under test.
const SERVER_BUYER_USDC: u64 = 1_000_000_000;

/// How long a daemon gets to reach a post-restart state or redeem a voucher.
const SETTLE: Duration = Duration::from_mins(1);

/// The WARN the SERVER logs when bootstrap drops a row from another deployment.
const FOREIGN_ROW_DROPPED: &str =
    "the tracked buyer pool belongs to a different PaymentPool deployment";

/// The WARN the SERVER logs when a pull meets a foreign row that bootstrap
/// failed to drop. Never expected here: bootstrap's drop is the whole guard.
const FOREIGN_ROW_IGNORED: &str =
    "ignoring a tracked buyer pool from another PaymentPool deployment";

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

/// The bytes a whole-blob pull of `blob` puts on the wire: content plus bao
/// proof, the figure a lane meters (ADR 038 §Payment metering).
fn wire_bytes(blob: &[u8]) -> anyhow::Result<u64> {
    let len = u64::try_from(blob.len()).context("blob length")?;
    Ok(decdn_bao_range::bao_encoded_size(
        len,
        &bao_tree::ChunkRanges::all(),
    ))
}

/// The price of `wire` bytes at [`RATE_PER_MB`], rounded up the way the contract
/// and the ledger both round: a partial megabyte is charged.
fn price_micro_usdc(wire: u64) -> u64 {
    const MB: u128 = 1024 * 1024;
    let cost = (u128::from(wire) * u128::from(RATE_PER_MB)).div_ceil(MB);
    u64::try_from(cost).unwrap_or(u64::MAX)
}

/// A node repointed at an empty redeploy drops its #1 row, opens nothing at
/// boot, and opens a fresh pool on #2 at its first cache miss.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_repointed_at_an_empty_redeploy_drops_its_stale_row_and_opens_fresh()
-> anyhow::Result<()> {
    ensure_decdn_cli_built()?;
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_empty_redeploy()))
        .await
        .context("empty-redeploy e2e exceeded the overall timeout")??;
    Ok(())
}

/// A node repointed at a redeploy on which its key already owns the tracked id
/// adopts that pool with the #2 deposit and no lanes, and pays #2 only for the
/// bytes #2 sees.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_repointed_at_a_redeploy_that_reissued_its_pool_id_adopts_it_without_its_lanes()
-> anyhow::Result<()> {
    ensure_decdn_cli_built()?;
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_reissued_id()))
        .await
        .context("reissued-pool-id e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run_empty_redeploy() -> anyhow::Result<()> {
    let r = stale_row_from_first_deployment().await?;
    let pool2 = r.pool_on(r.pool2);

    r.server
        .repoint_payment_pool(r.pool2, &[&r.seeder2])
        .await?;
    r.assert_foreign_row_dropped().await?;

    // The drop deletes the row outright, so the store reads empty — not merely
    // filtered — once bootstrap is through.
    let admin = r.server.admin_client()?;
    decdn_e2e::poll(SETTLE, || async {
        Ok(admin
            .pools()
            .await
            .ok()
            .filter(|p| p.pools.is_empty() && p.skipped.is_empty()))
    })
    .await?
    .context("the SERVER's buyer store never emptied after the foreign row was dropped")?;

    // The node opens lazily on a miss, so #2 has nothing for this owner yet.
    anyhow::ensure!(
        owner_pool_nonce(&pool2, r.owner).await? == 0,
        "bootstrap must not open a pool on the redeploy"
    );
    anyhow::ensure!(
        enumerate_owned_pools(&pool2, r.owner).await?.is_empty(),
        "getPools on the redeploy must be empty before the first pull"
    );

    r.pull_second().await?;

    // The miss opened exactly one pool on #2. Nonce 0 on #2 is the tracked id
    // from #1 — the collision — freshly escrowed at the #2 working deposit.
    anyhow::ensure!(
        owner_pool_nonce(&pool2, r.owner).await? == 1,
        "the first pull on the redeploy must open exactly one pool"
    );
    let on_2 = enumerate_owned_pools(&pool2, r.owner).await?;
    anyhow::ensure!(
        on_2 == [r.tracked],
        "the fresh pool on #2 must reissue the tracked id {}, found {on_2:?}",
        r.tracked
    );
    let opened = pool2
        .getPool(r.tracked)
        .call()
        .await
        .context("getPool on #2")?;
    anyhow::ensure!(
        opened.deposit == REDEPLOY_WORKING_DEPOSIT,
        "the fresh #2 pool must escrow the working deposit {REDEPLOY_WORKING_DEPOSIT}, got {}",
        opened.deposit
    );

    let row = r.sole_pool_on_redeploy().await?;
    anyhow::ensure!(
        row.deposit_micro_usdc == REDEPLOY_WORKING_DEPOSIT,
        "the tracked #2 row must record the fresh deposit {REDEPLOY_WORKING_DEPOSIT}, got {}",
        row.deposit_micro_usdc
    );
    r.assert_paid_for_second_only().await?;

    // Deployment #1 still holds the original pool, untouched by the repoint.
    let on_1 = r
        .pool_on(r.pool1)
        .getPool(r.tracked)
        .call()
        .await
        .context("getPool on #1")?;
    anyhow::ensure!(
        is_open(&on_1) && on_1.deposit == FIRST_WORKING_DEPOSIT,
        "the #1 pool must stay open with its deposit {FIRST_WORKING_DEPOSIT}, got open={} / {}",
        is_open(&on_1),
        on_1.deposit
    );

    r.assert_no_foreign_row_reached_the_pull_path()
}

async fn run_reissued_id() -> anyhow::Result<()> {
    let r = stale_row_from_first_deployment().await?;
    let pool2 = r.pool_on(r.pool2);

    // Walk the owner's #2 nonce up to the tracked one, under the SERVER's own key
    // while its daemon is down. Only `msg.sender` opens a pool for itself, so
    // this is the one way a pool at the tracked id exists on #2.
    let tracked_nonce = owner_pool_nonce(&r.pool_on(r.pool1), r.owner)
        .await?
        .checked_sub(1)
        .context("the SERVER opened no pool on #1")?;
    let op = r.chain.provider_for(r.server.operator());
    let approve = Erc20::new(r.chain.usdc(), &op)
        .approve(
            r.pool2,
            U256::from(PREOPENED_DEPOSIT) * (U256::from(tracked_nonce) + U256::from(1u8)),
        )
        .send()
        .await
        .context("approve #2 under the SERVER's key")?
        .get_receipt()
        .await
        .context("approve #2 receipt")?;
    decdn_e2e::ensure_mined(&approve, "approve #2")?;
    while owner_pool_nonce(&pool2, r.owner).await? <= tracked_nonce {
        let opened = PaymentPool::new(r.pool2, &op)
            .openPool(PREOPENED_DEPOSIT)
            .send()
            .await
            .context("openPool on #2 under the SERVER's key")?
            .get_receipt()
            .await
            .context("openPool on #2 receipt")?;
        decdn_e2e::ensure_mined(&opened, "openPool on #2")?;
    }
    anyhow::ensure!(
        decdn_e2e::assert::pool_id(r.owner, tracked_nonce) == r.tracked,
        "nonce {tracked_nonce} must derive the tracked id {}",
        r.tracked
    );
    let reissued = pool2
        .getPool(r.tracked)
        .call()
        .await
        .context("getPool on #2")?;
    anyhow::ensure!(
        reissued.owner == r.owner && is_open(&reissued) && reissued.deposit == PREOPENED_DEPOSIT,
        "#2 must hold an open pool at the tracked id owned by the SERVER, got owner {} / \
         open={} / {}",
        reissued.owner,
        is_open(&reissued),
        reissued.deposit
    );
    let nonce_before_repoint = owner_pool_nonce(&pool2, r.owner).await?;

    r.server
        .repoint_payment_pool(r.pool2, &[&r.seeder2])
        .await?;
    r.assert_foreign_row_dropped().await?;

    // The node adopts the #2 pool at the tracked id as a fresh row: the #2
    // deposit, no lanes. Keeping the #1 row would show the #1 deposit and the #1
    // lane — the production bug.
    let adopted = r.sole_pool_on_redeploy().await?;
    anyhow::ensure!(
        adopted.deposit_micro_usdc == PREOPENED_DEPOSIT && adopted.lanes.is_empty(),
        "the adopted row must carry the #2 deposit {PREOPENED_DEPOSIT} and no lanes, got \
         deposit {} and lanes {:?} (carried #1 cumulative was {})",
        adopted.deposit_micro_usdc,
        adopted.lanes,
        r.carried
    );
    anyhow::ensure!(
        owner_pool_nonce(&pool2, r.owner).await? == nonce_before_repoint,
        "the node must adopt the pool at the tracked id, not open another at boot"
    );

    r.pull_second().await?;

    anyhow::ensure!(
        owner_pool_nonce(&pool2, r.owner).await? == nonce_before_repoint,
        "the pull must reuse the adopted pool, not open another"
    );
    r.assert_paid_for_second_only().await?;
    r.assert_no_foreign_row_reached_the_pull_path()
}

/// The state both journeys start from: a SERVER whose buyer store tracks a pool
/// on #1 with lane progress to SEEDER #1, stopped, with #2 deployed beside it.
struct Redeploy {
    chain: ChainFixture,
    pool1: Address,
    pool2: Address,
    // Kept alive so it can finish redeeming leg 1.
    _seeder1: NodeFixture,
    seeder2: NodeFixture,
    server: NodeFixture,
    ns2: U256,
    second: Vec<u8>,
    second_hash: Hash,
    owner: Address,
    // The SERVER's pool id on #1, which #2 reissues.
    tracked: B256,
    // The #1 lane's cumulative the SERVER's row carried into the repoint.
    carried: u64,
}

/// Run leg 1 on #1 and stop the SERVER, leaving its buyer row as a node carries
/// it into a `PaymentPool` redeploy.
#[allow(
    clippy::too_many_lines,
    reason = "one sequential set-up: each step depends on the previous step's \
              on-chain/daemon state, so decomposing it would thread state through helpers \
              without reducing its length or making it easier to follow"
)]
async fn stale_row_from_first_deployment() -> anyhow::Result<Redeploy> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let pool1 = chain.addrs().payment_pool;
    let pool2 = chain.redeploy_payment_pool().await?;
    anyhow::ensure!(pool2 != pool1, "the redeploy must land at a new address");

    let first = make_blob(6 * CHUNK_GROUP + 37);
    let second = make_blob(2 * CHUNK_GROUP + 11);

    let (seeder1, first_hashes) = NodeFixture::launch_with_blobs(&chain, "US", &[&first]).await?;
    let first_hash = *first_hashes.first().context("seeder #1 missing its hash")?;
    let (seeder2, second_hashes) =
        NodeFixture::launch_with_blobs_on_payment_pool(&chain, pool2, "US", &[&second]).await?;
    let second_hash = *second_hashes
        .first()
        .context("seeder #2 missing its hash")?;
    for seeder in [&seeder1, &seeder2] {
        seeder.set_rate_per_mb(RATE_PER_MB).await?;
        seeder
            .set_pool_min_remaining_deposit(POOL_MIN_REMAINING)
            .await?;
    }

    // One namespace per leg, each seated on its own seeder, before the SERVER
    // first boots so its directory enumerates both.
    let publisher = PrivateKeySigner::random();
    chain.vet_publisher(publisher.address()).await?;
    let ns1 = chain.create_namespace(&publisher).await?;
    chain
        .add_origin(&publisher, ns1, seeder1.operator_addr())
        .await?;
    let ns2 = chain.create_namespace(&publisher).await?;
    chain
        .add_origin(&publisher, ns2, seeder2.operator_addr())
        .await?;

    let server = NodeFixture::launch_pull_through_cache(&chain, "US", &[&seeder1]).await?;
    server.set_rate_per_mb(RATE_PER_MB).await?;
    server
        .set_buyer_working_deposit(FIRST_WORKING_DEPOSIT)
        .await?;
    server
        .set_pool_min_remaining_deposit(POOL_MIN_REMAINING)
        .await?;
    chain
        .fund_node_as_buyer(server.operator_addr(), U256::from(SERVER_BUYER_USDC))
        .await?;
    let owner = server.operator_addr();

    // Leg 1: the SERVER misses, opens its pool on #1, and pays SEEDER #1.
    let client = ClientFixture::new(&chain).await?;
    let (_session, got_first) = client
        .open_session_in_namespace(&chain, &server, first_hash, ns1)
        .await
        .context("leg 1: node-to-node pull on #1")?;
    anyhow::ensure!(got_first == first, "leg 1 delivered the wrong bytes");

    let on_1 = PaymentPool::new(pool1, chain.admin().clone());
    let opened = enumerate_owned_pools(&on_1, owner).await?;
    anyhow::ensure!(
        opened.len() == 1,
        "leg 1 must open exactly one pool on #1, found {opened:?}"
    );
    let tracked = *opened.first().context("tracked pool id")?;
    anyhow::ensure!(
        tracked == decdn_e2e::assert::pool_id(owner, 0),
        "the SERVER's first pool on #1 must be its nonce-0 id"
    );

    // The row the SERVER carries into the repoint: tagged #1, with a lane to
    // SEEDER #1 at the leg-1 cumulative.
    let first_wire = wire_bytes(&first)?;
    let expected_carried = price_micro_usdc(first_wire);
    let seeder1_addr = seeder1.operator_addr();
    let admin = server.admin_client()?;
    let last_seen = std::sync::Mutex::new(Vec::new());
    let row = decdn_e2e::poll(SETTLE, || async {
        let rows = admin.pools().await.context("admin pools")?.pools;
        last_seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone_from(&rows);
        Ok(match rows.as_slice() {
            [row]
                if lane_to(row, seeder1_addr).is_some_and(|l| {
                    l.last_amount_micro_usdc == expected_carried
                        && l.last_bytes_delivered == first_wire
                }) =>
            {
                Some(row.clone())
            }
            _ => None,
        })
    })
    .await?
    .with_context(|| {
        format!(
            "the SERVER's buyer store never recorded one pool with a lane to SEEDER #1 at \
             {expected_carried} micro-USDC for {first_wire} bytes; last saw {:?}",
            last_seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        )
    })?;
    anyhow::ensure!(
        parse_b256(&row.pool_id)? == tracked && parse_address(&row.payment_pool)? == pool1,
        "the leg-1 row must be the tracked pool on #1, got {} on {}",
        row.pool_id,
        row.payment_pool
    );

    // Real, redeemed progress: SEEDER #1 cashed the leg-1 voucher on #1.
    let redeemed = decdn_e2e::poll(SETTLE, || async {
        let w = on_1
            .getWatermark(tracked, owner, seeder1_addr)
            .call()
            .await
            .context("read the #1 lane watermark")?;
        Ok((w.amount > 0).then_some(w))
    })
    .await?
    .context("SEEDER #1 never redeemed the SERVER's leg-1 voucher")?;
    anyhow::ensure!(
        redeemed.amount == expected_carried && redeemed.bytesDelivered == first_wire,
        "SEEDER #1 must redeem the leg-1 price {expected_carried} for {first_wire} bytes, got \
         {} for {}",
        redeemed.amount,
        redeemed.bytesDelivered
    );

    // The deposit a fresh #2 open escrows. The restart runs against #1, where
    // the row is on the configured deployment and is kept.
    server
        .set_buyer_working_deposit(REDEPLOY_WORKING_DEPOSIT)
        .await?;
    anyhow::ensure!(
        server.log_line(&[FOREIGN_ROW_DROPPED]).is_none(),
        "a node still on #1 must not drop its #1 row"
    );
    server.stop()?;

    Ok(Redeploy {
        chain,
        pool1,
        pool2,
        _seeder1: seeder1,
        seeder2,
        server,
        ns2,
        second,
        second_hash,
        owner,
        tracked,
        carried: expected_carried,
    })
}

impl Redeploy {
    /// A read handle on the `PaymentPool` at `at`.
    fn pool_on(&self, at: Address) -> PaymentPool::PaymentPoolInstance<DynProvider> {
        PaymentPool::new(at, self.chain.admin().clone())
    }

    /// Wait for the repointed SERVER's bootstrap to log the drop of its #1 row,
    /// naming the tracked id, #1 as `foreign_payment_pool`, and #2 as configured.
    async fn assert_foreign_row_dropped(&self) -> anyhow::Result<()> {
        let pool_id = format!("pool_id={}", self.tracked);
        let foreign = format!("foreign_payment_pool={}", self.pool1);
        let configured = format!("configured_payment_pool={}", self.pool2);
        self.server
            .wait_for_log_line(
                &[FOREIGN_ROW_DROPPED, &pool_id, &foreign, &configured],
                SETTLE,
            )
            .await
            .context("the repointed SERVER never logged dropping its #1 row")?;
        Ok(())
    }

    /// Wait until the SERVER's buyer store holds exactly one row, and that row
    /// is the tracked id on #2.
    async fn sole_pool_on_redeploy(&self) -> anyhow::Result<BuyerPoolSnapshot> {
        let admin = self.server.admin_client()?;
        let row = decdn_e2e::poll(SETTLE, || async {
            let Ok(response) = admin.pools().await else {
                return Ok(None);
            };
            Ok(match response.pools.as_slice() {
                [row] if parse_address(&row.payment_pool)? == self.pool2 => Some(row.clone()),
                _ => None,
            })
        })
        .await?
        .context("the SERVER never tracked a single pool on #2")?;
        anyhow::ensure!(
            parse_b256(&row.pool_id)? == self.tracked,
            "the #2 row must be the tracked id {}, got {}",
            self.tracked,
            row.pool_id
        );
        Ok(row)
    }

    /// Leg 2: a fresh client on #2 fetches `second` through the SERVER, whose
    /// miss is served only by SEEDER #2.
    async fn pull_second(&self) -> anyhow::Result<()> {
        let client = ClientFixture::new_on_payment_pool(&self.chain, self.pool2).await?;
        let (_session, got) = client
            .open_session_in_namespace(&self.chain, &self.server, self.second_hash, self.ns2)
            .await
            .context("leg 2: node-to-node pull on #2")?;
        anyhow::ensure!(got == self.second, "leg 2 delivered the wrong bytes");
        Ok(())
    }

    /// The SERVER's #2 lane — in its own store and on chain — covers the leg-2
    /// bytes alone. A resumed #1 row signs and redeems `carried + price`.
    async fn assert_paid_for_second_only(&self) -> anyhow::Result<()> {
        let bytes = wire_bytes(&self.second)?;
        let price = price_micro_usdc(bytes);
        let seeder2 = self.seeder2.operator_addr();

        let row = self.sole_pool_on_redeploy().await?;
        let admin = self.server.admin_client()?;
        let lane = decdn_e2e::poll(SETTLE, || async {
            let pools = admin.pools().await.context("admin pools")?.pools;
            Ok(pools
                .iter()
                .find(|p| p.pool_id == row.pool_id)
                .and_then(|p| lane_to(p, seeder2))
                .filter(|l| l.last_amount_micro_usdc > 0)
                .cloned())
        })
        .await?
        .context("the SERVER never recorded a lane to SEEDER #2")?;
        anyhow::ensure!(
            lane.last_amount_micro_usdc == price && lane.last_bytes_delivered == bytes,
            "the #2 lane must cover the leg-2 bytes alone ({price} for {bytes}), got {} for {} \
             ({} would be the #1 cumulative carried over)",
            lane.last_amount_micro_usdc,
            lane.last_bytes_delivered,
            self.carried.saturating_add(price)
        );

        let pool2 = self.pool_on(self.pool2);
        let redeemed = decdn_e2e::poll(SETTLE, || async {
            let w = pool2
                .getWatermark(self.tracked, self.owner, seeder2)
                .call()
                .await
                .context("read the #2 lane watermark")?;
            Ok((w.amount > 0).then_some(w))
        })
        .await?
        .context("SEEDER #2 never redeemed the SERVER's leg-2 voucher")?;
        anyhow::ensure!(
            redeemed.amount == price && redeemed.bytesDelivered == bytes,
            "#2 must be paid {price} for {bytes} bytes, got {} for {} ({} would be the #1 \
             cumulative carried over)",
            redeemed.amount,
            redeemed.bytesDelivered,
            self.carried.saturating_add(price)
        );
        Ok(())
    }

    /// The pull path never met the #1 row: bootstrap's drop is what removed it,
    /// not the per-pull filter behind it.
    fn assert_no_foreign_row_reached_the_pull_path(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.server.log_line(&[FOREIGN_ROW_IGNORED]).is_none(),
            "a pull met the #1 row after bootstrap should have dropped it"
        );
        Ok(())
    }
}

/// `owner`'s `ownerPoolNonce` on `pool` — how many pools it has ever opened there.
async fn owner_pool_nonce(
    pool: &PaymentPool::PaymentPoolInstance<DynProvider>,
    owner: Address,
) -> anyhow::Result<u64> {
    let nonce = pool
        .ownerPoolNonce(owner)
        .call()
        .await
        .context("ownerPoolNonce")?;
    u64::try_from(nonce).context("ownerPoolNonce overflows u64")
}

/// Whether `pool` is `Open` on chain.
const fn is_open(pool: &PaymentPool::Pool) -> bool {
    matches!(pool.status, PaymentPool::Status::Open)
}

/// `row`'s lane paying `provider`, if it has one.
fn lane_to(
    row: &BuyerPoolSnapshot,
    provider: Address,
) -> Option<&decdn_common::admin::BuyerLaneSnapshot> {
    row.lanes
        .iter()
        .find(|l| parse_address(&l.provider).is_ok_and(|p| p == provider))
}

fn parse_address(s: &str) -> anyhow::Result<Address> {
    s.parse().with_context(|| format!("parse address {s}"))
}

fn parse_b256(s: &str) -> anyhow::Result<B256> {
    s.parse().with_context(|| format!("parse pool id {s}"))
}
