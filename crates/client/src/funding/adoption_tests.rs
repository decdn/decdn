use super::*;
use alloy::primitives::{B256, Bytes};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;

use crate::Cumulative;
use crate::buyer_pool::{EscrowUnconfirmed, EscrowUntracked, OpenUnconfirmed};
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;
use alloy::sol_types::SolValue;
use decdn_incentive::test_rpc::SCRIPTED_TX;
use decdn_incentive::tx::SendLegLayer;
use std::assert_matches;

const PP: Address = Address::repeat_byte(0x9c);
/// The deployment every test buys on: chain 1, contract [`PP`].
const DEPLOYMENT: Deployment = Deployment {
    chain_id: 1,
    payment_pool: PP,
};
const PROVIDER: Address = Address::repeat_byte(0x77);
const TOKEN: Address = Address::repeat_byte(0x22);
/// The client's working deposit: 10 USDC, so low water is 2 USDC.
const WORKING: u64 = 10_000_000;

fn pool(owner: Address, deposit: u64, redeemed: u64) -> PaymentPool::Pool {
    PaymentPool::Pool {
        owner,
        status: PaymentPool::Status::Open,
        disputeDeadline: 0,
        deposit,
        totalRedeemed: redeemed,
    }
}

/// The `getPool` answer for `owner`'s tracked pool, still open: the read the
/// first lane build of a run makes before it reuses the row.
fn still_open(owner: Address) -> Bytes {
    pool(owner, WORKING, 0).abi_encode().into()
}

fn lane(amount: u64, bytes: u64) -> Bytes {
    PaymentPool::Lane {
        amount,
        bytesDelivered: bytes,
    }
    .abi_encode()
    .into()
}

/// Run `open_or_reuse_pool` with its `eth_call`s answered in order from
/// `calls`; `None` faults a call. A call past the end of the queue faults
/// too, which is how these tests prove a path was NOT taken.
async fn run(
    store: &RedbBuyerPoolStore,
    signer: &Arc<PrivateKeySigner>,
    adoption: ChainAdoption,
    calls: Vec<Option<Bytes>>,
) -> anyhow::Result<PoolContext> {
    run_in(store, signer, adoption, calls, &RunFunding::default())
        .await
        .0
        .map(|(ctx, _)| ctx)
}

/// [`run`] as one lane build of the run `funding` describes, with the
/// lane's [`LaneSpend`]. The second value is whether every queued answer
/// was read: a path that stops short leaves some unread, which is how these
/// tests prove a path WAS taken.
async fn run_in(
    store: &RedbBuyerPoolStore,
    signer: &Arc<PrivateKeySigner>,
    adoption: ChainAdoption,
    calls: Vec<Option<Bytes>>,
    funding: &RunFunding,
) -> (anyhow::Result<(PoolContext, LaneSpend)>, bool) {
    let asserter = Asserter::new();
    for call in calls {
        match call {
            Some(response) => asserter.push_success(&response),
            None => asserter.push_failure_msg("transient rpc fault"),
        }
    }
    let rpc = ProviderBuilder::new()
        .layer(SendLegLayer)
        .connect_mocked_client(asserter.clone());
    let contract = PaymentPool::new(PP, rpc.clone());
    let payer = match OwnedPayer::for_store(Arc::clone(signer), store) {
        Ok(payer) => payer,
        Err(err) => return (Err(err), asserter.read_q().is_empty()),
    };
    let result = open_or_reuse_pool(
        store,
        &contract,
        &rpc,
        funding,
        &payer,
        PROVIDER,
        DEPLOYMENT,
        DepositPolicy {
            working_deposit: U256::from(WORKING),
            max_approve: false,
            adoption,
        },
        &CancellationToken::new(),
    )
    .await;
    (result, asserter.read_q().is_empty())
}

fn client_store(dir: &tempfile::TempDir) -> RedbBuyerPoolStore {
    // A fresh subpath: the store requires 0o700 and creates it itself.
    RedbBuyerPoolStore::open(&dir.path().join("data")).unwrap()
}

/// The case that reached a user. The store has lost its row, all the
/// wallet's USDC sits in a live pool, and the client must reuse that pool
/// rather than try to escrow a second deposit it cannot afford.
///
/// Also pins the three things adoption has to get right together: it takes
/// the newest pool that can still pay (a drained newer one is passed over),
/// it resumes the lane from the chain watermark rather than zero, and it
/// records the row so the next run reuses it without asking the chain again.
#[tokio::test]
async fn a_lost_row_adopts_the_live_pool_and_resumes_from_the_chain_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let owner = signer.address();
    let older = B256::repeat_byte(0xAA);
    let newer = B256::repeat_byte(0xBB);

    let funding = RunFunding::default();
    let ctx = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        vec![
            Some(vec![older, newer].abi_encode().into()),
            // Walked newest-first: `newer` is drained, `older` still pays.
            Some(pool(owner, 10_000_000, 10_000_000).abi_encode().into()),
            Some(pool(owner, 10_000_000, 1_106_908).abi_encode().into()),
            Some(TOKEN.abi_encode().into()),
            Some(lane(1_106_908, 1_106_908_000)),
        ],
        &funding,
    )
    .await
    .0
    .map(|(ctx, _)| ctx)
    .expect("a live, solvent pool is adopted — nothing is opened");

    assert_eq!(ctx.pool_id, older);
    assert!(
        funding.escrows().saw_live(older),
        "the adoption read the pool on chain as this wallet's"
    );
    assert_eq!(
        (ctx.prior_bytes_delivered, ctx.prior_amount),
        (U256::from(1_106_908_000u64), U256::from(1_106_908u64)),
        "the lane resumes from the chain watermark; from zero, every voucher at or \
         below it would redeem nothing"
    );
    let row = store
        .get_by_owner(owner)
        .unwrap()
        .expect("adopted row is recorded");
    assert_eq!(row.pool_id, older);
    assert!(row.is_on(DEPLOYMENT));
}

/// A chain read that faults refuses the fetch. It must not fall through to
/// opening a pool: "could not tell" is not "owns nothing", and treating it
/// as such escrows a second deposit beside a pool the wallet already funded.
#[tokio::test]
async fn a_faulted_enumeration_refuses_rather_than_opening() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());

    let err = run(&store, &signer, ChainAdoption::Allowed, vec![None])
        .await
        .unwrap_err();
    // The open path would also error here (its first call finds an empty
    // queue), so only the message tells "refused on purpose" from "fell
    // through to opening".
    assert!(
        format!("{err:#}").contains("refusing to open one blind"),
        "expected the deliberate refusal, got: {err:#}"
    );
    assert!(store.get_by_owner(signer.address()).unwrap().is_none());
}

/// A stored pool meeting a new provider resumes that lane from the chain,
/// not from zero. This reaches every tracked pool, not only adopted ones.
#[tokio::test]
async fn a_tracked_pool_resumes_a_new_lane_from_the_chain_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let id = B256::repeat_byte(0xCC);
    store
        .record(&BuyerPoolState::new(
            id,
            DEPLOYMENT,
            signer.address(),
            TOKEN,
            U256::from(WORKING),
        ))
        .unwrap();

    let ctx = run(
        &store,
        &signer,
        ChainAdoption::Allowed,
        vec![Some(still_open(signer.address())), Some(lane(500, 500_000))],
    )
    .await
    .expect("a tracked pool reuses without touching getPools");

    assert_eq!(ctx.pool_id, id);
    assert_eq!(
        (ctx.prior_bytes_delivered, ctx.prior_amount),
        (U256::from(500_000u64), U256::from(500u64))
    );
}

/// A tracked row from another deployment is not reused, whether the
/// deployment differs in its contract address or only in its chain. The
/// pool id repeats across deployments, so reusing the row would resume lane
/// progress this pool never redeemed against.
///
/// `ChainAdoption::Refused` keeps the chain out of it: the mocked provider
/// has no answers queued, so any read would fault the call.
#[tokio::test]
async fn a_row_from_another_deployment_is_not_reused() {
    let signer = Arc::new(PrivateKeySigner::random());
    let rpc = ProviderBuilder::new()
        .layer(SendLegLayer)
        .connect_mocked_client(Asserter::new());
    let contract = PaymentPool::new(PP, rpc);
    let other_address = Deployment {
        payment_pool: Address::repeat_byte(0xDE),
        ..DEPLOYMENT
    };
    let other_chain = Deployment {
        chain_id: 421_614,
        ..DEPLOYMENT
    };
    for (foreign, reused) in [
        (other_address, false),
        (other_chain, false),
        (DEPLOYMENT, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        let id = B256::repeat_byte(0xCC);
        store
            .record(&BuyerPoolState::new(
                id,
                foreign,
                signer.address(),
                TOKEN,
                U256::from(WORKING),
            ))
            .unwrap();

        let tracked = pool_to_reuse(
            &store,
            &contract,
            &RunFunding::default(),
            signer.address(),
            DEPLOYMENT,
            ChainAdoption::Refused,
        )
        .await
        .expect("a refused adoption reads nothing from the chain");

        assert_eq!(
            tracked.as_ref().map(|state| state.pool_id),
            reused.then_some(id),
            "row on {foreign:?}, buying on {DEPLOYMENT:?}"
        );
        assert!(tracked.is_none_or(|state| state.redeemed_elsewhere().is_zero()));
    }
}

/// A tracked pool is reused only while the chain shows it open. A pool its
/// owner closed (`Closing`) or that anyone reclaimed (`Closed`) is treated as
/// no row, so the run opens another. A failed read, a pool the chain holds no
/// record of yet, and a pool another owner holds keep the row: none of them
/// shows this owner's pool closed.
#[tokio::test]
async fn a_tracked_pool_is_reused_only_while_the_chain_shows_it_open() {
    let signer = Arc::new(PrivateKeySigner::random());
    let owner = signer.address();
    let id = B256::repeat_byte(0xCC);
    let read = |owner: Address, status: PaymentPool::Status| -> Option<Bytes> {
        let mut answer = pool(owner, WORKING, 0);
        answer.status = status;
        Some(answer.abi_encode().into())
    };
    let cases = [
        ("open", read(owner, PaymentPool::Status::Open), true),
        ("closing", read(owner, PaymentPool::Status::Closing), false),
        ("closed", read(owner, PaymentPool::Status::Closed), false),
        ("read fault", None, true),
        (
            "no record",
            read(Address::ZERO, PaymentPool::Status::Open),
            true,
        ),
        (
            "another owner",
            read(Address::repeat_byte(0x99), PaymentPool::Status::Closed),
            true,
        ),
    ];
    for (case, answer, reused) in cases {
        let dir = tempfile::tempdir().unwrap();
        let store = client_store(&dir);
        store
            .record(&BuyerPoolState::new(
                id,
                DEPLOYMENT,
                owner,
                TOKEN,
                U256::from(WORKING),
            ))
            .unwrap();
        let asserter = Asserter::new();
        match answer {
            Some(response) => asserter.push_success(&response),
            None => asserter.push_failure_msg("transient rpc fault"),
        }
        let contract = PaymentPool::new(
            PP,
            ProviderBuilder::new()
                .layer(SendLegLayer)
                .connect_mocked_client(asserter.clone()),
        );
        let funding = RunFunding::default();
        let tracked = pool_to_reuse(
            &store,
            &contract,
            &funding,
            owner,
            DEPLOYMENT,
            ChainAdoption::Refused,
        )
        .await
        .unwrap();
        assert_eq!(
            tracked.map(|state| state.pool_id),
            reused.then_some(id),
            "{case}"
        );
        assert_eq!(funding.pool_id(), reused.then_some(id), "{case}");
        // Only a read of this owner's open pool shows it live, so a recovery
        // step later tells a pool gone from a lagging read.
        assert_eq!(
            funding.escrows().saw_live(id),
            case == "open",
            "{case}: the pool is noted live"
        );
        assert!(asserter.read_q().is_empty(), "{case}: the status was read");
    }
}

/// A buy from a node's data dir never adopts, even though the chain would
/// hand it a pool: that pool is the daemon's, and adopting it would spend the
/// daemon's deposit from a buyer the daemon cannot see.
///
/// The queue holds only the `usdc()` answer the open path reads first. Were
/// adoption attempted, `getPools` would consume that word, fail to decode it,
/// and surface the adoption refusal instead.
#[tokio::test]
async fn a_node_data_dir_never_adopts_the_daemons_pool() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());

    let err = run(
        &store,
        &signer,
        ChainAdoption::Refused,
        vec![Some(TOKEN.abi_encode().into())],
    )
    .await
    .unwrap_err();
    assert!(
        !format!("{err:#}").contains("already owns a pool"),
        "a node dir went down the adoption path: {err:#}"
    );
    assert!(
        store.get_by_owner(signer.address()).unwrap().is_none(),
        "nothing may be adopted into a node dir's client store"
    );
}

/// The amounts observed on the pool in #2288: six lanes, 8.655 of 10 USDC
/// spent in all, the largest lane 2.374. The first lane is [`PROVIDER`]'s.
const LIVE_LANES: [u64; 6] = [2_374_000, 2_360_000, 2_147_000, 1_043_000, 483_000, 248_000];

/// A tracked row on [`DEPLOYMENT`] holding `deposit`, with one lane per
/// `amounts` entry signed by `lane_signer`: the first is [`PROVIDER`]'s, each
/// later one another provider's. A lane's bytes are its amount times 1000.
fn tracked_row(
    id: B256,
    owner: Address,
    lane_signer: Address,
    deposit: u64,
    amounts: &[u64],
) -> BuyerPoolState {
    let lanes = amounts
        .iter()
        .zip(0u8..)
        .map(|(&amount, i)| {
            let provider = if i == 0 {
                PROVIDER
            } else {
                Address::repeat_byte(0x40 + i)
            };
            (
                lane_key(id, lane_signer, provider),
                decdn_incentive::BuyerLaneProgress {
                    last_amount: U256::from(amount),
                    last_bytes: U256::from(amount) * U256::from(1000u64),
                },
            )
        })
        .collect();
    BuyerPoolState::hydrate(
        id,
        DEPLOYMENT,
        owner,
        TOKEN,
        U256::from(deposit),
        lanes,
        U256::ZERO,
    )
}

/// The key `store`'s data dir signs `signer`'s owned lanes with.
fn lane_of(store: &RedbBuyerPoolStore, signer: &Arc<PrivateKeySigner>) -> Address {
    OwnedPayer::for_store(Arc::clone(signer), store)
        .unwrap()
        .lane()
        .address()
}

fn lane_key(id: B256, owner: Address, provider: Address) -> LaneKey {
    LaneKey {
        pool_id: id,
        signer: owner,
        provider,
    }
}

/// A refill that fails at the allowance read, then reads a wallet holding
/// `usdc` micro-USDC.
fn refill_fails_with_wallet(usdc: u64) -> Vec<Option<Bytes>> {
    vec![None, Some(U256::from(usdc).abi_encode().into())]
}

/// The refill compares the deposit with the pool's spend over every lane.
/// Each live lane alone leaves far more than the low water; together they
/// leave 1.345 USDC, below the 2 USDC mark, so the pool must refill.
#[test]
fn pool_spend_sums_every_tracked_lane() {
    let owner = Address::repeat_byte(0x01);
    let id = B256::repeat_byte(0xEE);
    let row = tracked_row(id, owner, owner, WORKING, &LIVE_LANES);
    let working = U256::from(WORKING);
    let low_water = working / U256::from(LOW_WATER_DIVISOR);

    let spent = row.pool_spend();
    assert_eq!(spent, U256::from(8_655_000u64));
    assert_eq!(
        refill_amount(row.deposit, spent, working, low_water),
        U256::from(8_655_000u64),
        "the refill restores the 1.345 USDC remaining to the 10 USDC working deposit"
    );
    for &one_lane in &LIVE_LANES {
        assert!(
            refill_amount(row.deposit, U256::from(one_lane), working, low_water).is_zero(),
            "no single lane reaches the low water, so the spend must be the sum over \
             every lane"
        );
    }
}

/// A lane the row has no record of resumes from the chain watermark, and
/// that watermark is spend the row's sum does not hold yet: the seed
/// records it. A tracked lane's seed is not counted twice.
#[test]
fn a_seeded_lanes_chain_prior_counts_toward_the_pool_spend() {
    let owner = Address::repeat_byte(0x01);
    let id = B256::repeat_byte(0xEE);
    let mut row = tracked_row(id, owner, owner, WORKING, &[1_000]);
    let new_lane = lane_key(id, owner, Address::repeat_byte(0x55));
    row.seed_lane(new_lane, U256::from(250_000u64), U256::from(250u64))
        .unwrap();
    assert_eq!(row.pool_spend(), U256::from(1_250u64));
    row.seed_lane(
        lane_key(id, owner, PROVIDER),
        U256::from(1_000_000u64),
        U256::from(1_000u64),
    )
    .unwrap();
    assert_eq!(row.pool_spend(), U256::from(1_250u64));
}

/// An adopted row has no lanes, so `totalRedeemed` is its only record of
/// what other lanes drained. Ignored, a pool with 0.5 USDC left reads as a
/// full 10 and no refill fires. A lane seeded from chain takes its share
/// over, so the other lanes' share still counts as that lane advances
/// (#2292 review): 100 redeemed, 60 of it on A, A advancing to 80 spends
/// 120.
#[test]
fn pool_spend_keeps_an_adopted_pools_redeemed_spend_as_its_lanes_advance() {
    let owner = Address::repeat_byte(0x01);
    let id = B256::repeat_byte(0xEE);
    let adopted = |redeemed: u64| {
        BuyerPoolState::adopt(
            id,
            DEPLOYMENT,
            owner,
            TOKEN,
            U256::from(WORKING),
            U256::from(redeemed),
        )
    };
    assert_eq!(adopted(9_500_000).pool_spend(), U256::from(9_500_000u64));

    let a = lane_key(id, owner, PROVIDER);
    let mut row = adopted(100);
    row.seed_lane(a, U256::from(60_000u64), U256::from(60u64))
        .unwrap();
    assert_eq!(row.redeemed_elsewhere(), U256::from(40u64));
    assert_eq!(row.pool_spend(), U256::from(100u64));
    row.advance_lane(a, U256::from(80_000u64), U256::from(80u64))
        .unwrap();
    assert_eq!(row.pool_spend(), U256::from(120u64));
}

/// #2288 end to end: five lanes of 2 USDC spend the whole 10 USDC deposit,
/// so building any lane fires the refill. The wallet holds no USDC, and with
/// nothing left in the pool the lane fails. A spend read from the one lane
/// alone would see 8 USDC remaining, fire no refill, and return `Ok`.
#[tokio::test]
async fn a_pool_spent_across_lanes_refills_and_fails_when_nothing_is_left() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let id = B256::repeat_byte(0xEE);
    store
        .record(&tracked_row(
            id,
            signer.address(),
            lane_of(&store, &signer),
            WORKING,
            &[2_000_000; 5],
        ))
        .unwrap();

    let mut calls = vec![Some(still_open(signer.address()))];
    calls.extend(refill_fails_with_wallet(0));
    let (result, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        calls,
        &RunFunding::default(),
    )
    .await;
    let err = result.expect_err("a pool with no unspent deposit cannot pay");
    assert!(
        format!("{err:#}").contains("has no unspent deposit, and the wallet cannot fund"),
        "expected the refill failure, got: {err:#}"
    );
    assert!(
        err.downcast_ref::<NoAffordableSource>().is_some(),
        "the lane build ends the command rather than retrying: {err:#}"
    );
    assert!(
        drained,
        "the refill read the allowance and the wallet balance"
    );
}

/// A lane the row has no record of joins its chain watermark to the
/// pool-wide spend. Here the tracked lane has spent 9 USDC and the new
/// lane's watermark the last 1, so nothing is left. Without the watermark
/// the pool reads 1 USDC remaining and the lane builds.
#[tokio::test]
async fn an_untracked_lanes_chain_watermark_counts_toward_the_pool_spend() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let id = B256::repeat_byte(0xEE);
    let other = lane_key(id, lane_of(&store, &signer), Address::repeat_byte(0x55));
    let progress = decdn_incentive::BuyerLaneProgress {
        last_amount: U256::from(9_000_000u64),
        last_bytes: U256::from(9_000_000_000u64),
    };
    store
        .record(&BuyerPoolState::hydrate(
            id,
            DEPLOYMENT,
            signer.address(),
            TOKEN,
            U256::from(WORKING),
            vec![(other, progress)],
            U256::ZERO,
        ))
        .unwrap();

    let mut calls = vec![
        Some(still_open(signer.address())),
        Some(lane(1_000_000, 1_000_000_000)),
    ];
    calls.extend(refill_fails_with_wallet(0));
    let (result, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        calls,
        &RunFunding::default(),
    )
    .await;
    let err = result.expect_err("the untracked lane's watermark spends the rest");
    assert!(
        format!("{err:#}").contains("has no unspent deposit"),
        "{err:#}"
    );
    assert!(drained);
}

/// #2289: a wallet too short of USDC for the refill keeps the lane while the
/// pool can still pay. The live pool has 1.345 USDC unspent, below the low
/// water, so the refill fires; the wallet holds none. The lane is built on
/// the current deposit, the row is untouched, and the run records the
/// shortfall for its closing warning.
#[tokio::test]
async fn a_wallet_short_of_usdc_keeps_the_lane_while_the_pool_can_still_pay() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let id = B256::repeat_byte(0xEE);
    store
        .record(&tracked_row(
            id,
            signer.address(),
            lane_of(&store, &signer),
            WORKING,
            &LIVE_LANES,
        ))
        .unwrap();
    let funding = RunFunding::default();

    let (result, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        [
            vec![Some(still_open(signer.address()))],
            refill_fails_with_wallet(0),
        ]
        .concat(),
        &funding,
    )
    .await;
    let (ctx, _) = result.expect("1.345 USDC is left, so a short wallet must not drop the lane");

    assert!(drained, "the refill fired and read the wallet balance");
    assert_eq!(ctx.pool_id, id);
    assert_eq!(
        (ctx.prior_bytes_delivered, ctx.prior_amount),
        (U256::from(LIVE_LANES[0] * 1000), U256::from(LIVE_LANES[0])),
        "the lane resumes from its own recorded progress"
    );
    let row = store.get_by_owner(signer.address()).unwrap().unwrap();
    assert_eq!(row.deposit, U256::from(WORKING), "nothing was credited");
    assert_eq!(row.committed_amount(), U256::from(8_655_000u64));
    let shortfall = funding.shortfall().expect("the shortfall is recorded");
    assert!(shortfall.contains("holds 0 µUSDC"), "{shortfall}");
    assert!(shortfall.contains("8655000 µUSDC top-up"), "{shortfall}");
}

/// Once the run has seen the wallet too short of USDC, a later lane build
/// does not ask again: no allowance read, no `topUp`. With no answers
/// queued, any chain read would fault the build.
#[tokio::test]
async fn a_later_lane_build_skips_the_refill_after_a_wallet_shortfall() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let id = B256::repeat_byte(0xEE);
    store
        .record(&tracked_row(
            id,
            signer.address(),
            lane_of(&store, &signer),
            WORKING,
            &LIVE_LANES,
        ))
        .unwrap();
    let funding = RunFunding::default();
    let (first, _) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        [
            vec![Some(still_open(signer.address()))],
            refill_fails_with_wallet(0),
        ]
        .concat(),
        &funding,
    )
    .await;
    first.expect("the first build records the shortfall and keeps its lane");

    let (later, _) = run_in(&store, &signer, ChainAdoption::Allowed, vec![], &funding).await;
    later.expect("the later build reads nothing from the chain");
}

/// A refill that fails while the wallet holds enough USDC is not a
/// shortfall: the cause may be transient, so the lane build fails and the
/// acquire loop retries it with backoff.
#[tokio::test]
async fn a_refill_failure_with_a_funded_wallet_fails_the_lane_build() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let id = B256::repeat_byte(0xEE);
    store
        .record(&tracked_row(
            id,
            signer.address(),
            lane_of(&store, &signer),
            WORKING,
            &LIVE_LANES,
        ))
        .unwrap();
    let funding = RunFunding::default();

    let (result, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        [
            vec![Some(still_open(signer.address()))],
            refill_fails_with_wallet(WORKING),
        ]
        .concat(),
        &funding,
    )
    .await;
    let err = result.expect_err("a funded wallet's failed refill is retried, not skipped");
    assert!(
        format!("{err:#}").contains("read USDC allowance"),
        "{err:#}"
    );
    assert!(drained);
    assert!(funding.shortfall().is_none());
}

/// An adopted pool that other lanes have drained to 0.5 USDC fires the
/// refill through `totalRedeemed`, and still buys on its remainder when
/// the wallet is empty. Adoption only takes a pool with deposit left.
#[tokio::test]
async fn an_adopted_pool_drained_by_other_lanes_refills_and_buys_on_its_remainder() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let owner = signer.address();
    let id = B256::repeat_byte(0xDD);
    let mut calls = vec![
        Some(vec![id].abi_encode().into()),
        Some(pool(owner, 10_000_000, 9_500_000).abi_encode().into()),
        Some(TOKEN.abi_encode().into()),
        // This lane itself has spent nothing — the drain is elsewhere.
        Some(lane(0, 0)),
    ];
    calls.extend(refill_fails_with_wallet(0));
    let funding = RunFunding::default();

    let (result, drained) = run_in(&store, &signer, ChainAdoption::Allowed, calls, &funding).await;
    let (ctx, spend) = result.expect("0.5 USDC is left, so a short wallet must not drop the lane");

    assert!(
        drained,
        "0.5 USDC left is below the 2 USDC low water, so the refill must fire"
    );
    assert_eq!(ctx.pool_id, id);
    assert_eq!(
        spend,
        LaneSpend {
            outside: U256::from(9_500_000u64),
            prior: U256::ZERO,
            recorded: false,
            from_chain: true,
        }
    );
}

/// #2292: the run after adoption reuses the row without asking the chain.
/// The pool's `totalRedeemed` stays in its spend: read only by the
/// adopting run, every later run would count just the lanes this machine
/// recorded since, and read the pool as fuller than it is. The lane the
/// adopting run seeds from chain takes over its own share of it, so the
/// other lanes' share still counts once that lane advances.
#[tokio::test]
async fn a_reused_adopted_row_keeps_the_pools_redeemed_spend() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let owner = signer.address();
    let id = B256::repeat_byte(0xDE);
    let lane_spend = |outside: u64, recorded: bool| LaneSpend {
        outside: U256::from(outside),
        prior: U256::from(1_000_000u64),
        recorded,
        from_chain: true,
    };

    // 3 USDC redeemed, 1 USDC of it on this lane.
    let (adopting, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        vec![
            Some(vec![id].abi_encode().into()),
            Some(pool(owner, 10_000_000, 3_000_000).abi_encode().into()),
            Some(TOKEN.abi_encode().into()),
            Some(lane(1_000_000, 1_000_000_000)),
        ],
        &RunFunding::default(),
    )
    .await;
    assert!(drained);
    assert_eq!(
        adopting.expect("the live pool is adopted").1,
        lane_spend(2_000_000, false)
    );
    let row = store
        .get_by_owner(owner)
        .unwrap()
        .expect("adopted row is recorded");
    assert_eq!(row.redeemed_elsewhere(), U256::from(2_000_000u64));
    assert_eq!(row.pool_spend(), U256::from(3_000_000u64));

    // No chain answers queued: the row and its lane are tracked now.
    let (reusing, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        vec![],
        &RunFunding::default(),
    )
    .await;
    assert!(drained);
    let (ctx, spend) = reusing.expect("the tracked row is reused");
    assert_eq!(ctx.pool_id, id);
    assert_eq!(
        spend,
        lane_spend(2_000_000, true),
        "a later run sees the redeemed spend the adopting run saw"
    );

    // The lane pays on to 1.5 USDC: the pool has spent 3.5 USDC.
    let a = lane_key(id, lane_of(&store, &signer), PROVIDER);
    let outcome = store
        .advance_progress(
            owner,
            id,
            a,
            U256::from(1_500_000_000u64),
            U256::from(1_500_000u64),
        )
        .unwrap();
    assert_matches!(outcome, AdvanceOutcome::Advanced);
    assert_eq!(
        store.get_by_owner(owner).unwrap().unwrap().pool_spend(),
        U256::from(3_500_000u64)
    );
}

/// #2468: two data dirs on one wallet adopt the same live pool, and each
/// signs on its own lanes. Signing with the wallet key in both would put two
/// processes on one `(pool, signer, provider)` lane, each from its own
/// watermark, and the provider rejects whichever falls behind as `UnderFold`.
#[tokio::test]
async fn two_data_dirs_adopt_one_pool_on_disjoint_lanes() {
    let signer = Arc::new(PrivateKeySigner::random());
    let owner = signer.address();
    let id = B256::repeat_byte(0xDE);
    let adopt_calls = || {
        vec![
            Some(vec![id].abi_encode().into()),
            Some(pool(owner, WORKING, 0).abi_encode().into()),
            Some(TOKEN.abi_encode().into()),
            Some(lane(0, 0)),
        ]
    };
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (store_a, store_b) = (client_store(&dir_a), client_store(&dir_b));
    let a = run(&store_a, &signer, ChainAdoption::Allowed, adopt_calls())
        .await
        .expect("data dir A adopts the pool");
    let b = run(&store_b, &signer, ChainAdoption::Allowed, adopt_calls())
        .await
        .expect("data dir B adopts the pool");

    assert_eq!(a.pool_id, id);
    assert_eq!(b.pool_id, id);
    let (lane_a, lane_b) = (a.client_signer.address(), b.client_signer.address());
    assert_ne!(lane_a, lane_b, "two data dirs share a lane signer");
    assert_ne!(lane_a, owner, "data dir A signs with the wallet key");
    assert_ne!(lane_b, owner, "data dir B signs with the wallet key");
    for ctx in [&a, &b] {
        let cap = ctx.capability.as_ref().expect("a self capability");
        assert_eq!(
            cap.recover_owner(&DEPLOYMENT.voucher_domain()).unwrap(),
            owner
        );
        assert_eq!(cap.capability.signer, ctx.client_signer.address());
    }
}

/// #2468 review: data dir A opened the pool and tracks it, and another data
/// dir on the wallet has since redeemed 9 of its 10 USDC on lanes A does not
/// track. A's next run reads that spend from chain, so the pool reads 1 USDC
/// remaining and the low-water refill fires. Read from A's row alone, the pool
/// would read full: no refill, and a funding recovery that never tops up.
#[tokio::test]
async fn a_tracked_row_counts_what_another_data_dir_redeemed() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let owner = signer.address();
    let id = B256::repeat_byte(0xEE);
    store
        .record(&tracked_row(
            id,
            owner,
            lane_of(&store, &signer),
            WORKING,
            &[],
        ))
        .unwrap();

    let mut calls = vec![
        Some(pool(owner, WORKING, 9_000_000).abi_encode().into()),
        Some(lane(0, 0)),
    ];
    calls.extend(refill_fails_with_wallet(0));
    let (result, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        calls,
        &RunFunding::default(),
    )
    .await;
    let (_, spend) = result.expect("1 USDC remains, so the lane builds");
    assert_eq!(
        spend.outside,
        U256::from(9_000_000u64),
        "the other data dir's redemptions count as spend"
    );
    assert!(drained, "the low pool fired the refill");
}

/// A ledger seeded at `amount`.
fn ledger_at(amount: u64) -> Arc<PoolLedger> {
    Arc::new(PoolLedger::new(Cumulative {
        bytes: U256::from(amount) * U256::from(1000u64),
        amount: U256::from(amount),
    }))
}

fn spend(outside: u64, prior: u64, recorded: bool, from_chain: bool) -> LaneSpend {
    LaneSpend {
        outside: U256::from(outside),
        prior: U256::from(prior),
        recorded,
        from_chain,
    }
}

/// The pool's spend is the run's baseline plus every joined lane's
/// ledger. The first lane sets the baseline; a later lane the row recorded
/// moves its prior out of it, because its ledger counts it now; a lane the
/// row did not hold moves nothing, because the baseline never counted its
/// chain prior; a lane that joins again changes nothing.
#[test]
fn run_funding_counts_every_lane_once_on_a_tracked_pool() {
    let id = B256::repeat_byte(0xEE);
    let owner = Address::repeat_byte(0x01);
    let funding = RunFunding::default();
    assert_eq!(funding.pool_spent(), None);

    // The live pool: PROVIDER's lane is the first to join.
    let first = lane_key(id, owner, PROVIDER);
    let first_ledger = ledger_at(2_374_000);
    funding.join_lane(
        first,
        spend(6_281_000, 2_374_000, true, false),
        &first_ledger,
    );
    assert_eq!(funding.pool_spent(), Some(U256::from(8_655_000u64)));

    let second = lane_key(id, owner, Address::repeat_byte(0x41));
    funding.join_lane(
        second,
        spend(0, 2_360_000, true, false),
        &ledger_at(2_360_000),
    );
    assert_eq!(funding.pool_spent(), Some(U256::from(8_655_000u64)));

    // A new provider whose chain watermark the row never held.
    let fresh = lane_key(id, owner, Address::repeat_byte(0x55));
    funding.join_lane(fresh, spend(0, 100, false, false), &ledger_at(100));
    assert_eq!(funding.pool_spent(), Some(U256::from(8_655_100u64)));

    funding.join_lane(first, spend(0, 6_281_000, true, false), &first_ledger);
    assert_eq!(funding.pool_spent(), Some(U256::from(8_655_100u64)));
}

/// An adopted pool's baseline holds its redeemed spend no seeded lane
/// accounts for, which counts every unseeded lane's redeemed watermark. A later lane's chain prior moves out of
/// it, recorded or not, so it is counted once, by the lane's ledger.
#[test]
fn run_funding_counts_every_lane_once_on_an_adopted_pool() {
    let id = B256::repeat_byte(0xEE);
    let owner = Address::repeat_byte(0x01);
    let funding = RunFunding::default();
    funding.join_lane(
        lane_key(id, owner, PROVIDER),
        spend(9_000_000, 500_000, false, true),
        &ledger_at(500_000),
    );
    assert_eq!(funding.pool_spent(), Some(U256::from(9_500_000u64)));
    funding.join_lane(
        lane_key(id, owner, Address::repeat_byte(0x41)),
        spend(0, 1_000_000, false, false),
        &ledger_at(1_000_000),
    );
    assert_eq!(funding.pool_spent(), Some(U256::from(9_500_000u64)));
}

/// A lane's spend is what its ledgers committed, so it grows as the lane
/// pays. A lane rebuilt with a fresh ledger from a stale prior keeps the
/// larger of its ledgers: vouchers on a lane are cumulative.
#[test]
fn run_funding_follows_each_lanes_largest_ledger() {
    let id = B256::repeat_byte(0xEE);
    let owner = Address::repeat_byte(0x01);
    let funding = RunFunding::default();
    let lane = lane_key(id, owner, PROVIDER);
    let paid = ledger_at(0);
    funding.join_lane(lane, spend(1_000, 0, false, false), &paid);
    assert!(paid.reseed(Cumulative {
        bytes: U256::from(300_000u64),
        amount: U256::from(300u64),
    }));
    assert_eq!(funding.pool_spent(), Some(U256::from(1_300u64)));

    funding.join_lane(lane, spend(1_000, 0, false, false), &ledger_at(0));
    assert_eq!(funding.pool_spent(), Some(U256::from(1_300u64)));
}

/// A lane build hands back what it learned about the pool's spend: the
/// spend on every other lane, and the lane's own recorded prior.
#[tokio::test]
async fn a_lane_build_reports_the_pools_other_spend() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let id = B256::repeat_byte(0xEE);
    store
        .record(&tracked_row(
            id,
            signer.address(),
            lane_of(&store, &signer),
            2 * WORKING,
            &LIVE_LANES,
        ))
        .unwrap();

    let (result, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        vec![],
        &RunFunding::default(),
    )
    .await;
    let (_, spend) = result.expect("11.345 USDC is left, so no refill fires");
    assert!(drained);
    assert_eq!(
        spend,
        LaneSpend {
            outside: U256::from(8_655_000u64 - LIVE_LANES[0]),
            prior: U256::from(LIVE_LANES[0]),
            recorded: true,
            from_chain: false,
        }
    );
}

/// The run's funder reports the pool's spend to the deposit gate. A funding
/// recovery step that fails for another reason is not a shortfall; after a
/// wallet shortfall, a step fails without a chain call and says so
/// ([`WalletShortfall`]). A delegated signer's zero working deposit leaves
/// the step no funding path.
#[tokio::test]
async fn the_cli_funder_reads_the_run_funding() {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let rpc = ProviderBuilder::new()
        .layer(SendLegLayer)
        .connect_mocked_client(Asserter::new());
    let contract = PaymentPool::new(PP, rpc.clone());
    let funding = RunFunding::default();
    funding.join_lane(
        lane_key(
            B256::repeat_byte(0xEE),
            Address::repeat_byte(0x01),
            PROVIDER,
        ),
        spend(700, 0, false, false),
        &ledger_at(0),
    );
    funding.note_pool(B256::repeat_byte(0xEE));
    let funder = PoolFunder {
        contract: &contract,
        rpc: &rpc,
        store: &store,
        owner: Address::repeat_byte(0x01),
        deployment: DEPLOYMENT,
        token: TOKEN,
        payment_pool_addr: PP,
        working_deposit: U256::from(WORKING),
        max_approve: false,
        funding: &funding,
    };
    assert_eq!(funder.pool_spent(), Some(U256::from(700u64)));

    // No answers are queued, so the allowance read faults: not a shortfall.
    let err = funder.recover(U256::from(5u64)).await.unwrap_err();
    assert!(funding.shortfall().is_none(), "{err:#}");
    assert!(err.downcast_ref::<WalletShortfall>().is_none(), "{err:#}");

    *funding.shortfall.lock().unwrap() = Some("wallet 0x01 holds 0 µUSDC".to_owned());
    let err = funder.recover(U256::from(5u64)).await.unwrap_err();
    assert!(
        format!("{err:#}").contains("the wallet cannot fund a top-up: wallet 0x01 holds 0"),
        "{err:#}"
    );
    assert!(err.downcast_ref::<WalletShortfall>().is_some(), "{err:#}");

    let delegated = PoolFunder {
        working_deposit: U256::ZERO,
        ..funder
    };
    assert_matches!(
        delegated.recover(U256::ZERO).await,
        Ok(Recovery::Unavailable)
    );
}

/// The owner key of [`step_on_a_full_pool`]'s pool, so a test can script its
/// `getPool` answer.
fn full_pool_owner() -> Arc<PrivateKeySigner> {
    Arc::new(PrivateKeySigner::from_bytes(&B256::repeat_byte(0x42)).unwrap())
}

/// The `getPool` answer a recovery step reads first: [`full_pool_owner`]'s
/// open pool at the working deposit, `redeemed` of it paid out.
fn full_pool_on_chain(redeemed: u64) -> Bytes {
    pool(full_pool_owner().address(), WORKING, redeemed)
        .abi_encode()
        .into()
}

/// Run a recovery step for a self-funded pool that, as far as the run knows,
/// already holds its working deposit. The step reads the pool on chain first
/// (`asserter`'s first answer, see [`full_pool_on_chain`]). Also returns the
/// opens the step left unconfirmed in the run's handle.
async fn step_on_a_full_pool(
    asserter: Asserter,
) -> (anyhow::Result<Recovery>, Vec<OpenUnconfirmed>) {
    step_on_a_full_pool_holding(asserter, None).await
}

/// [`step_on_a_full_pool`], in a run that holds `untracked` as an earlier
/// escrow no record credits ([`UnconfirmedEscrows::untracked`]) when it is
/// `Some`.
async fn step_on_a_full_pool_holding(
    asserter: Asserter,
    untracked: Option<anyhow::Error>,
) -> (anyhow::Result<Recovery>, Vec<OpenUnconfirmed>) {
    step_on_the_pool(asserter, untracked, false, WORKING).await
}

/// [`step_on_a_full_pool_holding`] for a run that sees `remaining` of the
/// pool's deposit, in a command that saw the pool live
/// ([`UnconfirmedEscrows::note_live`]) when `seen_live` is set.
async fn step_on_the_pool(
    asserter: Asserter,
    untracked: Option<anyhow::Error>,
    seen_live: bool,
    remaining: u64,
) -> (anyhow::Result<Recovery>, Vec<OpenUnconfirmed>) {
    let funding = RunFunding::default();
    if let Some(err) = untracked {
        funding.escrows().hold_untracked(&err);
    }
    if seen_live {
        funding.escrows().note_live(STEPPED_POOL);
    }
    let stepped = step_in(asserter, &funding, remaining).await;
    (stepped, funding.escrows().opens())
}

/// The pool [`step_in`] steps on.
const STEPPED_POOL: PoolId = B256::repeat_byte(0xEE);

/// Run a recovery step in the run `funding` describes, for a self-funded
/// pool ([`STEPPED_POOL`]) that holds its working deposit in the store, the
/// run seeing `remaining` of it. `asserter` answers the step's chain calls.
async fn step_in(
    asserter: Asserter,
    funding: &RunFunding,
    remaining: u64,
) -> anyhow::Result<Recovery> {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = full_pool_owner();
    store
        .record(&tracked_row(
            STEPPED_POOL,
            signer.address(),
            lane_of(&store, &signer),
            WORKING,
            &[],
        ))
        .unwrap();
    let rpc = ProviderBuilder::new()
        .layer(SendLegLayer)
        .connect_mocked_client(asserter);
    let contract = PaymentPool::new(PP, rpc.clone());
    funding.note_pool(STEPPED_POOL);
    let funder = PoolFunder {
        contract: &contract,
        rpc: &rpc,
        store: &store,
        owner: signer.address(),
        deployment: DEPLOYMENT,
        token: TOKEN,
        payment_pool_addr: PP,
        working_deposit: U256::from(WORKING),
        max_approve: false,
        funding,
    };
    funder.recover(U256::from(remaining)).await
}

/// A full pool that still accepts funds settles: the step escrows nothing and
/// reports the deposit the nodes have yet to see, so the settle window asks
/// them again.
#[tokio::test]
async fn a_full_open_pool_settles_without_a_top_up() {
    let asserter = Asserter::new();
    asserter.push_success(&full_pool_on_chain(0));
    asserter.push_success(&alloy::primitives::U64::from(50_000u64));
    let stepped = step_on_a_full_pool(asserter.clone()).await.0.unwrap();
    assert_matches!(stepped, Recovery::ToppedUp(d) if d == U256::from(WORKING));
    assert!(
        asserter.read_q().is_empty(),
        "only the pool read and the estimate ran"
    );
}

/// #2468 review: another data dir on the wallet redeemed 9 of the pool's 10
/// USDC while this run went on. The run's own view reads the pool full, but
/// the step reads the chain and tops up rather than only settling. Here the
/// wallet holds no USDC, so the top-up ends in a shortfall.
#[tokio::test]
async fn a_pool_another_data_dir_drained_tops_up() {
    let asserter = Asserter::new();
    asserter.push_success(&full_pool_on_chain(9_000_000));
    // The allowance read faults, and the wallet holds no USDC.
    asserter.push_failure_msg("allowance read faulted");
    asserter.push_success(&U256::ZERO.abi_encode());
    let err = step_on_a_full_pool(asserter.clone())
        .await
        .0
        .expect_err("a wallet with no USDC cannot top up");
    assert!(
        err.downcast_ref::<WalletShortfall>().is_some(),
        "the step tried a top-up rather than settling: {err:#}"
    );
    assert!(
        asserter.read_q().is_empty(),
        "the pool, allowance and wallet reads ran"
    );
}

/// How far past the working deposit the overdrawn-pool tests' lanes signed.
const OVER: u64 = 7_000_000;

/// Run a recovery step for a pool whose run's lanes signed `WORKING + OVER`,
/// with the store recording `recorded` as its deposit and the run seeing
/// `remaining` of it. `pool_read` answers the step's `getPool`; the allowance
/// read then faults and the wallet holds no USDC, so the step ends in a
/// shortfall. Returns the shortfall, which names the top-up the step sized.
async fn shortfall_on_an_overdrawn_pool(
    recorded: u64,
    remaining: u64,
    pool_read: impl FnOnce(&Asserter),
) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = full_pool_owner();
    let id = B256::repeat_byte(0xEE);
    store
        .record(&tracked_row(
            id,
            signer.address(),
            lane_of(&store, &signer),
            recorded,
            &[],
        ))
        .unwrap();
    let asserter = Asserter::new();
    pool_read(&asserter);
    asserter.push_failure_msg("allowance read faulted");
    asserter.push_success(&U256::ZERO.abi_encode());
    let rpc = ProviderBuilder::new()
        .layer(SendLegLayer)
        .connect_mocked_client(asserter.clone());
    let contract = PaymentPool::new(PP, rpc.clone());
    let funding = RunFunding::default();
    funding.join_lane(
        lane_key(id, signer.address(), PROVIDER),
        spend(WORKING + OVER, 0, false, false),
        &ledger_at(0),
    );
    funding.note_pool(id);
    let funder = PoolFunder {
        contract: &contract,
        rpc: &rpc,
        store: &store,
        owner: signer.address(),
        deployment: DEPLOYMENT,
        token: TOKEN,
        payment_pool_addr: PP,
        working_deposit: U256::from(WORKING),
        max_approve: false,
        funding: &funding,
    };

    let err = funder
        .recover(U256::from(remaining))
        .await
        .expect_err("a wallet with no USDC cannot top up");
    assert!(err.downcast_ref::<WalletShortfall>().is_some(), "{err:#}");
    assert!(
        asserter.read_q().is_empty(),
        "the pool, allowance and wallet reads ran"
    );
    funding.shortfall().unwrap_or_default()
}

/// A pool the run's lanes overdrew tops up by the overdraft as well as the
/// working deposit. The lanes signed vouchers past the deposit, so a node
/// redeems that overdraft out of the top-up on its next redeem sweep. A top-up
/// of only the working deposit would leave the pool below a node's floor, every
/// node would keep refusing it, and the fetch would end "funding needed"
/// (#2629).
#[tokio::test]
async fn an_overdrawn_pool_tops_up_by_its_overdraft_too() {
    let shortfall = shortfall_on_an_overdrawn_pool(WORKING, 0, |asserter| {
        asserter.push_success(&full_pool_on_chain(WORKING));
    })
    .await;
    let wanted = WORKING + OVER;
    assert!(
        shortfall.contains(&format!("less than the {wanted} µUSDC top-up")),
        "the top-up covers the overdraft and the working deposit: {shortfall}"
    );
}

/// A failed pool read still counts the overdraft the run knows: its lanes'
/// spend past the deposit its store records. Only other signers' redemptions,
/// which reach the run from chain alone, go unseen.
#[tokio::test]
async fn a_failed_pool_read_still_tops_up_the_runs_overdraft() {
    let shortfall = shortfall_on_an_overdrawn_pool(WORKING, 0, |asserter| {
        asserter.push_failure_msg("getPool faulted");
    })
    .await;
    let wanted = WORKING + OVER;
    assert!(
        shortfall.contains(&format!("less than the {wanted} µUSDC top-up")),
        "the run's own overdraft survives a failed read: {shortfall}"
    );
}

/// A lagging RPC can answer `getPool` from before a top-up the store already
/// credited. The step takes the larger of the recorded and the on-chain
/// deposit, as [`BuyerPoolState::reconcile_with_chain`] does, so the stale
/// read invents no overdraft and the top-up asks only for what the working
/// deposit lacks.
#[tokio::test]
async fn a_stale_pool_read_invents_no_overdraft() {
    let half = WORKING / 2;
    // The store holds a top-up the chain read does not show yet: the lanes'
    // `WORKING + OVER` spend leaves `half` of it.
    let recorded = WORKING + OVER + half;
    let shortfall = shortfall_on_an_overdrawn_pool(recorded, half, |asserter| {
        asserter.push_success(&full_pool_on_chain(0));
    })
    .await;
    assert!(
        shortfall.contains(&format!("less than the {half} µUSDC top-up")),
        "the top-up fills only the working deposit's gap: {shortfall}"
    );
}

/// A full pool that is closing or closed still needs a replacement: its
/// `topUp` estimate reverts `PoolNotOpen`, and the step goes on to open a new
/// pool (whose chain calls this mock leaves unanswered, so the open's gas
/// estimate fails before anything is sent).
#[tokio::test]
async fn a_full_closing_pool_is_replaced() {
    let selector = alloy::primitives::keccak256("PoolNotOpen()");
    let asserter = Asserter::new();
    asserter.push_success(&full_pool_on_chain(0));
    asserter.push_failure(
        serde_json::from_value::<alloy_json_rpc::ErrorPayload>(serde_json::json!({
            "code": 3,
            "message": "execution reverted",
            "data": format!("0x{}", alloy::primitives::hex::encode(&selector[..4])),
        }))
        .unwrap(),
    );
    // The open's allowance read finds enough, and its pending-nonce read
    // answers, so the open goes on to submit.
    asserter.push_success(&U256::MAX.abi_encode());
    asserter.push_success(&U256::ZERO);
    let (stepped, opens) = step_on_a_full_pool(asserter.clone()).await;
    let err = stepped.expect_err("the replacement open reaches the chain");
    assert!(
        format!("{err:#}").contains("submit openPool"),
        "the step submitted an openPool, not a topUp: {err:#}"
    );
    assert!(
        asserter.read_q().is_empty(),
        "the estimate and the allowance ran"
    );
    // The open's gas estimate failed before the send, so nothing was signed
    // and the run's handle holds no open (#2431).
    let msg = format!("{err:#}");
    assert!(msg.contains("eth_estimateGas"), "{msg}");
    assert!(!msg.contains("eth_sendTransaction"), "{msg}");
    assert!(opens.is_empty(), "{opens:?}");
}

/// The `getPool` answer for a pool the chain holds no record of: a zeroed
/// struct, whose status reads `Open` because `Open` is enum value 0.
fn no_record_on_chain() -> Bytes {
    pool(Address::ZERO, 0, 0).abi_encode().into()
}

/// Queue the chain calls of a replacement open: the allowance read finds
/// enough, and the pending-nonce read answers, so the open goes on to submit.
/// Its gas estimate is left unanswered, so nothing is sent.
fn answer_a_replacement_open(asserter: &Asserter) {
    asserter.push_success(&U256::MAX.abi_encode());
    asserter.push_success(&U256::ZERO);
}

/// Assert that a recovery step went straight to a replacement open, which the
/// mock stops at its gas estimate: no `topUp` and no settle estimate ran, and
/// nothing was signed.
fn assert_replaced_without_a_top_up(
    stepped: anyhow::Result<Recovery>,
    opens: &[OpenUnconfirmed],
    asserter: &Asserter,
) {
    let err = stepped.expect_err("the replacement open reaches the chain");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("submit openPool"),
        "the step submitted an openPool: {msg}"
    );
    assert!(!msg.contains("NotPoolOwner"), "no topUp reverted: {msg}");
    assert!(msg.contains("eth_estimateGas"), "{msg}");
    assert!(!msg.contains("eth_sendTransaction"), "{msg}");
    assert!(
        asserter.read_q().is_empty(),
        "the pool read and the open's allowance and nonce reads ran"
    );
    assert!(opens.is_empty(), "{opens:?}");
}

/// #2665: the stored row names a pool the chain holds no record of, as a row
/// from another deployment does. `getPool` reads a zero owner and the status
/// `Open`, and a `topUp` would revert `NotPoolOwner`. No run of the command
/// saw the pool live, so the step opens a replacement, as it does for a closed
/// pool. Here the run sees the pool short, so it would otherwise top up.
#[tokio::test]
async fn a_stored_pool_the_chain_does_not_know_is_replaced() {
    let asserter = Asserter::new();
    asserter.push_success(&no_record_on_chain());
    answer_a_replacement_open(&asserter);
    let (stepped, opens) = step_on_the_pool(asserter.clone(), None, false, 0).await;
    assert_replaced_without_a_top_up(stepped, &opens, &asserter);
}

/// #2665, the settle path: the run sees the unknown pool full, so a step on a
/// pool the chain knows would only settle. The settle estimate would revert
/// `NotPoolOwner` and its fallback read would show the zeroed pool `Open`, so
/// the step would settle on a pool that does not exist. It replaces it.
#[tokio::test]
async fn a_full_stored_pool_the_chain_does_not_know_is_replaced() {
    let asserter = Asserter::new();
    asserter.push_success(&no_record_on_chain());
    answer_a_replacement_open(&asserter);
    let (stepped, opens) = step_on_the_pool(asserter.clone(), None, false, WORKING).await;
    assert_replaced_without_a_top_up(stepped, &opens, &asserter);
}

/// A pool a run of this command saw live reads as no record only on a lagging
/// RPC. A replacement would escrow a second deposit beside it, so the step
/// keeps the run's view: here the run sees the pool full, and the step
/// settles.
#[tokio::test]
async fn a_pool_the_command_saw_live_is_not_replaced_on_a_lagging_read() {
    let asserter = Asserter::new();
    asserter.push_success(&no_record_on_chain());
    // The settle path's `topUp` gas estimate answers: the pool accepts funds.
    asserter.push_success(&alloy::primitives::U64::from(50_000u64));
    let (stepped, opens) = step_on_the_pool(asserter.clone(), None, true, WORKING).await;
    assert_matches!(stepped.unwrap(), Recovery::ToppedUp(d) if d == U256::from(WORKING));
    assert!(
        asserter.read_q().is_empty(),
        "only the pool read and the estimate ran"
    );
    assert!(opens.is_empty(), "{opens:?}");
}

/// A pool the chain holds no record of is not replaced while a `topUp` of
/// the command is unconfirmed: the `topUp` may still mine on it, and a
/// replacement would escrow a second deposit beside it.
#[tokio::test]
async fn an_unconfirmed_top_up_refuses_replacing_an_unknown_pool() {
    let asserter = Asserter::new();
    asserter.push_success(&no_record_on_chain());
    let funding = RunFunding::default();
    funding.escrows().hold_pending_for_test(HELD);
    let err = step_in(asserter.clone(), &funding, 0)
        .await
        .expect_err("the step refuses");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("while a topUp of this command is unconfirmed"),
        "{msg}"
    );
    assert!(asserter.read_q().is_empty(), "only the pool read ran");
    assert!(funding.escrows().opens().is_empty(), "no openPool");
}

/// A sibling refill's `topUp` holds the run's `topUp` lock while the step
/// reads the pool as unknown, and mines on it: the pool is live. The step
/// decides under the same lock, so it sees that and opens no replacement.
#[tokio::test]
async fn a_pool_a_sibling_top_up_shows_live_while_the_step_waits_is_kept() {
    let asserter = Asserter::new();
    asserter.push_success(&no_record_on_chain());
    let funding = RunFunding::default();
    let sibling = funding.top_up_serial.lock().await;
    let mined_meanwhile = async {
        while !asserter.read_q().is_empty() {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        funding.escrows().note_live(STEPPED_POOL);
        drop(sibling);
    };
    let (stepped, ()) = tokio::join!(step_in(asserter.clone(), &funding, 0), mined_meanwhile);
    let msg = format!("{:#}", stepped.expect_err("the step refuses"));
    assert!(msg.contains("which this command saw live"), "{msg}");
    assert!(funding.escrows().opens().is_empty(), "no openPool");
}

/// A refill whose first pool read was inconclusive (the chain held no record
/// yet) and whose `topUp` then mined shows the pool live, so a later recovery
/// step that reads it as unknown keeps it rather than opening a replacement.
#[tokio::test(start_paused = true)]
async fn a_mined_refill_shows_its_pool_live_after_an_inconclusive_read() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let pool_id = a_spent_pool(&store, &signer);
    let funding = RunFunding::default();
    let rpc = ScriptedRpc::mined(mined_top_up())
        .answering_calls([no_record_on_chain(), Bytes::from(U256::MAX.abi_encode())]);
    run_scripted(&store, &signer, &rpc, &funding, &CancellationToken::new())
        .await
        .expect("the refill mines and the lane builds");
    assert_eq!(rpc.sent_nonces().len(), 1, "the refill sent its topUp");
    assert!(
        funding.escrows().saw_live(pool_id),
        "the mined topUp shows the pool live"
    );

    // The step keeps the run's view of the pool and tops it up again.
    let rpc = ScriptedRpc::mined(mined_top_up())
        .answering_calls([no_record_on_chain(), Bytes::from(U256::MAX.abi_encode())]);
    let stepped = recover_scripted(&store, &signer, &rpc, &funding)
        .await
        .expect("the step tops the live pool up");
    assert_matches!(stepped, Recovery::ToppedUp(_));
    assert_eq!(rpc.sent_nonces().len(), 1, "one topUp, no openPool");
    assert!(funding.escrows().opens().is_empty(), "no openPool");
}

/// Run a lane build's open on `rpc`, in a run reporting to `opens`: `usdc()`,
/// then the allowance read finds enough, and the pending-nonce read answers
/// 3, so the open goes on to submit. Returns the submit's error.
async fn lane_build_open_on(
    rpc: impl alloy::providers::Provider + Clone,
    asserter: &Asserter,
    opens: &UnconfirmedEscrows,
) -> anyhow::Error {
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    asserter.push_success(&Bytes::from(TOKEN.abi_encode()));
    asserter.push_success(&U256::MAX.abi_encode());
    asserter.push_success(&U256::from(3u64));
    let contract = PaymentPool::new(PP, rpc.clone());
    let funding = RunFunding::reporting_escrows_to(opens.clone());
    let err = open_or_reuse_pool(
        &store,
        &contract,
        &rpc,
        &funding,
        &OwnedPayer::for_store(Arc::clone(&signer), &store).unwrap(),
        PROVIDER,
        DEPLOYMENT,
        DepositPolicy {
            working_deposit: U256::from(WORKING),
            max_approve: false,
            adoption: ChainAdoption::Refused,
        },
        &CancellationToken::new(),
    )
    .await
    .expect_err("the open's submit fails");
    assert!(format!("{err:#}").contains("submit openPool"), "{err:#}");
    err
}

/// A lane build that opens the run's pool holds the `openPool` in the run's
/// handle, so a command that drops the build can still name it (#2418). The
/// provider is filler-free and send-leg-marked, so the empty queue fails the
/// send request itself, which may have broadcast.
#[tokio::test]
async fn a_lane_build_open_is_held_in_the_runs_unconfirmed_escrows() {
    let asserter = Asserter::new();
    let rpc = ProviderBuilder::default()
        .layer(SendLegLayer)
        .connect_mocked_client(asserter.clone());
    let opens = UnconfirmedEscrows::default();
    lane_build_open_on(rpc, &asserter, &opens).await;
    assert_eq!(opens.opens(), vec![OpenUnconfirmed { tx: None, nonce: 3 }]);
}

/// A lane build whose open fails on a chain read before the send (here the
/// gas estimate, on a provider with fillers) signed nothing, so the run's
/// handle holds no open (#2431).
#[tokio::test]
async fn a_lane_build_open_whose_reads_fail_is_not_held() {
    let asserter = Asserter::new();
    let rpc = ProviderBuilder::new()
        .layer(SendLegLayer)
        .connect_mocked_client(asserter.clone());
    let opens = UnconfirmedEscrows::default();
    let err = lane_build_open_on(rpc, &asserter, &opens).await;
    assert!(err.downcast_ref::<OpenUnconfirmed>().is_none(), "{err:#}");
    assert!(format!("{err:#}").contains("eth_estimateGas"), "{err:#}");
    assert!(opens.opens().is_empty());
}

/// A delegated lane resumes from the delegate lanes the store holds for its
/// key at this provider on this deployment. The key's lane at another
/// provider is not this lane, and nor is a lane of the same key and pool on
/// another deployment: `pool_id` repeats across deployments.
#[tokio::test]
async fn a_delegated_lane_resumes_from_its_delegate_record() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let store = RedbBuyerPoolStore::open(&dir.path().join("data"))?;
    let owner = Arc::new(PrivateKeySigner::random());
    let delegate = Arc::new(PrivateKeySigner::random());
    let pool_id = PoolId::repeat_byte(0x11);
    let signed = decdn_incentive::Capability {
        signer: delegate.address(),
        spending_cap: 1_000_000,
        pool_id,
        expiry: u64::MAX,
    }
    .sign(owner.as_ref(), &bind_node_id_domain(1, Address::ZERO))?;
    let grant = CapabilityGrant::from_signed_capability(&signed);
    let lane = LaneKey {
        pool_id,
        signer: delegate.address(),
        provider: PROVIDER,
    };
    let foreign = Deployment {
        chain_id: 2,
        ..DEPLOYMENT
    };
    let elsewhere = LaneKey {
        provider: Address::repeat_byte(0x78),
        ..lane
    };
    let outcome = store.advance_delegate_progress(
        DEPLOYMENT,
        elsewhere,
        decdn_incentive::BuyerLaneProgress {
            last_amount: U256::from(500u64),
            last_bytes: U256::from(50_000u64),
        },
    )?;
    anyhow::ensure!(outcome == AdvanceOutcome::Advanced);
    for (deployment, bytes, amount) in [(DEPLOYMENT, 4_096u64, 40u64), (foreign, 9_999, 99)] {
        let outcome = store.advance_delegate_progress(
            deployment,
            lane,
            decdn_incentive::BuyerLaneProgress {
                last_amount: U256::from(amount),
                last_bytes: U256::from(bytes),
            },
        )?;
        anyhow::ensure!(outcome == AdvanceOutcome::Advanced);
    }

    let ctx = delegated_ctx(&store, owner.address(), &delegate, &grant).await?;
    anyhow::ensure!(
        (ctx.prior_bytes_delivered, ctx.prior_amount) == (U256::from(4_096u64), U256::from(40u64)),
        "resumed at ({}, {})",
        ctx.prior_bytes_delivered,
        ctx.prior_amount
    );
    Ok(())
}

/// Build `delegate`'s lane context at [`PROVIDER`] under `grant`, against a
/// mocked `PaymentPool` that answers `getPool` with `owner`'s open pool and
/// `usdc()` with [`TOKEN`].
async fn delegated_ctx(
    store: &dyn BuyerPoolStore,
    owner: Address,
    delegate: &Arc<PrivateKeySigner>,
    grant: &CapabilityGrant,
) -> anyhow::Result<PoolContext> {
    let asserter = Asserter::new();
    asserter.push_success(&pool(owner, WORKING, 0).abi_encode());
    asserter.push_success(&TOKEN.abi_encode());
    let rpc = ProviderBuilder::new()
        .layer(SendLegLayer)
        .connect_mocked_client(asserter);
    let contract = PaymentPool::new(PP, rpc);
    let chain = ChainConfig {
        rpc_url: "http://config:8545".to_string(),
        chain_id: DEPLOYMENT.chain_id,
        payment_pool: PP,
        slash_judge: Address::repeat_byte(0x44),
        capacity_bond: None,
    };
    let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await?;
    let ctx = build_delegated_pool_ctx(
        store,
        &contract,
        delegate,
        &DEPLOYMENT.voucher_domain(),
        PROVIDER,
        &chain,
        &endpoint,
        grant,
    )
    .await;
    endpoint.close().await;
    ctx
}

/// The in-memory store, except that every delegate-lane read faults.
#[derive(Debug, Default)]
struct UnreadableDelegateLanes(decdn_incentive::MemoryBuyerPoolStore);

impl BuyerPoolStore for UnreadableDelegateLanes {
    fn load_all(&self) -> Result<decdn_incentive::BuyerLoad, decdn_incentive::StoreError> {
        self.0.load_all()
    }
    fn record(&self, state: &BuyerPoolState) -> Result<(), decdn_incentive::StoreError> {
        self.0.record(state)
    }
    fn forget(&self, owner: Address) -> Result<(), decdn_incentive::StoreError> {
        self.0.forget(owner)
    }
    fn get_by_pool_id(
        &self,
        pool_id: PoolId,
    ) -> Result<Option<BuyerPoolState>, decdn_incentive::StoreError> {
        self.0.get_by_pool_id(pool_id)
    }
    fn forget_if_pool(
        &self,
        owner: Address,
        pool_id: PoolId,
    ) -> Result<bool, decdn_incentive::StoreError> {
        self.0.forget_if_pool(owner, pool_id)
    }
    fn get_by_owner(
        &self,
        owner: Address,
    ) -> Result<Option<BuyerPoolState>, decdn_incentive::StoreError> {
        self.0.get_by_owner(owner)
    }
    fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, decdn_incentive::StoreError> {
        self.0.advance_progress(owner, pool_id, lane, bytes, amount)
    }
    fn rebase_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        anchor: decdn_incentive::BuyerLaneProgress,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, decdn_incentive::StoreError> {
        self.0.rebase_progress(owner, pool_id, lane, anchor, totals)
    }
    fn seed_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, decdn_incentive::StoreError> {
        self.0.seed_progress(owner, pool_id, lane, bytes, amount)
    }
    fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> Result<decdn_incentive::DepositOutcome, decdn_incentive::StoreError> {
        self.0.add_deposit(owner, pool_id, additional)
    }
    fn delegate_lanes(
        &self,
        _deployment: Deployment,
        pool_id: PoolId,
        _signer: Address,
    ) -> Result<Vec<(Address, decdn_incentive::BuyerLaneProgress)>, decdn_incentive::StoreError>
    {
        Err(decdn_incentive::StoreError::Corrupt {
            pool_id: Some(pool_id),
            detail: "delegate lane record damaged".into(),
        })
    }
    fn advance_delegate_progress(
        &self,
        deployment: Deployment,
        lane: LaneKey,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, decdn_incentive::StoreError> {
        self.0.advance_delegate_progress(deployment, lane, totals)
    }
    fn rebase_delegate_progress(
        &self,
        deployment: Deployment,
        lane: LaneKey,
        anchor: decdn_incentive::BuyerLaneProgress,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, decdn_incentive::StoreError> {
        self.0
            .rebase_delegate_progress(deployment, lane, anchor, totals)
    }

    fn lane_salt(
        &self,
    ) -> Result<decdn_incentive::lane_signer::LaneSalt, decdn_incentive::StoreError> {
        self.0.lane_salt()
    }
}

/// A delegate-lane record the store cannot read fails the delegated lane
/// build as this machine's fault: the acquire loop ends the command and does
/// not retry the build as a chain-side fault that can never clear.
#[tokio::test]
async fn an_unreadable_delegate_record_fails_the_lane_build_fatally() -> anyhow::Result<()> {
    let owner = Arc::new(PrivateKeySigner::random());
    let delegate = Arc::new(PrivateKeySigner::random());
    let signed = decdn_incentive::Capability {
        signer: delegate.address(),
        spending_cap: 1_000_000,
        pool_id: PoolId::repeat_byte(0x11),
        expiry: u64::MAX,
    }
    .sign(owner.as_ref(), &bind_node_id_domain(1, Address::ZERO))?;
    let grant = CapabilityGrant::from_signed_capability(&signed);
    let Err(err) = delegated_ctx(
        &UnreadableDelegateLanes::default(),
        owner.address(),
        &delegate,
        &grant,
    )
    .await
    else {
        anyhow::bail!("an unreadable delegate record built a lane");
    };
    let fault = crate::fault::classify(&anyhow::Error::new(crate::fault::LaneBuildFault(err)));
    anyhow::ensure!(
        matches!(
            fault,
            crate::fault::Fault::Fatal(crate::fault::FatalScope::Command)
        ),
        "classified {fault:?}"
    );
    Ok(())
}

/// Run [`open_or_reuse_pool`] on `rpc` for at most twice [`RECEIPT_TIMEOUT`],
/// and return the error it ends with: a lapse of a transaction that never
/// mines, an untracked escrow, or a refusal that sends nothing.
async fn run_until_err(
    store: &dyn BuyerPoolStore,
    signer: &Arc<PrivateKeySigner>,
    rpc: &decdn_incentive::test_rpc::ScriptedRpc,
    funding: &RunFunding,
) -> anyhow::Error {
    run_scripted(store, signer, rpc, funding, &CancellationToken::new())
        .await
        .expect_err("the lane build must fail")
}

/// Run [`open_or_reuse_pool`] on `rpc`, for a fetch that `ended` marks the
/// end of, for at most twice [`RECEIPT_TIMEOUT`].
async fn run_scripted(
    store: &dyn BuyerPoolStore,
    signer: &Arc<PrivateKeySigner>,
    rpc: &decdn_incentive::test_rpc::ScriptedRpc,
    funding: &RunFunding,
    ended: &CancellationToken,
) -> anyhow::Result<(PoolContext, LaneSpend)> {
    let provider = rpc.clone().provider();
    let contract = PaymentPool::new(PP, provider.clone());
    tokio::time::timeout(
        RECEIPT_TIMEOUT * 2,
        open_or_reuse_pool(
            store,
            &contract,
            &provider,
            funding,
            &OwnedPayer::for_store(Arc::clone(signer), store).unwrap(),
            PROVIDER,
            DEPLOYMENT,
            DepositPolicy {
                working_deposit: U256::from(WORKING),
                max_approve: false,
                adoption: ChainAdoption::Refused,
            },
            ended,
        ),
    )
    .await
    .expect("the lane build must end within twice RECEIPT_TIMEOUT")
}

/// A lane build whose `openPool` never mines ends at [`RECEIPT_TIMEOUT`] with
/// the open unconfirmed, in the error and in the run's handle, so `decdn get`
/// names the tx instead of hanging (#2433).
#[tokio::test(start_paused = true)]
async fn a_lane_build_open_that_never_mines_lapses_unconfirmed() {
    use decdn_incentive::test_rpc::{SCRIPTED_NONCE, SCRIPTED_TX, ScriptedRpc};

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    // `usdc()`, then an allowance read that finds enough.
    let rpc = ScriptedRpc::parked_receipt().answering_calls([
        Bytes::from(TOKEN.abi_encode()),
        Bytes::from(U256::MAX.abi_encode()),
    ]);
    let opens = UnconfirmedEscrows::default();
    let funding = RunFunding::reporting_escrows_to(opens.clone());
    let err = run_until_err(&store, &signer, &rpc, &funding).await;
    let unconfirmed = OpenUnconfirmed {
        tx: Some(SCRIPTED_TX),
        nonce: SCRIPTED_NONCE,
    };
    assert_eq!(
        err.downcast_ref::<OpenUnconfirmed>(),
        Some(&unconfirmed),
        "{err:#}"
    );
    assert!(format!("{err:#}").contains("no receipt within"), "{err:#}");
    assert_eq!(opens.opens(), vec![unconfirmed]);
    assert!(rpc.calls_drained(), "the open read the token and allowance");
}

/// A reused pool's low-water refill whose `topUp` never mines ends at
/// [`RECEIPT_TIMEOUT`] with [`TopUpUnconfirmed`] naming the tx (#2433).
#[tokio::test(start_paused = true)]
async fn a_refill_that_never_mines_lapses_unconfirmed() {
    use crate::buyer_pool::TopUpUnconfirmed;
    use decdn_incentive::test_rpc::{SCRIPTED_NONCE, SCRIPTED_TX, ScriptedRpc};

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    // Five lanes of 2 USDC spend the whole deposit, so the build refills.
    store
        .record(&tracked_row(
            B256::repeat_byte(0xEE),
            signer.address(),
            lane_of(&store, &signer),
            WORKING,
            &[2_000_000; 5],
        ))
        .unwrap();
    // `getPool` shows the row's pool open, then the allowance read finds enough.
    let rpc = ScriptedRpc::parked_receipt().answering_calls([
        still_open(signer.address()),
        Bytes::from(U256::MAX.abi_encode()),
    ]);
    let err = run_until_err(&store, &signer, &rpc, &RunFunding::default()).await;
    assert_eq!(
        err.downcast_ref::<TopUpUnconfirmed>(),
        Some(&TopUpUnconfirmed {
            tx: Some(SCRIPTED_TX),
            nonce: SCRIPTED_NONCE,
        }),
        "{err:#}"
    );
    assert!(format!("{err:#}").contains("no receipt within"), "{err:#}");
    assert!(
        rpc.calls_drained(),
        "the refill read the pool and allowance"
    );
}

/// Record a tracked row for `signer`'s pool whose five lanes of 2 USDC spend
/// the whole deposit, so every lane build of it refills and a recovery step
/// tops it up, and return the pool.
fn a_spent_pool(store: &RedbBuyerPoolStore, signer: &Arc<PrivateKeySigner>) -> PoolId {
    let pool_id = B256::repeat_byte(0xEE);
    store
        .record(&tracked_row(
            pool_id,
            signer.address(),
            lane_of(store, signer),
            WORKING,
            &[2_000_000; 5],
        ))
        .unwrap();
    pool_id
}

/// Run a recovery step for `signer`'s pool on `rpc` for at most twice
/// [`RECEIPT_TIMEOUT`], and return the error it ends with, as
/// [`run_until_err`] does. The step reads the pool first (a `getPool`
/// answer).
async fn recover_until_err(
    store: &dyn BuyerPoolStore,
    signer: &Arc<PrivateKeySigner>,
    rpc: &decdn_incentive::test_rpc::ScriptedRpc,
    funding: &RunFunding,
) -> anyhow::Error {
    recover_scripted(store, signer, rpc, funding)
        .await
        .expect_err("the recovery step must fail")
}

/// Run a recovery step for `signer`'s pool on `rpc` for at most twice
/// [`RECEIPT_TIMEOUT`].
async fn recover_scripted(
    store: &dyn BuyerPoolStore,
    signer: &Arc<PrivateKeySigner>,
    rpc: &decdn_incentive::test_rpc::ScriptedRpc,
    funding: &RunFunding,
) -> anyhow::Result<Recovery> {
    let provider = rpc.clone().provider();
    let contract = PaymentPool::new(PP, provider.clone());
    let funder = PoolFunder {
        contract: &contract,
        rpc: &provider,
        store,
        owner: signer.address(),
        deployment: DEPLOYMENT,
        token: TOKEN,
        payment_pool_addr: PP,
        working_deposit: U256::from(WORKING),
        max_approve: false,
        funding,
    };
    tokio::time::timeout(RECEIPT_TIMEOUT * 2, funder.recover(U256::ZERO))
        .await
        .expect("the recovery step must end within twice RECEIPT_TIMEOUT")
}

/// The `topUp` every [`ScriptedRpc`](decdn_incentive::test_rpc::ScriptedRpc)
/// send lapses as.
const HELD: crate::buyer_pool::TopUpUnconfirmed = crate::buyer_pool::TopUpUnconfirmed {
    tx: Some(decdn_incentive::test_rpc::SCRIPTED_TX),
    nonce: decdn_incentive::test_rpc::SCRIPTED_NONCE,
};

/// Assert `err` lapsed waiting for a `topUp` it sent.
fn lapsed(err: &anyhow::Error) {
    assert_eq!(
        err.downcast_ref::<crate::buyer_pool::TopUpUnconfirmed>(),
        Some(&HELD),
        "{err:#}"
    );
    let text = format!("{err:#}");
    assert!(text.contains("no receipt within"), "{err:#}");
    assert!(!text.contains("refusing a second topUp"), "{err:#}");
}

/// Assert `err` refused a `topUp` because the run holds [`HELD`]. The marker
/// alone does not tell a refusal from a fresh lapse, since every scripted send
/// lapses as [`HELD`]; the context does.
fn refused(err: &anyhow::Error) {
    assert_eq!(
        err.downcast_ref::<crate::buyer_pool::TopUpUnconfirmed>(),
        Some(&HELD),
        "{err:#}"
    );
    assert!(
        format!("{err:#}").contains("refusing a second topUp"),
        "{err:#}"
    );
}

/// Once a run's refill `topUp` lapses unconfirmed, neither a sibling lane
/// build's refill nor a recovery step sends another while the first may still
/// mine: each refuses at once with the held [`TopUpUnconfirmed`]. A sibling
/// lane or bundle entry queued on `open_lock`, or a step queued on
/// `RecoveryGate`'s step lock, reaches one of them before the first fault ends
/// the run.
#[tokio::test(start_paused = true)]
async fn a_top_up_after_an_unconfirmed_refill_is_refused() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    a_spent_pool(&store, &signer);
    let funding = RunFunding::default();
    let rpc = ScriptedRpc::parked_receipt().answering_calls([
        still_open(signer.address()),
        Bytes::from(U256::MAX.abi_encode()),
    ]);
    lapsed(&run_until_err(&store, &signer, &rpc, &funding).await);
    assert_eq!(funding.escrows().top_ups(), vec![HELD]);

    // A sibling lane build of the run reuses the noted pool without a read, so
    // any call it makes would take the allowance answer.
    let rpc = ScriptedRpc::parked_receipt().answering_calls([Bytes::from(U256::MAX.abi_encode())]);
    let started = tokio::time::Instant::now();
    refused(&run_until_err(&store, &signer, &rpc, &funding).await);
    assert!(
        started.elapsed() < RECEIPT_TIMEOUT,
        "the refusal waits on no receipt"
    );
    assert!(!rpc.calls_drained(), "the refusal makes no call");

    // A recovery step reads the pool; any further call would take the
    // allowance answer.
    let rpc = ScriptedRpc::parked_receipt().answering_calls([
        still_open(signer.address()),
        Bytes::from(U256::MAX.abi_encode()),
    ]);
    let started = tokio::time::Instant::now();
    refused(&recover_until_err(&store, &signer, &rpc, &funding).await);
    assert!(
        started.elapsed() < RECEIPT_TIMEOUT,
        "the refusal waits on no receipt"
    );
    assert!(
        !rpc.calls_drained(),
        "the refusal makes no call past the pool read"
    );
}

/// A recovery step whose `topUp` lapses holds it the same way: a later refill
/// of the run sends nothing and fails with it.
#[tokio::test(start_paused = true)]
async fn a_refill_after_an_unconfirmed_recovery_step_is_refused() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let pool_id = a_spent_pool(&store, &signer);
    let funding = RunFunding::default();
    funding.note_pool(pool_id);
    let rpc = ScriptedRpc::parked_receipt().answering_calls([
        still_open(signer.address()),
        Bytes::from(U256::MAX.abi_encode()),
    ]);
    lapsed(&recover_until_err(&store, &signer, &rpc, &funding).await);
    assert!(
        rpc.calls_drained(),
        "the step read the pool and the allowance"
    );
    assert_eq!(funding.escrows().top_ups(), vec![HELD]);

    // The pool is noted, so the lane build makes no read before the refill.
    let rpc = ScriptedRpc::parked_receipt().answering_calls([Bytes::from(U256::MAX.abi_encode())]);
    refused(&run_until_err(&store, &signer, &rpc, &funding).await);
    assert!(!rpc.calls_drained(), "the refusal makes no call");
}

/// A refill and a recovery step of one run that both find the pool low at once
/// hold different locks (`open_lock`, the recovery step lock), yet send one
/// `topUp` between them: the second waits on the first, sees it lapse, and
/// refuses. Which of the two sends first is up to the scheduler, so the test
/// asserts on whichever did.
#[tokio::test(start_paused = true)]
async fn a_concurrent_refill_and_recovery_step_send_one_top_up() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let pool_id = a_spent_pool(&store, &signer);
    let funding = RunFunding::default();
    funding.note_pool(pool_id);
    let refill_rpc =
        ScriptedRpc::parked_receipt().answering_calls([Bytes::from(U256::MAX.abi_encode())]);
    let step_rpc = ScriptedRpc::parked_receipt().answering_calls([
        still_open(signer.address()),
        Bytes::from(U256::MAX.abi_encode()),
    ]);
    let (refill, step) = tokio::join!(
        run_until_err(&store, &signer, &refill_rpc, &funding),
        recover_until_err(&store, &signer, &step_rpc, &funding),
    );
    let (sent, waited) = if refill_rpc.calls_drained() {
        (&refill, &step)
    } else {
        (&step, &refill)
    };
    lapsed(sent);
    refused(waited);
    assert!(
        !(refill_rpc.calls_drained() && step_rpc.calls_drained()),
        "only one of the two read the allowance and sent"
    );
}

/// A recovery step dropped while its `topUp` waits for a receipt (a give-up,
/// a Ctrl-C, or a sibling fault ends the command) returns no error, yet the
/// run's handle still names the tx, and a later `topUp` of the run refuses
/// with it (#2491).
#[tokio::test(start_paused = true)]
async fn a_recovery_top_up_dropped_mid_wait_is_held_in_the_runs_escrows() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let pool_id = a_spent_pool(&store, &signer);
    let escrows = UnconfirmedEscrows::default();
    let funding = RunFunding::reporting_escrows_to(escrows.clone());
    funding.note_pool(pool_id);
    let rpc = ScriptedRpc::parked_receipt().answering_calls([
        still_open(signer.address()),
        Bytes::from(U256::MAX.abi_encode()),
    ]);
    let provider = rpc.clone().provider();
    let contract = PaymentPool::new(PP, provider.clone());
    let funder = PoolFunder {
        contract: &contract,
        rpc: &provider,
        store: &store,
        owner: signer.address(),
        deployment: DEPLOYMENT,
        token: TOKEN,
        payment_pool_addr: PP,
        working_deposit: U256::from(WORKING),
        max_approve: false,
        funding: &funding,
    };
    let waited = tokio::time::timeout(RECEIPT_TIMEOUT / 2, funder.recover(U256::ZERO)).await;
    assert!(waited.is_err(), "the step must still wait on its receipt");
    assert!(
        rpc.calls_drained(),
        "the step read the pool and the allowance"
    );
    assert_eq!(escrows.top_ups(), vec![HELD]);

    // The pool is noted, so the lane build makes no read before the refill.
    let rpc = ScriptedRpc::parked_receipt().answering_calls([Bytes::from(U256::MAX.abi_encode())]);
    refused(&run_until_err(&store, &signer, &rpc, &funding).await);
}

/// A `topUp` one `RunFunding` left unconfirmed refuses the `topUp` of a later
/// `RunFunding` built on the same handle: a fetch's next pass, a bundle's
/// replacement pass, or another fetch of the command.
#[tokio::test(start_paused = true)]
async fn a_top_up_left_unconfirmed_by_one_pass_refuses_the_next() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    a_spent_pool(&store, &signer);
    let escrows = UnconfirmedEscrows::default();
    let rpc = ScriptedRpc::parked_receipt().answering_calls([
        still_open(signer.address()),
        Bytes::from(U256::MAX.abi_encode()),
    ]);
    let first = RunFunding::reporting_escrows_to(escrows.clone());
    lapsed(&run_until_err(&store, &signer, &rpc, &first).await);
    drop(first);

    // The next pass reads the pool afresh; any further call would take the
    // allowance answer.
    let rpc = ScriptedRpc::parked_receipt().answering_calls([
        still_open(signer.address()),
        Bytes::from(U256::MAX.abi_encode()),
    ]);
    let next = RunFunding::reporting_escrows_to(escrows.clone());
    refused(&run_until_err(&store, &signer, &rpc, &next).await);
    assert!(!rpc.calls_drained(), "the refusal sends no allowance read");
    assert_eq!(escrows.top_ups(), vec![HELD]);
}

/// A receipt for [`SCRIPTED_TX`](decdn_incentive::test_rpc::SCRIPTED_TX),
/// sent to [`PP`], that mined with `logs`.
fn mined_with(logs: Vec<alloy::rpc::types::Log>) -> alloy::rpc::types::TransactionReceipt {
    decdn_incentive::test_rpc::mined_receipt(PP, logs)
}

/// A `topUp` receipt that mined with no `PoolToppedUp` log, so the top-up
/// credits the amount it asked for.
fn mined_top_up() -> alloy::rpc::types::TransactionReceipt {
    mined_with(Vec::new())
}

/// An `openPool` receipt whose `PoolOpened` log, emitted by [`PP`], opens
/// `pool_id` for `owner` at the working deposit.
fn mined_open(owner: Address, pool_id: PoolId) -> alloy::rpc::types::TransactionReceipt {
    use alloy::sol_types::SolEvent;

    let event = PaymentPool::PoolOpened {
        poolId: pool_id,
        owner,
        deposit: U256::from(WORKING),
    };
    mined_with(vec![alloy::rpc::types::Log {
        inner: alloy::primitives::Log {
            address: PP,
            data: event.encode_log_data(),
        },
        ..Default::default()
    }])
}

/// The store write [`StoreFaults`] fails.
#[derive(Debug, Clone, Copy)]
enum StoreFault {
    /// No write faults.
    None,
    /// Every deposit credit faults.
    Credit,
    /// Every pool record faults.
    Record,
    /// A deposit credit commits, and the credited row then reads as gone.
    RowAfterCredit,
}

/// A redb store with one [`StoreFault`]. Every other call reaches the store.
/// A store [`StoreFaults::watching`] a handle notes, at each pool record and
/// deposit credit, whether the handle names [`SCRIPTED_TX`] as the write runs.
struct StoreFaults {
    inner: RedbBuyerPoolStore,
    fault: StoreFault,
    credited: std::sync::atomic::AtomicBool,
    watch: Option<(UnconfirmedEscrows, Mutex<Vec<bool>>)>,
}

impl StoreFaults {
    fn new(inner: RedbBuyerPoolStore, fault: StoreFault) -> Self {
        Self {
            inner,
            fault,
            credited: std::sync::atomic::AtomicBool::new(false),
            watch: None,
        }
    }

    /// A store with no fault that watches `escrows` at each write.
    fn watching(inner: RedbBuyerPoolStore, escrows: UnconfirmedEscrows) -> Self {
        Self {
            watch: Some((escrows, Mutex::default())),
            ..Self::new(inner, StoreFault::None)
        }
    }

    /// Note whether the watched handle names [`SCRIPTED_TX`] now.
    fn note_write(&self) {
        let Some((escrows, writes)) = &self.watch else {
            return;
        };
        let pending = escrows.pending().iter().any(|escrow| match escrow {
            EscrowUnconfirmed::Open(open) => open.tx == Some(SCRIPTED_TX),
            EscrowUnconfirmed::TopUp(top_up) => top_up.tx == Some(SCRIPTED_TX),
        });
        let untracked = escrows
            .untracked()
            .iter()
            .any(|held| held.tx == SCRIPTED_TX);
        writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(pending || untracked);
    }

    /// For each pool record and deposit credit, whether the watched handle
    /// named [`SCRIPTED_TX`] as the write ran.
    fn named_writes(&self) -> Vec<bool> {
        self.watch.as_ref().map_or_else(Vec::new, |(_, writes)| {
            writes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        })
    }

    /// The fault a failed write returns.
    fn lost(pool_id: PoolId, what: &str) -> decdn_incentive::StoreError {
        decdn_incentive::StoreError::Corrupt {
            pool_id: Some(pool_id),
            detail: format!("{what} lost"),
        }
    }
}

impl BuyerPoolStore for StoreFaults {
    fn load_all(&self) -> Result<decdn_incentive::BuyerLoad, decdn_incentive::StoreError> {
        self.inner.load_all()
    }
    fn record(&self, state: &BuyerPoolState) -> Result<(), decdn_incentive::StoreError> {
        self.note_write();
        match self.fault {
            StoreFault::Record => Err(Self::lost(state.pool_id, "pool record")),
            StoreFault::None | StoreFault::Credit | StoreFault::RowAfterCredit => {
                self.inner.record(state)
            }
        }
    }
    fn forget(&self, owner: Address) -> Result<(), decdn_incentive::StoreError> {
        self.inner.forget(owner)
    }
    fn get_by_pool_id(
        &self,
        pool_id: PoolId,
    ) -> Result<Option<BuyerPoolState>, decdn_incentive::StoreError> {
        if self.credited.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(None);
        }
        self.inner.get_by_pool_id(pool_id)
    }
    fn forget_if_pool(
        &self,
        owner: Address,
        pool_id: PoolId,
    ) -> Result<bool, decdn_incentive::StoreError> {
        self.inner.forget_if_pool(owner, pool_id)
    }
    fn get_by_owner(
        &self,
        owner: Address,
    ) -> Result<Option<BuyerPoolState>, decdn_incentive::StoreError> {
        self.inner.get_by_owner(owner)
    }
    fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, decdn_incentive::StoreError> {
        self.inner
            .advance_progress(owner, pool_id, lane, bytes, amount)
    }
    fn rebase_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        anchor: decdn_incentive::BuyerLaneProgress,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, decdn_incentive::StoreError> {
        self.inner
            .rebase_progress(owner, pool_id, lane, anchor, totals)
    }
    fn seed_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, decdn_incentive::StoreError> {
        self.inner
            .seed_progress(owner, pool_id, lane, bytes, amount)
    }
    fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> Result<decdn_incentive::DepositOutcome, decdn_incentive::StoreError> {
        self.note_write();
        match self.fault {
            StoreFault::Credit => Err(Self::lost(pool_id, "deposit credit")),
            StoreFault::None | StoreFault::Record => {
                self.inner.add_deposit(owner, pool_id, additional)
            }
            StoreFault::RowAfterCredit => {
                let credited = self.inner.add_deposit(owner, pool_id, additional);
                self.credited
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                credited
            }
        }
    }
    fn delegate_lanes(
        &self,
        deployment: Deployment,
        pool_id: PoolId,
        signer: Address,
    ) -> Result<Vec<(Address, decdn_incentive::BuyerLaneProgress)>, decdn_incentive::StoreError>
    {
        self.inner.delegate_lanes(deployment, pool_id, signer)
    }
    fn advance_delegate_progress(
        &self,
        deployment: Deployment,
        lane: LaneKey,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, decdn_incentive::StoreError> {
        self.inner
            .advance_delegate_progress(deployment, lane, totals)
    }
    fn rebase_delegate_progress(
        &self,
        deployment: Deployment,
        lane: LaneKey,
        anchor: decdn_incentive::BuyerLaneProgress,
        totals: decdn_incentive::BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, decdn_incentive::StoreError> {
        self.inner
            .rebase_delegate_progress(deployment, lane, anchor, totals)
    }

    fn lane_salt(
        &self,
    ) -> Result<decdn_incentive::lane_signer::LaneSalt, decdn_incentive::StoreError> {
        self.inner.lane_salt()
    }
}

/// Assert `err` refused an escrow because the run holds an untracked one
/// whose text names `held`.
fn refused_after_untracked(err: &anyhow::Error, held: &str) {
    assert!(
        err.downcast_ref::<RefusedAfterUntracked>().is_some(),
        "{err:#}"
    );
    assert!(
        err.downcast_ref::<EscrowUntracked>().is_none(),
        "the refusal escrowed nothing: {err:#}"
    );
    assert!(
        format!("{err:#}").contains(held),
        "the refusal names the held escrow: {err:#}"
    );
}

/// Assert `err` is an untracked escrow whose text names `cause`.
fn untracked(err: &anyhow::Error, cause: &str) {
    assert!(err.downcast_ref::<EscrowUntracked>().is_some(), "{err:#}");
    assert!(format!("{err:#}").contains(cause), "{err:#}");
}

/// The answers a lane build on [`a_spent_pool`] reads before it sends its
/// refill `topUp`: `getPool` shows the pool open, and the allowance covers
/// the refill.
fn refill_reads(owner: Address) -> [Bytes; 2] {
    [still_open(owner), Bytes::from(U256::MAX.abi_encode())]
}

/// Once a run's refill `topUp` mines and its credit fails, neither a sibling
/// lane build's refill nor a recovery step sends another `topUp` (#2506). The
/// sibling reads a row that does not credit the escrow, so it sees the pool
/// low again. It runs before the acquire loop ends the run on the first
/// fault, or in the drain after it.
#[tokio::test(start_paused = true)]
async fn a_top_up_after_an_untracked_refill_is_refused() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = StoreFaults::new(client_store(&dir), StoreFault::Credit);
    let signer = Arc::new(PrivateKeySigner::random());
    a_spent_pool(&store.inner, &signer);
    let funding = RunFunding::default();
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    untracked(
        &run_until_err(&store, &signer, &rpc, &funding).await,
        "deposit credit lost",
    );
    assert_eq!(
        rpc.sent_nonces().len(),
        1,
        "the first refill sent its topUp"
    );
    assert!(funding.escrows().pending().is_empty(), "the topUp mined");

    // A sibling lane build of the run reuses the noted pool without a read, so
    // the allowance read, or the wallet read after a failed refill, would
    // drain the one answer.
    let rpc =
        ScriptedRpc::mined(mined_top_up()).answering_calls([Bytes::from(U256::MAX.abi_encode())]);
    refused_after_untracked(
        &run_until_err(&store, &signer, &rpc, &funding).await,
        "deposit credit lost",
    );
    assert!(!rpc.calls_drained(), "the refusal makes no call");
    assert!(rpc.sent_nonces().is_empty(), "the refusal sends nothing");

    // A recovery step reads the pool; any further call would take the
    // allowance answer.
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    refused_after_untracked(
        &recover_until_err(&store, &signer, &rpc, &funding).await,
        "deposit credit lost",
    );
    assert!(
        !rpc.calls_drained(),
        "the refusal makes no call past the pool read"
    );
    assert!(rpc.sent_nonces().is_empty(), "the refusal sends nothing");
}

/// Assert `err` refused an `openPool` or `topUp` because its fetch ended.
fn ended_before_send(err: &anyhow::Error) {
    assert!(err.downcast_ref::<FetchEnded>().is_some(), "{err:#}");
}

/// A fired token for a fetch that has ended.
fn fetch_ended() -> CancellationToken {
    let ended = CancellationToken::new();
    ended.cancel();
    ended
}

/// #2513: a lane build that reaches its refill after its fetch ended sends no
/// `topUp`: the lane would never run. It reads no allowance and no wallet
/// balance, and records no shortfall.
#[tokio::test(start_paused = true)]
async fn a_refill_after_the_fetch_ended_sends_no_top_up() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    a_spent_pool(&store, &signer);
    let funding = RunFunding::default();
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    let err = run_scripted(&store, &signer, &rpc, &funding, &fetch_ended())
        .await
        .expect_err("the refill is refused");
    ended_before_send(&err);
    assert!(rpc.sent_nonces().is_empty(), "the refusal sends nothing");
    assert!(!rpc.calls_drained(), "the refusal reads no allowance");
    assert!(funding.shortfall().is_none(), "no wallet read explains it");
    assert!(funding.escrows().pending().is_empty());
}

/// #2513: a lane build that finds no pool after its fetch ended opens none.
#[tokio::test(start_paused = true)]
async fn a_fresh_open_after_the_fetch_ended_sends_nothing() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let funding = RunFunding::default();
    let rpc = ScriptedRpc::mined(mined_open(signer.address(), B256::repeat_byte(0xEE)))
        .answering_calls([Bytes::from(TOKEN.abi_encode())]);
    let err = run_scripted(&store, &signer, &rpc, &funding, &fetch_ended())
        .await
        .expect_err("the open is refused");
    ended_before_send(&err);
    assert!(rpc.sent_nonces().is_empty(), "the refusal sends nothing");
    assert!(!rpc.calls_drained(), "the refusal reads no token");
    assert!(store.get_by_owner(signer.address()).unwrap().is_none());
}

/// #2513: a refill whose fetch ends during its allowance leg sends no `topUp`
/// after it: the check after that leg refuses it, with no wallet read and no
/// shortfall.
#[tokio::test(start_paused = true)]
async fn a_refill_that_ends_during_its_allowance_leg_sends_no_top_up() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    a_spent_pool(&store, &signer);
    let funding = RunFunding::default();
    let ended = CancellationToken::new();
    // The fetch ends as the refill reads its allowance, which is none, so
    // the refill approves first and the `approve` mines.
    let rpc = ScriptedRpc::mined(mined_top_up())
        .answering_calls([
            still_open(signer.address()),
            Bytes::from(U256::ZERO.abi_encode()),
        ])
        .after_calls({
            let ended = ended.clone();
            move || ended.cancel()
        });
    let err = run_scripted(&store, &signer, &rpc, &funding, &ended)
        .await
        .expect_err("the topUp is refused");
    ended_before_send(&err);
    assert!(rpc.calls_drained(), "the refill read its allowance");
    assert!(rpc.sent_nonces().is_empty(), "no topUp was sent");
    assert!(funding.shortfall().is_none(), "no wallet read explains it");
    assert!(funding.escrows().pending().is_empty());
}

/// #2513: a lane build whose fetch ends during its open's allowance leg
/// opens no pool.
#[tokio::test(start_paused = true)]
async fn an_open_that_ends_during_its_allowance_leg_sends_nothing() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let funding = RunFunding::default();
    let ended = CancellationToken::new();
    let rpc = ScriptedRpc::mined(mined_open(signer.address(), B256::repeat_byte(0xEE)))
        .answering_calls([
            Bytes::from(TOKEN.abi_encode()),
            Bytes::from(U256::MAX.abi_encode()),
        ])
        .after_calls({
            let ended = ended.clone();
            move || ended.cancel()
        });
    let err = run_scripted(&store, &signer, &rpc, &funding, &ended)
        .await
        .expect_err("the open is refused");
    ended_before_send(&err);
    assert!(rpc.calls_drained(), "the open read its allowance");
    assert!(rpc.sent_nonces().is_empty(), "no openPool was sent");
    assert!(store.get_by_owner(signer.address()).unwrap().is_none());
}

/// #2513: a refill already past its `topUp` send when the fetch ends runs to
/// its receipt: the end refuses a send, never a receipt read. Here the
/// `topUp` never mines, so the build lapses with it held.
#[tokio::test(start_paused = true)]
async fn a_refill_past_its_send_runs_to_its_receipt_after_the_fetch_ends() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    a_spent_pool(&store, &signer);
    let funding = RunFunding::default();
    let rpc = ScriptedRpc::parked_receipt().answering_calls(refill_reads(signer.address()));
    let ended = CancellationToken::new();
    let end_after_send = async {
        while rpc.sent_nonces().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        ended.cancel();
    };
    let started = tokio::time::Instant::now();
    let (built, ()) = tokio::join!(
        run_scripted(&store, &signer, &rpc, &funding, &ended),
        end_after_send
    );
    let err = built.expect_err("the topUp never mines");
    lapsed(&err);
    assert!(err.downcast_ref::<FetchEnded>().is_none(), "{err:#}");
    assert!(
        started.elapsed() >= RECEIPT_TIMEOUT,
        "it waited on the receipt"
    );
    assert_eq!(funding.escrows().top_ups(), vec![HELD]);
}

/// #2515: a lane build's mined `openPool` stays on the run's handle while its
/// pool record is written, and is released once the record commits. A
/// process exit during the write, such as a second Ctrl-C, still names it.
#[tokio::test(start_paused = true)]
async fn a_lane_build_open_is_named_while_its_record_is_written() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let escrows = UnconfirmedEscrows::default();
    let store = StoreFaults::watching(client_store(&dir), escrows.clone());
    let signer = Arc::new(PrivateKeySigner::random());
    let funding = RunFunding::reporting_escrows_to(escrows.clone());
    let rpc = ScriptedRpc::mined(mined_open(signer.address(), B256::repeat_byte(0xEE)))
        .answering_calls([
            Bytes::from(TOKEN.abi_encode()),
            Bytes::from(U256::MAX.abi_encode()),
        ]);
    run_scripted(&store, &signer, &rpc, &funding, &CancellationToken::new())
        .await
        .expect("the open is recorded");
    assert_eq!(
        store.named_writes(),
        [true],
        "the record write names the open"
    );
    assert!(escrows.pending().is_empty(), "the record released the open");
    assert!(escrows.untracked().is_empty());
}

/// #2515: a refill's mined `topUp` stays on the run's handle while its
/// deposit credit is written.
#[tokio::test(start_paused = true)]
async fn a_refill_is_named_while_its_credit_is_written() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let escrows = UnconfirmedEscrows::default();
    let store = StoreFaults::watching(client_store(&dir), escrows.clone());
    let signer = Arc::new(PrivateKeySigner::random());
    a_spent_pool(&store.inner, &signer);
    let funding = RunFunding::reporting_escrows_to(escrows.clone());
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    run_scripted(&store, &signer, &rpc, &funding, &CancellationToken::new())
        .await
        .expect("the refill is credited");
    assert_eq!(store.named_writes(), [true], "the credit names the topUp");
    assert!(
        escrows.pending().is_empty(),
        "the credit released the topUp"
    );
    assert!(escrows.untracked().is_empty());
}

/// #2515: a recovery step's mined `topUp` stays on the run's handle while its
/// deposit credit is written.
#[tokio::test(start_paused = true)]
async fn a_recovery_top_up_is_named_while_its_credit_is_written() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let escrows = UnconfirmedEscrows::default();
    let store = StoreFaults::watching(client_store(&dir), escrows.clone());
    let signer = Arc::new(PrivateKeySigner::random());
    let pool_id = a_spent_pool(&store.inner, &signer);
    let funding = RunFunding::reporting_escrows_to(escrows.clone());
    funding.note_pool(pool_id);
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    let step = recover_scripted(&store, &signer, &rpc, &funding)
        .await
        .expect("the step tops the pool up");
    assert!(matches!(step, Recovery::ToppedUp(_)), "{step:?}");
    assert_eq!(store.named_writes(), [true], "the credit names the topUp");
    assert!(
        escrows.pending().is_empty(),
        "the credit released the topUp"
    );
    assert!(escrows.untracked().is_empty());
}

/// #2515: a credit that fails holds the escrow untracked and releases its
/// pending entry, so the handle names it once, as untracked.
#[tokio::test(start_paused = true)]
async fn a_failed_credit_moves_the_top_up_from_pending_to_untracked() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = StoreFaults::new(client_store(&dir), StoreFault::Credit);
    let signer = Arc::new(PrivateKeySigner::random());
    a_spent_pool(&store.inner, &signer);
    let funding = RunFunding::default();
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    untracked(
        &run_until_err(&store, &signer, &rpc, &funding).await,
        "deposit credit lost",
    );
    assert!(funding.escrows().pending().is_empty());
    let held: Vec<_> = funding
        .escrows()
        .untracked()
        .into_iter()
        .map(|held| held.tx)
        .collect();
    assert_eq!(held, [SCRIPTED_TX]);
}

/// The handle holds a run's untracked escrow after the run's state is
/// dropped, so the command can name it at its end, and a later pass's
/// `RunFunding` on the same handle refuses its refill (#2511).
#[tokio::test(start_paused = true)]
async fn an_untracked_escrow_outlives_its_run_on_the_handle() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = StoreFaults::new(client_store(&dir), StoreFault::Credit);
    let signer = Arc::new(PrivateKeySigner::random());
    let pool_id = a_spent_pool(&store.inner, &signer);
    let escrows = UnconfirmedEscrows::default();
    let first = RunFunding::reporting_escrows_to(escrows.clone());
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    untracked(
        &run_until_err(&store, &signer, &rpc, &first).await,
        "deposit credit lost",
    );
    drop(first);

    let held = escrows.untracked();
    assert_eq!(held.len(), 1, "the handle holds the escrow: {held:?}");
    let held = held.into_iter().next().unwrap();
    assert_eq!(held.tx, mined_top_up().transaction_hash);
    assert!(held.text.contains("deposit credit lost"), "{held}");
    assert!(escrows.pending().is_empty(), "the topUp mined");

    // The next pass's run on the same handle, which reuses the run's pool.
    let next = RunFunding::reporting_escrows_to(escrows.clone());
    next.note_pool(pool_id);
    let rpc =
        ScriptedRpc::mined(mined_top_up()).answering_calls([Bytes::from(U256::MAX.abi_encode())]);
    refused_after_untracked(
        &run_until_err(&store, &signer, &rpc, &next).await,
        "deposit credit lost",
    );
    assert!(rpc.sent_nonces().is_empty(), "the refusal sends nothing");
}

/// A recovery step whose `topUp` mines and whose credit fails holds the
/// escrow the same way: a later refill of the run sends nothing.
#[tokio::test(start_paused = true)]
async fn a_refill_after_an_untracked_recovery_step_is_refused() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = StoreFaults::new(client_store(&dir), StoreFault::Credit);
    let signer = Arc::new(PrivateKeySigner::random());
    let pool_id = a_spent_pool(&store.inner, &signer);
    let funding = RunFunding::default();
    funding.note_pool(pool_id);
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    untracked(
        &recover_until_err(&store, &signer, &rpc, &funding).await,
        "deposit credit lost",
    );
    assert_eq!(rpc.sent_nonces().len(), 1, "the step sent its topUp");

    // The pool is noted, so the lane build makes no read before the refill.
    let rpc =
        ScriptedRpc::mined(mined_top_up()).answering_calls([Bytes::from(U256::MAX.abi_encode())]);
    refused_after_untracked(
        &run_until_err(&store, &signer, &rpc, &funding).await,
        "deposit credit lost",
    );
    assert!(!rpc.calls_drained(), "the refusal makes no call");
    assert!(rpc.sent_nonces().is_empty(), "the refusal sends nothing");
}

/// A `topUp` that waits for the run's `topUp` lock while an earlier `topUp`
/// resolves refuses once that `topUp` leaves its escrow untracked, though the
/// hold did not exist when it started to wait. The test holds the lock in
/// place of the earlier `topUp`, and notes its escrow before it lets go.
#[tokio::test(start_paused = true)]
async fn a_top_up_parked_behind_an_untracked_one_is_refused() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let pool_id = a_spent_pool(&store, &signer);
    let funding = RunFunding::default();
    funding.note_pool(pool_id);
    let in_flight = funding.top_up_serial.lock().await;
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    let step = recover_until_err(&store, &signer, &rpc, &funding);
    tokio::pin!(step);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut step)
            .await
            .is_err(),
        "the step must wait for the run's topUp lock"
    );
    funding
        .escrows()
        .hold_untracked(&crate::buyer_pool::escrowed_but_untracked(
            "pool 0x01 topped up by 5 µUSDC",
            decdn_incentive::test_rpc::SCRIPTED_TX,
            "deposit credit lost",
        ));
    drop(in_flight);
    refused_after_untracked(&step.await, "deposit credit lost");
    assert!(
        !rpc.calls_drained(),
        "the refusal makes no call past the pool read"
    );
    assert!(rpc.sent_nonces().is_empty(), "the refusal sends nothing");
}

/// #2520: a refill queued on the run's `topUp` lock behind a sibling's
/// receipt wait ends at once when its fetch ends, with the lock still held. It
/// sent nothing, so it does not wait out the drain of late lane builds.
#[tokio::test(start_paused = true)]
async fn a_refill_queued_on_the_top_up_lock_ends_with_its_fetch() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    a_spent_pool(&store, &signer);
    let funding = RunFunding::default();
    let in_flight = funding.top_up_serial.lock().await;
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    let ended = CancellationToken::new();
    let build = run_scripted(&store, &signer, &rpc, &funding, &ended);
    tokio::pin!(build);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut build)
            .await
            .is_err(),
        "the refill must wait for the run's topUp lock"
    );
    ended.cancel();
    let err = tokio::time::timeout(std::time::Duration::from_millis(1), &mut build)
        .await
        .expect("the refill ends at once, with the lock still held")
        .unwrap_err();
    ended_before_send(&err);
    drop(in_flight);
    assert!(rpc.sent_nonces().is_empty(), "the refill sends nothing");
    assert!(funding.escrows().pending().is_empty(), "it holds nothing");
}

/// #2520: the open and `topUp` locks refuse a lane build once its fetch ended,
/// whether the lock is held or free, and are a plain wait for a caller outside
/// any lane build.
#[tokio::test(start_paused = true)]
async fn a_lock_wait_ends_with_the_fetch() {
    let lock = tokio::sync::Mutex::new(());
    let ended = fetch_ended();
    ended_before_send(&lock_unless_ended(&lock, Some(&ended)).await.unwrap_err());
    let held = lock.lock().await;
    ended_before_send(&lock_unless_ended(&lock, Some(&ended)).await.unwrap_err());
    drop(held);
    let live = CancellationToken::new();
    drop(lock_unless_ended(&lock, Some(&live)).await.unwrap());
    drop(lock_unless_ended(&lock, None).await.unwrap());

    // A wait that starts on a live token behind a held lock ends as the token
    // fires, with the lock still held.
    let held = lock.lock().await;
    let wait = lock_unless_ended(&lock, Some(&live));
    tokio::pin!(wait);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut wait)
            .await
            .is_err(),
        "the wait parks on the held lock"
    );
    live.cancel();
    ended_before_send(
        &tokio::time::timeout(std::time::Duration::from_millis(1), &mut wait)
            .await
            .expect("the wait ends as the token fires")
            .unwrap_err(),
    );
    drop(held);
}

/// A credit that lands but whose row then reads as gone leaves the escrow
/// untracked too, and the run holds it.
#[tokio::test(start_paused = true)]
async fn a_credited_row_that_vanishes_holds_the_escrow() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = StoreFaults::new(client_store(&dir), StoreFault::RowAfterCredit);
    let signer = Arc::new(PrivateKeySigner::random());
    a_spent_pool(&store.inner, &signer);
    let funding = RunFunding::default();
    let rpc = ScriptedRpc::mined(mined_top_up()).answering_calls(refill_reads(signer.address()));
    untracked(
        &run_until_err(&store, &signer, &rpc, &funding).await,
        "the credited row vanished",
    );
    assert!(funding.escrows().pending().is_empty(), "held, not pending");
    refused_after_untracked(
        &funding
            .refuse_after_untracked()
            .expect_err("the run holds the escrow"),
        "the credited row vanished",
    );
}

/// A lane build whose `openPool` mines and whose pool record fails holds the
/// escrow: a sibling build, which finds no row and would open a second pool
/// at the full working deposit, refuses before it reads the chain (adoption
/// refused).
#[tokio::test(start_paused = true)]
async fn an_open_after_an_unrecorded_open_is_refused() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = StoreFaults::new(client_store(&dir), StoreFault::Record);
    let signer = Arc::new(PrivateKeySigner::random());
    let funding = RunFunding::default();
    // The open reads the token and the allowance, then sends.
    let rpc = ScriptedRpc::mined(mined_open(signer.address(), B256::repeat_byte(0xEE)))
        .answering_calls([
            Bytes::from(TOKEN.abi_encode()),
            Bytes::from(U256::MAX.abi_encode()),
        ]);
    untracked(
        &run_until_err(&store, &signer, &rpc, &funding).await,
        "pool record lost",
    );
    assert_eq!(
        rpc.sent_nonces().len(),
        1,
        "the first build sent its openPool"
    );
    assert!(funding.escrows().pending().is_empty(), "held, not pending");
    assert!(
        funding.escrows().saw_live(B256::repeat_byte(0xEE)),
        "the mined open names its pool live, though its record failed"
    );

    let rpc = ScriptedRpc::mined(mined_open(signer.address(), B256::repeat_byte(0xEF)))
        .answering_calls([Bytes::from(TOKEN.abi_encode())]);
    refused_after_untracked(
        &run_until_err(&store, &signer, &rpc, &funding).await,
        "pool record lost",
    );
    assert!(!rpc.calls_drained(), "the refusal reads no token");
    assert!(rpc.sent_nonces().is_empty(), "the refusal sends nothing");
}

/// An `openPool` that mines with no `PoolOpened` event for the owner escrowed
/// a deposit no record can name: the build fails as an untracked escrow, which
/// ends the command, and a sibling build opens no second pool.
#[tokio::test(start_paused = true)]
async fn an_open_with_no_pool_opened_event_is_untracked() {
    use decdn_incentive::test_rpc::ScriptedRpc;

    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let signer = Arc::new(PrivateKeySigner::random());
    let funding = RunFunding::default();
    // The receipt's only `PoolOpened` names another owner.
    let rpc = ScriptedRpc::mined(mined_open(
        Address::repeat_byte(0x55),
        B256::repeat_byte(0xEE),
    ))
    .answering_calls([
        Bytes::from(TOKEN.abi_encode()),
        Bytes::from(U256::MAX.abi_encode()),
    ]);
    let err = run_until_err(&store, &signer, &rpc, &funding).await;
    untracked(&err, "no PoolOpened event");
    assert_eq!(
        crate::fault::classify(&err),
        crate::Fault::Fatal(crate::fault::FatalScope::Command),
        "{err:#}"
    );
    assert!(
        store.get_by_owner(signer.address()).unwrap().is_none(),
        "no row names the pool"
    );

    let rpc = ScriptedRpc::mined(mined_open(signer.address(), B256::repeat_byte(0xEF)))
        .answering_calls([Bytes::from(TOKEN.abi_encode())]);
    refused_after_untracked(
        &run_until_err(&store, &signer, &rpc, &funding).await,
        "no PoolOpened event",
    );
    assert!(!rpc.calls_drained(), "the refusal reads no token");
    assert!(rpc.sent_nonces().is_empty(), "the refusal sends nothing");
}

/// A recovery step on a full pool that no longer accepts funds would open a
/// replacement pool. In a run that holds an untracked escrow, it refuses
/// before the replacement's allowance read.
#[tokio::test]
async fn a_replacement_open_after_an_untracked_escrow_is_refused() {
    let selector = alloy::primitives::keccak256("PoolNotOpen()");
    let asserter = Asserter::new();
    asserter.push_success(&full_pool_on_chain(0));
    asserter.push_failure(
        serde_json::from_value::<alloy_json_rpc::ErrorPayload>(serde_json::json!({
            "code": 3,
            "message": "execution reverted",
            "data": format!("0x{}", alloy::primitives::hex::encode(&selector[..4])),
        }))
        .unwrap(),
    );
    let held = crate::buyer_pool::escrowed_but_untracked(
        "pool 0x01 topped up by 5 µUSDC",
        decdn_incentive::test_rpc::SCRIPTED_TX,
        "deposit credit lost",
    );
    let (stepped, opens) = step_on_a_full_pool_holding(asserter.clone(), Some(held)).await;
    refused_after_untracked(
        &stepped.expect_err("the replacement open refuses"),
        "deposit credit lost",
    );
    assert!(
        asserter.read_q().is_empty(),
        "the pool read and the estimate ran, and nothing after them"
    );
    assert!(opens.is_empty(), "the refusal sends no openPool");
}
