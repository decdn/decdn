use super::*;
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;
use alloy::sol_types::SolValue;

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
    let rpc = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let contract = PaymentPool::new(PP, rpc.clone());
    let result = open_or_reuse_pool(
        store,
        &contract,
        &rpc,
        funding,
        signer,
        PROVIDER,
        signer.address(),
        DEPLOYMENT,
        U256::from(WORKING),
        false,
        adoption,
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

    let ctx = run(
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
    )
    .await
    .expect("a live, solvent pool is adopted — nothing is opened");

    assert_eq!(ctx.pool_id, older);
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
        vec![Some(lane(500, 500_000))],
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
    let rpc = ProviderBuilder::new().connect_mocked_client(Asserter::new());
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

/// A buy from a node's data dir never adopts, even though the chain would
/// hand it a pool: that pool is the daemon's, and adopting it would put a
/// second voucher series on the daemon's own lanes.
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
/// `amounts` entry: the first is [`PROVIDER`]'s, each later one another
/// provider's. A lane's bytes are its amount times 1000.
fn tracked_row(id: B256, owner: Address, deposit: u64, amounts: &[u64]) -> BuyerPoolState {
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
                lane_key(id, owner, provider),
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
    let row = tracked_row(id, owner, WORKING, &LIVE_LANES);
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
    let mut row = tracked_row(id, owner, WORKING, &[1_000]);
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
        .record(&tracked_row(id, signer.address(), WORKING, &[2_000_000; 5]))
        .unwrap();

    let (result, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        refill_fails_with_wallet(0),
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
    let other = lane_key(id, signer.address(), Address::repeat_byte(0x55));
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

    let mut calls = vec![Some(lane(1_000_000, 1_000_000_000))];
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
        .record(&tracked_row(id, signer.address(), WORKING, &LIVE_LANES))
        .unwrap();
    let funding = RunFunding::default();

    let (result, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        refill_fails_with_wallet(0),
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
        .record(&tracked_row(id, signer.address(), WORKING, &LIVE_LANES))
        .unwrap();
    let funding = RunFunding::default();
    let (first, _) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        refill_fails_with_wallet(0),
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
        .record(&tracked_row(id, signer.address(), WORKING, &LIVE_LANES))
        .unwrap();
    let funding = RunFunding::default();

    let (result, drained) = run_in(
        &store,
        &signer,
        ChainAdoption::Allowed,
        refill_fails_with_wallet(WORKING),
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
    let a = lane_key(id, owner, PROVIDER);
    let outcome = store
        .advance_progress(
            owner,
            id,
            a,
            U256::from(1_500_000_000u64),
            U256::from(1_500_000u64),
        )
        .unwrap();
    assert!(matches!(outcome, AdvanceOutcome::Advanced));
    assert_eq!(
        store.get_by_owner(owner).unwrap().unwrap().pool_spend(),
        U256::from(3_500_000u64)
    );
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
        .record(&tracked_row(id, signer.address(), 2 * WORKING, &LIVE_LANES))
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
    let signer = Arc::new(PrivateKeySigner::random());
    let dir = tempfile::tempdir().unwrap();
    let store = client_store(&dir);
    let rpc = ProviderBuilder::new().connect_mocked_client(Asserter::new());
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
    let pool_id = std::sync::OnceLock::new();
    pool_id.set(B256::repeat_byte(0xEE)).unwrap();
    let funder = CliFunder {
        contract: &contract,
        rpc: &rpc,
        store: &store,
        owner: Address::repeat_byte(0x01),
        signer: &signer,
        deployment: DEPLOYMENT,
        pool_id: &pool_id,
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

    let delegated = CliFunder {
        working_deposit: U256::ZERO,
        ..funder
    };
    assert!(matches!(
        delegated.recover(U256::ZERO).await,
        Ok(Recovery::Unavailable)
    ));
}
