use decdn_incentive::store::StoreError;
use decdn_incentive::{BuyerLaneProgress, BuyerPoolState, LaneKey, MemoryBuyerPoolStore};

use super::*;

/// The deployment the mocked contract ([`mocked_pool_contract`], at
/// `Address::ZERO`) belongs to, and the one every service here buys on.
const DEPLOYMENT: Deployment = Deployment {
    chain_id: 421_614,
    payment_pool: Address::ZERO,
};

/// The same `PaymentPool` address as [`DEPLOYMENT`] on another chain: a
/// different deployment that the contract address alone cannot tell apart.
const OTHER_CHAIN: Deployment = Deployment {
    chain_id: 1,
    payment_pool: Address::ZERO,
};

/// Another `PaymentPool` address on [`DEPLOYMENT`]'s chain.
const OTHER_CONTRACT: Deployment = Deployment {
    chain_id: 421_614,
    payment_pool: Address::repeat_byte(0xDE),
};

fn signer() -> Arc<PrivateKeySigner> {
    Arc::new(PrivateKeySigner::random())
}

/// A fresh metrics handle for a test that only needs somewhere to count.
fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new())
}

/// [`reconcile_with_chain`] as bootstrap runs it.
async fn bootstrap_reconcile<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    deployment: Deployment,
    store: &Arc<dyn BuyerPoolStore>,
    owner: Address,
    token: Address,
    metrics: &Arc<Metrics>,
) -> Reconciled {
    reconcile_with_chain(
        contract,
        deployment,
        store,
        owner,
        token,
        ReconcileRun::Bootstrap,
        metrics,
    )
    .await
}

/// An on-chain pool row in the state `getPool` returns it in.
fn onchain_pool(owner: Address, status: PaymentPool::Status, deposit: u64) -> PaymentPool::Pool {
    PaymentPool::Pool {
        owner,
        status,
        disputeDeadline: 0,
        deposit,
        totalRedeemed: 0,
    }
}

/// A `PaymentPool` bound to a mocked transport whose `eth_call` queue is
/// `responses`, in order. The first entry answers `getPools`, and each
/// subsequent one answers the `getPool` the adoption walk makes.
fn mocked_pool_contract(
    responses: Vec<alloy::primitives::Bytes>,
) -> PaymentPool::PaymentPoolInstance<impl Provider + Clone + 'static> {
    mocked_pool_contract_with(responses.into_iter().map(MockCall::Ok).collect()).0
}

/// One queued `eth_call` answer.
enum MockCall {
    /// Decode this payload.
    Ok(alloy::primitives::Bytes),
    /// Fault the call, as a transient RPC error does.
    Err,
}

/// [`mocked_pool_contract`] with per-call failure injection, returning the
/// [`Asserter`] so a test can assert the queue was fully consumed.
///
/// `Asserter` only faults on OVER-calls — unspent responses are dropped
/// silently — so "the code reached this call" is only provable by checking
/// the queue is empty afterwards.
fn mocked_pool_contract_with(
    calls: Vec<MockCall>,
) -> (
    PaymentPool::PaymentPoolInstance<impl Provider + Clone + 'static>,
    alloy::providers::mock::Asserter,
) {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;

    let asserter = Asserter::new();
    for call in calls {
        match call {
            MockCall::Ok(response) => asserter.push_success(&response),
            MockCall::Err => asserter.push_failure_msg("transient rpc fault"),
        }
    }
    (
        PaymentPool::new(
            Address::ZERO,
            ProviderBuilder::new().connect_mocked_client(asserter.clone()),
        ),
        asserter,
    )
}

/// Collects a `fmt` subscriber's output so a test can read the logged text.
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A provider error can carry the RPC URL, and the URL can carry the
/// provider's API key. The chain-read warning names the failure without the
/// key (#2264).
#[tokio::test]
async fn a_chain_read_warning_does_not_log_the_rpc_url() -> anyhow::Result<()> {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;

    let asserter = Asserter::new();
    asserter.push_failure_msg(
        "error sending request for url (https://rpc.example/v3/SECRETKEY): timed out",
    );
    let contract = PaymentPool::new(
        Address::ZERO,
        ProviderBuilder::new().connect_mocked_client(asserter),
    );

    let log = CapturedLog::default();
    let sink = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    let walked = {
        let _guard = tracing::subscriber::set_default(subscriber);
        OwnedPools::walk(
            &contract,
            vec![PoolId::from([0xAA; 32])],
            ReconcileRun::Bootstrap,
        )
        .await
    };
    assert_eq!(walked.unreadable.len(), 1);

    let text = String::from_utf8(
        log.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )?;
    assert!(
        text.contains("could not read an owned pool's state"),
        "the warning is logged: {text}"
    );
    assert!(!text.contains("SECRETKEY"), "the key is redacted: {text}");
    assert!(!text.contains("rpc.example"), "the URL is redacted: {text}");
    Ok(())
}

/// A node whose store was reset adopts the pool it already owns rather than
/// escrowing a second deposit beside it (#2072).
#[tokio::test]
async fn reconcile_adopts_the_newest_open_pool() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let token = Address::repeat_byte(2);
    let older = PoolId::from([0xAA; 32]);
    let newer = PoolId::from([0xBB; 32]);

    // `getPools` is oldest-first; the walk reads the newer one first.
    let contract = mocked_pool_contract(vec![
        vec![older, newer].abi_encode().into(),
        onchain_pool(owner, PaymentPool::Status::Open, 10_000_000)
            .abi_encode()
            .into(),
    ]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());

    assert_eq!(
        bootstrap_reconcile(&contract, DEPLOYMENT, &store, owner, token, &metrics()).await,
        Reconciled::Adopted(newer)
    );
    let adopted = store.get_by_owner(owner).unwrap().expect("row recorded");
    assert_eq!(adopted.pool_id, newer);
    assert_eq!(adopted.deposit, U256::from(10_000_000u64));
    assert_eq!(adopted.token, token);
}

/// #2292: an adopted pool other lanes drained keeps its `totalRedeemed` in
/// the row, and the node's low-water refill reads it. A row that counted
/// only its own (no) lanes would read the pool as full and never refill,
/// and providers would refuse the node's pulls on its spent deposit.
#[tokio::test]
async fn an_adopted_drained_pool_keeps_its_redeemed_spend_and_refills() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let token = Address::repeat_byte(2);
    let id = PoolId::from([0xCC; 32]);
    let mut drained = onchain_pool(owner, PaymentPool::Status::Open, 10_000_000);
    drained.totalRedeemed = 9_500_000;
    let contract = mocked_pool_contract(vec![
        vec![id].abi_encode().into(),
        drained.abi_encode().into(),
    ]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    assert!(matches!(
        bootstrap_reconcile(&contract, DEPLOYMENT, &store, owner, token, &metrics()).await,
        Reconciled::Adopted(_)
    ));
    let row = store.get_by_owner(owner).unwrap().expect("row recorded");
    assert_eq!(row.redeemed_elsewhere(), U256::from(9_500_000u64));
    assert_eq!(row.pool_spend(), U256::from(9_500_000u64));

    let service = mocked_service(vec![], Arc::clone(&store), signer(), owner);
    service.spawn_refill_if_low(&row);
    let requested = service
        .topup_in_flight
        .lock()
        .unwrap()
        .as_ref()
        .map(|t| t.requested);
    assert!(
        requested.is_some_and(|r| !r.is_zero()),
        "0.5 USDC left of 10 is below the low water: the refill fires"
    );

    let full = BuyerPoolState::adopt(
        id,
        DEPLOYMENT,
        owner,
        token,
        U256::from(10_000_000u64),
        U256::ZERO,
    );
    let control = mocked_service(vec![], Arc::clone(&store), signer(), owner);
    control.spawn_refill_if_low(&full);
    assert!(control.topup_in_flight.lock().unwrap().is_none());
}

/// A row written against a DIFFERENT `PaymentPool` deployment is dropped,
/// not reused — even though its `pool_id` is one this contract can also
/// mint.
///
/// This is the redeploy wedge. `poolId` is
/// `keccak256(owner, ownerPoolNonce)` with no contract address in it, and
/// a fresh deployment restarts the nonce at zero, so the tracked id of the
/// owner's Nth pool on the old contract is byte-identical to its Nth pool
/// here. Keeping the row would first pin every pull to a pool this
/// contract has never heard of, and then — once the nonce walks back over
/// that id — silently resume against a REAL and unrelated pool, carrying
/// lane progress that priced bytes it never delivered.
#[tokio::test]
async fn reconcile_drops_a_row_from_another_payment_pool_deployment() {
    reconcile_drops_a_row_from(OTHER_CONTRACT).await;
}

/// The same drop for a row on the configured contract address but another
/// chain. The address alone does not name a deployment: the same deployer
/// nonce yields the same `PaymentPool` address on every chain, and the pool
/// ids repeat there too.
#[tokio::test]
async fn reconcile_drops_a_row_from_the_same_address_on_another_chain() {
    reconcile_drops_a_row_from(OTHER_CHAIN).await;
}

/// Seed a row on `foreign`, reconcile against [`DEPLOYMENT`] whose contract
/// lists the same pool id, and check the row adopted in its place.
async fn reconcile_drops_a_row_from(foreign: Deployment) {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let token = Address::repeat_byte(2);
    // The id the stale row tracks — and the id this contract will hand out
    // again, because the derivation omits the chain and the contract.
    let colliding = PoolId::from([0xAA; 32]);

    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            colliding,
            foreign,
            owner,
            token,
            U256::from(10_000_000u64),
        ))
        .expect("seed the foreign row");

    // The foreign row is dropped before the walk, so adoption applies and
    // the enumeration runs: this contract really does list `colliding`.
    let contract = mocked_pool_contract(vec![
        vec![colliding].abi_encode().into(),
        onchain_pool(owner, PaymentPool::Status::Open, 4_000_000)
            .abi_encode()
            .into(),
    ]);

    assert!(matches!(
        bootstrap_reconcile(&contract, DEPLOYMENT, &store, owner, token, &metrics()).await,
        Reconciled::Adopted(_)
    ));

    let row = store.get_by_owner(owner).unwrap().expect("row recorded");
    assert_eq!(
        row.deployment, DEPLOYMENT,
        "the surviving row must belong to the deployment this node is configured against"
    );
    assert_eq!(
        row.deposit,
        U256::from(4_000_000u64),
        "the deposit must come from THIS contract's pool, not the stale row's 10_000_000 — \
         equality here would mean the foreign row was reused under a colliding id"
    );
}

/// The drop stands on its own, with nothing to adopt behind it.
///
/// This is the case that makes `forget_if_pool` load-bearing. When the live
/// contract lists no adoptable pool, `reconcile_with_chain` returns before
/// it records anything, so the foreign row survives unless the drop removed
/// it — and `reuse_or_report` would then hand it to the next pull. In the
/// sibling test above the enumeration happens to return the same colliding
/// id, so `record` rewrites that key either way and a no-op drop passes
/// unnoticed; here it cannot.
#[tokio::test]
async fn a_foreign_row_is_dropped_even_when_there_is_nothing_to_adopt() {
    a_foreign_row_is_dropped_with_nothing_to_adopt(OTHER_CONTRACT).await;
}

/// The same standalone drop for a row on the configured contract address
/// but another chain.
#[tokio::test]
async fn a_row_from_another_chain_is_dropped_even_when_there_is_nothing_to_adopt() {
    a_foreign_row_is_dropped_with_nothing_to_adopt(OTHER_CHAIN).await;
}

/// Seed a row with lane progress on `deployment`, reconcile against
/// [`DEPLOYMENT`] whose contract lists no pool, and check the row is gone.
async fn a_foreign_row_is_dropped_with_nothing_to_adopt(deployment: Deployment) {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let foreign = PoolId::from([0xAA; 32]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let mut row = BuyerPoolState::new(
        foreign,
        deployment,
        owner,
        Address::repeat_byte(2),
        U256::from(10_000_000u64),
    );
    // Lane progress is the hazard the drop exists to destroy: resumed here it
    // would seed the first voucher at a cumulative this contract has never
    // redeemed against.
    let lane = LaneKey {
        pool_id: foreign,
        signer: owner,
        provider: Address::repeat_byte(7),
    };
    let _ = row.advance_lane(lane, U256::from(4096u64), U256::from(41u64));
    store.record(&row).expect("seed the foreign row");

    // This contract knows no pool for this owner, so nothing is adoptable.
    let contract = mocked_pool_contract(vec![Vec::<PoolId>::new().abi_encode().into()]);

    assert_eq!(
        bootstrap_reconcile(
            &contract,
            DEPLOYMENT,
            &store,
            owner,
            Address::repeat_byte(2),
            &metrics()
        )
        .await,
        Reconciled::NoneHeld,
        "nothing to adopt, so the reconcile reports no adoption"
    );
    assert!(
        store.get_by_owner(owner).unwrap().is_none(),
        "the foreign row must be gone even though no replacement was adopted — otherwise \
         the next pull reuses it and pays against a contract that never saw its lanes"
    );
}

/// The foreign-row check keys on the whole deployment — chain id and
/// contract address: a row on the configured deployment is left to the
/// ordinary reconciliation, whatever its id.
#[test]
fn adoption_tracks_a_row_on_the_configured_deployment() {
    let owner = Address::repeat_byte(1);
    let pool_id = PoolId::from([0xAA; 32]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            pool_id,
            DEPLOYMENT,
            owner,
            Address::repeat_byte(2),
            U256::from(10_000_000u64),
        ))
        .expect("seed the local row");

    assert_eq!(
        adoption_applies(&store, owner, DEPLOYMENT, ReconcileRun::Bootstrap),
        AdoptionCheck::AlreadyTracked(pool_id),
        "a row on the configured PaymentPool is tracked, never foreign"
    );
}

/// A row on the configured contract address but another chain is foreign,
/// and the check carries the chain it came from: that deployment is the
/// only one its deposit can be reclaimed against.
#[test]
fn adoption_treats_a_row_on_the_same_address_on_another_chain_as_foreign() {
    let owner = Address::repeat_byte(1);
    let pool_id = PoolId::from([0xAA; 32]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            pool_id,
            OTHER_CHAIN,
            owner,
            Address::repeat_byte(2),
            U256::from(10_000_000u64),
        ))
        .expect("seed the local row");

    assert_eq!(
        adoption_applies(&store, owner, DEPLOYMENT, ReconcileRun::Bootstrap),
        AdoptionCheck::Foreign {
            pool_id,
            was_on: OTHER_CHAIN,
        },
        "the same PaymentPool address on another chain is another deployment"
    );
}

/// A pool the owner closed is not a pool to resume on, so the walk keeps
/// going and settles on the live one behind it.
#[tokio::test]
async fn reconcile_skips_a_closing_pool_for_the_open_one_behind_it() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let open = PoolId::from([0xAA; 32]);
    let closing = PoolId::from([0xBB; 32]);

    let contract = mocked_pool_contract(vec![
        vec![open, closing].abi_encode().into(),
        onchain_pool(owner, PaymentPool::Status::Closing, 10_000_000)
            .abi_encode()
            .into(),
        onchain_pool(owner, PaymentPool::Status::Open, 9_000_000)
            .abi_encode()
            .into(),
    ]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());

    assert_eq!(
        bootstrap_reconcile(
            &contract,
            DEPLOYMENT,
            &store,
            owner,
            Address::repeat_byte(2),
            &metrics()
        )
        .await,
        Reconciled::Adopted(open)
    );
    assert_eq!(store.get_by_owner(owner).unwrap().unwrap().pool_id, open);
}

/// An owner with no pools on chain has nothing to adopt, and the first miss
/// opens one the ordinary way.
#[tokio::test]
async fn reconcile_adopts_nothing_when_the_owner_holds_no_pool() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let contract = mocked_pool_contract(vec![Vec::<PoolId>::new().abi_encode().into()]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());

    assert_eq!(
        bootstrap_reconcile(
            &contract,
            DEPLOYMENT,
            &store,
            owner,
            Address::repeat_byte(2),
            &metrics()
        )
        .await,
        Reconciled::NoneHeld
    );
    assert!(store.get_by_owner(owner).unwrap().is_none());
}

/// A store that already tracks a pool it still owns is left alone, while
/// the boot-time sweep for stranded deposits runs around it.
///
/// The chain is stocked with a DIFFERENT adoptable pool, so a build that
/// dropped the already-tracked check would adopt it and fail the assertion.
/// The queue holds exactly the `getPools` + `getPool` pair that sweep
/// consumes; an empty one would not prove the same thing, because the
/// mocked transport errors an unexpected call and the error path also
/// returns `false`.
#[tokio::test]
async fn reconcile_is_a_no_op_when_the_store_already_tracks_a_pool() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let token = Address::repeat_byte(2);
    let existing = PoolId::from([9u8; 32]);
    let other = PoolId::from([0xCC; 32]);

    // Both are open on chain. `existing` must be listed: a tracked pool the
    // chain does NOT list as open is stale, and reconciliation replaces it
    // (see `a_tracked_pool_the_chain_has_closed_is_dropped_and_replaced`).
    // The walk is newest-first, so `other` is read before `existing` — and
    // `other` is what a build that dropped the already-tracked check would
    // adopt, which is what the row assertion below catches.
    let contract = mocked_pool_contract(vec![
        vec![existing, other].abi_encode().into(),
        onchain_pool(owner, PaymentPool::Status::Open, 10_000_000)
            .abi_encode()
            .into(),
        onchain_pool(owner, PaymentPool::Status::Open, 5_000)
            .abi_encode()
            .into(),
    ]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            existing,
            DEPLOYMENT,
            owner,
            token,
            U256::from(5_000u64),
        ))
        .unwrap();

    assert_eq!(
        bootstrap_reconcile(&contract, DEPLOYMENT, &store, owner, token, &metrics()).await,
        Reconciled::KeptTracked
    );
    assert_eq!(
        store.get_by_owner(owner).unwrap().unwrap().pool_id,
        existing
    );
}

/// The stranded set on the already-tracked path is measured against the
/// pool the STORE names, not the one an adoption would have picked (#2078).
///
/// The chain holds two open pools, neither of them the tracked one. A
/// build that reused the adoption selector would call the newer of the two
/// "in use" and report only the older; both are stranded.
#[test]
fn stranded_set_excludes_only_the_pool_actually_in_use() {
    let owner = Address::repeat_byte(1);
    let in_use = PoolId::from([9u8; 32]);
    let other_a = PoolId::from([0xAA; 32]);
    let other_b = PoolId::from([0xBB; 32]);
    let pools = OwnedPools {
        read: vec![
            (
                other_b,
                onchain_pool(owner, PaymentPool::Status::Open, 8_000),
            ),
            (
                in_use,
                onchain_pool(owner, PaymentPool::Status::Open, 5_000),
            ),
            (
                other_a,
                onchain_pool(owner, PaymentPool::Status::Open, 9_000),
            ),
        ],
        unreadable: Vec::new(),
    };
    assert_eq!(pools.recoverable_beside(in_use), vec![other_b, other_a]);
}

/// A fully-redeemed pool refunds nothing, so it is not reported as a
/// recoverable deposit — the warning promises recoverability, and chasing
/// a zero residual is noise on every boot.
#[test]
fn stranded_set_skips_a_fully_redeemed_pool() {
    let owner = Address::repeat_byte(1);
    let in_use = PoolId::from([9u8; 32]);
    let spent = PoolId::from([0xAA; 32]);
    let mut pool = onchain_pool(owner, PaymentPool::Status::Open, 9_000);
    pool.totalRedeemed = pool.deposit;
    let pools = OwnedPools {
        read: vec![(spent, pool)],
        unreadable: Vec::new(),
    };
    assert!(pools.recoverable_beside(in_use).is_empty());
}

/// The already-tracked path enumerates and reaches the stranded sweep
/// rather than returning early (#2078). Proven by mock consumption: the
/// queue holds exactly `getPools` + three `getPool`s, and the transport
/// errors on an unexpected call, so a build that returned early would
/// leave responses unspent and a build that over-called would fault.
#[tokio::test]
async fn reconcile_sweeps_for_stranded_pools_when_the_store_is_intact() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let token = Address::repeat_byte(2);
    let existing = PoolId::from([9u8; 32]);
    let stranded_a = PoolId::from([0xAA; 32]);
    let stranded_b = PoolId::from([0xBB; 32]);

    let (contract, asserter) = mocked_pool_contract_with(vec![
        MockCall::Ok(vec![existing, stranded_a, stranded_b].abi_encode().into()),
        // Walked newest-first: `stranded_b`, `stranded_a`, then `existing`.
        // `existing` is listed open because that is the steady state this
        // test describes — the tracked pool is live and the other two are
        // deposits an earlier build left behind.
        MockCall::Ok(
            onchain_pool(owner, PaymentPool::Status::Open, 8_000)
                .abi_encode()
                .into(),
        ),
        MockCall::Ok(
            onchain_pool(owner, PaymentPool::Status::Open, 9_000)
                .abi_encode()
                .into(),
        ),
        MockCall::Ok(
            onchain_pool(owner, PaymentPool::Status::Open, 5_000)
                .abi_encode()
                .into(),
        ),
    ]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            existing,
            DEPLOYMENT,
            owner,
            token,
            U256::from(5_000u64),
        ))
        .unwrap();

    let metrics = metrics();
    assert_eq!(
        bootstrap_reconcile(&contract, DEPLOYMENT, &store, owner, token, &metrics).await,
        Reconciled::KeptTracked
    );
    // Nothing adopted: the tracked row is untouched.
    assert_eq!(
        store.get_by_owner(owner).unwrap().unwrap().pool_id,
        existing
    );
    // The discriminating assertion. `Asserter` faults only on over-calls,
    // so an unspent queue is the only evidence the sweep ran at all — a
    // build that returned early on `AlreadyTracked` would satisfy both
    // assertions above and leave three responses behind.
    assert_eq!(
        asserter.read_q().len(),
        0,
        "the sweep must read every pool this owner holds"
    );
}

/// A row pointing at a pool the chain no longer lists as open is dropped,
/// and another open pool is adopted in its place (#2078).
///
/// This is the state the node-host `decdn pool close --pool` path leaves
/// behind: it cannot write the daemon's store, so the row survives the
/// close. Dropping it is what stops `adoption_applies` answering
/// `AlreadyTracked` forever and `reuse_or_report` pinning every pull to a
/// pool whose vouchers stop redeeming at its dispute deadline.
#[tokio::test]
async fn a_tracked_pool_the_chain_has_closed_is_dropped_and_replaced() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let token = Address::repeat_byte(2);
    let closed = PoolId::from([9u8; 32]);
    let live = PoolId::from([0xAA; 32]);

    // `getPools` is append-only — it derives ids from `ownerPoolNonce` and
    // `closePool`/`reclaim` only change a pool's status — so the closed
    // pool is still listed, with `Status::Closed`. That listing, not its
    // absence, is what makes the row droppable. Walked newest-first, so
    // `live` is read before `closed`.
    let contract = mocked_pool_contract(vec![
        vec![closed, live].abi_encode().into(),
        onchain_pool(owner, PaymentPool::Status::Open, 9_000)
            .abi_encode()
            .into(),
        onchain_pool(owner, PaymentPool::Status::Closed, 5_000)
            .abi_encode()
            .into(),
    ]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            closed,
            DEPLOYMENT,
            owner,
            token,
            U256::from(5_000u64),
        ))
        .unwrap();

    assert_eq!(
        bootstrap_reconcile(&contract, DEPLOYMENT, &store, owner, token, &metrics()).await,
        Reconciled::Adopted(live),
        "a stale row must not block adoption of a pool this owner really holds"
    );
    assert_eq!(
        store.get_by_owner(owner).unwrap().unwrap().pool_id,
        live,
        "the stale row must be replaced by the live pool, not kept beside it"
    );
}

/// A tracked pool whose status read FAULTED keeps its row (#2078).
///
/// This is the dangerous direction of the stale-row drop, and the reason
/// `OwnedPools` keeps "unreadable" apart from "not open". `getPool` has no
/// retry, so one rate-limited or timed-out call at bootstrap is enough. If
/// absence from the open set were read as closure, that single fault would
/// delete the node's only record of a funded pool and the next miss would
/// escrow a second deposit — the #2072 failure, self-inflicted, and
/// invisible because the stranded report cannot name a pool it failed to
/// read either.
#[tokio::test]
async fn a_tracked_pool_whose_status_read_failed_keeps_its_row() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let token = Address::repeat_byte(2);
    let tracked = PoolId::from([9u8; 32]);
    let other = PoolId::from([0xAA; 32]);

    // Walked newest-first, so `tracked` is read first — and faults.
    let (contract, asserter) = mocked_pool_contract_with(vec![
        MockCall::Ok(vec![other, tracked].abi_encode().into()),
        MockCall::Err,
        MockCall::Ok(
            onchain_pool(owner, PaymentPool::Status::Open, 9_000)
                .abi_encode()
                .into(),
        ),
    ]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            tracked,
            DEPLOYMENT,
            owner,
            token,
            U256::from(5_000u64),
        ))
        .unwrap();

    assert_eq!(
        bootstrap_reconcile(&contract, DEPLOYMENT, &store, owner, token, &metrics()).await,
        Reconciled::Unknown
    );
    assert_eq!(
        store.get_by_owner(owner).unwrap().unwrap().pool_id,
        tracked,
        "a getPool fault must never cost the node its tracked pool"
    );
    assert_eq!(
        asserter.read_q().len(),
        0,
        "the walk must reach every queued call"
    );
}

/// A tracked id `getPools` does not list keeps its row too.
///
/// `getPools` derives ids from `ownerPoolNonce` and `closePool`/`reclaim`
/// only change a pool's status, so it is append-only and no on-chain event
/// produces this state. An id missing from it is an anomaly — a wrong
/// contract address, a re-orged chain — and anomalies must not delete
/// records of escrowed funds.
#[tokio::test]
async fn a_tracked_pool_the_chain_does_not_list_keeps_its_row() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let token = Address::repeat_byte(2);
    let tracked = PoolId::from([9u8; 32]);
    let other = PoolId::from([0xAA; 32]);

    let contract = mocked_pool_contract(vec![
        vec![other].abi_encode().into(),
        onchain_pool(owner, PaymentPool::Status::Open, 9_000)
            .abi_encode()
            .into(),
    ]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            tracked,
            DEPLOYMENT,
            owner,
            token,
            U256::from(5_000u64),
        ))
        .unwrap();

    assert_eq!(
        bootstrap_reconcile(&contract, DEPLOYMENT, &store, owner, token, &metrics()).await,
        Reconciled::Unknown
    );
    assert_eq!(
        store.get_by_owner(owner).unwrap().unwrap().pool_id,
        tracked,
        "an id the chain does not list is unknown, not closed"
    );
}

/// The same stale row with nothing to replace it is still dropped, so the    /// The same stale row with nothing to replace it is still dropped, so the
/// next miss opens a fresh pool rather than reusing the closed one.
#[tokio::test]
async fn a_stale_row_is_dropped_even_when_no_other_pool_is_open() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let token = Address::repeat_byte(2);
    let closed = PoolId::from([9u8; 32]);

    // `getPools` still lists it, but its status is `Closed`, so it is not
    // in the open set.
    let contract = mocked_pool_contract(vec![
        vec![closed].abi_encode().into(),
        onchain_pool(owner, PaymentPool::Status::Closed, 5_000)
            .abi_encode()
            .into(),
    ]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            closed,
            DEPLOYMENT,
            owner,
            token,
            U256::from(5_000u64),
        ))
        .unwrap();

    assert_eq!(
        bootstrap_reconcile(&contract, DEPLOYMENT, &store, owner, token, &metrics()).await,
        Reconciled::NoneHeld
    );
    assert!(
        store.get_by_owner(owner).unwrap().is_none(),
        "the row must be gone so the next miss opens a fresh pool"
    );
}

/// A failed enumeration on the already-tracked path is a warning, not an
/// adoption failure. The counter's meaning — and the runbook's reading of
/// it — is "about to open a second pool", which this path never is.
#[tokio::test]
async fn a_failed_stranded_sweep_is_not_counted_as_an_adoption_failure() {
    let owner = Address::repeat_byte(1);
    let token = Address::repeat_byte(2);
    let existing = PoolId::from([9u8; 32]);

    // Empty queue: the mocked transport errors the `getPools` call.
    let contract = mocked_pool_contract(vec![]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            existing,
            DEPLOYMENT,
            owner,
            token,
            U256::from(5_000u64),
        ))
        .unwrap();

    let metrics = metrics();
    assert_eq!(
        bootstrap_reconcile(&contract, DEPLOYMENT, &store, owner, token, &metrics).await,
        Reconciled::Unknown
    );
    assert_eq!(
        adoption_failures(&metrics),
        0,
        "a stranded sweep that could not run is not an adoption failure"
    );
    assert_eq!(
        store.get_by_owner(owner).unwrap().unwrap().pool_id,
        existing,
        "a chain read that failed must never cost the node its tracked pool"
    );
}

/// A service wired to a mocked chain and an in-memory store, for the lane
/// seed. Built field-wise rather than through `bootstrap`, which would spend
/// mock responses on its `usdc()` self-check and approval.
fn mocked_service(
    responses: Vec<alloy::primitives::Bytes>,
    store: Arc<dyn BuyerPoolStore>,
    signer: Arc<PrivateKeySigner>,
    owner: Address,
) -> BuyerPoolService<impl Provider + Clone + 'static> {
    mocked_service_on(mocked_pool_contract(responses), store, signer, owner)
}

/// [`mocked_service`] on a caller-built `contract`.
fn mocked_service_on<P: Provider + Clone + 'static>(
    contract: PaymentPool::PaymentPoolInstance<P>,
    store: Arc<dyn BuyerPoolStore>,
    signer: Arc<PrivateKeySigner>,
    owner: Address,
) -> BuyerPoolService<P> {
    BuyerPoolService {
        contract,
        store,
        signer,
        deployment: DEPLOYMENT,
        voucher_domain: Eip712Domain::default(),
        token: Address::repeat_byte(2),
        owner,
        working_deposit: U256::from(10_000_000u64),
        open_in_flight: Arc::new(Mutex::new(None)),
        open_hold: OpenHold::new(),
        topup_in_flight: Arc::new(Mutex::new(None)),
        seed_slots: Mutex::new(HashMap::new()),
        metrics: Arc::new(Metrics::new()),
        _reclaimer: AbortOnDropHandle::new(tokio::spawn(std::future::pending())),
    }
}

/// `record_progress` with a rebase anchor overwrites the lane record DOWN to
/// the upstream's watermark and advances it to the totals; without an anchor
/// the same lower totals are a superseded write that leaves the record alone.
#[tokio::test]
async fn record_progress_overwrites_down_only_with_a_rebase_anchor() {
    let owner = Address::repeat_byte(1);
    let signer = Arc::new(PrivateKeySigner::random());
    let pool_id = PoolId::from([0x5A; 32]);
    let provider = Address::repeat_byte(0xB0);
    let lane = LaneKey {
        pool_id,
        signer: signer.address(),
        provider,
    };
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let mut state = BuyerPoolState::new(
        pool_id,
        DEPLOYMENT,
        owner,
        Address::repeat_byte(2),
        U256::from(10_000_000u64),
    );
    state
        .advance_lane(lane, U256::from(900u64), U256::from(90u64))
        .expect("seed the lane");
    store.record(&state).expect("record the pool");
    let svc = mocked_service(Vec::new(), Arc::clone(&store), Arc::clone(&signer), owner);
    let lane_record = || {
        store
            .get_by_owner(owner)
            .expect("read")
            .and_then(|s| s.lane_progress(lane))
            .expect("lane record")
    };

    let totals = BuyerLaneProgress {
        last_amount: U256::from(65u64),
        last_bytes: U256::from(600u64),
    };
    svc.record_progress(provider, pool_id, ProgressWrite::Advance { totals })
        .expect("a superseded write is not an error");
    assert_eq!(
        lane_record().last_amount,
        U256::from(90u64),
        "without an anchor the write is monotone"
    );

    let anchor = BuyerLaneProgress {
        last_amount: U256::from(60u64),
        last_bytes: U256::from(500u64),
    };
    svc.record_progress(provider, pool_id, ProgressWrite::Rebase { anchor, totals })
        .expect("the rebase write lands");
    assert_eq!(
        lane_record(),
        BuyerLaneProgress {
            last_amount: U256::from(65u64),
            last_bytes: U256::from(600u64),
        },
        "overwritten down to the anchor, then advanced to the totals"
    );
}

/// A `BytesRegression` rebase anchor is behind the record on amount and
/// ahead of it on bytes. The rebase write still overwrites the record with
/// it and advances to the totals, so the next run resumes in step with the
/// node instead of from the stale seed.
#[tokio::test]
async fn record_progress_takes_a_rebase_anchor_ahead_on_bytes() {
    let owner = Address::repeat_byte(1);
    let signer = Arc::new(PrivateKeySigner::random());
    let pool_id = PoolId::from([0x5A; 32]);
    let provider = Address::repeat_byte(0xB0);
    let lane = LaneKey {
        pool_id,
        signer: signer.address(),
        provider,
    };
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let mut state = BuyerPoolState::new(
        pool_id,
        DEPLOYMENT,
        owner,
        Address::repeat_byte(2),
        U256::from(10_000_000u64),
    );
    state
        .advance_lane(lane, U256::from(5_000u64), U256::from(90u64))
        .expect("seed the lane");
    store.record(&state).expect("record the pool");
    let svc = mocked_service(Vec::new(), Arc::clone(&store), Arc::clone(&signer), owner);

    let anchor = BuyerLaneProgress {
        last_amount: U256::from(80u64),
        last_bytes: U256::from(9_000u64),
    };
    let totals = BuyerLaneProgress {
        last_amount: U256::from(100u64),
        last_bytes: U256::from(9_500u64),
    };
    svc.record_progress(provider, pool_id, ProgressWrite::Rebase { anchor, totals })
        .expect("the rebase write lands");
    assert_eq!(
        store
            .get_by_owner(owner)
            .expect("read")
            .and_then(|s| s.lane_progress(lane)),
        Some(totals),
        "overwritten with the anchor, then advanced to the totals"
    );
}

/// The pull hot path refuses a foreign row on its own, without relying on
/// bootstrap having cleaned up.
///
/// Bootstrap's drop can fail — the store faults, and `drop_foreign_row`
/// leaves the row where it is. If this read trusted bootstrap, that single
/// failure would make every pull for the life of the process pay against a
/// pool whose contract has never heard of it. The guard has to live at the
/// read that decides what gets paid, which makes it structural rather than
/// a property of boot ordering.
#[tokio::test]
async fn the_pull_path_ignores_a_row_from_another_deployment() {
    the_pull_path_ignores_a_row_on(OTHER_CONTRACT);
}

/// The same pull-path filter for a row on the configured contract address
/// but another chain.
#[tokio::test]
async fn the_pull_path_ignores_a_row_from_the_same_address_on_another_chain() {
    the_pull_path_ignores_a_row_on(OTHER_CHAIN);
}

/// Seed a row on `foreign` and read it through the pull path of a service
/// that buys on [`DEPLOYMENT`]. Needs a Tokio runtime: the service spawns.
fn the_pull_path_ignores_a_row_on(foreign: Deployment) {
    let owner = Address::repeat_byte(1);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            PoolId::from([0xAA; 32]),
            foreign,
            owner,
            Address::repeat_byte(2),
            U256::from(10_000_000u64),
        ))
        .expect("seed the foreign row");
    let svc = mocked_service(
        Vec::new(),
        Arc::clone(&store),
        Arc::new(PrivateKeySigner::random()),
        owner,
    );

    assert!(
        svc.reuse_or_report().expect("a readable store").is_none(),
        "a row on another PaymentPool deployment must read as no row, so the caller \
         opens a fresh pool instead of paying against it"
    );
    assert!(
        store.get_by_owner(owner).unwrap().is_some(),
        "the read is a filter, not a write: dropping the row is bootstrap's job"
    );
}

/// The re-check under the open slot suppresses the open only for a row on
/// this deployment. A foreign row that bootstrap could not drop already
/// reads as no row on the pull path; if the re-check counted it as live,
/// every pull would skip the open and find no pool again.
#[tokio::test]
async fn the_open_recheck_ignores_a_row_from_another_deployment() {
    for (seeded, opens) in [
        (OTHER_CHAIN, true),
        (OTHER_CONTRACT, true),
        (DEPLOYMENT, false),
    ] {
        let owner = Address::repeat_byte(1);
        let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
        store
            .record(&BuyerPoolState::new(
                PoolId::from([0xAA; 32]),
                seeded,
                owner,
                Address::repeat_byte(2),
                U256::from(10_000_000u64),
            ))
            .expect("seed the row");
        // One faulting call: an open that goes ahead spends it and fails.
        let (contract, asserter) = mocked_pool_contract_with(vec![MockCall::Err]);
        let result = run_open(
            &contract,
            &store,
            open_request(owner),
            None,
            &OpenHold::new(),
            &metrics(),
        )
        .await;
        if opens {
            assert!(
                result.is_err(),
                "a row on {seeded:?} must not suppress the open"
            );
            assert!(asserter.read_q().is_empty(), "the open reached the chain");
        } else {
            assert!(result.is_ok(), "a row on this deployment is the live pool");
            assert_eq!(asserter.read_q().len(), 1, "no open was attempted");
        }
    }
}

/// A tracked pool that no longer accepts funds does not suppress the open of
/// its replacement (ADR 003 § Funding recovery): the funding recovery step
/// names it, and the open goes ahead although the store still tracks it.
#[tokio::test]
async fn a_replacement_open_runs_past_the_closed_pool_it_replaces() {
    let owner = Address::repeat_byte(1);
    let closed = PoolId::from([0xAA; 32]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            closed,
            DEPLOYMENT,
            owner,
            Address::repeat_byte(2),
            U256::from(10_000_000u64),
        ))
        .expect("seed the row");
    // One faulting call: an open that goes ahead spends it and fails.
    let (contract, asserter) = mocked_pool_contract_with(vec![MockCall::Err]);
    let result = run_open(
        &contract,
        &store,
        open_request(owner),
        Some(closed),
        &OpenHold::new(),
        &metrics(),
    )
    .await;
    assert!(result.is_err(), "the replacement open reached the chain");
    assert!(
        asserter.read_q().is_empty(),
        "the open spent the scripted call"
    );
}

/// A lane this node has been paid on, but has no local record of, resumes
/// from the chain's watermark.
///
/// `PoolLedger` signs `prior + accrued`, so resuming from zero would put
/// every cumulative at or below the contract's watermark, where
/// `_applyVoucher` pays nothing — the node would stream real bytes and buy
/// none of them (#2072).
#[tokio::test]
async fn a_lane_with_no_local_record_resumes_from_the_chain_watermark() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let provider_addr = Address::repeat_byte(3);
    let pool_id = PoolId::from([7u8; 32]);
    let signer = signer();

    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let adopted = BuyerPoolState::new(
        pool_id,
        DEPLOYMENT,
        owner,
        Address::repeat_byte(2),
        U256::from(10_000_000u64),
    );
    store.record(&adopted).unwrap();

    // `getWatermark(...)` returns one `Lane`; the struct is a static tuple,
    // so its `SolValue` encoding equals the single-struct return.
    let service = mocked_service(
        vec![
            PaymentPool::Lane {
                amount: 191_205,
                bytesDelivered: 4_096,
            }
            .abi_encode()
            .into(),
        ],
        Arc::clone(&store),
        Arc::clone(&signer),
        owner,
    );

    let seeded = service
        .reseed_lane_from_chain(adopted, provider_addr)
        .await
        .expect("seed succeeds");
    let lane = LaneKey {
        pool_id,
        signer: signer.address(),
        provider: provider_addr,
    };
    let progress = seeded.lane_progress(lane).expect("lane seeded");
    assert_eq!(progress.last_amount, U256::from(191_205u64));
    assert_eq!(progress.last_bytes, U256::from(4_096u64));
    // And it is durable, so the next pull does not re-read the chain.
    assert!(
        store
            .get_by_owner(owner)
            .unwrap()
            .unwrap()
            .lane_progress(lane)
            .is_some()
    );
}

/// Two streams that both find the lane missing seed it once. Both pass the
/// lock-free check and wait on the lane's seed slot; the second then takes
/// the first one's seed from the store and reads no watermark, so a mock
/// with one queued `getWatermark` answers both. The slot leaves the map
/// with the last of them.
#[tokio::test]
async fn concurrent_reseeds_of_one_lane_read_the_chain_once() {
    concurrent_reseeds_read_the_chain_once(191_205, 4_096).await;
}

/// A zero watermark persists nothing, so the row cannot stop the next
/// stream in the queue from asking the chain again. The slot's shared read
/// does: a mock with one queued zero `getWatermark` answers both streams,
/// and both resume the lane from zero.
#[tokio::test]
async fn concurrent_reseeds_of_a_zero_watermark_read_the_chain_once() {
    concurrent_reseeds_read_the_chain_once(0, 0).await;
}

async fn concurrent_reseeds_read_the_chain_once(amount: u64, bytes: u64) {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let provider_addr = Address::repeat_byte(3);
    let pool_id = PoolId::from([7u8; 32]);
    let signer = signer();

    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let adopted = BuyerPoolState::new(
        pool_id,
        DEPLOYMENT,
        owner,
        Address::repeat_byte(2),
        U256::from(10_000_000u64),
    );
    store.record(&adopted).unwrap();
    let service = mocked_service(
        vec![
            PaymentPool::Lane {
                amount,
                bytesDelivered: bytes,
            }
            .abi_encode()
            .into(),
        ],
        Arc::clone(&store),
        Arc::clone(&signer),
        owner,
    );
    let lane = LaneKey {
        pool_id,
        signer: signer.address(),
        provider: provider_addr,
    };

    // Hold the lane's slot until both seeds are parked on it, so the second
    // passes the lock-free check against the same empty snapshot.
    let turn = service.seed_turn(lane);
    let held = turn.slot.lock().await;
    let (first, second, ()) = tokio::join!(
        service.reseed_lane_from_chain(adopted.clone(), provider_addr),
        service.reseed_lane_from_chain(adopted, provider_addr),
        async move {
            tokio::task::yield_now().await;
            drop(held);
        },
    );
    for seeded in [first, second] {
        let seeded = seeded.expect("both seeds succeed on one chain read");
        if amount == 0 && bytes == 0 {
            assert_eq!(
                seeded.lane_progress(lane),
                None,
                "a zero lane resumes from zero"
            );
        } else {
            let progress = seeded.lane_progress(lane).expect("lane seeded");
            assert_eq!(progress.last_amount, U256::from(amount));
            assert_eq!(progress.last_bytes, U256::from(bytes));
        }
    }
    drop(turn);
    assert!(
        service
            .seed_slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty(),
        "the slot leaves the map with the last stream that held it"
    );
}

/// #2292: a lane seeded from chain on an adopted row takes its watermark
/// out of the row's redeemed spend, so the pool spend counts it once.
#[tokio::test]
async fn a_lane_seeded_on_an_adopted_row_counts_its_watermark_once() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let provider_addr = Address::repeat_byte(3);
    let pool_id = PoolId::from([7u8; 32]);
    let signer = signer();
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let adopted = BuyerPoolState::adopt(
        pool_id,
        DEPLOYMENT,
        owner,
        Address::repeat_byte(2),
        U256::from(10_000_000u64),
        U256::from(9_500_000u64),
    );
    store.record(&adopted).unwrap();
    let service = mocked_service(
        vec![
            PaymentPool::Lane {
                amount: 1_000_000,
                bytesDelivered: 4_096,
            }
            .abi_encode()
            .into(),
        ],
        Arc::clone(&store),
        Arc::clone(&signer),
        owner,
    );

    let seeded = service
        .reseed_lane_from_chain(adopted, provider_addr)
        .await
        .expect("seed succeeds");
    assert_eq!(seeded.redeemed_elsewhere(), U256::from(8_500_000u64));
    assert_eq!(seeded.pool_spend(), U256::from(9_500_000u64));
    let row = store.get_by_owner(owner).unwrap().unwrap();
    assert_eq!(row.redeemed_elsewhere(), U256::from(8_500_000u64));
    assert_eq!(row.pool_spend(), U256::from(9_500_000u64));
}

/// A lane the node already tracks locally is never re-read from the chain.
///
/// The chain is stocked with a watermark ABOVE the local one, so a build
/// that dropped the `lane_progress` guard would read it, advance the lane and
/// fail the equality. An empty queue would not distinguish the two: the
/// mocked transport errors an unexpected call, and the error path is now a
/// refusal, which this test would then see as a different failure.
#[tokio::test]
async fn a_lane_with_local_progress_is_not_re_read_from_the_chain() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let provider_addr = Address::repeat_byte(3);
    let signer = signer();
    let state = pool_with_lane(
        signer.address(),
        provider_addr,
        U256::from(8_192u64),
        U256::from(400_000u64),
    );

    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store.record(&state).unwrap();
    let service = mocked_service(
        vec![
            PaymentPool::Lane {
                amount: 999_999,
                bytesDelivered: 999_999,
            }
            .abi_encode()
            .into(),
        ],
        Arc::clone(&store),
        Arc::clone(&signer),
        owner,
    );

    let out = service
        .reseed_lane_from_chain(state.clone(), provider_addr)
        .await
        .expect("a tracked lane needs no chain read");
    assert_eq!(out, state);
}

/// A lane nobody has ever redeemed on reads `(0, 0)`, which is already the
/// right resume point — so nothing is written and the state is unchanged.
#[tokio::test]
async fn a_never_paid_lane_is_left_at_zero() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let signer = signer();
    let state = BuyerPoolState::new(
        PoolId::from([7u8; 32]),
        DEPLOYMENT,
        owner,
        Address::repeat_byte(2),
        U256::from(10_000_000u64),
    );
    // Recorded, so a build that dropped the `(0, 0)` guard would commit a
    // zero lane and raise `lane_count`, failing the assertion below. Against
    // an unrecorded state the write would be a no-op and pass vacuously.
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store.record(&state).unwrap();
    let service = mocked_service(
        vec![
            PaymentPool::Lane {
                amount: 0,
                bytesDelivered: 0,
            }
            .abi_encode()
            .into(),
        ],
        Arc::clone(&store),
        Arc::clone(&signer),
        owner,
    );

    let out = service
        .reseed_lane_from_chain(state.clone(), Address::repeat_byte(3))
        .await
        .expect("a never-paid lane resumes at zero, it does not refuse");
    assert_eq!(out, state);
    assert_eq!(out.lane_count(), 0);
    assert_eq!(
        store.get_by_owner(owner).unwrap().unwrap().lane_count(),
        0,
        "a zero watermark must not be committed as a lane"
    );
}

/// An unreadable watermark REFUSES the pull, marked as this node's own fault.
///
/// Resuming from zero is not a recoverable degradation: the pull would
/// persist its own progress, give the lane a local row, and the
/// `lane_progress` guard would then stop the reseed ever running for that
/// provider again — stranding the lane below the chain watermark
/// permanently. `LocalPullFault` is what makes the classifier exonerate the
/// peer and refuse rather than report the blob absent.
#[tokio::test]
async fn an_unreadable_watermark_refuses_the_pull_as_a_local_fault() {
    let owner = Address::repeat_byte(1);
    let state = seedable_row(owner);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store.record(&state).unwrap();
    // No queued response: the mocked transport errors the `getWatermark`.
    let service = mocked_service(Vec::new(), store, signer(), owner);

    let err = service
        .reseed_lane_from_chain(state, Address::repeat_byte(3))
        .await
        .expect_err("an unreadable watermark must refuse, not resume from zero");
    assert_seed_refusal(&err, &service.metrics, "read the lane's on-chain watermark");
}

/// A row that vanishes while its lane waits to seed refuses the pull: seeding
/// it would pay against a pool the store no longer records.
#[tokio::test]
async fn a_vanished_row_refuses_the_pull_as_a_local_fault() {
    let owner = Address::repeat_byte(1);
    let state = seedable_row(owner);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let service = mocked_service(Vec::new(), store, signer(), owner);

    let err = service
        .reseed_lane_from_chain(state, Address::repeat_byte(3))
        .await
        .expect_err("a vanished row must refuse, not seed an untracked pool");
    assert_seed_refusal(
        &err,
        &service.metrics,
        "vanished before its lane could be seeded",
    );
}

/// A store that cannot be read under the seed lock refuses the pull: the node
/// cannot tell whether a sibling already seeded the lane.
#[tokio::test]
async fn an_unreadable_store_under_the_seed_lock_refuses_the_pull() {
    let owner = Address::repeat_byte(1);
    let state = seedable_row(owner);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(FailingStore);
    let service = mocked_service(Vec::new(), store, signer(), owner);

    let err = service
        .reseed_lane_from_chain(state, Address::repeat_byte(3))
        .await
        .expect_err("an unreadable store must refuse, not seed blind");
    assert_seed_refusal(
        &err,
        &service.metrics,
        "re-read the buyer pool before seeding a lane",
    );
}

/// A seed the store cannot persist refuses the pull: an unpersisted seed
/// leaves the lane to resume from zero on the next pull.
#[tokio::test]
async fn a_failed_seed_persist_refuses_the_pull_as_a_local_fault() {
    let (service, result) = seed_against(SeedAnswer::Fault).await;
    let err = result.expect_err("an unpersisted seed must refuse, not resume from zero");
    assert_seed_refusal(
        &err,
        &service.metrics,
        "persist the lane's on-chain watermark",
    );
}

/// A row that vanishes between the re-read and the seed write refuses the
/// pull, exactly as one that vanished before the re-read does: paying on would
/// pay against a pool the store no longer records.
#[tokio::test]
async fn a_row_vanishing_under_the_seed_write_refuses_the_pull() {
    let (service, result) = seed_against(SeedAnswer::UnknownPool).await;
    let err = result.expect_err("a seed persisted against no row must refuse");
    assert_seed_refusal(
        &err,
        &service.metrics,
        "vanished before its lane seed could be persisted",
    );
}

/// A row a newer pool replaced under the seed write is not a refusal: the pull
/// pins the row the store holds now, and nothing is metered as a failure.
#[tokio::test]
async fn a_pool_replaced_under_the_seed_write_is_not_a_refusal() {
    let (service, result) = seed_against(SeedAnswer::PoolMismatch).await;
    result.expect("a replaced pool pins the replacement");
    let text = service.metrics.encode().expect("encode metrics");
    for name in [
        "decdn_node_pull_pool_open_failures_total",
        "decdn_buyer_lane_seed_failures_total",
    ] {
        assert!(
            text.lines().any(|l| l == format!("{name} 0")),
            "{name} must not move on a replaced pool"
        );
    }
}

/// Reseed a recorded row against a non-zero chain watermark, with the store's
/// `seed_progress` answering `answer`.
async fn seed_against(
    answer: SeedAnswer,
) -> (
    BuyerPoolService<impl Provider + Clone + 'static>,
    Result<BuyerPoolState>,
) {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let state = seedable_row(owner);
    let inner = MemoryBuyerPoolStore::new();
    inner.record(&state).unwrap();
    let store: Arc<dyn BuyerPoolStore> = Arc::new(WriteOnlyFault(inner, answer));
    let watermark = PaymentPool::Lane {
        amount: 191_205,
        bytesDelivered: 4_096,
    };
    let service = mocked_service(vec![watermark.abi_encode().into()], store, signer(), owner);
    let result = service
        .reseed_lane_from_chain(state, Address::repeat_byte(3))
        .await;
    (service, result)
}

/// A pool row with no lane progress, so a reseed reaches the chain.
fn seedable_row(owner: Address) -> BuyerPoolState {
    BuyerPoolState::new(
        PoolId::from([7u8; 32]),
        DEPLOYMENT,
        owner,
        Address::repeat_byte(2),
        U256::from(10_000_000u64),
    )
}

/// Assert `err` is the seed leg named by `leg`, refused as this node's own
/// fault and reported at the raising site.
///
/// `OpenReported` tells the classifier the failure is already metered, so the
/// site must have moved the open-failure total once, beside the by-cause
/// lane-seed counter.
fn assert_seed_refusal(err: &anyhow::Error, metrics: &Metrics, leg: &str) {
    assert!(
        format!("{err:#}").contains(leg),
        "expected the `{leg}` leg, got: {err:#}"
    );
    assert!(
        err.downcast_ref::<LocalPullFault>().is_some(),
        "the refusal is this node's fault, not the upstream's"
    );
    assert!(
        err.downcast_ref::<OpenReported>().is_some(),
        "the raising site reports it, so the classifier must not restate it"
    );
    let text = metrics.encode().expect("encode metrics");
    for name in [
        "decdn_node_pull_pool_open_failures_total",
        "decdn_buyer_lane_seed_failures_total",
    ] {
        assert!(
            text.lines().any(|l| l == format!("{name} 1")),
            "{name} must read 1 after one seed refusal"
        );
    }
}

/// A `BuyerPoolStore` whose every read and write faults, for the legs
/// `MemoryBuyerPoolStore` cannot express.
#[derive(Debug)]
struct FailingStore;

impl BuyerPoolStore for FailingStore {
    fn load_all(&self) -> std::result::Result<decdn_incentive::BuyerLoad, StoreError> {
        Err(StoreError::Backend("load_all faulted".into()))
    }
    fn record(&self, _state: &BuyerPoolState) -> std::result::Result<(), StoreError> {
        Err(StoreError::Backend("record faulted".into()))
    }
    fn forget(&self, _owner: Address) -> std::result::Result<(), StoreError> {
        Err(StoreError::Backend("forget faulted".into()))
    }
    fn get_by_pool_id(
        &self,
        _pool_id: PoolId,
    ) -> std::result::Result<Option<BuyerPoolState>, StoreError> {
        Err(StoreError::Backend("get_by_pool_id faulted".into()))
    }
    fn forget_if_pool(
        &self,
        _owner: Address,
        _pool_id: PoolId,
    ) -> std::result::Result<bool, StoreError> {
        Err(StoreError::Backend("forget_if_pool faulted".into()))
    }
    fn get_by_owner(
        &self,
        _owner: Address,
    ) -> std::result::Result<Option<BuyerPoolState>, StoreError> {
        Err(StoreError::Backend("get_by_owner faulted".into()))
    }
    fn advance_progress(
        &self,
        _owner: Address,
        _pool_id: PoolId,
        _lane: LaneKey,
        _bytes: U256,
        _amount: U256,
    ) -> std::result::Result<AdvanceOutcome, StoreError> {
        Err(StoreError::Backend("advance_progress faulted".into()))
    }
    fn rebase_progress(
        &self,
        _owner: Address,
        _pool_id: PoolId,
        _lane: LaneKey,
        _anchor: BuyerLaneProgress,
        _totals: BuyerLaneProgress,
    ) -> std::result::Result<AdvanceOutcome, StoreError> {
        Err(StoreError::Backend("rebase_progress faulted".into()))
    }
    fn seed_progress(
        &self,
        _owner: Address,
        _pool_id: PoolId,
        _lane: LaneKey,
        _bytes: U256,
        _amount: U256,
    ) -> std::result::Result<AdvanceOutcome, StoreError> {
        Err(StoreError::Backend("seed_progress faulted".into()))
    }
    fn add_deposit(
        &self,
        _owner: Address,
        _pool_id: PoolId,
        _additional: U256,
    ) -> std::result::Result<decdn_incentive::DepositOutcome, StoreError> {
        Err(StoreError::Backend("add_deposit faulted".into()))
    }
}

/// A fully-redeemed pool is still `Open` on chain, and adopting it would
/// wedge buying for good: `reuse_or_report` would answer `Some` forever
/// against a deposit that can fund no voucher, so no fresh pool would open.
/// The walk passes it over for the solvent one behind it.
#[tokio::test]
async fn reconcile_skips_a_fully_redeemed_pool() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let solvent = PoolId::from([0xAA; 32]);
    let drained = PoolId::from([0xBB; 32]);

    let mut spent = onchain_pool(owner, PaymentPool::Status::Open, 10_000_000);
    spent.totalRedeemed = 10_000_000;
    let contract = mocked_pool_contract(vec![
        vec![solvent, drained].abi_encode().into(),
        spent.abi_encode().into(),
        onchain_pool(owner, PaymentPool::Status::Open, 9_000_000)
            .abi_encode()
            .into(),
    ]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());

    assert_eq!(
        bootstrap_reconcile(
            &contract,
            DEPLOYMENT,
            &store,
            owner,
            Address::repeat_byte(2),
            &metrics()
        )
        .await,
        Reconciled::Adopted(solvent)
    );
    assert_eq!(store.get_by_owner(owner).unwrap().unwrap().pool_id, solvent);
}

/// An unreadable store adopts nothing and counts the fault. Writing an
/// adopted row into a store whose contents are unknown risks a second row
/// beside one already there — the failure adoption exists to prevent — so
/// `Unknown` must not be treated as `Applies`.
#[tokio::test]
async fn reconcile_counts_an_unreadable_store_and_adopts_nothing() {
    let owner = Address::repeat_byte(1);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(FailingStore);
    let metrics = metrics();
    // Empty queue: reaching the chain at all would be the bug.
    let contract = mocked_pool_contract(Vec::new());

    assert_eq!(
        adoption_applies(&store, owner, DEPLOYMENT, ReconcileRun::Bootstrap),
        AdoptionCheck::Unknown
    );
    assert_eq!(
        bootstrap_reconcile(
            &contract,
            DEPLOYMENT,
            &store,
            owner,
            Address::repeat_byte(2),
            &metrics
        )
        .await,
        Reconciled::Unknown
    );
    assert_eq!(
        adoption_failures(&metrics),
        1,
        "an unreadable store is a counted adoption fault, not a quiet skip"
    );
}

/// A store whose reads answer normally, whose `record` faults, and whose
/// `seed_progress` answers with the scripted [`SeedAnswer`]: the
/// escrowed-but-unpersisted and lane-seed legs, which `MemoryBuyerPoolStore`
/// cannot express.
#[derive(Debug)]
struct WriteOnlyFault(MemoryBuyerPoolStore, SeedAnswer);

/// What [`WriteOnlyFault`]'s `seed_progress` answers.
#[derive(Debug, Clone, Copy)]
enum SeedAnswer {
    /// A store fault.
    Fault,
    /// The owner's row is gone.
    UnknownPool,
    /// The owner's row names a newer pool.
    PoolMismatch,
}

impl BuyerPoolStore for WriteOnlyFault {
    fn load_all(&self) -> std::result::Result<decdn_incentive::BuyerLoad, StoreError> {
        self.0.load_all()
    }
    fn record(&self, _state: &BuyerPoolState) -> std::result::Result<(), StoreError> {
        Err(StoreError::Backend("disk full".into()))
    }
    fn forget(&self, owner: Address) -> std::result::Result<(), StoreError> {
        self.0.forget(owner)
    }
    fn get_by_pool_id(
        &self,
        pool_id: PoolId,
    ) -> std::result::Result<Option<BuyerPoolState>, StoreError> {
        self.0.get_by_pool_id(pool_id)
    }
    fn forget_if_pool(
        &self,
        owner: Address,
        pool_id: PoolId,
    ) -> std::result::Result<bool, StoreError> {
        self.0.forget_if_pool(owner, pool_id)
    }
    fn get_by_owner(
        &self,
        owner: Address,
    ) -> std::result::Result<Option<BuyerPoolState>, StoreError> {
        self.0.get_by_owner(owner)
    }
    fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> std::result::Result<AdvanceOutcome, StoreError> {
        self.0.advance_progress(owner, pool_id, lane, bytes, amount)
    }
    fn rebase_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        anchor: BuyerLaneProgress,
        totals: BuyerLaneProgress,
    ) -> std::result::Result<AdvanceOutcome, StoreError> {
        self.0.rebase_progress(owner, pool_id, lane, anchor, totals)
    }
    fn seed_progress(
        &self,
        _owner: Address,
        _pool_id: PoolId,
        _lane: LaneKey,
        _bytes: U256,
        _amount: U256,
    ) -> std::result::Result<AdvanceOutcome, StoreError> {
        match self.1 {
            SeedAnswer::Fault => Err(StoreError::Backend("disk full".into())),
            SeedAnswer::UnknownPool => Ok(AdvanceOutcome::UnknownPool),
            SeedAnswer::PoolMismatch => Ok(AdvanceOutcome::PoolMismatch),
        }
    }
    fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> std::result::Result<decdn_incentive::DepositOutcome, StoreError> {
        self.0.add_deposit(owner, pool_id, additional)
    }
}

/// Faults only `forget_if_pool`, so a test can hold a foreign row that
/// refuses to be dropped — the one state `drop_foreign_row` returns `false`
/// for. Every other operation is the real in-memory store.
struct ForgetFault(MemoryBuyerPoolStore);
impl BuyerPoolStore for ForgetFault {
    fn load_all(&self) -> std::result::Result<decdn_incentive::BuyerLoad, StoreError> {
        self.0.load_all()
    }
    fn record(&self, state: &BuyerPoolState) -> std::result::Result<(), StoreError> {
        self.0.record(state)
    }
    fn forget(&self, owner: Address) -> std::result::Result<(), StoreError> {
        self.0.forget(owner)
    }
    fn get_by_pool_id(
        &self,
        pool_id: PoolId,
    ) -> std::result::Result<Option<BuyerPoolState>, StoreError> {
        self.0.get_by_pool_id(pool_id)
    }
    fn forget_if_pool(
        &self,
        _owner: Address,
        _pool_id: PoolId,
    ) -> std::result::Result<bool, StoreError> {
        Err(StoreError::Backend("forget faulted".into()))
    }
    fn get_by_owner(
        &self,
        owner: Address,
    ) -> std::result::Result<Option<BuyerPoolState>, StoreError> {
        self.0.get_by_owner(owner)
    }
    fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> std::result::Result<AdvanceOutcome, StoreError> {
        self.0.advance_progress(owner, pool_id, lane, bytes, amount)
    }
    fn rebase_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        anchor: BuyerLaneProgress,
        totals: BuyerLaneProgress,
    ) -> std::result::Result<AdvanceOutcome, StoreError> {
        self.0.rebase_progress(owner, pool_id, lane, anchor, totals)
    }
    fn seed_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> std::result::Result<AdvanceOutcome, StoreError> {
        self.0.seed_progress(owner, pool_id, lane, bytes, amount)
    }
    fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> std::result::Result<decdn_incentive::DepositOutcome, StoreError> {
        self.0.add_deposit(owner, pool_id, additional)
    }
}

/// A foreign row that cannot be dropped refuses the adoption and leaves the
/// row exactly where it was — it must not be adopted around, because the
/// reuse lookup can still reach it.
///
/// It is also not an adoption *failure*: that counter means "about to
/// escrow a second deposit beside one it already holds", and this node
/// escrows nothing. It is stuck on a row it already has, which is the
/// opposite state, so counting it would send an operator hunting a
/// duplicate deposit that does not exist.
#[tokio::test]
async fn reconcile_refuses_when_a_foreign_row_cannot_be_dropped() {
    let owner = Address::repeat_byte(1);
    let foreign = PoolId::from([0xAA; 32]);
    let inner = MemoryBuyerPoolStore::new();
    inner
        .record(&BuyerPoolState::new(
            foreign,
            OTHER_CONTRACT,
            owner,
            Address::repeat_byte(2),
            U256::from(10_000_000u64),
        ))
        .expect("seed the foreign row");
    let store: Arc<dyn BuyerPoolStore> = Arc::new(ForgetFault(inner));
    let metrics = metrics();
    // Empty queue: reaching the chain at all would mean it adopted around
    // a row it could not drop.
    let contract = mocked_pool_contract(Vec::new());

    assert_eq!(
        bootstrap_reconcile(
            &contract,
            DEPLOYMENT,
            &store,
            owner,
            Address::repeat_byte(2),
            &metrics
        )
        .await,
        Reconciled::Unknown
    );
    assert_eq!(
        adoption_failures(&metrics),
        0,
        "a node stuck on a row it cannot drop escrows nothing; counting it as an \
         adoption failure would describe the opposite state"
    );
    let survivor = store.get_by_owner(owner).unwrap().expect("row survives");
    assert_eq!(
        survivor.pool_id, foreign,
        "a refused drop must leave the store untouched"
    );
}

/// A store that cannot persist the adopted row counts the fault, so an
/// operator sees the state in which the node is about to escrow a second
/// deposit rather than only a log line.
#[tokio::test]
async fn reconcile_counts_a_failed_persist() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(WriteOnlyFault(
        MemoryBuyerPoolStore::new(),
        SeedAnswer::Fault,
    ));
    let metrics = metrics();
    let contract = mocked_pool_contract(vec![
        vec![PoolId::from([0xAA; 32])].abi_encode().into(),
        onchain_pool(owner, PaymentPool::Status::Open, 10_000_000)
            .abi_encode()
            .into(),
    ]);

    assert!(matches!(
        bootstrap_reconcile(
            &contract,
            DEPLOYMENT,
            &store,
            owner,
            Address::repeat_byte(2),
            &metrics
        )
        .await,
        Reconciled::Unrecorded(_)
    ));
    assert_eq!(adoption_failures(&metrics), 1);
}

/// Read `decdn_buyer_pool_adoption_failures_total` off an encoded registry.
fn adoption_failures(metrics: &Arc<Metrics>) -> u64 {
    let text = metrics.encode().expect("encode metrics");
    text.lines()
        .find_map(|l| {
            l.strip_prefix("decdn_buyer_pool_adoption_failures_total")?
                .strip_prefix(' ')?
                .parse::<u64>()
                .ok()
        })
        .unwrap_or_default()
}

/// A pool that is only unreachable — an RPC blip on `getPools` — leaves the
/// store untouched and returns `false`, so the first miss falls through to
/// the ordinary lazy open instead of buying being disabled for the process.
#[tokio::test]
async fn reconcile_leaves_the_store_untouched_when_the_chain_is_unreachable() {
    let owner = Address::repeat_byte(1);
    // No queued response: the mocked transport errors the `getPools` call.
    let contract = mocked_pool_contract(Vec::new());
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());

    assert_eq!(
        bootstrap_reconcile(
            &contract,
            DEPLOYMENT,
            &store,
            owner,
            Address::repeat_byte(2),
            &metrics()
        )
        .await,
        Reconciled::Unknown
    );
    assert!(store.get_by_owner(owner).unwrap().is_none());
}

fn pool_with_lane(
    signer_addr: Address,
    provider: Address,
    bytes: U256,
    amount: U256,
) -> BuyerPoolState {
    let pool_id = decdn_incentive::PoolId::from([7u8; 32]);
    let mut state = BuyerPoolState::new(
        pool_id,
        DEPLOYMENT,
        Address::repeat_byte(1),
        Address::repeat_byte(2),
        U256::from(10_000u64),
    );
    let lane = LaneKey {
        pool_id,
        signer: signer_addr,
        provider,
    };
    state.advance_lane(lane, bytes, amount).unwrap();
    state
}

/// One scripted step of a funding call: the claim the slot hands out, and the
/// outcome of the `topUp` behind it.
enum Step {
    /// The `topUp` lands with this `(new_deposit, credited)`.
    Lands(u64, u64),
    /// The `topUp` fails with a plain error.
    Fails(&'static str),
    /// The `topUp` mined but its deposit is escrowed and untracked.
    Untracked,
}

/// A scripted `join_or_spawn` for [`top_up_at_least`]: each call pops the next
/// `(claim, step)` and records the amount it was asked for.
fn scripted(
    script: Vec<(TopUpClaim, Step)>,
    asked: &std::cell::RefCell<Vec<U256>>,
) -> impl FnMut(U256) -> (futures_util::future::Ready<TopUpOutcome>, TopUpClaim) + '_ {
    let mut script = script.into_iter();
    move |amount| {
        asked.borrow_mut().push(amount);
        let (claim, step) = script.next().expect("script ran out of calls");
        let outcome = match step {
            Step::Lands(new_deposit, credited) => Ok(TopUpLanded {
                new_deposit: U256::from(new_deposit),
                added: U256::from(credited),
            }),
            Step::Fails(msg) => Err(Arc::new(anyhow::anyhow!(msg))),
            Step::Untracked => Err(Arc::new(
                anyhow::anyhow!("row replaced").context(EscrowedUntracked),
            )),
        };
        (futures_util::future::ready(outcome), claim)
    }
}

fn u(v: u64) -> U256 {
    U256::from(v)
}

fn joined(claimed: u64, requested: u64) -> TopUpClaim {
    TopUpClaim::Joined {
        claimed: u(claimed),
        requested: u(requested),
    }
}

fn landed(new_deposit: u64, added: u64) -> TopUpLanded {
    TopUpLanded {
        new_deposit: u(new_deposit),
        added: u(added),
    }
}

const POOL: PoolId = PoolId::ZERO;

#[tokio::test]
async fn top_up_at_least_spawned_call_is_final() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(vec![(TopUpClaim::Spawned, Step::Lands(1_100, 100))], &asked);

    let got = top_up_at_least(POOL, u(100), f).await.unwrap();

    assert_eq!(got, landed(1_100, 100));
    assert_eq!(*asked.borrow(), vec![u(100)]);
}

/// A spawned `topUp` that credits less than requested is NOT retried: the
/// shortfall is not from a join, and a retry escrows a second `topUp`.
#[tokio::test]
async fn top_up_at_least_short_spawn_is_not_retried() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(vec![(TopUpClaim::Spawned, Step::Lands(1_040, 40))], &asked);

    let got = top_up_at_least(POOL, u(100), f).await.unwrap();

    assert_eq!(got, landed(1_040, 40));
    assert_eq!(asked.borrow().len(), 1);
}

/// A join whose claim exactly covers the request needs no follow-up, however
/// much the joined `topUp` itself raised the deposit.
#[tokio::test]
async fn top_up_at_least_exact_join_needs_no_follow_up() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(vec![(joined(100, 500), Step::Lands(1_500, 500))], &asked);

    let got = top_up_at_least(POOL, u(100), f).await.unwrap();

    assert_eq!(got, landed(1_500, 100));
    assert_eq!(asked.borrow().len(), 1);
}

/// The #2012 case: a join claims 40 of 100, so a follow-up asks for the other 60.
#[tokio::test]
async fn top_up_at_least_short_join_funds_the_remainder() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(
        vec![
            (joined(40, 40), Step::Lands(1_040, 40)),
            (TopUpClaim::Spawned, Step::Lands(1_100, 60)),
        ],
        &asked,
    );

    let got = top_up_at_least(POOL, u(100), f).await.unwrap();

    assert_eq!(got, landed(1_100, 100));
    assert_eq!(*asked.borrow(), vec![u(100), u(60)]);
}

/// Two concurrent recovery top-ups: the joiner claims none of the spawner's
/// escrow, so it funds its whole request itself instead of counting the
/// spawner's deposit growth as its own.
#[tokio::test]
async fn top_up_at_least_zero_claim_funds_the_whole_request() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(
        vec![
            (joined(0, 100), Step::Lands(1_100, 100)),
            (TopUpClaim::Spawned, Step::Lands(1_200, 100)),
        ],
        &asked,
    );

    let got = top_up_at_least(POOL, u(100), f).await.unwrap();

    assert_eq!(got, landed(1_200, 100));
    assert_eq!(*asked.borrow(), vec![u(100), u(100)]);
}

/// A short join followed by a join that covers the rest stops on the cumulative
/// claim.
#[tokio::test]
async fn top_up_at_least_second_join_covers_the_rest() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(
        vec![
            (joined(30, 300), Step::Lands(1_300, 300)),
            (joined(70, 200), Step::Lands(1_500, 200)),
        ],
        &asked,
    );

    let got = top_up_at_least(POOL, u(100), f).await.unwrap();

    assert_eq!(got, landed(1_500, 100));
    assert_eq!(*asked.borrow(), vec![u(100), u(70)]);
}

/// A joined refill that asked for 100 but the chain credited only 50 (the
/// contract credits the measured transfer). Claims of 60 and 40 were split out
/// of the 100 before it landed, so they add up to more than landed. The join
/// counts none of its claim and funds its whole need itself.
#[tokio::test]
async fn top_up_at_least_short_joined_topup_counts_no_claim() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(
        vec![
            (joined(60, 100), Step::Lands(1_050, 50)),
            (TopUpClaim::Spawned, Step::Lands(1_110, 60)),
        ],
        &asked,
    );

    let got = top_up_at_least(POOL, u(60), f).await.unwrap();

    assert_eq!(got, landed(1_110, 60));
    assert_eq!(*asked.borrow(), vec![u(60), u(60)]);
}

/// A failed joined `topUp` is another funder's failure: this caller funds the
/// same amount again with a `topUp` of its own.
#[tokio::test]
async fn top_up_at_least_failed_join_funds_with_its_own_topup() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(
        vec![
            (joined(100, 100), Step::Fails("refill rpc error")),
            (TopUpClaim::Spawned, Step::Lands(1_100, 100)),
        ],
        &asked,
    );

    let got = top_up_at_least(POOL, u(100), f).await.unwrap();

    assert_eq!(got, landed(1_100, 100));
    assert_eq!(*asked.borrow(), vec![u(100), u(100)]);
}

/// An escrowed-but-untracked joined `topUp` stops the top-up: a second `topUp`
/// against a row that cannot be credited strands a second deposit.
#[tokio::test]
async fn top_up_at_least_untracked_join_is_not_retried() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(vec![(joined(100, 100), Step::Untracked)], &asked);

    let err = top_up_at_least(POOL, u(100), f).await.unwrap_err();

    assert!(format!("{err:#}").contains("escrowed"), "{err:#}");
    assert_eq!(asked.borrow().len(), 1);
}

#[tokio::test]
async fn top_up_at_least_spawned_error_propagates() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(
        vec![
            (joined(40, 40), Step::Lands(1_040, 40)),
            (TopUpClaim::Spawned, Step::Fails("chain rejected")),
        ],
        &asked,
    );

    let err = top_up_at_least(POOL, u(100), f).await.unwrap_err();

    assert!(format!("{err:#}").contains("chain rejected"), "{err:#}");
    assert_eq!(asked.borrow().len(), 2);
}

/// When the funding calls run out short, the top-up returns what landed rather
/// than an error, so the pull keeps the headroom that is really escrowed.
#[tokio::test]
async fn top_up_at_least_returns_what_landed_after_repeated_short_joins() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(
        vec![
            (joined(10, 10), Step::Lands(1_010, 10)),
            (joined(10, 10), Step::Lands(1_020, 10)),
            (joined(10, 10), Step::Lands(1_030, 10)),
        ],
        &asked,
    );

    let got = top_up_at_least(POOL, u(100), f).await.unwrap();

    assert_eq!(got, landed(1_030, 30));
    assert_eq!(*asked.borrow(), vec![u(100), u(90), u(80)]);
    assert_eq!(asked.borrow().len(), MAX_TOPUP_CALLS as usize);
}

#[tokio::test]
async fn top_up_at_least_errors_when_every_join_fails() {
    let asked = std::cell::RefCell::new(Vec::new());
    let f = scripted(
        vec![
            (joined(0, 100), Step::Fails("rpc down")),
            (joined(0, 100), Step::Fails("rpc down")),
            (joined(0, 100), Step::Fails("rpc still down")),
        ],
        &asked,
    );

    let err = top_up_at_least(POOL, u(100), f).await.unwrap_err();

    assert!(format!("{err:#}").contains("rpc still down"), "{err:#}");
}

fn in_flight(amount: u64, funder: TopUpFunder) -> InFlightTopUp {
    let fut: BoxFuture<'static, TopUpOutcome> =
        Box::pin(futures_util::future::ready(Ok(landed(0, amount))));
    InFlightTopUp::new(fut.shared(), u(amount), funder)
}

/// A refill's amount is claimable once: reactive joiners split it, and a claim
/// never exceeds what is left.
#[test]
fn in_flight_refill_is_claimed_at_most_once() {
    let mut slot = in_flight(100, TopUpFunder::Refill);

    assert_eq!(slot.join(u(60), TopUpFunder::Recovery).1, joined(60, 100));
    assert_eq!(slot.join(u(60), TopUpFunder::Recovery).1, joined(40, 100));
    assert_eq!(slot.join(u(60), TopUpFunder::Recovery).1, joined(0, 100));
}

/// A recovery top-up's amount is its spawner's: a second recovery top-up that
/// joins it claims nothing (#2012).
#[test]
fn in_flight_recovery_topup_leaves_nothing_to_claim() {
    let mut slot = in_flight(100, TopUpFunder::Recovery);

    assert_eq!(slot.join(u(100), TopUpFunder::Recovery).1, joined(0, 100));
}

/// A refill that joins claims nothing, so it cannot take headroom a reactive
/// top-up could claim later.
#[test]
fn in_flight_refill_joiner_claims_nothing() {
    let mut slot = in_flight(100, TopUpFunder::Refill);

    assert_eq!(slot.join(u(100), TopUpFunder::Refill).1, joined(0, 100));
    assert_eq!(slot.join(u(100), TopUpFunder::Recovery).1, joined(100, 100));
}

#[test]
fn refill_decision_no_topup_with_headroom() {
    // committed 100 of 10_000; working 10_000 → low-water 2_000; remaining huge.
    assert_eq!(
        refill_decision(
            U256::from(10_000u64),
            U256::from(100u64),
            U256::from(10_000u64)
        ),
        U256::ZERO
    );
}

#[test]
fn refill_decision_tops_up_to_target_when_below_low_water() {
    // deposit 1_000, committed 900 → remaining 100 < low-water 2_000; refill to
    // target 10_000 restores remaining to 10_000 (adds 9_900).
    assert_eq!(
        refill_decision(
            U256::from(1_000u64),
            U256::from(900u64),
            U256::from(10_000u64)
        ),
        U256::from(9_900u64)
    );
}

#[test]
fn pin_ctx_pins_provider_and_lane_priors_and_capability() {
    let s = signer();
    let provider = Address::repeat_byte(3);
    let state = pool_with_lane(s.address(), provider, U256::from(10u64), U256::from(40u64));
    let domain = Eip712Domain::default();

    let ctx = pin_ctx(&state, &s, &domain, provider).expect("pin");

    assert_eq!(
        ctx.provider, provider,
        "the delivering provider must be pinned (never ZERO)"
    );
    assert_eq!(ctx.pool_id, state.pool_id);
    assert_eq!(
        ctx.prior_bytes_delivered,
        U256::from(10u64),
        "lane priors seed the resume"
    );
    assert_eq!(ctx.prior_amount, U256::from(40u64));
    assert!(
        ctx.capability.is_some(),
        "the self-issued capability rides the request"
    );
}

#[test]
fn pin_ctx_untouched_lane_starts_at_zero_priors() {
    let s = signer();
    let provider = Address::repeat_byte(9);
    let state = BuyerPoolState::new(
        decdn_incentive::PoolId::from([7u8; 32]),
        DEPLOYMENT,
        Address::repeat_byte(1),
        Address::repeat_byte(2),
        U256::from(10_000u64),
    );
    let domain = Eip712Domain::default();

    let ctx = pin_ctx(&state, &s, &domain, provider).expect("pin");

    assert_eq!(ctx.provider, provider);
    assert_eq!(ctx.prior_bytes_delivered, U256::ZERO);
    assert_eq!(ctx.prior_amount, U256::ZERO);
}

use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn happy_path_attempts_topup_once_and_never_reads_allowance() {
    let attempts = AtomicUsize::new(0);
    let recovers = AtomicUsize::new(0);
    let out = top_up_recovering_allowance(
        || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Ok(U256::from(500u64)) }
        },
        || {
            recovers.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        },
    )
    .await;
    assert_eq!(out.unwrap(), U256::from(500u64));
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "one topUp");
    assert_eq!(
        recovers.load(Ordering::SeqCst),
        0,
        "zero allowance reads on the happy path"
    );
}

#[tokio::test]
async fn bad_path_approves_then_retries_topup_once() {
    let attempts = AtomicUsize::new(0);
    let recovers = AtomicUsize::new(0);
    let out = top_up_recovering_allowance(
        || {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    Err(anyhow::Error::new(
                        decdn_client::buyer_pool::AllowanceShortfall,
                    ))
                } else {
                    Ok(U256::from(700u64))
                }
            }
        },
        || {
            recovers.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        },
    )
    .await;
    assert_eq!(out.unwrap(), U256::from(700u64));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "topUp, then retry after approve"
    );
    assert_eq!(recovers.load(Ordering::SeqCst), 1, "exactly one approve");
}

#[tokio::test]
async fn terminal_non_allowance_revert_is_not_retried() {
    let attempts = AtomicUsize::new(0);
    let recovers = AtomicUsize::new(0);
    let out: Result<U256> = top_up_recovering_allowance(
        || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err(anyhow::anyhow!("topUp reverted for pool: paused")) }
        },
        || {
            recovers.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        },
    )
    .await;
    assert!(out.is_err());
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "no retry for a non-allowance revert"
    );
    assert_eq!(
        recovers.load(Ordering::SeqCst),
        0,
        "no approve for a non-allowance revert"
    );
}

#[tokio::test]
async fn retry_still_shortfall_stops_after_one_retry() {
    let attempts = AtomicUsize::new(0);
    let out: Result<U256> = top_up_recovering_allowance(
        || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err(anyhow::Error::new(
                    decdn_client::buyer_pool::AllowanceShortfall,
                ))
            }
        },
        || async { Ok(()) },
    )
    .await;
    assert!(out.is_err());
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "one retry only, then give up"
    );
}

/// A service whose chain calls answer from `asserter`, over a pool row of
/// `deposit` with no spend, at a working deposit of 10 USDC.
fn recovery_service(
    asserter: &alloy::providers::mock::Asserter,
    deposit: u64,
) -> BuyerPoolService<impl Provider + Clone + 'static> {
    use alloy::providers::ProviderBuilder;

    let owner = Address::repeat_byte(1);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            PoolId::from([0x5A; 32]),
            DEPLOYMENT,
            owner,
            Address::repeat_byte(2),
            U256::from(deposit),
        ))
        .expect("seed the row");
    BuyerPoolService {
        contract: PaymentPool::new(
            Address::ZERO,
            ProviderBuilder::new().connect_mocked_client(asserter.clone()),
        ),
        store,
        signer: signer(),
        deployment: DEPLOYMENT,
        voucher_domain: Eip712Domain::default(),
        token: Address::repeat_byte(2),
        owner,
        working_deposit: U256::from(10_000_000u64),
        open_in_flight: Arc::new(Mutex::new(None)),
        open_hold: OpenHold::new(),
        topup_in_flight: Arc::new(Mutex::new(None)),
        seed_slots: Mutex::new(HashMap::new()),
        metrics: Arc::new(Metrics::new()),
        _reclaimer: AbortOnDropHandle::new(tokio::spawn(std::future::pending())),
    }
}

/// A row above the deposit the fill saw is a sibling fill's step that landed:
/// the step shares it and makes no chain call.
#[tokio::test]
async fn a_recovery_step_shares_a_siblings_landed_top_up() {
    let asserter = alloy::providers::mock::Asserter::new();
    let svc = recovery_service(&asserter, 10_000_000);
    let stepped = svc
        .recover_pool(U256::from(4_000_000u64))
        .await
        .expect("no chain call");
    assert_eq!(stepped, Recovery::ToppedUp(U256::from(10_000_000u64)));
}

/// Concurrent fills that reach the step while one recovery top-up is in
/// flight all wait for it: the pool escrows once, and every fill proceeds at
/// the deposit it landed.
#[tokio::test]
async fn concurrent_steps_share_one_recovery_top_up() {
    let asserter = alloy::providers::mock::Asserter::new();
    let svc = recovery_service(&asserter, 4_000_000);
    let landed: BoxFuture<'static, TopUpOutcome> = Box::pin(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(TopUpLanded {
            new_deposit: U256::from(10_000_000u64),
            added: U256::from(6_000_000u64),
        })
    });
    *svc.topup_in_flight.lock().unwrap() = Some(InFlightTopUp::new(
        landed.shared(),
        U256::from(6_000_000u64),
        TopUpFunder::Recovery,
    ));
    let steps =
        futures_util::future::join_all((0..4).map(|_| svc.recover_pool(U256::from(4_000_000u64))))
            .await;
    for stepped in steps {
        assert_eq!(
            stepped.expect("no chain call"),
            Recovery::ToppedUp(U256::from(10_000_000u64))
        );
    }
}

/// Concurrent fills that wait on a recovery top-up that reverts `PoolNotOpen`
/// join the replacement open: every fill reports the new pool, none a local
/// fault.
#[tokio::test]
async fn fills_waiting_on_a_top_up_into_a_closing_pool_join_its_replacement() {
    let asserter = alloy::providers::mock::Asserter::new();
    let svc = recovery_service(&asserter, 4_000_000);
    let closed = PoolId::from([0x5A; 32]);
    let opened = PoolId::from([0x6B; 32]);
    let reverted: BoxFuture<'static, TopUpOutcome> = Box::pin(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Err(Arc::new(
            anyhow::anyhow!("topUp reverted").context(PoolNotOpen),
        ))
    });
    *svc.topup_in_flight.lock().unwrap() = Some(InFlightTopUp::new(
        reverted.shared(),
        U256::from(6_000_000u64),
        TopUpFunder::Recovery,
    ));
    // The replacement open in flight records the new pool as the current one.
    let store = Arc::clone(&svc.store);
    let replacement: BoxFuture<'static, OpenOutcome> = Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        store
            .record(&BuyerPoolState::new(
                opened,
                DEPLOYMENT,
                Address::repeat_byte(1),
                Address::repeat_byte(2),
                U256::from(10_000_000u64),
            ))
            .map_err(|err| Arc::new(anyhow::Error::new(err)))
    });
    *svc.open_in_flight.lock().unwrap() = Some(replacement.shared());
    let steps =
        futures_util::future::join_all((0..3).map(|_| svc.recover_pool(U256::from(4_000_000u64))))
            .await;
    for stepped in steps {
        assert_eq!(
            stepped.expect("every fill joins the replacement"),
            Recovery::Replaced(PoolReplaced { closed, opened })
        );
    }
}

/// The reclaim sweep drops the row of a pool another caller already
/// reclaimed: nothing is left to reclaim, and a surviving row would send every
/// later fill to a pool that nodes answer `NotFound`. An open pool keeps its
/// row.
#[tokio::test]
async fn the_reclaim_sweep_drops_a_pool_someone_else_reclaimed() {
    use alloy::sol_types::SolValue;

    for (status, deadline, kept) in [
        (PaymentPool::Status::Closed, 1_000, false),
        (PaymentPool::Status::Open, 0, true),
    ] {
        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_success(
            &PaymentPool::Pool {
                owner: Address::repeat_byte(1),
                status,
                disputeDeadline: deadline,
                deposit: 10_000_000,
                totalRedeemed: 0,
            }
            .abi_encode(),
        );
        let svc = recovery_service(&asserter, 10_000_000);
        svc.sweep_reclaimable_once().await;
        assert_eq!(
            svc.store.get_by_owner(svc.owner).unwrap().is_some(),
            kept,
            "a pool read {}",
            if kept { "open" } else { "closed" }
        );
        assert!(asserter.read_q().is_empty(), "the sweep read the pool");
    }
}

/// A pool that already holds its working deposit and still accepts funds
/// settles: the upstreams have not seen the deposit yet, so the step returns
/// it and the fill's settle window asks them again. Only a gas estimate runs.
#[tokio::test]
async fn a_full_open_pool_settles_without_a_top_up() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&alloy::primitives::U64::from(50_000u64));
    let svc = recovery_service(&asserter, 10_000_000);
    let stepped = svc
        .recover_pool(U256::from(10_000_000u64))
        .await
        .expect("the estimate answers");
    assert_eq!(stepped, Recovery::ToppedUp(U256::from(10_000_000u64)));
    assert!(asserter.read_q().is_empty(), "the estimate ran");
}

/// A pool that already holds its working deposit but is closing or closed
/// still needs a replacement: the `topUp` estimate reverts `PoolNotOpen`, and
/// the step goes on to open a new pool.
#[tokio::test]
async fn a_full_closing_pool_is_replaced() {
    let selector = alloy::primitives::keccak256("PoolNotOpen()");
    let revert = format!("0x{}", alloy::primitives::hex::encode(&selector[..4]));
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_failure(
        serde_json::from_value::<alloy_json_rpc::ErrorPayload>(serde_json::json!({
            "code": 3,
            "message": "execution reverted",
            "data": revert,
        }))
        .unwrap(),
    );
    let svc = recovery_service(&asserter, 10_000_000);
    // The replacement open the step joins records the new pool as current.
    let opened = PoolId::from([0x6B; 32]);
    let store = Arc::clone(&svc.store);
    let replacement: BoxFuture<'static, OpenOutcome> = Box::pin(async move {
        store
            .record(&BuyerPoolState::new(
                opened,
                DEPLOYMENT,
                Address::repeat_byte(1),
                Address::repeat_byte(2),
                U256::from(10_000_000u64),
            ))
            .map_err(|err| Arc::new(anyhow::Error::new(err)))
    });
    *svc.open_in_flight.lock().unwrap() = Some(replacement.shared());
    let stepped = svc
        .recover_pool(U256::from(10_000_000u64))
        .await
        .expect("the step joins the replacement open");
    assert_eq!(
        stepped,
        Recovery::Replaced(PoolReplaced {
            closed: PoolId::from([0x5A; 32]),
            opened,
        })
    );
    assert!(asserter.read_q().is_empty(), "the estimate ran");
}

/// A sweep `reclaim` whose receipt cannot be read may still mine, so the
/// warning names its tx hash and the failure is metered (#2413).
#[tokio::test]
async fn an_unreadable_sweep_reclaim_receipt_logs_the_tx() -> anyhow::Result<()> {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let pool_id = PoolId::from([0xAA; 32]);
    let hash = alloy::primitives::B256::repeat_byte(0xab);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store.record(&BuyerPoolState::new(
        pool_id,
        DEPLOYMENT,
        owner,
        Address::repeat_byte(2),
        U256::from(10_000_000u64),
    ))?;

    // Filler-free, so `send()` is the one `eth_sendTransaction`: `getPool`
    // reads a `Closing` pool whose dispute deadline has passed, the chain-head
    // read faults (the sweep attempts the reclaim anyway), the send returns
    // `hash`, and the empty queue then faults the receipt read.
    let mut closing = onchain_pool(owner, PaymentPool::Status::Closing, 10_000_000);
    closing.disputeDeadline = 1;
    let asserter = Asserter::new();
    asserter.push_success(&alloy::primitives::Bytes::from(closing.abi_encode()));
    asserter.push_failure_msg("transient rpc fault");
    asserter.push_success(&hash);
    let contract = PaymentPool::new(
        Address::ZERO,
        ProviderBuilder::default().connect_mocked_client(asserter),
    );

    let metrics = metrics();
    let log = CapturedLog::default();
    let sink = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    {
        let _guard = tracing::subscriber::set_default(subscriber);
        reclaim_once(&contract, &store, owner, &metrics).await;
    }

    let text = String::from_utf8(
        log.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )?;
    assert!(text.contains("reclaim receipt failed"), "{text}");
    assert!(text.contains(&format!("{hash:#x}")), "{text}");
    let exported = metrics.encode()?;
    assert!(
        exported
            .lines()
            .any(|l| l == "decdn_buyer_reclaim_failures_total 1"),
        "{exported}"
    );
    // The reclaim may still mine, so the row stays for the next sweep.
    assert!(store.get_by_owner(owner)?.is_some(), "the row survives");
    Ok(())
}

// ---- unconfirmed `openPool` (#2415) ------------------------------------

/// A filler-free `PaymentPool` at [`DEPLOYMENT`]'s address whose RPC calls
/// `asserter` answers in order, so an `openPool` send is one
/// `eth_sendTransaction`.
fn bare_pool_contract(
    asserter: alloy::providers::mock::Asserter,
) -> PaymentPool::PaymentPoolInstance<impl Provider + Clone + 'static> {
    PaymentPool::new(
        Address::ZERO,
        alloy::providers::ProviderBuilder::default().connect_mocked_client(asserter),
    )
}

/// One scripted RPC answer.
enum Reply {
    /// `eth_getTransactionReceipt` finds nothing.
    NoReceipt,
    /// `eth_getTransactionReceipt` finds this receipt.
    Receipt(Box<alloy::rpc::types::TransactionReceipt>),
    /// `eth_blockNumber` answers this block.
    Block(u64),
    /// `eth_getTransactionCount` answers this nonce.
    Nonce(u64),
    /// `eth_sendTransaction` answers this hash.
    Sent(TxHash),
    /// An `eth_call` answers this payload.
    Call(alloy::primitives::Bytes),
    /// The call faults, as a transient RPC error does.
    Fault,
}

/// Push `replies` onto `asserter`, in order.
fn push(asserter: &alloy::providers::mock::Asserter, replies: Vec<Reply>) {
    for reply in replies {
        match reply {
            Reply::NoReceipt => asserter.push_success(&serde_json::Value::Null),
            Reply::Receipt(receipt) => asserter.push_success(&receipt),
            Reply::Block(number) | Reply::Nonce(number) => {
                asserter.push_success(&U256::from(number));
            }
            Reply::Sent(tx) => asserter.push_success(&tx),
            Reply::Call(bytes) => asserter.push_success(&bytes),
            Reply::Fault => asserter.push_failure_msg("transient rpc fault"),
        }
    }
}

/// An [`Asserter`](alloy::providers::mock::Asserter) that answers `replies` in
/// order.
fn script(replies: Vec<Reply>) -> alloy::providers::mock::Asserter {
    let asserter = alloy::providers::mock::Asserter::new();
    push(&asserter, replies);
    asserter
}

/// The hash of the `openPool` whose outcome the open task did not see.
fn open_tx() -> TxHash {
    TxHash::repeat_byte(0xAB)
}

/// The hash of the `openPool` the open task re-sends at the same nonce.
fn resent_tx() -> TxHash {
    TxHash::repeat_byte(0xCD)
}

/// A mined `openPool` receipt for `tx` whose `PoolOpened` log credits
/// `deposit` to `pool_id`, with the given execution `status`.
fn opened_receipt(
    tx: TxHash,
    pool_id: PoolId,
    owner: Address,
    deposit: u64,
    status: bool,
) -> Box<alloy::rpc::types::TransactionReceipt> {
    use alloy::sol_types::SolEvent;

    let event = PaymentPool::PoolOpened {
        poolId: pool_id,
        owner,
        deposit: U256::from(deposit),
    };
    let log = alloy::rpc::types::Log {
        inner: alloy::primitives::Log {
            address: Address::ZERO,
            data: event.encode_log_data(),
        },
        block_hash: None,
        block_number: None,
        block_timestamp: None,
        transaction_hash: None,
        transaction_index: None,
        log_index: None,
        removed: false,
    };
    Box::new(alloy::rpc::types::TransactionReceipt {
        inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
            receipt: alloy::consensus::Receipt {
                status: alloy::consensus::Eip658Value::Eip658(status),
                cumulative_gas_used: 0,
                logs: if status { vec![log] } else { Vec::new() },
            },
            logs_bloom: alloy::primitives::Bloom::ZERO,
        }),
        transaction_hash: tx,
        transaction_index: Some(0),
        block_hash: None,
        block_number: None,
        gas_used: 0,
        effective_gas_price: 0,
        blob_gas_used: None,
        blob_gas_price: None,
        from: owner,
        to: None,
        contract_address: None,
    })
}

/// The open a test's open task sends.
fn open_request(owner: Address) -> OpenRequest {
    OpenRequest {
        signer: signer(),
        deployment: DEPLOYMENT,
        token: Address::repeat_byte(2),
        owner,
        deposit: U256::from(10_000_000u64),
    }
}

/// The error `open_pool` returns for `unconfirmed`.
fn unconfirmed_error(unconfirmed: OpenUnconfirmed) -> anyhow::Error {
    anyhow::anyhow!("await openPool receipt")
        .context(unconfirmed)
        .context(PoolOpenFailureReason::RpcError)
}

/// The open whose receipt read failed, sent with nonce 5.
fn hashed() -> OpenUnconfirmed {
    OpenUnconfirmed {
        tx: Some(open_tx()),
        nonce: 5,
    }
}

/// The pool an open task records, or `None` when reconciliation adopted one.
fn opened_pool(landed: Landed) -> Option<Box<OpenedPool>> {
    match landed {
        Landed::Opened(opened) => Some(opened),
        Landed::Adopted => None,
    }
}

/// The current value of `decdn_buyer_pool_open_unresolved`.
fn open_unresolved(metrics: &Arc<Metrics>) -> u64 {
    let text = metrics.encode().expect("encode metrics");
    text.lines()
        .find_map(|l| {
            l.strip_prefix("decdn_buyer_pool_open_unresolved")?
                .strip_prefix(' ')?
                .parse::<u64>()
                .ok()
        })
        .expect("the gauge is exported")
}

/// Settle `unconfirmed` against `replies`, asserting every reply was read and
/// the hold is released.
async fn settle(
    replies: Vec<Reply>,
    unconfirmed: OpenUnconfirmed,
    store: &Arc<dyn BuyerPoolStore>,
    owner: Address,
    metrics: &Arc<Metrics>,
) -> Result<Landed> {
    let asserter = script(replies);
    let contract = bare_pool_contract(asserter.clone());
    let hold = OpenHold::new();
    let settled = settle_unconfirmed_open(
        &contract,
        store,
        &open_request(owner),
        unconfirmed,
        unconfirmed_error(unconfirmed),
        &hold,
        metrics,
    )
    .await;
    assert!(asserter.read_q().is_empty(), "every scripted read was made");
    assert!(!*hold.held.borrow(), "the hold is released");
    assert_eq!(open_unresolved(metrics), 0, "the gauge is cleared");
    settled
}

/// One chain read classifies an unconfirmed open by its receipts, then the
/// account's nonce at one block, then its pending nonce.
#[tokio::test]
async fn an_unconfirmed_open_resolves_by_receipt_then_nonce() {
    let owner = Address::repeat_byte(1);
    let id = PoolId::from([0xBB; 32]);
    let receipt = |tx| Reply::Receipt(opened_receipt(tx, id, owner, 1, true));
    let one = vec![open_tx()];
    let two = vec![open_tx(), resent_tx()];
    let none = Vec::new();
    let cases = vec![
        ("mined", &one, vec![receipt(open_tx())], "mined"),
        (
            "the re-send mined",
            &two,
            vec![Reply::NoReceipt, receipt(resent_tx())],
            "mined",
        ),
        (
            "pending",
            &one,
            vec![
                Reply::NoReceipt,
                Reply::Block(100),
                Reply::Nonce(5),
                Reply::Nonce(6),
            ],
            "pending",
        ),
        (
            "nonce spent",
            &one,
            vec![
                Reply::NoReceipt,
                Reply::Block(100),
                Reply::Nonce(6),
                Reply::NoReceipt,
            ],
            "spent at 100",
        ),
        (
            "mined between the reads",
            &one,
            vec![
                Reply::NoReceipt,
                Reply::Block(100),
                Reply::Nonce(6),
                receipt(open_tx()),
            ],
            "mined",
        ),
        (
            "vacant",
            &one,
            vec![
                Reply::NoReceipt,
                Reply::Block(100),
                Reply::Nonce(5),
                Reply::Nonce(5),
            ],
            "vacant",
        ),
        (
            "hashless, nonce spent",
            &none,
            vec![Reply::Block(100), Reply::Nonce(6)],
            "spent at 100",
        ),
        (
            "hashless, pending",
            &none,
            vec![Reply::Block(100), Reply::Nonce(5), Reply::Nonce(6)],
            "pending",
        ),
    ];
    for (name, txs, replies, want) in cases {
        let asserter = script(replies);
        let contract = bare_pool_contract(asserter.clone());
        let got = match resolve_unconfirmed_open(&contract, owner, 5, txs)
            .await
            .expect(name)
        {
            OpenResolution::Mined(_) => "mined".to_owned(),
            OpenResolution::NonceSpent { block } => format!("spent at {block}"),
            OpenResolution::Pending => "pending".to_owned(),
            OpenResolution::Vacant => "vacant".to_owned(),
        };
        assert_eq!(got, want, "{name}");
        assert!(asserter.read_q().is_empty(), "{name}: every read was made");
    }
}

/// The issue's check: an open whose receipt read failed holds until its
/// transaction mines, then yields the pool that transaction bought. A read
/// that faults on the way is no evidence either way.
#[tokio::test(start_paused = true)]
async fn an_unconfirmed_open_waits_out_its_pending_tx_and_takes_the_mined_pool() {
    let owner = Address::repeat_byte(1);
    let pool_id = PoolId::from([0xBB; 32]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let landed = settle(
        vec![
            // First tick: still pending.
            Reply::NoReceipt,
            Reply::Block(100),
            Reply::Nonce(5),
            Reply::Nonce(6),
            // Second tick: a read faults.
            Reply::Fault,
            // Third tick: mined.
            Reply::Receipt(opened_receipt(open_tx(), pool_id, owner, 9_000_000, true)),
        ],
        hashed(),
        &store,
        owner,
        &metrics(),
    )
    .await
    .expect("the mined open settles");
    let opened = opened_pool(landed).expect("the receipt names the pool");
    assert_eq!(opened.state.pool_id, pool_id);
    assert_eq!(
        opened.state.deposit,
        U256::from(9_000_000u64),
        "the row records the credited deposit"
    );
    assert_eq!(opened.tx, open_tx());
}

/// A nonce no transaction holds is re-sent at that same nonce, never
/// released: the next open would read a later pending nonce, and then both
/// could mine. A failed re-send waits for the next tick, and the re-send's
/// receipt settles the open.
#[tokio::test(start_paused = true)]
async fn a_vacant_nonce_is_re_sent_at_that_nonce_until_one_open_mines() {
    let owner = Address::repeat_byte(1);
    let pool_id = PoolId::from([0xBB; 32]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let landed = settle(
        vec![
            // First tick: vacant, and the re-send fails.
            Reply::NoReceipt,
            Reply::Block(100),
            Reply::Nonce(5),
            Reply::Nonce(5),
            Reply::Fault,
            // Second tick: vacant, and the re-send goes out.
            Reply::NoReceipt,
            Reply::Block(101),
            Reply::Nonce(5),
            Reply::Nonce(5),
            Reply::Sent(resent_tx()),
            // Third tick: the first open has no receipt; the re-send mined.
            Reply::NoReceipt,
            Reply::Receipt(opened_receipt(
                resent_tx(),
                pool_id,
                owner,
                10_000_000,
                true,
            )),
        ],
        hashed(),
        &store,
        owner,
        &metrics(),
    )
    .await
    .expect("the re-sent open settles");
    let opened = opened_pool(landed).expect("the re-send's receipt names the pool");
    assert_eq!(opened.state.pool_id, pool_id);
    assert_eq!(opened.tx, resent_tx());
}

/// A spent nonce with no receipt in hand asks `getPools` at the block it was
/// read at, and adopts the pool the open bought. A `getPools` fault, and a
/// pool whose read faults, are no answer: the slot stays held, and a settle
/// run never counts as an adoption failure.
#[tokio::test(start_paused = true)]
async fn a_spent_nonce_adopts_the_pool_the_open_bought() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let pool_id = PoolId::from([0xBB; 32]);
    let pool = || {
        Reply::Call(
            onchain_pool(owner, PaymentPool::Status::Open, 10_000_000)
                .abi_encode()
                .into(),
        )
    };
    let spent = || {
        vec![
            Reply::NoReceipt,
            Reply::Block(100),
            Reply::Nonce(6),
            Reply::NoReceipt,
        ]
    };
    let mut replies = spent();
    // `getPools` faults.
    replies.push(Reply::Fault);
    replies.extend(spent());
    // `getPools` lists the pool, and its read faults.
    replies.push(Reply::Call(vec![pool_id].abi_encode().into()));
    replies.push(Reply::Fault);
    replies.extend(spent());
    replies.push(Reply::Call(vec![pool_id].abi_encode().into()));
    replies.push(pool());

    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let metrics = metrics();
    let landed = settle(replies, hashed(), &store, owner, &metrics)
        .await
        .expect("the adoption settles the open");
    assert!(matches!(landed, Landed::Adopted), "{landed:?}");
    assert_eq!(
        store.get_by_owner(owner).unwrap().map(|row| row.pool_id),
        Some(pool_id)
    );
    assert_eq!(adoption_failures(&metrics), 0);
}

/// A replacement open settles through reconciliation too. The store tracks
/// the closed pool it replaces; reconciliation drops that row and adopts the
/// new pool. A faulted read of the closed pool keeps the slot held, because a
/// pool this node owns must not be dropped on one failed read.
#[tokio::test(start_paused = true)]
async fn a_replacement_open_settles_onto_the_new_pool() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let closed = PoolId::from([0xAA; 32]);
    let opened = PoolId::from([0xBB; 32]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    store
        .record(&BuyerPoolState::new(
            closed,
            DEPLOYMENT,
            owner,
            Address::repeat_byte(2),
            U256::from(10_000_000u64),
        ))
        .expect("seed the closed pool's row");
    let ids = || Reply::Call(vec![closed, opened].abi_encode().into());
    let new_pool = || {
        Reply::Call(
            onchain_pool(owner, PaymentPool::Status::Open, 10_000_000)
                .abi_encode()
                .into(),
        )
    };
    let closing = Reply::Call(
        onchain_pool(owner, PaymentPool::Status::Closing, 10_000_000)
            .abi_encode()
            .into(),
    );
    let landed = settle(
        vec![
            // First tick: the walk reads the new pool, and the closed one faults.
            Reply::NoReceipt,
            Reply::Block(100),
            Reply::Nonce(6),
            Reply::NoReceipt,
            ids(),
            new_pool(),
            Reply::Fault,
            // Second tick: the closed pool reads `Closing`.
            Reply::NoReceipt,
            Reply::Block(100),
            Reply::Nonce(6),
            Reply::NoReceipt,
            ids(),
            new_pool(),
            closing,
        ],
        hashed(),
        &store,
        owner,
        &metrics(),
    )
    .await
    .expect("the replacement settles");
    assert!(matches!(landed, Landed::Adopted), "{landed:?}");
    assert_eq!(
        store.get_by_owner(owner).unwrap().map(|row| row.pool_id),
        Some(opened)
    );
}

/// A spent nonce with no pool on chain means the open escrowed nothing. The
/// error is a fresh RPC fault, so the caller's classification is unchanged,
/// and it no longer carries [`OpenUnconfirmed`], because the outcome is known.
#[tokio::test(start_paused = true)]
async fn an_open_that_escrowed_nothing_fails_as_a_resolved_rpc_fault() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let err = settle(
        vec![
            Reply::NoReceipt,
            Reply::Block(100),
            Reply::Nonce(6),
            Reply::NoReceipt,
            Reply::Call(Vec::<PoolId>::new().abi_encode().into()),
        ],
        hashed(),
        &store,
        owner,
        &metrics(),
    )
    .await
    .expect_err("nothing escrowed");
    assert_eq!(
        err.downcast_ref::<PoolOpenFailureReason>(),
        Some(&PoolOpenFailureReason::RpcError),
        "{err:#}"
    );
    assert!(err.downcast_ref::<OpenUnconfirmed>().is_none(), "{err:#}");
    assert!(store.get_by_owner(owner).unwrap().is_none());
}

/// A mined open that reverted escrowed nothing, and says so by its own
/// receipt: the error is a contract revert, and no `getPools` read is made.
#[tokio::test(start_paused = true)]
async fn a_reverted_unconfirmed_open_fails_as_a_contract_revert() {
    let owner = Address::repeat_byte(1);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let err = settle(
        vec![Reply::Receipt(opened_receipt(
            open_tx(),
            PoolId::from([0xBB; 32]),
            owner,
            10_000_000,
            false,
        ))],
        hashed(),
        &store,
        owner,
        &metrics(),
    )
    .await
    .expect_err("the open reverted");
    assert_eq!(
        err.downcast_ref::<PoolOpenFailureReason>(),
        Some(&PoolOpenFailureReason::ContractRevert),
        "{err:#}"
    );
    assert!(store.get_by_owner(owner).unwrap().is_none());
}

/// A pool the open resolved to that cannot be recorded is escrowed and
/// untracked: the open fails as a local fault instead of waiting for a row
/// it cannot write.
#[tokio::test(start_paused = true)]
async fn an_unrecordable_adopted_pool_fails_as_escrowed_but_untracked() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(WriteOnlyFault(
        MemoryBuyerPoolStore::new(),
        SeedAnswer::Fault,
    ));
    let err = settle(
        vec![
            Reply::NoReceipt,
            Reply::Block(100),
            Reply::Nonce(6),
            Reply::NoReceipt,
            Reply::Call(vec![PoolId::from([0xBB; 32])].abi_encode().into()),
            Reply::Call(
                onchain_pool(owner, PaymentPool::Status::Open, 10_000_000)
                    .abi_encode()
                    .into(),
            ),
        ],
        hashed(),
        &store,
        owner,
        &metrics(),
    )
    .await
    .expect_err("the row cannot be written");
    assert!(err.downcast_ref::<LocalPullFault>().is_some(), "{err:#}");
    assert!(
        format!("{err:#}").contains("escrowed but untracked"),
        "{err:#}"
    );
}

/// A tracked pool the chain shows closed, whose row will not drop, leaves
/// reconciliation undecided rather than "nothing adopted": the node must not
/// open a second pool beside a row it could not clear.
#[tokio::test]
async fn a_stale_row_that_will_not_drop_is_undecided() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let tracked = PoolId::from([0xAA; 32]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(ForgetFault(MemoryBuyerPoolStore::new()));
    store
        .record(&BuyerPoolState::new(
            tracked,
            DEPLOYMENT,
            owner,
            Address::repeat_byte(2),
            U256::from(10_000_000u64),
        ))
        .expect("seed the row");
    let contract = mocked_pool_contract(vec![
        vec![tracked].abi_encode().into(),
        onchain_pool(owner, PaymentPool::Status::Closed, 10_000_000)
            .abi_encode()
            .into(),
    ]);
    assert_eq!(
        bootstrap_reconcile(
            &contract,
            DEPLOYMENT,
            &store,
            owner,
            Address::repeat_byte(2),
            &metrics()
        )
        .await,
        Reconciled::Unknown
    );
}

/// While the open's outcome is unknown the open slot stays held, and a miss
/// fails at once as a local fault instead of joining the open or sending a
/// second `openPool` (#2415). Every read past the send faults, so the outcome
/// never resolves.
#[tokio::test(start_paused = true)]
async fn an_unconfirmed_open_holds_the_open_slot_and_misses_fail_fast() {
    let owner = Address::repeat_byte(1);
    let asserter = script(vec![Reply::Nonce(5), Reply::Sent(open_tx())]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let svc = mocked_service_on(bare_pool_contract(asserter.clone()), store, signer(), owner);
    let provider = Address::repeat_byte(3);
    let budget = Duration::from_secs(30);

    let started = tokio::time::Instant::now();
    let first = svc
        .open_or_reuse_pool(provider, budget)
        .await
        .expect_err("the open cannot resolve");
    assert!(
        started.elapsed() < budget,
        "the miss fails when the slot becomes held, not when its budget runs out"
    );
    assert!(asserter.read_q().is_empty(), "the open was sent");

    tokio::time::sleep(Duration::from_mins(10)).await;
    assert!(
        svc.open_in_flight.lock().unwrap().is_some(),
        "the open task still holds the slot"
    );
    assert_eq!(open_unresolved(&svc.metrics), 1);
    let second = svc
        .open_or_reuse_pool(provider, budget)
        .await
        .expect_err("the open cannot resolve");
    for err in [first, second] {
        assert!(err.downcast_ref::<OpenHeld>().is_some(), "{err:#}");
        assert!(err.downcast_ref::<LocalPullFault>().is_some(), "{err:#}");
        assert!(err.downcast_ref::<OpenReported>().is_some(), "{err:#}");
    }
}

/// End to end: an `openPool` submit that fails in transport holds the slot,
/// misses fail fast meanwhile, and once the chain shows the nonce spent the
/// open adopts the pool that appeared, releases the slot, and the store
/// tracks the pool — with no second `openPool` sent.
///
/// The transport failure leaves no hash, so no receipt wait starts, and no
/// background block poller competes for the scripted replies.
#[tokio::test(start_paused = true)]
async fn a_transport_failed_open_adopts_its_pool_once_the_nonce_is_spent() {
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let pool_id = PoolId::from([0xBB; 32]);
    // The nonce read answers; the send then over-calls the queue and fails in
    // transport, which may have broadcast.
    let asserter = script(vec![Reply::Nonce(5)]);
    let store: Arc<dyn BuyerPoolStore> = Arc::new(MemoryBuyerPoolStore::new());
    let svc = mocked_service_on(
        bare_pool_contract(asserter.clone()),
        Arc::clone(&store),
        signer(),
        owner,
    );
    let provider = Address::repeat_byte(3);

    let err = svc
        .open_or_reuse_pool(provider, Duration::from_secs(30))
        .await
        .expect_err("the open is unconfirmed");
    assert!(err.downcast_ref::<OpenHeld>().is_some(), "{err:#}");

    push(
        &asserter,
        vec![
            Reply::Block(100),
            Reply::Nonce(6),
            Reply::Call(vec![pool_id].abi_encode().into()),
            Reply::Call(
                onchain_pool(owner, PaymentPool::Status::Open, 10_000_000)
                    .abi_encode()
                    .into(),
            ),
        ],
    );
    tokio::time::sleep(OPEN_RESOLVE_INTERVAL * 2).await;

    assert!(
        asserter.read_q().is_empty(),
        "the settle loop read the chain"
    );
    assert!(
        svc.open_in_flight.lock().unwrap().is_none(),
        "the open task released the slot"
    );
    assert!(!*svc.open_hold.held.borrow(), "the hold is released");
    assert_eq!(open_unresolved(&svc.metrics), 0);
    assert_eq!(
        store.get_by_owner(owner).unwrap().map(|row| row.pool_id),
        Some(pool_id),
        "the adopted pool is tracked, so the next miss reuses it"
    );
}
