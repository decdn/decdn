use super::{
    AllowanceShortfall, EscrowUnconfirmed, EscrowUntracked, LOW_WATER_DIVISOR, OpenUnconfirmed,
    OwnedPayer, PaymentPool, TopUpUnconfirmed, UnconfirmedEscrows, approval_floor,
    approve_decision, ensure_allowance, escrowed_but_untracked, grade_deposit_credit,
    issue_self_capability, open_pool, pool_accepts_funds, refill_amount, self_owned_lane_ctx,
    send_open_pool, send_top_up_at, top_up, top_up_unless_ended, topped_up_from_receipt,
};
use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Bytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::test_rpc::{SCRIPTED_NONCE, SCRIPTED_TX, ScriptedRpc};
use decdn_incentive::tx::RECEIPT_TIMEOUT;
use decdn_incentive::{BuyerPoolState, DepositOutcome, MemoryBuyerPoolStore, StoreError};
use decdn_incentive::{SignedCapability, voucher_domain};
use std::sync::Arc;

const CHAIN_ID: u64 = 421_614;

fn domain() -> Eip712Domain {
    voucher_domain(CHAIN_ID, Address::repeat_byte(0xCC))
}

// ---- `open_pool` / `top_up` signatures -------------------------------

/// Compile-time signature check that `open_pool` takes only `deposit` on the
/// value axis (no provider, no `voucher_signer`): naming the monomorphized
/// generic fn item as a value forces the compiler to check its parameter
/// list. `open_pool` submits a real `openPool` tx and decodes the mined
/// receipt, so end-to-end coverage lives in the anvil e2e.
#[test]
fn open_pool_signature_takes_deposit_only() {
    let _ = open_pool::<alloy::providers::RootProvider>;
}

/// Compile-time signature check that `top_up` takes `(contract, owner,
/// pool_id, additional)` — no store, no provider — mirroring the pool
/// contract's own `topUp(poolId, additionalDeposit)` sent by its owner.
#[test]
fn top_up_signature_takes_pool_id_and_amount() {
    let _ = top_up::<alloy::providers::RootProvider>;
}

/// A `PaymentPool` on a filler-free provider whose RPC calls are answered
/// in order by `asserter`; a call past the queue fails in transport.
fn mocked_pool(
    asserter: alloy::providers::mock::Asserter,
) -> PaymentPool::PaymentPoolInstance<impl Provider + Clone> {
    PaymentPool::new(
        Address::repeat_byte(0x01),
        alloy::providers::ProviderBuilder::default().connect_mocked_client(asserter),
    )
}

/// A `topUp` submit that fails in transport may have been broadcast, so it
/// carries [`TopUpUnconfirmed`] with no hash and the nonce it was sent with,
/// and `escrows` keeps the same entry: a caller that drops the error can still
/// name it.
#[tokio::test]
async fn a_transport_failed_top_up_submit_is_unconfirmed_with_its_nonce() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    let escrows = UnconfirmedEscrows::default();
    let err = top_up(
        &mocked_pool(asserter),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
        U256::from(1_000u64),
        Some(&escrows),
        None,
    )
    .await
    .unwrap_err();
    let unconfirmed = TopUpUnconfirmed { tx: None, nonce: 5 };
    assert_eq!(err.downcast_ref::<TopUpUnconfirmed>(), Some(&unconfirmed));
    assert!(format!("{err:#}").contains("nonce 5"), "{err:#}");
    assert_eq!(escrows.top_ups(), vec![unconfirmed]);
}

/// Run [`top_up`] against a mocked pool whose RPC calls `asserter` answers,
/// holding it in `escrows`, with a paused clock past the nonce backoff.
async fn top_up_mocked(
    asserter: alloy::providers::mock::Asserter,
    escrows: &UnconfirmedEscrows,
) -> anyhow::Result<(super::ToppedUpPool, super::UnrecordedEscrow)> {
    top_up(
        &mocked_pool(asserter),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
        U256::from(1_000u64),
        Some(escrows),
        None,
    )
    .await
}

/// A submit rejected as a nonce collision broadcast nothing, so its entry is
/// released before the re-send: after one collision at nonce 5 and a
/// broadcast at nonce 6 whose receipt read fails, only nonce 6 is held.
#[tokio::test(start_paused = true)]
async fn a_nonce_collision_releases_its_entry_before_the_re_send() {
    let hash = B256::repeat_byte(0xab);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    asserter.push_failure_msg("nonce too low");
    asserter.push_success(&U256::from(6u64));
    asserter.push_success(&hash);
    let escrows = UnconfirmedEscrows::default();
    let err = top_up_mocked(asserter.clone(), &escrows).await.unwrap_err();
    let unconfirmed = TopUpUnconfirmed {
        tx: Some(hash),
        nonce: 6,
    };
    assert_eq!(err.downcast_ref::<TopUpUnconfirmed>(), Some(&unconfirmed));
    assert_eq!(
        escrows.pending(),
        vec![EscrowUnconfirmed::TopUp(unconfirmed)]
    );
    assert!(
        asserter.read_q().is_empty(),
        "every scripted answer was read"
    );
}

/// A `topUp` whose every send collides runs out of re-sends: nothing was
/// broadcast, so the error carries no [`TopUpUnconfirmed`] and `escrows`
/// keeps no entry.
#[tokio::test(start_paused = true)]
async fn a_top_up_out_of_nonce_retries_holds_nothing() {
    let asserter = alloy::providers::mock::Asserter::new();
    for nonce in 5u64..9 {
        asserter.push_success(&U256::from(nonce));
        asserter.push_failure_msg("nonce too low");
    }
    let escrows = UnconfirmedEscrows::default();
    let err = top_up_mocked(asserter.clone(), &escrows).await.unwrap_err();
    assert!(err.downcast_ref::<TopUpUnconfirmed>().is_none(), "{err:#}");
    assert_eq!(escrows.pending(), vec![]);
    assert!(asserter.read_q().is_empty(), "four sends were tried");
}

/// #2520: a lane build's `topUp` that lost its nonce sends no re-send once
/// its fetch ends during the backoff. The collision broadcast nothing, so the
/// refusal escrows nothing, and the re-send's nonce read never runs.
#[tokio::test(start_paused = true)]
async fn a_collided_top_up_sends_no_re_send_after_the_fetch_ends() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    asserter.push_failure_msg("nonce too low");
    asserter.push_success(&U256::from(6u64));
    asserter.push_success(&B256::repeat_byte(0xab));
    let escrows = UnconfirmedEscrows::default();
    let ended = tokio_util::sync::CancellationToken::new();
    let end_in_the_backoff = async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        ended.cancel();
    };
    let pool = mocked_pool(asserter.clone());
    let (result, ()) = tokio::join!(
        top_up_unless_ended(
            &pool,
            Address::repeat_byte(0x22),
            B256::repeat_byte(0x33),
            U256::from(1_000u64),
            Some(&escrows),
            None,
            Some(&ended),
        ),
        end_in_the_backoff,
    );
    let err = result.unwrap_err();
    assert!(
        err.downcast_ref::<crate::funding::FetchEnded>().is_some(),
        "{err:#}"
    );
    assert_eq!(escrows.pending(), vec![]);
    assert_eq!(asserter.read_q().len(), 2, "the re-send was never tried");
}

/// Two calls that hold equal entries each release their own: releasing one
/// leaves the other held.
#[test]
fn removing_an_entry_releases_one_of_two_equal_ones() {
    let submitted = TopUpUnconfirmed { tx: None, nonce: 5 };
    let escrows = UnconfirmedEscrows::default();
    escrows.insert(submitted);
    escrows.insert(submitted);
    escrows.remove(submitted);
    assert_eq!(escrows.top_ups(), vec![submitted]);
}

/// Run [`open_pool`] against a mocked pool whose RPC calls `asserter` answers.
async fn open_on(asserter: alloy::providers::mock::Asserter) -> anyhow::Error {
    open_tracked(mocked_pool(asserter), &UnconfirmedEscrows::default(), None)
        .await
        .unwrap_err()
}

/// Run [`open_pool`] on `contract`, holding its broadcast in `opens`, with
/// its receipt wait bounded by `receipt_timeout`.
async fn open_tracked(
    contract: PaymentPool::PaymentPoolInstance<impl Provider + Clone>,
    opens: &UnconfirmedEscrows,
    receipt_timeout: Option<std::time::Duration>,
) -> anyhow::Result<(super::OpenedPool, super::UnrecordedEscrow)> {
    let deployment = decdn_incentive::Deployment {
        chain_id: CHAIN_ID,
        payment_pool: *contract.address(),
    };
    open_pool(
        &contract,
        deployment,
        Address::repeat_byte(0x44),
        Address::repeat_byte(0x22),
        U256::from(1_000u64),
        Some(opens),
        receipt_timeout,
    )
    .await
}

/// A `PaymentPool` whose RPC calls [`ScriptedRpc`] answers.
fn scripted_pool(script: ScriptedRpc) -> PaymentPool::PaymentPoolInstance<impl Provider + Clone> {
    PaymentPool::new(Address::repeat_byte(0x01), script.provider())
}

/// Run [`open_tracked`] on `script` until it parks, then drop it.
async fn drop_parked_open(script: ScriptedRpc) -> UnconfirmedEscrows {
    let opens = UnconfirmedEscrows::default();
    let waited = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        open_tracked(scripted_pool(script), &opens, None),
    )
    .await;
    assert!(waited.is_err(), "the open must still be pending");
    opens
}

/// An `open_pool` dropped inside its receipt wait returns no error, so its
/// broadcast stays in `opens`: the caller can still name the tx (#2418).
#[tokio::test(start_paused = true)]
async fn an_open_dropped_in_its_receipt_wait_stays_unconfirmed() {
    let opens = drop_parked_open(ScriptedRpc::parked_receipt()).await;
    assert_eq!(
        opens.opens(),
        vec![OpenUnconfirmed {
            tx: Some(SCRIPTED_TX),
            nonce: 5
        }]
    );
}

/// An `open_pool` dropped while its submit is in flight may have been
/// broadcast with no hash returned, so `opens` holds its nonce.
#[tokio::test(start_paused = true)]
async fn an_open_dropped_in_its_submit_stays_unconfirmed_by_nonce() {
    let opens = drop_parked_open(ScriptedRpc::parked_send()).await;
    assert_eq!(opens.opens(), vec![OpenUnconfirmed { tx: None, nonce: 5 }]);
}

/// The cause [`decdn_incentive::tx::await_receipt`] gives when no receipt
/// arrives within its bound.
const LAPSE: &str = "no receipt within";

/// An `open_pool` whose `opens` already holds an unconfirmed open refuses to
/// send another, before any chain read: the earlier tx may still mine, and a
/// second open would escrow a second deposit.
#[tokio::test]
async fn a_second_open_is_refused_while_one_is_unconfirmed() {
    let earlier = OpenUnconfirmed {
        tx: Some(SCRIPTED_TX),
        nonce: SCRIPTED_NONCE,
    };
    let opens = UnconfirmedEscrows::default();
    opens.insert(earlier);
    // An empty queue: any chain read would fail in transport.
    let err = open_tracked(
        mocked_pool(alloy::providers::mock::Asserter::new()),
        &opens,
        Some(RECEIPT_TIMEOUT),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.downcast_ref::<OpenUnconfirmed>(),
        Some(&earlier),
        "{err:#}"
    );
    assert!(
        format!("{err:#}").contains("refusing a second openPool"),
        "{err:#}"
    );
    assert_eq!(opens.opens(), vec![earlier]);
}

/// A bounded `open_pool` whose broadcast never mines returns at the bound.
/// The open stays unconfirmed in the error and in `opens`, because the tx may
/// still mine and escrow the deposit (#2433).
#[tokio::test(start_paused = true)]
async fn a_bounded_open_receipt_wait_lapses_unconfirmed() {
    let opens = UnconfirmedEscrows::default();
    let err = tokio::time::timeout(
        RECEIPT_TIMEOUT * 2,
        open_tracked(
            scripted_pool(ScriptedRpc::parked_receipt()),
            &opens,
            Some(RECEIPT_TIMEOUT),
        ),
    )
    .await
    .expect("the receipt wait must lapse within RECEIPT_TIMEOUT")
    .unwrap_err();
    let unconfirmed = OpenUnconfirmed {
        tx: Some(SCRIPTED_TX),
        nonce: SCRIPTED_NONCE,
    };
    assert_eq!(err.downcast_ref::<OpenUnconfirmed>(), Some(&unconfirmed));
    assert_eq!(
        err.downcast_ref::<decdn_incentive::PoolOpenFailureReason>(),
        Some(&decdn_incentive::PoolOpenFailureReason::RpcError),
        "{err:#}"
    );
    assert!(format!("{err:#}").contains(LAPSE), "{err:#}");
    assert_eq!(opens.opens(), vec![unconfirmed]);
}

/// Run [`top_up`] on a pool whose broadcast never mines, bounded by
/// `receipt_timeout`, for at most twice [`RECEIPT_TIMEOUT`].
async fn top_up_parked(
    receipt_timeout: Option<std::time::Duration>,
) -> Result<
    anyhow::Result<(super::ToppedUpPool, super::UnrecordedEscrow)>,
    tokio::time::error::Elapsed,
> {
    tokio::time::timeout(
        RECEIPT_TIMEOUT * 2,
        top_up(
            &scripted_pool(ScriptedRpc::parked_receipt()),
            Address::repeat_byte(0x22),
            B256::repeat_byte(0x33),
            U256::from(1_000u64),
            None,
            receipt_timeout,
        ),
    )
    .await
}

/// A bounded `top_up` whose broadcast never mines returns at the bound with
/// [`TopUpUnconfirmed`] naming the tx and its nonce (#2433).
#[tokio::test(start_paused = true)]
async fn a_bounded_top_up_receipt_wait_lapses_unconfirmed() {
    let err = top_up_parked(Some(RECEIPT_TIMEOUT))
        .await
        .expect("the receipt wait must lapse within RECEIPT_TIMEOUT")
        .unwrap_err();
    assert_eq!(
        err.downcast_ref::<TopUpUnconfirmed>(),
        Some(&TopUpUnconfirmed {
            tx: Some(SCRIPTED_TX),
            nonce: SCRIPTED_NONCE,
        })
    );
    assert!(format!("{err:#}").contains(LAPSE), "{err:#}");
}

/// An unbounded `top_up` keeps waiting: the node relies on it to hold its
/// top-up slot while the `topUp` may still mine.
#[tokio::test(start_paused = true)]
async fn an_unbounded_top_up_receipt_wait_keeps_waiting() {
    assert!(
        top_up_parked(None).await.is_err(),
        "the top-up must still be pending"
    );
}

/// Run [`top_up`] on `script`, holding it in `escrows`, with an unbounded
/// receipt wait.
async fn top_up_tracked(
    script: ScriptedRpc,
    escrows: &UnconfirmedEscrows,
) -> anyhow::Result<(super::ToppedUpPool, super::UnrecordedEscrow)> {
    top_up(
        &scripted_pool(script),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
        U256::from(1_000u64),
        Some(escrows),
        None,
    )
    .await
}

/// Run [`top_up_tracked`] on `script` until it parks, then drop it.
async fn drop_parked_top_up(script: ScriptedRpc) -> UnconfirmedEscrows {
    let escrows = UnconfirmedEscrows::default();
    let waited = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        top_up_tracked(script, &escrows),
    )
    .await;
    assert!(waited.is_err(), "the top-up must still be pending");
    escrows
}

/// A `top_up` dropped inside its receipt wait returns no error, so its
/// broadcast stays in `escrows`: the caller can still name the tx (#2491).
#[tokio::test(start_paused = true)]
async fn a_top_up_dropped_in_its_receipt_wait_stays_unconfirmed() {
    let escrows = drop_parked_top_up(ScriptedRpc::parked_receipt()).await;
    assert_eq!(
        escrows.pending(),
        vec![EscrowUnconfirmed::TopUp(TopUpUnconfirmed {
            tx: Some(SCRIPTED_TX),
            nonce: SCRIPTED_NONCE,
        })]
    );
}

/// A `top_up` dropped while its submit is in flight may have been broadcast
/// with no hash returned, so `escrows` holds its nonce.
#[tokio::test(start_paused = true)]
async fn a_top_up_dropped_in_its_submit_stays_unconfirmed_by_nonce() {
    let escrows = drop_parked_top_up(ScriptedRpc::parked_send()).await;
    assert_eq!(
        escrows.top_ups(),
        vec![TopUpUnconfirmed {
            tx: None,
            nonce: SCRIPTED_NONCE,
        }]
    );
    assert_eq!(escrows.opens(), vec![]);
}

/// A `topUp` the handle holds does not refuse an `openPool`: only an earlier
/// open does.
#[tokio::test(start_paused = true)]
async fn a_held_top_up_does_not_refuse_an_open() {
    let escrows = drop_parked_top_up(ScriptedRpc::parked_receipt()).await;
    let waited = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        open_tracked(scripted_pool(ScriptedRpc::parked_receipt()), &escrows, None),
    )
    .await;
    assert!(
        waited.is_err(),
        "the open must be sent and wait on its receipt"
    );
    assert_eq!(escrows.opens().len(), 1);
    assert_eq!(escrows.top_ups().len(), 1);
}

/// A mined receipt for [`SCRIPTED_TX`] that reverted.
fn reverted_receipt() -> alloy::rpc::types::TransactionReceipt {
    alloy::rpc::types::TransactionReceipt {
        inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
            receipt: alloy::consensus::Receipt {
                status: alloy::consensus::Eip658Value::Eip658(false),
                cumulative_gas_used: 0,
                logs: Vec::new(),
            },
            logs_bloom: alloy::primitives::Bloom::ZERO,
        }),
        transaction_hash: SCRIPTED_TX,
        transaction_index: Some(0),
        block_hash: Some(B256::repeat_byte(0x01)),
        block_number: Some(1),
        gas_used: 0,
        effective_gas_price: 0,
        blob_gas_used: None,
        blob_gas_price: None,
        from: Address::repeat_byte(0x22),
        to: Some(Address::repeat_byte(0x01)),
        contract_address: None,
    }
}

/// A receipt that is read releases the entry, here a mined revert: the open
/// is resolved, and nothing escrowed.
#[tokio::test(start_paused = true)]
async fn a_read_receipt_leaves_no_unconfirmed_open() {
    let receipt = reverted_receipt();
    let opens = UnconfirmedEscrows::default();
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        open_tracked(scripted_pool(ScriptedRpc::mined(receipt)), &opens, None),
    )
    .await
    .expect("the receipt is answered")
    .unwrap_err();
    assert_eq!(
        err.downcast_ref::<decdn_incentive::PoolOpenFailureReason>(),
        Some(&decdn_incentive::PoolOpenFailureReason::ContractRevert),
        "{err:#}"
    );
    assert_eq!(opens.opens(), vec![]);
}

/// A `topUp` receipt that is read releases its entry, here a mined revert:
/// nothing escrowed.
#[tokio::test(start_paused = true)]
async fn a_read_receipt_leaves_no_unconfirmed_top_up() {
    let escrows = UnconfirmedEscrows::default();
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        top_up_tracked(ScriptedRpc::mined(reverted_receipt()), &escrows),
    )
    .await
    .expect("the receipt is answered")
    .unwrap_err();
    assert!(format!("{err:#}").contains("topUp reverted"), "{err:#}");
    assert_eq!(escrows.pending(), vec![]);
}

/// #2515: a mined `topUp` stays pending on the handle until its caller
/// resolves the credit, so a command that ends during the credit still names
/// it. A committed credit releases it.
#[tokio::test(start_paused = true)]
async fn a_mined_top_up_stays_pending_until_its_record_resolves() {
    let pool = Address::repeat_byte(0x01);
    let receipt = top_up_receipt(
        vec![topped_up_log(pool, B256::repeat_byte(0x33), 1_000)],
        true,
    );
    let escrows = UnconfirmedEscrows::default();
    let (topped_up, unrecorded) = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        top_up_tracked(ScriptedRpc::mined(receipt), &escrows),
    )
    .await
    .expect("the receipt is answered")
    .unwrap();
    assert_eq!(topped_up.tx, SCRIPTED_TX);
    let held = TopUpUnconfirmed {
        tx: Some(SCRIPTED_TX),
        nonce: SCRIPTED_NONCE,
    };
    assert_eq!(escrows.top_ups(), vec![held], "the credit is not resolved");
    unrecorded.recorded();
    assert_eq!(escrows.pending(), vec![]);
    assert_eq!(escrows.untracked(), vec![]);
}

/// #2515: a credit that fails holds the escrow untracked and releases its
/// pending entry: the handle names it once.
#[tokio::test(start_paused = true)]
async fn a_failed_record_holds_the_escrow_untracked() {
    let pool = Address::repeat_byte(0x01);
    let receipt = top_up_receipt(
        vec![topped_up_log(pool, B256::repeat_byte(0x33), 1_000)],
        true,
    );
    let escrows = UnconfirmedEscrows::default();
    let (topped_up, unrecorded) = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        top_up_tracked(ScriptedRpc::mined(receipt), &escrows),
    )
    .await
    .expect("the receipt is answered")
    .unwrap();
    let err = unrecorded.untracked(escrowed_but_untracked(
        "pool 0x33 topped up by 1000 µUSDC",
        topped_up.tx,
        "disk full",
    ));
    assert!(err.downcast_ref::<EscrowUntracked>().is_some(), "{err:#}");
    assert_eq!(escrows.pending(), vec![]);
    let held: Vec<_> = escrows
        .untracked()
        .into_iter()
        .map(|held| held.tx)
        .collect();
    assert_eq!(held, [SCRIPTED_TX]);
}

/// An error that names no escrow the handle can hold leaves the transaction
/// pending: releasing it would leave the tx named nowhere.
#[tokio::test(start_paused = true)]
async fn an_unheld_record_error_keeps_the_escrow_pending() {
    let pool = Address::repeat_byte(0x01);
    let receipt = top_up_receipt(
        vec![topped_up_log(pool, B256::repeat_byte(0x33), 1_000)],
        true,
    );
    let escrows = UnconfirmedEscrows::default();
    let (_, unrecorded) = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        top_up_tracked(ScriptedRpc::mined(receipt), &escrows),
    )
    .await
    .expect("the receipt is answered")
    .unwrap();
    let _ = unrecorded.untracked(anyhow::anyhow!("disk full"));
    let held = TopUpUnconfirmed {
        tx: Some(SCRIPTED_TX),
        nonce: SCRIPTED_NONCE,
    };
    assert_eq!(escrows.top_ups(), vec![held]);
    assert_eq!(escrows.untracked(), vec![]);
}

/// #2515: a mined `openPool` whose receipt holds no `PoolOpened` event
/// escrowed a deposit no record can name. `open_pool` holds it untracked on
/// the handle before it releases the pending entry, so the handle names it
/// at every moment.
#[tokio::test(start_paused = true)]
async fn a_mined_open_with_no_pool_opened_event_is_held_untracked() {
    let escrows = UnconfirmedEscrows::default();
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        open_tracked(
            scripted_pool(ScriptedRpc::mined(top_up_receipt(Vec::new(), true))),
            &escrows,
            None,
        ),
    )
    .await
    .expect("the receipt is answered")
    .unwrap_err();
    assert!(err.downcast_ref::<EscrowUntracked>().is_some(), "{err:#}");
    assert_eq!(escrows.pending(), vec![]);
    let held: Vec<_> = escrows
        .untracked()
        .into_iter()
        .map(|held| held.tx)
        .collect();
    assert_eq!(held, [SCRIPTED_TX]);
}

/// A receipt read that fails leaves the open unconfirmed in `opens` as well as
/// in the error: a caller that drops the error can still name the tx.
#[tokio::test]
async fn an_unreadable_receipt_keeps_the_open_unconfirmed() {
    let hash = B256::repeat_byte(0xab);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    asserter.push_success(&hash);
    let opens = UnconfirmedEscrows::default();
    let err = open_tracked(mocked_pool(asserter), &opens, None)
        .await
        .unwrap_err();
    let unconfirmed = OpenUnconfirmed {
        tx: Some(hash),
        nonce: 5,
    };
    assert_eq!(err.downcast_ref::<OpenUnconfirmed>(), Some(&unconfirmed));
    assert_eq!(opens.opens(), vec![unconfirmed]);
}

/// A submit that fails in transport may have been broadcast, so `opens` keeps
/// its nonce, as the error does.
#[tokio::test]
async fn a_transport_failed_submit_stays_unconfirmed_by_nonce() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    let opens = UnconfirmedEscrows::default();
    let err = open_tracked(mocked_pool(asserter), &opens, None)
        .await
        .unwrap_err();
    let unconfirmed = OpenUnconfirmed { tx: None, nonce: 5 };
    assert_eq!(err.downcast_ref::<OpenUnconfirmed>(), Some(&unconfirmed));
    assert_eq!(opens.opens(), vec![unconfirmed]);
}

/// An `openPool` whose receipt cannot be read may still escrow its deposit, so
/// the error names its tx hash and nonce and is classified as an RPC fault
/// (#2413, #2415).
#[tokio::test]
async fn an_unreadable_open_pool_receipt_names_the_tx() {
    let hash = B256::repeat_byte(0xab);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    asserter.push_success(&hash);
    let err = open_on(asserter).await;
    assert!(
        format!("{err:#}").contains(&format!("{hash:#x}")),
        "{err:#}"
    );
    assert_eq!(
        err.downcast_ref::<OpenUnconfirmed>(),
        Some(&OpenUnconfirmed {
            tx: Some(hash),
            nonce: 5
        })
    );
    assert_eq!(
        err.downcast_ref::<decdn_incentive::PoolOpenFailureReason>(),
        Some(&decdn_incentive::PoolOpenFailureReason::RpcError)
    );
}

/// An `openPool` submit that fails in transport may have been broadcast, so it
/// carries [`OpenUnconfirmed`] with no hash and the nonce it was sent with.
#[tokio::test]
async fn a_transport_failed_open_pool_submit_is_unconfirmed_with_its_nonce() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    let err = open_on(asserter).await;
    assert_eq!(
        err.downcast_ref::<OpenUnconfirmed>(),
        Some(&OpenUnconfirmed { tx: None, nonce: 5 })
    );
    assert!(format!("{err:#}").contains("may have broadcast"), "{err:#}");
    assert_eq!(
        err.downcast_ref::<decdn_incentive::PoolOpenFailureReason>(),
        Some(&decdn_incentive::PoolOpenFailureReason::RpcError)
    );
}

/// A re-sent `openPool` returns its hash without waiting for a receipt, and
/// a rejected one carries no [`OpenUnconfirmed`]: the caller already holds the
/// nonce it re-sent at.
#[tokio::test]
async fn send_open_pool_returns_the_hash_without_a_receipt_wait() {
    let hash = B256::repeat_byte(0xcd);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&hash);
    asserter.push_failure_msg("nonce too low");
    let contract = mocked_pool(asserter.clone());
    let owner = Address::repeat_byte(0x22);
    let deposit = U256::from(1_000u64);
    assert_eq!(
        send_open_pool(&contract, owner, deposit, 5).await.unwrap(),
        hash
    );
    let err = send_open_pool(&contract, owner, deposit, 5)
        .await
        .unwrap_err();
    assert!(err.downcast_ref::<OpenUnconfirmed>().is_none(), "{err:#}");
    assert!(format!("{err:#}").contains("nonce 5"), "{err:#}");
    assert!(
        asserter.read_q().is_empty(),
        "one send each, no receipt read"
    );
}

/// A re-sent `topUp` returns its hash without waiting for a receipt, and a
/// rejected one carries no [`TopUpUnconfirmed`]: the caller already holds the
/// nonce it re-sent at.
#[tokio::test]
async fn send_top_up_at_returns_the_hash_without_a_receipt_wait() {
    let hash = B256::repeat_byte(0xcd);
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&hash);
    asserter.push_failure_msg("nonce too low");
    let contract = mocked_pool(asserter.clone());
    let owner = Address::repeat_byte(0x22);
    let pool_id = B256::repeat_byte(0xaa);
    let amount = U256::from(1_000u64);
    assert_eq!(
        send_top_up_at(&contract, owner, pool_id, amount, 5)
            .await
            .unwrap(),
        hash
    );
    let err = send_top_up_at(&contract, owner, pool_id, amount, 5)
        .await
        .unwrap_err();
    assert!(err.downcast_ref::<TopUpUnconfirmed>().is_none(), "{err:#}");
    assert!(format!("{err:#}").contains("nonce 5"), "{err:#}");
    assert!(
        asserter.read_q().is_empty(),
        "one send each, no receipt read"
    );
}

/// A mined `topUp` receipt with `logs`, emitted as given, and `status`.
fn top_up_receipt(
    logs: Vec<alloy::rpc::types::Log>,
    status: bool,
) -> alloy::rpc::types::TransactionReceipt {
    alloy::rpc::types::TransactionReceipt {
        inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
            receipt: alloy::consensus::Receipt {
                status: alloy::consensus::Eip658Value::Eip658(status),
                cumulative_gas_used: 0,
                logs,
            },
            logs_bloom: alloy::primitives::Bloom::ZERO,
        }),
        transaction_hash: SCRIPTED_TX,
        transaction_index: Some(0),
        block_hash: Some(B256::repeat_byte(0x01)),
        block_number: Some(1),
        gas_used: 0,
        effective_gas_price: 0,
        blob_gas_used: None,
        blob_gas_price: None,
        from: Address::repeat_byte(0x22),
        to: Some(Address::repeat_byte(0x01)),
        contract_address: None,
    }
}

/// A `PoolToppedUp` log for `pool_id` crediting `credited`, emitted by `address`.
fn topped_up_log(address: Address, pool_id: B256, credited: u64) -> alloy::rpc::types::Log {
    use alloy::sol_types::SolEvent;

    alloy::rpc::types::Log {
        inner: alloy::primitives::Log {
            address,
            data: PaymentPool::PoolToppedUp {
                poolId: pool_id,
                additionalDeposit: U256::from(credited),
                newDeposit: U256::from(credited),
            }
            .encode_log_data(),
        },
        ..Default::default()
    }
}

/// A `topUp` receipt credits the amount the pool's own `PoolToppedUp` names,
/// ignores a `PoolToppedUp` some other contract emitted, falls back to the
/// requested amount without one, and fails on a revert.
#[test]
fn topped_up_from_receipt_reads_the_pools_own_event() {
    let pool = Address::repeat_byte(0x01);
    let pool_id = B256::repeat_byte(0xaa);
    let requested = U256::from(1_000u64);
    let credit = |logs, status| {
        topped_up_from_receipt(&top_up_receipt(logs, status), pool, pool_id, requested)
    };

    let forged = topped_up_log(Address::repeat_byte(0x99), pool_id, 5_000);
    let own = topped_up_log(pool, pool_id, 900);
    let landed = credit(vec![forged.clone(), own], true).unwrap();
    assert_eq!(landed.credited, U256::from(900u64));
    assert_eq!(landed.tx, SCRIPTED_TX);

    let fallback = credit(vec![forged], true).unwrap();
    assert_eq!(fallback.credited, requested, "no event of the pool's own");

    let err = credit(Vec::new(), false).unwrap_err();
    assert!(format!("{err:#}").contains("reverted"), "{err:#}");
}

/// An `openPool` submit the RPC node rejects broadcast nothing, so it carries
/// no [`OpenUnconfirmed`] and the next open may go ahead.
#[tokio::test]
async fn a_rejected_open_pool_submit_is_not_unconfirmed() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    asserter.push_failure_msg("insufficient funds for gas");
    let opens = UnconfirmedEscrows::default();
    let err = open_tracked(mocked_pool(asserter), &opens, None)
        .await
        .unwrap_err();
    assert!(err.downcast_ref::<OpenUnconfirmed>().is_none(), "{err:#}");
    assert_eq!(opens.opens(), vec![]);
}

/// A `topUp` submit the RPC node rejects broadcast nothing, so it carries
/// no [`TopUpUnconfirmed`], `escrows` keeps no entry, and a caller may retry it.
#[tokio::test]
async fn a_rejected_top_up_submit_is_not_unconfirmed() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    asserter.push_failure_msg("insufficient funds for gas");
    let escrows = UnconfirmedEscrows::default();
    let err = top_up(
        &mocked_pool(asserter),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
        U256::from(1_000u64),
        Some(&escrows),
        None,
    )
    .await
    .unwrap_err();
    assert!(err.downcast_ref::<TopUpUnconfirmed>().is_none(), "{err:#}");
    assert_eq!(escrows.pending(), vec![]);
}

// ---- self-owned capability (#966 / ADR 003 §Capability delegation) ---

/// A data dir's payer: a random owner key, and the lane key derived from it
/// and `store`'s salt.
fn store_payer(store: &MemoryBuyerPoolStore) -> anyhow::Result<OwnedPayer> {
    OwnedPayer::for_store(Arc::new(PrivateKeySigner::random()), store)
}

/// The self capability recovers to the OWNER, which the contract checks
/// against `pool.owner`, and authorizes the LANE key, which signs the
/// vouchers (#2468).
#[test]
fn issue_self_capability_recovers_to_owner() -> anyhow::Result<()> {
    let payer = store_payer(&MemoryBuyerPoolStore::new())?;
    let pool_id = B256::repeat_byte(0x11);
    let cap: SignedCapability =
        issue_self_capability(&payer, pool_id, 1_000_000u64, u64::MAX, &domain())?;
    assert_eq!(cap.recover_owner(&domain())?, payer.owner().address());
    assert_eq!(
        cap.capability.signer,
        payer.lane().address(),
        "the capability authorizes the lane key"
    );
    assert_ne!(payer.lane().address(), payer.owner().address());
    assert_eq!(cap.capability.pool_id, pool_id);
    Ok(())
}

/// A sole payer's one key owns the pool and signs its lanes: the node's
/// upstream buyer.
#[test]
fn sole_payer_signs_with_the_owner_key() -> anyhow::Result<()> {
    let payer = OwnedPayer::sole(Arc::new(PrivateKeySigner::random()));
    assert_eq!(payer.lane().address(), payer.owner().address());
    let cap = issue_self_capability(&payer, B256::ZERO, 1, u64::MAX, &domain())?;
    assert_eq!(cap.capability.signer, payer.owner().address());
    Ok(())
}

/// One store derives one lane key for an owner, on every call: a data dir
/// resumes its lanes across runs. Another store derives another key, so two
/// data dirs on one wallet sign on disjoint lanes (#2468).
#[test]
fn for_store_is_stable_per_store_and_differs_across_stores() -> anyhow::Result<()> {
    let owner = Arc::new(PrivateKeySigner::random());
    let a = MemoryBuyerPoolStore::new();
    let b = MemoryBuyerPoolStore::new();
    let lane = |store: &MemoryBuyerPoolStore| -> anyhow::Result<Address> {
        Ok(OwnedPayer::for_store(Arc::clone(&owner), store)?
            .lane()
            .address())
    };
    assert_eq!(lane(&a)?, lane(&a)?);
    assert_ne!(lane(&a)?, lane(&b)?);
    Ok(())
}

/// A self-owned lane signs its vouchers with the lane key and carries a
/// capability for that key.
#[test]
fn self_owned_lane_ctx_signs_with_the_lane_key() -> anyhow::Result<()> {
    let payer = store_payer(&MemoryBuyerPoolStore::new())?;
    let state = BuyerPoolState::new(
        B256::repeat_byte(0x11),
        decdn_incentive::Deployment {
            chain_id: CHAIN_ID,
            payment_pool: Address::repeat_byte(0x01),
        },
        payer.owner().address(),
        Address::repeat_byte(0x44),
        U256::from(1_000u64),
    );
    let provider = Address::repeat_byte(0xaa);
    let ctx = self_owned_lane_ctx(&state, &payer, &domain(), provider, U256::ZERO, U256::ZERO)?;
    assert_eq!(ctx.client_signer.address(), payer.lane().address());
    let cap = ctx
        .capability
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("the lane context carries no capability"))?;
    assert_eq!(cap.capability.signer, payer.lane().address());
    Ok(())
}

/// Regenerating the capability from the same key + params is deterministic:
/// EIP-712 signing over a fixed digest is stable, so a per-connection
/// regeneration (never stored) yields byte-identical signatures.
#[test]
fn regenerated_capability_is_deterministic_for_same_params() -> anyhow::Result<()> {
    let payer = store_payer(&MemoryBuyerPoolStore::new())?;
    let pool_id = B256::repeat_byte(0x22);
    let a = issue_self_capability(&payer, pool_id, 5_000u64, 1_900_000_000, &domain())?;
    let b = issue_self_capability(&payer, pool_id, 5_000u64, 1_900_000_000, &domain())?;
    assert_eq!(
        a.signature, b.signature,
        "regenerated signatures must match"
    );
    Ok(())
}

// ---- allowance decision -------------

/// The `approve` note that a transport failure may have broadcast the tx.
const APPROVE_MAYBE_BROADCAST: &str = "may have reached the node and broadcast the transaction";

/// Run [`ensure_allowance`] on a filler-free mocked provider whose RPC calls
/// `asserter` answers in order, and return its error.
async fn approve_on(asserter: alloy::providers::mock::Asserter) -> anyhow::Error {
    ensure_allowance(
        &alloy::providers::ProviderBuilder::default().connect_mocked_client(asserter),
        Address::repeat_byte(0x44),
        Address::repeat_byte(0x22),
        Address::repeat_byte(0x01),
        None,
    )
    .await
    .unwrap_err()
}

/// An `approve` submit that fails in transport may have been broadcast with no
/// hash returned, so the error warns before the operator re-runs (#2416).
#[tokio::test]
async fn a_transport_failed_approve_submit_warns_it_may_have_broadcast() {
    let asserter = alloy::providers::mock::Asserter::new();
    // An ABI-encoded zero allowance, so `approve` is sent; the send itself
    // finds no reply.
    asserter.push_success(&B256::ZERO);
    let msg = format!("{:#}", approve_on(asserter).await);
    assert!(msg.contains("submit USDC approve"), "{msg}");
    assert!(msg.contains(APPROVE_MAYBE_BROADCAST), "{msg}");
    assert!(msg.contains("eth_sendTransaction"), "{msg}");
}

/// An `approve` whose receipt never arrives ends at [`RECEIPT_TIMEOUT`], naming
/// its tx, so a stuck approve cannot wedge the node's bootstrap or a CLI run
/// (#1109, #2433).
#[tokio::test(start_paused = true)]
async fn an_approve_that_never_mines_lapses_naming_the_tx() {
    // An ABI-encoded zero allowance, so `approve` is sent.
    let rpc = ScriptedRpc::parked_receipt().answering_calls([Bytes::from(B256::ZERO.to_vec())]);
    let err = tokio::time::timeout(
        RECEIPT_TIMEOUT * 2,
        ensure_allowance(
            &rpc.provider(),
            Address::repeat_byte(0x44),
            Address::repeat_byte(0x22),
            Address::repeat_byte(0x01),
            None,
        ),
    )
    .await
    .expect("the receipt wait must lapse within RECEIPT_TIMEOUT")
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains(&format!("{SCRIPTED_TX:#x}")), "{msg}");
    assert!(msg.contains("may still mine later"), "{msg}");
    assert!(msg.contains(LAPSE), "{msg}");
}

/// An `approve` submit the RPC node rejects broadcast nothing, so it carries no
/// broadcast warning.
#[tokio::test]
async fn a_rejected_approve_submit_does_not_warn() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&B256::ZERO);
    asserter.push_failure_msg("insufficient funds for gas");
    let msg = format!("{:#}", approve_on(asserter).await);
    assert!(msg.contains("submit USDC approve"), "{msg}");
    assert!(!msg.contains(APPROVE_MAYBE_BROADCAST), "{msg}");
}

#[test]
fn unlimited_zero_allowance_approves_max() {
    assert_eq!(approve_decision(U256::ZERO, None), Some(U256::MAX));
}

#[test]
fn unlimited_sufficient_skips() {
    assert_eq!(approve_decision(approval_floor(), None), None);
    assert_eq!(approve_decision(U256::MAX, None), None);
}

#[test]
fn unlimited_below_floor_reapproves_max() {
    let below = approval_floor() - U256::from(1);
    assert_eq!(approve_decision(below, None), Some(U256::MAX));
}

#[test]
fn exact_zero_allowance_approves_deposit() {
    let deposit = U256::from(10_000_000u64);
    assert_eq!(approve_decision(U256::ZERO, Some(deposit)), Some(deposit));
}

#[test]
fn exact_equal_allowance_skips() {
    let deposit = U256::from(10_000_000u64);
    assert_eq!(approve_decision(deposit, Some(deposit)), None);
}

#[test]
fn exact_below_deposit_reapproves() {
    let deposit = U256::from(10_000_000u64);
    let current = deposit - U256::from(1);
    assert_eq!(approve_decision(current, Some(deposit)), Some(deposit));
}

#[test]
fn exact_no_downgrade_from_unlimited() {
    let deposit = U256::from(10_000_000u64);
    assert_eq!(approve_decision(U256::MAX, Some(deposit)), None);
}

// ---- auto-refill decision (#1103, #1146) ----------------------------

fn target() -> U256 {
    U256::from(10_000_000u64) // 10 USDC
}
fn low_water() -> U256 {
    target() / U256::from(LOW_WATER_DIVISOR) // 2 USDC (20%)
}

#[test]
fn refill_amount_no_top_up_when_remaining_at_or_above_low_water() {
    assert_eq!(
        refill_amount(target(), U256::ZERO, target(), low_water()),
        U256::ZERO,
        "a full pool must not be topped up"
    );
    let prior = target() - low_water();
    assert_eq!(
        refill_amount(target(), prior, target(), low_water()),
        U256::ZERO,
        "remaining exactly at the low-water mark is still sufficient"
    );
}

#[test]
fn refill_amount_restores_to_target_when_low() {
    let remaining = low_water() - U256::from(1u64);
    let prior = target() - remaining;
    assert_eq!(
        refill_amount(target(), prior, target(), low_water()),
        target() - remaining,
        "refill must restore the remaining deposit back up to the target"
    );
    let prior_drained = target() - U256::from(1u64);
    assert_eq!(
        refill_amount(target(), prior_drained, target(), low_water()),
        target() - U256::from(1u64),
    );
}

#[test]
fn refill_amount_has_hysteresis_after_a_prior_top_up() {
    let deposit = target() * U256::from(2u64);
    let prior = deposit - low_water();
    assert_eq!(
        refill_amount(deposit, prior, target(), low_water()),
        U256::ZERO,
        "a topped-up pool with headroom must not refill on every reuse"
    );
}

#[test]
fn refill_amount_saturates_and_never_underflows() {
    assert_eq!(
        refill_amount(target(), target() * U256::from(3u64), target(), low_water()),
        target(),
        "remaining saturates to zero, so refill is a full target"
    );
    let deposit = target() * U256::from(2u64);
    assert_eq!(
        refill_amount(deposit, U256::ZERO, target(), deposit),
        U256::ZERO,
        "remaining already >= target yields no top-up even below low-water"
    );
}

#[test]
fn refill_targets_working_deposit_not_initial_open_size() {
    let initial = U256::from(500_000u64);
    let working = U256::from(10_000_000u64);
    let deposit = initial;
    let prior = U256::from(460_000u64); // remaining = 40_000
    let low_water = working / U256::from(LOW_WATER_DIVISOR);
    let add = refill_amount(deposit, prior, working, low_water);
    assert_eq!(add, working - (deposit - prior));
}

/// Pins the wire between `top_up`'s error tagging and the node's
/// `top_up_recovering_allowance` downcast: the marker is attached with
/// `.context(AllowanceShortfall)` on top of an already-wrapped
/// `anyhow::Error` (the `submit topUp` context over the concrete submit
/// error), exactly as `top_up` does. `anyhow::Error::downcast_ref` searches
/// the context chain, so a context-attached marker is still recoverable —
/// without this the whole daemon approve-and-retry path is silently dead.
#[test]
fn allowance_shortfall_context_is_downcastable_through_the_wrapping() {
    // Stand-in for the concrete `alloy` submit error `top_up` wraps first.
    #[derive(Debug)]
    struct SubmitError;
    impl std::fmt::Display for SubmitError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "rpc submit failed")
        }
    }
    impl std::error::Error for SubmitError {}

    let submit_err = anyhow::Error::new(SubmitError).context("submit topUp");
    let tagged = submit_err.context(AllowanceShortfall);
    assert!(
        tagged.downcast_ref::<AllowanceShortfall>().is_some(),
        "the daemon retry gate downcasts on this marker; it must survive the \
         `.context()` wrapping `top_up` applies"
    );
}

// ---- escrowed-but-untracked grading -----------------------------------
//
// The hazard these guard is asymmetric: the on-chain half already
// committed, so the only remaining lever is the exit code. A caller that
// logs and returns success is indistinguishable from one that did the
// work, and `if decdn pool top-up …; then mark_funded; fi` then records
// money as tracked that nobody will reconcile.

fn a_tx() -> TxHash {
    TxHash::from([0xab; 32])
}

#[test]
fn escrowed_but_untracked_names_the_tx_and_the_action() {
    let err = escrowed_but_untracked("pool 0x01 topped up by 5 µUSDC", a_tx(), "disk full");
    let msg = format!("{err:#}");
    assert!(msg.contains("pool 0x01 topped up by 5 µUSDC"), "{msg}");
    assert!(
        msg.contains(&format!("{}", a_tx())),
        "the tx is the handle an operator reconciles against: {msg}"
    );
    assert!(msg.contains("reconcile against the tx"), "{msg}");
    assert!(
        msg.contains("a retry escrows again"),
        "re-running is the obvious response to a non-zero exit and the one that \
         double-spends: {msg}"
    );
    assert!(msg.contains("disk full"), "the cause must survive: {msg}");
}

/// A handle holds every untracked escrow once, oldest first, and nothing
/// else: a second `openPool` that passed its check before the first escrow
/// went untracked can go untracked too, and the command names both (#2511).
#[test]
fn a_handle_holds_each_untracked_escrow_once() {
    let escrows = UnconfirmedEscrows::default();
    let (first, second) = (B256::repeat_byte(0x01), B256::repeat_byte(0x02));
    escrows.hold_untracked(&escrowed_but_untracked(
        "buyer pool opened",
        first,
        "disk full",
    ));
    escrows.hold_untracked(&anyhow::anyhow!("a transient fault"));
    let late = escrowed_but_untracked("buyer pool opened", second, "disk full");
    escrows.hold_untracked(&late.context("building the payment lane failed"));
    escrows.hold_untracked(&escrowed_but_untracked(
        "buyer pool opened",
        first,
        "a later cause",
    ));

    let held = escrows.untracked();
    let txs: Vec<B256> = held.iter().map(|escrow| escrow.tx).collect();
    assert_eq!(txs, [first, second]);
    let texts: Vec<&str> = held.iter().map(|escrow| escrow.text.as_str()).collect();
    assert!(
        texts[0].contains("disk full") && !texts[0].contains("a later cause"),
        "{texts:?}"
    );
    assert!(
        texts[1].contains("building the payment lane failed"),
        "{texts:?}"
    );
    assert_eq!(
        escrows.clone().untracked(),
        held,
        "a clone shares the held escrows"
    );
}

#[test]
fn a_credited_deposit_grades_to_the_new_total() {
    let new_deposit = grade_deposit_credit(
        Ok(DepositOutcome::Added(U256::from(140u64))),
        "topped up",
        a_tx(),
    )
    .expect("Added is the success path");
    assert_eq!(new_deposit, U256::from(140u64));
}

/// `add_deposit` splits its failures across two channels — a backend fault
/// is the `Err`, a committed-row mismatch is a non-`Added` `Ok`. For a
/// caller standing over escrowed USDC they mean the same thing, and the
/// `Ok` half is the one that reads like success at a glance.
#[test]
fn every_uncredited_outcome_is_an_error_naming_the_tx() {
    for (outcome, cause) in [
        (Ok(DepositOutcome::UnknownPool), "UnknownPool"),
        (Ok(DepositOutcome::PoolMismatch), "PoolMismatch"),
        (
            Err(StoreError::Backend("commit (fsync): no space".into())),
            "no space",
        ),
    ] {
        let err = grade_deposit_credit(outcome, "pool 0x01 topped up by 5 µUSDC", a_tx())
            .expect_err("an uncredited deposit must not read as success");
        let msg = format!("{err:#}");
        assert!(msg.contains("escrowed but untracked"), "{msg}");
        assert!(msg.contains(&format!("{}", a_tx())), "{msg}");
        assert!(
            msg.contains("pool 0x01 topped up by 5 µUSDC"),
            "the amount and pool are what an operator reconciles the escrow against: {msg}"
        );
        assert!(
            msg.contains(cause),
            "each channel must keep its own diagnosis, not collapse to one string: {msg}"
        );
    }
}

fn cum(bytes: u64, amount: u64) -> crate::Cumulative {
    crate::Cumulative {
        bytes: U256::from(bytes),
        amount: U256::from(amount),
    }
}

fn lane_progress(bytes: u64, amount: u64) -> decdn_incentive::BuyerLaneProgress {
    decdn_incentive::BuyerLaneProgress {
        last_amount: U256::from(amount),
        last_bytes: U256::from(bytes),
    }
}

/// Progress that neither advanced past its seed nor rebased has nothing to
/// write.
#[test]
fn progress_write_is_none_without_an_advance_or_an_anchor() {
    let progress = crate::VoucherProgress::from_cumulative(cum(500, 50), U256::from(50u64));
    assert_eq!(super::ProgressWrite::of(&progress), None);
}

/// An advance past the seed is a monotone advance to the totals.
#[test]
fn progress_write_advances_past_the_seed() {
    let progress = crate::VoucherProgress::from_cumulative(cum(600, 65), U256::from(50u64));
    assert_eq!(
        super::ProgressWrite::of(&progress),
        Some(super::ProgressWrite::Advance {
            totals: lane_progress(600, 65)
        })
    );
}

/// A pending anchor is written as a rebase even when the totals did not
/// advance: the write exists to move the record down.
#[test]
fn progress_write_rebases_a_pending_anchor_without_an_advance() {
    let progress = crate::VoucherProgress::from_cumulative(cum(500, 50), U256::from(50u64))
        .with_rebase_anchor(Some(cum(400, 40)));
    assert_eq!(
        super::ProgressWrite::of(&progress),
        Some(super::ProgressWrite::Rebase {
            anchor: lane_progress(400, 40),
            totals: lane_progress(500, 50),
        })
    );
}

/// A `topUp` estimate that fails for a reason other than `PoolNotOpen` says
/// nothing about the pool, so the pool's status decides: `Closing` and
/// `Closed` accept no funds, `Open` does. When the status read fails too,
/// the pool reads as open.
#[tokio::test]
async fn an_unreadable_estimate_falls_back_to_the_pools_status() {
    use alloy::providers::ProviderBuilder;
    use alloy::sol_types::SolValue;

    let owner = Address::repeat_byte(1);
    let status = |status: PaymentPool::Status| -> Vec<u8> {
        PaymentPool::Pool {
            owner,
            status,
            disputeDeadline: 0,
            deposit: 10,
            totalRedeemed: 0,
        }
        .abi_encode()
    };
    let cases = [
        ("open", Some(status(PaymentPool::Status::Open)), true),
        ("closing", Some(status(PaymentPool::Status::Closing)), false),
        ("closed", Some(status(PaymentPool::Status::Closed)), false),
        ("unreadable", None, true),
    ];
    for (case, read, accepts) in cases {
        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_failure_msg("transient rpc fault");
        match read {
            Some(answer) => asserter.push_success(&alloy::primitives::Bytes::from(answer)),
            None => asserter.push_failure_msg("transient rpc fault"),
        }
        let contract = PaymentPool::new(
            Address::ZERO,
            ProviderBuilder::new().connect_mocked_client(asserter.clone()),
        );
        assert_eq!(
            pool_accepts_funds(&contract, owner, B256::repeat_byte(2)).await,
            accepts,
            "{case}"
        );
        assert!(asserter.read_q().is_empty(), "{case}: both reads ran");
    }
}
