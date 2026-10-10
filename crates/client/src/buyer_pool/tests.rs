use super::{
    AllowanceShortfall, LOW_WATER_DIVISOR, OpenUnconfirmed, PaymentPool, TopUpUnconfirmed,
    approval_floor, approve_decision, escrowed_but_untracked, grade_deposit_credit,
    issue_self_capability, open_pool, pool_accepts_funds, refill_amount, send_open_pool, top_up,
};
use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::{DepositOutcome, StoreError};
use decdn_incentive::{SignedCapability, voucher_domain};

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
/// carries [`TopUpUnconfirmed`] with no hash and the nonce it was sent with.
#[tokio::test]
async fn a_transport_failed_top_up_submit_is_unconfirmed_with_its_nonce() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    let err = top_up(
        &mocked_pool(asserter),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
        U256::from(1_000u64),
    )
    .await
    .unwrap_err();
    let marker = err.downcast_ref::<TopUpUnconfirmed>().unwrap();
    assert_eq!(marker.tx, None);
    assert_eq!(marker.nonce, 5);
    assert!(format!("{err:#}").contains("nonce 5"), "{err:#}");
}

/// Run [`open_pool`] against a mocked pool whose RPC calls `asserter` answers.
async fn open_on(asserter: alloy::providers::mock::Asserter) -> anyhow::Error {
    let contract = mocked_pool(asserter);
    let deployment = decdn_incentive::Deployment {
        chain_id: CHAIN_ID,
        payment_pool: *contract.address(),
    };
    open_pool(
        &contract,
        std::sync::Arc::new(PrivateKeySigner::random()),
        deployment,
        Address::repeat_byte(0x44),
        Address::repeat_byte(0x22),
        U256::from(1_000u64),
    )
    .await
    .unwrap_err()
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

/// An `openPool` submit the RPC node rejects broadcast nothing, so it carries
/// no [`OpenUnconfirmed`] and the next open may go ahead.
#[tokio::test]
async fn a_rejected_open_pool_submit_is_not_unconfirmed() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    asserter.push_failure_msg("insufficient funds for gas");
    let err = open_on(asserter).await;
    assert!(err.downcast_ref::<OpenUnconfirmed>().is_none(), "{err:#}");
}

/// A `topUp` submit the RPC node rejects broadcast nothing, so it carries
/// no [`TopUpUnconfirmed`] and a caller may retry it.
#[tokio::test]
async fn a_rejected_top_up_submit_is_not_unconfirmed() {
    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_success(&U256::from(5u64));
    asserter.push_failure_msg("insufficient funds for gas");
    let err = top_up(
        &mocked_pool(asserter),
        Address::repeat_byte(0x22),
        B256::repeat_byte(0x33),
        U256::from(1_000u64),
    )
    .await
    .unwrap_err();
    assert!(err.downcast_ref::<TopUpUnconfirmed>().is_none(), "{err:#}");
}

// ---- self-owned capability (#966 / ADR 003 §Capability delegation) ---

/// The capability [`open_pool`] signs must recover to the OWNER — the buyer
/// signs its own key as the delegate, so `recover_owner` returns the buyer's
/// address.
#[test]
fn issue_self_capability_recovers_to_owner() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let pool_id = B256::repeat_byte(0x11);
    let cap: SignedCapability =
        issue_self_capability(&owner, pool_id, 1_000_000u64, u64::MAX, &domain())?;
    assert_eq!(cap.recover_owner(&domain())?, owner.address());
    assert_eq!(
        cap.capability.signer,
        owner.address(),
        "the delegated signer is the owner's own key"
    );
    assert_eq!(cap.capability.pool_id, pool_id);
    Ok(())
}

/// Regenerating the capability from the same key + params is deterministic:
/// EIP-712 signing over a fixed digest is stable, so a per-connection
/// regeneration (never stored) yields byte-identical signatures.
#[test]
fn regenerated_capability_is_deterministic_for_same_params() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let pool_id = B256::repeat_byte(0x22);
    let a = issue_self_capability(&owner, pool_id, 5_000u64, 1_900_000_000, &domain())?;
    let b = issue_self_capability(&owner, pool_id, 5_000u64, 1_900_000_000, &domain())?;
    assert_eq!(
        a.signature, b.signature,
        "regenerated signatures must match"
    );
    Ok(())
}

// ---- allowance decision -------------

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
