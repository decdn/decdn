use alloy::primitives::{B256, U256};

use super::*;
use crate::source::FakeFunder;

const DEPOSIT: U256 = U256::from_limbs([1_000, 0, 0, 0]);

fn replaced() -> PoolReplaced {
    PoolReplaced {
        closed: B256::repeat_byte(1),
        opened: B256::repeat_byte(2),
    }
}

#[tokio::test]
async fn the_first_step_of_a_fetch_is_always_allowed() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(2_000u64)));
    let stepped = gate
        .step(&funder, DEPOSIT, || DEPOSIT, U256::from(900u64))
        .await;
    assert!(matches!(stepped, Stepped::Raised(d) if d == U256::from(2_000u64)));
    // The funder tops up against the remaining deposit, deposit less spend.
    assert_eq!(funder.calls(), vec![U256::from(100u64)]);
}

#[tokio::test]
async fn a_second_step_without_a_verified_byte_is_refused() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(2_000u64)));
    let _ = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    let raised = U256::from(2_000u64);
    let stepped = gate.step(&funder, raised, || raised, U256::ZERO).await;
    assert!(matches!(stepped, Stepped::NoProgress), "{stepped:?}");
    assert_eq!(funder.calls().len(), 1, "no funding call without progress");
}

#[tokio::test]
async fn a_verified_byte_since_the_last_step_allows_another() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::scripted(vec![
        Recovery::ToppedUp(U256::from(2_000u64)),
        Recovery::ToppedUp(U256::from(3_000u64)),
    ]);
    let _ = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    gate.record_verified(1);
    let raised = U256::from(2_000u64);
    let stepped = gate.step(&funder, raised, || raised, U256::ZERO).await;
    assert!(matches!(stepped, Stepped::Raised(d) if d == U256::from(3_000u64)));
    assert_eq!(funder.calls().len(), 2);
}

#[tokio::test]
async fn a_deposit_a_sibling_already_raised_takes_no_step() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(9_000u64)));
    let now = U256::from(5_000u64);
    let stepped = gate.step(&funder, DEPOSIT, || now, U256::ZERO).await;
    assert!(matches!(stepped, Stepped::Raised(d) if d == now));
    assert!(
        funder.calls().is_empty(),
        "the sibling's step covers this one"
    );
    // The progress rule is untouched: this fetch's first step is still free.
    let stepped = gate.step(&funder, now, || now, U256::ZERO).await;
    assert!(matches!(stepped, Stepped::Raised(_)));
}

#[tokio::test]
async fn a_replaced_pool_stays_replaced_for_every_later_step() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::new(Recovery::Replaced(replaced()));
    let stepped = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    assert!(matches!(stepped, Stepped::Replaced(r) if r == replaced()));
    gate.record_verified(10);
    let stepped = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    assert!(matches!(stepped, Stepped::Replaced(r) if r == replaced()));
    assert_eq!(funder.calls().len(), 1);
}

/// The pass against the new pool steps on the new pool, under the same
/// progress rule: no verified byte since the replacement, no step.
#[tokio::test]
async fn the_next_pass_steps_on_the_new_pool_under_the_same_progress_rule() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::scripted(vec![
        Recovery::Replaced(replaced()),
        Recovery::ToppedUp(U256::from(2_000u64)),
    ]);
    let _ = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    gate.start_next_pass();
    let stepped = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    assert!(matches!(stepped, Stepped::NoProgress), "{stepped:?}");
    gate.record_verified(1);
    let stepped = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    assert!(matches!(stepped, Stepped::Raised(d) if d == U256::from(2_000u64)));
}

#[tokio::test]
async fn a_funder_with_no_way_to_fund_ends_funding_needed() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::new(Recovery::Unavailable);
    let stepped = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    let exhausted = anyhow::Error::new(crate::NoAffordableSource { deposit: DEPOSIT });
    let end = after_step(stepped, exhausted).unwrap_err();
    assert!(end.downcast_ref::<crate::NoAffordableSource>().is_some());
}

#[tokio::test]
async fn no_progress_ends_funding_needed_and_says_why() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(2_000u64)));
    let _ = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    let stepped = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    let exhausted = anyhow::Error::new(crate::NoAffordableSource { deposit: DEPOSIT });
    let end = after_step(stepped, exhausted).unwrap_err();
    assert!(end.downcast_ref::<crate::NoAffordableSource>().is_some());
    assert!(
        format!("{end:#}").contains("no byte was verified"),
        "{end:#}"
    );
}

#[test]
fn a_replacement_ends_the_pass_with_the_new_pool() {
    let exhausted = anyhow::Error::new(crate::NoAffordableSource { deposit: DEPOSIT });
    let end = after_step(Stepped::Replaced(replaced()), exhausted).unwrap_err();
    assert_eq!(end.downcast_ref::<PoolReplaced>(), Some(&replaced()));
    assert!(matches!(
        crate::classify(&end),
        crate::Fault::Fatal(crate::FatalScope::Command)
    ));
}

#[test]
fn a_failed_step_that_may_have_escrowed_stays_fatal() {
    let exhausted = anyhow::Error::new(crate::NoAffordableSource { deposit: DEPOSIT });
    let escrow = anyhow::Error::new(crate::buyer_pool::TopUpUnconfirmed { tx: None, nonce: 7 });
    let end = after_step(Stepped::Failed(escrow), exhausted).unwrap_err();
    assert!(
        end.downcast_ref::<crate::buyer_pool::TopUpUnconfirmed>()
            .is_some()
    );
    assert!(end.downcast_ref::<crate::NoAffordableSource>().is_none());
}

#[test]
fn any_other_failed_step_ends_funding_needed_with_its_cause() {
    let exhausted = anyhow::Error::new(crate::NoAffordableSource { deposit: DEPOSIT });
    let failed = anyhow::anyhow!("rpc down");
    let end = after_step(Stepped::Failed(failed), exhausted).unwrap_err();
    assert!(end.downcast_ref::<crate::NoAffordableSource>().is_some());
    assert!(format!("{end:#}").contains("rpc down"));
}

#[tokio::test(start_paused = true)]
async fn a_top_up_opens_the_settle_window_and_nothing_else_does() {
    let gate = RecoveryGate::with_settle(Duration::from_secs(10));
    assert!(!gate.settling(Instant::now()));
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(2_000u64)));
    let _ = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    assert!(gate.settling(Instant::now()));
    tokio::time::advance(Duration::from_secs(11)).await;
    assert!(!gate.settling(Instant::now()));

    let gate = RecoveryGate::new();
    let funder = FakeFunder::new(Recovery::Unavailable);
    let _ = gate.step(&funder, DEPOSIT, || DEPOSIT, U256::ZERO).await;
    assert!(!gate.settling(Instant::now()));
}
