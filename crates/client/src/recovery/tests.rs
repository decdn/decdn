use alloy::primitives::{B256, U256};

use super::*;
use crate::source::FakeFunder;
use std::assert_matches;

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
    let mut top_ups = 0;
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(2_000u64)));
    let stepped = gate
        .step(
            &funder,
            DEPOSIT,
            &mut top_ups,
            || DEPOSIT,
            U256::from(900u64),
        )
        .await;
    assert_matches!(stepped, Stepped::Raised(d) if d == U256::from(2_000u64));
    // The funder tops up against the remaining deposit, deposit less spend.
    assert_eq!(funder.calls(), vec![U256::from(100u64)]);
}

#[tokio::test]
async fn a_second_step_without_a_verified_byte_is_refused() {
    let gate = RecoveryGate::new();
    let mut top_ups = 0;
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(2_000u64)));
    let _ = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    let raised = U256::from(2_000u64);
    let stepped = gate
        .step(&funder, raised, &mut top_ups, || raised, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::NoProgress, "{stepped:?}");
    assert_eq!(funder.calls().len(), 1, "no funding call without progress");
}

#[tokio::test]
async fn a_verified_byte_since_the_last_step_allows_another() {
    let gate = RecoveryGate::new();
    let mut top_ups = 0;
    let funder = FakeFunder::scripted(vec![
        Recovery::ToppedUp(U256::from(2_000u64)),
        Recovery::ToppedUp(U256::from(3_000u64)),
    ]);
    let _ = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    gate.record_verified(1);
    let raised = U256::from(2_000u64);
    let stepped = gate
        .step(&funder, raised, &mut top_ups, || raised, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::Raised(d) if d == U256::from(3_000u64));
    assert_eq!(funder.calls().len(), 2);
}

#[tokio::test]
async fn a_deposit_a_sibling_already_raised_takes_no_step() {
    let gate = RecoveryGate::new();
    let mut top_ups = 0;
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(9_000u64)));
    let now = U256::from(5_000u64);
    let stepped = gate
        .step(&funder, DEPOSIT, &mut top_ups, || now, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::Raised(d) if d == now);
    assert!(
        funder.calls().is_empty(),
        "the sibling's step covers this one"
    );
    // The progress rule is untouched: this fetch's first step is still free.
    let stepped = gate
        .step(&funder, now, &mut top_ups, || now, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::Raised(_));
}

#[tokio::test]
async fn a_replaced_pool_stays_replaced_for_every_later_step() {
    let gate = RecoveryGate::new();
    let mut top_ups = 0;
    let funder = FakeFunder::new(Recovery::Replaced(replaced()));
    let stepped = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::Replaced(r) if r == replaced());
    gate.record_verified(10);
    let stepped = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::Replaced(r) if r == replaced());
    assert_eq!(funder.calls().len(), 1);
}

/// The pass against the new pool steps on the new pool, under the same
/// progress rule: no verified byte since the replacement, no step.
#[tokio::test]
async fn the_next_pass_steps_on_the_new_pool_under_the_same_progress_rule() {
    let gate = RecoveryGate::new();
    let mut top_ups = 0;
    let funder = FakeFunder::scripted(vec![
        Recovery::Replaced(replaced()),
        Recovery::ToppedUp(U256::from(2_000u64)),
    ]);
    let _ = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    gate.start_next_pass();
    let stepped = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::NoProgress, "{stepped:?}");
    gate.record_verified(1);
    let stepped = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::Raised(d) if d == U256::from(2_000u64));
}

#[tokio::test]
async fn a_funder_with_no_way_to_fund_ends_funding_needed() {
    let gate = RecoveryGate::new();
    let mut top_ups = 0;
    let funder = FakeFunder::new(Recovery::Unavailable);
    let stepped = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    let exhausted = anyhow::Error::new(crate::NoAffordableSource { deposit: DEPOSIT });
    let end = after_step(stepped, exhausted).unwrap_err();
    assert!(end.downcast_ref::<crate::NoAffordableSource>().is_some());
}

#[tokio::test]
async fn no_progress_ends_funding_needed_and_says_why() {
    let gate = RecoveryGate::new();
    let mut top_ups = 0;
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(2_000u64)));
    let _ = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    let raised = U256::from(2_000u64);
    let stepped = gate
        .step(&funder, raised, &mut top_ups, || raised, U256::ZERO)
        .await;
    let exhausted = anyhow::Error::new(crate::NoAffordableSource { deposit: DEPOSIT });
    let end = after_step(stepped, exhausted).unwrap_err();
    assert!(end.downcast_ref::<crate::NoAffordableSource>().is_some());
    assert!(
        format!("{end:#}").contains("no byte was verified"),
        "{end:#}"
    );
}

/// A sibling's step that settles leaves the deposit where it was. A caller
/// that found its set exhausted before that step makes one more pass on it,
/// and takes no step of its own; its next exhaustion with no byte verified
/// ends the fetch.
#[tokio::test]
async fn a_siblings_settle_step_answers_a_caller_that_has_not_seen_it() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::new(Recovery::ToppedUp(DEPOSIT));
    let mut first = gate.top_ups();
    let mut second = gate.top_ups();
    let stepped = gate
        .step(&funder, DEPOSIT, &mut first, || DEPOSIT, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::Raised(d) if d == DEPOSIT, "{stepped:?}");
    let stepped = gate
        .step(&funder, DEPOSIT, &mut second, || DEPOSIT, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::Raised(d) if d == DEPOSIT, "{stepped:?}");
    assert_eq!(
        funder.calls().len(),
        1,
        "the sibling's step covers this one"
    );
    let stepped = gate
        .step(&funder, DEPOSIT, &mut second, || DEPOSIT, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::NoProgress, "{stepped:?}");
}

/// Two callers exhausted at the same deposit step at once: one tops up, and
/// the other takes the raised deposit even when its own view of the deposit
/// has not caught up with the step yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_callers_share_one_top_up() {
    let gate = std::sync::Arc::new(RecoveryGate::new());
    let funder = std::sync::Arc::new(FakeFunder::new(Recovery::ToppedUp(U256::from(2_000u64))));
    let call = |gate: std::sync::Arc<RecoveryGate>, funder: std::sync::Arc<FakeFunder>| {
        tokio::spawn(async move {
            let mut top_ups = gate.top_ups();
            gate.step(&*funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
                .await
        })
    };
    let a = call(std::sync::Arc::clone(&gate), std::sync::Arc::clone(&funder));
    let b = call(std::sync::Arc::clone(&gate), std::sync::Arc::clone(&funder));
    for stepped in [a.await.unwrap(), b.await.unwrap()] {
        assert_matches!(
            stepped,
            Stepped::Raised(d) if d == U256::from(2_000u64),
            "{stepped:?}"
        );
    }
    assert_eq!(funder.calls().len(), 1, "one top-up between them");
}

/// A caller that starts after a sibling's top-up and finds its sources
/// priced out at the deposit before it takes the raised deposit.
#[tokio::test]
async fn a_caller_behind_a_siblings_top_up_takes_the_raised_deposit() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(2_000u64)));
    let mut first = gate.top_ups();
    let _ = gate
        .step(&funder, DEPOSIT, &mut first, || DEPOSIT, U256::ZERO)
        .await;
    let mut late = gate.top_ups();
    let stepped = gate
        .step(&funder, DEPOSIT, &mut late, || U256::ZERO, U256::ZERO)
        .await;
    assert_matches!(
        stepped,
        Stepped::Raised(d) if d == U256::from(2_000u64),
        "{stepped:?}"
    );
    assert_eq!(funder.calls().len(), 1);
}

/// The raised deposit a step reached belongs to the pool it topped up: the
/// pass against a replacement pool does not take it as raised.
#[tokio::test]
async fn the_next_pass_forgets_the_old_pools_deposit() {
    let gate = RecoveryGate::new();
    let funder = FakeFunder::scripted(vec![
        Recovery::ToppedUp(U256::from(2_000u64)),
        Recovery::Replaced(replaced()),
    ]);
    let mut top_ups = gate.top_ups();
    let _ = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    gate.record_verified(1);
    let raised = U256::from(2_000u64);
    let _ = gate
        .step(&funder, raised, &mut top_ups, || raised, U256::ZERO)
        .await;
    gate.start_next_pass();
    let stepped = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    assert_matches!(stepped, Stepped::NoProgress, "{stepped:?}");
}

#[test]
fn a_replacement_ends_the_pass_with_the_new_pool() {
    let exhausted = anyhow::Error::new(crate::NoAffordableSource { deposit: DEPOSIT });
    let end = after_step(Stepped::Replaced(replaced()), exhausted).unwrap_err();
    assert_eq!(end.downcast_ref::<PoolReplaced>(), Some(&replaced()));
    assert_matches!(
        crate::classify(&end),
        crate::Fault::Fatal(crate::FatalScope::Command)
    );
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
    let mut top_ups = 0;
    assert!(!gate.settling(Instant::now()));
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(2_000u64)));
    let _ = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    assert!(gate.settling(Instant::now()));
    tokio::time::advance(Duration::from_secs(11)).await;
    assert!(!gate.settling(Instant::now()));

    let gate = RecoveryGate::new();
    let mut top_ups = 0;
    let funder = FakeFunder::new(Recovery::Unavailable);
    let _ = gate
        .step(&funder, DEPOSIT, &mut top_ups, || DEPOSIT, U256::ZERO)
        .await;
    assert!(!gate.settling(Instant::now()));
}

/// A slot holding a fresh capability for a fresh key, waiting `wait`.
fn slot(wait: Duration) -> crate::CredentialSlot {
    let signer = alloy::signers::local::PrivateKeySigner::random();
    let capability = decdn_incentive::Capability {
        signer: signer.address(),
        spending_cap: 1,
        pool_id: B256::repeat_byte(0x11),
        expiry: 0,
    }
    .sign(
        &signer,
        &decdn_incentive::bind_node_id_domain(1, alloy::primitives::Address::ZERO),
    )
    .expect("sign");
    crate::CredentialSlot::new(std::sync::Arc::new(signer), capability)
        .unwrap()
        .with_swap_wait(wait)
}

/// A swap that came before the step counts as a step under the progress rule:
/// the first is allowed, and a second with no byte verified since is not,
/// even though the slot already holds a newer credential.
#[tokio::test(start_paused = true)]
async fn a_swap_that_came_before_the_step_counts_under_the_progress_rule() {
    let gate = RecoveryGate::new();
    let slot = slot(Duration::ZERO);
    let swap = |slot: &crate::CredentialSlot| {
        let next = self::slot(Duration::ZERO).current();
        slot.swap(next.signer, next.capability).unwrap();
    };
    swap(&slot);
    assert_eq!(gate.swap_step(&slot, 0).await, SwapStep::Swapped);
    swap(&slot);
    assert_eq!(gate.swap_step(&slot, 1).await, SwapStep::NoProgress);
    gate.record_verified(1);
    assert_eq!(gate.swap_step(&slot, 1).await, SwapStep::Swapped);
}

/// Two callers under one credential generation reach the delegated step at
/// once: one waits for the swap, and the other takes the same swap without a
/// step of its own.
#[tokio::test(start_paused = true)]
async fn concurrent_callers_share_one_swap() {
    let gate = std::sync::Arc::new(RecoveryGate::new());
    let slot = slot(Duration::from_mins(1));
    let waits = [0, 1].map(|_| {
        let (gate, slot) = (std::sync::Arc::clone(&gate), slot.clone());
        tokio::spawn(async move { gate.swap_step(&slot, 0).await })
    });
    tokio::time::advance(Duration::from_secs(1)).await;
    let next = self::slot(Duration::ZERO).current();
    slot.swap(next.signer, next.capability).unwrap();
    for wait in waits {
        assert_eq!(wait.await.unwrap(), SwapStep::Swapped);
    }
    // The swap counted as the one step: a pass under it that verifies no
    // byte allows no further step.
    assert_eq!(gate.swap_step(&slot, 1).await, SwapStep::NoProgress);
}

/// With no swap inside the wait, the step times out.
#[tokio::test(start_paused = true)]
async fn no_swap_inside_the_wait_times_out() {
    let gate = RecoveryGate::new();
    let slot = slot(Duration::from_secs(5));
    let started = Instant::now();
    assert_eq!(gate.swap_step(&slot, 0).await, SwapStep::TimedOut);
    assert!(started.elapsed() >= Duration::from_secs(5));
}
