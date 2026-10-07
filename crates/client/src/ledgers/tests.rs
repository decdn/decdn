use super::*;
use crate::ledger::Cumulative;
use alloy::primitives::{Address, B256, U256};
use decdn_incentive::LaneKey;

fn lane(p: u8) -> LaneKey {
    LaneKey {
        pool_id: B256::ZERO,
        signer: Address::ZERO,
        provider: Address::from([p; 20]),
    }
}

#[test]
fn get_or_insert_is_idempotent_per_lane() {
    let reg = LaneLedgers::new();
    let a = reg.get_or_insert(lane(1), || {
        LaneHandle::for_test(Cumulative {
            bytes: U256::from(10u64),
            amount: U256::from(3u64),
        })
    });
    // A second call for the same lane must return the SAME ledger Arc and never run `build`.
    let b = reg.get_or_insert(lane(1), || {
        panic!("build must not run for an existing lane")
    });
    assert!(Arc::ptr_eq(&a.ledger, &b.ledger));
}

#[test]
fn total_committed_sums_across_lanes() {
    let reg = LaneLedgers::new();
    let h1 = reg.get_or_insert(lane(1), || {
        LaneHandle::for_test(Cumulative {
            bytes: U256::ZERO,
            amount: U256::from(5u64),
        })
    });
    let h2 = reg.get_or_insert(lane(2), || {
        LaneHandle::for_test(Cumulative {
            bytes: U256::ZERO,
            amount: U256::from(7u64),
        })
    });
    // committed() reflects the seed until a voucher is issued.
    assert_eq!(h1.ledger.committed().amount, U256::from(5u64));
    assert_eq!(h2.ledger.committed().amount, U256::from(7u64));
    assert_eq!(reg.total_committed(), U256::from(12u64));
}

#[test]
fn credit_all_writes_deposit_on_every_lane() {
    let reg = LaneLedgers::new();
    let h1 = reg.get_or_insert(lane(1), || LaneHandle::for_test(Cumulative::default()));
    let h2 = reg.get_or_insert(lane(2), || LaneHandle::for_test(Cumulative::default()));
    reg.credit_all(U256::from(42u64));
    for h in [&h1, &h2] {
        assert_eq!(
            h.ctx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .deposit,
            U256::from(42u64),
            "credit_all must write every registered lane's deposit"
        );
    }
}

#[test]
fn raise_all_lifts_every_lane_and_never_lowers_one() {
    let reg = LaneLedgers::new();
    let low = reg.get_or_insert(lane(1), || LaneHandle::for_test(Cumulative::default()));
    let high = reg.get_or_insert(lane(2), || LaneHandle::for_test(Cumulative::default()));
    let deposit = |h: &LaneHandle| {
        h.ctx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .deposit
    };
    let set = |h: &LaneHandle, d: u64| {
        h.ctx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .deposit = U256::from(d);
    };
    set(&low, 100);
    set(&high, 500);
    reg.raise_all(U256::from(300u64));
    assert_eq!(deposit(&low), U256::from(300u64), "raised");
    assert_eq!(deposit(&high), U256::from(500u64), "never lowered");
}

// The lock-order invariant `credit_all` relies on: `total_committed` locks the
// map but never a `ctx`, so it completes even while a caller holds a lane `ctx`
// guard — the opposite order `credit_all` avoids by releasing the map lock
// before touching any `ctx`. If `total_committed` ever locked a `ctx` under the
// map lock this would deadlock against the held guard.
#[test]
fn total_committed_does_not_lock_a_ctx_held_elsewhere() {
    let reg = LaneLedgers::new();
    let handle = reg.get_or_insert(lane(1), || {
        LaneHandle::for_test(Cumulative {
            bytes: U256::ZERO,
            amount: U256::from(9u64),
        })
    });
    // Hold the lane's ctx guard for the duration of the read.
    let _guard = handle
        .ctx
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(reg.total_committed(), U256::from(9u64));
}
