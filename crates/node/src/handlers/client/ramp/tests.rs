use super::*;

const CAP: u64 = 128 << 20;

fn pool_with(credit: u64) -> Arc<RampPool> {
    let pool = Arc::new(RampPool::default());
    pool.bank(credit, CAP);
    pool
}

#[test]
fn a_stream_takes_the_whole_pool_and_leaves_it_empty() {
    let pool = pool_with(8 << 20);
    let carry = RampCarry::take(Arc::clone(&pool), CAP);
    assert_eq!(carry.carried(), 8 << 20);
    assert_eq!(pool.credit(), 0);
    let second = RampCarry::take(Arc::clone(&pool), CAP);
    assert_eq!(
        second.carried(),
        0,
        "a concurrent stream finds the pool empty"
    );
}

#[test]
fn a_zero_cap_takes_and_returns_nothing() {
    let pool = pool_with(8 << 20);
    let carry = RampCarry::take(Arc::clone(&pool), 0);
    assert_eq!(carry.carried(), 0);
    carry.return_paid(4 << 20);
    assert_eq!(pool.credit(), 8 << 20);
}

#[test]
fn the_pool_holds_at_most_the_cap() {
    let pool = pool_with(CAP);
    RampCarry::take(Arc::clone(&pool), CAP).return_paid(64 << 20);
    assert_eq!(pool.credit(), CAP);
}

#[test]
fn a_fully_paid_stream_returns_its_carry_plus_its_payment() {
    let pool = pool_with(3 << 20);
    let carry = RampCarry::take(Arc::clone(&pool), CAP);
    assert_eq!(carry.ramp_paid(5 << 20), 8 << 20);
    carry.return_paid(5 << 20);
    assert_eq!(pool.credit(), 8 << 20);
}

/// A stream that ends unpaid forfeits its carry, so a payer that paid once
/// cannot open a full window on stream after stream it abandons.
#[test]
fn an_abandoned_stream_forfeits_its_carry() {
    let pool = pool_with(CAP);
    let mut carry = RampCarry::take(Arc::clone(&pool), CAP);
    assert_eq!(carry.carried(), CAP);
    carry.start_delivery();
    drop(carry);
    assert_eq!(pool.credit(), 0);
    assert_eq!(RampCarry::take(Arc::clone(&pool), CAP).carried(), 0);
}

/// A stream refused before its first byte left nothing unpaid, so its carry
/// goes back to the lane whole.
#[test]
fn a_stream_refused_before_delivery_returns_its_carry() {
    let pool = pool_with(6 << 20);
    drop(RampCarry::take(Arc::clone(&pool), CAP));
    assert_eq!(pool.credit(), 6 << 20);
}

#[test]
fn live_carries_never_hold_more_than_the_lane_paid() {
    let pool = Arc::new(RampPool::default());
    let mut paid_total: u64 = 0;
    let mut live: Vec<(RampCarry, u64)> = Vec::new();
    for round in 0..64_u64 {
        let mut carry = RampCarry::take(Arc::clone(&pool), CAP);
        carry.start_delivery();
        let own = (round % 7 + 1) << 20;
        paid_total = paid_total.saturating_add(own);
        live.push((carry, own));
        if round % 3 == 0 {
            let (carry, own) = live.remove(0);
            if round % 2 == 0 {
                carry.return_paid(own);
            }
        }
        let held: u64 = live.iter().map(|(c, own)| c.ramp_paid(*own)).sum();
        assert!(pool.credit().saturating_add(held) <= paid_total);
    }
}
