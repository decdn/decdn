//! The lane ramp-credit pool (ADR 003 §Credit window).
//!
//! A stream's credit window ramps with `paid / credit_ramp_divisor`. The `paid`
//! input is the stream's own confirmed payment plus the credit it carries from
//! its lane. Each lane keeps one [`RampPool`] of paid wire bytes. A new stream
//! takes the whole pool as a [`RampCarry`], so a payer that pulls many ranges
//! back to back keeps its ramp instead of starting each request at the
//! one-chunk floor.
//!
//! Only a stream that ends fully paid returns credit: its carry plus its own
//! confirmed payment, just before its `StreamEnd`. A stream that ends any other
//! way (abandoned, rejected, faulted) forfeits its carry and its own payment.
//! Each paid byte therefore opens credit on at most one stream that leaves bytes
//! unpaid, so the lane's unpaid exposure over its whole life stays at
//! `paid / credit_ramp_divisor` plus one floor for each stream. Two live streams
//! never hold the same paid bytes, because a take empties the pool. The pool
//! holds at most `credit_max × credit_ramp_divisor`, which is the credit that
//! opens one window at the ceiling. The pool lives in memory only: a restart, or
//! a forgotten lane, starts the lane at the floor again.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// One lane's banked ramp credit, in wire bytes the lane's streams paid.
///
/// Atomic, not guarded by the lane's `tokio` mutex, so taking and returning
/// credit never waits on the lane lock.
#[derive(Debug, Default)]
pub(crate) struct RampPool {
    credit: AtomicU64,
}

impl RampPool {
    /// Remove and return all banked credit, at most `cap`.
    fn take(&self, cap: u64) -> u64 {
        let prev = self
            .credit
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                Some(c.saturating_sub(cap))
            })
            .unwrap_or_else(|c| c);
        prev.min(cap)
    }

    /// Add `bytes` of credit, holding the pool at `cap`.
    fn bank(&self, bytes: u64, cap: u64) {
        if bytes == 0 || cap == 0 {
            return;
        }
        let _ = self
            .credit
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                Some(c.saturating_add(bytes).min(cap))
            });
    }

    /// The credit the pool holds now.
    #[cfg(test)]
    pub(crate) fn credit(&self) -> u64 {
        self.credit.load(Ordering::Relaxed)
    }
}

/// The ramp credit one stream took from its lane.
///
/// The stream returns it, with its own confirmed payment, through
/// [`Self::return_paid`] when it ends fully paid. A carry dropped without that
/// call forfeits its credit.
#[derive(Debug)]
pub(crate) struct RampCarry {
    pool: Arc<RampPool>,
    cap: u64,
    carried: u64,
}

impl RampCarry {
    /// Take the whole of `pool`, capped at `cap`, for one new stream. A `cap` of
    /// `0` (a disabled ramp, whose window is already at the ceiling) takes and
    /// returns nothing.
    pub(crate) fn take(pool: Arc<RampPool>, cap: u64) -> Self {
        let carried = pool.take(cap);
        Self { pool, cap, carried }
    }

    /// A carry with no lane behind it: it carries nothing and returns nowhere.
    pub(crate) fn detached() -> Self {
        Self::take(Arc::new(RampPool::default()), 0)
    }

    /// The paid bytes this stream carries from its lane.
    pub(crate) const fn carried(&self) -> u64 {
        self.carried
    }

    /// The ramp input for a stream that confirmed `own_paid` bytes itself.
    pub(crate) const fn ramp_paid(&self, own_paid: u64) -> u64 {
        self.carried.saturating_add(own_paid)
    }

    /// Return `carried + own_paid` to the lane's pool. Call it only when the
    /// stream has delivered and been paid for every byte of its request.
    pub(crate) fn return_paid(self, own_paid: u64) {
        self.pool
            .bank(self.carried.saturating_add(own_paid), self.cap);
    }
}

#[cfg(test)]
mod tests {
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
        let carry = RampCarry::take(Arc::clone(&pool), CAP);
        assert_eq!(carry.carried(), CAP);
        drop(carry);
        assert_eq!(pool.credit(), 0);
        assert_eq!(RampCarry::take(Arc::clone(&pool), CAP).carried(), 0);
    }

    #[test]
    fn live_carries_never_hold_more_than_the_lane_paid() {
        let pool = Arc::new(RampPool::default());
        let mut paid_total: u64 = 0;
        let mut live: Vec<(RampCarry, u64)> = Vec::new();
        for round in 0..64_u64 {
            let carry = RampCarry::take(Arc::clone(&pool), CAP);
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
}
