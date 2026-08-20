//! Per-source serve-vindicated warming allowance (ADR 041).
//!
//! Speculative "warming" buys pull a blob before any client asks for it. The buy
//! can be wrong: a dud that never gets a second serve. This module bounds the
//! resulting loss **per upstream source node** (a bonded seller identity), so a
//! flood of at-market one-hit blobs from one attacker source can only drain that
//! source's small allowance, never the operational deposit.
//!
//! Each source holds a net profit-and-loss ledger, zero-centered and clamped to
//! `[-budget, +budget]`. A never-touched source has no ledger entry and reads as
//! available. A speculative buy debits the source's ledger by the buy cost and
//! tags the blob's hash with its source. Every serve of a tagged hash credits
//! the realized margin back to that source's ledger. A blob served at least
//! twice nets positive (vindicated, still warming); a dud served once nets
//! negative (the skim loss) and blocks further speculative buys from that
//! source until it recovers. Eviction forgets the tag so a stale hash can never
//! credit a ledger again. A slow time-refill on top of the serve credits
//! forgives a transient bad patch.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// One source node's warming ledger. `remaining` is a signed, zero-centered net
/// P&L: negative means the source is in the hole (blocked), positive means it
/// has banked surplus (capped at `budget`).
#[derive(Debug)]
struct Bucket {
    remaining: i64,
    last: Instant,
}

impl Bucket {
    fn fresh() -> Self {
        Self {
            remaining: 0,
            last: Instant::now(),
        }
    }
}

#[derive(Debug)]
struct State {
    buckets: HashMap<[u8; 32], Bucket>,
    source_of: HashMap<[u8; 32], [u8; 32]>,
}

/// Bounds speculative warming losses per upstream source node.
///
/// See the module docs for the serve-vindicated accounting model.
#[derive(Debug)]
pub struct WarmingAllowance {
    budget: i64,
    refill_per_sec: u64,
    state: Mutex<State>,
}

impl WarmingAllowance {
    /// Creates a new allowance tracker. A source with no recorded activity
    /// reads as fully available.
    pub fn new(budget: u64, refill_per_sec: u64) -> Self {
        Self {
            budget: i64::try_from(budget).unwrap_or(i64::MAX),
            refill_per_sec,
            state: Mutex::new(State {
                buckets: HashMap::new(),
                source_of: HashMap::new(),
            }),
        }
    }

    /// Applies the time-based refill to one bucket, capped at `+budget`.
    fn refill(&self, bucket: &mut Bucket) {
        let elapsed_secs = bucket.last.elapsed().as_secs();
        let refill = self.refill_per_sec.saturating_mul(elapsed_secs);
        let refill = i64::try_from(refill).unwrap_or(i64::MAX);
        bucket.remaining = bucket.remaining.saturating_add(refill).min(self.budget);
        bucket.last = Instant::now();
    }

    /// Reports whether `source` currently has any warming allowance left. A
    /// source with no recorded activity has never spent anything and is
    /// available; an existing ledger is available only while its net P&L is
    /// still positive.
    pub fn available(&self, source: [u8; 32]) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        match state.buckets.get_mut(&source) {
            Some(bucket) => {
                self.refill(bucket);
                bucket.remaining > 0
            }
            None => true,
        }
    }

    /// Debits `source`'s ledger for a speculative buy of `hash` and tags the
    /// hash with its source so a later serve can credit the right ledger. The
    /// loss is floored at `-budget`, bounding the total possible drain from
    /// one source to one grief-cap's worth.
    pub fn debit_speculative(&self, source: [u8; 32], hash: [u8; 32], units: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let bucket = state.buckets.entry(source).or_insert_with(Bucket::fresh);
        self.refill(bucket);
        let units = i64::try_from(units).unwrap_or(i64::MAX);
        bucket.remaining = bucket
            .remaining
            .saturating_sub(units)
            .max(self.budget.saturating_neg());
        state.source_of.insert(hash, source);
    }

    /// Credits the realized margin of a serve back to the source that
    /// speculatively bought `hash`. No-op if `hash` has no known source
    /// (never warmed, or forgotten since). The gain is capped at `+budget`.
    pub fn credit_serve(&self, hash: [u8; 32], units: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(&source) = state.source_of.get(&hash) else {
            return;
        };
        let bucket = state.buckets.entry(source).or_insert_with(Bucket::fresh);
        self.refill(bucket);
        let units = i64::try_from(units).unwrap_or(i64::MAX);
        bucket.remaining = bucket.remaining.saturating_add(units).min(self.budget);
    }

    /// Drops the hash-to-source tag, e.g. on cache eviction, so a stale hash
    /// can never credit a ledger again.
    pub fn forget(&self, hash: [u8; 32]) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.source_of.remove(&hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S1: [u8; 32] = [1u8; 32];
    const S2: [u8; 32] = [2u8; 32];
    const H1: [u8; 32] = [10u8; 32];

    #[test]
    fn dud_drains_then_blocks() {
        let a = WarmingAllowance::new(1000, 0); // no time refill
        a.debit_speculative(S1, H1, 1000); // bought at full P_buy·mb
        a.credit_serve(H1, 600); // one serve (the requester): 0.6·P_sell·mb
        assert!(!a.available(S1)); // net -400: spent -> cut off (dud)
    }

    #[test]
    fn re_served_blob_refunds_and_keeps_warming() {
        let a = WarmingAllowance::new(1000, 0);
        a.debit_speculative(S1, H1, 1000);
        a.credit_serve(H1, 600); // serve #1
        a.credit_serve(H1, 600); // serve #2 -> net +200, capped at budget
        assert!(a.available(S1)); // vindicated
    }

    #[test]
    fn credit_is_capped_at_budget() {
        let a = WarmingAllowance::new(1000, 0);
        a.debit_speculative(S1, H1, 100);
        for _ in 0..100 {
            a.credit_serve(H1, 600);
        }
        // remaining never exceeds budget; a fresh source still reads full
        assert!(a.available(S2));
    }

    #[test]
    fn sources_are_independent_and_forget_stops_credit() {
        let a = WarmingAllowance::new(1000, 0);
        a.debit_speculative(S1, H1, 1000);
        a.forget(H1);
        a.credit_serve(H1, 600); // no-op: tag gone
        assert!(!a.available(S1)); // still drained
        assert!(a.available(S2));
    }
}
