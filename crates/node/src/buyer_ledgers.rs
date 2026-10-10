//! The live voucher ledger for each buyer lane's current pool.
//!
//! One [`PoolLedger`] per [`LaneKey`] is a CORRECTNESS requirement, not a
//! cache. The contract it exists to satisfy: a pull that seeds its own voucher
//! state from `ctx.prior_*` shares that watermark with every other pull on the
//! lane, so N concurrent pulls with N ledgers all sign the next cumulative voucher
//! from the same baseline and collide — the node accepts exactly one and rejects
//! the rest as an amount regression. `open_progressive_pull` therefore takes the
//! CHANNEL's `Arc<PoolLedger>`, never a per-pull one.
//!
//! Both node pull paths built a FRESH ledger per pull and so broke that contract (#1145
//! review). It is reachable at the defaults, on an ordinary node doing nothing unusual: the
//! cache engine coalesces in-flight pulls **by hash** (`cache::engine::inflight`), so two
//! misses for *different* blobs run concurrent `NodeOrigin::fetch` calls, and on a
//! tens-of-nodes network both routinely rank the same provider first. Both then take the
//! lane-reuse fast path, read the same prior watermark, and collide.
//!
//! What made that fatal rather than merely wasteful is that an amount regression is a
//! terminal verdict: the losing pull's collision wedges the lane until the pool's deposit is
//! reclaimed at close (see [`crate::buyer_pool`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use decdn_client::{Cumulative, PoolLedger};
use decdn_incentive::LaneKey;

/// The voucher ledger of each lane's current pool, shared by every concurrent pull on it.
///
/// Keyed by [`LaneKey`] (`{pool_id, signer, provider}`) so the ledger a pull issues through
/// is pinned to the exact lane it read: a stale or rotated pool can neither collide with nor
/// evict a different lane's live ledger. Cardinality stays bounded without that hazard — a
/// rotated pool's ledger is dropped by [`Self::get_or_seed`] the moment no pull still holds
/// it (`Arc::strong_count == 1`), so at most one live lane per provider lingers, plus any
/// whose pulls are still in flight. That is the buyer store's bound, made safe against a
/// stale key (#1145 review): the earlier provider-only key evicted the live ledger on ANY
/// pool-id mismatch, which is exactly the collision this type exists to prevent.
#[derive(Debug, Default)]
pub struct BuyerLedgers {
    live: Mutex<HashMap<LaneKey, Arc<PoolLedger>>>,
}

impl BuyerLedgers {
    /// The ledger to issue this pull's vouchers through: the live one for `key`, or a fresh
    /// one seeded from `seed` if there is none.
    ///
    /// `seed` is the lane's persisted cumulative and the only state a fresh ledger inherits.
    /// A hash chain draws its own secret when it opens and is never reopened across a restart
    /// (ADR 003 §Resumption folds), so there is nothing else here for a caller to supply and
    /// nothing for a stale row to contradict.
    ///
    /// An existing entry for the SAME lane wins over `seed`, and that precedence is the
    /// whole point. `seed` comes from the persisted row that `open_or_reuse_pool` read; a
    /// concurrent pull holding the live ledger may already have issued vouchers the row does
    /// not carry yet (it is written on redeem, not on issue). Re-seeding from the row would
    /// rewind the watermark and re-create the collision this type exists to prevent.
    ///
    /// A DIFFERENT lane addresses a DIFFERENT entry — a rotated or stale pool can neither
    /// read nor evict this lane's ledger. The provider's other lanes are pruned here, but
    /// only those no pull still holds (`Arc::strong_count == 1`), so a rotation cannot throw
    /// away a ledger a concurrent pull is issuing through.
    pub fn get_or_seed(&self, key: LaneKey, seed: Cumulative) -> Arc<PoolLedger> {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        // Hot path — the lane already exists (every concurrent miss after the first on it):
        // one map probe, no scan. This is what a well-connected node does on almost every
        // pull, so the O(lanes) prune below must NOT sit on it.
        if let Some(ledger) = live.get(&key) {
            return Arc::clone(ledger);
        }
        // Cold path — a genuinely new lane (first pull to this provider, or a rotation).
        // Only here do we bound cardinality by dropping this provider's OTHER lanes, and
        // only those no pull still holds, so a stale/rotated key cannot evict a live ledger.
        // Running the prune solely on insert keeps the reuse path O(1) while holding the
        // exact same bound: a rotated lane is dropped the next time its provider opens a new
        // one. It must never drop a still-held ledger, because that ledger's in-memory
        // watermark can be ahead of the persisted row (written on redeem, not issue) —
        // re-seeding from the row would rewind it and re-create the collision this type
        // prevents.
        live.retain(|k, l| k.provider != key.provider || Arc::strong_count(l) > 1);
        Arc::clone(
            live.entry(key)
                .or_insert_with(|| Arc::new(PoolLedger::new(seed))),
        )
    }

    /// The pool's total committed spend: the sum, across every live lane whose
    /// [`LaneKey::pool_id`] matches `pool_id`, of that lane's committed voucher
    /// amount.
    ///
    /// This is the whole-pool spend the node's ranged-drive loop gates on
    /// (#1506). One buyer pool backs a voucher lane per provider, and the loop
    /// assembles a blob across those providers as a sequence of lanes drawing on
    /// the SAME deposit. Each lane's live ledger stays in the map for the whole
    /// assembly (a new provider's lane never evicts another provider's), so
    /// summing them here — including the run currently issuing vouchers, which
    /// seeded its own live ledger via [`Self::get_or_seed`] — yields the exact
    /// cumulative spend against the shared deposit. The live ledgers are the
    /// authoritative in-memory watermarks (a persisted row lags, written on
    /// redeem, not on issue), so a solvency gate reading this never under-counts
    /// a still-in-flight voucher.
    pub fn pool_committed(&self, pool_id: decdn_incentive::PoolId) -> alloy::primitives::U256 {
        let live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        live.iter()
            .filter(|(key, _)| key.pool_id == pool_id)
            .fold(alloy::primitives::U256::ZERO, |acc, (_, ledger)| {
                acc.saturating_add(ledger.committed().amount)
            })
    }

    /// Drop `key`'s ledger — the lane was retired, so no further voucher can be signed
    /// against it.
    ///
    /// Keyed by the exact lane, so a concurrent open that already rotated the provider onto a
    /// NEW pool is untouched: its ledger lives under a different key and is not thrown away.
    pub fn forget(&self, key: LaneKey) {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        live.remove(&key);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
