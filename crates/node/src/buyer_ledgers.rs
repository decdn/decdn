//! The live voucher ledger for each buyer lane's current pool.
//!
//! One [`PoolLedger`] per [`LaneKey`] is a CORRECTNESS requirement, not a
//! cache. `stream_fetch_shared` states the contract it exists to satisfy:
//!
//! > each `stream_fetch`/`stream_fetch_tracked` call seeds its own voucher state from
//! > `ctx.prior_*`, so N concurrent pulls on the same lane all sign the next cumulative
//! > voucher from the same watermark and collide — the node accepts exactly one and
//! > rejects the rest as an amount regression.
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
//! reclaimed at close (see [`crate::buyer_channel`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use decdn_client_pull::{Cumulative, PoolLedger};
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
        // Bound cardinality by dropping this provider's OTHER lanes — but only those no pull
        // still holds, so a stale/rotated key cannot evict a live ledger.
        live.retain(|k, l| k.provider != key.provider || *k == key || Arc::strong_count(l) > 1);
        Arc::clone(
            live.entry(key)
                .or_insert_with(|| Arc::new(PoolLedger::new(seed))),
        )
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
mod tests {
    use super::*;
    use alloy::primitives::{Address, B256, U256};

    fn lane(pool: u8, provider: u8) -> LaneKey {
        LaneKey {
            pool_id: B256::from([pool; 32]),
            signer: Address::from([0x5a; 20]),
            provider: Address::from([provider; 20]),
        }
    }

    fn seed_at(amount: u64) -> Cumulative {
        Cumulative {
            bytes: U256::ZERO,
            amount: U256::from(amount),
        }
    }

    /// The property the whole type exists for: two concurrent pulls to one lane issue through
    /// ONE ledger, so their vouchers are serialized instead of both signing from the same
    /// watermark and colliding.
    #[test]
    fn concurrent_pulls_on_one_lane_share_a_ledger() {
        let ledgers = BuyerLedgers::default();
        let a = ledgers.get_or_seed(lane(9, 1), seed_at(7));
        let b = ledgers.get_or_seed(lane(9, 1), seed_at(7));
        assert!(
            Arc::ptr_eq(&a, &b),
            "both pulls must issue through the same ledger, or they sign the same watermark"
        );
    }

    /// The live ledger outranks the persisted seed. The second pull's `seed` is the store
    /// row, which lags the in-flight pull's issuance — honouring it would rewind the watermark
    /// and re-create the collision.
    #[test]
    fn a_stale_seed_cannot_rewind_a_live_ledger() {
        let ledgers = BuyerLedgers::default();
        let live = ledgers.get_or_seed(lane(9, 1), seed_at(7));
        let rejoined = ledgers.get_or_seed(lane(9, 1), seed_at(7));
        assert!(Arc::ptr_eq(&live, &rejoined));
        assert_eq!(
            rejoined.committed().amount,
            U256::from(7u64),
            "the live ledger's own watermark stands; the seed is ignored on a hit"
        );
    }

    /// A rotated pool gets its own ledger — the old one can sign nothing.
    #[test]
    fn a_rotated_pool_gets_a_fresh_ledger() {
        let ledgers = BuyerLedgers::default();
        let old = ledgers.get_or_seed(lane(9, 1), seed_at(7));
        let new = ledgers.get_or_seed(lane(10, 1), seed_at(0));
        assert!(!Arc::ptr_eq(&old, &new));
        assert_eq!(
            new.committed().amount,
            U256::ZERO,
            "seeded from the new row"
        );
    }

    /// Retiring a lane drops its ledger, so a later reuse re-seeds from the store.
    #[test]
    fn forget_drops_only_the_named_lane() {
        let ledgers = BuyerLedgers::default();
        let first = ledgers.get_or_seed(lane(9, 1), seed_at(7));
        // A concurrent open already rotated us onto pool 10; a late retire of pool 9 must not
        // throw away the live ledger.
        let live = ledgers.get_or_seed(lane(10, 1), seed_at(0));
        ledgers.forget(lane(9, 1));
        let rejoined = ledgers.get_or_seed(lane(10, 1), seed_at(0));
        assert!(Arc::ptr_eq(&live, &rejoined), "pool 10's ledger survives");
        drop(first);

        ledgers.forget(lane(10, 1));
        let fresh = ledgers.get_or_seed(lane(10, 1), seed_at(3));
        assert!(!Arc::ptr_eq(&live, &fresh), "its own retire does drop it");
    }

    /// The bug this [`LaneKey`] keying closes (#1145 review): a call carrying a STALE pool —
    /// an older row a concurrent path read — must not evict the provider's LIVE ledger. The
    /// earlier provider-only key evicted on ANY mismatch, so the next pull on the live lane
    /// got a fresh ledger, re-signed a spent watermark, and wedged the lane.
    #[test]
    fn a_stale_pool_does_not_evict_the_live_ledger() {
        let ledgers = BuyerLedgers::default();
        // The live lane, held by a concurrent pull (so its `Arc` outlives the map entry).
        let live = ledgers.get_or_seed(lane(10, 1), seed_at(5));
        // A late call with a STALE pool: under the old provider-key this evicted pool 10's
        // ledger; keyed by `LaneKey` it just addresses pool 9's own entry.
        let _stale = ledgers.get_or_seed(lane(9, 1), seed_at(0));
        // pool 10's live ledger survives — a subsequent pull on it rejoins the SAME Arc.
        let rejoined = ledgers.get_or_seed(lane(10, 1), seed_at(0));
        assert!(
            Arc::ptr_eq(&live, &rejoined),
            "a stale pool must not evict the live lane's ledger"
        );
        assert_eq!(
            rejoined.committed().amount,
            U256::from(5u64),
            "the live ledger's watermark stands; the seed is ignored on a hit"
        );
    }

    /// Providers do not share a ledger with each other.
    #[test]
    fn distinct_providers_get_distinct_ledgers() {
        let ledgers = BuyerLedgers::default();
        let a = ledgers.get_or_seed(lane(9, 1), seed_at(0));
        let b = ledgers.get_or_seed(lane(9, 2), seed_at(0));
        assert!(!Arc::ptr_eq(&a, &b));
    }
}
