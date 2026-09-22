//! Run-scoped registry of live per-lane voucher ledgers.
//!
//! One `bundle pull` run shares a single [`PoolLedger`] per `(pool_id, signer,
//! provider)` lane across every concurrent entry fetch on that lane, so their
//! voucher cumulatives advance through one monotonic issuer instead of colliding
//! across per-fetch instances. `total_committed` is the pool-wide deposit-spend
//! view every lane's solvency gate subtracts from; `credit_all` propagates a
//! landed reactive top-up's new deposit to every live lane.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy::primitives::U256;
use decdn_incentive::LaneKey;

use crate::PoolContext;
use crate::ledger::PoolLedger;

/// A lane's shared live state: the one voucher ledger and one pool context every
/// concurrent fetch on the lane draws on.
#[derive(Clone, Debug)]
pub struct LaneHandle {
    /// The lane's shared, concurrency-safe voucher issuer.
    pub ledger: Arc<PoolLedger>,
    /// The lane's shared pool context (deposit, binding), written by reactive
    /// top-ups via [`LaneLedgers::credit_all`].
    pub ctx: Arc<Mutex<PoolContext>>,
}

/// Registry mapping each lane to its shared [`LaneHandle`] for the run.
#[derive(Default, Debug)]
pub struct LaneLedgers {
    map: Mutex<HashMap<LaneKey, LaneHandle>>,
    /// Serializes the run's reactive top-ups: every lane draws on one deposit,
    /// so two concurrent top-ups would each escrow the whole shortfall (see
    /// [`crate::SharedPool::topup_lock`]).
    topup_lock: tokio::sync::Mutex<()>,
}

impl LaneLedgers {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The shared handle for `lane`, building and inserting one via `build` only
    /// if the lane is not yet registered. Concurrent callers for one lane all
    /// receive the first-registered handle; the losers' `build` output is dropped.
    pub fn get_or_insert(&self, lane: LaneKey, build: impl FnOnce() -> LaneHandle) -> LaneHandle {
        let mut map = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.entry(lane).or_insert_with(build).clone()
    }

    /// Σ over every registered lane of its committed voucher amount — the pool's
    /// total spend, the basis every lane's remaining-deposit gate subtracts.
    #[must_use]
    pub fn total_committed(&self) -> U256 {
        let map = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.values()
            .map(|h| h.ledger.committed().amount)
            .fold(U256::ZERO, U256::saturating_add)
    }

    /// The lock every lane of the run takes around a reactive top-up, for the
    /// run's [`crate::SharedPool::topup_lock`].
    #[must_use]
    pub const fn topup_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.topup_lock
    }

    /// Write `new_deposit` onto every registered lane's pool context so no lane
    /// gates on a stale deposit after a reactive top-up lands.
    ///
    /// The lane contexts are cloned out under the map lock, which is then
    /// released BEFORE any `ctx` lock is taken: the driver holds a lane `ctx`
    /// guard while it calls [`Self::total_committed`], which locks the map, so
    /// locking a `ctx` while still holding the map lock would invert that order
    /// and could deadlock. A poisoned `ctx` is recovered with
    /// [`std::sync::PoisonError::into_inner`], the same as the other methods —
    /// a stale deposit never fails a top-up.
    pub fn credit_all(&self, new_deposit: U256) {
        let ctxs: Vec<Arc<Mutex<PoolContext>>> = {
            let map = self
                .map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.values().map(|h| Arc::clone(&h.ctx)).collect()
        };
        for ctx in ctxs {
            ctx.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .deposit = new_deposit;
        }
    }
}

#[cfg(test)]
impl LaneHandle {
    /// Build a throwaway handle for unit tests: a [`PoolLedger`] seeded with
    /// `seed` and a minimal [`PoolContext`] with zeroed money fields, a random
    /// signer, and no binding/capability. Only the ledger half is exercised by
    /// most tests; `credit_all` exercises the context half.
    fn for_test(seed: crate::ledger::Cumulative) -> Self {
        use alloy::primitives::{Address, B256};
        use alloy::signers::local::PrivateKeySigner;

        let signer = PrivateKeySigner::random();
        let ctx = PoolContext {
            pool_id: B256::ZERO,
            provider: Address::ZERO,
            deposit: U256::ZERO,
            client_signer: Arc::new(signer),
            voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        };
        Self {
            ledger: Arc::new(PoolLedger::new(seed)),
            ctx: Arc::new(Mutex::new(ctx)),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
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
}
