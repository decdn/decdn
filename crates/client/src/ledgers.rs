//! Run-scoped registry of live per-lane voucher ledgers.
//!
//! One `bundle pull` run shares a single [`PoolLedger`] per `(pool_id, signer,
//! provider)` lane across every concurrent entry fetch on that lane, so their
//! voucher cumulatives advance through one monotonic issuer instead of colliding
//! across per-fetch instances. `total_committed` is the pool-wide deposit-spend
//! view every lane's solvency gate subtracts from; `credit_all` propagates a
//! funding recovery step's new deposit to every live lane.

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
    /// The lane's shared pool context (deposit, binding), written by the
    /// funding recovery step via [`LaneLedgers::credit_all`].
    pub ctx: Arc<Mutex<PoolContext>>,
}

/// Registry mapping each lane to its shared [`LaneHandle`] for the run.
#[derive(Default, Debug)]
pub struct LaneLedgers {
    map: Mutex<HashMap<LaneKey, LaneHandle>>,
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

    /// Σ over every registered lane `signer` signs of its committed voucher
    /// amount: what the run signed under `signer`'s capability.
    #[must_use]
    pub(crate) fn committed_by(&self, signer: alloy::primitives::Address) -> U256 {
        let map = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.iter()
            .filter(|(lane, _)| lane.signer == signer)
            .map(|(_, h)| h.ledger.committed().amount)
            .fold(U256::ZERO, U256::saturating_add)
    }

    /// Write `new_deposit` onto every registered lane's pool context so no lane
    /// gates on a stale deposit after a funding recovery step lands.
    ///
    /// The lane contexts are cloned out under the map lock, which is then
    /// released BEFORE any `ctx` lock is taken: the driver holds a lane `ctx`
    /// guard while it calls [`Self::total_committed`], which locks the map, so
    /// locking a `ctx` while still holding the map lock would invert that order
    /// and could deadlock. A poisoned `ctx` is recovered with
    /// [`std::sync::PoisonError::into_inner`], the same as the other methods —
    /// a stale deposit never fails a recovery step.
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

    /// Raise every registered lane's pool context to at least `deposit`: a
    /// refill a lane build made reaches lanes built before it. Never lowers a
    /// deposit a concurrent recovery step raised further. Same lock order as
    /// [`Self::credit_all`].
    pub(crate) fn raise_all(&self, deposit: U256) {
        let ctxs: Vec<Arc<Mutex<PoolContext>>> = {
            let map = self
                .map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.values().map(|h| Arc::clone(&h.ctx)).collect()
        };
        for ctx in ctxs {
            let mut ctx = ctx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ctx.deposit = ctx.deposit.max(deposit);
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
mod tests;
