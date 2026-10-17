//! Pool owner lookup for `admin_v1_lanes` (#2484).
//!
//! A lane's reported counterparty is its pool's owner (`getPool.owner`), not the
//! lane signer. The owner comes from three places, in this order:
//!
//! 1. The event-fed [`PoolProjection`] the serve gates read. It knows every pool
//!    the settlement watcher saw open and every pool an admission resolved in
//!    this process.
//! 2. An owner cache local to the admin surface. A pool id's owner never
//!    changes: `openPool` sets it once, and no other call writes it. So a cached
//!    owner never goes stale, and the cache keeps every owner it learns.
//! 3. One bounded `getPool` read per pool that is in neither. This covers the
//!    lanes the node carries across a restart: the projection lives in memory
//!    and the watcher resumes at its checkpoint, so it does not see those pools
//!    open again.
//!
//! The read is separate from the serve path's
//! [`ResolvingPoolView`](crate::payment_settlement::ResolvingPoolView) on
//! purpose. That view negative-caches a failed read and then refuses serves on
//! the pool. An operator's `decdn node lanes` call must never cause that.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::payment_pool::PaymentPool;
use futures_util::StreamExt as _;

use crate::chain_events::timed;
use crate::pool_view::PoolProjection;

#[cfg(test)]
mod tests;

/// At most this many owner reads run at once for one `admin_v1_lanes` call.
/// The first call after a restart reads one owner per pool the node carries,
/// so the bound keeps that burst polite to a rate-limited RPC endpoint.
const OWNER_READ_CONCURRENCY: usize = 8;

/// A chain source of pool owners. A trait so [`PoolOwners`] runs against a
/// fake in tests, and so [`crate::admin::AdminState`] stays non-generic over
/// the provider type.
#[async_trait::async_trait]
pub trait PoolOwnerSource: Send + Sync {
    /// The owner of `pool_id`: `Ok(None)` when the deployment has no such pool,
    /// and `Err` when the read fails or times out.
    async fn owner(&self, pool_id: B256) -> anyhow::Result<Option<Address>>;
}

/// [`PoolOwnerSource`] over the node's `PaymentPool` binding: one bounded
/// `getPool` per call.
pub struct ChainPoolOwners<P: Provider + Clone> {
    contract: PaymentPool::PaymentPoolInstance<P>,
}

impl<P: Provider + Clone> std::fmt::Debug for ChainPoolOwners<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainPoolOwners")
            .field("payment_pool", self.contract.address())
            .finish_non_exhaustive()
    }
}

impl<P: Provider + Clone> ChainPoolOwners<P> {
    /// Wrap the node's `PaymentPool` binding.
    pub const fn new(contract: PaymentPool::PaymentPoolInstance<P>) -> Self {
        Self { contract }
    }
}

#[async_trait::async_trait]
impl<P: Provider + Clone + 'static> PoolOwnerSource for ChainPoolOwners<P> {
    async fn owner(&self, pool_id: B256) -> anyhow::Result<Option<Address>> {
        // Bounded: the alloy HTTP provider sets no timeout of its own. A
        // timed-out read reports the owner unknown for this call only.
        let pool = timed(None, "admin getPool", self.contract.getPool(pool_id).call()).await?;
        // `getPool` returns a zeroed struct for an id that was never opened. A
        // reclaimed pool keeps its owner, so its lanes still name it.
        Ok((pool.owner != Address::ZERO).then_some(pool.owner))
    }
}

/// Names the owner of each pool a lane draws from. Cheap to clone: every clone
/// shares one projection, one cache, and one source.
#[derive(Clone)]
pub struct PoolOwners {
    projection: PoolProjection,
    cache: Arc<Mutex<HashMap<B256, Address>>>,
    /// `None` when the node has no chain wiring to read from (tests, and
    /// hand-built admin states). Pools the projection lacks then stay unknown.
    source: Option<Arc<dyn PoolOwnerSource>>,
}

// `LaneStatusHandles` hand-rolls `Debug`, but name the fields that matter here
// too, without formatting the trait object.
impl std::fmt::Debug for PoolOwners {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolOwners")
            .field("projection", &self.projection)
            .field("chain_source", &self.source.is_some())
            .finish_non_exhaustive()
    }
}

impl PoolOwners {
    /// Look owners up in `projection` first, then in the cache, then through
    /// `source`.
    #[must_use]
    pub fn new(projection: PoolProjection, source: Option<Arc<dyn PoolOwnerSource>>) -> Self {
        Self {
            projection,
            cache: Arc::new(Mutex::new(HashMap::new())),
            source,
        }
    }

    /// The owner of each pool in `pool_ids` that can be named. A pool absent
    /// from the result is unknown: no such pool exists, or its read failed.
    /// A failed read is not cached, so the next call reads it again.
    pub async fn resolve(
        &self,
        pool_ids: impl IntoIterator<Item = B256>,
    ) -> HashMap<B256, Address> {
        let mut owners = HashMap::new();
        let mut misses = Vec::new();
        {
            let cache = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for pool_id in pool_ids.into_iter().collect::<HashSet<_>>() {
                let known = self
                    .projection
                    .snapshot(pool_id)
                    .map(|status| status.owner)
                    .or_else(|| cache.get(&pool_id).copied());
                match known {
                    Some(owner) => {
                        owners.insert(pool_id, owner);
                    }
                    None => misses.push(pool_id),
                }
            }
        }
        let Some(source) = self.source.as_ref() else {
            return owners;
        };
        if misses.is_empty() {
            return owners;
        }

        let reads: Vec<_> = futures_util::stream::iter(misses)
            .map(|pool_id| async move { (pool_id, source.owner(pool_id).await) })
            .buffer_unordered(OWNER_READ_CONCURRENCY)
            .collect()
            .await;
        let mut failed = 0usize;
        let mut first_error = None;
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (pool_id, read) in reads {
            match read {
                Ok(Some(owner)) => {
                    cache.insert(pool_id, owner);
                    owners.insert(pool_id, owner);
                }
                Ok(None) => {
                    tracing::debug!(%pool_id, "admin lanes: no such pool on this deployment");
                }
                Err(err) => {
                    failed += 1;
                    first_error.get_or_insert_with(|| (pool_id, sanitize_err_chain(&err)));
                }
            }
        }
        // One line per call, not one per pool: a flaky RPC endpoint fails many
        // reads at once.
        if let Some((pool_id, error)) = first_error {
            tracing::warn!(
                failed,
                first_pool_id = %pool_id,
                %error,
                "admin lanes: getPool failed; reporting those lanes' owners as unknown"
            );
        }
        owners
    }
}
