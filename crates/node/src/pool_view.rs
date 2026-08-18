//! Cached on-chain `getPool` view for the serve path (ADR 003 §Sizing).
//!
//! The `cdn/client/v1` serve handler holds no RPC client, but two of its gates
//! need a pool-level chain quantity per request:
//!
//! - **Floor-`M` solvency** — the pool's **remaining** balance
//!   (`getPool.deposit − getPool.totalRedeemed`) minus the refundable floor `M`
//!   must still cover the credit window, or the node refuses `InsufficientDeposit`
//!   before signing `ok: true`.
//! - **ADR 011 funder gate** — the pool **owner** (`getPool.owner`) is the funding
//!   address the origin-blacklist gate evaluates, at open time and mid-stream.
//!
//! Both come from one `getPool` read, so this view fetches the tuple once and
//! caches it for a short TTL. A read fault or an unknown pool yields `None`; the
//! callers fail **open** (serve) on `None` — a transient RPC blip must not refuse
//! paying clients, and the first voucher's on-chain `redeem` plus the open-time
//! hash gates still protect revenue and compliance.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use arc_swap::ArcSwap;
use decdn_incentive::payment_pool::PaymentPool;

/// The per-pool chain quantities the serve gates read.
#[derive(Clone, Copy, Debug)]
pub struct PoolStatus {
    /// The pool owner — the ADR 011 funder subject and refund destination.
    pub owner: Address,
    /// `deposit − totalRedeemed`, the balance the floor-`M` guard reserves against.
    pub remaining: U256,
}

/// A per-request source of [`PoolStatus`]. Trait so the handler holds it behind an
/// `Arc<dyn PoolView>` and tests pass a fake (or `None`) without a chain.
#[async_trait::async_trait]
pub trait PoolView: Send + Sync + std::fmt::Debug {
    /// The pool's cached `getPool` status, or `None` if the pool is unknown or the
    /// read faulted (callers fail open). MAY trigger an on-chain fetch on a cache
    /// miss — use only where blocking on an RPC is acceptable (e.g. stream admission).
    async fn status(&self, pool_id: B256) -> Option<PoolStatus>;

    /// A CACHE-ONLY status read: returns a fresh cached [`PoolStatus`] if one is
    /// held, and NEVER triggers an on-chain fetch — `None` when nothing fresh is
    /// cached. The mid-stream serve re-check uses this so a per-voucher-boundary
    /// solvency check never blocks the serve loop on a `getPool` `eth_call`, which on
    /// a slow RPC would stall delivery. The default delegates to [`Self::status`]
    /// for in-memory test doubles; [`ChainPoolView`] overrides it to read only its
    /// TTL cache.
    async fn cached_status(&self, pool_id: B256) -> Option<PoolStatus> {
        self.status(pool_id).await
    }
}

/// [`PoolView`] backed by a live `PaymentPool.getPool` read with a short TTL
/// cache, so a burst of requests against one pool costs at most one RPC per TTL.
pub struct ChainPoolView<P: Provider + Clone + 'static> {
    contract: PaymentPool::PaymentPoolInstance<P>,
    ttl: Duration,
    /// The TTL cache, published as a whole map through [`ArcSwap`] so the
    /// per-voucher-boundary read (`cached`) is a single atomic load — no store,
    /// no read-modify-write — and never contends with a concurrent reader. The
    /// TTL refresh (`store`) is the only writer and clones-then-publishes.
    cache: ArcSwap<HashMap<B256, (Instant, PoolStatus)>>,
}

impl<P: Provider + Clone + 'static> std::fmt::Debug for ChainPoolView<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainPoolView")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<P: Provider + Clone + 'static> ChainPoolView<P> {
    /// Build the view over `payment_pool_addr` using `provider`, caching each
    /// pool's status for `ttl`.
    pub fn new(provider: P, payment_pool_addr: Address, ttl: Duration) -> Self {
        Self {
            contract: PaymentPool::new(payment_pool_addr, provider),
            ttl,
            cache: ArcSwap::from(Arc::new(HashMap::new())),
        }
    }

    fn cached(&self, pool_id: B256) -> Option<PoolStatus> {
        self.cache
            .load()
            .get(&pool_id)
            .and_then(|(at, status)| (at.elapsed() < self.ttl).then_some(*status))
    }

    /// Publish an updated entry. Read-modify-write, because [`ArcSwap`] has no
    /// in-place mutation: clone the current map, insert, and swap the new map in.
    /// Writes are rare (one per pool per TTL) so the clone is irrelevant next to
    /// keeping the per-voucher-boundary read a lock-free atomic load.
    fn store(&self, pool_id: B256, status: PoolStatus) {
        let mut next = HashMap::clone(&self.cache.load());
        next.insert(pool_id, (Instant::now(), status));
        self.cache.store(Arc::new(next));
    }
}

#[async_trait::async_trait]
impl<P: Provider + Clone + 'static> PoolView for ChainPoolView<P> {
    /// Cache-only: return a fresh cached entry, never an on-chain fetch. Keeps the
    /// mid-stream serve re-check off the RPC — a stale/absent entry yields `None`
    /// and the caller fails open.
    async fn cached_status(&self, pool_id: B256) -> Option<PoolStatus> {
        self.cached(pool_id)
    }

    async fn status(&self, pool_id: B256) -> Option<PoolStatus> {
        if let Some(hit) = self.cached(pool_id) {
            return Some(hit);
        }
        let pool = match self.contract.getPool(pool_id).call().await {
            Ok(pool) => pool,
            Err(err) => {
                tracing::debug!(%pool_id, error = %err, "getPool read failed; serve gates fail open");
                return None;
            }
        };
        // A zero owner is the contract's "no such pool" sentinel — the serve
        // gates treat an unknown pool as "not my business" and fail open.
        if pool.owner.is_zero() {
            return None;
        }
        let status = PoolStatus {
            owner: pool.owner,
            remaining: U256::from(pool.deposit.saturating_sub(pool.totalRedeemed)),
        };
        self.store(pool_id, status);
        Some(status)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use alloy::providers::ProviderBuilder;

    use super::*;

    /// A real [`ChainPoolView`] whose provider is never called — the `cached`/`store`
    /// pair under test touch only the arc-swapped map, never the contract. The HTTP
    /// transport is constructed lazily and points at an unroutable address, so any
    /// accidental RPC would fail loudly rather than pass silently.
    fn view(ttl: Duration) -> ChainPoolView<impl Provider + Clone + 'static> {
        let url: reqwest::Url = "http://127.0.0.1:1".parse().expect("static url parses");
        let provider = ProviderBuilder::new().connect_http(url);
        ChainPoolView::new(provider, Address::ZERO, ttl)
    }

    fn status(remaining: u64) -> PoolStatus {
        PoolStatus {
            owner: Address::from([7u8; 20]),
            remaining: U256::from(remaining),
        }
    }

    #[test]
    fn store_then_read_hits() {
        let view = view(Duration::from_mins(1));
        let pool = B256::from([1u8; 32]);
        view.store(pool, status(100));

        let got = view.cached(pool).expect("fresh entry is a hit");
        assert_eq!(got.remaining, U256::from(100u64));
    }

    #[test]
    fn unknown_pool_misses() {
        let view = view(Duration::from_mins(1));
        view.store(B256::from([1u8; 32]), status(100));

        assert!(view.cached(B256::from([2u8; 32])).is_none());
    }

    #[test]
    fn expired_entry_misses() {
        // A zero TTL makes `at.elapsed() < ttl` never hold, so any stored entry
        // reads back as expired — the expiry branch without a real sleep.
        let view = view(Duration::ZERO);
        let pool = B256::from([1u8; 32]);
        view.store(pool, status(100));

        assert!(view.cached(pool).is_none());
    }

    #[test]
    fn store_preserves_other_entries() {
        let view = view(Duration::from_mins(1));
        let a = B256::from([1u8; 32]);
        let b = B256::from([2u8; 32]);
        view.store(a, status(10));
        view.store(b, status(20));

        // The read-modify-write publish must not drop the earlier entry.
        assert_eq!(view.cached(a).unwrap().remaining, U256::from(10u64));
        assert_eq!(view.cached(b).unwrap().remaining, U256::from(20u64));
    }

    #[test]
    fn store_overwrites_same_pool() {
        let view = view(Duration::from_mins(1));
        let pool = B256::from([1u8; 32]);
        view.store(pool, status(10));
        view.store(pool, status(99));

        assert_eq!(view.cached(pool).unwrap().remaining, U256::from(99u64));
    }
}
