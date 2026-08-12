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
use std::sync::Mutex;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
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
    /// read faulted (callers fail open).
    async fn status(&self, pool_id: B256) -> Option<PoolStatus>;
}

/// [`PoolView`] backed by a live `PaymentPool.getPool` read with a short TTL
/// cache, so a burst of requests against one pool costs at most one RPC per TTL.
pub struct ChainPoolView<P: Provider + Clone + 'static> {
    contract: PaymentPool::PaymentPoolInstance<P>,
    ttl: Duration,
    cache: Mutex<HashMap<B256, (Instant, PoolStatus)>>,
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
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn cached(&self, pool_id: B256) -> Option<PoolStatus> {
        let guard = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .get(&pool_id)
            .and_then(|(at, status)| (at.elapsed() < self.ttl).then_some(*status))
    }

    fn store(&self, pool_id: B256, status: PoolStatus) {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(pool_id, (Instant::now(), status));
    }
}

#[async_trait::async_trait]
impl<P: Provider + Clone + 'static> PoolView for ChainPoolView<P> {
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
            remaining: pool.deposit.saturating_sub(pool.totalRedeemed),
        };
        self.store(pool_id, status);
        Some(status)
    }
}
