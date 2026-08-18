//! Event-fed on-chain pool view for the serve path (ADR 003 §Sizing).
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
//! Both quantities are fully determined by the `PaymentPool` event log, so the
//! serve path reads them from an in-memory projection the settlement watcher folds
//! from that log ([`crate::payment_settlement`]) — no serve request costs a
//! `getPool` `eth_call`. An unknown pool (never opened in the projection's scan
//! window, or already reclaimed) yields `None`, and the callers fail **open**
//! (serve): a pool the projection has not caught up to must not refuse a paying
//! client, and the first voucher's on-chain `redeem` plus the open-time hash gates
//! still protect revenue and compliance.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use arc_swap::ArcSwap;
use decdn_incentive::payment_pool::PaymentPool;

/// Wall-clock cadence for the serve loops' mid-stream pool-solvency re-check
/// (ADR 003 §Pool solvency policy).
///
/// The re-check re-reads this projection to catch a pool other lanes drain
/// `remaining` down on mid-stream. Two facts make a wall-clock cadence — not a
/// per-voucher-boundary one — the right frequency:
///
/// - The projection only advances as the settlement watcher folds
///   `PoolRedeemed` logs (roughly the event-poll cadence), so re-reading it
///   faster than that returns the same value — wasted work on exactly the fast
///   streams that cross voucher boundaries most often.
/// - Per-stream throughput is bounded (credit window + voucher pacing), so a
///   wall-clock interval `T` bounds worst-case over-delivery on a drained pool
///   to `T × per-stream-rate` — an explicit, bounded exposure. The on-chain
///   `redeem` (`min(desired, remaining)`, partial-on-drain) remains the backstop.
///
/// At ~2 s a ~1 Gbps stream over-delivers at most ~250 MB before it stops —
/// sub-percent of a multi-GB blob, the core large-file workload.
pub const POOL_RECHECK_INTERVAL: Duration = Duration::from_secs(2);

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
    /// The pool's status, or `None` if the pool is unknown (callers fail open).
    /// The stream-admission read: a caller here may tolerate a blocking source,
    /// so an implementation MAY do slow work. The production implementation
    /// ([`PoolProjection`]) reads from memory and never blocks.
    async fn status(&self, pool_id: B256) -> Option<PoolStatus>;

    /// A read that MUST NOT block: it returns `None` rather than doing any slow or
    /// remote work. The mid-stream serve re-check calls it at each voucher
    /// boundary, where a blocking read would stall delivery. The default delegates
    /// to [`Self::status`] for in-memory test doubles; [`PoolProjection`] serves
    /// both from the same in-memory map, so neither ever touches the network.
    async fn cached_status(&self, pool_id: B256) -> Option<PoolStatus> {
        self.status(pool_id).await
    }
}

/// Per-pool state the projection folds from the `PaymentPool` event log.
#[derive(Clone, Debug, Default)]
struct PoolEntry {
    /// The pool owner, from `PoolOpened`.
    owner: Address,
    /// The current on-chain `deposit` (token base units): set by `PoolOpened`, then
    /// re-set to `newDeposit` by each `PoolToppedUp`.
    deposit: u64,
    /// `Σ` of every lane's latest `newPaidCumulative` across all providers. This
    /// equals on-chain `totalRedeemed`, which accumulates only per-lane deltas — so
    /// summing each lane's *latest* cumulative reconstructs the same total.
    total_redeemed: u64,
    /// Each `(signer, provider)` lane's latest paid cumulative, so a re-delivered
    /// `PoolRedeemed` (watcher retry / reorg rewind) folds only the positive
    /// advance and the total stays exact under replay.
    lanes: HashMap<(Address, Address), u64>,
}

impl PoolEntry {
    fn status(&self) -> PoolStatus {
        PoolStatus {
            owner: self.owner,
            remaining: U256::from(self.deposit.saturating_sub(self.total_redeemed)),
        }
    }
}

/// An event-fed [`PoolView`]: the serve gates read `{owner, remaining}` from an
/// in-memory projection the settlement watcher folds from the `PaymentPool` event
/// log, so no serve request costs a `getPool` `eth_call`.
///
/// The map is published through an [`ArcSwap`] so a read (`status` at admission,
/// `cached_status` at every voucher boundary) is a single atomic load — no lock,
/// no read-modify-write — and never contends with a concurrent reader on the serve
/// hot path. The settlement watcher's sink is the projection's single writer and
/// each fold clones-then-publishes the whole map; writes are rare (one per
/// `PaymentPool` event) next to the per-voucher-boundary reads.
///
/// Cheaply cloneable (an `Arc` around the cell). The settlement watcher's sink
/// holds one clone to WRITE the projection; the serve handler holds another as an
/// `Arc<dyn PoolView>` to READ it, both sharing the one `ArcSwap`.
#[derive(Clone, Default)]
pub struct PoolProjection {
    pools: Arc<ArcSwap<HashMap<B256, PoolEntry>>>,
}

impl std::fmt::Debug for PoolProjection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolProjection")
            .field("pools", &self.pools.load().len())
            .finish()
    }
}

impl PoolProjection {
    /// A fresh, empty projection.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Clone the current map, let `mutate` fold one event into it, and publish the
    /// result. `rcu` retries `mutate` under a compare-and-swap so an update is never
    /// lost — the projection has one writer today, but this keeps the fold correct
    /// without banking on that invariant.
    fn update(&self, mutate: impl Fn(&mut HashMap<B256, PoolEntry>)) {
        self.pools.rcu(|current| {
            let mut next = HashMap::clone(current);
            mutate(&mut next);
            next
        });
    }

    /// Apply a `PoolOpened(poolId, owner, deposit)`: record the owner and the
    /// initial deposit. Absolute, so a re-delivered log is idempotent. The deposit
    /// is a `uint256` on the wire but a `uint64` on-chain; an out-of-range value
    /// (impossible by construction) saturates to `u64::MAX`, which over-states
    /// remaining and so fails toward serving — the fail-open direction.
    pub fn record_opened(&self, pool_id: B256, owner: Address, deposit: U256) {
        self.update(|pools| {
            let entry = pools.entry(pool_id).or_default();
            entry.owner = owner;
            entry.deposit = u64::try_from(deposit).unwrap_or(u64::MAX);
        });
    }

    /// Apply a `PoolToppedUp(poolId, _, newDeposit)`: `deposit = newDeposit`.
    /// Absolute, so idempotent. Skipped for a pool never opened in the projection's
    /// scan window — there is no owner to serve, and the serve gate fails open.
    pub fn record_topup(&self, pool_id: B256, new_deposit: U256) {
        if !self.pools.load().contains_key(&pool_id) {
            return;
        }
        self.update(|pools| {
            if let Some(entry) = pools.get_mut(&pool_id) {
                entry.deposit = u64::try_from(new_deposit).unwrap_or(u64::MAX);
            }
        });
    }

    /// Apply a `PoolRedeemed(poolId, provider, lanes)` for ANY provider: fold each
    /// lane's positive advance into `total_redeemed`, matching on-chain
    /// `totalRedeemed += Σ delta`. Monotone and idempotent — a replayed event
    /// advances no lane and adds nothing. Skipped for a pool the projection has not
    /// opened (a redemption whose `PoolOpened` predates the scan window): without a
    /// deposit there is no remaining to reserve, and the serve gate fails open.
    pub fn record_redeemed(
        &self,
        pool_id: B256,
        provider: Address,
        lanes: &[PaymentPool::LaneSettled],
    ) {
        if !self.pools.load().contains_key(&pool_id) {
            return;
        }
        self.update(|pools| {
            let Some(entry) = pools.get_mut(&pool_id) else {
                return;
            };
            for lane in lanes {
                let key = (lane.signer, provider);
                let prev = entry.lanes.get(&key).copied().unwrap_or(0);
                if lane.newPaidCumulative > prev {
                    entry.total_redeemed = entry
                        .total_redeemed
                        .saturating_add(lane.newPaidCumulative - prev);
                    entry.lanes.insert(key, lane.newPaidCumulative);
                }
            }
        });
    }

    /// Apply a `PoolReclaimed(poolId, ..)`: the pool is `Closed` and its remainder
    /// refunded. Drop it — a later read returns `None` and the serve gate fails
    /// open, exactly as for a pool the projection has not yet seen. A
    /// `PoolCloseInitiated` is deliberately NOT applied here: `redeem` stays
    /// callable until the dispute deadline and on-chain `getPool` still reports the
    /// real `deposit − totalRedeemed`, so the projection keeps serving that pool
    /// until the reclaim actually lands.
    pub fn forget(&self, pool_id: B256) {
        if !self.pools.load().contains_key(&pool_id) {
            return;
        }
        self.update(|pools| {
            pools.remove(&pool_id);
        });
    }
}

#[async_trait::async_trait]
impl PoolView for PoolProjection {
    async fn status(&self, pool_id: B256) -> Option<PoolStatus> {
        self.pools.load().get(&pool_id).map(PoolEntry::status)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn pool(n: u8) -> B256 {
        B256::repeat_byte(n)
    }

    fn addr(n: u8) -> Address {
        Address::repeat_byte(n)
    }

    fn lane(signer: Address, new_cumulative: u64) -> PaymentPool::LaneSettled {
        PaymentPool::LaneSettled {
            signer,
            newPaidCumulative: new_cumulative,
            bytesPaid: 0,
        }
    }

    #[tokio::test]
    async fn unknown_pool_is_none() {
        let view = PoolProjection::new();
        assert!(view.status(pool(1)).await.is_none());
        // A top-up or redemption for a never-opened pool creates no entry.
        view.record_topup(pool(1), U256::from(100u64));
        view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 50)]);
        assert!(view.status(pool(1)).await.is_none());
    }

    #[tokio::test]
    async fn opened_pool_reports_full_deposit() {
        let view = PoolProjection::new();
        view.record_opened(pool(1), addr(7), U256::from(1_000u64));
        let status = view.status(pool(1)).await.expect("opened pool is known");
        assert_eq!(status.owner, addr(7));
        assert_eq!(status.remaining, U256::from(1_000u64));
    }

    #[tokio::test]
    async fn topup_raises_remaining() {
        let view = PoolProjection::new();
        view.record_opened(pool(1), addr(7), U256::from(1_000u64));
        view.record_topup(pool(1), U256::from(2_500u64));
        let status = view.status(pool(1)).await.unwrap();
        assert_eq!(status.remaining, U256::from(2_500u64));
    }

    #[tokio::test]
    async fn redeemed_subtracts_across_providers_and_lanes() {
        let view = PoolProjection::new();
        view.record_opened(pool(1), addr(7), U256::from(1_000u64));
        // Two providers, two signers — totalRedeemed sums every lane.
        view.record_redeemed(
            pool(1),
            addr(100),
            &[lane(addr(2), 200), lane(addr(3), 100)],
        );
        view.record_redeemed(pool(1), addr(101), &[lane(addr(2), 50)]);
        let status = view.status(pool(1)).await.unwrap();
        // remaining = 1000 − (200 + 100 + 50) = 650.
        assert_eq!(status.remaining, U256::from(650u64));
    }

    #[tokio::test]
    async fn redeemed_is_idempotent_and_monotone() {
        let view = PoolProjection::new();
        view.record_opened(pool(1), addr(7), U256::from(1_000u64));
        view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 300)]);
        // Replayed identical event (watcher retry / reorg rewind): no double count.
        view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 300)]);
        assert_eq!(
            view.status(pool(1)).await.unwrap().remaining,
            U256::from(700u64)
        );
        // A stale (lower) cumulative never lowers the total.
        view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 100)]);
        assert_eq!(
            view.status(pool(1)).await.unwrap().remaining,
            U256::from(700u64)
        );
        // A genuine advance folds only the delta.
        view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 450)]);
        assert_eq!(
            view.status(pool(1)).await.unwrap().remaining,
            U256::from(550u64)
        );
    }

    #[tokio::test]
    async fn fully_redeemed_pool_reports_zero_not_none() {
        let view = PoolProjection::new();
        view.record_opened(pool(1), addr(7), U256::from(500u64));
        view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 500)]);
        let status = view.status(pool(1)).await.unwrap();
        // Matches on-chain getPool: owner stays, remaining is zero (solvency gate
        // then refuses; the pool is not dropped until reclaim).
        assert_eq!(status.owner, addr(7));
        assert_eq!(status.remaining, U256::ZERO);
    }

    #[tokio::test]
    async fn over_redeemed_saturates_to_zero() {
        let view = PoolProjection::new();
        view.record_opened(pool(1), addr(7), U256::from(100u64));
        // Should never happen on-chain, but the projection must not underflow.
        view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 400)]);
        assert_eq!(view.status(pool(1)).await.unwrap().remaining, U256::ZERO);
    }

    #[tokio::test]
    async fn reclaim_drops_the_entry() {
        let view = PoolProjection::new();
        view.record_opened(pool(1), addr(7), U256::from(1_000u64));
        view.forget(pool(1));
        assert!(view.status(pool(1)).await.is_none());
    }

    #[tokio::test]
    async fn cached_status_matches_status() {
        let view = PoolProjection::new();
        view.record_opened(pool(1), addr(7), U256::from(1_000u64));
        let s = view.status(pool(1)).await.unwrap();
        let c = view.cached_status(pool(1)).await.unwrap();
        assert_eq!(s.owner, c.owner);
        assert_eq!(s.remaining, c.remaining);
    }
}
