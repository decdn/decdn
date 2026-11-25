//! `getPool`-fed pool view for the serve path (ADR 003 §Sizing).
//!
//! The `cdn/client/v1` serve handler holds no RPC client, but two of its gates
//! need a pool-level chain quantity per request:
//!
//! - **Floor-`M` solvency** — the pool's **remaining** balance
//!   (`getPool.deposit − getPool.totalRedeemed`) minus the refundable floor `M`
//!   must still cover the credit window, or the node refuses `Unfunded` before
//!   signing `ok: true`.
//! - **ADR 011 funder gate** — the pool **owner** (`getPool.owner`) is the funding
//!   address the origin-blacklist gate evaluates, at open time and mid-stream.
//!
//! Both come from `getPool` reads the node holds in memory ([`PoolProjection`]).
//! The node reads no event log. The two reads split by whether the caller can
//! block:
//!
//! - **Admission** ([`PoolView::status`]) MAY block. The production wrapper
//!   ([`crate::payment_settlement::ResolvingPoolView`]) answers from a read
//!   younger than [`POOL_REFRESH_INTERVAL`] and otherwise does ONE `getPool`
//!   before a serve is admitted. An absent, closed, or errored pool yields
//!   `None` and the admit gate refuses — it does not fail open.
//! - **Mid-stream** ([`PoolView::cached_status`]) MUST NOT block: it reads the
//!   held read only, whatever its age. The settlement service re-reads every
//!   pool with a live lane each [`POOL_REFRESH_INTERVAL`], so a live stream's
//!   pool is never older than that. A `None` here means the pool closed under a
//!   live stream. `redeemMany` reverts on a closed pool, so the re-check stops
//!   the stream rather than read the chain at a voucher boundary.
//!   While the node cannot reach the chain past its grace window
//!   ([`crate::chain_freshness`]), the held reads may be old. The re-check
//!   still reads them and applies its usual verdict, but each such re-check
//!   counts `decdn_serve_pool_recheck_stale_total`.
//!
//! The bare [`PoolProjection`] is the held reads themselves: its `status`
//! returns `None` for a pool it does not hold and never touches the network.
//! Tests wire it (or a fake) directly; production wraps it in the
//! `getPool`-on-miss view.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use arc_swap::ArcSwap;
use decdn_incentive::payment_pool::SignerAuthorization;
use tokio::time::Instant;

/// Wall-clock cadence for the serve loops' mid-stream pool-solvency re-check
/// (ADR 003 §Pool solvency policy).
///
/// The re-check reads the held pool read to catch a pool other lanes drain
/// `remaining` down on mid-stream. Per-stream throughput is bounded (credit
/// window + voucher pacing), so a wall-clock interval `T` bounds worst-case
/// over-delivery on a drained pool to `T × per-stream-rate` beyond the refresh
/// lag — an explicit, bounded exposure. The on-chain `redeem`
/// (`min(desired, remaining)`, partial-on-drain) remains the backstop.
pub const POOL_RECHECK_INTERVAL: Duration = Duration::from_secs(2);

/// How long a `getPool` or `getAuthorization` read answers before the node
/// reads again: the admit path re-reads an older one, and the settlement
/// service re-reads every pool and signer with a live lane at this cadence.
/// Thirty seconds bounds how long another node's drain or an owner's close
/// stays unseen here, at two `eth_call`s per live pool per interval.
pub const POOL_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// A pool's on-chain lifecycle, from `getPool.status`. `Open` pools accept
/// top-ups and redemptions; a `Closing` pool accepts redemptions only until
/// `deadline` (the on-chain dispute deadline), after which `redeemMany` reverts
/// `PoolClosed`. A pool never returns to `Open` once `Closing`, and a `Closing`
/// pool cannot be topped up, so its `remaining` only falls.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Lifecycle {
    /// Accepts top-ups and redemptions.
    #[default]
    Open,
    /// Accepts redemptions only, and only until the deadline. A pool never
    /// returns to `Open`.
    Closing {
        /// Unix seconds after which redemption reverts `PoolClosed`.
        deadline: u64,
    },
}

/// The per-pool chain quantities the serve gates read.
#[derive(Clone, Copy, Debug)]
pub struct PoolStatus {
    /// The pool owner — the ADR 011 funder subject and refund destination.
    pub owner: Address,
    /// `deposit − totalRedeemed`, the balance the floor-`M` guard reserves against.
    pub remaining: U256,
    /// The pool's lifecycle: `Open`, or `Closing` with its dispute deadline.
    pub lifecycle: Lifecycle,
}

/// A per-request source of [`PoolStatus`]. Trait so the handler holds it behind an
/// `Arc<dyn PoolView>` and tests pass a fake (or `None`) without a chain.
#[async_trait::async_trait]
pub trait PoolView: Send + Sync + std::fmt::Debug {
    /// The pool's status, or `None` if the pool cannot be confirmed. The
    /// stream-admission read: a caller here may tolerate a blocking source, so an
    /// implementation MAY do slow work. The bare [`PoolProjection`] reads from
    /// memory and returns `None` for a pool it does not hold; the production
    /// wrapper ([`crate::payment_settlement::ResolvingPoolView`]) confirms such a
    /// pool with one `getPool` before returning, so the admit gate refuses a
    /// pool it still cannot confirm rather than failing open.
    async fn status(&self, pool_id: B256) -> Option<PoolStatus>;

    /// A read that MUST NOT block: it returns `None` rather than doing any slow or
    /// remote work. The mid-stream serve re-check calls it at a voucher boundary,
    /// on the wall-clock re-check cadence, where a blocking read would stall
    /// delivery. The default delegates to [`Self::status`] for in-memory test
    /// doubles; [`PoolProjection`] serves both from the same in-memory map, so
    /// neither ever touches the network.
    async fn cached_status(&self, pool_id: B256) -> Option<PoolStatus> {
        self.status(pool_id).await
    }

    /// The admit-path signer confirm: the request's voucher `signer` on-chain
    /// authorization in `pool_id` (ADR 003 §Capability delegation, §Pool
    /// solvency). A signer's `cap` is shared across every provider, so a signer
    /// that has drawn its full `cap` at other nodes is uncashable here.
    ///
    /// - `Some(SignerAuthorization::Unregistered)` — no on-chain constraint. An
    ///   unregistered signer (it has spent nothing on-chain, so it admits on its
    ///   presented capability) or no chain wired.
    /// - `Some(SignerAuthorization::Registered { .. })` — the registered terms and
    ///   the signer's `spent` at the read.
    /// - `None` — the on-chain read faulted and no usable read of this signer is
    ///   held. The caller refuses it as unconfirmed rather than fail open.
    ///
    /// The default reports every signer unregistered: the bare [`PoolProjection`]
    /// and test doubles hold no chain. The production wrapper
    /// ([`crate::payment_settlement::ResolvingPoolView`]) overrides it with one
    /// `getAuthorization` per `(pool, signer)` that answers for
    /// [`POOL_REFRESH_INTERVAL`].
    async fn signer_authorization(
        &self,
        pool_id: B256,
        signer: Address,
    ) -> Option<SignerAuthorization> {
        let _ = (pool_id, signer);
        Some(SignerAuthorization::Unregistered)
    }

    /// The mid-stream signer-drain read: a `signer`'s total on-chain `spent` across
    /// every provider in `pool_id`, from what the node already holds — never a
    /// chain call. The serve loop pairs it with the `cap` the node already holds on
    /// the lane to test `held_cap − spent` headroom at the [`POOL_RECHECK_INTERVAL`]
    /// cadence, so a signer draining its shared `cap` at another node mid-stream stops
    /// this stream before it over-delivers unredeemable bytes (ADR 003 §Pool
    /// solvency).
    ///
    /// - `Some(spent)` — the `spent` of the latest held read, `0` for a signer
    ///   that read unregistered.
    /// - `None` — no chain is wired (a chain-free test double), so the re-check
    ///   is skipped and delivery continues; the on-chain `redeemMany` `min(desired,
    ///   cap − spent)` remains the backstop.
    ///
    /// The default returns `None`. The production wrapper
    /// ([`crate::payment_settlement::ResolvingPoolView`]) answers from its held
    /// `getAuthorization` read, which the settlement service refreshes for every
    /// live signer each [`POOL_REFRESH_INTERVAL`].
    async fn signer_spent_cached(&self, pool_id: B256, signer: Address) -> Option<u64> {
        let _ = (pool_id, signer);
        None
    }
}

/// One held `getPool` read.
#[derive(Clone, Copy, Debug)]
struct PoolEntry {
    status: PoolStatus,
    /// When the read landed.
    read_at: Instant,
}

/// The node's held `getPool` reads, one per pool.
///
/// The map is published through an [`ArcSwap`] so a read (`status` at admission,
/// `cached_status` at a voucher boundary on the re-check cadence) is a single
/// atomic load — no lock — and never contends with a concurrent reader on the
/// serve hot path. Writes are rare (one per `getPool` read) next to the reads
/// every admission and live stream make; each clones-then-publishes the map.
///
/// Cheaply cloneable (an `Arc` around the cell). The admit view, the settlement
/// refresh and the redeemer share one.
#[derive(Clone, Default)]
pub struct PoolProjection {
    pools: Arc<ArcSwap<HashMap<B256, PoolEntry>>>,
    /// Pools a read found `Closed`, waiting for the settlement service to
    /// forget their lanes.
    closed: Arc<Mutex<HashSet<B256>>>,
}

impl std::fmt::Debug for PoolProjection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolProjection")
            .field("pools", &self.pools.load().len())
            .field(
                "closed",
                &self
                    .closed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
            )
            .finish()
    }
}

impl PoolProjection {
    /// A fresh, empty projection.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Clone the current map, let `mutate` apply one write to it, and publish
    /// the result. `rcu` retries `mutate` under a compare-and-swap so an update
    /// is never lost to a concurrent writer.
    fn update(&self, mut mutate: impl FnMut(&mut HashMap<B256, PoolEntry>)) {
        self.pools.rcu(|current| {
            let mut next = HashMap::clone(current);
            mutate(&mut next);
            next
        });
    }

    /// Hold a fresh `getPool` read of a servable pool, replacing any older one.
    pub fn record_read(&self, pool_id: B256, status: PoolStatus) {
        let entry = PoolEntry {
            status,
            read_at: Instant::now(),
        };
        self.update(|pools| {
            pools.insert(pool_id, entry);
        });
    }

    /// Drop a pool: a later read returns `None`, which refuses the pool at
    /// admission and stops any live stream on it.
    pub fn forget(&self, pool_id: B256) {
        if !self.pools.load().contains_key(&pool_id) {
            return;
        }
        self.update(|pools| {
            pools.remove(&pool_id);
        });
    }

    /// A read found `pool_id` `Closed`: drop it and queue it for the settlement
    /// service, which forgets the pool's lanes.
    pub fn mark_closed(&self, pool_id: B256) {
        self.forget(pool_id);
        self.closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(pool_id);
    }

    /// Take the pools reads found `Closed` since the last call.
    #[must_use]
    pub fn take_closed(&self) -> Vec<B256> {
        self.closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
            .collect()
    }

    /// The held read of `pool_id`, whatever its age. Never blocks.
    #[must_use]
    pub fn snapshot(&self, pool_id: B256) -> Option<PoolStatus> {
        self.pools.load().get(&pool_id).map(|entry| entry.status)
    }

    /// The held read of `pool_id` while it is younger than
    /// [`POOL_REFRESH_INTERVAL`].
    #[must_use]
    pub fn fresh(&self, pool_id: B256) -> Option<PoolStatus> {
        self.pools
            .load()
            .get(&pool_id)
            .filter(|entry| entry.read_at.elapsed() < POOL_REFRESH_INTERVAL)
            .map(|entry| entry.status)
    }

    /// Drop every read older than [`POOL_REFRESH_INTERVAL`] of a pool not in
    /// `live`. A live pool's read is refreshed instead; an idle pool's is read
    /// again on its next admission.
    pub fn prune_idle(&self, live: &HashSet<B256>) {
        let stale = |(pool_id, entry): (&B256, &PoolEntry)| {
            !live.contains(pool_id) && entry.read_at.elapsed() >= POOL_REFRESH_INTERVAL
        };
        if !self.pools.load().iter().any(stale) {
            return;
        }
        self.update(|pools| pools.retain(|pool_id, entry| !stale((pool_id, entry))));
    }
}

#[async_trait::async_trait]
impl PoolView for PoolProjection {
    async fn status(&self, pool_id: B256) -> Option<PoolStatus> {
        self.snapshot(pool_id)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
