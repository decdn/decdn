//! Event-fed on-chain pool view for the serve path (ADR 003 §Sizing).
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
//! Both quantities are fully determined by the `PaymentPool` event log, so the
//! serve path reads them from an in-memory projection the settlement watcher folds
//! from that log ([`crate::payment_settlement`]) — the common case costs no
//! `getPool` `eth_call`. The two reads split by whether the caller can block:
//!
//! - **Admission** ([`PoolView::status`]) MAY block. The production wrapper
//!   ([`crate::payment_settlement::ResolvingPoolView`]) reads the projection first
//!   and, on a miss (a pool whose `PoolOpened` lies below the watcher's scan
//!   start: the head on a first boot, the rewound checkpoint after a restart), does ONE
//!   `getPool` to confirm the pool exists and is solvent BEFORE a serve is
//!   admitted. An absent, closed, or errored pool yields `None` and the admit gate
//!   refuses — it does not fail open. That read seeds a provisional snapshot of
//!   the pool's `totalRedeemed`, which the settlement watcher re-anchors at a
//!   pinned block at the end of its tick ([`PoolProjection::reanchor`]).
//! - **Mid-stream** ([`PoolView::cached_status`]) MUST NOT block: it reads the
//!   projection only. An admitted stream's pool was seeded at admission and a
//!   projection entry persists until the pool is forgotten (a `PoolReclaimed`, or
//!   a re-anchor read that finds it `Closed`, removes it), so a `None` here
//!   means the pool closed under a live stream. `redeemMany` reverts on a closed
//!   pool, so the re-check stops the stream rather than read the chain at a
//!   voucher boundary, which would stall delivery.
//!   While the node cannot reach the chain past its grace window
//!   ([`crate::chain_freshness`]), the projection may be frozen. The re-check
//!   still reads it and applies its usual verdict to what it holds, but each
//!   such re-check counts `decdn_serve_pool_recheck_stale_total`.
//!
//! The bare [`PoolProjection`] is the projection itself: its `status` returns
//! `None` for a pool it has not folded and never touches the network. Tests wire
//! it (or a fake) directly; production wraps it in the `getPool`-on-miss view.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use arc_swap::ArcSwap;
use decdn_incentive::payment_pool::{PaymentPool, SignerAuthorization};

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

/// A pool's on-chain lifecycle, folded from the `PaymentPool` event log. `Open`
/// pools accept top-ups and redemptions; a `Closing` pool accepts redemptions
/// only until `deadline` (the on-chain dispute deadline), after which
/// `redeemMany` reverts `PoolClosed`. A pool never returns to `Open` once
/// `Closing`, and a `Closing` pool cannot be topped up, so its `remaining` only
/// falls.
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
    /// memory and returns `None` for a pool it has not folded; the production
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
    /// that has drawn its full `cap` at other nodes is uncashable here. Its
    /// registered `cap` and `expiry` are write-once, so they bound every voucher
    /// whatever capability the client presents.
    ///
    /// - `Some(SignerAuthorization::Unregistered)` — no on-chain constraint. An
    ///   unregistered signer (it has spent nothing on-chain, so it admits on its
    ///   presented capability) or no chain wired.
    /// - `Some(SignerAuthorization::Registered { .. })` — the registered terms and
    ///   the signer's current `spent`.
    /// - `None` — the on-chain read faulted and no registered read of this
    ///   signer is held. The caller refuses it as unconfirmed rather than fail
    ///   open. An earlier `Unregistered` read is not trusted after a fault,
    ///   because a registration can land at any time.
    ///
    /// The default reports every signer unregistered: the bare [`PoolProjection`]
    /// and test doubles hold no chain. The production wrapper
    /// ([`crate::payment_settlement::ResolvingPoolView`]) overrides it with one
    /// `getAuthorization` per `(pool, signer)`, held for good once it reads
    /// `Registered` and with `spent` brought up to the projection's fold.
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
    /// - `Some(spent)` — the cross-provider total, `0` for a signer that has
    ///   redeemed nothing in a pool the projection knows.
    /// - `None` — no projection is wired (a chain-free test double), so the re-check
    ///   is skipped and delivery continues; the on-chain `redeemMany` `min(desired,
    ///   cap − spent)` remains the backstop.
    ///
    /// The default returns `None`. [`PoolProjection`] overrides it to read its
    /// per-signer fold. The production wrapper
    /// ([`crate::payment_settlement::ResolvingPoolView`]) answers from its cached
    /// admit-path `getAuthorization` read brought up to that fold, and reads the
    /// fold alone only when it holds no registered read for the signer. Both match
    /// how [`Self::cached_status`] reads a pool's `remaining` without blocking.
    async fn signer_spent_cached(&self, pool_id: B256, signer: Address) -> Option<u64> {
        let _ = (pool_id, signer);
        None
    }
}

/// Per-pool state the projection folds from the `PaymentPool` event log.
#[derive(Clone, Debug, Default)]
struct PoolEntry {
    /// The pool owner, from `PoolOpened`.
    owner: Address,
    /// The on-chain `deposit` (token base units) as of the latest folded log or
    /// read: the `PoolOpened` deposit, then each `PoolToppedUp` `newDeposit`. A
    /// snapshot-seeded pool starts from its read; a top-up it cannot order against
    /// the read only raises it (see [`PoolProjection::record_topup`]).
    deposit: u64,
    /// `Σ` of every folded lane's latest `newPaidCumulative` across all providers.
    /// On-chain `totalRedeemed` accumulates only per-lane deltas, so for a pool
    /// folded from its `PoolOpened` this is exactly `totalRedeemed`. For a pool
    /// seeded from a snapshot it counts only the lanes the watcher has seen, which
    /// is a lower bound; the served `remaining` subtracts the larger of this and
    /// the snapshot's count.
    total_redeemed: u64,
    /// Each `(signer, provider)` lane's latest paid cumulative, so a re-delivered
    /// `PoolRedeemed` (watcher retry / reorg rewind) folds only the positive
    /// advance and the total is idempotent under replay.
    lanes: HashMap<(Address, Address), u64>,
    /// Per-signer `Σ` of that signer's lane advances across every provider — its
    /// total on-chain `spent`, the same positive deltas `total_redeemed` folds but
    /// bucketed by signer. Maintained in `record_redeemed` so the mid-stream signer
    /// cap-headroom read ([`PoolProjection::signer_spent`]) is O(1) rather than a
    /// scan of `lanes` on every voucher-boundary re-check. Idempotent and monotone
    /// for the same reason `total_redeemed` is: only a lane's positive advance folds.
    signer_spent: HashMap<Address, u64>,
    /// The pool's lifecycle: `Open` by default, `Closing { deadline }` after a
    /// `PoolCloseInitiated`. `PoolReclaimed` removes the whole entry.
    lifecycle: Lifecycle,
    /// The on-chain `totalRedeemed` read for a pool the projection did not fold
    /// from its `PoolOpened`. `None` for a pool folded from its `PoolOpened`.
    snapshot: Option<Snapshot>,
}

/// A `getPool` read of `totalRedeemed` for a pool whose redemption history lies
/// below the watcher's scan start, plus the redemptions folded after it.
///
/// The lane fold cannot count such a pool on its own. A lane's first folded
/// `PoolRedeemed` has no baseline, so its `newPaidCumulative` cannot be split
/// into history the read already counts and a new advance. The snapshot counts
/// an advance only when it is certain of it: an event above the anchor block, on
/// a lane with a folded baseline. Any other advance it cannot place (no anchor
/// yet, no baseline, or no block) marks the snapshot stale, and the settlement
/// watcher reads the pool again at the end of its tick. A stale snapshot keeps
/// its anchor and keeps counting the advances it can place.
///
/// `snapshot.redeemed + snapshot.since` never exceeds on-chain `totalRedeemed`,
/// and neither does the lane fold, so `remaining` errs toward serving until a
/// re-read lands; a faulted or skipped re-read retries on a later tick. The one
/// error the other way is a reorg that drops a counted redemption: the projection
/// never rewinds a fold, so it keeps counting it until on-chain `totalRedeemed`
/// passes it again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Snapshot {
    /// On-chain `totalRedeemed` at the read.
    redeemed: u64,
    /// The block the read is pinned at. `None` for an admit-path seed, which
    /// reads at an unknown block, until the watcher re-reads it.
    anchor: Option<u64>,
    /// `Σ` of the lane advances folded above `anchor`.
    since: u64,
    /// A folded advance the snapshot could not place; the watcher re-reads the
    /// pool.
    stale: bool,
    /// The highest block of a `PoolRedeemed` or `PoolToppedUp` folded into the
    /// entry. A re-read pinned below it predates state the entry already holds,
    /// so it is ignored.
    folded_through: u64,
}

impl Snapshot {
    /// Whether the settlement watcher reads the pool again.
    const fn needs_read(&self) -> bool {
        self.anchor.is_none() || self.stale
    }

    /// Raise `folded_through` to an event's block.
    fn fold_block(&mut self, block: Option<u64>) {
        if let Some(block) = block {
            self.folded_through = self.folded_through.max(block);
        }
    }

    /// Whether the read at `anchor` already counts an event mined in `block`.
    const fn covers(&self, block: Option<u64>) -> bool {
        matches!((self.anchor, block), (Some(anchor), Some(block)) if block <= anchor)
    }
}

/// One `getPool` read at a pinned block, for [`PoolProjection::reanchor`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolRead {
    /// The pool read.
    pub pool_id: B256,
    /// `getPool.deposit`.
    pub deposit: u64,
    /// `getPool.totalRedeemed`.
    pub total_redeemed: u64,
    /// The lifecycle `getPool.status` and `disputeDeadline` give.
    pub lifecycle: Lifecycle,
}

/// What one [`PoolProjection::reanchor`] did with its reads. A read for a pool
/// no longer in the projection, or folded from its `PoolOpened`, is in neither
/// field.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReanchorReport {
    /// The reads that anchored their pool.
    pub anchored: usize,
    /// The reads skipped as older than what the projection holds, in read
    /// order.
    pub skipped: Vec<ReanchorSkip>,
}

/// A read [`PoolProjection::reanchor`] skipped because it predates what the
/// projection already holds for the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReanchorSkip {
    /// The skipped read.
    pub read: PoolRead,
    /// What the read is older than.
    pub reason: ReanchorStale,
}

/// What a skipped re-anchor read is older than. Checked in this order, and only
/// the first that applies is reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReanchorStale {
    /// The read's block is below the last event the entry folded.
    BelowFolded {
        /// The block of the last event the entry folded.
        folded_through: u64,
    },
    /// The read's `totalRedeemed` is below the entry's.
    LowerRedeemed {
        /// The entry's `totalRedeemed`.
        held: u64,
    },
    /// The read's deposit is below the entry's.
    LowerDeposit {
        /// The entry's deposit.
        held: u64,
    },
}

impl PoolEntry {
    /// The projection's lower bound on on-chain `totalRedeemed`.
    fn redeemed(&self) -> u64 {
        self.snapshot.map_or(self.total_redeemed, |snapshot| {
            self.total_redeemed
                .max(snapshot.redeemed.saturating_add(snapshot.since))
        })
    }

    fn status(&self) -> PoolStatus {
        PoolStatus {
            owner: self.owner,
            remaining: U256::from(self.deposit.saturating_sub(self.redeemed())),
            lifecycle: self.lifecycle,
        }
    }
}

/// An event-fed [`PoolView`]: the serve gates read `{owner, remaining}` from an
/// in-memory projection the settlement watcher folds from the `PaymentPool` event
/// log, so no serve request costs a `getPool` `eth_call`.
///
/// The map is published through an [`ArcSwap`] so a read (`status` at admission,
/// `cached_status` at a voucher boundary on the re-check cadence) is a single
/// atomic load — no lock, no read-modify-write — and never contends with a
/// concurrent reader on the serve hot path. The settlement watcher's sink writes
/// every fold and each tick's re-anchor, and the admit path inserts a pool the
/// projection does not hold; each write clones-then-publishes the whole map.
/// Writes are rare (one per `PaymentPool` event, one per re-anchoring tick, one
/// per admit-path seed) next to the reads every admission and live stream make.
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

    /// Clone the current map, let `mutate` apply one write to it, and publish the
    /// result. `rcu` retries `mutate` under a compare-and-swap so an update is never
    /// lost when the admit path seeds a pool while the settlement sink folds. A
    /// `mutate` that also collects output resets it first, because only the last
    /// run's map is published.
    fn update(&self, mut mutate: impl FnMut(&mut HashMap<B256, PoolEntry>)) {
        self.pools.rcu(|current| {
            let mut next = HashMap::clone(current);
            mutate(&mut next);
            next
        });
    }

    /// Apply a `PoolOpened(poolId, owner, deposit)`: record the owner and the
    /// initial deposit. Absolute, so a re-delivered log is idempotent. A pool
    /// seeded from a snapshot keeps the read's deposit, which postdates the open.
    /// The deposit is a `uint256` on the wire but a `uint64` on-chain; an
    /// out-of-range value (impossible by construction) saturates to `u64::MAX`,
    /// which over-states remaining and so fails toward serving — the fail-open
    /// direction.
    pub fn record_opened(&self, pool_id: B256, owner: Address, deposit: U256) {
        self.update(|pools| {
            let entry = pools.entry(pool_id).or_default();
            entry.owner = owner;
            if entry.snapshot.is_none() {
                entry.deposit = u64::try_from(deposit).unwrap_or(u64::MAX);
            }
        });
    }

    /// Apply a `PoolToppedUp(poolId, _, newDeposit)` mined in `block`: `deposit =
    /// newDeposit`. Absolute, so idempotent. Skipped for a pool not in the
    /// projection: there is no entry to update, and an admit-path seed reads the
    /// deposit itself.
    ///
    /// For a pool seeded from a snapshot, a top-up the read already counts is
    /// skipped, and one the snapshot cannot order against the read only raises
    /// the deposit, because the on-chain deposit only rises.
    pub fn record_topup(&self, pool_id: B256, new_deposit: U256, block: Option<u64>) {
        if !self.pools.load().contains_key(&pool_id) {
            return;
        }
        let new_deposit = u64::try_from(new_deposit).unwrap_or(u64::MAX);
        self.update(|pools| {
            let Some(entry) = pools.get_mut(&pool_id) else {
                return;
            };
            let Some(snapshot) = entry.snapshot.as_mut() else {
                entry.deposit = new_deposit;
                return;
            };
            let covered = snapshot.covers(block);
            let ordered = snapshot.anchor.is_some() && block.is_some();
            snapshot.fold_block(block);
            if covered {
                return;
            }
            entry.deposit = if ordered {
                new_deposit
            } else {
                entry.deposit.max(new_deposit)
            };
        });
    }

    /// Apply a `PoolRedeemed(poolId, provider, lanes)` for ANY provider, mined in
    /// `block`: fold each lane's positive advance into `total_redeemed`. For a pool
    /// folded from its `PoolOpened` this matches on-chain `totalRedeemed += Σ
    /// delta`. Monotone and idempotent — a replayed event advances no lane and adds
    /// nothing. Skipped for a pool not in the projection: there is no entry to
    /// update, and an admit-path seed reads `totalRedeemed` itself.
    ///
    /// For a pool seeded from a snapshot, an advance above the anchor block on a
    /// lane with a folded baseline also adds to the snapshot. An event at or below
    /// the anchor only sets lane baselines, because the read already counts it.
    /// Any other advance marks the snapshot stale (see [`PoolProjection::reanchor`]).
    pub fn record_redeemed(
        &self,
        pool_id: B256,
        provider: Address,
        lanes: &[PaymentPool::LaneSettled],
        block: Option<u64>,
    ) {
        if !self.pools.load().contains_key(&pool_id) {
            return;
        }
        self.update(|pools| {
            let Some(entry) = pools.get_mut(&pool_id) else {
                return;
            };
            let mut snapshot = entry.snapshot;
            if let Some(snapshot) = snapshot.as_mut() {
                snapshot.fold_block(block);
            }
            for lane in lanes {
                let key = (lane.signer, provider);
                let baseline = entry.lanes.get(&key).copied();
                let prev = baseline.unwrap_or(0);
                if lane.newPaidCumulative > prev {
                    let delta = lane.newPaidCumulative - prev;
                    entry.total_redeemed = entry.total_redeemed.saturating_add(delta);
                    let signer_total = entry.signer_spent.entry(lane.signer).or_insert(0);
                    *signer_total = signer_total.saturating_add(delta);
                    entry.lanes.insert(key, lane.newPaidCumulative);
                    if let Some(snapshot) = snapshot.as_mut()
                        && !snapshot.covers(block)
                    {
                        if snapshot.anchor.is_some() && block.is_some() && baseline.is_some() {
                            snapshot.since = snapshot.since.saturating_add(delta);
                        } else {
                            snapshot.stale = true;
                        }
                    }
                }
            }
            entry.snapshot = snapshot;
        });
    }

    /// Apply a `PoolCloseInitiated(poolId, _, disputeDeadline)`: mark the pool
    /// `Closing`. Skipped for a pool the projection has not opened — there is no
    /// entry to serve, and the admit-path `getPool` read finds the pool `Closing`
    /// and refuses it. A `Closing` pool keeps its
    /// `remaining` and stays redeemable until `deadline`; `PoolReclaimed` later
    /// removes it via `forget`.
    pub fn record_closing(&self, pool_id: B256, deadline: u64) {
        if !self.pools.load().contains_key(&pool_id) {
            return;
        }
        self.update(|pools| {
            if let Some(entry) = pools.get_mut(&pool_id) {
                entry.lifecycle = Lifecycle::Closing { deadline };
            }
        });
    }

    /// Apply a `PoolReclaimed(poolId, ..)`: the pool is `Closed` and its remainder
    /// refunded. Drop it — a later read returns `None`, which refuses the pool at
    /// admission and stops any live stream on it as `PoolClosed`. A
    /// `PoolCloseInitiated` is folded by [`Self::record_closing`], which marks the
    /// pool `Closing` but keeps it redeemable until the dispute deadline; `forget`
    /// removes the whole entry, lifecycle included, once the reclaim actually
    /// lands. The settlement watcher also drops a snapshot-seeded pool its
    /// re-anchor read finds `Closed`.
    pub fn forget(&self, pool_id: B256) {
        if !self.pools.load().contains_key(&pool_id) {
            return;
        }
        self.update(|pools| {
            pools.remove(&pool_id);
        });
    }

    /// Seed a pool the projection has NOT observed through the event log, from a
    /// direct `getPool` snapshot (the lazy backfill). The projection lives in
    /// memory, and the settlement watcher's forward-only `PoolOpened` scan starts
    /// at head on a first boot (`ColdStart::Head`) and at the rewound checkpoint
    /// after a restart. A pool opened below that start never appears in the scan.
    /// A node serving such a pool would then have no owner to verify a presented
    /// capability against and could never register the serve lane; the admit
    /// path's [`ResolvingPoolView`](crate::payment_settlement::ResolvingPoolView)
    /// resolves the owner with one `getPool` and folds it here.
    ///
    /// Inserts ONLY IF the pool is still absent, so a concurrent event fold — the
    /// projection's authoritative writer — always wins and this never clobbers a
    /// live entry. `deposit` and `total_redeemed` are the pool's `getPool.deposit`
    /// and `getPool.totalRedeemed`. The read's block is unknown, so the seed is an
    /// unanchored snapshot: `remaining` counts every redemption up to the read
    /// from the first admission on, and the settlement watcher re-anchors the pool
    /// at a pinned block at the end of its tick ([`Self::reanchor`]).
    pub fn record_resolved(&self, owner: Address, read: PoolRead) {
        let pool_id = read.pool_id;
        // Fast path: a pool already known (a race where an event fold inserted
        // between the resolve hint and here) needs no clone-and-publish, matching
        // `record_topup`/`record_redeemed`. The `update` closure repeats the check
        // because `rcu` may retry it against a map another writer changed.
        if self.pools.load().contains_key(&pool_id) {
            return;
        }
        self.update(|pools| {
            if pools.contains_key(&pool_id) {
                return;
            }
            pools.insert(
                pool_id,
                PoolEntry {
                    owner,
                    deposit: read.deposit,
                    total_redeemed: 0,
                    lanes: HashMap::new(),
                    signer_spent: HashMap::new(),
                    lifecycle: read.lifecycle,
                    snapshot: Some(Snapshot {
                        redeemed: read.total_redeemed,
                        anchor: None,
                        since: 0,
                        stale: false,
                        folded_through: 0,
                    }),
                },
            );
        });
    }

    /// Anchor snapshot-seeded pools at `getPool` reads pinned at `block`, in one
    /// publish: each takes its `totalRedeemed` and deposit from the read, a
    /// `Closing` lifecycle from the read, and restarts the count of advances
    /// above it. A read is skipped for a pool folded from its `PoolOpened` and for
    /// a pool no longer in the projection. A read that predates what the entry
    /// already holds is skipped too, and the pool stays pending: `block` below a
    /// folded event, or a `totalRedeemed` or deposit below the entry's, which on
    /// chain only rise. That covers a lagging backend behind the admit read, whose
    /// block is unknown. A reorg that lowers either value on chain also keeps the
    /// pool pending until the chain passes the held value again: a held
    /// `totalRedeemed` above the chain's refuses early, and a held deposit above
    /// the chain's serves past the pool's funds, which the on-chain `redeem`
    /// caps.
    ///
    /// Reports how many reads anchored their pool and which were skipped as
    /// older than the entry, so the caller can count them: a pool that stays
    /// pending this way can stay pending for good.
    pub fn reanchor(&self, block: u64, reads: &[PoolRead]) -> ReanchorReport {
        let mut report = ReanchorReport::default();
        if reads.is_empty() {
            return report;
        }
        self.update(|pools| {
            report = ReanchorReport::default();
            for read in reads {
                let Some(entry) = pools.get_mut(&read.pool_id) else {
                    continue;
                };
                let held_redeemed = entry.redeemed();
                let held_deposit = entry.deposit;
                let Some(snapshot) = entry.snapshot.as_mut() else {
                    continue;
                };
                let stale = if block < snapshot.folded_through {
                    Some(ReanchorStale::BelowFolded {
                        folded_through: snapshot.folded_through,
                    })
                } else if read.total_redeemed < held_redeemed {
                    Some(ReanchorStale::LowerRedeemed {
                        held: held_redeemed,
                    })
                } else if read.deposit < held_deposit {
                    Some(ReanchorStale::LowerDeposit { held: held_deposit })
                } else {
                    None
                };
                if let Some(reason) = stale {
                    report.skipped.push(ReanchorSkip {
                        read: *read,
                        reason,
                    });
                    continue;
                }
                *snapshot = Snapshot {
                    redeemed: read.total_redeemed,
                    anchor: Some(block),
                    since: 0,
                    stale: false,
                    folded_through: snapshot.folded_through,
                };
                entry.deposit = read.deposit;
                // A pool never returns to `Open`, so a read from a lagging
                // backend does not undo a folded `PoolCloseInitiated`.
                if matches!(read.lifecycle, Lifecycle::Closing { .. }) {
                    entry.lifecycle = read.lifecycle;
                }
                report.anchored += 1;
            }
        });
        report
    }

    /// The snapshot-seeded pools the settlement watcher reads again, in pool-id
    /// order: admit-path seeds not yet anchored and stale snapshots.
    #[must_use]
    pub fn pending_rereads(&self) -> Vec<B256> {
        let mut pending: Vec<B256> = self
            .pools
            .load()
            .iter()
            .filter(|(_, entry)| entry.snapshot.is_some_and(|s| s.needs_read()))
            .map(|(pool_id, _)| *pool_id)
            .collect();
        pending.sort_unstable();
        pending
    }

    /// A non-blocking snapshot read for callers outside an async trait object.
    #[must_use]
    pub fn snapshot(&self, pool_id: B256) -> Option<PoolStatus> {
        self.pools.load().get(&pool_id).map(PoolEntry::status)
    }

    /// A `signer`'s total on-chain `spent` in this pool: `Σ` of its lane advances
    /// across EVERY provider. A signer's `cap` is shared across all providers (ADR
    /// 003 §Deposit Economics), and on-chain `spent` for a signer is exactly what it
    /// has redeemed at each provider summed. The projection folds this per-signer
    /// total in `record_redeemed` alongside `total_redeemed`, so this read is an O(1)
    /// map lookup — the serve path's mid-stream signer cap-headroom re-check calls it
    /// every voucher-boundary interval per live stream (ADR 003 §Pool solvency), and
    /// a scan of the lane map here would cost `streams × lanes` on a busy pool.
    ///
    /// `0` for a pool the projection has not folded, or one in which the signer has
    /// redeemed nothing. On a pool seeded from one `getPool`
    /// ([`Self::record_resolved`]) the projection holds no lane baselines, so the
    /// total counts each lane's whole cumulative from its first redemption folded
    /// after the seed, and nothing for a lane that has not redeemed since; the
    /// production view adds the signer's spend from its admit-path
    /// `getAuthorization` read. The folded total
    /// cannot exceed `total_redeemed`, which is itself bounded by the deposit.
    #[must_use]
    pub fn signer_spent(&self, pool_id: B256, signer: Address) -> u64 {
        self.pools.load().get(&pool_id).map_or(0, |entry| {
            entry.signer_spent.get(&signer).copied().unwrap_or(0)
        })
    }
}

#[async_trait::async_trait]
impl PoolView for PoolProjection {
    async fn status(&self, pool_id: B256) -> Option<PoolStatus> {
        self.pools.load().get(&pool_id).map(PoolEntry::status)
    }

    async fn signer_spent_cached(&self, pool_id: B256, signer: Address) -> Option<u64> {
        Some(self.signer_spent(pool_id, signer))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
