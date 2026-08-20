//! Buyer-side payment-pool bookkeeping (#744).
//!
//! When a node pulls content from an upstream provider on a cache miss it
//! acts as the *client*: it opens (or reuses) a `PaymentPool` deposit against
//! itself as owner, signs cumulative vouchers per `(signer, provider)` lane
//! as bytes arrive (the signing primitives live in [`crate::voucher`]; the
//! requester in `decdn-node` drives them), and lets the redeeming provider —
//! or, if abandoned, its own `closePool` + grace-window `reclaim` — recover
//! the residual deposit (ADR 003 §node→node).
//!
//! This is the *buyer's* mirror of [`crate::lane::LaneState`] /
//! [`crate::store::PoolStateStore`], which track the *seller's* view. The
//! two are deliberately separate types:
//!
//! - The seller keys lane state by [`crate::lane::LaneKey`] `(pool_id,
//!   signer, provider)` and records the latest voucher *signature* (to
//!   submit on-chain). The buyer keys pool state by **owner address** — the
//!   reuse unit is "one open pool per owner", almost always the node's own
//!   address — and does not retain signatures (the buyer is the signer; it
//!   never submits another party's voucher on-chain). One pool fans out to
//!   many `(signer, provider)` lanes, so `BuyerPoolState` carries a
//!   per-[`LaneKey`] progress table alongside the pool-level fields.
//! - The seller validates inbound vouchers (replay guard, #527). The buyer
//!   only records its own monotonically-advancing cumulative totals — priced
//!   as `cumulative = received_bytes × rate` from its own BLAKE3-verified
//!   bytes, with no voucher nonce — so a reused lane resumes from the right
//!   `bytes`/`amount` after a restart.
//!
//! Persistence matters for two reasons: a restart must not re-`openPool`
//! (wasting a fresh deposit + gas) when a live pool already exists, and the
//! reclaim path must know which closed pools still hold a refundable
//! deposit.

use std::collections::HashMap;
use std::sync::Mutex;

use alloy::primitives::{Address, U256};

use crate::lane::{LaneKey, PoolId};
use crate::store::StoreError;

/// Buyer-tracked progress on one `(signer, provider)` lane inside a pool: the
/// cumulative totals of the most-recently-signed voucher on that lane. The
/// buyer prices `cumulative = received_bytes × rate` from its own
/// BLAKE3-verified bytes — there is no voucher nonce to track, unlike the
/// seller-side [`crate::lane::LaneState`], which additionally retains the
/// accepted signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BuyerLaneProgress {
    /// Cumulative amount of the most-recently-signed voucher on this lane
    /// (token base units). `U256::ZERO` before the first voucher.
    pub last_amount: U256,
    /// Cumulative bytes paid for as of the most-recently-signed voucher.
    pub last_bytes: U256,
    /// The hash-chain epoch this lane's **next** chain opens at (ADR 003
    /// §One chain per lane). `0` on a lane that has never metered.
    ///
    /// This is the only piece of chain state the payer persists. The seed is
    /// derived from the signing key on demand, so nothing secret is written to
    /// disk — but the counter must survive, because re-opening an epoch the
    /// node has already seen would re-release preimages it has already
    /// credited, and every one of them would pay nothing.
    pub next_epoch: u64,
}

/// Buyer-held state for one pool, fanned out across every `(signer,
/// provider)` lane the owner has signed vouchers on.
///
/// **Field invariant:** `pool_id` is the **identity key** (the store's
/// primary key; the on-chain `poolId` decoded from the `PoolOpened` event in
/// the open tx receipt). `owner` is a **secondary reuse index** — the
/// open-pool trigger looks it up to decide whether to reuse an existing pool
/// instead of opening a new one — not the identity key: two pools can
/// (transiently) exist for the same owner, e.g. across a rotate. Per-lane
/// progress MUST only advance — through [`BuyerPoolState::advance_lane`] (the
/// validated mutator) or hydration from a [`BuyerPoolStore`]. `lanes` is
/// private so nothing outside this module can insert a lane's progress
/// without going through the monotonicity guard; the cross-crate hydration
/// path (`decdn-node` decoding the redb record) uses
/// [`BuyerPoolState::hydrate`] instead of a struct literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuyerPoolState {
    /// On-chain `poolId` (`keccak256(owner, ownerPoolNonce)`) — learned by
    /// decoding the `PoolOpened` event from the open tx receipt (atomic with
    /// the open; no follow-up `getPool` read). The store's primary key.
    pub pool_id: PoolId,
    /// The on-chain pool owner: put up the deposit, receives the refund, and
    /// the only address `topUp`/`closePool`/`reclaim` accept. Equals the
    /// local key for a self-funded pool. The reuse index in
    /// [`BuyerPoolStore`] (see the field invariant above).
    pub owner: Address,
    /// `ERC-20` token bound by the pool (`USDC`).
    pub token: Address,
    /// On-chain deposited amount in token base units (initial + any
    /// top-ups).
    pub deposit: U256,
    /// Per-`(signer, provider)` lane progress. Private — mutate only through
    /// [`Self::advance_lane`] or [`Self::hydrate`].
    lanes: HashMap<LaneKey, BuyerLaneProgress>,
}

impl BuyerPoolState {
    /// Construct fresh state for a newly-opened buyer pool, with no lanes
    /// touched yet.
    #[must_use]
    pub fn new(pool_id: PoolId, owner: Address, token: Address, deposit: U256) -> Self {
        Self {
            pool_id,
            owner,
            token,
            deposit,
            lanes: HashMap::new(),
        }
    }

    /// Reconstruct pool state from a trusted persistent store — the one
    /// cross-crate path allowed to seed per-lane progress directly (mirrors
    /// [`crate::lane::LaneState::hydrate`]).
    #[must_use]
    pub fn hydrate(
        pool_id: PoolId,
        owner: Address,
        token: Address,
        deposit: U256,
        lanes: Vec<(LaneKey, BuyerLaneProgress)>,
    ) -> Self {
        Self {
            pool_id,
            owner,
            token,
            deposit,
            lanes: lanes.into_iter().collect(),
        }
    }

    /// The tracked progress on `lane`, or `None` if this pool has never
    /// signed a voucher on it.
    #[must_use]
    pub fn lane_progress(&self, lane: LaneKey) -> Option<BuyerLaneProgress> {
        self.lanes.get(&lane).copied()
    }

    /// Every tracked lane and its progress, in no particular order. Callers
    /// that need a stable order (e.g. a deterministic on-disk encoding) sort
    /// the result themselves.
    pub fn lanes(&self) -> impl Iterator<Item = (LaneKey, BuyerLaneProgress)> + '_ {
        self.lanes.iter().map(|(k, v)| (*k, *v))
    }

    /// Number of lanes with tracked progress.
    #[must_use]
    pub fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    /// Advance `lane`'s cumulative totals to the reported values, enforcing
    /// monotonicity (vouchers are cumulative over a lane's lifetime, so
    /// totals may stay equal — an idempotent re-record — or rise, never
    /// fall). A lane with no prior progress accepts any totals (there is no
    /// watermark to regress against). This is the sanctioned lane mutator;
    /// prefer it over reaching into `lanes` directly (see the type's field
    /// invariant).
    ///
    /// # Errors
    ///
    /// Returns [`BuyerProgressError`] if `bytes` or `amount` is below the
    /// lane's currently-recorded value.
    pub fn advance_lane(
        &mut self,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
        next_epoch: u64,
    ) -> Result<(), BuyerProgressError> {
        if let Some(existing) = self.lanes.get(&lane) {
            if bytes < existing.last_bytes {
                return Err(BuyerProgressError::Regressed {
                    field: "bytes",
                    recorded: existing.last_bytes,
                    got: bytes,
                });
            }
            if amount < existing.last_amount {
                return Err(BuyerProgressError::Regressed {
                    field: "amount",
                    recorded: existing.last_amount,
                    got: amount,
                });
            }
        }
        // The epoch counter is a MAXIMUM, not an overwrite. Concurrent pulls on
        // one lane share a ledger but report progress independently, so a
        // straggler reporting an older counter must not walk it back — that
        // would re-open a chain the node has already seen.
        let recorded_epoch = self.lanes.get(&lane).map_or(0, |e| e.next_epoch);
        self.lanes.insert(
            lane,
            BuyerLaneProgress {
                last_amount: amount,
                last_bytes: bytes,
                next_epoch: next_epoch.max(recorded_epoch),
            },
        );
        Ok(())
    }
}

/// Failure mode for [`BuyerPoolState::advance_lane`]: a reported cumulative
/// total regressed below the recorded one (a caller bug — vouchers never
/// decrease).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BuyerProgressError {
    /// `field`'s reported value `got` is below the recorded value.
    #[error("buyer lane {field} regressed: recorded {recorded}, got {got}")]
    Regressed {
        /// Which cumulative field regressed (`bytes` / `amount`).
        field: &'static str,
        /// The currently-recorded (higher) value.
        recorded: U256,
        /// The reported (lower) value that was rejected.
        got: U256,
    },
}

/// Result of hydrating the buyer-pool store.
///
/// A disk-backed store keeps hydration available when one row cannot be
/// decoded: healthy pools remain usable and reclaimable, while the row's
/// primary key (`pool_id`) is retained as the only available repair handle —
/// the primary table is keyed by `pool_id`, so that is the one piece of
/// identity an undecodable row still exposes; the owner lives inside the
/// bytes that failed to decode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuyerLoad {
    /// Successfully decoded buyer pools.
    pub pools: Vec<BuyerPoolState>,
    /// Pool ids whose persisted rows could not be decoded. Their deposits
    /// remain escrowed but untracked until the rows are repaired.
    pub skipped: Vec<PoolId>,
}

/// Outcome of an atomic [`BuyerPoolStore::advance_progress`].
///
/// The read-check-advance-write happens inside one serialized write
/// transaction, so these variants describe the committed-row decision rather
/// than a backend fault (those surface as [`StoreError`], as with
/// [`BuyerPoolStore::forget_if_pool`]).
///
/// `#[must_use]`: the variant is the only signal that nothing was persisted
/// (`UnknownPool` / `PoolMismatch`) or that the totals regressed — a dropped
/// outcome silently looks like success.
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub enum AdvanceOutcome {
    /// The committed row's lane was advanced and re-persisted durably.
    Advanced,
    /// No row is reachable for the owner/pool (the owner's secondary index
    /// has no entry, or the pool it names is absent — never recorded, or
    /// the table does not exist yet).
    UnknownPool,
    /// The owner's secondary index names a *different* pool than the one the
    /// caller expected — the owner's slot was replaced by a newer open. The
    /// caller should treat this as stale and must NOT escalate (writing
    /// would clobber the live replacement).
    PoolMismatch,
    /// The reported totals would regress the lane's committed watermark — a
    /// real caller bug (vouchers never decrease). Carries the rejecting
    /// error.
    Regressed(BuyerProgressError),
}

/// Outcome of an atomic [`BuyerPoolStore::add_deposit`].
///
/// `#[must_use]` for the same reason as [`AdvanceOutcome`]: a dropped
/// `PoolMismatch` / `UnknownPool` silently looks like a successful credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum DepositOutcome {
    /// `additional` was added to the committed deposit; carries the new
    /// total.
    Added(U256),
    /// No row is reachable for the owner/pool (see
    /// [`AdvanceOutcome::UnknownPool`]).
    UnknownPool,
    /// The committed row is for a different pool (stale; NOT escalated).
    PoolMismatch,
}

/// Durable backing store for [`BuyerPoolState`], keyed by `pool_id` (its
/// on-chain identity) with `owner` as a secondary reuse index.
///
/// Mirrors [`crate::store::PoolStateStore`] but for the buyer's view. The
/// reuse unit is one open pool per owner, so `get_by_owner` is the hot path
/// the pool-open trigger consults before deciding to reuse vs. open — it
/// resolves through the owner index to the primary `pool_id`-keyed row.
/// Implementations MUST persist `record`/`forget` durably (fsync, for
/// disk-backed impls) before returning `Ok`.
pub trait BuyerPoolStore: Send + Sync {
    /// Load every persisted buyer pool. Called once at bring-up to hydrate
    /// the in-memory pool map and seed the reclaim sweep.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store itself is unreadable.
    /// Individual undecodable rows are reported in [`BuyerLoad::skipped`] so
    /// they do not prevent healthy pools from loading.
    fn load_all(&self) -> Result<BuyerLoad, StoreError>;

    /// Persist (insert or overwrite) the state for one pool, keyed by
    /// `state.pool_id` (primary) with `state.owner` maintained as a
    /// secondary reuse index. MUST be durable before returning `Ok`.
    ///
    /// Callers SHOULD pass a `state` whose lane progress is a non-strict
    /// monotonic successor of any previously-recorded state for
    /// `state.pool_id` (advance via [`BuyerPoolState::advance_lane`]). The
    /// trait does not re-validate this — it is a dumb writer; the
    /// monotonicity invariant is owned upstream.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the write or fsync fails.
    fn record(&self, state: &BuyerPoolState) -> Result<(), StoreError>;

    /// Drop the persisted entry the owner index currently maps `owner` to
    /// (after the pool is reclaimed or closed), removing it from both the
    /// primary table and the owner index. A no-op if no record exists. MUST
    /// commit durably.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the delete or durable commit fails.
    fn forget(&self, owner: Address) -> Result<(), StoreError>;

    /// Point-lookup the live pool by its primary key, or `None` if none is
    /// tracked. Unlike [`Self::get_by_owner`] this needs no secondary index
    /// hop.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable or the
    /// record is corrupt.
    fn get_by_pool_id(&self, pool_id: PoolId) -> Result<Option<BuyerPoolState>, StoreError>;

    /// Compare-and-delete: drop `owner`'s entry **only if** the stored
    /// record's `pool_id` still equals `pool_id`. Returns `true` if a row was
    /// deleted, `false` if the stored row was for a different pool (already
    /// replaced by a newer open) or no row exists.
    ///
    /// This guards the reclaim sweep against a lost update: between the
    /// sweep loading a closed pool and forgetting it, a concurrent
    /// `open_or_reuse` may have opened a replacement under the same owner
    /// key. An unconditional [`Self::forget`] would delete the live
    /// replacement; this CAS deletes only the pool the sweep actually
    /// reclaimed. MUST commit durably.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the read, delete, or durable commit
    /// fails.
    fn forget_if_pool(&self, owner: Address, pool_id: PoolId) -> Result<bool, StoreError>;

    /// Point-lookup the live pool for `owner`, or `None` if none is tracked.
    /// The open-pool trigger uses this to reuse an existing pool instead of
    /// opening a new one.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable or the
    /// record is corrupt.
    fn get_by_owner(&self, owner: Address) -> Result<Option<BuyerPoolState>, StoreError>;

    /// Atomically advance the committed progress for `lane` inside `owner`'s
    /// pool.
    ///
    /// Reads the row, verifies its `pool_id` still equals `pool_id` (the pool
    /// the caller actually paid on), runs [`BuyerPoolState::advance_lane`]
    /// against the **committed** lane watermark, and writes the advanced row
    /// back — all inside one serialized write transaction. This closes the
    /// lost-update / watermark-regression race that a separate
    /// `get_by_owner` → mutate → [`Self::record`] sequence exposes when a
    /// concurrent writer (e.g. [`Self::add_deposit`]) touches the same row
    /// in the gap. MUST commit durably.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] only on a backend/codec fault. The
    /// committed-row decision (advanced / unknown / replaced / regressed) is
    /// the `Ok` value.
    fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
        next_epoch: u64,
    ) -> Result<AdvanceOutcome, StoreError>;

    /// Atomically add `additional` to the committed deposit for `owner`'s
    /// pool.
    ///
    /// Reads the **committed** deposit inside the write transaction (never a
    /// stale snapshot), verifies the row's `pool_id` still equals `pool_id`,
    /// `saturating_add`s `additional`, and writes back. Used after the
    /// on-chain `topUp` receipt lands so the persisted deposit is derived
    /// from the committed row even if a concurrent writer advanced it during
    /// the RPC. MUST commit durably.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] only on a backend/codec fault; the
    /// committed-row decision is the `Ok` value.
    fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError>;
}

/// In-memory backing for [`MemoryBuyerPoolStore`]: the primary
/// `pool_id`-keyed map plus the `owner → pool_id` secondary reuse index,
/// held behind one mutex so the pair updates atomically (mirrors the redb
/// table's single-write-transaction discipline).
#[derive(Debug, Default)]
struct MemoryInner {
    pools: HashMap<PoolId, BuyerPoolState>,
    owner_index: HashMap<Address, PoolId>,
}

/// In-memory [`BuyerPoolStore`] for tests and the trait's reference
/// semantics. Not durable — drops with the process. The runtime uses the
/// redb-backed impl in `crates/node`.
#[derive(Debug, Default)]
pub struct MemoryBuyerPoolStore {
    inner: Mutex<MemoryInner>,
}

impl MemoryBuyerPoolStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the current entry count (test helper).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |m| m.pools.len())
    }

    /// `true` when no pools are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl BuyerPoolStore for MemoryBuyerPoolStore {
    fn load_all(&self) -> Result<BuyerLoad, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(BuyerLoad {
            pools: guard.pools.values().cloned().collect(),
            skipped: Vec::new(),
        })
    }

    fn record(&self, state: &BuyerPoolState) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard.owner_index.insert(state.owner, state.pool_id);
        guard.pools.insert(state.pool_id, state.clone());
        Ok(())
    }

    fn forget(&self, owner: Address) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        if let Some(pool_id) = guard.owner_index.remove(&owner) {
            guard.pools.remove(&pool_id);
        }
        Ok(())
    }

    fn forget_if_pool(&self, owner: Address, pool_id: PoolId) -> Result<bool, StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        if guard.owner_index.get(&owner) == Some(&pool_id) {
            guard.owner_index.remove(&owner);
            guard.pools.remove(&pool_id);
            return Ok(true);
        }
        Ok(false)
    }

    fn get_by_pool_id(&self, pool_id: PoolId) -> Result<Option<BuyerPoolState>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard.pools.get(&pool_id).cloned())
    }

    fn get_by_owner(&self, owner: Address) -> Result<Option<BuyerPoolState>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard
            .owner_index
            .get(&owner)
            .and_then(|pool_id| guard.pools.get(pool_id))
            .cloned())
    }

    fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
        next_epoch: u64,
    ) -> Result<AdvanceOutcome, StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        let Some(mapped) = guard.owner_index.get(&owner).copied() else {
            return Ok(AdvanceOutcome::UnknownPool);
        };
        if mapped != pool_id {
            return Ok(AdvanceOutcome::PoolMismatch);
        }
        let Some(state) = guard.pools.get_mut(&pool_id) else {
            return Ok(AdvanceOutcome::UnknownPool);
        };
        match state.advance_lane(lane, bytes, amount, next_epoch) {
            Ok(()) => Ok(AdvanceOutcome::Advanced),
            Err(err) => Ok(AdvanceOutcome::Regressed(err)),
        }
    }

    fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        let Some(mapped) = guard.owner_index.get(&owner).copied() else {
            return Ok(DepositOutcome::UnknownPool);
        };
        if mapped != pool_id {
            return Ok(DepositOutcome::PoolMismatch);
        }
        let Some(state) = guard.pools.get_mut(&pool_id) else {
            return Ok(DepositOutcome::UnknownPool);
        };
        state.deposit = state.deposit.saturating_add(additional);
        Ok(DepositOutcome::Added(state.deposit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{address, b256};

    /// Every `owner_byte` gets a **distinct** `pool_id` too (derived from the
    /// same byte): `pool_id` is the store's primary key, so two samples
    /// sharing one `pool_id` would collide in the primary table instead of
    /// coexisting as two independent pools.
    fn sample(owner_byte: u8) -> BuyerPoolState {
        let mut obytes = [0u8; 20];
        obytes[19] = owner_byte;
        let mut idbytes = [0u8; 32];
        idbytes[31] = owner_byte;
        let owner = Address::from(obytes);
        let mut state = BuyerPoolState::new(
            PoolId::from(idbytes),
            owner,
            address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            U256::from(10_000_000u64),
        );
        let lane = LaneKey {
            pool_id: state.pool_id,
            signer: owner,
            provider: address!("00000000000000000000000000000000000000b2"),
        };
        // Ignore: fresh state, cannot regress.
        let _ = state.advance_lane(lane, U256::from(4_096u64), U256::from(1_234u64), 0);
        state
    }

    fn only_lane(state: &BuyerPoolState) -> LaneKey {
        state.lanes().next().map_or(
            LaneKey {
                pool_id: state.pool_id,
                signer: state.owner,
                provider: Address::ZERO,
            },
            |(k, _)| k,
        )
    }

    #[test]
    fn memory_store_round_trip() -> anyhow::Result<()> {
        let store = MemoryBuyerPoolStore::new();
        let a = sample(1);
        let b = sample(2);
        store.record(&a)?;
        store.record(&b)?;
        anyhow::ensure!(store.len() == 2);
        let got = store
            .get_by_owner(a.owner)?
            .ok_or_else(|| anyhow::anyhow!("missing a"))?;
        anyhow::ensure!(got == a);
        Ok(())
    }

    #[test]
    fn memory_store_record_overwrites_by_owner() -> anyhow::Result<()> {
        let store = MemoryBuyerPoolStore::new();
        let mut s = sample(1);
        store.record(&s)?;
        s.deposit = U256::from(99u64);
        store.record(&s)?;
        anyhow::ensure!(store.len() == 1, "same owner overwrites");
        let only = store
            .get_by_owner(s.owner)?
            .ok_or_else(|| anyhow::anyhow!("missing entry"))?;
        anyhow::ensure!(only.deposit == U256::from(99u64));
        Ok(())
    }

    #[test]
    fn memory_store_forget_removes_entry() -> anyhow::Result<()> {
        let store = MemoryBuyerPoolStore::new();
        let s = sample(1);
        store.record(&s)?;
        store.forget(s.owner)?;
        anyhow::ensure!(store.is_empty());
        // Forgetting an unknown owner is a no-op.
        store.forget(address!("00000000000000000000000000000000000000ff"))?;
        Ok(())
    }

    #[test]
    fn advance_lane_accepts_monotonic_and_equal() -> anyhow::Result<()> {
        let mut s = BuyerPoolState::new(
            b256!("11111111111111111111111111111111111111111111111111111111111111ab"),
            address!("00000000000000000000000000000000000000a1"),
            address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            U256::ZERO,
        );
        let lane = LaneKey {
            pool_id: s.pool_id,
            signer: s.owner,
            provider: address!("00000000000000000000000000000000000000b1"),
        };
        // First advance from an untouched lane.
        s.advance_lane(lane, U256::from(1_000u64), U256::from(10u64), 0)?;
        anyhow::ensure!(
            s.lane_progress(lane)
                == Some(BuyerLaneProgress {
                    next_epoch: 0,
                    last_amount: U256::from(10u64),
                    last_bytes: U256::from(1_000u64),
                })
        );
        // Equal totals are allowed (idempotent re-record).
        s.advance_lane(lane, U256::from(1_000u64), U256::from(10u64), 0)?;
        // Strictly higher advances.
        s.advance_lane(lane, U256::from(2_000u64), U256::from(20u64), 0)?;
        anyhow::ensure!(
            s.lane_progress(lane)
                .ok_or_else(|| anyhow::anyhow!("missing lane"))?
                .last_bytes
                == U256::from(2_000u64)
        );
        Ok(())
    }

    #[test]
    fn advance_lane_rejects_regression_and_leaves_state_unchanged() -> anyhow::Result<()> {
        let mut base = sample(1);
        let lane = only_lane(&base);
        // `sample` already left this lane at (bytes 4_096, amount 1_234);
        // advance past that before probing the regression cases below.
        base.advance_lane(lane, U256::from(5_000u64), U256::from(5_000u64), 0)?;

        // (reported (bytes, amount), expected regressed field).
        let cases = [
            ((U256::from(4_000u64), U256::from(6_000u64)), "bytes"),
            ((U256::from(6_000u64), U256::from(4_000u64)), "amount"),
        ];
        for ((bytes, amount), expected_field) in cases {
            let mut s = base.clone();
            let err = s
                .advance_lane(lane, bytes, amount, 0)
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected regression error for {expected_field}"))?;
            anyhow::ensure!(
                matches!(err, BuyerProgressError::Regressed { field, .. } if field == expected_field),
                "wrong error variant/field: {err:?}"
            );
            anyhow::ensure!(s == base, "rejected advance must not mutate state");
        }
        Ok(())
    }

    #[test]
    fn forget_if_pool_only_deletes_matching_pool() -> anyhow::Result<()> {
        let store = MemoryBuyerPoolStore::new();
        let s = sample(1);
        store.record(&s)?;

        // Wrong pool id → no delete, row preserved.
        let deleted = store.forget_if_pool(
            s.owner,
            b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        )?;
        anyhow::ensure!(!deleted, "mismatched pool must not delete");
        anyhow::ensure!(store.len() == 1, "row must survive a mismatched CAS");

        // Matching pool id → deletes.
        let deleted = store.forget_if_pool(s.owner, s.pool_id)?;
        anyhow::ensure!(deleted, "matching pool must delete");
        anyhow::ensure!(store.is_empty());

        // Unknown owner → false, no-op.
        anyhow::ensure!(!store.forget_if_pool(
            address!("00000000000000000000000000000000000000ff"),
            s.pool_id
        )?);
        Ok(())
    }

    #[test]
    fn advance_progress_advances_committed_watermark() -> anyhow::Result<()> {
        let store = MemoryBuyerPoolStore::new();
        let s = BuyerPoolState::new(
            b256!("22222222222222222222222222222222222222222222222222222222222222ab"),
            address!("00000000000000000000000000000000000000a3"),
            address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            U256::from(10_000_000u64),
        );
        store.record(&s)?;
        let lane = LaneKey {
            pool_id: s.pool_id,
            signer: s.owner,
            provider: address!("00000000000000000000000000000000000000b3"),
        };

        let outcome = store.advance_progress(
            s.owner,
            s.pool_id,
            lane,
            U256::from(3_000u64),
            U256::from(30u64),
            0,
        )?;
        anyhow::ensure!(outcome == AdvanceOutcome::Advanced, "got {outcome:?}");
        let stored = store
            .get_by_owner(s.owner)?
            .ok_or_else(|| anyhow::anyhow!("missing row"))?;
        let progress = stored
            .lane_progress(lane)
            .ok_or_else(|| anyhow::anyhow!("missing lane"))?;
        anyhow::ensure!(progress.last_bytes == U256::from(3_000u64));
        anyhow::ensure!(progress.last_amount == U256::from(30u64));
        Ok(())
    }

    #[test]
    fn advance_progress_rejects_regression_without_writing() -> anyhow::Result<()> {
        let store = MemoryBuyerPoolStore::new();
        let mut s = sample(1);
        let lane = only_lane(&s);
        // `sample` already left this lane at (bytes 4_096, amount 1_234);
        // advance past that before probing the regression cases below.
        s.advance_lane(lane, U256::from(9_000u64), U256::from(9_000u64), 0)?;
        store.record(&s)?;

        // Each cumulative field's regression must surface through the
        // wrapper as `AdvanceOutcome::Regressed` with the right field — and
        // must NOT touch the committed watermark.
        let cases = [
            ((U256::from(8_999u64), U256::from(9_000u64)), "bytes"),
            ((U256::from(9_000u64), U256::from(8_999u64)), "amount"),
        ];
        for ((bytes, amount), expected_field) in cases {
            let outcome = store.advance_progress(s.owner, s.pool_id, lane, bytes, amount, 0)?;
            anyhow::ensure!(
                matches!(outcome, AdvanceOutcome::Regressed(BuyerProgressError::Regressed { field, .. }) if field == expected_field),
                "expected {expected_field} regression, got {outcome:?}"
            );
            let stored = store
                .get_by_owner(s.owner)?
                .ok_or_else(|| anyhow::anyhow!("missing row"))?;
            let progress = stored
                .lane_progress(lane)
                .ok_or_else(|| anyhow::anyhow!("missing lane"))?;
            anyhow::ensure!(
                progress.last_bytes == U256::from(9_000u64)
                    && progress.last_amount == U256::from(9_000u64),
                "watermark regressed after rejecting {expected_field}"
            );
        }
        Ok(())
    }

    #[test]
    fn advance_and_deposit_guard_on_pool_and_owner() -> anyhow::Result<()> {
        let store = MemoryBuyerPoolStore::new();
        let s = sample(1);
        let lane = only_lane(&s);
        store.record(&s)?;
        let other_pool = b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        let unknown = address!("00000000000000000000000000000000000000ff");

        // Pool-id mismatch → stale, no write.
        anyhow::ensure!(
            store.advance_progress(
                s.owner,
                other_pool,
                lane,
                U256::from(99u64),
                U256::from(99u64),
                0
            )? == AdvanceOutcome::PoolMismatch
        );
        anyhow::ensure!(
            store.add_deposit(s.owner, other_pool, U256::from(1u64))?
                == DepositOutcome::PoolMismatch
        );
        let stored = store
            .get_by_owner(s.owner)?
            .ok_or_else(|| anyhow::anyhow!("missing row"))?;
        anyhow::ensure!(stored == s, "mismatched calls must not mutate the row");

        // Unknown owner → UnknownPool, no write.
        anyhow::ensure!(
            store.advance_progress(
                unknown,
                s.pool_id,
                lane,
                U256::from(1u64),
                U256::from(1u64),
                0
            )? == AdvanceOutcome::UnknownPool
        );
        anyhow::ensure!(
            store.add_deposit(unknown, s.pool_id, U256::from(1u64))? == DepositOutcome::UnknownPool
        );
        Ok(())
    }

    #[test]
    fn add_deposit_accumulates_committed_deposit() -> anyhow::Result<()> {
        let store = MemoryBuyerPoolStore::new();
        let mut s = sample(1);
        s.deposit = U256::from(100u64);
        store.record(&s)?;

        let outcome = store.add_deposit(s.owner, s.pool_id, U256::from(40u64))?;
        anyhow::ensure!(
            outcome == DepositOutcome::Added(U256::from(140u64)),
            "got {outcome:?}"
        );
        let stored = store
            .get_by_owner(s.owner)?
            .ok_or_else(|| anyhow::anyhow!("missing row"))?;
        anyhow::ensure!(stored.deposit == U256::from(140u64));
        Ok(())
    }
}
