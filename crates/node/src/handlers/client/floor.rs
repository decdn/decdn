//! The floor accumulator: how much un-vouchered credit this node has fronted
//! against a pool, and against each capability signer on it (ADR 003 §Pool
//! solvency).
//!
//! Two quantities, two shapes. [`PoolFloorState`] holds a pool's live reservation
//! and the per-signer rows it is the sum of; each row also carries that signer's
//! node-local abandonment bucket, which is deliberately NOT part of any pool total.
//! [`FloorReservation`] is the RAII hold one stream takes against them.
//!
//! The invariant every mutation must preserve is that the pool's `live_reservation`
//! equals the fold of the per-signer ones. That is why the counters are PRIVATE to
//! this module and moved only through [`PoolFloorState`]'s inherent mutators, each
//! of which touches both levels in one call. A one-sided update would be silent, and
//! nothing downstream would notice until the pool over- or under-admitted.
//!
//! The counters live here rather than in `mod.rs` so that privacy is real: a sibling
//! module cannot name a field, so the mutators are the only way to move one.

use super::{
    Address, Arc, AtomicBool, AtomicU64, B256, CHUNK_BYTES, ClientHandler, Hash, HashMap, Metrics,
    Ordering, ServeRejectReason, U256, min_payment,
};

/// Rebuild the in-memory accumulator from the durable bucket rows at bring-up.
///
/// No stream is live at boot, so every `live_reservation` starts at zero; each
/// `(pool, signer)` lane's persisted abandonment bucket carries forward, so a restart
/// resumes a signer's throttle where it left off rather than granting a fresh
/// allowance. The bucket refills, so the stored `refill_unix_ms` is loaded as-is and
/// the next admission replays the time-refill from it to now. A pool entry exists
/// only to hold its signer bucket rows; its `live_reservation` stays zero.
///
/// Fails CLOSED. A genuine first boot returns `Ok(vec![])` from `load_buckets` (the
/// table simply does not exist yet), so any error reaching here is a real store fault,
/// and coming up empty would hand every signer a clean allowance — the
/// abandon-then-restart escape the persistence exists to close.
///
/// # Errors
/// Returns the store's error if the tombstone sweep or the row load fails.
pub(super) fn hydrate(
    store: Option<&Arc<dyn decdn_incentive::PoolFloorLossStore>>,
) -> anyhow::Result<FloorAccumulator> {
    let mut pool_floor = FloorAccumulator::default();
    let Some(store) = store else {
        return Ok(pool_floor);
    };
    // Reclaim forget tombstones first: handler construction is the one point where no
    // reservation exists and no drop-time persist can be in flight, so the sweep cannot
    // reopen the resurrection window the tombstones close (#1781). This bounds
    // tombstone growth to one process lifetime.
    let swept = store.sweep_forgotten()?;
    if swept > 0 {
        tracing::debug!(swept, "reclaimed floor-loss tombstones of closed pools");
    }
    for (pool_id, signer, micro, refill_ms) in store.load_buckets()? {
        pool_floor.hydrate_bucket(pool_id, signer, U256::from(micro), refill_ms);
    }
    Ok(pool_floor)
}

/// Every pool's floor accounting, keyed by `pool_id`.
///
/// A newtype rather than a bare `HashMap` because the map is the last way to break
/// the invariant the entries themselves protect: `PoolFloorState::default()` mints a
/// fresh epoch, so a stray `entry(pool).or_default()` would wipe a pool's persisted
/// abandonment buckets and orphan every guard holding the old epoch — their
/// reservations would then never be released. Wrapping the map keeps `Default`
/// reachable only from here, and the operations below are the whole surface.
#[derive(Debug, Default)]
pub(super) struct FloorAccumulator(HashMap<B256, PoolFloorState>);

impl FloorAccumulator {
    /// The pool's live committed floor credit; a pool with no entry has committed
    /// nothing.
    fn live_committed(&self, pool_id: B256) -> U256 {
        self.0
            .get(&pool_id)
            .map_or(U256::ZERO, PoolFloorState::live_committed)
    }

    /// Read the pool's live total and this signer's live slice and bucket, refilling
    /// the bucket in place first.
    ///
    /// Mutates an EXISTING row only — `get_mut`, never `entry` — so a refused
    /// admission leaves no new pool or signer row behind and a client probing a full
    /// pool with fresh signer keys cannot grow the map.
    fn read_refilled(
        &mut self,
        pool_id: B256,
        signer: Address,
        now_ms: u64,
        one_window: U256,
        refill_secs: u64,
    ) -> (U256, U256, U256) {
        let Some(entry) = self.0.get_mut(&pool_id) else {
            return (U256::ZERO, U256::ZERO, U256::ZERO);
        };
        let pool_live = entry.live_committed();
        match entry.signers.get_mut(&signer) {
            Some(lane) => {
                lane.refill(now_ms, one_window, refill_secs);
                (pool_live, lane.live_reservation, lane.bucket_consumed)
            }
            None => (pool_live, U256::ZERO, U256::ZERO),
        }
    }

    /// Charge `amount` of live reservation to `(pool_id, signer)`, creating the pool
    /// entry on its first admission, and return the generation the charge landed in. A
    /// [`FloorReservation`] carries that stamp so it can only reconcile against the
    /// same generation.
    fn charge_live(&mut self, pool_id: B256, signer: Address, amount: U256) -> u64 {
        let entry = self.0.entry(pool_id).or_default();
        entry.charge_live(signer, amount);
        entry.epoch
    }

    /// The entry a reservation guard may reconcile against — see
    /// [`PoolFloorState::entry_for_epoch`].
    fn entry_for_epoch(&mut self, pool_id: B256, epoch: u64) -> Option<&mut PoolFloorState> {
        PoolFloorState::entry_for_epoch(&mut self.0, pool_id, epoch)
    }

    /// Load one persisted bucket row at bring-up.
    fn hydrate_bucket(&mut self, pool_id: B256, signer: Address, consumed: U256, refill_ms: u64) {
        self.0
            .entry(pool_id)
            .or_default()
            .hydrate_bucket(signer, consumed, refill_ms);
    }

    /// Drop a reclaimed pool's whole entry, signer rows and buckets with it.
    fn forget(&mut self, pool_id: B256) {
        self.0.remove(&pool_id);
    }
}

/// One capability signer's slice of a pool's floor accounting (ADR 003 §Pool
/// solvency, per-signer floor isolation). Two independent concerns live here.
///
/// `live_reservation` is the `µUSDC` this signer's in-flight streams currently
/// reserve — un-vouchered floor delivered ahead of payment, released as each stream
/// pays. It is the concurrency/solvency dimension: it carries no memory and never
/// penalizes a signer for quitting.
///
/// `bucket_consumed` + `last_refill_ms` are a per-`(node, signer)` refilling
/// leaky-bucket allowance. A stream that abandons debits the bucket by the REAL
/// un-recouped amount it left behind; the bucket refills over time. Occasional
/// abandonment never drains it, but a burst does, and a drained bucket
/// soft-throttles this signer on this node until it refills. This is node-local and
/// signer-isolated: it never rolls into the pool total, never reduces a co-tenant's
/// budget, and the signer can pull from any other node meanwhile. `last_refill_ms`
/// is unix-epoch millis of the last update (`0` means never touched).
#[derive(Debug, Default, Clone, Copy)]
struct SignerFloorState {
    live_reservation: U256,
    bucket_consumed: U256,
    last_refill_ms: u64,
}

impl SignerFloorState {
    /// A row that carries no information: no live reservation and a drained-to-empty
    /// bucket. An unseen signer reads the same, so such a row can be pruned.
    fn is_empty(self) -> bool {
        self.live_reservation.is_zero() && self.bucket_consumed.is_zero()
    }

    /// Apply the time-refill to the bucket: reduce `bucket_consumed` by
    /// `one_window · elapsed / refill_secs` since `last_refill_ms`, saturating at
    /// zero, and stamp `now_ms`. `one_window` is the `µUSDC` window size at the current
    /// request's rate; `refill_secs` is the seconds to refill one window. A never-
    /// touched row (`last_refill_ms == 0`) just adopts `now_ms`, banking no refill for
    /// time before it existed.
    fn refill(&mut self, now_ms: u64, one_window: U256, refill_secs: u64) {
        if self.last_refill_ms == 0 {
            self.last_refill_ms = now_ms;
            return;
        }
        let elapsed_ms = now_ms.saturating_sub(self.last_refill_ms);
        // `refill = one_window · elapsed_ms / (refill_secs · 1000)`, all in U256 so
        // nothing narrows. `refill_secs.max(1)` and the `· 1000` keep the divisor
        // nonzero, so `wrapping_div` — U256's total division — never divides by zero.
        let denom = U256::from(refill_secs.max(1)).saturating_mul(U256::from(1000u64));
        let refilled = one_window
            .saturating_mul(U256::from(elapsed_ms))
            .wrapping_div(denom);
        self.bucket_consumed = self.bucket_consumed.saturating_sub(refilled);
        self.last_refill_ms = now_ms;
    }

    /// Refill to `now_ms`, then debit the bucket by `amount` `µUSDC` of un-recouped
    /// floor an abandoning stream left behind (saturating).
    fn debit(&mut self, now_ms: u64, amount: U256, one_window: U256, refill_secs: u64) {
        self.refill(now_ms, one_window, refill_secs);
        self.bucket_consumed = self.bucket_consumed.saturating_add(amount);
    }
}

/// One pool's floor accounting (ADR 003 §Pool solvency). `live_reservation` is the
/// `µUSDC` currently reserved by in-flight streams — the hard money envelope, summed
/// across every signer and bounded by `remaining − M`. It is ephemeral: no stream is
/// live at restart, so it clears.
///
/// `signers` holds each capability signer's [`SignerFloorState`]: its slice of
/// `live_reservation` (so admission can bound one signer's concurrent un-vouchered
/// exposure) plus its node-local abandonment bucket. The pool's `live_reservation`
/// stays the sum of the signer slices — every charge and release touches both levels
/// under one lock hold. The buckets do NOT roll up into any pool total: they are
/// per-signer node-local throttles, persisted in a
/// [`decdn_incentive::PoolFloorLossStore`] and reloaded at bring-up.
#[derive(Debug, Clone)]
struct PoolFloorState {
    live_reservation: U256,
    signers: HashMap<Address, SignerFloorState>,
    /// Generation stamp, unique across every entry this process creates. A
    /// [`FloorReservation`] copies it at charge time and both reconcile paths
    /// compare it, so a guard whose pool was reclaimed
    /// ([`ClientHandler::forget_pool_floor`] removed the entry) and whose `pool_id`
    /// a later admission then re-entered reconciles against nothing, rather than
    /// decrementing counters it never contributed to and debiting its bucket against
    /// a signer row belonging to a different pool generation.
    epoch: u64,
}

/// Source of [`PoolFloorState::epoch`]. Process-wide and monotonic, so no two
/// entries share a stamp — including an entry re-created under a `pool_id` a
/// removed entry previously held.
static POOL_FLOOR_EPOCH: AtomicU64 = AtomicU64::new(0);

impl Default for PoolFloorState {
    fn default() -> Self {
        Self {
            live_reservation: U256::ZERO,
            signers: HashMap::new(),
            epoch: POOL_FLOOR_EPOCH.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl PoolFloorState {
    /// Live floor credit committed across every signer on the pool — the pool
    /// solvency quantity, bounded by `remaining − M`. The per-signer abandonment
    /// buckets are node-local and NOT part of this sum.
    const fn live_committed(&self) -> U256 {
        self.live_reservation
    }

    /// Drop a signer's entry once it carries no information — no live reservation and
    /// a drained-to-empty bucket. An unseen signer reads the same way (zero live
    /// reservation, and an absent bucket is available), so keeping such a row changes
    /// no decision.
    ///
    /// Without this the map only ever grows. ADR 003 §Revocation makes short-expiry
    /// session keys the intended usage, so a busy publisher mints signer identities
    /// steadily, and every one that reserves and pays cleanly (and whose bucket has
    /// refilled to zero) would leave an empty row alive until the pool is reclaimed
    /// on-chain — inside a map locked on every admission.
    ///
    /// Safe against a live guard: a repaid guard's `Drop` returns before touching
    /// the map at all, and an unrepaid guard holds `live_reservation > 0` — every
    /// admission reserves a non-zero span at a rate the chain floors above zero, so a
    /// reservation is never zero — and neither can have its row pruned out from
    /// under it. A row whose bucket still holds a debit is not empty, so a drained
    /// signer's throttle is not pruned away either.
    fn prune_spent(&mut self, signer: Address) {
        if self
            .signers
            .get(&signer)
            .is_some_and(|lane| lane.is_empty())
        {
            self.signers.remove(&signer);
        }
    }

    /// Charge `amount` of live reservation to this pool and to `signer`'s row.
    ///
    /// `entry().or_default()`: a charge is the one direction that legitimately creates
    /// a row, and the caller has already cleared every gate. Saturating, so the
    /// counters cannot wrap under a hostile amount.
    fn charge_live(&mut self, signer: Address, amount: U256) {
        self.live_reservation = self.live_reservation.saturating_add(amount);
        let lane = self.signers.entry(signer).or_default();
        lane.live_reservation = lane.live_reservation.saturating_add(amount);
        debug_assert!(self.levels_agree(), "charge_live left the two levels apart");
    }

    /// Release `amount` of live reservation from `signer`'s row and the same quantity
    /// from the pool total, then drop the row if it now carries no information.
    ///
    /// The pool moves by exactly what the row gave up, which is what makes the
    /// invariant hold by construction rather than by argument. Releasing `amount` from
    /// the pool unconditionally would take it out of the co-tenants' share whenever the
    /// row is absent or holds less — and a row IS absent once [`Self::prune_spent`] has
    /// taken it.
    fn release_live(&mut self, signer: Address, amount: U256) {
        let released = self.signers.get_mut(&signer).map_or(U256::ZERO, |lane| {
            let taken = lane.live_reservation.min(amount);
            lane.live_reservation = lane.live_reservation.saturating_sub(taken);
            taken
        });
        self.live_reservation = self.live_reservation.saturating_sub(released);
        self.prune_spent(signer);
        debug_assert!(
            self.levels_agree(),
            "release_live left the two levels apart"
        );
    }

    /// Release `amount` of live reservation and, when `debit` is non-zero, debit the
    /// signer's abandonment bucket by it — returning the bucket snapshot to persist.
    ///
    /// One call because a drop does both under one lock hold, and because the snapshot
    /// must be read after the debit and before [`Self::prune_spent`] can take the row.
    /// A clean or settled exit passes a zero `debit` and gets `None`: an honest
    /// completion is never charged, and skipping the write keeps a sub-interval request
    /// off the store's fsync.
    fn release_and_debit(
        &mut self,
        signer: Address,
        amount: U256,
        debit: U256,
        now_ms: u64,
        one_window: U256,
        refill_secs: u64,
    ) -> Option<(U256, u64)> {
        // `entry()`, not `get_mut()`: the debit must not be lost. The row is present
        // unless `prune_spent` took it, which it cannot while the unrepaid guard
        // calling this holds a non-zero reservation.
        let lane = self.signers.entry(signer).or_default();
        let released = lane.live_reservation.min(amount);
        lane.live_reservation = lane.live_reservation.saturating_sub(released);
        let snap = if debit.is_zero() {
            None
        } else {
            lane.debit(now_ms, debit, one_window, refill_secs);
            Some((lane.bucket_consumed, lane.last_refill_ms))
        };
        self.live_reservation = self.live_reservation.saturating_sub(released);
        self.prune_spent(signer);
        debug_assert!(
            self.levels_agree(),
            "release_and_debit left the two levels apart"
        );
        snap
    }

    /// Load one persisted bucket row at bring-up. No live reservation exists then, so
    /// this moves the bucket only and the pool total stays zero.
    fn hydrate_bucket(&mut self, signer: Address, consumed: U256, refill_ms: u64) {
        let lane = self.signers.entry(signer).or_default();
        lane.bucket_consumed = consumed;
        lane.last_refill_ms = refill_ms;
        debug_assert!(
            self.levels_agree(),
            "hydrate_bucket left the two levels apart"
        );
    }

    /// The invariant every mutator above restores before returning: the stored pool
    /// `live_reservation` is exactly the fold of the per-signer ones.
    ///
    /// The abandonment buckets are deliberately NOT folded. They are node-local,
    /// signer-isolated throttles that never roll into a pool total, so asserting over
    /// them would assert something false.
    fn levels_agree(&self) -> bool {
        self.live_reservation
            == self.signers.values().fold(U256::ZERO, |acc, lane| {
                acc.saturating_add(lane.live_reservation)
            })
    }

    /// The entry a reservation guard may reconcile against: present under `pool_id`
    /// AND stamped with the generation the guard charged.
    ///
    /// A missing entry means the pool was reclaimed
    /// ([`ClientHandler::forget_pool_floor`] removed it) and the live reservation went
    /// with it; re-inserting would resurrect a row for a closed pool that nothing
    /// removes again — the in-memory face of #1781. A present entry with a DIFFERENT
    /// stamp is a later generation, re-entered by an admission that ran after the
    /// remove: subtracting from it would report a reservation this guard never charged
    /// to it, and debiting into it would put this stream's abandonment against a signer
    /// row that outlives the pool it served. Both cases reconcile against nothing.
    fn entry_for_epoch(
        map: &mut HashMap<B256, PoolFloorState>,
        pool_id: B256,
        epoch: u64,
    ) -> Option<&mut Self> {
        map.get_mut(&pool_id).filter(|entry| entry.epoch == epoch)
    }
}

/// RAII hold for one stream's span-capped reservation against a pool's budget.
///
/// Construction (`FloorReservation::reserve`, test-only) charges the reserved
/// amount to the pool's
/// `live_reservation`. The serve loop keeps the current unpaid `µUSDC` updated via
/// [`Self::note_unpaid`], and calls [`Self::release_live_repaid`] once THIS stream's
/// cumulative payment reaches a floor — which frees the live reservation
/// immediately. On drop (every exit path — success, `?`, disconnect, panic) the
/// guard releases the live reservation if it was not already repaid and, on an
/// abnormal exit, debits the signer's node-local abandonment bucket by the REAL
/// un-recouped amount (the last-noted unpaid, capped at the reserved amount), then
/// persists the new bucket snapshot best-effort. A clean/settled exit debits
/// nothing. Mirrors [`super::LaneSlot`]: the reservation is owned by the guard and never
/// adjusted by hand, and every counter update saturates.
#[must_use = "dropping the guard at once releases its live reservation and, on an \
              abnormal exit, debits the signer's abandonment bucket"]
pub(super) struct FloorReservation {
    map: Arc<std::sync::Mutex<FloorAccumulator>>,
    store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
    /// Failure accounting for the best-effort drop-time persist
    /// (`floor_loss_persist_failures`, #1782). Held by the guard because the
    /// persist outlives the serve path that opened it.
    metrics: Arc<Metrics>,
    pool_id: B256,
    /// Generation of the [`PoolFloorState`] this reservation is charged against
    /// (see [`PoolFloorState::epoch`]). Both reconcile paths skip an entry whose
    /// stamp differs: such an entry belongs to a later pool generation, and this
    /// guard's reservation went with the one that was removed.
    epoch: u64,
    /// The capability signer this reservation belongs to. Drop debits this signer's
    /// abandonment bucket and persists its new snapshot, so per-signer isolation
    /// survives a restart.
    signer: Address,
    reserved: U256,
    /// One credit window in `µUSDC` at this stream's rate. Drop uses it to refill the
    /// signer's bucket to the drop instant before debiting.
    one_window: U256,
    /// Seconds to refill one window of the abandonment bucket (node-local policy).
    /// Drop threads it into the refill so a stream's own reservation refills the
    /// bucket at the same rate the admission check does.
    refill_secs: u64,
    /// Set by [`Self::mark_fronted_upstream`] when this stream is a cache-miss fill
    /// that fronts upstream USDC, rather than a direct-serve hit off the local store.
    /// On an abnormal exit a miss debits the FULL reserved window — the upstream spend
    /// the abandon stranded, which the downstream `delivered − paid` tail does not
    /// capture (the pull leg runs a window ahead of downstream payment, and a
    /// pre-delivery abort leaves `unpaid == 0` while upstream USDC is already gone) —
    /// while a hit debits only that real tail. Both go to the refilling bucket, so
    /// neither is permanent; the distinction only sizes the debit correctly.
    fronted_upstream: AtomicBool,
    /// Last-noted unpaid `µUSDC` (`u64`, saturating). Read once at drop to size the
    /// bucket debit.
    unpaid: AtomicU64,
    /// Set by [`Self::release_live_repaid`]; makes drop a no-op (live already freed,
    /// nothing to debit). Idempotent.
    repaid: AtomicBool,
    /// Set by [`Self::mark_settled`] at a stream's CLEAN completion, so drop debits
    /// the abandonment bucket nothing: a stream that delivered its whole request and
    /// paid every interval owes no un-recouped floor. An exit that does NOT call this
    /// (client disconnect, `?`, a voucher rejection, a mid-stream stop) is abnormal
    /// and debits the last-noted unpaid amount — the real floor the abandon left
    /// behind (ADR 003 §Pool solvency, per-signer abandonment allowance).
    settled: AtomicBool,
}

impl FloorReservation {
    /// Reserve `reserved` `µUSDC` of the pool's budget. Charges
    /// `live_reservation += reserved` (saturating) under the sync lock — an O(1) map
    /// touch with no `.await` held, so a blocking lock is correct even on the async
    /// serve path (mirrors `lane_gauge_publish`). A poisoned lock recovers the
    /// guard rather than panicking; the reservation is best-effort accounting, never
    /// a safety gate.
    ///
    /// The standalone increment-then-build form, used only by the accumulator's
    /// own unit tests. The serve path admits through
    /// [`ClientHandler::try_reserve_floor`] instead, which needs the increment to
    /// happen inside its own check-and-reserve lock hold and so builds the guard
    /// via [`Self::new_charged`].
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn reserve(
        map: Arc<std::sync::Mutex<FloorAccumulator>>,
        store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
        metrics: Arc<Metrics>,
        pool_id: B256,
        signer: Address,
        reserved: U256,
        one_window: U256,
        refill_secs: u64,
    ) -> Self {
        let epoch = {
            let mut guard = map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.charge_live(pool_id, signer, reserved)
        };
        Self::new_charged(
            map,
            store,
            metrics,
            pool_id,
            signer,
            reserved,
            one_window,
            refill_secs,
            epoch,
        )
    }

    /// Build a guard for a floor that is ALREADY charged to `live_reservation`
    /// under the caller's own lock hold. This does NOT touch the map — the
    /// increment happens exactly once, at the caller's atomic check-and-reserve,
    /// so re-incrementing here would double-charge the pool. Used by
    /// [`ClientHandler::try_reserve_floor`], whose single lock hold covers both the
    /// budget check and the increment; `FloorReservation::reserve` is the test-only
    /// standalone form that increments first, then delegates here.
    #[allow(clippy::too_many_arguments)]
    fn new_charged(
        map: Arc<std::sync::Mutex<FloorAccumulator>>,
        store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
        metrics: Arc<Metrics>,
        pool_id: B256,
        signer: Address,
        reserved: U256,
        one_window: U256,
        refill_secs: u64,
        epoch: u64,
    ) -> Self {
        Self {
            map,
            store,
            metrics,
            pool_id,
            epoch,
            signer,
            reserved,
            one_window,
            refill_secs,
            fronted_upstream: AtomicBool::new(false),
            unpaid: AtomicU64::new(0),
            repaid: AtomicBool::new(false),
            settled: AtomicBool::new(false),
        }
    }

    /// Mark this stream a cache-miss fill that fronts upstream USDC, so an abnormal
    /// exit debits the full reserved window rather than only the downstream unpaid
    /// tail (see the `fronted_upstream` field). Called on the miss-fill admission path;
    /// a direct-serve hit never calls it. Idempotent.
    pub(super) fn mark_fronted_upstream(&self) {
        self.fronted_upstream.store(true, Ordering::Relaxed);
    }

    /// Mark the stream cleanly completed, so drop debits the abandonment bucket
    /// nothing. Called at the serve loop's clean-completion point — the whole request
    /// delivered and every interval paid, so the last-noted unpaid is already zero.
    /// Any exit that does NOT call this (client disconnect, `?`, a voucher rejection,
    /// a mid-stream stop) is abnormal and debits the real un-recouped amount.
    /// Idempotent.
    pub(super) fn mark_settled(&self) {
        self.settled.store(true, Ordering::Relaxed);
    }

    /// Record the stream's current unpaid `µUSDC`, read at drop to size the
    /// abandonment-bucket debit if the reservation is never repaid. Saturating to
    /// `u64`.
    pub(super) fn note_unpaid(&self, micro: U256) {
        self.unpaid
            .store(micro.saturating_to::<u64>(), Ordering::Relaxed);
    }

    /// Mark the reservation repaid: the lane's cumulative payment reached a floor,
    /// so free the live reservation now (`live_reservation -= reserved`, saturating)
    /// and make the eventual drop a no-op. Idempotent — only the first call moves
    /// the live counter.
    fn release_live_repaid(&self) {
        if self.repaid.swap(true, Ordering::Relaxed) {
            return; // already repaid; live already released
        }
        let mut guard = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Both levels move together, through the one mutator — a release that touched
        // only the pool total would leave this signer's live cap permanently consumed
        // by a stream that paid for it. See [`PoolFloorState::entry_for_epoch`] for why
        // a missing or differently-stamped entry reconciles against nothing.
        if let Some(entry) = guard.entry_for_epoch(self.pool_id, self.epoch) {
            entry.release_live(self.signer, self.reserved);
        }
    }

    /// Release the live reservation once cumulative payment (`paid_micro`) reaches
    /// the amount that was reserved. Matches release to the reserved size at any
    /// `credit_ramp_divisor`: with the ramp disabled the reservation is the full
    /// `credit_max`, so release must wait for that much to be paid rather than a
    /// single chunk. Idempotent (delegates to [`Self::release_live_repaid`]).
    pub(super) fn release_if_repaid(&self, paid_micro: U256) {
        if paid_micro >= self.reserved {
            self.release_live_repaid();
        }
    }

    /// Release the reservation for a serve that was REFUSED before the serve loop
    /// ran — no upstream USDC fronted, no downstream byte delivered. Frees the
    /// live reservation and suppresses the drop-time bucket debit.
    ///
    /// An abnormal drop debits the abandonment bucket by the last-noted unpaid — the
    /// real un-recouped floor a stream left behind. A refusal BEFORE any spend — the
    /// pre-flight floor-`M` gate, the size gate, or an upstream that refused the free
    /// header handshake because its own `getPool` view has not yet caught up to this
    /// pool — fronts nothing and delivers nothing, so its last-noted unpaid is already
    /// zero and its drop would debit nothing. Calling this frees the live reservation
    /// promptly (rather than at drop) and marks the guard repaid so the drop is a
    /// clean no-op. Mechanically a refused-unspent serve and a fully-repaid one both
    /// owe nothing, so this delegates to [`Self::release_live_repaid`]; the distinct
    /// name states the intent at the refusal call sites.
    pub(super) fn release_unspent(&self) {
        self.release_live_repaid();
    }
}

impl Drop for FloorReservation {
    fn drop(&mut self) {
        if self.repaid.load(Ordering::Relaxed) {
            return; // repaid: live already released, nothing to debit
        }
        // Not repaid: release the live reservation and, on an ABNORMAL exit, debit the
        // signer's node-local abandonment bucket by the REAL un-recouped amount the
        // abandon left behind — the last-noted unpaid (`delivered − paid` in µUSDC),
        // capped at the reserved window. A CLEANLY completed stream
        // ([`Self::mark_settled`]) delivered its whole request and paid every interval,
        // so its last-noted unpaid is zero and it debits nothing; a stream that never
        // reached the serve loop (`release_unspent`) fronted nothing and likewise owes
        // nothing. This never folds the full reserved: honest abandonment (a viewer
        // stops, a download is cancelled) costs the bucket only the floor actually left
        // un-recouped, and the bucket refills, so only a BURST of abandonment drains it
        // and soft-throttles this signer on this node (ADR 003 §Pool solvency,
        // per-signer abandonment allowance). All under the sync lock, all saturating —
        // an O(1) update that never blocks the reactor.
        let debit = if self.settled.load(Ordering::Relaxed) {
            U256::ZERO
        } else if self.fronted_upstream.load(Ordering::Relaxed) {
            // A cache-miss abandon strands upstream USDC the pull leg fronted a window
            // ahead of downstream payment; `delivered − paid` under-measures it (it is
            // zero on a pre-delivery abort), so charge the reserved window — the bound
            // on that pull-ahead. Recoverable like any bucket debit.
            self.reserved
        } else {
            // A direct-serve hit fronts no upstream: its whole exposure IS the
            // downstream tail delivered ahead of payment.
            self.reserved
                .min(U256::from(self.unpaid.load(Ordering::Relaxed)))
        };
        let now_ms = now_unix_ms();
        let one_window = self.one_window;
        let refill_secs = self.refill_secs;
        let snapshot = {
            let mut guard = self
                .map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // `get_mut`, not `entry().or_default()`: if the pool was reclaimed
            // (`forget_pool_floor` removed its entry) there is nothing to release —
            // the live reservation went with the entry — and re-inserting would
            // resurrect a row `forget` just deleted. Skip the whole reconcile. The
            // epoch test is the same skip for the harder shape: the `pool_id` is
            // present but stamped as a LATER generation, because an admission
            // re-entered it after the remove. Reconciling there would subtract a
            // reservation that generation never held and debit this stream's abandon
            // against a signer row that outlives the pool this guard served.
            let Some(entry) = guard.entry_for_epoch(self.pool_id, self.epoch) else {
                return;
            };
            // Release and debit under one call: both levels move together, and the
            // bucket snapshot is read after the debit and before `prune_spent` can take
            // the row. A clean or settled exit passes a zero debit and gets `None`.
            entry.release_and_debit(
                self.signer,
                self.reserved,
                debit,
                now_ms,
                one_window,
                refill_secs,
            )
        };
        // A repaid, settled, or unspent stream debits nothing: skip the durable write
        // entirely so a sub-interval request does not fsync a value already on disk.
        // `record_bucket` commits with `Durability::Immediate`, and one redb file
        // takes one writer at a time, so an unconditional write here would put every
        // small paid request behind an fsync on the floor-loss store.
        let Some((consumed, refill_ms)) = snapshot else {
            return;
        };
        // Persist the new bucket snapshot best-effort. The in-memory bucket above is
        // authoritative for the running process; the durable copy only guards a
        // restart, so a lost persist is the documented small crash-window residual —
        // logged and counted, never panicked or propagated. `record_bucket` keeps the
        // greatest-timestamp snapshot per `(pool, signer)`, so two drops on the same
        // lane completing out of order cannot regress the throttle. `record_bucket` may
        // fsync, so offload it to a blocking task when a runtime is available; a drop
        // outside any runtime (e.g. a sync test) records inline.
        let Some(store) = self.store.clone() else {
            return;
        };
        let pool_id = self.pool_id;
        let signer = self.signer;
        let micro = consumed.saturating_to::<u128>();
        let metrics = Arc::clone(&self.metrics);
        // One closure for both dispatch paths so the failure accounting cannot
        // drift between them (#1782): bump `floor_loss_persist_failures`, log the
        // µUSDC total that failed to reach disk, and surface a corrupt payment
        // database at `error!` — after a mid-commit failure redb refuses further
        // writes until the file is closed and reopened, so every later persist
        // fails too and the fix is an operator restart, unlike a transient fault.
        let persist = move || {
            if let Err(e) = store.record_bucket(pool_id, signer, micro, refill_ms) {
                metrics.floor_loss_persist_failure();
                if matches!(e, decdn_incentive::StoreError::Corrupt { .. }) {
                    tracing::error!(
                        %pool_id, %signer, micro, refill_ms, error = %e,
                        "floor abandonment-bucket persist failed: payment store corrupt"
                    );
                } else {
                    tracing::warn!(
                        %pool_id, %signer, micro, refill_ms, error = %e,
                        "floor abandonment-bucket persist failed"
                    );
                }
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn_blocking(persist);
            }
            Err(_) => persist(),
        }
    }
}

/// Unix-epoch milliseconds now, saturating to `u64` and to zero on a pre-epoch
/// clock. The abandonment bucket stamps and refills against this wall clock so a
/// persisted snapshot's `refill_unix_ms` survives a restart; the mild consequence of
/// a clock jump (a slightly early or late refill) matches the throttle's own
/// severity.
fn now_unix_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// Failure accounting for a reclaimed pool's `forget_loss`
/// ([`ClientHandler::forget_pool_floor`]). A failed forget leaves the row in
/// place with NO tombstone, so a drop-dispatched `record_bucket` still in flight
/// can re-insert the closed pool's row and nothing ever deletes it again — the
/// #1781 leak through the error path. Counted under
/// `floor_loss_persist_failures` so `DecdnFloorLossPersistFailures` covers this
/// mode too; a corrupt payment store surfaces at `error!`, mirroring the
/// drop-time persist — redb refuses further writes until the file is reopened,
/// so the remedy is an operator restart.
fn note_forget_outcome(
    metrics: &Metrics,
    pool_id: B256,
    result: Result<Result<(), decdn_incentive::StoreError>, tokio::task::JoinError>,
) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            metrics.floor_loss_persist_failure();
            if matches!(e, decdn_incentive::StoreError::Corrupt { .. }) {
                tracing::error!(
                    %pool_id, error = %e,
                    "pool floor-bucket forget failed: payment store corrupt"
                );
            } else {
                tracing::warn!(%pool_id, error = %e, "pool floor-bucket forget failed");
            }
        }
        Err(e) => {
            metrics.floor_loss_persist_failure();
            tracing::warn!(%pool_id, error = %e, "pool floor-bucket forget join failed");
        }
    }
}

/// Which floor gate refused an admission
/// ([`ClientHandler::try_reserve_floor`]). All collapse to one `NotFound` on the
/// wire; they stay distinct here so the per-reason metric separates "this pool
/// cannot pay" from the two node-local per-signer throttles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FloorRefusal {
    /// The pool-wide ceiling: `remaining − M` cannot cover the live floor
    /// reservation already committed across every signer on the pool plus this new
    /// floor. This is the hard money envelope; it clears with a top-up.
    PoolExhausted,
    /// The per-signer LIVE concurrency cap: the pool can still pay, but THIS signer
    /// already holds `k` windows of live un-vouchered reservation, so admitting
    /// another would let one signer front more concurrent credit than its share of
    /// the pool covers. Carries the cap that refused, read under the same lock hold
    /// as the decision, so the operator line reports the number that actually
    /// applied. Clears as the signer's in-flight streams pay.
    SignerAtCap { signer_cap: U256 },
    /// The per-signer abandonment throttle: this signer's node-local leaky bucket is
    /// drained past capacity, so a burst of abandoned streams has out-run its refill.
    /// Node-local and signer-isolated — the pool is solvent and co-tenants are
    /// unaffected — and temporary: the bucket refills over time, and the signer can
    /// pull from another node meanwhile. Carries the drained level and capacity, read
    /// under the lock hold that made the decision.
    SignerThrottled { consumed: U256, capacity: U256 },
}

/// What a floor-admission refusal needs to describe itself
/// ([`ClientHandler::log_floor_refusal`]). Grouped rather than passed positionally
/// because the two messages read different fields: the pool arm wants the pool's
/// headroom against the reserved `ceiling`, the signer arm wants the signer. The
/// signer's cap is NOT here — it rides on [`FloorRefusal::SignerAtCap`], read under
/// the lock hold that made the decision. `remaining` is the raw `getPool` value, so
/// the callee derives the headroom rather than every call site deriving it.
#[derive(Debug, Clone, Copy)]
pub(super) struct FloorRefusalSite {
    pub pool_id: B256,
    pub signer: Address,
    pub hash: Hash,
    pub remaining: U256,
    /// The `µUSDC` this admission asked to reserve.
    pub ceiling: U256,
}

impl From<FloorRefusal> for ServeRejectReason {
    /// The refusal reason a serve-path admission gate reports for this cap. Keeping
    /// the mapping on the conversion makes an inconsistent cap/reason pairing
    /// unrepresentable at the call sites, exactly as
    /// [`ServeRejectReason::wire_error`] does for the wire codes.
    fn from(refusal: FloorRefusal) -> Self {
        match refusal {
            FloorRefusal::PoolExhausted => Self::InsufficientDeposit,
            FloorRefusal::SignerAtCap { .. } | FloorRefusal::SignerThrottled { .. } => {
                Self::SignerFloorAtCap
            }
        }
    }
}

impl ClientHandler {
    /// Emit the observable side of a floor-admission refusal, picking the message
    /// the refusing gate actually justifies.
    ///
    /// The gates need different words and different remedies.
    /// [`FloorRefusal::PoolExhausted`] is the pool running dry, which
    /// [`Self::log_deposit_refusal`] already describes. The two per-signer gates are
    /// the opposite situation: `try_reserve_floor` tests the pool ceiling FIRST, so
    /// reaching either proves the pool can pay. Reporting one as a deposit shortfall
    /// would print a headroom that visibly exceeds the ceiling beside a sentence
    /// denying it, and would send the operator to check RPC health while the real cause
    /// is one signer's node-local throttle.
    pub(super) fn log_floor_refusal(&self, refusal: FloorRefusal, at: FloorRefusalSite) {
        let FloorRefusalSite {
            pool_id,
            signer,
            hash,
            remaining,
            ceiling,
        } = at;
        let headroom = remaining.saturating_sub(self.pool_min_remaining_deposit);
        match refusal {
            FloorRefusal::PoolExhausted => {
                self.log_deposit_refusal(pool_id, hash, headroom, ceiling);
            }
            FloorRefusal::SignerAtCap { signer_cap } => {
                self.log_signer_at_cap(pool_id, signer, hash, signer_cap, ceiling, headroom);
            }
            FloorRefusal::SignerThrottled { consumed, capacity } => {
                self.log_signer_throttled(pool_id, signer, hash, consumed, capacity, headroom);
            }
        }
    }

    /// The observable side of a per-signer LIVE-cap refusal
    /// ([`FloorRefusal::SignerAtCap`]).
    fn log_signer_at_cap(
        &self,
        pool_id: B256,
        signer: Address,
        hash: Hash,
        signer_cap: U256,
        ceiling: U256,
        headroom: U256,
    ) {
        tracing::debug!(
            %pool_id, %signer, %hash, %signer_cap, %ceiling, %headroom,
            "refusing delivery: this capability signer already holds its live concurrency cap \
             of un-vouchered floor reservation; the pool itself can still pay"
        );
        if let Some(suppressed) = self.note_signer_cap_refusal() {
            tracing::warn!(
                %pool_id, %signer, %signer_cap, %headroom, suppressed,
                interval = ?Self::DEPOSIT_REFUSAL_WARN_INTERVAL,
                "refusing a paying client: one capability signer is running its whole live \
                 concurrency cap of un-vouchered streams at once while the pool is solvent. This \
                 clears as those streams pay; a sustained rate means that signer runs more \
                 concurrent un-vouchered streams than its cap covers. Rotate the session key or \
                 widen pool_floor_signer_live_windows — see docs/runbook.md, which covers why \
                 widening it needs a restart"
            );
        }
    }

    /// The observable side of a per-signer abandonment-throttle refusal
    /// ([`FloorRefusal::SignerThrottled`]).
    fn log_signer_throttled(
        &self,
        pool_id: B256,
        signer: Address,
        hash: Hash,
        consumed: U256,
        capacity: U256,
        headroom: U256,
    ) {
        tracing::debug!(
            %pool_id, %signer, %hash, %consumed, %capacity, %headroom,
            "refusing delivery: this capability signer's node-local abandonment bucket is drained \
             past capacity; the pool itself can still pay and the bucket refills over time"
        );
        if let Some(suppressed) = self.note_signer_throttle_refusal() {
            tracing::warn!(
                %pool_id, %signer, %consumed, %capacity, %headroom, suppressed,
                interval = ?Self::DEPOSIT_REFUSAL_WARN_INTERVAL,
                "temporarily throttling a paying client: one capability signer has drained its \
                 node-local abandonment allowance — a burst of streams abandoned before paying — \
                 while the pool is solvent. This is node-local and signer-scoped: the client simply \
                 pulls from another node, and the bucket refills over time (no operator action \
                 needed). A sustained rate means that signer keeps abandoning streams; widen \
                 pool_floor_signer_bucket_windows or speed pool_floor_signer_refill_secs if the \
                 throttle is too tight — see docs/runbook.md, which covers why either needs a \
                 restart"
            );
        }
    }

    /// Open a span-capped [`FloorReservation`] against `pool_id`'s budget for one
    /// stream. The serve loop holds the returned guard for the stream's lifetime:
    /// it notes the stream's unpaid balance as it delivers and releases the
    /// reservation once a floor is repaid; on an abnormal drop the guard releases the
    /// live reservation and debits the signer's node-local abandonment bucket, in
    /// memory and (best-effort) in the durable [`Self::floor_loss_store`].
    // Test-only: the serve path admits through `try_reserve_floor`, which checks
    // every floor gate and reserves under one lock hold.
    #[cfg(test)]
    pub(super) fn reserve_floor(
        &self,
        pool_id: B256,
        signer: Address,
        reserved: U256,
    ) -> FloorReservation {
        FloorReservation::reserve(
            Arc::clone(&self.pool_floor),
            self.floor_loss_store.clone(),
            Arc::clone(&self.metrics),
            pool_id,
            signer,
            reserved,
            // The standalone test guard treats its reservation as one window: refill
            // over a unit test's lifetime is negligible, so a drop debits the full
            // noted-unpaid amount, which is what the drop-accounting tests assert.
            reserved,
            self.pool_floor_signer_refill_secs,
        )
    }

    /// One credit window in `µUSDC` priced at `rate_per_mb`: the ramp-start credit
    /// window (one chunk normally, the full `credit_max` when
    /// `credit_ramp_divisor == 0`), which is exactly what a fresh stream reserves. It
    /// is the unit the per-signer live cap and abandonment bucket are both denominated
    /// in.
    pub(super) fn one_window(&self, rate_per_mb: u64) -> U256 {
        min_payment(self.credit_window(CHUNK_BYTES, 0), rate_per_mb)
    }

    /// The per-signer LIVE concurrency cap on un-vouchered floor reservation (ADR 003
    /// §Pool solvency, per-signer floor isolation): the most `live_reservation` any
    /// ONE capability signer may hold against this pool, `k · one_window`.
    ///
    /// **The window is the unit being rationed.** A signer's honest need for
    /// un-vouchered floor credit does not scale with the pool's size: every admission
    /// reserves at most one window, and a paying stream releases its reservation as
    /// soon as it covers it, so honest need is `concurrent un-vouchered streams × one
    /// window` whether the pool holds ten dollars or a hundred thousand. An ABSOLUTE
    /// `k`-window cap therefore fits the need directly, rather than a fraction of the
    /// deposit that would be loose in both directions on a large pool.
    ///
    /// `k` is lower-clamped to one window, so a lone signer's first stream is always
    /// admissible on any solvent pool. This carries no permanent memory: it is pure
    /// concurrency, released as each stream pays, so it never penalizes a signer for
    /// quitting — that is the abandonment bucket's job.
    ///
    /// Pure and total (saturating), so it is testable without any chain access.
    pub(super) fn signer_floor_cap(&self, rate_per_mb: u64) -> U256 {
        let one_window = self.one_window(rate_per_mb);
        // Lower-clamp `k` to one window: `k = 0` (or an unset field) still admits a
        // lone signer's first stream rather than wedging it.
        let k = self.pool_floor_signer_live_windows.max(1);
        one_window.saturating_mul(U256::from(k))
    }

    /// The per-signer abandonment-bucket capacity in `µUSDC` at `rate_per_mb`:
    /// `bucket_windows · one_window`, the un-recouped floor a signer may drain before
    /// this node soft-throttles it (ADR 003 §Pool solvency, per-signer abandonment
    /// allowance). Lower-clamped to one window. Pure and total (saturating).
    pub(super) fn signer_bucket_capacity(&self, rate_per_mb: u64) -> U256 {
        let one_window = self.one_window(rate_per_mb);
        let windows = self.pool_floor_signer_bucket_windows.max(1);
        one_window.saturating_mul(U256::from(windows))
    }

    /// POOL solvency: does the pool's `remaining − M` cover its already-committed LIVE
    /// floor reservation across every signer, plus `new_reserve`? Reads the floor
    /// accumulator; pure arithmetic otherwise. The per-signer abandonment buckets are
    /// node-local throttles and are NOT part of this money envelope. A poisoned
    /// accumulator lock recovers the guard rather than panicking: every critical
    /// section over this mutex is panic-free by construction — saturating `U256`
    /// arithmetic and infallible map operations only, no indexing and no `unwrap` — so
    /// a poison cannot originate from a holder of this lock, and a recovered guard
    /// cannot observe a torn pool/signer split. Do NOT add a fallible or panicking
    /// call under this lock; that argument is what the recovery rests on, because the
    /// pool total and the signer entries must agree.
    ///
    /// (The recovery is not justified by the accumulator being "best-effort": it
    /// gates whether the node fronts upstream USDC.)
    ///
    /// Deliberately the pool level ONLY, which is what makes it the right check for a
    /// stream already in flight. The per-signer gates are ADMISSION controls; an
    /// admitted stream's reservation is already counted in the pool total, so leaving
    /// them out of the mid-stream re-check bounds nothing less.
    /// [`Self::try_reserve_floor`] is where every gate applies.
    pub(super) fn pool_budget_covers_reserve(
        &self,
        pool_id: B256,
        remaining: U256,
        new_reserve: U256,
    ) -> bool {
        let committed = {
            let guard = self
                .pool_floor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.live_committed(pool_id)
        };
        decdn_incentive::pool_budget_covers(
            remaining,
            self.pool_min_remaining_deposit,
            committed,
            new_reserve,
        )
    }

    /// Atomically check EVERY floor gate and reserve one `floor` against the pool's
    /// budget, returning the [`FloorReservation`] guard on success or the gate that
    /// refused it.
    ///
    /// Three gates apply, all under ONE `pool_floor` lock hold so two concurrent
    /// admissions cannot both pass a check and then both reserve (the TOCTOU
    /// over-commit a separate check-then-reserve would allow):
    /// 1. **Pool solvency** — `remaining − M` must cover the pool's committed LIVE
    ///    reservation plus this floor. The hard money envelope.
    /// 2. **Per-signer live cap** — this signer's live reservation plus this floor must
    ///    stay within `k · one_window`.
    /// 3. **Per-signer abandonment bucket** — this signer's node-local bucket, refilled
    ///    to now, must not be drained past its capacity.
    ///
    /// The bucket is refilled in place on the existing signer row (an unseen signer's
    /// bucket reads as empty and available, and refilling never creates a row). Only on
    /// success is the row inserted and the reservation charged, so probing a full pool
    /// with fresh signer keys cannot grow the accumulator. The guard is built from the
    /// already-charged state ([`FloorReservation::new_charged`]) so the reserved amount
    /// is charged exactly once. A poisoned lock recovers the guard rather than
    /// panicking (best-effort accounting, never a safety gate).
    pub(super) fn try_reserve_floor(
        &self,
        pool_id: B256,
        signer: Address,
        remaining: U256,
        rate_per_mb: u64,
        reserved: U256,
    ) -> Result<FloorReservation, FloorRefusal> {
        let one_window = self.one_window(rate_per_mb);
        let signer_cap = self.signer_floor_cap(rate_per_mb);
        let capacity = self.signer_bucket_capacity(rate_per_mb);
        let refill_secs = self.pool_floor_signer_refill_secs;
        let now_ms = now_unix_ms();
        let epoch;
        {
            let mut guard = self
                .pool_floor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Reads the pool's live total and this signer's live slice and bucket,
            // refilling the bucket in place. Existing rows only, so a refused admission
            // leaves nothing behind — see [`FloorAccumulator::read_refilled`].
            let (pool_live, signer_live, bucket_consumed) =
                guard.read_refilled(pool_id, signer, now_ms, one_window, refill_secs);
            // Pool ceiling first: it is the solvency bound, and it is what an
            // operator reads as "this pool cannot pay".
            if !decdn_incentive::pool_budget_covers(
                remaining,
                self.pool_min_remaining_deposit,
                pool_live,
                reserved,
            ) {
                return Err(FloorRefusal::PoolExhausted);
            }
            // Per-signer live concurrency cap: one signer cannot hold more than `k`
            // windows of live un-vouchered reservation at once.
            if signer_live.saturating_add(reserved) > signer_cap {
                return Err(FloorRefusal::SignerAtCap { signer_cap });
            }
            // Per-signer abandonment throttle: a bucket drained to (or past) capacity
            // means a burst of abandons has out-run its refill; soft-throttle until it
            // recovers.
            if bucket_consumed >= capacity {
                return Err(FloorRefusal::SignerThrottled {
                    consumed: bucket_consumed,
                    capacity,
                });
            }
            epoch = guard.charge_live(pool_id, signer, reserved);
        }
        Ok(FloorReservation::new_charged(
            Arc::clone(&self.pool_floor),
            self.floor_loss_store.clone(),
            Arc::clone(&self.metrics),
            pool_id,
            signer,
            reserved,
            one_window,
            refill_secs,
            epoch,
        ))
    }

    /// Drop a reclaimed pool's floor accounting: remove its in-memory
    /// `PoolFloorState`, which carries every signer entry (live slice and abandonment
    /// bucket) with it, and every one of its durable bucket rows. Called once when a
    /// pool is reclaimed on-chain; a reclaimed `pool_id` never recurs (monotonic open
    /// nonce), so any bucket a signer drained against it is permanently moot.
    ///
    /// A [`FloorReservation`] drop that snapshotted its bucket before the in-memory
    /// remove here can still have its `record_bucket` in flight when the durable
    /// delete commits. The store closes that window, not this method:
    /// `forget_loss` tombstones the pool id — pool-wide, covering every signer on
    /// it — so the late write is a no-op instead of resurrecting a row for a closed
    /// pool that nothing would ever delete again (#1781).
    pub(crate) async fn forget_pool_floor(&self, pool_id: B256) {
        {
            let mut guard = self
                .pool_floor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.forget(pool_id);
        }
        let Some(store) = self.floor_loss_store.clone() else {
            return;
        };
        let result = tokio::task::spawn_blocking(move || store.forget_loss(pool_id)).await;
        note_forget_outcome(&self.metrics, pool_id, result);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::tests::{
        handler_for_tests, handler_for_tests_with_floor_store, handler_for_tests_with_signer_policy,
    };
    use super::*;

    /// The capability signer every floor-accumulator unit test reserves under.
    /// A second signer (`TEST_SIGNER_B`) exercises per-signer isolation.
    const TEST_SIGNER: Address = Address::new([0xa1u8; 20]);
    /// A distinct co-tenant on the same pool.
    const TEST_SIGNER_B: Address = Address::new([0xb2u8; 20]);
    /// The advertised `µUSDC`/MB rate the floor-cap tests price against. Only the
    /// one-credit-window clamp in [`ClientHandler::signer_floor_cap`] reads it.
    const TEST_RATE: u64 = 1_000;

    /// Lock the floor accumulator for a test assertion, surfacing a poisoned lock as
    /// an `anyhow` error rather than panicking (the anti-panic policy holds in tests).
    fn lock_floor(
        map: &Arc<std::sync::Mutex<FloorAccumulator>>,
    ) -> anyhow::Result<std::sync::MutexGuard<'_, FloorAccumulator>> {
        map.lock()
            .map_err(|e| anyhow::anyhow!("floor map poisoned: {e}"))
    }

    /// An abnormal exit — the guard drops without [`FloorReservation::mark_settled`],
    /// as on a client disconnect mid-stream — debits the signer's abandonment bucket
    /// by the REAL un-recouped amount it left behind (the last-noted unpaid), and frees
    /// the live reservation. It never debits the full reserved: honest abandonment
    /// costs the bucket only the floor actually un-recouped. No tokio runtime is
    /// present, so `Drop` persists synchronously via the direct-call fallback.
    #[test]
    fn floor_reservation_debits_real_unrecouped_on_abnormal_drop() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<FloorAccumulator>> =
            Arc::new(std::sync::Mutex::new(FloorAccumulator::default()));
        let store = Arc::new(decdn_incentive::MemoryPoolFloorLossStore::new());
        let pool = B256::repeat_byte(0x5A);
        let floor = decdn_incentive::floor_micro(1000);
        let quarter = floor / U256::from(4u64);
        {
            let res = FloorReservation::reserve(
                map.clone(),
                Some(store.clone()),
                Arc::new(Metrics::new()),
                pool,
                TEST_SIGNER,
                floor,
                floor, // one_window
                60,    // refill_secs
            );
            // Live reservation is held while the guard lives.
            let live = lock_floor(&map)?.0.get(&pool).map(|s| s.live_reservation);
            anyhow::ensure!(
                live == Some(floor),
                "live reservation is held while the guard lives"
            );
            // Stream delivered a quarter-floor ahead of payment, then vanished (never
            // settled): that quarter is the real un-recouped floor.
            res.note_unpaid(quarter);
        } // drop → live released, bucket debited by min(reserved, unpaid) = floor/4
        let st = lock_floor(&map)?.0.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation == U256::ZERO,
            "live reservation is released on drop"
        );
        anyhow::ensure!(
            st.signers.get(&TEST_SIGNER).map(|s| s.bucket_consumed) == Some(quarter),
            "an abnormal exit debits the signer bucket by the real un-recouped amount"
        );
        let persisted = persisted_bucket(&*store, pool)?;
        anyhow::ensure!(
            persisted == Some(quarter.to::<u128>()),
            "the new bucket level is persisted best-effort on drop"
        );
        Ok(())
    }

    /// A CLEANLY completed stream ([`FloorReservation::mark_settled`]) delivered its
    /// whole request and paid every interval, so its last-noted unpaid is zero and its
    /// drop debits the abandonment bucket NOTHING — an honest completion is never
    /// charged.
    #[test]
    fn floor_reservation_settled_exit_debits_nothing() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<FloorAccumulator>> =
            Arc::new(std::sync::Mutex::new(FloorAccumulator::default()));
        let pool = B256::repeat_byte(0x5B);
        let floor = decdn_incentive::floor_micro(1000);
        {
            let res = FloorReservation::reserve(
                map.clone(),
                None,
                Arc::new(Metrics::new()),
                pool,
                TEST_SIGNER,
                floor,
                floor,
                60,
            );
            // Fully delivered and fully paid: unpaid noted at zero, then settled.
            res.note_unpaid(U256::ZERO);
            res.mark_settled();
        } // drop → live released, bucket untouched
        let st = lock_floor(&map)?.0.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation == U256::ZERO && st.signers.is_empty(),
            "a settled stream releases its reservation and debits the bucket nothing, \
             so its now-empty row is pruned"
        );
        Ok(())
    }

    /// A serve REFUSED before the serve loop ran — [`FloorReservation::release_unspent`]
    /// called on the pre-spend refusal paths (the floor-`M` gate, the size gate, an
    /// upstream that refused the free header handshake) — frees the live reservation
    /// and debits the abandonment bucket NOTHING: it fronted no USDC and delivered no
    /// byte, so its last-noted unpaid is zero. A transient upstream stumble therefore
    /// never throttles an innocent signer.
    #[test]
    fn floor_reservation_refused_unspent_debits_nothing() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<FloorAccumulator>> =
            Arc::new(std::sync::Mutex::new(FloorAccumulator::default()));
        let pool = B256::repeat_byte(0x5C);
        let floor = decdn_incentive::floor_micro(1000);
        {
            let res = FloorReservation::reserve(
                map.clone(),
                None,
                Arc::new(Metrics::new()),
                pool,
                TEST_SIGNER,
                floor,
                floor,
                60,
            );
            let live = lock_floor(&map)?.0.get(&pool).map(|s| s.live_reservation);
            anyhow::ensure!(
                live == Some(floor),
                "live reservation is held while the guard lives"
            );
            // Refused before any spend — release cleanly, never marked settled.
            res.release_unspent();
        } // drop → no-op: release_unspent already freed the live reservation
        let st = lock_floor(&map)?.0.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation == U256::ZERO && st.signers.is_empty(),
            "a pre-spend refusal releases its reservation and debits no bucket, so its \
             row is pruned"
        );
        Ok(())
    }

    /// The pool-budget guard counts a pool's committed LIVE floor reservation against
    /// `remaining − M`. This is the re-check both the mid-stream gate and the
    /// direct-serve gate apply to a stream that already holds its reservation, so it is
    /// deliberately pool-level only, carries no signer dimension, and never counts a
    /// signer's node-local abandonment bucket.
    #[tokio::test]
    async fn pool_budget_covers_reserve_accounts_committed_floor() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await; // M = 0
        let pool = B256::repeat_byte(0x11);
        let floor = decdn_incentive::floor_micro(1_000_000);
        // Empty pool, M = 0: remaining must cover the new reserve exactly.
        anyhow::ensure!(
            handler.pool_budget_covers_reserve(pool, floor, floor),
            "remaining equal to the reserve is covered"
        );
        anyhow::ensure!(
            !handler.pool_budget_covers_reserve(
                pool,
                floor.saturating_sub(U256::from(1u64)),
                floor
            ),
            "remaining one below the reserve is not covered"
        );
        // A live reservation consumes the budget: the same remaining no longer
        // covers a second identical reserve.
        let _guard = handler.reserve_floor(pool, TEST_SIGNER, floor);
        anyhow::ensure!(
            !handler.pool_budget_covers_reserve(pool, floor, floor),
            "an in-flight floor reservation is committed against the budget"
        );
        // The signer dimension is absent by design: a co-tenant's reserve is
        // counted here just the same, because this asks only whether the POOL can
        // still pay.
        anyhow::ensure!(
            !handler.pool_budget_covers_reserve(pool, floor, floor),
            "the pool-level re-check does not vary with the signer"
        );
        Ok(())
    }

    /// `try_reserve_floor` reserves atomically: it charges the budget only when
    /// `remaining − M` covers the pool's committed live reservation plus the new floor,
    /// and the charge is visible to the very next call so a second reserve on an
    /// exhausted pool is refused. The no-op per-signer gates keep the pool ceiling the
    /// only bound under test.
    #[tokio::test]
    async fn try_reserve_floor_charges_only_when_budget_covers() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await; // M = 0
        let pool = B256::repeat_byte(0x22);
        let floor = decdn_incentive::floor_micro(1_000_000);
        // Budget covers exactly one floor: the first reserve succeeds.
        let first = handler.try_reserve_floor(pool, TEST_SIGNER, floor, TEST_RATE, floor);
        anyhow::ensure!(first.is_ok(), "a floor within remaining − M is reserved");
        // The charge is live: a second identical reserve against the SAME remaining
        // now sees `committed = floor` and is refused (no over-commit).
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, floor, TEST_RATE, floor)
                .err()
                == Some(FloorRefusal::PoolExhausted),
            "a second reserve over the same budget is refused as pool-exhausted"
        );
        // A CLEANLY completed stream (marked settled, nothing unpaid) frees its live
        // reservation on drop, reopening the budget. (Live reservation is what the pool
        // budget counts; the per-signer abandonment bucket never enters this check —
        // see `floor_reservation_debits_real_unrecouped_on_abnormal_drop`.)
        if let Ok(g) = first.as_ref() {
            g.mark_settled();
        }
        drop(first);
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, floor, TEST_RATE, floor)
                .is_ok(),
            "budget reopens once a cleanly-settled reservation is released"
        );
        Ok(())
    }

    /// The per-signer live cap isolates co-tenants of one shared pool: a signer that
    /// holds its `k`-window cap of live reservation is refused `SignerAtCap` while the
    /// pool can still pay, and a SECOND signer is admitted from its own cap at the same
    /// instant. Without the signer dimension the first signer's reservations would be
    /// the pool's, and the second would be refused too.
    #[tokio::test]
    async fn signer_live_cap_refuses_one_signer_and_admits_a_co_tenant() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        // k = 1 window each, a bottomless bucket, M = 0.
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 1, u64::MAX, 60).await;
        let pool = B256::repeat_byte(0x31);
        let window = handler.one_window(TEST_RATE);
        let remaining = window.saturating_mul(U256::from(4u64));
        anyhow::ensure!(
            handler.signer_floor_cap(TEST_RATE) == window,
            "k = 1 gives a one-window live cap"
        );
        let held = handler.try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, window);
        anyhow::ensure!(held.is_ok(), "the first window fits inside the signer cap");
        // Signer A is at its cap. The POOL is not — three windows of headroom are
        // untouched — so the refusal must name the signer cap, not the pool.
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, window)
                .err()
                .is_some_and(|e| matches!(e, FloorRefusal::SignerAtCap { .. })),
            "a second window on the SAME signer exceeds its cap while the pool can still pay"
        );
        // A co-tenant draws on its own untouched cap.
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER_B, remaining, TEST_RATE, window)
                .is_ok(),
            "a distinct signer is admitted from its own cap while the first is capped"
        );
        Ok(())
    }

    /// The per-pool ceiling still bounds the AGGREGATE across signers: solvency
    /// cannot be escaped by spraying identities. With k = 1 window each, four signers
    /// each take their own window on a four-window pool, none exceeding its live cap,
    /// and the fifth is refused `PoolExhausted` — the pool, not the signer, ran out.
    #[tokio::test]
    async fn pool_ceiling_still_bounds_the_aggregate_across_signers() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 1, u64::MAX, 60).await;
        let pool = B256::repeat_byte(0x32);
        let window = handler.one_window(TEST_RATE);
        let remaining = window.saturating_mul(U256::from(4u64));
        let mut held = Vec::new();
        for i in 0u8..4 {
            let signer = Address::new([i.saturating_add(1); 20]);
            let guard = handler
                .try_reserve_floor(pool, signer, remaining, TEST_RATE, window)
                .map_err(|e| anyhow::anyhow!("signer {i} refused: {e:?}"))?;
            held.push(guard);
        }
        // Every one of the four sits at exactly its own window, so no live cap is
        // exceeded; what refuses the fifth is the pool ceiling.
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, Address::new([0xee; 20]), remaining, TEST_RATE, window)
                .err()
                == Some(FloorRefusal::PoolExhausted),
            "signer fan-out cannot push the aggregate past remaining − M"
        );
        Ok(())
    }

    /// Assert the accumulator's load-bearing invariant on one pool: the stored pool
    /// `live_reservation` is exactly the fold of the per-signer `live_reservation`s.
    ///
    /// Deliberately the LIVE quantity only. The abandonment buckets are node-local and
    /// signer-isolated — [`PoolFloorState::live_committed`] says so — so they are not
    /// part of any pool total and folding them here would assert something false.
    fn ensure_floor_levels_agree(
        handler: &ClientHandler,
        pool: B256,
        when: &str,
    ) -> anyhow::Result<()> {
        let st = lock_floor(&handler.pool_floor)?
            .0
            .get(&pool)
            .cloned()
            .ok_or_else(|| {
                // Not `unwrap_or_default()`: an empty state folds `0 == 0` and would
                // report success for a regression that REMOVED the entry.
                anyhow::anyhow!("{when}: the pool entry is gone, so there is nothing to agree")
            })?;
        let folded = st.signers.values().fold(U256::ZERO, |acc, lane| {
            acc.saturating_add(lane.live_reservation)
        });
        anyhow::ensure!(
            st.live_reservation == folded,
            "{when}: pool live_reservation ({}) must stay the sum of its signer rows ({folded})",
            st.live_reservation
        );
        Ok(())
    }

    /// K threads race ONE admission through the pool ceiling: exactly one wins.
    ///
    /// [`ClientHandler::try_reserve_floor`] claims its gates and its
    /// `live_reservation` increments happen under ONE lock hold, so concurrent
    /// admissions cannot both pass a check and then both reserve. Every other floor
    /// test is sequential, so nothing else exercises that claim: a check-then-reserve
    /// split would still pass them all and over-commit only under contention.
    ///
    /// Each racer names a DISTINCT signer with a live cap wide enough never to bind,
    /// so the pool ceiling is unambiguously what refuses. Detection is probabilistic
    /// and the suite retries — the barrier makes the racers collide, it does not
    /// guarantee they land in the same stale window — so read a FLAKY line here as the
    /// defect signal `.config/nextest.toml` says it is.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_admissions_cannot_over_commit_the_pool_ceiling() -> anyhow::Result<()> {
        const RACERS: usize = 8;
        let metrics = Arc::new(Metrics::new());
        // A live cap and bucket far too wide to bind, so only the pool ceiling can.
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, u64::MAX, u64::MAX, 60)
                .await;
        let pool = B256::repeat_byte(0x3A);
        let window = handler.one_window(TEST_RATE);
        // Headroom for EXACTLY one window: slack strictly under a second.
        let remaining = window.saturating_add(U256::from(1u64));
        let barrier = std::sync::Barrier::new(RACERS);

        let outcomes: Vec<Result<FloorReservation, FloorRefusal>> = std::thread::scope(|scope| {
            let racers: Vec<_> = (0..RACERS)
                .map(|i| {
                    let handler = Arc::clone(&handler);
                    let barrier = &barrier;
                    scope.spawn(move || {
                        let signer = Address::new([u8::try_from(i).unwrap_or(0xff); 20]);
                        barrier.wait();
                        handler.try_reserve_floor(pool, signer, remaining, TEST_RATE, window)
                    })
                })
                .collect();
            racers
                .into_iter()
                .map(|h| h.join().map_err(|_| anyhow::anyhow!("racer panicked")))
                .collect::<anyhow::Result<Vec<_>>>()
        })?;

        let admitted = outcomes.iter().filter(|o| o.is_ok()).count();
        anyhow::ensure!(
            admitted == 1,
            "exactly one of {RACERS} concurrent admissions fits the one-window ceiling, \
             got {admitted}"
        );
        anyhow::ensure!(
            outcomes
                .iter()
                .filter_map(|o| o.as_ref().err())
                .all(|e| matches!(e, FloorRefusal::PoolExhausted)),
            "the losers are refused by the POOL ceiling, not a per-signer gate"
        );
        ensure_floor_levels_agree(&handler, pool, "after the race")?;
        drop(outcomes);
        ensure_floor_levels_agree(&handler, pool, "after every guard drops")?;
        Ok(())
    }

    /// The same race against ONE signer's live cap, on a pool the ceiling cannot bind:
    /// exactly one admission wins and the losers name the per-signer cap.
    ///
    /// The pool-ceiling twin cannot cover this. The two are separate tests under the
    /// same lock hold, and a check-then-reserve split on the signer arm alone would let
    /// two streams past one `k`-window cap while the pool stayed solvent.
    ///
    /// Only these two gates can race. The abandonment bucket is checked but never
    /// charged at admission (`bucket_consumed >= capacity` is a read), so concurrent
    /// admissions all observe the same level and there is no over-commit to expose —
    /// the TOCTOU claim bites exactly on the gates that mutate.
    ///
    /// Detection is probabilistic and the suite retries; see the twin above.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_admissions_cannot_over_commit_one_signers_live_cap() -> anyhow::Result<()> {
        const RACERS: usize = 8;
        let metrics = Arc::new(Metrics::new());
        // k = 1 window per signer; the bucket cannot bind.
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 1, u64::MAX, 60).await;
        let pool = B256::repeat_byte(0x3B);
        let window = handler.one_window(TEST_RATE);
        // Eight windows of pool headroom against a one-window signer cap, so the pool
        // ceiling has room for every racer and only the signer cap can refuse.
        let remaining = window.saturating_mul(U256::from(8u64));
        anyhow::ensure!(
            handler.signer_floor_cap(TEST_RATE) == window,
            "the per-signer live cap, not the pool, is the binding bound here"
        );
        let barrier = std::sync::Barrier::new(RACERS);

        let outcomes: Vec<Result<FloorReservation, FloorRefusal>> = std::thread::scope(|scope| {
            let racers: Vec<_> = (0..RACERS)
                .map(|_| {
                    let handler = Arc::clone(&handler);
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        handler.try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, window)
                    })
                })
                .collect();
            racers
                .into_iter()
                .map(|h| h.join().map_err(|_| anyhow::anyhow!("racer panicked")))
                .collect::<anyhow::Result<Vec<_>>>()
        })?;

        let admitted = outcomes.iter().filter(|o| o.is_ok()).count();
        anyhow::ensure!(
            admitted == 1,
            "exactly one of {RACERS} concurrent admissions fits the one-window live cap, \
             got {admitted}"
        );
        anyhow::ensure!(
            outcomes
                .iter()
                .filter_map(|o| o.as_ref().err())
                .all(|e| matches!(e, FloorRefusal::SignerAtCap { .. })),
            "the losers are refused by the per-signer LIVE cap while the pool can still pay"
        );
        ensure_floor_levels_agree(&handler, pool, "after the race")?;
        drop(outcomes);
        ensure_floor_levels_agree(&handler, pool, "after every guard drops")?;
        Ok(())
    }

    /// An admitted stream survives its pool draining below what its own signer could
    /// now be admitted for, as long as the POOL itself stays solvent.
    ///
    /// [`ClientHandler::pool_budget_covers_reserve`] is deliberately the pool level
    /// only: the per-signer gates are ADMISSION controls, and an admitted stream's
    /// reservation is already counted in the pool total, so re-testing it against them
    /// mid-stream would terminate a paying stream on a pool that can still pay while
    /// bounding nothing extra. This pins that choice — adding a per-signer arm to the
    /// re-check fails here.
    #[tokio::test]
    async fn an_admitted_stream_survives_a_mid_stream_recheck_on_a_solvent_pool()
    -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 1, u64::MAX, 60).await;
        let pool = B256::repeat_byte(0x3C);
        let window = handler.one_window(TEST_RATE);
        let wide = window.saturating_mul(U256::from(8u64));
        let _admitted = handler
            .try_reserve_floor(pool, TEST_SIGNER, wide, TEST_RATE, window)
            .map_err(|e| anyhow::anyhow!("admission refused: {e:?}"))?;

        // This signer is now AT its live cap: a fresh admission would be refused.
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, wide, TEST_RATE, window)
                .err()
                == Some(FloorRefusal::SignerAtCap { signer_cap: window }),
            "the setup must actually leave the signer at its cap"
        );
        // The already-admitted stream keeps serving anyway, because the pool can pay.
        anyhow::ensure!(
            handler.pool_budget_covers_reserve(pool, wide, U256::ZERO),
            "the mid-stream re-check reads the POOL level only, so a solvent pool keeps \
             serving a stream whose signer could no longer be admitted"
        );
        // Non-vacuous in the other direction: an insolvent pool does refuse.
        anyhow::ensure!(
            !handler.pool_budget_covers_reserve(pool, U256::ZERO, U256::ZERO),
            "an insolvent pool still fails the mid-stream re-check"
        );
        Ok(())
    }

    /// Bring-up hydration restores each signer's OWN abandonment bucket. Two signers on
    /// one pool carry different persisted bucket levels across the restart: the one
    /// whose bucket is drained past capacity is refused `SignerThrottled`, while its
    /// co-tenant — whose bucket has room — is admitted at the same instant. Dropping
    /// the per-signer hydration would grant every session key a fresh allowance, the
    /// withhold-then-restart escape the persistence exists to close.
    #[tokio::test]
    async fn restart_hydration_restores_each_signers_own_bucket() -> anyhow::Result<()> {
        use decdn_incentive::PoolFloorLossStore as _;
        let metrics = Arc::new(Metrics::new());
        let pool = B256::repeat_byte(0x37);
        let store = Arc::new(decdn_incentive::store::MemoryPoolFloorLossStore::new());
        // A large refill_secs and a fresh recent timestamp keep the loaded buckets from
        // refilling meaningfully during the test.
        let now_ms = now_unix_ms();
        // Bucket capacity below will be 2 windows (bucket_windows = 2). Seed A at 3
        // windows (drained past capacity) and B at half a window (room to spare). The
        // window value is fixed by the rate, so compute it from a throwaway handler.
        let (probe, _pd) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, u64::MAX, 2, u64::MAX).await;
        let window = probe.one_window(TEST_RATE);
        let three_windows = window.saturating_mul(U256::from(3u64)).to::<u128>();
        let half_window = (window / U256::from(2u64)).to::<u128>();
        store
            .record_bucket(pool, TEST_SIGNER, three_windows, now_ms)
            .map_err(|e| anyhow::anyhow!("seed A: {e}"))?;
        store
            .record_bucket(pool, TEST_SIGNER_B, half_window, now_ms)
            .map_err(|e| anyhow::anyhow!("seed B: {e}"))?;

        // bucket_windows = 2 (capacity 2 windows), never-refilling.
        let (handler, _dir) = handler_for_tests_with_floor_store(
            &metrics,
            2,
            u64::MAX,
            store as Arc<dyn decdn_incentive::PoolFloorLossStore>,
        )
        .await;
        let remaining = window.saturating_mul(U256::from(8u64));
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, window)
                .err()
                .is_some_and(|e| matches!(e, FloorRefusal::SignerThrottled { .. })),
            "the signer whose bucket restarted drained past capacity is throttled"
        );
        let admitted = handler.try_reserve_floor(pool, TEST_SIGNER_B, remaining, TEST_RATE, window);
        anyhow::ensure!(
            admitted.is_ok(),
            "its co-tenant's own hydrated bucket has room, so the pool still serves it"
        );
        drop(admitted);
        Ok(())
    }

    /// A refused admission inserts nothing at either level. This is what stops a
    /// client probing a full pool with throwaway signer keys from growing a map that
    /// is locked on every admission — reading through `entry().or_default()` instead
    /// of `get` would make every refusal a permanent row.
    #[tokio::test]
    async fn a_refused_admission_leaves_no_row_at_either_level() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 1, u64::MAX, 60).await;
        let pool = B256::repeat_byte(0x38);
        let window = handler.one_window(TEST_RATE);
        let floor = window;
        let remaining = window.saturating_mul(U256::from(4u64));

        // Refused by the POOL ceiling, on a pool with no entry at all.
        anyhow::ensure!(
            handler
                .try_reserve_floor(
                    pool,
                    TEST_SIGNER,
                    floor.saturating_sub(U256::from(1u64)),
                    TEST_RATE,
                    floor
                )
                .is_err(),
            "a pool that cannot cover one floor refuses"
        );
        anyhow::ensure!(
            !lock_floor(&handler.pool_floor)?.0.contains_key(&pool),
            "a pool-ceiling refusal creates no pool entry"
        );

        // Now admit one signer, then spray fresh signer keys past the pool ceiling.
        let mut held = Vec::new();
        for i in 0u8..4 {
            held.push(
                handler
                    .try_reserve_floor(
                        pool,
                        Address::new([i.saturating_add(1); 20]),
                        remaining,
                        TEST_RATE,
                        floor,
                    )
                    .map_err(|e| anyhow::anyhow!("signer {i} refused: {e:?}"))?,
            );
        }
        for i in 0u8..8 {
            anyhow::ensure!(
                handler
                    .try_reserve_floor(
                        pool,
                        Address::new([i.saturating_add(0xc0); 20]),
                        remaining,
                        TEST_RATE,
                        floor
                    )
                    .is_err(),
                "prober {i} is refused on a full pool"
            );
        }
        let signers = lock_floor(&handler.pool_floor)?
            .0
            .get(&pool)
            .map(|s| s.signers.len());
        anyhow::ensure!(
            signers == Some(4),
            "only the four admitted signers hold rows; eight refused probes added none"
        );
        Ok(())
    }

    /// A guard whose pool was reclaimed reconciles against nothing, even when a
    /// later admission has re-entered the same `pool_id`. Without the generation
    /// stamp the stale drop finds the NEW entry, subtracts a reservation that entry
    /// never held, and debits its bucket against a signer row belonging to the new
    /// pool — leaving the pool total below the sum of its signer rows, which is the
    /// direction that over-admits.
    #[test]
    fn a_stale_guard_does_not_reconcile_against_a_re_entered_pool() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<FloorAccumulator>> =
            Arc::new(std::sync::Mutex::new(FloorAccumulator::default()));
        let pool = B256::repeat_byte(0x39);
        let floor = decdn_incentive::floor_micro(1000);

        let stale = FloorReservation::reserve(
            Arc::clone(&map),
            None,
            Arc::new(Metrics::new()),
            pool,
            TEST_SIGNER,
            floor,
            floor,
            60,
        );
        // The stale guard abandoned a stream: note some un-recouped floor so its drop
        // would try to debit a bucket, which the epoch guard must then suppress.
        stale.note_unpaid(floor);
        // The pool is reclaimed on-chain: its whole entry goes, signer rows and all.
        lock_floor(&map)?.0.remove(&pool);
        // A later admission re-enters the same key — the cached `getPool` view can
        // still show headroom for a moment after the reclaim lands.
        let fresh = FloorReservation::reserve(
            Arc::clone(&map),
            None,
            Arc::new(Metrics::new()),
            pool,
            TEST_SIGNER_B,
            floor,
            floor,
            60,
        );
        drop(stale);

        let st = lock_floor(&map)?.0.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation == floor,
            "the stale drop must not release the new entry's live reservation"
        );
        anyhow::ensure!(
            st.signers.len() == 1 && st.signers.contains_key(&TEST_SIGNER_B),
            "the stale drop must not insert its own signer row under the new entry"
        );
        anyhow::ensure!(
            st.signers
                .get(&TEST_SIGNER_B)
                .is_some_and(|s| s.bucket_consumed.is_zero()),
            "the stale drop must not debit its abandon against the new entry's signer"
        );
        anyhow::ensure!(
            st.live_reservation
                == st
                    .signers
                    .values()
                    .fold(U256::ZERO, |acc, s| acc.saturating_add(s.live_reservation)),
            "the pool live total stays the sum of its signer rows"
        );
        drop(fresh);
        Ok(())
    }

    /// One signer's abandonment bucket is debited to its own entry alone, so a burst
    /// of abandons throttles that signer without touching any co-tenant's allowance.
    #[test]
    fn one_signers_abandonment_bucket_is_isolated() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<FloorAccumulator>> =
            Arc::new(std::sync::Mutex::new(FloorAccumulator::default()));
        let pool = B256::repeat_byte(0x33);
        let floor = decdn_incentive::floor_micro(1000);
        {
            // Signer A abandons a stream after delivering the whole floor un-paid:
            // that real un-recouped floor is debited to A's bucket.
            let res = FloorReservation::reserve(
                map.clone(),
                None,
                Arc::new(Metrics::new()),
                pool,
                TEST_SIGNER,
                floor,
                floor,
                60,
            );
            res.note_unpaid(floor);
        }
        let st = lock_floor(&map)?.0.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation.is_zero(),
            "the abandoned stream releases its live reservation"
        );
        anyhow::ensure!(
            st.signers.get(&TEST_SIGNER).map(|s| s.bucket_consumed) == Some(floor),
            "the abandoned floor is debited to the abandoning signer's own bucket"
        );
        anyhow::ensure!(
            !st.signers.contains_key(&TEST_SIGNER_B),
            "a co-tenant's bucket is untouched by another signer's abandon"
        );
        Ok(())
    }

    /// The live cap is `k · one_window`, lower-clamped to one window: `k = 0` (or an
    /// unset field) still admits a lone signer's first stream on any solvent pool
    /// rather than wedging it, and the cap is an ABSOLUTE window count — it does not
    /// scale with the pool's deposit.
    #[tokio::test]
    async fn signer_floor_cap_is_k_windows_lower_clamped_to_one() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        // k = 0 → lower-clamped to one window.
        let (clamped, _c) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 0, u64::MAX, 60).await;
        let one_window = clamped.one_window(TEST_RATE);
        anyhow::ensure!(
            clamped.signer_floor_cap(TEST_RATE) == one_window,
            "k = 0 clamps the live cap up to one window"
        );
        let pool = B256::repeat_byte(0x34);
        let remaining = one_window.saturating_mul(U256::from(2u64));
        anyhow::ensure!(
            clamped
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, one_window)
                .is_ok(),
            "a lone signer's first window is admitted even at k = 0"
        );
        // k = 8 → cap is eight windows, the same however large the pool's deposit.
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 8, u64::MAX, 60).await;
        let eight = one_window.saturating_mul(U256::from(8u64));
        anyhow::ensure!(
            handler.signer_floor_cap(TEST_RATE) == eight,
            "k = 8 gives an eight-window cap"
        );
        Ok(())
    }

    /// Headroom below one credit window: the one-window lower clamp then returns a
    /// live cap LARGER than the pool's entire headroom, and only the pool ceiling
    /// running FIRST keeps the node from admitting past the refundable minimum `M` the
    /// pool owner is guaranteed. Pins that order — the refusal must be `PoolExhausted`,
    /// never `Ok`, at any `k`.
    #[tokio::test]
    async fn pool_ceiling_refuses_below_one_window_whatever_the_cap() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        for k in [0u64, 1, 8] {
            let (handler, _dir) =
                handler_for_tests_with_signer_policy(&metrics, U256::ZERO, k, u64::MAX, 60).await;
            let pool = B256::repeat_byte(0x36);
            let one_window =
                decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
            let remaining = one_window.saturating_sub(U256::from(1u64));
            anyhow::ensure!(
                handler.signer_floor_cap(TEST_RATE) > remaining,
                "k {k}: the one-window lower clamp exceeds the headroom, which is what makes \
                 the check order load-bearing"
            );
            anyhow::ensure!(
                handler
                    .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, one_window)
                    .err()
                    == Some(FloorRefusal::PoolExhausted),
                "k {k}: a reserve past remaining − M must be refused by the pool ceiling, \
                 not admitted through the clamped live cap"
            );
        }
        Ok(())
    }

    /// A signer that reserves, pays, and leaves takes its row with it. Without the
    /// prune the map only grows: ADR 003 §Revocation makes short-expiry session keys
    /// the intended usage, so a busy publisher mints signer identities steadily and
    /// every one that pays cleanly would leave a zero row alive until the pool is
    /// reclaimed on-chain — inside a map locked on every admission.
    #[tokio::test]
    async fn a_paid_out_signer_leaves_no_row_behind() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await; // M = 0
        let pool = B256::repeat_byte(0x37);
        let floor = decdn_incentive::floor_micro(1_000_000);
        let guard = handler
            .try_reserve_floor(pool, TEST_SIGNER, floor, TEST_RATE, floor)
            .map_err(|e| anyhow::anyhow!("refused: {e:?}"))?;
        {
            let map = handler
                .pool_floor
                .lock()
                .map_err(|e| anyhow::anyhow!("floor map poisoned: {e}"))?;
            anyhow::ensure!(
                map.0
                    .get(&pool)
                    .is_some_and(|s| s.signers.contains_key(&TEST_SIGNER)),
                "the row exists while the reservation is live"
            );
        }
        // Paid in full, then dropped: the live reservation is released and the bucket
        // is never debited, so the row carries no information and is pruned.
        guard.release_live_repaid();
        drop(guard);
        let map = handler
            .pool_floor
            .lock()
            .map_err(|e| anyhow::anyhow!("floor map poisoned: {e}"))?;
        let entry = map.0.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            !entry.signers.contains_key(&TEST_SIGNER),
            "a signer that carries no information must not keep a row for the pool's lifetime"
        );
        anyhow::ensure!(
            entry.live_committed() == U256::ZERO,
            "and the pool live total still agrees with the (now empty) signer set"
        );
        Ok(())
    }

    /// The live cap makes one signer's concurrent exposure an ABSOLUTE window count,
    /// not a slice of the deposit (#1857): `k · one_window`, the same however large the
    /// pool's headroom, and lower-clamped to one window so it never wedges a lone
    /// signer off a small pool.
    #[tokio::test]
    async fn signer_cap_is_a_window_count_not_a_slice_of_the_deposit() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 16, u64::MAX, 60).await;
        let one_window =
            decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
        let ceiling = one_window.saturating_mul(U256::from(16u64));
        // The cap does not vary with the request's rate scale beyond one_window, and it
        // is exactly k windows — no deposit term enters.
        anyhow::ensure!(
            handler.signer_floor_cap(TEST_RATE) == ceiling,
            "the live cap is exactly k windows"
        );
        // k = 0 clamps up to one window (already covered), and the cap is independent of
        // any headroom value — it is not a function of remaining at all.
        let (clamped, _c) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 0, u64::MAX, 60).await;
        anyhow::ensure!(
            clamped.signer_floor_cap(TEST_RATE) == one_window,
            "k = 0 clamps up to one window regardless of the deposit"
        );
        Ok(())
    }

    /// The live cap scales the number of distinct signers needed to strand a pool's
    /// floor budget with the deposit. With a `k`-window cap on a pool holding `N`
    /// windows of headroom it takes `ceil(N / k)` signers — the escape a share alone
    /// could not give, where a constant number of keys strands any deposit.
    #[tokio::test]
    async fn live_cap_scales_the_signers_needed_to_strand_the_floor() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, 16, u64::MAX, 60).await;
        let pool = B256::repeat_byte(0x38);
        let one_window =
            decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
        let remaining = one_window.saturating_mul(U256::from(144u64));
        let ceiling = one_window.saturating_mul(U256::from(16u64));
        anyhow::ensure!(
            handler.signer_floor_cap(TEST_RATE) == ceiling,
            "k = 16 gives a 16-window live cap"
        );

        // Eight signers each fill their own ceiling and stop there. The pool keeps a
        // spare ceiling's worth of headroom throughout (8 × 16 == 128 of 144), so
        // every refusal in this loop is the live cap and not the pool running out.
        let mut held = Vec::new();
        for i in 0u8..8 {
            let signer = Address::new([i.saturating_add(1); 20]);
            held.push(
                handler
                    .try_reserve_floor(pool, signer, remaining, TEST_RATE, ceiling)
                    .map_err(|e| anyhow::anyhow!("signer {i} refused early: {e:?}"))?,
            );
            anyhow::ensure!(
                handler
                    .try_reserve_floor(pool, signer, remaining, TEST_RATE, one_window)
                    .err()
                    .is_some_and(|e| matches!(e, FloorRefusal::SignerAtCap { .. })),
                "signer {i} must stop at its window ceiling, not at a share of the deposit"
            );
        }
        // A ninth signer takes the last ceiling's worth, and only then is the pool
        // itself spent — after nine signers, not the four a bare quarter-share would
        // have needed however large the deposit.
        held.push(
            handler
                .try_reserve_floor(
                    pool,
                    Address::new([0x9au8; 20]),
                    remaining,
                    TEST_RATE,
                    ceiling,
                )
                .map_err(|e| anyhow::anyhow!("the ninth signer's own share must fit: {e:?}"))?,
        );
        anyhow::ensure!(
            handler
                .try_reserve_floor(
                    pool,
                    Address::new([0xeeu8; 20]),
                    remaining,
                    TEST_RATE,
                    one_window
                )
                .err()
                == Some(FloorRefusal::PoolExhausted),
            "the pool ceiling still bounds the aggregate once every share is spent"
        );
        Ok(())
    }

    /// A very large `k` makes the live cap wider than the pool's whole headroom, so
    /// the per-signer live gate never binds and the check collapses to exactly the
    /// pool-ceiling behavior. This is the escape hatch an operator serving
    /// single-signer pools sets.
    #[tokio::test]
    async fn wide_live_cap_reproduces_the_pool_only_bound() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, u64::MAX, u64::MAX, 60)
                .await;
        let pool = B256::repeat_byte(0x35);
        let floor = decdn_incentive::floor_micro(1_000_000);
        let remaining = floor.saturating_mul(U256::from(3u64));
        // ONE signer draws the pool's entire headroom, three floors, unrefused.
        let mut held = Vec::new();
        for _ in 0u8..3 {
            held.push(
                handler
                    .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                    .map_err(|e| anyhow::anyhow!("refused under a wide live cap: {e:?}"))?,
            );
        }
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                .err()
                == Some(FloorRefusal::PoolExhausted),
            "the pool ceiling is the only bound left at a wide live cap"
        );
        Ok(())
    }

    /// A burst of abandoned streams on one signer drains its abandonment bucket and
    /// trips `SignerThrottled` while the pool is solvent; the bucket then refills over
    /// time, and once it has room the same signer is admitted again. Node-local and
    /// signer-scoped — a co-tenant is never throttled by it.
    #[tokio::test]
    async fn abandonment_bucket_throttles_a_burst_then_refills() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Capacity 2 windows, refill 1 window/hour (so the millisecond-scale drain
        // phase banks no meaningful refill and the throttle is deterministic), an
        // unbounded live cap, and a huge pool so only the bucket can bite.
        let refill_secs = 3_600u64;
        let (handler, _dir) =
            handler_for_tests_with_signer_policy(&metrics, U256::ZERO, u64::MAX, 2, refill_secs)
                .await;
        let pool = B256::repeat_byte(0x3B);
        let window = handler.one_window(TEST_RATE);
        let remaining = window.saturating_mul(U256::from(1_000u64));
        // Abandon two streams, each leaving one window un-recouped: the bucket climbs
        // to two windows, exactly its two-window capacity, so the next admit throttles.
        for _ in 0u8..2 {
            let g = handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, window)
                .map_err(|e| anyhow::anyhow!("admit before throttle: {e:?}"))?;
            g.note_unpaid(window);
            drop(g); // abnormal: debits one window
        }
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, window)
                .err()
                .is_some_and(|e| matches!(e, FloorRefusal::SignerThrottled { .. })),
            "a burst of abandons drains the bucket past capacity and throttles the signer"
        );
        // A co-tenant is unaffected.
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER_B, remaining, TEST_RATE, window)
                .is_ok(),
            "the throttle is signer-scoped: a co-tenant is still admitted"
        );
        // Force the refill clock forward by rewriting the drained signer's
        // `last_refill_ms` three hours into the past: at one window/hour that refills
        // three windows, more than clearing the two-window debit.
        {
            let mut guard = handler
                .pool_floor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(lane) = guard
                .0
                .get_mut(&pool)
                .and_then(|e| e.signers.get_mut(&TEST_SIGNER))
            {
                let three_hours_ms = refill_secs.saturating_mul(3).saturating_mul(1_000);
                lane.last_refill_ms = now_unix_ms().saturating_sub(three_hours_ms);
            }
        }
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, window)
                .is_ok(),
            "once the bucket has refilled the same signer is admitted again"
        );
        Ok(())
    }

    /// A stream whose cumulative payment reaches a floor releases its live
    /// reservation immediately and debits the abandonment bucket nothing — the drop is
    /// a no-op.
    #[test]
    fn floor_reservation_repaid_debits_no_bucket() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<FloorAccumulator>> =
            Arc::new(std::sync::Mutex::new(FloorAccumulator::default()));
        let pool = B256::repeat_byte(0x5B);
        let floor = decdn_incentive::floor_micro(1000);
        {
            let res = FloorReservation::reserve(
                map.clone(),
                None,
                Arc::new(Metrics::new()),
                pool,
                TEST_SIGNER,
                floor,
                floor,
                60,
            );
            // Even with un-recouped floor noted, a repaid guard debits nothing.
            res.note_unpaid(floor);
            res.release_live_repaid(); // paid ≥ floor
            let live = lock_floor(&map)?.0.get(&pool).map(|s| s.live_reservation);
            anyhow::ensure!(
                live == Some(U256::ZERO),
                "live reservation is freed the moment the floor is repaid"
            );
        } // drop is a no-op: already repaid
        let st = lock_floor(&map)?.0.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.signers
                .get(&TEST_SIGNER)
                .is_none_or(|s| s.bucket_consumed.is_zero()),
            "a repaid reservation debits no bucket"
        );
        Ok(())
    }

    /// A repayment that lands after the pool was reclaimed releases nothing:
    /// `forget_pool_floor` removed the entry (its live reservation went with
    /// it), so `release_live_repaid` must not re-insert a default state for the
    /// closed pool — the in-memory face of the #1781 resurrection race. The
    /// pool id never recurs, so a re-inserted entry sits in the map for the
    /// process lifetime, collecting bucket debits from any later drop.
    #[test]
    fn repaid_release_after_forget_does_not_resurrect_entry() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<FloorAccumulator>> =
            Arc::new(std::sync::Mutex::new(FloorAccumulator::default()));
        let pool = B256::repeat_byte(0x5F);
        let floor = decdn_incentive::floor_micro(1000);
        let res = FloorReservation::reserve(
            map.clone(),
            None,
            Arc::new(Metrics::new()),
            pool,
            TEST_SIGNER,
            floor,
            floor,
            60,
        );
        // The pool closes mid-stream: the same in-memory remove
        // `forget_pool_floor` performs.
        lock_floor(&map)?.0.remove(&pool);
        res.release_live_repaid();
        anyhow::ensure!(
            !lock_floor(&map)?.0.contains_key(&pool),
            "a repaid release on a reclaimed pool must not re-insert its entry"
        );
        drop(res);
        anyhow::ensure!(
            !lock_floor(&map)?.0.contains_key(&pool),
            "the subsequent drop leaves the reclaimed pool absent too"
        );
        Ok(())
    }

    /// Read one signer's persisted bucket level out of a floor-loss store, for the
    /// drop-guard tests below.
    fn persisted_bucket(
        store: &dyn decdn_incentive::PoolFloorLossStore,
        pool: B256,
    ) -> anyhow::Result<Option<u128>> {
        Ok(store
            .load_buckets()
            .map_err(|e| anyhow::anyhow!("load_buckets: {e}"))?
            .into_iter()
            .find(|&(id, signer, _, _)| id == pool && signer == TEST_SIGNER)
            .map(|(_, _, micro, _)| micro))
    }

    /// Two abandoned streams on ONE signer, each reconciled through the real `Drop`
    /// guard against a real store (no runtime, so each drop persists through the
    /// synchronous fallback), with a huge `refill_secs` so no refill intrudes: the
    /// second drop accumulates onto the first's bucket level and persists the raised
    /// snapshot, and a later fully-repaid guard writes nothing. This is the drop guard
    /// driving the store through its caller — every other store test drives it directly
    /// (#1783).
    #[test]
    fn floor_reservation_sequential_drops_accumulate_bucket() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<FloorAccumulator>> =
            Arc::new(std::sync::Mutex::new(FloorAccumulator::default()));
        let store = Arc::new(decdn_incentive::MemoryPoolFloorLossStore::new());
        let metrics = Arc::new(Metrics::new());
        let pool = B256::repeat_byte(0x5C);
        let floor = decdn_incentive::floor_micro(1000);
        let quarter = floor / U256::from(4u64);
        {
            let res = FloorReservation::reserve(
                map.clone(),
                Some(store.clone()),
                Arc::clone(&metrics),
                pool,
                TEST_SIGNER,
                floor,
                floor,
                u64::MAX, // no refill during the test
            );
            res.note_unpaid(quarter);
        } // abnormal: debits the quarter un-recouped
        anyhow::ensure!(persisted_bucket(&*store, pool)? == Some(quarter.to::<u128>()));
        {
            let res = FloorReservation::reserve(
                map.clone(),
                Some(store.clone()),
                Arc::clone(&metrics),
                pool,
                TEST_SIGNER,
                floor,
                floor,
                u64::MAX,
            );
            res.note_unpaid(floor); // abandons a full floor un-recouped
        } // abnormal: debits the full floor on top of the quarter
        let want = quarter.saturating_add(floor);
        let st = lock_floor(&map)?.0.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.signers.get(&TEST_SIGNER).map(|s| s.bucket_consumed) == Some(want),
            "the second drop debits onto the first's bucket, not over it"
        );
        anyhow::ensure!(
            persisted_bucket(&*store, pool)? == Some(want.to::<u128>()),
            "the second drop persists the raised bucket snapshot"
        );
        {
            let res = FloorReservation::reserve(
                map.clone(),
                Some(store.clone()),
                Arc::clone(&metrics),
                pool,
                TEST_SIGNER,
                floor,
                floor,
                u64::MAX,
            );
            res.release_live_repaid();
        } // repaid: no debit, no write
        anyhow::ensure!(
            persisted_bucket(&*store, pool)? == Some(want.to::<u128>()),
            "a repaid guard disturbs neither the bucket nor the durable snapshot"
        );
        Ok(())
    }

    /// [`decdn_incentive::PoolFloorLossStore`] wrapper that HOLDS every
    /// `record_bucket` at its entry until released, so a test can deterministically
    /// land a drop-dispatched persist AFTER the pool's forget committed — the
    /// #1781 interleaving. Everything else delegates straight through.
    struct GatedLossStore {
        inner: decdn_incentive::MemoryPoolFloorLossStore,
        released: std::sync::Mutex<bool>,
        turnstile: std::sync::Condvar,
        records_done: std::sync::atomic::AtomicUsize,
    }

    impl GatedLossStore {
        fn new() -> Self {
            Self {
                inner: decdn_incentive::MemoryPoolFloorLossStore::new(),
                released: std::sync::Mutex::new(false),
                turnstile: std::sync::Condvar::new(),
                records_done: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn release(&self) {
            let mut open = self
                .released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *open = true;
            self.turnstile.notify_all();
        }
    }

    impl decdn_incentive::PoolFloorLossStore for GatedLossStore {
        fn record_bucket(
            &self,
            pool_id: B256,
            signer: Address,
            consumed_micro: u128,
            refill_unix_ms: u64,
        ) -> Result<(), decdn_incentive::StoreError> {
            let mut open = self
                .released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*open {
                open = self
                    .turnstile
                    .wait(open)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            drop(open);
            let result = self
                .inner
                .record_bucket(pool_id, signer, consumed_micro, refill_unix_ms);
            self.records_done.fetch_add(1, Ordering::SeqCst);
            result
        }

        fn load_buckets(
            &self,
        ) -> Result<Vec<(B256, Address, u128, u64)>, decdn_incentive::StoreError> {
            self.inner.load_buckets()
        }

        fn forget_loss(&self, pool_id: B256) -> Result<(), decdn_incentive::StoreError> {
            self.inner.forget_loss(pool_id)
        }

        fn sweep_forgotten(&self) -> Result<usize, decdn_incentive::StoreError> {
            self.inner.sweep_forgotten()
        }
    }

    /// The #1781 resurrection race through the REAL drop guard: the guard
    /// snapshots its bucket under the floor lock while the pool's entry still exists,
    /// dispatches `record_bucket` to a blocking task, and the pool's forget (in-memory
    /// remove + durable `forget_loss`) commits BEFORE that task runs. The gate makes
    /// the lost race deterministic. The forget's tombstone turns the late write into a
    /// no-op — without it, the write re-inserts a row for the closed pool, and (the
    /// pool id never recurring) nothing ever deletes it again: one leaked row per
    /// closed pool, rehydrated on every later boot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn late_drop_persist_after_forget_does_not_resurrect_row() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<FloorAccumulator>> =
            Arc::new(std::sync::Mutex::new(FloorAccumulator::default()));
        let store = Arc::new(GatedLossStore::new());
        let pool = B256::repeat_byte(0x5D);
        let floor = decdn_incentive::floor_micro(1000);
        let res = FloorReservation::reserve(
            map.clone(),
            Some(store.clone()),
            Arc::new(Metrics::new()),
            pool,
            TEST_SIGNER,
            floor,
            floor,
            u64::MAX,
        );
        // Abnormal drop inside the runtime: an un-recouped floor is noted, so the guard
        // snapshots a nonzero bucket (the map entry still exists) and dispatches its
        // persist, which parks on the gate.
        res.note_unpaid(floor);
        drop(res);
        // The pool closes: in-memory entry removed, durable row deleted +
        // tombstoned — the same order `forget_pool_floor` runs them in.
        {
            let mut guard = map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.0.remove(&pool);
        }
        decdn_incentive::PoolFloorLossStore::forget_loss(&*store, pool)
            .map_err(|e| anyhow::anyhow!("forget_loss: {e}"))?;
        // Only now does the drop's `record_bucket` land.
        store.release();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while store.records_done.load(Ordering::SeqCst) == 0 {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "the gated record_bucket never ran"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        anyhow::ensure!(
            persisted_bucket(&*store, pool)?.is_none(),
            "a record_bucket landing after forget_loss must not resurrect the row"
        );
        Ok(())
    }

    /// A failed drop-time persist bumps `floor_loss_persist_failures` (#1782) —
    /// the only alertable signal that bucket snapshots have stopped reaching disk
    /// (e.g. redb latching writes after a failed commit on `floor-loss.redb`) and
    /// that a restart would grant signers a fresh abandonment allowance.
    #[test]
    fn floor_persist_failure_bumps_the_counter() -> anyhow::Result<()> {
        struct FailingLossStore;
        impl decdn_incentive::PoolFloorLossStore for FailingLossStore {
            fn record_bucket(
                &self,
                _: B256,
                _: Address,
                _: u128,
                _: u64,
            ) -> Result<(), decdn_incentive::StoreError> {
                Err(decdn_incentive::StoreError::Backend("injected".into()))
            }
            fn load_buckets(
                &self,
            ) -> Result<Vec<(B256, Address, u128, u64)>, decdn_incentive::StoreError> {
                Ok(Vec::new())
            }
            fn forget_loss(&self, _: B256) -> Result<(), decdn_incentive::StoreError> {
                Ok(())
            }
            fn sweep_forgotten(&self) -> Result<usize, decdn_incentive::StoreError> {
                Ok(0)
            }
        }
        let map: Arc<std::sync::Mutex<FloorAccumulator>> =
            Arc::new(std::sync::Mutex::new(FloorAccumulator::default()));
        let metrics = Arc::new(Metrics::new());
        let pool = B256::repeat_byte(0x5E);
        let floor = decdn_incentive::floor_micro(1000);
        {
            let res = FloorReservation::reserve(
                map.clone(),
                Some(Arc::new(FailingLossStore)),
                Arc::clone(&metrics),
                pool,
                TEST_SIGNER,
                floor,
                floor,
                u64::MAX,
            );
            // Un-recouped floor noted so the abnormal drop actually persists.
            res.note_unpaid(floor);
        } // abnormal drop → synchronous persist fallback → injected failure
        let encoded = metrics.encode()?;
        anyhow::ensure!(
            encoded.contains("decdn_floor_loss_persist_failures_total 1"),
            "a failed bucket persist must bump the failure counter"
        );
        Ok(())
    }

    /// A failed `forget_loss` bumps `floor_loss_persist_failures` too: the
    /// closed pool's row survives with no tombstone, open to permanent
    /// re-insertion by a late persist (#1781's error-path residual), and
    /// without the counter `DecdnFloorLossPersistFailures` never sees this
    /// mode.
    #[test]
    fn floor_forget_failure_bumps_the_counter() -> anyhow::Result<()> {
        let metrics = Metrics::new();
        note_forget_outcome(
            &metrics,
            B256::repeat_byte(0x60),
            Ok(Err(decdn_incentive::StoreError::Backend("injected".into()))),
        );
        let encoded = metrics.encode()?;
        anyhow::ensure!(
            encoded.contains("decdn_floor_loss_persist_failures_total 1"),
            "a failed bucket forget must bump the failure counter"
        );
        Ok(())
    }
}
