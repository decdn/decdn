//! The floor accumulator: how much un-vouchered credit this node has fronted
//! against a pool, and against each capability signer on it (ADR 003 §Pool
//! solvency).
//!
//! Two levels, one accumulator. [`PoolFloorState`] holds a pool's totals and the
//! per-signer rows they are the sum of; [`FloorReservation`] is the RAII hold one
//! stream takes against them. The pool totals are stored rather than derived
//! because [`ClientHandler::try_reserve_floor`] reads them on every admission
//! under a `std::sync::Mutex`, and an O(1) read is what keeps that lock hold
//! short.
//!
//! The invariant every mutation must preserve is that the pool counters equal the
//! fold of the signer rows. That is why the counters are PRIVATE to this module
//! and moved only through [`PoolFloorState`]'s inherent mutators — `charge_live`,
//! `release_live`, `fold_dead`, `hydrate_dead` — each of which touches both levels
//! in one call. A one-sided update would be silent, and since `dead_charge` only
//! grows and clears only on pool reclaim, permanent.
//!
//! Unlike the other `handlers::client` submodules, which are bare `impl
//! ClientHandler` blocks over types defined in `mod.rs`, this one owns its types.
//! That is the point: `mod.rs` cannot reach the fields, so the mutators are the
//! only way to move them.

use super::{
    Address, Arc, AtomicBool, AtomicU64, B256, CHUNK_BYTES, ClientHandler, Hash, HashMap, Metrics,
    Ordering, ServeRejectReason, U256, min_payment,
};

/// Rebuild the in-memory accumulator from the durable dead-charge rows at bring-up.
///
/// No stream is live at boot, so every `live_reservation` starts at zero; each
/// `(pool, signer)` lane's persisted `dead_charge` carries forward, so a restart
/// grants a fresh free-floor budget neither to a pool nor to any one signer on it.
///
/// Fails CLOSED. A genuine first boot returns `Ok(vec![])` from `load_losses` (the
/// table simply does not exist yet), so any error reaching here is a real store
/// fault, and coming up empty would silently re-grant every pool its whole
/// free-floor budget — the "withhold then restart" escape the durable copy exists
/// to close.
///
/// # Errors
/// Returns the store's error if the tombstone sweep or the row load fails.
pub(super) fn hydrate(
    store: Option<&Arc<dyn decdn_incentive::PoolFloorLossStore>>,
) -> anyhow::Result<HashMap<B256, PoolFloorState>> {
    let mut pool_floor: HashMap<B256, PoolFloorState> = HashMap::new();
    let Some(store) = store else {
        return Ok(pool_floor);
    };
    // Reclaim forget tombstones first: handler construction is the one point where
    // no reservation exists and no drop-time persist can be in flight, so the sweep
    // cannot reopen the resurrection window the tombstones close (#1781). This
    // bounds tombstone growth to one process lifetime.
    let swept = store.sweep_forgotten()?;
    if swept > 0 {
        tracing::debug!(swept, "reclaimed floor-loss tombstones of closed pools");
    }
    // The pool total is the SUM of its signer rows, so fold rather than insert: one
    // pool contributes as many rows as it has signers that ever accrued a charge.
    // Each row's `micro_usdc` is that lane's cumulative total, never a delta, so
    // folding rows is correct and folding one row twice is not.
    for decdn_incentive::FloorLoss {
        pool_id,
        signer,
        micro_usdc,
    } in store.load_losses()?
    {
        pool_floor
            .entry(pool_id)
            .or_default()
            .hydrate_dead(signer, U256::from(micro_usdc));
    }
    Ok(pool_floor)
}

/// One capability signer's share of a pool's floor-credit accounting (ADR 003
/// §Pool solvency, per-signer floor isolation). Same two counters as the pool
/// total it rolls up into: `live_reservation` is the `µUSDC` this signer's
/// in-flight streams currently reserve, `dead_charge` its cumulative
/// unrecoverable floor loss.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct SignerFloorState {
    live_reservation: U256,
    dead_charge: U256,
}

impl SignerFloorState {
    /// Floor credit this signer has already committed against the pool: live
    /// reservations plus permanent dead charge, saturating.
    const fn committed(self) -> U256 {
        self.live_reservation.saturating_add(self.dead_charge)
    }
}

/// One pool's floor-credit accounting (ADR 003 §Pool solvency). `live_reservation`
/// is the `µUSDC` currently reserved by in-flight streams — ephemeral, cleared on
/// restart since no stream is live then; `dead_charge` is the durable, cumulative
/// unrecoverable floor loss, persisted in a [`decdn_incentive::PoolFloorLossStore`]
/// and reloaded at bring-up.
///
/// `signers` splits the SAME two quantities per capability signer, so admission
/// can bound one signer's un-vouchered exposure to a share of the pool budget
/// underneath the pool-wide `remaining − M` ceiling.
///
/// The pool-level counters ARE the sum of the signer entries, and every field here
/// is private so that stays true by construction: [`Self::charge_live`],
/// [`Self::release_live`], [`Self::fold_dead`], and [`Self::hydrate_dead`] are the
/// only ways to move them, each touching both levels in one call and asserting the
/// fold in debug builds. Bring-up rebuilds the pool total the same way, by folding
/// the persisted signer rows ([`hydrate`]).
#[derive(Debug, Clone)]
pub(super) struct PoolFloorState {
    live_reservation: U256,
    dead_charge: U256,
    signers: HashMap<Address, SignerFloorState>,
    /// Generation stamp, unique across every entry this process creates. A
    /// [`FloorReservation`] copies it at charge time and [`Self::reconcile`]
    /// compares it, so a guard whose pool was reclaimed
    /// ([`ClientHandler::forget_pool_floor`] removed the entry) and whose `pool_id`
    /// a later admission then re-entered reconciles against nothing, rather than
    /// decrementing counters it never contributed to and folding its dead charge
    /// into a signer row belonging to a different pool generation.
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
            dead_charge: U256::ZERO,
            signers: HashMap::new(),
            epoch: POOL_FLOOR_EPOCH.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl PoolFloorState {
    /// Floor credit committed across every signer on the pool: live reservations
    /// plus permanent dead charge, saturating.
    const fn committed(&self) -> U256 {
        self.live_reservation.saturating_add(self.dead_charge)
    }

    /// Drop a signer's entry once it commits nothing — no live reservation and no
    /// dead charge. A row that has fallen back to zero carries no information: an
    /// unseen signer reads as zero anyway ([`Self::signer_committed`]), so keeping
    /// it changes no decision.
    ///
    /// Without this the map only ever grows. ADR 003 §Revocation makes short-expiry
    /// session keys the intended usage, so a busy publisher mints signer identities
    /// steadily, and every one that reserves and pays cleanly would leave a zero row
    /// alive until the pool is reclaimed on-chain — inside a map locked on every
    /// admission.
    ///
    /// Safe against a live guard: a repaid guard's `Drop` returns before touching
    /// the map at all, and an unrepaid guard holds `live_reservation > 0` — every
    /// admission reserves at least one chunk at the on-chain-floored rate, so a
    /// reservation is never zero — and neither can have its row pruned out from
    /// under it.
    fn prune_spent(&mut self, signer: Address) {
        if self
            .signers
            .get(&signer)
            .is_some_and(|lane| lane.committed().is_zero())
        {
            self.signers.remove(&signer);
        }
    }

    /// This signer's committed floor credit; an unseen signer has committed
    /// nothing.
    fn signer_committed(&self, signer: Address) -> U256 {
        self.signers
            .get(&signer)
            .copied()
            .unwrap_or_default()
            .committed()
    }

    /// Charge `amount` of live reservation to this pool and to `signer`'s row.
    ///
    /// `entry().or_default()` at both levels: a charge is the one direction that
    /// legitimately creates a row, and the caller has already decided both caps
    /// cover it. Saturating, so the counters cannot wrap under a hostile amount.
    fn charge_live(&mut self, signer: Address, amount: U256) {
        self.live_reservation = self.live_reservation.saturating_add(amount);
        let lane = self.signers.entry(signer).or_default();
        lane.live_reservation = lane.live_reservation.saturating_add(amount);
        debug_assert!(self.levels_agree(), "charge_live left the two levels apart");
    }

    /// Release `amount` of live reservation from this pool and from `signer`'s row,
    /// then drop the row if it has fallen to nothing.
    ///
    /// `get_mut` at the signer level, not `entry().or_default()`: a release has
    /// nothing to create. A missing row means [`Self::prune_spent`] already took it,
    /// which it does only at zero, so defaulting one in would be a no-op that leaves
    /// an empty row behind.
    fn release_live(&mut self, signer: Address, amount: U256) {
        self.live_reservation = self.live_reservation.saturating_sub(amount);
        if let Some(lane) = self.signers.get_mut(&signer) {
            lane.live_reservation = lane.live_reservation.saturating_sub(amount);
        }
        self.prune_spent(signer);
        debug_assert!(
            self.levels_agree(),
            "release_live left the two levels apart"
        );
    }

    /// Release `live_release` of live reservation and fold `dead_add` into the
    /// permanent dead charge, at both levels, returning the SIGNER's new dead total.
    ///
    /// That return is what gets persisted: the pool total is the sum of its signer
    /// rows, so bring-up rebuilds it by folding them ([`hydrate`]). `entry()` rather
    /// than `get_mut()` at the signer level because the fold must not be dropped:
    /// the row is present unless `prune_spent` took it, which it cannot while the
    /// unrepaid guard calling this holds a non-zero reservation, and defaulting keeps
    /// the charge if that reasoning ever stops holding.
    fn fold_dead(&mut self, signer: Address, live_release: U256, dead_add: U256) -> U256 {
        self.live_reservation = self.live_reservation.saturating_sub(live_release);
        self.dead_charge = self.dead_charge.saturating_add(dead_add);
        let lane = self.signers.entry(signer).or_default();
        lane.live_reservation = lane.live_reservation.saturating_sub(live_release);
        lane.dead_charge = lane.dead_charge.saturating_add(dead_add);
        let signer_dead = lane.dead_charge;
        self.prune_spent(signer);
        debug_assert!(self.levels_agree(), "fold_dead left the two levels apart");
        signer_dead
    }

    /// Fold one persisted row's cumulative dead charge into this pool at bring-up.
    /// No live reservation exists then, so this moves the dead counters only.
    fn hydrate_dead(&mut self, signer: Address, amount: U256) {
        self.dead_charge = self.dead_charge.saturating_add(amount);
        let lane = self.signers.entry(signer).or_default();
        lane.dead_charge = lane.dead_charge.saturating_add(amount);
        debug_assert!(
            self.levels_agree(),
            "hydrate_dead left the two levels apart"
        );
    }

    /// The invariant every mutator above restores before returning: the stored pool
    /// counters are exactly the fold of the signer rows. Checked under
    /// `debug_assert!` rather than derived on every read, because the pool total is
    /// read on each admission under the accumulator lock.
    fn levels_agree(&self) -> bool {
        self.committed()
            == self
                .signers
                .values()
                .fold(U256::ZERO, |acc, lane| acc.saturating_add(lane.committed()))
    }

    /// The entry a reservation guard may reconcile against: present under `pool_id`
    /// AND stamped with the generation the guard charged.
    ///
    /// A missing entry means the pool was reclaimed ([`ClientHandler::forget_pool_floor`]
    /// removed it) and the live reservation went with it; re-inserting would
    /// resurrect a row for a closed pool that nothing removes again — the in-memory
    /// face of #1781. A present entry with a DIFFERENT stamp is a later generation,
    /// re-entered by an admission that ran after the remove: subtracting from it
    /// would report a reservation this guard never charged to it, and folding into it
    /// would put this stream's dead charge on a signer row that outlives the pool it
    /// served. Both cases reconcile against nothing.
    fn reconcile(
        map: &mut HashMap<B256, PoolFloorState>,
        pool_id: B256,
        epoch: u64,
    ) -> Option<&mut Self> {
        map.get_mut(&pool_id).filter(|entry| entry.epoch == epoch)
    }
}

/// What a [`FloorReservation`] needs from the handler that opened it: the shared
/// accumulator, the durable store and its failure counter, and the channel its drop
/// hands the new dead total to.
///
/// Bundled rather than passed positionally because every field is plumbing the guard
/// only forwards — none of them varies per reservation — so a call site reads as
/// "one guard against this handler's accumulator", not as five arguments in an order
/// that must be remembered.
struct FloorGuardDeps {
    map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
    store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
    metrics: Arc<Metrics>,
    persist_tx: Option<tokio::sync::mpsc::UnboundedSender<FloorLossWrite>>,
}

/// RAII hold for one stream's span-capped reservation against a pool's budget.
///
/// Construction (`FloorReservation::reserve`, test-only) charges the reserved
/// amount to the pool's
/// `live_reservation`. The serve loop keeps the current unpaid `µUSDC` updated via
/// [`Self::note_unpaid`], and calls [`Self::release_live_repaid`] once THIS stream's
/// cumulative payment reaches a floor — which frees the live reservation
/// immediately. On drop (every exit path — success, `?`, disconnect, panic) the
/// guard releases the live reservation if it was not already repaid and folds the
/// last-noted unpaid amount (capped at the reserved amount) into the durable
/// `dead_charge`,
/// then persists the new dead total best-effort. Mirrors [`LaneSlot`]: the
/// reservation is owned by the guard and never adjusted by hand, and every counter
/// update saturates.
#[must_use = "dropping the guard at once folds the FULL reservation into the pool's \
              permanent dead charge, as an abnormal exit"]
pub(super) struct FloorReservation {
    map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
    store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
    /// Where the drop-time persist goes when a [`spawn_persist_worker`] is running.
    /// `None` outside any runtime (a sync unit test), where drop writes inline.
    persist_tx: Option<tokio::sync::mpsc::UnboundedSender<FloorLossWrite>>,
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
    /// The capability signer this reservation belongs to. Drop folds the dead
    /// charge into BOTH this signer's entry and the pool total, and persists the
    /// signer's new total, so per-signer isolation survives a restart.
    signer: Address,
    reserved: U256,
    /// Last-noted unpaid `µUSDC` (`u64`, saturating). Read once at drop to size the
    /// `dead_charge` fold.
    unpaid: AtomicU64,
    /// Set by [`Self::release_live_repaid`]; makes drop a no-op (live already freed,
    /// no dead charge). Idempotent.
    repaid: AtomicBool,
    /// Set by [`Self::mark_settled`] at a stream's CLEAN completion. On drop it
    /// selects the fold size: a settled stream folds only its proportional unpaid
    /// tail; an abnormal exit that was never marked settled folds the full `reserved`
    /// — the safe, conservative direction for a solvency guard. Without this, an abort before
    /// any byte is delivered would drop with `unpaid == 0` and fold nothing, so a
    /// cache-miss stream that fronted upstream USDC could be repeated sequentially
    /// forever, never charging `dead_charge` (ADR 003 §Pool solvency).
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
    fn reserve(
        map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
        store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
        metrics: Arc<Metrics>,
        pool_id: B256,
        signer: Address,
        reserved: U256,
    ) -> Self {
        let epoch = {
            let mut guard = map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = guard.entry(pool_id).or_default();
            entry.charge_live(signer, reserved);
            entry.epoch
        };
        // Same rule the handler applies: a worker when there is a store to write to
        // and a runtime to spawn onto, inline otherwise. Matching it here is what
        // keeps the tests that use this form exercising the real drop dispatch —
        // notably the ones that need `Drop` to RETURN before the write lands.
        let persist_tx = start_persist_worker(store.as_ref(), &metrics);
        let deps = FloorGuardDeps {
            map,
            store,
            metrics,
            persist_tx,
        };
        Self::new_charged(deps, pool_id, signer, reserved, epoch)
    }

    /// Build a guard for a floor that is ALREADY charged to `live_reservation`
    /// under the caller's own lock hold. This does NOT touch the map — the
    /// increment happens exactly once, at the caller's atomic check-and-reserve,
    /// so re-incrementing here would double-charge the pool. Used by
    /// [`ClientHandler::try_reserve_floor`], whose single lock hold covers both the
    /// budget check and the increment; `FloorReservation::reserve` is the test-only
    /// standalone form that increments first, then delegates here.
    fn new_charged(
        deps: FloorGuardDeps,
        pool_id: B256,
        signer: Address,
        reserved: U256,
        epoch: u64,
    ) -> Self {
        let FloorGuardDeps {
            map,
            store,
            metrics,
            persist_tx,
        } = deps;
        Self {
            map,
            store,
            metrics,
            persist_tx,
            pool_id,
            epoch,
            signer,
            reserved,
            unpaid: AtomicU64::new(0),
            repaid: AtomicBool::new(false),
            settled: AtomicBool::new(false),
        }
    }

    /// Mark the stream cleanly completed, so drop folds only the proportional unpaid
    /// tail (`min(reserved, unpaid)`) rather than the full `reserved`. Called at the
    /// serve loop's clean-completion point — the whole request delivered and every
    /// interval paid. Any exit that does NOT call this (client disconnect, `?`, a
    /// voucher rejection, a mid-stream stop) is treated as abnormal and folds the
    /// full reservation. Idempotent.
    pub(super) fn mark_settled(&self) {
        self.settled.store(true, Ordering::Relaxed);
    }

    /// Record the stream's current unpaid `µUSDC`, read at drop to size the
    /// `dead_charge` fold if the reservation is never repaid. Saturating to `u64`.
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
        // Both levels move together, through the one mutator — a release that
        // touched only the pool total would leave the signer's share permanently
        // consumed by a stream that paid for it. See [`PoolFloorState::reconcile`]
        // for why a missing or differently-stamped entry reconciles against nothing.
        if let Some(entry) = PoolFloorState::reconcile(&mut guard, self.pool_id, self.epoch) {
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
    /// live reservation and suppresses the drop-time `dead_charge`.
    ///
    /// The un-settled drop folds the FULL `reserved` on purpose: it bounds
    /// sequential abuse where a cache-miss fill is aborted AFTER the node fronted
    /// upstream USDC (ADR 003 §Pool solvency). But a refusal BEFORE any spend —
    /// the pre-flight floor-`M` gate, the size gate, or an upstream that refused
    /// the free header handshake because its own `getPool` view has not yet caught
    /// up to this pool — fronts nothing and delivers nothing, so folding a dead
    /// charge there permanently penalizes an innocent pool for the serving node's
    /// own transient upstream unavailability, and a handful of such refusals strand
    /// a signer's whole floor share. That is the behavior `serve_stream` documents
    /// as "a refusal before the serve loop drops with `note_unpaid` at 0, so no
    /// dead charge is folded"; this restores it. Mechanically a refused-unspent
    /// serve and a fully-repaid one both owe nothing, so this delegates to
    /// [`Self::release_live_repaid`]; the distinct name states the intent at the
    /// refusal call sites.
    pub(super) fn release_unspent(&self) {
        self.release_live_repaid();
    }
}

impl Drop for FloorReservation {
    fn drop(&mut self) {
        if self.repaid.load(Ordering::Relaxed) {
            return; // repaid: live already released, no dead charge
        }
        // Not repaid: release the live reservation and fold a dead charge. A CLEANLY
        // completed stream ([`Self::mark_settled`]) folds only its proportional unpaid
        // tail (`min(reserved, unpaid)`, `== 0` for a fully-paid small blob). An
        // ABNORMAL exit — client disconnect, `?`, a voucher rejection, a mid-stream
        // stop — folds the FULL `reserved`: the conservative, safe direction for a
        // solvency guard. This is what bounds sequential abuse where a client aborts a
        // cache-miss fill before any byte is delivered (`unpaid == 0`) yet the node has
        // already fronted upstream USDC — without it the pool's budget would be
        // restored in full and the pattern could repeat forever (ADR 003 §Pool solvency).
        // All under the sync lock, all saturating — an O(1) update that never blocks
        // the reactor.
        let dead_add = if self.settled.load(Ordering::Relaxed) {
            self.reserved
                .min(U256::from(self.unpaid.load(Ordering::Relaxed)))
        } else {
            self.reserved
        };
        let snapshot = {
            let mut guard = self
                .map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // A reclaimed or re-entered pool reconciles against nothing — see
            // [`PoolFloorState::reconcile`] — so skip the whole reconcile rather
            // than resurrecting a row `forget` deleted or charging a later
            // generation.
            let Some(entry) = PoolFloorState::reconcile(&mut guard, self.pool_id, self.epoch)
            else {
                return;
            };
            entry.fold_dead(self.signer, self.reserved, dead_add)
        };
        // A fully-repaid or fully-settled stream folds nothing: skip the durable
        // write entirely so a sub-interval request does not fsync a value already
        // on disk. `record_loss` commits with `Durability::Immediate`, and one redb
        // file takes one writer at a time, so an unconditional write here would put
        // every small paid request behind an fsync on the floor-loss store.
        if dead_add.is_zero() {
            return;
        }
        // Hand the new total to the persist worker. `record_loss` is MONOTONIC per
        // `(pool, signer)` — it raises the stored total, never lowers it — so two
        // drops on the same lane arriving out of order cannot regress the row.
        //
        // A `send` on an unbounded channel is the whole of this guard's work: it
        // never blocks, and it never panics. Spawning here would do both wrong.
        // `spawn_blocking` panics once the blocking pool is shutting down, which is
        // exactly when guards drop en masse, and a panic inside a `Drop` is not
        // something the serve path can absorb.
        let Some(store) = self.store.clone() else {
            return;
        };
        let pool_id = self.pool_id;
        let signer = self.signer;
        let micro = snapshot.saturating_to::<u128>();
        if let Some(tx) = self.persist_tx.as_ref() {
            let queued = tx
                .send(FloorLossWrite::Record {
                    pool_id,
                    signer,
                    micro_usdc: micro,
                })
                .is_ok();
            if queued {
                return;
            }
            // The worker is gone — its task was aborted, or the runtime is past the
            // point where it can run one. Fall through and write inline rather than
            // lose the value: the alternative is this signer getting its whole share
            // back on the next boot.
            tracing::debug!(
                %pool_id, %signer, micro,
                "floor persist worker is gone; writing the dead charge inline"
            );
        }
        // Either no worker was ever started (a drop outside any runtime, i.e. a sync
        // unit test) or the send above found it gone.
        persist_loss(&store, &self.metrics, pool_id, signer, micro);
    }
}

/// Start the floor-loss persist worker, if there is anything for it to do.
///
/// `None` — so every drop writes inline — when no floor-loss store is configured, or
/// when there is no runtime to spawn onto (a sync unit test). Called once, at handler
/// construction.
pub(super) fn start_persist_worker(
    store: Option<&Arc<dyn decdn_incentive::PoolFloorLossStore>>,
    metrics: &Arc<Metrics>,
) -> Option<tokio::sync::mpsc::UnboundedSender<FloorLossWrite>> {
    let store = store?;
    if tokio::runtime::Handle::try_current().is_err() {
        return None;
    }
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    spawn_persist_worker(Arc::clone(store), Arc::clone(metrics), rx);
    Some(tx)
}

/// One message for the floor-loss persist worker ([`spawn_persist_worker`]).
pub(super) enum FloorLossWrite {
    /// Raise this `(pool, signer)` lane's durable dead-charge total to
    /// `micro_usdc`. The value is the lane's CUMULATIVE total, and `record_loss`
    /// raises monotonically, so two writes for one lane arriving out of order
    /// cannot regress the row.
    Record {
        pool_id: B256,
        signer: Address,
        micro_usdc: u128,
    },
    /// Acknowledge once every earlier `Record` has reached the store. The worker
    /// processes in order, so the ack is a proof about everything queued before it.
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// Drain floor-loss writes onto a blocking thread, one at a time, until every
/// sender is gone.
///
/// A [`FloorReservation`]'s `Drop` cannot await, and it runs at the worst possible
/// moment for spawning: guards drop en masse during runtime shutdown, when
/// `spawn_blocking` panics rather than returning — a panic inside a `Drop`. Sending
/// on an unbounded channel neither blocks nor panics, so the guard's only job is a
/// `send`, and this task owns every outcome: it awaits each blocking write, counts a
/// cancelled or panicked one instead of discarding the handle, and keeps the
/// `record_loss` fsync off the reactor.
///
/// Serial by construction. `record_loss` commits with `Durability::Immediate` and
/// one redb file takes one writer at a time, so concurrent writes would queue on the
/// file anyway; processing in order is what makes [`FloorLossWrite::Flush`] a proof
/// about everything before it.
fn spawn_persist_worker(
    store: Arc<dyn decdn_incentive::PoolFloorLossStore>,
    metrics: Arc<Metrics>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<FloorLossWrite>,
) {
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let (pool_id, signer, micro_usdc) = match msg {
                FloorLossWrite::Record {
                    pool_id,
                    signer,
                    micro_usdc,
                } => (pool_id, signer, micro_usdc),
                FloorLossWrite::Flush(ack) => {
                    // A dropped receiver means the flusher gave up waiting; the
                    // writes still landed, so there is nothing to report.
                    let _ = ack.send(());
                    continue;
                }
            };
            let write_store = Arc::clone(&store);
            let write_metrics = Arc::clone(&metrics);
            let joined = tokio::task::spawn_blocking(move || {
                persist_loss(&write_store, &write_metrics, pool_id, signer, micro_usdc);
            })
            .await;
            if let Err(e) = joined {
                // The write never ran, or panicked inside the blocking pool. Either
                // way the durable total is now behind the in-memory one, which is
                // exactly the restart-time re-grant `floor_loss_persist_failures`
                // exists to surface.
                note_join_failure(&metrics, pool_id, signer, &e);
            }
        }
    });
}

/// Raise one lane's durable dead-charge total, accounting for a failure.
///
/// The in-memory `dead_charge` is authoritative for the running process; the durable
/// copy only guards a restart, so a lost write is the documented small crash-window
/// residual — logged and counted, never panicked or propagated.
fn persist_loss(
    store: &Arc<dyn decdn_incentive::PoolFloorLossStore>,
    metrics: &Metrics,
    pool_id: B256,
    signer: Address,
    micro: u128,
) {
    if let Err(e) = store.record_loss(pool_id, signer, micro) {
        if note_store_failure(metrics, &e) {
            tracing::error!(
                %pool_id, %signer, micro, error = %e,
                "floor dead-charge persist failed: payment store corrupt"
            );
        } else {
            tracing::warn!(
                %pool_id, %signer, micro, error = %e,
                "floor dead-charge persist failed"
            );
        }
    }
}

/// Account for a floor-loss write that never completed — cancelled by runtime
/// shutdown, or panicked in the blocking pool. Counted like a failed write because
/// the consequence is identical: the durable total stays behind the in-memory one,
/// so a restart re-grants that signer the share this write was recording.
fn note_join_failure(
    metrics: &Metrics,
    pool_id: B256,
    signer: Address,
    err: &tokio::task::JoinError,
) {
    metrics.floor_loss_persist_failure();
    tracing::warn!(%pool_id, %signer, error = %err, "floor dead-charge persist join failed");
}

/// Count one failed floor-loss write and say whether the payment store is corrupt.
///
/// Shared by the drop-time persist and the reclaim-time forget so the two cannot
/// drift on the parts that must agree: both bump `floor_loss_persist_failures`, and
/// both surface a corrupt store one level louder than a transient fault. After a
/// mid-commit failure redb refuses further writes until the file is closed and
/// reopened, so every later write fails too and the remedy is an operator restart —
/// which is what earns the `error!` the `true` return selects. Each caller keeps its
/// own message and fields, which is the part that legitimately differs.
fn note_store_failure(metrics: &Metrics, err: &decdn_incentive::StoreError) -> bool {
    metrics.floor_loss_persist_failure();
    matches!(err, decdn_incentive::StoreError::Corrupt { .. })
}

/// Failure accounting for a reclaimed pool's `forget_loss`
/// ([`ClientHandler::forget_pool_floor`]). A failed forget leaves the row in
/// place with NO tombstone, so a drop-dispatched `record_loss` still in flight
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
            if note_store_failure(metrics, &e) {
                tracing::error!(
                    %pool_id, error = %e,
                    "pool dead-charge forget failed: payment store corrupt"
                );
            } else {
                tracing::warn!(%pool_id, error = %e, "pool dead-charge forget failed");
            }
        }
        Err(e) => {
            metrics.floor_loss_persist_failure();
            tracing::warn!(%pool_id, error = %e, "pool dead-charge forget join failed");
        }
    }
}

/// Which of the two floor caps refused an admission
/// ([`ClientHandler::try_reserve_floor`]). The two collapse to one `NotFound` on
/// the wire; they stay distinct here so the per-reason metric separates "this
/// pool cannot pay" from "this one signer has consumed its share".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FloorRefusal {
    /// The pool-wide ceiling: `remaining − M` cannot cover the floor credit
    /// already committed across every signer on the pool plus this new floor.
    PoolExhausted,
    /// The per-signer sub-cap: the pool can still pay, but THIS signer's
    /// un-vouchered `live + dead` already fills its share of the pool budget.
    /// Carries the cap that refused, read under the same lock hold as the
    /// decision, so the operator line reports the number that actually applied
    /// rather than a recomputation against a `remaining` that has since moved.
    SignerAtCap { signer_cap: U256 },
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
            FloorRefusal::SignerAtCap { .. } => Self::SignerFloorAtCap,
        }
    }
}

impl ClientHandler {
    /// Emit the observable side of a floor-admission refusal, picking the message
    /// the refusing cap actually justifies.
    ///
    /// The two caps need different words and different remedies.
    /// [`FloorRefusal::PoolExhausted`] is the pool running dry, which
    /// [`Self::log_deposit_refusal`] already describes. [`FloorRefusal::SignerAtCap`]
    /// is the opposite situation: `try_reserve_floor` tests the pool ceiling FIRST,
    /// so reaching the sub-cap proves the pool can pay. Reporting it as a deposit
    /// shortfall would print a headroom that visibly exceeds the ceiling beside a
    /// sentence denying it, and would send the operator to check RPC health while one
    /// capability-holder quietly holds a lockout that no top-up clears.
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
                tracing::debug!(
                    %pool_id, %signer, %hash, %signer_cap, %ceiling, %headroom,
                    "refusing delivery: this capability signer's un-vouchered floor credit fills \
                     its share of the pool budget; the pool itself can still pay"
                );
                if let Some(suppressed) = self.note_signer_cap_refusal() {
                    tracing::warn!(
                        %pool_id, %signer, %signer_cap, %headroom, suppressed,
                        interval = ?Self::DEPOSIT_REFUSAL_WARN_INTERVAL,
                        "refusing a paying client: one capability signer holds its whole share of \
                         this pool's un-vouchered floor credit while the pool is solvent. A \
                         sustained rate means that signer is abandoning streams, or running more \
                         concurrent un-vouchered streams than its share covers. Its dead charge is \
                         permanent until the pool is reclaimed on-chain, so a top-up does not \
                         clear it: rotate the session key, or widen the share — see \
                         docs/runbook.md, which covers why widening it needs a restart"
                    );
                }
            }
        }
    }

    /// Open a span-capped [`FloorReservation`] against `pool_id`'s budget for one
    /// stream. The serve loop holds the returned guard for the stream's lifetime:
    /// it notes the stream's unpaid balance as it delivers and releases the
    /// reservation once a floor is repaid; on drop the guard reconciles the live
    /// reservation and any residual dead charge against BOTH levels of the
    /// accumulator — the pool total and this signer's entry — and against the
    /// durable [`Self::floor_loss_store`].
    // Test-only: the serve path admits through `try_reserve_floor`, which checks
    // both floor caps and reserves under one lock hold.
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
        )
    }

    /// The per-signer sub-cap on un-vouchered floor credit (ADR 003 §Pool
    /// solvency, per-signer floor isolation): the most `live_reservation +
    /// dead_charge` any ONE capability signer may hold against this pool.
    ///
    /// `clamp(share, one window, max_windows × one window)`, where the share is a
    /// fraction of the pool's own refundable headroom `remaining − M` scaled by the
    /// node-local `pool_floor_signer_share_bps` policy, and one window is the
    /// ramp-start credit window priced at this request's rate — one chunk normally,
    /// the full `credit_max` when `credit_ramp_divisor == 0`, which is exactly what
    /// a fresh stream reserves either way.
    ///
    /// **The window is the unit being rationed.** A signer's honest need for
    /// un-vouchered floor credit does not scale with the pool's size: every admission
    /// reserves at most one window, and a paying stream releases its reservation as
    /// soon as it covers it, so honest need is `concurrent un-vouchered streams ×
    /// one window` whether the pool holds ten dollars or a hundred thousand. A share
    /// alone is therefore loose in both directions on a large pool — one session key
    /// could hold millions of windows, while a constant `10_000 / share_bps` keys
    /// would still strand the whole floor, which is exactly the case a shared pool
    /// exists for (one publisher, many session keys).
    ///
    /// The LOWER clamp keeps small pools usable: without it, a pool whose headroom is
    /// under `10_000 / share_bps` windows gives every signer a sub-window cap and
    /// serves nobody. With it the node admits a lone signer's first stream on any
    /// pool.
    ///
    /// The UPPER clamp is what makes the damage one compromised key can do constant
    /// rather than proportional to the deposit, and makes the number of keys needed
    /// to strand the floor scale with the deposit (`headroom / (k × window)`) instead
    /// of being a constant. `pool_floor_signer_max_windows = 0` disables it, which
    /// with a `10_000` bps share leaves the sub-cap at least as loose as the pool
    /// ceiling, i.e. a no-op.
    ///
    /// Pure and total (saturating), so it is testable without any chain access.
    pub(super) fn signer_floor_cap(&self, remaining: U256, rate_per_mb: u64) -> U256 {
        let headroom = remaining.saturating_sub(self.pool_min_remaining_deposit);
        // `wrapping_div` never divides by zero here — `BPS_DENOMINATOR` is a nonzero
        // constant — and it is the total form U256 offers. Share the constant with
        // the resolver that bounds the knob, so validation and arithmetic cannot
        // drift apart.
        let share = headroom
            .saturating_mul(U256::from(self.pool_floor_signer_share_bps))
            .wrapping_div(U256::from(decdn_common::config::BPS_DENOMINATOR));
        let one_window = min_payment(self.credit_window(CHUNK_BYTES, 0), rate_per_mb);
        let floored = share.max(one_window);
        if self.pool_floor_signer_max_windows == 0 {
            return floored;
        }
        // Saturating, so a ceiling wide enough to overflow simply never binds — the
        // same direction as disabling it. The ceiling is at least one window
        // whenever it is enabled, so it can never pull the cap below the lower
        // clamp and wedge a lone signer.
        let ceiling = one_window.saturating_mul(U256::from(self.pool_floor_signer_max_windows));
        floored.min(ceiling)
    }

    /// Stateful-B POOL solvency: does the pool's `remaining − M` cover its
    /// already-committed floor credit (`live_reservation + dead_charge`) across every
    /// signer, plus `new_reserve`? Reads the floor accumulator; pure arithmetic
    /// otherwise. A poisoned accumulator lock recovers the guard rather than
    /// panicking: every critical section over this mutex is panic-free by
    /// construction — saturating `U256` arithmetic and infallible map operations
    /// only, no indexing and no `unwrap` — so a poison cannot originate from a holder
    /// of this lock, and a recovered guard cannot observe a torn pool/signer split.
    /// Do NOT add a fallible or panicking call under this lock; that argument is what
    /// the recovery rests on, because the pool total and the signer entries must
    /// agree.
    ///
    /// (The recovery is not justified by the accumulator being "best-effort": it
    /// gates whether the node fronts upstream USDC.)
    ///
    /// Deliberately the pool level ONLY, which is what makes it the right check for a
    /// stream already in flight. The per-signer sub-cap is an ADMISSION control: a
    /// signer's cap is a share of `remaining − M`, so it shrinks as co-tenants draw
    /// the pool down, and testing a live stream against the shrunken cap would
    /// terminate a paying stream on a solvent pool — reporting `PoolExhausted` for a
    /// pool that can still pay. An admitted stream's reservation is already counted
    /// at both levels, so leaving the sub-cap out of the mid-stream re-check bounds
    /// nothing less. [`Self::try_reserve_floor`] is where both caps apply.
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
            guard
                .get(&pool_id)
                .map_or(U256::ZERO, PoolFloorState::committed)
        };
        decdn_incentive::pool_budget_covers(
            remaining,
            self.pool_min_remaining_deposit,
            committed,
            new_reserve,
        )
    }

    /// Atomically check BOTH floor caps and reserve one `floor` against the pool's
    /// budget, returning the [`FloorReservation`] guard on success or the cap that
    /// refused it.
    ///
    /// The budget reads (`live_reservation + dead_charge`, pool-wide and for this
    /// signer), both tests, and the `live_reservation += floor` increments all
    /// happen under ONE `pool_floor` lock hold, so two concurrent admissions on a
    /// near-exhausted pool — or on one near-exhausted signer share — cannot both
    /// pass the check and then both reserve; that is the TOCTOU over-commit a
    /// separate budget check followed by an unconditional reserve would allow. The
    /// guard is built from the
    /// already-charged state ([`FloorReservation::new_charged`]) so the reserved
    /// amount is charged exactly once, at both levels. A refusal charges nothing and
    /// inserts no entry, so probing a full pool cannot grow the accumulator. A
    /// poisoned lock recovers the guard rather than panicking (best-effort
    /// accounting, never a safety gate).
    pub(super) fn try_reserve_floor(
        &self,
        pool_id: B256,
        signer: Address,
        remaining: U256,
        rate_per_mb: u64,
        reserved: U256,
    ) -> Result<FloorReservation, FloorRefusal> {
        let signer_cap = self.signer_floor_cap(remaining, rate_per_mb);
        let epoch;
        {
            let mut guard = self
                .pool_floor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Read through `get`, not `entry().or_default()`: a refused admission
            // must leave no pool or signer row behind, so a client probing a full
            // pool with fresh signer keys cannot grow the map.
            let (committed, signer_committed) =
                guard.get(&pool_id).map_or((U256::ZERO, U256::ZERO), |s| {
                    (s.committed(), s.signer_committed(signer))
                });
            // Pool ceiling first: it is the solvency bound, and it is what an
            // operator reads as "this pool cannot pay".
            if !decdn_incentive::pool_budget_covers(
                remaining,
                self.pool_min_remaining_deposit,
                committed,
                reserved,
            ) {
                return Err(FloorRefusal::PoolExhausted);
            }
            if signer_committed.saturating_add(reserved) > signer_cap {
                return Err(FloorRefusal::SignerAtCap { signer_cap });
            }
            let entry = guard.entry(pool_id).or_default();
            entry.charge_live(signer, reserved);
            epoch = entry.epoch;
        }
        Ok(FloorReservation::new_charged(
            self.floor_guard_deps(),
            pool_id,
            signer,
            reserved,
            epoch,
        ))
    }

    /// Bundle what a new [`FloorReservation`] forwards: the shared accumulator, the
    /// durable store, the failure counter, and this handler's persist worker.
    fn floor_guard_deps(&self) -> FloorGuardDeps {
        FloorGuardDeps {
            map: Arc::clone(&self.pool_floor),
            store: self.floor_loss_store.clone(),
            metrics: Arc::clone(&self.metrics),
            persist_tx: self.floor_persist_tx.clone(),
        }
    }

    /// Wait until every floor-loss write queued so far has reached the store.
    ///
    /// Called once at shutdown, after the router has drained: every
    /// [`FloorReservation`] has dropped by then, so everything they folded is already
    /// queued and this is the last write before the process exits. The worker
    /// processes in order, so the ack proves the whole backlog landed.
    ///
    /// Best-effort, like the writes themselves. No worker (a handler built outside
    /// any runtime, or without a floor-loss store) has nothing to wait for; a worker
    /// that is already gone took its queue with it, which is the same lost-persist
    /// residual a crash leaves and is already counted at the write.
    pub(crate) async fn flush_floor_persists(&self) {
        let Some(tx) = self.floor_persist_tx.as_ref() else {
            return;
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        if tx.send(FloorLossWrite::Flush(ack_tx)).is_err() {
            tracing::warn!("floor persist worker is gone; shutdown flush skipped");
            return;
        }
        if ack_rx.await.is_err() {
            tracing::warn!("floor persist worker stopped before acknowledging the shutdown flush");
        }
    }

    /// Drop a reclaimed pool's floor-credit accounting: remove its in-memory
    /// `PoolFloorState`, which carries every signer entry with it, and every one of
    /// its durable `dead_charge` rows. Called once when a pool is reclaimed
    /// on-chain; a reclaimed `pool_id` never recurs (monotonic open nonce), so the
    /// `dead_charge` accumulated against it by any signer is permanently moot.
    ///
    /// A [`FloorReservation`] drop that snapshotted its total before the in-memory
    /// remove here can still have its `record_loss` in flight when the durable
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
            guard.remove(&pool_id);
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
        handler_for_tests, handler_for_tests_with_floor_policy, handler_for_tests_with_floor_store,
        handler_for_tests_with_signer_share,
    };
    use super::*;

    /// The capability signer every floor-accumulator unit test reserves under.
    /// A second signer (`TEST_SIGNER_B`) exercises the per-signer sub-cap.
    const TEST_SIGNER: Address = Address::new([0xa1u8; 20]);
    /// A distinct co-tenant on the same pool.
    const TEST_SIGNER_B: Address = Address::new([0xb2u8; 20]);
    /// The advertised `µUSDC`/MB rate the floor-cap tests price against. Only the
    /// one-credit-window clamp in [`ClientHandler::signer_floor_cap`] reads it.
    const TEST_RATE: u64 = 1_000;

    /// Lock the floor map for a test assertion, surfacing a poisoned lock as an
    /// `anyhow` error rather than panicking (the anti-panic policy holds in tests).
    fn lock_floor(
        map: &Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
    ) -> anyhow::Result<std::sync::MutexGuard<'_, HashMap<B256, PoolFloorState>>> {
        map.lock()
            .map_err(|e| anyhow::anyhow!("floor map poisoned: {e}"))
    }

    /// Assert the accumulator's load-bearing invariant on one pool: the stored O(1)
    /// pool counters are exactly the fold of its per-signer rows. A one-sided update
    /// at any of the hand-maintained mutation sites would be silent, and — since
    /// `dead_charge` only grows and clears only on pool reclaim — permanent.
    fn ensure_floor_levels_agree(
        handler: &ClientHandler,
        pool: B256,
        when: &str,
    ) -> anyhow::Result<()> {
        let st = lock_floor(&handler.pool_floor)?
            .get(&pool)
            .cloned()
            .unwrap_or_default();
        let folded = st
            .signers
            .values()
            .fold(U256::ZERO, |acc, s| acc.saturating_add(s.committed()));
        anyhow::ensure!(
            st.committed() == folded,
            "{when}: the pool total ({}) must stay the sum of its signer rows ({folded})",
            st.committed()
        );
        Ok(())
    }

    /// A stream that never reaches a floor of payment leaves its unpaid tail (capped
    /// at one floor) as the pool's durable `dead_charge`, and frees the live
    /// reservation, when its [`FloorReservation`] drops. No tokio runtime is present,
    /// so `Drop` persists synchronously via the direct-call fallback.
    #[test]
    fn floor_reservation_reconciles_partial_loss_on_drop() -> anyhow::Result<()> {
        use decdn_incentive::PoolFloorLossStore as _;
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
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
            );
            // Live reservation is held while the guard lives.
            let live = lock_floor(&map)?.get(&pool).map(|s| s.live_reservation);
            anyhow::ensure!(
                live == Some(floor),
                "live reservation is held while the guard lives"
            );
            // Stream delivered a partial (unpaid) floor, then completed cleanly.
            res.note_unpaid(quarter);
            res.mark_settled();
        } // drop → reconcile: live released, dead_charge = min(floor, unpaid) = floor/4
        let st = lock_floor(&map)?.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation == U256::ZERO,
            "live reservation is released on drop"
        );
        anyhow::ensure!(
            st.dead_charge == quarter,
            "a settled stream folds the proportional unpaid tail (min of floor and unpaid)"
        );
        let persisted = store
            .load_losses()
            .map_err(|e| anyhow::anyhow!("load_losses: {e}"))?
            .first()
            .map(|l| l.micro_usdc);
        anyhow::ensure!(
            persisted == Some(quarter.to::<u128>()),
            "the new dead total is persisted best-effort on drop"
        );
        Ok(())
    }

    /// An ABNORMAL exit — the guard drops without [`FloorReservation::mark_settled`],
    /// as on a client disconnect or abort before delivery — folds the FULL `reserved`
    /// into `dead_charge`, not the (here zero) unpaid tail. This is what bounds
    /// sequential abuse where a cache-miss fill is aborted before any byte is
    /// delivered yet the node already fronted upstream USDC (C3, ADR 003 §Pool solvency).
    #[test]
    fn floor_reservation_abnormal_exit_folds_full_reserved() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
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
            );
            // Aborted before delivering/paying anything: unpaid stays 0, and the guard
            // is never marked settled.
            res.note_unpaid(U256::ZERO);
        } // drop → conservative: dead_charge = full reserved despite unpaid == 0
        let st = lock_floor(&map)?.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation == U256::ZERO,
            "live reservation is released even on an abnormal exit"
        );
        anyhow::ensure!(
            st.dead_charge == floor,
            "an unsettled (abnormal) exit folds the full reserved floor, not the zero unpaid tail"
        );
        Ok(())
    }

    /// A serve REFUSED before the serve loop ran — [`FloorReservation::release_unspent`]
    /// called on the pre-spend refusal paths (the floor-`M` gate, the size gate, an
    /// upstream that refused the free header handshake) — frees the live reservation
    /// and folds NO `dead_charge`, even though the guard was never marked settled.
    /// This is the counterpart to `floor_reservation_abnormal_exit_folds_full_reserved`:
    /// an abort AFTER fronting USDC folds the full floor, but a refusal BEFORE any
    /// spend must not, or a transient upstream stumble permanently strands an
    /// innocent pool's floor credit.
    #[test]
    fn floor_reservation_refused_unspent_folds_no_dead_charge() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
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
            );
            let live = lock_floor(&map)?.get(&pool).map(|s| s.live_reservation);
            anyhow::ensure!(
                live == Some(floor),
                "live reservation is held while the guard lives"
            );
            // Refused before any spend — release cleanly, never marked settled.
            res.release_unspent();
        } // drop → no-op: release_unspent already freed the live reservation
        let st = lock_floor(&map)?.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation == U256::ZERO,
            "the live reservation is released by release_unspent"
        );
        anyhow::ensure!(
            st.dead_charge == U256::ZERO,
            "a pre-spend refusal folds NO dead charge, unlike an abnormal exit after spending"
        );
        Ok(())
    }

    /// The pool-budget guard counts a pool's committed floor credit (live
    /// reservations plus durable dead charge) against `remaining − M`. This is the
    /// re-check both the mid-stream gate and the direct-serve gate apply to a
    /// stream that already holds its reservation, so it is deliberately pool-level
    /// only and carries no signer dimension.
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
    /// `remaining − M` covers the pool's committed floor credit plus the new floor,
    /// and the charge is visible to the very next call so a second reserve on an
    /// exhausted pool is refused. A `10_000` bps signer share keeps the pool ceiling
    /// the only bound under test.
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
        // reservation on drop and folds no dead charge, reopening the budget. (An
        // abnormal exit would instead fold the full reserved into `dead_charge` and
        // keep the budget spent — see `floor_reservation_abnormal_exit_folds_full_reserved`.)
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

    /// The per-signer sub-cap isolates co-tenants of one shared pool: a signer that
    /// fills its own share is refused `SignerAtCap` while the pool can still pay,
    /// and a SECOND signer is admitted from its own share at the same instant. This
    /// is the whole point of the two-level cap — without the signer dimension the
    /// first signer's reservations would be the pool's, and the second would be
    /// refused too.
    #[tokio::test]
    async fn signer_sub_cap_refuses_one_signer_and_admits_a_co_tenant() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Quarter shares, M = 0: four floors of headroom, one floor each.
        let (handler, _dir) =
            handler_for_tests_with_signer_share(&metrics, U256::ZERO, 2_500).await;
        let pool = B256::repeat_byte(0x31);
        let floor = decdn_incentive::floor_micro(1_000_000);
        let remaining = floor.saturating_mul(U256::from(4u64));
        // The share (a quarter of four floors) is one floor, above the one-window
        // clamp, so it is the share — not the clamp — that binds below.
        anyhow::ensure!(
            handler.signer_floor_cap(remaining, TEST_RATE) == floor,
            "a quarter of four floors of headroom is one floor"
        );
        let held = handler.try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor);
        anyhow::ensure!(held.is_ok(), "the first floor fits inside the signer share");
        // Signer A is at its cap. The POOL is not — three floors of headroom are
        // untouched — so the refusal must name the signer cap, not the pool.
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                .err()
                .is_some_and(|e| matches!(e, FloorRefusal::SignerAtCap { .. })),
            "a second floor on the SAME signer exceeds its share while the pool can still pay"
        );
        // A co-tenant draws on its own untouched share.
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER_B, remaining, TEST_RATE, floor)
                .is_ok(),
            "a distinct signer is admitted from its own share while the first is capped"
        );
        Ok(())
    }

    /// The per-pool ceiling bounds the AGGREGATE however many signers draw on it:
    /// solvency cannot be escaped by spraying identities. Four signers each take
    /// their own full share, none of them ever exceeding its sub-cap, and the fifth
    /// is refused `PoolExhausted` — the pool, not the signer, is what ran out.
    ///
    /// A roll-up guard on the POOL dimension only. It is deliberately blind to the
    /// signer dimension — the arithmetic here holds with the sub-cap deleted
    /// entirely — so it is not evidence that per-signer isolation works;
    /// `signer_sub_cap_refuses_one_signer_and_admits_a_co_tenant` and
    /// `one_signers_dead_charge_does_not_consume_a_co_tenants_share` carry that.
    #[tokio::test]
    async fn pool_ceiling_bounds_the_aggregate_however_many_signers_draw() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_signer_share(&metrics, U256::ZERO, 2_500).await;
        let pool = B256::repeat_byte(0x32);
        let floor = decdn_incentive::floor_micro(1_000_000);
        let remaining = floor.saturating_mul(U256::from(4u64));
        let mut held = Vec::new();
        for i in 0u8..4 {
            let signer = Address::new([i.saturating_add(1); 20]);
            let guard = handler
                .try_reserve_floor(pool, signer, remaining, TEST_RATE, floor)
                .map_err(|e| anyhow::anyhow!("signer {i} refused: {e:?}"))?;
            held.push(guard);
        }
        // Every one of the four sits at exactly its own share, so no sub-cap is
        // exceeded; what refuses the fifth is the pool ceiling.
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, Address::new([0xee; 20]), remaining, TEST_RATE, floor)
                .err()
                == Some(FloorRefusal::PoolExhausted),
            "signer fan-out cannot push the aggregate past remaining − M"
        );
        Ok(())
    }

    /// Bring-up hydration restores each signer's OWN cap, not just the pool total.
    /// Two signers on one pool carry different persisted dead charges across the
    /// restart: the one that was at its share is refused `SignerAtCap`, its
    /// co-tenant is admitted at the same instant, and the pool total is the sum of
    /// both rows. Dropping the per-signer half of hydration leaves the pool total
    /// correct and re-grants every session key a fresh free-floor budget, which is
    /// the withhold-then-restart escape the sub-cap exists to close.
    #[tokio::test]
    async fn restart_hydration_restores_each_signers_own_cap() -> anyhow::Result<()> {
        use decdn_incentive::PoolFloorLossStore as _;
        let metrics = Arc::new(Metrics::new());
        let pool = B256::repeat_byte(0x37);
        let floor = decdn_incentive::floor_micro(1_000_000);
        let store = Arc::new(decdn_incentive::store::MemoryPoolFloorLossStore::new());
        // Headroom is eight floors, so a quarter share is a cap of two floors.
        // A is parked AT its cap; B at half of it.
        let two_floors = floor.saturating_mul(U256::from(2u64));
        store
            .record_loss(pool, TEST_SIGNER, two_floors.to::<u128>())
            .map_err(|e| anyhow::anyhow!("seed A: {e}"))?;
        store
            .record_loss(pool, TEST_SIGNER_B, floor.to::<u128>())
            .map_err(|e| anyhow::anyhow!("seed B: {e}"))?;

        let (handler, _dir) = handler_for_tests_with_floor_store(
            &metrics,
            2_500,
            store as Arc<dyn decdn_incentive::PoolFloorLossStore>,
        )
        .await;
        let remaining = floor.saturating_mul(U256::from(8u64));
        anyhow::ensure!(
            handler.signer_floor_cap(remaining, TEST_RATE) == two_floors,
            "a quarter of eight floors of headroom is two floors"
        );
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                .err()
                .is_some_and(|e| matches!(e, FloorRefusal::SignerAtCap { .. })),
            "the signer that restarted at its cap is still at its cap"
        );
        // Bound, not dropped: an unrepaid guard that falls out of scope folds its
        // FULL reservation into the permanent dead charge, which would move the
        // pool total this test is about to read.
        let admitted = handler.try_reserve_floor(pool, TEST_SIGNER_B, remaining, TEST_RATE, floor);
        anyhow::ensure!(
            admitted.is_ok(),
            "its co-tenant's own hydrated row leaves room, so the pool still serves it"
        );
        // The pool total folds BOTH rows: three floors of dead charge, which no
        // single signer's row accounts for.
        let dead = lock_floor(&handler.pool_floor)?
            .get(&pool)
            .map(|s| s.dead_charge);
        anyhow::ensure!(
            dead == Some(floor.saturating_mul(U256::from(3u64))),
            "the pool total is the sum of its signer rows, not the last one loaded"
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
            handler_for_tests_with_signer_share(&metrics, U256::ZERO, 2_500).await;
        let pool = B256::repeat_byte(0x38);
        let floor = decdn_incentive::floor_micro(1_000_000);
        let remaining = floor.saturating_mul(U256::from(4u64));

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
            !lock_floor(&handler.pool_floor)?.contains_key(&pool),
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
    /// never held, and folds its dead charge onto a signer row belonging to the new
    /// pool — leaving the pool total below the sum of its signer rows, which is the
    /// direction that over-admits.
    #[test]
    fn a_stale_guard_does_not_reconcile_against_a_re_entered_pool() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let pool = B256::repeat_byte(0x39);
        let floor = decdn_incentive::floor_micro(1000);

        let stale = FloorReservation::reserve(
            Arc::clone(&map),
            None,
            Arc::new(Metrics::new()),
            pool,
            TEST_SIGNER,
            floor,
        );
        // The pool is reclaimed on-chain: its whole entry goes, signer rows and all.
        lock_floor(&map)?.remove(&pool);
        // A later admission re-enters the same key — the cached `getPool` view can
        // still show headroom for a moment after the reclaim lands.
        let fresh = FloorReservation::reserve(
            Arc::clone(&map),
            None,
            Arc::new(Metrics::new()),
            pool,
            TEST_SIGNER_B,
            floor,
        );
        drop(stale);

        let st = lock_floor(&map)?.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation == floor,
            "the stale drop must not release the new entry's live reservation"
        );
        anyhow::ensure!(
            st.dead_charge == U256::ZERO,
            "the stale drop must not fold its dead charge into the new entry"
        );
        anyhow::ensure!(
            st.signers.len() == 1 && st.signers.contains_key(&TEST_SIGNER_B),
            "the stale drop must not insert its own signer row under the new entry"
        );
        anyhow::ensure!(
            st.live_reservation.saturating_add(st.dead_charge)
                == st
                    .signers
                    .values()
                    .fold(U256::ZERO, |acc, s| acc.saturating_add(s.committed())),
            "the pool total stays the sum of its signer rows"
        );
        drop(fresh);
        Ok(())
    }

    /// One signer's permanent `dead_charge` is charged to its own entry as well as to
    /// the pool total, so it locks that signer out of ADMISSION before it reaches any
    /// co-tenant's share.
    ///
    /// Driven through [`ClientHandler::try_reserve_floor`] rather than by reading the
    /// accumulator directly: the state assertions alone hold by construction — an
    /// unseen signer reads as zero whether or not admission consults the sub-cap — so
    /// they pass with the sub-cap deleted from `try_reserve_floor`. The admission
    /// assertions are what fail when it is.
    #[tokio::test]
    async fn one_signers_dead_charge_does_not_consume_a_co_tenants_share() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Quarter shares, M = 0: four floors of headroom, one floor of share each.
        let (handler, _dir) =
            handler_for_tests_with_signer_share(&metrics, U256::ZERO, 2_500).await;
        let pool = B256::repeat_byte(0x33);
        let floor = decdn_incentive::floor_micro(1_000_000);
        let remaining = floor.saturating_mul(U256::from(4u64));
        anyhow::ensure!(
            handler.signer_floor_cap(remaining, TEST_RATE) == floor,
            "a quarter of four floors of headroom is one floor, above the one-window clamp"
        );
        {
            // Signer A abandons a stream: never settled, so the FULL reserved floor
            // folds into its dead charge, permanently.
            let _res = handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                .map_err(|e| anyhow::anyhow!("signer A's first floor refused: {e:?}"))?;
        }
        {
            let st = lock_floor(&handler.pool_floor)?
                .get(&pool)
                .cloned()
                .unwrap_or_default();
            anyhow::ensure!(
                st.dead_charge == floor && st.committed() == floor,
                "the abandoned floor is charged to the pool total"
            );
            anyhow::ensure!(
                st.signer_committed(TEST_SIGNER) == floor,
                "and to the abandoning signer's own entry"
            );
            anyhow::ensure!(
                st.signer_committed(TEST_SIGNER_B) == U256::ZERO,
                "a co-tenant's entry is untouched by another signer's dead charge"
            );
        }
        // The permanent charge fills A's whole share, so A is refused at ADMISSION
        // while three floors of pool headroom remain untouched.
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                .err()
                .is_some_and(|e| matches!(e, FloorRefusal::SignerAtCap { .. })),
            "a signer whose dead charge fills its share is refused by the SUB-cap, \
             not the pool"
        );
        // ...and the co-tenant still draws on its own untouched share.
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER_B, remaining, TEST_RATE, floor)
                .is_ok(),
            "a co-tenant is admitted from its own share while the first signer is capped"
        );
        Ok(())
    }

    /// K threads race ONE admission through the pool ceiling: exactly one wins.
    ///
    /// [`ClientHandler::try_reserve_floor`] claims the budget read, both cap tests,
    /// and the `live_reservation` increments happen under ONE lock hold, so two
    /// concurrent admissions on a near-exhausted pool cannot both pass the check and
    /// then both reserve. Every other floor test is sequential, so nothing else
    /// exercises that claim: a check-then-reserve split would still pass them all and
    /// over-commit only under contention.
    ///
    /// Each thread names a DISTINCT signer at a `10_000` bps share, so the sub-cap is
    /// a no-op and the pool ceiling is unambiguously what refuses. The barrier makes
    /// the threads collide inside the same lock acquisition rather than queueing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_admissions_cannot_over_commit_the_pool_ceiling() -> anyhow::Result<()> {
        const RACERS: usize = 8;
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await; // M = 0, share 10_000 bps
        let pool = B256::repeat_byte(0x3A);
        let floor = decdn_incentive::floor_micro(1_000_000);
        // Headroom for EXACTLY one floor: slack strictly under a second.
        let remaining = floor.saturating_add(U256::from(1u64));
        let barrier = std::sync::Barrier::new(RACERS);

        let outcomes: Vec<Result<FloorReservation, FloorRefusal>> = std::thread::scope(|scope| {
            let racers: Vec<_> = (0..RACERS)
                .map(|i| {
                    let handler = Arc::clone(&handler);
                    let barrier = &barrier;
                    scope.spawn(move || {
                        // A distinct signer per racer: at a 10_000 bps share every one
                        // of them has the pool's whole headroom as its sub-cap, so the
                        // only bound that can bite is the pool ceiling.
                        let signer = Address::new([u8::try_from(i).unwrap_or(0xff); 20]);
                        barrier.wait();
                        handler.try_reserve_floor(pool, signer, remaining, TEST_RATE, floor)
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
            "exactly one of {RACERS} concurrent admissions fits the one-floor ceiling, \
             got {admitted}"
        );
        anyhow::ensure!(
            outcomes
                .iter()
                .filter_map(|o| o.as_ref().err())
                .all(|e| matches!(e, FloorRefusal::PoolExhausted)),
            "the losers are refused by the POOL ceiling, not the signer sub-cap"
        );
        ensure_floor_levels_agree(&handler, pool, "after the race")?;
        drop(outcomes);
        ensure_floor_levels_agree(&handler, pool, "after every guard drops")?;
        Ok(())
    }

    /// The same race against ONE signer's share, with pool headroom the ceiling
    /// cannot bind on: exactly one admission wins, and the losers name the SUB-cap.
    ///
    /// The pool-ceiling twin above cannot cover this: the two caps are separate tests
    /// under the same lock hold, and a check-then-reserve split on the signer arm
    /// alone would let two streams past one share while the pool stayed solvent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_admissions_cannot_over_commit_one_signers_share() -> anyhow::Result<()> {
        const RACERS: usize = 8;
        let metrics = Arc::new(Metrics::new());
        // Quarter shares, M = 0.
        let (handler, _dir) =
            handler_for_tests_with_signer_share(&metrics, U256::ZERO, 2_500).await;
        let pool = B256::repeat_byte(0x3B);
        let floor = decdn_incentive::floor_micro(1_000_000);
        // Four floors of headroom: a quarter share is exactly one floor, while the
        // pool ceiling covers four — so only the sub-cap can refuse a second racer.
        let remaining = floor.saturating_mul(U256::from(4u64));
        anyhow::ensure!(
            handler.signer_floor_cap(remaining, TEST_RATE) == floor,
            "the share, not the one-window clamp, is the binding cap here"
        );
        let barrier = std::sync::Barrier::new(RACERS);

        let outcomes: Vec<Result<FloorReservation, FloorRefusal>> = std::thread::scope(|scope| {
            let racers: Vec<_> = (0..RACERS)
                .map(|_| {
                    let handler = Arc::clone(&handler);
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        handler.try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
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
            "exactly one of {RACERS} concurrent admissions fits the one-floor share, \
             got {admitted}"
        );
        anyhow::ensure!(
            outcomes
                .iter()
                .filter_map(|o| o.as_ref().err())
                .all(|e| matches!(e, FloorRefusal::SignerAtCap { .. })),
            "the losers are refused by the SUB-cap while the pool can still pay"
        );
        ensure_floor_levels_agree(&handler, pool, "after the race")?;
        drop(outcomes);
        ensure_floor_levels_agree(&handler, pool, "after every guard drops")?;
        Ok(())
    }

    /// An admitted stream survives its signer's cap shrinking below the floor it
    /// already committed, as long as the POOL stays solvent.
    ///
    /// The sub-cap is a share of `remaining − M`, so it shrinks as co-tenants draw the
    /// pool down. [`ClientHandler::pool_budget_covers_reserve`] is deliberately the
    /// pool level only for that reason: re-testing an already-admitted reservation
    /// against the shrunken share would terminate a paying stream on a pool that can
    /// still pay, and the mid-stream gate has nothing left to bound — the admitted
    /// reservation is already counted at both levels. This pins that choice; adding
    /// the sub-cap back to the re-check fails here.
    #[tokio::test]
    async fn an_admitted_stream_survives_its_signer_cap_shrinking() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_signer_share(&metrics, U256::ZERO, 2_500).await;
        let pool = B256::repeat_byte(0x3C);
        let floor = decdn_incentive::floor_micro(1_000_000);
        // Eight floors of headroom: a quarter share is two floors, so one floor is
        // comfortably inside the cap at admission time.
        let wide = floor.saturating_mul(U256::from(8u64));
        let _admitted = handler
            .try_reserve_floor(pool, TEST_SIGNER, wide, TEST_RATE, floor)
            .map_err(|e| anyhow::anyhow!("admission refused: {e:?}"))?;

        // The owner's deposit drains to three floors — co-tenants spending, or the
        // pool's own remaining falling as vouchers redeem. A quarter of three floors
        // is under one floor, so this signer's cap is now BELOW what it committed.
        let drained = floor.saturating_mul(U256::from(3u64));
        anyhow::ensure!(
            handler.signer_floor_cap(drained, TEST_RATE) < floor,
            "the setup must actually shrink the cap below the committed floor"
        );
        anyhow::ensure!(
            handler.pool_budget_covers_reserve(pool, drained, U256::ZERO),
            "the mid-stream re-check reads the POOL level only, so a solvent pool \
             keeps serving a stream whose signer share has shrunk under it"
        );
        // Non-vacuous in the other direction: once the POOL itself cannot cover the
        // committed floor, the same re-check does refuse.
        anyhow::ensure!(
            !handler.pool_budget_covers_reserve(pool, U256::ZERO, U256::ZERO),
            "an insolvent pool still fails the mid-stream re-check"
        );
        Ok(())
    }

    /// The SHIPPED default share is exercised, not just the `10_000` bps no-op every
    /// other fixture pins.
    ///
    /// Every unit fixture defaults to `10_000` bps and the anvil e2e draws exactly one
    /// floor per lane — inside the one-window clamp — so the share arithmetic itself
    /// never runs at the value operators actually get. Read through the constant, so
    /// a change to the default lands here rather than silently going uncovered.
    #[tokio::test]
    async fn the_shipped_default_share_bounds_a_signer_to_a_quarter() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests_with_signer_share(
            &metrics,
            U256::ZERO,
            decdn_common::config::DEFAULT_POOL_FLOOR_SIGNER_SHARE_BPS,
        )
        .await;
        let pool = B256::repeat_byte(0x3D);
        let floor = decdn_incentive::floor_micro(1_000_000);
        // Well above the one-window clamp, so the share is what binds.
        let remaining = floor.saturating_mul(U256::from(40u64));
        let cap = handler.signer_floor_cap(remaining, TEST_RATE);
        anyhow::ensure!(
            cap == remaining
                .saturating_mul(U256::from(
                    decdn_common::config::DEFAULT_POOL_FLOOR_SIGNER_SHARE_BPS
                ))
                .wrapping_div(U256::from(decdn_common::config::BPS_DENOMINATOR)),
            "the default share is applied verbatim above the clamp"
        );
        anyhow::ensure!(
            cap == floor.saturating_mul(U256::from(10u64)),
            "a quarter of forty floors is ten"
        );
        // Exactly at the cap is admissible; one floor past it is not, while the pool
        // still holds thirty floors of headroom.
        let _held = handler
            .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, cap)
            .map_err(|e| anyhow::anyhow!("a signer's exact share is refused: {e:?}"))?;
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                .err()
                .is_some_and(|e| matches!(e, FloorRefusal::SignerAtCap { .. })),
            "one floor past the default share is refused by the sub-cap, not the pool"
        );
        Ok(())
    }

    /// The sub-cap is floored at ONE credit window priced at the request's rate.
    /// Without that clamp a pool whose headroom is smaller than `10_000/share_bps`
    /// windows would give every signer a sub-window cap and refuse every stream —
    /// an otherwise usable small pool would serve nobody. The clamp keeps a lone
    /// signer admissible; the pool ceiling still bounds the aggregate.
    #[tokio::test]
    async fn signer_floor_cap_clamps_up_to_one_credit_window() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_signer_share(&metrics, U256::ZERO, 2_500).await;
        let one_window =
            decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
        // Headroom of exactly two windows: a quarter share is half a window, which
        // would refuse every stream. The clamp lifts it back to one window.
        let remaining = one_window.saturating_mul(U256::from(2u64));
        anyhow::ensure!(
            handler.signer_floor_cap(remaining, TEST_RATE) == one_window,
            "a share below one credit window is clamped up to one"
        );
        let pool = B256::repeat_byte(0x34);
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, one_window)
                .is_ok(),
            "a lone signer on a small pool is still admitted for one window"
        );
        // Well above the clamp, the configured share is what applies.
        let wide = one_window.saturating_mul(U256::from(100u64));
        anyhow::ensure!(
            handler.signer_floor_cap(wide, TEST_RATE)
                == one_window.saturating_mul(U256::from(25u64)),
            "above the clamp the cap is the configured share of the headroom"
        );
        Ok(())
    }

    /// Headroom below one credit window: the one-window clamp then returns a cap
    /// LARGER than the pool's entire headroom, and only the pool ceiling running
    /// first keeps the node from admitting past the refundable minimum `M` the pool
    /// owner is guaranteed. Pins that order — the refusal must be `PoolExhausted`,
    /// never `Ok`, at any share.
    #[tokio::test]
    async fn pool_ceiling_refuses_below_one_window_whatever_the_share() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        for share in [1u64, 2_500, 10_000] {
            let (handler, _dir) =
                handler_for_tests_with_signer_share(&metrics, U256::ZERO, share).await;
            let pool = B256::repeat_byte(0x36);
            let one_window =
                decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
            let remaining = one_window.saturating_sub(U256::from(1u64));
            anyhow::ensure!(
                handler.signer_floor_cap(remaining, TEST_RATE) > remaining,
                "share {share}: the clamp does exceed the headroom, which is what makes \
                 the check order load-bearing"
            );
            anyhow::ensure!(
                handler
                    .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, one_window)
                    .err()
                    == Some(FloorRefusal::PoolExhausted),
                "share {share}: a reserve past remaining − M must be refused by the pool \
                 ceiling, not admitted through the clamped sub-cap"
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
                map.get(&pool)
                    .is_some_and(|s| s.signers.contains_key(&TEST_SIGNER)),
                "the row exists while the reservation is live"
            );
        }
        // Paid in full, then dropped: the live reservation is released and no dead
        // charge folds, so the row commits nothing and is pruned.
        guard.release_live_repaid();
        drop(guard);
        let map = handler
            .pool_floor
            .lock()
            .map_err(|e| anyhow::anyhow!("floor map poisoned: {e}"))?;
        let entry = map.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            !entry.signers.contains_key(&TEST_SIGNER),
            "a signer that committed nothing must not keep a row for the pool's lifetime"
        );
        anyhow::ensure!(
            entry.committed() == U256::ZERO,
            "and the pool total still agrees with the (now empty) signer set"
        );
        Ok(())
    }

    /// The window ceiling makes one signer's exposure constant instead of
    /// deposit-proportional (#1857). A share of a large pool's headroom is worth
    /// many windows; the ceiling caps it at `k` of them, and — the point — `k` does
    /// not move when the deposit grows.
    #[tokio::test]
    async fn signer_cap_is_capped_at_a_window_count_not_a_slice_of_the_deposit()
    -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_floor_policy(&metrics, U256::ZERO, 2_500, 16).await;
        let one_window =
            decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
        let ceiling = one_window.saturating_mul(U256::from(16u64));

        // A quarter of 400 windows is 100 windows; the ceiling cuts it to 16.
        let wide = one_window.saturating_mul(U256::from(400u64));
        anyhow::ensure!(
            handler.signer_floor_cap(wide, TEST_RATE) == ceiling,
            "a share worth more than the ceiling is cut to the ceiling"
        );
        // Ten times the deposit, same cap. This is the property the share alone
        // could not give: damage per compromised key stops tracking pool size.
        let wider = one_window.saturating_mul(U256::from(4_000u64));
        anyhow::ensure!(
            handler.signer_floor_cap(wider, TEST_RATE) == ceiling,
            "growing the deposit tenfold must not grow one signer's cap"
        );
        // Below the ceiling the share still governs: a quarter of 8 windows is 2.
        let narrow = one_window.saturating_mul(U256::from(8u64));
        anyhow::ensure!(
            handler.signer_floor_cap(narrow, TEST_RATE)
                == one_window.saturating_mul(U256::from(2u64)),
            "under the ceiling the configured share is what applies"
        );
        // And the lower clamp still wins beneath one window, so the ceiling can
        // never wedge a lone signer off a small pool.
        anyhow::ensure!(
            handler.signer_floor_cap(one_window / U256::from(2u64), TEST_RATE) == one_window,
            "the ceiling never pulls the cap below one credit window"
        );
        Ok(())
    }

    /// The window ceiling scales the number of distinct signers needed to strand a
    /// pool's floor budget with the deposit. Under a bare 2500 bps share exactly four
    /// signers fill any pool, however large; with a 16-window ceiling on a pool
    /// holding 144 windows of headroom it takes nine — and eighteen if the deposit
    /// doubles again.
    #[tokio::test]
    async fn window_ceiling_scales_the_signers_needed_to_strand_the_floor() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_floor_policy(&metrics, U256::ZERO, 2_500, 16).await;
        let pool = B256::repeat_byte(0x38);
        let one_window =
            decdn_incentive::min_payment(handler.credit_window(CHUNK_BYTES, 0), TEST_RATE);
        let remaining = one_window.saturating_mul(U256::from(144u64));
        let ceiling = one_window.saturating_mul(U256::from(16u64));
        anyhow::ensure!(
            handler.signer_floor_cap(remaining, TEST_RATE) == ceiling,
            "a quarter of 144 windows is 36, so the 16-window ceiling is what binds"
        );

        // Eight signers each fill their own ceiling and stop there. The pool keeps a
        // spare ceiling's worth of headroom throughout (8 × 16 == 128 of 144), so
        // every refusal in this loop is the sub-cap and not the pool running out.
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

    /// A `10_000` bps share makes the sub-cap at least the pool's whole headroom, so
    /// the two-level check collapses to exactly the pool-ceiling behavior. This is
    /// the escape hatch an operator serving single-signer pools sets.
    #[tokio::test]
    async fn full_signer_share_reproduces_the_pool_only_bound() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) =
            handler_for_tests_with_signer_share(&metrics, U256::ZERO, 10_000).await;
        let pool = B256::repeat_byte(0x35);
        let floor = decdn_incentive::floor_micro(1_000_000);
        let remaining = floor.saturating_mul(U256::from(3u64));
        // ONE signer draws the pool's entire headroom, three floors, unrefused.
        let mut held = Vec::new();
        for _ in 0u8..3 {
            held.push(
                handler
                    .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                    .map_err(|e| anyhow::anyhow!("refused under a full share: {e:?}"))?,
            );
        }
        anyhow::ensure!(
            handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                .err()
                == Some(FloorRefusal::PoolExhausted),
            "the pool ceiling is the only bound left at a full share"
        );
        Ok(())
    }

    /// A stream whose cumulative payment reaches a floor releases its live
    /// reservation immediately and leaves no `dead_charge` — the drop is a no-op.
    #[test]
    fn floor_reservation_repaid_leaves_no_dead_charge() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
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
            );
            res.release_live_repaid(); // paid ≥ floor
            let live = lock_floor(&map)?.get(&pool).map(|s| s.live_reservation);
            anyhow::ensure!(
                live == Some(U256::ZERO),
                "live reservation is freed the moment the floor is repaid"
            );
        } // drop is a no-op: already repaid
        let st = lock_floor(&map)?.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.dead_charge == U256::ZERO,
            "a repaid reservation folds no dead charge"
        );
        Ok(())
    }

    /// A repayment that lands after the pool was reclaimed releases nothing:
    /// `forget_pool_floor` removed the entry (its live reservation went with
    /// it), so `release_live_repaid` must not re-insert a default state for the
    /// closed pool — the in-memory face of the #1781 resurrection race. The
    /// pool id never recurs, so a re-inserted entry sits in the map for the
    /// process lifetime, collecting `dead_charge` from any later drop.
    #[test]
    fn repaid_release_after_forget_does_not_resurrect_entry() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let pool = B256::repeat_byte(0x5F);
        let floor = decdn_incentive::floor_micro(1000);
        let res = FloorReservation::reserve(
            map.clone(),
            None,
            Arc::new(Metrics::new()),
            pool,
            TEST_SIGNER,
            floor,
        );
        // The pool closes mid-stream: the same in-memory remove
        // `forget_pool_floor` performs.
        lock_floor(&map)?.remove(&pool);
        res.release_live_repaid();
        anyhow::ensure!(
            lock_floor(&map)?.get(&pool).is_none(),
            "a repaid release on a reclaimed pool must not re-insert its entry"
        );
        drop(res);
        anyhow::ensure!(
            lock_floor(&map)?.get(&pool).is_none(),
            "the subsequent drop leaves the reclaimed pool absent too"
        );
        Ok(())
    }

    /// Read one pool's persisted dead charge out of a floor-loss store, for the
    /// drop-guard tests below.
    fn persisted_loss(
        store: &dyn decdn_incentive::PoolFloorLossStore,
        pool: B256,
    ) -> anyhow::Result<Option<u128>> {
        Ok(store
            .load_losses()
            .map_err(|e| anyhow::anyhow!("load_losses: {e}"))?
            .into_iter()
            .find(|l| l.pool_id == pool && l.signer == TEST_SIGNER)
            .map(|l| l.micro_usdc))
    }

    /// Two withheld streams on ONE pool, each reconciled through the real `Drop`
    /// guard against a real store (no runtime, so each drop persists through the
    /// synchronous fallback): the second drop persists a cumulative total larger
    /// than the first, and a later fully-repaid guard writes nothing. This is the
    /// drop guard driving the store's monotonic contract — every other
    /// monotonicity test drives the store directly, without its caller (#1783).
    #[test]
    fn floor_reservation_sequential_drops_accumulate_dead_charge() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
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
            );
            res.note_unpaid(quarter);
            res.mark_settled();
        } // settled: folds the quarter unpaid tail
        anyhow::ensure!(persisted_loss(&*store, pool)? == Some(quarter.to::<u128>()));
        {
            let _res = FloorReservation::reserve(
                map.clone(),
                Some(store.clone()),
                Arc::clone(&metrics),
                pool,
                TEST_SIGNER,
                floor,
            );
        } // abnormal (never settled): folds the FULL reserved floor on top
        let want = quarter.saturating_add(floor);
        let st = lock_floor(&map)?.get(&pool).cloned().unwrap_or_default();
        anyhow::ensure!(
            st.dead_charge == want,
            "the second drop folds onto the first's total, not over it"
        );
        anyhow::ensure!(
            persisted_loss(&*store, pool)? == Some(want.to::<u128>()),
            "the second drop persists the RAISED cumulative total"
        );
        {
            let res = FloorReservation::reserve(
                map.clone(),
                Some(store.clone()),
                Arc::clone(&metrics),
                pool,
                TEST_SIGNER,
                floor,
            );
            res.release_live_repaid();
        } // repaid: no fold, no write
        anyhow::ensure!(
            persisted_loss(&*store, pool)? == Some(want.to::<u128>()),
            "a repaid guard disturbs neither the accumulator nor the durable total"
        );
        Ok(())
    }

    /// [`decdn_incentive::PoolFloorLossStore`] wrapper that HOLDS every
    /// `record_loss` at its entry until released, so a test can deterministically
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
        fn record_loss(
            &self,
            pool_id: B256,
            signer: Address,
            micro_usdc: u128,
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
            let result = self.inner.record_loss(pool_id, signer, micro_usdc);
            self.records_done.fetch_add(1, Ordering::SeqCst);
            result
        }

        fn load_losses(
            &self,
        ) -> Result<Vec<decdn_incentive::FloorLoss>, decdn_incentive::StoreError> {
            self.inner.load_losses()
        }

        fn forget_loss(&self, pool_id: B256) -> Result<(), decdn_incentive::StoreError> {
            self.inner.forget_loss(pool_id)
        }

        fn sweep_forgotten(&self) -> Result<usize, decdn_incentive::StoreError> {
            self.inner.sweep_forgotten()
        }
    }

    /// The #1781 resurrection race through the REAL drop guard: the guard
    /// snapshots its cumulative total under the floor lock while the pool's entry
    /// still exists, dispatches `record_loss` to a blocking task, and the pool's
    /// forget (in-memory remove + durable `forget_loss`) commits BEFORE that task
    /// runs. The gate makes the lost race deterministic. The forget's tombstone
    /// turns the late write into a no-op — without it, the write re-inserts a
    /// row for the closed pool, and (`record_loss` being monotonic, the pool id
    /// never recurring) nothing ever deletes it again: one leaked row per closed
    /// pool, rehydrated on every later boot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn late_drop_persist_after_forget_does_not_resurrect_row() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
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
        );
        // Abnormal drop inside the runtime: the guard snapshots `floor` (the map
        // entry still exists) and dispatches its persist, which parks on the gate.
        drop(res);
        // The pool closes: in-memory entry removed, durable row deleted +
        // tombstoned — the same order `forget_pool_floor` runs them in.
        {
            let mut guard = map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.remove(&pool);
        }
        decdn_incentive::PoolFloorLossStore::forget_loss(&*store, pool)
            .map_err(|e| anyhow::anyhow!("forget_loss: {e}"))?;
        // Only now does the drop's `record_loss` land.
        store.release();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while store.records_done.load(Ordering::SeqCst) == 0 {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "the gated record_loss never ran"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        anyhow::ensure!(
            persisted_loss(&*store, pool)?.is_none(),
            "a record_loss landing after forget_loss must not resurrect the row"
        );
        Ok(())
    }

    /// A drop inside a runtime reaches the store through the persist worker, and
    /// `flush_floor_persists` is what makes that observable at a point in time.
    ///
    /// The drop-time write is queued, not awaited — `Drop` cannot await — so without
    /// the flush a test could only poll. The flush is also the shutdown contract:
    /// the worker processes in order, so an ack proves every earlier write landed.
    #[tokio::test(flavor = "multi_thread")]
    async fn queued_persists_are_durable_once_flushed() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(decdn_incentive::MemoryPoolFloorLossStore::new());
        let (handler, _dir) =
            handler_for_tests_with_floor_store(&metrics, 10_000, store.clone()).await;
        let pool = B256::repeat_byte(0x71);
        let floor = decdn_incentive::floor_micro(1_000_000);
        let remaining = floor.saturating_mul(U256::from(4u64));

        // Two abandoned streams on one signer: each folds its full reservation, and
        // the SECOND write carries the cumulative total, so the row ends at both.
        for _ in 0..2 {
            let _abandoned = handler
                .try_reserve_floor(pool, TEST_SIGNER, remaining, TEST_RATE, floor)
                .map_err(|e| anyhow::anyhow!("admission refused: {e:?}"))?;
        }
        handler.flush_floor_persists().await;
        anyhow::ensure!(
            persisted_loss(&*store, pool)? == Some(floor.saturating_mul(U256::from(2u64)).to()),
            "both queued drops must be durable once the flush acknowledges"
        );
        Ok(())
    }

    /// A drop whose persist worker is gone writes inline rather than losing the
    /// value.
    ///
    /// The worker's task dies with the runtime, and guards drop en masse exactly
    /// then. A lost write is not a lost log line: the durable total falls behind the
    /// in-memory one, so the next boot hands that signer back the share this write
    /// was recording. Driven through a handler built with no runtime, which is the
    /// same `persist_tx == None` state.
    #[test]
    fn a_drop_with_no_persist_worker_still_writes_inline() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(decdn_incentive::MemoryPoolFloorLossStore::new());
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let pool = B256::repeat_byte(0x72);
        let floor = decdn_incentive::floor_micro(1000);
        {
            let _abandoned = FloorReservation::reserve(
                Arc::clone(&map),
                Some(store.clone()),
                Arc::clone(&metrics),
                pool,
                TEST_SIGNER,
                floor,
            );
        }
        anyhow::ensure!(
            persisted_loss(&*store, pool)? == Some(floor.to()),
            "with no worker to send to, the drop must persist on its own thread"
        );
        Ok(())
    }

    /// A persist cancelled or panicked in the blocking pool is COUNTED, not
    /// discarded.
    ///
    /// The drop path used to spawn and drop the `JoinHandle`, so a write cancelled by
    /// runtime shutdown — the case that happens when guards drop en masse — left no
    /// trace at all. `floor_loss_persist_failures` is what an operator alerts on, and
    /// the consequence of a cancelled write is identical to a failed one: the durable
    /// total falls behind, and a restart re-grants the share.
    #[test]
    fn a_cancelled_persist_is_counted_like_a_failed_one() -> anyhow::Result<()> {
        let metrics = Metrics::new();
        let cancelled = tokio::runtime::Builder::new_current_thread()
            .build()?
            .block_on(async {
                let task = tokio::spawn(std::future::pending::<()>());
                task.abort();
                task.await
            })
            .err()
            .ok_or_else(|| anyhow::anyhow!("an aborted task must yield a JoinError"))?;
        note_join_failure(&metrics, B256::repeat_byte(0x73), TEST_SIGNER, &cancelled);
        anyhow::ensure!(
            metrics
                .encode()?
                .contains("decdn_floor_loss_persist_failures_total 1"),
            "a persist that never completed must bump the failure counter"
        );
        Ok(())
    }

    /// A failed drop-time persist bumps `floor_loss_persist_failures` (#1782) —
    /// the only alertable signal that dead charges have stopped reaching disk
    /// (e.g. redb latching writes after a failed commit on `floor-loss.redb`) and
    /// that a restart would re-grant pools their consumed free-floor budget.
    #[test]
    fn floor_persist_failure_bumps_the_counter() -> anyhow::Result<()> {
        struct FailingLossStore;
        impl decdn_incentive::PoolFloorLossStore for FailingLossStore {
            fn record_loss(
                &self,
                _: B256,
                _: Address,
                _: u128,
            ) -> Result<(), decdn_incentive::StoreError> {
                Err(decdn_incentive::StoreError::Backend("injected".into()))
            }
            fn load_losses(
                &self,
            ) -> Result<Vec<decdn_incentive::FloorLoss>, decdn_incentive::StoreError> {
                Ok(Vec::new())
            }
            fn forget_loss(&self, _: B256) -> Result<(), decdn_incentive::StoreError> {
                Ok(())
            }
            fn sweep_forgotten(&self) -> Result<usize, decdn_incentive::StoreError> {
                Ok(0)
            }
        }
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let metrics = Arc::new(Metrics::new());
        let pool = B256::repeat_byte(0x5E);
        let floor = decdn_incentive::floor_micro(1000);
        {
            let _res = FloorReservation::reserve(
                map.clone(),
                Some(Arc::new(FailingLossStore)),
                Arc::clone(&metrics),
                pool,
                TEST_SIGNER,
                floor,
            );
        } // abnormal drop → synchronous persist fallback → injected failure
        let encoded = metrics.encode()?;
        anyhow::ensure!(
            encoded.contains("decdn_floor_loss_persist_failures_total 1"),
            "a failed dead-charge persist must bump the failure counter"
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
            "a failed dead-charge forget must bump the failure counter"
        );
        Ok(())
    }
}
