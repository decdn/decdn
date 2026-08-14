//! LRU cache-eviction driver loop (#1173, appendix-blob-cache-eviction.md
//! § Eviction driver loop).
//!
//! A single async task, owned by the node wiring layer (per
//! `appendix-poc-production-seams.md`), that enforces the `cache.cache_size_mb`
//! ceiling the cache write path deliberately does not. It periodically measures
//! on-disk footprint and, once over the high-water mark, releases
//! least-recently-used blobs (via [`CacheEngine::release_for_eviction`], the
//! soft-evict path that is *not* the durable operator-evict) down to the target
//! mark, bounded by a per-sweep budget.
//!
//! ## The driver's actuator is indirect — hence pending-reclaim accounting
//!
//! `release_for_eviction` does **not** free disk. It drops the blob's
//! GC-protecting tag; the bytes are reclaimed later by the independent
//! iroh-blobs GC sweep (`cache.gc_interval_sec`, default 300s). The driver ticks
//! far more often than that (`eviction_tick_secs`, default 1s), so a naive loop
//! that re-measured raw disk each tick would see its own releases have no effect
//! and keep releasing — roughly `gc_interval_sec / eviction_tick_secs` sweeps'
//! worth (~300 × 16 ≈ 4800 blobs at defaults) for a pressure event needing a
//! handful. That would drain the cache on a single high-water crossing.
//!
//! So the driver tracks `pending_reclaim`: bytes it has released but not yet
//! seen reclaimed. Decisions are made against the *effective* footprint
//! (`raw − pending_reclaim`) — what disk will look like once GC lands — while
//! the `decdn_cache_bytes` gauge still reports honest raw usage. When a
//! measurement comes in below the previous one, GC has run, and the observed
//! drop is credited against `pending_reclaim`. The accounting is self-correcting:
//! it needs no coupling to the GC schedule and recovers if a release is never
//! reclaimed.
//!
//! With GC disabled (`gc_interval_sec == 0`) nothing is ever reclaimed, so the
//! driver releases roughly one target's worth once and then idles rather than
//! stripping every tag in the cache. The runtime warns at startup in that case —
//! the driver cannot reclaim space and the ceiling is unenforceable.
//!
//! ## Hysteresis
//!
//! A latch: the driver starts evicting when the effective footprint crosses
//! `eviction_high_water_pct` and does not stop until it reaches
//! `eviction_target_pct`, then idles until the next high-water crossing. The
//! ≥5-point gap between the two (enforced at config resolution) is what prevents
//! thrash on writes hovering near the trigger.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use decdn_cache::{CacheEngine, CacheMetrics};
use tokio::sync::oneshot;

/// One mebibyte, the unit `cache.cache_size_mb` is expressed in.
const BYTES_PER_MB: u64 = 1_048_576;

/// Saturating `u64 -> i64` for gauge writes (`iroh_metrics::Gauge` is `i64`).
fn as_gauge(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Resolved driver parameters. Every field is range-checked at config
/// resolution (`resolve_cache_into`): `high_water_pct ∈ [60, 95]`,
/// `target_pct ∈ [40, 90]` **and** `target_pct <= high_water_pct - 5` (the
/// structural hysteresis gap), `per_sweep_budget ∈ [1, 256]`,
/// `tick ∈ [1s, 60s]`. The type itself carries no invariant — it is a wiring
/// DTO with a single production construction site.
#[derive(Debug, Clone, Copy)]
pub struct EvictionParams {
    pub cache_size_mb: u64,
    pub high_water_pct: u64,
    pub target_pct: u64,
    pub per_sweep_budget: u64,
    pub tick: Duration,
}

/// Cross-tick driver state. Kept out of `run`'s body so `tick` can be unit-tested.
#[derive(Debug, Default)]
struct DriverState {
    /// Hysteresis latch: `true` between a high-water crossing and reaching target.
    evicting: bool,
    /// Bytes released but not yet observed reclaimed by the iroh-blobs GC.
    pending_reclaim: u64,
    /// Previous raw measurement, used to detect a GC reclaim (a decrease).
    last_raw: Option<u64>,
}

/// `base * pct / 100` in u128 space, saturated back to u64.
fn pct_of(base: u64, pct: u64) -> u64 {
    u64::try_from(u128::from(base).saturating_mul(u128::from(pct)) / 100).unwrap_or(u64::MAX)
}

/// Fold a fresh raw measurement into the driver's pending-reclaim bookkeeping
/// and return the **effective** footprint to decide against.
///
/// A decrease since the previous tick means the iroh-blobs GC sweep landed, so
/// the observed drop is credited against what we are still waiting on. An
/// increase is unrelated new writes and leaves `pending_reclaim` alone. The
/// effective footprint is `raw - pending_reclaim`: what disk will look like once
/// the releases already issued are reclaimed.
///
/// This is the whole fix for the driver's original non-convergence: without it,
/// releases (which only drop GC protection) never moved the measurement, so the
/// latch stayed engaged and the driver re-released every tick for the whole GC
/// interval — ~`gc_interval_sec / eviction_tick_secs` sweeps' worth.
const fn reconcile(state: &mut DriverState, raw: u64) -> u64 {
    if let Some(prev) = state.last_raw
        && raw < prev
    {
        state.pending_reclaim = state.pending_reclaim.saturating_sub(prev - raw);
    }
    state.last_raw = Some(raw);
    raw.saturating_sub(state.pending_reclaim)
}

/// Advance the hysteresis latch for this tick's `effective` footprint and report
/// whether a sweep should run.
///
/// Latch, not a threshold: eviction starts only on a `high_water_bytes` crossing
/// and continues until `effective` reaches `target_bytes`, so writes hovering at
/// the trigger cannot thrash the driver. The ≥5-point gap between the two marks
/// is enforced at config resolution.
///
/// Pure and separated from [`tick`] so the boundary cases are directly testable
/// (the same reason [`reconcile`] is). `state.evicting` after the call is what
/// the `decdn_cache_evicting` gauge publishes.
const fn advance_latch(
    state: &mut DriverState,
    effective: u64,
    high_water_bytes: u64,
    target_bytes: u64,
) -> bool {
    if state.evicting {
        // Latched: keep sweeping until we actually reach target. Note this is
        // `<=`, so a driver that lands exactly ON target releases the latch —
        // reaching the goal is finishing, not a reason for one more pass.
        if effective <= target_bytes {
            state.evicting = false;
            return false;
        }
        return true;
    }
    if effective <= high_water_bytes {
        return false;
    }
    state.evicting = true;
    true
}

/// One budget-bounded eviction pass. Releases up to `budget` LRU candidates,
/// oldest access first, stopping early once the projected effective footprint
/// reaches `target_bytes`. Returns the number of bytes actually released (to be
/// added to `pending_reclaim`).
///
/// Emits `evictions_starved` when there is nothing left to evict while still
/// over target — either because the candidate set is empty (everything pinned,
/// operator-evicted, or probe-held, **or** a cold-started node whose in-memory
/// access map has not been populated by traffic yet) or because a whole pass
/// released nothing.
async fn sweep(
    cache: &CacheEngine,
    metrics: &CacheMetrics,
    effective: u64,
    target_bytes: u64,
    budget: u64,
    sizes: &HashMap<decdn_cache::Hash, u64>,
) -> u64 {
    let candidates = cache.eviction_candidates();
    if candidates.is_empty() {
        // Over target but no eligible victim — the operational-alarm signal.
        metrics.evictions_starved.inc();
        return 0;
    }

    let mut ordered: Vec<(decdn_cache::Hash, std::time::Instant)> =
        candidates.into_inner().into_iter().collect();
    ordered.sort_by_key(|(_, last)| *last); // oldest access first

    let mut freed: u64 = 0;
    let mut removed: u64 = 0;
    for (hash, _) in ordered {
        if effective.saturating_sub(freed) <= target_bytes {
            break;
        }
        if removed >= budget {
            // Budget spent for this tick; the latch keeps us evicting so the
            // next tick resumes toward target.
            break;
        }
        match cache.release_for_eviction(hash).await {
            // `Ok(0)` means nothing was released — the hash was pinned (the
            // documented defence-in-depth refusal) or carried no protecting tag.
            // Crediting it would charge the budget and, worse, add its size to
            // `freed`, letting the sweep break early believing it had freed
            // bytes it never touched.
            Ok(0) => {}
            Ok(_) => {
                freed = freed.saturating_add(sizes.get(&hash).copied().unwrap_or(0));
                removed = removed.saturating_add(1);
                metrics.evictions.inc();
            }
            Err(err) => {
                tracing::warn!(%hash, %err, "eviction driver: release_for_eviction failed; skipping hash");
            }
        }
    }

    if removed == 0 {
        // Had candidates but released none (all pinned/untagged/erroring). Same
        // operator signal as an empty candidate set — otherwise the driver looks
        // busy while making no progress.
        metrics.evictions_starved.inc();
    }
    metrics.evictions_bytes.inc_by(freed);
    freed
}

/// One driver tick: measure, refresh cache-health gauges, reconcile
/// pending-reclaim against observed GC progress, apply the hysteresis latch, and
/// (when latched over target) run one budget-bounded [`sweep`].
async fn tick(
    cache: &CacheEngine,
    metrics: &CacheMetrics,
    high_water_bytes: u64,
    target_bytes: u64,
    budget: u64,
    state: &mut DriverState,
) {
    // Exactly ONE store walk per tick: this snapshot is both the footprint
    // source and the sweep's per-hash size lookup, so `sweep` takes it by
    // reference rather than re-walking. (An earlier shape called `total_bytes()`
    // here and `size_snapshot()` again in the sweep — two walks under sustained
    // pressure, and the two readings could disagree.)
    //
    // A snapshot failure skips the whole tick rather than evicting blind:
    // without sizes there is no way to know when target is reached, and
    // degrading to an empty map silently drained the full budget every tick.
    let sizes = match cache.size_snapshot().await {
        Ok(s) => s,
        Err(err) => {
            metrics.size_measure_failures.inc();
            tracing::warn!(
                %err,
                "eviction driver: cache size measurement failed; skipping tick \
                 (decdn_cache_bytes is now stale — alert on decdn_cache_size_measure_failures_total)"
            );
            return;
        }
    };

    let raw = sizes.values().fold(0u64, |acc, sz| acc.saturating_add(*sz));
    let effective = reconcile(state, raw);

    // Gauges report honest raw usage; `pending_reclaim` is driver bookkeeping —
    // but it is EXPORTED too (#1678), because the gap between "released" and
    // "reclaimed" is the reclaim-lag signal, and with no admission control it is
    // the thing that decides whether one is needed.
    metrics.bytes.set(as_gauge(raw));
    metrics.pinned_count.set(as_gauge(
        u64::try_from(cache.pinned_snapshot().len()).unwrap_or(u64::MAX),
    ));
    metrics
        .pending_reclaim_bytes
        .set(as_gauge(state.pending_reclaim));
    // In-flight fills ride this tick rather than their own: the eviction driver
    // is already the one periodic cache observer, and `total_in_flight_bytes`
    // walks the registry map under a lock. Sampled BEFORE the latch returns early
    // below, so the gauge stays live while the cache sits under high-water — the
    // regime where a burst outrunning reclaim would first show up.
    metrics
        .fill_in_flight_bytes
        .set(as_gauge(cache.total_in_flight_bytes()));

    // Hysteresis latch, advanced as a pure function so a test can just ask what
    // it does at a boundary (same reasoning as `reconcile`).
    //
    // The `evicting` gauge is published on EVERY tick, latched or not — a gauge
    // that only updated when the driver swept would freeze at its last value
    // through the whole idle stretch, which is exactly the stretch an operator
    // reads it to rule out.
    let sweep_now = advance_latch(state, effective, high_water_bytes, target_bytes);
    metrics.evicting.set(i64::from(state.evicting));
    if !sweep_now {
        return;
    }

    let freed = sweep(cache, metrics, effective, target_bytes, budget, &sizes).await;
    state.pending_reclaim = state.pending_reclaim.saturating_add(freed);
    // Re-publish after the sweep: `pending_reclaim` just grew by everything this
    // pass released, and that increment is the lag this gauge exists to show.
    metrics
        .pending_reclaim_bytes
        .set(as_gauge(state.pending_reclaim));
}

/// Run the eviction driver until `shutdown` fires. Intended to be
/// `tasks.spawn(run(...))`-ed into the runtime `JoinSet`; returns when the
/// oneshot is dropped or sent.
pub async fn run(
    cache: CacheEngine,
    metrics: Arc<CacheMetrics>,
    params: EvictionParams,
    mut shutdown: oneshot::Receiver<()>,
) {
    let limit_bytes = params.cache_size_mb.saturating_mul(BYTES_PER_MB);
    let high_water_bytes = pct_of(limit_bytes, params.high_water_pct);
    let target_bytes = pct_of(limit_bytes, params.target_pct);

    // Static gauge: the configured ceiling. Set once — `cache_size_mb` is
    // restart-required.
    metrics.size_limit_bytes.set(as_gauge(limit_bytes));

    let mut ticker = tokio::time::interval(params.tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // burn the immediate first tick

    let mut state = DriverState::default();

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::debug!("eviction driver shutdown signal received");
                return;
            }
            _ = ticker.tick() => {}
        }
        tick(
            &cache,
            &metrics,
            high_water_bytes,
            target_bytes,
            params.per_sweep_budget,
            &mut state,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pct_of_computes_thresholds() {
        // 10 GiB ceiling → 90% high-water, 80% target.
        let limit = 10_240u64 * BYTES_PER_MB;
        assert_eq!(pct_of(limit, 90), limit * 9 / 10);
        assert_eq!(pct_of(limit, 80), limit * 8 / 10);
        assert_eq!(pct_of(limit, 100), limit);
        assert_eq!(pct_of(0, 90), 0);
    }

    #[test]
    fn pct_of_saturates_rather_than_overflowing() {
        // base * pct done in u128 space, so a huge base can't wrap.
        assert_eq!(pct_of(u64::MAX, 100), u64::MAX);
        assert_eq!(pct_of(u64::MAX, 50), u64::MAX / 2);
    }

    #[test]
    fn as_gauge_saturates_beyond_i64() {
        assert_eq!(as_gauge(0), 0);
        assert_eq!(as_gauge(1_048_576), 1_048_576);
        assert_eq!(as_gauge(u64::MAX), i64::MAX);
    }

    /// THE regression this driver's pending-reclaim accounting exists to
    /// prevent. `release_for_eviction` only drops GC protection, so raw disk
    /// does not move until the GC sweep runs — at defaults, 300 ticks later.
    /// Without the accounting the effective footprint stayed over target every
    /// tick and the driver kept releasing (~300 × 16 ≈ 4800 blobs) for a
    /// pressure event needing one sweep, draining the cache.
    #[test]
    fn released_bytes_are_not_re_released_while_gc_has_not_run_yet() {
        let mut state = DriverState::default();
        let raw = 1_000u64;
        let target = 800u64;

        // Tick 1: nothing pending, so effective == raw and we are over target.
        assert_eq!(reconcile(&mut state, raw), 1_000);
        // The sweep releases 250 bytes' worth.
        state.pending_reclaim += 250;

        // Tick 2..N: GC has NOT run, so raw is unchanged. Effective must now
        // read below target so the driver stops releasing.
        for _ in 0..10 {
            let effective = reconcile(&mut state, raw);
            assert_eq!(effective, 750, "effective must net out pending reclaim");
            assert!(
                effective <= target,
                "driver must not keep releasing for bytes already in flight"
            );
        }
    }

    /// When GC lands, the observed drop is credited against pending so the
    /// driver does not permanently under-count its own footprint.
    #[test]
    fn observed_gc_drop_is_credited_against_pending() {
        let mut state = DriverState::default();
        reconcile(&mut state, 1_000);
        state.pending_reclaim += 250;
        assert_eq!(reconcile(&mut state, 1_000), 750);

        // GC reclaims the 250 bytes: raw falls to 750, pending clears, and the
        // effective footprint equals the real one again.
        assert_eq!(reconcile(&mut state, 750), 750);
        assert_eq!(state.pending_reclaim, 0, "pending fully credited");
        assert_eq!(reconcile(&mut state, 750), 750);
    }

    /// New writes (a raw increase) must not be mistaken for GC progress.
    #[test]
    fn writes_growing_the_cache_do_not_clear_pending() {
        let mut state = DriverState::default();
        reconcile(&mut state, 1_000);
        state.pending_reclaim += 200;
        // Cache grows by 500 from new writes while the release is still pending.
        assert_eq!(reconcile(&mut state, 1_500), 1_300);
        assert_eq!(state.pending_reclaim, 200, "growth is not GC progress");
    }

    /// A partial GC reclaim credits only what was actually observed.
    #[test]
    fn partial_gc_reclaim_credits_only_the_observed_drop() {
        let mut state = DriverState::default();
        reconcile(&mut state, 1_000);
        state.pending_reclaim += 300;
        // Only 100 of the 300 pending bytes get reclaimed this cycle.
        assert_eq!(reconcile(&mut state, 900), 700);
        assert_eq!(state.pending_reclaim, 200);
    }

    // ---- the hysteresis latch, and the gauge that publishes it (#1678) ----

    const HIGH: u64 = 900;
    const TARGET: u64 = 800;

    #[test]
    fn latch_stays_clear_below_high_water() {
        let mut state = DriverState::default();
        assert!(!advance_latch(&mut state, HIGH, HIGH, TARGET));
        assert!(
            !state.evicting,
            "exactly AT high-water is not yet a crossing"
        );
    }

    #[test]
    fn latch_engages_above_high_water_and_holds_through_the_gap() {
        let mut state = DriverState::default();
        assert!(advance_latch(&mut state, HIGH + 1, HIGH, TARGET));
        assert!(state.evicting);

        // The whole point of the hysteresis gap: back under high-water but not
        // yet at target, the latch must HOLD. If it cleared here the driver
        // would stop short of target and re-trigger on the next write, which is
        // the thrash the gap exists to prevent.
        assert!(advance_latch(&mut state, HIGH - 1, HIGH, TARGET));
        assert!(state.evicting);
    }

    #[test]
    fn latch_releases_on_reaching_target_exactly() {
        let mut state = DriverState::default();
        advance_latch(&mut state, HIGH + 1, HIGH, TARGET);
        assert!(
            !advance_latch(&mut state, TARGET, HIGH, TARGET),
            "landing exactly on target is finishing, not a reason for one more pass"
        );
        assert!(!state.evicting);
    }

    /// The gauge is a direct read of the latch, so a sustained pressure event
    /// reads as a continuous `1` rather than a series of unrelated spikes —
    /// which is the distinction it was added to make.
    #[test]
    fn the_evicting_gauge_tracks_the_latch_across_a_full_cycle() {
        let mut state = DriverState::default();
        let gauge = |s: &DriverState| i64::from(s.evicting);

        assert_eq!(gauge(&state), 0);
        advance_latch(&mut state, HIGH + 50, HIGH, TARGET);
        assert_eq!(gauge(&state), 1);
        advance_latch(&mut state, HIGH - 10, HIGH, TARGET);
        assert_eq!(gauge(&state), 1, "still latched between the two marks");
        advance_latch(&mut state, TARGET - 1, HIGH, TARGET);
        assert_eq!(gauge(&state), 0);
    }
}
