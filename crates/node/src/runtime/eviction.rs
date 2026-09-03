//! Cache-eviction driver loop (#1173, ADR 040 §Whole-blob reclaim; range
//! eviction is upstream-gated).
//!
//! A single async task, owned by the node wiring layer (per
//! `appendix-poc-production-seams.md`), that enforces the cache ceiling the
//! write path deliberately does not. It periodically measures on-disk footprint
//! and, once over the high-water mark, releases the blobs the injected
//! `EvictionPolicy` ranks for eviction (via
//! [`CacheEngine::release_for_eviction`], the soft-evict path that is *not* the
//! durable operator-evict) down to the target mark, bounded by a per-sweep
//! budget.
//!
//! ## The ceiling is disk-aware
//!
//! The ceiling is not the static `cache.cache_size_mb`. That is an upper bound;
//! each tick the driver probes free disk on the `cache_dir` volume and clamps
//! the ceiling down to keep `cache.disk_headroom_mb` free, defended against any
//! process (#1930). See `effective_cap`. So a large `cache_size_mb` lets a node
//! size its cache to whatever disk the machine offers, and the high-water and
//! target marks track that dynamic ceiling.
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
    /// Configured cache size in MiB; the upper bound on the ceiling (the disk
    /// clamp can only lower it, never raise it).
    pub cache_size_mb: u64,
    /// Free disk in MiB the driver keeps unused on the `cache_dir` volume by
    /// *any* process (#1930). Each tick the effective ceiling is capped so the
    /// cache cannot grow into the last `disk_headroom_mb` of free space; below
    /// it, the driver evicts to claw disk back. `0` opts out of the disk clamp
    /// (only `cache_size_mb` binds).
    pub disk_headroom_mb: u64,
    /// Crossing this percentage of the effective ceiling starts a sweep.
    pub high_water_pct: u64,
    /// A sweep runs until usage falls to this percentage. The gap to
    /// `high_water_pct` is the hysteresis that stops it flapping.
    pub target_pct: u64,
    /// Maximum blobs released in one sweep, so a sweep cannot monopolize the
    /// store.
    pub per_sweep_budget: u64,
    /// How often the driver re-reads usage.
    pub tick: Duration,
}

/// Per-tick ceiling inputs, precomputed once in [`run`] from [`EvictionParams`].
/// The effective ceiling itself is recomputed every tick from these plus the
/// live footprint and free disk (#1930), so a shared volume's ceiling tracks
/// real free space rather than a static config number.
#[derive(Debug, Clone, Copy)]
struct Ceiling {
    /// `cache_size_mb × MiB` — the absolute upper bound on the ceiling.
    config_limit_bytes: u64,
    /// `disk_headroom_mb × MiB` — free space kept on the volume.
    headroom_bytes: u64,
    /// Percent of the effective ceiling above which a sweep starts.
    high_water_pct: u64,
    /// Percent of the effective ceiling a sweep evicts down to.
    target_pct: u64,
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
    /// Whether the disk-headroom clamp — not `cache_size_mb` — is the binding
    /// ceiling. Tracked so the driver logs the transition once, not every tick.
    disk_clamped: bool,
    /// Whether the free-disk probe is currently failing. Tracked so a persistent
    /// statvfs error logs once on the way in and once on recovery, not per tick.
    disk_probe_failing: bool,
}

/// `base * pct / 100` in u128 space, saturated back to u64.
fn pct_of(base: u64, pct: u64) -> u64 {
    u64::try_from(u128::from(base).saturating_mul(u128::from(pct)) / 100).unwrap_or(u64::MAX)
}

/// The effective cache ceiling for one tick: the smaller of the configured
/// `cache_size_mb` budget and what free disk allows, recomputed every tick so a
/// shared volume's ceiling tracks real free space (#1930).
///
/// Free-space tracking keeps `disk_headroom_mb` of the volume unused by *any*
/// process. The cache may grow into whatever is free beyond that headroom, on
/// top of the bytes it already holds:
/// `disk_ceiling = footprint + max(0, free - headroom)`. At or below headroom
/// the ceiling collapses to the current footprint, so the driver evicts to claw
/// disk back toward the margin. A `free` of `u64::MAX` — the statvfs-failed
/// sentinel the driver substitutes on a probe error — saturates the disk term
/// so the configured budget binds, and a transient probe failure never shrinks
/// the cache to its footprint.
const fn effective_cap(
    config_limit_bytes: u64,
    footprint: u64,
    free_bytes: u64,
    headroom_bytes: u64,
) -> u64 {
    let disk_ceiling = footprint.saturating_add(free_bytes.saturating_sub(headroom_bytes));
    if config_limit_bytes < disk_ceiling {
        config_limit_bytes
    } else {
        disk_ceiling
    }
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

/// One budget-bounded eviction pass. Releases up to `budget` candidates in the
/// order `policy` ranks them, stopping early once the projected effective
/// footprint reaches `target_bytes`. Returns the number of bytes actually
/// released (to be added to `pending_reclaim`).
///
/// Emits `evictions_starved` when there is nothing left to evict while still
/// over target — either because the candidate set is empty (everything pinned,
/// operator-evicted, or probe-held, **or** a cold-started node whose in-memory
/// access map has not been populated by traffic yet) or because a whole pass
/// released nothing.
#[allow(clippy::too_many_arguments)]
async fn sweep(
    cache: &CacheEngine,
    metrics: &CacheMetrics,
    effective: u64,
    target_bytes: u64,
    budget: u64,
    cache_bytes: u64,
    sizes: &HashMap<decdn_cache::Hash, u64>,
    policy: &Arc<dyn decdn_cache::EvictionPolicy>,
    warming: &Arc<crate::warming_allowance::WarmingAllowance>,
) -> u64 {
    let candidates = cache.eviction_candidates();
    if candidates.is_empty() {
        // Over target but no eligible victim — the operational-alarm signal.
        metrics.evictions_starved.inc();
        return 0;
    }

    // The policy owns the whole sweep decision (ADR 040): it ranks candidates,
    // applies the target/budget stop conditions, and returns what to evict and
    // what to promote. The driver is a dumb executor.
    let segments = cache.segments_snapshot();
    let plan = policy.plan(&decdn_cache::EvictionContext {
        candidates: &candidates,
        sizes,
        segments: &segments,
        total_bytes: effective,
        target_bytes,
        budget,
        cache_bytes,
    });

    // Promotion is a pure in-memory segment move — no store I/O.
    for (hash, seg) in plan.promote {
        cache.set_segment(hash, seg);
    }

    let mut freed: u64 = 0;
    let mut removed: u64 = 0;
    for hash in plan.evict {
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
                // ADR 041: drop the warming tag for the evicted hash, so a later
                // reuse of this slot can never credit a stale source's allowance.
                warming.forget(hash);
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

/// Recompute the effective ceiling for one tick, publish the ceiling gauge, and
/// log the disk-clamp transition once (#1930).
///
/// `footprint` is the live raw on-disk footprint, paired with `free_bytes`
/// (both real on-disk state) inside [`effective_cap`]. Returns
/// `(cap, high_water_bytes, target_bytes)` for the latch and sweep. The clamp
/// becoming — or ceasing to be — the binding ceiling is the operator-visible
/// event, so it is logged on the transition, not every tick.
fn resolve_ceiling(
    ceiling: Ceiling,
    footprint: u64,
    free_bytes: u64,
    state: &mut DriverState,
    metrics: &CacheMetrics,
) -> (u64, u64, u64) {
    let cap = effective_cap(
        ceiling.config_limit_bytes,
        footprint,
        free_bytes,
        ceiling.headroom_bytes,
    );
    metrics.size_limit_bytes.set(as_gauge(cap));

    let clamped = cap < ceiling.config_limit_bytes;
    if clamped && !state.disk_clamped {
        tracing::warn!(
            effective_cap_bytes = cap,
            config_limit_bytes = ceiling.config_limit_bytes,
            free_bytes,
            headroom_bytes = ceiling.headroom_bytes,
            "eviction driver: free disk is the binding cache ceiling; cache.cache_size_mb \
             is clamped down to keep cache.disk_headroom_mb free on the volume"
        );
    } else if !clamped && state.disk_clamped {
        tracing::info!(
            config_limit_bytes = ceiling.config_limit_bytes,
            "eviction driver: free disk recovered; cache.cache_size_mb is the binding ceiling again"
        );
    }
    state.disk_clamped = clamped;

    (
        cap,
        pct_of(cap, ceiling.high_water_pct),
        pct_of(cap, ceiling.target_pct),
    )
}

/// One driver tick: measure, recompute the effective ceiling from the live
/// footprint and free disk (#1930), refresh cache-health gauges, reconcile
/// pending-reclaim against observed GC progress, apply the hysteresis latch, and
/// (when latched over target) run one budget-bounded [`sweep`].
///
/// `free_bytes` is the volume's available bytes probed for this tick, or
/// `u64::MAX` when the probe failed — the sentinel that disables the disk clamp
/// so `config_limit_bytes` binds (see [`effective_cap`]).
#[allow(clippy::too_many_arguments)]
async fn tick(
    cache: &CacheEngine,
    metrics: &CacheMetrics,
    ceiling: Ceiling,
    free_bytes: u64,
    budget: u64,
    state: &mut DriverState,
    policy: &Arc<dyn decdn_cache::EvictionPolicy>,
    warming: &Arc<crate::warming_allowance::WarmingAllowance>,
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

    // Gauges report honest raw usage; `pending_reclaim` is driver bookkeeping.
    metrics.bytes.set(as_gauge(raw));
    metrics.pinned_count.set(as_gauge(
        u64::try_from(cache.pinned_snapshot().len()).unwrap_or(u64::MAX),
    ));

    // Effective ceiling for this tick: the configured budget, clamped down to
    // real free disk (#1930). Publishes the ceiling gauge and logs the clamp
    // transition.
    let (cap, high_water_bytes, target_bytes) =
        resolve_ceiling(ceiling, raw, free_bytes, state, metrics);

    // Hysteresis latch: only start evicting on a high-water crossing; once
    // latched, keep evicting until at/below target, then release.
    if state.evicting {
        if effective <= target_bytes {
            state.evicting = false;
            return;
        }
    } else if effective <= high_water_bytes {
        return;
    } else {
        state.evicting = true;
    }

    let freed = sweep(
        cache,
        metrics,
        effective,
        target_bytes,
        budget,
        cap,
        &sizes,
        policy,
        warming,
    )
    .await;
    state.pending_reclaim = state.pending_reclaim.saturating_add(freed);
}

/// Probe free disk for one tick, or return the `u64::MAX` clamp-disabled
/// sentinel on a `statvfs` failure (#1930). One cheap syscall, dwarfed by the
/// store walk `tick` already does. On failure the driver falls back to the
/// config-only ceiling rather than evicting blind. The failure and recovery
/// transitions are each logged once, not every tick.
fn probe_free_bytes(cache_dir: &std::path::Path, state: &mut DriverState) -> u64 {
    match decdn_common::disk::statvfs_target(cache_dir) {
        Ok(space) => {
            if state.disk_probe_failing {
                tracing::info!(
                    cache_dir = %cache_dir.display(),
                    "eviction driver: free-disk probe recovered; disk-headroom clamp re-enabled"
                );
                state.disk_probe_failing = false;
            }
            space.avail
        }
        Err(err) => {
            if !state.disk_probe_failing {
                tracing::warn!(
                    %err,
                    cache_dir = %cache_dir.display(),
                    "eviction driver: free-disk probe failed; disk-headroom clamp disabled \
                     until it recovers (cache.cache_size_mb still enforced)"
                );
                state.disk_probe_failing = true;
            }
            u64::MAX
        }
    }
}

/// Run the eviction driver until `shutdown` fires. Intended to be
/// `tasks.spawn(run(...))`-ed into the runtime `JoinSet`; returns when the
/// oneshot is dropped or sent.
pub async fn run(
    cache: CacheEngine,
    metrics: Arc<CacheMetrics>,
    params: EvictionParams,
    cache_dir: std::path::PathBuf,
    policy: Arc<dyn decdn_cache::EvictionPolicy>,
    warming: Arc<crate::warming_allowance::WarmingAllowance>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let ceiling = Ceiling {
        config_limit_bytes: params.cache_size_mb.saturating_mul(BYTES_PER_MB),
        headroom_bytes: params.disk_headroom_mb.saturating_mul(BYTES_PER_MB),
        high_water_pct: params.high_water_pct,
        target_pct: params.target_pct,
    };

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

        let free_bytes = probe_free_bytes(&cache_dir, &mut state);

        tick(
            &cache,
            &metrics,
            ceiling,
            free_bytes,
            params.per_sweep_budget,
            &mut state,
            &policy,
            &warming,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write `payload` into an `Origin`-shaped on-disk layout (`<origin_dir>/<2-hex
    /// shard>/<full-hex hash>`) so a [`decdn_cache::FilesystemOrigin`] pointed at
    /// `origin_dir` can serve it. Shared by the Task 10 end-to-end policy tests
    /// below — each needs several distinct one-off blobs.
    fn write_origin_blob(
        origin_dir: &std::path::Path,
        payload: &[u8],
    ) -> anyhow::Result<decdn_cache::Hash> {
        let hash = decdn_cache::Hash::new(payload);
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let dir = origin_dir.join(shard);
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join(hex.as_str()), payload)?;
        Ok(hash)
    }

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

    /// The disk clamp binds below the configured budget when free space is
    /// scarce: with only 2 GiB of growth room past headroom, the ceiling is the
    /// footprint plus that room, far under the 100 GiB config.
    #[test]
    fn disk_ceiling_binds_below_configured_size_when_free_is_scarce() {
        const GIB: u64 = 1024 * 1024 * 1024;
        // config 100 GiB, footprint 20 GiB, 10 GiB free, 8 GiB headroom:
        // growth room = 10 - 8 = 2 GiB, so ceiling = 20 + 2 = 22 GiB.
        assert_eq!(
            effective_cap(100 * GIB, 20 * GIB, 10 * GIB, 8 * GIB),
            22 * GIB
        );
    }

    /// When the volume dwarfs the configured budget, the config number binds and
    /// disk imposes no extra clamp.
    #[test]
    fn configured_size_binds_when_disk_is_abundant() {
        const GIB: u64 = 1024 * 1024 * 1024;
        assert_eq!(effective_cap(10 * GIB, GIB, 500 * GIB, 8 * GIB), 10 * GIB);
    }

    /// At or below headroom there is no growth room, so the ceiling collapses to
    /// the current footprint and the driver will evict to claw disk back toward
    /// the headroom margin.
    #[test]
    fn ceiling_collapses_to_footprint_at_or_below_headroom() {
        const GIB: u64 = 1024 * 1024 * 1024;
        // Below headroom: 4 GiB free, 8 GiB demanded.
        assert_eq!(
            effective_cap(100 * GIB, 20 * GIB, 4 * GIB, 8 * GIB),
            20 * GIB
        );
        // Exactly at headroom: zero growth room, same boundary.
        assert_eq!(
            effective_cap(100 * GIB, 20 * GIB, 8 * GIB, 8 * GIB),
            20 * GIB
        );
    }

    /// `free = u64::MAX` is the statvfs-failed sentinel: the disk term saturates
    /// and the configured budget binds, so a transient probe error never shrinks
    /// the cache to its footprint.
    #[test]
    fn probe_failure_sentinel_disables_the_disk_clamp() {
        const GIB: u64 = 1024 * 1024 * 1024;
        assert_eq!(effective_cap(10 * GIB, GIB, u64::MAX, 8 * GIB), 10 * GIB);
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

    /// Proves `sweep` delegates victim ordering to the injected policy rather
    /// than sorting inline. `NewestFirst` reverses LRU order, so with a
    /// budget of 1 the most-recently-touched hash must be the one released —
    /// the opposite of what the old inline oldest-first sort would pick.
    #[tokio::test]
    async fn sweep_orders_via_injected_policy() -> anyhow::Result<()> {
        #[derive(Debug)]
        struct NewestFirst;
        impl decdn_cache::EvictionPolicy for NewestFirst {
            fn plan(&self, ctx: &decdn_cache::EvictionContext<'_>) -> decdn_cache::EvictionPlan {
                let mut v: Vec<_> = ctx.candidates.iter().map(|(h, t)| (*h, *t)).collect();
                v.sort_by_key(|(_, t)| std::cmp::Reverse(*t));
                let mut evict = Vec::new();
                let mut freed = 0u64;
                for (h, _) in v {
                    if ctx.total_bytes.saturating_sub(freed) <= ctx.target_bytes {
                        break;
                    }
                    if evict.len() as u64 >= ctx.budget {
                        break;
                    }
                    freed = freed.saturating_add(ctx.sizes.get(&h).copied().unwrap_or(0));
                    evict.push(h);
                }
                decdn_cache::EvictionPlan {
                    evict,
                    promote: Vec::new(),
                }
            }
        }

        let older_payload = b"eviction policy test: older blob";
        let newer_payload = b"eviction policy test: newer blob";
        let older_hash = decdn_cache::Hash::new(older_payload);
        let newer_hash = decdn_cache::Hash::new(newer_payload);

        let origin_dir = tempfile::tempdir()?;
        for (hash, payload) in [(older_hash, older_payload), (newer_hash, newer_payload)] {
            let hex = hash.to_hex();
            let shard = hex
                .get(..2)
                .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
            let dir = origin_dir.path().join(shard);
            std::fs::create_dir_all(&dir)?;
            std::fs::write(dir.join(hex.as_str()), payload)?;
        }

        let cache_dir = tempfile::tempdir()?;
        let origin =
            std::sync::Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
        let cache = CacheEngine::open(
            cache_dir.path(),
            vec![origin as std::sync::Arc<dyn decdn_cache::Origin>],
            16,
        )
        .await?;

        // Touch older first, then newer, so the two access times are ordered.
        let _ = cache.get(older_hash).await?;
        let _ = cache.get(newer_hash).await?;

        let sizes = cache.size_snapshot().await?;
        let metrics = CacheMetrics::default();
        let policy: Arc<dyn decdn_cache::EvictionPolicy> = Arc::new(NewestFirst);

        // target_bytes = 0 keeps the sweep over target for the whole pass;
        // budget = 1 stops it after exactly one release, so only the
        // policy's first-ranked victim gets evicted.
        let warming = Arc::new(crate::warming_allowance::WarmingAllowance::new(0, 0));
        sweep(
            &cache,
            &metrics,
            u64::MAX,
            0,
            1,
            u64::MAX,
            &sizes,
            &policy,
            &warming,
        )
        .await;

        let remaining = cache.eviction_candidates();
        assert!(
            remaining.contains_key(&older_hash),
            "older hash must survive — NewestFirst evicts the newer one first"
        );
        assert!(
            !remaining.contains_key(&newer_hash),
            "newer hash must be the one released under NewestFirst"
        );
        Ok(())
    }

    /// Free-space tracking (#1930): with a configured budget far larger than
    /// the cache, a scarce volume (here: zero free against a large headroom)
    /// collapses the effective ceiling to the current footprint, so a `tick`
    /// evicts even though `cache_size_mb` alone would never trigger.
    #[tokio::test]
    async fn disk_clamp_drives_eviction_below_configured_size() -> anyhow::Result<()> {
        const GIB: u64 = 1024 * 1024 * 1024;
        let origin_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;

        let mut hashes = Vec::new();
        for i in 0..4u32 {
            let payload = format!("disk clamp test: blob #{i}").into_bytes();
            hashes.push(write_origin_blob(origin_dir.path(), &payload)?);
        }

        let origin =
            std::sync::Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
        let cache = CacheEngine::open(
            cache_dir.path(),
            vec![origin as std::sync::Arc<dyn decdn_cache::Origin>],
            16,
        )
        .await?;
        for hash in &hashes {
            let _ = cache.get(*hash).await?;
        }

        let metrics = CacheMetrics::default();
        let policy: Arc<dyn decdn_cache::EvictionPolicy> = Arc::new(decdn_cache::LruEviction);
        let warming = Arc::new(crate::warming_allowance::WarmingAllowance::new(0, 0));
        let mut state = DriverState::default();

        // config budget is effectively unbounded, so on its own it would never
        // evict; the disk clamp is the only pressure. free_bytes = 0 against a
        // 100 GiB headroom forces the ceiling down to the footprint.
        let ceiling = Ceiling {
            config_limit_bytes: u64::MAX,
            headroom_bytes: 100 * GIB,
            high_water_pct: 90,
            target_pct: 80,
        };
        tick(
            &cache, &metrics, ceiling, 0, 16, &mut state, &policy, &warming,
        )
        .await;

        let remaining = cache.eviction_candidates();
        assert!(
            remaining.len() < hashes.len(),
            "disk clamp must force at least one eviction (had {}, left {})",
            hashes.len(),
            remaining.len()
        );
        assert!(
            state.disk_clamped,
            "state must record that the disk clamp is the binding ceiling"
        );
        Ok(())
    }

    /// Task 10 (ADR 040 end-to-end coverage): `tinylfu` admission + eviction,
    /// wired the way `crates/node/src/runtime/mod.rs` wires them — one shared
    /// [`decdn_cache::policy::TinyLfuEstimator`] feeding both a
    /// [`decdn_cache::policy::ProbationAdmission`] and the
    /// [`decdn_cache::policy::TinyLfuEviction`] sweep policy. Admits many
    /// distinct one-hit blobs (real `populate_local` misses through a real
    /// `CacheEngine` + `FilesystemOrigin`), drives one real [`sweep`] under a
    /// small `probation_target_pct`, and asserts the probation footprint the
    /// sweep leaves behind is under the cap while a twice-requested blob has
    /// graduated to `Main` — the observable [`CacheEngine::segment_of`] exposes.
    #[tokio::test]
    async fn probation_cap_bounds_one_hit_wonders() -> anyhow::Result<()> {
        use decdn_cache::Segment;
        use decdn_cache::policy::{ProbationAdmission, TinyLfuEstimator, TinyLfuEviction};

        let promotion_threshold: u32 = 2;
        let probation_target_pct: u64 = 10;
        let cache_bytes: u64 = 1_000;

        let origin_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;

        let mut cold_hashes = Vec::new();
        for i in 0..20u32 {
            let payload = format!("probation cap test: one-hit blob #{i}").into_bytes();
            cold_hashes.push(write_origin_blob(origin_dir.path(), &payload)?);
        }
        let hot_hash = write_origin_blob(origin_dir.path(), b"probation cap test: hot blob")?;

        let origin =
            std::sync::Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
        let cache = CacheEngine::open(
            cache_dir.path(),
            vec![origin as std::sync::Arc<dyn decdn_cache::Origin>],
            16,
        )
        .await?;

        let freq: std::sync::Arc<dyn decdn_cache::FrequencyEstimator> =
            std::sync::Arc::new(TinyLfuEstimator::new(4096));
        cache.set_frequency_estimator(freq.clone());
        cache.set_admission_policy(std::sync::Arc::new(ProbationAdmission {
            freq: freq.clone(),
            promotion_threshold,
        }));

        // Each cold blob is one served miss: `populate_local` fills (admission
        // reads estimate 0 < threshold, so Probation) and the paired serve emits
        // the one sighting via `observe_hit` (estimate -> 1). ADR 040 splits the
        // fill from the hit signal, so the test drives both, mirroring the real
        // fill-then-serve path.
        for hash in &cold_hashes {
            cache.populate_local(*hash).await?;
            cache.observe_hit(*hash);
        }
        // The hot blob is requested twice. First request: fill admits (estimate
        // 0 < threshold) into Probation, serve observes (estimate -> 1). Second
        // request: fill is a hit (no re-admission), serve observes (estimate ->
        // 2), so the shared estimator now reads >= threshold for the sweep below
        // to promote.
        cache.populate_local(hot_hash).await?;
        cache.observe_hit(hot_hash);
        cache.populate_local(hot_hash).await?;
        cache.observe_hit(hot_hash);

        for hash in cold_hashes.iter().chain(std::iter::once(&hot_hash)) {
            assert_eq!(
                cache.segment_of(*hash),
                Segment::Probation,
                "first sight must land in probation"
            );
        }

        let sizes = cache.size_snapshot().await?;
        let metrics = CacheMetrics::default();
        let policy: Arc<dyn decdn_cache::EvictionPolicy> = Arc::new(TinyLfuEviction::new(
            freq.clone(),
            promotion_threshold,
            probation_target_pct,
        ));

        let effective: u64 = sizes.values().sum();
        // target_bytes = u64::MAX keeps the sweep's global-target loop
        // (phase 3) inert, so only the probation cap (phase 2) drives
        // eviction here; a wide budget lets the whole cap overage clear in
        // one sweep.
        let warming = Arc::new(crate::warming_allowance::WarmingAllowance::new(0, 0));
        sweep(
            &cache,
            &metrics,
            effective,
            u64::MAX,
            100,
            cache_bytes,
            &sizes,
            &policy,
            &warming,
        )
        .await;

        let probation_limit = cache_bytes
            .saturating_mul(probation_target_pct)
            .saturating_div(100);
        let footprint = cache.segment_bytes(Segment::Probation, &sizes);
        assert!(
            footprint <= probation_limit,
            "probation footprint {footprint} must drop under the cap {probation_limit}"
        );
        assert_eq!(
            cache.segment_of(hot_hash),
            Segment::Main,
            "twice-requested blob must be promoted to Main"
        );

        Ok(())
    }

    /// Task 10: a hot blob primed with many requests must survive a sweep that
    /// clears a whole burst of one-hit cold blobs, driven through the real
    /// engine + [`sweep`] with `tinylfu` eviction (frequency-ranked, not
    /// recency-ranked — an LRU policy would have evicted the hot blob here
    /// since it was the least-recently-touched by wall-clock order once the
    /// colds land after it).
    #[tokio::test]
    async fn hot_set_survives_cold_scan() -> anyhow::Result<()> {
        use decdn_cache::policy::{TinyLfuEstimator, TinyLfuEviction};

        let origin_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;

        let hot_hash = write_origin_blob(origin_dir.path(), b"cold scan test: hot blob")?;
        let mut cold_hashes = Vec::new();
        for i in 0..10u32 {
            let payload = format!("cold scan test: cold blob #{i}").into_bytes();
            cold_hashes.push(write_origin_blob(origin_dir.path(), &payload)?);
        }

        let origin =
            std::sync::Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
        let cache = CacheEngine::open(
            cache_dir.path(),
            vec![origin as std::sync::Arc<dyn decdn_cache::Origin>],
            16,
        )
        .await?;

        let freq: std::sync::Arc<dyn decdn_cache::FrequencyEstimator> =
            std::sync::Arc::new(TinyLfuEstimator::new(4096));
        cache.set_frequency_estimator(freq.clone());
        // Admission stays default (`AlwaysAdmit`) — this test exercises the
        // eviction ranking, not the probation lifecycle.

        // Prime the hot blob well past any cold blob's frequency: each request
        // fills (first admits, the rest are hits) and the paired serve emits one
        // sighting via `observe_hit`, bumping the shared estimator each time (ADR
        // 040 splits fill from the hit signal).
        for _ in 0..20 {
            cache.populate_local(hot_hash).await?;
            cache.observe_hit(hot_hash);
        }
        for hash in &cold_hashes {
            cache.populate_local(*hash).await?;
            cache.observe_hit(*hash);
        }

        let sizes = cache.size_snapshot().await?;
        let metrics = CacheMetrics::default();
        let policy: Arc<dyn decdn_cache::EvictionPolicy> =
            Arc::new(TinyLfuEviction::new(freq.clone(), 2, 100));

        let effective: u64 = sizes.values().sum();
        let hot_size = sizes.get(&hot_hash).copied().unwrap_or(0);
        // Target = the hot blob's own size: least-frequent-first ranking
        // must clear every cold blob before it ever reaches the hot one, and
        // a budget spanning every candidate leaves no room for the sweep to
        // stop early for the wrong reason.
        sweep(
            &cache,
            &metrics,
            effective,
            hot_size,
            (cold_hashes.len() + 1) as u64,
            effective,
            &sizes,
            &policy,
            &Arc::new(crate::warming_allowance::WarmingAllowance::new(0, 0)),
        )
        .await;

        let remaining = cache.eviction_candidates();
        assert!(
            remaining.contains_key(&hot_hash),
            "hot blob must survive the cold scan"
        );
        for hash in &cold_hashes {
            assert!(
                !remaining.contains_key(hash),
                "cold blob must be evicted under pressure"
            );
        }

        Ok(())
    }

    /// Task 10: with the config defaults (`always` admission + `lru` eviction,
    /// no `tinylfu` estimator wired at all) a real engine's sweep still matches
    /// a golden least-recently-used sequence — the end-to-end guard that
    /// selecting `tinylfu` elsewhere in this test module left the default path
    /// behaviorally untouched (Stage A neutrality).
    #[tokio::test]
    async fn lru_default_unchanged() -> anyhow::Result<()> {
        let origin_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;

        let a = write_origin_blob(origin_dir.path(), b"lru golden test: blob A")?;
        let b = write_origin_blob(origin_dir.path(), b"lru golden test: blob B")?;
        let c = write_origin_blob(origin_dir.path(), b"lru golden test: blob C")?;
        let d = write_origin_blob(origin_dir.path(), b"lru golden test: blob D")?;

        let origin =
            std::sync::Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
        let cache = CacheEngine::open(
            cache_dir.path(),
            vec![origin as std::sync::Arc<dyn decdn_cache::Origin>],
            16,
        )
        .await?;

        // No frequency estimator, no custom admission policy: exactly the
        // engine's out-of-the-box defaults.
        let _ = cache.get(a).await?;
        let _ = cache.get(b).await?;
        let _ = cache.get(c).await?;
        let _ = cache.get(d).await?;
        // Re-access B: the golden recency order is now oldest-first A, C, D, B.
        let _ = cache.get(b).await?;

        let sizes = cache.size_snapshot().await?;
        let metrics = CacheMetrics::default();
        let policy: Arc<dyn decdn_cache::EvictionPolicy> = Arc::new(decdn_cache::LruEviction);
        let effective: u64 = sizes.values().sum();

        // budget = 2, target = 0: evicts exactly the two oldest-by-access —
        // the golden LRU sequence this test guards end to end.
        sweep(
            &cache,
            &metrics,
            effective,
            0,
            2,
            effective,
            &sizes,
            &policy,
            &Arc::new(crate::warming_allowance::WarmingAllowance::new(0, 0)),
        )
        .await;

        let remaining = cache.eviction_candidates();
        assert!(
            !remaining.contains_key(&a),
            "oldest access must be evicted first"
        );
        assert!(
            !remaining.contains_key(&c),
            "second-oldest access must be evicted next"
        );
        assert!(
            remaining.contains_key(&d),
            "recently accessed D must survive"
        );
        assert!(remaining.contains_key(&b), "re-touched B must survive");

        Ok(())
    }
}
