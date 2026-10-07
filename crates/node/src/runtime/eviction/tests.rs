use super::*;

/// Write `payload` into an `Origin`-shaped on-disk layout (`<origin_dir>/<2-hex
/// shard>/<full-hex hash>`) so a [`decdn_cache::FilesystemOrigin`] pointed at
/// `origin_dir` can serve it. Shared by the ADR 040 end-to-end policy tests
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
    let origin = std::sync::Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
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

    let origin = std::sync::Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
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

/// A node restarted above its size limit releases blobs from before the
/// restart on its first tick, with no traffic, instead of starving (#2221).
#[tokio::test]
async fn restarted_over_limit_cache_evicts_without_traffic() -> anyhow::Result<()> {
    let origin_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let mut hashes = Vec::new();
    for i in 0..4u32 {
        let payload = format!("restart eviction test: blob #{i}").into_bytes();
        hashes.push(write_origin_blob(origin_dir.path(), &payload)?);
    }
    {
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
        cache.shutdown().await?;
    }

    // Reopen with no origin and serve nothing: every blob is from before
    // the restart.
    let cache = CacheEngine::open(cache_dir.path(), Vec::new(), 16).await?;
    let metrics = CacheMetrics::default();
    let policy: Arc<dyn decdn_cache::EvictionPolicy> = Arc::new(decdn_cache::LruEviction);
    let warming = Arc::new(crate::warming_allowance::WarmingAllowance::new(0, 0));
    let mut state = DriverState::default();

    // A 1-byte budget puts the reopened cache over high water. free_bytes =
    // u64::MAX disables the disk clamp, so only the config limit binds.
    let ceiling = Ceiling {
        config_limit_bytes: 1,
        headroom_bytes: 0,
        high_water_pct: 90,
        target_pct: 80,
    };
    tick(
        &cache,
        &metrics,
        ceiling,
        u64::MAX,
        16,
        &mut state,
        &policy,
        &warming,
    )
    .await;

    anyhow::ensure!(
        metrics.evictions.get() > 0,
        "the first tick after a restart must release pre-restart blobs"
    );
    anyhow::ensure!(
        metrics.evictions_starved.get() == 0,
        "a restarted over-limit cache must not starve"
    );
    anyhow::ensure!(
        cache.eviction_candidates().len() < hashes.len(),
        "released blobs must leave the candidate set"
    );
    Ok(())
}

/// ADR 040 end-to-end coverage: `tinylfu` admission + eviction,
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

    let origin = std::sync::Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
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

/// A hot blob primed with many requests must survive a sweep that
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

    let origin = std::sync::Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
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

/// With the config defaults (`always` admission + `lru` eviction, no
/// `tinylfu` estimator wired at all) a real engine's sweep matches a golden
/// least-recently-used sequence — the end-to-end guard that the default
/// path stays plain LRU, independent of the `tinylfu` wiring.
#[tokio::test]
async fn lru_default_unchanged() -> anyhow::Result<()> {
    let origin_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let a = write_origin_blob(origin_dir.path(), b"lru golden test: blob A")?;
    let b = write_origin_blob(origin_dir.path(), b"lru golden test: blob B")?;
    let c = write_origin_blob(origin_dir.path(), b"lru golden test: blob C")?;
    let d = write_origin_blob(origin_dir.path(), b"lru golden test: blob D")?;

    let origin = std::sync::Arc::new(decdn_cache::FilesystemOrigin::new(origin_dir.path()).await?);
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
