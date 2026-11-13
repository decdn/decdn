use super::*;

fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new())
}

fn peer(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

/// A distinct `NodeId` per `i`, packed into the first two bytes.
fn peer_n(i: u16) -> NodeId {
    let mut id = [0u8; 32];
    id[0] = u8::try_from((i >> 8) & 0xff).unwrap_or(0);
    id[1] = u8::try_from(i & 0xff).unwrap_or(0);
    NodeId::from_bytes(id)
}

fn strict_cfg() -> DhtRateLimitConfig {
    DhtRateLimitConfig {
        per_peer_rate_per_sec: 1.0,
        per_peer_burst: 1,
        global_rate_per_sec: 1.0,
        global_burst: 1,
        max_tracked_per_peer: 4096,
    }
}

#[test]
fn admits_first_request_under_default_caps() {
    let lim = DhtRateLimiter::new(&DhtRateLimitConfig::default(), metrics());
    assert_eq!(lim.check(&peer(1)), Ok(()));
}

#[test]
fn per_peer_layer_rejects_after_burst() {
    let mut cfg = strict_cfg();
    // Loosen the global layer so we're testing per-peer alone.
    cfg.global_burst = u32::MAX;
    cfg.global_rate_per_sec = 1e9;
    let lim = DhtRateLimiter::new(&cfg, metrics());
    let p = peer(1);
    assert_eq!(lim.check(&p), Ok(()));
    assert_eq!(lim.check(&p), Err(DhtRejectLayer::PerPeer));
}

#[test]
fn global_layer_rejects_after_burst() {
    let mut cfg = strict_cfg();
    cfg.per_peer_burst = u32::MAX;
    cfg.per_peer_rate_per_sec = 1e9;
    let lim = DhtRateLimiter::new(&cfg, metrics());
    // A different peer every time — only the global cap can fire.
    assert_eq!(lim.check(&peer(1)), Ok(()));
    assert_eq!(lim.check(&peer(2)), Err(DhtRejectLayer::Global));
}

#[test]
fn narrowest_first_per_peer_short_circuits_global() {
    // burst=1 everywhere. The first request consumes every bucket; the second
    // from the same peer must be reported as per-peer (narrowest layer), not
    // global.
    let lim = DhtRateLimiter::new(&strict_cfg(), metrics());
    assert_eq!(lim.check(&peer(1)), Ok(()));
    assert_eq!(lim.check(&peer(1)), Err(DhtRejectLayer::PerPeer));
}

/// A request the per-peer layer refuses spends no token of the shared global
/// budget: one peer that floods past its own cap cannot starve the others.
#[test]
fn a_refused_flooder_leaves_the_global_budget_to_other_peers() {
    let mut cfg = strict_cfg();
    // Global burst 2 and no refill: the flooder's one admitted request and one
    // honest request are the whole budget.
    cfg.global_burst = 2;
    cfg.global_rate_per_sec = 1e-9;
    cfg.per_peer_rate_per_sec = 1e-9;
    let lim = DhtRateLimiter::new(&cfg, metrics());
    assert_eq!(lim.check(&peer(1)), Ok(()));
    for _ in 0..100 {
        assert_eq!(lim.check(&peer(1)), Err(DhtRejectLayer::PerPeer));
    }
    assert_eq!(
        lim.check(&peer(2)),
        Ok(()),
        "the flooder's refused requests must not have spent the global budget"
    );
}

#[test]
fn rejection_increments_layer_metric_in_scrape() {
    // One unlabeled Counter per layer — the sibling-counter convention
    // settled in #1475, not a backend limitation (`iroh_metrics` does support
    // labels via `Family<L, M>`; `probe_hold_unavailable` uses one).
    // Operators recover the rolled-up rate with
    // `sum(rate({__name__=~"decdn_dht_rate_limit_rejected_(per_peer|global)_total"}[1m]))`.
    let metrics = metrics();
    let lim = DhtRateLimiter::new(&strict_cfg(), Arc::clone(&metrics));
    lim.check(&peer(1)).expect("first ok");
    let _ = lim.check(&peer(1));
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_dht_rate_limit_rejected_per_peer_total 1"),
        "per-peer rejection must appear in /metrics; got:\n{text}"
    );
}

#[test]
fn disabled_layer_admits_all() {
    let cfg = DhtRateLimitConfig {
        per_peer_rate_per_sec: 0.0,
        per_peer_burst: 0,
        global_rate_per_sec: 0.0,
        global_burst: 0,
        max_tracked_per_peer: 0,
    };
    let lim = DhtRateLimiter::new(&cfg, metrics());
    for _ in 0..1000 {
        assert_eq!(lim.check(&peer(1)), Ok(()));
    }
}

// ---- #648: two-stage batch token accounting (ADR 022 §Batch token
// accounting). `admit_batch_extra` is stage 2: after `check` has
// charged the stage-1 frame token, it consumes up to `extra` (= n-1)
// more units narrowest-first with short-circuit, exactly as `extra`
// separate `StoreRequest` admissions would. ----

#[test]
fn admit_batch_extra_grants_full_budget_when_permissive() {
    let lim = DhtRateLimiter::new(&DhtRateLimitConfig::default(), metrics());
    let p = peer(1);
    // Stage 1: the frame token (matches `serve`).
    assert_eq!(lim.check(&p), Ok(()));
    // Stage 2: a batch of n=10 wants n-1=9 extra; all granted under
    // the default burst caps (per-peer burst 40).
    assert_eq!(lim.admit_batch_extra(&p, 9), 9);
}

#[test]
fn admit_batch_extra_returns_zero_for_zero_extra() {
    // n=1 batch: extra = 0, nothing more to charge.
    let lim = DhtRateLimiter::new(&DhtRateLimitConfig::default(), metrics());
    assert_eq!(lim.admit_batch_extra(&peer(1), 0), 0);
}

#[test]
fn admit_batch_extra_bounded_by_per_peer_remaining() {
    // per-peer burst 5, global loose. Stage 1 burns 1 (4 left); a batch
    // wanting 10 extra gets only the 4 remaining.
    let mut cfg = strict_cfg();
    cfg.per_peer_rate_per_sec = 1.0;
    cfg.per_peer_burst = 5;
    cfg.global_burst = u32::MAX;
    cfg.global_rate_per_sec = 1e9;
    let lim = DhtRateLimiter::new(&cfg, metrics());
    let p = peer(1);
    assert_eq!(lim.check(&p), Ok(()));
    assert_eq!(lim.admit_batch_extra(&p, 10), 4);
    // Bucket now empty — a second batch gets nothing more.
    assert_eq!(lim.admit_batch_extra(&p, 10), 0);
}

#[test]
fn admit_batch_extra_bounded_by_min_across_layers() {
    // Global is the tightest layer (burst 3). Stage 1 burns 1 from each
    // (global 2 left); the batch gets min remaining = 2.
    let mut cfg = strict_cfg();
    cfg.per_peer_rate_per_sec = 1e9;
    cfg.per_peer_burst = u32::MAX;
    cfg.global_rate_per_sec = 1.0;
    cfg.global_burst = 3;
    let lim = DhtRateLimiter::new(&cfg, metrics());
    let p = peer(1);
    assert_eq!(lim.check(&p), Ok(()));
    assert_eq!(lim.admit_batch_extra(&p, 8), 2);
}

#[test]
fn admit_batch_extra_does_not_bump_rejection_counters() {
    // The partial-admit boundary is NOT a request rejection — the
    // frame was already admitted at stage 1. Draining the budget in
    // stage 2 must leave the rate-limit rejection counters at 0 so
    // operators don't see phantom rejections for a partially-admitted
    // batch.
    let mut cfg = strict_cfg();
    cfg.per_peer_rate_per_sec = 1.0;
    cfg.per_peer_burst = 2;
    cfg.global_burst = u32::MAX;
    cfg.global_rate_per_sec = 1e9;
    let metrics = metrics();
    let lim = DhtRateLimiter::new(&cfg, Arc::clone(&metrics));
    let p = peer(1);
    assert_eq!(lim.check(&p), Ok(()));
    // extra=5 but only 1 token left after stage 1 → grants 1, then
    // the budget is exhausted for the rest (no counter bump).
    assert_eq!(lim.admit_batch_extra(&p, 5), 1);
    let text = metrics.encode().unwrap();
    for layer in ["per_peer", "global"] {
        assert!(
            text.contains(&format!("decdn_dht_rate_limit_rejected_{layer}_total 0")),
            "stage-2 partial admit must not bump the {layer} rejection counter:\n{text}"
        );
    }
}

#[test]
fn layer_label_strings_are_stable() {
    // Pinned because operators alert on these label values.
    assert_eq!(DhtRejectLayer::PerPeer.as_str(), "per_peer");
    assert_eq!(DhtRejectLayer::Global.as_str(), "global");
}

// ---- #645: keyspace bound — periodic GC + lazy prune ----

#[test]
fn gc_per_peer_is_noop_when_layer_disabled() {
    let mut cfg = strict_cfg();
    cfg.per_peer_rate_per_sec = 0.0;
    cfg.per_peer_burst = 0;
    let lim = DhtRateLimiter::new(&cfg, metrics());
    assert!(lim.gc_per_peer().is_none());
    assert_eq!(lim.per_peer_tracked(), 0);
}

/// Fast refill (`rate=1000.0, burst=1`) so a 100ms sleep is enough for
/// `retain_recent` to drop every populated bucket.
#[tokio::test]
async fn gc_per_peer_drops_refilled_buckets() {
    let cfg = DhtRateLimitConfig {
        per_peer_rate_per_sec: 1000.0,
        per_peer_burst: 1,
        global_rate_per_sec: 1e6,
        global_burst: u32::MAX,
        // 0 so lazy-prune in `check` is out of scope here — the GC
        // method is the only path that can drop refilled buckets.
        max_tracked_per_peer: 0,
    };
    let lim = DhtRateLimiter::new(&cfg, metrics());
    for byte in 0..8u8 {
        let _ = lim.check(&peer(byte));
    }
    assert_eq!(lim.per_peer_tracked(), 8);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (before, after) = lim
        .gc_per_peer()
        .expect("per-peer layer enabled and CAS uncontended");
    assert_eq!(before, 8);
    assert_eq!(after, 0);
    assert_eq!(lim.per_peer_tracked(), 0);
}

/// Pins the `0 = unbounded` contract: 1000 distinct keys with cap=0
/// stay in the keyed map (no prune fires). Operators opting in to
/// the unbounded mode see the `tracing::warn!` from the resolver
/// (covered by the `decdn-common` config tests, not here).
#[test]
fn unbounded_when_cap_is_zero_per_peer() {
    let cfg = DhtRateLimitConfig {
        per_peer_rate_per_sec: 1e6,
        per_peer_burst: u32::MAX,
        global_rate_per_sec: 1e6,
        global_burst: u32::MAX,
        max_tracked_per_peer: 0,
    };
    let lim = DhtRateLimiter::new(&cfg, metrics());
    for i in 0..1000u16 {
        assert!(lim.check(&peer_n(i)).is_ok());
    }
    assert_eq!(lim.per_peer_tracked(), 1000);
}

/// `cap_per_peer` bounds the per-peer map. 200 distinct peers with a `cap`
/// of 16 should not exceed `cap + cap/10 + 1` (the `+1` covers
/// floor-division slack — the lazy prune fires at `len() > cap + cap/10`).
/// Fast refill so `retain_recent` can drop refilled buckets between bursts.
#[tokio::test]
async fn lazy_prune_bounds_per_peer_under_flood() {
    let cap: usize = 16;
    let cfg = DhtRateLimitConfig {
        per_peer_rate_per_sec: 1000.0,
        per_peer_burst: 1,
        global_rate_per_sec: 1e6,
        global_burst: u32::MAX,
        max_tracked_per_peer: cap,
    };
    let metrics_handle = metrics();
    let lim = DhtRateLimiter::new(&cfg, Arc::clone(&metrics_handle));
    for i in 0..200u16 {
        let _ = lim.check(&peer_n(i));
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let final_size = lim.per_peer_tracked();
    let bound = cap.saturating_add(cap / 10).saturating_add(1);
    assert!(
        final_size <= bound,
        "expected per_peer_tracked <= {bound}, got {final_size}"
    );
    // The lazy-prune path in `check` must also bump the prune-sweep counter,
    // which `gauge_and_sweep_counter_emit_in_scrape` (GC path only) does not
    // cover.
    let text = metrics_handle.encode().unwrap();
    assert!(
        text.contains("decdn_dht_rate_limit_prune_sweeps_per_peer_total"),
        "per-peer prune-sweep counter missing from scrape:\n{text}"
    );
    assert!(
        !text.contains("decdn_dht_rate_limit_prune_sweeps_per_peer_total 0"),
        "per-peer prune-sweep counter must have fired at least once during the flood:\n{text}"
    );
}

/// The sweep counter and gauge must appear in the encoded scrape after
/// `gc_per_peer` runs on an enabled limiter.
#[tokio::test]
async fn gauge_and_sweep_counter_emit_in_scrape() {
    let metrics_handle = metrics();
    let cfg = DhtRateLimitConfig {
        per_peer_rate_per_sec: 1000.0,
        per_peer_burst: 1,
        global_rate_per_sec: 1e6,
        global_burst: u32::MAX,
        max_tracked_per_peer: 0,
    };
    let lim = DhtRateLimiter::new(&cfg, Arc::clone(&metrics_handle));
    for i in 0..4u8 {
        let _ = lim.check(&peer(i));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(lim.gc_per_peer().is_some());
    let text = metrics_handle.encode().unwrap();
    assert!(
        text.contains("decdn_dht_rate_limit_prune_sweeps_per_peer_total 1"),
        "per-peer sweep counter missing from scrape:\n{text}"
    );
    assert!(
        text.contains("decdn_dht_rate_limit_tracked_per_peer 0"),
        "per-peer tracked gauge missing from scrape:\n{text}"
    );
}
