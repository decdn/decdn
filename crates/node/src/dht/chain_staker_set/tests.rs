use super::*;

fn nid(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

fn fresh_state() -> (Arc<RwLock<HashSet<NodeId>>>, Arc<Metrics>) {
    (
        Arc::new(RwLock::new(HashSet::new())),
        Arc::new(Metrics::new()),
    )
}

/// `apply_change(Active)` on an absent `NodeId` inserts and reports the
/// mutation. Re-applying the same Active is a no-op — the set is unchanged
/// and the return says so. Idempotence is load-bearing: a re-scanned
/// `eth_getLogs` window replays events routinely.
#[test]
fn apply_change_active_idempotent() {
    let (active, metrics) = fresh_state();
    assert!(
        apply_change(&active, &metrics, StakerChange::Active(nid(1))),
        "first insert of an absent id is a real change"
    );
    assert!(active.read().unwrap().contains(&nid(1)));

    assert!(
        !apply_change(&active, &metrics, StakerChange::Active(nid(1))),
        "re-inserting a present id must report no change"
    );
    assert_eq!(active.read().unwrap().len(), 1);
}

/// `apply_change(Inactive)` removes and reports the mutation. Removing an
/// absent `NodeId` is a no-op.
#[test]
fn apply_change_inactive_idempotent() {
    let (active, metrics) = fresh_state();
    active.write().unwrap().insert(nid(1));
    assert!(
        apply_change(&active, &metrics, StakerChange::Inactive(nid(1))),
        "removing a present id is a real change"
    );
    assert!(!active.read().unwrap().contains(&nid(1)));

    assert!(
        !apply_change(&active, &metrics, StakerChange::Inactive(nid(2))),
        "removing an id that was never present must report no change"
    );
}

/// A real membership change republishes `decdn_staker_set_active_count`;
/// an idempotent no-op leaves it untouched. This is the gauge that lets
/// an operator spot a frozen/collapsed cache during a watcher outage
/// (#783).
#[test]
fn apply_change_updates_active_count_gauge_only_on_real_change() {
    let (active, metrics) = fresh_state();
    // Two distinct inserts → gauge tracks the growing set.
    apply_change(&active, &metrics, StakerChange::Active(nid(1)));
    apply_change(&active, &metrics, StakerChange::Active(nid(2)));
    let text = metrics.encode().unwrap();
    assert!(
        text.lines().any(|l| l == "decdn_staker_set_active_count 2"),
        "active-count gauge should report 2 after two distinct inserts:\n{text}"
    );

    // A no-op re-insert must not move the gauge.
    apply_change(&active, &metrics, StakerChange::Active(nid(1)));
    let text = metrics.encode().unwrap();
    assert!(
        text.lines().any(|l| l == "decdn_staker_set_active_count 2"),
        "active-count gauge should stay at 2 after an idempotent re-insert:\n{text}"
    );

    // A real removal shrinks it.
    apply_change(&active, &metrics, StakerChange::Inactive(nid(1)));
    let text = metrics.encode().unwrap();
    assert!(
        text.lines().any(|l| l == "decdn_staker_set_active_count 1"),
        "active-count gauge should report 1 after a removal:\n{text}"
    );
}

/// Simulate the watcher's drift-window accounting (#788): the restart
/// counter is edge-triggered, so repeated `backoff_started` calls during
/// one continuous outage (no intervening `cycle_established`) count as ONE
/// window. A fresh window requires a `cycle_established` in between.
/// Mirrors the `on_backoff`/`on_established` hooks the resumable poller fires.
/// Exercises the metric wiring without a live RPC provider (the real poll loop
/// needs a chain endpoint).
#[test]
fn watcher_error_restart_counts_one_per_drift_window() {
    let metrics = Arc::new(Metrics::new());
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_staker_set_watcher_restarts_total 0"),
        "restart counter should start at zero:\n{text}"
    );

    // First outage: three failed re-open attempts (three backoff
    // iterations) but a single continuous drift window → counts once.
    metrics.staker_set_watcher_backoff_started();
    metrics.staker_set_watcher_backoff_started();
    metrics.staker_set_watcher_backoff_started();
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_staker_set_watcher_restarts_total 1"),
        "one continuous outage should count exactly one restart:\n{text}"
    );

    // Filters re-establish (window closes), then a second outage opens a
    // new window → counts again.
    metrics.staker_set_watcher_cycle_established();
    metrics.staker_set_watcher_backoff_started();
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_staker_set_watcher_restarts_total 2"),
        "a second distinct outage should count a second restart:\n{text}"
    );
}

/// A `nodeIdOf` resolution failure in `RegistrySink::on_operator_change` bumps
/// `decdn_staker_set_watcher_resolve_failures_total` (#788). Exercises the
/// metric wiring directly — the `Err` arm calls exactly this method.
#[test]
fn watcher_resolve_failure_bumps_counter() {
    let metrics = Arc::new(Metrics::new());
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_staker_set_watcher_resolve_failures_total 0"),
        "resolve-failure counter should start at zero:\n{text}"
    );

    metrics.staker_set_watcher_resolve_failure();
    metrics.staker_set_watcher_resolve_failure();

    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_staker_set_watcher_resolve_failures_total 2"),
        "expected 2 resolve failures:\n{text}"
    );
}
