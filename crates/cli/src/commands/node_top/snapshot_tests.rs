use super::*;
use std::collections::HashMap;
use std::time::Duration;

fn fake_metrics(
    uptime: f64,
    hits: f64,
    misses: f64,
    bytes: f64,
    in_flight: f64,
    conns: f64,
    rpc: f64,
) -> HashMap<String, f64> {
    let mut m = HashMap::new();
    m.insert("decdn_node_uptime_seconds".into(), uptime);
    m.insert("decdn_cache_hits_total".into(), hits);
    m.insert("decdn_cache_misses_total".into(), misses);
    m.insert("decdn_cache_bytes_returned_total".into(), bytes);
    m.insert("decdn_dispatch_in_flight".into(), in_flight);
    m.insert("decdn_active_connections".into(), conns);
    m.insert("decdn_rpc_healthy".into(), rpc);
    m
}

#[test]
fn snapshot_reads_known_fields() {
    let s = Snapshot::from_metrics(&fake_metrics(
        312.0,
        812.0,
        94.0,
        14_900_000.0,
        2.0,
        4.0,
        1.0,
    ));
    assert_eq!(s.uptime_seconds, 312);
    assert_eq!(s.cache_hits, 812);
    assert_eq!(s.cache_misses, 94);
    assert_eq!(s.cache_bytes_returned, 14_900_000);
    assert_eq!(s.dispatch_in_flight, 2);
    assert_eq!(s.active_connections, 4);
    assert!(s.rpc_healthy);
}

#[test]
fn snapshot_missing_fields_default_to_zero() {
    // A scrape against an older daemon (missing some metric) must
    // not crash — render the missing fields as 0 so the operator
    // sees they're not flowing rather than getting an opaque error.
    let s = Snapshot::from_metrics(&HashMap::new());
    assert_eq!(s.cache_hits, 0);
    assert_eq!(s.cache_misses, 0);
    assert_eq!(s.cache_bytes_returned, 0);
    assert!(!s.rpc_healthy);
}

#[test]
fn hit_rate_basic() {
    assert_eq!(hit_rate(90, 10), Some(0.9));
    assert_eq!(hit_rate(0, 0), None); // avoid 0/0
    assert_eq!(hit_rate(7, 0), Some(1.0));
    assert_eq!(hit_rate(0, 5), Some(0.0));
}

#[test]
fn per_second_delta_uses_elapsed() {
    let prev = Snapshot::from_metrics(&fake_metrics(
        310.0,
        800.0,
        90.0,
        14_000_000.0,
        2.0,
        4.0,
        1.0,
    ));
    let now = Snapshot::from_metrics(&fake_metrics(
        312.0,
        812.0,
        94.0,
        14_420_000.0,
        2.0,
        4.0,
        1.0,
    ));
    let dt = Duration::from_secs(2);
    let d = SnapshotDelta::between(&prev, &now, dt);
    assert!((d.hits - 6.0).abs() < 1e-9); // (812-800)/2
    assert!((d.misses - 2.0).abs() < 1e-9); // (94-90)/2
    assert!((d.bytes - 210_000.0).abs() < 1e-9); // (14_420_000-14_000_000)/2
}

#[test]
fn per_second_delta_saturates_on_counter_regression() {
    // If the underlying counter went backwards between ticks
    // (daemon restart, URL re-pointing, instance reset), the
    // delta must show 0/s for that one tick rather than a huge
    // wraparound spike. This locks in the `saturating_sub` at
    // SnapshotDelta::between — a regression to plain `-` would
    // pass every other test in this file but fail this one.
    let prev = Snapshot::from_metrics(&fake_metrics(
        310.0,
        800.0,
        90.0,
        14_000_000.0,
        2.0,
        4.0,
        1.0,
    ));
    let now = Snapshot::from_metrics(&fake_metrics(311.0, 5.0, 1.0, 1024.0, 0.0, 0.0, 1.0));
    let d = SnapshotDelta::between(&prev, &now, Duration::from_secs(1));
    assert_eq!(d.hits, 0.0);
    assert_eq!(d.misses, 0.0);
    assert_eq!(d.bytes, 0.0);
}

#[test]
fn per_second_delta_zero_elapsed_returns_zero() {
    // If `Instant::now() - prev_at` is somehow zero (or sub-tick),
    // dividing would produce NaN/inf — return 0 instead so the
    // table never renders gibberish on a too-fast first refresh.
    let prev = Snapshot::from_metrics(&fake_metrics(0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0));
    let now = Snapshot::from_metrics(&fake_metrics(0.0, 2.0, 2.0, 2.0, 0.0, 0.0, 1.0));
    let d = SnapshotDelta::between(&prev, &now, Duration::ZERO);
    assert_eq!(d.hits, 0.0);
    assert_eq!(d.misses, 0.0);
    assert_eq!(d.bytes, 0.0);
}
