use super::*;

#[test]
fn json_includes_all_fields() {
    let s = Snapshot {
        uptime_seconds: 312,
        active_connections: 4,
        dispatch_in_flight: 2,
        cache_hits: 812,
        cache_misses: 94,
        cache_bytes_returned: 14_900_000,
        unredeemed_usdc: 12_345_678,
        rpc_healthy: true,
    };
    let json = render_json_snapshot(&s).unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["uptime_seconds"], 312);
    assert_eq!(v["cache_hits_total"], 812);
    assert_eq!(v["cache_misses_total"], 94);
    assert_eq!(v["cache_bytes_returned_total"], 14_900_000);
    assert_eq!(v["dispatch_in_flight"], 2);
    assert_eq!(v["active_connections"], 4);
    // Raw micro-USDC integer, not the dollar-formatted table value.
    assert_eq!(v["unredeemed_usdc_micro"], 12_345_678u64);
    assert_eq!(v["rpc_healthy"], true);
    // Cumulative hit_rate emitted as a fraction in [0,1] so
    // downstream tooling does its own formatting.
    let hr = v["cache_hit_rate"].as_f64().unwrap();
    assert!((hr - 812.0 / (812.0 + 94.0)).abs() < 1e-9);
}

#[test]
fn json_hit_rate_null_on_empty_cache() {
    let s = Snapshot {
        uptime_seconds: 0,
        active_connections: 0,
        dispatch_in_flight: 0,
        cache_hits: 0,
        cache_misses: 0,
        cache_bytes_returned: 0,
        unredeemed_usdc: 0,
        rpc_healthy: false,
    };
    let json = render_json_snapshot(&s).unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(v["cache_hit_rate"].is_null());
}
