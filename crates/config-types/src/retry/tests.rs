use super::*;

#[test]
fn default_values_match_documented_constants() {
    let p = RetryPolicy::default();
    assert_eq!(p.max_retries, 3);
    assert_eq!(p.initial_backoff_ms, 100);
    assert_eq!(p.max_backoff_ms, 10_000);
    assert!((p.jitter_ratio - 0.1).abs() < f64::EPSILON);
    assert_eq!(p.buffered_max_bytes, DEFAULT_BUFFERED_MAX_BYTES);
    assert_eq!(p.buffered_max_bytes, 4 << 20);
}

#[test]
fn disabled_has_zero_retries() {
    let p = RetryPolicy::disabled();
    assert_eq!(p.max_retries, 0);
    // `disabled()` must also disable the buffer path (#519) — the
    // field name promises "no retry"; an operator setting the
    // policy to disabled and still seeing buffered drains would
    // be surprised.
    assert_eq!(p.buffered_max_bytes, 0);
}

#[test]
fn partial_section_fills_missing_fields_from_defaults() {
    // Operator-facing wire contract: a partial `[cache.origin_retry]`
    // (here as JSON, same serde path as the TOML config) must fill
    // every omitted field from `#[serde(default)]` / the named
    // `default = "default_buffered_max_bytes"` shim — NOT zero them.
    // A regression dropping either attribute would silently disable
    // retry or the #519 buffer path; pin it here.
    let p: RetryPolicy = serde_json::from_str(r#"{"max_retries": 5}"#).expect("deserialise");
    let d = RetryPolicy::default();
    assert_eq!(p.max_retries, 5, "explicit field must win");
    assert_eq!(p.initial_backoff_ms, d.initial_backoff_ms);
    assert_eq!(p.max_backoff_ms, d.max_backoff_ms);
    assert!((p.jitter_ratio - d.jitter_ratio).abs() < f64::EPSILON);
    // The load-bearing one: omitted `buffered_max_bytes` must route
    // through `default_buffered_max_bytes()` (4 MiB), not default to 0.
    assert_eq!(p.buffered_max_bytes, 4 << 20);
    assert_eq!(p.buffered_max_bytes, d.buffered_max_bytes);
}

#[test]
fn default_and_disabled_are_distinct() {
    // Footgun guard: `default()` is *not* `disabled()` — defaults
    // are opt-out (3 retries on by default per #285). A test
    // author writing `RetryPolicy::default()` to "get a no-op"
    // would actually retry 3 times with real sleeps. Pin the
    // distinction so a future "make default = disabled" change
    // is a deliberate, test-visible decision.
    assert_ne!(RetryPolicy::default(), RetryPolicy::disabled());
    assert!(RetryPolicy::default().max_retries > 0);
}
