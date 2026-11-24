use super::*;

#[test]
fn default_values_match_documented_constants() {
    let p = RetryPolicy::default();
    assert_eq!(p.max_retries, 3);
    assert_eq!(p.initial_backoff_ms, 100);
    assert_eq!(p.max_backoff_ms, 10_000);
    assert!((p.jitter_ratio - 0.1).abs() < f64::EPSILON);
}

#[test]
fn disabled_has_zero_retries() {
    let p = RetryPolicy::disabled();
    assert_eq!(p.max_retries, 0);
}

#[test]
fn partial_section_fills_missing_fields_from_defaults() {
    // Operator-facing wire contract: a partial `[cache.origin_retry]`
    // (here as JSON, same serde path as the TOML config) must fill
    // every omitted field from `#[serde(default)]` — NOT zero them.
    let p: RetryPolicy = serde_json::from_str(r#"{"max_retries": 5}"#).expect("deserialise");
    let d = RetryPolicy::default();
    assert_eq!(p.max_retries, 5, "explicit field must win");
    assert_eq!(p.initial_backoff_ms, d.initial_backoff_ms);
    assert_eq!(p.max_backoff_ms, d.max_backoff_ms);
    assert!((p.jitter_ratio - d.jitter_ratio).abs() < f64::EPSILON);
}
