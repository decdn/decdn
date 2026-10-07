use super::*;

#[test]
fn default_values_match_documented_constants() {
    let p = CircuitBreakerPolicy::default();
    assert!(p.enabled);
    assert_eq!(p.failure_threshold, 5);
    assert_eq!(p.cooldown_ms, 30_000);
    assert_eq!(p.half_open_max_calls, 1);
    assert!(p.is_active());
}

#[test]
fn disabled_is_not_active() {
    let p = CircuitBreakerPolicy::disabled();
    assert!(!p.enabled);
    assert!(!p.is_active());
}

#[test]
fn zero_threshold_collapses_to_inactive_even_if_enabled() {
    // Footgun guard: an operator who sets `enabled = true` but
    // `failure_threshold = 0` must NOT get a breaker that trips on
    // the first failure — it collapses to disabled.
    let p = CircuitBreakerPolicy {
        enabled: true,
        failure_threshold: 0,
        ..CircuitBreakerPolicy::default()
    };
    assert!(!p.is_active());
}

#[test]
fn partial_section_fills_missing_fields_from_defaults() {
    // Operator-facing wire contract: a partial `[cache.circuit_breaker]`
    // (here as JSON, same serde path as the TOML config) must fill
    // every omitted field from `#[serde(default)]` — NOT zero them.
    let p: CircuitBreakerPolicy =
        serde_json::from_str(r#"{"failure_threshold": 10}"#).expect("deserialise");
    let d = CircuitBreakerPolicy::default();
    assert_eq!(p.failure_threshold, 10, "explicit field must win");
    assert_eq!(p.enabled, d.enabled);
    assert_eq!(p.cooldown_ms, d.cooldown_ms);
    assert_eq!(p.half_open_max_calls, d.half_open_max_calls);
}

#[test]
fn default_and_disabled_are_distinct() {
    assert_ne!(
        CircuitBreakerPolicy::default(),
        CircuitBreakerPolicy::disabled()
    );
    assert!(CircuitBreakerPolicy::default().is_active());
}

#[test]
fn rejects_unknown_fields() {
    // `deny_unknown_fields` guards against typo'd config keys
    // silently being ignored.
    let r: Result<CircuitBreakerPolicy, _> = serde_json::from_str(r#"{"failrue_threshold": 10}"#);
    assert!(r.is_err());
}
