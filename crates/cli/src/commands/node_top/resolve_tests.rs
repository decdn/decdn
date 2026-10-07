use super::*;

#[test]
fn append_metrics_path_handles_operator_url_shapes() {
    // Bare base — the canonical default.
    assert_eq!(
        append_metrics_path("http://127.0.0.1:9090"),
        "http://127.0.0.1:9090/metrics"
    );
    // Trailing slash — operator habit; must not produce `//metrics`.
    assert_eq!(
        append_metrics_path("http://127.0.0.1:9090/"),
        "http://127.0.0.1:9090/metrics"
    );
    // Already-complete URL — operator copy-pasted the full
    // endpoint; must not produce `/metrics/metrics`.
    assert_eq!(
        append_metrics_path("http://127.0.0.1:9090/metrics"),
        "http://127.0.0.1:9090/metrics"
    );
    // Trailing slash *and* `/metrics` — the worst case combo.
    assert_eq!(
        append_metrics_path("http://127.0.0.1:9090/metrics/"),
        "http://127.0.0.1:9090/metrics"
    );
    // Path-prefixed bases (reverse proxy etc.) get the suffix
    // appended cleanly — this is the documented "base URL" use.
    assert_eq!(
        append_metrics_path("http://h:9090/proxy"),
        "http://h:9090/proxy/metrics"
    );
}

#[test]
fn append_metrics_path_preserves_query_and_fragment() {
    // Operator-supplied URL with a query string: the path must
    // be appended *before* the `?`, not inside the query value.
    // Without the split, "http://h:9090?token=foo" would become
    // "http://h:9090?token=foo/metrics", which is a 404 on every
    // sane server.
    assert_eq!(
        append_metrics_path("http://h:9090?token=foo"),
        "http://h:9090/metrics?token=foo"
    );
    assert_eq!(
        append_metrics_path("http://h:9090/?token=foo"),
        "http://h:9090/metrics?token=foo"
    );
    assert_eq!(
        append_metrics_path("http://h:9090/metrics?token=foo"),
        "http://h:9090/metrics?token=foo"
    );
    assert_eq!(
        append_metrics_path("http://h:9090#frag"),
        "http://h:9090/metrics#frag"
    );
    // `?` then `#` — fragment after query, the URL standard
    // way; the split-on-first-special handles it because we
    // capture everything from the first delimiter onward.
    assert_eq!(
        append_metrics_path("http://h:9090?a=1#frag"),
        "http://h:9090/metrics?a=1#frag"
    );
}

#[test]
fn resolve_metrics_url_prefers_flag() {
    let got = resolve_metrics_url(Some("http://custom:1234"), None).unwrap();
    assert_eq!(got, "http://custom:1234");
}

#[test]
fn metrics_url_from_env_unset_is_none() {
    assert_eq!(metrics_url_from_env(None).unwrap(), None);
}

#[test]
fn metrics_url_from_env_valid_port_builds_loopback_url() {
    assert_eq!(
        metrics_url_from_env(Some("9999".to_string())).unwrap(),
        Some("http://127.0.0.1:9999".to_string())
    );
    assert_eq!(
        metrics_url_from_env(Some(" 9999 ".to_string())).unwrap(),
        Some("http://127.0.0.1:9999".to_string())
    );
}

#[test]
fn metrics_url_from_env_zero_and_malformed_error() {
    assert!(metrics_url_from_env(Some("0".to_string())).is_err());
    assert!(metrics_url_from_env(Some("notaport".to_string())).is_err());
    assert!(metrics_url_from_env(Some("70000".to_string())).is_err());
}

#[test]
fn port_from_config_file_returns_none_when_path_is_none() {
    // Locks the helper-layer contract that resolve_metrics_url
    // relies on for its `unwrap_or(DEFAULT_METRICS_PORT)`
    // fallback. Tested at the helper rather than the resolver
    // because resolve_metrics_url(None, None) reads
    // `~/.decdn/node.toml` if it exists, which is non-hermetic
    // on developer machines that have a real config file.
    assert_eq!(
        port_from_config_file(None, ConfigPathSource::Default).unwrap(),
        None
    );
    assert_eq!(
        port_from_config_file(None, ConfigPathSource::Explicit).unwrap(),
        None
    );
}

#[test]
fn resolve_metrics_url_reads_observability_metrics_port_from_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(&path, b"[observability]\nmetrics_port = 19999\n").unwrap();
    let got = resolve_metrics_url(None, Some(&path)).unwrap();
    assert_eq!(got, "http://127.0.0.1:19999");
}

#[test]
fn resolve_metrics_url_zero_port_errors() {
    // metrics_port = 0 is the operator opt-out (no metrics
    // server). Mirroring resolve_admin_url's behaviour. Asserts
    // the exact "disables the metrics server" wording so a
    // regression that drops the operator-actionable phrasing
    // (e.g. by collapsing the bail to just `bail!("port = 0")`)
    // breaks here.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(&path, b"[observability]\nmetrics_port = 0\n").unwrap();
    let err = resolve_metrics_url(None, Some(&path))
        .expect_err("expected error for zero port")
        .to_string();
    assert!(
        err.contains("disables the metrics server"),
        "missing context: {err}"
    );
}

#[test]
fn port_from_config_file_missing_default_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("absent.toml");
    assert_eq!(
        port_from_config_file(Some(&path), ConfigPathSource::Default).unwrap(),
        None
    );
}

#[test]
fn port_from_config_file_missing_explicit_errors() {
    // Explicit path that doesn't exist is a typo, not a
    // not-yet-configured operator — must surface as an error
    // (unlike the default-path branch above).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("absent.toml");
    let err = port_from_config_file(Some(&path), ConfigPathSource::Explicit)
        .expect_err("expected explicit-missing error")
        .to_string();
    assert!(
        err.contains("failed to read config file"),
        "missing context: {err}"
    );
}

#[test]
fn port_from_config_file_errors_on_invalid_toml() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("broken.toml");
    std::fs::write(&path, b"not = valid = toml").unwrap();
    let err = port_from_config_file(Some(&path), ConfigPathSource::Default)
        .expect_err("expected parse error")
        .to_string();
    assert!(err.contains("parse"), "missing context: {err}");
}

#[test]
fn port_from_config_file_ignores_unrelated_field_errors() {
    // Locks in the partial-deserializer choice: a wrong type in
    // some unrelated section (e.g. a malformed `[network]`
    // field that the full FileConfig would reject) must not stop
    // `decdn node top` from resolving the metrics port.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(
        &path,
        b"[observability]\nmetrics_port = 4242\n[network]\nbind_addr = 7\n",
    )
    .unwrap();
    assert_eq!(
        port_from_config_file(Some(&path), ConfigPathSource::Default).unwrap(),
        Some(4242)
    );
}

#[test]
fn port_from_config_file_reads_metrics_port_explicit_path() {
    // Parity with the admin-side test of the same name: locks
    // that the Explicit-source success arm produces the parsed
    // port. Without this, a regression that broadened the
    // explicit-source error arm to swallow successes would
    // still pass CI because the other tests use `Default`.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(&path, b"[observability]\nmetrics_port = 7777\n").unwrap();
    assert_eq!(
        port_from_config_file(Some(&path), ConfigPathSource::Explicit).unwrap(),
        Some(7777)
    );
}
