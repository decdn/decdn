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

/// The shared resolver is covered against the admin port in `node/tests.rs`;
/// these pin that `node top` reads the metrics key and names the metrics
/// server.
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
