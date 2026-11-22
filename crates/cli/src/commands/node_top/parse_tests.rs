use super::*;

#[test]
fn parses_counter_and_gauge_lines() {
    let text = "\
# HELP decdn_serve_cache_hit Hits
# TYPE decdn_serve_cache_hit counter
decdn_serve_cache_hit_total 812
decdn_serve_cache_hit_created 1700000000.0
# HELP decdn_active_connections Active conns
# TYPE decdn_active_connections gauge
decdn_active_connections 4
# EOF
";
    let parsed = parse_openmetrics(text);
    assert_eq!(
        parsed.get("decdn_serve_cache_hit_total").copied(),
        Some(812.0)
    );
    assert_eq!(parsed.get("decdn_active_connections").copied(), Some(4.0));
    // `_created` lines are OpenMetrics counter-creation timestamps.
    // We pass them through (they parse as floats) — callers ignore
    // names they don't care about. The point of this assertion is
    // to lock in that we do not crash on them.
    assert!(parsed.contains_key("decdn_serve_cache_hit_created"));
}

#[test]
fn skips_comments_blank_lines_and_eof_marker() {
    let text = "\n# comment\n\n# EOF\n";
    let parsed = parse_openmetrics(text);
    assert!(parsed.is_empty(), "got: {parsed:?}");
}

#[test]
fn handles_scientific_notation_and_negative_values() {
    // OpenMetrics permits these for gauges; clock_skew or float
    // counters from histogram quantiles can show up scientific.
    let text = "decdn_some_gauge 1.5e3\ndecdn_other -1\n";
    let parsed = parse_openmetrics(text);
    assert_eq!(parsed.get("decdn_some_gauge").copied(), Some(1500.0));
    assert_eq!(parsed.get("decdn_other").copied(), Some(-1.0));
}

#[test]
fn ignores_lines_with_labels_silently() {
    // We only consume label-free metrics. A line like
    // `decdn_cache_misses_total{tenant="a"} 5` has a `{` in the
    // first whitespace-delimited token, which won't parse as a
    // bare metric name. Do not crash; just skip it.
    let text = "decdn_cache_misses_total{tenant=\"a\"} 5\ndecdn_cache_misses_total 7\n";
    let parsed = parse_openmetrics(text);
    assert_eq!(parsed.get("decdn_cache_misses_total").copied(), Some(7.0));
}
