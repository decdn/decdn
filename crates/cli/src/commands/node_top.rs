//! `decdn node top` — live view of node activity scraped from the
//! daemon's loopback `/metrics` HTTP endpoint (issue #275).

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use decdn_common::cli;

/// Parse an `OpenMetrics` text body into `name -> value`.
///
/// Scope is deliberate: only label-free counters and gauges. Lines
/// starting with `#` (HELP/TYPE/UNIT/EOF directives) are skipped.
/// Lines whose name token contains `{` (label-bearing series) are
/// skipped — `node top` only consumes the bare `decdn_*` series the
/// daemon registers (`crates/node/src/metrics.rs`), none of which
/// carry labels today, so silently ignoring labelled lines is the
/// correct degradation if the metric surface grows them later.
///
/// Counter `_created` timestamp lines parse as floats; callers look
/// up the names they want and ignore the rest.
#[allow(dead_code)] // Wired into the fetch loop in a later task.
pub(crate) fn parse_openmetrics(text: &str) -> HashMap<String, f64> {
    let mut out = HashMap::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((name, rest)) = trimmed.split_once(char::is_whitespace) else {
            continue;
        };
        if name.contains('{') {
            continue;
        }
        let value_token = rest.split_whitespace().next().unwrap_or("");
        if let Ok(v) = value_token.parse::<f64>() {
            out.insert(name.to_string(), v);
        }
    }
    out
}

/// Selected fields of a single `/metrics` scrape, normalised to
/// integer types so the renderer can format them without re-checking
/// for fractional values from the `OpenMetrics` float wire type.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Wired into the fetch loop in a later task.
pub struct Snapshot {
    pub uptime_seconds: u64,
    pub active_connections: u64,
    pub dispatch_in_flight: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_bytes_returned: u64,
    pub rpc_healthy: bool,
}

impl Snapshot {
    #[allow(dead_code)] // Wired into the fetch loop in a later task.
    pub(crate) fn from_metrics(m: &HashMap<String, f64>) -> Self {
        // Saturating cast: gauges/counters are non-negative in
        // practice (the `rpc_healthy` 0/1 gauge included). A negative
        // value here would only arise from a clock-skew gauge we
        // don't read, so saturating-to-zero is safe.
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let u = |name: &str| -> u64 {
            m.get(name).copied().map_or(0, |v| {
                if v < 0.0 || !v.is_finite() {
                    0
                } else if v >= u64::MAX as f64 {
                    u64::MAX
                } else {
                    v as u64
                }
            })
        };
        Self {
            uptime_seconds: u("decdn_uptime_seconds"),
            active_connections: u("decdn_active_connections"),
            dispatch_in_flight: u("decdn_dispatch_in_flight"),
            cache_hits: u("decdn_cache_hits_total"),
            cache_misses: u("decdn_cache_misses_total"),
            cache_bytes_returned: u("decdn_cache_bytes_returned_total"),
            rpc_healthy: m.get("decdn_rpc_healthy").copied().unwrap_or(0.0) >= 0.5,
        }
    }
}

/// Per-second deltas between two consecutive snapshots.
#[derive(Debug, Clone)]
#[allow(dead_code, clippy::struct_field_names)] // Wired into the renderer in a later task.
pub(crate) struct SnapshotDelta {
    pub hits_per_sec: f64,
    pub misses_per_sec: f64,
    pub bytes_per_sec: f64,
}

impl SnapshotDelta {
    #[allow(dead_code)] // Wired into the renderer in a later task.
    pub(crate) fn between(prev: &Snapshot, now: &Snapshot, elapsed: Duration) -> Self {
        let secs = elapsed.as_secs_f64();
        if secs <= 0.0 {
            return Self {
                hits_per_sec: 0.0,
                misses_per_sec: 0.0,
                bytes_per_sec: 0.0,
            };
        }
        // Saturating subtraction in case the daemon was restarted
        // mid-loop and the new counter is lower than the previous —
        // showing 0/s for that tick is more honest than a huge spike
        // back from the wraparound.
        #[allow(clippy::cast_precision_loss)]
        let d = |a: u64, b: u64| (a.saturating_sub(b)) as f64 / secs;
        Self {
            hits_per_sec: d(now.cache_hits, prev.cache_hits),
            misses_per_sec: d(now.cache_misses, prev.cache_misses),
            bytes_per_sec: d(now.cache_bytes_returned, prev.cache_bytes_returned),
        }
    }
}

/// Cumulative hit ratio over `(hits + misses)`. `None` when the
/// denominator is 0 — distinct from `Some(0.0)` ("only misses so
/// far") so the renderer prints a literal `n/a` rather than a
/// misleading `0.0%`.
#[allow(dead_code, clippy::cast_precision_loss)] // Wired into the renderer in a later task.
pub(crate) fn hit_rate(hits: u64, misses: u64) -> Option<f64> {
    let total = hits + misses;
    if total == 0 {
        None
    } else {
        Some(hits as f64 / total as f64)
    }
}

/// Entry point dispatched from `node_dispatch`. Currently a stub —
/// later tasks add the metrics fetch, parse, and render loop. The
/// signature is `async` because the dispatch arm awaits it; clippy
/// would otherwise flag the missing await on this scaffolding.
#[allow(clippy::unused_async)]
pub async fn run(_args: &cli::TopArgs, _global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::bail!("decdn node top is not yet implemented")
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod parse_tests {
    use super::*;

    #[test]
    fn parses_counter_and_gauge_lines() {
        let text = "\
# HELP decdn_cache_hits_total Hits
# TYPE decdn_cache_hits_total counter
decdn_cache_hits_total 812
decdn_cache_hits_created 1700000000.0
# HELP decdn_active_connections Active conns
# TYPE decdn_active_connections gauge
decdn_active_connections 4
# EOF
";
        let parsed = parse_openmetrics(text);
        assert_eq!(parsed.get("decdn_cache_hits_total").copied(), Some(812.0));
        assert_eq!(parsed.get("decdn_active_connections").copied(), Some(4.0));
        // `_created` lines are OpenMetrics counter-creation timestamps.
        // We pass them through (they parse as floats) — callers ignore
        // names they don't care about. The point of this assertion is
        // to lock in that we do not crash on them.
        assert!(parsed.contains_key("decdn_cache_hits_created"));
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
        // `decdn_cache_hits_total{tenant="a"} 5` has a `{` in the
        // first whitespace-delimited token, which won't parse as a
        // bare metric name. Do not crash; just skip it.
        let text = "decdn_cache_hits_total{tenant=\"a\"} 5\ndecdn_cache_hits_total 7\n";
        let parsed = parse_openmetrics(text);
        assert_eq!(parsed.get("decdn_cache_hits_total").copied(), Some(7.0));
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::float_cmp
)]
mod snapshot_tests {
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
        m.insert("decdn_uptime_seconds".into(), uptime);
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
        assert!((d.hits_per_sec - 6.0).abs() < 1e-9); // (812-800)/2
        assert!((d.misses_per_sec - 2.0).abs() < 1e-9); // (94-90)/2
        assert!((d.bytes_per_sec - 210_000.0).abs() < 1e-9); // (14_420_000-14_000_000)/2
    }

    #[test]
    fn per_second_delta_zero_elapsed_returns_zero() {
        // If `Instant::now() - prev_at` is somehow zero (or sub-tick),
        // dividing would produce NaN/inf — return 0 instead so the
        // table never renders gibberish on a too-fast first refresh.
        let prev = Snapshot::from_metrics(&fake_metrics(0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0));
        let now = Snapshot::from_metrics(&fake_metrics(0.0, 2.0, 2.0, 2.0, 0.0, 0.0, 1.0));
        let d = SnapshotDelta::between(&prev, &now, Duration::ZERO);
        assert_eq!(d.hits_per_sec, 0.0);
        assert_eq!(d.misses_per_sec, 0.0);
        assert_eq!(d.bytes_per_sec, 0.0);
    }
}
