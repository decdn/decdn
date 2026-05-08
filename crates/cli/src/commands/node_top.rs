//! `decdn node top` — live view of node activity scraped from the
//! daemon's loopback `/metrics` HTTP endpoint (issue #275).

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;

use decdn_common::cli;
use decdn_common::cli::common::{default_config_path, expand_tilde};
use decdn_common::config::DEFAULT_METRICS_PORT;

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

/// Compact byte size for display: largest binary unit at which the
/// value is < 1024, one decimal of precision. Mirrors how `du -h`
/// renders sizes — operators are used to that. Strict binary units
/// (KiB/MiB/...) so the displayed value matches the raw counter
/// when divided by the obvious power of two.
#[allow(dead_code, clippy::cast_precision_loss)] // Wired into the renderer below.
pub(crate) fn format_bytes(b: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    const TIB: u64 = GIB * 1024;
    if b < KIB {
        return format!("{b} B");
    }
    let (val, unit) = if b < MIB {
        (b as f64 / KIB as f64, "KiB")
    } else if b < GIB {
        (b as f64 / MIB as f64, "MiB")
    } else if b < TIB {
        (b as f64 / GIB as f64, "GiB")
    } else {
        (b as f64 / TIB as f64, "TiB")
    };
    format!("{val:.1} {unit}")
}

#[allow(
    dead_code,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub(crate) fn format_rate_bytes(per_sec: f64) -> String {
    if per_sec <= 0.0 || !per_sec.is_finite() {
        return "0 B/s".to_string();
    }
    let b = if per_sec >= u64::MAX as f64 {
        u64::MAX
    } else {
        per_sec as u64
    };
    let s = format_bytes(b);
    format!("{s}/s")
}

#[allow(dead_code)] // Wired into the renderer below.
fn format_uptime(secs: u64) -> String {
    if secs < 60 {
        return format!("{secs}s");
    }
    let m = secs / 60;
    let s = secs % 60;
    if m < 60 {
        return format!("{m}m {s}s");
    }
    let h = m / 60;
    let m = m % 60;
    if h < 24 {
        return format!("{h}h {m}m");
    }
    let d = h / 24;
    let h = h % 24;
    format!("{d}d {h}h")
}

/// Render the live-view table to `w`. Writing to `&mut impl Write`
/// rather than stdout makes the formatter unit-testable and matches
/// the `write_peers_table` pattern used elsewhere in the CLI.
#[allow(dead_code)] // Wired into the fetch loop in a later task.
pub(crate) fn write_top_table(
    w: &mut impl io::Write,
    metrics_url: &str,
    now: &Snapshot,
    prev_with_elapsed: Option<(&Snapshot, Duration)>,
    refresh_interval: Duration,
) -> io::Result<()> {
    let rpc = if now.rpc_healthy { "ok" } else { "unhealthy" };
    writeln!(
        w,
        "decdn node top — {metrics_url}    uptime={uptime}    rpc={rpc}    \
         active_connections={ac}    dispatch_in_flight={inf}",
        uptime = format_uptime(now.uptime_seconds),
        ac = now.active_connections,
        inf = now.dispatch_in_flight,
    )?;
    writeln!(w)?;
    writeln!(w, "{:<35} {:>14} {:>12}", "METRIC", "VALUE", "/sec")?;

    let delta = prev_with_elapsed.map(|(p, dt)| SnapshotDelta::between(p, now, dt));

    writeln!(
        w,
        "{:<35} {:>14} {:>12}",
        "active_connections", now.active_connections, "-"
    )?;
    writeln!(
        w,
        "{:<35} {:>14} {:>12}",
        "dispatch_in_flight", now.dispatch_in_flight, "-"
    )?;

    let hits_rate = delta
        .as_ref()
        .map_or_else(|| "-".to_string(), |d| format!("{:.1}", d.hits_per_sec));
    let miss_rate = delta
        .as_ref()
        .map_or_else(|| "-".to_string(), |d| format!("{:.1}", d.misses_per_sec));
    let bytes_rate = delta
        .as_ref()
        .map_or_else(|| "-".to_string(), |d| format_rate_bytes(d.bytes_per_sec));

    writeln!(
        w,
        "{:<35} {:>14} {:>12}",
        "cache_hits_total", now.cache_hits, hits_rate
    )?;
    writeln!(
        w,
        "{:<35} {:>14} {:>12}",
        "cache_misses_total", now.cache_misses, miss_rate
    )?;

    let hit_rate_s = match hit_rate(now.cache_hits, now.cache_misses) {
        Some(r) => format!("{:.1}%", r * 100.0),
        None => "n/a".to_string(),
    };
    writeln!(w, "{:<35} {:>14} {:>12}", "cache_hit_rate", hit_rate_s, "-")?;
    writeln!(
        w,
        "{:<35} {:>14} {:>12}",
        "cache_bytes_returned_total",
        format_bytes(now.cache_bytes_returned),
        bytes_rate,
    )?;
    writeln!(w)?;
    writeln!(
        w,
        "(refreshes every {:.1}s — Ctrl-C to exit)",
        refresh_interval.as_secs_f64(),
    )?;
    Ok(())
}

/// Render a single snapshot as pretty JSON. Schema is hand-rolled
/// (rather than `serde_json::to_string_pretty(&Snapshot)`) so the
/// public output shape — field names, `hit_rate` as a fraction in
/// `[0, 1]` or null, ordering — is decoupled from the internal
/// struct's field order.
#[allow(dead_code)] // Wired into the fetch loop in a later task.
pub(crate) fn render_json_snapshot(s: &Snapshot) -> anyhow::Result<String> {
    use anyhow::Context;
    let value = serde_json::json!({
        "uptime_seconds": s.uptime_seconds,
        "active_connections": s.active_connections,
        "dispatch_in_flight": s.dispatch_in_flight,
        "cache_hits_total": s.cache_hits,
        "cache_misses_total": s.cache_misses,
        "cache_bytes_returned_total": s.cache_bytes_returned,
        "cache_hit_rate": hit_rate(s.cache_hits, s.cache_misses),
        "rpc_healthy": s.rpc_healthy,
    });
    serde_json::to_string_pretty(&value).context("encode top snapshot as JSON")
}

#[derive(Debug, Default, Deserialize)]
struct MetricsPortConfig {
    observability: Option<MetricsPortObservability>,
}

#[derive(Debug, Default, Deserialize)]
struct MetricsPortObservability {
    metrics_port: Option<u16>,
}

#[derive(Debug, Clone, Copy)]
enum ConfigPathSource {
    Explicit,
    Default,
}

/// Resolve the metrics URL. Mirrors `resolve_admin_url` in
/// `crates/cli/src/commands/node.rs` so the operator's config-file
/// experience for `node top` is identical to `node peers`. The host
/// is hard-coded to `127.0.0.1` because the daemon's `metrics_bind`
/// can be `0.0.0.0` / `::`, and a client that dials that goes
/// nowhere; the operator overrides the host explicitly via
/// `--metrics-url` if they exposed metrics on a non-loopback address.
#[allow(dead_code)] // Wired into the fetch loop in a later task.
pub(crate) fn resolve_metrics_url(
    flag: Option<&str>,
    config_path: Option<&Path>,
) -> anyhow::Result<String> {
    if let Some(url) = flag {
        return Ok(url.to_string());
    }
    let (resolved, source) = match config_path {
        Some(p) => (Some(expand_tilde(p)), ConfigPathSource::Explicit),
        None => (default_config_path(), ConfigPathSource::Default),
    };
    let port = port_from_config_file(resolved.as_deref(), source)?.unwrap_or(DEFAULT_METRICS_PORT);
    Ok(format!("http://127.0.0.1:{port}"))
}

fn port_from_config_file(
    path: Option<&Path>,
    source: ConfigPathSource,
) -> anyhow::Result<Option<u16>> {
    let Some(path) = path else { return Ok(None) };
    let path: PathBuf = path.to_path_buf();
    match std::fs::read_to_string(&path) {
        Ok(contents) => {
            // Partial deserializer: a typo in some unrelated section
            // (e.g. `[payments]`) shouldn't cost the operator the
            // ability to point `node top` at the daemon. Mirrors
            // `port_from_config_file` for `admin_port`.
            let parsed: MetricsPortConfig = toml::from_str(&contents)
                .with_context(|| format!("failed to parse config file {}", path.display()))?;
            match parsed.observability.and_then(|o| o.metrics_port) {
                Some(0) => anyhow::bail!(
                    "config {} disables the metrics server (observability.metrics_port = 0); \
                     pass --metrics-url or enable the metrics port",
                    path.display(),
                ),
                other => Ok(other),
            }
        }
        Err(err) => match (err.kind(), source) {
            (io::ErrorKind::NotFound, ConfigPathSource::Default) => Ok(None),
            (io::ErrorKind::PermissionDenied, _) => Err(anyhow::anyhow!(
                "cannot read config file {}: permission denied; check file mode and ownership",
                path.display()
            )),
            _ => Err(anyhow::anyhow!(
                "failed to read config file {}: {err}",
                path.display()
            )),
        },
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod render_tests {
    use super::*;
    use std::time::Duration;

    fn snap(
        uptime: u64,
        hits: u64,
        misses: u64,
        bytes: u64,
        in_flight: u64,
        conns: u64,
        healthy: bool,
    ) -> Snapshot {
        Snapshot {
            uptime_seconds: uptime,
            active_connections: conns,
            dispatch_in_flight: in_flight,
            cache_hits: hits,
            cache_misses: misses,
            cache_bytes_returned: bytes,
            rpc_healthy: healthy,
        }
    }

    #[test]
    fn format_bytes_picks_unit() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(2048), "2.0 KiB");
        assert_eq!(format_bytes(15 * 1024 * 1024), "15.0 MiB");
        assert_eq!(format_bytes(3 * 1024_u64.pow(3)), "3.0 GiB");
    }

    #[test]
    fn format_rate_uses_per_second_suffix() {
        assert_eq!(format_rate_bytes(0.0), "0 B/s");
        assert_eq!(format_rate_bytes(1024.0), "1.0 KiB/s");
        assert_eq!(format_rate_bytes(2_500_000.0), "2.4 MiB/s");
    }

    #[test]
    fn write_top_table_first_tick_no_deltas() {
        // First tick has no previous snapshot, so the /sec column
        // shows a stable placeholder ("-") rather than a nonsense 0.
        let s = snap(312, 812, 94, 14_900_000, 2, 4, true);
        let mut buf = Vec::<u8>::new();
        write_top_table(
            &mut buf,
            "http://127.0.0.1:9090",
            &s,
            None,
            Duration::from_millis(1000),
        )
        .unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("decdn node top"), "header missing: {out}");
        assert!(
            out.contains("uptime=5m 12s") || out.contains("uptime=312s"),
            "uptime missing: {out}"
        );
        assert!(out.contains("rpc=ok"), "rpc status missing: {out}");
        assert!(
            out.contains("dispatch_in_flight"),
            "in_flight row missing: {out}"
        );
        assert!(
            out.contains("cache_hit_rate"),
            "hit_rate row missing: {out}"
        );
        for needle in [
            "cache_hits_total",
            "cache_misses_total",
            "cache_bytes_returned_total",
        ] {
            assert!(out.contains(needle), "row {needle} missing: {out}");
        }
    }

    #[test]
    fn write_top_table_renders_per_second_when_prev_present() {
        let prev = snap(310, 800, 90, 14_000_000, 2, 4, true);
        let now = snap(312, 812, 94, 14_420_000, 2, 4, true);
        let mut buf = Vec::<u8>::new();
        write_top_table(
            &mut buf,
            "http://127.0.0.1:9090",
            &now,
            Some((&prev, Duration::from_secs(2))),
            Duration::from_millis(1000),
        )
        .unwrap();
        let out = String::from_utf8(buf).unwrap();
        // /sec column for cache_hits_total should be "6.0" since
        // (812-800)/2 = 6.0. Asserted as substring rather than line
        // shape so column-width tweaks don't break the test.
        assert!(out.contains("6.0"), "hits/s missing: {out}");
        // bytes/s = (14_420_000 - 14_000_000) / 2 = 210_000 B/s, which
        // format_rate_bytes renders as "205.1 KiB/s" (210000/1024).
        assert!(
            out.contains("205.1 KiB/s"),
            "bytes/s missing (expected 205.1 KiB/s for 210000 B/s): {out}"
        );
    }

    #[test]
    fn write_top_table_hit_rate_na_on_empty_cache() {
        let s = snap(0, 0, 0, 0, 0, 0, true);
        let mut buf = Vec::<u8>::new();
        write_top_table(
            &mut buf,
            "http://127.0.0.1:9090",
            &s,
            None,
            Duration::from_millis(1000),
        )
        .unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("n/a"), "hit_rate must show n/a on 0/0: {out}");
    }

    #[test]
    fn write_top_table_marks_rpc_unhealthy() {
        let s = snap(312, 812, 94, 14_900_000, 2, 4, false);
        let mut buf = Vec::<u8>::new();
        write_top_table(
            &mut buf,
            "http://127.0.0.1:9090",
            &s,
            None,
            Duration::from_millis(1000),
        )
        .unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(
            out.contains("rpc=unhealthy"),
            "expected rpc=unhealthy: {out}"
        );
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod json_tests {
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
            rpc_healthy: false,
        };
        let json = render_json_snapshot(&s).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(v["cache_hit_rate"].is_null());
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod resolve_tests {
    use super::*;

    #[test]
    fn resolve_metrics_url_prefers_flag() {
        let got = resolve_metrics_url(Some("http://custom:1234"), None).unwrap();
        assert_eq!(got, "http://custom:1234");
    }

    #[test]
    fn resolve_metrics_url_falls_back_to_default_port_when_no_config() {
        // No explicit config, no override: the resolver must use the
        // canonical metrics port. Locks against a regression that
        // hard-coded the wrong number or stopped exporting it.
        let got = resolve_metrics_url(None, None).unwrap();
        assert_eq!(got, format!("http://127.0.0.1:{DEFAULT_METRICS_PORT}"));
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
        // server). Mirroring resolve_admin_url's behaviour.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nmetrics_port = 0\n").unwrap();
        let err = resolve_metrics_url(None, Some(&path))
            .expect_err("expected error for zero port")
            .to_string();
        assert!(
            err.contains("disables") || err.contains('0'),
            "missing context: {err}"
        );
    }
}
