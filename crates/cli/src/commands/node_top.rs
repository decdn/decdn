//! `decdn node top` — live view of node activity scraped from the
//! daemon's loopback `/metrics` HTTP endpoint (issue #275).

use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Context;
use serde::Deserialize;

use decdn_common::cli;
use decdn_common::cli::ConfigPathSource;
use decdn_common::cli::common::{default_config_path, expand_tilde};
use decdn_common::config::DEFAULT_METRICS_PORT;

/// Parse an `OpenMetrics` text body into `name -> value`.
///
/// Scope is deliberate: only label-free counters and gauges. Any
/// line starting with `#` is skipped — this covers the
/// `OpenMetrics` `HELP`/`TYPE`/`UNIT`/`EOF` directives the encoder
/// emits, plus any other comments in future revisions. Lines whose name token
/// contains `{` (label-bearing series) are skipped — `node top`
/// only consumes the bare `decdn_*` series the daemon registers
/// (`crates/node/src/metrics.rs`), none of which carry labels today,
/// so silently ignoring labelled lines is the correct degradation
/// if the metric surface grows them later.
///
/// Counter `_created` timestamp lines parse as floats; callers look
/// up the names they want and ignore the rest.
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
pub(crate) struct Snapshot {
    pub uptime_seconds: u64,
    pub active_connections: u64,
    pub dispatch_in_flight: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_bytes_returned: u64,
    pub rpc_healthy: bool,
}

impl Snapshot {
    pub(crate) fn from_metrics(m: &HashMap<String, f64>) -> Self {
        // Saturating cast: gauges/counters are non-negative in
        // practice. Negative or non-finite floats clamp to 0;
        // values above `u64::MAX` clamp to `u64::MAX`. The negative
        // branch is mostly defensive (a clock-skew gauge we don't
        // read could go negative); the high-end clamp matters
        // because `f64::INFINITY >= u64::MAX as f64` is true.
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
            uptime_seconds: u("decdn_node_uptime_seconds"),
            active_connections: u("decdn_active_connections"),
            dispatch_in_flight: u("decdn_dispatch_in_flight"),
            cache_hits: u("decdn_cache_hits_total"),
            cache_misses: u("decdn_cache_misses_total"),
            cache_bytes_returned: u("decdn_cache_bytes_returned_total"),
            rpc_healthy: m.get("decdn_rpc_healthy").copied().unwrap_or(0.0) >= 0.5,
        }
    }
}

/// Per-second deltas between two consecutive snapshots. Unit is
/// implicit in the type name; the field-level comments make it
/// re-readable at use sites where `SnapshotDelta` isn't visible
/// (e.g. `format_rate_bytes(d.bytes)`).
#[derive(Debug, Clone)]
pub(crate) struct SnapshotDelta {
    /// Cache hits per second.
    pub hits: f64,
    /// Cache misses per second.
    pub misses: f64,
    /// Cache bytes returned per second.
    pub bytes: f64,
}

impl SnapshotDelta {
    pub(crate) fn between(prev: &Snapshot, now: &Snapshot, elapsed: Duration) -> Self {
        let secs = elapsed.as_secs_f64();
        if secs <= 0.0 {
            return Self {
                hits: 0.0,
                misses: 0.0,
                bytes: 0.0,
            };
        }
        // Saturating subtraction in case the underlying counter
        // went backwards between ticks — typically a daemon
        // restart (we keep `prev` from before the restart and the
        // first post-restart scrape returns lower values), but
        // also any future per-instance counter reset. Showing 0/s
        // for that one tick is more honest than a huge spike from
        // the wraparound.
        #[allow(clippy::cast_precision_loss)]
        let d = |a: u64, b: u64| (a.saturating_sub(b)) as f64 / secs;
        Self {
            hits: d(now.cache_hits, prev.cache_hits),
            misses: d(now.cache_misses, prev.cache_misses),
            bytes: d(now.cache_bytes_returned, prev.cache_bytes_returned),
        }
    }
}

/// Cumulative hit ratio over `(hits + misses)`. `None` when the
/// denominator is 0 — distinct from `Some(0.0)` ("only misses so
/// far") so the renderer prints a literal `n/a` rather than a
/// misleading `0.0%`. Uses `saturating_add` for symmetry with
/// `SnapshotDelta::between`'s arithmetic, even though u64 overflow
/// from these counters is unreachable in practice.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn hit_rate(hits: u64, misses: u64) -> Option<f64> {
    let total = hits.saturating_add(misses);
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
#[allow(clippy::cast_precision_loss)]
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
        .map_or_else(|| "-".to_string(), |d| format!("{:.1}", d.hits));
    let miss_rate = delta
        .as_ref()
        .map_or_else(|| "-".to_string(), |d| format!("{:.1}", d.misses));
    let bytes_rate = delta
        .as_ref()
        .map_or_else(|| "-".to_string(), |d| format_rate_bytes(d.bytes));

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

/// Render a single snapshot as pretty JSON. The schema is hand-rolled
/// so the public output shape (field names, ordering, `hit_rate` as
/// a fraction in `[0, 1]` or null) is stable across internal
/// `Snapshot` field reorderings.
pub(crate) fn render_json_snapshot(s: &Snapshot) -> anyhow::Result<String> {
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

/// Resolve the metrics URL. Mirrors `resolve_admin_url` in
/// `crates/cli/src/commands/node.rs` so the operator's config-file
/// experience for `node top` is identical to `node peers`. The host
/// is hard-coded to `127.0.0.1` because the daemon's `metrics_bind`
/// can be `0.0.0.0` / `::`, and a client that dials that goes
/// nowhere; the operator overrides the host explicitly via
/// `--metrics-url` if they exposed metrics on a non-loopback address.
///
/// Precedence: `--metrics-url` flag → `DECDN_METRICS_PORT` env (the same var
/// the daemon reads, `common/src/cli/run.rs`) → `observability.metrics_port`
/// from the config file → default 9090. The env step (#864) keeps an env-only
/// deployment reachable instead of dialing the default and reporting the node
/// is down.
pub(crate) fn resolve_metrics_url(
    flag: Option<&str>,
    config_path: Option<&Path>,
) -> anyhow::Result<String> {
    if let Some(url) = flag {
        return Ok(url.to_string());
    }
    if let Some(url) = metrics_url_from_env(std::env::var("DECDN_METRICS_PORT").ok())? {
        return Ok(url);
    }
    let (resolved, source) = match config_path {
        Some(p) => (Some(expand_tilde(p)), ConfigPathSource::Explicit),
        None => (default_config_path(), ConfigPathSource::Default),
    };
    let port = port_from_config_file(resolved.as_deref(), source)?.unwrap_or(DEFAULT_METRICS_PORT);
    Ok(format!("http://127.0.0.1:{port}"))
}

/// Build the loopback metrics URL from the raw `DECDN_METRICS_PORT` env value,
/// or `None` when unset. Pure (takes the value rather than reading the
/// environment) so the parse/validation logic is testable without mutating
/// process-global state. A non-numeric value or `0` is an error rather than a
/// silent fall-through — port 0 cannot be dialed and signals misconfiguration.
fn metrics_url_from_env(raw: Option<String>) -> anyhow::Result<Option<String>> {
    let Some(raw) = raw else { return Ok(None) };
    let port: u16 = raw
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("DECDN_METRICS_PORT is not a valid port number: {raw:?}"))?;
    if port == 0 {
        anyhow::bail!(
            "DECDN_METRICS_PORT=0 cannot be dialed; pass --metrics-url or set a non-zero port"
        );
    }
    Ok(Some(format!("http://127.0.0.1:{port}")))
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

/// ANSI escape: clear screen + move cursor home. Used between live
/// ticks so the table redraws in place rather than scrolling.
const ANSI_CLEAR_HOME: &str = "\x1b[2J\x1b[H";

/// Cap on consecutive failed scrapes before the loop bails. 30
/// ticks at the default `--interval-ms 1000` is ≈30s on fast-fail
/// (connection refused), comfortably longer than a normal
/// `decdn-node` restart. With `--timeout-ms` defaulting to 5000
/// every consecutive timeout extends the wall-clock cap by up to
/// 5s, so a sustained timeout takes ≈30 ticks but ≈150s. Operators
/// chasing a real flap will see the per-tick `eprintln` in the
/// meantime.
const MAX_CONSECUTIVE_SCRAPE_FAILURES: u32 = 30;

/// Entry point dispatched from `node_dispatch`. Polls the daemon's
/// `/metrics` endpoint every `--interval-ms`, parses it, and renders
/// the table. Single-shot JSON mode bypasses the loop and exits
/// after one fetch.
pub async fn run(args: &cli::TopArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(args.timeout_ms > 0, "--timeout-ms must be > 0");
    anyhow::ensure!(args.interval_ms > 0, "--interval-ms must be > 0");

    let config_path = args.config.as_deref().or(global_config);
    let url_base = resolve_metrics_url(args.metrics_url.as_deref(), config_path)?;
    let metrics_url = append_metrics_path(&url_base);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(args.timeout_ms))
        .build()
        .context("failed to build HTTP client")?;

    if args.json {
        let body = fetch_metrics(&client, &metrics_url).await?;
        let parsed = parse_openmetrics(&body);
        let snap = Snapshot::from_metrics(&parsed);
        println!("{}", render_json_snapshot(&snap)?);
        return Ok(());
    }

    let interval = Duration::from_millis(args.interval_ms);
    let mut ticker = tokio::time::interval(interval);
    // `Skip` so a stalled scrape doesn't cause a burst of catch-up
    // ticks once the daemon comes back.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut prev: Option<(Snapshot, Instant)> = None;
    let mut consecutive_failures: u32 = 0;

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            res = tokio::signal::ctrl_c() => {
                res.context("failed to install ctrl_c handler")?;
                return Ok(());
            }
        }

        let now_at = Instant::now();
        match fetch_metrics(&client, &metrics_url).await {
            Ok(body) => {
                consecutive_failures = 0;
                let parsed = parse_openmetrics(&body);
                let snap = Snapshot::from_metrics(&parsed);
                let prev_pair = prev
                    .as_ref()
                    .map(|(s, t)| (s, now_at.saturating_duration_since(*t)));
                let mut stdout = io::stdout().lock();
                stdout
                    .write_all(ANSI_CLEAR_HOME.as_bytes())
                    .context("failed to write clear-screen escape")?;
                write_top_table(&mut stdout, &metrics_url, &snap, prev_pair, interval)
                    .context("failed to write top table")?;
                stdout.flush().context("failed to flush top table")?;
                prev = Some((snap, now_at));
            }
            Err(err) => {
                // Bail immediately on errors that can't possibly
                // become transient: reqwest builder class (URL
                // parse / unsupported scheme) and most 4xx
                // statuses. Retrying for 30s achieves nothing.
                if is_permanent_scrape_error(&err) {
                    return Err(err.context(
                        "permanent scrape error (URL malformed, unsupported scheme, or persistent 4xx)",
                    ));
                }
                consecutive_failures += 1;
                eprintln!("scrape failed: {err}");
                if consecutive_failures >= MAX_CONSECUTIVE_SCRAPE_FAILURES {
                    return Err(err.context(format!(
                        "scrape failed {MAX_CONSECUTIVE_SCRAPE_FAILURES} times in a row; \
                         giving up — check the daemon and --metrics-url"
                    )));
                }
            }
        }
    }
}

/// Build the full `/metrics` URL from an operator-supplied base.
/// Tolerates the natural mistakes — a trailing `/` on the base
/// (`http://h:9090/`), a base already ending in `/metrics`
/// (`http://h:9090/metrics`), and a query string or fragment on
/// either (`http://h:9090?token=foo`) — without producing
/// `//metrics`, `/metrics/metrics`, or splicing the path inside the
/// query value. The resolver in this crate hands us a clean
/// `http://127.0.0.1:{port}` so the public-facing exposure here is
/// the `--metrics-url` / `DECDN_METRICS_URL` path.
fn append_metrics_path(base: &str) -> String {
    // Split off any query string or fragment first so we can
    // manipulate the path independently. Without this split, an
    // operator URL like `http://h:9090?token=foo` would have
    // `/metrics` appended *inside* the query value.
    let split_at = base.find(['?', '#']);
    let (path_part, suffix) = match split_at {
        Some(i) => (base.get(..i).unwrap_or(base), base.get(i..).unwrap_or("")),
        None => (base, ""),
    };
    let stripped = path_part.trim_end_matches('/');
    if stripped.ends_with("/metrics") {
        format!("{stripped}{suffix}")
    } else {
        format!("{stripped}/metrics{suffix}")
    }
}

/// Recognise scrape errors that can't recover by retrying:
/// - reqwest builder errors (URL parse failures, unsupported
///   schemes, missing host) — the URL is fixed for the lifetime of
///   the process, so retrying for 30 ticks is just noise.
/// - 4xx HTTP status (except 408 Request Timeout / 429 Too Many
///   Requests, which can resolve on their own) — the daemon is
///   reachable but the path/auth is wrong. Tagged in
///   `fetch_metrics` via the `PERMANENT_STATUS_TAG` sentinel so we
///   can match without inspecting the bare `anyhow::Error` shape.
///
/// Note that this helper is the only thing standing between a
/// misconfigured `--metrics-url` and an indefinite retry loop.
/// The `reqwest::Error` is matched by walking the source chain
/// because `fetch_metrics` wraps it with `with_context`; if that
/// wrapping stops being a direct chain element (e.g. via an
/// indirection that boxes through a different error type), the
/// `is_builder()` arm silently stops firing — guard with the
/// `is_permanent_scrape_error_recognises_*` tests.
fn is_permanent_scrape_error(err: &anyhow::Error) -> bool {
    if err.chain().any(|e| e.to_string() == PERMANENT_STATUS_TAG) {
        return true;
    }
    err.chain().any(|e| {
        e.downcast_ref::<reqwest::Error>()
            .is_some_and(reqwest::Error::is_builder)
    })
}

async fn fetch_metrics(client: &reqwest::Client, url: &str) -> anyhow::Result<String> {
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url} failed"))?;
    let status = resp.status();
    if !status.is_success() {
        // Permanent vs. transient: 4xx (except 408 Request Timeout
        // and 429 Too Many Requests, both of which can resolve on
        // their own) means the daemon is reachable but the URL
        // path/auth is wrong. Retrying for 30 ticks won't fix that.
        // Tagging the error so `is_permanent_scrape_error` picks
        // it up and bails on the first failure rather than
        // burning 30s of operator-watching-zeros.
        let is_permanent_status = status.is_client_error()
            && status != reqwest::StatusCode::REQUEST_TIMEOUT
            && status != reqwest::StatusCode::TOO_MANY_REQUESTS;
        let err = anyhow::anyhow!("GET {url} returned HTTP {status}");
        return if is_permanent_status {
            Err(err.context(PERMANENT_STATUS_TAG))
        } else {
            Err(err)
        };
    }
    resp.text()
        .await
        .with_context(|| format!("read body from {url}"))
}

/// Sentinel context string used by `fetch_metrics` to mark a 4xx
/// (except 408/429) so `is_permanent_scrape_error` can match it
/// without re-classifying the bare `anyhow::Error` shape. A typed
/// scrape-error enum would replace this; until a second permanent
/// class shows up the sentinel is enough.
const PERMANENT_STATUS_TAG: &str = "permanent HTTP status (will not retry)";

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
    fn format_uptime_crosses_unit_boundaries() {
        // The three cross-unit boundaries: 60s (s→m), 3600s (m→h),
        // 86400s (h→d), plus in-band cases. `write_top_table`
        // tests exercise the m+s shape via substring; cross-units
        // need direct asserts so a regression at the boundary
        // arithmetic doesn't sneak past unnoticed.
        assert_eq!(format_uptime(0), "0s");
        assert_eq!(format_uptime(59), "59s");
        assert_eq!(format_uptime(60), "1m 0s");
        assert_eq!(format_uptime(3_599), "59m 59s");
        assert_eq!(format_uptime(3_600), "1h 0m");
        assert_eq!(format_uptime(86_399), "23h 59m");
        assert_eq!(format_uptime(86_400), "1d 0h");
        assert_eq!(format_uptime(2 * 86_400 + 3 * 3_600), "2d 3h");
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
            Duration::from_secs(1),
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
            Duration::from_secs(1),
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
            Duration::from_secs(1),
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
            Duration::from_secs(1),
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
mod e2e_tests {
    //! End-to-end test against the daemon's real metrics server.
    //! In-module rather than a `tests/` integration test because
    //! the assertions exercise `pub(crate)` items (parser +
    //! `Snapshot::from_metrics`); going via `tests/` would force a
    //! `pub` API surface that no non-test caller needs.

    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn e2e_pipeline_reads_real_metrics_server() -> anyhow::Result<()> {
        // Bind on an ephemeral loopback port; same pattern the
        // sibling tests in `tests/admin_peers.rs` use.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr: SocketAddr = listener.local_addr()?;
        let metrics = Arc::new(decdn_node::metrics::Metrics::new());
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let metrics_for_server = Arc::clone(&metrics);
        // Capture the server's Result so a panic or encoder error
        // surfaces as a real test failure with the actual cause,
        // rather than a confusing "got 0, expected 1" downstream.
        let server = tokio::spawn(async move {
            decdn_node::metrics::serve(listener, metrics_for_server, stop_rx).await
        });

        // Bump a couple of counters so the snapshot has non-zero
        // fields — proves the parser saw the real bytes the encoder
        // wrote, not just an empty/200 response.
        metrics.dispatch_permit_acquired();
        metrics.cache_metrics().hits.inc();
        metrics.cache_metrics().bytes_returned.inc_by(1024);

        // Fetch via reqwest, parse, build a Snapshot — the same
        // path the CLI's `run` takes in JSON mode.
        let url = format!("http://{addr}/metrics");
        let body = reqwest::Client::new()
            .get(&url)
            .send()
            .await?
            .text()
            .await?;
        let parsed = parse_openmetrics(&body);
        let snap = Snapshot::from_metrics(&parsed);
        assert_eq!(
            snap.dispatch_in_flight, 1,
            "in_flight should reflect the dispatch_permit_acquired bump"
        );
        assert_eq!(
            snap.cache_hits, 1,
            "hits should reflect the cache_metrics().hits.inc() bump"
        );
        assert_eq!(
            snap.cache_bytes_returned, 1024,
            "bytes should reflect the inc_by(1024) bump"
        );

        // Drive the high-level `run` in JSON mode against the same
        // address. Smoke-tests resolver → reqwest → parser →
        // renderer → exit against a real daemon endpoint.
        let args = decdn_common::cli::TopArgs {
            metrics_url: Some(format!("http://{addr}")),
            config: None,
            interval_ms: 1_000,
            json: true,
            timeout_ms: 5_000,
        };
        run(&args, None).await?;

        // Tell the server to stop, then await the JoinHandle so a
        // panic in the spawned task surfaces here. The outer
        // `timeout` Elapsed is fine to drop — that's a cleanup
        // race, not a logic bug — but the inner JoinError is not.
        let _ = stop_tx.send(());
        let join = tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .context("metrics server task did not exit within 2s")?;
        join.context("metrics server task panicked")?
            .context("metrics server returned an error")?;
        Ok(())
    }

    #[tokio::test]
    async fn fetch_metrics_surfaces_non_2xx_as_error() -> anyhow::Result<()> {
        // Spawn a tiny listener that replies 503 to every request.
        // Locks in the bail at fetch_metrics' status check so a
        // future refactor that stops checking status fails here
        // rather than silently feeding garbage to the parser.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr: SocketAddr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            // One-shot: accept, write a 503, drop. Any subsequent
            // connections close cleanly when the listener drops.
            if let Ok((mut sock, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let _ = sock
                    .write_all(
                        b"HTTP/1.1 503 Service Unavailable\r\n\
                          Content-Length: 0\r\n\r\n",
                    )
                    .await;
                let _ = sock.shutdown().await;
            }
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()?;
        let url = format!("http://{addr}/metrics");
        let err = fetch_metrics(&client, &url)
            .await
            .expect_err("503 must surface as Err");
        let msg = format!("{err:#}");
        // Assert the status code shows up — `StatusCode`'s Display
        // emits `503 Service Unavailable`, so checking for "503" is
        // sufficient and tighter than a `||` over both substrings
        // (which always co-occur today).
        assert!(
            msg.contains("503"),
            "expected 503 in error chain, got: {msg}"
        );

        // Wait for the listener task and propagate JoinError so a
        // panic in the spawned write loop surfaces with the real
        // cause, rather than the assertion above passing on a
        // torn-down server. Same shape as the e2e test above.
        let join = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .context("listener task did not exit within 1s")?;
        join.context("listener task panicked")?;
        Ok(())
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
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod classifier_tests {
    //! Direct tests for `is_permanent_scrape_error`. The classifier
    //! is the only thing standing between a misconfigured
    //! `--metrics-url` and an indefinite retry loop, so a future
    //! refactor that breaks the predicate (e.g. changes the
    //! `is_builder()` arm to `is_request()`, or drops the sentinel
    //! match) needs to fail here rather than going unnoticed.
    use super::*;

    /// Build an `anyhow::Error` from a real reqwest send-error so
    /// the chain shape matches what `fetch_metrics` would produce at
    /// runtime (rather than a hand-constructed mock that could diverge
    /// from reqwest's internal error structure).
    async fn reqwest_send_error(url: &str) -> anyhow::Error {
        let err = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect_err("test url must fail to send");
        anyhow::Error::from(err).context(format!("GET {url} failed"))
    }

    #[tokio::test]
    async fn recognises_builder_error_as_permanent() {
        // "not a url" produces a reqwest::Error::is_builder() == true.
        let err = reqwest_send_error("not a url").await;
        assert!(
            is_permanent_scrape_error(&err),
            "URL parse error must be classed as permanent: {err:#}"
        );
    }

    #[tokio::test]
    async fn does_not_class_connection_refused_as_permanent() {
        // Port 1 is reserved + nothing listens; the error is a
        // transport-level connect failure, not a builder error.
        // Must be left for the consecutive-failure budget to
        // handle (transient — daemon may come back).
        let err = reqwest_send_error("http://127.0.0.1:1/metrics").await;
        assert!(
            !is_permanent_scrape_error(&err),
            "connection refused must NOT be classed as permanent: {err:#}"
        );
    }

    #[test]
    fn recognises_permanent_status_tag_in_chain() {
        // fetch_metrics tags persistent 4xx with this sentinel
        // string. A regression that drops the tag (or changes the
        // string) must fail here.
        let err = anyhow::anyhow!("GET http://h:9090/metrics returned HTTP 404 Not Found")
            .context(PERMANENT_STATUS_TAG);
        assert!(
            is_permanent_scrape_error(&err),
            "PERMANENT_STATUS_TAG must be recognised: {err:#}"
        );
    }

    #[test]
    fn does_not_class_unrelated_anyhow_as_permanent() {
        // A bare anyhow error with no reqwest in the chain and no
        // sentinel tag must be transient. Guards against a too-broad
        // predicate that classes everything as permanent.
        let err = anyhow::anyhow!("something else went wrong");
        assert!(
            !is_permanent_scrape_error(&err),
            "unrelated error must NOT be classed as permanent: {err:#}"
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
mod loop_budget_tests {
    //! Direct test for `MAX_CONSECUTIVE_SCRAPE_FAILURES`. The cap
    //! is part of the operator-visible contract — without a test,
    //! a refactor that silently raises it to `u32::MAX` (or removes
    //! the check entirely) would let `decdn node top` run forever
    //! against a misconfigured target.
    use super::*;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn run_bails_after_consecutive_failures_cap() {
        // Bind a listener and immediately drop it so subsequent
        // connects from the CLI fail with ECONNREFUSED. Using a
        // bound-then-dropped port keeps the test hermetic — no
        // reliance on a specific port being unused.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        drop(listener);

        // 1ms interval + 100ms timeout means 30 ticks finish in
        // under a second on the fast-fail (ECONNREFUSED) path.
        // Pause the runtime clock so the bound is enforced even on
        // a heavily-loaded CI runner.
        let args = decdn_common::cli::TopArgs {
            metrics_url: Some(format!("http://{addr}")),
            config: None,
            interval_ms: 1,
            json: false,
            timeout_ms: 100,
        };

        let err = run(&args, None)
            .await
            .expect_err("loop must bail after the consecutive-failure cap");
        let msg = format!("{err:#}");
        let cap = MAX_CONSECUTIVE_SCRAPE_FAILURES;
        assert!(
            msg.contains(&format!("{cap} times in a row")),
            "expected '{cap} times in a row' in error chain, got: {msg}"
        );
    }
}
