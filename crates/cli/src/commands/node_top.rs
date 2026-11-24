//! `decdn node top` — live view of node activity scraped from the
//! daemon's loopback `/metrics` HTTP endpoint (issue #275).

use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Context;

use decdn_common::cli;

#[cfg(doc)]
use crate::commands::node::LoopbackPort;
use crate::commands::node::{self, METRICS_PORT};

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
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct Snapshot {
    pub uptime_seconds: u64,
    pub active_connections: u64,
    pub dispatch_in_flight: u64,
    /// Paid serves the availability gate answered from locally held bytes:
    /// `decdn_serve_cache_hit_total` (complete blob) plus
    /// `decdn_serve_cache_partial_hit_total` (held chunk groups cover the
    /// span). A partial hit serves without a fill, so it counts as a hit.
    pub serve_cache_hits: u64,
    /// Paid serves the availability gate could not answer from locally held
    /// bytes (`decdn_serve_cache_miss_total`).
    pub serve_cache_misses: u64,
    /// Bao wire bytes the node wrote to clients and downstream nodes
    /// (`decdn_bytes_served_total`).
    pub bytes_served: u64,
    /// Raw USDC held in accepted vouchers not yet redeemed on-chain, refreshed
    /// once per redeemer self-tick (`decdn_unredeemed_usdc`).
    pub unredeemed_usdc: u64,
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
            serve_cache_hits: u("decdn_serve_cache_hit_total")
                .saturating_add(u("decdn_serve_cache_partial_hit_total")),
            serve_cache_misses: u("decdn_serve_cache_miss_total"),
            bytes_served: u("decdn_bytes_served_total"),
            unredeemed_usdc: u("decdn_unredeemed_usdc"),
            rpc_healthy: m.get("decdn_rpc_healthy").copied().unwrap_or(0.0) >= 0.5,
        }
    }
}

/// Per-second deltas between two consecutive snapshots. Unit is
/// implicit in the type name; the field-level comments make it
/// re-readable at use sites where `SnapshotDelta` isn't visible
/// (e.g. `format_rate_bytes(d.bytes)`).
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct SnapshotDelta {
    /// Serve-path cache hits per second.
    pub hits: f64,
    /// Serve-path cache misses per second.
    pub misses: f64,
    /// Wire bytes served per second.
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
            hits: d(now.serve_cache_hits, prev.serve_cache_hits),
            misses: d(now.serve_cache_misses, prev.serve_cache_misses),
            bytes: d(now.bytes_served, prev.bytes_served),
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

/// Render a raw micro-USDC amount (6 decimals, the on-chain base unit) as a
/// dollar figure, e.g. `12_345_678` → `$12.345678` ([`node::format_usdc`]
/// behind a `$`). The value is exact only below 2^53 micro-USDC (~$9e9, far
/// past any realistic per-node total): the `/metrics` scrape parses series as
/// `f64` in [`parse_openmetrics`], which cannot represent every larger `u64`.
fn format_dollars(micro: u64) -> String {
    format!("${}", node::format_usdc(micro))
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
        "serve_cache_hits_total", now.serve_cache_hits, hits_rate
    )?;
    writeln!(
        w,
        "{:<35} {:>14} {:>12}",
        "serve_cache_misses_total", now.serve_cache_misses, miss_rate
    )?;

    let hit_rate_s = match hit_rate(now.serve_cache_hits, now.serve_cache_misses) {
        Some(r) => format!("{:.1}%", r * 100.0),
        None => "n/a".to_string(),
    };
    writeln!(
        w,
        "{:<35} {:>14} {:>12}",
        "serve_cache_hit_rate", hit_rate_s, "-"
    )?;
    writeln!(
        w,
        "{:<35} {:>14} {:>12}",
        "bytes_served_total",
        format_bytes(now.bytes_served),
        bytes_rate,
    )?;
    // Pending redemption: refreshed once per redeemer self-tick, so no
    // meaningful /sec rate — the `-` placeholder matches the other gauges.
    writeln!(
        w,
        "{:<35} {:>14} {:>12}",
        "unredeemed_usdc",
        format_dollars(now.unredeemed_usdc),
        "-",
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
/// so the public output shape (field names, ordering, the hit rate as
/// a fraction in `[0, 1]` or null) is stable across internal
/// `Snapshot` field reorderings.
pub(crate) fn render_json_snapshot(s: &Snapshot) -> anyhow::Result<String> {
    let value = serde_json::json!({
        "uptime_seconds": s.uptime_seconds,
        "active_connections": s.active_connections,
        "dispatch_in_flight": s.dispatch_in_flight,
        "serve_cache_hits_total": s.serve_cache_hits,
        "serve_cache_misses_total": s.serve_cache_misses,
        "bytes_served_total": s.bytes_served,
        "serve_cache_hit_rate": hit_rate(s.serve_cache_hits, s.serve_cache_misses),
        // Raw micro-USDC (6 decimals) so downstream tooling gets the exact
        // integer the gauge exports; the human table renders it as dollars.
        "unredeemed_usdc_micro": s.unredeemed_usdc,
        "rpc_healthy": s.rpc_healthy,
    });
    serde_json::to_string_pretty(&value).context("encode top snapshot as JSON")
}

/// Resolve the metrics URL ([`LoopbackPort::resolve_url`] over
/// [`METRICS_PORT`]), the same way the other `node` subcommands find the
/// admin listener.
pub(crate) fn resolve_metrics_url(
    flag: Option<&str>,
    config_path: Option<&Path>,
) -> anyhow::Result<String> {
    METRICS_PORT.resolve_url(flag, config_path)
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
pub(crate) fn append_metrics_path(base: &str) -> String {
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

pub(crate) async fn fetch_metrics(client: &reqwest::Client, url: &str) -> anyhow::Result<String> {
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
mod parse_tests;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::float_cmp
)]
mod snapshot_tests;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod render_tests;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod json_tests;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod e2e_tests;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod resolve_tests;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod classifier_tests;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod loop_budget_tests;
