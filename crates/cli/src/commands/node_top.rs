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
            cache_hits: u("decdn_cache_hits_total"),
            cache_misses: u("decdn_cache_misses_total"),
            cache_bytes_returned: u("decdn_cache_bytes_returned_total"),
            unredeemed_usdc: u("decdn_unredeemed_usdc"),
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

/// Render a raw micro-USDC amount (6 decimals, the on-chain base unit) as a
/// dollar figure, e.g. `12_345_678` → `$12.345678`. Emits all six fractional
/// digits rather than rounding, so the operator reads back the exact base-unit
/// value this formatter is handed. That value is itself exact only below 2^53
/// micro-USDC (~$9e9, far past any realistic per-node total): the `/metrics`
/// scrape parses series as `f64` in [`parse_openmetrics`], which cannot
/// represent every larger `u64` — an upstream limit of the text exposition,
/// not of this formatter.
fn format_usdc(micro: u64) -> String {
    let dollars = micro / 1_000_000;
    let frac = micro % 1_000_000;
    format!("${dollars}.{frac:06}")
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
    // Pending redemption: refreshed once per redeemer self-tick, so no
    // meaningful /sec rate — the `-` placeholder matches the other gauges.
    writeln!(
        w,
        "{:<35} {:>14} {:>12}",
        "unredeemed_usdc",
        format_usdc(now.unredeemed_usdc),
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
        // Raw micro-USDC (6 decimals) so downstream tooling gets the exact
        // integer the gauge exports; the human table renders it as dollars.
        "unredeemed_usdc_micro": s.unredeemed_usdc,
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
/// experience for `node top` is identical to the other `node`
/// subcommands. The host
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
