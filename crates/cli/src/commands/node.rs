//! `decdn node ...` — operator-local admin commands that talk to a running
//! node over its loopback JSON-RPC admin surface (ADR 025).

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use jsonrpsee::core::client::Error as JsonRpcClientError;
use jsonrpsee::http_client::HttpClientBuilder;
use serde::Deserialize;

use decdn_common::admin::{
    AdminRpcClient, AnnounceResponse, DrainResponse, EvictRequest, EvictResponse, HealthResponse,
    PeerView, PeersResponse, ReloadResponse,
};
use decdn_common::cli;
use decdn_common::cli::common::expand_tilde;
use decdn_common::config::DEFAULT_ADMIN_PORT;

/// Whether the TOML config path we're about to read was chosen by the
/// operator or defaulted. Drives the "missing file" policy in
/// [`port_from_config_file`]: an explicit path that isn't there is
/// almost certainly a typo and should error, while a default path that
/// isn't there is normal (operator just hasn't made a config yet).
#[derive(Debug, Clone, Copy)]
enum ConfigPathSource {
    /// Path came from `--config` (subcommand) or the global `decdn
    /// --config` flag.
    Explicit,
    /// Path came from the built-in default (`~/.decdn/node.toml`).
    Default,
}

/// Partial deserializer for the TOML config — only the path
/// `observability.admin_port` is interesting to `decdn node peers`.
/// Kept private here (rather than reusing `decdn_common::config::FileConfig`)
/// so an operator's typo in an unrelated section can't make peer
/// listing unusable. `serde(default)` and serde-toml's default
/// "ignore unknown fields" together guarantee that any other valid
/// TOML — including missing tables — round-trips through with no
/// effect.
#[derive(Debug, Default, Deserialize)]
struct AdminPortConfig {
    observability: Option<AdminPortObservability>,
}

#[derive(Debug, Default, Deserialize)]
struct AdminPortObservability {
    admin_port: Option<u16>,
}

/// Dispatch a `decdn node <sub>` invocation.
///
/// `global_config` is the path (if any) from the top-level `decdn
/// --config` flag. It's consulted as a fallback when the subcommand
/// didn't set its own `--config`, so `decdn --config foo.toml node
/// peers` works the way the help text implies.
pub async fn node_dispatch(
    args: &cli::NodeArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    match &args.cmd {
        cli::NodeCommand::Peers(p) => peers(p, global_config).await,
        cli::NodeCommand::Health(h) => health(h, global_config).await,
        cli::NodeCommand::Evict(e) => evict(e, global_config).await,
        cli::NodeCommand::Announce(a) => announce(a, global_config).await,
        cli::NodeCommand::Reload(r) => reload(r, global_config).await,
        cli::NodeCommand::Drain(d) => drain(d, global_config).await,
    }
}

/// `decdn node health`: call `admin_v1_health` on the running node and
/// print the result. Two-line plain text by default (`node_id=…` /
/// `uptime_s=…`), or pretty JSON with `--json`.
pub async fn health(args: &cli::HealthArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: HealthResponse = client
        .health()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode health as JSON")?;
        println!("{pretty}");
    } else {
        // Two stable, grep-friendly lines so `decdn node health | grep
        // node_id=` works in operator scripts without `--json`.
        println!("node_id={}", resp.node_id);
        println!("uptime_s={}", resp.uptime_s);
    }

    Ok(())
}

/// `decdn node evict`: call `admin_v1_evict` on the running node to
/// forcibly remove a single blob from the local cache (issue #279), or
/// preview what the evict would touch when `--dry-run` is set (issue
/// #379).
pub async fn evict(args: &cli::EvictArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: EvictResponse = client
        .evict(EvictRequest {
            hash: args.hash.clone(),
            dry_run: args.dry_run,
        })
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode evict response")?;
        println!("{pretty}");
    } else if args.dry_run {
        // Multi-line, grep-friendly so operators can pipe through
        // `grep size_bytes=` / `grep already_evicted=` in scripts. Each
        // line stands on its own; the "dry_run=true" tag is the
        // load-bearing indicator that no state was mutated.
        write_dry_run_human(&mut io::stdout().lock(), &args.hash, &resp)
            .context("failed to write dry-run preview")?;
    } else {
        let presence = if resp.was_present {
            "evicted"
        } else {
            // Operator ran evict on a hash the node never held — log it
            // explicitly rather than printing nothing, otherwise scripts
            // can't tell success-with-no-effect from a hung command.
            "not present"
        };
        println!("hash={} status={presence}", args.hash);
    }

    Ok(())
}

/// Format the dry-run preview as a multi-line plain-text block. Pure
/// function (takes `&mut impl Write`) so unit tests can assert exact
/// output without an HTTP round-trip — same pattern as
/// [`write_peers_table`].
fn write_dry_run_human(w: &mut impl io::Write, hash: &str, resp: &EvictResponse) -> io::Result<()> {
    writeln!(w, "hash={hash}")?;
    writeln!(w, "dry_run=true")?;
    writeln!(w, "was_present={}", resp.was_present)?;
    writeln!(w, "pinned={}", resp.preview.pinned)?;
    writeln!(w, "already_evicted={}", resp.preview.already_evicted)?;
    match resp.preview.size_bytes {
        Some(b) => writeln!(w, "size_bytes={b}")?,
        // Distinct from "0" (a legitimately empty blob); operators
        // seeing `not_stored` know the iroh-blobs status reported
        // `NotFound`, not `Complete { size: 0 }`.
        None => writeln!(w, "size_bytes=not_stored")?,
    }
    match resp.preview.last_accessed_us_ago {
        Some(us) => writeln!(w, "last_accessed={}", format_age(us))?,
        // Distinct from "<1s ago"; operators want to know the
        // engine has *no* access record vs a very recent one.
        None => writeln!(w, "last_accessed=never")?,
    }
    // Origin egress-cost cue (#439). Distinct from omitting the line
    // when the engine has no origin: operators evaluating disk-reclaim
    // potential against re-fetch cost want this signal explicitly,
    // not buried in "the field is missing because there's no origin
    // at all". `none` matches the JSON serialisation skip-condition
    // semantically (`Option::is_none` is omitted in JSON, surfaced as
    // `none` here).
    match resp.preview.origin_kind {
        Some(kind) => writeln!(w, "origin_kind={kind}")?,
        None => writeln!(w, "origin_kind=none")?,
    }
    Ok(())
}

/// `decdn node announce`: call `admin_v1_announce` on the running node to
/// publish a one-shot `NodeAnnounce` outside the periodic interval (issue
/// #280).
pub async fn announce(
    args: &cli::AnnounceArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: AnnounceResponse = client
        .announce()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode announce response")?;
        println!("{pretty}");
    } else {
        // The trigger is fire-and-forget on the publisher side, so this
        // confirms only that the node accepted the request — `queued`
        // rather than `triggered` so a script reader can't mistake this
        // for "broadcast hit the wire". Actual peer delivery is
        // observable via `decdn node peers` on a peer; broadcast failures
        // (no neighbors, transport error) surface as `warn!` lines in the
        // node's own log.
        println!("announce_queued={}", resp.triggered);
    }

    Ok(())
}

/// `decdn node reload`: call `admin_v1_reload` on the running node to
/// re-read the config file it was started with and apply the
/// hot-reloadable subset (issue #373). Equivalent to `kill -HUP <pid>`
/// for operators who'd rather not stat the PID — and shares the same
/// internal mutex, so concurrent SIGHUPs queue rather than race.
pub async fn reload(args: &cli::ReloadArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: ReloadResponse = client
        .reload()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode reload response")?;
        println!("{pretty}");
    } else {
        // Two stable, grep-friendly lines so an operator can do
        // `decdn node reload | grep rate_per_mb=` without `--json`.
        // Same shape as `decdn node health`'s plain output.
        println!("rate_per_mb={}", resp.rate_per_mb);
        println!("log_level={}", resp.log_level);
    }

    Ok(())
}

/// `decdn node drain`: call `admin_v1_drain` on the running node to trigger
/// graceful shutdown (issue #244, ADR 025). Fire-and-forget: returns
/// `drain_initiated=true` as soon as the trigger is queued; the runtime
/// then begins the same shutdown sequence SIGTERM triggers. Observe
/// completion via process exit or `decdn node health` until ECONNREFUSED.
pub async fn drain(args: &cli::DrainArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: DrainResponse = client
        .drain()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    if args.json {
        let pretty =
            serde_json::to_string_pretty(&resp).context("failed to encode drain response")?;
        println!("{pretty}");
    } else {
        // One stable, grep-friendly line. "Initiated", not "completed":
        // fire-and-forget semantics mean the process is still running when
        // this prints. Observe completion via `decdn node health` until
        // ECONNREFUSED, or let the process supervisor (systemd/K8s) notify.
        println!("drain_initiated={}", resp.initiated);
    }

    Ok(())
}

/// `decdn node peers`: call `admin_v1_peersList` on the running node and
/// print the result.
pub async fn peers(args: &cli::PeersArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );

    let config_path = args.config.as_deref().or(global_config);
    let url = resolve_admin_url(args.admin_url.as_deref(), config_path)?;

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let parsed: PeersResponse = client
        .peers_list()
        .await
        .map_err(|err| classify_client_error(&url, args.timeout_ms, err))?;

    let filtered = filter_peers(parsed.peers, args.region.as_deref());

    if args.json {
        let pretty = render_json(&filtered).context("failed to encode filtered peers as JSON")?;
        println!("{pretty}");
    } else {
        let mut stdout = io::stdout().lock();
        write_peers_table(&mut stdout, &filtered, wall_clock_us())
            .context("failed to write peers table")?;
    }

    Ok(())
}

/// Map a `jsonrpsee` client error into the three operator-actionable
/// classes the previous reqwest path exposed:
///
/// - `Transport` with an `ECONNREFUSED` in the source chain → "admin
///   isn't there" (check it's running / port).
/// - `RequestTimeout` → "admin is slow" (stuck lock, overloaded).
/// - `Call` → the server returned a JSON-RPC application error; surface
///   the code and message so the operator can tell a misconfigured
///   method name from a real server failure.
/// - Anything else → passed through with the URL as context.
fn classify_client_error(url: &str, timeout_ms: u64, err: JsonRpcClientError) -> anyhow::Error {
    match err {
        JsonRpcClientError::RequestTimeout => anyhow::anyhow!(
            "admin at {url} did not respond within {timeout_ms}ms; \
             the node may be overloaded or blocked on a long lock hold",
        ),
        JsonRpcClientError::Transport(inner) => {
            if is_connection_refused(inner.as_ref()) {
                anyhow::anyhow!(
                    "admin at {url} refused the connection ({inner}); is the node \
                     running, and is admin_port configured correctly?",
                )
            } else {
                anyhow::anyhow!("admin request to {url} failed: {inner}")
            }
        }
        JsonRpcClientError::Call(obj) => anyhow::anyhow!(
            "admin at {url} returned JSON-RPC error {code}: {msg}",
            code = obj.code(),
            msg = obj.message(),
        ),
        other => anyhow::Error::new(other).context(format!("admin request to {url} failed")),
    }
}

/// Walk the `source()` chain looking for an `std::io::Error` of kind
/// `ConnectionRefused`. The hyper/jsonrpsee error hierarchy is several
/// layers deep and the exact intermediate types are implementation
/// details, so match on the innermost `io::Error` kind instead of any
/// particular transport type.
fn is_connection_refused(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
            if io_err.kind() == std::io::ErrorKind::ConnectionRefused {
                return true;
            }
        }
        current = e.source();
    }
    false
}

/// Resolve the admin URL in this precedence order:
///
/// 1. `--admin-url` flag or the `DECDN_ADMIN_URL` env var (clap folds the
///    env into `args.admin_url`).
/// 2. `observability.admin_port` from the TOML config file — either the
///    caller's explicit path or the default `~/.decdn/node.toml`. An
///    explicit path that doesn't exist is an error; a missing default
///    path falls through. `admin_port = 0` in the file is an operator
///    opt-out and errors here rather than silently probing the default.
/// 3. Default `http://127.0.0.1:9191`.
fn resolve_admin_url(flag: Option<&str>, config_path: Option<&Path>) -> anyhow::Result<String> {
    if let Some(url) = flag {
        return Ok(url.to_string());
    }
    let (resolved_path, source) = match config_path {
        Some(p) => (Some(expand_tilde(p)), ConfigPathSource::Explicit),
        None => (
            cli::common::default_config_path(),
            ConfigPathSource::Default,
        ),
    };
    let port =
        port_from_config_file(resolved_path.as_deref(), source)?.unwrap_or(DEFAULT_ADMIN_PORT);
    Ok(format!("http://127.0.0.1:{port}"))
}

/// Read `observability.admin_port` from a TOML config file. Returns:
/// - `Ok(None)` when `path` is `None`, or when `path` is the *default*
///   path and the file doesn't exist (operator hasn't set up a config
///   file yet — fall through to the built-in default).
/// - `Ok(Some(port))` for a positive port value.
/// - `Err` if the file exists but can't be parsed, if the operator
///   explicitly set `admin_port = 0` (which disables the server), or if
///   `source` is `Explicit` and the file is missing/unreadable.
///
/// Callers are expected to have already tilde-expanded the path;
/// `resolve_admin_url` is the only intended caller and does so via
/// [`expand_tilde`] / [`cli::common::default_config_path`].
fn port_from_config_file(
    path: Option<&Path>,
    source: ConfigPathSource,
) -> anyhow::Result<Option<u16>> {
    let Some(path) = path else { return Ok(None) };
    let path: PathBuf = path.to_path_buf();
    match std::fs::read_to_string(&path) {
        Ok(contents) => {
            // Deliberately a partial deserializer: parsing the full
            // `FileConfig` would couple `decdn node peers` to every
            // unrelated field's well-formedness. An operator with a
            // typo'd `[payments]` table shouldn't lose the ability to
            // list peers. Serde's TOML mode ignores unknown fields by
            // default, so this only fails on (a) genuinely malformed
            // TOML or (b) a wrong type for `observability.admin_port`
            // itself — both of which we genuinely want to surface.
            let parsed: AdminPortConfig = toml::from_str(&contents)
                .with_context(|| format!("failed to parse config file {}", path.display()))?;
            match parsed.observability.and_then(|o| o.admin_port) {
                Some(0) => anyhow::bail!(
                    "config {} disables the admin server (observability.admin_port = 0); \
                     pass --admin-url or enable the admin port",
                    path.display()
                ),
                other => Ok(other),
            }
        }
        Err(err) => match (err.kind(), source) {
            // Default path not yet created: fall through to the built-in.
            (io::ErrorKind::NotFound, ConfigPathSource::Default) => Ok(None),
            // Permission problems usually point at file mode / ownership,
            // not a wrong path — give the operator that nudge rather than
            // a generic "failed to read".
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

fn filter_peers(peers: Vec<PeerView>, region: Option<&str>) -> Vec<PeerView> {
    match region {
        None => peers,
        Some(want) => peers
            .into_iter()
            .filter(|p| p.region.eq_ignore_ascii_case(want))
            .collect(),
    }
}

/// Render the (possibly filtered) peer list as pretty JSON.
///
/// Kept as a pure function so tests can round-trip filter-then-render
/// without an HTTP hop; the filter's effect on the `--json` output is
/// otherwise only observable at the shell level.
fn render_json(peers: &[PeerView]) -> anyhow::Result<String> {
    let out = PeersResponse {
        peers: peers.to_vec(),
    };
    serde_json::to_string_pretty(&out).context("encode peers as JSON")
}

/// Write the peer table to `w`. Taking `&mut impl Write` instead of
/// writing directly to `stdout` makes the formatter testable and makes
/// it a straightforward component for future TUI consumers.
fn write_peers_table(w: &mut impl io::Write, peers: &[PeerView], now_us: u64) -> io::Result<()> {
    if peers.is_empty() {
        return writeln!(w, "(no peers known)");
    }
    // Fixed-column layout: 14 (node_id preview) | 8 (region) | 18 (load) | rest (last_seen).
    let (node_hdr, region_hdr, load_hdr, last_hdr) = ("NODE_ID", "REGION", "LOAD", "LAST_SEEN");
    writeln!(
        w,
        "{node_hdr:<14} {region_hdr:<8} {load_hdr:<18} {last_hdr}"
    )?;
    for p in peers {
        let preview = short_node_id(&p.node_id);
        let load = format!(
            "{}s/{}%",
            p.load.active_streams, p.load.bandwidth_utilization
        );
        let age = relative_age(now_us, p.last_seen_us);
        let region = truncate(&p.region, 8);
        writeln!(w, "{preview:<14} {region:<8} {load:<18} {age}")?;
    }
    Ok(())
}

fn short_node_id(hex: &str) -> String {
    // Unicode '…' (U+2026) rather than "..." so a pasted preview is
    // unambiguously a preview and never parses as hex.
    let mut chars = hex.chars();
    let prefix: String = chars.by_ref().take(12).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn truncate(s: &str, max: usize) -> String {
    let mut chars = s.chars();
    let mut out: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() {
        out.push('…');
    }
    out
}

fn wall_clock_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// Coarse operator-facing age bucket. On-call use case is "is this peer
/// fresh in the last N {s,m,h,d}?"; sub-second precision would only add
/// noise to the table.
fn relative_age(now_us: u64, last_seen_us: u64) -> String {
    if last_seen_us == 0 {
        return "unknown".to_string();
    }
    if last_seen_us > now_us {
        // Clock skew: report as "in the future" rather than a huge negative
        // unsigned delta. Honest about the condition without panicking.
        return "in future".to_string();
    }
    let delta_us = now_us - last_seen_us;
    format_age(delta_us)
}

fn format_age(delta_us: u64) -> String {
    const US_PER_SEC: u64 = 1_000_000;
    const US_PER_MIN: u64 = 60 * US_PER_SEC;
    const US_PER_HOUR: u64 = 60 * US_PER_MIN;
    const US_PER_DAY: u64 = 24 * US_PER_HOUR;
    if delta_us < US_PER_SEC {
        return "<1s ago".to_string();
    }
    if delta_us < US_PER_MIN {
        return format!("{}s ago", delta_us / US_PER_SEC);
    }
    if delta_us < US_PER_HOUR {
        return format!("{}m ago", delta_us / US_PER_MIN);
    }
    if delta_us < US_PER_DAY {
        return format!("{}h ago", delta_us / US_PER_HOUR);
    }
    format!("{}d ago", delta_us / US_PER_DAY)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use decdn_protocol::LoadHint;

    fn mk_peer(node_id: &str, region: &str, last_seen_us: u64) -> PeerView {
        PeerView {
            node_id: node_id.to_string(),
            region: region.to_string(),
            first_seen_us: last_seen_us,
            last_seen_us,
            load: LoadHint {
                active_streams: 0,
                bandwidth_utilization: 0,
            },
            announced_at_us: last_seen_us,
        }
    }

    #[test]
    fn filter_by_region_is_case_insensitive() {
        let peers = vec![
            mk_peer("aa", "US", 10),
            mk_peer("bb", "us", 20),
            mk_peer("cc", "EU", 30),
        ];
        let filtered = filter_peers(peers, Some("US"));
        // Assert exact surviving ids so a regression that filtered on the
        // wrong field (e.g. node_id vs region) can't produce a matching
        // count by accident.
        let ids: Vec<&str> = filtered.iter().map(|p| p.node_id.as_str()).collect();
        assert_eq!(ids, vec!["aa", "bb"]);
    }

    #[test]
    fn filter_none_is_passthrough() {
        let peers = vec![mk_peer("aa", "US", 10)];
        assert_eq!(filter_peers(peers.clone(), None).len(), peers.len());
    }

    #[test]
    fn render_json_roundtrips_through_filter() -> anyhow::Result<()> {
        let peers = vec![
            mk_peer("aa", "US", 10),
            mk_peer("bb", "EU", 20),
            mk_peer("cc", "US", 30),
        ];
        let filtered = filter_peers(peers, Some("US"));
        let pretty = render_json(&filtered)?;
        let value: serde_json::Value = serde_json::from_str(&pretty)?;
        let out_peers = value["peers"].as_array().expect("peers array");
        assert_eq!(out_peers.len(), 2);
        let out_ids: Vec<&str> = out_peers
            .iter()
            .map(|p| p["node_id"].as_str().unwrap_or_default())
            .collect();
        assert_eq!(out_ids, vec!["aa", "cc"]);
        Ok(())
    }

    #[test]
    fn write_peers_table_empty_emits_sentinel() -> anyhow::Result<()> {
        let mut buf = Vec::<u8>::new();
        write_peers_table(&mut buf, &[], 1_000)?;
        let s = String::from_utf8(buf)?;
        assert_eq!(s, "(no peers known)\n");
        Ok(())
    }

    #[test]
    fn write_peers_table_renders_header_and_row() -> anyhow::Result<()> {
        let peer = mk_peer(&"a".repeat(64), "US", 1_000_000); // last_seen = 1s past epoch
        let now_us = 2_000_000; // 1s after last_seen → "1s ago"
        let mut buf = Vec::<u8>::new();
        write_peers_table(&mut buf, &[peer], now_us)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("NODE_ID"), "header missing: {s}");
        assert!(s.contains("LAST_SEEN"), "header missing: {s}");
        assert!(s.contains("aaaaaaaaaaaa…"), "node_id preview missing: {s}");
        assert!(s.contains("US"), "region missing: {s}");
        assert!(s.contains("1s ago"), "relative age missing: {s}");
        Ok(())
    }

    #[test]
    fn resolve_admin_url_prefers_flag() {
        let got = resolve_admin_url(Some("http://custom:1234"), None).expect("flag path ok");
        assert_eq!(got, "http://custom:1234");
    }

    #[test]
    fn port_from_config_file_missing_default_returns_none() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("absent.toml");
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Default)?,
            None
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_missing_explicit_errors() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("absent.toml");
        let err = port_from_config_file(Some(&path), ConfigPathSource::Explicit)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected explicit-missing error"))?
            .to_string();
        assert!(
            err.contains("failed to read config file"),
            "missing context: {err}"
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_reads_admin_port_default_path() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 12345\n")?;
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Default)?,
            Some(12345)
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_reads_admin_port_explicit_path() -> anyhow::Result<()> {
        // Explicit-source + valid file must succeed the same way as the
        // default-source case. Without this test, a regression that
        // broadened the explicit-source error arm to swallow successes
        // would still pass CI.
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 7777\n")?;
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Explicit)?,
            Some(7777)
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_errors_on_explicit_zero() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 0\n")?;
        let err = port_from_config_file(Some(&path), ConfigPathSource::Default)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected error for explicit 0"))?
            .to_string();
        assert!(
            err.contains("disables the admin server"),
            "missing context: {err}"
        );
        Ok(())
    }

    #[test]
    fn port_from_config_file_errors_on_invalid_toml() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("broken.toml");
        std::fs::write(&path, b"not = valid = toml")?;
        let err = port_from_config_file(Some(&path), ConfigPathSource::Default)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected parse error"))?
            .to_string();
        assert!(err.contains("parse"), "missing context: {err}");
        Ok(())
    }

    // Locks in the partial-deserializer choice: a wrong type in some
    // unrelated section (here, a malformed `[network]` field that the
    // full FileConfig would reject) must not stop `decdn node peers`
    // from resolving the admin port. If a future refactor reverts to
    // parsing FileConfig, this test fails.
    #[test]
    fn port_from_config_file_ignores_unrelated_field_errors() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        // `network.bind_addr` would be a string in the real schema;
        // making it an integer is a guaranteed type-mismatch for the
        // full FileConfig, but the narrow AdminPortConfig deserializer
        // never sees `network` so it must round-trip fine.
        std::fs::write(
            &path,
            b"[observability]\nadmin_port = 4242\n[network]\nbind_addr = 7\n",
        )?;
        assert_eq!(
            port_from_config_file(Some(&path), ConfigPathSource::Default)?,
            Some(4242)
        );
        Ok(())
    }

    #[test]
    fn format_age_units() {
        assert_eq!(format_age(500_000), "<1s ago");
        assert_eq!(format_age(2_000_000), "2s ago");
        assert_eq!(format_age(90 * 1_000_000), "1m ago");
        assert_eq!(format_age(2 * 3600 * 1_000_000), "2h ago");
        assert_eq!(format_age(36 * 3600 * 1_000_000), "1d ago");
    }

    #[test]
    fn relative_age_handles_future_and_zero() {
        assert_eq!(relative_age(100, 200), "in future");
        assert_eq!(relative_age(100, 0), "unknown");
    }

    #[test]
    fn short_node_id_trims_long_hex() {
        let full = "a".repeat(64);
        let s = short_node_id(&full);
        assert_eq!(s.chars().count(), 13); // 12 hex + ellipsis
        assert!(s.ends_with('…'));
    }

    #[test]
    fn short_node_id_passthrough_when_already_short() {
        let s = short_node_id("abcd");
        assert_eq!(s, "abcd");
    }

    /// `--dry-run` plain output is multi-line, key=value, grep-friendly.
    /// Asserts every load-bearing field appears on its own line so a
    /// regression that collapsed the table back to one line (or dropped
    /// e.g. `pinned=`) breaks here rather than silently eating the
    /// information the operator needs to decide whether to run the real
    /// evict.
    #[test]
    fn write_dry_run_human_emits_all_fields() -> anyhow::Result<()> {
        use decdn_common::admin::EvictPreview;
        let resp = EvictResponse {
            was_present: true,
            dry_run: true,
            preview: EvictPreview {
                size_bytes: Some(1024),
                last_accessed_us_ago: Some(2_000_000), // 2s ago via format_age
                pinned: true,
                already_evicted: false,
                origin_kind: Some(decdn_cache::OriginKind::Http),
            },
        };
        let mut buf = Vec::<u8>::new();
        write_dry_run_human(&mut buf, "abcd", &resp)?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("hash=abcd"), "missing hash: {s}");
        assert!(s.contains("dry_run=true"), "missing dry_run tag: {s}");
        assert!(s.contains("was_present=true"), "missing was_present: {s}");
        assert!(s.contains("pinned=true"), "missing pinned: {s}");
        assert!(
            s.contains("already_evicted=false"),
            "missing already_evicted: {s}"
        );
        assert!(s.contains("size_bytes=1024"), "missing size_bytes: {s}");
        assert!(
            s.contains("last_accessed=2s ago"),
            "expected formatted last_accessed, got: {s}"
        );
        assert!(
            s.contains("origin_kind=http"),
            "missing origin_kind (#439): {s}"
        );
        Ok(())
    }

    /// Sentinels for the "no information available" cases:
    /// `size_bytes=not_stored` and `last_accessed=never`. Distinct from
    /// "0" / "<1s ago" so an operator can tell "the engine has no
    /// record" from "the record is at the floor".
    #[test]
    fn write_dry_run_human_uses_sentinels_for_absent_fields() -> anyhow::Result<()> {
        use decdn_common::admin::EvictPreview;
        let resp = EvictResponse {
            was_present: false,
            dry_run: true,
            preview: EvictPreview {
                size_bytes: None,
                last_accessed_us_ago: None,
                pinned: false,
                already_evicted: false,
                // Cache-only mode: no origin configured, so the
                // dry-run reports `none` rather than omitting the
                // line entirely (#439).
                origin_kind: None,
            },
        };
        let mut buf = Vec::<u8>::new();
        write_dry_run_human(&mut buf, "deadbeef", &resp)?;
        let s = String::from_utf8(buf)?;
        assert!(
            s.contains("size_bytes=not_stored"),
            "expected not_stored sentinel, got: {s}"
        );
        assert!(
            s.contains("origin_kind=none"),
            "expected origin_kind=none sentinel for cache-only mode, got: {s}"
        );
        assert!(
            s.contains("last_accessed=never"),
            "expected never sentinel, got: {s}"
        );
        Ok(())
    }
}
