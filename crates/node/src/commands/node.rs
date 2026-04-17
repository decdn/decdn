//! `decdn node ...` — operator-local admin commands that talk to a running
//! node over its loopback HTTP admin surface (ADR 025).

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;

use crate::admin::{PeerView, PeersResponse};
use crate::cli;
use crate::cli::common::expand_tilde;
use crate::config::DEFAULT_ADMIN_PORT;

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
    }
}

/// `decdn node peers`: fetch `/v1/peers` from the running node and print it.
pub async fn peers(args: &cli::PeersArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (reqwest treats Duration::ZERO as an \
         implementation-defined sentinel, not a sub-millisecond deadline)"
    );

    let config_path = args.config.as_deref().or(global_config);
    let base = resolve_admin_url(args.admin_url.as_deref(), config_path)?;
    let url = format!("{}/v1/peers", base.trim_end_matches('/'));

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(args.timeout_ms))
        .build()
        .context("failed to build HTTP client")?;

    let resp = client.get(&url).send().await.map_err(|err| {
        // Three operator-actionable classes: "admin isn't there"
        // (ECONNREFUSED → check it's running / port), "admin is slow"
        // (timeout → check for stuck locks), and everything else.
        if err.is_connect() {
            anyhow::anyhow!(
                "admin at {url} refused the connection ({err}); is the node running, \
                 and is admin_port configured correctly?",
            )
        } else if err.is_timeout() {
            anyhow::anyhow!(
                "admin at {url} did not respond within {}ms ({err}); the node may be \
                 overloaded or blocked on a long lock hold",
                args.timeout_ms,
            )
        } else {
            anyhow::Error::new(err).context(format!("admin request to {url} failed"))
        }
    })?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .with_context(|| format!("failed to read response body from {url}"))?;

    if !status.is_success() {
        anyhow::bail!("admin returned HTTP {status} from {url}: {body}");
    }

    let parsed: PeersResponse =
        serde_json::from_str(&body).with_context(|| format!("failed to parse JSON from {url}"))?;

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
            let file: crate::config::FileConfig = toml::from_str(&contents)
                .with_context(|| format!("failed to parse config file {}", path.display()))?;
            match file.observability.and_then(|o| o.admin_port) {
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
    let prefix: String = hex.chars().take(12).collect();
    if hex.chars().count() > 12 {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn truncate(s: &str, max: usize) -> String {
    let mut out: String = s.chars().take(max).collect();
    if s.chars().count() > max {
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
}
