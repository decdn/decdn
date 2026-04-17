//! `decdn node ...` — operator-local admin commands that talk to a running
//! node over its loopback HTTP admin surface (ADR 025).

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;

use crate::admin::{PeerView, PeersResponse};
use crate::cli;
use crate::cli::common::expand_tilde;
use crate::config::DEFAULT_ADMIN_PORT;

/// Dispatch a `decdn node <sub>` invocation.
pub async fn node_dispatch(args: &cli::NodeArgs) -> anyhow::Result<()> {
    match &args.cmd {
        cli::NodeCommand::Peers(p) => peers(p).await,
    }
}

/// `decdn node peers`: fetch `/v1/peers` from the running node and print it.
pub async fn peers(args: &cli::PeersArgs) -> anyhow::Result<()> {
    let base = resolve_admin_url(args.admin_url.as_deref(), args.config.as_deref())?;
    let url = format!("{}/v1/peers", base.trim_end_matches('/'));

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(args.timeout_ms))
        .build()
        .context("failed to build HTTP client")?;

    let resp = client.get(&url).send().await.map_err(|err| {
        // Split connect vs. timeout vs. everything-else so the operator
        // message points at the right fix. An ECONNREFUSED almost always
        // means "admin isn't running / wrong port"; a timeout means the
        // node is up but slow (often a lock-contention issue); other
        // reqwest errors cover DNS/TLS/protocol — rarer in loopback but
        // not free.
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
        print_peers_table(&filtered);
    }

    Ok(())
}

/// Resolve the admin URL in this precedence order:
///
/// 1. `--admin-url` flag (`args.admin_url`). Clap's `env = "DECDN_ADMIN_URL"`
///    attribute already folds the env var into this field, so a single
///    check here covers both sources.
/// 2. `observability.admin_port` from the TOML config file — either the
///    explicit `--config <path>` or the default `~/.decdn/node.toml`. An
///    explicit `--config` path that doesn't exist is an error (mirrors
///    `decdn run`'s handling); a missing *default* path falls through to
///    the built-in default. Explicit `admin_port = 0` in the file is an
///    operator opt-out and errors here rather than silently probing the
///    default.
/// 3. Default `http://127.0.0.1:9191`.
fn resolve_admin_url(flag: Option<&str>, config_path: Option<&Path>) -> anyhow::Result<String> {
    if let Some(url) = flag {
        return Ok(url.to_string());
    }
    let (resolved_path, explicit) = match config_path {
        Some(p) => (Some(expand_tilde(p)), true),
        None => (cli::common::default_config_path(), false),
    };
    let port =
        port_from_config_file(resolved_path.as_deref(), explicit)?.unwrap_or(DEFAULT_ADMIN_PORT);
    Ok(format!("http://127.0.0.1:{port}"))
}

/// Read `observability.admin_port` from a TOML config file. Returns:
/// - `Ok(None)` when `path` is `None`, or when `path` is the *default*
///   path and the file doesn't exist (operator hasn't set up a config
///   file yet — fall through to the built-in default).
/// - `Ok(Some(port))` for a positive port value.
/// - `Err` if the file exists but can't be parsed, if the operator
///   explicitly set `admin_port = 0` (which disables the server), or if
///   `explicit` is true and the file is missing/unreadable (operator
///   passed `--config` pointing at the wrong place).
fn port_from_config_file(path: Option<&Path>, explicit: bool) -> anyhow::Result<Option<u16>> {
    let Some(path) = path else { return Ok(None) };
    let expanded = if path.is_absolute() {
        PathBuf::from(path)
    } else {
        expand_tilde(path)
    };
    match std::fs::read_to_string(&expanded) {
        Ok(contents) => {
            let file: crate::config::FileConfig = toml::from_str(&contents)
                .with_context(|| format!("failed to parse config file {}", expanded.display()))?;
            match file.observability.and_then(|o| o.admin_port) {
                Some(0) => anyhow::bail!(
                    "config {} disables the admin server (observability.admin_port = 0); \
                     pass --admin-url or enable the admin port",
                    expanded.display()
                ),
                other => Ok(other),
            }
        }
        // Missing file: fine only if we fell back to the default path. An
        // explicit --config that's missing is almost always an operator typo.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && !explicit => Ok(None),
        Err(err) => Err(anyhow::anyhow!(
            "failed to read config file {}: {err}",
            expanded.display()
        )),
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

/// Re-serialize the (possibly filtered) peer list so the output reflects
/// `--region` instead of the raw server body. Kept as a pure function so
/// tests can round-trip filter-then-render without an HTTP hop.
fn render_json(peers: &[PeerView]) -> Result<String, serde_json::Error> {
    let out = PeersResponse {
        peers: peers.to_vec(),
    };
    serde_json::to_string_pretty(&out)
}

fn print_peers_table(peers: &[PeerView]) {
    if peers.is_empty() {
        println!("(no peers known)");
        return;
    }
    // Fixed-column layout: 14 (node_id preview) | 8 (region) | 18 (load) | rest (last_seen).
    let (node_hdr, region_hdr, load_hdr, last_hdr) = ("NODE_ID", "REGION", "LOAD", "LAST_SEEN");
    println!("{node_hdr:<14} {region_hdr:<8} {load_hdr:<18} {last_hdr}");
    let now_us = wall_clock_us();
    for p in peers {
        let preview = short_node_id(&p.node_id);
        let load = format!(
            "{}s/{}%",
            p.load.active_streams, p.load.bandwidth_utilization
        );
        let age = relative_age(now_us, p.last_seen_us);
        let region = truncate(&p.region, 8);
        println!("{preview:<14} {region:<8} {load:<18} {age}");
    }
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
        // Both the length and the exact surviving node_ids are asserted
        // so a regression that filtered on the wrong field (e.g. node_id
        // vs region) can't accidentally produce a matching count.
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
    fn resolve_admin_url_prefers_flag() {
        let got = resolve_admin_url(Some("http://custom:1234"), None).expect("flag path ok");
        assert_eq!(got, "http://custom:1234");
    }

    #[test]
    fn port_from_config_file_missing_default_returns_none() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("absent.toml");
        assert_eq!(port_from_config_file(Some(&path), false)?, None);
        Ok(())
    }

    #[test]
    fn port_from_config_file_missing_explicit_errors() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("absent.toml");
        let err = port_from_config_file(Some(&path), true)
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
    fn port_from_config_file_reads_admin_port() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 12345\n")?;
        assert_eq!(port_from_config_file(Some(&path), false)?, Some(12345));
        Ok(())
    }

    #[test]
    fn port_from_config_file_errors_on_explicit_zero() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("node.toml");
        std::fs::write(&path, b"[observability]\nadmin_port = 0\n")?;
        let err = port_from_config_file(Some(&path), false)
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
        let err = port_from_config_file(Some(&path), false)
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
