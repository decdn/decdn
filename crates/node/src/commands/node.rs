//! `decdn node ...` — operator-local admin commands that talk to a running
//! node over its loopback HTTP admin surface (ADR 025).

use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;

use crate::cli;

const DEFAULT_ADMIN_URL: &str = "http://127.0.0.1:9191";

/// Dispatch a `decdn node <sub>` invocation.
pub async fn node_dispatch(args: &cli::NodeArgs) -> anyhow::Result<()> {
    match &args.cmd {
        cli::NodeCommand::Peers(p) => peers(p).await,
    }
}

/// `decdn node peers`: fetch `/v1/peers` from the running node and print it.
pub async fn peers(args: &cli::PeersArgs) -> anyhow::Result<()> {
    let base = resolve_admin_url(args.admin_url.as_deref());
    let url = format!("{}/v1/peers", base.trim_end_matches('/'));

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(args.timeout_ms))
        .build()
        .context("failed to build HTTP client")?;

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("admin request to {url} failed"))?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .with_context(|| format!("failed to read response body from {url}"))?;

    if !status.is_success() {
        anyhow::bail!("admin returned HTTP {status}: {body}");
    }

    let parsed: PeersResponse =
        serde_json::from_str(&body).with_context(|| format!("failed to parse JSON from {url}"))?;

    let filtered = filter_peers(parsed.peers, args.region.as_deref());

    if args.json {
        // Re-serialize so the filter (if any) is reflected in the output and
        // the JSON is pretty-printed consistently regardless of the server's
        // whitespace.
        let out = PeersResponse {
            peers: filtered.clone(),
        };
        let pretty = serde_json::to_string_pretty(&out)
            .context("failed to encode filtered peers as JSON")?;
        println!("{pretty}");
    } else {
        print_peers_table(&filtered);
    }

    Ok(())
}

fn resolve_admin_url(flag: Option<&str>) -> String {
    flag.map(str::to_string)
        .or_else(|| std::env::var("DECDN_ADMIN_URL").ok())
        .unwrap_or_else(|| DEFAULT_ADMIN_URL.to_string())
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
    // 12 leading hex chars + ellipsis so a full terminal column stays under
    // 14 glyphs. Unicode '…' (U+2026) instead of "..." keeps the preview
    // unambiguous when copy-pasted.
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

#[derive(Debug, Clone, serde::Serialize, Deserialize)]
struct PeersResponse {
    peers: Vec<PeerView>,
}

#[derive(Debug, Clone, serde::Serialize, Deserialize)]
struct PeerView {
    node_id: String,
    region: String,
    first_seen_us: u64,
    last_seen_us: u64,
    load: decdn_protocol::LoadHint,
    announced_at_us: u64,
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
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|p| p.region.eq_ignore_ascii_case("US")));
    }

    #[test]
    fn filter_none_is_passthrough() {
        let peers = vec![mk_peer("aa", "US", 10)];
        assert_eq!(filter_peers(peers.clone(), None).len(), peers.len());
    }

    #[test]
    fn resolve_admin_url_prefers_flag() {
        let got = resolve_admin_url(Some("http://custom:1234"));
        assert_eq!(got, "http://custom:1234");
    }

    #[test]
    fn resolve_admin_url_default_when_flag_missing() {
        // Guard: DECDN_ADMIN_URL must not be set in the test env, otherwise
        // the precedence is different from what we're asserting. Skip-if
        // rather than mutate the environment (edition 2024 env::set_var is
        // unsafe and workspace forbids unsafe_code).
        if std::env::var_os("DECDN_ADMIN_URL").is_some() {
            return;
        }
        assert_eq!(resolve_admin_url(None), DEFAULT_ADMIN_URL);
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
