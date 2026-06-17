//! `decdn probe` — send a `cdn/probe/v1` content-availability query to a
//! running node and print the signed response.

use decdn_common::cli;

use super::probe_client::probe_once;

/// Parse a user-supplied BLAKE3 hash (64 hex chars, optional `0x` prefix —
/// the same form `cache.pinned_hashes` accepts) into raw bytes.
fn parse_hash(s: &str) -> anyhow::Result<[u8; 32]> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    let h = blake3::Hash::from_hex(hex).map_err(|e| {
        anyhow::anyhow!("invalid --hash {s:?}: expected 64 hex chars (BLAKE3 digest): {e}")
    })?;
    Ok(*h.as_bytes())
}

/// Send a `cdn/probe/v1` content-availability request to a running node and
/// print the signed response.
///
/// The transport path (0-RTT attempt + 1-RTT fallback, ADR 015) lives in
/// [`probe_once`]; response correlation and the mandatory `slash_sig` check
/// (ADR 014 §1) are applied here on the returned response. 0-RTT is always
/// attempted (`enable_0rtt = true`): a probe is read-only and idempotent, so
/// early data is safe, and there is no per-invocation reason to disable it. A
/// one-shot `decdn probe` starts cold — iroh's session cache is per-endpoint
/// and this process builds a fresh endpoint — so the first (and only)
/// attempt resolves 1-RTT; the machinery exists for the node's long-lived
/// reuse, not the CLI's.
pub async fn probe(
    args: &cli::ProbeArgs,
    config_path: Option<&std::path::Path>,
) -> anyhow::Result<()> {
    use std::str::FromStr;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use iroh::endpoint::presets;
    use iroh::{Endpoint, EndpointAddr, PublicKey};

    use decdn_common::identity::fresh_secret_key;

    use super::client_endpoint;

    let node_id = PublicKey::from_str(&args.node_id)
        .map_err(|e| anyhow::anyhow!("invalid --node-id {:?}: {e}", args.node_id))?;
    let hash = parse_hash(&args.hash)?;

    // Relays come from `network.relay_urls` in config; `--relay-url` overrides (#935).
    let relays = client_endpoint::resolve_relays(args.relay_url.as_deref(), config_path)?;
    if args.addr.is_none() && relays.is_empty() {
        anyhow::bail!(
            "no way to reach the node: pass --addr, or set network.relay_urls in config \
             (or --relay-url)"
        );
    }

    // Bind to an unspecified IPv4 address in both cases. Loopback-only binding
    // prevents the probe client from reaching a non-loopback `--addr`, which
    // is the whole point of the subcommand. `0.0.0.0:0` lets the OS pick an
    // ephemeral port on any interface; we're a client, nothing listens here.
    let bind_addr = std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0);
    let relay_mode = client_endpoint::relay_mode(&relays);

    let client_sk = fresh_secret_key();
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(client_sk)
        .relay_mode(relay_mode)
        // ADR 015 §Session Ticket Management: shared ticket-cache size
        // (iroh's default is 256). Harmless for the one-shot CLI; matches
        // the node's endpoint so the mechanism is identical.
        .max_tls_tickets(decdn_protocol::SESSION_TICKET_CACHE_SIZE)
        .bind_addr(bind_addr)
        .map_err(|e| anyhow::anyhow!("invalid bind addr {bind_addr}: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("endpoint bind failed: {e}"))?;

    let mut target = EndpointAddr::new(node_id);
    if let Some(addr) = args.addr {
        target = target.with_ip_addr(addr);
    }
    // Attach a relay hint so a node dialed without --addr is reachable via relay.
    if let Some(url) = relays.first() {
        target = target.with_relay_url(url.clone());
    }

    let timeout = Duration::from_millis(args.timeout_ms);
    // ADR 005: `timestamp_us` is the requester-generated microsecond
    // timestamp echoed back; it serves response correlation. RTT is measured
    // from the local monotonic clock (`Instant`), which is strictly better
    // than `receive_time - timestamp_us` for a single-host CLI and avoids
    // cross-host clock-skew artefacts.
    let timestamp_us = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros()),
    )
    .unwrap_or(u64::MAX);

    // Transport (0-RTT attempt + 1-RTT fallback, ADR 015) and RTT
    // measurement live in `probe_once`. The CLI passes no `ProbeMetrics`
    // sink — the one-shot `decdn` binary has no metrics registry.
    let result = probe_once(&endpoint, target, hash, timestamp_us, true, None, timeout).await;
    endpoint.close().await;
    let (resp, rtt_ms) = result?;

    // Correlation: the node echoes both the queried hash and the
    // requester timestamp (ADR 005). A mismatch means a stale/confused
    // response — reject it.
    if resp.body.timestamp_us != timestamp_us {
        anyhow::bail!(
            "timestamp mismatch: sent {timestamp_us}, received {}",
            resp.body.timestamp_us
        );
    }
    if resp.body.hash != hash {
        anyhow::bail!("hash mismatch: response is for a different blob");
    }

    // ADR 014 §1 / #252: `slash_sig` is mandatory and non-empty, and a
    // `rate_per_mb` of 0 MUST be rejected on receive. Route through
    // `ProbeResponse::validate()` so these requester obligations share one
    // definition with the protocol layer rather than open-coded checks that
    // can drift. The two live obligations on this path are the `slash_sig`
    // length and the `rate_per_mb != 0` rule (#252) — the `MAX_RATE_PER_MB`
    // upper bound is already enforced at decode time by `deserialize_rate_per_mb`,
    // but zero is a valid wire value the decoder accepts and the requester must
    // not. Full attribution (recover signer, confirm NodeId↔address via
    // `CapacityBond`) is the on-chain `SlashJudge`'s job — the CLI has no
    // registry client, so it enforces presence/shape only.
    resp.validate()
        .map_err(|e| anyhow::anyhow!("rejecting probe response: {e}"))?;

    let mut stdout = std::io::stdout().lock();
    write_probe_response(&mut stdout, &resp, rtt_ms, args.json)
        .map_err(|e| anyhow::anyhow!("failed to write probe response: {e}"))?;
    Ok(())
}

/// Render a successful probe response to `w` in pretty or JSON form.
///
/// Public-in-crate so unit tests can capture the output into a buffer and
/// assert on the wire contract — in particular the `--json` shape (key set,
/// `rtt_ms` quantization, `slash_sig` hex encoding).
pub(crate) fn write_probe_response(
    w: &mut impl std::io::Write,
    resp: &decdn_protocol::message::ProbeResponse,
    rtt_ms: f64,
    json: bool,
) -> std::io::Result<()> {
    use std::fmt::Write as _;

    let hash_hex = {
        let mut s = String::with_capacity(64);
        for b in resp.body.hash {
            let _ = write!(s, "{b:02x}");
        }
        s
    };
    let slash_sig_hex = {
        let mut s = String::with_capacity(resp.slash_sig.len() * 2);
        for b in &resp.slash_sig {
            let _ = write!(s, "{b:02x}");
        }
        s
    };

    if json {
        // Quantize `rtt_ms` to ms precision before serialization (see the
        // CHANGELOG BREAKING note — the #421 manual `{:.3}` format is gone;
        // serde_json drops insignificant trailing zeros).
        let rtt_ms_quantized = (rtt_ms * 1000.0).round() / 1000.0;
        let output = serde_json::json!({
            "hash": hash_hex,
            "has_blob": resp.body.has_blob,
            "rate_per_mb": resp.body.rate_per_mb,
            "total_bytes": resp.total_bytes,
            "timestamp_us": resp.body.timestamp_us,
            "rtt_ms": rtt_ms_quantized,
            "slash_sig": slash_sig_hex,
        });
        writeln!(w, "{output}")
    } else {
        writeln!(w, "hash:          {hash_hex}")?;
        writeln!(w, "has_blob:      {}", resp.body.has_blob)?;
        writeln!(w, "rate_per_mb:   {} (base units)", resp.body.rate_per_mb)?;
        match resp.total_bytes {
            Some(n) => writeln!(w, "total_bytes:   {n}")?,
            None => writeln!(w, "total_bytes:   (unknown)")?,
        }
        writeln!(w, "timestamp_us:  {} (echoed ok)", resp.body.timestamp_us)?;
        writeln!(w, "rtt:           {rtt_ms:.3} ms")?;
        writeln!(w, "slash_sig:     {slash_sig_hex}")
    }
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
    use decdn_protocol::SLASH_SIG_LEN;
    use decdn_protocol::message::{ProbeResponse, ProbeResponseBody};

    fn fixture(rate_per_mb: u64, has_blob: bool, total_bytes: Option<u64>) -> ProbeResponse {
        ProbeResponse {
            body: ProbeResponseBody {
                hash: [0xAB; 32],
                has_blob,
                rate_per_mb,
                timestamp_us: 1_700_000_000_000_000,
            },
            total_bytes,
            slash_sig: vec![0xCD; SLASH_SIG_LEN],
        }
    }

    /// #252: the requester gate (`ProbeResponse::validate`, invoked on the
    /// receive path in `run_probe`) MUST reject a zero `rate_per_mb` — a
    /// zero-rate node trivially wins selection while earning nothing, so it is
    /// an obvious misconfiguration the client refuses. The upper `MAX_RATE_PER_MB`
    /// bound is enforced at decode time; zero is the requester-side obligation.
    #[test]
    fn validate_rejects_zero_rate_response() {
        let resp = fixture(0, true, Some(4096));
        let err = resp.validate().expect_err("zero rate must be rejected");
        assert!(
            matches!(err, decdn_protocol::MessageValidationError::RateIsZero),
            "expected RateIsZero, got {err:?}"
        );
    }

    /// JSON wire shape: every key the operator-facing `--json` contract
    /// guarantees. Catches a renamed key, a dropped field, or a type drift.
    #[test]
    fn json_output_keys_and_types() {
        let resp = fixture(10, true, Some(4096));
        let mut buf: Vec<u8> = Vec::new();
        write_probe_response(&mut buf, &resp, 12.5, true).unwrap();
        let text = std::str::from_utf8(&buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();

        assert!(parsed.is_object(), "top level must be an object");
        let obj = parsed.as_object().unwrap();
        assert_eq!(
            obj.len(),
            7,
            "seven keys: hash/has_blob/rate_per_mb/total_bytes/timestamp_us/rtt_ms/slash_sig"
        );
        assert_eq!(obj.get("hash").unwrap().as_str().map(str::len), Some(64));
        assert_eq!(obj.get("has_blob").unwrap().as_bool(), Some(true));
        assert_eq!(obj.get("rate_per_mb").unwrap().as_u64(), Some(10));
        assert_eq!(obj.get("total_bytes").unwrap().as_u64(), Some(4096));
        assert_eq!(
            obj.get("timestamp_us").unwrap().as_u64(),
            Some(1_700_000_000_000_000)
        );
        assert!(obj.get("rtt_ms").unwrap().is_number());
        assert_eq!(
            obj.get("slash_sig").unwrap().as_str().map(str::len),
            Some(SLASH_SIG_LEN * 2),
            "slash_sig is hex-encoded (2 chars/byte)"
        );
    }

    /// `total_bytes` serializes as JSON `null` when the node didn't include
    /// a size (ADR 005: optional field).
    #[test]
    fn json_total_bytes_null_when_absent() {
        let resp = fixture(10, false, None);
        let mut buf: Vec<u8> = Vec::new();
        write_probe_response(&mut buf, &resp, 1.0, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(parsed["total_bytes"].is_null());
        assert_eq!(parsed["has_blob"].as_bool(), Some(false));
    }

    /// `rtt_ms` is quantized to ms precision. `12.5009` rounds to `12.501`.
    #[test]
    fn json_rtt_ms_quantized_to_ms_precision() {
        let resp = fixture(10, true, None);
        let mut buf: Vec<u8> = Vec::new();
        write_probe_response(&mut buf, &resp, 12.500_9, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let rtt = parsed["rtt_ms"].as_f64().unwrap();
        assert!(
            (rtt - 12.501).abs() < 1e-9,
            "expected rtt_ms ≈ 12.501, got {rtt}"
        );
    }

    /// `--json` output is single-line — operator scripts pipe through `jq`
    /// without `-s`/slurp.
    #[test]
    fn json_output_is_single_line() {
        let resp = fixture(10, true, Some(1));
        let mut buf: Vec<u8> = Vec::new();
        write_probe_response(&mut buf, &resp, 1.0, true).unwrap();
        let text = std::str::from_utf8(&buf).unwrap();
        assert_eq!(
            text.lines().count(),
            1,
            "expected one line of JSON, got {text:?}"
        );
    }

    /// Pretty output emits seven labelled lines in a stable order — operator
    /// scripts grep for `has_blob:` / `rtt:` etc. without `--json`.
    #[test]
    fn pretty_output_emits_labelled_lines_in_stable_order() {
        let resp = fixture(7, true, Some(2048));
        let mut buf: Vec<u8> = Vec::new();
        write_probe_response(&mut buf, &resp, 1.234, false).unwrap();
        let text = std::str::from_utf8(&buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 7);
        assert!(lines[0].starts_with("hash:"));
        assert!(lines[1].starts_with("has_blob:"));
        assert!(lines[2].starts_with("rate_per_mb:"));
        assert!(lines[3].starts_with("total_bytes:"));
        assert!(lines[4].starts_with("timestamp_us:"));
        assert!(lines[5].starts_with("rtt:"));
        assert!(lines[6].starts_with("slash_sig:"));
        assert!(
            lines[5].contains("1.234 ms"),
            "pretty rtt: line keeps 3-decimal format, got {:?}",
            lines[5],
        );
    }

    #[test]
    fn parse_hash_accepts_hex_with_and_without_prefix() {
        let hex = "ab".repeat(32);
        let a = parse_hash(&hex).unwrap();
        let b = parse_hash(&format!("0x{hex}")).unwrap();
        assert_eq!(a, b);
        assert_eq!(a, [0xABu8; 32]);
    }

    #[test]
    fn parse_hash_rejects_wrong_length() {
        assert!(parse_hash("abc").is_err());
        assert!(parse_hash(&"ab".repeat(33)).is_err());
    }
}
