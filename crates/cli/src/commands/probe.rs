//! `decdn probe` — send a `cdn/probe/v1` request to a running node and
//! print the response.

use decdn_common::cli;

use super::probe_client::probe_once;

/// Send a `cdn/probe/v1` request to a running node and print the response.
///
/// The transport path (0-RTT attempt + 1-RTT fallback, ADR 015) lives in
/// [`probe_once`]. 0-RTT is always
/// attempted here (`enable_0rtt = true`): a probe is read-only and
/// idempotent, so early data is safe, and there is no per-invocation
/// reason to disable it. A one-shot `decdn probe` starts cold — iroh's
/// session cache is per-endpoint and this process builds a fresh
/// endpoint — so the first (and only) attempt resolves 1-RTT; the
/// machinery exists for the node's long-lived reuse, not the CLI's.
pub async fn probe(args: &cli::ProbeArgs) -> anyhow::Result<()> {
    use std::str::FromStr;
    use std::time::Duration;

    use iroh::endpoint::presets;
    use iroh::{Endpoint, EndpointAddr, PublicKey, RelayMap, RelayMode, RelayUrl};
    use rand::Rng;

    use decdn_common::identity::fresh_secret_key;

    let node_id = PublicKey::from_str(&args.node_id)
        .map_err(|e| anyhow::anyhow!("invalid --node-id {:?}: {e}", args.node_id))?;

    let relay_url = match args.relay_url.as_deref() {
        Some(s) => Some(
            RelayUrl::from_str(s).map_err(|e| anyhow::anyhow!("invalid --relay-url {s:?}: {e}"))?,
        ),
        None => None,
    };

    // Bind to an unspecified IPv4 address in both cases. Loopback-only binding
    // prevents the probe client from reaching a non-loopback `--addr`, which
    // is the whole point of the subcommand. `0.0.0.0:0` lets the OS pick an
    // ephemeral port on any interface; we're a client, nothing listens here.
    let bind_addr = std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0);
    let relay_mode = match relay_url.clone() {
        Some(url) => RelayMode::Custom(RelayMap::from_iter([url])),
        None => RelayMode::Disabled,
    };

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
    if let Some(url) = relay_url {
        target = target.with_relay_url(url);
    }

    let timeout = Duration::from_millis(args.timeout_ms);
    let nonce: u64 = rand::rng().next_u64();

    let result = probe_once(&endpoint, target, nonce, true, None, timeout).await;
    endpoint.close().await;
    let (resp, rtt_ms) = result?;

    let mut stdout = std::io::stdout().lock();
    write_probe_response(&mut stdout, &resp, rtt_ms, nonce, args.json)
        .map_err(|e| anyhow::anyhow!("failed to write probe response: {e}"))?;
    Ok(())
}

/// Render a successful probe response to `w` in pretty or JSON form.
///
/// Public-in-crate so unit tests can capture the output into a buffer
/// and assert on the wire contract — in particular the `--json` shape
/// (key set, `rtt_ms` quantization, `nonce` hex padding).
pub(crate) fn write_probe_response(
    w: &mut impl std::io::Write,
    resp: &decdn_protocol::message::ProbeResponse,
    rtt_ms: f64,
    nonce: u64,
    json: bool,
) -> std::io::Result<()> {
    // Format as the canonical iroh node-id string (z-base-32 via PublicKey's
    // Display impl) — matches what the server logs on startup. Fall back to
    // raw hex if the key fails to parse; the fallback stays alphanumeric so
    // downstream `--json` consumers never see non-conforming output.
    let node_id = iroh::PublicKey::from_bytes(&resp.node_id).map_or_else(
        |_| {
            use std::fmt::Write as _;
            let mut s = String::with_capacity(2 + 64);
            s.push_str("0x");
            for b in resp.node_id {
                let _ = write!(s, "{b:02x}");
            }
            s
        },
        |pk| pk.to_string(),
    );
    if json {
        // Quantize `rtt_ms` to ms precision before serialization. The
        // pre-#421 manual format used `{:.3}` (always 3 decimal digits,
        // e.g. `12.500`); serde_json drops insignificant trailing zeros,
        // so a probe at exactly 12.5 ms now emits `"rtt_ms":12.5` (was
        // `"rtt_ms":12.500`). Numerically identical to any JSON parser;
        // operator scripts that match a `\.\d{3}` regex will need
        // `\.\d+`. Called out under the BREAKING note in CHANGELOG.
        // `as_secs_f64()` carries microsecond noise past 3 decimals, so
        // the quantization itself isn't lossy in any meaningful sense.
        let rtt_ms_quantized = (rtt_ms * 1000.0).round() / 1000.0;
        let output = serde_json::json!({
            "node_id": node_id,
            "rate_per_mb": resp.rate_per_mb,
            "measured_at_unix_ms": resp.measured_at_unix_ms,
            "rtt_ms": rtt_ms_quantized,
            "nonce": format!("0x{nonce:016x}"),
        });
        writeln!(w, "{output}")
    } else {
        writeln!(w, "node_id:       {node_id}")?;
        writeln!(w, "rate_per_mb:   {} (base units)", resp.rate_per_mb)?;
        writeln!(w, "measured_at:   {} (unix ms)", resp.measured_at_unix_ms)?;
        writeln!(w, "rtt:           {rtt_ms:.3} ms")?;
        writeln!(w, "nonce:         0x{nonce:016x} (echoed ok)")
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
    use decdn_protocol::message::ProbeResponse;

    fn fixture(rate_per_mb: u64, measured_at_unix_ms: u64) -> ProbeResponse {
        // Iroh PublicKey::from_bytes accepts any 32-byte slice, so the
        // happy-path branch ("canonical z-base-32") fires for this fixture.
        ProbeResponse {
            node_id: [0xAB; 32],
            rate_per_mb,
            measured_at_unix_ms,
            nonce: 0,
        }
    }

    /// JSON wire shape: every key the operator-facing `--json` contract
    /// guarantees. Catches a renamed key, a dropped field, or a type drift
    /// (e.g. `rate_per_mb` becoming a string).
    #[test]
    fn json_output_keys_and_types() {
        let resp = fixture(10, 1_700_000_000_000);
        let mut buf: Vec<u8> = Vec::new();
        write_probe_response(&mut buf, &resp, 12.5, 0xDEAD_BEEF_CAFE_F00D, true).unwrap();
        let text = std::str::from_utf8(&buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();

        assert!(parsed.is_object(), "top level must be an object");
        let obj = parsed.as_object().unwrap();
        assert_eq!(
            obj.len(),
            5,
            "five keys: node_id/rate_per_mb/measured_at_unix_ms/rtt_ms/nonce"
        );
        assert!(obj.get("node_id").unwrap().is_string());
        assert_eq!(obj.get("rate_per_mb").unwrap().as_u64(), Some(10));
        assert_eq!(
            obj.get("measured_at_unix_ms").unwrap().as_u64(),
            Some(1_700_000_000_000)
        );
        assert!(obj.get("rtt_ms").unwrap().is_number());
        assert_eq!(
            obj.get("nonce").unwrap().as_str(),
            Some("0xdeadbeefcafef00d"),
            "nonce is hex-encoded with 0x prefix and 16 padded digits"
        );
    }

    /// `rtt_ms` is quantized to ms precision. `12.5009` rounds to `12.501`.
    /// Pinning the exact rounding so the published behaviour can't drift
    /// (e.g. someone "improving" precision past ms would silently expand
    /// the rendered float-tail across operator dashboards).
    #[test]
    fn json_rtt_ms_quantized_to_ms_precision() {
        let resp = fixture(10, 0);
        let mut buf: Vec<u8> = Vec::new();
        write_probe_response(&mut buf, &resp, 12.500_9, 0, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        // 12.5009 * 1000 = 12500.9, round = 12501.0, / 1000 = 12.501.
        let rtt = parsed["rtt_ms"].as_f64().unwrap();
        assert!(
            (rtt - 12.501).abs() < 1e-9,
            "expected rtt_ms ≈ 12.501, got {rtt}"
        );
    }

    /// `--json` output is single-line — operator scripts pipe through `jq`
    /// without `-s`/slurp, and grep-friendly per-line tooling stays simple.
    #[test]
    fn json_output_is_single_line() {
        let resp = fixture(10, 0);
        let mut buf: Vec<u8> = Vec::new();
        write_probe_response(&mut buf, &resp, 1.0, 0, true).unwrap();
        let text = std::str::from_utf8(&buf).unwrap();
        assert_eq!(
            text.lines().count(),
            1,
            "expected one line of JSON, got {text:?}"
        );
    }

    /// Pretty output emits five labelled lines in a stable order — operator
    /// scripts grep for `node_id:` / `rtt:` etc. without `--json`.
    #[test]
    fn pretty_output_emits_five_labelled_lines_in_stable_order() {
        let resp = fixture(7, 1_700_000_000_000);
        let mut buf: Vec<u8> = Vec::new();
        write_probe_response(&mut buf, &resp, 1.234, 0xABCD, false).unwrap();
        let text = std::str::from_utf8(&buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 5);
        assert!(lines[0].starts_with("node_id:"));
        assert!(lines[1].starts_with("rate_per_mb:"));
        assert!(lines[2].starts_with("measured_at:"));
        assert!(lines[3].starts_with("rtt:"));
        assert!(lines[4].starts_with("nonce:"));
        // rtt: line keeps the `{:.3}` format on the pretty path.
        assert!(
            lines[3].contains("1.234 ms"),
            "pretty rtt: line should keep 3-decimal format, got {:?}",
            lines[3],
        );
    }
}
