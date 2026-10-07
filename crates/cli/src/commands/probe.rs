//! `decdn probe` — send a `cdn/probe/v1` content-availability query to a
//! running node and print the signed response.

use decdn_common::cli;

use decdn_client::probe::probe_once;

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
/// The transport path lives in [`probe_once`]; response correlation and the
/// mandatory `slash_sig` check (ADR 014 §1) are applied here on the returned
/// response.
pub async fn probe(
    args: &cli::ProbeArgs,
    config_path: Option<&std::path::Path>,
) -> anyhow::Result<()> {
    use std::str::FromStr;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use iroh::{EndpointAddr, PublicKey};

    use decdn_client::endpoint as client_endpoint;

    let node_id = PublicKey::from_str(&args.node_id)
        .map_err(|e| anyhow::anyhow!("invalid --node-id {:?}: {e}", args.node_id))?;
    let hash = parse_hash(&args.hash)?;

    // Relays come from `network.relay_urls` in config; `--relay-url` overrides
    // (#935). Discovery (#936): `[network.discovery]` composes operator
    // resolution legs, else `presets::N0`, so a node-id can be dialed without an
    // explicit `--addr` when discovery is configured.
    let relays = client_endpoint::resolve_relays(args.relay_url.as_deref(), config_path)?;
    let discovery = client_endpoint::client_discovery(config_path)?;

    // No reachability pre-check: the endpoint is discovery-enabled, so a node-id
    // resolves via `[network.discovery]` / `presets::N0` (plus its default
    // relays) even without `--addr` or configured relays. A direct `--addr`
    // still pins the address when given.
    let endpoint = client_endpoint::client_endpoint(&relays, &discovery).await?;

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

    // Transport and RTT measurement live in `probe_once`.
    let result = probe_once(&endpoint, target, hash, timestamp_us, timeout).await;
    endpoint.close().await;
    let (resp, resp_ext, rtt) = result?;

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
    write_probe_response(&mut stdout, &resp, &resp_ext, rtt.ms, args.json)
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
    resp_ext: &decdn_protocol::ProbeResponseExt,
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
            "total_bytes": resp_ext.total_bytes,
            "timestamp_us": resp.body.timestamp_us,
            "rtt_ms": rtt_ms_quantized,
            "slash_sig": slash_sig_hex,
        });
        writeln!(w, "{output}")
    } else {
        writeln!(w, "hash:          {hash_hex}")?;
        writeln!(w, "has_blob:      {}", resp.body.has_blob)?;
        writeln!(w, "rate_per_mb:   {} (base units)", resp.body.rate_per_mb)?;
        match resp_ext.total_bytes {
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
mod tests;
