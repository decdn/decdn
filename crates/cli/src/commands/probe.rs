//! `decdn probe` — send a `cdn/probe/v1` request to a running node and
//! print the response.

use decdn_common::cli;

/// Send a `cdn/probe/v1` request to a running node and print the response.
pub async fn probe(args: &cli::ProbeArgs) -> anyhow::Result<()> {
    use std::str::FromStr;
    use std::time::{Duration, Instant};

    use decdn_protocol::{
        ALPN_PROBE, ProbeMessage, decode_message, encode_message,
        message::{ProbeRequest, ProbeResponse},
        read_frame, write_frame,
    };
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

    let started = Instant::now();
    let result = tokio::time::timeout(timeout, async {
        let conn = endpoint
            .connect(target, ALPN_PROBE)
            .await
            .map_err(|e| anyhow::anyhow!("connect failed: {e}"))?;

        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi failed: {e}"))?;

        let payload = encode_message(&ProbeMessage::Request(ProbeRequest { nonce }))
            .map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
        write_frame(&mut send, &payload)
            .await
            .map_err(|e| anyhow::anyhow!("write request: {e}"))?;
        send.finish()
            .map_err(|e| anyhow::anyhow!("finish stream: {e}"))?;

        let frame = read_frame(&mut recv)
            .await
            .map_err(|e| anyhow::anyhow!("read response: {e}"))?;
        let (msg, _rest) = decode_message::<ProbeMessage>(&frame)
            .map_err(|e| anyhow::anyhow!("decode response: {e}"))?;
        let resp: ProbeResponse = match msg {
            ProbeMessage::Response(r) => r,
            ProbeMessage::Request(_) => {
                anyhow::bail!("unexpected ProbeMessage::Request from server");
            }
        };

        conn.close(0u32.into(), b"probe-done");
        Ok::<_, anyhow::Error>(resp)
    })
    .await;

    let rtt_ms = started.elapsed().as_secs_f64() * 1000.0;
    endpoint.close().await;

    let resp = match result {
        Ok(inner) => inner?,
        Err(_) => anyhow::bail!("probe timed out after {} ms", args.timeout_ms),
    };

    if resp.nonce != nonce {
        anyhow::bail!(
            "nonce mismatch: sent 0x{nonce:016x}, received 0x{:016x}",
            resp.nonce
        );
    }

    print_probe_response(&resp, rtt_ms, nonce, args.json);
    Ok(())
}

/// Render a successful probe response to stdout in pretty or JSON form.
fn print_probe_response(
    resp: &decdn_protocol::message::ProbeResponse,
    rtt_ms: f64,
    nonce: u64,
    json: bool,
) {
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
        // Quantize `rtt_ms` to 3 decimals before serialization so the
        // emitted JSON keeps the same precision the manual format
        // (`{:.3}`) used to produce — preserves the wire contract for
        // downstream `--json` consumers. `as_secs_f64()` carries
        // microsecond-resolution noise past 3 decimals anyway, so the
        // quantization isn't lossy in any meaningful sense.
        let rtt_ms_quantized = (rtt_ms * 1000.0).round() / 1000.0;
        let output = serde_json::json!({
            "node_id": node_id,
            "rate_per_mb": resp.rate_per_mb,
            "measured_at_unix_ms": resp.measured_at_unix_ms,
            "rtt_ms": rtt_ms_quantized,
            "nonce": format!("0x{nonce:016x}"),
        });
        println!("{output}");
    } else {
        println!("node_id:       {node_id}");
        println!("rate_per_mb:   {} (base units)", resp.rate_per_mb);
        println!("measured_at:   {} (unix ms)", resp.measured_at_unix_ms);
        println!("rtt:           {rtt_ms:.3} ms");
        println!("nonce:         0x{nonce:016x} (echoed ok)");
    }
}
