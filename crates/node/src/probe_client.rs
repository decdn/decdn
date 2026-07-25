//! Reusable `cdn/probe/v1` client, node-side copy.
//!
//! [`probe_once`] performs one probe round trip against a remote node over a
//! plain QUIC handshake. The node's cache-miss provider-discovery loop
//! ([`crate::node_origin`], ADR 001/022) probes each candidate through this to
//! measure `rate_per_mb` + RTT before [`crate::selection::rank_candidates`]
//! picks one.
//!
//! **Duplication note (#831).** This is a verbatim copy of
//! `cli::commands::probe_client::probe_once`. The `node` crate cannot depend on
//! `cli`, and `protocol` is a minimal-deps leaf with no `iroh` connect surface,
//! so neither is a viable shared home; a future shared internal transport crate
//! should host the single copy. Keep the two in sync until then.
//!
//! The reused mechanism is **transport-only**: echoed-field correlation (ADR
//! 005) and `slash_sig` validation (ADR 014 §1) are the caller's
//! responsibility, not performed here — see [`probe_once`].

use std::time::{Duration, Instant};

use decdn_protocol::{
    ALPN_PROBE, ProbeMessage, decode_message, encode_message,
    message::{ProbeRequest, ProbeResponse},
    read_frame, write_frame,
};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};

/// Run one probe round trip and return the decoded response together with the
/// measured round-trip time in milliseconds.
///
/// `timeout` bounds the whole connect+request+response exchange.
///
/// The decoded [`ProbeResponse`] is returned without echoed-field correlation
/// or `slash_sig` validation: those requester-side obligations (ADR 005 / ADR
/// 014 §1) are the caller's. The only check performed internally is the
/// protocol-level one that the server sent a `Response` (not a `Request`)
/// variant.
pub async fn probe_once(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hash: [u8; 32],
    timestamp_us: u64,
    timeout: Duration,
) -> anyhow::Result<(ProbeResponse, f64)> {
    let started = Instant::now();

    let (conn, resp) = tokio::time::timeout(timeout, async {
        let conn = endpoint
            .connect(target, ALPN_PROBE)
            .await
            .map_err(|e| anyhow::anyhow!("connect failed: {e}"))?;
        let resp = exchange(&conn, hash, timestamp_us).await?;
        Ok::<_, anyhow::Error>((conn, resp))
    })
    .await
    .map_err(|_| anyhow::anyhow!("probe timed out after {} ms", timeout.as_millis()))??;

    let rtt_ms = started.elapsed().as_secs_f64() * 1000.0;
    conn.close(0u32.into(), b"probe-done");

    Ok((resp, rtt_ms))
}

/// Open a fresh bidirectional stream, send the request, read the response.
async fn exchange(
    conn: &Connection,
    hash: [u8; 32],
    timestamp_us: u64,
) -> anyhow::Result<ProbeResponse> {
    let (mut send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi failed: {e}"))?;
    write_request(&mut send, hash, timestamp_us).await?;
    read_response(recv).await
}

/// Frame and write a `ProbeMessage::Request`, then finish the send half.
async fn write_request(
    send: &mut SendStream,
    hash: [u8; 32],
    timestamp_us: u64,
) -> anyhow::Result<()> {
    let payload = encode_message(&ProbeMessage::Request(ProbeRequest { hash, timestamp_us }))
        .map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
    write_frame(send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write request: {e}"))?;
    send.finish()
        .map_err(|e| anyhow::anyhow!("finish stream: {e}"))?;
    Ok(())
}

/// Read and decode one framed `ProbeMessage::Response`. Echoed-field
/// correlation and the `slash_sig` shape check are the caller's obligation
/// (ADR 005 / ADR 014 §1), not enforced here — see [`probe_once`].
async fn read_response(mut recv: RecvStream) -> anyhow::Result<ProbeResponse> {
    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| anyhow::anyhow!("read response: {e}"))?;
    let (msg, _rest) = decode_message::<ProbeMessage>(&frame)
        .map_err(|e| anyhow::anyhow!("decode response: {e}"))?;
    match msg {
        ProbeMessage::Response(r) => Ok(r),
        ProbeMessage::Request(_) => {
            anyhow::bail!("unexpected ProbeMessage::Request from server");
        }
    }
}
