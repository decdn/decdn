//! Reusable `cdn/probe/v1` client with QUIC 0-RTT (ADR 015), node-side copy.
//!
//! [`probe_once`] performs one probe round trip against a remote node,
//! attempting 0-RTT early data when the endpoint already holds a TLS session
//! ticket for the peer and falling back transparently to 1-RTT otherwise. The
//! node's cache-miss provider-discovery loop ([`crate::node_origin`], ADR
//! 001/022) probes each candidate through this to measure `rate_per_mb` + RTT
//! before [`crate::selection::rank_candidates`] picks one.
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
use iroh::endpoint::{ConnectOptions, Connection, RecvStream, SendStream, ZeroRttStatus};
use iroh::{Endpoint, EndpointAddr};

/// Sink for the ADR 015 §Observability 0-RTT counters. Callers that have no
/// metrics registry pass `None`; the node probe loop passes an implementation
/// that forwards into the `decdn_quic_0rtt_*` counters.
///
/// Only the client-observable transitions live here. The
/// `quic_session_ticket_cache_size` gauge is server-side state and is wired
/// separately in the probe handler, not through this trait.
pub trait ProbeMetrics: Send + Sync {
    /// A cached ticket existed and early data was sent.
    fn record_0rtt_attempt(&self);
    /// The server accepted the early data.
    fn record_0rtt_accepted(&self);
    /// The server rejected 0-RTT; the client fell back to 1-RTT.
    fn record_0rtt_rejected(&self);
}

/// Bounds on the post-exchange linger (ADR 015 §Session Ticket Management):
/// wait long enough for the server's `NewSessionTicket` to arrive and be
/// ingested by rustls, but never block a probe for long.
const LINGER_MIN: Duration = Duration::from_millis(100);
const LINGER_MAX: Duration = Duration::from_secs(2);

/// Run one probe round trip and return the decoded response together with the
/// measured round-trip time in milliseconds.
///
/// `enable_0rtt` is the ADR 015 master switch (`network.enable_0rtt`): when
/// `false`, a plain 1-RTT handshake is always used and no 0-RTT counters move.
/// When `true`, 0-RTT early data is attempted whenever `endpoint` already holds
/// a session ticket for `target`; a server rejection falls back to 1-RTT and
/// re-sends on a confirmed stream (ADR 015 §0-RTT Rejection Handling) — never
/// surfaced as an error.
///
/// `timeout` bounds the connect+request+response exchange. The best-effort
/// ticket linger that follows is bounded independently (≤ `LINGER_MAX`) so
/// capturing the ticket can't starve, nor inflate, the caller's deadline.
///
/// The decoded [`ProbeResponse`] is returned without echoed-field correlation
/// or `slash_sig` validation: those requester-side obligations (ADR 005 / ADR
/// 014 §1) are the caller's. The only check performed internally is the
/// protocol-level one that the server sent a `Response` (not a `Request`)
/// variant.
#[allow(clippy::too_many_arguments)] // transport knobs; each arg is distinct.
pub async fn probe_once(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hash: [u8; 32],
    timestamp_us: u64,
    enable_0rtt: bool,
    metrics: Option<&dyn ProbeMetrics>,
    timeout: Duration,
) -> anyhow::Result<(ProbeResponse, f64)> {
    let started = Instant::now();

    let (conn, resp) = tokio::time::timeout(timeout, async {
        let connecting = endpoint
            .connect_with_opts(target, ALPN_PROBE, ConnectOptions::new())
            .await
            .map_err(|e| anyhow::anyhow!("connect failed: {e}"))?;

        if !enable_0rtt {
            // Master switch off (or caller never wants early data): take the
            // plain full-handshake path. Equivalent to `endpoint.connect(...)`.
            let conn = connecting
                .await
                .map_err(|e| anyhow::anyhow!("handshake failed: {e}"))?;
            let resp = exchange(&conn, hash, timestamp_us).await?;
            return Ok::<_, anyhow::Error>((conn, resp));
        }

        match connecting.into_0rtt() {
            // Cold: no cached ticket, 0-RTT not even attempted. Resolve the
            // normal 1-RTT connection. No attempt counter — nothing attempted.
            Err(connecting) => {
                let conn = connecting
                    .await
                    .map_err(|e| anyhow::anyhow!("handshake failed: {e}"))?;
                let resp = exchange(&conn, hash, timestamp_us).await?;
                Ok((conn, resp))
            }
            // Warm: a ticket exists. Send the request as early data, then learn
            // from `handshake_completed` whether the server took it.
            Ok(zrtt) => {
                if let Some(m) = metrics {
                    m.record_0rtt_attempt();
                }
                let (mut send, recv) = zrtt
                    .open_bi()
                    .await
                    .map_err(|e| anyhow::anyhow!("0-RTT open_bi failed: {e}"))?;
                write_request(&mut send, hash, timestamp_us).await?;

                match zrtt
                    .handshake_completed()
                    .await
                    .map_err(|e| anyhow::anyhow!("0-RTT handshake failed: {e}"))?
                {
                    ZeroRttStatus::Accepted(conn) => {
                        if let Some(m) = metrics {
                            m.record_0rtt_accepted();
                        }
                        let resp = read_response(recv).await?;
                        Ok((conn, resp))
                    }
                    ZeroRttStatus::Rejected(conn) => {
                        // The early stream's data was discarded by the server;
                        // re-send on a confirmed post-handshake stream.
                        if let Some(m) = metrics {
                            m.record_0rtt_rejected();
                        }
                        let resp = exchange(&conn, hash, timestamp_us).await?;
                        Ok((conn, resp))
                    }
                }
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("probe timed out after {} ms", timeout.as_millis()))??;

    let rtt = started.elapsed();
    let rtt_ms = rtt.as_secs_f64() * 1000.0;

    // ADR 015 §Session Ticket Management: linger up to 2x the measured RTT
    // (clamped) so the server's NewSessionTicket reaches us and rustls caches
    // it for the next attempt. The timeout elapsing is the expected case.
    let linger = (rtt * 2).clamp(LINGER_MIN, LINGER_MAX);
    let _ = tokio::time::timeout(linger, conn.closed()).await;
    conn.close(0u32.into(), b"probe-done");

    Ok((resp, rtt_ms))
}

/// Open a fresh bidirectional stream, send the request, read the response.
/// Used for the 1-RTT path and the 0-RTT-rejected fallback.
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
