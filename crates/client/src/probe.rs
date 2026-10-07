//! Reusable `cdn/probe/v1` client.
//!
//! [`probe_once`](crate::probe::probe_once) performs one probe round trip against a remote node
//! over a plain QUIC handshake. The transport lives here rather than inline
//! in the `decdn probe` command so the node's cache-miss probe-collection loop
//! (DHT `FIND_VALUE` → parallel probes, [ADR 001]) can reuse it; that loop
//! runs in `decdn_node::node_origin`. The reused mechanism is
//! **transport-only**:
//! echoed-field correlation (ADR 005) and `slash_sig` validation (ADR 014
//! §1) are the caller's responsibility, not performed here — see
//! [`probe_once`](crate::probe::probe_once).
//!
//! [ADR 001]: ../../../adr/001-network.md

use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, Signature};
use alloy::sol_types::Eip712Domain;
use decdn_incentive::ProbeSlashData;
use decdn_protocol::{
    ALPN_PROBE, ProbeMessage, ProbeResponseExt, decode_message, encode_message,
    message::{ProbeRequest, ProbeResponse},
    parse_probe_response_ext, read_frame, write_frame,
};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};

use crate::rate_limited::transport_error;

/// The round-trip time one probe measured.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProbeRtt {
    /// The round-trip time in milliseconds.
    pub ms: f64,
    /// Whether hole punching selected a direct path before the probe ended.
    /// When `true`, `ms` is no higher than that path's RTT. When `false`, `ms`
    /// is the exchange RTT, and the exchange can have ridden the relay, so
    /// `ms` says nothing about how far away the peer is.
    pub direct: bool,
}

/// Run one probe round trip and return the decoded response together with
/// the measured round-trip time.
///
/// The round-trip time is the smaller of the request/response exchange on the
/// established connection and the estimate of the direct path hole punching
/// selects after it. It never covers the connect:
/// a cold dial pays handshake, relay, and hole-punch time that says nothing
/// about the path the paid stream then uses, so timing it would rank a warm
/// peer far ahead of an equally near cold one.
///
/// `timeout` bounds the connect and the exchange. The wait for a direct path
/// gets only what is left of `timeout`, capped at 500 ms, and it never fails a
/// probe that already has its answer: when the wait runs out, the probe keeps
/// the exchange RTT and reports [`ProbeRtt::direct`] as `false`. A cold dial
/// that rides the relay therefore still returns its answer to a caller with a
/// tight budget, such as the node's upstream probe round.
///
/// The decoded [`ProbeResponse`] is returned without echoed-field
/// correlation or `slash_sig` validation: the requester-side obligations —
/// echoed `hash`/`timestamp_us` correlation (ADR 005) and the mandatory
/// `slash_sig` shape check (`ProbeResponse::validate`, ADR 014 §1) — are the
/// caller's, so the node's probe-collection loop and the CLI each apply their
/// own policy on the same transport. The only check performed internally is the
/// protocol-level one that the server sent a `Response` (not a `Request`)
/// variant.
///
/// # Errors
///
/// A node that sheds the probe at the transport with `APP_ERR_RATE_LIMITED`
/// surfaces as the [`UpstreamRateLimited`](crate::UpstreamRateLimited) sentinel
/// (recoverable with `downcast_ref` through any added context), so a caller can
/// tell a peer refusing work from a peer that could not be reached. Every other
/// failure is a plain message naming the stage.
pub async fn probe_once(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hash: [u8; 32],
    timestamp_us: u64,
    timeout: Duration,
) -> anyhow::Result<(ProbeResponse, ProbeResponseExt, ProbeRtt)> {
    let (conn, resp, rtt) = exchange_within(
        timeout,
        async {
            let conn = endpoint
                .connect(target, ALPN_PROBE)
                .await
                .map_err(|e| transport_error("connect failed", e))?;
            let started = Instant::now();
            let resp = exchange(&conn, hash, timestamp_us).await?;
            Ok((conn, resp, started.elapsed()))
        },
        async |conn: &Connection| direct_path_rtt(conn).await,
    )
    .await?;

    conn.close(0u32.into(), b"probe-done");

    Ok((
        resp.0,
        resp.1,
        ProbeRtt {
            ms: rtt.rtt.as_secs_f64() * 1000.0,
            direct: rtt.direct,
        },
    ))
}

/// The longest [`probe_once`] waits after its exchange for hole punching to
/// select a direct path. The probe's `timeout` can cut the wait shorter.
const DIRECT_PATH_GRACE: Duration = Duration::from_millis(500);

/// [`ProbeRtt`] before the conversion to milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PathRtt {
    /// The smaller of the exchange RTT and the direct-path RTT.
    rtt: Duration,
    /// Whether a direct path reported its RTT in time.
    direct: bool,
}

/// Run `exchange` within `budget`, then let `direct` report a direct-path RTT
/// for what is left of `budget`, at most [`DIRECT_PATH_GRACE`].
///
/// `exchange` yields the connection, the response, and the exchange RTT. Only
/// `exchange` can fail the probe. A `direct` that does not answer in time
/// leaves the exchange RTT in place. A direct-path RTT replaces the exchange
/// RTT only when it is lower.
async fn exchange_within<C, R>(
    budget: Duration,
    exchange: impl Future<Output = anyhow::Result<(C, R, Duration)>>,
    direct: impl AsyncFnOnce(&C) -> Option<Duration>,
) -> anyhow::Result<(C, R, PathRtt)> {
    let deadline = tokio::time::Instant::now() + budget;
    let (conn, resp, exchanged) = tokio::time::timeout_at(deadline, exchange)
        .await
        .map_err(|_| anyhow::anyhow!("probe timed out after {} ms", budget.as_millis()))??;
    let grace = deadline
        .saturating_duration_since(tokio::time::Instant::now())
        .min(DIRECT_PATH_GRACE);
    let rtt = match tokio::time::timeout(grace, direct(&conn))
        .await
        .ok()
        .flatten()
    {
        Some(direct) => PathRtt {
            rtt: direct.min(exchanged),
            direct: true,
        },
        None => PathRtt {
            rtt: exchanged,
            direct: false,
        },
    };
    Ok((conn, resp, rtt))
}

/// The RTT estimate of the direct path `conn` selects, or `None` when the
/// paths stream ends without one. A first exchange rides the relay until hole
/// punching finishes, and the relay path says nothing about the direct path
/// the paid stream then uses. A path is selected only once validated, so its
/// estimate comes from real samples. The caller bounds the wait.
async fn direct_path_rtt(conn: &Connection) -> Option<Duration> {
    use futures_util::StreamExt as _;
    let mut snapshots = conn.paths_stream();
    while let Some(paths) = snapshots.next().await {
        if let Some(path) = paths.iter().find(|path| path.is_ip() && path.is_selected()) {
            return Some(path.rtt());
        }
    }
    None
}

/// Verify a [`ProbeResponse`] the way a requester must before acting on it
/// (ADR 005 §Correlation, ADR 014 §1).
///
/// Three checks, in the order a cheap one should precede an expensive one:
/// the value invariants (`ProbeResponse::validate` — non-zero rate, `slash_sig`
/// length), the echoed-field correlation that ties the answer to *this* request,
/// and finally the secp256k1 recovery of `slash_sig` to `expected_signer`.
///
/// The recovery is the load-bearing one and the reason this exists. `slash_sig`
/// is what makes a quoted rate non-repudiable: a node that quotes `R₁` on probe
/// and then charges `R₂ > R₁` within the 30-second window is slashable on the
/// strength of these two signed messages alone. A response whose signature is
/// never recovered is not evidence of anything — it is an unattributed claim, so
/// a node could win selection on a rate it never committed to and face nothing
/// when it fails to honour it.
///
/// Requester-local policy, not an attributable fault: a caller that gets an
/// `Err` here MUST drop the response and move on, exactly as it would for a
/// timeout, and MUST NOT feed it to reputation. The signature is the only thing
/// binding a response to a peer, so a failed verification cannot attribute
/// anything to that peer — an unverified message is precisely a message with no
/// established author.
///
/// # Errors
///
/// A failed invariant, a mismatched echoed `hash`/`timestamp_us`, an unparseable
/// signature, or a signature that recovers to any address but `expected_signer`.
pub fn verify_probe_response(
    resp: &ProbeResponse,
    expected_signer: Address,
    slash_domain: &Eip712Domain,
    hash: [u8; 32],
    timestamp_us: u64,
) -> anyhow::Result<()> {
    resp.validate()
        .map_err(|e| anyhow::anyhow!("invalid probe response: {e}"))?;
    if resp.body.hash != hash {
        anyhow::bail!("probe response hash does not match the request");
    }
    if resp.body.timestamp_us != timestamp_us {
        anyhow::bail!("probe response timestamp_us not echoed");
    }
    let sig = Signature::try_from(resp.slash_sig.as_slice())
        .map_err(|e| anyhow::anyhow!("slash_sig parse: {e}"))?;
    ProbeSlashData {
        hash: B256::from(resp.body.hash),
        has_blob: resp.body.has_blob,
        rate_per_mb: resp.body.rate_per_mb,
        timestamp_us: resp.body.timestamp_us,
    }
    .verify_signer(&sig, expected_signer, slash_domain)
    .map_err(|e| anyhow::anyhow!("probe slash_sig verification failed: {e}"))?;
    Ok(())
}

/// Open a fresh bidirectional stream, send the request, read the response.
async fn exchange(
    conn: &Connection,
    hash: [u8; 32],
    timestamp_us: u64,
) -> anyhow::Result<(ProbeResponse, ProbeResponseExt)> {
    let (mut send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| transport_error("open_bi failed", e))?;
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
        .map_err(|e| transport_error("write request", e))?;
    send.finish()
        .map_err(|e| anyhow::anyhow!("finish stream: {e}"))?;
    Ok(())
}

/// Read and decode one framed `ProbeMessage::Response`. Echoed-field
/// correlation (`hash`/`timestamp_us`) and the mandatory `slash_sig` shape
/// check are the caller's obligation (ADR 005 / ADR 014 §1), not enforced
/// here — see [`probe_once`].
async fn read_response(mut recv: RecvStream) -> anyhow::Result<(ProbeResponse, ProbeResponseExt)> {
    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| transport_error("read response", e))?;
    let (msg, rest) = decode_message::<ProbeMessage>(&frame)
        .map_err(|e| anyhow::anyhow!("decode response: {e}"))?;
    match msg {
        ProbeMessage::Response(r) => {
            // Trailing bytes are the Tier-1 extension, not junk: parse what this
            // build knows and skip the rest (ADR 013 §Tier 1).
            let ext = parse_probe_response_ext(rest)
                .map_err(|e| anyhow::anyhow!("decode response ext: {e}"))?;
            Ok((r, ext))
        }
        ProbeMessage::Request(_) => {
            anyhow::bail!("unexpected ProbeMessage::Request from server");
        }
    }
}

#[cfg(test)]
mod tests;
