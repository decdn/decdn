//! Client-side `cdn/dht/v1` request primitives.
//!
//! The handler in [`crate::handlers::dht`] is the server half — it
//! receives `FindNode` / `FindValue` / `Store` requests and writes
//! responses. This module is the *client* half: outbound requests
//! issued by the bootstrap path (initial `FindNode(self.node_id)`),
//! the republish scheduler (one `Store` per K+3-closest peer per
//! cached blob), and (in a follow-up) the iterative requester-side
//! `FindValue` lookup.
//!
//! Each call:
//! 1. Connects to the peer's [`EndpointAddr`] with ALPN `cdn/dht/v1`.
//! 2. Opens a bi stream.
//! 3. Writes one framed [`wire::DhtMessage`] request.
//! 4. Reads one framed response, decodes, and returns it.
//! 5. Closes the connection with `0u32` (no app error).
//!
//! The signatures mirror the server-side message types: callers
//! provide the request fields, get back the response struct. Error
//! handling surfaces via `anyhow::Result`; ADR 013 application error
//! codes returned by the peer surface as `ConnectionError` /
//! `ReadError` variants with the appropriate `error_code`. Callers
//! that want the precise code use
//! [`extract_app_error_code`] on the returned error.

use std::time::Duration;

use anyhow::Context;
use decdn_protocol::{
    ALPN_DHT, FrameError, decode_message, dht as wire, encode_message, read_frame, write_frame,
};
use iroh::endpoint::{ConnectOptions, Connection, ConnectionError, ReadError, ReadToEndError};
use iroh::{Endpoint, EndpointAddr};

/// Per-request hard timeout for outbound DHT exchanges. Shorter than
/// the sum of the server-side accept-bi timeout (5s) and read
/// timeout (5s) in [`crate::handlers::dht`] so a hung peer doesn't
/// wedge a republish sweep.
pub const DHT_CLIENT_TIMEOUT: Duration = Duration::from_secs(8);

/// Maximum response size — same as the framing layer ceiling, but we
/// pass it explicitly to `read_to_end` so a misbehaving peer can't
/// stream an arbitrary amount of data on the response stream.
const MAX_RESPONSE_BYTES: usize = decdn_protocol::MAX_MESSAGE_SIZE as usize;

/// Send a single `FindNode` request to `target` and return the response.
///
/// Used by the bootstrap path (`FindNode(self.node_id)` to seed the
/// routing table) and by the bucket-refresh task
/// (`FindNode(random_id_in_bucket)`).
pub async fn find_node(
    endpoint: &Endpoint,
    target: EndpointAddr,
    target_id: [u8; 32],
    requester: [u8; 32],
) -> anyhow::Result<wire::FindNodeResponse> {
    let request = wire::DhtMessage::FindNode(wire::FindNodeRequest {
        target: target_id,
        requester,
    });
    let response = exchange(endpoint, target, &request).await?;
    match response {
        wire::DhtMessage::FindNodeResponse(r) => Ok(r),
        other => anyhow::bail!(
            "dht client: expected FindNodeResponse, got {} variant",
            variant_name(&other)
        ),
    }
}

/// Send a single `Store` request to `target` and return the ack.
///
/// Used by the republish scheduler — one call per K+3 closest peer per
/// cached blob, per ADR 022 §STORE Flow steps 1-2.
pub async fn store(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hash: [u8; 32],
    holder: [u8; 32],
) -> anyhow::Result<wire::StoreAck> {
    let request = wire::DhtMessage::Store(wire::StoreRequest { hash, holder });
    let response = exchange(endpoint, target, &request).await?;
    match response {
        wire::DhtMessage::StoreAck(a) => Ok(a),
        other => anyhow::bail!(
            "dht client: expected StoreAck, got {} variant",
            variant_name(&other)
        ),
    }
}

/// Send a single `FindValue` request to `target` and return the response.
///
/// Used by the requester-side iterative lookup (PR 5 of #320). Exposed
/// here so the same connect/encode/decode plumbing isn't duplicated
/// across the bootstrap / republish / lookup call sites.
pub async fn find_value(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hash: [u8; 32],
    requester: [u8; 32],
) -> anyhow::Result<wire::FindValueResponse> {
    let request = wire::DhtMessage::FindValue(wire::FindValueRequest { hash, requester });
    let response = exchange(endpoint, target, &request).await?;
    match response {
        wire::DhtMessage::FindValueResponse(r) => Ok(r),
        other => anyhow::bail!(
            "dht client: expected FindValueResponse, got {} variant",
            variant_name(&other)
        ),
    }
}

/// Inner: connect, `open_bi`, write request frame, read response
/// frame, decode. Wraps the whole sequence in a single
/// [`DHT_CLIENT_TIMEOUT`] so a hung peer can't block the caller's
/// scheduler indefinitely.
async fn exchange(
    endpoint: &Endpoint,
    target: EndpointAddr,
    request: &wire::DhtMessage,
) -> anyhow::Result<wire::DhtMessage> {
    let payload = encode_message(request).context("dht client: encode request")?;
    let response = tokio::time::timeout(DHT_CLIENT_TIMEOUT, async {
        let connecting = endpoint
            .connect_with_opts(target, ALPN_DHT, ConnectOptions::new())
            .await
            .map_err(|e| anyhow::anyhow!("dht client: connect failed: {e}"))?;
        let conn = connecting
            .await
            .map_err(|e| anyhow::anyhow!("dht client: handshake failed: {e}"))?;
        let resp = exchange_on(&conn, &payload).await?;
        // `0u32` = "no app error"; matches the server's normal-close
        // code so the peer's `conn.closed()` arm reads the same way
        // it does for a probe.
        conn.close(0u32.into(), b"dht-done");
        Ok::<_, anyhow::Error>(resp)
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "dht client: request timed out after {} ms",
            DHT_CLIENT_TIMEOUT.as_millis()
        )
    })??;
    Ok(response)
}

/// Inner-inner: do the bi-stream exchange on an already-connected
/// [`Connection`]. Split out so a future caller that wants to bundle
/// multiple DHT requests on one connection (e.g. a republish sweep to
/// the same target peer) can re-use the connection.
async fn exchange_on(conn: &Connection, payload: &[u8]) -> anyhow::Result<wire::DhtMessage> {
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("dht client: open_bi failed: {e}"))?;
    write_frame(&mut send, payload)
        .await
        .map_err(|e| anyhow::anyhow!("dht client: write request: {e}"))?;
    send.finish()
        .map_err(|e| anyhow::anyhow!("dht client: finish send: {e}"))?;
    let frame = read_frame(&mut recv).await.map_err(|e| map_frame_err(&e))?;
    let (msg, _tail) = decode_message::<wire::DhtMessage>(&frame)
        .map_err(|e| anyhow::anyhow!("dht client: decode response: {e}"))?;
    Ok(msg)
}

fn map_frame_err(e: &FrameError) -> anyhow::Error {
    anyhow::anyhow!("dht client: read response: {e}")
}

const fn variant_name(msg: &wire::DhtMessage) -> &'static str {
    match msg {
        wire::DhtMessage::FindValue(_) => "FindValue",
        wire::DhtMessage::FindValueResponse(_) => "FindValueResponse",
        wire::DhtMessage::Store(_) => "Store",
        wire::DhtMessage::StoreAck(_) => "StoreAck",
        wire::DhtMessage::BatchStore(_) => "BatchStore",
        wire::DhtMessage::BatchStoreAck(_) => "BatchStoreAck",
        wire::DhtMessage::FindNode(_) => "FindNode",
        wire::DhtMessage::FindNodeResponse(_) => "FindNodeResponse",
    }
}

/// Extract the ADR 013 application error code from a `read_to_end` /
/// `read_frame` error chain, if the peer closed the stream or
/// connection with one. Returns `None` for any other shape of error
/// (transport-level, decode-level, framing-level).
///
/// Callers (rate-limit handling, retry policy) match on this to decide
/// whether to back off (`0x10` = `RATE_LIMITED`) vs. give up
/// (`0x01`/`0x02`/`0x03`) vs. retry on transport flakiness (None).
#[must_use]
pub fn extract_app_error_code(err: &anyhow::Error) -> Option<u32> {
    // The error chain travels through `anyhow::Error` from various
    // iroh layers; walk it looking for the typed `ConnectionError` or
    // `ReadError` we can extract a code from.
    for cause in err.chain() {
        if let Some(code) = downcast_app_code(cause) {
            return Some(code);
        }
    }
    let _ = MAX_RESPONSE_BYTES; // currently informational; kept so a future
    // chunked-read switch has the right constant on hand.
    None
}

/// Helper: try to pull an ADR 013 application error code out of a
/// single error in the chain. Returns `None` for any error variant
/// that doesn't carry one (transport, decode, framing).
fn downcast_app_code(cause: &(dyn std::error::Error + 'static)) -> Option<u32> {
    if let Some(ce) = cause.downcast_ref::<ConnectionError>()
        && let ConnectionError::ApplicationClosed(c) = ce
    {
        return c.error_code.into_inner().try_into().ok();
    }
    if let Some(re) = cause.downcast_ref::<ReadError>() {
        return read_error_code(re);
    }
    if let Some(rte) = cause.downcast_ref::<ReadToEndError>()
        && let ReadToEndError::Read(re) = rte
    {
        return read_error_code(re);
    }
    None
}

fn read_error_code(re: &ReadError) -> Option<u32> {
    match re {
        ReadError::Reset(code) => code.into_inner().try_into().ok(),
        ReadError::ConnectionLost(ConnectionError::ApplicationClosed(c)) => {
            c.error_code.into_inner().try_into().ok()
        }
        _ => None,
    }
}
