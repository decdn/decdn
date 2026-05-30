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
    ALPN_DHT, ContentHash, NodeId, decode_message, dht as wire, encode_message, read_frame,
    write_frame,
};
use iroh::endpoint::{ConnectOptions, Connection, ConnectionError, ReadError, ReadToEndError};
use iroh::{Endpoint, EndpointAddr};

/// Per-request hard timeout for outbound DHT exchanges. Shorter than
/// the sum of the server-side accept-bi timeout (5s) and read
/// timeout (5s) in [`crate::handlers::dht`] so a hung peer doesn't
/// wedge a republish sweep.
pub const DHT_CLIENT_TIMEOUT: Duration = Duration::from_secs(8);

/// Send a single `FindNode` request to `target` and return the response.
///
/// Used by the bootstrap path (`FindNode(self.node_id)` to seed the
/// routing table) and by the bucket-refresh task
/// (`FindNode(random_id_in_bucket)`).
pub async fn find_node(
    endpoint: &Endpoint,
    target: EndpointAddr,
    target_id: NodeId,
    requester: NodeId,
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
    hash: ContentHash,
    holder: NodeId,
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

/// ADR 013 application error code a receiver closes the stream with when
/// it does not implement `BatchStore` admission (the pre-#648 deCDN
/// handler arm). It is the canonical "fall back to per-hash" signal:
/// `APP_ERR_UNSUPPORTED_MESSAGE` (also defined in the handlers; the codes
/// are protocol-wide). See [`batch_store_with_fallback`].
const APP_ERR_UNSUPPORTED_MESSAGE: u32 = 0x01;

/// Send a single `BatchStore` request to `target` and return the ack
/// (ADR 022 §STORE Flow Batched STORE, #648). One `bool` per request
/// hash comes back in request order. Callers that need automatic
/// fallback to per-hash `Store` against receivers that don't implement
/// batching use [`batch_store_with_fallback`] instead of calling this
/// directly.
///
/// `hashes` MUST be ≤ [`decdn_protocol::dht::MAX_BATCH_STORE_HASHES`];
/// an oversize batch is rejected by the receiver at wire decode with
/// `MALFORMED_MESSAGE` and surfaces here as an error (the caller must
/// split the set, not retry).
pub async fn batch_store(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hashes: Vec<ContentHash>,
    holder: NodeId,
) -> anyhow::Result<wire::BatchStoreAck> {
    let request = wire::DhtMessage::BatchStore(wire::BatchStoreRequest { hashes, holder });
    let response = exchange(endpoint, target, &request).await?;
    match response {
        wire::DhtMessage::BatchStoreAck(a) => Ok(a),
        other => anyhow::bail!(
            "dht client: expected BatchStoreAck, got {} variant",
            variant_name(&other)
        ),
    }
}

/// What a failed [`batch_store`] attempt means for fallback (ADR 022
/// §STORE Flow / AC 20). Decided purely from the receiver's close code so
/// the policy is unit-testable without a network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchAttemptOutcome {
    /// Receiver doesn't speak `BatchStore` (closed with
    /// `APP_ERR_UNSUPPORTED_MESSAGE`). Cache the negative result and fall
    /// back to per-hash `Store` — this is the AC 20 trigger.
    Unsupported,
    /// A failure that is NOT a "doesn't support batching" signal:
    /// `MALFORMED_MESSAGE` / `MESSAGE_TOO_LARGE` (a publisher-side bug —
    /// e.g. an over-cap batch — that per-hash retry would only mask),
    /// `RATE_LIMITED` (receiver overloaded; per-hash would add load), or
    /// a transport-level error where we never reached a stream close
    /// (`None` — connect/handshake/timeout). Surface the failure; the
    /// caller's normal republish cycle retries.
    HardFail,
}

/// Classify a [`batch_store`] error for fallback. Only the explicit
/// `APP_ERR_UNSUPPORTED_MESSAGE` close — the signal a pre-#648 deCDN node
/// emits for an unimplemented `BatchStore` — is treated as "fall back to
/// per-hash and cache". Everything else is a hard failure so a transient
/// blip or a client bug doesn't trigger an `n`-deep per-hash retry storm
/// against an unreachable or overloaded receiver.
const fn classify_batch_error(code: Option<u32>) -> BatchAttemptOutcome {
    match code {
        Some(APP_ERR_UNSUPPORTED_MESSAGE) => BatchAttemptOutcome::Unsupported,
        _ => BatchAttemptOutcome::HardFail,
    }
}

/// Publish `hashes` to `target` as a single [`batch_store`], falling back
/// to per-hash [`store`] when the receiver doesn't support batching
/// (ADR 022 §STORE Flow / AC 20). Returns one `bool` per hash in request
/// order (`accepted` for each).
///
/// Fallback negotiation:
/// - If `fallback` already records `target_id` as batch-unsupported
///   (within its bounded window), skip the batch attempt and go straight
///   to per-hash — no wasted drop-and-fallback round-trip.
/// - On an `UNSUPPORTED` close, mark `target_id` unsupported in
///   `fallback` and retry the whole set per-hash.
/// - On any other failure (malformed/too-large/rate-limited/transport),
///   return all-`false` without a per-hash storm; the caller retries on
///   its next cycle.
///
/// A receiver that returns a `BatchStoreAck` whose length doesn't match
/// the request is treated as non-conformant and the set is retried
/// per-hash (we don't trust a mis-sized ack to map onto our hashes).
//
// Linear negotiation ladder (cache → batch → classify → fall back); the
// branches are the spec's distinct outcomes, not incidental complexity.
#[allow(clippy::cognitive_complexity)]
pub async fn batch_store_with_fallback(
    endpoint: &Endpoint,
    target: EndpointAddr,
    target_id: NodeId,
    hashes: Vec<ContentHash>,
    holder: NodeId,
    fallback: &crate::dht::batch_fallback::BatchStoreFallback,
) -> Vec<bool> {
    let n = hashes.len();
    if n == 0 {
        return Vec::new();
    }
    // Caller bug: a batch over the wire cap can never be admitted (the
    // receiver rejects it at decode with `MALFORMED_MESSAGE`). Fail fast
    // with a clear error log + per-hash fallback rather than burn a
    // doomed batch round-trip that would look like a normal all-`false`
    // publish rejection. Splitting publish sets ≤ `MAX_BATCH_STORE_HASHES`
    // is the caller's responsibility (ADR 022 §STORE Flow).
    if n > decdn_protocol::dht::MAX_BATCH_STORE_HASHES {
        tracing::error!(
            n,
            cap = decdn_protocol::dht::MAX_BATCH_STORE_HASHES,
            "dht batch_store_with_fallback: over-cap batch (caller must split); using per-hash"
        );
        return per_hash_fallback(endpoint, target, &hashes, holder, DHT_CLIENT_TIMEOUT).await;
    }
    if !fallback.supports_batch(&target_id) {
        return per_hash_fallback(endpoint, target, &hashes, holder, DHT_CLIENT_TIMEOUT).await;
    }
    match batch_store(endpoint, target.clone(), hashes.clone(), holder).await {
        Ok(ack) if ack.results.len() == n => ack.results,
        Ok(ack) => {
            // A conformant receiver MUST return one bool per request hash;
            // a mis-sized ack is a protocol violation. Warn (not debug) so
            // a broken/non-conformant peer is visible on a default-level
            // scrape, then retry per-hash rather than trust the mapping.
            tracing::warn!(
                expected = n,
                got = ack.results.len(),
                "dht batch_store: receiver returned mis-sized ack; retrying per-hash"
            );
            per_hash_fallback(endpoint, target, &hashes, holder, DHT_CLIENT_TIMEOUT).await
        }
        Err(e) => match classify_batch_error(extract_app_error_code(&e)) {
            BatchAttemptOutcome::Unsupported => {
                tracing::debug!(
                    peer = ?target_id,
                    "dht batch_store: receiver does not support batching; \
                     caching + falling back to per-hash Store"
                );
                fallback.mark_unsupported(target_id);
                per_hash_fallback(endpoint, target, &hashes, holder, DHT_CLIENT_TIMEOUT).await
            }
            BatchAttemptOutcome::HardFail => {
                tracing::debug!(error = %e, "dht batch_store failed; no per-hash fallback this cycle");
                vec![false; n]
            }
        },
    }
}

/// After this many **consecutive** per-hash exchange timeouts, abandon the
/// rest of a [`per_hash_fallback`] set. A receiver that accepts the
/// connection but then hangs on every read would otherwise pin one publish
/// task for up to `MAX_BATCH_STORE_HASHES × DHT_CLIENT_TIMEOUT` (~34 min at
/// the 256 / 8 s defaults). Three back-to-back timeouts is a wedged peer,
/// not transient flakiness; the remaining hashes stay deferred (`false`)
/// and the caller retries them next cycle. Any non-timeout outcome (an
/// `accepted`/`rejected` ack, an error response, an unexpected variant)
/// resets the run — a peer that is merely slow-but-answering is not cut off.
pub const MAX_CONSECUTIVE_FALLBACK_TIMEOUTS: usize = 3;

/// Issue a per-hash `Store` for each hash and collect `accepted` flags in
/// request order. A failed exchange (or the whole connection failing) maps
/// to `false` for the affected hashes — the caller retries on its next
/// republish cycle.
///
/// Reuses a **single** QUIC connection for the whole set via the private
/// `exchange_on`: calling [`store`] per hash would open and tear down a
/// fresh connection for each, up to [`MAX_BATCH_STORE_HASHES`] sequential
/// handshakes for a full fallback. Each per-hash exchange keeps its own
/// `per_hash_timeout` so one hung hash can't wedge the rest, and the loop
/// abandons the remaining hashes after [`MAX_CONSECUTIVE_FALLBACK_TIMEOUTS`]
/// back-to-back timeouts so a wedged-but-connected peer can't pin the task
/// for the full `n × per_hash_timeout`.
///
/// `per_hash_timeout` is [`DHT_CLIENT_TIMEOUT`] in production; it is a
/// parameter only so tests can drive the timeout/bail path on a short
/// budget. Exposed (rather than private) as a reusable building block for
/// the republish scheduler (#630).
///
/// [`MAX_BATCH_STORE_HASHES`]: decdn_protocol::dht::MAX_BATCH_STORE_HASHES
//
// Linear connect → per-hash-exchange loop; the nested timeout/variant
// matching reads as one flow (same rationale as `exchange`).
#[allow(clippy::cognitive_complexity)]
pub async fn per_hash_fallback(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hashes: &[ContentHash],
    holder: NodeId,
    per_hash_timeout: Duration,
) -> Vec<bool> {
    let mut out = vec![false; hashes.len()];
    let connect = tokio::time::timeout(per_hash_timeout, async {
        let connecting = endpoint
            .connect_with_opts(target, ALPN_DHT, ConnectOptions::new())
            .await
            .context("dht client: connect failed")?;
        connecting.await.context("dht client: handshake failed")
    })
    .await;
    let Ok(Ok(conn)) = connect else {
        // Connect/handshake failed or timed out — every hash stays
        // `false`; the caller retries next cycle.
        tracing::debug!("dht per-hash fallback: connect to target failed; all hashes deferred");
        return out;
    };
    let mut consecutive_timeouts = 0usize;
    for (slot, &hash) in out.iter_mut().zip(hashes) {
        let request = wire::DhtMessage::Store(wire::StoreRequest { hash, holder });
        let payload = match encode_message(&request) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(hash = ?hash, error = %e, "dht per-hash fallback encode failed");
                continue;
            }
        };
        match tokio::time::timeout(per_hash_timeout, exchange_on(&conn, &payload)).await {
            Ok(Ok(wire::DhtMessage::StoreAck(ack))) => {
                *slot = ack.accepted;
                consecutive_timeouts = 0;
            }
            Ok(Ok(other)) => {
                consecutive_timeouts = 0;
                tracing::debug!(
                    hash = ?hash,
                    got = variant_name(&other),
                    "dht per-hash fallback: unexpected response variant"
                );
            }
            Ok(Err(e)) => {
                consecutive_timeouts = 0;
                tracing::debug!(hash = ?hash, error = %e, "dht per-hash fallback Store failed");
            }
            Err(_) => {
                consecutive_timeouts = consecutive_timeouts.saturating_add(1);
                tracing::debug!(
                    hash = ?hash,
                    consecutive_timeouts,
                    "dht per-hash fallback Store timed out"
                );
                if consecutive_timeouts >= MAX_CONSECUTIVE_FALLBACK_TIMEOUTS {
                    tracing::debug!(
                        consecutive_timeouts,
                        "dht per-hash fallback: peer wedged on reads; \
                         deferring remaining hashes"
                    );
                    break;
                }
            }
        }
    }
    conn.close(0u32.into(), b"dht-done");
    out
}

/// Send a single `FindValue` request to `target` and return the response.
///
/// Used by the requester-side iterative lookup ([`crate::dht::lookup`]).
/// Exposed here so the same connect/encode/decode plumbing isn't
/// duplicated across the bootstrap / republish / lookup call sites.
pub async fn find_value(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hash: ContentHash,
    requester: NodeId,
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
    // Use `.context()` (not `anyhow::anyhow!("...: {e}")`) so the
    // underlying typed iroh error stays in the error chain — that
    // way `extract_app_error_code` can downcast it to `ConnectionError`
    // / `ReadError` and pull out the ADR 013 application error code
    // the peer closed the stream with. The string-format wrap form
    // discarded the typed source and made the helper unreliable.
    let response = tokio::time::timeout(DHT_CLIENT_TIMEOUT, async {
        let connecting = endpoint
            .connect_with_opts(target, ALPN_DHT, ConnectOptions::new())
            .await
            .context("dht client: connect failed")?;
        let conn = connecting.await.context("dht client: handshake failed")?;
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
    let frame = match read_frame(&mut recv).await {
        Ok(frame) => frame,
        Err(frame_err) => {
            // A peer that signals via a *stream reset* (e.g.
            // `APP_ERR_UNSUPPORTED_MESSAGE` for an unimplemented
            // `BatchStore`, or `RATE_LIMITED`) is invisible to
            // `extract_app_error_code` through this path: iroh's
            // `AsyncRead` adapter collapses the reset into an `io::Error`
            // with no typed source, so the `ReadError::Reset(code)` is
            // lost (only its Display text survives). Probe the stream
            // directly for the reset code and re-surface it *typed* so
            // `extract_app_error_code` can recover the ADR 013 code and
            // callers (e.g. `batch_store_with_fallback`) can negotiate.
            if let Ok(Some(code)) = recv.received_reset().await {
                return Err(anyhow::Error::new(ReadError::Reset(code))
                    .context("dht client: peer reset response stream"));
            }
            return Err(anyhow::Error::new(frame_err).context("dht client: read response"));
        }
    };
    let (msg, _tail) = decode_message::<wire::DhtMessage>(&frame)
        .map_err(|e| anyhow::anyhow!("dht client: decode response: {e}"))?;
    Ok(msg)
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    // ADR 013 application error codes (protocol-wide constants).
    const APP_ERR_MESSAGE_TOO_LARGE: u32 = 0x02;
    const APP_ERR_MALFORMED_MESSAGE: u32 = 0x03;
    const APP_ERR_RATE_LIMITED: u32 = 0x10;

    #[test]
    fn unsupported_close_triggers_fallback() {
        // The canonical "I don't speak BatchStore" signal a pre-#648 node
        // emits — the only outcome that should cache + fall back per-hash.
        assert_eq!(
            classify_batch_error(Some(APP_ERR_UNSUPPORTED_MESSAGE)),
            BatchAttemptOutcome::Unsupported
        );
    }

    #[test]
    fn client_bug_and_overload_codes_are_hard_failures() {
        // Over-cap batch (MALFORMED) / oversize frame (TOO_LARGE) are
        // publisher bugs; per-hash retry would only mask them.
        // RATE_LIMITED means the receiver is overloaded — per-hash would
        // add load. None of these should fall back per-hash.
        for code in [
            APP_ERR_MALFORMED_MESSAGE,
            APP_ERR_MESSAGE_TOO_LARGE,
            APP_ERR_RATE_LIMITED,
        ] {
            assert_eq!(
                classify_batch_error(Some(code)),
                BatchAttemptOutcome::HardFail,
                "code {code:#x} must be a hard fail, not a batch-unsupported signal"
            );
        }
    }

    #[test]
    fn transport_error_without_app_code_is_a_hard_failure() {
        // `None` = connect/handshake/timeout (never reached a stream
        // close). Not a "stream-close-without-ack", so it must NOT poison
        // the fallback cache or trigger an n-deep per-hash timeout storm.
        assert_eq!(classify_batch_error(None), BatchAttemptOutcome::HardFail);
    }
}
