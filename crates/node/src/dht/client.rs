//! Client-side `cdn/dht/v1` request primitives.
//!
//! The handler in [`crate::handlers::dht`] is the server half — it
//! receives `FindNode` / `FindValue` / `Store` / `BatchStore` requests
//! and writes responses. This module is the *client* half: outbound
//! requests issued by the bootstrap path (initial
//! `FindNode(self.node_id)`), the republish scheduler (a per-hash
//! `Store` on eager publish and one `BatchStore` per receiver on the
//! periodic drain), and the iterative requester-side `FindValue` lookup
//! in [`crate::dht::lookup`].
//!
//! Each call:
//! 1. Takes the peer's pooled `cdn/dht/v1` connection from the node's
//!    [`Connections`] map, which dials one when the peer has none.
//! 2. Opens a bi stream.
//! 3. Writes one framed [`wire::DhtMessage`] request.
//! 4. Reads one framed response, decodes, and returns it.
//! 5. Leaves the connection open for the next call to the same peer. The
//!    server closes it once it idles past its window or spends its budget.
//!
//! A call whose pinned connection turns out closed (the server closed it
//! between calls) redials once and retries. A call that fails on a
//! connection it dialled itself does not retry: that is the peer's failure.
//!
//! The signatures mirror the server-side message types: callers
//! provide the request fields, get back the response struct. Error
//! handling surfaces via `anyhow::Result`; a peer that closes the
//! stream with an ADR 013 application error code (rate-limited,
//! malformed) surfaces as an `anyhow` error the caller logs and retries
//! on its next cycle.

use std::time::Duration;

use std::sync::Arc;

use anyhow::Context;
use decdn_client::{Connections, WarmConnection};
use decdn_protocol::{
    ALPN_DHT, ContentHash, Coverage, NodeId, decode_message, dht as wire, encode_message,
    encode_store_request, read_frame, write_frame,
};
use iroh::EndpointAddr;
use iroh::endpoint::Connection;

/// Per-request hard timeout for outbound DHT exchanges. Shorter than
/// the sum of the server-side first-stream timeout (5s) and read
/// timeout (5s) in [`crate::handlers::dht`] so a hung peer doesn't
/// wedge a republish sweep.
pub const DHT_CLIENT_TIMEOUT: Duration = Duration::from_secs(8);

/// Send a single `FindNode` request to `target` and return the response.
///
/// Used by the bootstrap path (`FindNode(self.node_id)` to seed the
/// routing table) and by the bucket-refresh task
/// (`FindNode(random_id_in_bucket)`).
pub async fn find_node(
    conns: &Connections,
    target: EndpointAddr,
    target_id: NodeId,
    requester: NodeId,
) -> anyhow::Result<wire::FindNodeResponse> {
    let request = wire::DhtMessage::FindNode(wire::FindNodeRequest {
        target: target_id,
        requester,
    });
    let response = exchange(conns, target, &request).await?;
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
/// Used by the republish scheduler's eager publish of a newly advertisable
/// hash — one call per K+3 closest peer, per ADR 022 §STORE Flow steps 1-2.
pub async fn store(
    conns: &Connections,
    target: EndpointAddr,
    hash: ContentHash,
    holder: NodeId,
    coverage: Coverage,
) -> anyhow::Result<wire::StoreAck> {
    let request = wire::StoreRequest {
        hash,
        holder,
        coverage,
    };
    // Encode through the typed two-phase helper (ADR 013 §Tier 1) so the
    // client half mirrors the server's `parse_store_request_ext` seam: the
    // `StoreRequestExt` is appended here (empty today) and the payload is
    // sent as pre-encoded bytes via `exchange_payload`.
    let payload =
        encode_store_request(&request, None).context("dht client: encode store request")?;
    let response = exchange_payload(conns, target, &payload).await?;
    match response {
        wire::DhtMessage::StoreAck(a) => Ok(a),
        other => anyhow::bail!(
            "dht client: expected StoreAck, got {} variant",
            variant_name(&other)
        ),
    }
}

/// Send a single `BatchStore` request to `target` and return the ack
/// (ADR 022 §STORE Flow Batched STORE). One `bool` per request hash comes
/// back in request order. Used by the re-publish scheduler, which groups
/// a drain cycle's hashes by receiver and sends each receiver its set via
/// this. Every
/// DHT node implements `BatchStore`, so there is no per-hash fallback.
///
/// `entries` MUST be ≤ [`decdn_protocol::dht::MAX_BATCH_STORE_HASHES`];
/// an oversize batch is rejected by the receiver at wire decode with
/// `MALFORMED_MESSAGE` and surfaces here as an error (the caller must
/// split the set, not retry).
pub async fn batch_store(
    conns: &Connections,
    target: EndpointAddr,
    entries: Vec<(ContentHash, Coverage)>,
    holder: NodeId,
) -> anyhow::Result<wire::BatchStoreAck> {
    let n = entries.len();
    let request = wire::DhtMessage::BatchStore(wire::BatchStoreRequest { entries, holder });
    let response = exchange(conns, target, &request).await?;
    match response {
        // A conformant receiver returns exactly one `bool` per request hash
        // (`results[i]` ↔ `hashes[i]`); the wire type only caps the max
        // length. Reject a mis-sized ack as an error rather than hand back a
        // result vector that silently mis-maps onto the request hashes.
        wire::DhtMessage::BatchStoreAck(a) if a.results.len() == n => Ok(a),
        wire::DhtMessage::BatchStoreAck(a) => anyhow::bail!(
            "dht client: BatchStoreAck has {} results for a {n}-hash request",
            a.results.len(),
        ),
        other => anyhow::bail!(
            "dht client: expected BatchStoreAck, got {} variant",
            variant_name(&other)
        ),
    }
}

/// Send a single `FindValue` request to `target` and return the response.
///
/// Used by the requester-side iterative lookup ([`crate::dht::lookup`]).
/// Exposed here so the same connect/encode/decode plumbing isn't
/// duplicated across the bootstrap / republish / lookup call sites.
pub async fn find_value(
    conns: &Connections,
    target: EndpointAddr,
    hash: ContentHash,
    requester: NodeId,
) -> anyhow::Result<wire::FindValueResponse> {
    let request = wire::DhtMessage::FindValue(wire::FindValueRequest { hash, requester });
    let response = exchange(conns, target, &request).await?;
    match response {
        wire::DhtMessage::FindValueResponse(r) => Ok(r),
        other => anyhow::bail!(
            "dht client: expected FindValueResponse, got {} variant",
            variant_name(&other)
        ),
    }
}

/// Inner: encode `request`, then [`exchange_payload`].
async fn exchange(
    conns: &Connections,
    target: EndpointAddr,
    request: &wire::DhtMessage,
) -> anyhow::Result<wire::DhtMessage> {
    let payload = encode_message(request).context("dht client: encode request")?;
    exchange_payload(conns, target, &payload).await
}

/// Inner: take `target`'s pooled connection, `open_bi`, write a
/// pre-encoded request frame, read the response frame, decode. The
/// payload is already encoded so a caller that needs a typed two-phase
/// encode (e.g. `store` via [`encode_store_request`]) can hand over its
/// bytes directly.
///
/// Wraps the whole call, retry included, in one [`DHT_CLIENT_TIMEOUT`]
/// so a hung peer can't block the caller's scheduler indefinitely. A call
/// that times out unpins the connection it used, so the next call to the
/// peer dials afresh rather than queueing on a connection that stopped
/// answering.
async fn exchange_payload(
    conns: &Connections,
    target: EndpointAddr,
    payload: &[u8],
) -> anyhow::Result<wire::DhtMessage> {
    debug_assert_eq!(
        conns.alpn(),
        ALPN_DHT,
        "the DHT client needs a cdn/dht/v1 map"
    );
    let node = target.id;
    let mut used: Option<Arc<WarmConnection>> = None;
    let outcome = tokio::time::timeout(DHT_CLIENT_TIMEOUT, async {
        let (warm, dialled) = conns
            .get_or_dial(&target)
            .await
            .context("dht client: connect failed")?;
        used = Some(Arc::clone(&warm));
        let err = match exchange_on(warm.connection(), payload).await {
            Ok(resp) => return Ok(resp),
            Err(err) => err,
        };
        if !warm.is_closed() {
            return Err(err);
        }
        conns.invalidate(node, &warm).await;
        if dialled {
            return Err(err);
        }
        // The pinned connection closed under this call (the server spent
        // its budget or idled it out between calls): redial once.
        let (fresh, _) = conns
            .get_or_dial(&target)
            .await
            .context("dht client: connect failed")?;
        used = Some(Arc::clone(&fresh));
        exchange_on(fresh.connection(), payload).await
    })
    .await;
    let Ok(result) = outcome else {
        if let Some(warm) = used {
            conns.invalidate(node, &warm).await;
        }
        anyhow::bail!(
            "dht client: request timed out after {} ms",
            DHT_CLIENT_TIMEOUT.as_millis()
        );
    };
    result
}

/// Inner-inner: do one bi-stream exchange on an already-connected
/// [`Connection`], leaving the connection open.
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
    let frame = read_frame(&mut recv)
        .await
        .map_err(|e| anyhow::Error::new(e).context("dht client: read response"))?;
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
