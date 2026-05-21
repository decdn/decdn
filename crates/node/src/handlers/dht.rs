//! `cdn/dht/v1` handler (ADR 022) — Kademlia content discovery.
//!
//! Current slice (PR 2 of #320):
//!
//! - `FindNode` is fully honored against the in-memory routing table.
//! - `FindValue` and `Store` produce placeholder responses (empty
//!   `providers` + the K-closest peers; `accepted: false`). Real
//!   admission lands in PR 3 of #320.
//! - `BatchStore` is rejected at frame read with `APP_ERR_UNSUPPORTED_MESSAGE`
//!   (`0x01`). The wire variant exists at its ADR-022 canonical
//!   discriminant so a future handler doesn't shuffle wire positions,
//!   but per ADR 022 §Schema Evolution + §STORE Flow line 138 the
//!   unsupported signal MUST be **stream-close-without-ack** —
//!   publishers detect that and fall back to per-hash `Store`.
//! - Response variants arriving on a server-accepted stream are
//!   similarly rejected with `APP_ERR_UNSUPPORTED_MESSAGE`.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use decdn_protocol::{
    ALPN_DHT, APP_ERR_RATE_LIMITED, FrameError, MAX_CLOSER_NODES, decode_message, dht as wire,
    encode_message, read_frame, write_frame,
};
use iroh::PublicKey;
use iroh::TransportAddr;
use iroh::Watcher as _;
use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};

use crate::dht::{DhtRateLimiter, DhtRejectLayer, RoutingTable};
use crate::dispatch::{ConnectionLimiter, RejectReason};
use crate::metrics::Metrics;

const ACCEPT_BI_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);
const REJECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(250);

// ADR 013 application error codes (also defined in `handlers::probe`; the
// codes are protocol-wide constants, not per-ALPN values).
const APP_ERR_UNSUPPORTED_MESSAGE: u32 = 0x01;
const APP_ERR_MESSAGE_TOO_LARGE: u32 = 0x02;
const APP_ERR_MALFORMED_MESSAGE: u32 = 0x03;

/// Serves `cdn/dht/v1`. Owns the routing table + the per-request rate
/// limiter; `accept()` is reentrant — many concurrent streams from
/// different peers all share the same `Arc<DhtHandler>`.
///
/// Routing-table updates take a short `Mutex` — the table is touched once
/// per request to refresh the requester's recency and once again to
/// produce the `closer_nodes` snapshot. We use `std::sync::Mutex` rather
/// than `tokio::sync::Mutex` since the critical section is microseconds
/// long (no awaits) and the lock would just add scheduler overhead.
pub struct DhtHandler {
    self_id: PublicKey,
    routing: Arc<Mutex<RoutingTable>>,
    rate_limiter: Arc<DhtRateLimiter>,
    dispatch_limiter: Arc<ConnectionLimiter>,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for DhtHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtHandler")
            .field("self_id", &self.self_id)
            .finish_non_exhaustive()
    }
}

impl DhtHandler {
    pub const ALPN: &'static [u8] = ALPN_DHT;

    /// Construct a handler. The routing table is empty at startup; PR 4
    /// of #320 wires bootstrap from the on-chain staker set, and the
    /// handler additionally updates the table from every incoming
    /// request's `requester` field once it knows the peer is rate-limit
    /// admitted.
    #[must_use]
    pub fn new(
        self_id: PublicKey,
        rate_limiter: Arc<DhtRateLimiter>,
        dispatch_limiter: Arc<ConnectionLimiter>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let routing = Arc::new(Mutex::new(RoutingTable::new(*self_id.as_bytes())));
        Self {
            self_id,
            routing,
            rate_limiter,
            dispatch_limiter,
            metrics,
        }
    }

    /// Construct with a pre-built routing table. Used by tests and by the
    /// bootstrap path that wants to seed the table before the handler
    /// goes live.
    #[must_use]
    pub const fn with_routing(
        self_id: PublicKey,
        routing: Arc<Mutex<RoutingTable>>,
        rate_limiter: Arc<DhtRateLimiter>,
        dispatch_limiter: Arc<ConnectionLimiter>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            self_id,
            routing,
            rate_limiter,
            dispatch_limiter,
            metrics,
        }
    }

    /// Shared handle to the routing table — exposed so PR 4 of #320 can
    /// drive bucket refresh and bootstrap without re-allocating the
    /// `Arc<Mutex<RoutingTable>>` from outside.
    #[must_use]
    pub fn routing_table(&self) -> Arc<Mutex<RoutingTable>> {
        Arc::clone(&self.routing)
    }

    // Linear protocol sequence — same rationale as `ProbeHandler::serve`
    // for keeping the ADR 013 error-code mapping and the ADR 022 admission
    // order in one auditable function.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn serve(&self, conn: Connection) -> anyhow::Result<()> {
        // Layer 1: dispatch limiter (connection-level cap).
        let _permit = match self.dispatch_limiter.acquire(&conn) {
            Ok(p) => p,
            Err(reason) => {
                conn.close(
                    VarInt::from_u32(APP_ERR_RATE_LIMITED),
                    reason.as_str().as_bytes(),
                );
                if reason != RejectReason::GlobalFull {
                    let _ = tokio::time::timeout(REJECTION_CLOSE_TIMEOUT, conn.closed()).await;
                }
                return Ok(());
            }
        };
        let _guard = self.metrics.connection_guard();

        // Resolve the peer's NodeId once (it's the authenticated QUIC
        // identity and cannot change for the lifetime of the connection).
        // The IP, by contrast, is re-evaluated per stream below — iroh
        // can switch the selected path mid-connection (e.g. relay → direct
        // UDP after hole-punching) and caching the initial value would
        // permanently disable the per-IP rate-limit layer for any
        // connection that started relay-only.
        let peer_node_id = *conn.remote_id().as_bytes();

        // ADR 022 §DHT Rate Limiting check fires before any deserialization
        // of the per-stream frame body — we do it per stream below since
        // each DHT request is one stream. The check on the *connection*
        // here would be redundant; we run it inside the per-stream loop
        // because a single connection may carry multiple requests.

        // Iroh-side connection cap: DHT is short-lived request/response,
        // but a peer may bundle multiple requests on one connection. Accept
        // streams in a loop until the peer closes (or we time out waiting).
        loop {
            // Peer closed the connection (`Ok(Err)`) or we hit the idle
            // accept timeout (`Err`) — both mean we're done serving this
            // connection; break the per-stream loop.
            let Ok(Ok((send, recv))) =
                tokio::time::timeout(ACCEPT_BI_TIMEOUT, conn.accept_bi()).await
            else {
                break;
            };
            // Rate-limit per request (ADR 022 §DHT Rate Limiting). Re-read
            // `peer_ip` here so a relay→direct path switch over the
            // connection's lifetime engages the per-IP layer for
            // subsequent streams.
            let peer_ip = peer_ip(&conn);
            if let Err(layer) = self.rate_limiter.check(&peer_node_id, peer_ip) {
                Self::close_stream_with_rate_limit(send, recv, layer);
                continue;
            }
            // Refresh routing-table entry — the requester just authenticated
            // their NodeId via QUIC and successfully spent a rate-limit
            // token. That's the same admission criteria ADR 022 §Routing
            // Table demands for table insertion.
            self.note_peer_seen(peer_node_id);
            if let Err(e) = self.handle_one(send, recv).await {
                // The per-request error already carries the ADR 013 app
                // error code via the stream reset inside `handle_one` /
                // `read_dht_request`; here we surface the failure to
                // operators via a debug log AND a counter bump so a node
                // running at default `RUST_LOG=info` still reflects "DHT
                // requests are failing" in scrapes without needing log
                // verbosity changes.
                self.metrics.dht_request_failed();
                tracing::debug!(error = %e, "dht request handling failed");
                // Drop this stream and continue accepting; the peer may
                // succeed on the next request.
            }
        }
        // Best-effort flush of any in-flight stream-finish bytes.
        let _ = tokio::time::timeout(CLOSE_TIMEOUT, conn.closed()).await;
        Ok(())
    }

    // No `self` needed — kept as an inherent fn so the rate-limit close
    // path lives next to the only call site (the per-stream loop in
    // `serve`).
    fn close_stream_with_rate_limit(
        mut send: SendStream,
        mut recv: RecvStream,
        layer: DhtRejectLayer,
    ) {
        let code = VarInt::from_u32(APP_ERR_RATE_LIMITED);
        let _ = send.reset(code);
        let _ = recv.stop(code);
        tracing::debug!(
            layer = layer.as_str(),
            "dht request rejected by rate limiter",
        );
    }

    /// Refresh `peer_id` in the routing table. Silently ignores the
    /// node's own id (the table itself enforces that, but skipping the
    /// lock-acquire is cheap).
    fn note_peer_seen(&self, peer_id: [u8; 32]) {
        if peer_id == *self.self_id.as_bytes() {
            return;
        }
        if let Ok(mut table) = self.routing.lock() {
            table.insert(peer_id);
        }
    }

    /// Read one framed `DhtMessage`, dispatch on variant, write the
    /// response frame back. Each stream carries exactly one
    /// request/response pair.
    async fn handle_one(&self, mut send: SendStream, mut recv: RecvStream) -> anyhow::Result<()> {
        let req = match read_dht_request(&mut send, &mut recv).await {
            Ok(req) => req,
            Err(DhtReadError { err, app_code }) => {
                let _ = send.reset(VarInt::from_u32(app_code));
                let _ = recv.stop(VarInt::from_u32(app_code));
                return Err(err);
            }
        };

        let resp = self.dispatch(req);
        let payload = encode_message(&resp)
            .map_err(|e| anyhow::anyhow!("dht response encode failed: {e}"))?;
        write_frame(&mut send, &payload)
            .await
            .map_err(|e| anyhow::anyhow!("dht response write failed: {e}"))?;
        send.finish()
            .map_err(|e| anyhow::anyhow!("dht stream finish failed: {e}"))?;
        Ok(())
    }

    /// Convert a decoded request into a response. The argument's enum
    /// shape encodes the precondition that this is one of the request
    /// variants the handler implements — `read_dht_request` filters out
    /// responses-on-server-stream and unsupported requests (`BatchStore`)
    /// before producing a [`DhtServerRequest`], so this match is
    /// exhaustive over the active set without needing a panic or echo
    /// fallback for the impossible cases.
    //
    // `msg` is moved-in so future PR slices' record-store insertion
    // paths can take ownership of larger payloads (e.g. the
    // FindValueResponse providers Vec) without re-allocating. Every
    // current request variant is `Copy`, so by-value is a no-op at the
    // ABI level today; the by-value signature is the steady-state shape.
    #[allow(clippy::needless_pass_by_value)]
    fn dispatch(&self, msg: DhtServerRequest) -> wire::DhtMessage {
        match msg {
            DhtServerRequest::FindNode(req) => {
                wire::DhtMessage::FindNodeResponse(self.handle_find_node(req))
            }
            DhtServerRequest::FindValue(req) => {
                // PR 3 of #320: implement record-store lookup.
                wire::DhtMessage::FindValueResponse(wire::FindValueResponse {
                    hash: req.hash,
                    providers: Vec::new(),
                    closer_nodes: self.closest_to(&req.hash),
                })
            }
            DhtServerRequest::Store(req) => {
                // PR 3 of #320: implement quota + active-staker check.
                wire::DhtMessage::StoreAck(wire::StoreAck {
                    hash: req.hash,
                    accepted: false,
                })
            }
        }
    }

    fn handle_find_node(&self, req: wire::FindNodeRequest) -> wire::FindNodeResponse {
        // NB: do NOT insert `req.requester` here. It is an attacker-
        // controlled field on the wire — letting it through would allow
        // an authenticated peer to inject arbitrary (potentially
        // unreachable) NodeIds into the routing table without owning the
        // matching keys (routing-table poisoning). The authenticated peer
        // NodeId from the QUIC handshake is already refreshed once per
        // admitted request in `serve()`, which is the only honest signal
        // we have about who is on the other end.
        let closer_nodes = self.closest_to(&req.target);
        wire::FindNodeResponse {
            target: req.target,
            closer_nodes,
        }
    }

    fn closest_to(&self, target: &[u8; 32]) -> Vec<[u8; 32]> {
        let Ok(table) = self.routing.lock() else {
            // Poisoned lock — return empty; the caller will treat this as
            // "responder has nothing closer", which is honest at this
            // moment regardless of the lock state.
            return Vec::new();
        };
        table.closest(target, MAX_CLOSER_NODES)
    }
}

impl ProtocolHandler for DhtHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.serve(connection)
            .await
            .map_err(|e| AcceptError::from_err(std::io::Error::other(e.to_string())))
    }
}

const fn frame_err_code(e: &FrameError) -> u32 {
    match e {
        FrameError::TooLarge(_) => APP_ERR_MESSAGE_TOO_LARGE,
        FrameError::Io(_) | FrameError::Varint | FrameError::Decode(_) => APP_ERR_MALFORMED_MESSAGE,
    }
}

struct DhtReadError {
    err: anyhow::Error,
    app_code: u32,
}

/// Request variants the DHT handler accepts on a server stream.
///
/// The enum is the narrowed shape produced by [`read_dht_request`] — it
/// excludes the response variants (responses belong on the client side of
/// the stream) and any request variants the handler does not yet
/// implement (`BatchStore` in this PR slice). Carrying the narrowing in a
/// type, rather than a runtime match-all fallback inside `dispatch`,
/// keeps the dispatcher exhaustive over the *active* set and prevents an
/// accidentally-silent "I echoed a bogus `FindNodeResponse`" failure mode
/// if the wire enum grows new request variants.
enum DhtServerRequest {
    FindNode(wire::FindNodeRequest),
    FindValue(wire::FindValueRequest),
    Store(wire::StoreRequest),
}

async fn read_dht_request(
    send: &mut SendStream,
    recv: &mut RecvStream,
) -> Result<DhtServerRequest, DhtReadError> {
    let reset = |send: &mut SendStream, recv: &mut RecvStream, code: u32| {
        let v = VarInt::from_u32(code);
        let _ = send.reset(v);
        let _ = recv.stop(v);
    };

    let frame = match tokio::time::timeout(READ_TIMEOUT, read_frame(recv)).await {
        Err(_) => {
            reset(send, recv, 0);
            return Err(DhtReadError {
                err: anyhow::anyhow!("dht request read timed out after {READ_TIMEOUT:?}"),
                app_code: 0,
            });
        }
        Ok(Err(e)) => {
            let app_code = frame_err_code(&e);
            reset(send, recv, app_code);
            return Err(DhtReadError {
                err: anyhow::anyhow!("dht frame read failed: {e}"),
                app_code,
            });
        }
        Ok(Ok(frame)) => frame,
    };

    match decode_message::<wire::DhtMessage>(&frame) {
        Err(e) => {
            reset(send, recv, APP_ERR_MALFORMED_MESSAGE);
            Err(DhtReadError {
                err: anyhow::anyhow!("dht decode failed: {e}"),
                app_code: APP_ERR_MALFORMED_MESSAGE,
            })
        }
        Ok((msg, _tail)) => {
            // Narrow the full wire enum down to the handler-supported
            // request set. Two filtering cases close the stream with
            // `APP_ERR_UNSUPPORTED_MESSAGE`:
            //
            // 1. Response variants on a server-accepted stream — the
            //    handler is a request-only endpoint; responses belong on
            //    the client side.
            // 2. `BatchStore` requests. Wire types are pinned at their
            //    ADR-022 canonical discriminants so a future handler can
            //    land without a wire shuffle, but the PR-2-of-#320 slice
            //    does not implement batch admission (two-stage rate
            //    limit, single holder check, per-hash quota). ADR 022
            //    §Schema Evolution line 138 and §STORE Flow specify that
            //    the unsupported signal MUST be stream-close-without-ack
            //    — an all-`false` `BatchStoreAck` would be read by
            //    publishers as "your batch was processed, every hash
            //    rejected" and trigger no fallback. Closing the stream
            //    with `APP_ERR_UNSUPPORTED_MESSAGE` is the conformant "I
            //    don't speak this yet" signal.
            match msg {
                wire::DhtMessage::FindNode(req) => Ok(DhtServerRequest::FindNode(req)),
                wire::DhtMessage::FindValue(req) => Ok(DhtServerRequest::FindValue(req)),
                wire::DhtMessage::Store(req) => Ok(DhtServerRequest::Store(req)),
                wire::DhtMessage::FindNodeResponse(_)
                | wire::DhtMessage::FindValueResponse(_)
                | wire::DhtMessage::StoreAck(_)
                | wire::DhtMessage::BatchStoreAck(_) => {
                    reset(send, recv, APP_ERR_UNSUPPORTED_MESSAGE);
                    Err(DhtReadError {
                        err: anyhow::anyhow!("peer sent dht response on server stream"),
                        app_code: APP_ERR_UNSUPPORTED_MESSAGE,
                    })
                }
                wire::DhtMessage::BatchStore(_) => {
                    reset(send, recv, APP_ERR_UNSUPPORTED_MESSAGE);
                    Err(DhtReadError {
                        err: anyhow::anyhow!(
                            "BatchStore handler not implemented (deferred to follow-up of #320); \
                             stream-close-without-ack triggers publisher fallback to per-hash Store"
                        ),
                        app_code: APP_ERR_UNSUPPORTED_MESSAGE,
                    })
                }
            }
        }
    }
}

/// Lift the peer IP out of a connection's currently-selected path.
/// Mirrors the private `peer_ip` helper in [`crate::dispatch`] — same rationale:
/// relay-only connections have no IP key, so the per-IP rate-limit layer
/// is skipped for them.
fn peer_ip(conn: &Connection) -> Option<IpAddr> {
    let paths = conn.paths().peek().clone();
    paths
        .iter()
        .find(|p| p.is_selected() && !p.is_closed())
        .and_then(|p| match p.remote_addr() {
            TransportAddr::Ip(addr) => Some(addr.ip()),
            _ => None,
        })
        .or_else(|| {
            paths
                .iter()
                .filter(|p| !p.is_closed())
                .find_map(|p| match p.remote_addr() {
                    TransportAddr::Ip(addr) => Some(addr.ip()),
                    _ => None,
                })
        })
}
