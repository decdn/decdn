//! Loopback-only admin HTTP surface (ADR 025).
//!
//! Mirrors the structure of [`crate::metrics`]: hyper `service_fn` over a
//! pre-bound `TcpListener`, shutdown via a `oneshot`, per-connection
//! concurrency capped by a semaphore. The admin server is a local-operator
//! control plane — it is expected to bind on `127.0.0.1` only.
//!
//! Today it exposes a single route, `GET /v1/peers`, which returns the
//! current gossip peer table as JSON. Future operational endpoints (drain,
//! health, etc.) will land here as additional `/v1/...` routes without
//! requiring a new transport.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use decdn_gossip::{PeerEntry, PeerTable};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, Semaphore, oneshot};

/// Cap concurrent admin connections. The surface is local-operator-only,
/// but an errant operator script looping curl must not be able to exhaust
/// the runtime's task budget.
const MAX_ADMIN_CONNECTIONS: usize = 16;

/// Shared state for admin HTTP handlers.
#[derive(Debug, Clone)]
pub struct AdminState {
    peer_table: Arc<RwLock<PeerTable>>,
}

impl AdminState {
    pub const fn new(peer_table: Arc<RwLock<PeerTable>>) -> Self {
        Self { peer_table }
    }
}

/// JSON view of a [`PeerEntry`] emitted by `GET /v1/peers`.
///
/// Defined separately from `PeerEntry` so that table-internal fields
/// (per-peer counters, debug flags, etc.) that may accrete in the future
/// can't silently leak into the wire format. Transitively-included
/// protocol types (e.g. [`decdn_protocol::LoadHint`]) do remain on the
/// wire, so changes to those still need to be treated as wire-format
/// changes.
///
/// Also used by `decdn node peers` to deserialize the server response —
/// sharing the type here prevents the two sides from drifting field-for-
/// field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PeerView {
    /// Lowercase hex of the peer's Ed25519 public key (ADR 001).
    pub(crate) node_id: String,
    /// ISO 3166-1 alpha-2 region code from the announce.
    pub(crate) region: String,
    /// Microseconds-since-epoch the peer was first inserted into the table.
    pub(crate) first_seen_us: u64,
    /// Microseconds-since-epoch the peer's most recent announce was accepted.
    pub(crate) last_seen_us: u64,
    /// `LoadHint` from the most recent announce.
    pub(crate) load: decdn_protocol::LoadHint,
    /// `timestamp_us` carried inside the signed announce body.
    pub(crate) announced_at_us: u64,
}

impl PeerView {
    fn from_entry(node_id: &[u8; 32], entry: &PeerEntry) -> Self {
        Self {
            node_id: hex_encode(node_id),
            region: entry.announce.body.region.clone(),
            first_seen_us: entry.first_seen_us,
            last_seen_us: entry.last_seen_us,
            load: entry.announce.body.load,
            announced_at_us: entry.announce.body.timestamp_us,
        }
    }
}

/// Owned variant used by both the server (borrow-free because `serde_json`
/// serializes equally from `&[PeerView]` or `Vec<PeerView>`) and by the
/// CLI when re-serializing a filtered subset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PeersResponse {
    pub(crate) peers: Vec<PeerView>,
}

/// Bind the admin HTTP listener. Kept synchronous-at-startup so port
/// conflicts fail fast rather than deep inside the runtime task graph.
///
/// # Errors
/// Returns an error if `TcpListener::bind` fails.
pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("admin bind {addr} failed: {e}"))?;
    tracing::info!(%addr, "admin server listening");
    Ok(listener)
}

/// Serve admin routes on `listener` until `shutdown` fires.
///
/// Per-connection tasks are detached, same rationale as
/// [`crate::metrics::serve`]: admin responses are short and dropping an
/// in-flight one on shutdown is harmless to the operator CLI, which will
/// surface the broken connection as an error return.
#[allow(clippy::cognitive_complexity)] // Accept+permit+spawn is the same linear pattern as metrics::serve.
pub async fn serve(
    listener: TcpListener,
    state: AdminState,
    mut shutdown: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let limiter = Arc::new(Semaphore::new(MAX_ADMIN_CONNECTIONS));

    loop {
        let (stream, peer) = tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::debug!("admin server shutdown signal received");
                return Ok(());
            }
            res = listener.accept() => match res {
                Ok(pair) => pair,
                Err(err) => {
                    tracing::warn!(%err, "admin accept failed");
                    continue;
                }
            },
        };

        let Ok(permit) = Arc::clone(&limiter).try_acquire_owned() else {
            tracing::warn!(
                %peer,
                limit = MAX_ADMIN_CONNECTIONS,
                "admin connection rejected: at capacity",
            );
            drop(stream);
            continue;
        };

        let state = state.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let state = state.clone();
                async move { handle(req, state).await }
            });
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                tracing::debug!(%err, "admin connection ended");
            }
        });
    }
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    state: AdminState,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let method = req.method();
    let path = req.uri().path();
    match (method, path) {
        (&Method::GET, "/v1/peers") => Ok(peers_response(&state).await),
        (_, "/v1/peers") => Ok(method_not_allowed("GET")),
        _ => Ok(not_found()),
    }
}

async fn peers_response(state: &AdminState) -> Response<Full<Bytes>> {
    // Snapshot the table under a read lock: copying the entries keeps the
    // lock hold time proportional to peer count, not to the JSON encode
    // time (which grows with `popular_hashes` lengths etc.).
    let mut snapshot: Vec<PeerView> = {
        let guard = state.peer_table.read().await;
        guard
            .iter()
            .map(|(id, entry)| PeerView::from_entry(id, entry))
            .collect()
    };
    // Most recently seen first — on-call use case is "is gossip alive?".
    // Sorting happens after the read lock is released so concurrent
    // announce-writers aren't blocked on the sort's CPU time.
    snapshot.sort_by_key(|v| std::cmp::Reverse(v.last_seen_us));

    let body = PeersResponse { peers: snapshot };
    match serde_json::to_vec(&body) {
        Ok(bytes) => json_ok(bytes),
        Err(err) => {
            tracing::warn!(%err, "peers response encode failed");
            internal_error()
        }
    }
}

fn json_ok(body: Vec<u8>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

fn not_found() -> Response<Full<Bytes>> {
    static BODY: &[u8] = br#"{"error":"not_found"}"#;
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(BODY)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

fn method_not_allowed(allow: &'static str) -> Response<Full<Bytes>> {
    static BODY: &[u8] = br#"{"error":"method_not_allowed"}"#;
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header("content-type", "application/json")
        .header("allow", allow)
        .body(Full::new(Bytes::from_static(BODY)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

fn internal_error() -> Response<Full<Bytes>> {
    static BODY: &[u8] = br#"{"error":"internal"}"#;
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(BODY)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

const HEX: &[u8; 16] = b"0123456789abcdef";

fn hex_encode(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for b in bytes {
        let hi = usize::from(b >> 4);
        let lo = usize::from(b & 0x0f);
        out.push(char::from(HEX.get(hi).copied().unwrap_or(b'0')));
        out.push(char::from(HEX.get(lo).copied().unwrap_or(b'0')));
    }
    out
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
    use decdn_protocol::{LoadHint, NodeAnnounce, NodeAnnounceBody};
    use http_body_util::BodyExt;

    fn mk_announce(node_id: [u8; 32], region: &str, ts_us: u64) -> NodeAnnounce {
        NodeAnnounce {
            body: NodeAnnounceBody {
                node_id,
                region: region.to_string(),
                load: LoadHint {
                    active_streams: 0,
                    bandwidth_utilization: 0,
                },
                popular_hashes: vec![],
                timestamp_us: ts_us,
            },
            signature: vec![0u8; 64],
        }
    }

    fn state_with(peers: Vec<([u8; 32], &str, u64, u64)>) -> AdminState {
        let mut table = PeerTable::new(0);
        for (id, region, ts_us, now_us) in peers {
            table
                .insert_or_refresh(mk_announce(id, region, ts_us), now_us)
                .expect("seed insert succeeds");
        }
        AdminState::new(Arc::new(RwLock::new(table)))
    }

    async fn body_to_string(resp: Response<Full<Bytes>>) -> String {
        let collected = resp.into_body().collect().await.expect("body collect");
        String::from_utf8(collected.to_bytes().to_vec()).expect("utf-8 body")
    }

    #[tokio::test]
    async fn empty_peer_table_returns_empty_array() {
        let state = state_with(vec![]);
        let resp = peers_response(&state).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        let body = body_to_string(resp).await;
        assert_eq!(body, r#"{"peers":[]}"#);
    }

    #[tokio::test]
    async fn single_peer_serializes_with_hex_node_id() {
        let id = [0xABu8; 32];
        // Seed the entry once at now_us=100, then refresh with a later
        // announce at now_us=300 so first_seen_us and last_seen_us differ.
        // Distinct values catch a field-swap regression (first↔last) that
        // identical seeds would not.
        let mut table = PeerTable::new(0);
        table
            .insert_or_refresh(mk_announce(id, "US", 10), 100)
            .expect("seed insert");
        table
            .insert_or_refresh(mk_announce(id, "US", 20), 300)
            .expect("refresh insert");
        let state = AdminState::new(Arc::new(RwLock::new(table)));

        let resp = peers_response(&state).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_to_string(resp).await;
        let value: serde_json::Value = serde_json::from_str(&body).expect("valid json");
        let peers = value["peers"].as_array().expect("peers array");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0]["node_id"], "ab".repeat(32));
        assert_eq!(peers[0]["region"], "US");
        assert_eq!(peers[0]["first_seen_us"], 100);
        assert_eq!(peers[0]["last_seen_us"], 300);
        assert_eq!(peers[0]["announced_at_us"], 20);
    }

    #[tokio::test]
    async fn peers_sorted_by_last_seen_descending() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let c = [3u8; 32];
        // (id, region, announce ts, now_us) — `now_us` becomes last_seen_us.
        let state = state_with(vec![
            (a, "US", 1, 500),
            (b, "EU", 1, 700),
            (c, "AP", 1, 600),
        ]);
        let resp = peers_response(&state).await;
        let body = body_to_string(resp).await;
        let value: serde_json::Value = serde_json::from_str(&body).expect("valid json");
        let peers = value["peers"].as_array().expect("peers array");
        let order: Vec<u64> = peers
            .iter()
            .map(|p| p["last_seen_us"].as_u64().unwrap_or_default())
            .collect();
        assert_eq!(order, vec![700, 600, 500]);
    }

    #[test]
    fn hex_encode_round_trip_nibble_order() {
        let mut buf = [0u8; 32];
        buf[0] = 0x01;
        buf[1] = 0x23;
        buf[31] = 0xef;
        let out = hex_encode(&buf);
        assert!(out.starts_with("0123"));
        assert!(out.ends_with("ef"));
        assert_eq!(out.len(), 64);
    }

    #[test]
    fn method_not_allowed_sets_allow_header() {
        let resp = method_not_allowed("GET");
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            resp.headers().get("allow").and_then(|v| v.to_str().ok()),
            Some("GET")
        );
    }

    #[test]
    fn not_found_is_json() {
        let resp = not_found();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
    }
}
