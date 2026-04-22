//! Loopback-only admin JSON-RPC surface (ADR 025).
//!
//! The admin surface is a local-operator control plane — it is expected
//! to bind on `127.0.0.1` only. Methods are dispatched via JSON-RPC 2.0
//! over HTTP `POST /`; the `AdminRpc` trait is the single source of
//! truth for both the server impl and the generated client bindings in
//! [`crate::commands`] and integration tests. jsonrpsee's
//! `#[rpc(server, client)]` macro consumes `AdminRpc` and emits
//! separate `AdminRpcServer` / `AdminRpcClient` traits; the original
//! `AdminRpc` name is not a linkable rustdoc item, hence the bare
//! backticks rather than an intra-doc link.
//!
//! Today the trait exposes a single method, `admin_v1_peersList`, which
//! returns the current gossip peer table as JSON. Future operational
//! methods (drain, health, etc.) will land here as additional entries
//! on the same trait without requiring a new transport.

use std::net::SocketAddr;
use std::sync::Arc;

use decdn_gossip::{PeerEntry, PeerTable};
use jsonrpsee::core::{RpcResult, async_trait};
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{Server, ServerConfig};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, oneshot};

/// Cap concurrent admin connections. The surface is local-operator-only,
/// but an errant operator script looping requests must not be able to
/// exhaust the runtime's task budget. `u32` rather than `usize` because
/// `ServerConfig::max_connections` is typed that way.
const MAX_ADMIN_CONNECTIONS: u32 = 16;

/// Shared state for admin RPC handlers.
#[derive(Debug, Clone)]
pub struct AdminState {
    peer_table: Arc<RwLock<PeerTable>>,
}

impl AdminState {
    pub const fn new(peer_table: Arc<RwLock<PeerTable>>) -> Self {
        Self { peer_table }
    }
}

/// JSON view of a [`PeerEntry`] emitted by `admin_v1_peersList`.
///
/// Defined separately from `PeerEntry` so that table-internal fields
/// (per-peer counters, debug flags, etc.) that may accrete in the future
/// can't silently leak into the wire format. Transitively-included
/// protocol types (e.g. [`decdn_protocol::LoadHint`]) do remain on the
/// wire, so changes to those still need to be treated as wire-format
/// changes.
///
/// Also used by `decdn node peers` and the integration tests to
/// deserialize the server response — sharing the type here prevents the
/// two sides from drifting field-for-field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerView {
    /// Lowercase hex of the peer's Ed25519 public key (ADR 001).
    pub node_id: String,
    /// ISO 3166-1 alpha-2 region code from the announce.
    pub region: String,
    /// Microseconds-since-epoch the peer was first inserted into the table.
    pub first_seen_us: u64,
    /// Microseconds-since-epoch the peer's most recent announce was accepted.
    pub last_seen_us: u64,
    /// `LoadHint` from the most recent announce.
    pub load: decdn_protocol::LoadHint,
    /// `timestamp_us` carried inside the signed announce body.
    pub announced_at_us: u64,
}

impl PeerView {
    fn from_raw(raw: RawPeer) -> Self {
        Self {
            node_id: hex_encode(&raw.node_id),
            region: raw.region,
            first_seen_us: raw.first_seen_us,
            last_seen_us: raw.last_seen_us,
            load: raw.load,
            announced_at_us: raw.announced_at_us,
        }
    }
}

/// Owned snapshot of one [`PeerEntry`]'s fields-of-interest, captured
/// under the read lock so the lock can be dropped before [`hex_encode`]
/// and final DTO assembly run. Hex-encoding the node id and allocating
/// the wire-format `node_id` string don't need to see live state, so
/// keeping them inside the locked region would block concurrent
/// announce-writers for no benefit.
struct RawPeer {
    node_id: [u8; 32],
    region: String,
    first_seen_us: u64,
    last_seen_us: u64,
    load: decdn_protocol::LoadHint,
    announced_at_us: u64,
}

impl RawPeer {
    fn from_entry(node_id: &[u8; 32], entry: &PeerEntry) -> Self {
        Self {
            node_id: *node_id,
            region: entry.announce.body.region.clone(),
            first_seen_us: entry.first_seen_us,
            last_seen_us: entry.last_seen_us,
            load: entry.announce.body.load,
            announced_at_us: entry.announce.body.timestamp_us,
        }
    }
}

/// Response body for `admin_v1_peersList`. Shared between the server
/// (serializes), `decdn node peers` (deserializes via the generated
/// client), and the integration tests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeersResponse {
    pub peers: Vec<PeerView>,
}

/// Admin RPC surface. Versioned via the namespace prefix
/// (`admin_v1_...`): new methods may be added backwards-compatibly
/// within `v1`, a breaking change cuts over to `admin_v2_...`.
#[rpc(server, client, namespace = "admin_v1")]
pub trait AdminRpc {
    /// Return the current gossip peer table. Ordering is most-recently-
    /// seen first.
    #[method(name = "peersList")]
    async fn peers_list(&self) -> RpcResult<PeersResponse>;
}

/// Concrete server implementation backed by the live gossip peer table.
#[derive(Debug, Clone)]
pub struct AdminRpcImpl {
    state: AdminState,
}

impl AdminRpcImpl {
    pub const fn new(state: AdminState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AdminRpcServer for AdminRpcImpl {
    async fn peers_list(&self) -> RpcResult<PeersResponse> {
        // Two-pass snapshot: under the read lock we copy only the
        // owned data needed to build a PeerView (raw node_id bytes,
        // region clone, scalar fields). Hex encoding of node_id and
        // final DTO assembly run *after* the lock is released, along
        // with sorting and (later, in the framework) JSON encoding of
        // `popular_hashes`. Lock hold time stays proportional to peer
        // count and to the per-entry data extraction, no further.
        let raw: Vec<RawPeer> = {
            let guard = self.state.peer_table.read().await;
            guard
                .iter()
                .map(|(id, entry)| RawPeer::from_entry(id, entry))
                .collect()
        };
        let mut snapshot: Vec<PeerView> = raw.into_iter().map(PeerView::from_raw).collect();
        // Most recently seen first — on-call use case is "is gossip alive?".
        snapshot.sort_by_key(|v| std::cmp::Reverse(v.last_seen_us));
        Ok(PeersResponse { peers: snapshot })
    }
}

/// Bind the admin listener. Kept synchronous-at-startup so port
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

/// Serve admin RPC methods on `listener` until `shutdown` fires.
///
/// Concurrency is capped via `ServerConfig::max_connections`; shutdown
/// is signalled by calling `ServerHandle::stop()`, then we await
/// `stopped()` so a caller that drops the future cannot leave the
/// background accept loop running.
#[allow(clippy::cognitive_complexity)] // Config build + select on two shutdown paths reads linearly.
pub async fn serve(
    listener: TcpListener,
    state: AdminState,
    mut shutdown: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    // jsonrpsee's `build_from_tcp` expects a `std::net::TcpListener` in
    // blocking mode. `into_std` is the official handoff; it must be
    // called before any incoming connections have been accepted on the
    // tokio side, which is the case here (we've only just bound).
    let std_listener = listener
        .into_std()
        .map_err(|e| anyhow::anyhow!("convert admin listener to std: {e}"))?;

    let config = ServerConfig::builder()
        .max_connections(MAX_ADMIN_CONNECTIONS)
        .http_only()
        .build();

    let server = Server::builder()
        .set_config(config)
        .build_from_tcp(std_listener)
        .map_err(|e| anyhow::anyhow!("build admin RPC server: {e}"))?;

    let rpc = AdminRpcImpl::new(state);
    let handle = server.start(rpc.into_rpc());

    // Two shutdown paths:
    //   1. Runtime fires the oneshot -> we call `handle.stop()` and
    //      wait for the accept loop to drain.
    //   2. Server exits on its own (shouldn't happen for HTTP-only but
    //      guard against it) -> return Ok so the runtime can notice
    //      via the `admin_stop_tx` / `warn!` path.
    let stopped = handle.clone().stopped();
    tokio::pin!(stopped);
    tokio::select! {
        biased;
        _ = &mut shutdown => {
            tracing::debug!("admin server shutdown signal received");
            if handle.stop().is_err() {
                tracing::debug!("admin server already stopped before shutdown signal");
            }
            handle.stopped().await;
        }
        () = &mut stopped => {
            tracing::warn!("admin server self-stopped before shutdown signal");
        }
    }
    Ok(())
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

    #[tokio::test]
    async fn empty_peer_table_returns_empty_vec() {
        let rpc = AdminRpcImpl::new(state_with(vec![]));
        let resp = rpc.peers_list().await.expect("peers_list ok");
        assert!(resp.peers.is_empty());
    }

    #[tokio::test]
    async fn single_peer_serializes_with_hex_node_id() {
        let id = [0xABu8; 32];
        // Seed once at now_us=100, then refresh at now_us=300 so
        // first_seen_us and last_seen_us differ. Distinct values catch a
        // field-swap regression (first↔last) that identical seeds would
        // not.
        let mut table = PeerTable::new(0);
        table
            .insert_or_refresh(mk_announce(id, "US", 10), 100)
            .expect("seed insert");
        table
            .insert_or_refresh(mk_announce(id, "US", 20), 300)
            .expect("refresh insert");
        let state = AdminState::new(Arc::new(RwLock::new(table)));
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc.peers_list().await.expect("peers_list ok");
        assert_eq!(resp.peers.len(), 1);
        let p = &resp.peers[0];
        assert_eq!(p.node_id, "ab".repeat(32));
        assert_eq!(p.region, "US");
        assert_eq!(p.first_seen_us, 100);
        assert_eq!(p.last_seen_us, 300);
        assert_eq!(p.announced_at_us, 20);
    }

    #[tokio::test]
    async fn peers_sorted_by_last_seen_descending() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let c = [3u8; 32];
        // (id, region, announce ts, now_us) — `now_us` becomes last_seen_us.
        let rpc = AdminRpcImpl::new(state_with(vec![
            (a, "US", 1, 500),
            (b, "EU", 1, 700),
            (c, "AP", 1, 600),
        ]));
        let resp = rpc.peers_list().await.expect("peers_list ok");
        let order: Vec<u64> = resp.peers.iter().map(|p| p.last_seen_us).collect();
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
}
