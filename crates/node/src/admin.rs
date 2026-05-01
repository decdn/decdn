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
//! Today the trait exposes `admin_v1_peersList` (gossip peer table as
//! JSON) and `admin_v1_health` (node id + uptime). Future operational
//! methods (drain, etc.) will land here as additional entries on the
//! same trait without requiring a new transport.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use decdn_cache::{CacheEngine, CacheError, Hash};
use decdn_gossip::{AnnounceTrigger, PeerEntry, PeerTable};
use jsonrpsee::core::{RpcResult, async_trait};
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{Server, ServerConfig};
use jsonrpsee::types::ErrorObjectOwned;
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
    /// Raw bytes of this node's iroh `PublicKey`. Hex-encoded on the
    /// wire by `admin_v1_health`, matching the encoding `PeerView`
    /// already uses for peer node ids on `admin_v1_peersList`.
    node_id: [u8; 32],
    /// Process-start `Instant`, captured by the runtime at the top of
    /// `run()` before any `await` or I/O. Used as the origin of the
    /// `uptime_s` field returned by `admin_v1_health`. `Instant` (not
    /// `SystemTime`) so wall-clock skew during the process's lifetime
    /// can't make uptime go backwards. May predate `AdminState`
    /// construction by the time the RPC preflight, identity load, and
    /// cache open take.
    started_at: Instant,
    /// Cache engine handle for `admin_v1_evict` (issue #279). The engine
    /// is `Clone` (its `Arc<Inner>` is shared), so cloning into
    /// `AdminState` is cheap.
    cache: CacheEngine,
    /// One-shot announce trigger for `admin_v1_announce` (issue #280).
    /// `None` when the publisher is disabled (no region configured); the
    /// RPC method translates that into a "publisher disabled" error so the
    /// operator gets a specific message rather than a generic failure.
    announce_trigger: Option<Arc<AnnounceTrigger>>,
}

impl AdminState {
    pub const fn new(
        peer_table: Arc<RwLock<PeerTable>>,
        node_id: [u8; 32],
        started_at: Instant,
        cache: CacheEngine,
        announce_trigger: Option<Arc<AnnounceTrigger>>,
    ) -> Self {
        Self {
            peer_table,
            node_id,
            started_at,
            cache,
            announce_trigger,
        }
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
            node_id: alloy::primitives::hex::encode(raw.node_id),
            region: raw.region,
            first_seen_us: raw.first_seen_us,
            last_seen_us: raw.last_seen_us,
            load: raw.load,
            announced_at_us: raw.announced_at_us,
        }
    }
}

/// Owned snapshot of one [`PeerEntry`]'s fields-of-interest, captured
/// under the read lock so the lock can be dropped before hex encoding
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

/// Response body for `admin_v1_health`. Shared between the server
/// (serializes) and `decdn node health` (deserializes via the generated
/// client). Intentionally minimal: this method exists so an operator
/// script can answer "is this admin port the node I think it is, and
/// has it been up since I started watching?" with one RPC call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Lowercase hex of this node's iroh `PublicKey` — same encoding as
    /// `PeerView::node_id`.
    pub node_id: String,
    /// Whole seconds since the runtime captured the process-start
    /// `Instant` at the top of `decdn run` (before any `await`, before
    /// the RPC preflight). Computed from a monotonic `Instant` so
    /// wall-clock skew can't produce a negative or non-monotonic value.
    pub uptime_s: u64,
}

/// Request body for `admin_v1_evict` (issue #279).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictRequest {
    /// Lowercase hex of the BLAKE3 hash to evict. Must be exactly 64 hex
    /// characters; rejected at the server with `INVALID_PARAMS` otherwise.
    pub hash: String,
    /// If `true`, after evicting clear the eviction flag and re-pull the
    /// blob from the configured origin. Used by `decdn node evict
    /// --re-pin` to forcibly refresh a corrupted local copy.
    #[serde(default)]
    pub re_pin: bool,
}

/// Response body for `admin_v1_evict`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictResponse {
    /// Whether the hash was present in the cache *before* the evict ran.
    /// `false` means the operator's evict was a no-op safety measure (the
    /// blob was never cached or had already been evicted).
    pub was_present: bool,
    /// `true` only when `re_pin: true` was requested AND the subsequent
    /// pull-through from the origin succeeded. `false` otherwise — including
    /// when `re_pin` was not requested.
    pub repinned: bool,
}

/// Response body for `admin_v1_announce` (issue #280).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnounceResponse {
    /// Always `true` on success — the trigger landed on the publisher's
    /// notify slot. The actual broadcast is racy with concurrent ticker
    /// fires, so a `true` here means "your request was queued", not "your
    /// announce hit the wire by the time this RPC returned."
    pub triggered: bool,
}

/// JSON-RPC error code: the request shape was wrong (bad hex, etc.).
/// Matches the standard JSON-RPC 2.0 `Invalid params` code.
const INVALID_PARAMS_CODE: i32 = -32_602;

/// JSON-RPC error code: the server can't satisfy this method right now.
/// Used when `admin_v1_announce` is invoked on a node whose gossip
/// publisher is disabled (no region configured) — operators get a
/// specific message instead of a generic failure.
const PUBLISHER_DISABLED_CODE: i32 = -32_001;

/// JSON-RPC error code: the cache layer reported an error during evict
/// (e.g. the underlying iroh-blobs store I/O failed, or the origin
/// returned an error during the optional `--re-pin` pull-through).
const CACHE_ERROR_CODE: i32 = -32_002;

/// Admin RPC surface. Versioned via the namespace prefix
/// (`admin_v1_...`): new methods may be added backwards-compatibly
/// within `v1`, a breaking change cuts over to `admin_v2_...`.
#[rpc(server, client, namespace = "admin_v1")]
pub trait AdminRpc {
    /// Return the current gossip peer table. Ordering is most-recently-
    /// seen first.
    #[method(name = "peersList")]
    async fn peers_list(&self) -> RpcResult<PeersResponse>;

    /// Return this node's identity and process uptime.
    #[method(name = "health")]
    async fn health(&self) -> RpcResult<HealthResponse>;

    /// Evict a single blob from the local cache (issue #279). Optionally
    /// re-pull from the configured origin afterward.
    #[method(name = "evict")]
    async fn evict(&self, req: EvictRequest) -> RpcResult<EvictResponse>;

    /// Trigger an immediate `NodeAnnounce` broadcast (issue #280). Returns
    /// `PUBLISHER_DISABLED_CODE` when the publisher is off (no region).
    #[method(name = "announce")]
    async fn announce(&self) -> RpcResult<AnnounceResponse>;
}

/// Convert a [`CacheError`] into a JSON-RPC error suitable for
/// `admin_v1_evict`. Distinguished mainly so the operator-facing message
/// can name the underlying failure mode (`NotFound`, `OriginError`, etc.)
/// rather than a generic "cache failed".
fn cache_error_to_rpc(err: &CacheError) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(CACHE_ERROR_CODE, err.to_string(), None::<()>)
}

/// Decode a 64-character hex BLAKE3 hash into [`Hash`].
///
/// Goes through `alloy::primitives::hex::decode` (case-insensitive,
/// `0x`-prefix-tolerant) rather than `Hash::from_str` because the
/// iroh-blobs implementation falls through to `data_encoding`'s base32
/// decoder for short inputs and **panics** when the decoder's output
/// buffer is the wrong size for the requested decoding. Operator-driven
/// inputs reach this path; a panic on malformed hex would tear down the
/// admin RPC handler thread instead of returning an `INVALID_PARAMS`
/// error to the caller.
fn parse_hash_arg(hex: &str) -> Result<Hash, ErrorObjectOwned> {
    let trimmed = hex.strip_prefix("0x").unwrap_or(hex);
    let bytes = alloy::primitives::hex::decode(trimmed).map_err(|err| {
        ErrorObjectOwned::owned(
            INVALID_PARAMS_CODE,
            format!("invalid hash {hex:?}: {err}"),
            None::<()>,
        )
    })?;
    let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
        ErrorObjectOwned::owned(
            INVALID_PARAMS_CODE,
            format!(
                "invalid hash {hex:?}: expected 32 bytes (64 hex chars), got {}",
                bytes.len()
            ),
            None::<()>,
        )
    })?;
    Ok(Hash::from_bytes(arr))
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
    async fn health(&self) -> RpcResult<HealthResponse> {
        Ok(HealthResponse {
            node_id: alloy::primitives::hex::encode(self.state.node_id),
            uptime_s: self.state.started_at.elapsed().as_secs(),
        })
    }

    async fn evict(&self, req: EvictRequest) -> RpcResult<EvictResponse> {
        let hash = parse_hash_arg(&req.hash)?;

        // Snapshot presence first so we can report a meaningful
        // `was_present` to the operator. Any cache I/O failure here is a
        // genuine problem (the iroh-blobs store is misbehaving), not a
        // routine "blob is absent" path — surface it.
        let was_present = self
            .state
            .cache
            .has(hash)
            .await
            .map_err(|err| cache_error_to_rpc(&err))?;

        self.state.cache.evict(hash);

        let repinned = if req.re_pin {
            // Clear the eviction flag so `get` is allowed to pull from
            // the origin instead of short-circuiting on the flag (#279).
            self.state.cache.unevict(hash);
            match self.state.cache.get(hash).await {
                Ok(_) => true,
                Err(err) => {
                    // Pull-through failed: re-evict so the node doesn't
                    // serve a possibly-stale local copy and surface the
                    // origin's error to the operator.
                    self.state.cache.evict(hash);
                    return Err(cache_error_to_rpc(&err));
                }
            }
        } else {
            false
        };

        Ok(EvictResponse {
            was_present,
            repinned,
        })
    }

    async fn announce(&self) -> RpcResult<AnnounceResponse> {
        let trigger = self.state.announce_trigger.as_ref().ok_or_else(|| {
            ErrorObjectOwned::owned(
                PUBLISHER_DISABLED_CODE,
                "gossip publisher is disabled (no identity.region configured); \
                 set a region in the node config and restart to enable announces",
                None::<()>,
            )
        })?;
        trigger.announce_now();
        Ok(AnnounceResponse { triggered: true })
    }

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

    /// Build a minimal in-memory cache for tests. The peers / health
    /// methods don't touch it, but `AdminState::new` requires one — using
    /// a tempdir keeps each test self-contained.
    async fn test_cache() -> (CacheEngine, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = CacheEngine::open(tmp.path(), None, 1)
            .await
            .expect("cache open");
        (cache, tmp)
    }

    async fn state_with(peers: Vec<([u8; 32], &str, u64, u64)>) -> (AdminState, tempfile::TempDir) {
        let mut table = PeerTable::new(0);
        for (id, region, ts_us, now_us) in peers {
            table
                .insert_or_refresh(mk_announce(id, region, ts_us), now_us)
                .expect("seed insert succeeds");
        }
        let (cache, tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(table)),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
        );
        (state, tmp)
    }

    #[tokio::test]
    async fn empty_peer_table_returns_empty_vec() {
        let (state, _tmp) = state_with(vec![]).await;
        let rpc = AdminRpcImpl::new(state);
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
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(table)),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
        );
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
    async fn health_returns_hex_node_id_and_nondecreasing_uptime() {
        let id = [0xCDu8; 32];
        let started = Instant::now();
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0))),
            id,
            started,
            cache,
            None,
        );
        let rpc = AdminRpcImpl::new(state);

        let first = rpc.health().await.expect("health ok");
        assert_eq!(first.node_id, "cd".repeat(32));

        // Uptime is monotonic non-decreasing across calls — `Instant`
        // is monotonic, so a second call after at least one elapsed-tick
        // worth of work must report a value >= the first.
        let second = rpc.health().await.expect("health ok");
        assert!(
            second.uptime_s >= first.uptime_s,
            "uptime regressed: {} -> {}",
            first.uptime_s,
            second.uptime_s,
        );
    }

    #[tokio::test]
    async fn peers_sorted_by_last_seen_descending() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let c = [3u8; 32];
        // (id, region, announce ts, now_us) — `now_us` becomes last_seen_us.
        let (state, _tmp) = state_with(vec![
            (a, "US", 1, 500),
            (b, "EU", 1, 700),
            (c, "AP", 1, 600),
        ])
        .await;
        let rpc = AdminRpcImpl::new(state);
        let resp = rpc.peers_list().await.expect("peers_list ok");
        let order: Vec<u64> = resp.peers.iter().map(|p| p.last_seen_us).collect();
        assert_eq!(order, vec![700, 600, 500]);
    }

    /// Evict round-trip: `admin_v1_evict` of a hex hash that's been pulled
    /// into the cache returns `was_present: true` and subsequent gets fail
    /// with `NotFound`. Without this, a regression that no-op'd
    /// `admin_v1_evict` would leak through unit tests.
    #[tokio::test]
    async fn admin_evict_blocks_subsequent_serve() -> anyhow::Result<()> {
        use bytes::Bytes;
        use std::future::Future;
        use std::pin::Pin;

        // Inline stub origin so we don't have to depend on a test-only
        // `decdn-cache` export. Single-blob, hash matches payload.
        #[derive(Debug)]
        struct StubOrigin {
            data: Bytes,
            hash: Hash,
        }
        impl decdn_cache::Origin for StubOrigin {
            fn fetch(
                &self,
                hash: Hash,
                _max_bytes: u64,
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<decdn_cache::OriginFetch>> + Send + '_>>
            {
                let result = if hash == self.hash {
                    Ok(decdn_cache::OriginFetch::Found(self.data.clone()))
                } else {
                    Ok(decdn_cache::OriginFetch::NotFound)
                };
                Box::pin(async move { result })
            }
        }

        let payload = b"admin evict";
        let hash = Hash::new(payload);
        let tmp = tempfile::tempdir()?;
        let origin = Arc::new(StubOrigin {
            data: Bytes::from(payload.to_vec()),
            hash,
        }) as Arc<dyn decdn_cache::Origin>;
        let cache = CacheEngine::open(tmp.path(), Some(origin), 1).await?;

        // Prime the cache with the blob so the evict has something to remove.
        let _ = cache.get(hash).await?;

        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0))),
            [0u8; 32],
            Instant::now(),
            cache.clone(),
            None,
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc
            .evict(EvictRequest {
                hash: alloy::primitives::hex::encode(hash.as_bytes()),
                re_pin: false,
            })
            .await
            .expect("evict ok");
        assert!(resp.was_present, "expected was_present=true");
        assert!(!resp.repinned, "expected repinned=false (no --re-pin)");

        match cache.get(hash).await {
            Err(CacheError::NotFound { .. }) => Ok(()),
            other => Err(anyhow::anyhow!(
                "expected NotFound after admin evict, got {other:?}"
            )),
        }
    }

    #[tokio::test]
    async fn admin_evict_rejects_bad_hex() {
        let (state, _tmp) = state_with(vec![]).await;
        let rpc = AdminRpcImpl::new(state);
        let err = rpc
            .evict(EvictRequest {
                hash: "not-hex".into(),
                re_pin: false,
            })
            .await
            .expect_err("expected invalid-params error");
        // INVALID_PARAMS_CODE; double-checked here so a typo'd code constant
        // still surfaces as a test failure.
        assert_eq!(err.code(), -32_602);
    }

    #[tokio::test]
    async fn admin_announce_without_publisher_returns_publisher_disabled() {
        // No region configured → AnnounceTrigger absent → method must
        // return PUBLISHER_DISABLED rather than silently succeeding.
        let (state, _tmp) = state_with(vec![]).await;
        let rpc = AdminRpcImpl::new(state);
        let err = rpc.announce().await.expect_err("expected error");
        assert_eq!(err.code(), -32_001);
    }

    #[tokio::test]
    async fn admin_announce_fires_trigger_when_publisher_present() {
        use tokio::sync::Notify;

        let notify = Arc::new(Notify::new());
        // `AnnounceTrigger::announce_now` calls `notify_one` on this same
        // `Notify`. We construct one matching the trigger's internal
        // shape via a public helper or bypass — since `AnnounceTrigger`'s
        // field is private, we route through `notify_one` indirectly by
        // using the public `from_notify` test seam below.
        let trigger = Arc::new(decdn_gossip::AnnounceTrigger::for_test(Arc::clone(&notify)));
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0))),
            [0u8; 32],
            Instant::now(),
            cache,
            Some(trigger),
        );
        let rpc = AdminRpcImpl::new(state);

        // Park a `notified()` future so the announce_now permit is
        // observable. Notify::notify_one stores a permit if no waiter is
        // pending; the awaitable below claims it.
        let resp = rpc.announce().await.expect("announce ok");
        assert!(resp.triggered);

        // The published permit is observable as an immediate `notified()`
        // resolution. Use a short timeout so a regression that lost the
        // notify still fails fast.
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), notify.notified()).await;
        assert!(
            waited.is_ok(),
            "announce_now did not fire the underlying Notify"
        );
    }
}
