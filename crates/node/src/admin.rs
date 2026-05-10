//! Loopback-only admin JSON-RPC server (ADR 025).
//!
//! Wire types (the `AdminRpc` trait, DTOs, error codes, [`parse_hash_arg`]) live
//! in [`decdn_common::admin`] so the user-facing `decdn` CLI can speak the
//! generated client without dragging in the daemon's runtime
//! dependencies. This module keeps the server-side implementation:
//! [`AdminState`], the [`AdminRpcImpl`] that backs the trait against a
//! live cache + peer table, and the bind/serve helpers the runtime calls
//! during start-up and graceful shutdown.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{CacheEngine, CacheError};
use decdn_common::admin::{
    AdminRpcServer, AnnounceResponse, CACHE_ERROR_CODE, CONFIG_PATH_UNSET_CODE, DrainResponse,
    EvictPreview, EvictRequest, EvictResponse, HealthResponse, PUBLISHER_DISABLED_CODE, PeerView,
    PeersResponse, RELOAD_ERROR_CODE, ReloadResponse, parse_hash_arg,
};
use decdn_gossip::{AnnounceTrigger, PeerEntry, PeerTable};
use jsonrpsee::core::{RpcResult, async_trait};
use jsonrpsee::server::{Server, ServerConfig};
use jsonrpsee::types::ErrorObjectOwned;
use tokio::net::TcpListener;
use tokio::sync::{Notify, RwLock, oneshot};

// Wire types live in `decdn_common::admin`. We import the server-side
// trait, the request DTO, and the few error codes the server impl
// raises — clients reach the response DTOs through `decdn_common`
// directly.

use crate::runtime::RuntimeReloadState;

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
    /// Hot-reload hook for `admin_v1_reload` (issue #373). `None` when
    /// the node was started without a config file path (CLI-only flag
    /// invocation), in which case there's nothing on disk for reload to
    /// re-read; the RPC method translates that into a "config path
    /// unset" error so the operator gets a specific message instead of
    /// silently no-op'ing.
    reload_hook: Option<ReloadHook>,
    /// Drain trigger for `admin_v1_drain` (issue #244). Always present —
    /// drain has no preconditions analogous to "publisher disabled" or
    /// "no config path", so this field is `Arc<DrainTrigger>` (not
    /// `Option<…>` like `announce_trigger` / `reload_hook`) and the
    /// runtime wires it unconditionally.
    drain_trigger: Arc<DrainTrigger>,
    /// Loaded Ethereum keystore signer (issue #406). Required because the
    /// resolved blockchain config validates the keystore at startup, so any
    /// successful runtime is guaranteed to have a signer. Stored here
    /// because `AdminState` is the cross-runtime shared-state struct that
    /// already plumbs to multiple consumers; future non-RPC consumers
    /// (vouchers per #319, on-chain txs per #327) clone this `Arc`. The
    /// `Debug` impl on `PrivateKeySigner` prints the address only — never
    /// the secret scalar.
    #[allow(dead_code)] // Wired in #406; consumed by #319 / #327.
    signer: Arc<PrivateKeySigner>,
}

/// One-shot trigger that lets `admin_v1_drain` wake the runtime's main
/// select loop and request graceful shutdown (issue #244). Modelled on
/// [`tokio::sync::Notify`] so the RPC handler can fire-and-return without
/// blocking on the runtime's shutdown latency. The runtime side awaits
/// `wait()` in its select loop; firing twice is a no-op (the second
/// `notify_one` coalesces, same as `Notify`).
#[derive(Debug, Default)]
pub struct DrainTrigger {
    notify: Notify,
}

impl DrainTrigger {
    /// Create a new `DrainTrigger`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal the runtime to begin graceful shutdown. Fire-and-forget:
    /// calling this more than once is a no-op (the second `notify_one`
    /// coalesces into the pending permit the first call stored).
    pub fn fire(&self) {
        self.notify.notify_one();
    }

    /// Wait until [`fire`](Self::fire) is called. Returns immediately if
    /// `fire` was already called before `wait` was polled.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

/// Pair of values needed by `admin_v1_reload`: the runtime's reload state
/// (the same `Arc` the SIGHUP arm holds in `runtime::run`'s select loop)
/// and the path to the config file the operator started with. Bundled so
/// `AdminState` can carry "both or neither" as a single `Option`, matching
/// the runtime's "no path → no reload" invariant.
#[derive(Debug, Clone)]
pub struct ReloadHook {
    pub reload_state: Arc<RuntimeReloadState>,
    pub config_path: PathBuf,
}

impl AdminState {
    // Now at 8 params after the #406 signer addition. A builder would be
    // tidier but is out of scope for #406 — defer until a third call site
    // appears.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        peer_table: Arc<RwLock<PeerTable>>,
        node_id: [u8; 32],
        started_at: Instant,
        cache: CacheEngine,
        announce_trigger: Option<Arc<AnnounceTrigger>>,
        reload_hook: Option<ReloadHook>,
        drain_trigger: Arc<DrainTrigger>,
        signer: Arc<PrivateKeySigner>,
    ) -> Self {
        Self {
            peer_table,
            node_id,
            started_at,
            cache,
            announce_trigger,
            reload_hook,
            drain_trigger,
            signer,
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

    fn into_view(self) -> PeerView {
        PeerView {
            node_id: alloy::primitives::hex::encode(self.node_id),
            region: self.region,
            first_seen_us: self.first_seen_us,
            last_seen_us: self.last_seen_us,
            load: self.load,
            announced_at_us: self.announced_at_us,
        }
    }
}

/// Convert a [`CacheError`] into a JSON-RPC error suitable for
/// `admin_v1_evict`. Distinguished mainly so the operator-facing message
/// can name the underlying failure mode (`NotFound`, `OriginError`, etc.)
/// rather than a generic "cache failed".
fn cache_error_to_rpc(err: &CacheError) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(CACHE_ERROR_CODE, err.to_string(), None::<()>)
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

        // One `inspect` call snapshots size, pin, evicted, served — the
        // pre-evict state the response will carry. Any cache I/O
        // failure here is a genuine problem (the iroh-blobs store is
        // misbehaving), not a routine "blob is absent" path. Note that
        // `inspect` reads `BlobStatus` directly so the size we report
        // is the on-disk byte count, even when `already_evicted` is
        // already true — operators want to see disk-reclaim potential.
        let preview = self
            .state
            .cache
            .inspect(hash)
            .await
            .map_err(|err| cache_error_to_rpc(&err))?;

        // For a real evict we mutate after the inspect. For a dry-run
        // we skip the `evict()` call entirely — the response is the
        // snapshot only.
        if !req.dry_run {
            // `evict` returns `Err` if it can't durably persist the
            // eviction (e.g. evicted-log fsync failed). For DMCA
            // takedowns the operator must learn about that failure
            // rather than getting an "ok" response that silently
            // degraded to in-memory-only.
            self.state
                .cache
                .evict(hash)
                .map_err(|err| cache_error_to_rpc(&err))?;
        }

        Ok(EvictResponse {
            was_present: preview.served,
            dry_run: req.dry_run,
            preview: EvictPreview {
                size_bytes: preview.size_bytes,
                last_accessed_us_ago: preview.last_accessed_us_ago,
                pinned: preview.pinned,
                already_evicted: preview.already_evicted,
                origin_kind: preview.origin_kind,
            },
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

    async fn reload(&self) -> RpcResult<ReloadResponse> {
        let hook = self.state.reload_hook.as_ref().ok_or_else(|| {
            ErrorObjectOwned::owned(
                CONFIG_PATH_UNSET_CODE,
                "no config file path is in use; restart the node with \
                 --config <path> to enable admin_v1_reload",
                None::<()>,
            )
        })?;
        // Delegating to `RuntimeReloadState::reload` keeps the SIGHUP
        // and RPC paths byte-identical: same parse, same resolution,
        // same "previous values retained on error" contract, and the
        // same internal mutexes serialise concurrent reloads (a SIGHUP
        // arriving mid-RPC waits, and vice versa). Surface the error
        // text via `{err:#}` so the operator sees the underlying
        // resolution failure rather than a generic wrapper.
        hook.reload_state
            .reload(&hook.config_path)
            .await
            .map_err(|err| {
                ErrorObjectOwned::owned(
                    RELOAD_ERROR_CODE,
                    format!("config reload failed: {err:#}"),
                    None::<()>,
                )
            })?;
        let snap = hook.reload_state.current();
        Ok(ReloadResponse {
            rate_per_mb: snap.rate_per_mb,
            // After a successful reload `current_log_level` is `Some`;
            // the `unwrap_or` branch is the (unreachable in practice)
            // poisoned-mutex case where `current()` returned `None`.
            // Returning a stable string ("unknown") rather than the
            // empty string makes operator scripts that parse the
            // response trivially unambiguous.
            log_level: snap
                .log_level
                .map_or_else(|| "unknown".to_string(), |l| l.to_string()),
        })
    }

    async fn drain(&self) -> RpcResult<DrainResponse> {
        // `initiated: true` reflects "the trigger was fired", not "the
        // runtime is now in the AdminDrain branch". If shutdown is
        // already underway (a SIGTERM/SIGINT raced this RPC) the runtime
        // has already passed `drain_trigger.wait()` in its select loop,
        // so this `fire()` lands in an abandoned arm. Functionally fine
        // — shutdown is happening anyway — but a future reader shouldn't
        // infer causation from the response.
        self.state.drain_trigger.fire();
        Ok(DrainResponse { initiated: true })
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
        let mut snapshot: Vec<PeerView> = raw.into_iter().map(RawPeer::into_view).collect();
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
    use decdn_cache::Hash;
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

    /// Build a throwaway tempdir-backed cache for tests. The peers /
    /// health methods don't touch it, but `AdminState::new` requires one
    /// — wrapping the engine over a `tempfile::TempDir` keeps each test
    /// self-contained, and the returned `TempDir` must outlive the engine
    /// (callers bind it with `_tmp` so RAII handles cleanup at end of test).
    async fn test_cache() -> (CacheEngine, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = CacheEngine::open(tmp.path(), None, 1)
            .await
            .expect("cache open");
        (cache, tmp)
    }

    /// Throwaway `PrivateKeySigner` for tests that need an `AdminState` but
    /// don't exercise any signing logic. The chain id is pinned to Arbitrum
    /// Sepolia to keep parity with the real runtime — tests that *do*
    /// exercise signing should construct their own signer with the chain id
    /// under test.
    fn throwaway_signer() -> Arc<PrivateKeySigner> {
        use alloy::signers::Signer;
        use decdn_incentive::eth_identity::ARBITRUM_SEPOLIA_CHAIN_ID;
        Arc::new(PrivateKeySigner::random().with_chain_id(Some(ARBITRUM_SEPOLIA_CHAIN_ID)))
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
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
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
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
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
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
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
            fn kind(&self) -> decdn_cache::OriginKind {
                decdn_cache::OriginKind::Http
            }

            fn fetch(
                &self,
                hash: Hash,
                _max_bytes: u64,
            ) -> Pin<
                Box<
                    dyn Future<
                            Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>,
                        > + Send
                        + '_,
                >,
            > {
                let result = if hash == self.hash {
                    Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
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
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc
            .evict(EvictRequest {
                hash: alloy::primitives::hex::encode(hash.as_bytes()),
                dry_run: false,
            })
            .await
            .expect("evict ok");
        assert!(resp.was_present, "expected was_present=true");
        assert!(!resp.dry_run, "real evict must not set dry_run");

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
                dry_run: false,
            })
            .await
            .expect_err("expected invalid-params error");
        // INVALID_PARAMS_CODE; double-checked here so a typo'd code constant
        // still surfaces as a test failure.
        assert_eq!(err.code(), -32_602);
    }

    #[tokio::test]
    async fn admin_evict_accepts_uppercase_0x_prefix() {
        // `0X` and uppercase hex must both be tolerated; this guards
        // against the case-sensitive `strip_prefix("0x")` regression
        // flagged in PR review.
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
        );
        let rpc = AdminRpcImpl::new(state);
        let hash = Hash::new(b"prefix-test");
        let upper = format!(
            "0X{}",
            alloy::primitives::hex::encode(hash.as_bytes()).to_uppercase()
        );
        let resp = rpc
            .evict(EvictRequest {
                hash: upper,
                dry_run: false,
            })
            .await
            .expect("0X-prefixed uppercase hex should parse");
        // was_present=false because the test cache has no origin and we
        // never `get`-ed the hash; the parse alone must succeed.
        assert!(!resp.was_present);
    }

    /// `admin_v1_evict { dry_run: true }` returns the pre-evict
    /// snapshot but does *not* mutate cache state — a follow-up `has`
    /// must still report the blob present, and a follow-up `get` must
    /// still serve. Without this assertion a regression that ignored
    /// the flag and ran the real `evict()` would silently slip through
    /// (the response shape is the same; the side-effect is what
    /// matters).
    #[tokio::test]
    async fn admin_evict_dry_run_does_not_mutate() -> anyhow::Result<()> {
        use bytes::Bytes;
        use std::future::Future;
        use std::pin::Pin;

        #[derive(Debug)]
        struct StubOrigin {
            data: Bytes,
            hash: Hash,
        }
        impl decdn_cache::Origin for StubOrigin {
            fn kind(&self) -> decdn_cache::OriginKind {
                decdn_cache::OriginKind::Http
            }

            fn fetch(
                &self,
                hash: Hash,
                _max_bytes: u64,
            ) -> Pin<
                Box<
                    dyn Future<
                            Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>,
                        > + Send
                        + '_,
                >,
            > {
                let result = if hash == self.hash {
                    Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
                } else {
                    Ok(decdn_cache::OriginFetch::NotFound)
                };
                Box::pin(async move { result })
            }
        }

        let payload = b"dry-run preview";
        let hash = Hash::new(payload);
        let tmp = tempfile::tempdir()?;
        let origin = Arc::new(StubOrigin {
            data: Bytes::from(payload.to_vec()),
            hash,
        }) as Arc<dyn decdn_cache::Origin>;
        let cache = CacheEngine::open(tmp.path(), Some(origin), 1).await?;
        let _ = cache.get(hash).await?;

        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0))),
            [0u8; 32],
            Instant::now(),
            cache.clone(),
            None,
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc
            .evict(EvictRequest {
                hash: alloy::primitives::hex::encode(hash.as_bytes()),
                dry_run: true,
            })
            .await
            .expect("dry-run evict ok");

        // Wire shape — every dry-run-only field is meaningful.
        assert!(resp.dry_run, "expected dry_run=true on response");
        assert!(resp.was_present, "blob primed via get(); should be served");
        assert_eq!(
            resp.preview.size_bytes,
            Some(payload.len() as u64),
            "expected size_bytes={}, got {:?}",
            payload.len(),
            resp.preview.size_bytes,
        );
        assert!(
            resp.preview.last_accessed_us_ago.is_some(),
            "expected Some(last_accessed_us_ago) after get()"
        );
        assert!(!resp.preview.pinned);
        assert!(
            !resp.preview.already_evicted,
            "dry-run must not flip evicted flag"
        );
        // Origin egress-cost cue (#439). Engine here is configured
        // with an origin, so the preview must carry a non-None tag —
        // the StubOrigin reports `Http`.
        assert_eq!(
            resp.preview.origin_kind,
            Some(decdn_cache::OriginKind::Http),
            "expected Some(Http), got {:?}",
            resp.preview.origin_kind,
        );

        // Cache state untouched: the blob is still served, the
        // evicted-log entry was not created, and no fsync hit disk.
        assert!(
            !cache.is_evicted(hash),
            "dry-run must not commit to evicted set"
        );
        assert!(cache.has(hash).await?, "dry-run must not stop serve");
        assert!(
            !tmp.path().join("evicted.log").exists(),
            "dry-run must not create evicted.log"
        );
        Ok(())
    }

    /// A real evict followed by a dry-run on the same hash must report
    /// `already_evicted: true` and `was_present: false` — the operator
    /// is using dry-run to confirm an idempotent re-run is in fact a
    /// no-op. The size field still reports the on-disk bytes since
    /// the iroh-blobs store hasn't been GC'd yet (#233).
    #[tokio::test]
    async fn admin_evict_dry_run_after_real_evict_reports_already_evicted() -> anyhow::Result<()> {
        use bytes::Bytes;
        use std::future::Future;
        use std::pin::Pin;

        #[derive(Debug)]
        struct StubOrigin {
            data: Bytes,
            hash: Hash,
        }
        impl decdn_cache::Origin for StubOrigin {
            fn kind(&self) -> decdn_cache::OriginKind {
                decdn_cache::OriginKind::Http
            }

            fn fetch(
                &self,
                hash: Hash,
                _max_bytes: u64,
            ) -> Pin<
                Box<
                    dyn Future<
                            Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>,
                        > + Send
                        + '_,
                >,
            > {
                let result = if hash == self.hash {
                    Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
                } else {
                    Ok(decdn_cache::OriginFetch::NotFound)
                };
                Box::pin(async move { result })
            }
        }

        let payload = b"already-evicted preview";
        let hash = Hash::new(payload);
        let tmp = tempfile::tempdir()?;
        let origin = Arc::new(StubOrigin {
            data: Bytes::from(payload.to_vec()),
            hash,
        }) as Arc<dyn decdn_cache::Origin>;
        let cache = CacheEngine::open(tmp.path(), Some(origin), 1).await?;
        let _ = cache.get(hash).await?;
        cache.evict(hash)?;

        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc
            .evict(EvictRequest {
                hash: alloy::primitives::hex::encode(hash.as_bytes()),
                dry_run: true,
            })
            .await
            .expect("dry-run evict ok");

        assert!(resp.dry_run);
        assert!(
            !resp.was_present,
            "post-evict has() should report absent → was_present=false"
        );
        assert!(
            resp.preview.already_evicted,
            "expected already_evicted=true on a re-run"
        );
        // On-disk size still reported — operators want to see the
        // disk-reclaim potential even though `served=false`.
        assert_eq!(resp.preview.size_bytes, Some(payload.len() as u64));
        Ok(())
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

        // `AnnounceTrigger::announce_now` is a thin wrapper that calls
        // `notify_one` on the inner `Arc<Notify>`. We construct the
        // trigger with a `Notify` we own (via the `for_test` seam, which
        // is `#[doc(hidden)]` and exists for exactly this assertion path)
        // so we can `.notified()` after the RPC fires and observe that
        // the permit landed. This proves the admin handler -> trigger ->
        // notify chain end-to-end without spinning up a real publisher
        // task; the publisher's own `select!` arm is exercised by
        // `service::tests::publisher_publishes_on_announce_trigger`.
        let notify = Arc::new(Notify::new());
        let trigger = Arc::new(decdn_gossip::AnnounceTrigger::for_test(Arc::clone(&notify)));
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0))),
            [0u8; 32],
            Instant::now(),
            cache,
            Some(trigger),
            None,
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc.announce().await.expect("announce ok");
        assert!(resp.triggered);

        // `Notify::notify_one` stores a permit if no waiter is pending;
        // calling `notified()` after the RPC claims that permit
        // immediately. A short timeout makes a regression that lost the
        // notify fail fast rather than hanging the test runner.
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), notify.notified()).await;
        assert!(
            waited.is_ok(),
            "announce_now did not fire the underlying Notify"
        );
    }

    /// `admin_v1_reload` on a node with no `ReloadHook` (started without
    /// `--config`) must surface `CONFIG_PATH_UNSET_CODE` rather than a
    /// generic failure, so an operator script can tell "no path on disk
    /// to re-read" from "reload tried and failed".
    #[tokio::test]
    async fn admin_reload_without_hook_returns_config_path_unset() {
        let (state, _tmp) = state_with(vec![]).await;
        let rpc = AdminRpcImpl::new(state);
        let err = rpc.reload().await.expect_err("expected error");
        assert_eq!(err.code(), -32_003);
    }

    /// Happy path: a hook pointing at a valid config file applies the
    /// reload via the same `RuntimeReloadState::reload` SIGHUP uses, and
    /// the response carries the post-reload `rate_per_mb` and
    /// `log_level`. Asserts both wire-format fields so a regression that
    /// dropped one (or stringified the level wrong) fails the unit test.
    #[tokio::test]
    async fn admin_reload_applies_and_returns_post_reload_values() {
        use crate::runtime::RuntimeReloadState;
        use decdn_common::cli::common::LogLevel;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("node.toml");
        std::fs::write(
            &path,
            "[payment]\nrate_per_mb = 99\n\n[observability]\nlog_level = \"debug\"\n",
        )
        .expect("write config");

        let setter: crate::runtime::LogLevelSetter = Box::new(|_| Ok(()));
        let reload_state = Arc::new(RuntimeReloadState::for_test_with_setter(
            10,
            LogLevel::Info,
            setter,
        ));
        let hook = ReloadHook {
            reload_state: Arc::clone(&reload_state),
            config_path: path.clone(),
        };
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            Some(hook),
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc.reload().await.expect("reload ok");
        assert_eq!(resp.rate_per_mb, 99);
        assert_eq!(resp.log_level, "debug");
    }

    /// `DrainTrigger::fire` followed by `wait()` resolves. The Notify
    /// stores a permit when no waiter is present, so the `notified()`
    /// future claims it immediately — no race window or ordering
    /// requirement between `fire` and `wait` in tests.
    #[tokio::test]
    async fn drain_trigger_fire_then_wait_resolves() {
        let trigger = DrainTrigger::new();
        trigger.fire();
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
        assert!(waited.is_ok(), "wait() did not resolve after fire()");
    }

    /// Firing repeatedly must not deadlock a subsequent `wait`. `Notify`
    /// coalesces multiple `notify_one` calls into a single permit, so the
    /// second and third `fire` while no waiter is pending are no-ops and
    /// the *first* permit is still available for the next `wait`. Catches
    /// a future reimplementation that internally tracks "fired" state and
    /// burns one permit per call (e.g. a hand-rolled `Mutex<bool>` that
    /// returns a never-resolving future on the second call). The third
    /// fire makes the test robust against a "burns one permit per fire"
    /// bug — two would still resolve under that buggy implementation if
    /// the first fire stored a permit and the second consumed it before
    /// `wait` was polled.
    #[tokio::test]
    async fn drain_trigger_repeated_fire_does_not_deadlock_wait() {
        let trigger = DrainTrigger::new();
        trigger.fire();
        trigger.fire(); // second fire — coalesces, permit still available
        trigger.fire(); // third fire — same coalesce; defends against the
        // "burns one permit per fire" regression class
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
        assert!(
            waited.is_ok(),
            "wait() did not resolve after three fire() calls"
        );
    }

    /// `admin_v1_drain` fires the trigger and returns `initiated: true`.
    /// Mirrors the `admin_announce_fires_trigger_when_publisher_present`
    /// test — we verify the RPC handler -> trigger -> Notify chain end-to-end
    /// without spinning up a full runtime.
    #[tokio::test]
    async fn admin_drain_fires_trigger_and_returns_initiated() {
        let trigger = Arc::new(DrainTrigger::new());
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            None,
            Arc::clone(&trigger),
            throwaway_signer(),
        );
        let rpc = AdminRpcImpl::new(state);

        let resp = rpc.drain().await.expect("drain ok");
        assert!(resp.initiated, "expected initiated=true");

        // Verify the trigger actually fired: `wait()` should resolve
        // immediately because the Notify stored a permit.
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(100), trigger.wait()).await;
        assert!(
            waited.is_ok(),
            "drain RPC did not fire the underlying DrainTrigger"
        );
    }

    /// Failure path: a hook pointing at a missing file must surface
    /// `RELOAD_ERROR_CODE` and the underlying reload state must keep its
    /// previous values (the SIGHUP arm's "previous values retained"
    /// contract — we route through the same code, so this is a check
    /// that the RPC layer didn't accidentally swap an `Ok(...)` somewhere
    /// in the error mapping).
    #[tokio::test]
    async fn admin_reload_with_missing_config_returns_reload_error() {
        use crate::runtime::RuntimeReloadState;
        use decdn_common::cli::common::LogLevel;

        let dir = tempfile::tempdir().expect("tempdir");
        // Path inside a tempdir that we never write to — guaranteed
        // missing without depending on filesystem state outside the test.
        let path = dir.path().join("does-not-exist.toml");

        let setter: crate::runtime::LogLevelSetter = Box::new(|_| Ok(()));
        let reload_state = Arc::new(RuntimeReloadState::for_test_with_setter(
            42,
            LogLevel::Info,
            setter,
        ));
        let hook = ReloadHook {
            reload_state: Arc::clone(&reload_state),
            config_path: path,
        };
        let (cache, _tmp) = test_cache().await;
        let state = AdminState::new(
            Arc::new(RwLock::new(PeerTable::new(0))),
            [0u8; 32],
            Instant::now(),
            cache,
            None,
            Some(hook),
            Arc::new(DrainTrigger::new()),
            throwaway_signer(),
        );
        let rpc = AdminRpcImpl::new(state);

        let err = rpc.reload().await.expect_err("expected error");
        assert_eq!(err.code(), -32_004);
        // Previous rate retained — RPC error path didn't accidentally
        // commit anything to the live state.
        assert_eq!(
            reload_state
                .rate_per_mb()
                .load(std::sync::atomic::Ordering::Relaxed),
            42,
        );
    }
}
