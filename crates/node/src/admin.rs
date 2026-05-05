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
use std::path::PathBuf;
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
use tokio::sync::{Notify, RwLock, oneshot};

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
    pub const fn new(
        peer_table: Arc<RwLock<PeerTable>>,
        node_id: [u8; 32],
        started_at: Instant,
        cache: CacheEngine,
        announce_trigger: Option<Arc<AnnounceTrigger>>,
        reload_hook: Option<ReloadHook>,
        drain_trigger: Arc<DrainTrigger>,
    ) -> Self {
        Self {
            peer_table,
            node_id,
            started_at,
            cache,
            announce_trigger,
            reload_hook,
            drain_trigger,
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
    /// Hex of the BLAKE3 hash to evict. Must be exactly 64 hex characters
    /// (mixed case accepted, optional `0x`/`0X` prefix tolerated); rejected
    /// at the server with `INVALID_PARAMS` otherwise.
    pub hash: String,
    /// If `true`, return only the pre-evict snapshot (size, last access,
    /// pin status, already-evicted flag) without mutating cache state
    /// (issue #379). Backs `decdn node evict --dry-run`. Defaulted via
    /// `serde(default)` so older clients sending `{ "hash": "..." }`
    /// continue to parse cleanly with no flag, preserving the prior
    /// "real evict" behaviour.
    #[serde(default)]
    pub dry_run: bool,
}

/// Response body for `admin_v1_evict`.
///
/// Carries both the pre-evict snapshot (always populated, so an operator's
/// audit log captures size and pin status at the moment of evict) and a
/// `dry_run` flag indicating whether the cache state was actually mutated.
/// Older clients that only deserialize `was_present` are unaffected — the
/// new fields are silently dropped on their side.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EvictResponse {
    /// Whether the hash would have been served by the cache before this
    /// call (i.e. `BlobStatus::Complete` *and* not already evicted).
    /// `false` means the evict was a no-op safety measure (blob never
    /// cached, or already in `evicted.log`). For a real evict the
    /// effect is durable; for a dry-run no effect is committed and the
    /// `was_present` snapshot describes the current state only.
    pub was_present: bool,
    /// `true` when this response describes a dry-run preview — the
    /// cache state was *not* mutated and only [`Self::preview`] is
    /// meaningful. `false` (the default) means the eviction was
    /// applied per the existing `admin_v1_evict` behaviour.
    #[serde(default)]
    pub dry_run: bool,
    /// Pre-evict snapshot of the blob's local-cache state (#379).
    /// Populated for both real and dry-run calls so an operator's
    /// audit log captures size and pin status at the moment of
    /// evict. The struct is nested (rather than flattened into
    /// [`Self`]) so the `--dry-run` view doesn't push the bool count
    /// past the `clippy::struct_excessive_bools` threshold and so a
    /// future addition to the snapshot doesn't churn the top-level
    /// response shape.
    #[serde(default)]
    pub preview: EvictPreview,
}

/// Pre-evict snapshot returned inside [`EvictResponse::preview`].
/// Mirrors [`decdn_cache::EvictionPreview`] minus the engine-internal
/// `served` field (folded into [`EvictResponse::was_present`]).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EvictPreview {
    /// Bytes the iroh-blobs store reports for this hash, read straight
    /// from the underlying store regardless of evicted-log state.
    /// `None` when the blob isn't in the store. `Partial` blobs (an
    /// interrupted pull) report whatever size the store has so far —
    /// operators can spot a half-finished pull while inspecting.
    #[serde(default)]
    pub size_bytes: Option<u64>,
    /// Microseconds elapsed since the last `get()` against this hash.
    /// `None` when no access has been recorded — typical for a hash
    /// that was just inserted but never re-served, or one that has
    /// been logically evicted (eviction clears the access entry).
    #[serde(default)]
    pub last_accessed_us_ago: Option<u64>,
    /// Whether the hash is in the operator-pinned set (#276).
    /// Pinning protects against LRU eviction but **not** against an
    /// explicit `admin_v1_evict`; surfaced here so dry-run callers
    /// can confirm policy state before issuing the real takedown.
    #[serde(default)]
    pub pinned: bool,
    /// Whether the hash is already in `<cache_dir>/evicted.log`.
    /// `true` means a real `admin_v1_evict` would short-circuit
    /// (idempotent re-run, no log line appended).
    #[serde(default)]
    pub already_evicted: bool,
}

/// Response body for `admin_v1_announce` (issue #280).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnounceResponse {
    /// Always `true` on a non-error response — the request landed on the
    /// publisher task's notify slot. This is "queued", not "delivered":
    /// the actual gossip broadcast happens asynchronously after this RPC
    /// returns and may still fail (no neighbors, transport error), in
    /// which case the publisher emits a `warn!` log line. The "publisher
    /// disabled" case (no region configured) returns
    /// `PUBLISHER_DISABLED_CODE` rather than `triggered: false` so
    /// operators get a specific message.
    pub triggered: bool,
}

/// Response body for `admin_v1_reload` (issue #373). Reports the
/// post-reload values the SIGHUP arm logs to stdout, so operators using
/// the RPC path get the same after-state confirmation without scraping
/// `tracing` output. Only the *reloadable* fields appear here: changes
/// to non-reloadable sections are logged by the reload path itself
/// (one `info!` per changed-but-ignored field) and aren't echoed in
/// the RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReloadResponse {
    /// `payment.rate_per_mb` after the reload. Atomic-loaded *after*
    /// `RuntimeReloadState::reload` returns `Ok`, so the reported value
    /// is the one any new probe handler request will see.
    pub rate_per_mb: u64,
    /// `observability.log_level` after the reload, lowercase
    /// (`"trace"` / `"debug"` / `"info"` / `"warn"` / `"error"`) — same
    /// spelling the resolver and config file accept. `"unknown"` only
    /// appears on the (unreachable post-success) path where the reload
    /// committed a level but the snapshot mutex was poisoned by a
    /// concurrent reader; emitted as a string rather than a literal so
    /// operator scripts can parse one stable shape.
    pub log_level: String,
}

/// Response body for `admin_v1_drain` (issue #244). Always `initiated:
/// true` on a non-error response — drain is fire-and-forget; the runtime
/// begins the same graceful sequence SIGTERM triggers, and the admin server
/// is among the first surfaces to stop (metrics first, then admin, both
/// before `router.shutdown`), so an operator that needs to observe
/// completion polls process exit (systemd/K8s) or `decdn node health`
/// until the connection is refused.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrainResponse {
    /// Always `true` on a non-error response — the trigger has been fired
    /// and the runtime's shutdown sequence is underway. "Initiated", not
    /// "completed": the admin server may close before the response
    /// returns because the admin server is intentionally one of the first
    /// surfaces to stop during shutdown.
    pub initiated: bool,
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
/// (e.g. the underlying iroh-blobs store I/O failed when persisting the
/// evicted-hash log).
const CACHE_ERROR_CODE: i32 = -32_002;

/// JSON-RPC error code: the node was started without a config file
/// path, so `admin_v1_reload` has nothing to re-read. Distinct from
/// `RELOAD_ERROR_CODE` so an operator script can tell "this node
/// can't reload, ever, until it's restarted with `--config <path>`"
/// from "this node tried and failed".
const CONFIG_PATH_UNSET_CODE: i32 = -32_003;

/// JSON-RPC error code: `RuntimeReloadState::reload` returned an error
/// (file unreadable, malformed TOML, rejected resolution, mutex poison,
/// log-level setter failed). Mirrors the SIGHUP arm's "previous values
/// retained" guarantee — by the time this surfaces, the running config
/// is unchanged.
const RELOAD_ERROR_CODE: i32 = -32_004;

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

    /// Evict a single blob from the local cache (issue #279). The
    /// eviction is logical (the iroh-blobs store still holds the bytes
    /// until #233 lands a public `delete`) but is persisted to
    /// `<cache_dir>/evicted.log` so it survives a restart.
    ///
    /// When `req.dry_run` is `true` (issue #379) the cache state is
    /// *not* mutated: the response carries the pre-evict snapshot
    /// (size, last-access elapsed time, pin status, already-evicted
    /// flag) so operators running DMCA takedowns or
    /// corruption-recovery can confirm what the real evict will touch
    /// before committing. The same response shape is used for the
    /// real-evict path with the snapshot reflecting the
    /// pre-mutation state.
    #[method(name = "evict")]
    async fn evict(&self, req: EvictRequest) -> RpcResult<EvictResponse>;

    /// Trigger an immediate `NodeAnnounce` broadcast (issue #280). Returns
    /// `PUBLISHER_DISABLED_CODE` when the publisher is off (no region).
    #[method(name = "announce")]
    async fn announce(&self) -> RpcResult<AnnounceResponse>;

    /// Re-read the config file the node was started with and apply the
    /// reloadable subset (issue #373) — the same path SIGHUP triggers,
    /// exposed over the loopback admin surface for operators who want
    /// scripted control without `kill -HUP`. Both paths share the same
    /// `RuntimeReloadState::reload`, whose internal mutexes serialise
    /// concurrent reloads, so a SIGHUP racing this RPC waits behind it
    /// rather than corrupting state. Returns:
    ///
    /// - `CONFIG_PATH_UNSET_CODE` when the node was started without a
    ///   config path (CLI-only flag invocation has nothing to re-read).
    /// - `RELOAD_ERROR_CODE` when the reload itself fails — previous
    ///   values are retained, matching the SIGHUP behaviour.
    /// - On success: `ReloadResponse` carrying the post-reload
    ///   `rate_per_mb` and `log_level`.
    #[method(name = "reload")]
    async fn reload(&self) -> RpcResult<ReloadResponse>;

    /// Trigger graceful shutdown via the same path SIGTERM exercises (issue
    /// #244, ADR 025). Stops the iroh router's accept loop and awaits
    /// in-flight `ProtocolHandler::shutdown` calls; the subsequent task
    /// drain is bounded by the runtime's 15s `SHUTDOWN_DEADLINE` (the
    /// router-shutdown step itself is unbounded — a stuck handler hangs
    /// the runtime, only the post-router task join is timeout-gated).
    /// Fire-and-forget: the response returns as soon as the trigger lands,
    /// not when shutdown completes — the admin server is one of the first
    /// surfaces to stop, so a blocking-until-drained RPC would race its
    /// own listener closing. Equivalent to `kill -TERM <pid>` for
    /// operators who'd rather not stat the PID.
    #[method(name = "drain")]
    async fn drain(&self) -> RpcResult<DrainResponse>;
}

/// Convert a [`CacheError`] into a JSON-RPC error suitable for
/// `admin_v1_evict`. Distinguished mainly so the operator-facing message
/// can name the underlying failure mode (`NotFound`, `OriginError`, etc.)
/// rather than a generic "cache failed".
fn cache_error_to_rpc(err: &CacheError) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(CACHE_ERROR_CODE, err.to_string(), None::<()>)
}

/// Decode a 64-character hex BLAKE3 hash into [`struct@Hash`].
///
/// Goes through `alloy::primitives::hex::decode` (case-insensitive,
/// `0x`/`0X`-prefix-tolerant) rather than `Hash::from_str` because the
/// iroh-blobs implementation falls through to `data_encoding`'s base32
/// decoder for short inputs and **panics** when the decoder's output
/// buffer is the wrong size for the requested decoding. Operator-driven
/// inputs reach this path; a panic on malformed hex would tear down the
/// admin RPC handler thread instead of returning an `INVALID_PARAMS`
/// error to the caller.
fn parse_hash_arg(hex: &str) -> Result<Hash, ErrorObjectOwned> {
    let trimmed = hex
        .strip_prefix("0x")
        .or_else(|| hex.strip_prefix("0X"))
        .unwrap_or(hex);
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
            ) -> Pin<
                Box<
                    dyn Future<
                            Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>,
                        > + Send
                        + '_,
                >,
            > {
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
            None,
            Arc::new(DrainTrigger::new()),
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
                    Ok(decdn_cache::OriginFetch::Found(self.data.clone()))
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
                    Ok(decdn_cache::OriginFetch::Found(self.data.clone()))
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
        use crate::cli::common::LogLevel;
        use crate::runtime::RuntimeReloadState;

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
        use crate::cli::common::LogLevel;
        use crate::runtime::RuntimeReloadState;

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
