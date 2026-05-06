//! Wire types for the loopback admin JSON-RPC surface (ADR 025).
//!
//! Shared between the daemon (which implements [`AdminRpcServer`] over a
//! local cache + peer table) and the user CLI (which speaks
//! [`AdminRpcClient`] from `decdn node …`). jsonrpsee's
//! `#[rpc(server, client)]` macro consumes the source `AdminRpc` trait
//! and emits separate `AdminRpcServer` / `AdminRpcClient` traits in its
//! place; the original `AdminRpc` name is not a linkable rustdoc item,
//! hence the bare backticks.
//!
//! All types here are deliberately wire-only: no engine handles, no
//! peer-table types, no cache-internal types. The point is that pulling
//! `decdn-common` into the CLI does not transitively pull the daemon's
//! runtime. DTOs use simple owned values (`String`, `u64`, primitive
//! arrays) plus [`decdn_protocol::LoadHint`], a leaf protocol type.

use decdn_cache::Hash;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::types::ErrorObjectOwned;
use serde::{Deserialize, Serialize};

/// JSON view of a gossip peer entry emitted by `admin_v1_peersList`.
///
/// Defined separately from any peer-table internal type so that
/// table-internal fields (per-peer counters, debug flags, etc.) that may
/// accrete in the future can't silently leak into the wire format.
/// Transitively-included protocol types (e.g. [`decdn_protocol::LoadHint`])
/// do remain on the wire, so changes to those still need to be treated
/// as wire-format changes.
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
    /// `Instant` at the top of `decdn-node run` (before any `await`,
    /// before the RPC preflight). Computed from a monotonic `Instant`
    /// so wall-clock skew can't produce a negative or non-monotonic
    /// value.
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
/// Mirrors `decdn_cache::EvictionPreview` minus the engine-internal
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
    /// [`PUBLISHER_DISABLED_CODE`] rather than `triggered: false` so
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
pub const INVALID_PARAMS_CODE: i32 = -32_602;

/// JSON-RPC error code: the server can't satisfy this method right now.
/// Used when `admin_v1_announce` is invoked on a node whose gossip
/// publisher is disabled (no region configured) — operators get a
/// specific message instead of a generic failure.
pub const PUBLISHER_DISABLED_CODE: i32 = -32_001;

/// JSON-RPC error code: the cache layer reported an error during evict
/// (e.g. the underlying iroh-blobs store I/O failed when persisting the
/// evicted-hash log).
pub const CACHE_ERROR_CODE: i32 = -32_002;

/// JSON-RPC error code: the node was started without a config file
/// path, so `admin_v1_reload` has nothing to re-read. Distinct from
/// [`RELOAD_ERROR_CODE`] so an operator script can tell "this node
/// can't reload, ever, until it's restarted with `--config <path>`"
/// from "this node tried and failed".
pub const CONFIG_PATH_UNSET_CODE: i32 = -32_003;

/// JSON-RPC error code: `RuntimeReloadState::reload` returned an error
/// (file unreadable, malformed TOML, rejected resolution, mutex poison,
/// log-level setter failed). Mirrors the SIGHUP arm's "previous values
/// retained" guarantee — by the time this surfaces, the running config
/// is unchanged.
pub const RELOAD_ERROR_CODE: i32 = -32_004;

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
    /// [`PUBLISHER_DISABLED_CODE`] when the publisher is off (no region).
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
    /// - [`CONFIG_PATH_UNSET_CODE`] when the node was started without a
    ///   config path (CLI-only flag invocation has nothing to re-read).
    /// - [`RELOAD_ERROR_CODE`] when the reload itself fails — previous
    ///   values are retained, matching the SIGHUP behaviour.
    /// - On success: [`ReloadResponse`] carrying the post-reload
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

/// Decode a 64-character hex BLAKE3 hash into a [`struct@Hash`].
///
/// Goes through `alloy::primitives::hex::decode` (case-insensitive,
/// `0x`/`0X`-prefix-tolerant) rather than `Hash::from_str` because the
/// iroh-blobs implementation falls through to `data_encoding`'s base32
/// decoder for short inputs and **panics** when the decoder's output
/// buffer is the wrong size for the requested decoding. Operator-driven
/// inputs reach this path; a panic on malformed hex would tear down the
/// admin RPC handler thread instead of returning an `INVALID_PARAMS`
/// error to the caller.
///
/// # Errors
/// Returns an [`ErrorObjectOwned`] with [`INVALID_PARAMS_CODE`] when the
/// input is not valid hex or doesn't decode to exactly 32 bytes.
pub fn parse_hash_arg(hex: &str) -> Result<Hash, ErrorObjectOwned> {
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
