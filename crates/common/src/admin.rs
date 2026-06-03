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
//! DTO fields are deliberately wire-only: no engine handles, no
//! peer-table types, no daemon runtime types. They use simple owned
//! values (`String`, `u64`, primitive arrays). Pulling `decdn-common`
//! into the CLI does not pull the daemon runtime.
//!
//! The helper [`parse_hash_arg`] returns [`struct@Hash`] from the
//! `decdn-config-types` leaf crate (the same leaf the typed config
//! fields `DecompressMode`, `RetryPolicy`, `OriginUrl`, `PinnedHashes`
//! come from). That keeps `decdn-cache`/iroh-blobs out of the publisher
//! CLI's dependency tree (#578); the daemon's `evict` handler converts
//! the leaf hash to the blob-store hash at its boundary via
//! `decdn_cache::to_store_hash`. The parser is used only by that
//! handler today.

use decdn_config_types::Hash;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::types::ErrorObjectOwned;
use serde::{Deserialize, Serialize};

/// JSON view of a gossip peer entry emitted by `admin_v1_peersList`.
///
/// Defined separately from any peer-table internal type so that
/// table-internal fields (per-peer counters, debug flags, etc.) that may
/// accrete in the future can't silently leak into the wire format.
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
    /// Currently in-flight QUIC handler tasks holding a dispatch permit
    /// (read off the `decdn_dispatch_in_flight` gauge — see
    /// `ConnectionLimiter` in `decdn-node`). Polled by `decdn node drain
    /// --wait` (issue #604) to detect when all client streams have
    /// completed during a graceful drain. `#[serde(default)]` keeps
    /// older servers (which don't serialize the field) round-tripping
    /// cleanly through new clients as `0`.
    #[serde(default)]
    pub in_flight_streams: u64,
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
/// Mirrors the cache engine's `EvictionPreview` minus the
/// engine-internal `served` field (folded into
/// [`EvictResponse::was_present`]).
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
    /// Ordered list of origin backends the node would consult on a
    /// post-eviction miss (#439, #284). Empty when the engine has no
    /// origin configured (cache-only mode); a single-element vec is
    /// the pre-#284 common case. Operators running DMCA takedowns or
    /// LRU sweeps use this to estimate worst-case origin egress cost
    /// — re-pulling from a `Filesystem` origin is a local read;
    /// re-pulling through a chain of `Http`/`S3` mirrors multiplies
    /// the metered-bandwidth bill on a deep fallback.
    ///
    /// `skip_serializing_if = "Vec::is_empty"` elides the field
    /// entirely in cache-only mode, keeping the JSON output minimal.
    /// Origin-mode responses always carry the field, so any
    /// deserialiser using `deny_unknown_fields` pinned to a pre-#284
    /// schema (which expected `origin_kind: Option<…>`) will see a
    /// surface change — back-compat here only covers the cache-only
    /// path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub origin_kinds: Vec<decdn_config_types::OriginKind>,
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

/// Request body for `admin_v1_drain` (issue #244).
///
/// Per-field `#[serde(default)]` lets a caller send `{}` (object with
/// no keys) and get the SIGTERM-equivalent default. Older clients that
/// omit the `params` field entirely are handled separately by the RPC
/// signature: the trait declares `req: Option<DrainRequest>` so
/// jsonrpsee's proc-macro uses `optional_next()` and decodes a missing
/// parameter to `None`, which the server impl normalizes to
/// `DrainRequest::default()`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DrainRequest {
    /// When `true`, ask the runtime to keep the admin server alive
    /// *through* `router.shutdown` instead of closing it early (issue
    /// #604). This is the opt-in seam for `decdn node drain --wait`: the
    /// CLI polls `admin_v1_health` for `in_flight_streams == 0` and the
    /// admin port must stay open long enough for that loop to observe
    /// completion. Default `false` preserves the deliberate ADR-025
    /// SIGTERM ordering (admin closes before `router.shutdown`); setting
    /// `true` only reorders that single drain.
    #[serde(default)]
    pub wait_admin: bool,
}

/// Response body for `admin_v1_drain` (issue #244). `initiated: true`
/// reports that the trigger fired; `wait_admin_honored` reports
/// whether the server actually plans to keep admin alive through
/// `router.shutdown` (issue #604). The `decdn node drain --wait`
/// client uses `wait_admin_honored` as a cross-version safety check:
/// against an older server (or any handler that doesn't propagate the
/// flag) the field deserializes to its serde default of `false`, and
/// the client refuses to enter the polling loop instead of treating
/// the imminent ECONNREFUSED as drain completion.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DrainResponse {
    /// `true` once the trigger has been fired and the runtime's
    /// shutdown sequence is underway. "Initiated", not "completed":
    /// when `wait_admin_honored` is `false`, the admin server may
    /// close before the response itself is delivered (the original
    /// ADR-025 ordering).
    pub initiated: bool,
    /// `true` when the server received `wait_admin: true` *and* is
    /// keeping admin alive through `router.shutdown` on this drain.
    /// `#[serde(default)]` so older servers (which don't serialize the
    /// field) round-trip cleanly as `false`; the `--wait` client treats
    /// `false` as "server cannot observe completion safely" and refuses
    /// to poll.
    #[serde(default)]
    pub wait_admin_honored: bool,
}

/// Per-bucket fill stat for `admin_v1_status` (issue #741). One entry
/// per *non-empty* Kademlia k-bucket — empty buckets (the vast majority
/// of the 256-bucket keyspace on a small network) are omitted so the
/// snapshot stays compact. The bucket capacity is the same Kademlia `K`
/// for every bucket and so lives once on [`RoutingHealth::bucket_capacity`]
/// rather than being repeated here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketStat {
    /// Bucket index `0..=255` (XOR-distance shell from this node's id).
    pub index: u16,
    /// Live peers currently held in the bucket
    /// (`1..=`[`RoutingHealth::bucket_capacity`]).
    pub fill: u16,
}

/// Routing-table health section of `admin_v1_status` (issue #741).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingHealth {
    /// Total peers across all buckets. Denormalized convenience for
    /// `--json` consumers and the grep-stable summary line; equals the sum
    /// of every [`BucketStat::fill`].
    pub total_peers: u64,
    /// Number of non-empty buckets. Denormalized convenience equal to
    /// `buckets.len()`; kept as a stable `key=value` token on the summary
    /// line so operator scripts need not sum the table.
    pub non_empty_buckets: u64,
    /// Per-bucket fill, one entry per non-empty bucket, ascending by index.
    pub buckets: Vec<BucketStat>,
    /// Maximum entries any bucket can hold (Kademlia `K`, currently 20).
    /// One value for the whole table — the renderer pairs it with each
    /// [`BucketStat::fill`] to show a per-bucket fill ratio.
    pub bucket_capacity: u16,
    /// Bucket-refresh interval in whole seconds (ADR 022 §Routing Table,
    /// default 1 hour). The refresh task refreshes *every* non-empty
    /// bucket once per interval.
    pub refresh_interval_s: u64,
    /// Microseconds-since-epoch the most recent bucket-refresh pass
    /// completed, or `None` if no pass has run yet (node up less than one
    /// interval). The refresh pass touches all non-empty buckets at once,
    /// so this is a single network-wide timestamp rather than a per-bucket
    /// value. `#[serde(default)]` keeps older servers round-tripping as
    /// `None`.
    #[serde(default)]
    pub last_refresh_us: Option<u64>,
}

/// DHT provider-record store utilization for `admin_v1_status` (issue #741).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordStoreHealth {
    /// Provider records currently held.
    pub records: u64,
    /// Global record cap (`RecordStoreConfig::max_records_global`).
    pub capacity: u64,
}

/// Republish-scheduler health for `admin_v1_status` (issue #741).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepublishHealth {
    /// Content hashes currently scheduled for periodic republish.
    pub scheduled_records: u64,
}

/// Response body for `admin_v1_status` (issue #741): a single-shot
/// snapshot of this node's DHT participation health, so an operator can
/// diagnose cold-start / routing-table degradation without scraping
/// Prometheus or reading logs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    /// Lowercase hex of this node's iroh `PublicKey` — same encoding as
    /// [`HealthResponse::node_id`].
    pub node_id: String,
    /// Kademlia routing-table health.
    pub routing: RoutingHealth,
    /// Active stakers known to this node (the chain-backed `StakerSet`
    /// cardinality — the set the DHT admits records and routes from).
    pub known_stakers: u64,
    /// DHT provider-record store utilization.
    pub record_store: RecordStoreHealth,
    /// Republish-scheduler depth.
    pub republish: RepublishHealth,
}

/// JSON view of one open payment channel emitted by `admin_v1_channels`
/// (issue #749). Defined separately from `decdn-incentive`'s internal
/// `ChannelState` so the replay-critical `last_*` accessors, the `U256`
/// money types, and any future internal fields can't leak into the wire
/// format: every field here is a plain owned wire value. Shared between
/// the server (serializes) and `decdn node channels` (deserializes via
/// the generated client).
///
/// Money amounts are reported in **micro-USDC** (`u64`) — the same base
/// unit `blockchain.redeem_threshold_micro_usdc` is configured in. The
/// underlying `ChannelState` carries them as `U256`; the server narrows
/// with `u64::try_from(...).unwrap_or(u64::MAX)`, so a value that somehow
/// exceeded `u64::MAX` micro-USDC (~1.8e13 USDC — unreachable for a real
/// channel bounded by the on-chain deposit) saturates rather than wraps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelSnapshot {
    /// Lowercase hex of the on-chain `channelId` (`keccak256(client,
    /// provider, channelNonce)` per ADR 003), `0x`-prefixed — same
    /// 32-byte encoding `alloy`'s `B256` `Display` produces.
    pub channel_id: String,
    /// The client (buyer) Ethereum address that opened the channel and
    /// signs vouchers, rendered as an EIP-55 mixed-case checksummed hex
    /// string (`0x`-prefixed) — `alloy`'s `Address` `Display`. Note this
    /// differs from [`PeerView::node_id`], which is plain lowercase hex of
    /// an iroh public key (a different identity type, not an EVM address).
    pub counterparty: String,
    /// Sequence number of the most-recently-accepted voucher
    /// (`ChannelState::last_nonce`). `0` before any voucher has been
    /// applied — matches the on-chain `claimedNonce == 0` sentinel.
    pub last_nonce: u64,
    /// Cumulative amount of the most-recently-accepted voucher, in
    /// micro-USDC (`ChannelState::last_amount`). This is the node's
    /// total accrued claim on the channel — the figure that crosses the
    /// redemption threshold. `0` before any voucher.
    pub outstanding_micro_usdc: u64,
    /// On-chain escrowed deposit backing the channel, in micro-USDC
    /// (`ChannelState::deposit`). Vouchers can never exceed this, so
    /// `outstanding_micro_usdc / deposit_micro_usdc` is the channel's
    /// drawn-down fraction — operators watch channels approaching full
    /// draw-down as a liquidity signal.
    pub deposit_micro_usdc: u64,
    /// Whole seconds since this process last accepted a voucher on this
    /// channel, or `None` when no voucher has been observed *since the
    /// node started*. The activity clock is in-memory: a channel
    /// hydrated from `channels.redb` at boot reports `None` until its
    /// next voucher, because the persisted `ChannelState` carries no
    /// last-voucher wall-clock. Operators use this to spot stale
    /// channels (high `outstanding` but no recent vouchers).
    /// `#[serde(default)]` keeps older servers round-tripping as `None`.
    #[serde(default)]
    pub seconds_since_last_voucher: Option<u64>,
    /// `true` when the accrued claim (`outstanding_micro_usdc`) has
    /// reached the node's configured redemption threshold
    /// (`blockchain.redeem_threshold_micro_usdc`), i.e. the redeemer
    /// would `withdraw` this channel on its next tick. This is an
    /// **upper-bound** signal: the admin surface does not read the
    /// on-chain `withdrawnAmount`, so it compares the full accrued claim
    /// (not the un-redeemed delta) against the threshold. A channel that
    /// already redeemed up to its current claim may still report `true`
    /// until the next voucher advances it — surfaced so operators can
    /// see which channels are *at or above* the redemption bar.
    pub settlement_eligible: bool,
}

/// Response body for `admin_v1_channels` (issue #749). Shared between the
/// server (serializes), `decdn node channels` (deserializes via the
/// generated client), and the integration tests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelsResponse {
    /// One entry per channel the node currently tracks. Ordering is most
    /// recently active first (channels with a known last-voucher time
    /// ahead of those without), then by descending outstanding amount —
    /// the on-call use case is "which channels are closest to a
    /// settlement / liquidity event?".
    pub channels: Vec<ChannelSnapshot>,
    /// The node's configured redemption threshold in micro-USDC
    /// (`blockchain.redeem_threshold_micro_usdc`). Echoed once at the top
    /// level — rather than repeated per channel — so the renderer can
    /// show the bar each [`ChannelSnapshot::settlement_eligible`] is
    /// measured against without the operator cross-referencing the config.
    pub redeem_threshold_micro_usdc: u64,
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

/// JSON-RPC error code: `admin_v1_status` was called on a node whose DHT
/// subsystem is not wired (e.g. a future CLI-only or test invocation that
/// brings up the admin surface without the DHT handler). A benign,
/// expected configuration state — distinct from a generic failure so an
/// operator gets "this node has no DHT to report on" rather than a
/// confusing transport error, and distinct from [`DHT_POISONED_CODE`] so
/// it isn't confused with an in-process fault.
pub const DHT_UNAVAILABLE_CODE: i32 = -32_005;

/// JSON-RPC error code: `admin_v1_status` found a DHT subsystem mutex
/// (routing table or record store) poisoned — i.e. a thread panicked
/// while holding it, so the DHT's in-memory state may be inconsistent.
/// This is a severe in-process fault, kept distinct from the benign
/// [`DHT_UNAVAILABLE_CODE`] ("no DHT wired") so an operator script can
/// tell "this node never had a DHT" from "this node's DHT just broke".
/// The server also logs the poisoning at `error` level.
pub const DHT_POISONED_CODE: i32 = -32_006;

/// JSON-RPC error code: `admin_v1_channels` could not read the channel
/// state store (issue #749) — e.g. the redb load failed or a poisoned
/// in-memory mutex. A read-side fault distinct from the cache/DHT codes
/// so an operator script can tell "channel snapshot is unavailable right
/// now" from a generic transport failure. The server also logs the
/// underlying store error.
pub const CHANNEL_STORE_ERROR_CODE: i32 = -32_007;

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
    /// eviction is logical: the iroh-blobs store still holds the bytes
    /// (`Blobs::delete` is `pub(crate)` in iroh-blobs and reserved for
    /// the GC task) until the next periodic GC sweep reclaims them
    /// (#518). The takedown itself is persisted to
    /// `<cache_dir>/evicted.log` so it survives a restart even when
    /// the next sweep hasn't run yet.
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
    ///
    /// Fire-and-forget by default: the response returns as soon as the
    /// trigger lands, not when shutdown completes. Equivalent to `kill
    /// -TERM <pid>` for operators who'd rather not stat the PID.
    ///
    /// Send `{"wait_admin": true}` as `params` (issue #604) — or
    /// from Rust, `Some(DrainRequest { wait_admin: true })` — to ask
    /// the runtime to keep the admin server alive through
    /// `router.shutdown` so a polling client can observe
    /// `admin_v1_health.in_flight_streams` reach 0. The parameter is
    /// `Option<DrainRequest>`: jsonrpsee's proc-macro maps
    /// `Option<T>` arguments to `optional_next()` in its server
    /// renderer, so older clients that omit the `params` field
    /// entirely decode to `None` rather than `InvalidParams`. The
    /// server impl normalizes `None` to `DrainRequest::default()` and
    /// gets the original SIGTERM-equivalent shutdown order.
    #[method(name = "drain")]
    async fn drain(&self, req: Option<DrainRequest>) -> RpcResult<DrainResponse>;

    /// Return a snapshot of this node's DHT participation health (issue
    /// #741): routing-table bucket fill rates, the network-wide last
    /// bucket-refresh timestamp, active-staker count, provider-record
    /// store utilization, and republish-scheduler depth. Intended for
    /// `decdn node status`, giving operators a single human-readable (or
    /// `--json`) view to diagnose cold-start or routing-table
    /// degradation without scraping Prometheus. Returns
    /// [`DHT_UNAVAILABLE_CODE`] when the DHT subsystem is not wired on
    /// this node.
    #[method(name = "status")]
    async fn status(&self) -> RpcResult<StatusResponse>;

    /// Return a live snapshot of every open payment channel this node
    /// tracks (issue #749): per channel the last-accepted nonce,
    /// outstanding accrued claim, escrowed deposit, time since the last
    /// voucher (in-memory, `None` after a restart until the next
    /// voucher), and whether the accrued claim has reached the
    /// redemption threshold. Backs `decdn node channels`, giving
    /// operators a single view to spot channels approaching settlement,
    /// stale channels, or unusually high outstanding balances before
    /// they become a liquidity risk — without scraping metrics or logs.
    /// Returns [`CHANNEL_STORE_ERROR_CODE`] if the channel state store
    /// cannot be read.
    #[method(name = "channels")]
    async fn channels(&self) -> RpcResult<ChannelsResponse>;
}

/// Decode a 64-character hex BLAKE3 hash into a [`struct@Hash`].
///
/// Goes through `alloy::primitives::hex::decode` (case-insensitive,
/// `0x`/`0X`-prefix-tolerant) rather than `Hash::from_str` so operators
/// may paste a `0x`-prefixed hash and get a uniform `INVALID_PARAMS`
/// error on malformed input. (The leaf [`struct@Hash`]'s own `FromStr`
/// is panic-free since #578 — unlike the old iroh-blobs `Hash::from_str`
/// which fell through to a base32 decoder that panicked on a
/// wrong-size output buffer — but it does not accept a `0x` prefix, so
/// the alloy path is kept for the operator ergonomics.)
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// Wire back-compat for `DrainResponse.wait_admin_honored`
    /// (issue #604 review): a pre-#604 server's response omits the
    /// field. `#[serde(default)]` must deserialize the omitted field
    /// to `false` so the new CLI's safety guard fires ("server
    /// doesn't honor --wait") rather than the CLI proceeding to poll
    /// against a server that will close admin early.
    ///
    /// Accidental removal of the `#[serde(default)]` attribute would
    /// otherwise turn an old-server response into a deserialize error,
    /// surfacing as a confusing "transport" failure instead of the
    /// actionable "upgrade the node" message.
    #[test]
    fn drain_response_legacy_shape_defaults_wait_admin_honored_false() {
        let legacy = r#"{"initiated":true}"#;
        let resp: DrainResponse =
            serde_json::from_str(legacy).expect("legacy DrainResponse must deserialize");
        assert!(resp.initiated);
        assert!(
            !resp.wait_admin_honored,
            "missing wait_admin_honored must default to false"
        );
    }

    /// Same guarantee for `HealthResponse.in_flight_streams`: an
    /// older server lacking the field must round-trip as `0` so
    /// pre-#604 `decdn node health` clients (and the new --wait
    /// polling loop, against any old server) don't break.
    #[test]
    fn health_response_legacy_shape_defaults_in_flight_streams_zero() {
        let legacy = r#"{"node_id":"abc","uptime_s":42}"#;
        let resp: HealthResponse =
            serde_json::from_str(legacy).expect("legacy HealthResponse must deserialize");
        assert_eq!(resp.node_id, "abc");
        assert_eq!(resp.uptime_s, 42);
        assert_eq!(
            resp.in_flight_streams, 0,
            "missing in_flight_streams must default to 0"
        );
    }

    /// `DrainRequest` round-trips through `{}` (empty object) by
    /// deserializing each field to its serde default. The whole-
    /// parameter-missing case (no `params` field at all) is handled
    /// at the RPC layer by `req: Option<DrainRequest>` in the trait
    /// (jsonrpsee's `optional_next`); this test guards the field-
    /// level default semantics.
    #[test]
    fn drain_request_empty_object_deserializes_to_default() {
        let req: DrainRequest =
            serde_json::from_str("{}").expect("empty-object DrainRequest must deserialize");
        assert!(
            !req.wait_admin,
            "missing wait_admin must default to false (SIGTERM-equivalent)"
        );
    }

    /// Wire back-compat for `RoutingHealth.last_refresh_us` (issue #741):
    /// a server that has never completed a bucket-refresh pass omits the
    /// field (or sends `null`). `#[serde(default)]` must deserialize the
    /// omitted field to `None` so a `decdn node status` client renders
    /// "not yet refreshed" rather than failing the whole roundtrip.
    #[test]
    fn routing_health_legacy_shape_defaults_last_refresh_none() {
        let legacy = r#"{
            "total_peers": 3,
            "non_empty_buckets": 2,
            "buckets": [{"index":0,"fill":1}],
            "bucket_capacity": 20,
            "refresh_interval_s": 3600
        }"#;
        let resp: RoutingHealth =
            serde_json::from_str(legacy).expect("legacy RoutingHealth must deserialize");
        assert_eq!(resp.total_peers, 3);
        assert_eq!(resp.bucket_capacity, 20);
        assert_eq!(resp.refresh_interval_s, 3600);
        assert!(
            resp.last_refresh_us.is_none(),
            "missing last_refresh_us must default to None"
        );
    }

    /// `StatusResponse` round-trips through serde unchanged — guards the
    /// nested DTO shapes (`RoutingHealth` / `BucketStat` /
    /// `RecordStoreHealth` / `RepublishHealth`) the CLI and server both
    /// (de)serialize.
    #[test]
    fn status_response_round_trips() {
        let resp = StatusResponse {
            node_id: "abc".to_string(),
            routing: RoutingHealth {
                total_peers: 21,
                non_empty_buckets: 2,
                buckets: vec![
                    BucketStat { index: 0, fill: 1 },
                    BucketStat {
                        index: 255,
                        fill: 20,
                    },
                ],
                bucket_capacity: 20,
                refresh_interval_s: 3600,
                last_refresh_us: Some(1_700_000_000_000_000),
            },
            known_stakers: 7,
            record_store: RecordStoreHealth {
                records: 12,
                capacity: 100_000,
            },
            republish: RepublishHealth {
                scheduled_records: 5,
            },
        };
        let json = serde_json::to_string(&resp).expect("serialize StatusResponse");
        let back: StatusResponse = serde_json::from_str(&json).expect("deserialize StatusResponse");
        assert_eq!(back.node_id, "abc");
        assert_eq!(back.routing.total_peers, 21);
        assert_eq!(back.routing.buckets.len(), 2);
        assert_eq!(back.routing.bucket_capacity, 20);
        assert_eq!(back.routing.last_refresh_us, Some(1_700_000_000_000_000));
        assert_eq!(back.known_stakers, 7);
        assert_eq!(back.record_store.capacity, 100_000);
        assert_eq!(back.republish.scheduled_records, 5);
    }

    /// `ChannelsResponse` round-trips through serde unchanged — guards the
    /// nested `ChannelSnapshot` shape both the server and `decdn node
    /// channels` (de)serialize (issue #749).
    #[test]
    fn channels_response_round_trips() {
        let resp = ChannelsResponse {
            redeem_threshold_micro_usdc: 1_000_000,
            channels: vec![
                ChannelSnapshot {
                    channel_id: "0xabcd".to_string(),
                    counterparty: "0x00aa".to_string(),
                    last_nonce: 7,
                    outstanding_micro_usdc: 2_500_000,
                    deposit_micro_usdc: 10_000_000,
                    seconds_since_last_voucher: Some(42),
                    settlement_eligible: true,
                },
                ChannelSnapshot {
                    channel_id: "0xbeef".to_string(),
                    counterparty: "0x00bb".to_string(),
                    last_nonce: 0,
                    outstanding_micro_usdc: 0,
                    deposit_micro_usdc: 5_000_000,
                    seconds_since_last_voucher: None,
                    settlement_eligible: false,
                },
            ],
        };
        let json = serde_json::to_string(&resp).expect("serialize ChannelsResponse");
        let back: ChannelsResponse =
            serde_json::from_str(&json).expect("deserialize ChannelsResponse");
        assert_eq!(back.redeem_threshold_micro_usdc, 1_000_000);
        assert_eq!(back.channels.len(), 2);
        let first = back.channels.first().expect("first channel");
        assert_eq!(first.channel_id, "0xabcd");
        assert_eq!(first.counterparty, "0x00aa");
        assert_eq!(first.last_nonce, 7);
        assert_eq!(first.outstanding_micro_usdc, 2_500_000);
        assert_eq!(first.deposit_micro_usdc, 10_000_000);
        assert_eq!(first.seconds_since_last_voucher, Some(42));
        assert!(first.settlement_eligible);
        let second = back.channels.get(1).expect("second channel");
        assert_eq!(second.seconds_since_last_voucher, None);
        assert!(!second.settlement_eligible);
    }

    /// Wire back-compat for `ChannelSnapshot.seconds_since_last_voucher`
    /// (issue #749): an older server (or a channel with no in-process
    /// voucher activity) omits the field. `#[serde(default)]` must
    /// deserialize the omitted field to `None` so a `decdn node channels`
    /// client renders "never" rather than failing the whole roundtrip.
    #[test]
    fn channel_snapshot_legacy_shape_defaults_seconds_none() {
        let legacy = r#"{
            "channel_id": "0xabcd",
            "counterparty": "0x00aa",
            "last_nonce": 3,
            "outstanding_micro_usdc": 100,
            "deposit_micro_usdc": 200,
            "settlement_eligible": false
        }"#;
        let snap: ChannelSnapshot =
            serde_json::from_str(legacy).expect("legacy ChannelSnapshot must deserialize");
        assert_eq!(snap.last_nonce, 3);
        assert_eq!(snap.outstanding_micro_usdc, 100);
        assert!(
            snap.seconds_since_last_voucher.is_none(),
            "missing seconds_since_last_voucher must default to None"
        );
    }
}
