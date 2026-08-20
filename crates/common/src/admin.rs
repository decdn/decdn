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

/// Whether the node's local iroh key is the one bound to its operator address
/// on-chain (#1034).
///
/// This is a correctness signal, not a liveness one. `SlashJudge` resolves an
/// accused node through `CapacityBond.nodeIdOf`, so a node serving under a key
/// no binding points at is **unslashable**: it earns normally while its bond is
/// unreachable, which is a protocol fault rather than an outage — and one with
/// no symptom the operator would otherwise notice. A key rotation that swapped
/// `node.secret` without submitting `bindNodeId` lands exactly here, which is
/// why `decdn node rotate-key` orders its steps to make the state unreachable
/// and why the daemon reports it when it happens anyway.
///
/// Advisory by construction. The check is one RPC at bring-up, and a daemon
/// that refused to start on it would be brickable by a transient RPC failure
/// and unable to run the very rotation that repairs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingStatus {
    /// The on-chain binding names this node's key. Nothing to do.
    Bound,
    /// The operator is bound to a DIFFERENT node id than the local key —
    /// the un-slashable state. Either rotate on-chain to match the local key
    /// (`decdn node rotate-key --key iroh --bind-existing`) or restore the
    /// key the binding names. Sampled once at bring-up like every other
    /// variant here, so a live repair does not clear this to `Bound` until
    /// the daemon restarts — a `mismatch` seen right after running
    /// `--bind-existing` is not a failed repair, just a stale sample.
    Mismatch,
    /// The operator address has no binding at all: never registered, or the
    /// node id was reclaimed. `decdn node register` makes the initial binding.
    Unbound,
    /// Not determined — no `[blockchain]` configuration to check against, or
    /// the read failed. Explicitly not a synonym for "fine".
    Unknown,
}

/// Response body for `admin_v1_health`. Shared between the server
/// (serializes) and `decdn node health` (deserializes via the generated
/// client). Intentionally minimal: this method exists so an operator
/// script can answer "is this admin port the node I think it is, and
/// has it been up since I started watching?" with one RPC call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Lowercase hex of this node's iroh `PublicKey`.
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
    /// completed during a graceful drain.
    pub in_flight_streams: u64,
    /// Whether `node_id` is the key bound to this operator on-chain (#1034).
    /// Sampled once at bring-up — the binding only changes by an explicit
    /// operator transaction, and re-reading it on every health poll would put
    /// an RPC round trip behind the readiness probe.
    pub binding: BindingStatus,
    /// Lowercase hex of the node id the operator IS bound to, when that could
    /// be read. Present for [`BindingStatus::Bound`] (where it equals
    /// `node_id`) and, more usefully, for [`BindingStatus::Mismatch`], where it
    /// names the key to restore. `None` for `Unbound` and `Unknown`.
    pub bound_node_id: Option<String>,
    /// Whether this node is in the on-chain active-staker set right now
    /// (ADR 019 §Phase 4, criterion 1; #1030).
    ///
    /// Purely diagnostic. The daemon does **not** gate delivery on it — a node
    /// policing its own registration constrains only operators who were never
    /// the threat (see ADR 019 §How criterion 1 is enforced). What `false`
    /// means is that the two EXTERNAL constraints are in force: the operator
    /// accrues no governance weight, because its declared capacity caps
    /// credited bytes at zero, and peers have no reason to route to a node they
    /// cannot slash.
    ///
    /// Unlike [`Self::binding`], this is read LIVE on every poll — it is a
    /// lookup in the registry projection the daemon already keeps current off
    /// `CapacityBond` events, not a chain round trip — so an operator who has
    /// just run `decdn setup` watches it flip to `true` without restarting the
    /// daemon.
    ///
    /// `false` is the answer to "my node is up and healthy, why is it earning
    /// nothing". Nothing on the wire says so; the traffic simply does not
    /// arrive. The cause is one of never registered, registered under a key
    /// this process no longer holds (see [`Self::binding`]), deregistered,
    /// ejected, bond below `minBond`, or an unbonding request in flight.
    pub registry_active: bool,
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
    /// meaningful. `false` means the eviction was applied per the
    /// existing `admin_v1_evict` behaviour.
    pub dry_run: bool,
    /// Pre-evict snapshot of the blob's local-cache state (#379).
    /// Populated for both real and dry-run calls so an operator's
    /// audit log captures size and pin status at the moment of
    /// evict. The struct is nested (rather than flattened into
    /// [`Self`]) so the `--dry-run` view doesn't push the bool count
    /// past the `clippy::struct_excessive_bools` threshold and so a
    /// future addition to the snapshot doesn't churn the top-level
    /// response shape.
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
    pub size_bytes: Option<u64>,
    /// Microseconds elapsed since the last `get()` against this hash.
    /// `None` when no access has been recorded — typical for a hash
    /// that was just inserted but never re-served, or one that has
    /// been logically evicted (eviction clears the access entry).
    pub last_accessed_us_ago: Option<u64>,
    /// Whether the hash is in the operator-pinned set (#276).
    /// Pinning protects against LRU eviction but **not** against an
    /// explicit `admin_v1_evict`; surfaced here so dry-run callers
    /// can confirm policy state before issuing the real takedown.
    pub pinned: bool,
    /// Whether the hash is already in `<cache_dir>/evicted.log`.
    /// `true` means a real `admin_v1_evict` would short-circuit
    /// (idempotent re-run, no log line appended).
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

/// Response body for `admin_v1_reload` (issue #373). Reports the
/// post-reload values the SIGHUP arm logs to stdout, so operators using
/// the RPC path get the same after-state confirmation without scraping
/// `tracing` output. Only the *reloadable* fields appear here: changes
/// to non-reloadable sections are logged by the reload path itself
/// (one `info!` per changed-but-ignored field) and aren't echoed in
/// the RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReloadResponse {
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
    /// The `--wait` client treats `false` as "server cannot observe
    /// completion safely" and refuses to poll.
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
    /// value.
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

/// JSON view of one lane this node provides against a `PaymentPool`,
/// emitted by `admin_v1_lanes` (issue #749). A lane is keyed by
/// `(pool_id, signer, provider)` — `decdn_incentive::LaneKey`. Defined
/// separately from `decdn-incentive`'s internal `LaneState` so the
/// replay-critical `last_*` accessors, the `U256` money types, and any
/// future internal fields can't leak into the wire format: every field
/// here is a plain owned wire value. Shared between the server
/// (serializes) and `decdn node lanes` (deserializes via the generated
/// client).
///
/// Money amounts are reported in **micro-USDC** (`u64`) — the same base
/// unit `blockchain.redeem_threshold_micro_usdc` is configured in. The
/// underlying `LaneState` carries them as `U256`; the server narrows with
/// `u64::try_from(...).unwrap_or(u64::MAX)`, so a value that somehow
/// exceeded `u64::MAX` micro-USDC (~1.8e13 USDC — unreachable for a real
/// pool) saturates rather than wraps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneSnapshot {
    /// Lowercase hex of the on-chain `PaymentPool` id (`LaneKey::pool_id`),
    /// `0x`-prefixed — same 32-byte encoding `alloy`'s `B256` `Display`
    /// produces. Shared by every lane drawing from the same pool.
    pub pool_id: String,
    /// The pool owner's Ethereum address — the deposit owner and refund
    /// destination, and the subject of the ADR-011 blacklist gates.
    /// Rendered as an EIP-55 mixed-case checksummed hex string
    /// (`0x`-prefixed) — `alloy`'s `Address` `Display`. Note this differs
    /// from an iroh node id, which is plain lowercase hex of an Ed25519
    /// public key (a different identity type, not an EVM address). It is
    /// *not* necessarily the voucher signer — see [`Self::voucher_signer`].
    pub counterparty: String,
    /// The capability signer authorizing vouchers on this lane
    /// (`LaneKey::signer`): the EIP-712 recovery target for every voucher,
    /// in the same EIP-55 rendering as [`Self::counterparty`]. Equal to
    /// `counterparty` for the ordinary self-signing case, and a distinct
    /// delegate key when the pool owner delegated signing.
    pub voucher_signer: String,
    /// Sequence number of the most-recently-accepted voucher. Not tracked
    /// in the shared-pool model (a pool has no per-lane nonce), so this
    /// always reports `0`.
    pub last_nonce: u64,
    /// Cumulative amount of the most-recently-accepted voucher, in
    /// micro-USDC (`LaneState::last_amount`). This is the node's total
    /// accrued claim on this lane — the figure that crosses the
    /// redemption threshold. `0` before any voucher.
    pub outstanding_micro_usdc: u64,
    /// Not tracked in the shared-pool model (the pool's deposit is shared
    /// across every lane it funds, so no single lane owns a deposit), so
    /// this always reports `0`.
    pub deposit_micro_usdc: u64,
    /// Whole seconds since this process last accepted a voucher on this
    /// lane, or `None` when no voucher has been observed *since the node
    /// started*. The activity clock is in-memory: a lane hydrated from
    /// `lanes.redb` at boot reports `None` until its next voucher, because
    /// the persisted `LaneState` carries no last-voucher wall-clock.
    /// Operators use this to spot stale lanes (high `outstanding` but no
    /// recent vouchers).
    pub seconds_since_last_voucher: Option<u64>,
    /// `true` when the accrued claim (`outstanding_micro_usdc`) has
    /// reached the node's configured redemption threshold
    /// (`blockchain.redeem_threshold_micro_usdc`), i.e. the redeemer would
    /// redeem this lane on its next tick. This is an **upper-bound**
    /// signal: the admin surface does not read the on-chain redeemed
    /// amount, so it compares the full accrued claim (not the
    /// un-redeemed delta) against the threshold. A lane that already
    /// redeemed up to its current claim may still report `true` until the
    /// next voucher advances it — surfaced so operators can see which
    /// lanes are *at or above* the redemption bar.
    pub settlement_eligible: bool,
}

/// Response body for `admin_v1_lanes` (issue #749). Shared between the
/// server (serializes), `decdn node lanes` (deserializes via the
/// generated client), and the integration tests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanesResponse {
    /// One entry per lane the node currently tracks. Ordering is most
    /// recently active first (lanes with a known last-voucher time ahead
    /// of those without), then by descending outstanding amount — the
    /// on-call use case is "which lanes are closest to a settlement /
    /// liquidity event?".
    pub lanes: Vec<LaneSnapshot>,
    /// The node's configured redemption threshold in micro-USDC
    /// (`blockchain.redeem_threshold_micro_usdc`). Echoed once at the top
    /// level — rather than repeated per lane — so the renderer can show
    /// the bar each [`LaneSnapshot::settlement_eligible`] is measured
    /// against without the operator cross-referencing the config.
    pub redeem_threshold_micro_usdc: u64,
}

/// One slash detected against this node's operator (`SlashJudge.Slashed`,
/// #1032, G-NODE-05). Surfaced so an operator (or a keeper script) can notice a
/// slash and file `decdn appeal slash` within the 30-day window without watching
/// the chain directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlashRecordDto {
    /// The globally-monotonic on-chain `slashId` (`CapacityBond.slash`),
    /// rendered as a decimal string — it is a `uint256` and can exceed `u64`.
    /// This is the value passed to `decdn appeal slash <SLASH_ID>`.
    pub slash_id: String,
    /// Offense taxonomy index (ADR 014): `0` = `RateManipulation`,
    /// `1` = Blacklist. Kept numeric to avoid drift if the enum grows.
    pub offense_type: u8,
    /// Bond amount slashed, in TOKEN base units, as a decimal string (`uint256`).
    pub amount: String,
    /// `keccak256` evidence digest from the `Slashed` event (ADR 014),
    /// `0x`-prefixed 32-byte hex.
    pub evidence_hash: String,
    /// Block number the `Slashed` log was mined in, or `None` if the log was
    /// still pending when observed (rare; live logs carry a block number).
    pub block_number: Option<u64>,
    /// **Nominal** appeal-window close (Unix seconds): the `Slashed` block
    /// timestamp + 30 days, or `None` if the block read failed. This is a cheap
    /// client-side hint, NOT read from the `CapacityBond` slash record, and it
    /// ignores protocol-pause extensions — which only ever move the real
    /// deadline *later* (`markAppealOpen` adds `pausedTotal`). Safe to file
    /// before this; a keeper must not treat a just-past value as final.
    pub appeal_window_close: Option<u64>,
}

/// Snapshot of every slash the watcher has detected against this node's
/// operator (#1032). Empty list when none — a clean operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlashesResponse {
    /// One entry per distinct detected `slashId`, most-recent first.
    pub slashes: Vec<SlashRecordDto>,
}

/// JSON-RPC error code: the request shape was wrong (bad hex, etc.).
/// Matches the standard JSON-RPC 2.0 `Invalid params` code.
pub const INVALID_PARAMS_CODE: i32 = -32_602;

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

/// JSON-RPC error code: `admin_v1_lanes` could not read the pool state
/// store (issue #749) — e.g. the redb load failed or a poisoned
/// in-memory mutex. A read-side fault distinct from the cache/DHT codes
/// so an operator script can tell "lane snapshot is unavailable right
/// now" from a generic transport failure. The server also logs the
/// underlying store error.
pub const POOL_STORE_ERROR_CODE: i32 = -32_007;

/// JSON-RPC error code: `admin_v1_slashes` was called on a node whose slash
/// watcher is not wired (e.g. no `slash_judge_address`, or a test/CLI-only
/// invocation). Benign config state, distinct from a generic failure so an
/// operator gets "no slash detection on this node" rather than an opaque error.
pub const SLASH_DETECTION_UNAVAILABLE_CODE: i32 = -32_009;

/// Admin RPC surface. Versioned via the namespace prefix
/// (`admin_v1_...`): new methods may be added backwards-compatibly
/// within `v1`, a breaking change cuts over to `admin_v2_...`.
#[rpc(server, client, namespace = "admin_v1")]
pub trait AdminRpc {
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
    ///   `log_level`.
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

    /// Return a live snapshot of every lane this node provides against a
    /// `PaymentPool` (issue #749): per lane the outstanding accrued claim,
    /// time since the last voucher (in-memory, `None` after a restart
    /// until the next voucher), and whether the accrued claim has reached
    /// the redemption threshold. Backs `decdn node lanes`, giving
    /// operators a single view to spot lanes approaching settlement,
    /// stale lanes, or unusually high outstanding balances before they
    /// become a liquidity risk — without scraping metrics or logs.
    /// Returns [`POOL_STORE_ERROR_CODE`] if the pool state store cannot
    /// be read.
    #[method(name = "lanes")]
    async fn lanes(&self) -> RpcResult<LanesResponse>;

    /// Return every slash the node's watcher has detected against its own
    /// operator (#1032, G-NODE-05): per slash the `slashId`, offense type,
    /// amount, evidence digest, block, and the 30-day appeal-window close time.
    /// Consumed directly by operators/keepers (no CLI subcommand wraps it) so
    /// they notice a slash and file `decdn appeal slash` in time. Returns
    /// [`SLASH_DETECTION_UNAVAILABLE_CODE`]
    /// when the slash watcher is not wired on this node.
    #[method(name = "slashes")]
    async fn slashes(&self) -> RpcResult<SlashesResponse>;
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

    /// `LanesResponse` round-trips through serde unchanged — guards the
    /// nested `LaneSnapshot` shape both the server and `decdn node
    /// lanes` (de)serialize (issue #749).
    #[test]
    fn lanes_response_round_trips() {
        let resp = LanesResponse {
            redeem_threshold_micro_usdc: 1_000_000,
            lanes: vec![
                LaneSnapshot {
                    pool_id: "0xabcd".to_string(),
                    counterparty: "0x00aa".to_string(),
                    voucher_signer: "0x00cc".to_string(),
                    last_nonce: 7,
                    outstanding_micro_usdc: 2_500_000,
                    deposit_micro_usdc: 10_000_000,
                    seconds_since_last_voucher: Some(42),
                    settlement_eligible: true,
                },
                LaneSnapshot {
                    pool_id: "0xbeef".to_string(),
                    counterparty: "0x00bb".to_string(),
                    voucher_signer: "0x00bb".to_string(),
                    last_nonce: 0,
                    outstanding_micro_usdc: 0,
                    deposit_micro_usdc: 5_000_000,
                    seconds_since_last_voucher: None,
                    settlement_eligible: false,
                },
            ],
        };
        let json = serde_json::to_string(&resp).expect("serialize LanesResponse");
        let back: LanesResponse = serde_json::from_str(&json).expect("deserialize LanesResponse");
        assert_eq!(back.redeem_threshold_micro_usdc, 1_000_000);
        assert_eq!(back.lanes.len(), 2);
        let first = back.lanes.first().expect("first lane");
        assert_eq!(first.pool_id, "0xabcd");
        assert_eq!(first.counterparty, "0x00aa");
        assert_eq!(
            first.voucher_signer, "0x00cc",
            "a delegated signer must survive the round trip distinct from the funder"
        );
        assert_eq!(first.last_nonce, 7);
        assert_eq!(first.outstanding_micro_usdc, 2_500_000);
        assert_eq!(first.deposit_micro_usdc, 10_000_000);
        assert_eq!(first.seconds_since_last_voucher, Some(42));
        assert!(first.settlement_eligible);
        let second = back.lanes.get(1).expect("second lane");
        assert_eq!(second.seconds_since_last_voucher, None);
        assert!(!second.settlement_eligible);
    }
}
