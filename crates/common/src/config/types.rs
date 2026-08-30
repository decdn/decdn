//! TOML-deserializable configuration file types.
//!
//! All fields are `Option` so that missing keys in the TOML file are
//! accepted. The merge logic in [`super::resolve_config`] fills in defaults.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::secret::SecretString;
use crate::cli::common::{LogFormat, LogLevel};

/// Top-level TOML configuration file structure.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    /// Identity settings.
    pub identity: Option<IdentityConfig>,
    /// Network settings.
    pub network: Option<NetworkConfig>,
    /// Blockchain settings.
    pub blockchain: Option<BlockchainConfig>,
    /// Cache settings.
    pub cache: Option<CacheConfig>,
    /// Payment settings.
    pub payment: Option<PaymentConfig>,
    /// Observability settings.
    pub observability: Option<ObservabilityConfig>,
    /// Connection rate-limiting settings.
    pub security: Option<SecurityConfig>,
    /// Node-local load-shedding thresholds (overload protection). Absent
    /// => the resolved defaults.
    pub load_shed: Option<LoadShedConfig>,
    /// `cdn/dht/v1` Kademlia DHT settings (ADR 022). Absent => defaults
    /// from the ADR 022 §DHT Rate Limiting table.
    pub dht: Option<DhtConfig>,
    /// `cdn/probe/v1` settings (ADR 005). Absent => defaults from the
    /// ADR 005 §Probe rate limiting table.
    pub probe: Option<ProbeConfig>,
    /// Download-receipt audit-log retention settings (#802). Absent =>
    /// defaults (128 MiB per file, 4 retained backups).
    pub receipts: Option<ReceiptsConfig>,
    /// Local content denylist (ADR 011 §Local Denylist). Absent => both lists
    /// empty; nothing is denied locally.
    pub content: Option<ContentConfig>,
}

/// Identity section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    /// Node data directory.
    pub data_dir: Option<PathBuf>,
    /// ISO 3166-1 alpha-2 region code.
    pub region: Option<String>,
}

/// Network section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    /// QUIC bind port.
    pub bind_port: Option<u16>,
    /// iroh relay URLs for NAT traversal. Multiple entries give relay
    /// redundancy/failover. Reachability is probed at bring-up and logged but
    /// never fatal: if every probeable relay is unreachable the node warns and
    /// proceeds (iroh retries in the background); entries with no derivable
    /// host/port are skipped.
    pub relay_urls: Option<Vec<String>>,
    /// Operator-configurable address discovery (#818 scope 1). Absent => the
    /// node uses the n0-hosted pkarr/DNS discovery (`presets::N0`, unchanged).
    /// Present => the node drops the n0 discovery leg and composes only the
    /// providers configured here (the wiring layer builds on `presets::Minimal`).
    /// Relay selection ([`Self::relay_urls`]) is an independent, orthogonal knob.
    pub discovery: Option<DiscoveryConfig>,
}

/// Operator-configurable address-discovery providers (#818 scope 1).
///
/// Two mechanisms, selectable by which keys are present and combinable:
/// - **Custom pkarr + DNS** ([`Self::pkarr_url`] + [`Self::dns_origin`]) —
///   publish this node's signed address record to an operator-run pkarr relay
///   and resolve peers via an operator-run DNS server. Closest to n0's model;
///   supports dynamic addresses.
/// - **Static peer map** ([`Self::peers`]) — a `MemoryLookup` address book
///   supplied directly in config. Fully offline/isolated; addresses must be
///   known ahead of time.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryConfig {
    /// pkarr relay URL this node PUBLISHES its signed address record to. Must
    /// be paired with [`Self::dns_origin`] (a resolver for the same namespace):
    /// publishing to a relay nothing resolves from is rejected at config
    /// validation. Parse-checked as a URL at resolution.
    pub pkarr_url: Option<String>,
    /// DNS origin domain this node RESOLVES peer addresses from (TXT queries
    /// `_iroh.<z32-id>.<dns_origin>`). Valid on its own (a resolve-only node
    /// that does not publish). Checked non-empty at resolution.
    pub dns_origin: Option<String>,
    /// Static peer address book seeded into a `MemoryLookup`, keyed by peer
    /// `NodeId` (the canonical 64-char lowercase-hex form iroh emits; validated
    /// with the same parser the node uses at bring-up). Composes alongside the
    /// pkarr/DNS leg when both are set.
    pub peers: Option<std::collections::HashMap<String, DiscoveryPeer>>,
}

/// A single static peer entry for [`DiscoveryConfig::peers`]. To be useful at
/// least one of [`Self::relay_url`] / [`Self::addrs`] should be set — an
/// id-only entry yields a peer with no transport addresses to dial.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryPeer {
    /// This peer's home relay URL (becomes a relay transport address).
    pub relay_url: Option<String>,
    /// Direct socket addresses for this peer (`host:port`, each becomes a
    /// direct transport address). Each entry is parsed as a `SocketAddr` at
    /// resolution.
    #[serde(default)]
    pub addrs: Vec<String>,
}

/// Blockchain section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockchainConfig {
    /// JSON-RPC endpoint URL.
    pub rpc_url: Option<String>,
    /// Ethereum keystore file path.
    pub eth_keystore: Option<PathBuf>,
    /// `PaymentPool` contract address — the shared payment pool this node
    /// registers against for buyer and seller flows alike.
    pub payment_pool_address: Option<String>,
    /// `CapacityBond` contract address.
    pub capacity_bond_address: Option<String>,
    /// `OriginAssignment` contract address. Optional: when set, the node runs the
    /// chain-backed origin directory for node-to-node routing, resolving a
    /// request's namespace via `getOrigins(namespaceId)` (ADR 022) for the
    /// `FIND_VALUE` fallback. Unset => the origin directory is empty, so that
    /// fallback resolves nothing.
    pub origin_assignment_address: Option<String>,
    /// Positive-hit TTL for the lazy origin directory cache, in seconds. Only
    /// consulted when `origin_assignment_address` is set. Absent =>
    /// `DEFAULT_ORIGIN_DIRECTORY_POSITIVE_TTL_SEC`.
    pub origin_directory_positive_ttl_sec: Option<u64>,
    /// Negative-hit TTL for the lazy origin directory cache, in seconds. Only
    /// consulted when `origin_assignment_address` is set. Absent =>
    /// `DEFAULT_ORIGIN_DIRECTORY_NEGATIVE_TTL_SEC`.
    pub origin_directory_negative_ttl_sec: Option<u64>,
    /// Max distinct namespaces held in the lazy origin directory cache (LRU
    /// eviction). Only consulted when `origin_assignment_address` is set.
    /// Absent => `DEFAULT_ORIGIN_DIRECTORY_CACHE_CAPACITY`.
    pub origin_directory_cache_capacity: Option<usize>,
    /// `PublisherRegistry` contract address. Independent of the origin directory:
    /// it is the publish CLI's `namespace create` target and is not consumed by
    /// the node runtime.
    pub publisher_registry_address: Option<String>,
    /// `SlashJudge` contract address — the EIP-712 `verifyingContract` for
    /// `ProbeResponse` / `StreamResponse` `slash_sig` signatures (ADR 014
    /// §1–2). Required: a wrong/zero address silently produces signatures no
    /// verifier accepts, so resolution fails fast when it is missing rather
    /// than defaulting.
    pub slash_judge_address: Option<String>,
    /// `SlashAppeal` contract address — the target for `decdn appeal slash`
    /// (ADR 028). Consumed **only** by that CLI (via `chain_ctx::resolve_appeal`,
    /// which validates it); the daemon accepts the key here — `[blockchain]`
    /// denies unknown fields and a node's `node.toml` is shared with the CLI —
    /// but does not resolve or use it.
    pub slash_appeal_address: Option<String>,
    /// `ContentBlacklist` contract address. Required at node config resolution;
    /// startup completes the initial global + operator-scope replay before any
    /// ALPN accepts connections (ADR 011/019/031).
    pub content_blacklist_address: Option<String>,
    /// Seconds between the blacklist watcher's periodic replay + re-scope pass
    /// (ADR 011 §Polling cadence). This backstop is what catches scope changes
    /// with no `ContentBlacklist` event — an operator region/ripening
    /// transition. Absent =>
    /// [`super::DEFAULT_CONTENT_BLACKLIST_POLL_INTERVAL_SEC`] (600s). Only
    /// consulted when `content_blacklist_address` is set.
    pub content_blacklist_poll_interval_sec: Option<u64>,
    /// EIP-712 `chainId` bound into every `slash_sig` domain separator.
    /// Absent => [`super::DEFAULT_CHAIN_ID`] (Arbitrum Sepolia, the initial
    /// network target — matches the chain id bound on the runtime signer).
    pub chain_id: Option<u64>,
    /// Seconds between RPC connectivity watchdog probes. `0` disables the
    /// watchdog entirely; absent => default (30s). Non-zero values below
    /// `MIN_RPC_WATCHDOG_INTERVAL_SEC` are rejected at config resolution.
    pub rpc_watchdog_interval_sec: Option<u64>,
    /// Milliseconds between chain-event poll ticks (#1011, #1106). Drives two
    /// unrelated consumers: the `eth_getLogs` tick cadence of every chain
    /// watcher, and alloy's pending-transaction receipt heartbeat (see
    /// `ResolvedBlockchain::event_poll_interval_ms`). The heartbeat is why this
    /// overrides alloy's own default, which is 250 ms for a localhost RPC (it
    /// auto-detects `127.0.0.1`/`localhost`) and 7000 ms otherwise — the 250 ms
    /// local default hammers a dev anvil. Absent => default
    /// (`DEFAULT_EVENT_POLL_INTERVAL_MS`, 7000 ms, matching alloy's non-local
    /// cadence so live-RPC load is unchanged). Values below
    /// `MIN_EVENT_POLL_INTERVAL_MS` are rejected at config resolution.
    pub event_poll_interval_ms: Option<u64>,
    /// Seconds between authoritative `PaymentPool.getRateBounds()` re-reads
    /// by the rate-bounds watcher (#1172, ADR 019 §3.1). This is the safety-net
    /// cadence *in addition to* the `RateBoundsUpdated` event subscription
    /// (which follows [`Self::event_poll_interval_ms`]); it reconciles any log
    /// the event tail missed. Absent =>
    /// [`super::DEFAULT_RATE_BOUNDS_POLL_INTERVAL_SEC`] (3600s / 1h). Must not
    /// be `0` (that would poll every tick); rejected at config resolution.
    pub rate_bounds_poll_interval_sec: Option<u64>,
    /// Seconds between authoritative `FeeRouter.getShares()` re-reads by the
    /// fee-shares watcher (ADR 041 / ADR 016 § Tunable Economics). This is the
    /// safety-net cadence *in addition to* the `SharesUpdated` event
    /// subscription (which follows [`Self::event_poll_interval_ms`]); it
    /// reconciles any log the event tail missed. Absent =>
    /// [`super::DEFAULT_FEE_SHARES_POLL_INTERVAL_SEC`] (3600s / 1h). Must not
    /// be `0` (that would poll every tick); rejected at config resolution.
    pub fee_shares_poll_interval_sec: Option<u64>,
    /// Per-chunk redemption floor (base units, `µUSDC`, ADR 003 § Operator
    /// early withdrawal). The node submits an on-chain redemption
    /// transaction for a chunk of lanes only once the aggregate un-redeemed
    /// value across that chunk's lanes reaches this floor; every lane in a
    /// submitted chunk settles, so a small (dust) lane rides alongside the
    /// larger lanes that cleared the floor. Larger values amortize gas
    /// across more delivery; smaller values bound unsettled exposure.
    /// Absent => default (1 USDC = `1_000_000` `µUSDC`).
    pub redeem_threshold_micro_usdc: Option<u64>,
    /// Maximum vouchers packed into one `redeemMany` transaction. The redeemer
    /// splits a sweep across this many vouchers per transaction so a high-fan-out
    /// node stays under the block gas limit; a chunk that still fails to send
    /// oversized is halved and retried. Absent => default (`300`). Must not be
    /// `0`; rejected at config resolution.
    pub redeem_max_vouchers_per_tx: Option<u64>,
    /// Seconds between the redeemer's self-tick sweeps (#327, #751): the
    /// low-frequency backstop that sweeps every lane and redeems the chunks
    /// that clear the redemption floor, independent of the advisory per-voucher
    /// hints, so a dropped hint can never strand an accrued balance. Smaller values withdraw earnings sooner
    /// at the cost of more pool-state reads; larger values lean harder on the
    /// hints. Absent => default (300s / 5 min). Must not be `0`; rejected at
    /// config resolution.
    pub redeem_interval_secs: Option<u64>,
    /// Deposit (base units, `µUSDC`) the buyer path escrows when it **opens** a
    /// payment pool, and the target every top-up refills the pool balance
    /// toward once it is reused or runs short mid-transfer. The shared pool is
    /// fully withdrawable, so the buyer opens at this amount directly rather
    /// than escrowing a smaller first-contact lock. Both refill legs target
    /// it: the proactive low-water refill, which both binaries run on pool
    /// reuse, and the reactive mid-transfer top-up, which the `decdn fetch`
    /// streaming path and the daemon's node-to-node cache-miss pull both run
    /// (#1530). `decdn bundle pull` is the one remaining fetch path with the
    /// proactive leg only. Larger values amortize gas across more delivery at
    /// the cost of more capital locked. Absent => default (10 USDC =
    /// `10_000_000`). Must be nonzero (the pool's `openPool` reverts on a zero
    /// deposit).
    pub buyer_working_deposit_micro_usdc: Option<u64>,
    /// Whether to issue an unlimited (max) USDC approval for the
    /// `PaymentPool` contract so the buyer path can join a pool (#744).
    /// The absent-default is **profile-dependent**: the node daemon defaults to
    /// `true` (a long-lived operator amortizes one unlimited approval across many
    /// node-to-node miss pulls), while the `decdn` client fetch commands (`fetch`,
    /// `bundle pull`) default to `false`, i.e. an **exact deposit-sized** approval
    /// scoped to what each pool escrows. Set `true` on the client to opt a
    /// power user back into the unlimited allowance.
    pub buyer_max_approve: Option<bool>,
    /// The node's refundable floor `M` (base units, `µUSDC`): the minimum
    /// remaining on-chain pool balance the node insists on keeping in reserve
    /// (ADR 003 § Sizing, `M = k·ρ·B·Δ` — a function of the redeem cadence `k`,
    /// the node's advertised rate `ρ`, the credit window `B`, and the round-trip
    /// slack `Δ`). The node stops serving a pool once its remaining on-chain
    /// balance minus `M` can no longer cover the next credit window, so a buyer
    /// that drains its pool balance cannot leave the node's already-delivered,
    /// not-yet-redeemed bytes stranded. Absent => default (1 USDC =
    /// `1_000_000` `µUSDC`), a conservative static value at the redeem-threshold
    /// scale; sizing this precisely per ADR 003 is governance/ops policy, not a
    /// build-time constant.
    pub pool_min_remaining_deposit_micro_usdc: Option<u64>,
    /// USDC bond-funding swap venue for `decdn setup --pay-bond-with usdc`
    /// (#991). One of `uniswap-v3` / `balancer-v3`. Absent => no swap (the
    /// operator funds the bond in TOKEN directly). Consumed only by the CLI
    /// `setup`/`bond` path (see `cli::commands::chain_ctx`), not the node
    /// runtime — declared here so a config that drives `setup` also passes
    /// `decdn config validate` under this section's `deny_unknown_fields`.
    pub swap_venue: Option<String>,
    /// Swap router contract address for the configured `swap_venue` (#991).
    /// Uniswap `SwapRouter02` or the Balancer V3 `Router`. Required by the CLI
    /// when `swap_venue` is set; CLI-consumed only (see [`Self::swap_venue`]).
    pub swap_router_address: Option<String>,
    /// Uniswap `QuoterV2` contract address (#991). Only the Uniswap venue needs
    /// it — Balancer quotes through its own router — so it is optional even when
    /// `swap_venue` is set. CLI-consumed only (see [`Self::swap_venue`]).
    pub swap_quoter_address: Option<String>,
    /// USDC token contract address the swap spends (#991). Required by the CLI
    /// when `swap_venue` is set; CLI-consumed only (see [`Self::swap_venue`]).
    pub usdc_address: Option<String>,
    /// Uniswap V3 pool fee tier (e.g. `500`/`3000`/`10000`) for the swap (#991).
    /// Only meaningful for the Uniswap venue. CLI-consumed only (see
    /// [`Self::swap_venue`]).
    pub swap_fee_tier: Option<u32>,
    /// Balancer V3 pool contract address for the swap (#991). Only meaningful
    /// for the Balancer venue (V3 pools are addressed directly). CLI-consumed
    /// only (see [`Self::swap_venue`]).
    pub swap_balancer_pool: Option<String>,
    /// Uniswap V3 TOKEN/USDC pool address for the advisory price-impact check
    /// (#991). Optional even for the Uniswap venue — unset degrades the spot
    /// read to the quoter's expected-in (the price-impact gate stays inert).
    /// CLI-consumed only (see [`Self::swap_venue`]).
    pub swap_pool_address: Option<String>,
}

/// Cache section of the config file.
///
/// `deny_unknown_fields` is set so that operators upgrading from the
/// pre-#437 schema (flat `origin_url` / `origin_path` / `decompress`
/// fields) get a clear "unknown field" error at config load instead of
/// a silent "no origin configured" surprise at the first cache miss.
/// The new schema lives under the tagged `[cache.origin]` table — see
/// [`OriginConfig`].
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    /// Blob cache directory.
    pub cache_dir: Option<PathBuf>,
    /// Maximum cache size in megabytes.
    pub cache_size_mb: Option<u64>,
    /// Maximum single blob size in megabytes.
    pub max_blob_size_mb: Option<u64>,
    /// Buyer-side ABSOLUTE per-MB rate ceiling for paid pulls (#1375), in the same
    /// per-MB units as the wire `StreamResponse.rate_per_mb`. Absent / `0` =
    /// unlimited. Bounds what this node, as a BUYER on a cache-miss pull, will
    /// accept a provider to quote — on top of the always-applied probe-relative
    /// bound. Distinct from the seller-side `delivery_floor` clamp, which raises
    /// this node's own quote rather than bounding what it will pay.
    pub max_rate_per_mb: Option<u64>,
    /// Single origin backend for cache pull-through (#437). Absent =>
    /// no pull-through (unless [`Self::origins`] is set); cache misses
    /// fail with `NoOrigin`. The variant (`http`, `fs`, or `s3`) is
    /// selected by the `kind` field on the inner `[cache.origin]`
    /// table. Mutually exclusive with [`Self::origins`] — operators
    /// who need an ordered fallback list use the `[[cache.origins]]`
    /// array-of-tables form instead. Setting both at once is rejected
    /// at config resolution.
    pub origin: Option<OriginConfig>,
    /// Ordered list of origin backends with automatic fallback (#284).
    /// On a cache miss the engine tries each entry in order; the next
    /// entry is consulted when the current one returns `NotFound`,
    /// returns a permanent error, or exhausts its per-origin transient
    /// retry budget. Deterministic per-origin failures (hash mismatch,
    /// blob-too-large) do **not** trigger fallback — they indicate a
    /// misbehaving or misconfigured backend that must surface, not be
    /// masked. Mutually exclusive with [`Self::origin`]; an empty
    /// array is rejected (omit the key for "no pull-through" rather
    /// than configuring zero origins). Duplicate entries are
    /// permitted but logged as a warning at startup — operators may
    /// legitimately want two entries with the same target for
    /// connection-pool sharding. Set once at startup; changes require
    /// a process restart, consistent with [`Self::origin_retry`].
    ///
    /// Worst-case *backoff* latency for an all-failing chain is
    /// `len(origins) × sum_of_backoffs(origin_retry)` plus per-attempt
    /// origin RTT/timeout. With default [`decdn_config_types::RetryPolicy`]
    /// (3 retries, sleeps `100 + 200 + 400 ms` ± jitter — see
    /// `RetryPolicy::default`) that's ~700 ms backoff per origin
    /// (~2.1 s for three origins) — *not* `max_retries × backoff_cap`,
    /// because the default schedule never saturates the 10 s cap. The
    /// `max_backoff_ms` cap only dominates if `initial_backoff_ms` is
    /// raised enough to saturate it; for the worst-case-cap regime use
    /// `max_retries × max_backoff_ms` and multiply by chain length.
    /// Plan operator timeouts accordingly — RTT/timeout per attempt
    /// usually dominates the backoff term.
    pub origins: Option<Vec<OriginConfig>>,
    /// Hex-encoded BLAKE3 hashes that must stay cached regardless of LRU
    /// pressure (#276). Each entry is 64 lowercase hex chars (BLAKE3
    /// digest size). Invalid hex or wrong-length entries cause config
    /// resolution to fail — fail-fast at load time beats a silent
    /// "entry was ignored" surprise hours later when the operator
    /// discovers the blob got evicted anyway.
    pub pinned_hashes: Option<Vec<String>>,
    /// Origin pull-through retry policy (#285, extended in #519).
    /// Controls exponential backoff for transient HTTP / S3 /
    /// filesystem errors *and* the body-phase retry strategy for
    /// mid-stream `io::Error`s. Absent => defaults from
    /// [`decdn_config_types::RetryPolicy::default`] (3 retries, 100ms
    /// initial backoff doubling to 10s cap, 10% jitter,
    /// `buffered_max_bytes = 4 MiB`). `max_retries = 0` opts out and
    /// reproduces pre-#285 behaviour; `buffered_max_bytes = 0`
    /// independently disables the body-phase buffer path so all
    /// body-phase failures route through the streaming abort+restart
    /// path. Set once at startup; changes require a restart.
    ///
    /// **Body-phase retry trade-offs (#519):**
    ///
    /// - **Buffer-then-commit** applies when the origin's advertised
    ///   `size_hint` is at or below `buffered_max_bytes`. The body
    ///   drains into memory before the iroh-blobs commit, so failed
    ///   attempts cost RAM (bounded by `buffered_max_bytes` per
    ///   in-flight fetch) but leave no on-disk garbage.
    /// - **Abort + restart** applies above the threshold or when
    ///   `size_hint` is unknown. Each failed attempt strands up to
    ///   `max_blob_size_mb` of partial-import bytes until iroh-blobs
    ///   GC reclaims them at `cache.gc_interval_sec` cadence.
    ///   Worst-case disk amplification per fetch is
    ///   `(1 + max_retries) * max_blob_size_mb`.
    ///
    /// `decdn_config_types::RetryPolicy` carries `#[serde(default)]` so
    /// partial sections (e.g. just `max_retries = 5`) get the rest of
    /// the fields filled from defaults.
    pub origin_retry: Option<decdn_config_types::RetryPolicy>,
    /// Per-origin circuit-breaker policy (#963). Fronts each origin's
    /// pull-through retry loop: after `failure_threshold` consecutive
    /// origin-unavailable failures (transient errors that exhausted the
    /// retry budget — NOT 404s or other permanent per-object errors) the
    /// breaker trips OPEN and fast-fails every miss for `cooldown_ms`
    /// without incurring any retry/backoff, then admits
    /// `half_open_max_calls` trial pulls to probe recovery. Absent =>
    /// defaults from [`decdn_config_types::CircuitBreakerPolicy::default`]
    /// (enabled, trip after 5 failures, 30s cooldown, 1 half-open trial).
    /// Set `enabled = false` (or `failure_threshold = 0`) to opt out and
    /// reproduce pre-#963 behaviour where every miss runs the full retry
    /// loop regardless of origin health. Set once at startup; changes
    /// require a restart.
    ///
    /// `CircuitBreakerPolicy` carries `#[serde(default)]` so partial
    /// sections (e.g. just `cooldown_ms = 60000`) get the rest of the
    /// fields filled from defaults.
    pub circuit_breaker: Option<decdn_config_types::CircuitBreakerPolicy>,
    /// Optional `User-Agent` override sent on every HTTP origin
    /// pull-through request (#435). Absent => the workspace default
    /// (`decdn-node/<version>`); set to attribute CDN traffic in origin
    /// access logs or to apply origin-side rate limits and routing
    /// policy that distinguish CDN pulls from end-user clients. Empty
    /// string is rejected at resolution.
    pub user_agent: Option<String>,
    /// Interval between iroh-blobs GC sweeps in seconds (#518). Absent =>
    /// [`crate::config::DEFAULT_GC_INTERVAL_SEC`]. Set to `0` to disable
    /// the periodic sweep — operators with external disk-reclaim
    /// scheduling can opt out, accepting that bytes orphaned by
    /// hash-mismatch / mid-stream-error pull-through paths leak until
    /// they manually reset state. Tighter intervals shrink the
    /// hostile-origin amplification window at the cost of more
    /// list+sweep CPU per minute.
    pub gc_interval_sec: Option<u64>,
    /// Interval between origin-held-index rescans in seconds (#1130). Absent =>
    /// [`crate::config::DEFAULT_FS_RESCAN_INTERVAL_SEC`]. The node re-walks its
    /// `fs` origin directory (and re-checks present pins) at this cadence so a
    /// file dropped into the origin becomes discoverable — probe `has_blob` +
    /// DHT announce — within one interval, without a restart or reload. `0`
    /// disables the periodic rescan (startup + `decdn node reload` still run
    /// it). Shorter intervals pick up new files faster at the cost of more
    /// directory-walk + `size()` I/O per minute.
    pub fs_rescan_interval_sec: Option<u64>,
    /// TTL, in seconds, for a memoised positive live-origin probe answer (#1130
    /// pt3). Absent => [`crate::config::DEFAULT_ORIGIN_PROBE_TTL_SEC`] (15). A
    /// probe for a hash absent from the `fs`-enumeration ∪ pins index falls back
    /// to a live `HEAD`/`HeadObject` against the http/s3 origin; a present-with-
    /// size answer is cached for this long so a non-pinned bucket object is
    /// discoverable without a per-probe origin round-trip on every probe.
    /// Shorter TTLs track origin deletions faster at the cost of more `HEAD`
    /// traffic. See `origin_probe_negative_ttl_sec` for the absent-answer TTL.
    pub origin_probe_ttl_sec: Option<u64>,
    /// TTL, in seconds, for a memoised negative (absent) live-origin probe
    /// answer (#1130 pt3). Absent =>
    /// [`crate::config::DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL_SEC`] (2). Caching the
    /// negative answer is what blunts a random-hash probe flood; keeping this
    /// TTL short bounds how long a stale `Absent` can hide newly-available own
    /// content from a probe.
    pub origin_probe_negative_ttl_sec: Option<u64>,
    /// TTL, in seconds, for a memoised fault live-origin probe answer (#1130
    /// pt3). Absent => [`crate::config::DEFAULT_ORIGIN_PROBE_FAULT_TTL_SEC`]
    /// (5). A fault is a backend outage affecting an entire namespace, not a
    /// per-hash absence, so it needs a longer memo TTL than `Absent` to stop a
    /// failing origin being re-probed on every request, yet shorter than
    /// `origin_probe_ttl_sec` so a recovered origin is re-probed promptly.
    pub origin_probe_fault_ttl_sec: Option<u64>,
    /// Per-probe ceiling, in milliseconds, on the live-origin `HEAD`/`HeadObject`
    /// (#1130 pt3). Absent => [`crate::config::DEFAULT_ORIGIN_PROBE_TIMEOUT_MS`]
    /// (2000). A slow origin must never stall the probe hot path; on timeout the
    /// probe answers `has_blob: false` (safe — never slashable post-#1512) and
    /// the miss is memoised as absent for one TTL.
    pub origin_probe_timeout_ms: Option<u64>,
    /// Maximum distinct hashes held in the live-origin probe memo (#1130 pt3).
    /// Absent => [`crate::config::DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY`] (4096).
    /// Bounds memo memory under a random-hash probe flood; the cache is
    /// best-effort, so at capacity one arbitrary entry is dropped to admit a new
    /// answer (expired entries reclaimed first).
    pub origin_probe_memo_capacity: Option<u64>,
    /// Cache eviction driver: percent of [`Self::cache_size_mb`] above which
    /// the driver actively evicts (#1173, ADR 040). Absent =>
    /// [`crate::config::DEFAULT_EVICTION_HIGH_WATER_PCT`] (90). Hard bounds
    /// `[60, 95]`; set above the 25% probe-hold recommendation so a full hold
    /// budget plus in-flight writes don't trip it, below 95% for write
    /// headroom between sweeps.
    pub eviction_high_water_pct: Option<u64>,
    /// LRU eviction driver: percent of [`Self::cache_size_mb`] the driver
    /// evicts down to before returning to idle (#1173). Absent =>
    /// [`crate::config::DEFAULT_EVICTION_TARGET_PCT`] (80). Hard bounds
    /// `[40, 90]`, and MUST be `<= eviction_high_water_pct - 5` — the 5-point
    /// hysteresis gap is structural (prevents thrash on writes hovering near
    /// the trigger), not a tunable nicety.
    pub eviction_target_pct: Option<u64>,
    /// LRU eviction driver: maximum candidates removed per tick before the
    /// driver yields (#1173). Absent =>
    /// [`crate::config::DEFAULT_EVICTION_PER_SWEEP_BUDGET`] (16). Hard bounds
    /// `[1, 256]`. Bounds worst-case driver-induced latency on the cache hot
    /// path (~1 ms filesystem-unlink per entry); the driver continues across
    /// consecutive ticks until below target or candidates are exhausted.
    pub eviction_per_sweep_budget: Option<u64>,
    /// LRU eviction driver: seconds between driver wakeups (#1173). Absent =>
    /// [`crate::config::DEFAULT_EVICTION_TICK_SECS`] (1). Hard bounds
    /// `[1, 60]`.
    ///
    /// **Cost note:** every tick measures the on-disk footprint, which walks the
    /// blob list and issues one `status()` per blob, plus an O(n) clone of the
    /// pinned set — latched or not. An idle tick is not free: the driver keeps
    /// no cached footprint, so it re-measures on every tick (ADR 040). On a
    /// cache holding many blobs,
    /// raise this to trade eviction latency for steady-state store load.
    pub eviction_tick_secs: Option<u64>,
    /// Maximum number of concurrently held (eviction-exempt) blobs for the
    /// probe-triggered hold (ADR 005 §Hold budget, #318). Holds are
    /// per-blob: multiple peers probing the same hash share one slot. When the
    /// budget is exhausted, a probe for a blob that is present but gets no slot
    /// still advertises (`has_blob: true`) and places no eviction hold — the
    /// blob may be LRU-evicted before the pull, costing one wasted round trip
    /// (never a slash). Absent => [`crate::config::DEFAULT_MAX_PROBE_HOLDS`]
    /// (256). `0` is the operator opt-out: holds are disabled and probes answer
    /// `has_blob: false` for store-backed content — content servable from a
    /// configured origin takes no hold and is still advertised. Operators with
    /// small caches SHOULD set this to ≤25% of cache capacity.
    pub max_probe_holds: Option<u64>,
    /// Number of probe-hold slots reserved for the **stake lane** —
    /// registered operators issuing node-to-node cache-miss probes (#757,
    /// ADR 003 §Admission and Priority). An end-client probe places no eviction
    /// hold once usage reaches `max_probe_holds - stake_lane_reserved_holds` (it
    /// still advertises `has_blob: true` if the blob is present), keeping the
    /// last `stake_lane_reserved_holds` slots available for node-to-node probes
    /// so end-client load cannot starve their holds. Absent / `0` (the default)
    /// disables the reservation entirely — a single-lane node is unaffected.
    /// Values `>= max_probe_holds` reserve the whole budget for the stake lane:
    /// no end-client probe takes a hold slot, regardless of current usage.
    pub stake_lane_reserved_holds: Option<u64>,
    /// Enable node-to-node paid cache-miss pull-through (#831, ADR 001/022).
    /// Absent / `false` (the default) → a cache miss serves `NotFound` as
    /// before. When `true` *and* the buyer-channel service bootstrapped, a
    /// miss triggers DHT provider discovery → probe → ranked paid pull from an
    /// upstream node, which populates the cache and is then served. OFF by
    /// default for the initial network: enabling it makes the node front USDC
    /// egress to fill misses (bounded by `blockchain.buyer_working_deposit_micro_usdc`
    /// and the upstream's per-MB rate), and the serving path only triggers it
    /// behind a valid, pool-bound client so an unpaid request cannot drive
    /// egress.
    pub node_to_node_pull_through_enabled: Option<bool>,
    /// When `false`, the node serves and seeds only content its own backend
    /// holds; a foreign hash is declined like a miss. Absent resolves to a
    /// role-derived default: `false` (origin-only) when an origin backend is
    /// configured, `true` (relay) when none is. Set this explicitly to
    /// override either default. Node-local; `cache.*` is restart-required.
    pub relay_foreign_namespaces: Option<bool>,
    /// Number of discovered providers to probe before ranking on a
    /// node-to-node pull (#831). Absent =>
    /// [`crate::config::DEFAULT_NODE_PULL_PROBE_FANOUT`] (5). Higher widens
    /// provider choice at the cost of more probe round trips per miss; `0`
    /// probes none, so no pull can succeed (a way to disable the pull while
    /// keeping the feature flag on).
    pub node_pull_probe_fanout: Option<usize>,
    /// Wall-clock timeout in seconds for the STREAM-OPEN stage of a single upstream pull
    /// on a node-to-node miss (#831) — connect, handshake, and the
    /// signed `StreamResponse`. It does NOT bound the buyer-channel open, which precedes
    /// it on its own 5 s budget. Absent =>
    /// [`crate::config::DEFAULT_NODE_PULL_TIMEOUT_SEC`] (20). Bounds how long a
    /// miss blocks the serving path on one upstream before falling through to
    /// the next ranked candidate (or `NotFound`). This is the *per-upstream*
    /// budget; the overall pull-through deadline is derived as roughly
    /// `MAX_PROVIDER_ATTEMPTS × (channel open + it + stall)` plus a fixed discovery
    /// allowance, so the fallback loop reaches every ranked candidate (#859).
    ///
    /// It does NOT bound the streaming stage (#1134) — that is
    /// [`Self::node_pull_stall_timeout_sec`]. A wall clock over the bytes would
    /// cap the blob size a node can pull through at roughly
    /// `this × link speed`.
    pub node_pull_timeout_sec: Option<u64>,
    /// Inactivity timeout in seconds for the STREAMING stage of an upstream pull
    /// (#1134). Absent => [`crate::config::DEFAULT_NODE_PULL_STALL_TIMEOUT_SEC`]
    /// (20). The clock resets on every byte received, so this trips only when an
    /// upstream goes silent mid-transfer — never because a blob is large or a link
    /// is slow. It is what makes a pull of any size safe to leave uncapped.
    ///
    /// Tune it against upstream responsiveness, not content size: too low and a
    /// brief network hiccup abandons a healthy transfer (and scores the upstream
    /// `Unreachable`); too high and a dead upstream is held onto for longer than
    /// necessary before the fallback loop moves on.
    ///
    /// "Longer" is multiplied, not added. A silent candidate costs one full window of
    /// this, and the derived outer deadline budgets that for EVERY candidate, so a second
    /// here is ~3 seconds of worst-case client wait on a total miss (167.5 s at defaults).
    pub node_pull_stall_timeout_sec: Option<u64>,
    /// Cache eviction policy selector (ADR 040). Absent =>
    /// [`crate::config::DEFAULT_EVICTION_POLICY`] (`"lru"`). Must be `"lru"` or
    /// `"tinylfu"`; an unrecognized name is rejected at config load rather than
    /// silently falling back.
    pub eviction_policy: Option<String>,
    /// Cache admission policy selector (ADR 040). Absent =>
    /// [`crate::config::DEFAULT_ADMISSION_POLICY`] (`"always"`). Must be
    /// `"always"` or `"tinylfu"`; an unrecognized name is rejected at config
    /// load rather than silently falling back.
    pub admission_policy: Option<String>,
    /// W-TinyLFU tuning knobs (ADR 040). Consulted whenever
    /// [`Self::eviction_policy`] or [`Self::admission_policy`] is `"tinylfu"`;
    /// otherwise unused. Absent => every field defaults.
    pub tinylfu: Option<TinyLfuConfig>,
    /// Refuse-to-serve economics tuning (ADR 041): the margin policy, its
    /// discount, and per-source speculative-warming allowance. Absent =>
    /// every field defaults.
    pub serve_economics: Option<ServeEconomicsConfig>,
}

/// W-TinyLFU tuning knobs (ADR 040). See [`CacheConfig::tinylfu`].
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TinyLfuConfig {
    /// Count-min sketch size in bytes, shared by admission and eviction.
    /// Absent => [`crate::config::DEFAULT_TINYLFU_SKETCH_BYTES`] (262144).
    /// Larger sketches reduce frequency-estimate collisions at the cost of
    /// more resident memory.
    pub sketch_bytes: Option<usize>,
    /// Prior sightings a probation member needs before promotion to the main
    /// segment. Absent => [`crate::config::DEFAULT_TINYLFU_PROMOTION_THRESHOLD`]
    /// (2).
    pub promotion_threshold: Option<u32>,
    /// Percent of `cache.cache_size_mb` the probation segment is capped to.
    /// Absent => [`crate::config::DEFAULT_TINYLFU_PROBATION_TARGET_PCT`] (10).
    /// Hard bounds `[1, 50]`.
    pub probation_target_pct: Option<u64>,
    /// Reserved: half-life in seconds for aging the frequency sketch. Absent
    /// => [`crate::config::DEFAULT_TINYLFU_AGING_HALFLIFE_SEC`] (600).
    /// Currently resolved and stored but not consulted — the shipped sketch
    /// ages via a fixed sample-count reset rather than a wall-clock half-life.
    pub aging_halflife_sec: Option<u64>,
}

/// Refuse-to-serve economics tuning knobs (ADR 041). See
/// [`CacheConfig::serve_economics`].
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServeEconomicsConfig {
    /// Refuse-to-serve policy selector. `"off"` or `"margin"`. Absent =>
    /// [`crate::config::DEFAULT_SERVE_ECONOMICS_POLICY`] (`"margin"`). An
    /// unrecognized name is rejected at config load rather than silently
    /// falling back.
    pub policy: Option<String>,
    /// Discount applied to the sell price when deciding whether a
    /// speculative pull clears its margin gate. Within `(0.0, 1.0]`. Absent
    /// => [`crate::config::DEFAULT_SERVE_ECONOMICS_DISCOUNT_BPS`] (5000,
    /// i.e. `0.5`).
    pub discount: Option<f64>,
    /// Maximum number of concurrent speculative-warming sources. `>= 1`.
    /// Absent => [`crate::config::DEFAULT_SERVE_ECONOMICS_N_MAX`] (64).
    pub n_max: Option<u32>,
    /// Per-source speculative-warming allowance, in payment base units.
    /// `> 0`. Absent =>
    /// [`crate::config::DEFAULT_SERVE_ECONOMICS_WARMING_BUDGET`]
    /// (5,000,000).
    pub warming_budget: Option<u64>,
    /// Per-source allowance refill rate, in payment base units per second.
    /// `>= 0`; `0` disables time-based refill (allowance only resets on
    /// restart or via the serve-vindicated upgrade). Absent =>
    /// [`crate::config::DEFAULT_SERVE_ECONOMICS_WARMING_REFILL`] (58).
    pub warming_refill: Option<u64>,
}

/// Origin backend selection (#437). Tagged on the inner `kind` field.
///
/// `deny_unknown_fields` is set on the enum and on each variant's
/// payload so that an operator typo (`decompres = "auto"` on the Http
/// variant, for instance) fails at config load instead of silently
/// no-opping.
///
/// ```toml
/// [cache.origin]
/// kind = "http"
/// url = "https://origin.example/"
/// decompress = "auto"          # optional; defaults to "auto"
///
/// # — or —
/// [cache.origin]
/// kind = "fs"
/// path = "/var/lib/decdn/origin"
///
/// # — or —
/// [cache.origin]
/// kind = "s3"
/// bucket = "decdn-blobs"
/// region = "us-east-1"
/// # endpoint_url = "https://<accountid>.r2.cloudflarestorage.com"  # for R2/B2/MinIO
/// # path_style = true                                               # for MinIO
/// # prefix = "blobs/"
/// # [cache.origin.credentials] source = "static" / "default-chain"
/// ```
///
/// For multi-origin fallback (#284) use the plural `[[cache.origins]]`
/// array-of-tables form (mutually exclusive with `[cache.origin]`):
///
/// ```toml
/// [[cache.origins]]
/// kind = "http"
/// url = "https://primary.example/"
///
/// [[cache.origins]]
/// kind = "s3"
/// bucket = "mirror-blobs"
/// region = "us-east-1"
///
/// [[cache.origins]]
/// kind = "fs"
/// path = "/var/lib/decdn/archive"
/// ```
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum OriginConfig {
    /// HTTP(S) origin served at `{url}/{blake3_hex}`.
    Http {
        /// Base URL; must be `http`/`https` and is path-normalized to
        /// end with `/` so `{base}.join(&hex)` produces `{base}/{hex}`.
        url: String,
        /// How to handle `Content-Encoding` on the HTTP response
        /// (#312). `"auto"` (default) decompresses gzip/zstd
        /// transparently; `"strict"` refuses any non-identity
        /// encoding. The BLAKE3 content-address is computed over the
        /// canonical (decompressed) form, so `"strict"` is only safe
        /// for origins guaranteed to serve already-canonical bytes.
        decompress: Option<decdn_config_types::DecompressMode>,
    },
    /// Local filesystem origin rooted at `path`; blobs live at
    /// `{path}/{hex[0..2]}/{hex}`.
    Fs {
        /// Filesystem root.
        path: PathBuf,
    },
    /// S3-compatible object storage (#437): AWS S3, Cloudflare R2,
    /// Backblaze B2, `MinIO`, etc. Object key layout is
    /// `{prefix?}{hex[0..2]}/{hex}` — sharded the same way as the
    /// filesystem backend so operators can copy blobs between
    /// backends without rewriting tooling.
    S3(S3OriginConfig),
}

/// S3-compatible origin configuration (#437) — wire form.
///
/// Validation runs at config-resolution time via the (private)
/// `resolve_s3_origin` helper, which produces the runtime form
/// [`super::resolved::ResolvedS3Config`]. Unvalidated instances of
/// this type cannot reach the cache wiring layer because
/// [`super::resolved::ResolvedOrigin::S3`] holds the resolved form,
/// not this one.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3OriginConfig {
    /// Bucket name. Validated DNS-safe at config resolution
    /// (lowercase `[a-z0-9.-]`, 3–63 chars; see
    /// `validate_s3_bucket_name`).
    pub bucket: String,
    /// AWS region (e.g. `"us-east-1"`). Required because the S3 SDK
    /// uses it for `SigV4` signing even when a custom `endpoint_url`
    /// is set. Validated non-empty at config resolution.
    pub region: String,
    /// Custom endpoint URL for non-AWS S3-compatible providers (R2,
    /// B2, `MinIO`). Omit for AWS S3. Validated as `http`/`https` at
    /// config resolution.
    pub endpoint_url: Option<String>,
    /// Use path-style addressing (`https://endpoint/bucket/key`)
    /// instead of virtual-hosted-style (`https://bucket.endpoint/key`).
    /// Required `true` for `MinIO` and many self-hosted providers;
    /// AWS S3 and Cloudflare R2 use virtual-hosted-style by default.
    /// Absent => fall back to the SDK default (virtual-hosted-style).
    pub path_style: Option<bool>,
    /// Optional key prefix prepended to every fetched object. Final
    /// key is `{prefix}{hex[0..2]}/{hex}`. Validation rejects
    /// `prefix` starting with `/`, containing `..`, containing
    /// backslash, or containing ASCII control / whitespace
    /// characters; auto-appends a trailing `/` if the prefix is
    /// non-empty and missing one (mirrors `parse_origin_url`'s
    /// trailing-slash normalization).
    pub prefix: Option<String>,
    /// Credential source. Absent => use the AWS default credential
    /// chain (env vars, `~/.aws/credentials`, IAM role / instance
    /// profile).
    pub credentials: Option<S3Credentials>,
    /// How to handle `Content-Encoding` on the S3 response (#804).
    /// `"auto"` (default) decompresses gzip/zstd transparently;
    /// `"strict"` refuses any non-identity encoding. The BLAKE3
    /// content-address is computed over the canonical (decompressed)
    /// form, so `"strict"` is only safe for origins guaranteed to serve
    /// already-canonical bytes. Mirrors the HTTP origin's `decompress`
    /// knob.
    pub decompress: Option<decdn_config_types::DecompressMode>,
}

/// S3 credential source (#437). Tagged on the inner `source` field.
///
/// Static credential fields use [`SecretString`] so an incidental
/// `tracing::debug!(?cfg)` or panic backtrace cannot leak the
/// material — the wrapper redacts itself in `Debug` output, and
/// its `Serialize` impl emits a non-reversible hashed placeholder
/// rather than the cleartext, so any incidental serialization of the
/// config is redacted too (see [`SecretString`] for the full
/// contract). The codebase already redacts HTTP-origin URL credentials
/// (see `decdn_config_types::redact_for_log`); this is the same pattern
/// for TOML-borne secrets.
///
/// ```toml
/// [cache.origin.credentials]
/// source = "static"
/// access_key_id = "AKIA..."
/// secret_access_key = "..."
/// # session_token = "..."        # optional
///
/// # — or —
/// [cache.origin.credentials]
/// source = "default-chain"
/// # profile = "my-profile"       # optional, override default profile
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "kebab-case", deny_unknown_fields)]
pub enum S3Credentials {
    /// Static IAM credentials embedded in the config file. Common
    /// for R2/B2/MinIO operator deployments where simple key-pair
    /// audit is preferred; for AWS S3, prefer the default credential
    /// chain so credentials live outside the TOML.
    Static {
        /// AWS access key ID. Stored in [`SecretString`] so it
        /// redacts in `Debug` output alongside the secret key.
        access_key_id: SecretString,
        /// AWS secret access key. Redacted in `Debug` output.
        secret_access_key: SecretString,
        /// Optional STS session token (for assume-role / SSO flows
        /// where a static key isn't enough on its own). Redacted.
        session_token: Option<SecretString>,
    },
    /// Use the standard AWS credential chain: `AWS_ACCESS_KEY_ID` /
    /// `AWS_SECRET_ACCESS_KEY` env vars, then `~/.aws/credentials`
    /// profile, then IAM role / instance profile.
    DefaultChain {
        /// Override the default profile name when reading
        /// `~/.aws/credentials`. Threaded through to the SDK as the
        /// equivalent of the `AWS_PROFILE` env var.
        profile: Option<String>,
    },
}

/// Payment section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaymentConfig {
    /// Rate per MB in USDC base units.
    pub rate_per_mb: Option<u64>,
    /// Pre-chain **seed** for the lower bound the node clamps `rate_per_mb` to
    /// before signing a `ProbeResponse` (ADR 005 §Rate bounds validation).
    ///
    /// This does not govern the live clamp: the node reads
    /// `PaymentPool.getRateBounds()` at startup and overwrites this value
    /// before it serves anything, then tracks `RateBoundsUpdated`. Setting it
    /// only affects the window before that read completes (and a failed read
    /// refuses startup outright), so treat the on-chain value as authoritative.
    /// Absent => `0`.
    pub delivery_floor: Option<u64>,
    /// Downstream credit-window ceiling in bytes (ADR 003 §Credit window). The
    /// per-stream window ramps toward this cap as the stream pays, bounding the
    /// node's credit exposure (unbilled egress already on the wire) to
    /// `paid / credit_ramp_divisor`; the client's exposure stays zero (vouchers
    /// are cumulative over delivered bytes). Absent =>
    /// [`crate::config::DEFAULT_CREDIT_MAX`] (64 MiB). Floored at one chunk
    /// ([`decdn_protocol::client::CHUNK_BYTES`], 1 MiB); a value at or below one
    /// chunk reproduces the strict stop-and-wait cadence — deliver a chunk, then
    /// recoup it. It is node-local config, not a governance-owned parameter — no
    /// contract holds a delivery-cadence value.
    pub credit_max: Option<decdn_config_types::Bytes>,
    /// Ramp divisor for the credit window (ADR 003 §Credit window): the window is
    /// `paid / credit_ramp_divisor`, floored at one chunk and capped at
    /// [`Self::credit_max`]. Absent => [`crate::config::DEFAULT_CREDIT_RAMP_DIVISOR`]
    /// (2). `0` opens the full ceiling immediately, reproducing the flat-window
    /// behavior. It is node-local config, not a governance-owned parameter.
    pub credit_ramp_divisor: Option<u64>,
    /// Target wire-frame size for the serve path, in bytes (ADR 005
    /// §`cdn/client/v1`). Absent => [`crate::config::DEFAULT_FRAME_TARGET_BYTES`]
    /// (1 MiB). Must be > 0.
    ///
    /// Node-local policy that is never negotiated: no message carries it, the payer
    /// accepts any non-empty frame, and both bao verification and the payment meter
    /// are defined over byte counts rather than frame boundaries. Raising it lowers
    /// per-frame CPU per byte served, up to one `CHUNK_BYTES` payment interval
    /// (1 MiB) — a frame never crosses a payment-chunk boundary, so a larger value
    /// is rejected rather than silently ignored. The serve loop also clamps each
    /// frame to the credit window's remaining room, so a large value does not widen
    /// credit exposure.
    pub frame_target_bytes: Option<decdn_config_types::Bytes>,
    /// Background flush period for the lane store, in ms. See
    /// [`crate::config::DEFAULT_VOUCHER_COMMIT_INTERVAL_MS`] (5 s). Must be > 0.
    pub voucher_commit_interval_ms: Option<u64>,
}

/// Security / rate-limiting section of the config file.
///
/// All fields are optional; defaults produce a safe configuration out of
/// the box.
///
/// **All fields are hot-reloadable** on SIGHUP and via `admin_v1_reload`.
/// The live `ConnectionLimiter` rebuilds its keyed `governor` rate
/// limiter from the new quota and swaps it under an `RwLock`. The
/// `Arc<Semaphore>` identity is preserved across cap resizes via
/// `add_permits` / `acquire_many_owned(...).forget()` so every
/// in-flight `OwnedSemaphorePermit` keeps draining into the same
/// semaphore on `Drop`.
///
/// **Caveats:** during a shrink the live concurrency cap is `>= new`
/// until enough handlers drain; under racing reloads (N→N+1→N) the
/// post-task permit count may briefly land anywhere in `[N, N+1]`
/// until the next reload reconciles. Accepted as a known trade-off:
/// strict resize semantics would require a full handler-drain barrier.
///
/// Token-bucket state is *not* preserved across a reload — the keyed
/// limiter is rebuilt from scratch. Operators tuning the per-source
/// quota live should expect the next acquire after a reload to start
/// with a fresh burst budget.
///
/// **`0` means "disable this layer":**
///   - `max_concurrent_handlers = 0`: no global concurrency cap.
///   - `per_source_rate_per_sec = 0`: per-source rate-limit disabled. The
///     paired `per_source_burst` field is ignored — the limiter
///     short-circuits before the keyed map is consulted.
///   - `max_tracked_sources = 0`: rate-limit bookkeeping map is unbounded.
///     **Warning:** an attacker churning source addresses can grow the
///     map without bound in this mode — operator opt-in only.
///
/// `burst > 0` is required only when paired with a positive rate. Setting
/// `rate > 0` together with `burst = 0` would deny every request after
/// the first burst-many — the resolver rejects that combination as a
/// likely-typo. Setting `rate = 0` together with any `burst` value is
/// fine; burst is unused once the layer is disabled.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityConfig {
    /// Maximum number of concurrently in-flight QUIC handler tasks across
    /// all deCDN-authored ALPNs. New connections beyond this limit are
    /// closed immediately with `APP_ERR_RATE_LIMITED`. Default: 256.
    /// `0` disables the global cap (no concurrency limit).
    pub max_concurrent_handlers: Option<u32>,
    /// Refill rate for per-source rate limiting (cells per second).
    /// More generous than older per-NodeID-only schemes because a single
    /// source IP may host a legitimate fleet. Default: 100. `0.0`
    /// disables the per-source layer (the paired `per_source_burst` is
    /// then ignored).
    ///
    /// The current implementation keys on the remote `IpAddr`. IPv6
    /// addresses are bucketed by their `/64` prefix, not the full
    /// 128-bit address. A customer-grade IPv6 allocation is typically
    /// `/64` or larger, so without this prefix grouping an attacker can
    /// rotate through `2^64` distinct source addresses inside one
    /// allocation and trivially defeat the layer. IPv4 addresses are
    /// used in full.
    ///
    /// "Per-source" rather than "per-IP" because the keying axis may
    /// extend in the future (e.g. `NodeID`) without renaming the field.
    pub per_source_rate_per_sec: Option<f64>,
    /// Burst capacity for per-source limiting. Default: 200 (2× the
    /// rate, providing headroom for jitter so well-behaved peers don't
    /// trip the limit on naturally-clumped requests). Required `> 0`
    /// when `per_source_rate_per_sec > 0`; ignored when the layer is
    /// disabled (`per_source_rate_per_sec = 0`).
    pub per_source_burst: Option<u32>,
    /// Hard cap on the number of distinct sources tracked in the
    /// rate-limit state. When the live keyed map exceeds this size the
    /// limiter prunes entries whose state has refilled to a fresh
    /// baseline. Default: 4096. `0` makes the map unbounded — see the
    /// type-level docs for the operator-opt-in warning.
    pub max_tracked_sources: Option<usize>,
}

/// `[load_shed]` section — node-local load-shedding thresholds (overload
/// protection). All fields optional; unset falls back to the resolved
/// defaults.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadShedConfig {
    /// `resource-pressure` (default) or `always-admit`.
    pub policy: Option<String>,
    /// Serving egress budget in Mbps; `0` disables the egress ceiling.
    pub egress_budget_mbps: Option<u64>,
    /// Concurrency high-water mark (start shedding misses at/above).
    pub max_concurrent_serves_high: Option<u32>,
    /// Concurrency low-water mark (resume at/below).
    pub max_concurrent_serves_low: Option<u32>,
    /// Per-client concurrent-serve cap, enforced only under pressure; `0` off.
    pub per_client_serve_cap: Option<u32>,
}

/// `[dht]` section — `cdn/dht/v1` settings (ADR 022).
///
/// The Kademlia routing-table parameters (k, α, bucket count, refresh
/// interval) are pinned by the protocol and not exposed here. The
/// rate-limit knobs nest under `[dht.rate_limit]` to match ADR 022 §DHT
/// Rate Limiting and to leave room for other future `dht.*` top-level knobs (e.g.
/// bootstrap peers, republish overrides) without breaking the operator
/// key path.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DhtConfig {
    /// Rate-limiter settings (ADR 022 §DHT Rate Limiting). Absent =>
    /// the defaults from the ADR (20/100/1000 req/s with 40/200/2000
    /// bursts).
    pub rate_limit: Option<DhtRateLimitConfig>,
}

/// `[dht.rate_limit]` — three-layer token-bucket settings (ADR 022 §DHT
/// Rate Limiting).
///
/// Defaults match the ADR's conservative ceilings on adversarial load;
/// they are NOT steady-state operating targets — the ADR's bandwidth
/// analysis (§DHT Bandwidth Analysis "Headroom against rate limits")
/// shows realistic node traffic is two orders of magnitude below the
/// per-peer cap.
///
/// Setting any `*_rate_per_sec` to `0.0` disables that layer (operator
/// opt-out); the matching `*_burst` must also be `0` to avoid a deny-all
/// configuration. The resolver enforces this pairing the same way
/// `security.per_source_rate_per_sec` / `per_source_burst` are paired.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DhtRateLimitConfig {
    /// Per-peer (source `NodeId`) sustained rate. Absent => 20 req/s
    /// (ADR 022 §DHT Rate Limiting). `0.0` disables the layer.
    pub per_peer_rate_per_sec: Option<f64>,
    /// Per-peer burst capacity. Absent => 40. Required `> 0` when
    /// `per_peer_rate_per_sec > 0`.
    pub per_peer_burst: Option<u32>,
    /// Per-IP sustained rate. Absent => 100 req/s. `0.0` disables.
    pub per_ip_rate_per_sec: Option<f64>,
    /// Per-IP burst capacity. Absent => 200.
    pub per_ip_burst: Option<u32>,
    /// Global inbound DHT sustained rate. Absent => 1000 req/s.
    pub global_rate_per_sec: Option<f64>,
    /// Global inbound DHT burst capacity. Absent => 2000.
    pub global_burst: Option<u32>,
    /// Hard cap on the per-IP keyed-limiter map. Absent => 4096. `0`
    /// makes the map unbounded — operator opt-in (#645). Mirrors
    /// `security.max_tracked_sources` for the dispatch layer.
    pub max_tracked_per_ip: Option<usize>,
    /// Hard cap on the per-peer (`NodeId`) keyed-limiter map. Absent =>
    /// 4096. `0` makes the map unbounded (#645).
    pub max_tracked_per_peer: Option<usize>,
}

/// `[probe]` section — `cdn/probe/v1` settings (ADR 005).
///
/// The rate-limit knobs nest under `[probe.rate_limit]` to match ADR 005
/// §Probe rate limiting and to leave room for other future `probe.*` top-level knobs without
/// breaking the operator key path.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeConfig {
    /// Rate-limiter settings (ADR 005 §Probe rate limiting). Absent =>
    /// the defaults from the ADR (5/50/1000 req/s with 5/200/2000 bursts).
    pub rate_limit: Option<ProbeRateLimitConfig>,
}

/// `[probe.rate_limit]` — three-layer token-bucket settings (ADR 005 §Probe
/// rate limiting).
///
/// Mirrors `[dht.rate_limit]` in shape; only the defaults differ (ADR 005
/// specifies a tighter per-peer cap than ADR 022's DHT layer because a probe
/// is unauthenticated and cheaper to flood). Setting any `*_rate_per_sec` to
/// `0.0` disables that layer (operator opt-out); the matching `*_burst` must
/// also be `0` to avoid a deny-all configuration — the resolver enforces this
/// pairing, the same way `security.per_source_*` and `dht.rate_limit.*` are
/// paired.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeRateLimitConfig {
    /// Per-peer (source `NodeId`) sustained rate. Absent => 5 req/s
    /// (ADR 005 §Probe rate limiting). `0.0` disables the layer.
    pub per_peer_rate_per_sec: Option<f64>,
    /// Per-peer burst capacity. Absent => 5. Required `> 0` when
    /// `per_peer_rate_per_sec > 0`.
    pub per_peer_burst: Option<u32>,
    /// Per-IP sustained rate. Absent => 50 req/s. `0.0` disables.
    pub per_ip_rate_per_sec: Option<f64>,
    /// Per-IP burst capacity. Absent => 200.
    pub per_ip_burst: Option<u32>,
    /// Global inbound probe sustained rate. Absent => 1000 req/s.
    pub global_rate_per_sec: Option<f64>,
    /// Global inbound probe burst capacity. Absent => 2000.
    pub global_burst: Option<u32>,
    /// Hard cap on the per-IP keyed-limiter map. Absent => 4096. `0`
    /// makes the map unbounded — operator opt-in (#645).
    pub max_tracked_per_ip: Option<usize>,
    /// Hard cap on the per-peer (`NodeId`) keyed-limiter map. Absent =>
    /// 4096. `0` makes the map unbounded (#645).
    pub max_tracked_per_peer: Option<usize>,
}

/// Observability section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// Log verbosity level.
    pub log_level: Option<LogLevel>,
    /// Log output format.
    pub log_format: Option<LogFormat>,
    /// Prometheus metrics HTTP port.
    pub metrics_port: Option<u16>,
    /// IP address to bind the metrics HTTP server on. Defaults to
    /// `127.0.0.1`. Set to `0.0.0.0` for containerised deployments.
    pub metrics_bind: Option<std::net::IpAddr>,
    /// Loopback admin HTTP port (ADR 025). Explicit `0` disables the
    /// admin server; if the key is absent, resolution defaults to
    /// `9191`. Any positive value binds on `127.0.0.1:<port>`.
    pub admin_port: Option<u16>,
    /// OTLP collector endpoint URL.
    pub otlp_endpoint: Option<String>,
}

/// Download-receipt audit-log retention section of the config file (#802).
///
/// Bounds the otherwise-unbounded `download_receipts.jsonl` (one line per
/// accepted payment proof, ~1 per MiB delivered) with size-based rotation:
/// once the live file reaches `max_file_bytes` it is rotated to a numbered
/// backup (`download_receipts.jsonl.1`, `.2`, …) and a fresh file is opened;
/// the oldest backup beyond `retained_files` is deleted. Both fields are
/// optional; the defaults bound disk to roughly `(retained_files + 1) *
/// max_file_bytes` while keeping a useful audit window.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptsConfig {
    /// Rotate the live receipt log once it reaches this many bytes. Absent =>
    /// default 128 MiB. Validated against a floor (1 MiB) and ceiling (1 GiB)
    /// so a typo cannot rotate every line or defeat rotation entirely.
    pub max_file_bytes: Option<u64>,
    /// Number of rotated backup files to retain (`.1`..=`.N`). Absent =>
    /// default 4. `0` keeps no backups (the live file is truncated in place on
    /// rotation). Capped at 100.
    pub retained_files: Option<u32>,
}

/// Local content-denylist section of the config file (ADR 011 §Local Denylist).
///
/// The operator's own removal lever, independent of governance: entries take
/// effect on the next reload and bind only this node. ADR 011 §One-hour
/// removal orders makes this the only mechanism sized to a sub-day statutory
/// deadline (the EU TCO one-hour clock), because it is the only one entirely
/// within the order recipient's control.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentConfig {
    /// Blob hashes this node refuses to serve. Bare 64-character lowercase hex,
    /// the same spelling as `cache.pinned_hashes`. An invalid entry fails
    /// resolution rather than being skipped, so a typo in a takedown cannot
    /// silently leave content served. Duplicates are de-duplicated, not
    /// rejected — a repeated deny is still a deny.
    pub denied_hashes: Option<Vec<String>>,
    /// Operator addresses whose payment pools this node refuses to serve.
    /// `0x`-prefixed hex, checksum-agnostic (any case accepted); the zero
    /// address is rejected.
    pub denied_origins: Option<Vec<String>>,
}
