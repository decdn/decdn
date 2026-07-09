//! Fully resolved configuration with concrete types.
//!
//! Most fields are non-optional. `region` remains `Option` because it has no
//! universal default; `relay_urls` is a (possibly empty) list — empty means
//! "fall back to the n0 default relays".

#![allow(dead_code)] // Fields will be consumed by the node runtime.

use std::path::PathBuf;

use crate::cli::common::{LogFormat, LogLevel};

/// Resolved identity fields.
#[derive(Debug)]
pub struct ResolvedIdentity {
    /// Node data directory.
    pub data_dir: PathBuf,
    /// Region code (e.g. `"US"`).
    pub region: Option<String>,
}

/// Resolved network fields.
#[derive(Debug)]
pub struct ResolvedNetwork {
    /// QUIC bind port.
    pub bind_port: u16,
    /// iroh relay URLs for NAT traversal. Empty => use the n0 default relays;
    /// non-empty => swap in these self-hosted relays (`RelayMode::Custom`).
    /// Reachability is probed at bring-up and logged but never fatal: an
    /// all-unreachable set warns and proceeds (iroh retries in the background);
    /// entries with no derivable host/port are skipped.
    pub relay_urls: Vec<String>,
    /// Operator-configurable address discovery (#818 scope 1). Empty
    /// ([`ResolvedDiscovery::is_empty`]) => the node keeps the n0-hosted
    /// pkarr/DNS default (`presets::N0`); otherwise the node builds on
    /// `presets::Minimal` and composes only the providers configured here.
    pub discovery: ResolvedDiscovery,
    /// QUIC 0-RTT master switch for `cdn/probe/v1` (ADR 015). Default `true`.
    pub enable_0rtt: bool,
}

/// Resolved discovery providers (#818). Strings are already shape-validated at
/// resolution (`pkarr_url` a parseable URL, `dns_origin` non-empty, each peer
/// `node_id` a valid iroh `NodeId`, each `addr` a `SocketAddr`); the node wiring
/// layer parses them into iroh types — the discovery-provider seam, per
/// `adr/appendix-poc-production-seams.md`.
#[derive(Debug, Default, Clone)]
pub struct ResolvedDiscovery {
    /// pkarr relay URL to publish this node's address record to.
    pub pkarr_url: Option<String>,
    /// DNS origin domain to resolve peers from.
    pub dns_origin: Option<String>,
    /// Static peer address book, sorted by `node_id` for a deterministic build.
    pub peers: Vec<ResolvedDiscoveryPeer>,
}

impl ResolvedDiscovery {
    /// No discovery overrides configured => the node uses `presets::N0`.
    pub const fn is_empty(&self) -> bool {
        self.pkarr_url.is_none() && self.dns_origin.is_none() && self.peers.is_empty()
    }
}

/// One resolved static peer ([`ResolvedDiscovery::peers`]).
#[derive(Debug, Clone)]
pub struct ResolvedDiscoveryPeer {
    /// Peer `NodeId` (the canonical 64-char lowercase-hex form; validated at
    /// resolution).
    pub node_id: String,
    /// Peer home relay URL, if configured.
    pub relay_url: Option<String>,
    /// Peer direct socket addresses (`host:port`).
    pub addrs: Vec<String>,
}

/// Resolved blockchain fields.
#[derive(Debug)]
pub struct ResolvedBlockchain {
    /// JSON-RPC endpoint URL.
    pub rpc_url: String,
    /// Ethereum keystore file path.
    pub eth_keystore: PathBuf,
    /// Optional path to a file holding the keystore password. CLI/env-only;
    /// not surfaced via the TOML schema (passwords don't belong in config
    /// files even by reference). The runtime first consults the
    /// `DECDN_KEYSTORE_PASSWORD` env var, then this file, then prompts on
    /// stdin if connected to a TTY.
    pub keystore_password_file: Option<PathBuf>,
    /// `PaymentChannel` contract address.
    pub payment_channel_address: String,
    /// `CapacityBond` contract address.
    pub capacity_bond_address: String,
    /// `OriginAssignment` contract address. `Some` only when the operator
    /// opts into the chain-backed origin directory (paired with
    /// `publisher_registry_address`); `None` => empty deny-all directory, so
    /// the prefetch authorized-origin gate finds no origins (ADR 022).
    pub origin_assignment_address: Option<String>,
    /// `PublisherRegistry` contract address. Set together with
    /// `origin_assignment_address` (both-or-neither, enforced at resolution).
    pub publisher_registry_address: Option<String>,
    /// Starting block for the chain-backed origin directory's `ContentClaimed`
    /// log replay (the `PublisherRegistry` deployment block). Defaults to `0`.
    pub origin_directory_from_block: u64,
    /// `SlashJudge` contract address — the EIP-712 `verifyingContract` for
    /// `slash_sig` signatures (ADR 014). Required (no default).
    pub slash_judge_address: String,
    /// `ContentBlacklist` contract address. `Some` only when the operator opts
    /// into the blacklist compliance watcher (ADR 011/031); `None` => no
    /// watcher, so blacklisted content is not locally evicted.
    pub content_blacklist_address: Option<String>,
    /// Starting block for the blacklist watcher's `HashBlacklisted` log replay
    /// (the `ContentBlacklist` deployment block). Defaults to `0`. Only used
    /// when `content_blacklist_address` is set.
    pub content_blacklist_from_block: u64,
    /// EIP-712 `chainId` for the `slash_sig` domain separator. Defaults to
    /// [`super::DEFAULT_CHAIN_ID`] (Arbitrum Sepolia) when unset.
    pub chain_id: u64,
    /// Seconds between RPC connectivity watchdog probes. `0` disables the
    /// watchdog entirely; otherwise the resolver enforces a minimum (see
    /// `MIN_RPC_WATCHDOG_INTERVAL_SEC`).
    pub rpc_watchdog_interval_sec: u64,
    /// Milliseconds between `eth_getFilterChanges` polls for the chain event
    /// watchers (#1011). Applied to every provider via `with_poll_interval` so
    /// it overrides alloy's localhost-detected 250 ms default. Defaults to
    /// 7000 ms (`DEFAULT_EVENT_POLL_INTERVAL_MS`); the resolver enforces a
    /// minimum (see `MIN_EVENT_POLL_INTERVAL_MS`).
    pub event_poll_interval_ms: u64,
    /// Accrued un-redeemed USDC (base units, `µUSDC`) at which the seller
    /// settlement path submits an on-chain `withdraw` (#327). Defaults to
    /// 1 USDC (`1_000_000` `µUSDC`) when unset.
    pub redeem_threshold_micro_usdc: u64,
    /// Deposit (base units, `µUSDC`) used when the buyer path opens a new
    /// `PaymentChannel` against an upstream provider on a cache miss (#744).
    /// Defaults to 10 USDC (`10_000_000` `µUSDC`); clamped up to the on-chain
    /// `minDeposit` floor at open time.
    pub buyer_deposit_micro_usdc: u64,
    /// Whether the buyer path issues a one-time max USDC approval for the
    /// `PaymentChannel` contract at startup (#744, ADR 003 § Deposit
    /// Economics). Defaults to `true`; set `false` to manage the allowance
    /// out-of-band (e.g. a tighter per-channel approval policy).
    pub buyer_max_approve: bool,
    /// Outstanding-USDC threshold (base units, `µUSDC`) at which the seller
    /// settlement path proactively `closeChannel`s a channel to secure a large
    /// un-redeemed balance on-chain before the client can go dark (#742). `None`
    /// disables auto-settlement (the default — behavior unchanged). When `Some`,
    /// the resolver guarantees it is `> 0`.
    pub settlement_auto_threshold_micro_usdc: Option<u64>,
    /// Nonce-span companion threshold (#742): close once a channel's un-redeemed
    /// nonce span (`last_nonce − claimedNonce`) reaches this value, independent of
    /// USDC value. The span is an UPPER BOUND on the un-redeemed voucher count
    /// (nonces may skip values per ADR 003 §Voucher Nonce Convention), not an
    /// exact count. `None` disables the span trigger; when `Some`, the resolver
    /// guarantees `> 0`.
    pub settlement_auto_by_voucher_nonce_span: Option<u64>,
}

/// Resolved cache fields.
#[derive(Debug)]
pub struct ResolvedCache {
    /// Blob cache directory.
    pub cache_dir: PathBuf,
    /// Maximum cache size in megabytes.
    pub cache_size_mb: u64,
    /// Maximum single blob size in megabytes.
    pub max_blob_size_mb: u64,
    /// Resolved ordered list of origin backends (#437, #284). Empty
    /// vec => no pull-through; cache misses return `NoOrigin`. A
    /// single-element vec preserves the pre-#284 single-origin
    /// semantics and is also the form produced from a `[cache.origin]`
    /// (singular) TOML table — both wire forms (singular and plural
    /// `[[cache.origins]]`) collapse here so downstream wiring sees
    /// one canonical representation. Per-variant validation (URL
    /// parse, S3 bucket/region/prefix shape) has already run at
    /// resolution time — the wiring layer can construct the concrete
    /// `Origin` impl without re-validating. Order is significant: the
    /// engine tries entries in this order on a cache miss and falls
    /// back to the next on `NotFound`, permanent error, or transient
    /// retry exhaustion.
    pub origins: Vec<ResolvedOrigin>,
    /// Operator-pinned blob hashes (#276). Hashes here are excluded from
    /// LRU eviction candidates by the cache engine. Resolved
    /// from the hex-encoded TOML form at load time, so any wrong-length
    /// or non-hex entry fails config loading rather than turning into a
    /// silent "this hash will be ignored" surprise.
    ///
    /// Held as [`decdn_config_types::PinnedHashes`] (a typed wrapper around
    /// `Arc<HashSet<Hash>>`) so the engine, runtime, and resolver
    /// share one nominal type — a future "blocklist" or similar
    /// `HashSet<Hash>`-shaped feature can't be silently swapped into
    /// the pinning slot.
    pub pinned_hashes: decdn_config_types::PinnedHashes,
    /// Origin pull-through retry policy (#285). Set once at startup;
    /// changes require a process restart.
    pub origin_retry: decdn_config_types::RetryPolicy,
    /// Per-origin circuit-breaker policy (#963). Fronts each origin's
    /// pull-through retry loop so a sustained outage fast-fails the
    /// origin's misses (no retry/backoff incurred) and the cache sheds
    /// load, then probes for recovery. Set once at startup; changes
    /// require a process restart.
    pub circuit_breaker: decdn_config_types::CircuitBreakerPolicy,
    /// `User-Agent` header sent on every HTTP origin pull-through (#435).
    /// Defaults to [`decdn_config_types::DEFAULT_USER_AGENT`] (which embeds
    /// the `decdn-config-types` crate's `CARGO_PKG_VERSION`; the
    /// `decdn-node/` prefix — not the version — is the stable contract,
    /// #578); operators can override
    /// via `cache.user_agent` to attribute CDN traffic in origin access
    /// logs or to drive origin-side rate limits and routing policy.
    pub user_agent: String,
    /// Interval between iroh-blobs GC sweeps in seconds (#518). `0`
    /// disables the periodic sweep; any positive value is forwarded to
    /// iroh-blobs' built-in GC loop. Default
    /// [`crate::config::DEFAULT_GC_INTERVAL_SEC`] when the TOML section
    /// omits the field.
    pub gc_interval_sec: u64,
    /// Maximum number of concurrently held (eviction-exempt) blobs for the
    /// probe-triggered hold (ADR 005 §Hold budget, #318). Default
    /// [`crate::config::DEFAULT_MAX_PROBE_HOLDS`]; `0` disables
    /// `has_blob: true`.
    pub max_probe_holds: usize,
    /// Probe-hold slots reserved for the stake lane (registered node-to-node
    /// requesters) under hold-budget pressure (#757, ADR 003 §Admission and
    /// Priority). Default `0` disables the reservation (single-lane node).
    /// The runtime only builds a `StakeLanePolicy` when this is `> 0`, so the
    /// probe handler's hot path is unchanged for the default.
    pub stake_lane_reserved_holds: usize,
    /// Enable node-to-node paid cache-miss pull-through (#831). Default
    /// `false`. When `true` and the buyer-channel service bootstrapped, the
    /// runtime provisions the `NodeOrigin` and the client handler triggers a
    /// pull on a miss (behind a valid client channel). `cache.*` is
    /// restart-required, so this is read once at bring-up.
    pub node_to_node_pull_through_enabled: bool,
    /// Providers probed before ranking on a node-to-node pull (#831). Default
    /// [`crate::config::DEFAULT_NODE_PULL_PROBE_FANOUT`].
    pub node_pull_probe_fanout: usize,
    /// Per-pull wall-clock timeout in seconds for a node-to-node miss fill
    /// (#831). Default [`crate::config::DEFAULT_NODE_PULL_TIMEOUT_SEC`].
    pub node_pull_timeout_sec: u64,
    /// Window-paced pull-through pipeline window in bytes (#856, ADR 037
    /// `pull_ahead_bytes`). Default [`crate::config::DEFAULT_PULL_AHEAD_BYTES`].
    /// Bounds per-request speculative loss to this window.
    pub pull_ahead_bytes: decdn_config_types::Bytes,
    /// Node-wide unrecouped-leech budget in bytes (#856, ADR 037
    /// `max_unrecouped_leech_bytes`). Default
    /// [`crate::config::DEFAULT_MAX_UNRECOUPED_LEECH_BYTES`]; `0` disables.
    pub max_unrecouped_leech_bytes: decdn_config_types::Bytes,
    /// Per-peer share ratio as a percentage (#856, ADR 037 `share_ratio`;
    /// `100` == 1.0×). Default [`crate::config::DEFAULT_PULL_SHARE_RATIO_PERCENT`].
    pub pull_share_ratio_percent: decdn_config_types::Percent,
    /// Gate the reactive cache-miss pull-through path on an authorized origin
    /// (#821, ADR 037 §Seed-leech caps). Default `false` — the cache role stays
    /// permissionless. When `true`, the handler refuses to initiate an upstream
    /// pull (and its cache-warming write) for a hash whose namespace has no
    /// authorized origin, returning `NotFound`; ranges already held are still
    /// served. `cache.*` is restart-required, so this is read once at bring-up.
    /// The runtime attaches the gate only when `node_to_node_pull_through_enabled`
    /// is also set, so it is a no-op without that path (a miss already returns
    /// `NotFound`).
    pub pull_through_require_authorized_origin: bool,
}

/// Resolved + validated origin backend selection (#437). Mirrors
/// [`crate::config::types::OriginConfig`] but carries pre-parsed types
/// for the variants that need them (HTTP base URL, S3 endpoint URL).
///
/// The resolution layer (`crate::config::resolve_origin`) is the
/// **intended** construction path — it's the only producer the
/// runtime relies on, and the only path that runs the validators.
/// The fields on each variant are `pub` for consistency with the
/// other `Resolved*` types in this crate (see `ResolvedCache`,
/// `ResolvedBlockchain`, etc.); Rust visibility doesn't *enforce*
/// the validator-only contract, but the runtime never bypasses it.
#[derive(Debug, Clone)]
pub enum ResolvedOrigin {
    /// HTTP(S) origin. The base URL has already been parsed by
    /// [`decdn_config_types::parse_origin_url`] at resolution time, so
    /// invariants (http/https scheme, trailing-slash path, no
    /// query/fragment) are type-enforced — they cannot be reconstructed
    /// outside the parser.
    Http {
        /// Validated base URL.
        url: decdn_config_types::OriginUrl,
        /// How to handle `Content-Encoding` on the HTTP origin response
        /// (#312). Defaults to [`decdn_config_types::DecompressMode::Auto`].
        decompress: decdn_config_types::DecompressMode,
    },
    /// Local filesystem origin root. Directory-existence is validated
    /// when the runtime constructs the
    /// cache engine's filesystem origin — config resolution carries
    /// the raw path so resolution stays filesystem-free and testable
    /// without real I/O.
    Fs {
        /// Filesystem root.
        path: PathBuf,
    },
    /// S3-compatible origin. Field-shape validation (DNS-safe bucket
    /// name, region non-empty, endpoint URL scheme, prefix shape) has
    /// already run at resolution time and is encoded in the
    /// [`ResolvedS3Config`] type.
    S3(ResolvedS3Config),
}

/// Validated runtime form of an S3 origin (#437). The intended
/// construction path is `crate::config::resolve_s3_origin`, which
/// runs the validators and lifts the wire-form
/// [`crate::config::types::S3OriginConfig`] into this resolved form
/// with pre-parsed types (notably `endpoint_url: Option<OriginUrl>`).
/// `Default` is intentionally not derived. The fields are `pub` for
/// consistency with the other `Resolved*` types in this crate, so
/// in-crate construction with field-init is technically possible —
/// the runtime simply doesn't do that. Pattern of intent rather
/// than visibility-enforced invariant.
///
/// Resolved-vs-wire-form differences:
/// - `endpoint_url` is `Option<OriginUrl>` (parsed) instead of
///   `Option<String>`, so PR2's S3 backend cannot accidentally pass
///   an un-normalized URL to the SDK and produce `SigV4` mismatches
///   between, say, `http://minio:9000` and `http://minio:9000/`.
/// - `path_style` is `bool` (collapsed from `Option<bool>`), with
///   `None` mapped to the SDK default (virtual-hosted-style = false).
/// - `prefix` is a `String` with the trailing-slash
///   auto-append already applied — the runtime never sees the raw
///   pre-normalised form.
#[derive(Debug, Clone)]
pub struct ResolvedS3Config {
    /// Bucket name. Validated DNS-safe at construction time.
    pub bucket: String,
    /// AWS region, validated non-empty at construction time.
    pub region: String,
    /// Custom endpoint URL for non-AWS S3-compatible providers (R2,
    /// B2, `MinIO`). Parsed and trailing-slash-normalized.
    pub endpoint_url: Option<decdn_config_types::OriginUrl>,
    /// Whether to use path-style addressing. `false` (the SDK
    /// default) selects virtual-hosted-style addressing.
    pub path_style: bool,
    /// Optional key prefix prepended to every fetched object.
    /// Trailing-slash-normalized so the runtime can build keys via
    /// `format!("{prefix}{shard}/{hex}")` without re-checking the
    /// trailing slash.
    pub prefix: String,
    /// Resolved credential source.
    pub credentials: Option<ResolvedS3Credentials>,
    /// How to handle `Content-Encoding` on the S3 origin response
    /// (#804). Defaults to [`decdn_config_types::DecompressMode::Auto`].
    pub decompress: decdn_config_types::DecompressMode,
}

/// Validated runtime form of S3 credentials (#437). See
/// [`ResolvedS3Config`] for the constructor invariant.
#[derive(Debug, Clone)]
pub enum ResolvedS3Credentials {
    /// Static IAM credentials.
    Static {
        /// AWS access key ID. Held in [`crate::config::secret::SecretString`]
        /// so an incidental `Debug` print or panic backtrace cannot
        /// leak it.
        access_key_id: crate::config::secret::SecretString,
        /// AWS secret access key. Same redaction discipline.
        secret_access_key: crate::config::secret::SecretString,
        /// Optional STS session token, redacted.
        session_token: Option<crate::config::secret::SecretString>,
    },
    /// Use the AWS default credential chain. `profile` overrides the
    /// default profile name for `~/.aws/credentials`.
    DefaultChain {
        /// Optional profile name override.
        profile: Option<String>,
    },
}

/// Resolved payment fields.
#[derive(Debug)]
pub struct ResolvedPayment {
    /// Rate per MB in USDC base units (6 decimals).
    pub rate_per_mb: u64,
    /// Lower clamp bound applied to `rate_per_mb` before signing a
    /// `ProbeResponse` (ADR 005 §Rate bounds validation). Locally
    /// enforced stand-in for on-chain `getRateBounds().deliveryFloor`;
    /// default `0`.
    pub delivery_floor: u64,
    /// Upper clamp bound applied to `rate_per_mb` before signing a
    /// `ProbeResponse`. Locally enforced stand-in for
    /// `getRateBounds().deliveryCeiling`; default
    /// [`decdn_protocol::MAX_RATE_PER_MB`].
    pub delivery_ceiling: u64,
    /// Voucher cadence advertised in `StreamResponse` for `cdn/client/v1`
    /// (ADR 003 §Voucher Interval Negotiation); default
    /// [`decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB`], range
    /// `1..=`[`decdn_protocol::MAX_VOUCHER_INTERVAL_MB`].
    pub voucher_interval_mb: u64,
}

/// Resolved gossip fields (ADR 001).
#[derive(Debug)]
pub struct ResolvedGossip {
    /// Seconds between outgoing `NodeAnnounce` messages.
    pub announce_interval_sec: u64,
    /// Seconds after which a peer-table entry is evicted if unrefreshed.
    pub peer_ttl_sec: u64,
    /// Whether to subscribe to and publish on `cdn/global/v1`.
    pub subscribe_global: bool,
    /// Whether to subscribe to (and publish on) the global
    /// `cdn/reputation/v1` topic (ADR 008). Independent of `region`.
    pub subscribe_reputation: bool,
    /// Interval between reputation-report publish ticks (seconds). Matches the
    /// ADR 008 1-hour per-(reporter, node) rate limit by default.
    pub reputation_publish_interval_sec: u64,
    /// Validated allowlist of accepted announcer node IDs. Empty = accept any
    /// signature-valid announce (local substitute for ADR 001 rule 2 until
    /// the on-chain staking registry contract lands).
    pub allowlist: Vec<[u8; 32]>,
    /// Hard cap on `PeerTable` entry count (#577 H3). Always positive
    /// (the resolver rejects `0`); the runtime casts to `usize` when
    /// constructing the table.
    pub max_peer_table_entries: u64,
}

/// Resolved observability fields.
#[derive(Debug)]
pub struct ResolvedObservability {
    /// Log verbosity level.
    pub log_level: LogLevel,
    /// Log output format.
    pub log_format: LogFormat,
    /// Prometheus metrics HTTP port.
    pub metrics_port: u16,
    /// IP address to bind the metrics HTTP server on.
    pub metrics_bind: std::net::IpAddr,
    /// Loopback admin HTTP port (ADR 025). `None` disables the admin
    /// server entirely; `Some(port)` binds on `127.0.0.1:<port>`.
    pub admin_port: Option<u16>,
    /// OTLP collector endpoint URL (if set, span export is enabled).
    pub otlp_endpoint: Option<String>,
    /// Interval in seconds for the per-region bandwidth accounting log
    /// (issue #750). `0` disables the periodic log.
    pub region_accounting_interval_sec: u64,
}

/// Resolved `cdn/dht/v1` settings (ADR 022).
///
/// Each `*_rate_per_sec == 0.0` and matching `*_burst == 0` disables that
/// layer. `trusted_ips` is the parsed-and-deduplicated IP set; the
/// resolver rejects malformed entries.
///
/// `Default` returns the ADR 022 §DHT Rate Limiting defaults so test sites
/// that build a `ResolvedConfig` by hand can write `ResolvedDht::default()`
/// instead of restating the constants. The production `resolve_dht_into`
/// path threads the defaults through the resolver bag and is what
/// `decdn-common`'s integration tests cover.
#[derive(Debug, Clone)]
pub struct ResolvedDht {
    pub per_peer_rate_per_sec: f64,
    pub per_peer_burst: u32,
    pub per_ip_rate_per_sec: f64,
    pub per_ip_burst: u32,
    pub global_rate_per_sec: f64,
    pub global_burst: u32,
    pub trusted_ips: std::collections::HashSet<std::net::IpAddr>,
    /// Hard cap on the per-IP keyed-limiter map (#645). `0` => unbounded.
    pub max_tracked_per_ip: usize,
    /// Hard cap on the per-peer (`NodeId`) keyed-limiter map (#645). `0`
    /// => unbounded.
    pub max_tracked_per_peer: usize,
}

impl Default for ResolvedDht {
    fn default() -> Self {
        Self {
            per_peer_rate_per_sec: 20.0,
            per_peer_burst: 40,
            per_ip_rate_per_sec: 100.0,
            per_ip_burst: 200,
            global_rate_per_sec: 1000.0,
            global_burst: 2000,
            trusted_ips: std::collections::HashSet::new(),
            max_tracked_per_ip: 4096,
            max_tracked_per_peer: 4096,
        }
    }
}

/// Resolved `cdn/probe/v1` settings (ADR 005 §Probe rate limiting).
///
/// Mirrors [`ResolvedDht`] in shape; `Default` returns the ADR 005 defaults
/// (a tighter per-peer cap than the DHT layer, since probes are
/// unauthenticated and cheaper to flood) so hand-built `ResolvedConfig` test
/// sites can write `ResolvedProbe::default()`. The production
/// `resolve_probe_into` path threads the defaults through the resolver bag.
#[derive(Debug, Clone)]
pub struct ResolvedProbe {
    pub per_peer_rate_per_sec: f64,
    pub per_peer_burst: u32,
    pub per_ip_rate_per_sec: f64,
    pub per_ip_burst: u32,
    pub global_rate_per_sec: f64,
    pub global_burst: u32,
    pub trusted_ips: std::collections::HashSet<std::net::IpAddr>,
    /// Hard cap on the per-IP keyed-limiter map (#645). `0` => unbounded.
    pub max_tracked_per_ip: usize,
    /// Hard cap on the per-peer (`NodeId`) keyed-limiter map (#645). `0`
    /// => unbounded.
    pub max_tracked_per_peer: usize,
}

impl Default for ResolvedProbe {
    fn default() -> Self {
        Self {
            per_peer_rate_per_sec: 5.0,
            per_peer_burst: 5,
            per_ip_rate_per_sec: 50.0,
            per_ip_burst: 200,
            global_rate_per_sec: 1000.0,
            global_burst: 2000,
            trusted_ips: std::collections::HashSet::new(),
            max_tracked_per_ip: 4096,
            max_tracked_per_peer: 4096,
        }
    }
}

/// Resolved security / rate-limiting fields.
#[derive(Debug, Clone)]
pub struct ResolvedSecurity {
    /// Global cap on concurrent in-flight QUIC handler tasks.
    pub max_concurrent_handlers: u32,
    /// Per-source rate-limit refill (cells/second). `0.0` disables the
    /// layer.
    pub per_source_rate_per_sec: f64,
    /// Per-source rate-limit burst capacity. Ignored when
    /// `per_source_rate_per_sec == 0.0`.
    pub per_source_burst: u32,
    /// Hard cap on the number of tracked sources in the keyed limiter.
    pub max_tracked_sources: usize,
}

/// Resolved download-receipt audit-log retention fields (#802).
///
/// `max_file_bytes` is always within `[MIN_RECEIPT_MAX_FILE_BYTES,
/// MAX_RECEIPT_MAX_FILE_BYTES]` and `retained_files` within
/// `[0, MAX_RECEIPT_RETAINED_FILES]` (the resolver rejects out-of-range
/// values), so the runtime can bound disk to roughly `(retained_files + 1) *
/// max_file_bytes`.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedReceipts {
    /// Rotate the live receipt log once it reaches this many bytes.
    pub max_file_bytes: u64,
    /// Number of rotated backup files retained (`.1`..=`.N`). `0` keeps none.
    pub retained_files: u32,
}

impl Default for ResolvedReceipts {
    /// Reuses the `DEFAULT_RECEIPT_*` resolver constants so hand-built
    /// `ResolvedConfig`s in tests cannot drift from the production defaults.
    fn default() -> Self {
        Self {
            max_file_bytes: super::DEFAULT_RECEIPT_MAX_FILE_BYTES,
            retained_files: super::DEFAULT_RECEIPT_RETAINED_FILES,
        }
    }
}

/// Resolved speculative-prefetch policy (ADR 022 §Prefetch Decision).
///
/// All fields validated by the resolver: `find_value_threshold > 0`,
/// `threshold_window_secs > 0`, `demand_quality_window_secs > 0`, and
/// `demand_quality_min_ratio` finite in `[0.0, 1.0]`. `Default` reuses the
/// `DEFAULT_PREFETCH_*` resolver constants so hand-built `ResolvedConfig`s in
/// tests cannot drift from production defaults.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedPrefetch {
    pub enabled: bool,
    pub require_authorized_origin: bool,
    pub budget_usdc_per_hour: u64,
    pub find_value_threshold: u32,
    pub threshold_window_secs: u64,
    pub demand_quality_min_ratio: f64,
    pub demand_quality_window_secs: u64,
    /// Max prefetch acquisitions running concurrently (#820). `> 0`.
    pub max_concurrent_acquisitions: u32,
    /// Per-acquisition pull-through deadline in seconds (#820). `> 0`.
    pub acquisition_timeout_secs: u64,
}

impl Default for ResolvedPrefetch {
    fn default() -> Self {
        Self {
            enabled: super::DEFAULT_PREFETCH_ENABLED,
            require_authorized_origin: super::DEFAULT_PREFETCH_REQUIRE_AUTHORIZED_ORIGIN,
            budget_usdc_per_hour: super::DEFAULT_PREFETCH_BUDGET_USDC_PER_HOUR,
            find_value_threshold: super::DEFAULT_PREFETCH_FIND_VALUE_THRESHOLD,
            threshold_window_secs: super::DEFAULT_PREFETCH_THRESHOLD_WINDOW_SECS,
            demand_quality_min_ratio: super::DEFAULT_PREFETCH_DEMAND_QUALITY_MIN_RATIO,
            demand_quality_window_secs: super::DEFAULT_PREFETCH_DEMAND_QUALITY_WINDOW_SECS,
            max_concurrent_acquisitions: super::DEFAULT_PREFETCH_MAX_CONCURRENT_ACQUISITIONS,
            acquisition_timeout_secs: super::DEFAULT_PREFETCH_ACQUISITION_TIMEOUT_SECS,
        }
    }
}

/// Fully resolved node configuration.
///
/// Every field has a value determined by the three-layer merge:
/// CLI flag > config file > built-in default.
///
/// Required fields that have no default (`rpc_url`,
/// `payment_channel_address`, `capacity_bond_address`) cause
/// [`super::resolve_config`] to return an error if not provided.
#[derive(Debug)]
pub struct ResolvedConfig {
    pub identity: ResolvedIdentity,
    pub network: ResolvedNetwork,
    pub blockchain: ResolvedBlockchain,
    pub cache: ResolvedCache,
    pub payment: ResolvedPayment,
    pub observability: ResolvedObservability,
    pub gossip: ResolvedGossip,
    pub security: ResolvedSecurity,
    pub dht: ResolvedDht,
    pub probe: ResolvedProbe,
    pub receipts: ResolvedReceipts,
    pub prefetch: ResolvedPrefetch,
}
