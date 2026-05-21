//! Fully resolved configuration with concrete types.
//!
//! Most fields are non-optional. `region` and `relay_url` remain
//! `Option` because they have no universal default.

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
    /// iroh relay URL.
    pub relay_url: Option<String>,
    /// QUIC 0-RTT master switch for `cdn/probe/v1` (ADR 015). Default `true`.
    pub enable_0rtt: bool,
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
    /// `StablePaymentChannel` contract address.
    pub payment_channel_address: String,
    /// `StakingRegistry` contract address.
    pub staking_registry_address: String,
    /// `SlashJudge` contract address — the EIP-712 `verifyingContract` for
    /// `slash_sig` signatures (ADR 014). Required (no default).
    pub slash_judge_address: String,
    /// EIP-712 `chainId` for the `slash_sig` domain separator. Defaults to
    /// [`super::DEFAULT_CHAIN_ID`] (Arbitrum Sepolia) when unset.
    pub chain_id: u64,
    /// Seconds between RPC connectivity watchdog probes. `0` disables the
    /// watchdog entirely; otherwise the resolver enforces a minimum (see
    /// `MIN_RPC_WATCHDOG_INTERVAL_SEC`).
    pub rpc_watchdog_interval_sec: u64,
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
    /// Validated allowlist of accepted announcer node IDs. Empty = accept any
    /// signature-valid announce (local substitute for ADR 001 rule 2 until
    /// the on-chain staking registry contract lands).
    pub allowlist: Vec<[u8; 32]>,
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

/// Fully resolved node configuration.
///
/// Every field has a value determined by the three-layer merge:
/// CLI flag > config file > built-in default.
///
/// Required fields that have no default (`rpc_url`,
/// `payment_channel_address`, `staking_registry_address`) cause
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
}
