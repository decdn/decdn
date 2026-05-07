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
    /// Seconds between RPC connectivity watchdog probes. `0` disables the
    /// watchdog entirely.
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
    /// Resolved origin backend, if any (#437). `None` => no
    /// pull-through; cache misses return `NoOrigin`. Per-variant
    /// validation (URL parse, S3 bucket/region/prefix shape) has
    /// already run at resolution time — the wiring layer can construct
    /// the concrete `Origin` impl without re-validating.
    pub origin: Option<ResolvedOrigin>,
    /// Operator-pinned blob hashes (#276). Hashes here are excluded from
    /// LRU eviction candidates by [`decdn_cache::CacheEngine`]. Resolved
    /// from the hex-encoded TOML form at load time, so any wrong-length
    /// or non-hex entry fails config loading rather than turning into a
    /// silent "this hash will be ignored" surprise.
    ///
    /// Held as [`decdn_cache::PinnedHashes`] (a typed wrapper around
    /// `Arc<HashSet<Hash>>`) so the engine, runtime, and resolver
    /// share one nominal type — a future "blocklist" or similar
    /// `HashSet<Hash>`-shaped feature can't be silently swapped into
    /// the pinning slot.
    pub pinned_hashes: decdn_cache::PinnedHashes,
    /// Origin pull-through retry policy (#285). Set once at startup;
    /// changes require a process restart.
    pub origin_retry: decdn_cache::RetryPolicy,
}

/// Resolved + validated origin backend selection (#437). Mirrors
/// [`crate::config::types::OriginConfig`] but carries pre-parsed types
/// for the variants that need them (HTTP base URL, S3 endpoint URL).
///
/// Construction is restricted to the resolution layer
/// (`crate::config::resolve_origin`) — runtime callers cannot
/// fabricate a `ResolvedOrigin::S3` from an unvalidated
/// `S3OriginConfig` because the `S3` variant holds the
/// [`ResolvedS3Config`] newtype, whose fields are non-public-default
/// and whose only constructor goes through the validator.
#[derive(Debug, Clone)]
pub enum ResolvedOrigin {
    /// HTTP(S) origin. The base URL has already been parsed by
    /// [`decdn_cache::parse_origin_url`] at resolution time, so
    /// invariants (http/https scheme, trailing-slash path, no
    /// query/fragment) are type-enforced — they cannot be reconstructed
    /// outside the parser.
    Http {
        /// Validated base URL.
        url: decdn_cache::OriginUrl,
        /// How to handle `Content-Encoding` on the HTTP origin response
        /// (#312). Defaults to [`decdn_cache::DecompressMode::Auto`].
        decompress: decdn_cache::DecompressMode,
    },
    /// Local filesystem origin root. Directory-existence is validated
    /// when the runtime constructs the
    /// [`decdn_cache::FilesystemOrigin`] — config resolution carries
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

/// Validated runtime form of an S3 origin (#437). Constructing one
/// goes through `crate::config::resolve_s3_origin` — there is no
/// `Default`, no public field-by-field constructor, and the
/// underlying TOML form ([`crate::config::types::S3OriginConfig`])
/// cannot be passed directly to the runtime. This mirrors the
/// `OriginUrl` precedent (`decdn_cache::parse_origin_url` is the only
/// path to `OriginUrl`).
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
    pub endpoint_url: Option<decdn_cache::OriginUrl>,
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
    /// signature-valid announce (`PoC` substitute for ADR 001 rule 2).
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
}
