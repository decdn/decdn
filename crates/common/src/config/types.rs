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
    /// Gossip settings.
    pub gossip: Option<GossipConfig>,
    /// Connection rate-limiting settings.
    pub security: Option<SecurityConfig>,
}

/// Identity section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct IdentityConfig {
    /// Node data directory.
    pub data_dir: Option<PathBuf>,
    /// ISO 3166-1 alpha-2 region code.
    pub region: Option<String>,
}

/// Network section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// QUIC bind port.
    pub bind_port: Option<u16>,
    /// iroh relay URL.
    pub relay_url: Option<String>,
    /// Master switch for QUIC 0-RTT on `cdn/probe/v1` (ADR 015). Absent =>
    /// default (`true`). When `false`, the probe handler keeps the default
    /// full-handshake `on_accepting` and probe clients fall back to plain
    /// 1-RTT `connect` — an operational kill switch, not a per-ALPN knob
    /// (0-RTT is structurally probe-only via the handler override).
    pub enable_0rtt: Option<bool>,
}

/// Blockchain section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BlockchainConfig {
    /// JSON-RPC endpoint URL.
    pub rpc_url: Option<String>,
    /// Ethereum keystore file path.
    pub eth_keystore: Option<PathBuf>,
    /// `StablePaymentChannel` contract address.
    pub payment_channel_address: Option<String>,
    /// `StakingRegistry` contract address.
    pub staking_registry_address: Option<String>,
    /// Seconds between RPC connectivity watchdog probes. `0` disables the
    /// watchdog entirely; absent => default (30s). Non-zero values below
    /// `MIN_RPC_WATCHDOG_INTERVAL_SEC` are rejected at config resolution.
    pub rpc_watchdog_interval_sec: Option<u64>,
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
    /// origin RTT/timeout. With default [`decdn_cache::RetryPolicy`]
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
    /// [`decdn_cache::RetryPolicy::default`] (3 retries, 100ms
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
    /// `decdn_cache::RetryPolicy` carries `#[serde(default)]` so
    /// partial sections (e.g. just `max_retries = 5`) get the rest of
    /// the fields filled from defaults.
    pub origin_retry: Option<decdn_cache::RetryPolicy>,
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
        decompress: Option<decdn_cache::DecompressMode>,
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
}

/// S3 credential source (#437). Tagged on the inner `source` field.
///
/// Static credential fields use [`SecretString`] so an incidental
/// `tracing::debug!(?cfg)` or panic backtrace cannot leak the
/// material — the wrapper redacts itself in `Debug` output, and
/// its `Serialize` impl emits a non-reversible hashed placeholder
/// rather than the cleartext (the hash is what
/// `runtime::reload::FileSectionSnapshot` compares for SIGHUP
/// diff detection; see [`SecretString`] for the full contract).
/// The codebase already redacts HTTP-origin URL credentials (see
/// `decdn_cache::redact_for_log`); this is the same pattern for
/// TOML-borne secrets.
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
/// # profile = "production"       # optional, override default profile
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
pub struct PaymentConfig {
    /// Rate per MB in USDC base units.
    pub rate_per_mb: Option<u64>,
}

/// Gossip section of the config file (ADR 001).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GossipConfig {
    /// Seconds between outgoing `NodeAnnounce` messages. Default 60.
    pub announce_interval_sec: Option<u64>,
    /// Seconds after which a peer-table entry is evicted if unrefreshed.
    /// Default 600.
    pub peer_ttl_sec: Option<u64>,
    /// Whether to subscribe to and publish on the global topic
    /// (`cdn/global/v1`). Default true.
    pub subscribe_global: Option<bool>,
    /// Optional allowlist of accepted announcer node IDs, hex-encoded
    /// (64 hex chars, either case). Absent/empty = accept any signature-valid
    /// announce. `PoC` replacement for ADR 001 rule 2 (staked-node check).
    pub allowlist: Option<Vec<String>>,
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
/// until the next reload reconciles. Acceptable at `PoC` scale.
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

/// Observability section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
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
