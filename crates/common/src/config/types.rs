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
    /// Gossip settings.
    pub gossip: Option<GossipConfig>,
    /// Connection rate-limiting settings.
    pub security: Option<SecurityConfig>,
    /// `cdn/dht/v1` Kademlia DHT settings (ADR 022). Absent => defaults
    /// from the ADR 022 §DHT Rate Limiting table.
    pub dht: Option<DhtConfig>,
    /// Download-receipt audit-log retention settings (#802). Absent =>
    /// defaults (128 MiB per file, 4 retained backups).
    pub receipts: Option<ReceiptsConfig>,
    /// Speculative-prefetch operator policy (ADR 022 §Prefetch Decision).
    /// Absent => prefetch disabled with the ADR's recommended defaults.
    pub prefetch: Option<PrefetchConfig>,
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
    /// Deprecated single-relay alias for [`Self::relay_urls`]. When set and
    /// `relay_urls` is absent, it is folded into the list as a single entry.
    pub relay_url: Option<String>,
    /// Operator-configurable address discovery (#818 scope 1). Absent => the
    /// node uses the n0-hosted pkarr/DNS discovery (`presets::N0`, unchanged).
    /// Present => the node drops the n0 discovery leg and composes only the
    /// providers configured here (the wiring layer builds on `presets::Minimal`).
    /// Relay selection ([`Self::relay_urls`]) is an independent, orthogonal knob.
    pub discovery: Option<DiscoveryConfig>,
    /// Master switch for QUIC 0-RTT on `cdn/probe/v1` (ADR 015). Absent =>
    /// default (`true`). When `false`, probe clients fall back to plain
    /// 1-RTT `connect` (the effective downgrade — they emit no early
    /// data) and the probe handler keeps the default `on_accepting`. An
    /// operational kill switch; replay safety for non-probe ALPNs is
    /// client-side (only the probe client emits 0-RTT), not gated here.
    pub enable_0rtt: Option<bool>,
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
    /// `PaymentChannel` contract address.
    pub payment_channel_address: Option<String>,
    /// `CapacityBond` contract address.
    pub capacity_bond_address: Option<String>,
    /// `OriginAssignment` contract address. Optional: when set (together with
    /// `publisher_registry_address`), the node runs the chain-backed origin
    /// directory that gates DHT prefetch (ADR 022). Both must be set or unset
    /// together; unset => the origin directory is empty (deny-all) and the
    /// prefetch authorized-origin gate finds no origins.
    pub origin_assignment_address: Option<String>,
    /// `PublisherRegistry` contract address. Pairs with
    /// `origin_assignment_address` (see its docs).
    pub publisher_registry_address: Option<String>,
    /// Block height at which the chain-backed origin directory begins its
    /// `ContentClaimed` log replay. SHOULD be the `PublisherRegistry`
    /// deployment block; absent => `0`, which is correct but scans the entire
    /// chain history (slow / RPC-heavy on an established L2). Only consulted
    /// when the origin-directory addresses are set.
    pub origin_directory_from_block: Option<u64>,
    /// `SlashJudge` contract address — the EIP-712 `verifyingContract` for
    /// `ProbeResponse` / `StreamResponse` `slash_sig` signatures (ADR 014
    /// §1–2). Required: a wrong/zero address silently produces signatures no
    /// verifier accepts, so resolution fails fast when it is missing rather
    /// than defaulting.
    pub slash_judge_address: Option<String>,
    /// EIP-712 `chainId` bound into every `slash_sig` domain separator.
    /// Absent => [`super::DEFAULT_CHAIN_ID`] (Arbitrum Sepolia, the initial
    /// network target — matches the chain id bound on the runtime signer).
    pub chain_id: Option<u64>,
    /// Seconds between RPC connectivity watchdog probes. `0` disables the
    /// watchdog entirely; absent => default (30s). Non-zero values below
    /// `MIN_RPC_WATCHDOG_INTERVAL_SEC` are rejected at config resolution.
    pub rpc_watchdog_interval_sec: Option<u64>,
    /// Accrued un-redeemed USDC (base units, `µUSDC`) at which the node
    /// submits an on-chain `withdraw` for a channel (#327, ADR 003 § Operator
    /// early withdrawal). Larger values amortize gas across more delivery;
    /// smaller values bound unsettled exposure. Absent => default
    /// (1 USDC = `1_000_000` `µUSDC`).
    pub redeem_threshold_micro_usdc: Option<u64>,
    /// Deposit (base units, `µUSDC`) the buyer path escrows when opening a
    /// `PaymentChannel` against an upstream provider on a cache miss (#744).
    /// Absent => default (10 USDC = `10_000_000` `µUSDC`, ADR 003 § Deposit
    /// Economics recommended minimum). Clamped up to the on-chain `minDeposit`
    /// floor at open time.
    pub buyer_deposit_micro_usdc: Option<u64>,
    /// Whether to issue a one-time max USDC approval for the `PaymentChannel`
    /// contract at startup so the buyer path can `openChannel` (#744). Absent
    /// => `true`. Set `false` to manage the allowance out-of-band.
    pub buyer_max_approve: Option<bool>,
    /// Outstanding (un-redeemed) USDC (base units, `µUSDC`) at which the node
    /// proactively `closeChannel`s a channel to start its dispute window, so a
    /// large unsubmitted voucher balance is secured on-chain before the client
    /// can go dark (#742, ADR 003 § payment-channel lifecycle). A close starts
    /// the dispute window; the existing settle sweep finalizes the remainder
    /// after it elapses. Distinct from (and should be set above)
    /// [`Self::redeem_threshold_micro_usdc`] — `withdraw` reclaims earnings on a
    /// still-open channel, whereas this caps total at-risk exposure by closing.
    /// Absent => disabled (`None`): behavior is unchanged unless an operator
    /// opts in. A configured value of `0` is rejected at resolution.
    pub settlement_auto_threshold_micro_usdc: Option<u64>,
    /// Optional nonce-span companion to
    /// [`Self::settlement_auto_threshold_micro_usdc`] (#742): close a channel
    /// once its un-redeemed nonce span reaches at least this value, independent
    /// of USDC value. The span is the off-chain latest voucher nonce minus the
    /// on-chain `claimedNonce` — an UPPER BOUND on the un-redeemed voucher count,
    /// NOT an exact count: voucher nonces may skip values (ADR 003 §Voucher Nonce
    /// Convention), so a gapped stream reaches a given span with fewer vouchers
    /// than the span implies. Useful when many small vouchers accrue without
    /// crossing the value threshold. Either trigger firing closes the channel
    /// (logical OR). Absent => disabled (`None`); a configured value of `0` is
    /// rejected at resolution.
    pub settlement_auto_by_voucher_nonce_span: Option<u64>,
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
    /// Maximum number of concurrently held (eviction-exempt) blobs for the
    /// probe-triggered hold (ADR 005 §Hold budget, #318). Holds are
    /// per-blob: multiple peers probing the same hash share one slot. When
    /// the budget is exhausted, additional probes for unheld blobs receive
    /// `has_blob: false` rather than risk a phantom-announcement slash.
    /// Absent => [`crate::config::DEFAULT_MAX_PROBE_HOLDS`] (256). `0`
    /// disables `has_blob: true` entirely (every probe answers false).
    /// Operators with small caches SHOULD set this to ≤25% of cache
    /// capacity.
    pub max_probe_holds: Option<u64>,
    /// Number of probe-hold slots reserved for the **stake lane** —
    /// registered operators issuing node-to-node cache-miss probes (#757,
    /// ADR 003 §Admission and Priority). Under hold-budget pressure,
    /// end-client probes are answered `has_blob: false` once usage reaches
    /// `max_probe_holds - stake_lane_reserved_holds`, keeping the last
    /// `stake_lane_reserved_holds` slots available for node-to-node probes
    /// so end-client load cannot starve them. Absent / `0` (the default)
    /// disables the reservation entirely — a single-lane node is unaffected.
    /// Values `>= max_probe_holds` reserve the whole budget for the stake
    /// lane: the end-client ceiling collapses to `0`, so every end-client
    /// probe is shed whenever holds are enabled — regardless of current
    /// usage, not only under pressure.
    pub stake_lane_reserved_holds: Option<u64>,
    /// Enable node-to-node paid cache-miss pull-through (#831, ADR 001/022).
    /// Absent / `false` (the default) → a cache miss serves `NotFound` as
    /// before. When `true` *and* the buyer-channel service bootstrapped, a
    /// miss triggers DHT provider discovery → probe → ranked paid pull from an
    /// upstream node, which populates the cache and is then served. OFF by
    /// default for the initial network: enabling it makes the node front USDC
    /// egress to fill misses (bounded by `blockchain.buyer_deposit_micro_usdc`
    /// and the upstream's per-MB rate), and the serving path only triggers it
    /// behind a valid, channel-bound client so an unpaid request cannot drive
    /// egress.
    pub node_to_node_pull_through_enabled: Option<bool>,
    /// Number of discovered providers to probe before ranking on a
    /// node-to-node pull (#831). Absent =>
    /// [`crate::config::DEFAULT_NODE_PULL_PROBE_FANOUT`] (5). Higher widens
    /// provider choice at the cost of more probe round trips per miss; `0`
    /// probes none, so no pull can succeed (a way to disable the pull while
    /// keeping the feature flag on).
    pub node_pull_probe_fanout: Option<usize>,
    /// Wall-clock timeout in seconds for a single upstream pull on a
    /// node-to-node miss (#831). Absent =>
    /// [`crate::config::DEFAULT_NODE_PULL_TIMEOUT_SEC`] (20). Bounds how long a
    /// miss blocks the serving path on one upstream before falling through to
    /// the next ranked candidate (or `NotFound`). This is the *per-upstream*
    /// budget; the overall pull-through deadline is derived as roughly
    /// `MAX_PROVIDER_ATTEMPTS ×` it plus a fixed discovery allowance, so the
    /// fallback loop reaches every ranked candidate (#859).
    pub node_pull_timeout_sec: Option<u64>,
    /// Window-paced pull-through per-request pipeline window in bytes (#856, ADR
    /// 037 `pull_ahead_bytes`). Absent =>
    /// [`crate::config::DEFAULT_PULL_AHEAD_BYTES`] (1 MiB ≈ one voucher
    /// interval). The serving node pulls at most this many bytes ahead of what
    /// the requesting client has paid for, so the loss on an abandoned request
    /// is bounded to this window rather than the whole blob. Larger keeps the
    /// upstream pull more pipelined (higher throughput) at a larger per-request
    /// speculative exposure. The serve loop floors the effective window at one
    /// voucher interval so it can always make progress, so a value below one
    /// interval (including `0`) collapses to one-interval pacing, not one chunk.
    pub pull_ahead_bytes: Option<decdn_config_types::Bytes>,
    /// Node-wide circuit breaker on aggregate speculative pull-through spend, in
    /// bytes (#856, ADR 037 `max_unrecouped_leech_bytes`). Absent =>
    /// [`crate::config::DEFAULT_MAX_UNRECOUPED_LEECH_BYTES`]. When the rolling
    /// `Σ(bytes pulled for misses) − Σ(bytes served)` reaches this, speculative
    /// pull-through pauses and resumes as the node serves and recoups. Bounds
    /// distributed manufactured-demand abuse in aggregate. `0` disables the
    /// global cap (the per-request window and per-peer ratio still apply).
    pub max_unrecouped_leech_bytes: Option<decdn_config_types::Bytes>,
    /// Per-peer speculative-pull ceiling as a percentage of bytes served to that
    /// peer (#856, ADR 037 `share_ratio`); `100` == 1.0×. Absent =>
    /// [`crate::config::DEFAULT_PULL_SHARE_RATIO_PERCENT`]. The node will not
    /// pull more than this ratio of what it has served a peer, plus an opening
    /// allowance of `pull_ahead_bytes`, bounding concentrated single-peer abuse.
    /// `0` pins a peer to only the opening window.
    pub pull_share_ratio_percent: Option<decdn_config_types::Percent>,
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
/// rather than the cleartext (the hash is what
/// `runtime::reload::FileSectionSnapshot` compares for SIGHUP
/// diff detection; see [`SecretString`] for the full contract).
/// The codebase already redacts HTTP-origin URL credentials (see
/// `decdn_config_types::redact_for_log`); this is the same pattern for
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
    /// Lower bound the node clamps `rate_per_mb` to before signing a
    /// `ProbeResponse` (ADR 005 §Rate bounds validation). Locally
    /// enforced stand-in for the on-chain `getRateBounds().deliveryFloor`.
    /// Absent => `0` (no floor; current behavior unchanged).
    pub delivery_floor: Option<u64>,
    /// Upper bound the node clamps `rate_per_mb` to before signing a
    /// `ProbeResponse` (ADR 005 §Rate bounds validation). Locally
    /// enforced stand-in for the on-chain `getRateBounds().deliveryCeiling`.
    /// Absent => [`decdn_protocol::MAX_RATE_PER_MB`] (no effective ceiling;
    /// current behavior unchanged).
    pub delivery_ceiling: Option<u64>,
    /// Voucher cadence the node advertises in `StreamResponse` for
    /// `cdn/client/v1` delivery (ADR 003 §Voucher Interval Negotiation): the
    /// node pauses delivery once outstanding unvouchered bytes exceed
    /// `voucher_interval_mb * 1_048_576`. Absent =>
    /// [`decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB`] (1 MB). Governable range
    /// `1..=`[`decdn_protocol::MAX_VOUCHER_INTERVAL_MB`].
    pub voucher_interval_mb: Option<u64>,
}

/// Gossip section of the config file (ADR 001).
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GossipConfig {
    /// Seconds between outgoing `NodeAnnounce` messages. Default 60.
    pub announce_interval_sec: Option<u64>,
    /// Seconds after which a peer-table entry is evicted if unrefreshed.
    /// Default 600.
    pub peer_ttl_sec: Option<u64>,
    /// Whether to subscribe to and publish on the global topic
    /// (`cdn/global/v1`). Default true.
    pub subscribe_global: Option<bool>,
    /// Whether to join the global `cdn/reputation/v1` topic (ADR 008) to
    /// exchange signed reputation reports. Independent of `region`.
    /// Default true.
    pub subscribe_reputation: Option<bool>,
    /// Seconds between reputation-report publish ticks. Default 3600 (matches
    /// the ADR 008 1-hour per-(reporter, node) rate limit). Must be `> 0`.
    pub reputation_publish_interval_sec: Option<u64>,
    /// Optional allowlist of accepted announcer node IDs, hex-encoded
    /// (64 hex chars, either case). Absent/empty = accept any signature-valid
    /// announce. Local stand-in for ADR 001 rule 2 (staked-node check)
    /// until the on-chain staking registry contract lands.
    pub allowlist: Option<Vec<String>>,
    /// Hard cap on `PeerTable` entry count (#577 H3). Once the table is at
    /// the cap, new announces from previously-unseen node IDs are
    /// rejected after a one-shot inline TTL sweep; existing entries are
    /// still refreshed. Absent => default 100 000. Must be `> 0`. A
    /// generous default sized for a ~tens-of-MB memory budget at the
    /// ~few-hundred-byte `PeerEntry` size, while still capping the
    /// fresh-keypair memory-DoS that the empty-allowlist `PoC` stand-in
    /// would otherwise leave unbounded.
    pub max_peer_table_entries: Option<u64>,
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

/// `[dht]` section — `cdn/dht/v1` settings (ADR 022).
///
/// The Kademlia routing-table parameters (k, α, bucket count, refresh
/// interval) are pinned by the protocol and not exposed here. The
/// rate-limit knobs nest under `[dht.rate_limit]` to match ADR 022 §DHT
/// Rate Limiting "Trusted-IP exemption" (`dht.rate_limit.trusted_ips`)
/// and to leave room for other future `dht.*` top-level knobs (e.g.
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
    /// IPs that bypass the per-IP layer only (per-peer + global still
    /// apply). Format: dotted IPv4 or RFC 5952 IPv6. Absent or empty =>
    /// no trusted IPs. ADR 022 §Trusted-IP exemption.
    pub trusted_ips: Option<Vec<String>>,
    /// Hard cap on the per-IP keyed-limiter map. Absent => 4096. `0`
    /// makes the map unbounded — operator opt-in (#645). Mirrors
    /// `security.max_tracked_sources` for the dispatch layer.
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
    /// Interval in seconds between per-region bandwidth accounting log lines
    /// (issue #750). `0` disables the periodic log. Absent → default
    /// `DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC` (3600).
    pub region_accounting_interval_sec: Option<u64>,
}

/// Download-receipt audit-log retention section of the config file (#802).
///
/// Bounds the otherwise-unbounded `download_receipts.jsonl` (one line per
/// accepted voucher interval, ~1 per MiB delivered) with size-based rotation:
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

/// `[prefetch]` — speculative-prefetch operator policy (ADR 022 §Prefetch
/// Decision "Recommended configuration"). Every field is optional; absent
/// keys take the ADR's recommended defaults. The whole feature is gated off
/// by `enabled = false` by default — operators must affirmatively opt in.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrefetchConfig {
    /// Master switch. Absent => `false` (opt-in).
    pub enabled: Option<bool>,
    /// Require an authorized origin in the `FIND_VALUE` candidate set before
    /// prefetching. Absent => `true`. Closes the demand-supply Sybil attack.
    pub require_authorized_origin: Option<bool>,
    /// Hard cap on aggregate prefetch spend over a rolling 1-hour window, in
    /// micro-USDC. Absent => `0` (no budget => never prefetches; a finite cap
    /// is the load-bearing recommendation).
    pub budget_usdc_per_hour: Option<u64>,
    /// `FIND_VALUE` queries for a hash within `threshold_window_secs` that trip
    /// the prefetch trigger. Absent => `5`. Must be `> 0`.
    pub find_value_threshold: Option<u32>,
    /// Rolling-window length (seconds) for the `FIND_VALUE` trigger. Absent =>
    /// `300`. Must be `> 0`.
    pub threshold_window_secs: Option<u64>,
    /// Auto-throttle floor on `served_bytes / acquired_bytes` over the
    /// demand-quality window. Absent => `0.1`. Must be finite in `[0.0, 1.0]`.
    pub demand_quality_min_ratio: Option<f64>,
    /// Rolling-window length (seconds) for the demand-quality predicate.
    /// Absent => `3600`. Must be `> 0`.
    pub demand_quality_window_secs: Option<u64>,
    /// Maximum prefetch acquisitions running concurrently (#820). Caps the
    /// background fan-out of speculative pulls so prefetch cannot starve demand
    /// traffic. Absent => `4`. Must be `> 0`.
    pub max_concurrent_acquisitions: Option<u32>,
    /// Wall-clock deadline (seconds) for a single prefetch acquisition's
    /// pull-through (#820). Absent => `30`. Must be `> 0`.
    pub acquisition_timeout_secs: Option<u64>,
}
