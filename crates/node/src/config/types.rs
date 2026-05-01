//! TOML-deserializable configuration file types.
//!
//! All fields are `Option` so that missing keys in the TOML file are
//! accepted. The merge logic in [`super::resolve_config`] fills in defaults.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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
    /// Connection rate-limiting settings (issue #235).
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
    /// watchdog entirely; absent => default (30s).
    pub rpc_watchdog_interval_sec: Option<u64>,
}

/// Cache section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Blob cache directory.
    pub cache_dir: Option<PathBuf>,
    /// Maximum cache size in megabytes.
    pub cache_size_mb: Option<u64>,
    /// Maximum single blob size in megabytes.
    pub max_blob_size_mb: Option<u64>,
    /// Origin base URL (HTTP/HTTPS) served at `{url}/{blake3_hex}`. When
    /// absent, cache misses fail with `NoOrigin` — useful for nodes that
    /// only serve already-pinned content.
    pub origin_url: Option<String>,
    /// Local filesystem origin root; blobs live at
    /// `{path}/{hex[0..2]}/{hex}`. Mutually exclusive with `origin_url`.
    pub origin_path: Option<PathBuf>,
    /// How to handle `Content-Encoding` on the HTTP origin response
    /// (#312). `"auto"` (default) decompresses gzip/zstd transparently;
    /// `"strict"` refuses any non-identity encoding. The BLAKE3
    /// content-address is computed over the canonical (decompressed)
    /// form, so `"strict"` is only safe for origins guaranteed to
    /// serve already-canonical bytes.
    pub decompress: Option<decdn_cache::DecompressMode>,
    /// Hex-encoded BLAKE3 hashes that must stay cached regardless of LRU
    /// pressure (#276). Each entry is 64 lowercase hex chars (BLAKE3
    /// digest size). Invalid hex or wrong-length entries cause config
    /// resolution to fail — fail-fast at load time beats a silent
    /// "entry was ignored" surprise hours later when the operator
    /// discovers the blob got evicted anyway.
    pub pinned_hashes: Option<Vec<String>>,
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

/// Security / rate-limiting section of the config file (issue #235).
///
/// All fields are optional; defaults produce a safe configuration out of the box.
/// Values can only be changed by restarting the node — none of these fields are
/// hot-reloadable because rebuilding the semaphore or resetting in-flight token
/// buckets mid-run would transiently allow or deny traffic in unpredictable ways.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Maximum number of concurrently in-flight QUIC handler tasks across all
    /// deCDN-authored ALPNs. New connections beyond this limit are closed
    /// immediately with `APP_ERR_RATE_LIMITED`. Default: 256.
    pub max_concurrent_handlers: Option<u32>,
    /// Token-bucket refill rate for per-NodeID limiting (tokens per second).
    /// Matches ADR 001's inbound probe limit. Default: 20.
    pub per_node_rate_per_sec: Option<f64>,
    /// Token-bucket burst capacity for per-NodeID limiting. Default: 20.
    pub per_node_burst: Option<u32>,
    /// Token-bucket refill rate for per-IP limiting (tokens per second).
    /// More generous than per-NodeID because one IP may host a legitimate fleet.
    /// Default: 100.
    pub per_ip_rate_per_sec: Option<f64>,
    /// Token-bucket burst capacity for per-IP limiting. Default: 200.
    pub per_ip_burst: Option<u32>,
    /// Hard cap on the number of distinct `NodeIDs` (and separately, IPs) tracked
    /// in the rate-limit state. When full, the oldest entry is evicted. Default: 4096.
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
