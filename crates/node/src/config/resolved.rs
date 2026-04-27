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
    /// `StablePaymentChannel` contract address.
    pub payment_channel_address: String,
    /// `StakingRegistry` contract address.
    pub staking_registry_address: String,
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
    /// Optional HTTP origin base URL for pull-through on cache misses.
    /// Parsed, scheme-validated, and path-normalized at resolution time via
    /// [`decdn_cache::parse_origin_url`] so invalid URLs fail config
    /// loading. Constructing an [`decdn_cache::OriginUrl`] outside the
    /// parser is impossible — the invariants (http/https scheme,
    /// trailing-slash path, no query/fragment) are type-enforced.
    pub origin_url: Option<decdn_cache::OriginUrl>,
    /// Optional filesystem origin root. Blobs live at
    /// `{path}/{hex[0..2]}/{hex}`. Directory-existence is validated when
    /// the runtime constructs the [`decdn_cache::FilesystemOrigin`] —
    /// config resolution carries the raw path so resolution stays
    /// filesystem-free and testable without real I/O.
    pub origin_path: Option<PathBuf>,
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
}
