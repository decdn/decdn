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
}

/// Payment section of the config file.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PaymentConfig {
    /// Rate per MB in USDC base units.
    pub rate_per_mb: Option<u64>,
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
    /// OTLP collector endpoint URL.
    pub otlp_endpoint: Option<String>,
}
