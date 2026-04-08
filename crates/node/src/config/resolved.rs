//! Fully resolved configuration with concrete types (no `Option` fields).

use std::path::PathBuf;

use crate::cli::common::{LogFormat, LogLevel};

/// Fully resolved node configuration.
///
/// Every field has a value determined by the three-layer merge:
/// CLI flag > config file > built-in default.
///
/// Required fields that have no default (`rpc_url`,
/// `payment_channel_address`, `staking_registry_address`) cause
/// [`super::resolve_config`] to return an error if not provided.
#[derive(Debug)]
#[allow(dead_code)] // Fields will be consumed by the node runtime.
pub struct ResolvedConfig {
    // Identity
    /// Node data directory.
    pub data_dir: PathBuf,
    /// Region code (e.g. `"US"`).
    pub region: Option<String>,

    // Network
    /// QUIC bind port.
    pub bind_port: u16,
    /// iroh relay URL.
    pub relay_url: Option<String>,

    // Blockchain
    /// JSON-RPC endpoint URL.
    pub rpc_url: String,
    /// Ethereum keystore file path.
    pub eth_keystore: PathBuf,
    /// `StablePaymentChannel` contract address.
    pub payment_channel_address: String,
    /// `StakingRegistry` contract address.
    pub staking_registry_address: String,

    // Cache
    /// Blob cache directory.
    pub cache_dir: PathBuf,
    /// Maximum cache size in megabytes.
    pub cache_size_mb: u64,
    /// Maximum single blob size in megabytes.
    pub max_blob_size_mb: u64,

    // Payment
    /// Rate per MB in USDC base units (6 decimals).
    pub rate_per_mb: u64,

    // Observability
    /// Log verbosity level.
    pub log_level: LogLevel,
    /// Log output format.
    pub log_format: LogFormat,
    /// Prometheus metrics HTTP port.
    pub metrics_port: u16,
}
