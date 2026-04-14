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
    /// Rate per MB in USDC base units (6 decimals).
    pub rate_per_mb: u64,
    pub observability: ResolvedObservability,
}
