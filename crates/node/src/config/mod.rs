//! Configuration loading and resolution.
//!
//! Three-layer merge: CLI flags > TOML config file > built-in defaults.

pub mod resolved;
pub mod types;

use std::path::Path;

use crate::cli::common::{self, expand_tilde};
use crate::cli::run::RunArgs;

pub use resolved::ResolvedConfig;
pub use types::FileConfig;

/// Default QUIC bind port.
const DEFAULT_BIND_PORT: u16 = 4433;
/// Default maximum cache size in megabytes (10 GB).
const DEFAULT_CACHE_SIZE_MB: u64 = 10_240;
/// Default maximum single blob size in megabytes (10 GB).
const DEFAULT_MAX_BLOB_SIZE_MB: u64 = 10_240;
/// Default rate per MB in USDC base units ($0.00001/MB).
const DEFAULT_RATE_PER_MB: u64 = 10;
/// Default Prometheus metrics port.
const DEFAULT_METRICS_PORT: u16 = 9090;

/// Load config from file (if present) and merge with CLI args.
///
/// CLI args take precedence over file values; defaults fill gaps.
///
/// # Errors
///
/// Returns an error if:
/// - The config file exists but cannot be read or parsed.
/// - A required field (`rpc_url`, `payment_channel_address`,
///   `staking_registry_address`) is not provided by any source.
/// - The home directory cannot be determined for default paths.
pub fn resolve_config(config_path: Option<&Path>, cli: &RunArgs) -> anyhow::Result<ResolvedConfig> {
    let file = load_file_config(config_path)?;

    let identity = resolve_identity(&cli.identity, file.identity.as_ref())?;
    let network = resolve_network(&cli.network, file.network.as_ref());
    let blockchain = resolve_blockchain(&cli.blockchain, file.blockchain.as_ref(), &identity.0)?;
    let cache = resolve_cache(&cli.cache, file.cache.as_ref(), &identity.0);
    let payment = resolve_payment(&cli.payment, file.payment.as_ref());
    let obs = resolve_observability(&cli.observability, file.observability.as_ref());

    Ok(ResolvedConfig {
        data_dir: identity.0,
        region: identity.1,
        bind_port: network.0,
        relay_url: network.1,
        rpc_url: blockchain.0,
        eth_keystore: blockchain.1,
        payment_channel_address: blockchain.2,
        staking_registry_address: blockchain.3,
        cache_dir: cache.0,
        cache_size_mb: cache.1,
        max_blob_size_mb: cache.2,
        rate_per_mb: payment,
        log_level: obs.0,
        log_format: obs.1,
        metrics_port: obs.2,
    })
}

/// Resolve identity fields: `(data_dir, region)`.
fn resolve_identity(
    cli: &crate::cli::run::IdentityArgs,
    file: Option<&types::IdentityConfig>,
) -> anyhow::Result<(std::path::PathBuf, Option<String>)> {
    let data_dir = cli
        .data_dir
        .clone()
        .or_else(|| {
            file.and_then(|i| i.data_dir.clone())
                .map(|p| expand_tilde(&p))
        })
        .or_else(common::default_data_dir)
        .ok_or_else(|| anyhow::anyhow!("cannot determine data directory: home dir not found"))?;

    let region = cli
        .region
        .clone()
        .or_else(|| file.and_then(|i| i.region.clone()));

    Ok((data_dir, region))
}

/// Resolve network fields: `(bind_port, relay_url)`.
fn resolve_network(
    cli: &crate::cli::run::NetworkArgs,
    file: Option<&types::NetworkConfig>,
) -> (u16, Option<String>) {
    let bind_port = cli
        .bind_port
        .or_else(|| file.and_then(|n| n.bind_port))
        .unwrap_or(DEFAULT_BIND_PORT);

    let relay_url = cli
        .relay_url
        .clone()
        .or_else(|| file.and_then(|n| n.relay_url.clone()));

    (bind_port, relay_url)
}

/// Resolve blockchain fields: `(rpc_url, eth_keystore, payment_channel_address, staking_registry_address)`.
fn resolve_blockchain(
    cli: &crate::cli::run::BlockchainArgs,
    file: Option<&types::BlockchainConfig>,
    data_dir: &std::path::Path,
) -> anyhow::Result<(String, std::path::PathBuf, String, String)> {
    let rpc_url = cli
        .rpc_url
        .clone()
        .or_else(|| file.and_then(|b| b.rpc_url.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing required option: --rpc-url (or blockchain.rpc_url in config file)"
            )
        })?;

    let eth_keystore = cli
        .eth_keystore
        .clone()
        .or_else(|| {
            file.and_then(|b| b.eth_keystore.clone())
                .map(|p| expand_tilde(&p))
        })
        .unwrap_or_else(|| data_dir.join("keystore.json"));

    let payment_channel_address = cli
        .payment_channel_address
        .clone()
        .or_else(|| file.and_then(|b| b.payment_channel_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing required option: --payment-channel-address \
                 (or blockchain.payment_channel_address in config file)"
            )
        })?;

    let staking_registry_address = cli
        .staking_registry_address
        .clone()
        .or_else(|| file.and_then(|b| b.staking_registry_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing required option: --staking-registry-address \
                 (or blockchain.staking_registry_address in config file)"
            )
        })?;

    Ok((
        rpc_url,
        eth_keystore,
        payment_channel_address,
        staking_registry_address,
    ))
}

/// Resolve cache fields: `(cache_dir, cache_size_mb, max_blob_size_mb)`.
fn resolve_cache(
    cli: &crate::cli::run::CacheArgs,
    file: Option<&types::CacheConfig>,
    data_dir: &std::path::Path,
) -> (std::path::PathBuf, u64, u64) {
    let cache_dir = cli
        .cache_dir
        .clone()
        .or_else(|| {
            file.and_then(|c| c.cache_dir.clone())
                .map(|p| expand_tilde(&p))
        })
        .unwrap_or_else(|| data_dir.join("cache"));

    let cache_size_mb = cli
        .cache_size_mb
        .or_else(|| file.and_then(|c| c.cache_size_mb))
        .unwrap_or(DEFAULT_CACHE_SIZE_MB);

    let max_blob_size_mb = cli
        .max_blob_size_mb
        .or_else(|| file.and_then(|c| c.max_blob_size_mb))
        .unwrap_or(DEFAULT_MAX_BLOB_SIZE_MB);

    (cache_dir, cache_size_mb, max_blob_size_mb)
}

/// Resolve payment fields: `rate_per_mb`.
fn resolve_payment(cli: &crate::cli::run::PaymentArgs, file: Option<&types::PaymentConfig>) -> u64 {
    cli.rate_per_mb
        .or_else(|| file.and_then(|p| p.rate_per_mb))
        .unwrap_or(DEFAULT_RATE_PER_MB)
}

/// Resolve observability fields: `(log_level, log_format, metrics_port)`.
fn resolve_observability(
    cli: &crate::cli::run::ObservabilityArgs,
    file: Option<&types::ObservabilityConfig>,
) -> (
    crate::cli::common::LogLevel,
    crate::cli::common::LogFormat,
    u16,
) {
    let log_level = cli
        .log_level
        .or_else(|| file.and_then(|o| o.log_level))
        .unwrap_or_default();

    let log_format = cli
        .log_format
        .or_else(|| file.and_then(|o| o.log_format))
        .unwrap_or_default();

    let metrics_port = cli
        .metrics_port
        .or_else(|| file.and_then(|o| o.metrics_port))
        .unwrap_or(DEFAULT_METRICS_PORT);

    (log_level, log_format, metrics_port)
}

/// Load a [`FileConfig`] from disk.
///
/// - If `explicit_path` is `Some`, reads that file (errors if missing).
/// - If `explicit_path` is `None`, tries the default path; returns
///   `FileConfig::default()` if the file does not exist.
fn load_file_config(explicit_path: Option<&Path>) -> anyhow::Result<FileConfig> {
    let path = match explicit_path {
        Some(p) => p.to_path_buf(),
        None => match common::default_config_path() {
            Some(p) if p.exists() => p,
            _ => return Ok(FileConfig::default()),
        },
    };

    let contents = std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("failed to read config file {}: {e}", path.display()))?;

    let config: FileConfig = toml::from_str(&contents)
        .map_err(|e| anyhow::anyhow!("failed to parse config file {}: {e}", path.display()))?;

    Ok(config)
}
