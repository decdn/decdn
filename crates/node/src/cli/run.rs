//! Arguments for the `decdn run` subcommand.

use std::path::PathBuf;

use clap::Args;

use super::common::{LogFormat, LogLevel};

/// Run the deCDN node.
#[derive(Args, Debug)]
pub struct RunArgs {
    /// Identity and data storage options.
    #[command(flatten)]
    pub identity: IdentityArgs,

    /// Network binding and relay options.
    #[command(flatten)]
    pub network: NetworkArgs,

    /// Blockchain / EVM connection options.
    #[command(flatten)]
    pub blockchain: BlockchainArgs,

    /// Cache storage options.
    #[command(flatten)]
    pub cache: CacheArgs,

    /// Payment rate options.
    #[command(flatten)]
    pub payment: PaymentArgs,

    /// Logging and metrics options.
    #[command(flatten)]
    pub observability: ObservabilityArgs,
}

/// Node identity and data storage.
#[derive(Args, Debug)]
#[command(next_help_heading = "Identity")]
pub struct IdentityArgs {
    /// Directory for node data (keys, cache, state) [default: ~/.decdn].
    #[arg(long, value_name = "DIR", env = "DECDN_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// ISO 3166-1 alpha-2 region code (e.g. US, DE, SG).
    #[arg(long, value_name = "CODE", env = "DECDN_REGION")]
    pub region: Option<String>,
}

/// Network binding and relay configuration.
#[derive(Args, Debug)]
#[command(next_help_heading = "Network")]
pub struct NetworkArgs {
    /// QUIC bind port [default: 4433].
    #[arg(long, value_name = "PORT", env = "DECDN_BIND_PORT")]
    pub bind_port: Option<u16>,

    /// iroh relay URL for NAT traversal.
    #[arg(long, value_name = "URL", env = "DECDN_RELAY_URL")]
    pub relay_url: Option<String>,
}

/// Blockchain / EVM connection settings.
#[derive(Args, Debug)]
#[command(next_help_heading = "Blockchain")]
pub struct BlockchainArgs {
    /// Arbitrum Sepolia JSON-RPC endpoint URL.
    #[arg(long, value_name = "URL", env = "DECDN_RPC_URL")]
    pub rpc_url: Option<String>,

    /// Path to Ethereum keystore file (JSON).
    #[arg(long, value_name = "PATH", env = "DECDN_ETH_KEYSTORE")]
    pub eth_keystore: Option<PathBuf>,

    /// `StablePaymentChannel` contract address (0x-prefixed hex).
    #[arg(long, value_name = "ADDR", env = "DECDN_PAYMENT_CHANNEL_ADDRESS")]
    pub payment_channel_address: Option<String>,

    /// `StakingRegistry` contract address (0x-prefixed hex).
    #[arg(long, value_name = "ADDR", env = "DECDN_STAKING_REGISTRY_ADDRESS")]
    pub staking_registry_address: Option<String>,
}

/// Cache storage configuration.
#[derive(Args, Debug)]
#[command(next_help_heading = "Cache")]
pub struct CacheArgs {
    /// Directory for cached blobs \[default: \<data-dir\>/cache\].
    #[arg(long, value_name = "DIR", env = "DECDN_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,

    /// Maximum cache size in megabytes [default: 10240].
    #[arg(long, value_name = "MB", env = "DECDN_CACHE_SIZE_MB")]
    pub cache_size_mb: Option<u64>,

    /// Maximum single blob size in megabytes [default: 1024]. Must be
    /// strictly less than `cache_size_mb`.
    #[arg(long, value_name = "MB", env = "DECDN_MAX_BLOB_SIZE_MB")]
    pub max_blob_size_mb: Option<u64>,

    /// Origin base URL served at `{url}/{blake3_hex}`. Absent = no pull-through.
    #[arg(long, value_name = "URL", env = "DECDN_ORIGIN_URL")]
    pub origin_url: Option<String>,

    /// Filesystem origin root. Blobs live at `{path}/{hex[0..2]}/{hex}`.
    /// Mutually exclusive with `--origin-url`.
    #[arg(long, value_name = "DIR", env = "DECDN_ORIGIN_PATH")]
    pub origin_path: Option<PathBuf>,
}

/// Payment rate configuration.
#[derive(Args, Debug)]
#[command(next_help_heading = "Payment")]
pub struct PaymentArgs {
    /// Rate per MB in USDC base units (6 decimals; 10 = $0.00001/MB) [default: 10].
    #[arg(long, value_name = "UNITS", env = "DECDN_RATE_PER_MB")]
    pub rate_per_mb: Option<u64>,
}

/// Observability settings (logging, metrics).
#[derive(Args, Debug)]
#[command(next_help_heading = "Observability")]
pub struct ObservabilityArgs {
    /// Log verbosity level [default: info].
    #[arg(long, value_name = "LEVEL", env = "DECDN_LOG_LEVEL")]
    pub log_level: Option<LogLevel>,

    /// Log output format [default: pretty].
    #[arg(long, value_name = "FORMAT", env = "DECDN_LOG_FORMAT")]
    pub log_format: Option<LogFormat>,

    /// HTTP port for Prometheus metrics and /health endpoint [default: 9090].
    #[arg(long, value_name = "PORT", env = "DECDN_METRICS_PORT")]
    pub metrics_port: Option<u16>,

    /// OTLP collector endpoint URL (enables span export; requires `--features otlp`).
    #[arg(long, value_name = "URL", env = "DECDN_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,
}
