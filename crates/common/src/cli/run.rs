//! Arguments for `decdn-node run` (the daemon's only subcommand).
//!
//! `RunArgs` is also flattened into `decdn config validate` on the user
//! CLI for env-var parity — operators get one set of `DECDN_*` env
//! mappings whether they're starting the daemon or dry-running the
//! resolver.

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

    /// Path to a file containing the keystore password. When unset, the
    /// `DECDN_KEYSTORE_PASSWORD` env var is consulted; when that is also
    /// unset and stdin is a TTY, an interactive prompt is used. A single
    /// trailing newline is stripped from the file contents.
    #[arg(long, value_name = "PATH", env = "DECDN_KEYSTORE_PASSWORD_FILE")]
    pub keystore_password_file: Option<PathBuf>,

    /// `StablePaymentChannel` contract address (0x-prefixed hex).
    #[arg(long, value_name = "ADDR", env = "DECDN_PAYMENT_CHANNEL_ADDRESS")]
    pub payment_channel_address: Option<String>,

    /// `StakingRegistry` contract address (0x-prefixed hex).
    #[arg(long, value_name = "ADDR", env = "DECDN_STAKING_REGISTRY_ADDRESS")]
    pub staking_registry_address: Option<String>,

    /// `SlashJudge` contract address (0x-prefixed hex) — EIP-712
    /// `verifyingContract` for probe `slash_sig` (ADR 014).
    #[arg(long, value_name = "ADDR", env = "DECDN_SLASH_JUDGE_ADDRESS")]
    pub slash_judge_address: Option<String>,

    /// EIP-712 chain id for the `slash_sig` domain [default: 421614].
    #[arg(long, value_name = "ID", env = "DECDN_CHAIN_ID")]
    pub chain_id: Option<u64>,
}

/// Cache storage configuration.
///
/// As of #437, origin selection (HTTP / filesystem / S3-compatible)
/// lives only in the config-file `[cache.origin]` table — there are no
/// CLI flags for it. The S3 backend has many fields (bucket, region,
/// endpoint, credentials) that don't fit cleanly on a command line, and
/// keeping all three backends file-only avoids the trap of a
/// CLI-vs-TOML mismatch silently picking the wrong backend.
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

    /// Max concurrently held (eviction-exempt) blobs for the probe hold
    /// (ADR 005 §Hold budget) [default: 256]. `0` disables `has_blob: true`.
    #[arg(long, value_name = "N", env = "DECDN_MAX_PROBE_HOLDS")]
    pub max_probe_holds: Option<u64>,
}

/// Payment rate configuration.
#[derive(Args, Debug, Clone)]
#[command(next_help_heading = "Payment")]
pub struct PaymentArgs {
    /// Rate per MB in USDC base units (6 decimals; 10 = $0.00001/MB) [default: 10].
    #[arg(long, value_name = "UNITS", env = "DECDN_RATE_PER_MB")]
    pub rate_per_mb: Option<u64>,

    /// Lower bound `rate_per_mb` is clamped to before signing a
    /// `ProbeResponse` (ADR 005 §Rate bounds validation) [default: 0].
    #[arg(long, value_name = "UNITS", env = "DECDN_DELIVERY_FLOOR")]
    pub delivery_floor: Option<u64>,

    /// Upper bound `rate_per_mb` is clamped to before signing a
    /// `ProbeResponse` [default: protocol `MAX_RATE_PER_MB`].
    #[arg(long, value_name = "UNITS", env = "DECDN_DELIVERY_CEILING")]
    pub delivery_ceiling: Option<u64>,
}

/// Observability settings (logging, metrics).
#[derive(Args, Debug, Clone)]
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

    /// IP address to bind the metrics HTTP server on [default: 127.0.0.1].
    /// Set to 0.0.0.0 for containerised deployments where the Prometheus
    /// scraper runs on a different host.
    #[arg(long, value_name = "ADDR", env = "DECDN_METRICS_BIND")]
    pub metrics_bind: Option<std::net::IpAddr>,

    /// Loopback admin HTTP port (ADR 025); `0` disables [default: 9191].
    #[arg(long, value_name = "PORT", env = "DECDN_ADMIN_PORT")]
    pub admin_port: Option<u16>,

    /// OTLP collector endpoint URL (enables span export; requires `--features otlp`).
    #[arg(long, value_name = "URL", env = "DECDN_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,
}
