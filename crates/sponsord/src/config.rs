//! Command-line and `SPONSORD_*` environment configuration. Every setting
//! is a flag and an environment variable; `sponsord --help` lists them with
//! their defaults. Secrets can also be given as files (`*_FILE`).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use clap::{ArgGroup, Parser};
use sponsord_api::secret::Secret;
use sponsord_core::{ChainConfig, KeeperConfig, Limits, MicroUsdc};

/// Shortest accepted API token, in bytes (`openssl rand -hex 32` gives 64).
const MIN_API_TOKEN_LEN: usize = 32;

/// sponsord: signs capped, expiring capabilities against the sponsor's
/// PaymentPool for callers holding its bearer token, and keeps the pool
/// funded from the treasury.
#[derive(Debug, Parser)]
#[command(name = "sponsord", version)]
#[command(group(ArgGroup::new("api_token_src").required(true).args(["api_token", "api_token_file"])))]
#[command(group(ArgGroup::new("treasury_password_src").required(true).args(["treasury_password", "treasury_password_file"])))]
pub struct Args {
    /// Address the HTTP API listens on.
    #[arg(long, env = "SPONSORD_BIND", default_value = "127.0.0.1:8090")]
    pub bind: SocketAddr,

    /// Bearer token callers present; at least 32 bytes
    /// (`openssl rand -hex 32`).
    #[arg(long, env = "SPONSORD_API_TOKEN", hide_env_values = true)]
    pub api_token: Option<String>,
    /// File holding the bearer token, instead of `--api-token`.
    #[arg(long, env = "SPONSORD_API_TOKEN_FILE")]
    pub api_token_file: Option<PathBuf>,

    /// JSON-RPC endpoint of the chain the pool is on.
    #[arg(long, env = "SPONSORD_RPC_URL")]
    pub rpc_url: String,
    /// Chain id (421614 is Arbitrum Sepolia).
    #[arg(long, env = "SPONSORD_CHAIN_ID", default_value_t = 421_614)]
    pub chain_id: u64,
    /// `PaymentPool` contract address.
    #[arg(long, env = "SPONSORD_PAYMENT_POOL_ADDR")]
    pub payment_pool: Address,
    /// Id of the sponsor's pool, opened out-of-band with `decdn pool open`.
    #[arg(long, env = "SPONSORD_POOL_ID")]
    pub pool_id: B256,

    /// The treasury wallet's encrypted keystore JSON. Its key owns the pool:
    /// it signs capabilities and pays for top-ups.
    #[arg(long, env = "SPONSORD_TREASURY_KEYSTORE")]
    pub treasury_keystore: PathBuf,
    /// Password of the treasury keystore.
    #[arg(long, env = "SPONSORD_TREASURY_PASSWORD", hide_env_values = true)]
    pub treasury_password: Option<String>,
    /// File holding the keystore password, instead of `--treasury-password`.
    #[arg(long, env = "SPONSORD_TREASURY_PASSWORD_FILE")]
    pub treasury_password_file: Option<PathBuf>,

    /// Largest cap a caller may request per capability, in micro-USDC
    /// (5000000 = $5).
    #[arg(long, env = "SPONSORD_MAX_SPENDING_CAP_MICRO_USDC", default_value_t = MicroUsdc(5_000_000))]
    pub max_spending_cap: MicroUsdc,
    /// Longest TTL a caller may request, in seconds (172800 = 48 hours).
    #[arg(long, env = "SPONSORD_MAX_TTL_SECS", default_value_t = 172_800)]
    pub max_ttl_secs: u64,

    /// Top the pool up when its remaining balance drops below this, in
    /// micro-USDC ($20).
    #[arg(long, env = "SPONSORD_POOL_LOW_WATER_MICRO_USDC", default_value_t = MicroUsdc(20_000_000))]
    pub pool_low_water: MicroUsdc,
    /// Amount each top-up adds, in micro-USDC ($100).
    #[arg(long, env = "SPONSORD_POOL_REFILL_MICRO_USDC", default_value_t = MicroUsdc(100_000_000))]
    pub pool_refill: MicroUsdc,
    /// How often the pool balance is checked, in seconds.
    #[arg(
        long,
        env = "SPONSORD_POOL_WATCH_INTERVAL_SECS",
        default_value_t = 3600
    )]
    pub pool_watch_interval_secs: u64,
}

/// Resolved configuration.
#[derive(Debug)]
pub struct DaemonConfig {
    pub bind: SocketAddr,
    pub api_token: Secret,
    /// The treasury wallet's keystore: the pool owner, which signs
    /// capabilities and pays for top-ups.
    pub treasury_keystore: PathBuf,
    pub treasury_password: Secret,
    pub chain: ChainConfig,
    pub limits: Limits,
    pub keeper: KeeperConfig,
}

impl DaemonConfig {
    /// Parse the command line and environment.
    ///
    /// # Errors
    ///
    /// See [`DaemonConfig::from_args`].
    pub fn load() -> anyhow::Result<Self> {
        Self::from_args(Args::parse())
    }

    /// # Errors
    ///
    /// A secret file cannot be read, the API token is shorter than 32 bytes,
    /// or a limit or the watch interval is zero.
    pub fn from_args(args: Args) -> anyhow::Result<Self> {
        let api_token = Secret::resolve(
            "SPONSORD_API_TOKEN",
            args.api_token,
            args.api_token_file.as_deref(),
        )?;
        anyhow::ensure!(
            api_token.expose().len() >= MIN_API_TOKEN_LEN,
            "SPONSORD_API_TOKEN must be at least {MIN_API_TOKEN_LEN} bytes"
        );
        let treasury_password = Secret::resolve(
            "SPONSORD_TREASURY_PASSWORD",
            args.treasury_password,
            args.treasury_password_file.as_deref(),
        )?;
        anyhow::ensure!(
            args.max_spending_cap > MicroUsdc::ZERO && args.max_ttl_secs > 0,
            "SPONSORD_MAX_SPENDING_CAP_MICRO_USDC and SPONSORD_MAX_TTL_SECS must be above zero"
        );
        anyhow::ensure!(
            args.pool_watch_interval_secs > 0,
            "SPONSORD_POOL_WATCH_INTERVAL_SECS must be above zero"
        );
        Ok(Self {
            bind: args.bind,
            api_token,
            treasury_keystore: args.treasury_keystore,
            treasury_password,
            chain: ChainConfig {
                rpc_url: args.rpc_url,
                chain_id: args.chain_id,
                payment_pool: args.payment_pool,
                pool_id: args.pool_id,
            },
            limits: Limits {
                max_spending_cap: args.max_spending_cap,
                max_ttl_secs: args.max_ttl_secs,
            },
            keeper: KeeperConfig {
                low_water: args.pool_low_water,
                refill: args.pool_refill,
                interval: Duration::from_secs(args.pool_watch_interval_secs),
            },
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    const POOL: &str = "0x0000000000000000000000000000000000000001";

    fn args(extra: &[&str]) -> Result<Args, clap::Error> {
        let pool_id = format!("0x{}", "11".repeat(32));
        let token = "a".repeat(32);
        let mut argv = vec![
            "sponsord",
            "--rpc-url",
            "http://localhost:8545",
            "--payment-pool",
            POOL,
            "--pool-id",
            &pool_id,
            "--treasury-keystore",
            "/tmp/ks.json",
            "--treasury-password",
            "pw",
        ];
        if !extra.iter().any(|a| a.starts_with("--api-token")) {
            argv.extend(["--api-token", &token]);
        }
        argv.extend(extra);
        Args::try_parse_from(argv)
    }

    #[test]
    fn reads_required_and_defaults() {
        let cfg = DaemonConfig::from_args(args(&[]).unwrap()).unwrap();
        assert_eq!(cfg.bind, "127.0.0.1:8090".parse().unwrap());
        assert_eq!(cfg.chain.chain_id, 421_614);
        assert_eq!(cfg.chain.payment_pool, POOL.parse::<Address>().unwrap());
        assert_eq!(cfg.limits.max_spending_cap, MicroUsdc(5_000_000));
        assert_eq!(cfg.limits.max_ttl_secs, 172_800);
        assert_eq!(cfg.keeper.low_water, MicroUsdc(20_000_000));
        assert_eq!(cfg.keeper.interval, Duration::from_secs(3600));
    }

    #[test]
    fn parse_errors_name_the_setting() {
        let err = args(&["--max-ttl-secs", "abc"]).unwrap_err().to_string();
        assert!(err.contains("--max-ttl-secs"), "{err}");
    }

    #[test]
    fn refuses_a_short_api_token_and_requires_one() {
        let short = "a".repeat(31);
        let err = DaemonConfig::from_args(args(&["--api-token", &short]).unwrap()).unwrap_err();
        assert!(err.to_string().contains("at least 32"), "{err}");
    }

    #[test]
    fn secrets_can_come_from_files_but_not_from_both() {
        let dir = std::env::temp_dir().join(format!("sponsord-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let token_file = dir.join("token");
        std::fs::write(&token_file, format!("{}\n", "b".repeat(40))).unwrap();
        let path = token_file.to_str().unwrap();

        let cfg = DaemonConfig::from_args(args(&["--api-token-file", path]).unwrap()).unwrap();
        assert_eq!(cfg.api_token.expose(), "b".repeat(40));
        assert!(args(&["--api-token-file", path, "--api-token", &"c".repeat(32)]).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn debug_redacts_secrets() {
        let cfg = DaemonConfig::from_args(args(&[]).unwrap()).unwrap();
        let shown = format!("{cfg:?}");
        assert!(!shown.contains(&"a".repeat(32)));
        assert!(!shown.contains("\"pw\""));
        assert!(shown.contains("<redacted>"));
    }

    #[test]
    fn zero_limits_are_refused() {
        let err = DaemonConfig::from_args(args(&["--max-ttl-secs", "0"]).unwrap()).unwrap_err();
        assert!(err.to_string().contains("above zero"), "{err}");
    }
}
