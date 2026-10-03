//! `SPONSORD_*` environment configuration.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use sponsord_core::SponsorConfig;
use sponsord_core::money::MicroUsdc;

/// Shortest accepted `SPONSORD_API_TOKEN`, in bytes (`openssl rand -hex 32`
/// gives 64).
const MIN_API_TOKEN_LEN: usize = 32;

fn env(k: &str) -> anyhow::Result<String> {
    std::env::var(k).map_err(|_| anyhow::anyhow!("missing env {k}"))
}

fn env_u64(k: &str, default: u64) -> anyhow::Result<u64> {
    match std::env::var(k) {
        Ok(v) => v.parse().map_err(|e| anyhow::anyhow!("{k}: {e}")),
        Err(_) => Ok(default),
    }
}

pub struct DaemonConfig {
    pub bind: SocketAddr,
    pub api_token: String,
    pub sponsor: SponsorConfig,
    pub pool_low_water: MicroUsdc,
    pub pool_refill: MicroUsdc,
    pub pool_watch_interval: Duration,
}

impl DaemonConfig {
    /// # Errors
    ///
    /// A required variable is missing or malformed, or the API token is
    /// shorter than 32 bytes.
    pub fn from_env() -> anyhow::Result<Self> {
        let api_token = env("SPONSORD_API_TOKEN")?;
        anyhow::ensure!(
            api_token.len() >= MIN_API_TOKEN_LEN,
            "SPONSORD_API_TOKEN must be at least {MIN_API_TOKEN_LEN} bytes"
        );
        Ok(Self {
            bind: SocketAddr::from_str(
                &std::env::var("SPONSORD_BIND").unwrap_or_else(|_| "127.0.0.1:8090".into()),
            )
            .map_err(|e| anyhow::anyhow!("SPONSORD_BIND: {e}"))?,
            api_token,
            sponsor: SponsorConfig {
                rpc_url: env("SPONSORD_RPC_URL")?,
                chain_id: env_u64("SPONSORD_CHAIN_ID", 421_614)?,
                payment_pool: Address::from_str(&env("SPONSORD_PAYMENT_POOL_ADDR")?)
                    .map_err(|e| anyhow::anyhow!("SPONSORD_PAYMENT_POOL_ADDR: {e}"))?,
                pool_id: B256::from_str(&env("SPONSORD_POOL_ID")?)
                    .map_err(|e| anyhow::anyhow!("SPONSORD_POOL_ID: {e}"))?,
                treasury_keystore: PathBuf::from(env("SPONSORD_TREASURY_KEYSTORE")?),
                treasury_password: env("SPONSORD_TREASURY_PASSWORD")?,
                max_spending_cap: env_u64("SPONSORD_MAX_SPENDING_CAP_MICRO_USDC", 5_000_000)?,
                max_ttl_secs: env_u64("SPONSORD_MAX_TTL_SECS", 172_800)?,
            },
            pool_low_water: MicroUsdc(env_u64("SPONSORD_POOL_LOW_WATER_MICRO_USDC", 20_000_000)?),
            pool_refill: MicroUsdc(env_u64("SPONSORD_POOL_REFILL_MICRO_USDC", 100_000_000)?),
            pool_watch_interval: Duration::from_secs(env_u64(
                "SPONSORD_POOL_WATCH_INTERVAL_SECS",
                3600,
            )?),
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
    use serial_test::serial;

    fn set_required(token: &str) {
        unsafe {
            std::env::set_var("SPONSORD_API_TOKEN", token);
            std::env::set_var("SPONSORD_RPC_URL", "http://localhost:8545");
            std::env::set_var(
                "SPONSORD_PAYMENT_POOL_ADDR",
                "0x0000000000000000000000000000000000000001",
            );
            std::env::set_var("SPONSORD_POOL_ID", format!("0x{}", "11".repeat(32)));
            std::env::set_var("SPONSORD_TREASURY_KEYSTORE", "/tmp/ks.json");
            std::env::set_var("SPONSORD_TREASURY_PASSWORD", "pw");
            for k in [
                "SPONSORD_BIND",
                "SPONSORD_CHAIN_ID",
                "SPONSORD_MAX_SPENDING_CAP_MICRO_USDC",
                "SPONSORD_MAX_TTL_SECS",
            ] {
                std::env::remove_var(k);
            }
        }
    }

    #[test]
    #[serial]
    fn reads_required_and_defaults() {
        set_required(&"a".repeat(32));
        let cfg = DaemonConfig::from_env().unwrap();
        assert_eq!(cfg.bind, "127.0.0.1:8090".parse().unwrap());
        assert_eq!(cfg.sponsor.chain_id, 421_614);
        assert_eq!(cfg.sponsor.max_spending_cap, 5_000_000);
        assert_eq!(cfg.sponsor.max_ttl_secs, 172_800);
        assert_eq!(cfg.pool_watch_interval, Duration::from_secs(3600));
    }

    #[test]
    #[serial]
    fn parse_errors_name_the_variable() {
        set_required(&"a".repeat(32));
        unsafe { std::env::set_var("SPONSORD_MAX_TTL_SECS", "abc") };
        let err = DaemonConfig::from_env().err().unwrap().to_string();
        unsafe { std::env::remove_var("SPONSORD_MAX_TTL_SECS") };
        assert!(err.contains("SPONSORD_MAX_TTL_SECS"), "{err}");
    }

    #[test]
    #[serial]
    fn refuses_a_short_or_missing_api_token() {
        set_required(&"a".repeat(31));
        assert!(DaemonConfig::from_env().is_err());
        unsafe { std::env::remove_var("SPONSORD_API_TOKEN") };
        assert!(DaemonConfig::from_env().is_err());
    }
}
