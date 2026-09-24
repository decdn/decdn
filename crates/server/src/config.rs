use std::{net::SocketAddr, path::PathBuf, str::FromStr};

use alloy::primitives::{Address, B256};
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::voucher_domain;

use crate::issuer::Issuer;
use crate::money::MicroUsdc;
use crate::treasury::{self, Treasury, TreasuryConfig};

fn env(k: &str) -> anyhow::Result<String> {
    std::env::var(k).map_err(|_| anyhow::anyhow!("missing env {k}"))
}
fn env_u64(k: &str, default: u64) -> anyhow::Result<u64> {
    match std::env::var(k) {
        Ok(v) => Ok(v.parse()?),
        Err(_) => Ok(default),
    }
}

pub struct ServerConfig {
    pub bind: SocketAddr,
    pub public_url: String,
    pub rpc_url: String,
    pub chain_id: u64,
    pub payment_pool: Address,
    pub pool_id: B256,
    pub capacity_bond: Address,
    pub treasury_keystore: PathBuf,
    pub capability_cap: MicroUsdc,
    pub capability_ttl_secs: u64,
    pub pool_low_water: MicroUsdc,
    pub pool_refill: MicroUsdc,
    pub pool_watch_interval_secs: u64,
    pub turnstile_secret: String,
    pub turnstile_sitekey: String,
    pub data_dir: PathBuf,
}

impl ServerConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            bind: SocketAddr::from_str(
                &std::env::var("SPONSOR_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into()),
            )?,
            public_url: std::env::var("SPONSOR_PUBLIC_URL")
                .unwrap_or_else(|_| "https://up.decdn.org".into()),
            rpc_url: env("SPONSOR_RPC_URL")?,
            chain_id: env_u64("SPONSOR_CHAIN_ID", 421_614)?,
            payment_pool: Address::from_str(&env("SPONSOR_PAYMENT_POOL_ADDR")?)?,
            pool_id: B256::from_str(&env("SPONSOR_POOL_ID")?)?,
            capacity_bond: Address::from_str(&env("SPONSOR_CAPACITY_BOND_ADDR")?)?,
            treasury_keystore: PathBuf::from(env("SPONSOR_TREASURY_KEYSTORE")?),
            capability_cap: MicroUsdc(env_u64("SPONSOR_CAPABILITY_CAP_MICRO_USDC", 5_000_000)?),
            capability_ttl_secs: env_u64("SPONSOR_CAPABILITY_TTL_SECS", 172_800)?,
            pool_low_water: MicroUsdc(env_u64("SPONSOR_POOL_LOW_WATER_MICRO_USDC", 20_000_000)?),
            pool_refill: MicroUsdc(env_u64("SPONSOR_POOL_REFILL_MICRO_USDC", 100_000_000)?),
            pool_watch_interval_secs: env_u64("SPONSOR_POOL_WATCH_INTERVAL_SECS", 3600)?,
            turnstile_secret: env("SPONSOR_TURNSTILE_SECRET")?,
            turnstile_sitekey: env("SPONSOR_TURNSTILE_SITEKEY")?,
            data_dir: PathBuf::from(
                std::env::var("SPONSOR_DATA_DIR").unwrap_or_else(|_| "./data".into()),
            ),
        })
    }

    pub async fn load_treasury_signer(&self) -> anyhow::Result<PrivateKeySigner> {
        let ks = self.treasury_keystore.clone();
        let pw = std::env::var("SPONSOR_TREASURY_PASSWORD")
            .map_err(|_| anyhow::anyhow!("missing env SPONSOR_TREASURY_PASSWORD"))?;
        tokio::task::spawn_blocking(move || decdn_incentive::eth_identity::load_signer(&ks, &pw))
            .await?
    }

    pub async fn build_treasury(
        &self,
        signer: PrivateKeySigner,
    ) -> anyhow::Result<Box<dyn Treasury>> {
        treasury::connect(&TreasuryConfig {
            rpc_url: self.rpc_url.clone(),
            payment_pool: self.payment_pool,
            chain_id: self.chain_id,
            signer,
        })
        .await
    }

    pub fn build_issuer(&self, signer: PrivateKeySigner) -> Issuer {
        Issuer::new(
            signer,
            voucher_domain(self.chain_id, self.payment_pool),
            self.pool_id,
            self.capability_cap.0,
            self.capability_ttl_secs,
        )
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

    #[test]
    #[serial]
    fn from_env_reads_required_and_defaults() {
        unsafe {
            std::env::set_var("SPONSOR_RPC_URL", "http://localhost:8545");
            std::env::set_var(
                "SPONSOR_PAYMENT_POOL_ADDR",
                "0x0000000000000000000000000000000000000001",
            );
            std::env::set_var("SPONSOR_POOL_ID", format!("0x{}", "11".repeat(32)));
            std::env::set_var(
                "SPONSOR_CAPACITY_BOND_ADDR",
                "0x0000000000000000000000000000000000000002",
            );
            std::env::set_var("SPONSOR_TREASURY_KEYSTORE", "/tmp/ks.json");
            std::env::set_var("SPONSOR_TURNSTILE_SECRET", "s");
            std::env::set_var("SPONSOR_TURNSTILE_SITEKEY", "k");
            std::env::remove_var("SPONSOR_CHAIN_ID");
            std::env::remove_var("SPONSOR_CAPABILITY_CAP_MICRO_USDC");
            std::env::remove_var("SPONSOR_CAPABILITY_TTL_SECS");
        }
        let cfg = ServerConfig::from_env().unwrap();
        assert_eq!(cfg.chain_id, 421_614);
        assert_eq!(cfg.capability_cap.0, 5_000_000);
        assert_eq!(cfg.capability_ttl_secs, 172_800);
        assert_eq!(cfg.pool_low_water.0, 20_000_000);
    }
}
