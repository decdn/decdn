use std::{net::SocketAddr, path::PathBuf, str::FromStr};

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;

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

/// Sponsor server configuration, loaded from `SPONSOR_*` environment
/// variables. See `from_env` for the exact variable names and defaults.
pub struct ServerConfig {
    pub bind: SocketAddr,
    /// The gateway's own public base URL, e.g. `https://up.decdn.org`. Baked
    /// into the templated `GET /decdn.sh` installer script (`{{GATEWAY_BASE}}`)
    /// so end users never set an env var for it.
    pub public_url: String,
    pub rpc_url: String,
    pub chain_id: u64,
    pub payment_channel: Address,
    pub capacity_bond: Address,
    pub treasury_keystore: PathBuf,
    pub initial_deposit: MicroUsdc,
    pub working_balance: MicroUsdc,
    pub monthly_cap: MicroUsdc,
    pub turnstile_secret: String,
    pub turnstile_sitekey: String,
    pub data_dir: PathBuf,
    pub topup_max_skew_secs: u64,
    /// Local bookkeeping TTL the expiry-reclaim sweep (`crate::reclaim`)
    /// uses to pick channels to attempt reclaiming: `opened_unix + ttl <=
    /// now`. Distinct from (and expected to stay below) the on-chain
    /// `PaymentChannel.Channel.expiresAt` the contract itself enforces.
    pub channel_ttl_secs: u64,
    /// How often `reclaim::run` sweeps the store for expired channels.
    pub reclaim_interval_secs: u64,
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
            payment_channel: Address::from_str(&env("SPONSOR_PAYMENT_CHANNEL_ADDR")?)?,
            capacity_bond: Address::from_str(&env("SPONSOR_CAPACITY_BOND_ADDR")?)?,
            treasury_keystore: PathBuf::from(env("SPONSOR_TREASURY_KEYSTORE")?),
            initial_deposit: MicroUsdc(env_u64("SPONSOR_INITIAL_DEPOSIT_MICRO_USDC", 2_000_000)?),
            working_balance: MicroUsdc(env_u64("SPONSOR_WORKING_BALANCE_MICRO_USDC", 2_000_000)?),
            monthly_cap: MicroUsdc(env_u64("SPONSOR_MONTHLY_CAP_MICRO_USDC", 10_000_000)?),
            turnstile_secret: env("SPONSOR_TURNSTILE_SECRET")?,
            turnstile_sitekey: env("SPONSOR_TURNSTILE_SITEKEY")?,
            data_dir: PathBuf::from(
                std::env::var("SPONSOR_DATA_DIR").unwrap_or_else(|_| "./data".into()),
            ),
            topup_max_skew_secs: env_u64("SPONSOR_TOPUP_MAX_SKEW_SECS", 120)?,
            // Default matches `MAX_CHANNEL_DURATION_FLOOR` (7 days) in
            // `contracts/src/PaymentChannel.sol` — the shortest duration
            // governance can configure a channel's on-chain expiry to, so
            // the sweep never fires meaningfully ahead of the earliest a
            // real channel could actually be expired.
            channel_ttl_secs: env_u64("SPONSOR_CHANNEL_TTL_SECS", 7 * 24 * 60 * 60)?,
            reclaim_interval_secs: env_u64("SPONSOR_RECLAIM_INTERVAL_SECS", 3600)?,
        })
    }

    /// Load the treasury hot-wallet signer from `treasury_keystore`, with the
    /// password taken from `SPONSOR_TREASURY_PASSWORD`. Runs the (blocking,
    /// scrypt-backed) keystore decrypt on the blocking thread pool.
    pub async fn load_treasury_signer(&self) -> anyhow::Result<PrivateKeySigner> {
        let ks = self.treasury_keystore.clone();
        let pw = std::env::var("SPONSOR_TREASURY_PASSWORD")
            .map_err(|_| anyhow::anyhow!("missing env SPONSOR_TREASURY_PASSWORD"))?;
        tokio::task::spawn_blocking(move || decdn_incentive::eth_identity::load_signer(&ks, &pw))
            .await?
    }

    /// Bridge to Task 7's `treasury::connect`: load the signer, assemble a
    /// `TreasuryConfig`, and connect. Kept as a method here (rather than
    /// changing `connect`'s signature) so `treasury.rs`'s `Treasury` trait
    /// and `connect` free function stay exactly as Task 7 built them.
    pub async fn build_treasury(&self) -> anyhow::Result<Box<dyn Treasury>> {
        let signer = self.load_treasury_signer().await?;
        treasury::connect(&TreasuryConfig {
            rpc_url: self.rpc_url.clone(),
            payment_channel: self.payment_channel,
            chain_id: self.chain_id,
            signer,
        })
        .await
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
    fn from_env_reads_required_fields() {
        // set the minimal env then parse. `set_var`/`remove_var` are unsafe
        // in edition 2024 (not thread-safe wrt other threads' env reads);
        // guarded by #[serial] to ensure single-threaded execution.
        unsafe {
            std::env::set_var("SPONSOR_RPC_URL", "http://localhost:8545");
            std::env::set_var(
                "SPONSOR_PAYMENT_CHANNEL_ADDR",
                "0x0000000000000000000000000000000000000001",
            );
            std::env::set_var(
                "SPONSOR_CAPACITY_BOND_ADDR",
                "0x0000000000000000000000000000000000000002",
            );
            std::env::set_var("SPONSOR_TREASURY_KEYSTORE", "/tmp/ks.json");
            std::env::set_var("SPONSOR_TURNSTILE_SECRET", "s");
            std::env::set_var("SPONSOR_TURNSTILE_SITEKEY", "k");
            std::env::remove_var("SPONSOR_CHAIN_ID");
            std::env::remove_var("SPONSOR_INITIAL_DEPOSIT_MICRO_USDC");
            std::env::remove_var("SPONSOR_MONTHLY_CAP_MICRO_USDC");
        }
        let cfg = ServerConfig::from_env().unwrap();
        assert_eq!(cfg.chain_id, 421_614); // default
        assert_eq!(cfg.initial_deposit.0, 2_000_000); // default
        assert_eq!(cfg.monthly_cap.0, 10_000_000); // default
    }
}
