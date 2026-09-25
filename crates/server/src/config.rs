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

/// A GitHub Release the installers download binaries from, pinned by its tag
/// and by the SHA-256 of its `SHA256SUMS` file. The installer checks the
/// downloaded `SHA256SUMS` against `sums_sha256`, then each archive against
/// `SHA256SUMS`, so a release asset replaced after pinning is rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleasePin {
    /// `vMAJOR.MINOR.PATCH`, optionally with a `-pre.release` suffix.
    pub tag: String,
    /// 64 lowercase hex characters.
    pub sums_sha256: String,
}

impl ReleasePin {
    /// Both values are interpolated into the POSIX and PowerShell installer
    /// scripts, so they are held to a strict shape rather than escaped.
    ///
    /// # Errors
    ///
    /// Returns an error if `tag` is not a semver release tag or
    /// `sums_sha256` is not 64 lowercase hex characters.
    pub fn new(tag: &str, sums_sha256: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            is_release_tag(tag),
            "release tag {tag:?} is not vMAJOR.MINOR.PATCH[-pre]"
        );
        anyhow::ensure!(
            sums_sha256.len() == 64
                && sums_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "SHA256SUMS digest {sums_sha256:?} is not 64 lowercase hex characters"
        );
        Ok(Self {
            tag: tag.to_owned(),
            sums_sha256: sums_sha256.to_owned(),
        })
    }

    fn from_env(tag_var: &str, sums_var: &str) -> anyhow::Result<Self> {
        Self::new(&env(tag_var)?, &env(sums_var)?)
            .map_err(|e| anyhow::anyhow!("{tag_var}/{sums_var}: {e}"))
    }
}

fn is_release_tag(tag: &str) -> bool {
    let Some(version) = tag.strip_prefix('v') else {
        return false;
    };
    let (core, pre) = match version.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (version, None),
    };
    let parts: Vec<&str> = core.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
        && pre.is_none_or(|p| {
            !p.is_empty()
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
        })
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
    /// The `decdn/decdn` release the installers install `decdn` from.
    pub decdn_release: ReleasePin,
    /// The `decdn/sponsord` release the installers install `decdn-sponsored`
    /// from.
    pub wrapper_release: ReleasePin,
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
            decdn_release: ReleasePin::from_env(
                "SPONSOR_DECDN_RELEASE",
                "SPONSOR_DECDN_SUMS_SHA256",
            )?,
            wrapper_release: ReleasePin::from_env(
                "SPONSOR_WRAPPER_RELEASE",
                "SPONSOR_WRAPPER_SUMS_SHA256",
            )?,
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
            std::env::set_var("SPONSOR_DECDN_RELEASE", "v0.1.0");
            std::env::set_var("SPONSOR_DECDN_SUMS_SHA256", "ab".repeat(32));
            std::env::set_var("SPONSOR_WRAPPER_RELEASE", "v0.2.0-rc.1");
            std::env::set_var("SPONSOR_WRAPPER_SUMS_SHA256", "cd".repeat(32));
            std::env::remove_var("SPONSOR_CHAIN_ID");
            std::env::remove_var("SPONSOR_CAPABILITY_CAP_MICRO_USDC");
            std::env::remove_var("SPONSOR_CAPABILITY_TTL_SECS");
        }
        let cfg = ServerConfig::from_env().unwrap();
        assert_eq!(cfg.chain_id, 421_614);
        assert_eq!(cfg.capability_cap.0, 5_000_000);
        assert_eq!(cfg.capability_ttl_secs, 172_800);
        assert_eq!(cfg.pool_low_water.0, 20_000_000);
        assert_eq!(cfg.decdn_release.tag, "v0.1.0");
        assert_eq!(cfg.wrapper_release.tag, "v0.2.0-rc.1");
    }

    #[test]
    fn release_pin_accepts_semver_tags_and_hex_digests() {
        let digest = "0123456789abcdef".repeat(4);
        for tag in ["v0.1.0", "v10.20.30", "v1.0.0-rc.1", "v1.0.0-beta-2"] {
            assert!(ReleasePin::new(tag, &digest).is_ok(), "{tag}");
        }
    }

    #[test]
    fn release_pin_rejects_anything_a_script_could_misread() {
        let digest = "ab".repeat(32);
        for tag in [
            "0.1.0",
            "v0.1",
            "v0.1.0.1",
            "v0..0",
            "v0.1.0-",
            "v0.1.0 ",
            "v0.1.0;rm",
            "v0.1.0$(id)",
            "v0.1.0'",
        ] {
            assert!(ReleasePin::new(tag, &digest).is_err(), "{tag:?}");
        }
        for bad in [&"AB".repeat(32), &"ab".repeat(31), &"zz".repeat(32)] {
            assert!(ReleasePin::new("v0.1.0", bad).is_err(), "{bad}");
        }
    }
}
