use std::{net::SocketAddr, path::PathBuf, str::FromStr};

use alloy::primitives::Address;

use crate::daemon::DaemonInfo;

fn env(k: &str) -> anyhow::Result<String> {
    std::env::var(k).map_err(|_| anyhow::anyhow!("missing env {k}"))
}

fn env_opt_u64(k: &str) -> anyhow::Result<Option<u64>> {
    match std::env::var(k) {
        Ok(v) => v.parse().map(Some).map_err(|e| anyhow::anyhow!("{k}: {e}")),
        Err(_) => Ok(None),
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

pub struct OnrampConfig {
    pub bind: SocketAddr,
    /// This onramp's own public base URL, baked into the installers.
    pub public_url: String,
    pub daemon_url: String,
    pub daemon_token: String,
    /// Public RPC URL baked into the installers for end users.
    pub rpc_url: String,
    pub capacity_bond: Address,
    /// Cap requested for each capability; `None` takes the daemon maximum.
    pub spending_cap: Option<u64>,
    /// TTL requested for each capability; `None` takes the daemon maximum.
    pub ttl_secs: Option<u64>,
    pub turnstile_secret: String,
    pub turnstile_sitekey: String,
    pub data_dir: PathBuf,
    /// The `decdn/decdn` release the installers install `decdn` from.
    pub decdn_release: ReleasePin,
    /// The `decdn/sponsord` release the installers install `decdn-sponsored`
    /// from.
    pub wrapper_release: ReleasePin,
}

impl OnrampConfig {
    /// # Errors
    ///
    /// A required variable is missing or malformed.
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            bind: SocketAddr::from_str(
                &std::env::var("ONRAMP_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into()),
            )
            .map_err(|e| anyhow::anyhow!("ONRAMP_BIND: {e}"))?,
            public_url: std::env::var("ONRAMP_PUBLIC_URL")
                .unwrap_or_else(|_| "https://up.decdn.org".into()),
            daemon_url: std::env::var("ONRAMP_DAEMON_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8090".into()),
            daemon_token: env("ONRAMP_DAEMON_TOKEN")?,
            rpc_url: env("ONRAMP_RPC_URL")?,
            capacity_bond: Address::from_str(&env("ONRAMP_CAPACITY_BOND_ADDR")?)
                .map_err(|e| anyhow::anyhow!("ONRAMP_CAPACITY_BOND_ADDR: {e}"))?,
            spending_cap: env_opt_u64("ONRAMP_SPENDING_CAP_MICRO_USDC")?,
            ttl_secs: env_opt_u64("ONRAMP_TTL_SECS")?,
            turnstile_secret: env("ONRAMP_TURNSTILE_SECRET")?,
            turnstile_sitekey: env("ONRAMP_TURNSTILE_SITEKEY")?,
            data_dir: PathBuf::from(
                std::env::var("ONRAMP_DATA_DIR").unwrap_or_else(|_| "./data".into()),
            ),
            decdn_release: ReleasePin::from_env(
                "ONRAMP_DECDN_RELEASE",
                "ONRAMP_DECDN_SUMS_SHA256",
            )?,
            wrapper_release: ReleasePin::from_env(
                "ONRAMP_WRAPPER_RELEASE",
                "ONRAMP_WRAPPER_SUMS_SHA256",
            )?,
        })
    }

    /// Refuse requested terms the daemon would reject on every request.
    ///
    /// # Errors
    ///
    /// A configured cap or TTL is zero or above the daemon's maximum.
    pub fn check_against(&self, info: &DaemonInfo) -> anyhow::Result<()> {
        check_term(
            "ONRAMP_SPENDING_CAP_MICRO_USDC",
            self.spending_cap,
            info.max_spending_cap,
        )?;
        check_term("ONRAMP_TTL_SECS", self.ttl_secs, info.max_ttl_secs)
    }
}

fn check_term(name: &str, value: Option<u64>, max: u64) -> anyhow::Result<()> {
    if let Some(v) = value {
        anyhow::ensure!(v > 0, "{name} must be above zero");
        anyhow::ensure!(v <= max, "{name} {v} exceeds the daemon maximum {max}");
    }
    Ok(())
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
            std::env::set_var("ONRAMP_DAEMON_TOKEN", "t".repeat(32));
            std::env::set_var("ONRAMP_RPC_URL", "https://rpc.example");
            std::env::set_var(
                "ONRAMP_CAPACITY_BOND_ADDR",
                "0x0000000000000000000000000000000000000002",
            );
            std::env::set_var("ONRAMP_TURNSTILE_SECRET", "s");
            std::env::set_var("ONRAMP_TURNSTILE_SITEKEY", "k");
            std::env::set_var("ONRAMP_DECDN_RELEASE", "v0.1.0");
            std::env::set_var("ONRAMP_DECDN_SUMS_SHA256", "ab".repeat(32));
            std::env::set_var("ONRAMP_WRAPPER_RELEASE", "v0.2.0-rc.1");
            std::env::set_var("ONRAMP_WRAPPER_SUMS_SHA256", "cd".repeat(32));
            for k in [
                "ONRAMP_BIND",
                "ONRAMP_DAEMON_URL",
                "ONRAMP_SPENDING_CAP_MICRO_USDC",
                "ONRAMP_TTL_SECS",
            ] {
                std::env::remove_var(k);
            }
        }
        let cfg = OnrampConfig::from_env().unwrap();
        assert_eq!(cfg.bind, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(cfg.daemon_url, "http://127.0.0.1:8090");
        assert_eq!(cfg.spending_cap, None);
        assert_eq!(cfg.ttl_secs, None);
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
