//! Wrapper configuration: loaded from the installer-written profile file
//! (`~/.decdn/sponsor.toml`) plus one environment variable for the keystore
//! password (never written to disk).
//!
//! Field names here are the contract Task 18's `decdn.sh` installer must
//! write to `~/.decdn/sponsor.toml` — keep them in sync if either side
//! changes.

use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use serde::Deserialize;

/// Env var holding the keystore decryption password. Never written to the
/// profile file on disk.
const PASSWORD_ENV: &str = "DECDN_KEYSTORE_PASSWORD";

/// Default location of the installer-written profile, relative to `$HOME`.
const DEFAULT_PROFILE_REL: &str = ".decdn/sponsor.toml";

/// On-disk shape of `~/.decdn/sponsor.toml`, written by the Task 18
/// installer. Everything except the keystore password (kept out of the
/// file, supplied via `DECDN_KEYSTORE_PASSWORD`) lives here.
#[derive(Debug, Clone, Deserialize)]
struct Profile {
    gateway_base: String,
    keystore_path: PathBuf,
    decdn_bin: String,
    data_dir: PathBuf,
    rpc_url: String,
    payment_channel: Address,
    capacity_bond: Option<Address>,
    slash_judge: Option<Address>,
    chain_id: u64,
}

/// Fully resolved wrapper configuration: the on-disk profile plus the
/// password from the environment.
#[derive(Debug, Clone)]
pub struct WrapperConfig {
    pub gateway_base: String,
    pub keystore_path: PathBuf,
    pub keystore_password: String,
    pub decdn_bin: String,
    pub data_dir: PathBuf,
    pub rpc_url: String,
    pub payment_channel: Address,
    pub capacity_bond: Option<Address>,
    pub slash_judge: Option<Address>,
    pub chain_id: u64,
}

/// Expand a leading `~` (or `~/...`) to `$HOME`. Any other path (including
/// one with no leading `~`) is returned unchanged.
///
/// # Errors
///
/// Returns an error if the path starts with `~` but `$HOME` isn't set.
fn expand_home(path: &Path) -> anyhow::Result<PathBuf> {
    let Some(s) = path.to_str() else {
        return Ok(path.to_path_buf());
    };
    if s == "~" || s.starts_with("~/") {
        let home = std::env::var("HOME")
            .map_err(|_| anyhow::anyhow!("path {s} starts with ~ but $HOME is not set"))?;
        let rest = s.strip_prefix('~').unwrap_or(s);
        let rest = rest.strip_prefix('/').unwrap_or(rest);
        return Ok(PathBuf::from(home).join(rest));
    }
    Ok(path.to_path_buf())
}

impl WrapperConfig {
    /// Load the profile written by the Task 18 installer at
    /// `~/.decdn/sponsor.toml` (path overridable via `DECDN_SPONSOR_PROFILE`
    /// for tests/dev), and pull the keystore password from
    /// `DECDN_KEYSTORE_PASSWORD`.
    ///
    /// # Errors
    ///
    /// Returns an error if `$HOME` can't be resolved, the profile file
    /// can't be read or parsed, or `DECDN_KEYSTORE_PASSWORD` isn't set.
    pub fn load() -> anyhow::Result<Self> {
        let profile_path = match std::env::var("DECDN_SPONSOR_PROFILE") {
            Ok(p) => PathBuf::from(p),
            Err(_) => {
                let home = std::env::var("HOME")
                    .map_err(|_| anyhow::anyhow!("$HOME is not set; cannot locate sponsor.toml"))?;
                PathBuf::from(home).join(DEFAULT_PROFILE_REL)
            }
        };
        let text = std::fs::read_to_string(&profile_path).map_err(|e| {
            anyhow::anyhow!("failed to read profile {}: {e}", profile_path.display())
        })?;
        Self::from_toml_str(&text)
    }

    /// Parse a profile from an in-memory TOML string (used by `load` and
    /// directly by tests, to avoid touching the real filesystem/`$HOME`).
    ///
    /// # Errors
    ///
    /// Returns an error if the TOML doesn't parse into `Profile`, a `~`
    /// path can't be expanded, or `DECDN_KEYSTORE_PASSWORD` isn't set.
    pub fn from_toml_str(text: &str) -> anyhow::Result<Self> {
        let profile: Profile = toml::from_str(text)?;
        let keystore_password = std::env::var(PASSWORD_ENV)
            .map_err(|_| anyhow::anyhow!("missing env {PASSWORD_ENV}"))?;
        Ok(Self {
            gateway_base: profile.gateway_base,
            keystore_path: expand_home(&profile.keystore_path)?,
            keystore_password,
            decdn_bin: profile.decdn_bin,
            data_dir: expand_home(&profile.data_dir)?,
            rpc_url: profile.rpc_url,
            payment_channel: profile.payment_channel,
            capacity_bond: profile.capacity_bond,
            slash_judge: profile.slash_judge,
            chain_id: profile.chain_id,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serial_test::serial;

    const SAMPLE: &str = r#"
        gateway_base = "https://sponsor.example.com"
        keystore_path = "~/.decdn/keystore.json"
        decdn_bin = "decdn"
        data_dir = "~/.decdn/data"
        rpc_url = "https://sepolia-rollup.arbitrum.io/rpc"
        payment_channel = "0x0000000000000000000000000000000000000001"
        capacity_bond = "0x0000000000000000000000000000000000000002"
        chain_id = 421614
    "#;

    #[test]
    #[serial]
    fn parses_profile_and_expands_home() {
        unsafe {
            std::env::set_var("DECDN_KEYSTORE_PASSWORD", "pw");
            std::env::set_var("HOME", "/home/testuser");
        }
        let cfg = WrapperConfig::from_toml_str(SAMPLE).unwrap();
        assert_eq!(cfg.gateway_base, "https://sponsor.example.com");
        assert_eq!(
            cfg.keystore_path,
            PathBuf::from("/home/testuser/.decdn/keystore.json")
        );
        assert_eq!(cfg.data_dir, PathBuf::from("/home/testuser/.decdn/data"));
        assert_eq!(cfg.keystore_password, "pw");
        assert_eq!(cfg.chain_id, 421_614);
        assert!(cfg.slash_judge.is_none());
        unsafe {
            std::env::remove_var("DECDN_KEYSTORE_PASSWORD");
        }
    }

    #[test]
    #[serial]
    fn missing_password_env_errors() {
        unsafe {
            std::env::remove_var("DECDN_KEYSTORE_PASSWORD");
        }
        let res = WrapperConfig::from_toml_str(SAMPLE);
        assert!(res.is_err());
    }
}
