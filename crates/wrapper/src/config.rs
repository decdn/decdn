//! Wrapper configuration, loaded from the installer-written profile file
//! (`~/.decdn/sponsor.toml`).
//!
//! Field names here are the contract the `decdn.sh` installer writes — keep
//! them in sync if either side changes. Unknown fields are ignored.

use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use serde::Deserialize;

/// Default location of the installer-written profile, relative to `$HOME`.
const DEFAULT_PROFILE_REL: &str = ".decdn/sponsor.toml";

/// On-disk shape of `~/.decdn/sponsor.toml`.
#[derive(Debug, Clone, Deserialize)]
struct Profile {
    gateway_base: String,
    decdn_bin: String,
    data_dir: PathBuf,
    rpc_url: String,
    payment_pool: Address,
    capacity_bond: Option<Address>,
    slash_judge: Option<Address>,
    chain_id: u64,
}

/// Fully resolved wrapper configuration.
#[derive(Debug, Clone)]
pub struct WrapperConfig {
    pub gateway_base: String,
    pub decdn_bin: String,
    /// Root for per-download state (`<data_dir>/downloads/<hash>/`).
    pub data_dir: PathBuf,
    pub rpc_url: String,
    pub payment_pool: Address,
    pub capacity_bond: Option<Address>,
    pub slash_judge: Option<Address>,
    pub chain_id: u64,
}

/// The user's home directory: `$HOME` on Unix, the profile folder
/// (`%USERPROFILE%`) on Windows.
///
/// # Errors
///
/// Returns an error if the platform reports no home directory.
fn home() -> anyhow::Result<PathBuf> {
    std::env::home_dir().ok_or_else(|| anyhow::anyhow!("cannot determine the home directory"))
}

/// Expand a leading `~` (or `~/...`) to the home directory. Any other path
/// (including one with no leading `~`) is returned unchanged.
///
/// # Errors
///
/// Returns an error if the path starts with `~` but there is no home
/// directory.
fn expand_home(path: &Path) -> anyhow::Result<PathBuf> {
    let Some(s) = path.to_str() else {
        return Ok(path.to_path_buf());
    };
    if s == "~" || s.starts_with("~/") {
        let home = home()?;
        let rest = s.strip_prefix('~').unwrap_or(s);
        let rest = rest.strip_prefix('/').unwrap_or(rest);
        return Ok(home.join(rest));
    }
    Ok(path.to_path_buf())
}

impl WrapperConfig {
    /// Load the installer-written profile at `~/.decdn/sponsor.toml` (path
    /// overridable via `DECDN_SPONSOR_PROFILE` for tests/dev).
    ///
    /// # Errors
    ///
    /// Returns an error if the home directory can't be resolved or the profile file
    /// can't be read or parsed.
    pub fn load() -> anyhow::Result<Self> {
        let profile_path = match std::env::var("DECDN_SPONSOR_PROFILE") {
            Ok(p) => PathBuf::from(p),
            Err(_) => home()?.join(DEFAULT_PROFILE_REL),
        };
        let text = std::fs::read_to_string(&profile_path).map_err(|e| {
            anyhow::anyhow!("failed to read profile {}: {e}", profile_path.display())
        })?;
        Self::from_toml_str(&text)
    }

    /// Parse a profile from an in-memory TOML string.
    ///
    /// # Errors
    ///
    /// Returns an error if the TOML doesn't parse into `Profile` or a `~`
    /// path can't be expanded.
    pub fn from_toml_str(text: &str) -> anyhow::Result<Self> {
        let profile: Profile = toml::from_str(text)?;
        Ok(Self {
            gateway_base: profile.gateway_base,
            decdn_bin: profile.decdn_bin,
            data_dir: expand_home(&profile.data_dir)?,
            rpc_url: profile.rpc_url,
            payment_pool: profile.payment_pool,
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

    const SAMPLE: &str = r#"
        gateway_base = "https://gateway.example.com"
        decdn_bin = "decdn"
        data_dir = "~/.decdn/sponsored"
        rpc_url = "https://sepolia-rollup.arbitrum.io/rpc"
        payment_pool = "0x0000000000000000000000000000000000000001"
        capacity_bond = "0x0000000000000000000000000000000000000002"
        chain_id = 421614
    "#;

    /// Compared against the platform's own home directory (`$HOME` on Unix,
    /// the profile folder on Windows) rather than a faked `HOME`, which
    /// Windows does not consult.
    #[test]
    fn parses_profile_and_expands_home() {
        let cfg = WrapperConfig::from_toml_str(SAMPLE).unwrap();
        assert_eq!(cfg.gateway_base, "https://gateway.example.com");
        assert_eq!(cfg.data_dir, home().unwrap().join(".decdn/sponsored"));
        assert_eq!(cfg.chain_id, 421_614);
        assert!(cfg.slash_judge.is_none());
    }

    #[test]
    fn ignores_unknown_fields() {
        let with_extra = format!("{SAMPLE}\nkeystore_path = \"~/.decdn/client/keystore.json\"\n");
        assert!(WrapperConfig::from_toml_str(&with_extra).is_ok());
    }
}
