//! CLI configuration, loaded from the installer-written file
//! (`~/.decdn/sponsor.toml`). It only says which onramp to use and where
//! `decdn` is; the chain and contracts come from the onramp's `/v1/profile`
//! on every run.
//!
//! Field names here are the contract the installers (`decdn.sh`,
//! `decdn.ps1`) write: keep them in sync. Unknown fields are ignored.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Default location of the installer-written file, relative to the home
/// directory.
const DEFAULT_FILE_REL: &str = ".decdn/sponsor.toml";

/// Default root for per-download state, relative to the home directory.
const DEFAULT_DATA_DIR_REL: &str = ".decdn/sponsored";

/// On-disk shape of `~/.decdn/sponsor.toml`.
#[derive(Debug, Clone, Deserialize)]
struct File {
    onramp_url: String,
    decdn_bin: String,
    data_dir: Option<PathBuf>,
}

/// Fully resolved CLI configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// The onramp's public base URL.
    pub onramp_url: String,
    /// The `decdn` binary to run: a path, or a name looked up on `PATH`.
    pub decdn_bin: String,
    /// Root for per-download state (`<data_dir>/downloads/<hash>/`).
    pub data_dir: PathBuf,
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

impl Config {
    /// Load `~/.decdn/sponsor.toml` (path overridable via
    /// `DECDN_SPONSOR_PROFILE` for tests/dev).
    ///
    /// # Errors
    ///
    /// Returns an error if the home directory can't be resolved or the file
    /// can't be read or parsed.
    pub fn load() -> anyhow::Result<Self> {
        let path = match std::env::var("DECDN_SPONSOR_PROFILE") {
            Ok(p) => PathBuf::from(p),
            Err(_) => home()?.join(DEFAULT_FILE_REL),
        };
        let text = std::fs::read_to_string(&path).map_err(|e| {
            anyhow::anyhow!(
                "failed to read {}: {e}. Run the onramp's installer to set it up.",
                path.display()
            )
        })?;
        Self::from_toml_str(&text)
    }

    /// Parse the file from an in-memory TOML string.
    ///
    /// # Errors
    ///
    /// Returns an error if the TOML doesn't parse or a `~` path can't be
    /// expanded.
    pub fn from_toml_str(text: &str) -> anyhow::Result<Self> {
        let file: File = toml::from_str(text)?;
        let data_dir = match file.data_dir {
            Some(dir) => expand_home(&dir)?,
            None => home()?.join(DEFAULT_DATA_DIR_REL),
        };
        Ok(Self {
            onramp_url: file.onramp_url,
            decdn_bin: file.decdn_bin,
            data_dir,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
