//! CLI configuration, loaded from the installer-written file
//! (`~/.decdn/sponsor.toml`), or, when `DECDN_SPONSOR_ONRAMP_URL` is set,
//! from the environment, for where there is no installer (the container
//! image). It only says which onramp to use and where `decdn` is; the chain
//! and contracts come from the onramp's `/v1/profile` on every run.
//!
//! Field names here are the contract the installers (`decdn.sh`,
//! `decdn.ps1`) write: keep them in sync. Unknown fields are ignored. The
//! environment variable names are a contract with anyone who sets them, the
//! image's users among them.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Default location of the installer-written file, relative to the home
/// directory.
const DEFAULT_FILE_REL: &str = ".decdn/sponsor.toml";

/// Default root for per-download state, relative to the home directory.
const DEFAULT_DATA_DIR_REL: &str = ".decdn/sponsored";

/// Set, it configures the CLI from the environment and the file is not read.
/// An empty value counts as unset, for every variable here: compose and CI
/// templates write `VAR=` for a value they do not have.
const ENV_ONRAMP_URL: &str = "DECDN_SPONSOR_ONRAMP_URL";

/// With [`ENV_ONRAMP_URL`]: the `decdn` binary, [`DEFAULT_DECDN_BIN`] if unset.
const ENV_DECDN_BIN: &str = "DECDN_SPONSOR_DECDN_BIN";

/// With [`ENV_ONRAMP_URL`]: the state root (`~` expanded),
/// `~/`[`DEFAULT_DATA_DIR_REL`] if unset.
const ENV_DATA_DIR: &str = "DECDN_SPONSOR_DATA_DIR";

/// Without [`ENV_ONRAMP_URL`]: the file to read instead of
/// `~/`[`DEFAULT_FILE_REL`], for tests and development.
const ENV_PROFILE: &str = "DECDN_SPONSOR_PROFILE";

/// The `decdn` binary when the environment names none: looked up on `PATH`.
const DEFAULT_DECDN_BIN: &str = "decdn";

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

/// `name` through `var`, with an empty value read as unset.
fn nonempty(
    var: impl Fn(&str) -> anyhow::Result<Option<String>>,
    name: &str,
) -> anyhow::Result<Option<String>> {
    Ok(var(name)?.filter(|v| !v.is_empty()))
}

/// A variable of the process environment: `None` when unset, and an error,
/// never a silent `None`, when it is set but not UTF-8.
///
/// # Errors
///
/// Returns an error if the variable is set to a value that is not UTF-8.
fn process_var(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(anyhow::anyhow!("{name} is set but is not valid UTF-8"))
        }
    }
}

impl Config {
    /// Load the configuration: from the environment when
    /// `DECDN_SPONSOR_ONRAMP_URL` is set (see `from_env`), else from
    /// `~/.decdn/sponsor.toml` (path overridable via `DECDN_SPONSOR_PROFILE`
    /// for tests/dev).
    ///
    /// # Errors
    ///
    /// Returns an error if a variable is not UTF-8, the home directory can't
    /// be resolved, or the file can't be read or parsed.
    pub fn load() -> anyhow::Result<Self> {
        Self::load_with(process_var)
    }

    /// [`Config::load`] over the lookup `var` instead of the process
    /// environment, so tests need not mutate it.
    fn load_with(var: impl Fn(&str) -> anyhow::Result<Option<String>>) -> anyhow::Result<Self> {
        if let Some(cfg) = Self::from_env(&var)? {
            return Ok(cfg);
        }
        let path = match nonempty(&var, ENV_PROFILE)? {
            Some(p) => PathBuf::from(p),
            None => home()?.join(DEFAULT_FILE_REL),
        };
        let text = std::fs::read_to_string(&path).map_err(|e| {
            anyhow::anyhow!(
                "failed to read {}: {e}. Run the onramp's installer to set it up, \
                 or set {ENV_ONRAMP_URL}.",
                path.display()
            )
        })?;
        Self::from_toml_str(&text)
    }

    /// The configuration `var` describes, or `None` when it has no
    /// `DECDN_SPONSOR_ONRAMP_URL`. `DECDN_SPONSOR_DECDN_BIN` defaults to
    /// `decdn` on `PATH`, and `DECDN_SPONSOR_DATA_DIR` (`~` expanded) to
    /// `~/.decdn/sponsored`. The other two are read only alongside the URL;
    /// the file has its own fields for them.
    ///
    /// # Errors
    ///
    /// Returns an error if `var` fails or a home directory is needed and
    /// can't be resolved.
    fn from_env(
        var: impl Fn(&str) -> anyhow::Result<Option<String>>,
    ) -> anyhow::Result<Option<Self>> {
        let Some(onramp_url) = nonempty(&var, ENV_ONRAMP_URL)? else {
            return Ok(None);
        };
        let data_dir = match nonempty(&var, ENV_DATA_DIR)? {
            Some(dir) => expand_home(Path::new(&dir))?,
            None => home()?.join(DEFAULT_DATA_DIR_REL),
        };
        Ok(Some(Self {
            onramp_url,
            decdn_bin: nonempty(&var, ENV_DECDN_BIN)?
                .unwrap_or_else(|| DEFAULT_DECDN_BIN.to_owned()),
            data_dir,
        }))
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
