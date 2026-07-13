//! Shared CLI types and helpers.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Log output format.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable log lines.
    #[default]
    Pretty,
    /// Structured JSON (one object per line).
    Json,
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pretty => f.write_str("pretty"),
            Self::Json => f.write_str("json"),
        }
    }
}

/// Blockchain coordinates + keys shared by every on-chain command, flattened
/// into each command's args so they expose an identical flag group and can't
/// drift. Each field is taken from a flag when present, otherwise the
/// `[blockchain]` / `[identity]` tables of the TOML config (the same file the
/// daemon reads). `ChainArgs` / `PublishChainArgs` layer their contract-address
/// flags on top of this; the shared resolver (`chain_ctx::resolve_common`)
/// reads these fields so the flag > config > default precedence lives in one
/// place.
#[derive(clap::Args, Debug)]
pub struct CommonChainArgs {
    /// Path to the TOML config file supplying `[blockchain]` / `[identity]`
    /// fields not passed as flags. Takes precedence over the top-level
    /// `decdn --config`; falls through to `~/.decdn/node.toml`.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// JSON-RPC endpoint URL. Overrides `blockchain.rpc_url`.
    #[arg(long, value_name = "URL")]
    pub rpc_url: Option<String>,

    /// EIP-712 `chainId` for signing domains. Overrides `blockchain.chain_id`;
    /// defaults to Arbitrum Sepolia. Must match the target contract
    /// deployment's chain or signatures are rejected on-chain. The `publish`
    /// and `setup` commands additionally read the RPC's own chain id and refuse
    /// to submit on a mismatch; the other on-chain commands do not.
    #[arg(long, value_name = "ID")]
    pub chain_id: Option<u64>,

    /// Ethereum keystore file. Overrides `blockchain.eth_keystore`; defaults
    /// to `<data_dir>/keystore.json`.
    #[arg(long, value_name = "PATH")]
    pub keystore: Option<PathBuf>,

    /// Data directory holding `node.secret`. Overrides `identity.data_dir`;
    /// defaults to `~/.decdn`.
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// File whose contents are the keystore password. Consulted after the
    /// `DECDN_KEYSTORE_PASSWORD` env var and before an interactive prompt.
    #[arg(long, value_name = "PATH", env = "DECDN_KEYSTORE_PASSWORD_FILE")]
    pub keystore_password_file: Option<PathBuf>,

    /// Build and print what would be submitted without sending any
    /// transaction.
    #[arg(long)]
    pub dry_run: bool,

    /// Emit the result as JSON instead of human-readable `key=value` lines.
    #[arg(long)]
    pub json: bool,
}

/// Log verbosity level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// Most verbose: all trace spans and events.
    Trace,
    /// Debug-level diagnostics.
    Debug,
    /// Standard operational information.
    #[default]
    Info,
    /// Warnings only.
    Warn,
    /// Errors only.
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        };
        f.write_str(s)
    }
}

/// Returns the platform-appropriate default data directory (`~/.decdn`).
///
/// Returns `None` if the home directory cannot be determined.
pub fn default_data_dir() -> Option<PathBuf> {
    resolve_home().map(|h| h.join(".decdn"))
}

/// Returns the default config file path (`~/.decdn/node.toml`).
///
/// Returns `None` if the home directory cannot be determined.
pub fn default_config_path() -> Option<PathBuf> {
    default_data_dir().map(|d| d.join("node.toml"))
}

/// Returns the client-scoped default data directory (`~/.decdn/client`).
///
/// The `decdn` client commands (`fetch`, `bundle pull`, `channel coop-close`)
/// keep their spending keystore and buyer-channel store here rather than in the
/// node-shaped `~/.decdn`, so a pure client install does not masquerade as a
/// node. An explicit `--data-dir` / `identity.data_dir` still wins.
///
/// Returns `None` if the home directory cannot be determined.
pub fn default_client_data_dir() -> Option<PathBuf> {
    default_data_dir().map(|d| d.join("client"))
}

/// Whether a TOML config path was chosen by the operator or
/// defaulted. Drives the "missing file" policy for config-file
/// lookups in the `decdn node *` subcommands: an explicit path that
/// isn't there is almost certainly a typo and should error, while a
/// default path that isn't there is normal (operator just hasn't
/// made a config yet).
#[derive(Debug, Clone, Copy)]
pub enum ConfigPathSource {
    /// Path came from `--config` (subcommand) or the global
    /// `decdn --config` flag.
    Explicit,
    /// Path came from the built-in default (`~/.decdn/node.toml`).
    Default,
}

/// Expands a leading `~/` or bare `~` in a path to the user's home directory.
///
/// Logs a warning and returns the path unchanged if `~` is present but
/// the home directory cannot be determined (e.g. in minimal containers).
/// Does not handle `~username` syntax.
pub fn expand_tilde(path: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = resolve_home() {
            return home.join(rest);
        }
        tracing::warn!(path = %s, "cannot expand '~': home directory not available");
    } else if s == "~" {
        if let Some(home) = resolve_home() {
            return home;
        }
        tracing::warn!("cannot expand '~': home directory not available");
    }
    path.to_path_buf()
}

/// Resolve the user's home directory.
///
/// Wraps `dirs::home_dir()`, with a `#[cfg(test)]` thread-local seam that
/// lets unit tests inject a deterministic home (or explicitly the
/// "no home" branch). Non-test builds always go straight to
/// `dirs::home_dir()`. See `test_support::with_home_override` (only
/// compiled under `#[cfg(test)]`, so a rustdoc intra-doc link would
/// fail to resolve in regular `cargo doc` builds).
fn resolve_home() -> Option<PathBuf> {
    #[cfg(test)]
    if let Some(o) = test_support::current_home_override() {
        return o.into_inner();
    }
    dirs::home_dir()
}

/// Test-only helpers for overriding the home-directory lookup.
///
/// These exist because both branches of `dirs::home_dir()` must be tested
/// deterministically — and edition-2024 marks `std::env::set_var` as
/// `unsafe`, which the workspace forbids. The override lives in a
/// thread-local so concurrent test harness threads don't race.
#[cfg(test)]
pub(crate) mod test_support {
    use std::cell::RefCell;
    use std::path::{Path, PathBuf};

    /// Test-only override value. Distinguishes "no override active"
    /// (caller should use `dirs::home_dir()`) from "override set to
    /// no-home" (caller should treat as if the home directory is
    /// unavailable). A `clippy::option_option` waiver in disguise.
    #[derive(Clone)]
    pub(crate) struct HomeOverride(Option<PathBuf>);

    impl HomeOverride {
        pub(crate) fn into_inner(self) -> Option<PathBuf> {
            self.0
        }
    }

    thread_local! {
        static HOME_OVERRIDE: RefCell<Option<HomeOverride>> = const { RefCell::new(None) };
    }

    pub(crate) fn current_home_override() -> Option<HomeOverride> {
        HOME_OVERRIDE.with(|c| c.borrow().clone())
    }

    /// Run `f` with `home` standing in for `dirs::home_dir()` on the
    /// current thread. Restores the previous override on return, even
    /// if `f` panics.
    pub fn with_home_override<R>(home: Option<&Path>, f: impl FnOnce() -> R) -> R {
        struct Guard(Option<HomeOverride>);
        impl Drop for Guard {
            fn drop(&mut self) {
                let prev = self.0.take();
                HOME_OVERRIDE.with(|c| *c.borrow_mut() = prev);
            }
        }
        let next = HomeOverride(home.map(Path::to_path_buf));
        let prev = HOME_OVERRIDE.with(|c| c.replace(Some(next)));
        let _g = Guard(prev);
        f()
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::{default_client_data_dir, default_data_dir};
    use std::ffi::OsStr;

    #[test]
    fn client_data_dir_is_client_subdir_of_data_dir() {
        // Home-independent: whatever the base data dir resolves to (or `None`),
        // the client dir is its `client` subdirectory. Guards against a silent
        // revert to the node-shaped data dir or a wrong subdir name.
        match (default_data_dir(), default_client_data_dir()) {
            (Some(base), Some(client)) => {
                assert_eq!(client.file_name(), Some(OsStr::new("client")));
                assert_eq!(client.parent(), Some(base.as_path()));
            }
            (None, None) => {} // no home available; both absent, consistent
            other => panic!("data-dir/client-dir availability mismatch: {other:?}"),
        }
    }
}
