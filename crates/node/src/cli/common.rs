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
/// "no home" branch). Production builds always go straight to
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
