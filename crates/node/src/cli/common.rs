//! Shared CLI types and helpers.

use std::fmt;
use std::path::PathBuf;

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
    dirs::home_dir().map(|h| h.join(".decdn"))
}

/// Returns the default config file path (`~/.decdn/node.toml`).
///
/// Returns `None` if the home directory cannot be determined.
pub fn default_config_path() -> Option<PathBuf> {
    default_data_dir().map(|d| d.join("node.toml"))
}

/// Expands a leading `~` in a path to the user's home directory.
///
/// Returns the path unchanged if it does not start with `~` or if the
/// home directory cannot be determined.
pub fn expand_tilde(path: &std::path::Path) -> PathBuf {
    let s = path.to_string_lossy();
    if s.starts_with('~') {
        if let Some(home) = dirs::home_dir() {
            return home.join(
                s.strip_prefix("~/")
                    .unwrap_or(s.strip_prefix('~').unwrap_or(&s)),
            );
        }
    }
    path.to_path_buf()
}
