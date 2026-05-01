//! Arguments for the `decdn node` subcommand group.
//!
//! `node` is the operator-local admin namespace: subcommands talk to the
//! loopback admin HTTP surface (ADR 025) exposed by a running node.

use std::path::PathBuf;

use clap::{Args, Subcommand};

/// Operator-local admin commands that query a running deCDN node.
#[derive(Args, Debug)]
pub struct NodeArgs {
    #[command(subcommand)]
    pub cmd: NodeCommand,
}

/// Subcommands under `decdn node`.
#[derive(Subcommand, Debug)]
pub enum NodeCommand {
    /// List gossip peers currently known to a running node.
    Peers(PeersArgs),
    /// Print a running node's identity and process uptime.
    Health(HealthArgs),
}

/// `decdn node health` — report identity (hex `node_id`) and process
/// uptime via `admin_v1_health`.
#[derive(Args, Debug)]
pub struct HealthArgs {
    /// Base URL of the node's admin HTTP surface.
    ///
    /// Also read from `DECDN_ADMIN_URL` when unset; clap folds the env
    /// var into this field. If still unset, the admin port is derived
    /// from `observability.admin_port` in the config file (see
    /// `--config`). Example: `http://127.0.0.1:9191`.
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Path to the TOML config file used to derive the admin URL when
    /// `--admin-url` / `DECDN_ADMIN_URL` are unset. Takes precedence
    /// over the top-level `decdn --config`; if neither is set,
    /// resolution falls through to `~/.decdn/node.toml` and then the
    /// built-in default port.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Emit the admin response body as JSON instead of two human-
    /// readable lines.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}

/// `decdn node peers` — list the gossip peer table of a running node.
#[derive(Args, Debug)]
pub struct PeersArgs {
    /// Base URL of the node's admin HTTP surface.
    ///
    /// Also read from `DECDN_ADMIN_URL` when unset; clap folds the env
    /// var into this field. If still unset, the admin port is derived
    /// from `observability.admin_port` in the config file (see
    /// `--config`). Example: `http://127.0.0.1:9191`.
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Path to the TOML config file used to derive the admin URL when
    /// `--admin-url` / `DECDN_ADMIN_URL` are unset. Takes precedence
    /// over the top-level `decdn --config`; if neither is set,
    /// resolution falls through to `~/.decdn/node.toml` and then the
    /// built-in default port.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Filter output to peers whose region matches exactly
    /// (case-insensitive). Applied client-side after fetching.
    #[arg(long, value_name = "CODE")]
    pub region: Option<String>,

    /// Emit the admin response body as JSON instead of a human table.
    #[arg(long)]
    pub json: bool,

    /// Roundtrip timeout in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}
