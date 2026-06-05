//! CLI argument parsing for the `decdn` binary.
//!
//! [`RunArgs`] is also re-used (flattened) by [`ConfigValidateArgs`] —
//! `decdn config validate` runs the same resolver `decdn-node run`
//! does, against the same `DECDN_*` env vars, without binding ports
//! or starting any tasks. The daemon's own clap shell lives in
//! `crates/node/src/main.rs` and is not part of this module.

pub mod bundle;
pub mod common;
pub mod config_cmd;
pub mod key_gen;
pub mod node;
pub mod probe;
pub mod run;

pub use bundle::{BundleArgs, BundleCommand, BundleCreateArgs};
pub use common::{ConfigPathSource, LogFormat, default_config_path, default_data_dir};
pub use config_cmd::{ConfigArgs, ConfigCommand, ConfigInitArgs, ConfigValidateArgs};
pub use key_gen::KeyGenArgs;
pub use node::{
    AnnounceArgs, ChannelsArgs, DrainArgs, EvictArgs, HealthArgs, NodeArgs, NodeCommand, PeersArgs,
    RegionStatsArgs, ReloadArgs, StatusArgs, TopArgs,
};
pub use probe::ProbeArgs;
pub use run::RunArgs;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// User-facing deCDN CLI. The daemon lives in the separate `decdn-node`
/// binary; this CLI is what you type from a terminal — probe, node admin,
/// key-gen, config.
#[derive(Parser, Debug)]
#[command(
    name = "decdn",
    version,
    about = "deCDN user CLI — probe, node admin, key-gen, config",
    long_about = "User-facing deCDN CLI. Pairs with the `decdn-node` daemon: \
                  this binary carries every command a human types in a \
                  terminal (probe, node admin, key-gen, config). The \
                  `node` subcommand group talks to a running daemon over \
                  the loopback admin RPC surface (ADR 025) and fails \
                  cleanly on a host with no daemon running."
)]
pub struct Cli {
    /// Path to TOML config file [default: ~/.decdn/node.toml].
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: Command,
}

/// Available subcommands. There is intentionally no `run` subcommand —
/// running the daemon is `decdn-node run` (issue #421). For a dry-run
/// resolution of the daemon's startup config without binding ports,
/// see [`ConfigValidateArgs`] (`decdn config validate`), which
/// flattens the same [`RunArgs`] the daemon parses.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Generate a new Ed25519 node key and Ethereum keystore.
    KeyGen(KeyGenArgs),
    /// Manage configuration files (`init`, `validate`).
    Config(ConfigArgs),
    /// Probe a running node over the `cdn/probe/v1` ALPN.
    Probe(ProbeArgs),
    /// Operator-local admin commands that query a running node
    /// over its loopback HTTP surface (ADR 025).
    Node(NodeArgs),
    /// Build and (eventually) fetch directory bundles — a JSON manifest
    /// linking BLAKE3-content-addressed blobs by relative path. See
    /// `appendix-bundles.md` for the format and issue #391 for status.
    Bundle(BundleArgs),
}
