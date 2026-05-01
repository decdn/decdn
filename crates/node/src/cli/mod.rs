//! CLI argument parsing for the `decdn` binary.

pub mod common;
pub mod config_cmd;
pub mod key_gen;
pub mod node;
pub mod probe;
pub mod run;

pub use common::{LogFormat, default_config_path, default_data_dir};
pub use config_cmd::{ConfigArgs, ConfigCommand, ConfigInitArgs, ConfigValidateArgs};
pub use key_gen::KeyGenArgs;
pub use node::{
    AnnounceArgs, DrainArgs, EvictArgs, HealthArgs, NodeArgs, NodeCommand, PeersArgs, ReloadArgs,
};
pub use probe::ProbeArgs;
pub use run::RunArgs;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// A deCDN node: cache and serve content-addressed blobs over iroh QUIC.
#[derive(Parser, Debug)]
#[command(name = "decdn", version, about, long_about = None)]
pub struct Cli {
    /// Path to TOML config file [default: ~/.decdn/node.toml].
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: Command,
}

/// Available subcommands.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the deCDN node.
    Run(Box<RunArgs>),
    /// Generate a new Ed25519 node key and Ethereum keystore.
    KeyGen(KeyGenArgs),
    /// Manage configuration files (`init`, `validate`).
    Config(ConfigArgs),
    /// Probe a running node over the `cdn/probe/v1` ALPN.
    Probe(ProbeArgs),
    /// Operator-local admin commands that query a running node
    /// over its loopback HTTP surface (ADR 025).
    Node(NodeArgs),
}
