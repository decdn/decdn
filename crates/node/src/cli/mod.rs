//! CLI argument parsing for the `decdn` binary.

pub mod common;
pub mod config_cmd;
pub mod key_gen;
pub mod run;

pub use common::{LogFormat, default_config_path, default_data_dir};
pub use config_cmd::ConfigInitArgs;
pub use key_gen::KeyGenArgs;
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
    /// Initialize a default configuration file.
    #[command(name = "config")]
    Config(ConfigInitArgs),
}
