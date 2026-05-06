//! Arguments for the `decdn config` command group (`init`, `validate`).

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::run::RunArgs;

/// Manage `decdn` configuration files.
#[derive(Args, Debug)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub command: ConfigCommand,
}

/// Subcommands of `decdn config`.
#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Write a default configuration file.
    Init(ConfigInitArgs),
    /// Validate a configuration file without starting the node.
    //
    // Boxed to keep the enum variant size close to `Init`'s — `ConfigValidateArgs`
    // flattens the large `RunArgs` struct. Mirrors `Command::Run(Box<RunArgs>)`.
    Validate(Box<ConfigValidateArgs>),
}

/// Write a default TOML configuration file.
#[derive(Args, Debug)]
pub struct ConfigInitArgs {
    /// Path to write the config file [default: ~/.decdn/node.toml].
    #[arg(long, value_name = "PATH")]
    pub output: Option<PathBuf>,

    /// Overwrite existing config file if present.
    #[arg(long)]
    pub force: bool,
}

/// Validate the resolved configuration without binding ports or connecting to
/// the RPC endpoint.
///
/// Flattens the same argument groups as `run` so that `DECDN_*` environment
/// variables and optional CLI overrides are honored exactly as they would be
/// during `decdn run`.
#[derive(Args, Debug)]
pub struct ConfigValidateArgs {
    #[command(flatten)]
    pub run: RunArgs,
}
