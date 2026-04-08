//! Arguments for the `decdn config init` subcommand.

use std::path::PathBuf;

use clap::Args;

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
