//! Arguments for the `decdn key-gen` subcommand.

use std::path::PathBuf;

use clap::Args;

/// Generate a new Ed25519 node key and Ethereum keystore.
#[derive(Args, Debug)]
pub struct KeyGenArgs {
    /// Output directory for generated keys [default: ~/.decdn].
    #[arg(long, value_name = "DIR")]
    pub output_dir: Option<PathBuf>,

    /// Overwrite existing keys if present.
    #[arg(long)]
    pub force: bool,
}
