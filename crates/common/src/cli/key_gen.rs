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

    /// Read the keystore password from this file. When unset, the
    /// `DECDN_KEYSTORE_PASSWORD` env var is consulted, then an interactive
    /// prompt (if stdin is a TTY) is used. A single trailing newline in the
    /// file is stripped.
    #[arg(long, value_name = "PATH", env = "DECDN_KEYSTORE_PASSWORD_FILE")]
    pub password_file: Option<PathBuf>,
}
