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

    /// File whose contents are the keystore password. Consulted after the
    /// `DECDN_KEYSTORE_PASSWORD` env var and before an interactive prompt on a
    /// TTY. A single trailing newline is stripped; a file that is empty after
    /// that strip is a deliberate empty password, not an absent source. A path
    /// that does not exist falls through; one that exists but cannot be read is
    /// an error.
    #[arg(long, value_name = "PATH", env = "DECDN_KEYSTORE_PASSWORD_FILE")]
    pub keystore_password_file: Option<PathBuf>,
}
