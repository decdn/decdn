//! Arguments for the `decdn whoami` subcommand.

use std::path::PathBuf;

use clap::Args;

/// Print the local identity — iroh node id, eth address, and the resolved key
/// paths — read-only, without generating or rotating any keys.
#[derive(Args, Debug)]
pub struct WhoamiArgs {
    /// Directory holding the keys. Overrides `identity.data_dir` from the
    /// config file; when neither is set, defaults to `~/.decdn`.
    #[arg(long, value_name = "DIR")]
    pub output_dir: Option<PathBuf>,

    /// File whose contents are the keystore password, used to decrypt the eth
    /// address when it is available. Consulted after the
    /// `DECDN_KEYSTORE_PASSWORD` env var and before an interactive prompt on a
    /// TTY. A single trailing newline is stripped. When no password source is
    /// available, the node id and paths still print and the eth address is
    /// reported as needing a password. A path that does not exist falls
    /// through; one that exists but cannot be read is an error.
    #[arg(long, value_name = "PATH", env = "DECDN_KEYSTORE_PASSWORD_FILE")]
    pub keystore_password_file: Option<PathBuf>,
}
