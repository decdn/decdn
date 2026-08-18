//! Arguments for the `decdn origin` command group (`index`).

use clap::{Args, Subcommand};

/// Top-level `decdn origin` group.
#[derive(Args, Debug)]
pub struct OriginArgs {
    #[command(subcommand)]
    pub cmd: OriginCommand,
}

/// Subcommands under `decdn origin`.
#[derive(Subcommand, Debug)]
pub enum OriginCommand {
    /// Generate the `{hex}.obao4` pre-order outboard sibling for every blob in
    /// each configured filesystem origin that lacks one. Enables zero-copy serve.
    Index(OriginIndexArgs),
}

/// Generate `.obao4` outboards for every blob file in each configured
/// filesystem origin that lacks one.
#[derive(Args, Debug)]
pub struct OriginIndexArgs {
    /// Recompute and overwrite outboards that already exist.
    #[arg(long)]
    pub force: bool,

    #[command(flatten)]
    pub run: super::run::RunArgs,
}
