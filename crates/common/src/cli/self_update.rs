//! Arguments for the `decdn self-update` subcommand.

use clap::Args;

/// Replace the running `decdn` binary with the latest signed release.
///
/// The release's `SHA256SUMS.minisig` must verify against a minisign key this
/// build embeds, and the archive must match the signed manifest, before any
/// byte of the running binary changes. Only a binary the user can write and
/// that is not in a cargo bin directory (`cargo install`, `cargo binstall`) is
/// replaced; the command names the path and the reason otherwise.
#[derive(Args, Debug)]
pub struct SelfUpdateArgs {
    /// Report whether a newer signed release exists and whether this install
    /// can be replaced; download no archive and change nothing.
    #[arg(long)]
    pub check: bool,
}
