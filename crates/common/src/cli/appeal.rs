//! `decdn appeal` command surface — operator-facing slash-appeal filing
//! (ADR 028). Currently one subcommand, `slash`, which posts the appeal bond
//! and files an appeal against a slash via `SlashAppeal.openSlashAppeal`.
//!
//! The role-gated fast-track / grant / uphold actions (emergency multisig and
//! Governor) are intentionally *not* exposed here — they are governance
//! operations, not user-CLI surface.

use clap::{Args, Subcommand};

use crate::cli::common::CommonChainArgs;

/// `decdn appeal` — file a slash appeal.
#[derive(Args, Debug)]
pub struct AppealArgs {
    /// Appeal subcommand to execute.
    #[command(subcommand)]
    pub command: AppealCommand,
}

/// `decdn appeal` subcommands.
#[derive(Subcommand, Debug)]
pub enum AppealCommand {
    /// File an appeal against a slash, posting the appeal bond
    /// (`SlashAppeal.openSlashAppeal`). Must be run by the slashed operator,
    /// within the 30-day filing window.
    Slash(AppealSlashArgs),
}

/// Arguments for `decdn appeal slash <SLASH_ID> <EVIDENCE_BUNDLE_HASH>`.
#[derive(Args, Debug)]
pub struct AppealSlashArgs {
    /// The on-chain slash id to appeal (surfaced by the daemon admin RPC
    /// `admin_v1_slashes`, or the `Slashed` event's `slashId`). A `uint256`
    /// decimal string — taken as text so an id above `u64::MAX` still parses.
    #[arg(value_name = "SLASH_ID")]
    pub slash_id: String,

    /// Evidence bundle hash — a 0x-prefixed 32-byte hex reference to the
    /// off-chain evidence bundle (ADR 028). Stored on-chain verbatim.
    #[arg(value_name = "EVIDENCE_BUNDLE_HASH")]
    pub evidence_bundle_hash: String,

    /// `SlashAppeal` contract address. Overrides
    /// `blockchain.slash_appeal_address`. The config-file value is read as
    /// plain TOML (no `${VAR}` expansion); use this flag or the env var for
    /// substitution.
    #[arg(long, value_name = "ADDR", env = "DECDN_SLASH_APPEAL_ADDRESS")]
    pub slash_appeal_address: Option<String>,

    /// Shared chain coordinates (rpc, keystore, chain id, `--dry-run`,
    /// `--json`). Only the [`CommonChainArgs`] subset is flattened here — the
    /// appeal path reads nothing from the `CapacityBond` / swap flag group, so
    /// exposing it would advertise flags this command silently ignores.
    #[command(flatten)]
    pub chain: CommonChainArgs,
}
