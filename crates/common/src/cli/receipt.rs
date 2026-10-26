//! Arguments for `decdn receipt` (#2599): check a pull receipt `decdn get
//! --receipt` wrote, and add the on-chain redemptions that settle it.

use std::path::PathBuf;

use clap::{Args, Subcommand};

/// `decdn receipt` — check or settle a pull receipt.
#[derive(Args, Debug)]
pub struct ReceiptArgs {
    /// The receipt operation.
    #[command(subcommand)]
    pub command: ReceiptCommand,
}

/// The `decdn receipt` operations.
#[derive(Subcommand, Debug)]
pub enum ReceiptCommand {
    /// Re-check every signature in a receipt, offline.
    Verify(ReceiptVerifyArgs),
    /// Find the redemptions that settle a receipt's claims, and record them
    /// in the file.
    Settle(ReceiptSettleArgs),
}

/// `decdn receipt verify <FILE>`.
///
/// Checks each node's signed answer (well-formed, accepting, on its lane's
/// pool, for a hash the receipt lists, signed by the node under the
/// `SlashJudge` domain the receipt names), the size of each entry an answer
/// covers, each voucher against the `PaymentPool` domain, each revealed
/// preimage against its voucher's hash chain, and that every claim is the one
/// its voucher proves and sits at or above the prior watermark. On a chain
/// this build knows, the receipt's contract addresses must match the build's;
/// on any other chain they are reported unchecked. Nothing is read from the
/// network. Exits non-zero when any check fails. `adr/appendix-pull-receipts.md`
/// says what a receipt proves.
#[derive(Args, Debug)]
pub struct ReceiptVerifyArgs {
    /// The receipt file.
    #[arg(value_name = "FILE")]
    pub file: PathBuf,

    /// Print the result as one line of JSON.
    #[arg(long)]
    pub json: bool,
}

/// `decdn receipt settle <FILE>`.
///
/// Verifies the receipt as `verify` does and refuses one that fails. Then
/// scans `PaymentPool.PoolRedeemed` for each lane's pool and node, from an
/// hour before the receipt's start (or `--from-block`) to the chain head, and
/// records for each lane the first redemption that covers its claim, or the
/// highest one short of it. A node redeems in its own sweep, so a lane can
/// stay unsettled for a while after the pull. The file is rewritten in place
/// (atomically: temp in the same directory, then rename).
#[derive(Args, Debug)]
pub struct ReceiptSettleArgs {
    /// The receipt file.
    #[arg(value_name = "FILE")]
    pub file: PathBuf,

    /// JSON-RPC endpoint for the receipt's chain. Falls back to
    /// `blockchain.rpc_url` when the config names the same chain, then to the
    /// chain's public RPC when this build knows it.
    #[arg(long, value_name = "URL")]
    pub rpc_url: Option<String>,

    /// First block to scan. The default is the block at the receipt's start,
    /// less an hour for clock skew.
    #[arg(long, value_name = "BLOCK")]
    pub from_block: Option<u64>,

    /// Print the updated receipt as one line of JSON.
    #[arg(long)]
    pub json: bool,
}
