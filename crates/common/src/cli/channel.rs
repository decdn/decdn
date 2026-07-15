//! `decdn channel` — client-side payment-channel lifecycle commands.
//!
//! Sibling of [`super::fetch`]: where `fetch` opens/reuses a channel and pays
//! for delivery, the `channel` subcommands inspect and wind down those
//! channels. `list`/`status` (#1133) is a read-only dump of the tracked buyer
//! channels and their local voucher watermark; `coop-close` (#971) asks the
//! provider node for a `CooperativeClose` waiver over the channel's final
//! watermark and submits `cooperativeClose` on-chain — one tx, no 48h dispute
//! window (ADR 003 §Cooperative close). The channel to close is the one tracked
//! for `--provider-address` in the buyer store, so its final `(amount, nonce,
//! bytes)` come from what this client already paid; no manual tuple is passed.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Args, Subcommand};

/// `decdn channel <subcommand>`.
#[derive(Args, Debug)]
pub struct ChannelArgs {
    /// Channel lifecycle subcommand.
    #[command(subcommand)]
    pub command: ChannelCommand,
}

/// Client-side channel lifecycle operations.
#[derive(Subcommand, Debug)]
pub enum ChannelCommand {
    /// List the tracked buyer channels and their local voucher watermark
    /// (read-only; reads the buyer store, no chain or network access).
    #[command(visible_alias = "status")]
    List(ChannelListArgs),
    /// Cooperatively close the channel tracked for a provider: fetch the
    /// provider's waiver and settle on-chain with no dispute window.
    CoopClose(CoopCloseArgs),
    /// Unilaterally close the channel tracked for a provider with the latest
    /// client voucher, opening the on-chain dispute window (no provider needed).
    Close(ChannelCloseArgs),
    /// Finalize the channel tracked for a provider: `settleChannel` after its
    /// dispute window, or `reclaimExpired` if it expired while still open.
    Settle(ChannelSettleArgs),
    /// Reclaim USDC from every tracked channel by driving each through the
    /// unilateral close -> dispute-window -> settle/reclaim sweep. Idempotent:
    /// re-run to advance channels whose windows have since elapsed.
    Clean(ChannelCleanArgs),
}

/// `decdn channel close` flags.
#[derive(Args, Debug)]
pub struct ChannelCloseArgs {
    /// Provider's Ethereum address — selects the tracked buyer channel to close.
    #[arg(long, value_name = "ADDRESS")]
    pub provider_address: String,

    /// Shared chain + store coordinates.
    #[command(flatten)]
    pub chain: ChannelChainArgs,
}

/// `decdn channel settle` flags.
#[derive(Args, Debug)]
pub struct ChannelSettleArgs {
    /// Provider's Ethereum address — selects the tracked buyer channel to
    /// finalize.
    #[arg(long, value_name = "ADDRESS")]
    pub provider_address: String,

    /// Shared chain + store coordinates.
    #[command(flatten)]
    pub chain: ChannelChainArgs,
}

/// `decdn channel clean` flags.
#[derive(Args, Debug)]
pub struct ChannelCleanArgs {
    /// Shared chain + store coordinates.
    #[command(flatten)]
    pub chain: ChannelChainArgs,
}

/// Shared chain + store coordinates for `decdn channel` subcommands.
///
/// The chain coordinates resolve flag > `[blockchain]` config; the keystore and
/// data dir resolve flag > `[blockchain]`/`[identity]` config > default, exactly
/// as `decdn fetch` does. Flattened into each subcommand's flags so every
/// `channel` command shares one precedence path.
#[derive(Args, Debug)]
pub struct ChannelChainArgs {
    /// JSON-RPC endpoint of the chain (else `blockchain.rpc_url`).
    #[arg(long, value_name = "URL")]
    pub rpc_url: Option<String>,

    /// `PaymentChannel` contract address — the on-chain call target and the
    /// EIP-712 `verifyingContract` (else `blockchain.payment_channel_address`).
    #[arg(long, value_name = "ADDRESS")]
    pub payment_channel_address: Option<String>,

    /// EVM chain id for the EIP-712 domain (else `blockchain.chain_id` >
    /// default).
    #[arg(long, value_name = "ID")]
    pub chain_id: Option<u64>,

    /// Ethereum keystore path (else `blockchain.eth_keystore` > under the data
    /// dir).
    #[arg(long, value_name = "PATH")]
    pub keystore: Option<PathBuf>,

    /// Data dir holding the buyer-channel store (else `identity.data_dir` >
    /// default).
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,
}

/// `decdn channel list` / `status` flags.
///
/// Read-only: only the data dir (to locate the buyer store) is consulted; no
/// chain coordinates, keystore, or network endpoint are needed.
#[derive(Args, Debug)]
pub struct ChannelListArgs {
    /// Data dir holding the buyer-channel store (else `identity.data_dir` >
    /// default).
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// Emit the tracked channels as JSON instead of the aligned table.
    #[arg(long)]
    pub json: bool,
}

/// `decdn channel coop-close` flags.
#[derive(Args, Debug)]
pub struct CoopCloseArgs {
    /// Node ID (Ed25519 public key) of the provider to request the waiver from.
    #[arg(long, value_name = "NODE_ID")]
    pub node_id: String,

    /// Direct socket address of the provider node (e.g. `127.0.0.1:4433`).
    /// Optional — the discovery-enabled endpoint resolves `--node-id` otherwise.
    #[arg(long, value_name = "ADDR")]
    pub addr: Option<SocketAddr>,

    /// Relay URL override for reaching the node (else `network.relay_urls`).
    #[arg(long, value_name = "URL")]
    pub relay_url: Option<String>,

    /// Provider's Ethereum address — selects the tracked buyer channel and is
    /// the expected signer of the returned waiver.
    #[arg(long, value_name = "ADDRESS")]
    pub provider_address: String,

    /// Shared chain + store coordinates.
    #[command(flatten)]
    pub chain: ChannelChainArgs,

    /// Overall timeout for the waiver request, in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 30_000)]
    pub timeout_ms: u64,
}
