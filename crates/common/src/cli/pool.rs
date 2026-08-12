//! `decdn pool` — client-side payment-pool lifecycle commands.
//!
//! Sibling of [`super::fetch`]: where `fetch` opens/reuses the caller's own
//! `PaymentPool` deposit and pays for delivery from it, the `pool` subcommands
//! manage that deposit directly. `list`/`status` is a read-only dump of the
//! tracked buyer pools and their per-lane voucher watermark; `open` escrows a
//! fresh deposit (`openPool`); `top-up` adds funds to a pool the caller owns;
//! `close` starts the on-chain grace-window close (`closePool`); `reclaim`
//! refunds the residual once that window has elapsed (`reclaim`, callable by
//! anyone). Each of `top-up`/`close`/`reclaim` takes the target pool
//! explicitly via `--pool` — there is no implicit per-provider lookup, because
//! one pool fans out to every provider the owner pays (ADR 003).

use std::path::PathBuf;

use clap::{Args, Subcommand};

/// `decdn pool <subcommand>`.
#[derive(Args, Debug)]
pub struct PoolArgs {
    /// Pool lifecycle subcommand.
    #[command(subcommand)]
    pub command: PoolCommand,
}

/// Client-side payment-pool lifecycle operations.
#[derive(Subcommand, Debug)]
pub enum PoolCommand {
    /// List the tracked buyer pools and their per-lane voucher watermark
    /// (read-only; reads the buyer store, no chain or network access).
    #[command(visible_alias = "status")]
    List(PoolListArgs),
    /// Open a fresh `PaymentPool` deposit, escrowing `--deposit-micro-usdc`
    /// USDC (`openPool`). The pool fans out to every provider paid from it
    /// (ADR 003) — there is no per-provider open.
    Open(PoolOpenArgs),
    /// Add funds to a pool the caller owns (`topUp`).
    TopUp(PoolTopUpArgs),
    /// Start the grace-window close on a pool the caller owns (`closePool`).
    /// Redemptions stay valid until the dispute deadline; run `pool reclaim`
    /// after it elapses to recover the residual deposit.
    Close(PoolCloseArgs),
    /// Refund the residual deposit of a pool once its grace window has
    /// elapsed (`reclaim`; callable by anyone, but only the owner receives
    /// funds).
    Reclaim(PoolReclaimArgs),
}

/// `decdn pool open` flags.
#[derive(Debug, clap::Args)]
pub struct PoolOpenArgs {
    /// Deposit to escrow, in micro-USDC (USDC base units).
    #[arg(long, value_name = "MICRO_USDC")]
    pub deposit_micro_usdc: u64,
    #[command(flatten)]
    pub chain: PoolChainArgs,
}

/// `decdn pool top-up` flags.
#[derive(Args, Debug)]
pub struct PoolTopUpArgs {
    /// The pool to fund (0x-prefixed 32-byte `poolId`, from `pool open`'s output
    /// or `pool list`).
    #[arg(long, value_name = "0xHASH")]
    pub pool: String,

    /// Amount to add, in micro-USDC (USDC base units).
    #[arg(long, value_name = "MICRO_USDC")]
    pub amount_micro_usdc: u64,

    /// Shared chain + store coordinates.
    #[command(flatten)]
    pub chain: PoolChainArgs,
}

/// `decdn pool close` flags.
#[derive(Args, Debug)]
pub struct PoolCloseArgs {
    /// The pool to close (0x-prefixed 32-byte `poolId`).
    #[arg(long, value_name = "0xHASH")]
    pub pool: String,

    /// Shared chain + store coordinates.
    #[command(flatten)]
    pub chain: PoolChainArgs,
}

/// `decdn pool reclaim` flags.
#[derive(Args, Debug)]
pub struct PoolReclaimArgs {
    /// The pool to reclaim (0x-prefixed 32-byte `poolId`).
    #[arg(long, value_name = "0xHASH")]
    pub pool: String,

    /// Shared chain + store coordinates.
    #[command(flatten)]
    pub chain: PoolChainArgs,
}

/// Shared chain + store coordinates for `decdn pool` subcommands.
///
/// The chain coordinates resolve flag > `[blockchain]` config; the keystore and
/// data dir resolve flag > `[blockchain]`/`[identity]` config > default, exactly
/// as `decdn fetch` does. Flattened into each subcommand's flags so every
/// `pool` command shares one precedence path.
#[derive(Args, Debug)]
pub struct PoolChainArgs {
    /// JSON-RPC endpoint of the chain (else `blockchain.rpc_url`).
    #[arg(long, value_name = "URL")]
    pub rpc_url: Option<String>,

    /// `PaymentPool` contract address — the on-chain call target and the
    /// EIP-712 `verifyingContract` (else `blockchain.payment_pool_address`).
    #[arg(long, value_name = "ADDRESS")]
    pub payment_pool_address: Option<String>,

    /// EVM chain id for the EIP-712 domain (else `blockchain.chain_id` >
    /// default).
    #[arg(long, value_name = "ID")]
    pub chain_id: Option<u64>,

    /// Ethereum keystore path (else `blockchain.eth_keystore` > under the data
    /// dir).
    #[arg(long, value_name = "PATH")]
    pub keystore: Option<PathBuf>,

    /// Data dir holding the buyer-pool store (else `identity.data_dir` >
    /// default).
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,
}

/// `decdn pool list` / `status` flags.
///
/// Read-only: only the data dir (to locate the buyer store) is consulted; no
/// chain coordinates, keystore, or network endpoint are needed.
#[derive(Args, Debug)]
pub struct PoolListArgs {
    /// Data dir holding the buyer-pool store (else `identity.data_dir` >
    /// default).
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// Emit the tracked pools as JSON instead of the aligned table.
    #[arg(long)]
    pub json: bool,
}
