//! `decdn pool` — client-side payment-pool lifecycle commands.
//!
//! Sibling of [`super::fetch`]: where `fetch` opens/reuses the caller's own
//! `PaymentPool` deposit and pays for delivery from it, the `pool` subcommands
//! manage that deposit directly. `list`/`status` is a read-only dump of the
//! tracked buyer pools and their per-lane voucher watermark; `open` escrows a
//! fresh deposit (`openPool`); `top-up` adds funds to a pool the caller owns;
//! `close` starts the on-chain grace-window close (`closePool`); `reclaim`
//! refunds the residual once that window has elapsed (`reclaim`, callable by
//! anyone); `assign` issues an owner-signed spending capability delegating a
//! bounded spend on the pool to a delegate signer key, printing the `dcap1:`
//! token to hand off. Each of `top-up`/`close`/`reclaim`/`assign` names its
//! target pool explicitly via `--pool` — there is no implicit per-provider
//! lookup, because one pool fans out to every provider the owner pays
//! (ADR 003). `close` and `reclaim` also accept `--all`, which enumerates every
//! pool this keystore owns on-chain (`getPools`) and acts on each eligible one.
//!
//! These commands own the **client's** buyer store, `buyer-pools.redb`. A
//! `decdn-node` daemon keeps its own buyer pools in `buyer.redb` in its data
//! dir; the two files share a table format and nothing else. A data dir holding
//! any of the daemon's stores — not just `buyer.redb`, which is the file a
//! reset loses — is a node's. Pointed at one, `list` reports the node's pools
//! over the admin RPC (while the daemon runs it holds that file exclusively,
//! so there is no disk path to it),
//! and the commands that would escrow into a store the node never reads —
//! `open`, `top-up`, and the chain-enumerating `close --all` / `reclaim --all` —
//! refuse. `close --pool` and `reclaim --pool` still run, and say plainly that
//! the daemon's own record was left untouched.

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
    /// List the tracked buyer pools and their per-lane voucher watermark.
    ///
    /// Read-only, and it names the store it read — on the `store=` line, or
    /// the `store` field under `--json`. On a client data dir that is the
    /// CLI's own `buyer-pools.redb`, read directly. On a `decdn-node` daemon's
    /// data dir it is the daemon's `buyer.redb`, read over the admin RPC:
    /// while that daemon runs it holds the file exclusively, so nothing can
    /// open it from disk. The two are separate stores — a client listing says
    /// nothing about a node, and vice versa — and their `--json` pool objects
    /// differ, so read `source` before parsing them.
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
    /// Issue an owner-signed spending capability delegating a bounded spend on
    /// a pool the caller owns to a delegate `--signer` key, and print the
    /// `dcap1:` token to hand to that delegated client (ADR 003 §Capability
    /// delegation). Offline-capable: only the owner keystore is required to
    /// sign. An owner check that cannot be performed warns and issues the
    /// token anyway. An owner check that reaches the chain and finds a
    /// different owner, or no such pool, fails the command, because that
    /// token would be rejected at redemption.
    Assign(PoolAssignArgs),
}

/// `decdn pool assign` flags.
#[derive(Args, Debug)]
pub struct PoolAssignArgs {
    /// The pool to delegate spend on (0x-prefixed 32-byte `poolId`). The caller
    /// must own it — the capability is signed with the owner keystore.
    #[arg(long, value_name = "0xHASH")]
    pub pool: String,

    /// The delegate's Ethereum address (0x-prefixed) — the voucher-signing key
    /// authorized to spend against the pool under this capability.
    #[arg(long, value_name = "0xADDR")]
    pub signer: String,

    /// The delegate's cumulative spend ceiling, in micro-USDC (USDC base units).
    #[arg(long, value_name = "MICRO_USDC")]
    pub cap_micro_usdc: u64,

    /// Capability lifetime in seconds from now — the absolute Unix expiry is
    /// `now + this`. Mutually exclusive with `--expiry-at`; exactly one is
    /// required.
    #[arg(long, value_name = "SECS", conflicts_with = "expiry_at")]
    pub expiry_secs: Option<u64>,

    /// Absolute Unix-seconds expiry. Mutually exclusive with `--expiry-secs`;
    /// exactly one is required.
    #[arg(long, value_name = "UNIX_TS", conflicts_with = "expiry_secs")]
    pub expiry_at: Option<u64>,

    /// Shared chain + store coordinates.
    #[command(flatten)]
    pub chain: PoolChainArgs,
}

/// `decdn pool open` flags.
#[derive(Debug, clap::Args)]
pub struct PoolOpenArgs {
    /// Deposit to escrow, in micro-USDC (USDC base units).
    #[arg(long, value_name = "MICRO_USDC")]
    pub deposit_micro_usdc: u64,
    /// Chain coordinates and the payment-pool address.
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
///
/// Targets exactly one of a single `--pool` or `--all`. `--all` enumerates every
/// pool this keystore owns from chain (`getPools`) and closes the ones that are
/// still `Open`.
#[derive(Args, Debug)]
#[command(group(clap::ArgGroup::new("close_target").required(true).args(["pool", "all"])))]
pub struct PoolCloseArgs {
    /// The pool to close (0x-prefixed 32-byte `poolId`). Mutually exclusive
    /// with `--all`.
    #[arg(long, value_name = "0xHASH")]
    pub pool: Option<String>,

    /// Close every pool this keystore owns that is currently `Open`, discovered
    /// on-chain via `getPools`. Pools already closing or closed are skipped;
    /// one pool's failure does not abort the rest. Mutually exclusive with
    /// `--pool`.
    #[arg(long)]
    pub all: bool,

    /// Shared chain + store coordinates.
    #[command(flatten)]
    pub chain: PoolChainArgs,
}

/// `decdn pool reclaim` flags.
///
/// Targets exactly one of a single `--pool` or `--all`. `--all` enumerates every
/// pool this keystore owns from chain (`getPools`) and reclaims the ones whose
/// dispute window has elapsed.
#[derive(Args, Debug)]
#[command(group(clap::ArgGroup::new("reclaim_target").required(true).args(["pool", "all"])))]
pub struct PoolReclaimArgs {
    /// The pool to reclaim (0x-prefixed 32-byte `poolId`). Mutually exclusive
    /// with `--all`.
    #[arg(long, value_name = "0xHASH")]
    pub pool: Option<String>,

    /// Reclaim every pool this keystore owns whose grace window has elapsed,
    /// discovered on-chain via `getPools`. Pools still `Open`, still inside
    /// their dispute window, or already reclaimed are skipped; one pool's
    /// failure does not abort the rest. Mutually exclusive with `--pool`.
    #[arg(long)]
    pub all: bool,

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
    /// dir). Password from `$DECDN_KEYSTORE_PASSWORD`, else
    /// `--keystore-password-file`, else a TTY prompt.
    #[arg(long, value_name = "PATH")]
    pub keystore: Option<PathBuf>,

    /// File whose contents are the keystore password. Consulted after the
    /// `DECDN_KEYSTORE_PASSWORD` env var and before an interactive prompt on a
    /// TTY. A single trailing newline is stripped; a file that is empty after
    /// that strip is a deliberate empty password, not an absent source. A path
    /// that does not exist falls through; one that exists but cannot be read is
    /// an error.
    #[arg(long, value_name = "PATH", env = "DECDN_KEYSTORE_PASSWORD_FILE")]
    pub keystore_password_file: Option<PathBuf>,

    /// Data dir holding the buyer-pool store (else `identity.data_dir` >
    /// default).
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,
}

/// `decdn pool list` / `status` flags.
///
/// Read-only. Needs no chain coordinates and no keystore. It reads the data
/// dir's client buyer store — unless that data dir belongs to a `decdn-node`
/// daemon, in which case it asks the daemon over the admin RPC instead (the
/// daemon's own store is locked for its lifetime), and the admin flags below
/// apply.
#[derive(Args, Debug)]
pub struct PoolListArgs {
    /// Data dir holding the buyer-pool store (else `identity.data_dir` >
    /// default).
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// Emit the tracked pools as JSON instead of the aligned table.
    #[arg(long)]
    pub json: bool,

    /// Base URL of the daemon's admin HTTP surface, used only when the data
    /// dir turns out to be a node's. Also read from `DECDN_ADMIN_URL`; if
    /// unset, the port is derived from `observability.admin_port` in the
    /// config file, then the built-in default.
    #[arg(long, value_name = "URL", env = "DECDN_ADMIN_URL")]
    pub admin_url: Option<String>,

    /// Roundtrip timeout in milliseconds for that admin call.
    #[arg(long, value_name = "MS", default_value_t = 5_000)]
    pub timeout_ms: u64,
}
