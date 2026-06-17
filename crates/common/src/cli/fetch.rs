//! `decdn fetch` — the standalone client-side paid pull of a single
//! content-addressed blob over `cdn/client/v1` (issues #391, #940).
//!
//! The paying sibling of [`super::probe::ProbeArgs`]: a one-shot client command
//! that dials a node by explicit `--node-id`/`--addr`/`--relay-url` and pays
//! per-MB from a `PaymentChannel`. The channel is **auto-opened and reused**
//! (#940): `fetch` looks up a live channel with `--provider-address` in its
//! persistent buyer-channel store, resuming that channel's voucher watermark;
//! if none exists it opens (and funds) one on-chain and records it. So there is
//! no `--channel-id` — the channel is derived.
//!
//! The on-chain coordinates (RPC, contract addresses, chain id, keystore, data
//! dir) resolve **flag > `[blockchain]`/`[identity]` config > default**, so a
//! configured client runs `decdn fetch --node-id … --addr … --provider-address
//! … --hash … -o …` with no chain flags. Fields are kept as `String`/`Option`
//! (mirroring [`super::probe::ProbeArgs`]); the `cli` binary parses addresses
//! and merges the config.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Args;

/// Arguments for `decdn fetch`.
///
/// Reachability (a direct `--addr` or a relay) and the required chain
/// coordinates are validated at runtime rather than by clap, because the relay
/// (`network.relay_urls`, #935) and the chain coordinates (`[blockchain]`) can
/// come from config, which clap cannot see.
#[derive(Debug, Clone, Args)]
pub struct FetchArgs {
    /// Target node id (iroh `EndpointId`, z-base32).
    #[arg(long, value_name = "ID")]
    pub node_id: String,

    /// BLAKE3 hash of the blob to fetch: 64 hex chars (optional `0x` prefix).
    #[arg(long, value_name = "HASH")]
    pub hash: String,

    /// Destination path for the fetched blob. Written atomically
    /// (temp-in-dir then rename); an existing file is replaced.
    #[arg(short = 'o', long, value_name = "PATH")]
    pub output: PathBuf,

    /// Direct socket address of the target node (e.g. `127.0.0.1:4433`).
    #[arg(long, value_name = "HOST:PORT")]
    pub addr: Option<SocketAddr>,

    /// iroh relay URL to use for discovery-based resolution. Overrides
    /// `network.relay_urls` from config when set (#935).
    #[arg(long, value_name = "URL")]
    pub relay_url: Option<String>,

    /// The delivering node's Ethereum address (0x-prefixed hex). The channel is
    /// opened/reused against it, and the response `slash_sig` must recover to it
    /// (ADR 014 §1); a mismatch aborts the pull.
    #[arg(long, value_name = "0xADDR")]
    pub provider_address: String,

    /// JSON-RPC endpoint for on-chain channel open. Overrides
    /// `blockchain.rpc_url` from config.
    #[arg(long, value_name = "URL")]
    pub rpc_url: Option<String>,

    /// `PaymentChannel` contract address (0x hex) — the voucher EIP-712
    /// `verifyingContract` and the `openChannel` target. Overrides
    /// `blockchain.payment_channel_address`.
    #[arg(long, value_name = "0xADDR")]
    pub payment_channel_address: Option<String>,

    /// `SlashJudge` contract address (0x hex) — the `slash_sig` EIP-712
    /// `verifyingContract`. Overrides `blockchain.slash_judge_address`.
    #[arg(long, value_name = "0xADDR")]
    pub slash_judge_address: Option<String>,

    /// EIP-712 `chainId` for both domains. Overrides `blockchain.chain_id`;
    /// defaults to Arbitrum Sepolia.
    #[arg(long, value_name = "ID")]
    pub chain_id: Option<u64>,

    /// Path to the buyer's Ethereum keystore that signs vouchers and the
    /// `openChannel` tx. Overrides `blockchain.eth_keystore`; defaults to
    /// `<data-dir>/keystore.json`. Password from `$DECDN_KEYSTORE_PASSWORD`,
    /// else a TTY prompt.
    #[arg(long, value_name = "PATH")]
    pub keystore: Option<PathBuf>,

    /// Data dir holding the persistent buyer-channel store (and the default
    /// keystore). Overrides `identity.data_dir`; defaults to the platform
    /// data dir.
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// Deposit (`µUSDC`) to escrow when opening a new channel (ignored on reuse).
    /// Overrides `blockchain.buyer_deposit_micro_usdc`; defaults to 10 USDC.
    /// Clamped up to the on-chain `minDeposit`.
    #[arg(long, value_name = "MICRO_USDC")]
    pub deposit_micro_usdc: Option<u64>,

    /// Overall timeout for the fetch, in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 30_000)]
    pub timeout_ms: u64,
}
