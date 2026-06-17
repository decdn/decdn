//! `decdn fetch` — the standalone client-side paid pull of a single
//! content-addressed blob over `cdn/client/v1` (issue #391).
//!
//! The sibling of [`super::probe::ProbeArgs`]: a one-shot client command that
//! dials a node by explicit `--node-id`/`--addr`/`--relay-url`. Unlike probe it
//! *pays* — so it also needs the channel the bytes are billed against
//! (`--channel-id`), the provider's Ethereum address to verify the delivery
//! `slash_sig` against (`--provider-address`), and the EIP-712 domains
//! (`--chain-id` plus the `PaymentChannel`/`SlashJudge` addresses).
//!
//! Opening/funding the channel is a separate concern (its own issue), exactly
//! as `bundle create` defers publishing: `fetch` assumes `--channel-id` names a
//! channel that is already open and **unused** (the requester starts vouchers
//! at nonce 1 — there is no client-side voucher-watermark store yet, so a
//! partially-spent channel would re-sign a stale nonce and be rejected).
//!
//! Fields are kept as `String` (mirroring [`super::probe::ProbeArgs`]) so this
//! crate stays free of an `alloy` dependency; the `cli` binary parses the
//! addresses/hashes.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Args;

/// Arguments for `decdn fetch`.
///
/// Reachability (a direct `--addr` or a relay) is validated at runtime rather
/// than by a clap `ArgGroup`, because the relay can also come from config
/// (`network.relay_urls`, #935) which clap cannot see.
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
    /// `network.relay_urls` from config when set (#935); omit it to use the
    /// configured relays.
    #[arg(long, value_name = "URL")]
    pub relay_url: Option<String>,

    /// The delivering node's Ethereum address (0x-prefixed hex). The response
    /// `slash_sig` must recover to it (ADR 014 §1); a mismatch aborts the pull.
    #[arg(long, value_name = "0xADDR")]
    pub provider_address: String,

    /// On-chain `channelId` (0x-prefixed 32-byte hex) of an already-open,
    /// unused `PaymentChannel` the bytes are billed against.
    #[arg(long, value_name = "0xHEX32")]
    pub channel_id: String,

    /// The ERC-20 (USDC) token address the channel is bound to (0x hex).
    #[arg(long, value_name = "0xADDR")]
    pub token: String,

    /// `PaymentChannel` contract address (0x hex) — the voucher EIP-712
    /// `verifyingContract`.
    #[arg(long, value_name = "0xADDR")]
    pub payment_channel_address: String,

    /// `SlashJudge` contract address (0x hex) — the `slash_sig` EIP-712
    /// `verifyingContract`.
    #[arg(long, value_name = "0xADDR")]
    pub slash_judge_address: String,

    /// EIP-712 `chainId` for both domains. Defaults to Arbitrum Sepolia.
    #[arg(long, value_name = "ID", default_value_t = 421_614)]
    pub chain_id: u64,

    /// Path to the buyer's Ethereum keystore (Web3 Secret Storage v3) that
    /// signs vouchers. The password is read from `$DECDN_KEYSTORE_PASSWORD`,
    /// else prompted on a TTY.
    #[arg(long, value_name = "PATH")]
    pub keystore: PathBuf,

    /// Overall timeout for the fetch, in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 30_000)]
    pub timeout_ms: u64,
}
