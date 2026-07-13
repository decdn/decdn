//! `decdn fetch` — the standalone client-side paid pull of a single
//! content-addressed blob over `cdn/client/v1` (issues #391, #940).
//!
//! The paying sibling of [`super::probe::ProbeArgs`]: a one-shot client command
//! that dials a node by explicit `--node-id`/`--addr`/`--relay-url` (or
//! auto-discovers one, #936) and pays per-MB from a `PaymentChannel`. The
//! channel is **auto-opened and reused** (#940): `fetch` looks up a live channel
//! with `--provider-address` in its persistent buyer-channel store, resuming
//! that channel's voucher watermark; if none exists it opens (and funds) one
//! on-chain and records it. So there is no `--channel-id` — the channel is
//! derived.
//!
//! The on-chain coordinates (RPC, contract addresses, chain id, keystore, data
//! dir) resolve **flag > `[blockchain]`/`[identity]` config > default**, so a
//! configured client runs `decdn fetch --node-id … --addr … --provider-address
//! … --hash … -o …` with no chain flags. Fields are kept as `String`/`Option`
//! (mirroring [`super::probe::ProbeArgs`]); the `cli` binary parses addresses
//! and merges the config.
//!
//! The network/chain/target/limit flags live in [`ClientFetchArgs`], shared
//! (via `#[command(flatten)]`) with `decdn bundle pull` (#391) so both client
//! commands resolve their coordinates and select a node identically.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Args;

/// The network, chain, target, and per-blob-limit flags shared by the paid
/// client commands (`decdn fetch` and `decdn bundle pull`, #391). Flattened into
/// each command's args so the resolution (`flag > config > default`) and
/// node-selection logic have one definition.
///
/// Reachability and the required chain coordinates are validated at runtime
/// rather than by clap, because the relay (`network.relay_urls`, #935) and the
/// chain coordinates (`[blockchain]`) can come from config, which clap cannot
/// see. The `--node-id`/`--provider-address`/`--addr` pairing IS enforced by
/// clap `requires`.
#[derive(Debug, Clone, Args)]
pub struct ClientFetchArgs {
    /// Target node id (iroh `EndpointId`, z-base32). Omit it to auto-discover:
    /// the active node set is read from `CapacityBond`, the region-nearest
    /// candidates probed, and one that holds the blob picked (#936). When set,
    /// `--provider-address` is required (they pair).
    #[arg(long, value_name = "ID", requires = "provider_address")]
    pub node_id: Option<String>,

    /// Direct socket address of the target node (e.g. `127.0.0.1:4433`). Only
    /// meaningful with an explicit `--node-id`; auto-discovery resolves the
    /// address itself, so this requires `--node-id`.
    #[arg(long, value_name = "HOST:PORT", requires = "node_id")]
    pub addr: Option<SocketAddr>,

    /// iroh relay URL to use for discovery-based resolution. Overrides
    /// `network.relay_urls` from config when set (#935).
    #[arg(long, value_name = "URL")]
    pub relay_url: Option<String>,

    /// The delivering node's Ethereum address (0x-prefixed hex). The channel is
    /// opened/reused against it, and the response `slash_sig` must recover to it
    /// (ADR 014 §1); a mismatch aborts the pull. Omit it when auto-discovering
    /// (no `--node-id`): it is derived from the selected node's registry entry.
    /// Pairs with `--node-id`, so it requires it.
    #[arg(long, value_name = "0xADDR", requires = "node_id")]
    pub provider_address: Option<String>,

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

    /// `CapacityBond` contract address (0x hex) — the active-node registry read
    /// when auto-discovering (no `--node-id`). Overrides
    /// `blockchain.capacity_bond_address`. Unused on the explicit-node path.
    #[arg(long, value_name = "0xADDR")]
    pub capacity_bond_address: Option<String>,

    /// Client region used to prefer same-region nodes when auto-discovering
    /// (#936). Matched by case-insensitive equality against each node's on-chain
    /// self-attested region (`CapacityBond` `regionHint`, ISO 3166-1 alpha-2 per
    /// ADR 030 — e.g. `US`, `DE`), so pass the same form operators register.
    /// Overrides `identity.region`; when unset (and no config region), discovery
    /// skips the region-first ordering.
    #[arg(long, value_name = "REGION")]
    pub region: Option<String>,

    /// EIP-712 `chainId` for both domains. Overrides `blockchain.chain_id`;
    /// defaults to Arbitrum Sepolia.
    #[arg(long, value_name = "ID")]
    pub chain_id: Option<u64>,

    /// Path to the buyer's Ethereum keystore that signs vouchers and the
    /// `openChannel` tx. Overrides `blockchain.eth_keystore`; when unset defaults
    /// to `keystore.json` under the (client-scoped) data dir. Password from
    /// `$DECDN_KEYSTORE_PASSWORD`, else a TTY prompt.
    #[arg(long, value_name = "PATH")]
    pub keystore: Option<PathBuf>,

    /// Data dir holding the persistent buyer-channel store (and the default
    /// keystore). Overrides `identity.data_dir`; when unset defaults to the
    /// client-scoped `~/.decdn/client` (not the node-shaped `~/.decdn`).
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// Deposit (`µUSDC`) to escrow when opening a new channel (ignored on reuse).
    /// Overrides `blockchain.buyer_deposit_micro_usdc`; defaults to 10 USDC.
    /// Clamped up to the on-chain `minDeposit`.
    #[arg(long, value_name = "MICRO_USDC")]
    pub deposit_micro_usdc: Option<u64>,

    /// Reject a delivery whose claimed total size exceeds this many MiB
    /// **before** buffering it — guards client memory against a provider that
    /// over-claims `total_bytes`. Defaults to 1024 MiB (the node's default
    /// serve ceiling); raise it to fetch larger blobs. For `bundle pull` this is
    /// the per-entry ceiling.
    #[arg(long, value_name = "MB", default_value_t = 1024)]
    pub max_blob_mb: u64,

    /// Abandon a fetch when the provider sends no data for this long, in
    /// milliseconds. This is the primary timeout (#1134): the clock resets on
    /// every byte received, so it catches a dead or stalled provider — what a
    /// timeout is *for* — without penalising transfer size or link speed. A
    /// 700 MiB blob on a slow link keeps going as long as bytes keep arriving.
    /// For `bundle pull` this applies per entry.
    #[arg(long, value_name = "MS", default_value_t = 30_000)]
    pub stall_timeout_ms: u64,

    /// Optional hard cap on the total wall-clock time of a single blob fetch, in
    /// milliseconds. **Unset by default** — normally you want `--stall-timeout-ms`,
    /// which bounds the fetch by provider health rather than by the clock.
    ///
    /// This used to be the primary (and only) mechanism, defaulting to 30 s, which
    /// made it a poor health signal: an overall deadline has to be sized against
    /// `blob size × link speed`, so it killed legitimate large or slow-but-healthy
    /// transfers while a value small enough to catch a dead node quickly could not
    /// serve a big blob at all. Set it only when you must bound total runtime
    /// regardless of whether the transfer is making progress. For `bundle pull`
    /// this applies per entry.
    #[arg(long, value_name = "MS")]
    pub timeout_ms: Option<u64>,
}

impl ClientFetchArgs {
    /// Inactivity bound for the streaming stage — the primary timeout (#1134).
    ///
    /// Returned as its own value (rather than a `decdn_client_pull::PullDeadlines`)
    /// because `decdn-common` is upstream of the pull crate in the dependency flow;
    /// the CLI assembles the two halves into a `PullDeadlines`.
    #[must_use]
    pub const fn stall_timeout(&self) -> Duration {
        Duration::from_millis(self.stall_timeout_ms)
    }

    /// Optional overall wall-clock cap; `None` unless `--timeout-ms` is given.
    #[must_use]
    pub fn hard_cap(&self) -> Option<Duration> {
        self.timeout_ms.map(Duration::from_millis)
    }
}

/// Arguments for `decdn fetch` — one blob to a file, atop [`ClientFetchArgs`].
#[derive(Debug, Clone, Args)]
pub struct FetchArgs {
    /// BLAKE3 hash of the blob to fetch: 64 hex chars (optional `0x` or `b3:`
    /// prefix — the `b3:` form is what bundle manifests carry).
    #[arg(long, value_name = "HASH")]
    pub hash: String,

    /// Destination path for the fetched blob. Written atomically
    /// (temp-in-dir then rename); an existing file is replaced.
    #[arg(short = 'o', long, value_name = "PATH")]
    pub output: PathBuf,

    #[command(flatten)]
    pub common: ClientFetchArgs,
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::ClientFetchArgs;
    use clap::Parser;
    use std::time::Duration;

    /// Parse `ClientFetchArgs` the way clap will at runtime, so the tests below
    /// assert on the real defaults rather than a hand-built struct that could
    /// drift from them.
    #[derive(Debug, Parser)]
    struct TestCli {
        #[command(flatten)]
        common: ClientFetchArgs,
    }

    fn parse(args: &[&str]) -> ClientFetchArgs {
        let mut with_bin = vec!["test"];
        with_bin.extend_from_slice(args);
        TestCli::parse_from(with_bin).common
    }

    /// The headline of #1134: out of the box a fetch has NO overall deadline, so
    /// blob size and link speed cannot kill a healthy transfer. Liveness comes
    /// from the stall bound instead.
    #[test]
    fn no_overall_deadline_by_default() {
        let c = parse(&[]);
        assert_eq!(c.hard_cap(), None);
        assert_eq!(c.stall_timeout(), Duration::from_secs(30));
    }

    /// The hard cap is opt-in, for a caller that must bound total runtime whether
    /// or not the transfer is progressing.
    #[test]
    fn hard_cap_is_opt_in() {
        let c = parse(&["--timeout-ms", "5000"]);
        assert_eq!(c.hard_cap(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn stall_timeout_is_overridable() {
        let c = parse(&["--stall-timeout-ms", "1500"]);
        assert_eq!(c.stall_timeout(), Duration::from_millis(1500));
        assert_eq!(c.hard_cap(), None);
    }

    /// `--max-blob-mb` is a memory ceiling, nothing more. It used to also scale the
    /// timeout (at an assumed 35 MiB/s), which is precisely the coupling #1134
    /// removed: a size flag has no business setting a deadline.
    #[test]
    fn max_blob_mb_does_not_influence_the_deadlines() {
        let small = parse(&["--max-blob-mb", "1"]);
        let huge = parse(&["--max-blob-mb", "1048576"]);
        assert_eq!(small.stall_timeout(), huge.stall_timeout());
        assert_eq!(small.hard_cap(), huge.hard_cap());
        assert_eq!(huge.hard_cap(), None);
    }
}
