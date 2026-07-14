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
    ///
    /// Must be non-zero: at 0 the deadline elapses on the first poll and every fetch
    /// fails instantly.
    #[arg(long, value_name = "MS", default_value_t = 30_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub stall_timeout_ms: u64,

    /// Hard cap on the total wall-clock time of a single blob fetch, in milliseconds.
    /// Defaults to 1 hour. For `bundle pull` this applies per entry.
    ///
    /// This is a leak guard, not the health signal — `--stall-timeout-ms` is what catches
    /// a dead provider. Lower it when you must bound total runtime regardless of whether
    /// the transfer is progressing.
    ///
    /// It must exceed TWICE `--stall-timeout-ms`. The cap bounds the whole exchange, and the
    /// open stage is bounded by the same stall budget, so both can run inside it
    /// consecutively; below `2 ×` the cap always elapses first and a stalled provider could
    /// never be detected.
    #[arg(long, value_name = "MS", default_value_t = 3_600_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub timeout_ms: u64,
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

    /// Overall wall-clock cap on one blob fetch — always present, so a fetch always
    /// terminates even against a provider that drip-feeds bytes to keep the stall
    /// deadline alive.
    ///
    /// # Why it exists, and why it is not the health signal
    ///
    /// It used to be the primary (and only) mechanism, at a 30 s default, which made it a
    /// poor health signal: an overall deadline has to be sized against
    /// `blob size × link speed`, so it killed legitimate large or slow-but-healthy
    /// transfers, while a value small enough to catch a dead node quickly could not serve
    /// a big blob at all (#1134).
    ///
    /// It cannot simply be removed, though, because inactivity is not liveness: the stall
    /// clock resets on ANY byte, so a provider trickling one byte per stall window would
    /// hang the fetch forever with no error. The default is therefore deliberately
    /// generous — far above any honest transfer under `--max-blob-mb` — and mirrors the
    /// node's own `BACKGROUND_FILL_HARD_CAP`.
    ///
    /// Returns a `Duration`, not an `Option<Duration>`. `--timeout-ms` has a default and clap
    /// rejects a zero, so the cap is ALWAYS present on this path; the `Option` this used to
    /// return was structurally always `Some`, and existed only to shape-match
    /// `PullDeadlines`'s optional cap — misinforming every reader and forcing a pointless
    /// match (#1145 review). A caller that wants the optional form wraps it.
    #[must_use]
    pub const fn hard_cap(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// Reject a deadline pair whose hard cap would silently disable stall detection.
    ///
    /// Clap enforces each knob is non-zero, but the two are only meaningful in relation to
    /// each other, and the relation is not the obvious one. `--timeout-ms` is a cap on the
    /// WHOLE exchange, and both CLI call sites build `PullDeadlines` with the open bound
    /// ALSO set from `--stall-timeout-ms` (a node that accepts a connection and never
    /// answers is as dead as one that stops mid-stream, so the same budget answers both).
    /// So before the stall clock even starts, up to `stall_timeout_ms` may already have
    /// gone on the open — and `PullStalled` can only fire if the cap outlasts both:
    ///
    /// ```text
    /// timeout_ms > open (= stall_timeout_ms) + stall_timeout_ms  =  2 × stall_timeout_ms
    /// ```
    ///
    /// A check of merely `timeout_ms > stall_timeout_ms` admits the whole band up to
    /// `2 × stall_timeout_ms`, where the health signal is still dead — no error, no
    /// warning, just a bound that cannot do its job.
    ///
    /// # This is the early check, not the enforcement
    ///
    /// `PullDeadlines::capped` is what actually enforces `hard_cap > open + stall`, on the
    /// type that holds all three values, and both CLI call sites go through it (#1145
    /// review). This exists so the user gets the error at argument-parse time — naming the
    /// flags they typed — rather than several frames into a fetch.
    ///
    /// It restates the rule rather than calling it because `decdn-common` sits UPSTREAM of
    /// `decdn-client-pull` in the dependency flow and cannot import `PullDeadlines`. The
    /// `2 ×` is that rule specialised to these two call sites, which set the open bound from
    /// `--stall-timeout-ms` as well. If a future `--open-timeout-ms` breaks that assumption,
    /// this check goes stale — but it can no longer go WRONG, because the constructor
    /// downstream still refuses to build a `PullDeadlines` whose cap cannot outlast its
    /// stages. That is the whole reason the invariant was moved onto the type.
    ///
    /// # Errors
    ///
    /// When `--timeout-ms` does not exceed twice `--stall-timeout-ms`.
    pub fn validate(&self) -> anyhow::Result<()> {
        let need = self.stall_timeout_ms.saturating_mul(2);
        anyhow::ensure!(
            self.timeout_ms > need,
            "--timeout-ms ({}) must exceed twice --stall-timeout-ms ({} × 2 = {}): the hard \
             cap bounds the whole exchange, and the open stage is bounded by the SAME stall \
             budget — so below that, the cap always elapses before the inactivity deadline \
             can fire and a stalled provider could never be detected",
            self.timeout_ms,
            self.stall_timeout_ms,
            need,
        );
        Ok(())
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

    /// The headline of #1134: the default deadlines are sized so that blob size and
    /// link speed cannot kill a healthy transfer. Liveness comes from the STALL bound;
    /// the overall cap is a leak guard set far above any honest transfer.
    #[test]
    fn the_stall_bound_is_the_health_signal_and_the_hard_cap_is_a_leak_guard() {
        let c = parse(&[]);
        assert_eq!(c.stall_timeout(), Duration::from_secs(30));
        assert_eq!(c.hard_cap(), Duration::from_hours(1));
    }

    /// A fetch must ALWAYS terminate (#1145 review). The stall clock resets on any
    /// byte, so with no overall cap a provider trickling one byte per stall window
    /// hangs `decdn fetch` forever, with no error and no diagnostic — and hangs the
    /// whole manifest for `bundle pull`. The cap is what makes that impossible, so it
    /// cannot be absent, and it cannot be zero.
    ///
    /// "Cannot be absent" is now a fact about the TYPE — `hard_cap()` returns a `Duration`,
    /// not an `Option<Duration>` — so the only thing left for a test to pin is that it cannot
    /// be zero, which is clap's job.
    #[test]
    fn a_fetch_always_has_an_overall_cap() {
        assert!(
            !parse(&[]).hard_cap().is_zero(),
            "an unbounded fetch can be hung forever by a drip-feeding provider"
        );
        assert!(
            TestCli::try_parse_from(["test", "--timeout-ms", "0"]).is_err(),
            "a zero hard cap would abort every fetch on the first poll"
        );
    }

    #[test]
    fn hard_cap_is_overridable() {
        let c = parse(&["--timeout-ms", "5000"]);
        assert_eq!(c.hard_cap(), Duration::from_secs(5));
    }

    /// The two deadlines are only meaningful in relation to each other, and the threshold
    /// is `2 × stall`, not `1 × stall` (#1145 review).
    ///
    /// Both CLI call sites set the OPEN bound from `--stall-timeout-ms` too, and
    /// `--timeout-ms` caps the whole exchange — so up to one stall budget can be spent on
    /// the open before the inactivity clock even starts. A cap of `stall + 1ms` therefore
    /// still leaves `PullStalled` unable to fire in practice: the health signal is quietly
    /// dead while both knobs look configured.
    ///
    /// The first version of this guard checked `timeout > stall` and pinned `5000/5001` as
    /// GOOD, which is the very band where the bug lives.
    #[test]
    fn a_hard_cap_that_would_disable_stall_detection_is_rejected() {
        assert!(
            parse(&["--stall-timeout-ms", "60000", "--timeout-ms", "1000"])
                .validate()
                .is_err(),
            "a cap below the stall budget makes a stalled provider undetectable"
        );
        assert!(
            parse(&["--stall-timeout-ms", "5000", "--timeout-ms", "5000"])
                .validate()
                .is_err(),
            "equal is no better: the cap's clock starts first, so it still always wins"
        );
        assert!(
            parse(&["--stall-timeout-ms", "5000", "--timeout-ms", "5001"])
                .validate()
                .is_err(),
            "a cap of stall+1ms leaves nothing for the stall clock: the open stage alone is \
             bounded by the same 5000ms, so the cap fires first unless the open took <1ms"
        );
        assert!(
            parse(&["--stall-timeout-ms", "5000", "--timeout-ms", "10000"])
                .validate()
                .is_err(),
            "exactly 2× is still not enough — the cap must strictly exceed open + stall"
        );
        assert!(
            parse(&["--stall-timeout-ms", "5000", "--timeout-ms", "10001"])
                .validate()
                .is_ok(),
            "past open + stall, the inactivity deadline can actually fire"
        );
        assert!(
            parse(&[]).validate().is_ok(),
            "the defaults (30s stall, 1h cap) must be a legal pair"
        );
    }

    #[test]
    fn stall_timeout_is_overridable() {
        let c = parse(&["--stall-timeout-ms", "1500"]);
        assert_eq!(c.stall_timeout(), Duration::from_millis(1500));
    }

    /// `stall_timeout()` feeds BOTH `PullDeadlines::open` and `.stall`, so a zero here
    /// elapses on the first poll of the stream open and kills every fetch. The node's
    /// config resolver already rejects the equivalent knob; the CLI must too.
    #[test]
    fn a_zero_stall_timeout_is_rejected() {
        assert!(TestCli::try_parse_from(["test", "--stall-timeout-ms", "0"]).is_err());
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
    }
}
