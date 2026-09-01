//! `decdn fetch` — the standalone client-side paid pull of a single
//! content-addressed blob over `cdn/client/v1` (issues #391, #940).
//!
//! The paying sibling of [`super::probe::ProbeArgs`]: a one-shot client command
//! that dials a node by explicit `--node-id`/`--addr`/`--relay-url` (or
//! auto-discovers one, #936) and pays per-MB from the caller's own
//! `PaymentPool` deposit. The pool is **auto-opened and reused**: `fetch`
//! looks up the caller's live pool in its persistent buyer-pool store, resuming
//! its per-`(signer, provider)` lane watermark for the chosen provider; if none
//! exists it opens (and funds) one on-chain and records it. One pool fans out
//! to every provider the caller pays (ADR 003) — there is no per-provider open.
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

/// Validate `--region` at parse time, returning the NORMALIZED code so the
/// value that reaches discovery is already canonical.
///
/// Only the flag goes through this — a `[identity] region` from the config file
/// does not, so `select_candidates` still parses defensively. Rejecting there
/// too would turn a stale config key into a hard failure of every fetch, which
/// is a bigger behavior change than this fix is for.
///
/// The error names the accepted set rather than echoing the input back, matching
/// `decdn_protocol::InvalidRegion` (a region code reaches log fields and metric
/// labels).
fn parse_region_flag(raw: &str) -> Result<String, String> {
    decdn_protocol::Region::parse(raw)
        .map(|r| r.to_string())
        .ok_or_else(|| decdn_protocol::InvalidRegion.to_string())
}

/// The network, chain, target, and per-blob-limit flags shared by the paid
/// client commands (`decdn fetch` and `decdn bundle pull`, #391). Flattened into
/// each command's args so the resolution (`flag > config > default`) and
/// node-selection logic have one definition.
///
/// Reachability and the required chain coordinates are validated at runtime
/// rather than by clap, because the relay (`network.relay_urls`, #935) and the
/// chain coordinates (`[blockchain]`) can come from config, which clap cannot
/// see. `--node-id` requires `--provider-address` at the clap layer; the reverse
/// pairing (`--provider-address` needs `--node-id`) is enforced in `validate()`.
/// `--addr` requires `--node-id` at the clap layer.
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

    /// The delivering node's Ethereum address (0x-prefixed hex). The caller's
    /// pool lane for this provider is opened/reused against it, and the
    /// response `slash_sig` must recover to it (ADR 014 §1); a mismatch aborts
    /// the pull. Omit it when auto-discovering (no `--node-id`): it is derived
    /// from the selected node's registry entry.
    ///
    /// Requires `--node-id` (enforced in `validate()`, not by clap) — on its
    /// own it names no node to dial.
    #[arg(long, value_name = "0xADDR")]
    pub provider_address: Option<String>,

    /// JSON-RPC endpoint for on-chain pool open. Overrides
    /// `blockchain.rpc_url` from config.
    #[arg(long, value_name = "URL")]
    pub rpc_url: Option<String>,

    /// `PaymentPool` contract address (0x hex) — the voucher EIP-712
    /// `verifyingContract` and the `openPool` target. Overrides
    /// `blockchain.payment_pool_address`.
    #[arg(long, value_name = "0xADDR")]
    pub payment_pool_address: Option<String>,

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
    /// (#936). Matched against each node's on-chain self-attested region
    /// (`CapacityBond` `regionHint`, ISO 3166-1 alpha-2 per ADR 030 — e.g. `US`,
    /// `DE`); case and surrounding whitespace are normalized away, so any
    /// spelling of a valid code works. Overrides `identity.region`; when unset
    /// (and no config region), discovery skips the region-first ordering.
    ///
    /// Rejected at parse time if it is not an accepted code, so a request for
    /// locality never silently degrades to round-robin selection with the flag
    /// discarded.
    #[arg(long, value_name = "REGION", value_parser = parse_region_flag)]
    pub region: Option<String>,

    /// Latency-driven proxy warming (#1174, ADR 037): on a cache miss whose only
    /// holders are distant, route the paid request through a nearer bonded
    /// non-holder so it caches the blob and becomes the first regional copy.
    /// Engages only when it strictly helps (see `--proxy-warming-rtt-threshold-ms`
    /// / `--proxy-warming-margin-ms`); otherwise routes direct.
    ///
    /// **Defaults on (ADR 037 § Fallback).** A chosen proxy that declines or
    /// stalls is not a regression: `fetch` fails over to the next candidate and
    /// finally to the direct holder over the SAME shared pool (each provider is
    /// its own lane, so no new on-chain deposit is escrowed), resuming the
    /// partial it already has. The only client-observable cost is a bounded
    /// one-request latency premium the first time a locale warms a given blob.
    /// Pass `--proxy-warming false` to route direct and never warm.
    #[arg(long, value_name = "BOOL", default_value_t = true, action = clap::ArgAction::Set)]
    pub proxy_warming: bool,

    /// Proxy warming engages only when the best holder's RTT exceeds this many
    /// milliseconds (the holders are all distant). Below it, route direct.
    #[arg(long, value_name = "MS", default_value_t = 150)]
    pub proxy_warming_rtt_threshold_ms: u64,

    /// A candidate proxy must beat the best holder's RTT by at least this many
    /// milliseconds to be chosen — proxy warming is never a gamble.
    #[arg(long, value_name = "MS", default_value_t = 30)]
    pub proxy_warming_margin_ms: u64,

    /// Multi-source parallel fetch (ADR 039): fan a large blob out across
    /// several admissible holders at once via a shared-store scheduler with
    /// tail-stealing, rather than the single-source failover loop. Engages
    /// only when the blob clears `--multi-source-min-bytes` AND at least two
    /// admissible holders are found (see [`Self::multi_source_min_bytes`],
    /// [`Self::max_sources`]); a small blob or a single-holder blob always
    /// takes the single-source path regardless of this flag.
    ///
    /// **Defaults on.** Pass `--no-multi-source` to always use the
    /// single-source path.
    ///
    /// Read directly ONLY when `--no-multi-source` cannot also be set (e.g.
    /// after `overrides_with` has resolved a conflict some other way);
    /// callers wanting the effective value use [`Self::multi_source_enabled`],
    /// which also accounts for `--no-multi-source`.
    #[arg(long, default_value_t = true, overrides_with = "no_multi_source")]
    pub multi_source: bool,

    /// Off-switch for `--multi-source` (clap negation companion — mirrors the
    /// `--proxy-warming`/direct-route pairing above, but as a flag pair
    /// rather than a `bool`-valued flag). Never read directly outside
    /// [`Self::multi_source_enabled`]: clap has no built-in way to make one
    /// flag *write* another derive field, so the two fields are resolved by
    /// that method rather than by clap itself. `pub` only because callers
    /// outside this crate build `ClientFetchArgs` literals (test fixtures)
    /// rather than going through clap.
    #[arg(long, action = clap::ArgAction::SetTrue, overrides_with = "multi_source")]
    pub no_multi_source: bool,

    /// Cap on concurrently-used holders for a multi-source fetch — the initial
    /// segment count `admit_sources` admits and `multi_source_fetch` fans out
    /// across (ADR 039). Enough to saturate typical downlinks without paying
    /// for marginal lanes.
    #[arg(long, value_name = "N", default_value_t = 4)]
    pub max_sources: usize,

    /// Multi-source engagement floor, in bytes: a blob at or below this size
    /// always takes the single-source path — fanning it out across several
    /// lanes only adds redemption overhead (one `redeem` call per lane) for no
    /// parallelism win on a transfer that small. Defaults to 64 MiB.
    #[arg(long, value_name = "BYTES", default_value_t = 67_108_864)]
    pub multi_source_min_bytes: u64,

    /// No-verified-progress deadline before a multi-source worker's remaining
    /// range is reassigned to another source (ADR 039), in milliseconds. Must
    /// sit comfortably above `4 MiB / min-expected-throughput` — progress is
    /// checkpoint-granular at that size, so a smaller deadline can falsely
    /// reassign a healthy-but-slow source mid-checkpoint. Defaults to 10 s.
    #[arg(long, value_name = "MS", default_value_t = 10_000)]
    pub unit_deadline_ms: u64,

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

    /// Deposit (`µUSDC`) to escrow when OPENING a pool, and the target each
    /// top-up refills the pool toward once it has served verified bytes.
    /// Overrides `blockchain.buyer_working_deposit_micro_usdc`; default
    /// 10 USDC. Must be nonzero (openPool reverts on a zero deposit).
    #[arg(long, value_name = "MICRO_USDC")]
    pub working_deposit_micro_usdc: Option<u64>,

    /// Client-side size ceiling: refuse a delivery whose claimed total size
    /// exceeds this many MiB before pulling any bytes. Defaults to 1 TiB
    /// (1,048,576 MiB) — far above any real blob, so `decdn fetch` and
    /// `bundle pull` download normal content by its hash without tuning, yet low
    /// enough to bound what a provider's over-claimed `total_bytes` can cost
    /// locally. Lower it as a tighter budget guard, or set `0` to disable the
    /// ceiling entirely. For `bundle pull` this is the per-entry ceiling.
    ///
    /// The cost it bounds is on disk, not memory: the streaming pull buffers only
    /// received bytes in RAM, so RAM stays bounded whatever the claim, and a
    /// provider that over-claims `total_bytes` fails bao verification against the
    /// requested hash regardless of this value. But the ranged store pre-sizes a
    /// sparse bao outboard sidecar (~0.4% of `total_bytes`) via `set_len` at
    /// creation, so without a ceiling an absurd claim could surface a local disk
    /// or quota error before verification rejects it. This gate caps that pre-size
    /// (1 TiB → ~4 GiB) by refusing the claim first.
    #[arg(long, value_name = "MB", default_value_t = 1_048_576)]
    pub max_blob_mb: u64,

    /// Refuse a provider that quotes a per-MB rate above this many USDC base units
    /// **before** paying any voucher (#1375). Guards against a provider that quotes
    /// low on the probe then high on the stream. `0` (the default) = no ceiling.
    /// For `bundle pull` this applies per entry. Note: unlike the node's automatic
    /// node-to-node pull (which also caps at the candidate's own probe rate), the
    /// CLI applies only this absolute value — set it to opt into buyer rate
    /// protection on the client path.
    #[arg(long, value_name = "UNITS", default_value_t = 0)]
    pub max_rate_per_mb: u64,

    /// Trailing window, in milliseconds, over which the fetch measures upstream throughput
    /// (#1797). This is the primary timeout: bytes are counted off the QUIC stream sub-frame,
    /// and the fetch is abandoned when the bytes across this window fall below
    /// `--min-throughput-bps`. Frame-size-independent, so a 700 MiB blob on a slow link keeps
    /// going as long as it stays above the floor. For `bundle pull` this applies per entry.
    ///
    /// Must be non-zero: at 0 the throughput floor is unsatisfiable and every fetch fails
    /// instantly.
    #[arg(long, value_name = "MS", default_value_t = 30_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub stall_timeout_ms: u64,

    /// Minimum sustained upstream throughput in bytes per second over `--stall-timeout-ms`
    /// (#1797). A stream that stays below this floor for a full window is abandoned — catching
    /// both a wedged provider (throughput to zero) and a slow drip (a trickle that never trips
    /// a bare idle timeout). `0` disables the throughput test and leaves pure idle detection:
    /// at least one byte per window. For `bundle pull` this applies per entry.
    #[arg(long, value_name = "BPS", default_value_t = 4096)]
    pub min_throughput_bps: u64,

    /// Hard cap on the total wall-clock time of the `CapacityBond` registry read, and
    /// separately of a single blob fetch, in milliseconds. Defaults to 1 hour. For
    /// `bundle pull` the fetch half applies per entry.
    ///
    /// This is a leak guard, not the health signal — `--stall-timeout-ms` is what catches
    /// a dead provider. Lower it when you must bound total runtime regardless of whether
    /// the transfer is progressing.
    ///
    /// The registry read (including its retry schedule) gets its own budget of this size
    /// rather than sharing the transfer's, so `bundle pull`'s per-entry accounting is
    /// unaffected. Before #1349 nothing bounded it at all: a failing read could burn its
    /// full retry schedule with `--timeout-ms 5000` set. Exceeding the budget is treated
    /// as a read failure, so a cached peer list is still used if one is present.
    ///
    /// It does NOT bound the probe fan-out that ranks candidates. Probing is bounded per
    /// node by the probe timeout, not by this flag.
    ///
    /// It must exceed TWICE `--stall-timeout-ms`. The cap bounds the whole exchange, and the
    /// open stage is bounded by the same stall budget, so both can run inside it
    /// consecutively; below `2 ×` the cap always elapses first and a stalled provider could
    /// never be detected.
    #[arg(long, value_name = "MS", default_value_t = 3_600_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub timeout_ms: u64,

    /// Adopt a delegated pool + capability instead of opening/reusing the
    /// caller's OWN pool. Pass the `dcap1:` token printed by `decdn pool assign`:
    /// it names the pool and authorizes THIS client's loaded key to spend against
    /// it up to the owner-set cap. The loaded keystore MUST be the delegate signer
    /// the token authorizes (a mismatch aborts). The delegate does not own the
    /// pool, so reactive top-up is disabled on this path — an exhausted cap or
    /// drained pool needs the owner to top up or re-issue a higher-cap capability.
    /// Mutually exclusive with `--capability-file`.
    #[arg(long, value_name = "TOKEN", conflicts_with = "capability_file")]
    pub capability: Option<String>,

    /// Read the `dcap1:` capability token from a file (its whole trimmed
    /// contents) rather than the command line, keeping it out of shell history
    /// and the process table. Mutually exclusive with `--capability`.
    #[arg(long, value_name = "PATH", conflicts_with = "capability")]
    pub capability_file: Option<PathBuf>,
}

impl ClientFetchArgs {
    /// Throughput-floor window for the streaming stage — the primary timeout (#1797).
    ///
    /// Returned as its own value (rather than a `decdn_client_pull::PullDeadlines`)
    /// because `decdn-common` is upstream of the pull crate in the dependency flow;
    /// the CLI assembles the halves into a `PullDeadlines`.
    #[must_use]
    pub const fn stall_timeout(&self) -> Duration {
        Duration::from_millis(self.stall_timeout_ms)
    }

    /// Minimum sustained upstream throughput (bytes/sec) over [`Self::stall_timeout`]; `0` =
    /// idle detection only (#1797). Assembled with the window into a `PullDeadlines`.
    #[must_use]
    pub const fn min_throughput_bps(&self) -> u64 {
        self.min_throughput_bps
    }

    /// Overall wall-clock cap on one blob fetch — always present, so a fetch always
    /// terminates even against a provider that drip-feeds bytes to keep the stall
    /// deadline alive.
    ///
    /// # Why it exists, and why it is not the health signal
    ///
    /// It is a poor health signal on its own: an overall deadline has to be sized against
    /// `blob size × link speed`, so it kills legitimate large or slow-but-healthy
    /// transfers, while a value small enough to catch a dead node quickly cannot serve
    /// a big blob at all (#1134).
    ///
    /// It cannot simply be removed, though, because inactivity is not liveness: the stall
    /// clock resets on ANY byte, so a provider trickling one byte per stall window would
    /// hang the fetch forever with no error. The default is therefore deliberately
    /// generous — far above any honest transfer of a typical blob.
    ///
    /// Returns a `Duration`, not an `Option<Duration>`. `--timeout-ms` has a default and clap
    /// rejects a zero, so the cap is ALWAYS present on this path; an `Option` here would be
    /// structurally always `Some`, and would exist only to shape-match
    /// `PullDeadlines`'s optional cap — misinforming every reader and forcing a pointless
    /// match (#1145 review). A caller that wants the optional form wraps it.
    ///
    /// The same value also bounds the registry read, as a SEPARATE budget rather than a
    /// shared one — see [`Self::discovery_cap`].
    #[must_use]
    pub const fn hard_cap(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// Wall-clock cap on the `CapacityBond` registry read, including its ADR 012 retry
    /// schedule (#1349).
    ///
    /// The same `--timeout-ms` value as [`Self::hard_cap`], deliberately not a knob of
    /// its own. The read had no bound at all, and a dedicated flag would be a fifth
    /// timeout for operators to reason about when the one they already reach for —
    /// "how long may this command take" — is the right question.
    ///
    /// A separate budget, not a shared one: sharing would make `bundle pull`'s per-entry
    /// fetch cap depend on how long the read took, turning a documented per-entry bound
    /// into a whole-run one.
    ///
    /// # What this does NOT bound
    ///
    /// The probe fan-out. `fetch` probes `SELECT_K` candidates after the read, and
    /// `bundle pull` probes per entry; neither is inside this budget. Named here because
    /// the obvious reading of "discovery" includes probing, and it does not.
    #[must_use]
    pub const fn discovery_cap(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// Reject a deadline pair whose hard cap would silently disable stall detection.
    ///
    /// Clap enforces each knob is non-zero, but the two are only meaningful in relation to
    /// each other, and the relation is not the obvious one. `--timeout-ms` is a cap on the
    /// WHOLE exchange, and both CLI call sites build `PullDeadlines` with the open bound
    /// ALSO set from `--stall-timeout-ms` (a node that accepts a connection and never
    /// answers is as dead as one that stops mid-stream, so the same budget answers both).
    /// So before the streaming window even starts, up to `stall_timeout_ms` may already have
    /// gone on the open — and the throughput floor can only fire if the cap outlasts both:
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
    /// `PullDeadlines::capped` is what actually enforces `hard_cap > open + window`, on the
    /// type that holds the values, and both CLI call sites go through it (#1145
    /// review). This exists so the user gets the error at argument-parse time — naming the
    /// flags they typed — rather than several frames into a fetch.
    ///
    /// It restates the rule rather than calling it because `decdn-common` sits UPSTREAM of
    /// `decdn-client-pull` in the dependency flow and cannot import `PullDeadlines`. The
    /// `2 ×` is that rule specialised to these two call sites, which set the open bound from
    /// `--stall-timeout-ms` as well. If a future `--open-timeout-ms` breaks that assumption,
    /// this check goes stale — but it cannot go WRONG, because the constructor
    /// downstream still refuses to build a `PullDeadlines` whose cap cannot outlast its
    /// stages. That is why the invariant lives on the type.
    ///
    /// # Errors
    ///
    /// When `--timeout-ms` does not exceed twice `--stall-timeout-ms`.
    ///
    /// When `--provider-address` is set without `--node-id`.
    pub fn validate(&self) -> anyhow::Result<()> {
        let need = self.stall_timeout_ms.saturating_mul(2);
        anyhow::ensure!(
            self.timeout_ms > need,
            "--timeout-ms ({}) must exceed twice --stall-timeout-ms ({} × 2 = {}): the hard \
             cap bounds the whole exchange, and the open stage is bounded by the SAME window \
             budget — so below that, the cap always elapses before the throughput floor \
             can fire and a stalled provider could never be detected",
            self.timeout_ms,
            self.stall_timeout_ms,
            need,
        );

        // `--provider-address` is the delivering node's address on the
        // explicit-node path, where it pairs with `--node-id`. Standing alone it
        // names no node to dial, so it is meaningless.
        anyhow::ensure!(
            self.provider_address.is_none() || self.node_id.is_some(),
            "--provider-address requires --node-id (the delivering node to dial); alone it \
             names no node",
        );
        Ok(())
    }

    /// The effective multi-source kill switch: on by default, off if either
    /// `--no-multi-source` was passed (regardless of `--multi-source`'s own
    /// value — the negation always wins) or `--multi-source` was explicitly
    /// set to `false`.
    #[must_use]
    pub const fn multi_source_enabled(&self) -> bool {
        self.multi_source && !self.no_multi_source
    }

    /// The `dcap1:` capability token this fetch adopts, if any: the inline
    /// `--capability` value, or the trimmed contents of `--capability-file`.
    /// `None` selects the self-owned pool path (unchanged). Clap's
    /// `conflicts_with` guarantees at most one of the two is set.
    ///
    /// # Errors
    ///
    /// When `--capability-file` is set but the file cannot be read.
    pub fn resolve_capability_token(&self) -> anyhow::Result<Option<String>> {
        if let Some(token) = &self.capability {
            return Ok(Some(token.clone()));
        }
        match &self.capability_file {
            Some(path) => {
                let raw = std::fs::read_to_string(path).map_err(|e| {
                    anyhow::anyhow!("read --capability-file {}: {e}", path.display())
                })?;
                Ok(Some(raw.trim().to_string()))
            }
            None => Ok(None),
        }
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

    /// Namespace the content is published under (ADR 002 § Retrieval by
    /// namespace). When set, the serving node routes a cache-miss origin pull to
    /// that namespace's DAO-authorized origins. Absent => no namespace: served
    /// best-effort from cache / DHT only, with no authorized origins.
    #[arg(long, value_name = "ID", value_parser = parse_fetch_namespace_id)]
    pub namespace: Option<u64>,

    /// Retrieval flags shared with `decdn fetch`: peer discovery, payment
    /// pool, and output handling.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}

/// Parse a `--namespace` id (rejecting the reserved `0` — namespace 0 is "no
/// namespace"; omit the flag for best-effort retrieval). Shared by `decdn fetch`
/// and `decdn bundle pull`.
pub(crate) fn parse_fetch_namespace_id(s: &str) -> Result<u64, String> {
    let id: u64 = s
        .parse()
        .map_err(|_| format!("invalid namespace id: {s}"))?;
    if id == 0 {
        return Err("namespace id must be >= 1 (omit --namespace for no namespace)".to_string());
    }
    Ok(id)
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

    /// `--namespace` accepts a non-zero id, rejects the reserved `0` (the
    /// `NO_NAMESPACE` sentinel — users omit the flag instead), and rejects
    /// non-numeric input. Mirrors the publish-side `assign` parser's coverage.
    #[test]
    fn parse_fetch_namespace_id_validates() {
        assert_eq!(super::parse_fetch_namespace_id("7"), Ok(7));
        assert_eq!(super::parse_fetch_namespace_id("1"), Ok(1));

        let zero = super::parse_fetch_namespace_id("0").expect_err("0 must be rejected");
        assert!(
            zero.contains(">= 1"),
            "0 error should point to the floor: {zero}"
        );

        let nan = super::parse_fetch_namespace_id("abc").expect_err("non-numeric must be rejected");
        assert!(
            nan.contains("invalid namespace id"),
            "non-numeric error should name the field: {nan}"
        );
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

    /// `--region` is validated at parse time, not silently discarded. Before
    /// this, `--region usa` parsed fine and then failed `Region::parse` deep in
    /// discovery, so the user got round-robin selection with no hint that the
    /// flag they passed had been thrown away.
    #[test]
    fn an_unrecognized_region_flag_is_rejected_not_ignored() {
        let err = TestCli::try_parse_from(["test", "--region", "usa"])
            .expect_err("`usa` is not an ISO 3166-1 alpha-2 code");
        let msg = err.to_string();
        assert!(
            msg.contains("ISO 3166-1"),
            "the error names the format: {msg}"
        );

        // Any spelling of a valid code is accepted, and normalized on the way in
        // so discovery compares canonical values.
        assert_eq!(parse(&["--region", " us "]).region.as_deref(), Some("US"));
        assert_eq!(parse(&["--region", "De"]).region.as_deref(), Some("DE"));
        assert_eq!(parse(&[]).region, None, "the flag stays optional");
    }

    /// A fetch must ALWAYS terminate (#1145 review). The stall clock resets on any
    /// byte, so with no overall cap a provider trickling one byte per stall window
    /// hangs `decdn fetch` forever, with no error and no diagnostic — and hangs the
    /// whole manifest for `bundle pull`. The cap is what makes that impossible, so it
    /// cannot be absent, and it cannot be zero.
    ///
    /// "Cannot be absent" is a fact about the TYPE — `hard_cap()` returns a `Duration`,
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

    /// `--timeout-ms` bounds discovery as well as the transfer (#1349), from the
    /// same value and with no separate flag. Before this, `--timeout-ms 5000`
    /// could still sit through the registry read's full 36 s retry schedule
    /// before the transfer it capped had started.
    ///
    /// Asserted as equality to `hard_cap` rather than a literal so the two
    /// cannot silently diverge: if a future change gives discovery its own
    /// knob, this is the test that says so out loud.
    #[test]
    fn discovery_is_bounded_by_the_same_flag() {
        let c = parse(&["--timeout-ms", "5000"]);
        assert_eq!(c.discovery_cap(), Duration::from_secs(5));
        assert_eq!(c.discovery_cap(), c.hard_cap());
        assert_eq!(
            parse(&[]).discovery_cap(),
            parse(&[]).hard_cap(),
            "the default is shared too — discovery is never left unbounded"
        );
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
    /// Hence the `5000/5001` case below: it clears a naive `timeout > stall` check while
    /// sitting squarely in the dead band, so it is pinned as an ERROR.
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

    /// `--max-blob-mb` is an optional size ceiling, nothing more. It does not scale
    /// the timeout: a size flag has no business setting a deadline (#1134).
    #[test]
    fn max_blob_mb_does_not_influence_the_deadlines() {
        let small = parse(&["--max-blob-mb", "1"]);
        let huge = parse(&["--max-blob-mb", "1048576"]);
        assert_eq!(small.stall_timeout(), huge.stall_timeout());
        assert_eq!(small.hard_cap(), huge.hard_cap());
    }

    /// `--provider-address` alone (no `--node-id`) names no node to dial and is
    /// rejected by `validate()`, even though clap itself admits it (the `requires`
    /// pairing runs only from `--node-id`'s side, so this direction needs its own
    /// runtime check).
    #[test]
    fn provider_address_alone_is_rejected_by_validate() {
        let addr = "0x0000000000000000000000000000000000000001";
        let c = parse(&["--provider-address", addr]);
        let err = c
            .validate()
            .expect_err("provider-address alone names no node");
        let msg = err.to_string();
        assert!(msg.contains("--node-id"), "{msg}");
    }

    #[test]
    fn provider_address_with_node_id_is_the_unchanged_auto_open_path() {
        let addr = "0x0000000000000000000000000000000000000001";
        let c = parse(&["--node-id", "n", "--provider-address", addr]);
        assert!(c.validate().is_ok(), "explicit node + provider still valid");
    }

    #[test]
    fn node_id_alone_is_still_a_clap_error() {
        // `--node-id` keeps `requires = "provider_address"`, enforced at the clap layer.
        assert!(
            TestCli::try_parse_from(["test", "--node-id", "n"]).is_err(),
            "--node-id without --provider-address is a parse error"
        );
    }

    /// No capability flag => the self-owned path (`None`), unchanged.
    #[test]
    fn capability_token_absent_is_none() {
        assert_eq!(parse(&[]).resolve_capability_token().unwrap(), None);
    }

    /// `--capability` returns the inline token verbatim.
    #[test]
    fn capability_token_inline_is_returned() {
        let token = parse(&["--capability", "dcap1:abc"])
            .resolve_capability_token()
            .unwrap();
        assert_eq!(token.as_deref(), Some("dcap1:abc"));
    }

    /// `--multi-source` defaults on; `--no-multi-source` is the off-switch.
    /// [`ClientFetchArgs::multi_source_enabled`] is the resolved value every
    /// caller reads (mirroring `--proxy-warming`'s bool style above, but as a
    /// flag pair rather than a `bool`-valued flag). `overrides_with` clears
    /// the OTHER flag's occurrence, so whichever of the pair appears LAST on
    /// the command line wins.
    #[test]
    fn multi_source_defaults_on_and_no_multi_source_disables_it() {
        assert!(parse(&[]).multi_source_enabled(), "defaults on");
        assert!(
            !parse(&["--no-multi-source"]).multi_source_enabled(),
            "--no-multi-source disables it"
        );
        assert!(
            parse(&["--no-multi-source", "--multi-source"]).multi_source_enabled(),
            "a later --multi-source overrides an earlier --no-multi-source"
        );
    }

    /// `--capability-file` returns the file's trimmed contents; the two forms are
    /// mutually exclusive at the clap layer.
    #[test]
    fn capability_token_from_file_is_trimmed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cap.txt");
        std::fs::write(&path, "  dcap1:fromfile\n").unwrap();
        let token = parse(&["--capability-file", path.to_str().unwrap()])
            .resolve_capability_token()
            .unwrap();
        assert_eq!(token.as_deref(), Some("dcap1:fromfile"));

        assert!(
            TestCli::try_parse_from(["test", "--capability", "dcap1:a", "--capability-file", "x",])
                .is_err(),
            "--capability and --capability-file are mutually exclusive"
        );
    }
}
