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

    /// Ignore the persisted peer store for selection this run and re-probe/discover
    /// afresh; the store is still updated from what this fetch learns.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    pub rediscover: bool,

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
    /// stalls is not a regression: it cools, and the fetch carries on from the
    /// other candidates and the direct holder over the SAME shared pool (each
    /// provider is its own lane, so no new on-chain deposit is escrowed),
    /// resuming the partial it already has. The only client-observable cost is a bounded
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

    /// Cap on concurrently-used holders (ADR 039): a fetch stripes a blob
    /// across at most this many holders at once. Enough to saturate typical
    /// downlinks without paying for marginal lanes. It also caps how many
    /// holders a `bundle pull` range-dedup entry stripes its ranges across.
    #[arg(long, value_name = "N", default_value_t = 4)]
    pub max_sources: usize,

    /// Stop after this many seconds with no verified progress. Without it, a
    /// terminal waits until Ctrl-C and a script stops after 10 minutes.
    #[arg(long, value_name = "SECS", value_parser = clap::value_parser!(u64).range(1..))]
    pub give_up_after_secs: Option<u64>,

    /// EIP-712 `chainId` for both domains. Overrides `blockchain.chain_id`;
    /// defaults to Arbitrum Sepolia.
    #[arg(long, value_name = "ID")]
    pub chain_id: Option<u64>,

    /// Path to the buyer's Ethereum keystore that signs vouchers and the
    /// `openChannel` tx. Overrides `blockchain.eth_keystore`; when unset defaults
    /// to `keystore.json` under the (client-scoped) data dir. Password from
    /// `$DECDN_KEYSTORE_PASSWORD`, else `--keystore-password-file`, else a TTY
    /// prompt.
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

    /// Hard cap on the total wall-clock time of the `CapacityBond` registry read
    /// (discovery), in milliseconds. Defaults to 1 hour.
    ///
    /// It does NOT bound the blob fetch: a progressing pull carries no overall wall-clock
    /// cap, so a healthy transfer of any size completes as long as the upstream keeps
    /// feeding it bytes (#1134). `--give-up-after-secs` bounds a fetch that stops making
    /// progress.
    ///
    /// Exceeding this budget on the registry read (including its retry schedule) is treated
    /// as a read failure, so a cached peer list is still used if one is present. It does NOT
    /// bound the probe fan-out that ranks candidates either — probing is bounded per node by
    /// the probe timeout, not by this flag.
    #[arg(long, value_name = "MS", default_value_t = 3_600_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub timeout_ms: u64,

    /// Adopt a delegated pool + capability instead of opening/reusing the
    /// caller's OWN pool. Pass the `dcap1:` token printed by `decdn pool assign`:
    /// it names the pool and authorizes THIS client's loaded key to spend against
    /// it up to the owner-set cap. The loaded keystore MUST be the delegate signer
    /// the token authorizes (a mismatch aborts). The delegate does not own the
    /// pool, so its funding recovery step cannot top up: an exhausted cap or a
    /// drained pool needs the owner to top up or issue a capability for a new
    /// signer key.
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
    /// Wall-clock cap on the `CapacityBond` registry read, including its ADR 012 retry
    /// schedule (#1349): the `--timeout-ms` value.
    ///
    /// A separate budget, not a shared one: sharing would make `bundle pull`'s per-entry
    /// fetch depend on how long the read took.
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

    /// The `--give-up-after-secs` override of the no-progress limit, or `None`
    /// for the default (a terminal waits, a script stops after 10 minutes).
    #[must_use]
    pub const fn give_up_after(&self) -> Option<Duration> {
        match self.give_up_after_secs {
            Some(secs) => Some(Duration::from_secs(secs)),
            None => None,
        }
    }

    /// Reject a flag combination clap cannot express.
    ///
    /// # Errors
    ///
    /// When `--provider-address` is set without `--node-id`.
    pub fn validate(&self) -> anyhow::Result<()> {
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
    /// that namespace's DAO-authorized origins. Absent => no namespace: no
    /// routing to a namespace's authorized origin set — served from cache, from
    /// peers, or from the serving node's OWN configured origin, which stays a
    /// serve target either way (`namespace_id` is a routing hint, not a serve
    /// gate — ADR 002).
    #[arg(long, value_name = "ID", value_parser = parse_fetch_namespace_id)]
    pub namespace: Option<u64>,

    /// Retrieval flags shared with `decdn bundle pull`: peer discovery, chain
    /// and payment-pool coordinates, the source cap and the give-up limit.
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
mod tests;
