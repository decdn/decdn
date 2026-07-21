//! Client-side node discovery (#936): read the active node set from
//! `CapacityBond.getActiveNodes` so `decdn fetch`/`probe` can pick a node
//! instead of being handed an explicit `--node-id`/`--addr`/`--provider-address`.
//!
//! This is the **read + select** half, plus the peer cache that backs it up.
//! Dialing the chosen node uses its iroh `NodeId` via a discovery-enabled
//! endpoint (`presets::N0` / configured `[network.discovery]`) — the same
//! mechanism the node uses — so the registry `multiaddrs` field is not decoded
//! here; a self-contained `multiaddrs` decoder is the deferred, more-
//! decentralized fallback (#936 § fallback).
//!
//! `bootstrap_nodes` is the entry point: it wraps the registry read in ADR
//! 012's retry schedule and persists the result to `peers.json` under the
//! client data dir, falling back to that file when the registry cannot be read.
//! That cache is the only filesystem state this module owns.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, U256};
use alloy::providers::ProviderBuilder;
use anyhow::Context;
use decdn_common::redact::{sanitize_err_chain, sanitize_rpc_display};
use decdn_incentive::capacity_bond::CapacityBond;
use iroh::PublicKey;
use serde::{Deserialize, Serialize};

/// Page size for the paginated `getActiveNodes` read (ADR 019 § Step 3.3's
/// worked-example limit). At `PoC` scale (tens of nodes) one page suffices; the
/// loop preserves the pattern for production scale.
const PAGE_SIZE: u64 = 100;

/// Backoff before each retry of a failed `getActiveNodes` page call — ADR 012
/// § Bootstrap step 3: "retry 3× exponential backoff (1 s, 5 s, 30 s)". The
/// length of the table is the retry count, so a fully-failing page costs
/// 1 + 5 + 30 = 36 s across four attempts.
///
/// The budget is **per page**: [`paginate_with_retry`] resets it on each page,
/// so a read that spans N pages can sleep up to 36 s × N. At `PoC` scale
/// (one page, see [`PAGE_SIZE`]) that is the 36 s. Either way the wait is
/// silent from the caller's point of view — nothing streams progress out of
/// this loop — so it is time `decdn fetch` appears to hang.
///
/// Deliberately not `decdn_config_types::RetryPolicy`: that is a *doubling*
/// policy with jitter, scoped to node-side origin fetches — its defaults are
/// 100/200/400 ms, and even seeded with a 1 s first step it would give
/// 1/2/4 s, not 1/5/30. Keeping the schedule literal also keeps an origin-side
/// config vocabulary out of the client's discovery path. (Nothing new would be
/// *linked*: `decdn-config-types` is already in this crate's graph via
/// `decdn-common`. The objection is layering, not the build graph.)
const REGISTRY_RETRY_BACKOFF: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(30),
];

/// Filename of the peer cache inside the resolved client data dir.
const PEER_CACHE_FILE: &str = "peers.json";

/// Version tag written into (and required of) the peer cache file, so a future
/// shape change is a cache miss rather than a decode error. For that to hold,
/// [`read_peer_cache`] decodes the tag through [`CacheVersion`] *before* the
/// body — a whole-`PeerCache` decode would fail on the changed `peers` shape
/// and never reach the check.
const PEER_CACHE_VERSION: u32 = 1;

/// The outermost context on a failed bootstrap — the wording pinned by ADR 012
/// § Bootstrap step 4. `decdn`'s error boundary renders `{err:#}`
/// (`sanitize_err_chain`), so the user sees this sentence followed by the
/// underlying cause chain.
///
/// Applied on *any* registry read failure with no usable cache, which is wider
/// than the ADR's "all retries exhausted": a malformed `rpc_url` fails in
/// `active_nodes` before the retry loop is ever entered.
pub const BOOTSTRAP_UNREACHABLE: &str =
    "Cannot reach bootstrap sources. Check network connectivity and RPC endpoint configuration.";

/// A node the client may fetch from, distilled from a registry `NodeInfo`.
/// `multiaddrs` is intentionally dropped — dialing is by `node_id` via iroh
/// discovery (see module docs).
///
/// Serializable so the resolved set can be persisted to the peer cache and
/// reloaded when the registry is unreachable (ADR 012 § Bootstrap step 4).
///
/// That encoding is a private implementation detail of `peers.json`, **not** a
/// stable format: the JSON keys are the field names, and the bytes are
/// codec-dependent (iroh renders `PublicKey` as z-base-32 under a
/// human-readable codec and as raw bytes otherwise). The version tag that makes
/// a format change safe lives on the private `PeerCache` wrapper, so nothing
/// outside this module should persist or transmit a `NodeCandidate`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCandidate {
    /// iroh endpoint id — dialed via discovery, never needs an explicit addr.
    pub node_id: PublicKey,
    /// The node's Ethereum address — the `--provider-address` discovery derives
    /// (the channel is opened/reused against it and the `slash_sig` verified
    /// against it).
    pub eth_address: Address,
    /// The node's self-attested region (ISO 3166-1 alpha-2, ADR 030), used for
    /// locality-aware selection. Always a string from the registry — empty when
    /// the node registered without one.
    pub region_hint: String,
}

/// Distill a registry `NodeInfo` into a [`NodeCandidate`], or `None` if it is
/// not currently active or its `nodeId` is not a valid ed25519 key.
///
/// The `active` field is trusted as a fast filter; a node that is stale-active
/// (e.g. mid-unbonding) is harmless here because the downstream probe-and-rank
/// step still has to reach and serve from it, so an unservable node falls out
/// of selection anyway.
fn candidate_from(info: &CapacityBond::NodeInfo) -> Option<NodeCandidate> {
    if !info.active {
        return None;
    }
    let node_id = PublicKey::from_bytes(&info.nodeId.0).ok()?;
    Some(NodeCandidate {
        node_id,
        eth_address: info.ethAddress,
        region_hint: info.regionHint.clone(),
    })
}

/// Whether a `getActiveNodes` failure is deterministic — the same call will
/// fail the same way however often it is repeated, so retrying it only burns
/// the 36 s backoff schedule before reporting the error it was always going to
/// report.
///
/// This matters because the two likeliest first-run mistakes both land here: a
/// typo'd `blockchain.capacity_bond_address` (the call returns `0x`, decoded as
/// [`ZeroData`](alloy::contract::Error::ZeroData)) and an expired or wrong RPC
/// API key (HTTP 401/403). Without this check either one makes `decdn fetch`
/// sit silently for 36 s and then blame network connectivity.
///
/// Note `TransportErrorKind::is_retry_err` is deliberately *not* used at the
/// transport layer: it is an allowlist of 429/503/missing-batch, so it would
/// classify an ordinary connection refusal as non-retryable and defeat the
/// ADR's retry entirely. This is the complementary denylist — retry unless we
/// know better. `ErrorPayload::is_retry_err` *is* used, because there the
/// allowlist is the right shape: see below.
fn is_permanent(err: &alloy::contract::Error) -> bool {
    use alloy::contract::Error as ContractError;
    use alloy::transports::{RpcError, TransportErrorKind};

    match err {
        ContractError::TransportError(e) => match e {
            // A JSON-RPC error response is usually deterministic — an unknown
            // method, a revert, a rejected key. But providers also signal rate
            // limiting this way, over HTTP 200 with an error body, so the HTTP
            // check below never sees it: Infura's -32005, Alchemy's -32016,
            // QuickNode's -32007/-32012, and a plain 429 in the JSON `code`.
            // Those are exactly what the backoff schedule is for, so defer to
            // alloy's list of them.
            RpcError::ErrorResp(resp) => !resp.is_retry_err(),
            RpcError::UnsupportedFeature(_)
            | RpcError::LocalUsageError(_)
            | RpcError::SerError(_) => true,
            // 4xx other than 429 is a client-side fault — a wrong path, an
            // unauthorized or expired API key. 429 stays retryable.
            RpcError::Transport(TransportErrorKind::HttpError(h)) => {
                (400..500).contains(&h.status) && !h.is_rate_limit_err()
            }
            // Everything else is transport-shaped: refused connections, resets,
            // timeouts, truncated bodies. Exactly what the schedule is for.
            _ => false,
        },
        // Only a transport failure can be transient. Every other variant is a
        // contract-level fault that repeats identically: no contract at the
        // configured address (`ZeroData` — the typo'd-address case), an ABI
        // that does not match our binding, an unknown function or selector, a
        // failed deployment.
        _ => true,
    }
}

/// Drive `fetch_page` across the paginated `getActiveNodes` read, retrying each
/// page on the ADR 012 § Bootstrap step 3 schedule
/// ([`REGISTRY_RETRY_BACKOFF`]) and distilling every entry through
/// [`candidate_from`].
///
/// Split out from [`active_nodes`] so the retry and pagination control flow is
/// drivable from a test without a live RPC — the schedule alone is a `const`
/// that proves nothing about the loop that reads it.
///
/// # Errors
///
/// Fails when a page's retries are exhausted, or immediately when the failure
/// is [`is_permanent`].
async fn paginate_with_retry<F, Fut>(fetch_page: F) -> anyhow::Result<Vec<NodeCandidate>>
where
    F: Fn(u64) -> Fut,
    Fut: Future<Output = Result<Vec<CapacityBond::NodeInfo>, alloy::contract::Error>>,
{
    let mut out = Vec::new();
    let mut offset = 0u64;
    loop {
        // The retry budget is per page — see `REGISTRY_RETRY_BACKOFF`.
        let mut attempt = 0usize;
        let page = loop {
            match fetch_page(offset).await {
                Ok(page) => break page,
                Err(e) if is_permanent(&e) => {
                    return Err(e).with_context(|| {
                        format!(
                            "CapacityBond.getActiveNodes(offset={offset}) failed and will not \
                             succeed on retry — check that blockchain.capacity_bond_address \
                             holds the registry contract and that rpc_url points at the same \
                             chain and accepts this key"
                        )
                    });
                }
                Err(e) => {
                    let Some(backoff) = REGISTRY_RETRY_BACKOFF.get(attempt).copied() else {
                        return Err(e).with_context(|| {
                            format!(
                                "CapacityBond.getActiveNodes(offset={offset}) failed after {} \
                                 retries",
                                REGISTRY_RETRY_BACKOFF.len()
                            )
                        });
                    };
                    // Sanitized, not `%e`: an alloy transport error's Display
                    // forwards reqwest's ` for url (<url>)` tail, and an
                    // `rpc_url` commonly carries an API key in its path/query
                    // (issue #954). The `main()` boundary only sanitizes the
                    // error chain, never a `tracing` field.
                    tracing::warn!(
                        offset,
                        attempt,
                        backoff_ms = backoff.as_millis(),
                        error = %sanitize_rpc_display(&e),
                        "CapacityBond.getActiveNodes page failed; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        };
        let page_len = page.len() as u64;
        out.extend(page.iter().filter_map(candidate_from));
        // A short page is the last page (Kademlia-style termination).
        if page_len < PAGE_SIZE {
            break;
        }
        offset = offset.saturating_add(PAGE_SIZE);
    }
    Ok(out)
}

/// Read the active node set from `CapacityBond.getActiveNodes` at
/// `capacity_bond_addr` over `rpc_url` (a read-only HTTP provider — no signer
/// needed for a view call). Paginated; inactive / undecodable entries are
/// skipped.
///
/// Private on purpose: this is the un-cached half, and it can sleep for the
/// full retry schedule. [`bootstrap_nodes`] is the ADR 012 entry point, and a
/// caller reaching past it would silently opt out of the peer-cache fallback.
///
/// # Errors
///
/// Fails if `rpc_url` is not a valid URL (before any retry is attempted) or a
/// `getActiveNodes` page call fails permanently or past its retries.
async fn active_nodes(
    rpc_url: &str,
    capacity_bond_addr: Address,
) -> anyhow::Result<Vec<NodeCandidate>> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse().with_context(|| {
        // An rpc_url secret commonly lives in the path/query (Infura/Alchemy
        // keys), which userinfo redaction wouldn't scrub — so hide the value
        // entirely (the policy `config validate` follows of never echoing
        // rpc_url), like the sibling parse sites in `chain_ctx` / `setup`
        // (issue #954).
        format!(
            "rpc_url is not a valid URL (<redacted>, {} chars)",
            rpc_url.len()
        )
    })?);
    let registry = CapacityBond::new(capacity_bond_addr, provider);

    paginate_with_retry(|offset| {
        let registry = &registry;
        async move {
            registry
                .getActiveNodes(U256::from(offset), U256::from(PAGE_SIZE))
                .call()
                .await
        }
    })
    .await
}

/// On-disk shape of the peer cache. The version tag lets a later shape change
/// be read as "no usable cache" instead of a hard decode failure — see
/// [`PEER_CACHE_VERSION`] for why the tag is decoded separately to make that
/// true.
#[derive(Debug, Serialize, Deserialize)]
struct PeerCache {
    version: u32,
    /// Seconds since the Unix epoch at which this cache was written, so the
    /// fallback can tell the user how old the peer list it is serving is.
    /// Present from version 1 — adding it later would have cost a version bump
    /// that invalidates every deployed cache.
    written_at: u64,
    peers: Vec<NodeCandidate>,
}

/// Just the version tag. Decoded first (serde ignores the rest) so that a
/// future change to the `peers` shape is reported as a version mismatch rather
/// than as an opaque decode failure — a whole-[`PeerCache`] decode would choke
/// on the new shape before the tag was ever read.
#[derive(Deserialize)]
struct CacheVersion {
    version: u32,
}

impl PeerCache {
    /// How long ago this cache was written, saturating at zero so a clock that
    /// moved backwards reads as "just now" rather than underflowing.
    fn age(&self) -> Duration {
        Duration::from_secs(now_secs().saturating_sub(self.written_at))
    }
}

/// Wall-clock seconds since the Unix epoch, or 0 if the clock predates it.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// Path of the cached peer list inside the resolved client data dir
/// (`--data-dir` / `identity.data_dir`, defaulting to `~/.decdn/client`).
fn peer_cache_path(data_dir: &Path) -> PathBuf {
    data_dir.join(PEER_CACHE_FILE)
}

/// Outcome of a peer-cache read.
///
/// [`Unusable`](Self::Unusable) is kept distinct from [`Absent`](Self::Absent)
/// because the two mean opposite things to whoever has to fix the problem: no
/// file is the ordinary first-run state, while a file that exists and cannot be
/// used is a misconfiguration the user can act on — and it is at its most
/// confusing precisely when the registry is *also* down, which is the only time
/// this is read. The reason travels back to the caller rather than into a log,
/// for the same reason [`Bootstrap`] carries its provenance: `decdn` has no log
/// sink.
enum CacheRead {
    Ok(Box<PeerCache>),
    Absent,
    /// Why the existing cache could not be used, phrased for the error chain.
    Unusable(String),
}

/// Read the cached peer list.
fn read_peer_cache(data_dir: &Path) -> CacheRead {
    let path = peer_cache_path(data_dir);
    let at = path.display();
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        // The ordinary first-run case.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return CacheRead::Absent,
        // Anything else is a cache that is *there* and being ignored — most
        // often a root-owned `peers.json` left by an earlier `sudo` run, or a
        // data dir that is really a file.
        Err(e) => return CacheRead::Unusable(format!("the peer cache at {at} is unreadable: {e}")),
    };
    // Version before body — see `PEER_CACHE_VERSION`.
    let version = match serde_json::from_slice::<CacheVersion>(&raw) {
        Ok(v) => v.version,
        Err(e) => {
            return CacheRead::Unusable(format!("the peer cache at {at} is undecodable: {e}"));
        }
    };
    if version != PEER_CACHE_VERSION {
        return CacheRead::Unusable(format!(
            "the peer cache at {at} is version {version}, but this build reads version \
             {PEER_CACHE_VERSION}"
        ));
    }
    let cache: PeerCache = match serde_json::from_slice(&raw) {
        Ok(cache) => cache,
        Err(e) => {
            return CacheRead::Unusable(format!("the peer cache at {at} has a bad body: {e}"));
        }
    };
    if cache.peers.is_empty() {
        return CacheRead::Unusable(format!("the peer cache at {at} lists no peers"));
    }
    CacheRead::Ok(Box::new(cache))
}

/// Persist `peers` to the peer cache, creating `data_dir` if needed. Written to
/// a uniquely-named sibling temp file, fsynced, and atomically renamed, so a
/// crash can only lose the *new* cache, never truncate the old one, and a
/// second `decdn` process sharing the data dir cannot interleave into the same
/// temp file (last writer wins, with a whole file).
///
/// Two limits worth knowing: only the file's data is fsynced, not the directory
/// entry, so the rename itself is not crash-durable — `Ok` does not guarantee
/// the new cache survives a power loss. And a crash between temp-create and
/// rename strands a `.tmpXXXXXX` file that nothing reaps.
///
/// # Errors
///
/// Fails if the data dir cannot be created or the write/sync/rename fails.
fn write_peer_cache(data_dir: &Path, peers: &[NodeCandidate]) -> anyhow::Result<()> {
    use std::io::Write as _;

    let path = peer_cache_path(data_dir);
    let body = serde_json::to_vec_pretty(&PeerCache {
        version: PEER_CACHE_VERSION,
        written_at: now_secs(),
        peers: peers.to_vec(),
    })?;
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("creating client data dir {}", data_dir.display()))?;
    // Same dir as the target so `persist` is a rename, not a cross-device copy.
    let mut tmp = tempfile::NamedTempFile::new_in(data_dir)
        .with_context(|| format!("creating a temp file in {}", data_dir.display()))?;
    tmp.write_all(&body)
        .with_context(|| format!("writing {}", tmp.path().display()))?;
    tmp.as_file()
        .sync_all()
        .with_context(|| format!("syncing {}", tmp.path().display()))?;
    tmp.persist(&path)
        .map_err(|e| e.error)
        .with_context(|| format!("renaming into {}", path.display()))?;
    Ok(())
}

/// Turn a registry read outcome into the bootstrap peer set (ADR 012
/// § Bootstrap steps 4 and 7 — step 6, building the peer table, is
/// `select_candidates` in the callers): persist a successful read to the peer
/// cache, or on failure fall back to the cache, or — with no cache — surface
/// [`BOOTSTRAP_UNREACHABLE`] with the registry failure as its cause.
///
/// An empty successful read is returned as-is but does **not** overwrite the
/// cache: an emptied registry is not a reason to discard the last known-good
/// peer list.
fn resolve_bootstrap(
    registry: anyhow::Result<Vec<NodeCandidate>>,
    data_dir: &Path,
) -> anyhow::Result<Bootstrap> {
    let err = match registry {
        Ok(peers) => {
            // A cache we could not persist must not fail a fetch that already
            // succeeded — but it is not free either: it silently disarms the
            // fallback, and a data dir that is read-only or full fails this way
            // on *every* invocation, so the user believes they have outage
            // protection they have never actually had. Hence it travels back to
            // the caller rather than only into a log.
            let cache_error = (!peers.is_empty())
                .then(|| write_peer_cache(data_dir, &peers).err())
                .flatten()
                .map(|e| sanitize_err_chain(&e));
            return Ok(Bootstrap::Live { peers, cache_error });
        }
        Err(e) => e,
    };
    match read_peer_cache(data_dir) {
        CacheRead::Ok(cache) => Ok(Bootstrap::Cached {
            age: cache.age(),
            peers: cache.peers,
            // Sanitized `{err:#}`, not `%err`: plain Display on an
            // `anyhow::Error` renders only the outermost context and drops the
            // reason the registry read actually failed.
            registry_error: sanitize_err_chain(&err),
        }),
        CacheRead::Absent => Err(err.context(BOOTSTRAP_UNREACHABLE)),
        // Layered *under* `BOOTSTRAP_UNREACHABLE` so `{err}` still renders the
        // ADR-pinned sentence verbatim, while `{err:#}` — what `main()` prints —
        // names the cache that was ignored and why. Otherwise the one moment
        // the cache matters is the one moment its failure is invisible.
        CacheRead::Unusable(why) => Err(err.context(why).context(BOOTSTRAP_UNREACHABLE)),
    }
}

/// Where a bootstrap peer set came from.
///
/// The provenance is in the return type rather than in a log line because
/// `decdn` installs no `tracing` subscriber — a warning here would reach
/// nobody, and "the registry is down and this list may be months old" is
/// exactly what a user must not miss. Callers render [`Self::warning`] to
/// stderr and then take [`Self::into_peers`].
#[derive(Debug)]
pub enum Bootstrap {
    /// Read live from the on-chain registry.
    Live {
        peers: Vec<NodeCandidate>,
        /// Set when the read succeeded but could not be persisted, which leaves
        /// the next outage without a fallback.
        cache_error: Option<String>,
    },
    /// The registry could not be read; these peers came from `peers.json`.
    Cached {
        peers: Vec<NodeCandidate>,
        /// How long ago the cache was written.
        age: Duration,
        /// Why the registry read failed, sanitized for display.
        registry_error: String,
    },
}

impl Bootstrap {
    /// A line to print to stderr, or `None` when the bootstrap was wholly
    /// healthy.
    #[must_use]
    pub fn warning(&self) -> Option<String> {
        match self {
            Self::Live { cache_error, .. } => cache_error.as_ref().map(|e| {
                format!(
                    "warning: could not save the peer cache ({e}); a registry outage will not \
                     be survivable until this is fixed"
                )
            }),
            Self::Cached {
                peers,
                age,
                registry_error,
            } => Some(format!(
                "warning: could not reach the node registry ({registry_error}); using the peer \
                 list cached {} ago ({} node(s)). These nodes may have been deactivated or \
                 slashed since.",
                humanize(*age),
                peers.len()
            )),
        }
    }

    /// The resolved peer set, whatever its provenance.
    #[must_use]
    pub fn into_peers(self) -> Vec<NodeCandidate> {
        match self {
            Self::Live { peers, .. } | Self::Cached { peers, .. } => peers,
        }
    }
}

/// Coarse human-readable duration for the staleness warning — the difference
/// between "2 minutes" and "6 months" is what the user acts on; minutes of
/// precision inside a month are not.
fn humanize(d: Duration) -> String {
    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    let secs = d.as_secs();
    let (n, unit) = match secs {
        s if s < MINUTE => (s, "second"),
        s if s < HOUR => (s / MINUTE, "minute"),
        s if s < DAY => (s / HOUR, "hour"),
        s => (s / DAY, "day"),
    };
    format!("{n} {unit}{}", if n == 1 { "" } else { "s" })
}

/// Bootstrap the client's peer set (ADR 012 § Bootstrap): read the active node
/// set from `CapacityBond` with the ADR's retry schedule, persisting it to the
/// peer cache under `data_dir` on success and falling back to that cache when
/// the registry cannot be read.
///
/// `data_dir` is the resolved client data dir, so an explicit `--data-dir`
/// moves the cache with the rest of the client's state.
///
/// Callers must print [`Bootstrap::warning`] — the degraded paths are invisible
/// otherwise.
///
/// # Errors
///
/// Fails with [`BOOTSTRAP_UNREACHABLE`] when the registry read fails and no
/// cached peer list exists.
pub async fn bootstrap_nodes(
    rpc_url: &str,
    capacity_bond_addr: Address,
    data_dir: &Path,
) -> anyhow::Result<Bootstrap> {
    resolve_bootstrap(active_nodes(rpc_url, capacity_bond_addr).await, data_dir)
}

/// Candidates probed before ranking (decision 3): take the top-K by region,
/// then probe those K for liveness + blob-holding. At `PoC` scale a small K
/// keeps the probe fan-out cheap while still giving the ranker a choice.
pub const SELECT_K: usize = 5;

/// RTT tolerance for preferring an already-open channel over the strictly
/// nearest node (decision 4): reuse a node the client holds a live channel with
/// when its RTT is within this multiple of the best observed RTT. Tunable.
pub const RTT_REUSE_TOLERANCE: f64 = 1.5;

/// Order `candidates` for probing (decision 3): same-region candidates first
/// (case-insensitive equality on `region_hint` — locality, not geo distance),
/// then the rest, capped at `k`. When `client_region` is `None` the region-first
/// ordering is skipped and the first `k` candidates are returned unreordered.
#[must_use]
pub fn select_candidates(
    mut candidates: Vec<NodeCandidate>,
    client_region: Option<&str>,
    k: usize,
) -> Vec<NodeCandidate> {
    if let Some(region) = client_region.map(str::trim).filter(|r| !r.is_empty()) {
        // Stable sort by a bool key: same-region (`false`) sorts before the rest
        // (`true`), and within each group the on-chain order is preserved.
        // `region_hint` is trimmed too — on-chain data is operator-submitted and
        // may carry stray whitespace.
        candidates.sort_by_key(|c| !c.region_hint.trim().eq_ignore_ascii_case(region));
    }
    candidates.truncate(k);
    candidates
}

/// A probed candidate that holds the blob, with its measured RTT and whether the
/// client already has a live payment channel with it.
#[derive(Debug, Clone)]
pub struct Probed {
    /// The node that answered the probe with `has_blob = true`.
    pub candidate: NodeCandidate,
    /// Round-trip time measured by the probe, in milliseconds.
    pub rtt_ms: f64,
    /// Whether the buyer-channel store already holds a live (non-expired)
    /// channel for `candidate.eth_address`.
    pub has_live_channel: bool,
}

/// A bonded non-holder the client has a measured RTT for, and could route a
/// warming request through (ADR 037 § Candidate pool). Distinct from
/// [`Probed`], which is a confirmed blob-holder.
#[derive(Debug, Clone)]
pub struct WarmingCandidate {
    /// The candidate proxy's iroh id.
    pub node_id: PublicKey,
    /// Its Ethereum address — the `--provider-address` a warming channel opens
    /// and the `slash_sig` is verified against.
    pub eth_address: Address,
    /// Measured round-trip time in milliseconds, from the **live `cdn/probe/v1`
    /// probe issued for this request**.
    ///
    /// Note this diverges from ADR 037 § RTT source, which specifies the
    /// client's longitudinal per-peer RTT map. That map ([`crate::rtt_map`]) is
    /// staged but not yet wired, so the candidate pool is presently limited to
    /// the ≤`SELECT_K` nodes this request happened to probe rather than the full
    /// peer table minus holders.
    pub rtt_ms: f64,
}

/// ADR 037 § Client selection policy: the ordered list of nearby non-holders to
/// route a warming request through, nearest-RTT first, or empty when proxy
/// warming does not engage.
///
/// Proxy warming engages **only** when the best holder's RTT exceeds
/// `rtt_threshold_ms` (the holders are all distant) **and** a candidate beats
/// the best holder's RTT by at least `margin_ms`. Ranking is measured RTT only
/// — self-attested region is never consulted, so a region-spoofing node simply
/// exhibits a high measured RTT and is never selected. An empty result means
/// "route directly to the best holder"; proxy warming is a no-op, never a
/// gamble.
///
/// `candidates` must already exclude the holders — the caller does this.
///
/// # Contract not yet enforced
///
/// ADR 037 § Candidate pool also requires the pool to be filtered to
/// reputation ≥ the client's minimum-reputation floor. **No caller does this
/// today**; it is stated here as the intended contract, not a satisfied
/// precondition (#1174 follow-up).
///
/// The full ordered list is returned so a caller *can* fall back from a proxy
/// that declines to the next candidate and finally to the direct holder, per
/// ADR 037 § Fallback. **The current caller uses only the first entry** — a real
/// fallback needs a second payment channel against the fallback provider, which
/// is deliberate follow-up work. Proxy warming is opt-in (default off) until it
/// lands, so a declining proxy cannot regress a default fetch.
#[must_use]
pub fn proxy_warming_order(
    best_holder_rtt_ms: f64,
    rtt_threshold_ms: f64,
    margin_ms: f64,
    candidates: &[WarmingCandidate],
) -> Vec<&WarmingCandidate> {
    // Trigger 1: the best holder must be distant enough to warrant warming. The
    // `is_finite()` guard also makes a NaN best-holder RTT fail closed (route
    // direct) rather than sneak past a bare `>` comparison.
    if !(best_holder_rtt_ms.is_finite() && best_holder_rtt_ms > rtt_threshold_ms) {
        return Vec::new();
    }
    // Trigger 2 + ranking: keep only candidates that beat the best holder by the
    // margin, nearest first. If none clear the margin the list is empty and the
    // caller routes direct.
    let mut qualifying: Vec<&WarmingCandidate> = candidates
        .iter()
        .filter(|c| c.rtt_ms.is_finite() && best_holder_rtt_ms - c.rtt_ms >= margin_ms)
        .collect();
    qualifying.sort_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms));
    qualifying
}

/// Pick the node to fetch from among probed blob-holders (decision 4): prefer a
/// node the client already has a live channel with when its RTT is within
/// [`RTT_REUSE_TOLERANCE`]× the best observed RTT; otherwise the lowest-RTT
/// node. `holders` must already be filtered to blob-holders. Returns `None`
/// when `holders` is empty.
#[must_use]
pub fn rank(holders: &[Probed]) -> Option<&Probed> {
    let best = holders
        .iter()
        .min_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms))?;
    let threshold = best.rtt_ms * RTT_REUSE_TOLERANCE;
    holders
        .iter()
        .filter(|p| p.has_live_channel && p.rtt_ms <= threshold)
        .min_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms))
        .or(Some(best))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use alloy::primitives::{Bytes, FixedBytes};

    fn node_info(node_id: [u8; 32], active: bool) -> CapacityBond::NodeInfo {
        CapacityBond::NodeInfo {
            nodeId: FixedBytes::from(node_id),
            ethAddress: Address::repeat_byte(0xab),
            active,
            lastMultiaddrUpdate: 0,
            multiaddrs: Bytes::new(),
            regionHint: "US".to_string(),
        }
    }

    #[test]
    fn skips_inactive_nodes() {
        // A valid ed25519 key: the all-zero point is rejected by `from_bytes`,
        // so derive a real one from a known secret.
        let key = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let bytes = *key.as_bytes();
        assert!(candidate_from(&node_info(bytes, true)).is_some());
        assert!(
            candidate_from(&node_info(bytes, false)).is_none(),
            "inactive nodes must be filtered out"
        );
    }

    #[test]
    fn carries_eth_address_and_region() {
        let key = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        let c = candidate_from(&node_info(*key.as_bytes(), true)).unwrap();
        assert_eq!(c.eth_address, Address::repeat_byte(0xab));
        assert_eq!(c.region_hint, "US");
    }

    fn candidate(seed: u8, region: &str) -> NodeCandidate {
        NodeCandidate {
            node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            eth_address: Address::repeat_byte(seed),
            region_hint: region.to_string(),
        }
    }

    fn probed(seed: u8, rtt_ms: f64, has_live_channel: bool) -> Probed {
        Probed {
            candidate: candidate(seed, "US"),
            rtt_ms,
            has_live_channel,
        }
    }

    #[test]
    fn select_puts_same_region_first_and_caps_at_k() {
        // Regions are on-chain self-attested ISO 3166-1 alpha-2 codes (ADR 030);
        // seed 4 carries stray case + whitespace to exercise the trim +
        // case-insensitive match.
        let cands = vec![
            candidate(1, "DE"),
            candidate(2, "US"),
            candidate(3, "DE"),
            candidate(4, " us "),
        ];
        let out = select_candidates(cands, Some("US"), 3);
        assert_eq!(out.len(), 3, "capped at k");
        // Both US entries (seeds 2 and 4) come first, in their original order.
        assert_eq!(out[0].eth_address, Address::repeat_byte(2));
        assert_eq!(out[1].eth_address, Address::repeat_byte(4));
        assert_eq!(out[2].eth_address, Address::repeat_byte(1));
    }

    #[test]
    fn select_without_region_preserves_order_and_caps() {
        let cands = vec![candidate(1, "DE"), candidate(2, "US")];
        // Unknown region (None) and blank region both skip reordering.
        for region in [None, Some("  ")] {
            let out = select_candidates(cands.clone(), region, 5);
            assert_eq!(out[0].eth_address, Address::repeat_byte(1));
            assert_eq!(out[1].eth_address, Address::repeat_byte(2));
        }
    }

    #[test]
    fn rank_empty_is_none() {
        assert!(rank(&[]).is_none());
    }

    #[test]
    fn rank_prefers_channel_within_tolerance() {
        // Nearest is seed 1 (10ms, no channel); seed 2 has a channel at 14ms
        // (≤ 1.5×10 = 15) so it wins on reuse.
        let holders = vec![probed(1, 10.0, false), probed(2, 14.0, true)];
        let pick = rank(&holders).unwrap();
        assert_eq!(pick.candidate.eth_address, Address::repeat_byte(2));
    }

    #[test]
    fn rank_falls_back_to_nearest_when_channel_too_slow() {
        // Channel-holder seed 2 is at 16ms (> 1.5×10 = 15): pick the nearest.
        let holders = vec![probed(1, 10.0, false), probed(2, 16.0, true)];
        let pick = rank(&holders).unwrap();
        assert_eq!(pick.candidate.eth_address, Address::repeat_byte(1));
    }

    #[test]
    fn rank_nearest_when_no_channels() {
        let holders = vec![probed(1, 30.0, false), probed(2, 12.0, false)];
        let pick = rank(&holders).unwrap();
        assert_eq!(pick.candidate.eth_address, Address::repeat_byte(2));
    }

    #[test]
    fn retry_backoff_is_the_adr_012_schedule() {
        // ADR 012 § Bootstrap step 3: "retry 3× exponential backoff
        // (1 s, 5 s, 30 s)". A tripwire on the table, not coverage of the loop
        // that reads it — that is `a_failing_page_is_retried_on_the_adr_schedule`
        // below, which binds the two together.
        assert_eq!(
            REGISTRY_RETRY_BACKOFF,
            [
                Duration::from_secs(1),
                Duration::from_secs(5),
                Duration::from_secs(30),
            ]
        );
    }

    /// A retryable transport failure — a refused connection, a reset, a timeout.
    fn transient() -> alloy::contract::Error {
        alloy::contract::Error::TransportError(alloy::transports::RpcError::NullResp)
    }

    /// A failure that will never succeed on retry.
    fn permanent() -> alloy::contract::Error {
        alloy::contract::Error::ContractNotDeployed
    }

    /// A JSON-RPC error *response* — HTTP 200 with an error body, so the HTTP
    /// status check never sees it.
    ///
    /// Built by deserializing the wire shape: `ErrorPayload` is not re-exported
    /// through `alloy::transports` (only `RpcError` is), so the variant's own
    /// type inference is what names it here.
    fn error_resp(code: i64, message: &str) -> alloy::contract::Error {
        alloy::contract::Error::TransportError(alloy::transports::RpcError::ErrorResp(
            serde_json::from_value(serde_json::json!({ "code": code, "message": message }))
                .unwrap(),
        ))
    }

    #[test]
    fn provider_rate_limits_stay_retryable() {
        // Providers signal rate limiting as a JSON-RPC error response over HTTP
        // 200, so treating every `ErrorResp` as deterministic would skip the
        // ADR's retry for one of the most common transient failures there is.
        for (code, message) in [
            (429, "Too Many Requests"),
            (-32005, "exceeded project rate limit"),
            (-32016, "Your app has exceeded its rate limit"),
            (-32007, "100/second request limit reached"),
        ] {
            assert!(
                !is_permanent(&error_resp(code, message)),
                "JSON-RPC {code} ({message}) is a rate limit and must be retried"
            );
        }
        // A genuinely deterministic response still short-circuits.
        assert!(is_permanent(&error_resp(-32601, "method not found")));
        assert!(is_permanent(&error_resp(3, "execution reverted")));
    }

    #[tokio::test(start_paused = true)]
    async fn a_rate_limited_page_is_retried_not_abandoned() {
        // Regression: the rate limit must consume the full 1/5/30 s schedule
        // rather than falling through to the cache on the first response.
        let calls = std::cell::Cell::new(0usize);
        let start = tokio::time::Instant::now();

        let out = paginate_with_retry(|_offset| {
            let n = calls.get();
            calls.set(n.saturating_add(1));
            async move {
                match n {
                    // Rate-limited twice, then the provider lets us through.
                    0 => Err(error_resp(429, "Too Many Requests")),
                    1 => Err(error_resp(-32005, "exceeded project rate limit")),
                    _ => Ok(vec![node_info(valid_node_id(3), true)]),
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(calls.get(), 3, "two rate limits were retried, not surfaced");
        assert_eq!(
            tokio::time::Instant::now() - start,
            REGISTRY_RETRY_BACKOFF[0] + REGISTRY_RETRY_BACKOFF[1],
            "backed off on the first two steps of the schedule"
        );
        assert_eq!(out.len(), 1);
    }

    fn valid_node_id(seed: u8) -> [u8; 32] {
        *iroh::SecretKey::from_bytes(&[seed; 32]).public().as_bytes()
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_page_is_retried_on_the_adr_schedule() {
        // Paused time auto-advances on idle, so the whole 36 s schedule runs
        // instantly and the elapsed virtual time is itself assertable.
        let calls = std::cell::Cell::new(0usize);
        let start = tokio::time::Instant::now();

        let err = paginate_with_retry(|_offset| {
            calls.set(calls.get().saturating_add(1));
            async { Err(transient()) }
        })
        .await
        .unwrap_err();

        assert_eq!(
            calls.get(),
            1 + REGISTRY_RETRY_BACKOFF.len(),
            "one initial call plus one per backoff step"
        );
        assert_eq!(
            tokio::time::Instant::now() - start,
            REGISTRY_RETRY_BACKOFF.iter().sum::<Duration>(),
            "slept exactly 1 + 5 + 30 s"
        );
        assert!(format!("{err:#}").contains("failed after 3 retries"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_failure_is_not_retried() {
        // A typo'd capacity_bond_address must not cost the user 36 s of silence
        // before being told what is wrong.
        let calls = std::cell::Cell::new(0usize);
        let start = tokio::time::Instant::now();

        let err = paginate_with_retry(|_offset| {
            calls.set(calls.get().saturating_add(1));
            async { Err(permanent()) }
        })
        .await
        .unwrap_err();

        assert_eq!(calls.get(), 1, "a deterministic failure is not repeated");
        assert_eq!(tokio::time::Instant::now() - start, Duration::ZERO);
        assert!(format!("{err:#}").contains("will not succeed on retry"));
        assert!(format!("{err:#}").contains("capacity_bond_address"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_that_succeeds_resumes_pagination() {
        // The offsets requested are the assertion: a retry must re-request the
        // same page, and only a full page may advance the cursor.
        let seen = std::cell::RefCell::new(Vec::new());
        let attempt = std::cell::Cell::new(0usize);

        let out = paginate_with_retry(|offset| {
            seen.borrow_mut().push(offset);
            let n = attempt.get();
            attempt.set(n.saturating_add(1));
            async move {
                match n {
                    // The first page fails once, then serves a full page…
                    0 => Err(transient()),
                    1 => Ok((0..PAGE_SIZE)
                        .map(|_| node_info(valid_node_id(1), true))
                        .collect()),
                    // …and the short second page ends the read.
                    _ => Ok(vec![node_info(valid_node_id(2), true)]),
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(
            *seen.borrow(),
            vec![0, 0, PAGE_SIZE],
            "the retry re-requests offset 0, then the cursor advances by one page"
        );
        assert_eq!(out.len(), usize::try_from(PAGE_SIZE).unwrap() + 1);
    }

    /// The cached peers alone, for the many assertions that do not care about
    /// the surrounding [`PeerCache`] metadata.
    fn cached_peers(data_dir: &Path) -> Option<Vec<NodeCandidate>> {
        match read_peer_cache(data_dir) {
            CacheRead::Ok(c) => Some(c.peers),
            _ => None,
        }
    }

    /// The reason an existing cache was rejected, or `None` if it was usable or
    /// absent.
    fn cache_rejection(data_dir: &Path) -> Option<String> {
        match read_peer_cache(data_dir) {
            CacheRead::Unusable(why) => Some(why),
            _ => None,
        }
    }

    #[test]
    fn peer_cache_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let peers = vec![candidate(1, "DE"), candidate(2, "US")];
        write_peer_cache(dir.path(), &peers).unwrap();
        assert_eq!(cached_peers(dir.path()), Some(peers));
    }

    #[test]
    fn the_cache_json_shape_is_pinned() {
        // `peer_cache_round_trips` structurally cannot catch a format change,
        // because the writer and reader move together. If iroh alters how
        // `PublicKey` serializes, every deployed `peers.json` silently becomes
        // undecodable at version 1 while the suite stays green. Pin the shape so
        // that change has to be a deliberate `PEER_CACHE_VERSION` bump.
        let dir = tempfile::tempdir().unwrap();
        let peer = candidate(1, "DE");
        write_peer_cache(dir.path(), std::slice::from_ref(&peer)).unwrap();

        let raw = std::fs::read(peer_cache_path(dir.path())).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(v["version"], PEER_CACHE_VERSION);
        assert!(v["written_at"].is_u64());
        assert_eq!(
            v["peers"][0]["node_id"],
            serde_json::Value::String(peer.node_id.to_string()),
            "node_id is the z-base-32 string form"
        );
        assert_eq!(v["peers"][0]["region_hint"], "DE");
        assert_eq!(
            v["peers"][0]["eth_address"],
            serde_json::Value::String(peer.eth_address.to_string())
        );
    }

    #[test]
    fn a_cache_of_an_unknown_version_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        let body = serde_json::json!({
            "version": PEER_CACHE_VERSION + 1,
            "written_at": 0,
            "peers": [candidate(1, "DE")],
        });
        std::fs::write(peer_cache_path(dir.path()), body.to_string()).unwrap();
        let why = cache_rejection(dir.path()).unwrap();
        assert!(why.contains("is version 2"), "{why}");
        assert!(why.contains("reads version 1"), "{why}");
    }

    #[test]
    fn the_version_tag_survives_a_future_peers_shape() {
        // This is what the separate `CacheVersion` decode buys: a later version
        // that changes the `peers` shape is still reportable as a version
        // mismatch. Decoding the whole `PeerCache` first would fail on the body
        // and never reach the tag.
        let body = serde_json::json!({
            "version": PEER_CACHE_VERSION + 1,
            "peers": { "shape": "changed" },
        })
        .to_string();

        assert_eq!(
            serde_json::from_str::<CacheVersion>(&body).unwrap().version,
            PEER_CACHE_VERSION + 1
        );
        assert!(
            serde_json::from_str::<PeerCache>(&body).is_err(),
            "the body is undecodable — only the narrow decode can recover the tag"
        );

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(peer_cache_path(dir.path()), &body).unwrap();
        assert!(
            cache_rejection(dir.path())
                .unwrap()
                .contains("is version 2")
        );
    }

    #[test]
    fn a_malformed_cache_reads_as_none() {
        // A truncated `peers.json` from a pre-atomic-write crash is exactly the
        // scenario `write_peer_cache` was hardened against; reading one must
        // degrade to "no cache", not propagate.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(peer_cache_path(dir.path()), b"{\"version\": 1, \"pee").unwrap();
        assert!(cache_rejection(dir.path()).unwrap().contains("undecodable"));
    }

    #[test]
    fn writing_the_cache_leaves_no_temp_file_behind() {
        let dir = tempfile::tempdir().unwrap();
        write_peer_cache(dir.path(), &[candidate(6, "US")]).unwrap();
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from(PEER_CACHE_FILE)]);
    }

    #[test]
    fn absent_cache_reads_as_absent() {
        // Distinct from `Unusable`: no file is the ordinary first-run state and
        // must not be reported to the user as a problem.
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(read_peer_cache(dir.path()), CacheRead::Absent));
    }

    #[test]
    fn successful_read_persists_the_cache_and_warns_about_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let peers = vec![candidate(5, "FR")];
        let out = resolve_bootstrap(Ok(peers.clone()), dir.path()).unwrap();
        assert!(out.warning().is_none(), "a healthy bootstrap is quiet");
        assert_eq!(out.into_peers(), peers);
        assert_eq!(cached_peers(dir.path()), Some(peers));
    }

    #[test]
    fn a_cache_write_failure_does_not_fail_a_successful_read() {
        // A data dir that is really a regular file makes `create_dir_all` fail
        // deterministically everywhere — unlike a chmod-based test, which no-ops
        // when CI runs as root.
        let dir = tempfile::tempdir().unwrap();
        let not_a_dir = dir.path().join("occupied");
        std::fs::write(&not_a_dir, b"").unwrap();

        let peers = vec![candidate(7, "US")];
        let out = resolve_bootstrap(Ok(peers.clone()), &not_a_dir).unwrap();
        assert!(
            matches!(
                &out,
                Bootstrap::Live {
                    cache_error: Some(_),
                    ..
                }
            ),
            "an unpersistable cache is reported, not swallowed"
        );
        assert!(
            out.warning()
                .unwrap()
                .contains("could not save the peer cache")
        );
        assert_eq!(out.into_peers(), peers, "but the fetch still proceeds");
    }

    #[test]
    fn registry_failure_falls_back_to_the_cache() {
        let dir = tempfile::tempdir().unwrap();
        let peers = vec![candidate(3, "US"), candidate(4, "DE")];
        write_peer_cache(dir.path(), &peers).unwrap();

        let out = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap();
        let warning = out.warning().unwrap();
        assert!(warning.contains("could not reach the node registry"));
        assert!(
            warning.contains("rpc down"),
            "the warning names the cause, not just the symptom: {warning}"
        );
        assert!(warning.contains("deactivated or slashed"));
        let Bootstrap::Cached { age, .. } = &out else {
            panic!("expected a cached bootstrap, got {out:?}");
        };
        assert!(*age < Duration::from_mins(1), "a just-written cache is new");
        assert_eq!(out.into_peers(), peers);
    }

    #[test]
    fn registry_failure_without_a_cache_reports_the_adr_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap_err();
        assert_eq!(
            format!("{err}"),
            "Cannot reach bootstrap sources. Check network connectivity and RPC endpoint \
             configuration.",
            "the ADR 012 § Bootstrap step 4 wording, verbatim"
        );
        assert_eq!(format!("{err}"), BOOTSTRAP_UNREACHABLE);
        // The registry failure stays in the chain as the cause, and `main()`
        // renders `{err:#}`, so this is what the user actually sees.
        assert!(format!("{err:#}").contains("rpc down"));
    }

    #[test]
    fn an_ignored_cache_is_named_in_the_rendered_error() {
        // The worst case for silence: the registry is down *and* the cache that
        // would have covered it is unusable. `main()` prints `{err:#}`, so that
        // is the rendering asserted here — the user must be told a cache
        // existed and why it was skipped, not just that the RPC failed.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(peer_cache_path(dir.path()), b"not json at all").unwrap();

        let err = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap_err();
        assert_eq!(
            format!("{err}"),
            BOOTSTRAP_UNREACHABLE,
            "the ADR-pinned sentence stays the outermost context"
        );
        let rendered = sanitize_err_chain(&err);
        assert!(rendered.contains(BOOTSTRAP_UNREACHABLE));
        assert!(
            rendered.contains("undecodable"),
            "the cache-read reason survives into the chain: {rendered}"
        );
        assert!(
            rendered.contains(PEER_CACHE_FILE),
            "and names the file to fix: {rendered}"
        );
        assert!(rendered.contains("rpc down"), "as does the registry cause");
    }

    #[test]
    fn an_absent_cache_adds_nothing_to_the_error() {
        // The mirror of the above: with no cache there is nothing to report, so
        // the chain must not gain a spurious "cache" layer on a fresh install.
        let dir = tempfile::tempdir().unwrap();
        let err = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap_err();
        let rendered = sanitize_err_chain(&err);
        assert_eq!(rendered, format!("{BOOTSTRAP_UNREACHABLE}: rpc down"));
    }

    #[test]
    fn an_empty_registry_read_leaves_a_populated_cache_intact() {
        let dir = tempfile::tempdir().unwrap();
        let peers = vec![candidate(6, "US")];
        write_peer_cache(dir.path(), &peers).unwrap();
        assert!(
            resolve_bootstrap(Ok(Vec::new()), dir.path())
                .unwrap()
                .into_peers()
                .is_empty()
        );
        assert_eq!(cached_peers(dir.path()), Some(peers));
    }

    #[test]
    fn humanize_picks_a_coarse_unit() {
        assert_eq!(humanize(Duration::from_secs(1)), "1 second");
        assert_eq!(humanize(Duration::from_secs(42)), "42 seconds");
        assert_eq!(humanize(Duration::from_secs(90)), "1 minute");
        assert_eq!(humanize(Duration::from_hours(3)), "3 hours");
        assert_eq!(humanize(Duration::from_hours(25)), "1 day");
        assert_eq!(humanize(Duration::from_hours(24 * 60)), "60 days");
    }

    fn warming(seed: u8, rtt_ms: f64) -> WarmingCandidate {
        WarmingCandidate {
            node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            eth_address: Address::repeat_byte(seed),
            rtt_ms,
        }
    }

    #[test]
    fn proxy_warming_no_op_when_best_holder_is_near() {
        // Best holder RTT 40ms is below the 100ms threshold: holders aren't
        // distant, so warming does not engage even with a fast candidate.
        let cands = vec![warming(1, 5.0)];
        assert!(proxy_warming_order(40.0, 100.0, 20.0, &cands).is_empty());
    }

    #[test]
    fn proxy_warming_no_op_when_no_candidate_clears_margin() {
        // Best holder is distant (300ms > 100ms threshold) but the nearest
        // candidate (290ms) only beats it by 10ms, below the 20ms margin.
        let cands = vec![warming(1, 290.0)];
        assert!(proxy_warming_order(300.0, 100.0, 20.0, &cands).is_empty());
    }

    #[test]
    fn proxy_warming_picks_qualifying_candidates_nearest_first() {
        // Best holder 300ms; threshold 100, margin 20. Candidates at 30 and 80ms
        // both clear the margin; 290ms does not. Ordered nearest-first.
        let cands = vec![warming(1, 80.0), warming(2, 290.0), warming(3, 30.0)];
        let order = proxy_warming_order(300.0, 100.0, 20.0, &cands);
        assert_eq!(order.len(), 2);
        assert_eq!(order[0].eth_address, Address::repeat_byte(3)); // 30ms first
        assert_eq!(order[1].eth_address, Address::repeat_byte(1)); // then 80ms
    }

    /// A NaN best-holder RTT must fail closed (route direct) rather than sneak
    /// past the trigger comparison, and must never panic the sort.
    ///
    /// The companion region guarantee (ADR 037 §"Ranking key is measured RTT
    /// only") is enforced structurally, not by this test: `WarmingCandidate`
    /// has no region field, so `proxy_warming_order` cannot consult one. Adding
    /// such a field would require editing the struct — a visible, reviewable
    /// change — which is the point.
    #[test]
    fn nan_best_holder_rtt_fails_closed() {
        let cands = vec![warming(1, 10.0)];
        assert!(proxy_warming_order(f64::NAN, 100.0, 20.0, &cands).is_empty());
    }

    /// A non-finite candidate RTT must be filtered out rather than sorted
    /// first — `total_cmp` orders NaN, so an unfiltered NaN would win.
    #[test]
    fn non_finite_candidate_rtt_is_filtered_out() {
        let cands = vec![warming(1, f64::NAN), warming(2, 30.0)];
        let order = proxy_warming_order(300.0, 100.0, 20.0, &cands);
        assert_eq!(order.len(), 1);
        assert_eq!(order[0].eth_address, Address::repeat_byte(2));
    }
}
