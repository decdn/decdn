//! Client-side node discovery (#936): read the active node set from
//! `CapacityBond.getRegisteredNodes` so a client can pick a node instead of being
//! handed an explicit `--node-id`/`--addr`/`--provider-address`. Read from
//! `fetch::discover_provider` and from the discovery branch of
//! `bundle_pull::bundle_pull`; the ranking half is also used by
//! `fetch::probe_and_rank`. `decdn probe` deliberately still requires an
//! explicit target.
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

use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, U256};
use alloy::providers::ProviderBuilder;
use anyhow::Context;
use decdn_common::redact::{sanitize_err_chain, sanitize_rpc_display};
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_protocol::{Coverage, Region};
use iroh::PublicKey;
use serde::{Deserialize, Serialize};

/// Page size for the paginated `getRegisteredNodes` read (ADR 019 § Step 3.3's
/// worked-example limit). At `PoC` scale (tens of nodes) one page suffices; the
/// loop preserves the pattern for production scale.
const PAGE_SIZE: u64 = 100;

/// Backoff before each retry of a failed `getRegisteredNodes` page call — ADR 012
/// § Bootstrap step 3: "retry 3× exponential backoff (1 s, 5 s, 30 s)". The
/// length of the table is the retry count, so a fully-failing page costs
/// 1 + 5 + 30 = 36 s across four attempts.
///
/// The budget is **aggregate across the whole read**, not per page:
/// [`paginate_with_retry`] carries one attempt counter across pagination, so a
/// multi-page read still costs at most 36 s of sleeping in total rather than
/// 36 s × N (#1349). The ADR's "retry 3×" is read as a property of the read,
/// which is the unit the caller waits on — a per-page reset made the worst case
/// scale with a number the caller cannot see or bound.
///
/// The wait is silent from the caller's point of view — nothing streams
/// progress out of this loop — so it is time `decdn fetch` appears to hang.
/// `--timeout-ms` bounds it from the outside.
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
///
/// `2` since #1348 retyped `region_hint` from `String` to `Option<Region>`: a
/// v1 file can hold a region string the validating `Region` deserializer now
/// rejects, which would fail the whole-file decode rather than the one field.
const PEER_CACHE_VERSION: u32 = 2;

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
    /// The node's self-attested region (ADR 030), used for locality-aware
    /// selection. `None` when the node registered without one, or with a code
    /// outside [`Region`]'s accepted set.
    ///
    /// An unrecognized code parses to `None` and sorts into the "rest" bucket
    /// rather than dropping the candidate: `CapacityBond` permits any string up
    /// to 16 bytes and ADR 030 § Cross-ADR Impact explicitly declines to
    /// tighten it, so rejecting a node over a purely advisory field would be
    /// stricter than the source of truth and would silently shrink the
    /// fetchable set. Parsing at this boundary is what makes the derived `Eq`
    /// above correct and keeps an unbounded operator-submitted string off the
    /// peer-cache read path (#1348).
    pub region_hint: Option<Region>,
}

/// Distill a registry `NodeInfo` into a [`NodeCandidate`], or `None` if it is
/// not currently active or its `nodeId` is not a valid ed25519 key.
///
/// `is_active` is the on-chain `isActive` predicate that `getRegisteredNodes`
/// returns per entry (registered AND bond ≥ minBond AND no unbonding AND not
/// ejected) — NOT the raw `NodeInfo.active` registration flag, which stays
/// `true` for an operator mid-unbonding. Filtering on the strict predicate drops
/// stale-active nodes up front rather than paying a probe round-trip to a node
/// that is on its way out. The downstream probe-and-rank step is still the final
/// arbiter of who can actually serve.
fn candidate_from(info: &CapacityBond::NodeInfo, is_active: bool) -> Option<NodeCandidate> {
    if !is_active {
        return None;
    }
    let node_id = PublicKey::from_bytes(&info.nodeId.0).ok()?;
    Some(NodeCandidate {
        node_id,
        eth_address: info.ethAddress,
        // Normalize once, here — see `NodeCandidate::region_hint`. An
        // unparseable hint costs the node its locality bonus, not its place in
        // the candidate set.
        region_hint: Region::parse(&info.regionHint),
    })
}

/// Whether a `getRegisteredNodes` failure is deterministic — the same call will
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

/// Drive `fetch_page` across the paginated `getRegisteredNodes` read, retrying on
/// the ADR 012 § Bootstrap step 3 schedule ([`REGISTRY_RETRY_BACKOFF`]) and
/// distilling every entry through [`candidate_from`].
///
/// The retry budget is **aggregate**, spent across the whole read rather than
/// reset per page (#1349): `attempt` is declared outside the pagination loop, so
/// a page that succeeds after two retries leaves one for everything after it.
/// That is what bounds the worst case at the schedule's own 36 s instead of
/// 36 s × page-count — a figure the caller cannot see, since the page count
/// depends on how many nodes are registered.
///
/// A successful page deliberately does NOT refund the budget. Refunding would
/// restore the unbounded case exactly: a registry that fails every *other* call
/// would alternate success and retry forever.
///
/// Split out from [`active_nodes`] so the retry and pagination control flow is
/// drivable from a test without a live RPC — the schedule alone is a `const`
/// that proves nothing about the loop that reads it.
///
/// # Errors
///
/// Fails when the read's retries are exhausted, or immediately when the failure
/// is [`is_permanent`].
async fn paginate_with_retry<F, Fut>(fetch_page: F) -> anyhow::Result<Vec<NodeCandidate>>
where
    F: Fn(u64) -> Fut,
    Fut: Future<Output = Result<(Vec<CapacityBond::NodeInfo>, Vec<bool>), alloy::contract::Error>>,
{
    let mut out = Vec::new();
    let mut offset = 0u64;
    // Outside the pagination loop: one budget for the whole read.
    let mut attempt = 0usize;
    loop {
        let (page, active) = loop {
            match fetch_page(offset).await {
                Ok(page) => break page,
                Err(e) if is_permanent(&e) => {
                    return Err(e).with_context(|| {
                        format!(
                            "CapacityBond.getRegisteredNodes(offset={offset}) failed and will not \
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
                                "CapacityBond.getRegisteredNodes(offset={offset}) failed after {} \
                                 retries across the whole registry read",
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
                        "CapacityBond.getRegisteredNodes page failed; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        };
        // `page` and `active` are equal-length by construction (the contract
        // fills both in one loop). A divergence is an ABI/decoder fault; the
        // `zip` below would silently truncate and drop candidates, so fail loudly.
        anyhow::ensure!(
            page.len() == active.len(),
            "CapacityBond.getRegisteredNodes(offset={offset}) returned mismatched \
             page/active lengths ({} vs {}) — ABI or decoder fault",
            page.len(),
            active.len()
        );
        let page_len = page.len() as u64;
        // `active[i]` is the on-chain `isActive(page[i])`, index-aligned with the
        // page. Zip so the strict predicate — not the raw `NodeInfo.active` flag —
        // gates each candidate.
        out.extend(
            page.iter()
                .zip(active.iter())
                .filter_map(|(info, &is_active)| candidate_from(info, is_active)),
        );
        // A short page is the last page (Kademlia-style termination).
        if page_len < PAGE_SIZE {
            break;
        }
        offset = offset.saturating_add(PAGE_SIZE);
    }
    Ok(out)
}

/// Read the active node set from `CapacityBond.getRegisteredNodes` at
/// `capacity_bond_addr` over `rpc_url` (a read-only HTTP provider — no signer
/// needed for a view call). Paginated; inactive / undecodable entries are
/// skipped.
///
/// Uncached: this is the un-cached half, and it can sleep for the full retry
/// schedule. For the fetch/bundle-pull bootstrap path, reach it only through
/// [`bootstrap_nodes`] (the ADR 012 entry point) — calling this directly would
/// silently opt out of the peer-cache fallback. It is also, deliberately, the
/// direct read `decdn node lookup` (#1481) uses: that command is an unpaid,
/// one-shot listing with no client data dir to cache into, so the peer-cache
/// layer above would have nothing to read from or write to.
///
/// # Errors
///
/// Fails if `rpc_url` is not a valid URL (before any retry is attempted) or a
/// `getRegisteredNodes` page call fails permanently or past its retries.
pub async fn active_nodes(
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
                .getRegisteredNodes(U256::from(offset), U256::from(PAGE_SIZE))
                .call()
                .await
                .map(|resp| (resp.page, resp.active))
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
        /// The peers the registry returned.
        peers: Vec<NodeCandidate>,
        /// Set when the read succeeded but could not be persisted, which leaves
        /// the next outage without a fallback.
        cache_error: Option<String>,
    },
    /// The registry could not be read; these peers came from `peers.json`.
    Cached {
        /// The peers `peers.json` held.
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
    registry_cap: Duration,
) -> anyhow::Result<Bootstrap> {
    // The deadline bounds the REGISTRY READ ONLY, not the whole bootstrap
    // (#1349). Wrapping `bootstrap_nodes` from outside would cancel
    // `resolve_bootstrap` along with it, and that is where the ADR 012
    // § Bootstrap step 4 cache fallback lives — so a client with a perfectly
    // good `peers.json` and a flaky RPC would get a hard failure instead of a
    // degraded-but-working fetch. Timing out is just another way for the
    // registry read to fail, so it is fed in as one and the existing
    // `Bootstrap::Cached` arm handles it, warning and all.
    let registry =
        match tokio::time::timeout(registry_cap, active_nodes(rpc_url, capacity_bond_addr)).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "the CapacityBond registry read did not finish within {} ms (--timeout-ms); \
             the ADR 012 retry schedule alone can take {} s",
                registry_cap.as_millis(),
                REGISTRY_RETRY_BACKOFF.iter().sum::<Duration>().as_secs(),
            )),
        };
    resolve_bootstrap(registry, data_dir)
}

/// Candidates probed before ranking (decision 3): take the top-K by region,
/// then probe those K for liveness + blob-holding. At `PoC` scale a small K
/// keeps the probe fan-out cheap while still giving the ranker a choice.
pub const SELECT_K: usize = 5;

/// Order `candidates` for probing (decision 3): same-region candidates first
/// (equality on `region_hint` — locality, not geo distance), then the rest,
/// capped at `k`. When `client_region` is `None`, empty, or not an accepted
/// region code the region-first ordering is skipped and the first `k`
/// candidates are returned unreordered.
///
/// Both sides are [`Region`]s, so this is a plain equality: the trim +
/// case-fold runs at the parse boundary, not on every comparison (#1348).
#[must_use]
pub fn select_candidates(
    mut candidates: Vec<NodeCandidate>,
    client_region: Option<&str>,
    k: usize,
) -> Vec<NodeCandidate> {
    // `client_region` stays a `&str` and is parsed HERE, not assumed valid.
    // `decdn-common`'s `normalize_region` validates the node daemon's config
    // region, but the client path does not go through it: `decdn fetch`'s
    // `--region` / `[identity] region` reach `ResolvedChain::region` as a raw
    // `String`. So this is the validating boundary for the client, and an
    // unrecognized value means "no locality information" — the ordering is
    // skipped rather than applied against a value that means nothing.
    if let Some(region) = client_region.and_then(Region::parse) {
        // Stable sort by a bool key: same-region (`false`) sorts before the rest
        // (`true`), and within each group the on-chain order is preserved.
        candidates.sort_by_key(|c| c.region_hint != Some(region));
    }
    candidates.truncate(k);
    candidates
}

/// Pick up to `max_sources` candidates from an already-ranked `ordered` list,
/// admitting at most ONE per `eth_address` (operator), without disturbing rank
/// order. Used by the multi-source scheduler's engagement gate to pick the
/// source set for a parallel fetch — `select_candidates`/`rank` pick a single
/// best node, this picks a *set*.
///
/// One greedy pass, walking `ordered` in rank order: admit a candidate the first
/// time its `eth_address` is seen. A candidate is skipped only when it adds no
/// new operator, so two equally-diversifying candidates are never reordered —
/// the pass only skips ahead over already-represented operators.
///
/// # Why the set never repeats an operator
///
/// A voucher is scoped to one `(signer, provider)` lane, and `provider` IS the
/// operator's `eth_address`. Two admitted nodes of one operator therefore become
/// two payment lanes on ONE watermark: their concurrent voucher streams regress
/// each other, and their persisted watermarks collide under one `LaneKey`, last
/// write winning at the LOWER value. Filling spare slots with a repeat operator
/// buys parallelism the payment model cannot express, so the set shrinks
/// instead: with one operator present this returns ONE candidate, and the
/// caller's two-holder engagement gate then declines multi-source entirely.
///
/// Region is not a separate tiebreaking pass: the pass already walks candidates
/// in rank order, so among several unseen operators the one ranked first (which
/// is also, incidentally, the first with a given region) is the one admitted —
/// there is nothing left for a region check to change without reordering by
/// something other than rank, which the contract forbids.
///
/// The result holds `min(max_sources, distinct operators in ordered)`
/// candidates, in rank order.
#[must_use]
pub fn admit_sources(ordered: Vec<NodeCandidate>, max_sources: usize) -> Vec<NodeCandidate> {
    if max_sources == 0 {
        return Vec::new();
    }
    let mut seen_operators = HashSet::with_capacity(max_sources);
    let mut out = Vec::with_capacity(max_sources.min(ordered.len()));
    for candidate in ordered {
        if out.len() >= max_sources {
            break;
        }
        if seen_operators.insert(candidate.eth_address) {
            out.push(candidate);
        }
    }
    out
}

/// A probed candidate that holds the blob, with its measured RTT.
#[derive(Debug, Clone)]
pub struct Probed {
    /// The node that answered the probe with `has_blob = true`.
    pub candidate: NodeCandidate,
    /// Round-trip time measured by the probe, in milliseconds.
    pub rtt_ms: f64,
    /// The blob size the node reported, when it knew it. UNSIGNED and outside
    /// `slash_sig` (ADR 005 §`cdn/probe/v1`), so it is a hint for sizing
    /// decisions only — never for anything a lying node could profit from. It
    /// spares the multi-source engagement gate a throwaway header open just to
    /// learn whether the blob clears the fan-out floor.
    pub total_bytes: Option<u64>,
    /// Which discovery blocks this holder answered `has_blob:true` for
    /// (`decdn_protocol::coverage`), taken from the probe's `ProbeResponseExt`
    /// (PR1). Unsigned, like `total_bytes` — a hint for the scheduler's segment
    /// assignment, never a commitment. `has_blob:true` means
    /// "will serve at least one block", so this may be a proper subset of the
    /// blob rather than the whole thing — a **partial holder** is admitted here
    /// exactly like a full one; nothing in this module treats the two
    /// differently. The requester-side `has_blob`/`coverage.is_empty()`
    /// consistency check (ADR 013 §Tier 1, `ProbeResponseExt::consistent_with`)
    /// runs before a `Probed` is ever constructed, so a malformed pairing never
    /// reaches this field.
    pub coverage: Coverage,
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
    /// Note this diverges from ADR 037 § RTT source, which specifies a
    /// longitudinal per-peer RTT map; no such map exists, so the candidate pool
    /// is limited to the ≤`SELECT_K` nodes this request happened to probe rather
    /// than the full peer table minus holders.
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
/// The full ordered list is returned so the caller falls back from a proxy that
/// declines to the next candidate and finally to the direct holder, per ADR 037
/// § Fallback. The CLI `fetch` loop iterates this whole list: each provider is
/// its own lane on the same shared payment pool, so a fallback escrows no new
/// on-chain deposit and resumes the partial it already has. Proxy warming is
/// therefore default-on, and a declining proxy never regresses a fetch.
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
        assert!(candidate_from(&node_info(bytes, true), true).is_some());
        assert!(
            candidate_from(&node_info(bytes, true), false).is_none(),
            "inactive nodes must be filtered out"
        );
    }

    /// The filter keys on the on-chain `isActive` (the `active[]` column of
    /// `getRegisteredNodes`), NOT the raw `NodeInfo.active` registration flag. A
    /// node mid-unbonding is still `NodeInfo.active == true` but `isActive ==
    /// false`, and the client must drop it up front rather than defer to probing.
    #[test]
    fn excludes_stale_active_node_when_isactive_false() {
        let key = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let info = node_info(*key.as_bytes(), true); // raw registration flag true
        assert!(
            candidate_from(&info, true).is_some(),
            "isActive true → kept"
        );
        assert!(
            candidate_from(&info, false).is_none(),
            "stale-active (isActive false) dropped despite NodeInfo.active == true"
        );
    }

    #[test]
    fn carries_eth_address_and_region() {
        let key = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        let c = candidate_from(&node_info(*key.as_bytes(), true), true).unwrap();
        assert_eq!(c.eth_address, Address::repeat_byte(0xab));
        assert_eq!(c.region_hint, Region::parse("US"));
    }

    /// An unrecognized on-chain hint must cost the node its locality bonus, not
    /// its place in the candidate set — `CapacityBond` accepts any string up to
    /// 16 bytes and ADR 030 declines to tighten it (#1348).
    #[test]
    fn an_unparseable_region_keeps_the_candidate_with_no_region() {
        let key = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        let mut info = node_info(*key.as_bytes(), true);
        info.regionHint = "not-a-region".to_string();
        let c = candidate_from(&info, true).unwrap();
        assert_eq!(c.region_hint, None, "unparseable, not rejected");
        assert_eq!(c.eth_address, Address::repeat_byte(0xab));
    }

    /// `region` is parsed, not stored raw, so a fixture written with stray case
    /// or whitespace produces the SAME candidate as its canonical spelling.
    /// That is the property the derived `Eq` depends on.
    fn candidate(seed: u8, region: &str) -> NodeCandidate {
        NodeCandidate {
            node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            eth_address: Address::repeat_byte(seed),
            region_hint: Region::parse(region),
        }
    }

    #[test]
    fn select_puts_same_region_first_and_caps_at_k() {
        // Regions are on-chain self-attested ISO 3166-1 alpha-2 codes (ADR 030);
        // seed 4 carries stray case + whitespace, which `Region::parse`
        // normalizes at the boundary so the comparison here is plain equality.
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

    /// The normalization lives in the type, so ` us ` and `US` are the same
    /// VALUE, not merely two things a comparison happens to fold together. This
    /// is what makes the derived `Eq` on `NodeCandidate` correct: without it the
    /// same node under two spellings would compare unequal, so any
    /// `dedup`/`contains`/`retain` over candidates would silently fail to
    /// dedupe.
    #[test]
    fn candidates_differing_only_in_region_spelling_are_equal() {
        assert_eq!(candidate(1, " us "), candidate(1, "US"));
        assert_ne!(candidate(1, "US"), candidate(1, "DE"));
        assert_ne!(
            candidate(1, "US"),
            candidate(1, "nonsense"),
            "an unparseable hint is None, which is not the same as any region"
        );
    }

    #[test]
    fn select_without_region_preserves_order_and_caps() {
        let cands = vec![candidate(1, "DE"), candidate(2, "US")];
        // No region, a blank one, and an unrecognized code all skip reordering
        // rather than reordering against a value that means nothing.
        for region in [None, Some("  "), Some("not-a-region")] {
            let out = select_candidates(cands.clone(), region, 5);
            assert_eq!(out[0].eth_address, Address::repeat_byte(1));
            assert_eq!(out[1].eth_address, Address::repeat_byte(2));
        }
    }

    /// A candidate whose hint did not parse sorts into the "rest" bucket — it
    /// must never be treated as matching the client's region.
    #[test]
    fn select_never_promotes_an_unparseable_region() {
        let cands = vec![candidate(1, "nonsense"), candidate(2, "US")];
        let out = select_candidates(cands, Some("US"), 5);
        assert_eq!(out[0].eth_address, Address::repeat_byte(2));
        assert_eq!(out[1].eth_address, Address::repeat_byte(1));
    }

    /// An iroh node id derived from a small seed, for [`cand`] fixtures — mirrors
    /// [`candidate`]'s own key derivation but named to match the brief's helper
    /// naming (`pk`/`addr`/`cand`) for the `admit_sources` tests below.
    fn pk(seed: u8) -> PublicKey {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    /// A distinct Ethereum address per seed, for [`cand`] fixtures — the
    /// "operator" identity `admit_sources` spreads across.
    fn addr(seed: u8) -> Address {
        Address::repeat_byte(seed)
    }

    /// Build a [`NodeCandidate`] from an already-derived node id and address,
    /// plus a region code (parsed the same way [`candidate`] does). Distinct
    /// from `candidate(seed, region)` above because `admit_sources` tests need
    /// the node id and operator address to vary independently (several nodes
    /// under the same operator).
    fn cand(node_id: PublicKey, eth_address: Address, region: Option<&str>) -> NodeCandidate {
        NodeCandidate {
            node_id,
            eth_address,
            region_hint: region.and_then(Region::parse),
        }
    }

    #[test]
    fn admit_sources_admits_one_node_per_operator_in_rank_order() {
        // Ranked: [op1/us, op1/us, op2/eu, op3/us]. max=3 → one node per
        // operator, in rank order: the FIRST op1 node, then op2, then op3 —
        // never the second op1 node, which would share op1's voucher lane.
        let ranked = vec![
            cand(pk(1), addr(1), Some("US")),
            cand(pk(2), addr(1), Some("US")),
            cand(pk(3), addr(2), Some("EU")),
            cand(pk(4), addr(3), Some("US")),
        ];
        let out = admit_sources(ranked, 3);
        // Identity, not just count: reversing the skip would still yield three
        // distinct operators, but from the wrong (lower-ranked) nodes.
        assert_eq!(
            out.iter().map(|c| c.node_id).collect::<Vec<_>>(),
            vec![pk(1), pk(3), pk(4)]
        );
    }

    /// The set SHRINKS rather than repeating an operator. Two nodes of one
    /// operator would become two payment lanes on one `(signer, provider)`
    /// watermark — concurrent voucher streams that regress each other, and two
    /// watermark writes colliding under one `LaneKey`.
    #[test]
    fn admit_sources_never_repeats_an_operator() {
        let ranked = vec![
            cand(pk(1), addr(1), Some("US")),
            cand(pk(2), addr(1), Some("US")),
            cand(pk(3), addr(1), Some("EU")),
        ];
        let out = admit_sources(ranked, 4);
        assert_eq!(out.len(), 1, "one operator admits one source, not three");
        assert_eq!(
            out[0].node_id,
            pk(1),
            "the rank-first node of that operator"
        );
    }

    /// A spare slot left by the operator-distinctness rule is NOT filled with a
    /// repeat operator: with two operators behind four nodes and `max = 4`, the
    /// admitted set is two, not four.
    #[test]
    fn admit_sources_leaves_slots_empty_rather_than_repeating() {
        let ranked = vec![
            cand(pk(1), addr(1), None),
            cand(pk(2), addr(2), None),
            cand(pk(3), addr(1), None),
            cand(pk(4), addr(2), None),
        ];
        let out = admit_sources(ranked, 4);
        assert_eq!(
            out.iter().map(|c| c.eth_address).collect::<Vec<_>>(),
            vec![addr(1), addr(2)]
        );
    }

    /// `max_sources` larger than the candidate count returns everything, not a
    /// padded or truncated set.
    #[test]
    fn admit_sources_max_larger_than_candidates_returns_all() {
        let ranked = vec![
            cand(pk(1), addr(1), Some("US")),
            cand(pk(2), addr(2), Some("EU")),
        ];
        let out = admit_sources(ranked.clone(), 10);
        assert_eq!(out.len(), 2);
        assert_eq!(out, ranked, "rank order preserved when nothing is dropped");
    }

    /// `max_sources == 0` is a valid, non-panicking request for nothing.
    #[test]
    fn admit_sources_zero_max_returns_empty() {
        let ranked = vec![cand(pk(1), addr(1), Some("US"))];
        assert_eq!(admit_sources(ranked, 0), Vec::new());
    }

    /// A [`Probed`] holder carries the coverage its probe reported, and a
    /// **partial** holder (a proper subset of the blob's blocks) is admitted
    /// exactly like a full one: `admit_sources` operates on `NodeCandidate`
    /// rank order and operator identity only, so it has no way to see —
    /// and must not need to see — that one holder's coverage is a strict
    /// subset of another's. Two operators here each answered for a disjoint
    /// half of the blob; both are admitted, and each one's `Probed` still
    /// carries its own half, not the other's or the full blob's.
    #[test]
    fn partial_holders_carry_their_own_coverage_and_are_admitted_like_full_holders() {
        let op1 = cand(pk(1), addr(1), Some("US"));
        let op2 = cand(pk(2), addr(2), Some("US"));

        // op1 holds only block 0; op2 holds only block 1 — both partial, of a
        // 2-block blob, and disjoint.
        let probed = [
            Probed {
                candidate: op1.clone(),
                rtt_ms: 10.0,
                total_bytes: Some(128 * 1024 * 1024),
                coverage: Coverage::from_block_indices(2, [0].into_iter()),
            },
            Probed {
                candidate: op2.clone(),
                rtt_ms: 12.0,
                total_bytes: Some(128 * 1024 * 1024),
                coverage: Coverage::from_block_indices(2, [1].into_iter()),
            },
        ];

        // Operator-dedup, unchanged: both are distinct operators, so both are
        // admitted — coverage never enters the admission decision.
        let admitted = admit_sources(vec![op1.clone(), op2.clone()], 2);
        assert_eq!(
            admitted.iter().map(|c| c.node_id).collect::<Vec<_>>(),
            vec![pk(1), pk(2)],
            "a partial holder is admitted exactly like a full holder"
        );

        // Each admitted candidate's own probed coverage is still its own —
        // never full, never the other holder's block.
        let cov = |node_id: PublicKey| {
            probed
                .iter()
                .find(|p| p.candidate.node_id == node_id)
                .map(|p| p.coverage.clone())
        };
        let cov1 = cov(pk(1)).expect("op1 was probed");
        let cov2 = cov(pk(2)).expect("op2 was probed");
        assert!(cov1.covers(0) && !cov1.covers(1), "op1 holds only block 0");
        assert!(cov2.covers(1) && !cov2.covers(0), "op2 holds only block 1");
        assert_ne!(cov1, Coverage::full(2), "op1 is a partial holder, not full");
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
                    _ => Ok(active_page(vec![node_info(valid_node_id(3), true)])),
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

    /// Wrap a page of `NodeInfo` with an all-`true` `active[]` of matching length,
    /// mirroring what `getRegisteredNodes` returns for a page of active operators.
    /// These pagination tests exercise the retry/cursor control flow, not the
    /// active-filter, so every entry is active.
    fn active_page(infos: Vec<CapacityBond::NodeInfo>) -> (Vec<CapacityBond::NodeInfo>, Vec<bool>) {
        let n = infos.len();
        (infos, vec![true; n])
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

    /// The retry budget is spent across the WHOLE read, not refilled per page
    /// (#1349). Without this, a fully-failing N-page read cost 36 s × N — a
    /// figure the caller cannot bound, since the page count follows how many
    /// nodes are registered.
    ///
    /// The fixture puts retries on both sides of a page boundary: page 0 fails
    /// twice then succeeds with a FULL page (so pagination continues), and
    /// page 1 fails forever. Only the third backoff step is left for page 1, so
    /// the read ends after 5 calls having slept the schedule exactly once.
    /// Under the old per-page reset it would be 7 calls and 42 s.
    #[tokio::test(start_paused = true)]
    async fn the_retry_budget_is_spent_across_pages_not_refilled() {
        let calls = std::cell::Cell::new(0usize);
        let start = tokio::time::Instant::now();

        let err = paginate_with_retry(|offset| {
            let n = calls.get();
            calls.set(n.saturating_add(1));
            async move {
                match (offset, n) {
                    // Page 0: two transient failures burn steps 1 and 2 …
                    (0, 0 | 1) => Err(transient()),
                    // … then a full page, which is what makes the loop ask for
                    // a second page rather than terminating on a short one.
                    (0, _) => Ok(active_page(
                        (0..PAGE_SIZE)
                            .map(|i| {
                                // `u8` seeds wrap past 255; PAGE_SIZE is 100, so
                                // every id here is distinct regardless.
                                node_info(valid_node_id(u8::try_from(i).unwrap_or(0)), true)
                            })
                            .collect(),
                    )),
                    // Page 1 never succeeds.
                    _ => Err(transient()),
                }
            }
        })
        .await
        .unwrap_err();

        assert_eq!(
            calls.get(),
            5,
            "3 calls for page 0 (2 failures + success), then 2 for page 1 \
             (initial + the single remaining retry) — not 7"
        );
        assert_eq!(
            tokio::time::Instant::now() - start,
            REGISTRY_RETRY_BACKOFF.iter().sum::<Duration>(),
            "the schedule is slept once for the whole read, not once per page"
        );
        let msg = format!("{err:#}");
        assert!(msg.contains("across the whole registry read"), "{msg}");
        assert!(
            msg.contains(&format!("offset={PAGE_SIZE}")),
            "the error names the page that ran the budget out: {msg}"
        );
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

    /// `getRegisteredNodes` returns `page` and `active` as equal-length arrays by
    /// construction (the contract fills both in one loop). A divergence means an
    /// ABI/decoder fault, and zipping would silently truncate — dropping
    /// candidates without a trace. Fail loudly instead.
    #[tokio::test]
    async fn mismatched_page_and_active_lengths_error_rather_than_truncate() {
        let err = paginate_with_retry(|_offset| async {
            Ok((vec![node_info(valid_node_id(1), true)], Vec::<bool>::new()))
        })
        .await
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("mismatched"),
            "expected a length-mismatch error, got: {err:#}"
        );
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
                    1 => Ok(active_page(
                        (0..PAGE_SIZE)
                            .map(|_| node_info(valid_node_id(1), true))
                            .collect(),
                    )),
                    // …and the short second page ends the read.
                    _ => Ok(active_page(vec![node_info(valid_node_id(2), true)])),
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
        // Derived from the constant, not hardcoded: the point of the assertion
        // is that both numbers reach the operator, and a bump must not silently
        // turn it into a comparison of two stale literals.
        assert!(
            why.contains(&format!("is version {}", PEER_CACHE_VERSION + 1)),
            "{why}"
        );
        assert!(
            why.contains(&format!("reads version {PEER_CACHE_VERSION}")),
            "{why}"
        );
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
                .contains(&format!("is version {}", PEER_CACHE_VERSION + 1))
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

    /// The `--timeout-ms` bound must not cost a client its outage protection
    /// (#1349). Timing out is one more way for the registry read to fail, so it
    /// has to land on the cache-fallback path like any other failure.
    ///
    /// This is the regression a naive fix reintroduces: wrapping
    /// `bootstrap_nodes` in `tokio::time::timeout` from the CALL SITE cancels
    /// `resolve_bootstrap` along with the read, so a client holding a perfectly
    /// good `peers.json` gets a hard error instead of a working fetch. Any
    /// `--timeout-ms` below the schedule's own 36 s hits this, which is exactly
    /// the range the flag was widened for.
    #[tokio::test(start_paused = true)]
    async fn a_timed_out_registry_read_still_falls_back_to_the_cache() {
        let dir = tempfile::tempdir().unwrap();
        let peers = vec![candidate(7, "US")];
        write_peer_cache(dir.path(), &peers).unwrap();

        // An unroutable address, so the read cannot finish inside the budget.
        // Paused time makes the 5 s deadline instant.
        let out = bootstrap_nodes(
            "http://127.0.0.1:1",
            Address::repeat_byte(0x11),
            dir.path(),
            Duration::from_secs(5),
        )
        .await
        .expect("a timeout with a usable cache must not fail the fetch");

        let warning = out.warning().unwrap();
        assert!(
            warning.contains("did not finish within"),
            "the warning names the deadline as the cause: {warning}"
        );
        assert!(
            warning.contains("--timeout-ms"),
            "and names the flag that set it: {warning}"
        );
        assert_eq!(
            out.into_peers(),
            peers,
            "the cached peers are what the fetch proceeds with"
        );
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
