//! Client-side node discovery (#936): read the active node set from
//! `CapacityBond.getRegisteredNodes` so a client can pick a node instead of being
//! handed an explicit `--node-id`/`--addr`/`--provider-address`. Read from
//! `fetch::discover_provider` and from the discovery branch of
//! `bundle_pull::bundle_pull`; the ranking half is also used by
//! `fetch::probe_and_rank`. `decdn probe` deliberately still requires an
//! explicit target.
//!
//! This is the **read + select** half, plus the peer store that backs it up.
//! Dialing the chosen node uses its iroh `NodeId` on a discovery-enabled
//! endpoint (`presets::N0` / configured `[network.discovery]`), and the
//! registry's `NodeInfo.multiaddrs` ride along as iroh direct-address hints
//! ([`NodeCandidate::dial_addrs`](crate::discovery::NodeCandidate::dial_addrs),
//! [`with_dial_addrs`](crate::discovery::with_dial_addrs)): a reachable node then
//! connects without a relay, while discovery and the relay path stay as the
//! fallback for a node behind NAT (ADR 001 § Node Discovery). The hints are
//! additive — a stale or malformed address loses the path race but never fails
//! a dial.
//!
//! `bootstrap_nodes` is the entry point: it wraps the registry read in ADR
//! 012's retry schedule and refreshes the [`crate::PeerStore`] identity
//! records under the client data dir on success, falling back to the store's
//! surviving identities when the registry cannot be read. The store — one
//! JSON file per peer under `<data_dir>/peers` — is the only filesystem state
//! this module owns.

use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::ProviderBuilder;
use anyhow::Context;
use decdn_common::redact::{sanitize_err_chain, sanitize_error_sources};
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_protocol::{Coverage, Region};
use iroh::PublicKey;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use crate::provider::is_permanent_contract_error;

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
/// Dialing is by `node_id`; the registry-published `multiaddrs` ride along as
/// iroh direct-address hints so a reachable peer connects without a relay (see
/// module docs and [`NodeCandidate::dial_addrs`]).
///
/// Serializable so the resolved set can be projected into [`crate::PeerStore`]
/// identity records and reloaded from the store when the registry is
/// unreachable (ADR 012 § Bootstrap step 4).
///
/// That encoding is a private implementation detail of the peer store, **not**
/// a stable format: the JSON keys are the field names, and the bytes are
/// codec-dependent (iroh renders `PublicKey` as z-base-32 under a
/// human-readable codec and as raw bytes otherwise).
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
    /// peer-store read path (#1348).
    pub region_hint: Option<Region>,
    /// The node's registry-published, packed `multiaddrs` field
    /// (`pack_multiaddrs` framing), decoded on demand by [`Self::dial_addrs`]
    /// into iroh direct-address hints. Self-attested and additive: it can only
    /// add a direct dial path, never gate one. Carried through the peer store,
    /// so a candidate rebuilt on a registry outage keeps the last-seen addresses
    /// and can dial a reachable peer directly, without iroh discovery.
    pub multiaddrs: Bytes,
}

impl NodeCandidate {
    /// Registry-published direct dial addresses, decoded leniently from the
    /// packed on-chain `multiaddrs` field for use as iroh direct-address hints
    /// (ADR 001 § Node Discovery). Empty when the node published none or the
    /// field is malformed. A returned address is only ever one dial path among
    /// discovery and the relay fallback, so a stale entry loses the path race
    /// but never fails the connection.
    #[must_use]
    pub fn dial_addrs(&self) -> Vec<std::net::SocketAddr> {
        decdn_incentive::node_register::decode_dial_addrs(&self.multiaddrs)
    }
}

/// Attach a candidate's registry-published direct addresses to `target` as iroh
/// direct-address hints (ADR 001 § Node Discovery), so a reachable peer connects
/// without a relay. Additive: an empty or malformed `multiaddrs` field adds
/// nothing and leaves iroh discovery and the relay fallback untouched, so this
/// can only speed or enable a dial, never fail one.
#[must_use]
pub fn with_dial_addrs(mut target: iroh::EndpointAddr, cand: &NodeCandidate) -> iroh::EndpointAddr {
    for sock in cand.dial_addrs() {
        target = target.with_ip_addr(sock);
    }
    target
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
        // Carried raw and decoded only at dial time (`dial_addrs`); malformed
        // bytes cost a direct-dial hint, never the candidate.
        multiaddrs: info.multiaddrs.clone(),
    })
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
/// is [`is_permanent_contract_error`].
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
                // A deterministic failure short-circuits the schedule. The two
                // likeliest first-run mistakes both land here: a typo'd
                // `blockchain.capacity_bond_address` (the call returns `0x`,
                // decoded as `ZeroData`) and an expired or wrong RPC API key
                // (HTTP 401/403 with a plain body; a JSON-RPC error body is
                // judged by its code instead). Retrying either one would sit silently for
                // 36 s and then blame network connectivity.
                Err(e) if is_permanent_contract_error(&e) => {
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
                        error = %sanitize_error_sources(&e),
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
#[doc(hidden)]
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

/// Wall-clock seconds since the Unix epoch, or 0 if the clock predates it.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// Turn a registry read outcome into the bootstrap peer set (ADR 012
/// § Bootstrap steps 4 and 7 — step 6, building the peer table, is
/// `select_candidates` in the callers): on a live read, refresh every
/// candidate's identity into the [`crate::PeerStore`] under `data_dir` and
/// prune/cap the store, or on failure fall back to the store's
/// non-prunable identities, or — with the store empty too — surface
/// [`BOOTSTRAP_UNREACHABLE`] with the registry failure as its cause.
///
/// An empty successful read is returned as-is but does **not** touch the
/// store: an emptied registry is not a reason to discard the last known-good
/// identities.
fn resolve_bootstrap(
    registry: anyhow::Result<Vec<NodeCandidate>>,
    data_dir: &Path,
) -> anyhow::Result<Bootstrap> {
    let store = crate::PeerStore::open(data_dir);
    let cfg = crate::StoreConfig::default();
    let now = now_secs();
    let err = match registry {
        Ok(peers) => {
            // Best-effort identity refresh; a write failure must not fail a
            // fetch that already succeeded — the store is a fallback, not the
            // source of truth for a live read. An empty read touches nothing:
            // an emptied registry is not a reason to prune or discard the
            // last known-good identities. The first write failure is still
            // carried on the return value so a degraded store is visible to
            // the caller instead of failing silently.
            let mut store_warning = None;
            if !peers.is_empty() {
                for cand in &peers {
                    if let Err(e) = store.upsert_identity(cand, now) {
                        store_warning.get_or_insert_with(|| e.to_string());
                    }
                }
                if let Err(e) = store.prune_and_cap(now, &cfg) {
                    store_warning.get_or_insert_with(|| e.to_string());
                }
            }
            return Ok(Bootstrap::Live {
                peers,
                store_warning,
            });
        }
        Err(e) => e,
    };
    let records = store.load_all();
    // The staleness of the fallback identities, computed from the store
    // records before they are projected down to `NodeCandidate`s below —
    // `as_candidate` drops `identity_seen_at_secs`.
    let oldest_identity_secs = records
        .iter()
        .filter(|r| !r.identity_prunable(now, &cfg))
        .map(|r| r.identity_seen_at_secs)
        .min();
    let cached: Vec<NodeCandidate> = records
        .into_iter()
        .filter(|r| !r.identity_prunable(now, &cfg))
        .map(|r| r.as_candidate())
        .collect();
    let Some(oldest_identity_secs) = oldest_identity_secs else {
        return Err(err.context(BOOTSTRAP_UNREACHABLE));
    };
    Ok(Bootstrap::Cached {
        peers: cached,
        // Sanitized `{err:#}`, not `%err`: plain Display on an
        // `anyhow::Error` renders only the outermost context and drops the
        // reason the registry read actually failed.
        registry_error: sanitize_err_chain(&err),
        oldest_identity_secs,
    })
}

/// Where a bootstrap peer set came from.
///
/// The provenance is returned rather than logged from here so the caller
/// decides how to surface "the registry is down and this list may be months
/// old" — exactly what a user must not miss. Callers log [`Self::warning`] and
/// then take [`Self::into_peers`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Bootstrap {
    /// Read live from the on-chain registry.
    Live {
        /// The peers the registry returned.
        peers: Vec<NodeCandidate>,
        /// The first peer-store write error hit while refreshing identities
        /// or pruning, if any. The fetch itself never fails on this — the
        /// store is best-effort — but a persistent write failure degrades
        /// the next run's fallback and selection, so it is surfaced here
        /// rather than swallowed.
        store_warning: Option<String>,
    },
    /// The registry could not be read; these peers came from the peer store.
    Cached {
        /// The peers the peer store held.
        peers: Vec<NodeCandidate>,
        /// Why the registry read failed, sanitized for display.
        registry_error: String,
        /// Seconds since the Unix epoch when the stalest identity among
        /// `peers` was last confirmed against the registry — the minimum
        /// `identity_seen_at_secs` across the fallback set, so it bounds how
        /// old the least-fresh record in the set may be.
        oldest_identity_secs: u64,
    },
}

/// Render a duration in seconds as a coarse, human-readable age: seconds
/// under a minute, minutes under an hour, hours under a day, else days.
fn format_age_secs(age_secs: u64) -> String {
    if age_secs < 60 {
        format!("{age_secs}s")
    } else if age_secs < 3_600 {
        format!("{}m", age_secs / 60)
    } else if age_secs < 86_400 {
        format!("{}h", age_secs / 3_600)
    } else {
        format!("{}d", age_secs / 86_400)
    }
}

impl Bootstrap {
    /// A degraded-bootstrap message for the caller to log at `WARN`, or `None`
    /// when the bootstrap was wholly healthy. Carries no severity prefix — the
    /// caller's log level supplies it.
    #[must_use]
    pub fn warning(&self) -> Option<String> {
        match self {
            Self::Live { store_warning, .. } => store_warning.as_ref().map(|e| {
                format!("peer store write failed: {e} (selection may be degraded next run)")
            }),
            Self::Cached {
                peers,
                registry_error,
                oldest_identity_secs,
            } => {
                let age = format_age_secs(now_secs().saturating_sub(*oldest_identity_secs));
                Some(format!(
                    "could not reach the node registry ({registry_error}); using {} \
                     previously known node(s) from the local peer store, identity up to {age} \
                     old. These nodes may have been deactivated or slashed since.",
                    peers.len()
                ))
            }
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

/// Bootstrap the client's peer set (ADR 012 § Bootstrap): read the active node
/// set from `CapacityBond` with the ADR's retry schedule, refreshing the
/// [`crate::PeerStore`] under `data_dir` on success and falling back to the
/// store's surviving identities when the registry cannot be read.
///
/// `data_dir` is the resolved client data dir, so an explicit `--data-dir`
/// moves the peer store with the rest of the client's state.
///
/// Callers must log [`Bootstrap::warning`] — the degraded paths are invisible
/// otherwise.
///
/// # Errors
///
/// Fails with [`BOOTSTRAP_UNREACHABLE`] when the registry read fails and the
/// peer store holds no usable identity.
pub async fn bootstrap_nodes(
    rpc_url: &str,
    capacity_bond_addr: Address,
    data_dir: &Path,
    registry_cap: Duration,
) -> anyhow::Result<Bootstrap> {
    // The deadline bounds the REGISTRY READ ONLY, not the whole bootstrap
    // (#1349). Wrapping `bootstrap_nodes` from outside would drop
    // `resolve_bootstrap`'s result along with it, and that is where the ADR
    // 012 § Bootstrap step 4 peer-store fallback lives — so a client with a
    // perfectly good peer store and a flaky RPC would get a hard failure
    // instead of a degraded-but-working fetch. Timing out is just another way
    // for the registry read to fail, so it is fed in as one and the existing
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
    // Off the runtime: a live read writes and fsyncs one peer record per
    // registered node, and a caller's lanes share this task (#2211). A panic
    // in the store pass fails the bootstrap, and discards a registry read
    // that succeeded along with it.
    let data_dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || resolve_bootstrap(registry, &data_dir))
        .await
        .map_err(|e| {
            anyhow::Error::new(e).context("the peer-store bootstrap task panicked or was cancelled")
        })?
}

/// Candidates probed before ranking (decision 3): shuffle, order region-first,
/// take K, then probe those K for liveness + blob-holding. At `PoC` scale a
/// small K keeps the probe fan-out cheap while still giving the ranker a choice.
#[doc(hidden)]
pub const SELECT_K: usize = 5;

/// Pick `candidates` for probing (decision 3): shuffle, then same-region
/// candidates first (equality on `region_hint` — locality, not geo distance),
/// then the rest, capped at `k`. When `client_region` is `None`, empty, or not
/// an accepted region code the region-first ordering is skipped and the result
/// is a uniform random sample of `k`.
///
/// The shuffle is the load-bearing step. The input order is never one the
/// ranker chose. A live registry read arrives in the `CapacityBond` registry
/// array order — push on `registerNode`, swap-and-pop on deregister — so an
/// operator that registers or leaves decides who takes the slot it frees,
/// and every client reading the same block sees the same order. A
/// peer-store fallback or the store's identity-fresh set arrives in
/// whatever order the filesystem returns the `<node-id>.json` entries in,
/// stable across runs for that client. Truncating any of them as-is hands
/// out the probe slots by registration timing or filename. The shuffle is the same requester-side
/// defense the node's DHT lookup applies (ADR 022 § `FIND_VALUE` Flow,
/// *Lookup integrity*). It runs on the client, the party it protects, so no
/// node can patch it out.
///
/// Both sides are [`Region`]s, so this is a plain equality: the trim +
/// case-fold runs at the parse boundary, not on every comparison (#1348).
#[must_use]
pub fn select_candidates(
    mut candidates: Vec<NodeCandidate>,
    client_region: Option<&str>,
    k: usize,
) -> Vec<NodeCandidate> {
    // Shuffle FIRST: the sort below is stable, so a shuffle before it leaves
    // region-first in place and randomizes the order within each group. A
    // shuffle after the sort would undo the region preference; a shuffle
    // after the truncate would only permute the deterministic prefix.
    candidates.shuffle(&mut rand::rng());
    // `client_region` stays a `&str` and is parsed HERE, not assumed valid.
    // `decdn-common`'s `normalize_region` validates the node daemon's config
    // region, but the client path does not go through it: `decdn fetch`'s
    // `--region` / `[identity] region` reach `ResolvedChain::region` as a raw
    // `String`. So this is the validating boundary for the client, and an
    // unrecognized value means "no locality information" — the ordering is
    // skipped rather than applied against a value that means nothing.
    if let Some(region) = client_region.and_then(Region::parse) {
        // Stable sort by a bool key: same-region (`false`) before the rest (`true`).
        candidates.sort_by_key(|c| c.region_hint != Some(region));
    }
    candidates.truncate(k);
    candidates
}

/// Pre-filter `candidates` by an optional region allowlist, then sample via
/// [`select_candidates`].
///
/// A candidate whose `region_hint` is `Some(r)` with `r` not in `allow` is
/// dropped; a candidate with `region_hint == None` is kept. An empty `allow`
/// is a no-op. Ranking is unchanged (region is a self-attested hint, never a
/// ranking key) — this only narrows which candidates are eligible to be
/// probed/discovered at all, independent of the peer store.
#[must_use]
#[doc(hidden)]
pub fn select_candidates_filtered(
    candidates: Vec<NodeCandidate>,
    client_region: Option<&str>,
    k: usize,
    allow: &[Region],
) -> Vec<NodeCandidate> {
    let filtered = if allow.is_empty() {
        candidates
    } else {
        candidates
            .into_iter()
            .filter(|c| match c.region_hint {
                Some(r) => allow.contains(&r),
                None => true,
            })
            .collect()
    };
    select_candidates(filtered, client_region, k)
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
    /// The blob size the node reported, when it knew it. Unsigned and
    /// outside `slash_sig` (ADR 005 §`cdn/probe/v1`), so it is a hint: a fetch
    /// takes it as its first size claim and grows or shrinks it as verified
    /// bytes land ([`crate::acquire`]).
    pub total_bytes: Option<u64>,
    /// Which discovery blocks this holder answered `has_blob:true` for
    /// (`decdn_protocol::coverage`), taken from the probe's `ProbeResponseExt`.
    /// Unsigned and outside `slash_sig`: a hint for the scheduler's segment
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
#[doc(hidden)]
pub struct WarmingCandidate {
    /// The candidate proxy's iroh id.
    pub node_id: PublicKey,
    /// Its Ethereum address — the `--provider-address` a warming channel opens
    /// and the `slash_sig` is verified against.
    pub eth_address: Address,
    /// Measured round-trip time in milliseconds, from the **live `cdn/probe/v1`
    /// probe issued for this request**.
    ///
    /// The persisted peer knowledge base (ADR 037 § RTT source,
    /// `decdn_client::peer_store`) does not track which peers hold which
    /// blobs, only identity, latency, and price — it cannot tell a non-holder
    /// from a holder for this hash. So this warming candidate pool still comes
    /// only from nodes this request actually probed (`has_blob: false`
    /// responses), not from the full peer store minus holders.
    pub rtt_ms: f64,
    /// The candidate's registry-published, packed `multiaddrs` field, carried
    /// from its [`NodeCandidate`] so a warming lane dials the proxy directly
    /// (see [`NodeCandidate::multiaddrs`]). Never read by ranking.
    pub multiaddrs: Bytes,
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
#[doc(hidden)]
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
mod tests;
