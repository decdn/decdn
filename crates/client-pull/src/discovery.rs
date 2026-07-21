//! Client-side node discovery (#936): read the active node set from
//! `CapacityBond.getActiveNodes` so `decdn fetch`/`probe` can pick a node
//! instead of being handed an explicit `--node-id`/`--addr`/`--provider-address`.
//!
//! This is the **read + select** half. Dialing the chosen node uses its iroh
//! `NodeId` via a discovery-enabled endpoint (`presets::N0` / configured
//! `[network.discovery]`) — the same mechanism the node uses — so the registry
//! `multiaddrs` field is not decoded here; a self-contained `multiaddrs`
//! decoder is the deferred, more-decentralized fallback (#936 § fallback).

use std::path::{Path, PathBuf};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use alloy::providers::ProviderBuilder;
use anyhow::Context;
use decdn_incentive::capacity_bond::CapacityBond;
use iroh::PublicKey;
use serde::{Deserialize, Serialize};

/// Page size for the paginated `getActiveNodes` read (ADR 019 § Step 3.3's
/// worked-example limit). At `PoC` scale (tens of nodes) one page suffices; the
/// loop preserves the pattern for production scale.
const PAGE_SIZE: u64 = 100;

/// Backoff before each retry of a failed `getActiveNodes` page call — ADR 012
/// § Bootstrap step 3: "retry 3× exponential backoff (1 s, 5 s, 30 s)". The
/// length of the table is the retry count.
///
/// Deliberately not `decdn_config_types::RetryPolicy`: that is a *doubling*
/// policy scoped to node-side origin fetches, so reusing it here would silently
/// produce 1 s / 2 s / 4 s, and it would drag an origin-side config type across
/// the client's dependency edge.
const REGISTRY_RETRY_BACKOFF: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(30),
];

/// Filename of the peer cache inside the resolved client data dir.
const PEER_CACHE_FILE: &str = "peers.json";

/// Version tag written into (and required of) the peer cache file, so a future
/// shape change is a cache miss rather than a decode error.
const PEER_CACHE_VERSION: u32 = 1;

/// The error the client exits with when the registry is unreachable and no
/// cached peer list exists — the exact wording pinned by ADR 012 § Bootstrap
/// step 4.
pub const BOOTSTRAP_UNREACHABLE: &str =
    "Cannot reach bootstrap sources. Check network connectivity and RPC endpoint configuration.";

/// A node the client may fetch from, distilled from a registry `NodeInfo`.
/// `multiaddrs` is intentionally dropped — dialing is by `node_id` via iroh
/// discovery (see module docs).
///
/// Serializable so the resolved set can be persisted to the peer cache and
/// reloaded when the registry is unreachable (ADR 012 § Bootstrap step 4).
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

/// Read the active node set from `CapacityBond.getActiveNodes` at
/// `capacity_bond_addr` over `rpc_url` (a read-only HTTP provider — no signer
/// needed for a view call). Paginated; inactive / undecodable entries are
/// skipped.
///
/// # Errors
///
/// Fails if `rpc_url` is not a valid URL or a `getActiveNodes` page call fails.
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

    let mut out = Vec::new();
    let mut offset = 0u64;
    loop {
        // ADR 012 § Bootstrap step 3: retry a failed page on the fixed backoff
        // schedule before giving up on the whole read.
        let mut attempt = 0usize;
        let page = loop {
            match registry
                .getActiveNodes(U256::from(offset), U256::from(PAGE_SIZE))
                .call()
                .await
            {
                Ok(page) => break page,
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
                    // The transport error is logged as-is (never the `rpc_url`,
                    // which commonly carries an API key) — the same error text
                    // already reaches the user through the context chain above.
                    tracing::warn!(
                        offset,
                        attempt,
                        backoff_ms = backoff.as_millis(),
                        error = %e,
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

/// On-disk shape of the peer cache. The version tag lets a later shape change
/// be read as "no usable cache" instead of a hard decode failure.
#[derive(Debug, Serialize, Deserialize)]
struct PeerCache {
    version: u32,
    peers: Vec<NodeCandidate>,
}

/// Path of the cached peer list inside the resolved client data dir
/// (`--data-dir` / `identity.data_dir`, defaulting to `~/.decdn/client`).
#[must_use]
pub fn peer_cache_path(data_dir: &Path) -> PathBuf {
    data_dir.join(PEER_CACHE_FILE)
}

/// Read the cached peer list, or `None` when the cache is absent, unreadable,
/// undecodable, of an unknown version, or empty. The one caller — the
/// exhausted-retries fallback — cannot act on any of those differently, so a
/// broken cache is deliberately collapsed into a missing one.
fn read_peer_cache(data_dir: &Path) -> Option<Vec<NodeCandidate>> {
    let path = peer_cache_path(data_dir);
    let raw = std::fs::read(&path).ok()?;
    let cache: PeerCache = serde_json::from_slice(&raw)
        .inspect_err(|e| {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "peer cache is undecodable; ignoring it"
            );
        })
        .ok()?;
    if cache.version != PEER_CACHE_VERSION {
        tracing::warn!(
            path = %path.display(),
            found = cache.version,
            expected = PEER_CACHE_VERSION,
            "peer cache version mismatch; ignoring it"
        );
        return None;
    }
    if cache.peers.is_empty() {
        return None;
    }
    Some(cache.peers)
}

/// Persist `peers` to the peer cache, creating `data_dir` if needed. Written to
/// a uniquely-named sibling temp file, fsynced, and renamed, so neither a crash
/// mid-write nor a second `decdn` process writing the same data dir can leave a
/// truncated or interleaved cache in place.
///
/// # Errors
///
/// Fails if the data dir cannot be created or the write/sync/rename fails.
fn write_peer_cache(data_dir: &Path, peers: &[NodeCandidate]) -> anyhow::Result<()> {
    use std::io::Write as _;

    let path = peer_cache_path(data_dir);
    let body = serde_json::to_vec_pretty(&PeerCache {
        version: PEER_CACHE_VERSION,
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
/// § Bootstrap steps 4, 6, and 7): persist a successful read to the peer cache,
/// or on failure fall back to the cache, or — with no cache — surface
/// [`BOOTSTRAP_UNREACHABLE`] with the registry failure as its cause.
///
/// An empty successful read is returned as-is but does **not** overwrite the
/// cache: an emptied registry is not a reason to discard the last known-good
/// peer list, and the callers reject an empty set anyway.
fn resolve_bootstrap(
    registry: anyhow::Result<Vec<NodeCandidate>>,
    data_dir: &Path,
) -> anyhow::Result<Vec<NodeCandidate>> {
    let err = match registry {
        Ok(peers) => {
            if !peers.is_empty()
                && let Err(e) = write_peer_cache(data_dir, &peers)
            {
                // A cache we could not persist only costs us the next outage's
                // fallback; it must not fail a fetch that already succeeded.
                tracing::warn!(error = %e, "could not persist the peer cache");
            }
            return Ok(peers);
        }
        Err(e) => e,
    };
    match read_peer_cache(data_dir) {
        Some(peers) => {
            tracing::warn!(
                peers = peers.len(),
                error = %err,
                "registry unreachable; falling back to the cached peer list"
            );
            Ok(peers)
        }
        None => Err(err.context(BOOTSTRAP_UNREACHABLE)),
    }
}

/// Bootstrap the client's peer set (ADR 012 § Bootstrap): read the active node
/// set from `CapacityBond` with the ADR's retry schedule, persisting it to the
/// peer cache under `data_dir` on success and falling back to that cache when
/// the registry cannot be reached.
///
/// `data_dir` is the resolved client data dir, so an explicit `--data-dir`
/// moves the cache with the rest of the client's state.
///
/// # Errors
///
/// Fails with [`BOOTSTRAP_UNREACHABLE`] when every retry is exhausted and no
/// cached peer list exists.
pub async fn bootstrap_nodes(
    rpc_url: &str,
    capacity_bond_addr: Address,
    data_dir: &Path,
) -> anyhow::Result<Vec<NodeCandidate>> {
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
        // (1 s, 5 s, 30 s)". Asserted against the table the retry loop reads so
        // the schedule is checked without sleeping 36 s.
        assert_eq!(
            REGISTRY_RETRY_BACKOFF,
            [
                Duration::from_secs(1),
                Duration::from_secs(5),
                Duration::from_secs(30),
            ]
        );
    }

    #[test]
    fn peer_cache_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let peers = vec![candidate(1, "DE"), candidate(2, "US")];
        write_peer_cache(dir.path(), &peers).unwrap();
        assert_eq!(read_peer_cache(dir.path()), Some(peers));
    }

    #[test]
    fn a_cache_of_an_unknown_version_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        let body = serde_json::json!({
            "version": PEER_CACHE_VERSION + 1,
            "peers": [candidate(1, "DE")],
        });
        std::fs::write(peer_cache_path(dir.path()), body.to_string()).unwrap();
        assert!(read_peer_cache(dir.path()).is_none());
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
    fn absent_cache_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_peer_cache(dir.path()).is_none());
    }

    #[test]
    fn successful_read_persists_the_cache() {
        let dir = tempfile::tempdir().unwrap();
        let peers = vec![candidate(5, "FR")];
        assert_eq!(
            resolve_bootstrap(Ok(peers.clone()), dir.path()).unwrap(),
            peers
        );
        assert_eq!(read_peer_cache(dir.path()), Some(peers));
    }

    #[test]
    fn exhausted_retries_fall_back_to_the_cache() {
        let dir = tempfile::tempdir().unwrap();
        let peers = vec![candidate(3, "US"), candidate(4, "DE")];
        write_peer_cache(dir.path(), &peers).unwrap();
        let out = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap();
        assert_eq!(out, peers);
    }

    #[test]
    fn exhausted_retries_without_a_cache_report_the_adr_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap_err();
        assert_eq!(format!("{err}"), BOOTSTRAP_UNREACHABLE);
        assert_eq!(
            BOOTSTRAP_UNREACHABLE,
            "Cannot reach bootstrap sources. Check network connectivity and RPC endpoint \
             configuration."
        );
        // The registry failure stays in the chain as the cause.
        assert!(format!("{err:#}").contains("rpc down"));
    }

    #[test]
    fn an_empty_registry_read_leaves_a_populated_cache_intact() {
        let dir = tempfile::tempdir().unwrap();
        let peers = vec![candidate(6, "US")];
        write_peer_cache(dir.path(), &peers).unwrap();
        assert!(
            resolve_bootstrap(Ok(Vec::new()), dir.path())
                .unwrap()
                .is_empty()
        );
        assert_eq!(read_peer_cache(dir.path()), Some(peers));
    }
}
