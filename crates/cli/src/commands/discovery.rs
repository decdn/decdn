//! Client-side node discovery (#936): read the active node set from
//! `CapacityBond.getActiveNodes` so `decdn fetch`/`probe` can pick a node
//! instead of being handed an explicit `--node-id`/`--addr`/`--provider-address`.
//!
//! This is the **read + select** half. Dialing the chosen node uses its iroh
//! `NodeId` via a discovery-enabled endpoint (`presets::N0` / configured
//! `[network.discovery]`) — the same mechanism the node uses — so the registry
//! `multiaddrs` field is not decoded here; a self-contained `multiaddrs`
//! decoder is the deferred, more-decentralized fallback (#936 § fallback).

use alloy::primitives::{Address, U256};
use alloy::providers::ProviderBuilder;
use anyhow::Context;
use decdn_incentive::capacity_bond::CapacityBond;
use iroh::PublicKey;

/// Page size for the paginated `getActiveNodes` read (ADR 019 § Step 3.3's
/// worked-example limit). At `PoC` scale (tens of nodes) one page suffices; the
/// loop preserves the pattern for production scale.
const PAGE_SIZE: u64 = 100;

/// A node the client may fetch from, distilled from a registry `NodeInfo`.
/// `multiaddrs` is intentionally dropped — dialing is by `node_id` via iroh
/// discovery (see module docs).
#[derive(Debug, Clone)]
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
        let page = registry
            .getActiveNodes(U256::from(offset), U256::from(PAGE_SIZE))
            .call()
            .await
            .with_context(|| format!("CapacityBond.getActiveNodes(offset={offset})"))?;
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
}
