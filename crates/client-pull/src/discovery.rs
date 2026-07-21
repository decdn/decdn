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
