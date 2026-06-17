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
    /// Optional ISO-3166 region hint for locality-aware selection.
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
    let provider = ProviderBuilder::new().connect_http(
        rpc_url
            .parse()
            .with_context(|| format!("rpc_url {rpc_url:?} is not a valid URL"))?,
    );
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
            regionHint: "us-east".to_string(),
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
        assert_eq!(c.region_hint, "us-east");
    }
}
