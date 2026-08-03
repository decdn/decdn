//! Hash → node discovery (Task 10): resolve a content hash to a node to serve
//! it from, by reading the active node set from `CapacityBond` (via
//! `decdn_client_pull::discovery::active_nodes`) and picking deterministically.
//!
//! v1 policy: any active, bonded node can serve any content-addressed blob
//! after a pull-through miss, so `resolve` ignores `hash` entirely and just
//! picks a node from the active set. Picking is deterministic (lowest eth
//! address) so repeated resolves for the same active set agree, which keeps
//! routing idempotent even though there is no content-aware placement yet.

use alloy::primitives::Address;
use decdn_client_pull::discovery::active_nodes;

/// A node selected to serve a request: its iroh node id and the Ethereum
/// address a payment channel is opened against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePick {
    pub node_id: [u8; 32],
    pub provider: Address,
}

/// Pick the candidate with the lowest Ethereum address, or `None` if `cands`
/// is empty. Deterministic so the same active set always resolves to the same
/// node.
#[must_use]
pub fn pick_deterministic(cands: &[([u8; 32], Address)]) -> Option<NodePick> {
    cands
        .iter()
        .min_by_key(|(_, addr)| *addr)
        .map(|(node_id, provider)| NodePick {
            node_id: *node_id,
            provider: *provider,
        })
}

/// Resolves a content hash to a node to fetch it from.
pub struct Discovery {
    pub rpc_url: String,
    pub capacity_bond: Address,
}

impl Discovery {
    /// Resolve `hash` to a node pick by reading the active node set and
    /// picking deterministically. `hash` is currently unused (v1 policy: any
    /// active node can serve any blob) but stays part of the signature so a
    /// future content-aware placement policy is a body change, not a call-site
    /// change.
    ///
    /// # Errors
    ///
    /// Fails if the `CapacityBond.getActiveNodes` read fails (see
    /// `decdn_client_pull::discovery::active_nodes`).
    pub async fn resolve(&self, _hash: [u8; 32]) -> anyhow::Result<Option<NodePick>> {
        let nodes = active_nodes(&self.rpc_url, self.capacity_bond).await?;
        let cands: Vec<([u8; 32], Address)> = nodes
            .into_iter()
            .map(|c| (*c.node_id.as_bytes(), c.eth_address))
            .collect();
        Ok(pick_deterministic(&cands))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    #[test]
    fn picks_lowest_eth_address_for_determinism() {
        let cands = vec![
            ([2u8; 32], address!("0000000000000000000000000000000000000022")),
            ([1u8; 32], address!("0000000000000000000000000000000000000011")),
        ];
        let pick = pick_deterministic(&cands).unwrap();
        assert_eq!(
            pick.provider,
            address!("0000000000000000000000000000000000000011")
        );
        assert_eq!(pick.node_id, [1u8; 32]);
    }

    #[test]
    fn none_when_empty() {
        assert!(pick_deterministic(&[]).is_none());
    }
}
