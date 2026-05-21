//! Active-staker set abstraction (ADR 022 §STORE Flow, §FIND\_VALUE Flow).
//!
//! The DHT handler consults the staker set on every `Store` admission
//! (reject records from non-staked publishers per ADR 022 §STORE Flow
//! line 140) and the requester-side iterative lookup consults it on
//! every response (ADR 022 §Lookup integrity step 2 — drop non-staked
//! `NodeId`s from `providers` and `closer_nodes`). This module owns the
//! trait + a file-config-driven implementation; the chain-backed impl
//! lives behind the same trait and lands with the on-chain
//! origin-directory follow-up.

use std::collections::HashSet;

use crate::dht::routing::NodeId;

/// Read-only view of the cached active-staker set.
///
/// Implementations MUST be cheap to clone (typically `Arc<inner>`); the
/// handler holds a long-lived `Arc<dyn StakerSet>` and every admitted
/// `Store` / responded-`FindValue` request consults it on the hot path.
///
/// The trait is intentionally minimal — `is_active` is the only call the
/// handler makes per request. `active_nodes` is exposed for the
/// bootstrap path (PR 4 of #320 seeds the routing table from it) and
/// for operator tooling.
pub trait StakerSet: Send + Sync + std::fmt::Debug {
    /// Whether `node_id` is in the active-staker set.
    fn is_active(&self, node_id: &NodeId) -> bool;

    /// Snapshot of the active-staker set. Order is unspecified; the caller
    /// MUST randomize before any selection step that an attacker could
    /// influence (e.g. bootstrap target picking).
    fn active_nodes(&self) -> Vec<NodeId>;

    /// Cardinality of the active set. Default implementation walks
    /// [`Self::active_nodes`]; production impls (chain-backed cache, etc.)
    /// SHOULD override with an O(1) counter.
    fn len(&self) -> usize {
        self.active_nodes().len()
    }

    /// Convenience over [`Self::len`].
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// File-config-driven [`StakerSet`] implementation.
///
/// The initial network rollout lists active stakers in the node TOML
/// config (`dht.static_active_nodes`); the chain-backed `ChainStakerSet`
/// (issue tracked in the on-chain origin-directory follow-up) reads
/// `StakingRegistry.getActiveNodes()` once at startup and on `Staked` /
/// `Unstaked` event subscription. Both implementations satisfy the same
/// trait so the handler is independent of the source.
#[derive(Debug)]
pub struct ConfigStakerSet {
    active: HashSet<NodeId>,
}

impl ConfigStakerSet {
    /// Build from a parsed `NodeId` set.
    #[must_use]
    pub const fn new(active: HashSet<NodeId>) -> Self {
        Self { active }
    }

    /// Empty set — useful in tests where no staker check is desired
    /// (paired with a permissive handler config). Production code MUST
    /// NOT use this without an explicit operator opt-in; an empty
    /// staker set means every `Store` is rejected.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            active: HashSet::new(),
        }
    }
}

impl StakerSet for ConfigStakerSet {
    fn is_active(&self, node_id: &NodeId) -> bool {
        self.active.contains(node_id)
    }

    fn active_nodes(&self) -> Vec<NodeId> {
        self.active.iter().copied().collect()
    }

    fn len(&self) -> usize {
        self.active.len()
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    fn nid(byte: u8) -> NodeId {
        [byte; 32]
    }

    #[test]
    fn config_staker_set_is_active_matches_membership() {
        let mut s = HashSet::new();
        s.insert(nid(1));
        s.insert(nid(2));
        let set = ConfigStakerSet::new(s);
        assert!(set.is_active(&nid(1)));
        assert!(set.is_active(&nid(2)));
        assert!(!set.is_active(&nid(3)));
    }

    #[test]
    fn config_staker_set_empty_rejects_every_node() {
        let set = ConfigStakerSet::empty();
        assert!(set.is_empty());
        assert!(!set.is_active(&nid(0)));
        assert!(!set.is_active(&nid(0xFF)));
    }

    #[test]
    fn config_staker_set_active_nodes_returns_full_membership() {
        let mut s = HashSet::new();
        s.insert(nid(1));
        s.insert(nid(2));
        s.insert(nid(3));
        let set = ConfigStakerSet::new(s.clone());
        let got: HashSet<NodeId> = set.active_nodes().into_iter().collect();
        assert_eq!(got, s);
        assert_eq!(set.len(), 3);
        assert!(!set.is_empty());
    }
}
