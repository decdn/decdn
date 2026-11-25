//! Chain-backed [`OriginDirectory`] implementation.
//!
//! Resolves a request's **namespace** to the set of currently-active operator
//! `NodeId`s authorised as origins for it. The resolution chain (ADR 022 §
//! FIND\_VALUE Flow "Origin discovery", ADR 011 § Origin Assignment
//! Authority) is:
//!
//!   seated-origin set (`OriginAssignment.getOrigins(namespaceId)`) → `[operator...]`
//!     → drop operators the local [`ContentDenylist`] denies (the origin ∪
//!       operator blacklist union the watcher syncs from `ContentBlacklist`,
//!       plus the operator's own `[content] denied_origins`)
//!     → capacity-bond reverse projection (`operator → NodeId`, binding only)
//!     → keep operators the shared [`StakerSet`] reports `active`
//!
//! The blacklist drop is this node protecting itself: `getOrigins` is a routing
//! hint the contract does not filter (ADR 011 § Interaction with
//! `ContentBlacklist`), so the consumer applies the same deny-set its delivery
//! gate already holds rather than trusting the seat set to be blacklist-clean.
//!
//! The bare hash carries no origin information (ADR 002 § Retrieval by
//! namespace): the request supplies the namespace its content is published
//! under. `namespaceId == 0` names no namespace, so it resolves to no origins.
//!
//! The operator set comes from the shared [`SeatedOrigins`] snapshot, which
//! holds every seat in every namespace and is kept current by
//! [`crate::origin_seat_watcher`]. A lookup makes no chain read: a namespace
//! the snapshot does not list resolves to no origins and asks the snapshot for
//! an early re-read, which the snapshot's refresh loop rate-limits.
//!
//! Every stage after the seat set reads live state: the deny-set, the
//! `operator → NodeId` binding and staker liveness, so a blacklist or a
//! liveness change takes effect on the next lookup.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use alloy::primitives::{Address, U256};

use crate::content_deny::ContentDenylist;
use crate::dht::origin::OriginDirectory;
use crate::dht::routing::NodeId;
use crate::dht::seated_origins::SeatedOrigins;
use crate::dht::staker_set::StakerSet;

/// Chain-backed origin directory: resolves a namespace from the seated-origin
/// snapshot to active `NodeId`s against the shared capacity-bond reverse
/// projection.
pub struct ChainOriginDirectory {
    seated: Arc<dyn SeatedOrigins>,
    operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
    staker_set: Arc<dyn StakerSet>,
    /// The node's live origin deny-set — the same union the delivery gate
    /// consults. A blacklisted operator is dropped from every resolved origin
    /// set, so this node never routes an origin-pull to one.
    content_deny: Arc<ContentDenylist>,
}

impl std::fmt::Debug for ChainOriginDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainOriginDirectory")
            .finish_non_exhaustive()
    }
}

impl ChainOriginDirectory {
    /// Build a directory over the shared seated-origin snapshot and chain
    /// projections. No RPC.
    #[must_use]
    pub fn new(
        seated: Arc<dyn SeatedOrigins>,
        operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
        staker_set: Arc<dyn StakerSet>,
        content_deny: Arc<ContentDenylist>,
    ) -> Self {
        Self {
            seated,
            operator_to_node,
            staker_set,
            content_deny,
        }
    }

    /// Resolve an operator address set to active origin `NodeId`s: drop any
    /// operator the local deny-set blacklists, map each through the
    /// capacity-bond reverse projection, keep only operators the `StakerSet`
    /// reports active, and sort + dedup for a deterministic order. An operator
    /// with no reverse binding is dropped: the registry has not observed its
    /// node registration.
    fn resolve(&self, operators: &[Address]) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = crate::dht::chain_projection::with_read(
            &self.operator_to_node,
            "chain operator reverse map",
            |rev| {
                operators
                    .iter()
                    .filter(|op| !self.content_deny.is_origin_denied(op))
                    .filter_map(|op| rev.get(op).copied())
                    .filter(|nid| self.staker_set.is_active(nid))
                    .collect()
            },
        );
        nodes.sort_unstable();
        nodes.dedup();
        nodes
    }
}

#[async_trait::async_trait]
impl OriginDirectory for ChainOriginDirectory {
    async fn lookup_origins(&self, namespace_id: U256) -> Vec<NodeId> {
        // Namespace 0 (NO_NAMESPACE) authorizes nothing by construction
        // (ADR 002 §Namespace 0).
        if namespace_id == U256::ZERO {
            return Vec::new();
        }
        self.resolve(&self.seated.origins_in(namespace_id))
    }

    fn origin_nodes(&self, operators: &[Address]) -> Vec<NodeId> {
        self.resolve(operators)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
