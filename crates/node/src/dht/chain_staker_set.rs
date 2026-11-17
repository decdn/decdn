//! Chain-backed [`StakerSet`] implementation.
//!
//! This module owns the **projection** only: the cached `NodeId → active?` set
//! and its accessors. The `CapacityBond` snapshot that keeps it current lives in
//! [`crate::dht::capacity_bond_registry`], which builds this via
//! `ChainStakerSet::from_parts` and shares one `getRegisteredNodes` read with
//! the other registry projections.
//!
//! The cached set is the parallel `active[]` that
//! `CapacityBond.getRegisteredNodes()` computes per entry — the full `isActive`
//! predicate: registered, bond ≥ minBond, no unbonding, not ejected. The
//! registry re-reads it every five minutes, and early when a lookup here misses
//! (a node that registered since the last read), at most once per minute.
//!
//! # Spec mapping
//!
//! - ADR 022 §STORE Flow: the receiver checks `holder` is in the cached
//!   active-staker set populated from `CapacityBond.getRegisteredNodes()`.
//! - ADR 019 § Step 3.3: the registry snapshot pattern.
//!
//! # Failure model
//!
//! Bootstrap RPC failure → the registry bootstrap propagates the error (the
//! runtime treats it as fatal; the DHT cannot function without a staker set). A
//! failed re-read keeps the previous set and counts
//! `decdn_capacity_bond_registry_resync_failures_total`;
//! `decdn_capacity_bond_registry_last_resync_timestamp_seconds` reports when
//! the last one succeeded, and `decdn_staker_set_active_count` the set size.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use crate::chain_events::refresh::RefreshTrigger;
use crate::dht::chain_projection::ChainProjection;
use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;

/// Names the projection in the poison-recovery `warn!`.
const LABEL: &str = "ChainStakerSet active set";

/// Chain-backed staker set. The runtime holds one `Arc<dyn StakerSet>` and
/// consumers read the cached set through it.
#[derive(Debug)]
pub struct ChainStakerSet {
    proj: ChainProjection<HashSet<NodeId>>,
    /// Asks the registry for an early re-read when a lookup misses.
    on_miss: RefreshTrigger,
}

impl ChainStakerSet {
    /// Assemble from the active-set state owned by `capacity_bond_registry`.
    pub(super) const fn from_parts(
        active: Arc<RwLock<HashSet<NodeId>>>,
        on_miss: RefreshTrigger,
    ) -> Self {
        Self {
            proj: ChainProjection::from_parts(active, LABEL),
            on_miss,
        }
    }
}

impl StakerSet for ChainStakerSet {
    fn is_active(&self, node_id: &NodeId) -> bool {
        let active = self.proj.read(|s| s.contains(node_id));
        if !active {
            // A node registered since the last snapshot reads inactive until
            // the next one; the request pulls that read forward. An inactive
            // node asks too, and the registry's rate limit bounds the cost.
            self.on_miss.request();
        }
        active
    }

    fn active_nodes(&self) -> Vec<NodeId> {
        self.proj.read(|s| s.iter().copied().collect())
    }

    fn len(&self) -> usize {
        self.proj.read(HashSet::len)
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
