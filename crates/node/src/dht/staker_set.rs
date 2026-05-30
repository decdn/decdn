//! Active-staker set abstraction (ADR 022 §STORE Flow, §FIND\_VALUE Flow).
//!
//! The DHT handler consults the staker set on every `Store` admission
//! (reject records from non-staked publishers per ADR 022 §STORE Flow
//! line 140) and the requester-side iterative lookup consults it on
//! every response (ADR 022 §Lookup integrity step 2 — drop non-staked
//! `NodeId`s from `providers` and `closer_nodes`). This module owns the
//! trait + an in-memory implementation suitable for tests / the no-op
//! runtime default; the chain-backed `ChainStakerSet` lives next to it
//! in [`super::chain_staker_set`] and is wired by the runtime once the
//! alloy `Provider` is available.

use std::collections::HashSet;

use tokio::sync::broadcast;

use crate::dht::routing::NodeId;

/// Membership change emitted by [`StakerSet::subscribe_changes`].
///
/// The chain-backed implementation emits one of these for every
/// observed `StakingRegistry` event that flips the canonical
/// `isActive` predicate (`NodeRegistered` / `NodeDeregistered` /
/// `NodeAutoEjected` / `Reinstated` / `UnbondingRequested`), after
/// the in-memory active set has been updated. `ConfigStakerSet` never
/// emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StakerChange {
    /// `node_id` joined the active set (was absent, now present).
    Active(NodeId),
    /// `node_id` left the active set (was present, now absent).
    Inactive(NodeId),
}

/// Read-only view of the cached active-staker set.
///
/// Implementations MUST be cheap to clone (typically `Arc<inner>`); the
/// handler holds a long-lived `Arc<dyn StakerSet>` and every admitted
/// `Store` / responded-`FindValue` request consults it on the hot path.
///
/// `is_active` is the only call the handler makes per request.
/// `active_nodes` is exposed for the bootstrap path (seed the routing
/// table from it) and for operator tooling. `subscribe_changes` lets
/// long-running consumers follow membership without polling; the
/// chain-backed impl emits on every relevant event, while the
/// in-memory [`ConfigStakerSet`] returns a receiver that stays open
/// forever without firing — this lets call sites be implementation-
/// agnostic instead of branching on the concrete impl.
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

    /// Subscribe to membership changes. The chain-backed impl emits on
    /// every `StakingRegistry` event that flips a node's `isActive`
    /// predicate, after its own cache has been updated — so a
    /// `recv().await` followed by `is_active` returns the post-event
    /// state. `ConfigStakerSet` returns a receiver that never fires.
    ///
    /// Receivers must handle `RecvError::Lagged` (a slow consumer is
    /// not a fatal condition for the producer); subscribers that fall
    /// further behind than the channel capacity can re-sync by
    /// reading `active_nodes()`.
    fn subscribe_changes(&self) -> broadcast::Receiver<StakerChange>;
}

/// In-memory [`StakerSet`] implementation, built from a parsed
/// `NodeId` set or empty.
///
/// The runtime wires this with [`Self::empty`] until the chain-backed
/// `ChainStakerSet` (reads `StakingRegistry.getActiveNodes()` once at
/// startup and subscribes to the registry's membership events) is
/// ready — same trait, drop-in swap at the runtime construction site.
/// Tests construct via [`Self::new`] with a known `HashSet`.
///
/// The struct holds a [`broadcast::Sender`] to satisfy
/// [`StakerSet::subscribe_changes`], but no code path ever sends on
/// it; receivers stay open indefinitely with no incoming change events.
#[derive(Debug)]
pub struct ConfigStakerSet {
    active: HashSet<NodeId>,
    /// Sender kept alive solely to hand out live receivers via
    /// [`Self::subscribe_changes`]. Capacity is the minimum (1) — the
    /// channel never carries traffic so its buffer size is irrelevant.
    changes_tx: broadcast::Sender<StakerChange>,
}

impl ConfigStakerSet {
    /// Build from a parsed `NodeId` set.
    #[must_use]
    pub fn new(active: HashSet<NodeId>) -> Self {
        let (changes_tx, _) = broadcast::channel(1);
        Self { active, changes_tx }
    }

    /// Empty set — useful in tests where no staker check is desired
    /// (paired with a permissive handler config). Production code MUST
    /// NOT use this without an explicit operator opt-in; an empty
    /// staker set means every `Store` is rejected.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(HashSet::new())
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

    fn subscribe_changes(&self) -> broadcast::Receiver<StakerChange> {
        self.changes_tx.subscribe()
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
        NodeId::from_bytes([byte; 32])
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

    /// `subscribe_changes` returns a receiver that stays open for the
    /// lifetime of the `ConfigStakerSet` (since the sender lives inside
    /// the struct) but never carries an event. The receiver MUST NOT
    /// observe `RecvError::Closed` while the staker set is alive — that
    /// would force every call site into `Result`-handling for a path
    /// that can never fire.
    #[tokio::test]
    async fn config_staker_set_subscribe_changes_stays_open_never_fires() {
        let set = ConfigStakerSet::empty();
        let mut rx = set.subscribe_changes();
        // try_recv on an empty channel that hasn't dropped its sender
        // returns Empty, not Closed. If we ever start returning Closed
        // here, downstream code that just calls `recv().await` would
        // spuriously error on Closed before any event arrived.
        let err = rx
            .try_recv()
            .expect_err("subscribe_changes never fires on ConfigStakerSet");
        assert!(matches!(err, broadcast::error::TryRecvError::Empty));
    }
}
