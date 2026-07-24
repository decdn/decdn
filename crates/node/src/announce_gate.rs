//! Node-side wiring for the `NodeAnnounce` staked-admission gate (ADR 001
//! rule 2).
//!
//! The `decdn-gossip` crate defines the [`StakedNodeSet`] membership seam; this
//! module implements it over the node's chain [`StakerSet`]. The crate-private
//! `NodeStakedNodeSet` adapts the two, and [`announce_staked_gate`] is the
//! single entry point the runtime uses to build the gate handed to
//! [`decdn_gossip::GossipService::spawn`].

use std::collections::HashSet;
use std::sync::Arc;

use arc_swap::ArcSwap;
use decdn_gossip::{AnnounceGate, OwnedAnnounceGate, StakedNodeSet};
use decdn_protocol::NodeId as ProtocolNodeId;

use crate::dht::staker_set::StakerSet;

/// `NodeId`-keyed deny-set consulted by the `NodeAnnounce` admission gate to bar
/// an operator blacklisted via the origin-only path (`setOriginBlacklist` /
/// `emergencyAddOrigin`). Unlike `addOperator`, those paths do **not** eject the
/// operator from `CapacityBond`, so the staker set still recognises it and
/// nothing else stops its re-announce — `PeerTable::remove` is advisory, and
/// `insert_or_refresh` re-admits the peer on its very next announce (#1398).
///
/// Keyed by `NodeId` because the gossip gate only ever sees the announcing
/// node's 32-byte key; the on-chain blacklist is keyed by operator `Address`,
/// and the `Address → NodeId` translation (`CapacityBond.nodeIdOf`) is a
/// node-crate concern the blacklist watcher already performs. It therefore feeds
/// this set the already-translated `NodeId`s.
///
/// Reads land on the announce-validation path, so the set is an [`ArcSwap`] — a
/// lookup is one atomic load and a hash-set probe; a swap never blocks a reader
/// — matching [`crate::content_deny::ContentDenylist`]. The only writer is the
/// single blacklist-watcher sink task, so the load-clone-store writes below race
/// no other writer.
#[derive(Debug, Default)]
pub struct AnnounceOriginDenySet {
    denied: ArcSwap<HashSet<[u8; 32]>>,
}

impl AnnounceOriginDenySet {
    /// An empty deny-set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Is this `NodeId` barred from announcing?
    #[must_use]
    pub fn contains(&self, node_id: &[u8; 32]) -> bool {
        self.denied.load().contains(node_id)
    }

    /// Bar `node_id`. Returns whether the set changed (so a no-op replay of an
    /// already-applied event skips logging).
    pub fn insert(&self, node_id: [u8; 32]) -> bool {
        let current = self.denied.load();
        if current.contains(&node_id) {
            return false;
        }
        let mut next = HashSet::clone(&current);
        next.insert(node_id);
        self.denied.store(Arc::new(next));
        true
    }

    /// Un-bar `node_id`. Returns whether the set changed.
    pub fn remove(&self, node_id: &[u8; 32]) -> bool {
        let current = self.denied.load();
        if !current.contains(node_id) {
            return false;
        }
        let mut next = HashSet::clone(&current);
        next.remove(node_id);
        self.denied.store(Arc::new(next));
        true
    }

    /// Number of barred `NodeId`s. For telemetry / the admin surface.
    #[must_use]
    pub fn len(&self) -> usize {
        self.denied.load().len()
    }

    /// Whether nothing is barred.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.denied.load().is_empty()
    }
}

/// Adapts the chain [`StakerSet`] to the gossip [`StakedNodeSet`] gate: only
/// currently-staked nodes that are not on the origin deny-set may announce
/// (ADR 001 rule 2 + ADR 011 § Hash Evasion and Origin Blacklisting).
///
/// `pub(crate)` because nothing outside this crate needs the raw adapter;
/// [`announce_staked_gate`] is the only intended entry point (#1345).
///
/// Note what this does *not* buy: it is not what keeps the runtime from failing
/// open. Exporting this type would create no un-gated path — anything an
/// external caller could build with it is the same wrapping
/// [`announce_staked_gate`] already returns. The genuinely un-gated seam was
/// `AnnounceGate::Disabled`, which narrowing this struct did not touch — that is
/// now closed separately by making the variant `#[cfg(test)]` in `decdn-gossip`,
/// i.e. absent from production builds.
#[derive(Debug)]
pub(crate) struct NodeStakedNodeSet {
    staker_set: Arc<dyn StakerSet>,
    origin_deny: Arc<AnnounceOriginDenySet>,
}

impl NodeStakedNodeSet {
    /// Wrap the runtime's staker set and origin deny-set.
    pub(crate) fn new(
        staker_set: Arc<dyn StakerSet>,
        origin_deny: Arc<AnnounceOriginDenySet>,
    ) -> Self {
        Self {
            staker_set,
            origin_deny,
        }
    }
}

impl StakedNodeSet for NodeStakedNodeSet {
    fn contains(&self, node_id: &[u8; 32]) -> bool {
        // `NodeId` is a freely-constructible 32-byte newtype; the bytes were
        // already signature-verified in the gossip validator. Admit only a
        // staked node that governance has not origin-blacklisted: a blacklisted
        // operator stays out even though `CapacityBond` still lists it active
        // (the origin-only path never ejected it).
        if self.origin_deny.contains(node_id) {
            return false;
        }
        self.staker_set
            .is_active(&ProtocolNodeId::from_bytes(*node_id))
    }
}

/// Build the `NodeAnnounce` admission gate handed to
/// [`decdn_gossip::GossipService::spawn`]. ADR 001 rule 2: the runtime *always*
/// enforces the gate against the live staker set, so this returns
/// [`AnnounceGate::Enforce`], never a disabled gate. `AnnounceGate::Disabled`
/// fails open (skips the staked-membership check) and is `#[cfg(test)]` in
/// `decdn-gossip`, so it cannot be named from this crate at all.
///
/// `origin_deny` is the shared handle the blacklist watcher feeds; the gate and
/// the watcher hold the same `Arc`, so a takedown bars the operator's next
/// announce without rebuilding the gate.
///
/// Named and unit-tested so a future refactor cannot silently drop the runtime
/// to `Disabled`: that would reopen the exact hole #1170 closed while every
/// existing test still passed (`run()` is otherwise reachable only via the anvil
/// e2e).
pub fn announce_staked_gate(
    staker_set: Arc<dyn StakerSet>,
    origin_deny: Arc<AnnounceOriginDenySet>,
) -> OwnedAnnounceGate {
    AnnounceGate::Enforce(Arc::new(NodeStakedNodeSet::new(staker_set, origin_deny)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::dht::staker_set::ConfigStakerSet;
    use iroh::{PublicKey, SecretKey};
    use std::collections::HashSet;

    /// A staker set containing exactly `member`.
    fn staker_set_with(member: PublicKey) -> Arc<ConfigStakerSet> {
        Arc::new(ConfigStakerSet::new(HashSet::from([
            ProtocolNodeId::from_bytes(*member.as_bytes()),
        ])))
    }

    fn pk() -> PublicKey {
        SecretKey::generate().public()
    }

    /// The gossip-facing gate keys on the raw 32-byte `NodeId` (the identity
    /// function over the staker set): a member's bytes return `true`, a
    /// non-member's `false`. This is what enforces ADR 001 rule 2 on the
    /// `NodeAnnounce` path.
    #[test]
    fn node_staked_node_set_delegates_to_staker_set() {
        let member = pk();
        let outsider = pk();
        let gate = NodeStakedNodeSet::new(
            staker_set_with(member),
            Arc::new(AnnounceOriginDenySet::new()),
        );
        assert!(gate.contains(member.as_bytes()));
        assert!(!gate.contains(outsider.as_bytes()));
    }

    /// A staked member on the origin deny-set is refused, and un-barring it
    /// restores admission — the #1398 gap: `setOriginBlacklist` does not eject,
    /// so the staker set still reports the operator active. The gate must
    /// nonetheless keep it out (ADR 011 § Hash Evasion and Origin Blacklisting).
    #[test]
    fn origin_deny_set_bars_a_staked_member() {
        let member = pk();
        let deny = Arc::new(AnnounceOriginDenySet::new());
        let gate = NodeStakedNodeSet::new(staker_set_with(member), Arc::clone(&deny));
        assert!(
            gate.contains(member.as_bytes()),
            "staked + not denied ⇒ admitted"
        );

        assert!(deny.insert(*member.as_bytes()), "first bar changes the set");
        assert!(!deny.insert(*member.as_bytes()), "replay is a no-op");
        assert!(
            !gate.contains(member.as_bytes()),
            "denied operator is refused even though the staker set still lists it active"
        );

        assert!(deny.remove(member.as_bytes()), "un-bar changes the set");
        assert!(
            gate.contains(member.as_bytes()),
            "un-barred member is admitted again"
        );
    }

    /// The runtime's `NodeAnnounce` gate must admit against the *live* staker
    /// set (ADR 001 rule 2) — not an `Enforce` stub that admits everyone, which
    /// a variant-tag check alone would not catch. Guards against a future
    /// refactor silently disabling rule 2 (the #1170 hole), which `run()` alone
    /// would only surface under the anvil e2e (#1222).
    ///
    /// The other half of that guarantee — "never the fail-open variant" — used
    /// to be a `let ... else { panic!() }` here. It is no longer assertable, and
    /// that is the improvement: `AnnounceGate::Disabled` is `#[cfg(test)]` in
    /// `decdn-gossip`, so from this crate the pattern is irrefutable and the
    /// invariant is the compiler's rather than this test's.
    #[test]
    fn announce_staked_gate_admits_against_the_live_set() {
        let member = pk();
        let AnnounceGate::Enforce(gate) = announce_staked_gate(
            staker_set_with(member),
            Arc::new(AnnounceOriginDenySet::new()),
        );
        assert!(
            gate.contains(member.as_bytes()),
            "staked member must be admitted"
        );
        assert!(
            !gate.contains(pk().as_bytes()),
            "non-member must be rejected"
        );
    }
}
