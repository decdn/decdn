//! Node-side wiring for the `NodeAnnounce` staked-admission gate (ADR 001
//! rule 2).
//!
//! The `decdn-gossip` crate defines the [`StakedNodeSet`] membership seam; this
//! module implements it over the node's chain [`StakerSet`]. The crate-private
//! `NodeStakedNodeSet` adapts the two, and [`announce_staked_gate`] is the
//! single entry point the runtime uses to build the gate handed to
//! [`decdn_gossip::GossipService::spawn`].

use std::sync::Arc;

use decdn_gossip::{AnnounceGate, OwnedAnnounceGate, StakedNodeSet};
use decdn_protocol::NodeId as ProtocolNodeId;

use crate::dht::staker_set::StakerSet;

/// Adapts the chain [`StakerSet`] to the gossip [`StakedNodeSet`] gate: only
/// currently-staked nodes may announce (ADR 001 rule 2).
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
}

impl NodeStakedNodeSet {
    /// Wrap the runtime's staker set.
    pub(crate) fn new(staker_set: Arc<dyn StakerSet>) -> Self {
        Self { staker_set }
    }
}

impl StakedNodeSet for NodeStakedNodeSet {
    fn contains(&self, node_id: &[u8; 32]) -> bool {
        // `NodeId` is a freely-constructible 32-byte newtype; the bytes were
        // already signature-verified in the gossip validator.
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
/// Named and unit-tested so a future refactor cannot silently drop the runtime
/// to `Disabled`: that would reopen the exact hole #1170 closed while every
/// existing test still passed (`run()` is otherwise reachable only via the anvil
/// e2e).
pub fn announce_staked_gate(staker_set: Arc<dyn StakerSet>) -> OwnedAnnounceGate {
    AnnounceGate::Enforce(Arc::new(NodeStakedNodeSet::new(staker_set)))
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
        let gate = NodeStakedNodeSet::new(staker_set_with(member));
        assert!(gate.contains(member.as_bytes()));
        assert!(!gate.contains(outsider.as_bytes()));
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
        let AnnounceGate::Enforce(gate) = announce_staked_gate(staker_set_with(member));
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
