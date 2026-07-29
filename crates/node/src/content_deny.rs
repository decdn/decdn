//! The node's live ORIGIN deny-set (ADR 011 §Local Denylist, §On Blacklist
//! Event) — which operator addresses this node refuses to accept payment from.
//!
//! Neither hash half of the denylist lives here — local and governance alike are
//! held by `CacheEngine` beside the eviction set, because "will this node
//! serve/announce/acquire this hash" has five consumers (serve, probe hold, DHT
//! republish, populate, and the per-MB in-flight re-check) and ADR 011 requires
//! one answer for all of them. See `CacheEngine::refuses`. Addresses have two —
//! the open-time delivery gate and that same in-flight re-check — and the cache
//! knows nothing about them, so they stay here.
//!
//! Two sources feed it and they are deliberately kept in separate slots:
//!
//! - **Local**, from `[content] denied_origins`. Operator-set, ungossiped, and
//!   applied on the next reload without a restart. ADR 011 §One-hour removal
//!   orders makes this the only mechanism sized to a sub-day statutory deadline,
//!   because it is the only one entirely within the order recipient's control.
//! - **On-chain**, from `ContentBlacklist`'s `OriginBlacklistUpdated` AND
//!   `OperatorBlacklisted` — two separate on-chain mappings that
//!   `OriginAssignment` itself unions, so the node does too. Watching only the
//!   first would leave the primary governance path (`addOperator`, which also
//!   ejects from `CapacityBond`) unenforced at the delivery gate.
//!
//! Separate slots because their lifecycles are independent — a config reload
//! must not clobber what the chain watcher learned, and vice versa — but
//! [`ContentDenylist::is_origin_denied`] unions them, and the wire refusal that
//! results does not say which matched. That is a requirement, not an omission: ADR 011 §`StreamRequest`
//! Response specifies the response must not distinguish governance from local
//! sources. A client able to tell them apart could map an operator's private
//! legal exposure by probing.
//!
//! Reads land on the request hot path, so each slot is an [`ArcSwap`] — the same
//! shape `CacheEngine` uses for its pinned set. A lookup is one atomic load and
//! a hash-set probe; a swap never blocks a reader.

use std::collections::HashSet;
use std::sync::Arc;

use alloy::primitives::Address;
use arc_swap::ArcSwap;
use decdn_common::config::ResolvedContent;

/// The union of the operator's local denylist and the on-chain origin
/// blacklist, as consulted by the delivery path.
#[derive(Debug)]
pub struct ContentDenylist {
    /// `[content] denied_origins`.
    local_origins: ArcSwap<HashSet<Address>>,
    /// Addresses governance has blacklisted at the origin OR operator level —
    /// the union of `ContentBlacklist`'s two mappings, matching
    /// `OriginAssignment.sol`'s own predicate.
    chain_origins: ArcSwap<HashSet<Address>>,
}

impl ContentDenylist {
    /// Build from resolved config, with an empty on-chain set — the watcher
    /// fills that in once its bootstrap enumeration completes (#1504).
    #[must_use]
    pub fn new(content: &ResolvedContent) -> Self {
        Self {
            local_origins: ArcSwap::from(Arc::new(content.denied_origins.clone())),
            chain_origins: ArcSwap::from(Arc::new(HashSet::new())),
        }
    }

    /// An empty deny-set, for tests and for wiring paths with no `[content]`
    /// section resolved yet.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(&ResolvedContent::default())
    }

    /// Is this address denied as a channel funder, by either source?
    #[must_use]
    pub fn is_origin_denied(&self, addr: &Address) -> bool {
        self.local_origins.load().contains(addr) || self.chain_origins.load().contains(addr)
    }

    /// Swap in the origin half of a freshly resolved `[content]` section,
    /// returning the new entry count for the reload log line.
    ///
    /// Touches only the local slot — the on-chain set is the watcher's, and the
    /// hash half is the cache engine's.
    pub fn set_local_origins(&self, content: &ResolvedContent) -> usize {
        let origin_count = content.denied_origins.len();
        self.local_origins
            .store(Arc::new(content.denied_origins.clone()));
        origin_count
    }

    /// Replace the on-chain origin set wholesale — the watcher's boot seed from
    /// the enumerated address union, and its periodic re-enumeration refresh.
    ///
    /// The union is the origin ∪ operator deny set read from
    /// `ContentBlacklist.blacklistedAddresses`, liveness-filtered by
    /// `isOriginBlacklisted || isOperatorBlacklisted` (see
    /// `decdn-node::blacklist_watcher`). It is rebuilt from chain each boot, so
    /// there is no durable projection to reload.
    pub fn set_chain_origins(&self, origins: HashSet<Address>) {
        self.chain_origins.store(Arc::new(origins));
    }

    /// Apply a single `OriginBlacklistUpdated` event.
    ///
    /// Read-modify-write rather than an in-place mutation, because [`ArcSwap`]
    /// has no such thing. Origin events are rare (a governance action), so the
    /// clone is irrelevant next to keeping reads lock-free; a `RwLock` would
    /// trade that for a write that is still rare. Returns whether the set
    /// actually changed, so callers can skip logging a no-op replay — the
    /// watcher re-scans a block range after a restart and will re-deliver
    /// events it has already applied.
    pub fn apply_chain_origin(&self, addr: Address, blacklisted: bool) -> bool {
        let current = self.chain_origins.load();
        if current.contains(&addr) == blacklisted {
            return false;
        }
        let mut next = HashSet::clone(&current);
        if blacklisted {
            next.insert(addr);
        } else {
            next.remove(&addr);
        }
        self.chain_origins.store(Arc::new(next));
        true
    }

    /// Number of on-chain blacklisted origins currently tracked. For telemetry
    /// and the admin surface.
    #[must_use]
    pub fn chain_origin_count(&self) -> usize {
        self.chain_origins.load().len()
    }
}

impl Default for ContentDenylist {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn addr(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    fn content(origins: &[Address]) -> ResolvedContent {
        ResolvedContent {
            denied_origins: origins.iter().copied().collect(),
            ..ResolvedContent::default()
        }
    }

    #[test]
    fn empty_denies_nothing() {
        assert!(!ContentDenylist::empty().is_origin_denied(&addr(1)));
    }

    #[test]
    fn local_origins_are_denied() {
        let deny = ContentDenylist::new(&content(&[addr(9)]));
        assert!(deny.is_origin_denied(&addr(9)));
        assert!(!deny.is_origin_denied(&addr(10)));
    }

    /// The reload path must not clobber what the chain watcher learned, and the
    /// watcher must not clobber the operator's local list. This is the whole
    /// reason the two live in separate slots.
    #[test]
    fn local_reload_and_chain_updates_are_independent() {
        let deny = ContentDenylist::new(&content(&[addr(1)]));
        deny.apply_chain_origin(addr(2), true);
        assert!(deny.is_origin_denied(&addr(1)));
        assert!(deny.is_origin_denied(&addr(2)));

        // A reload that drops the local entry leaves the chain entry standing.
        deny.set_local_origins(&content(&[]));
        assert!(!deny.is_origin_denied(&addr(1)));
        assert!(deny.is_origin_denied(&addr(2)));

        // ...and a chain removal leaves a re-added local entry standing.
        deny.set_local_origins(&content(&[addr(1)]));
        deny.apply_chain_origin(addr(2), false);
        assert!(deny.is_origin_denied(&addr(1)));
        assert!(!deny.is_origin_denied(&addr(2)));
    }

    /// An address on BOTH lists must survive removal from one. A naive single
    /// set would drop it and silently resume serving a blacklisted origin.
    #[test]
    fn origin_on_both_lists_survives_removal_from_one() {
        let deny = ContentDenylist::new(&content(&[addr(3)]));
        deny.apply_chain_origin(addr(3), true);
        deny.apply_chain_origin(addr(3), false);
        assert!(deny.is_origin_denied(&addr(3)), "local entry still stands");
    }

    #[test]
    fn apply_chain_origin_reports_whether_it_changed_anything() {
        let deny = ContentDenylist::empty();
        assert!(deny.apply_chain_origin(addr(4), true));
        assert!(!deny.apply_chain_origin(addr(4), true), "replay is a no-op");
        assert!(deny.apply_chain_origin(addr(4), false));
        assert!(!deny.apply_chain_origin(addr(4), false));
    }

    #[test]
    fn set_chain_origins_replaces_wholesale() {
        let deny = ContentDenylist::empty();
        deny.apply_chain_origin(addr(5), true);
        deny.set_chain_origins([addr(6)].into_iter().collect());
        assert!(!deny.is_origin_denied(&addr(5)));
        assert!(deny.is_origin_denied(&addr(6)));
        assert_eq!(deny.chain_origin_count(), 1);
    }
}
