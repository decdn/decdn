//! The node's live ORIGIN deny-set (ADR 011 §Local Denylist, §On Blacklist
//! Event) — which operator addresses this node refuses to accept payment from.
//!
//! Neither hash half of the denylist lives here — local and governance alike are
//! held by `CacheEngine` beside the eviction set, because "will this node
//! serve/announce/acquire this hash" has four consumers (serve, DHT republish,
//! populate, and the per-MB in-flight re-check) and ADR 011 requires
//! one answer for all of them. See `CacheEngine::refuses`. Addresses have two —
//! the open-time delivery gate and that same in-flight re-check — and the cache
//! knows nothing about them, so they stay here.
//!
//! Two sources feed it and they are deliberately kept in separate slots:
//!
//! - **Local**, from `[content] denied_origins`. Operator-set, local-only, and
//!   applied on the next reload without a restart. ADR 011 §One-hour removal
//!   orders makes this the only mechanism sized to a sub-day statutory deadline,
//!   because it is the only one entirely within the order recipient's control.
//! - **On-chain**, from `ContentBlacklist`'s origin leg AND operator leg — two
//!   separate on-chain mappings the node unions. Tracking only the first would
//!   leave the primary governance path (`addOperator`, which also ejects from
//!   `CapacityBond`) unenforced at the delivery gate.
//!
//! Separate slots because their lifecycles are independent — a config reload
//! must not clobber what the blacklist sync learned, and vice versa — but
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
    /// the union of `ContentBlacklist`'s two mappings.
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

    /// Is this address denied as a pool funder, by either source?
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

    /// Replace the on-chain origin set wholesale: the live address union of
    /// the blacklist sync's deny index, published on every enforcement pass.
    ///
    /// The union holds every address whose origin leg or operator leg is live
    /// (see `decdn-node::blacklist_watcher`). It is rebuilt from chain each
    /// boot, so there is no durable projection to reload.
    pub fn set_chain_origins(&self, origins: HashSet<Address>) {
        self.chain_origins.store(Arc::new(origins));
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
mod tests;
