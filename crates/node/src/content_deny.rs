//! The node's live content deny-set (ADR 011 §Local Denylist, §On Blacklist
//! Event) — the single lookup the delivery path consults before serving.
//!
//! Two sources feed it and they are deliberately kept in separate slots:
//!
//! - **Local**, from `[content] denied_hashes` / `denied_origins`. Operator-set,
//!   ungossiped, hot-reloadable, effective immediately. ADR 011 §One-hour
//!   removal orders makes this the only mechanism sized to a sub-day statutory
//!   deadline, because it is the only one entirely within the order recipient's
//!   control.
//! - **On-chain**, from `ContentBlacklist.OriginBlacklistUpdated`. Governance-set
//!   and network-wide.
//!
//! Separate slots because their lifecycles are independent — a config reload
//! must not clobber what the chain watcher learned, and vice versa — but the
//! *lookups* union them, and the wire refusal that results does not say which
//! matched. That is a requirement, not an omission: ADR 011 §`StreamRequest`
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

/// Hash-set delta across a reload, for the log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DenyDiff {
    /// Hashes present in the new set but not the old.
    pub added: usize,
    /// Hashes present in the old set but not the new.
    pub removed: usize,
}

/// The union of the operator's local denylist and the on-chain origin
/// blacklist, as consulted by the delivery path.
///
/// Hashes are keyed as raw `[u8; 32]`, which is what `StreamRequest` carries and
/// what both `decdn_config_types::Hash` and `iroh_blobs::Hash` wrap. Keying on
/// either named type would drag its crate into `decdn-node`'s dependency graph
/// (or force a conversion at the hot-path call site) to buy nothing: this type
/// only ever tests set membership.
#[derive(Debug)]
pub struct ContentDenylist {
    /// `[content] denied_hashes`. There is no on-chain counterpart slot here:
    /// governance hash entries reach the delivery path through the blacklist
    /// watcher's eviction lever (`CacheEngine::evict`), which predates this type
    /// and is durable across restarts.
    denied_hashes: ArcSwap<HashSet<[u8; 32]>>,
    /// `[content] denied_origins`.
    local_origins: ArcSwap<HashSet<Address>>,
    /// Addresses currently blacklisted per `ContentBlacklist`.
    chain_origins: ArcSwap<HashSet<Address>>,
}

impl ContentDenylist {
    /// Build from resolved config, with an empty on-chain set — the watcher
    /// fills that in once it has replayed.
    #[must_use]
    pub fn new(content: &ResolvedContent) -> Self {
        Self {
            denied_hashes: ArcSwap::from(Arc::new(hash_bytes(content))),
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

    /// Is this blob on the operator's local denylist?
    ///
    /// Governance hash entries are NOT covered here — they arrive as cache
    /// evictions and are answered by `CacheEngine::is_evicted`. Both refuse with
    /// the same wire code, so the split is invisible to a client.
    #[must_use]
    pub fn is_hash_denied(&self, hash: &[u8; 32]) -> bool {
        self.denied_hashes.load().contains(hash)
    }

    /// Is this address denied as a channel funder, by either source?
    #[must_use]
    pub fn is_origin_denied(&self, addr: &Address) -> bool {
        self.local_origins.load().contains(addr) || self.chain_origins.load().contains(addr)
    }

    /// Swap in a freshly resolved `[content]` section. Returns the hash-set
    /// delta for the reload log line, plus the new `denied_origins` count.
    ///
    /// Touches only the local slots — the on-chain set is the watcher's.
    pub fn set_local(&self, content: &ResolvedContent) -> (DenyDiff, usize) {
        let next = hash_bytes(content);
        let prev = self.denied_hashes.load();
        let diff = DenyDiff {
            added: next.difference(&prev).count(),
            removed: prev.difference(&next).count(),
        };
        self.denied_hashes.store(Arc::new(next));
        let origin_count = content.denied_origins.len();
        self.local_origins
            .store(Arc::new(content.denied_origins.clone()));
        (diff, origin_count)
    }

    /// Replace the on-chain origin set wholesale — the watcher's boot
    /// reconcile, after reading `isOriginBlacklisted` for the addresses it knows.
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

/// Project the resolved `denied_hashes` onto the raw key type. The only place
/// the config's `Hash` newtype crosses into this module.
fn hash_bytes(content: &ResolvedContent) -> HashSet<[u8; 32]> {
    content
        .denied_hashes
        .iter()
        .map(|h| *h.as_bytes())
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn addr(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    fn content(hashes: &[[u8; 32]], origins: &[Address]) -> ResolvedContent {
        ResolvedContent {
            denied_hashes: decdn_common::config::parse_denied_hashes(Some(
                &hashes.iter().map(alloy::hex::encode).collect::<Vec<_>>(),
            ))
            .expect("valid hex fixtures"),
            denied_origins: origins.iter().copied().collect(),
        }
    }

    #[test]
    fn empty_denies_nothing() {
        let deny = ContentDenylist::empty();
        assert!(!deny.is_hash_denied(&[1u8; 32]));
        assert!(!deny.is_origin_denied(&addr(1)));
    }

    #[test]
    fn local_hashes_and_origins_are_denied() {
        let deny = ContentDenylist::new(&content(&[[7u8; 32]], &[addr(9)]));
        assert!(deny.is_hash_denied(&[7u8; 32]));
        assert!(!deny.is_hash_denied(&[8u8; 32]));
        assert!(deny.is_origin_denied(&addr(9)));
        assert!(!deny.is_origin_denied(&addr(10)));
    }

    /// The reload path must not clobber what the chain watcher learned, and the
    /// watcher must not clobber the operator's local list. This is the whole
    /// reason the two live in separate slots.
    #[test]
    fn local_reload_and_chain_updates_are_independent() {
        let deny = ContentDenylist::new(&content(&[], &[addr(1)]));
        deny.apply_chain_origin(addr(2), true);
        assert!(deny.is_origin_denied(&addr(1)));
        assert!(deny.is_origin_denied(&addr(2)));

        // A reload that drops the local entry leaves the chain entry standing.
        deny.set_local(&content(&[], &[]));
        assert!(!deny.is_origin_denied(&addr(1)));
        assert!(deny.is_origin_denied(&addr(2)));

        // ...and a chain removal leaves a re-added local entry standing.
        deny.set_local(&content(&[], &[addr(1)]));
        deny.apply_chain_origin(addr(2), false);
        assert!(deny.is_origin_denied(&addr(1)));
        assert!(!deny.is_origin_denied(&addr(2)));
    }

    /// An address on BOTH lists must survive removal from one. A naive single
    /// set would drop it and silently resume serving a blacklisted origin.
    #[test]
    fn origin_on_both_lists_survives_removal_from_one() {
        let deny = ContentDenylist::new(&content(&[], &[addr(3)]));
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
    fn set_local_reports_the_hash_delta() {
        let deny = ContentDenylist::new(&content(&[[1u8; 32]], &[]));
        let (diff, origins) = deny.set_local(&content(&[[1u8; 32], [2u8; 32]], &[addr(1)]));
        assert_eq!(diff.added, 1);
        assert_eq!(diff.removed, 0);
        assert_eq!(origins, 1);
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
