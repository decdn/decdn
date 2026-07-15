//! Requester-side negative probe cache (ADR 001 §Probe cache).
//!
//! Suppresses repeated `cdn/probe/v1` requests to a `(NodeId, hash)`
//! pair that already returned `has_blob: false` within the TTL window.
//! Per ADR 001 § Probe cache the cache:
//!
//! - is keyed `(NodeId, hash)`;
//! - holds at most 1024 entries with LRU eviction;
//! - retains entries for 5 minutes (TTL anchored at insertion);
//! - does NOT retain the probe response signature — purely a request-
//!   suppression structure, not slashing evidence.
//!
//! The DHT iterative lookup ([`super::lookup`]) consumes
//! [`NegativeProbeCache::contains_active`] as the third ADR 022
//! §Lookup-integrity filter on `FindValueResponse.providers`.
//! [`NegativeProbeCache::record_failure`] is the producer-side hook
//! for the outbound `cdn/probe/v1` client.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use tracing::warn;

use crate::dht::routing::NodeId;

pub use crate::dht::records::Hash;

/// ADR 001 § Probe cache: negative cache TTL is 5 minutes — longer than the
/// positive probe cache (15s) because false-STORE results are less
/// time-sensitive, and shorter than the DHT record TTL (1h) so a
/// publisher that genuinely acquires the blob during the negative
/// window can re-establish reachability after one cache lifetime.
const DEFAULT_TTL: Duration = Duration::from_mins(5);

/// ADR 001 § Probe cache: max 1024 entries.
const DEFAULT_CAPACITY: usize = 1024;

type Key = (NodeId, Hash);

#[derive(Debug)]
struct Inner {
    /// Key → expiry deadline. `IndexMap` collapses what would
    /// otherwise be a `HashMap` + side `VecDeque` (kept in lockstep
    /// to track LRU order) into a single store: insertion order is
    /// the LRU ordering, with index 0 = most-recently-used and
    /// `len()-1` = least-recently-used. Bumping a hit means
    /// `shift_insert(0, …)`; eviction means `pop` from the back.
    entries: IndexMap<Key, Instant>,
    /// Hard cap on live entries. Clamped to ≥ 1 in the constructor.
    cap: usize,
    /// TTL applied to each entry on insert. Anchored at insertion
    /// (NOT refreshed on read) — see [`NegativeProbeCache::contains_active`].
    ttl: Duration,
}

/// Bounded LRU cache of `(NodeId, hash)` pairs returned
/// `has_blob: false` within the TTL window.
#[derive(Debug)]
pub struct NegativeProbeCache {
    inner: Mutex<Inner>,
}

impl Default for NegativeProbeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl NegativeProbeCache {
    /// Build a cache with the ADR 001 § Probe cache defaults (1024 entries, 5-
    /// minute TTL).
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity_and_ttl(DEFAULT_CAPACITY, DEFAULT_TTL)
    }

    #[cfg(test)]
    #[must_use]
    fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_ttl(cap, DEFAULT_TTL)
    }

    /// Build a cache with an explicit capacity and TTL.
    ///
    /// Production code uses [`Self::new`] (the ADR 001 § Probe cache
    /// 1024-entry, 5-minute defaults); this constructor is the test /
    /// tuning seam. Integration tests in particular need it: the TTL
    /// is anchored on [`std::time::Instant`], so `tokio::time` pause /
    /// advance has no effect on it, and the only way to exercise
    /// expiry deterministically without a multi-minute wall-clock wait
    /// is to inject a short TTL here. `cap` is clamped to ≥ 1.
    #[must_use]
    pub fn with_capacity_and_ttl(cap: usize, ttl: Duration) -> Self {
        let cap = cap.max(1);
        Self {
            inner: Mutex::new(Inner {
                entries: IndexMap::with_capacity(cap),
                cap,
                ttl,
            }),
        }
    }

    /// Filter 3 (ADR 022 § Lookup integrity): returns `true` iff
    /// `(node_id, hash)` is in the cache and its TTL hasn't elapsed.
    /// A live hit bumps the entry to the front of the LRU ordering
    /// (index 0) **without refreshing the entry's expiry** — TTL is
    /// anchored at insertion per ADR 001 § Probe cache, not at read. An
    /// expired entry is evicted before returning `false`.
    #[must_use]
    pub fn contains_active(&self, node_id: &NodeId, hash: &Hash) -> bool {
        let key = (*node_id, *hash);
        let now = Instant::now();
        let mut guard = self.lock();
        let Some(expiry) = guard.entries.get(&key).copied() else {
            return false;
        };
        if expiry <= now {
            guard.entries.shift_remove(&key);
            return false;
        }
        // Bump to front of LRU, preserving the original expiry.
        // `shift_insert` on an existing key moves it to the given
        // index — no explicit remove needed.
        guard.entries.shift_insert(0, key, expiry);
        true
    }

    /// Insert / refresh `(node_id, hash)` with `now + TTL` expiry,
    /// moving the entry to the front of the LRU ordering. Evicts the
    /// least-recently-used entry on cap overflow. Producer-side hook
    /// for the outbound probe client; the lookup module never calls
    /// this directly.
    ///
    /// Uses the cache-wide TTL, which suits an AUTHORITATIVE negative:
    /// a probe's `has_blob: false` is the peer having just looked. For
    /// a weaker signal, pass its own TTL — see
    /// [`Self::record_failure_with_ttl`].
    pub fn record_failure(&self, node_id: NodeId, hash: Hash) {
        let ttl = self.lock().ttl;
        self.record_failure_with_ttl(node_id, hash, ttl);
    }

    /// [`Self::record_failure`] with an explicit TTL, for a negative
    /// that is weaker than a probe's.
    ///
    /// Entries store an absolute expiry, so mixing TTLs is free: a
    /// short-lived entry simply falls out sooner, and
    /// [`Self::contains_active`] cannot tell the two apart (nor does it
    /// need to). The caller owns the judgement of how much its evidence
    /// is worth — see `node_origin::REFUSAL_SUPPRESSION_TTL`, which is
    /// far shorter because a delivery refusal, unlike a probe answer,
    /// may not be about the peer at all.
    pub fn record_failure_with_ttl(&self, node_id: NodeId, hash: Hash, ttl: Duration) {
        let key = (node_id, hash);
        let mut guard = self.lock();
        let expiry = Instant::now() + ttl;
        // `shift_insert` moves an existing key to the new index and
        // updates the value (returning the old) — which is exactly
        // the MRU bump we want on the refresh path.
        guard.entries.shift_insert(0, key, expiry);
        if guard.entries.len() > guard.cap {
            // `pop` removes the last entry — the LRU back.
            guard.entries.pop();
        }
    }

    /// Current entry count. Includes expired entries that haven't
    /// been swept yet — call [`Self::contains_active`] first if a
    /// precise live count is needed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Whether the cache holds zero entries (including stale).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().entries.is_empty()
    }

    /// Poison-tolerant lock acquisition. Matches the in-repo pattern
    /// already established for the chain-backed staker set's
    /// `RwLock` and `decdn-cache`'s GC sweeps: a poisoned mutex
    /// means *something panicked while holding the lock*, but our
    /// writers never panic, so the inner state is structurally
    /// sound and we recover rather than propagating.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!("NegativeProbeCache mutex poisoned; recovering inner state");
                poisoned.into_inner()
            }
        }
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
    use std::thread;

    fn nid(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 32])
    }
    fn h(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }

    #[test]
    fn absent_key_returns_false() {
        let c = NegativeProbeCache::new();
        assert!(!c.contains_active(&nid(1), &h(1)));
        assert!(c.is_empty());
    }

    #[test]
    fn record_then_contains_returns_true() {
        let c = NegativeProbeCache::new();
        c.record_failure(nid(1), h(1));
        assert!(c.contains_active(&nid(1), &h(1)));
        assert!(!c.contains_active(&nid(2), &h(1)));
        assert!(!c.contains_active(&nid(1), &h(2)));
        assert_eq!(c.len(), 1);
    }

    /// Entries carry their own expiry, so a caller with weaker evidence can suppress a
    /// (peer, hash) for less time than the cache's default (#1145 review). This is what
    /// lets an unattributable delivery refusal — a wire `NotFound`, onto which seven
    /// reject reasons deliberately collapse, three of them ours or transient — cost a peer
    /// seconds of suppression rather than the five minutes an authoritative probe answer
    /// earns.
    #[test]
    fn a_short_ttl_entry_expires_while_a_default_one_is_still_active() {
        let c = NegativeProbeCache::new(); // 5-minute default
        c.record_failure(nid(1), h(1)); // authoritative: the full TTL
        c.record_failure_with_ttl(nid(2), h(1), Duration::from_millis(30)); // weak evidence

        assert!(c.contains_active(&nid(1), &h(1)));
        assert!(c.contains_active(&nid(2), &h(1)));

        thread::sleep(Duration::from_millis(60));

        assert!(
            c.contains_active(&nid(1), &h(1)),
            "the default-TTL entry must outlive the short one"
        );
        assert!(
            !c.contains_active(&nid(2), &h(1)),
            "a short-TTL entry must expire on its OWN clock — if it inherited the cache \
             default, a healthy peer stays blackholed long after the transient cause passed"
        );
    }

    #[test]
    fn expired_entry_returns_false_and_is_evicted() {
        // Margins kept generous (TTL 500ms, sleep 750ms) so loaded
        // CI runners with cargo-nextest parallelism don't flake on
        // wall-clock checks.
        let c = NegativeProbeCache::with_capacity_and_ttl(8, Duration::from_millis(500));
        c.record_failure(nid(1), h(1));
        assert!(c.contains_active(&nid(1), &h(1)));
        thread::sleep(Duration::from_millis(750));
        assert!(!c.contains_active(&nid(1), &h(1)));
        assert!(
            c.is_empty(),
            "expired entry should have been evicted on read"
        );
    }

    #[test]
    fn lru_eviction_at_cap_drops_oldest() {
        let c = NegativeProbeCache::with_capacity(2);
        c.record_failure(nid(1), h(1));
        c.record_failure(nid(2), h(2));
        c.record_failure(nid(3), h(3));
        // nid(1) was least-recently-used → evicted.
        assert!(!c.contains_active(&nid(1), &h(1)));
        assert!(c.contains_active(&nid(2), &h(2)));
        assert!(c.contains_active(&nid(3), &h(3)));
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn read_hit_bumps_lru_so_oldest_eviction_changes() {
        let c = NegativeProbeCache::with_capacity(2);
        c.record_failure(nid(1), h(1));
        c.record_failure(nid(2), h(2));
        // Bump nid(1) by reading it → nid(2) becomes LRU.
        assert!(c.contains_active(&nid(1), &h(1)));
        c.record_failure(nid(3), h(3));
        assert!(c.contains_active(&nid(1), &h(1)));
        assert!(!c.contains_active(&nid(2), &h(2)));
        assert!(c.contains_active(&nid(3), &h(3)));
    }

    /// Pins indexmap's `shift_insert(0, existing_key, value)`
    /// semantic: an existing key MOVES to index 0 (MRU position).
    /// The LRU bumping in `contains_active` and the refresh path in
    /// `record_failure` both rely on this. If indexmap ever changes
    /// to "keep at original index" (a major-version concern), this
    /// test fails and surfaces the regression before LRU silently
    /// degrades.
    #[test]
    fn shift_insert_on_existing_key_moves_to_front_of_lru() {
        let c = NegativeProbeCache::with_capacity(4);
        c.record_failure(nid(1), h(1));
        c.record_failure(nid(2), h(2));
        c.record_failure(nid(3), h(3));
        // Re-record nid(1) — should become MRU (index 0).
        c.record_failure(nid(1), h(1));
        let guard = c.lock();
        assert_eq!(
            guard.entries.get_index_of(&(nid(1), h(1))),
            Some(0),
            "shift_insert should have moved nid(1) to index 0"
        );
    }

    /// Pins the ADR 001 § Probe cache invariant that TTL is anchored
    /// at insertion, NOT refreshed on read. A regression in
    /// [`NegativeProbeCache::contains_active`] that re-stamped
    /// expiry during the LRU bump would extend the suppression
    /// window beyond spec and ship green without this test.
    #[test]
    fn read_hit_does_not_refresh_ttl() {
        let c = NegativeProbeCache::with_capacity_and_ttl(8, Duration::from_millis(500));
        c.record_failure(nid(1), h(1));
        // Half-TTL — entry still live; read bumps LRU.
        thread::sleep(Duration::from_millis(250));
        assert!(c.contains_active(&nid(1), &h(1)));
        // Past the original TTL window. If the read had refreshed
        // the expiry, the entry would still be live here.
        thread::sleep(Duration::from_millis(500));
        assert!(
            !c.contains_active(&nid(1), &h(1)),
            "read-hit illegally extended the TTL window"
        );
    }

    #[test]
    fn re_recording_same_key_refreshes_ttl_does_not_grow_len() {
        // TTL=1000ms; insert, wait 500ms, re-record, wait 750ms.
        // Without the refresh the first insert (t=0, TTL 1000ms)
        // would have expired by t=1250ms; the refresh at t=500ms
        // reset the window, so the entry should still be live at
        // t=1250ms (750ms post-refresh, inside the 1000ms TTL).
        let c = NegativeProbeCache::with_capacity_and_ttl(8, Duration::from_secs(1));
        c.record_failure(nid(1), h(1));
        thread::sleep(Duration::from_millis(500));
        c.record_failure(nid(1), h(1));
        thread::sleep(Duration::from_millis(750));
        assert!(c.contains_active(&nid(1), &h(1)));
        assert_eq!(c.len(), 1);
    }
}
