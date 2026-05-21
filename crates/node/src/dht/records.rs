//! Receiver-side DHT record store (ADR 022 §Content Records and TTL).
//!
//! When a publisher P sends `StoreRequest { hash: H, holder: P }` and the
//! handler admits it (rate limit + `holder == authenticated NodeId` +
//! `StakerSet::is_active(holder)` all pass), the record `(H, P)` is
//! inserted here with `expiry_us = receive_us + ttl_us` and counted
//! against P's per-publisher quota.
//!
//! # Admission policy (ADR 022 §Content Records and TTL)
//!
//! | Limit | Default | Behaviour at cap |
//! |-------|---------|-------------------|
//! | Per-publisher records | 200 | Hard reject — `StoreAck { accepted: false }` |
//! | Per-hash providers | 50 | Evict oldest holder for that hash |
//! | Per-node global records | 100,000 | Evict globally-oldest record (only when inserting publisher is below per-publisher cap) |
//! | Record TTL | 1 hour | Receiver-anchored; refreshed on re-publish |
//!
//! The per-publisher hard cap is the load-bearing defense against the
//! exhaustion attack where one publisher fills every slot and forces
//! eviction of other publishers' records. With that cap in place, the
//! global LRU only fires when *the inserting publisher is below quota*
//! — i.e. there is a non-malicious publisher pushing the total over
//! capacity, and evicting the globally-oldest record (likely from a
//! publisher who has stopped re-publishing) is the right call.
//!
//! # Re-publish semantics
//!
//! Re-publish from the same `(holder, hash)` pair refreshes `expiry_us`
//! in place rather than creating a second record. The per-publisher
//! count is unchanged. Per ADR 022 line 122 this is what extends record
//! lifetime ahead of the previous expiry while the holder still has the
//! blob.

use std::collections::{BTreeSet, HashMap};

use crate::dht::routing::NodeId;
use decdn_protocol::MAX_PROVIDERS_PER_HASH;

/// 32-byte content hash. Distinct from `NodeId` only at the type alias
/// level — the wire format treats both as `[u8; 32]`.
pub type Hash = [u8; 32];

/// Outcome of a [`RecordStore::insert_at`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// Record was newly inserted.
    Inserted,
    /// Record already existed; its `expiry_us` was refreshed in place.
    Refreshed,
    /// Publisher is at the per-publisher cap — hard reject.
    RejectedQuotaExceeded,
}

impl InsertOutcome {
    /// Whether this outcome should map to `StoreAck { accepted: true }`.
    #[must_use]
    pub const fn accepted(self) -> bool {
        matches!(self, Self::Inserted | Self::Refreshed)
    }
}

/// A single `(holder, hash)` record. `receive_us` is the receiver-anchored
/// wall-clock at acceptance; `expiry_us = receive_us + ttl_us`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HolderEntry {
    holder: NodeId,
    receive_us: u64,
    expiry_us: u64,
}

/// Configuration for the record store (ADR 022 §Content Records and TTL).
#[derive(Debug, Clone)]
pub struct RecordStoreConfig {
    /// `Max records per publisher (per receiver)`. ADR 022 default: 200.
    pub max_records_per_publisher: usize,
    /// `Max records per node`. ADR 022 default: 100,000.
    pub max_records_global: usize,
    /// `Max providers per hash`. ADR 022 default: 50 — pinned to the
    /// wire response cap so a fully-populated record can be serialised
    /// straight into a [`decdn_protocol::FindValueResponse`].
    pub max_providers_per_hash: usize,
    /// `Record TTL` in microseconds. ADR 022 default: 1 hour =
    /// `3_600_000_000` μs.
    pub ttl_us: u64,
}

impl Default for RecordStoreConfig {
    fn default() -> Self {
        Self {
            max_records_per_publisher: 200,
            max_records_global: 100_000,
            max_providers_per_hash: MAX_PROVIDERS_PER_HASH,
            ttl_us: 3_600_000_000,
        }
    }
}

/// Receiver-side DHT record store.
///
/// Not internally synchronised — the handler owns it behind a `Mutex` /
/// `RwLock`, mirroring how `RoutingTable` is composed. The methods are
/// `&mut self` because every admission path touches multiple indexes
/// (the primary `(hash → entries)` map, the per-publisher quota count,
/// and the global LRU ordering); requiring exclusive access keeps those
/// invariants atomic without internal locking overhead.
#[derive(Debug)]
pub struct RecordStore {
    cfg: RecordStoreConfig,
    /// Primary: `hash → list of holder entries`.
    by_hash: HashMap<Hash, Vec<HolderEntry>>,
    /// Per-publisher record count. Keyed by holder; value is the count
    /// of distinct hashes that holder currently has records for. Stays
    /// in sync with `by_hash` via every insert/evict/expire path.
    by_publisher_count: HashMap<NodeId, usize>,
    /// Global LRU ordering keyed by `(receive_us, hash, holder)`. The
    /// tuple is unique enough in practice (microsecond resolution +
    /// 32-byte hash + 32-byte holder); under burst collisions the
    /// `BTreeSet` semantics naturally serialise.
    global_lru: BTreeSet<(u64, Hash, NodeId)>,
    /// Monotonic counter applied to `receive_us` so two
    /// `insert_at(holder, hash, ts)` calls with the same wall-clock
    /// timestamp still produce distinct keys in `global_lru`. We mix it
    /// into the low bits of `receive_us` at `insert_at` time.
    monotonic_tail: u64,
}

impl RecordStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new(cfg: RecordStoreConfig) -> Self {
        Self {
            cfg,
            by_hash: HashMap::new(),
            by_publisher_count: HashMap::new(),
            global_lru: BTreeSet::new(),
            monotonic_tail: 0,
        }
    }

    /// Total record count across all publishers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.global_lru.len()
    }

    /// True iff the store has zero records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.global_lru.is_empty()
    }

    /// Record count for one publisher. Returns 0 for unknown publishers.
    #[must_use]
    pub fn publisher_record_count(&self, holder: &NodeId) -> usize {
        self.by_publisher_count.get(holder).copied().unwrap_or(0)
    }

    /// Insert or refresh `(holder, hash)` with `receive_us` as the
    /// receiver-anchored wall-clock at acceptance. Returns the outcome.
    ///
    /// The caller is responsible for the upstream admission checks (rate
    /// limit, `holder == authenticated NodeId`, `StakerSet::is_active`).
    /// This method enforces only the storage-level invariants (per-publisher
    /// quota with hard reject, global LRU with eviction, per-hash provider
    /// cap with oldest-provider eviction).
    pub fn insert_at(&mut self, holder: NodeId, hash: Hash, receive_us: u64) -> InsertOutcome {
        // Mix in a monotonic tail so the global_lru key stays unique
        // even under bursty wall-clock returns. The mixing is additive
        // and dominated by the high-order microsecond bits in normal
        // operation; the BTreeSet ordering is still effectively "oldest
        // first" because the wall-clock dominates.
        self.monotonic_tail = self.monotonic_tail.wrapping_add(1);
        let receive_us = receive_us.saturating_add(self.monotonic_tail);

        // Refresh path: holder already has a record for this hash.
        if let Some(entries) = self.by_hash.get_mut(&hash)
            && let Some(idx) = entries.iter().position(|e| e.holder == holder)
        {
            // Remove old `global_lru` key, insert refreshed.
            let Some(slot) = entries.get_mut(idx) else {
                // `position` returned a valid index — this branch is
                // unreachable, but the get_mut keeps clippy's
                // anti-panic indexing rule satisfied without a bare
                // `[idx]` expression.
                return InsertOutcome::Refreshed;
            };
            let prev = *slot;
            self.global_lru.remove(&(prev.receive_us, hash, holder));
            *slot = HolderEntry {
                holder,
                receive_us,
                expiry_us: receive_us.saturating_add(self.cfg.ttl_us),
            };
            self.global_lru.insert((receive_us, hash, holder));
            return InsertOutcome::Refreshed;
        }

        // New record path. Per-publisher hard cap first — this is the
        // rule that closes the exhaustion attack.
        let publisher_count = self.publisher_record_count(&holder);
        if publisher_count >= self.cfg.max_records_per_publisher {
            return InsertOutcome::RejectedQuotaExceeded;
        }

        // Global cap: when full, evict globally-oldest. By construction
        // the inserting publisher is below its per-publisher cap here
        // (we just checked), so the eviction is safe under ADR 022's
        // two-tier rule.
        if self.global_lru.len() >= self.cfg.max_records_global {
            self.evict_globally_oldest();
        }

        // Per-hash provider cap: if at the wire-response cap, evict the
        // oldest holder for that hash to make room. ADR 022 doesn't
        // mandate a specific eviction policy at the per-hash cap — we
        // pick oldest-receive because (a) `closer_nodes` is already
        // capped at this number on the response side and (b) the
        // newcomer is the one most likely to still be live.
        let entries = self.by_hash.entry(hash).or_default();
        if entries.len() >= self.cfg.max_providers_per_hash {
            // Find the entry with the smallest receive_us.
            if let Some((idx, oldest)) = entries
                .iter()
                .enumerate()
                .min_by_key(|(_, e)| e.receive_us)
                .map(|(i, e)| (i, *e))
            {
                self.global_lru
                    .remove(&(oldest.receive_us, hash, oldest.holder));
                let old_holder = oldest.holder;
                entries.swap_remove(idx);
                Self::decrement_publisher_count(&mut self.by_publisher_count, &old_holder);
            }
        }

        let entry = HolderEntry {
            holder,
            receive_us,
            expiry_us: receive_us.saturating_add(self.cfg.ttl_us),
        };
        entries.push(entry);
        self.global_lru.insert((receive_us, hash, holder));
        *self.by_publisher_count.entry(holder).or_insert(0) += 1;
        InsertOutcome::Inserted
    }

    /// Return the current holders for `hash`, dropping any whose
    /// `expiry_us <= now_us`. The result is capped at the configured
    /// per-hash provider count (which equals the wire
    /// [`MAX_PROVIDERS_PER_HASH`] in production), so the handler can
    /// serialise the return value directly into a
    /// [`decdn_protocol::FindValueResponse`].
    ///
    /// `now_us` is the requester-side wall-clock — passed in rather
    /// than read inline so the same store can be exercised under a
    /// mock clock in tests.
    pub fn providers_at(&mut self, hash: &Hash, now_us: u64) -> Vec<NodeId> {
        // Scrub expired entries for this hash before returning.
        self.scrub_hash_expired(hash, now_us);
        self.by_hash
            .get(hash)
            .map(|entries| entries.iter().map(|e| e.holder).collect())
            .unwrap_or_default()
    }

    /// Periodic GC sweep — removes every expired record from every
    /// index. Intended to run on a tokio interval; the wall-clock cost
    /// scales linearly with the global record count.
    pub fn gc(&mut self, now_us: u64) -> usize {
        let mut removed = 0usize;
        // Walk hashes; for each, drop expired entries and update indexes.
        let hashes: Vec<Hash> = self.by_hash.keys().copied().collect();
        for h in hashes {
            removed += self.scrub_hash_expired(&h, now_us);
        }
        removed
    }

    /// Drop expired holders for `hash`. Returns the number removed.
    fn scrub_hash_expired(&mut self, hash: &Hash, now_us: u64) -> usize {
        let Some(entries) = self.by_hash.get_mut(hash) else {
            return 0;
        };
        let mut removed = 0usize;
        let mut i = 0usize;
        while i < entries.len() {
            // `get` then unwrap_or — anti-panic policy keeps us off
            // direct indexing even though the bound is loop-invariant.
            let Some(e) = entries.get(i).copied() else {
                break;
            };
            if e.expiry_us <= now_us {
                self.global_lru.remove(&(e.receive_us, *hash, e.holder));
                entries.swap_remove(i);
                Self::decrement_publisher_count(&mut self.by_publisher_count, &e.holder);
                removed += 1;
            } else {
                i += 1;
            }
        }
        if entries.is_empty() {
            self.by_hash.remove(hash);
        }
        removed
    }

    /// Evict the globally-oldest record from every index.
    fn evict_globally_oldest(&mut self) {
        let Some(&(receive_us, hash, holder)) = self.global_lru.iter().next() else {
            return;
        };
        self.global_lru.remove(&(receive_us, hash, holder));
        if let Some(entries) = self.by_hash.get_mut(&hash)
            && let Some(idx) = entries.iter().position(|e| e.holder == holder)
        {
            entries.swap_remove(idx);
            if entries.is_empty() {
                self.by_hash.remove(&hash);
            }
        }
        Self::decrement_publisher_count(&mut self.by_publisher_count, &holder);
    }

    /// Decrement a publisher's record count; remove the map entry when
    /// the count drops to zero so `publisher_record_count` stays a
    /// truthful "0 means unknown" view.
    fn decrement_publisher_count(counts: &mut HashMap<NodeId, usize>, holder: &NodeId) {
        if let Some(c) = counts.get_mut(holder) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                counts.remove(holder);
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation
)]
mod tests {
    use super::*;

    fn nid(b: u8) -> NodeId {
        [b; 32]
    }
    fn h(b: u8) -> Hash {
        [b; 32]
    }

    fn small_cfg() -> RecordStoreConfig {
        RecordStoreConfig {
            max_records_per_publisher: 3,
            max_records_global: 6,
            max_providers_per_hash: 4,
            ttl_us: 1_000_000, // 1 second for fast tests
        }
    }

    #[test]
    fn insert_new_returns_inserted_and_increments_counts() {
        let mut s = RecordStore::new(small_cfg());
        assert_eq!(s.len(), 0);
        let out = s.insert_at(nid(1), h(1), 0);
        assert_eq!(out, InsertOutcome::Inserted);
        assert!(out.accepted());
        assert_eq!(s.len(), 1);
        assert_eq!(s.publisher_record_count(&nid(1)), 1);
        assert_eq!(s.providers_at(&h(1), 0), vec![nid(1)]);
    }

    #[test]
    fn re_insert_same_holder_hash_refreshes_not_duplicates() {
        let mut s = RecordStore::new(small_cfg());
        s.insert_at(nid(1), h(1), 100);
        let out = s.insert_at(nid(1), h(1), 200);
        assert_eq!(out, InsertOutcome::Refreshed);
        assert!(out.accepted());
        // Count stays at 1 (refresh, not new).
        assert_eq!(s.len(), 1);
        assert_eq!(s.publisher_record_count(&nid(1)), 1);
        // expiry_us must reflect the LATER receive_us.
        let entries = s.by_hash.get(&h(1)).unwrap();
        assert_eq!(entries.len(), 1);
        // receive_us is monotonic-counter-mixed but later >= earlier.
        assert!(entries[0].receive_us > 100);
        assert!(entries[0].expiry_us > 100 + 1_000_000);
    }

    #[test]
    fn per_publisher_cap_hard_rejects() {
        let mut s = RecordStore::new(small_cfg());
        // cap = 3
        for i in 1..=3u8 {
            assert_eq!(s.insert_at(nid(7), h(i), i.into()), InsertOutcome::Inserted);
        }
        // Fourth distinct hash from publisher 7 must reject.
        let out = s.insert_at(nid(7), h(4), 10);
        assert_eq!(out, InsertOutcome::RejectedQuotaExceeded);
        assert!(!out.accepted());
        // Counts unchanged.
        assert_eq!(s.publisher_record_count(&nid(7)), 3);
        assert_eq!(s.len(), 3);
    }

    #[test]
    fn per_publisher_cap_does_not_block_other_publishers() {
        let mut s = RecordStore::new(small_cfg());
        for i in 1..=3u8 {
            s.insert_at(nid(7), h(i), i.into());
        }
        // Publisher 8 is below their cap and should be admitted.
        assert_eq!(s.insert_at(nid(8), h(4), 100), InsertOutcome::Inserted);
        assert_eq!(s.publisher_record_count(&nid(8)), 1);
    }

    #[test]
    fn global_cap_evicts_oldest_when_publisher_below_quota() {
        // cap_global = 6, cap_per_publisher = 3.
        let mut s = RecordStore::new(small_cfg());
        // Fill: publisher 1 → hashes 1..3, publisher 2 → hashes 4..6.
        for (p, h_byte, ts) in [
            (1u8, 1u8, 10u64),
            (1, 2, 20),
            (1, 3, 30),
            (2, 4, 40),
            (2, 5, 50),
            (2, 6, 60),
        ] {
            s.insert_at(nid(p), h(h_byte), ts);
        }
        assert_eq!(s.len(), 6);
        // Insert from publisher 3 (below their cap) — should evict
        // the globally-oldest (publisher 1 / hash 1, receive_us 10).
        let out = s.insert_at(nid(3), h(7), 70);
        assert_eq!(out, InsertOutcome::Inserted);
        assert_eq!(s.len(), 6);
        assert!(s.providers_at(&h(1), 0).is_empty(), "oldest hash evicted");
        assert_eq!(s.providers_at(&h(7), 0), vec![nid(3)]);
        // Publisher 1's count dropped by 1.
        assert_eq!(s.publisher_record_count(&nid(1)), 2);
    }

    #[test]
    fn ttl_expiry_drops_record_on_providers_at() {
        let mut s = RecordStore::new(small_cfg());
        s.insert_at(nid(1), h(1), 1_000);
        // Before TTL: present.
        assert_eq!(s.providers_at(&h(1), 1_000).len(), 1);
        // After TTL (now_us > expiry_us): scrubbed.
        let after = 1_000 + 2_000_000;
        assert!(s.providers_at(&h(1), after).is_empty());
        // Indexes cleaned up.
        assert_eq!(s.len(), 0);
        assert_eq!(s.publisher_record_count(&nid(1)), 0);
    }

    #[test]
    fn gc_sweep_removes_every_expired_record() {
        let mut cfg = small_cfg();
        cfg.ttl_us = 100;
        let mut s = RecordStore::new(cfg);
        for i in 1..=4u8 {
            s.insert_at(nid(i), h(i), 0);
        }
        assert_eq!(s.len(), 4);
        let removed = s.gc(10_000);
        assert_eq!(removed, 4);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn per_hash_cap_evicts_oldest_holder_for_that_hash() {
        // cap_per_hash = 4. Insert 4 holders for hash 1, then a 5th
        // (different publishers each so per-publisher cap doesn't fire).
        let mut s = RecordStore::new(small_cfg());
        for i in 1..=4u8 {
            assert_eq!(s.insert_at(nid(i), h(1), i.into()), InsertOutcome::Inserted);
        }
        assert_eq!(s.providers_at(&h(1), 0).len(), 4);
        // Fifth holder for the same hash: oldest (publisher 1) should
        // be evicted.
        assert_eq!(s.insert_at(nid(5), h(1), 100), InsertOutcome::Inserted);
        let providers: std::collections::HashSet<_> =
            s.providers_at(&h(1), 0).into_iter().collect();
        assert!(!providers.contains(&nid(1)));
        assert!(providers.contains(&nid(5)));
        assert_eq!(providers.len(), 4);
    }

    #[test]
    fn outcome_accepted_helper() {
        assert!(InsertOutcome::Inserted.accepted());
        assert!(InsertOutcome::Refreshed.accepted());
        assert!(!InsertOutcome::RejectedQuotaExceeded.accepted());
    }

    #[test]
    fn refresh_does_not_count_against_quota() {
        let mut s = RecordStore::new(small_cfg());
        // Fill publisher 1 to cap.
        for i in 1..=3u8 {
            s.insert_at(nid(1), h(i), i.into());
        }
        assert_eq!(s.publisher_record_count(&nid(1)), 3);
        // Refresh hash 1 — still 3.
        let out = s.insert_at(nid(1), h(1), 100);
        assert_eq!(out, InsertOutcome::Refreshed);
        assert_eq!(s.publisher_record_count(&nid(1)), 3);
        // A NEW hash from publisher 1 still rejects.
        assert_eq!(
            s.insert_at(nid(1), h(99), 200),
            InsertOutcome::RejectedQuotaExceeded
        );
    }
}
