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
//! Re-publish from the same `(holder, hash)` pair refreshes the record
//! (re-derives `expiry_us` from the new `receive_us`) rather than creating
//! a second record. The per-publisher count nets out unchanged — the
//! refresh removes the stale record and re-adds it, so the count is
//! decremented then re-incremented for the same holder. Per ADR 022
//! §Content Records and TTL this is what extends record lifetime ahead of
//! the previous expiry while the holder still has the blob.

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
    /// Record already existed; it was refreshed (re-created at the new
    /// `receive_us`) rather than duplicated.
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
/// wall-clock at acceptance (ADR 022 §Content Records and TTL line 122);
/// `expiry_us = receive_us + ttl_us` strictly — no monotonic tie-breaker
/// mixed in, so TTL never drifts with insert volume. `sequence` is the
/// per-insert monotonic counter used only to break wall-clock collisions
/// in the [`RecordStore::global_lru`] ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HolderEntry {
    holder: NodeId,
    receive_us: u64,
    sequence: u64,
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
    /// Global LRU ordering keyed by `(receive_us, sequence, hash, holder)`.
    /// `receive_us` is the wall-clock at acceptance; `sequence` is a
    /// monotonic counter that breaks wall-clock collisions under burst.
    /// Since `ttl_us` is constant, ordering by this tuple is also
    /// ordering by `expiry_us` — which is what makes [`RecordStore::gc`]
    /// an `O(expired)` front-walk rather than an `O(N)` scan.
    global_lru: BTreeSet<(u64, u64, Hash, NodeId)>,
    /// Monotonic per-insert counter feeding the `sequence` field of new
    /// records (and the second component of `global_lru` keys). Kept
    /// strictly distinct from `receive_us` so the
    /// `expiry_us = receive_us + ttl_us` invariant from ADR 022 line
    /// 122 holds — TTL does not drift forward with insert volume.
    next_sequence: u64,
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
            next_sequence: 0,
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
        // Allocate one monotonic sequence per call. Stored on the entry
        // and used as the second component of the `global_lru` key, so
        // wall-clock collisions don't collapse two records onto one
        // BTreeSet slot. Crucially this is NOT mixed into `receive_us`
        // or `expiry_us` — TTL stays exactly `ttl_us` after wall-clock
        // acceptance per ADR 022 line 122.
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);

        // Refresh path: holder already has a record for this hash. Drop
        // the stale tri-index entry and re-add at the new timestamp.
        // `remove_entry` then `add_entry` is net-zero on the per-publisher
        // count and keeps the tri-index update atomic, so there is no
        // hand-rolled in-place mutation (and no `get_mut` corruption
        // branch) to keep in sync.
        if self.remove_entry(holder, hash).is_some() {
            self.add_entry(holder, hash, receive_us, sequence);
            return InsertOutcome::Refreshed;
        }

        // New record path. Per-publisher hard cap first — this is the
        // rule that closes the exhaustion attack.
        if self.publisher_record_count(&holder) >= self.cfg.max_records_per_publisher {
            return InsertOutcome::RejectedQuotaExceeded;
        }

        // Per-hash provider cap, FIRST. If this hash is at the wire cap
        // we evict its oldest holder and net out at the same global count
        // — so the global LRU below MUST NOT also fire (otherwise one
        // insert produces two evictions and leaves the store under-full).
        // We only read `by_hash` here to pick the victim; the mutation
        // goes through `remove_entry` so all three indexes move together.
        // ADR 022 doesn't mandate a per-hash eviction policy — we pick
        // oldest-receive because the newcomer is most likely still live.
        let evict_holder = self.by_hash.get(&hash).and_then(|entries| {
            (entries.len() >= self.cfg.max_providers_per_hash)
                .then(|| {
                    entries
                        .iter()
                        .min_by_key(|e| e.receive_us)
                        .map(|e| e.holder)
                })
                .flatten()
        });
        let grows_global = match evict_holder {
            Some(old_holder) => {
                self.remove_entry(old_holder, hash);
                false
            }
            None => true,
        };

        // Global cap fires only when we're actually about to grow the
        // store. By construction the inserting publisher is below its
        // per-publisher cap here (checked above) so global LRU eviction
        // is safe under ADR 022's two-tier rule.
        if grows_global && self.global_lru.len() >= self.cfg.max_records_global {
            self.evict_globally_oldest();
        }

        self.add_entry(holder, hash, receive_us, sequence);
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

    /// Periodic GC sweep — drops every expired record across the store.
    /// Returns the number removed.
    ///
    /// `ttl_us` is constant for all records, so `global_lru` (ordered by
    /// `receive_us`) is ALSO ordered by `expiry_us` — the oldest entries
    /// are always at the front. The sweep pops from `global_lru.first()`
    /// until it sees a non-expired record and returns, which is
    /// `O(expired × log N)` rather than the previous `O(N)` full scan.
    pub fn gc(&mut self, now_us: u64) -> usize {
        let mut removed = 0usize;
        while let Some(&key @ (receive_us, _, hash, holder)) = self.global_lru.iter().next() {
            // `expiry_us = receive_us + ttl_us` by construction. We
            // recompute it here rather than carrying it in the key so
            // the BTreeSet ordering doesn't depend on a derived field.
            let expiry_us = receive_us.saturating_add(self.cfg.ttl_us);
            if expiry_us > now_us {
                break;
            }
            if self.remove_entry(holder, hash).is_none() {
                // The tri-index invariant guarantees a `global_lru` key
                // has a matching `by_hash` entry, so this is unreachable
                // except under heap corruption. Drop the orphaned key
                // directly so the front-walk still terminates (never
                // spins on a key `remove_entry` can't clear).
                self.global_lru.remove(&key);
                tracing::error!(
                    ?holder,
                    ?hash,
                    "RecordStore::gc: global_lru key with no by_hash entry; dropping orphan"
                );
            }
            removed += 1;
        }
        removed
    }

    /// Drop expired holders for `hash`. Returns the number removed.
    /// Called by [`Self::providers_at`] on the read path so a stale
    /// holder never appears in a `FindValueResponse` even between GC
    /// ticks.
    fn scrub_hash_expired(&mut self, hash: &Hash, now_us: u64) -> usize {
        // Collect the expired holders under an immutable borrow, then
        // drop each through `remove_entry` so the tri-index update (and
        // the empty-bucket cleanup) stays in one place.
        let Some(entries) = self.by_hash.get(hash) else {
            return 0;
        };
        let expired: Vec<NodeId> = entries
            .iter()
            .filter(|e| e.expiry_us <= now_us)
            .map(|e| e.holder)
            .collect();
        for holder in &expired {
            self.remove_entry(*holder, *hash);
        }
        expired.len()
    }

    /// Evict the globally-oldest record from every index.
    fn evict_globally_oldest(&mut self) {
        let Some(&(_, _, hash, holder)) = self.global_lru.iter().next() else {
            return;
        };
        self.remove_entry(holder, hash);
    }

    /// Atomically insert `(holder, hash)` across all three indexes with
    /// the given receive timestamp + sequence: the `by_hash` provider
    /// list, the `global_lru` ordering, and the per-publisher count.
    /// The caller has already verified (or, on the refresh path, ensured
    /// by construction) that the publisher is below quota and that any
    /// eviction owed for this insert has happened. Touching one index
    /// without the others is the tri-index bug class this method exists to
    /// make impossible — see [`Self::remove_entry`].
    fn add_entry(&mut self, holder: NodeId, hash: Hash, receive_us: u64, sequence: u64) {
        let expiry_us = receive_us.saturating_add(self.cfg.ttl_us);
        self.by_hash.entry(hash).or_default().push(HolderEntry {
            holder,
            receive_us,
            sequence,
            expiry_us,
        });
        self.global_lru.insert((receive_us, sequence, hash, holder));
        *self.by_publisher_count.entry(holder).or_insert(0) += 1;
    }

    /// Atomically remove `(holder, hash)` from all three indexes. Returns
    /// the removed [`HolderEntry`], or `None` if no such record exists.
    /// `(holder, hash)` is unique within a hash's provider list —
    /// [`Self::insert_at`] removes any existing `(holder, hash)` before it
    /// adds, so a holder never accumulates two records for one hash — so
    /// removal by holder is unambiguous.
    fn remove_entry(&mut self, holder: NodeId, hash: Hash) -> Option<HolderEntry> {
        let entries = self.by_hash.get_mut(&hash)?;
        let idx = entries.iter().position(|e| e.holder == holder)?;
        let Some(removed) = entries.get(idx).copied() else {
            // `position` just yielded `idx`, so `get(idx)` is `Some`
            // except under heap corruption. The two legitimate
            // "not a record" misses (`?` above) returned already, so a
            // `None` here is never a normal miss — surface it loudly.
            // This restores the error signal the pre-refactor in-place
            // refresh branch logged, now at its true source so all
            // callers (refresh, evictions, gc, scrub) inherit it. The
            // `get(idx)` keeps clippy's anti-panic policy off bare `[idx]`.
            tracing::error!(
                ?holder,
                ?hash,
                idx,
                "RecordStore::remove_entry: position->Some but get->None; tri-index corruption"
            );
            return None;
        };
        entries.swap_remove(idx);
        if entries.is_empty() {
            self.by_hash.remove(&hash);
        }
        self.global_lru
            .remove(&(removed.receive_us, removed.sequence, hash, holder));
        Self::decrement_publisher_count(&mut self.by_publisher_count, &holder);
        Some(removed)
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
        // expiry_us must reflect the LATER receive_us (the refresh is
        // at wall-clock 200 with `ttl_us = 1_000_000`, so the refreshed
        // entry's expiry is exactly 200 + ttl_us = 1_000_200).
        let entries = s.by_hash.get(&h(1)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].receive_us, 200);
        assert_eq!(entries[0].expiry_us, 200 + 1_000_000);
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

    /// `RecordStoreConfig::default` MUST match the ADR 022 §Content
    /// Records and TTL table verbatim. Every other test uses
    /// `small_cfg()`, so without this assertion a regression that
    /// halves the production defaults would not fail any test.
    #[test]
    fn default_config_matches_adr_022() {
        let d = RecordStoreConfig::default();
        assert_eq!(d.max_records_per_publisher, 200);
        assert_eq!(d.max_records_global, 100_000);
        assert_eq!(d.max_providers_per_hash, MAX_PROVIDERS_PER_HASH);
        assert_eq!(d.max_providers_per_hash, 50);
        // 1 hour in microseconds.
        assert_eq!(d.ttl_us, 3_600_000_000);
    }

    /// Boundary case: when both the global cap and the per-publisher
    /// cap are saturated simultaneously, the per-publisher cap MUST
    /// fire first (hard reject). The global LRU MUST NOT evict
    /// another publisher's record on a request that's about to be
    /// rejected for quota reasons — that would let a quota-exceeded
    /// publisher displace records they should not be touching.
    #[test]
    fn publisher_quota_beats_global_lru() {
        // Tight caps: per-publisher = 3, global = 3. Publisher 1 fills
        // both caps simultaneously.
        let cfg = RecordStoreConfig {
            max_records_per_publisher: 3,
            max_records_global: 3,
            max_providers_per_hash: 100,
            ttl_us: 1_000_000,
        };
        let mut s = RecordStore::new(cfg);
        for i in 1..=3u8 {
            assert_eq!(s.insert_at(nid(1), h(i), i.into()), InsertOutcome::Inserted);
        }
        // Both global (3 records) and per-publisher (3 from nid(1))
        // caps are now at the threshold. A 4th Store from nid(1)
        // must hard-reject without touching the global LRU.
        let len_before = s.len();
        let count_before = s.publisher_record_count(&nid(1));
        let out = s.insert_at(nid(1), h(99), 100);
        assert_eq!(out, InsertOutcome::RejectedQuotaExceeded);
        assert_eq!(s.len(), len_before, "rejected insert must not evict");
        assert_eq!(s.publisher_record_count(&nid(1)), count_before);
        // Existing records are intact.
        for i in 1..=3u8 {
            assert!(!s.providers_at(&h(i), 0).is_empty(), "h({i}) intact");
        }
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

    /// ADR 022 §Content Records and TTL line 122: `expiry_us =
    /// receive_us + record_ttl_us`. A previous implementation mixed a
    /// monotonic insert counter into `receive_us`, which made TTL drift
    /// forward with insert volume. This test pins the no-drift
    /// contract by inserting one record after 10k unrelated inserts
    /// and confirming its TTL is *exactly* `ttl_us` after the
    /// wall-clock it was admitted at.
    #[test]
    fn ttl_is_anchored_to_wall_clock_regardless_of_insert_volume() {
        let mut cfg = small_cfg();
        cfg.max_records_per_publisher = usize::MAX;
        cfg.max_records_global = usize::MAX;
        cfg.max_providers_per_hash = usize::MAX;
        cfg.ttl_us = 1_000;
        let mut s = RecordStore::new(cfg);
        // Burn 10k inserts on a separate hash so the monotonic counter
        // advances well past `ttl_us` (10_000 > 1_000). The prior
        // implementation would have shifted `expiry_us` forward by
        // ~10k μs at this point; we want exact equality with
        // `receive_us + ttl_us`.
        for i in 0..10_000u32 {
            let mut holder = [0u8; 32];
            holder[..4].copy_from_slice(&i.to_le_bytes());
            s.insert_at(holder, h(0xEE), 500);
        }
        // Insert the record under test at wall-clock 500.
        s.insert_at(nid(0xCC), h(0xFF), 500);
        // Expiry MUST be exactly 500 + 1_000 = 1_500, with no drift
        // from the prior 10k inserts.
        let entries = s.by_hash.get(&h(0xFF)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].expiry_us, 1_500,
            "TTL drifted to {} — must be exactly receive_us + ttl_us",
            entries[0].expiry_us,
        );
        // And consequently the record is still alive at wall-clock 1_499
        // and dead at 1_501 (i.e. the TTL window is exactly ttl_us long).
        assert_eq!(s.providers_at(&h(0xFF), 1_499).len(), 1);
        assert!(s.providers_at(&h(0xFF), 1_501).is_empty());
    }

    /// Regression guard for the "two evictions for one insert" bug. When
    /// the global cap is full AND the inserting hash is at the per-hash
    /// provider cap, only the per-hash eviction must fire (it nets the
    /// store out at the same global count). A prior implementation
    /// evicted globally first, then evicted within the hash — leaving
    /// the store under-full by one record AND dropping an unrelated
    /// hash's holder.
    #[test]
    fn insert_at_per_hash_cap_does_not_also_trigger_global_eviction() {
        // Build a config that's easy to saturate: global = 4,
        // per-hash = 2, per-publisher = 4.
        let cfg = RecordStoreConfig {
            max_records_per_publisher: 4,
            max_records_global: 4,
            max_providers_per_hash: 2,
            ttl_us: 1_000_000,
        };
        let mut s = RecordStore::new(cfg);
        // Fill hash A (target) to per-hash cap (2 holders).
        s.insert_at(nid(1), h(0xAA), 10);
        s.insert_at(nid(2), h(0xAA), 20);
        // Fill hash B with two unrelated records to take the store to
        // the global cap (4 total).
        s.insert_at(nid(3), h(0xBB), 30);
        s.insert_at(nid(4), h(0xBB), 40);
        assert_eq!(s.len(), 4);
        // Insert into hash A from a new publisher. Per-hash cap fires
        // (evicts oldest A holder = nid(1)). Global cap MUST NOT also
        // fire — otherwise hash B's oldest (nid(3) for hash 0xBB)
        // would be wrongly dropped.
        let out = s.insert_at(nid(5), h(0xAA), 50);
        assert_eq!(out, InsertOutcome::Inserted);
        assert_eq!(s.len(), 4, "global count must remain at the cap");
        // Hash A: nid(2) and nid(5); nid(1) was evicted by per-hash cap.
        let a_holders: std::collections::HashSet<_> =
            s.providers_at(&h(0xAA), 0).into_iter().collect();
        assert!(!a_holders.contains(&nid(1)));
        assert!(a_holders.contains(&nid(2)));
        assert!(a_holders.contains(&nid(5)));
        // Hash B: BOTH original holders survive — global LRU did NOT
        // fire on the same insert.
        let b_holders: std::collections::HashSet<_> =
            s.providers_at(&h(0xBB), 0).into_iter().collect();
        assert!(
            b_holders.contains(&nid(3)),
            "hash B's nid(3) must NOT be evicted by the per-hash-cap insert into hash A"
        );
        assert!(b_holders.contains(&nid(4)));
    }

    /// Assert the tri-index invariant the `add_entry`/`remove_entry`
    /// helpers exist to enforce: `global_lru.len()` equals the total
    /// `by_hash` entry count equals the sum of `by_publisher_count`,
    /// every `by_hash` entry has a matching `global_lru` key, and no
    /// empty provider bucket lingers in `by_hash`.
    fn assert_tri_index_consistent(s: &RecordStore) {
        let by_hash_total: usize = s.by_hash.values().map(Vec::len).sum();
        assert_eq!(
            by_hash_total,
            s.global_lru.len(),
            "by_hash total vs global_lru"
        );

        let mut counted: std::collections::HashMap<NodeId, usize> =
            std::collections::HashMap::new();
        for (hash, entries) in &s.by_hash {
            assert!(!entries.is_empty(), "empty provider bucket must be pruned");
            for e in entries {
                assert!(
                    s.global_lru
                        .contains(&(e.receive_us, e.sequence, *hash, e.holder)),
                    "by_hash entry missing from global_lru"
                );
                *counted.entry(e.holder).or_insert(0) += 1;
            }
        }
        assert_eq!(counted, s.by_publisher_count, "by_publisher_count drift");
    }

    /// After a deterministic sequence that fires every mutation kind —
    /// new inserts, per-hash-cap eviction, global-cap eviction, refresh,
    /// and a TTL GC that actually expires a cohort — the three indexes
    /// stay in lockstep. With the mutations hand-rolled at each site the
    /// "stale `by_publisher_count` after eviction" and "two evictions for
    /// one insert" bug classes were defended by convention; routing every
    /// mutation through the two helpers makes this invariant structural.
    /// `assert_tri_index_consistent` runs after each phase, so a path that
    /// updated only one or two of the three indexes would fail here even
    /// though `providers_at` alone might still look right.
    #[test]
    fn tri_index_stays_consistent_across_mixed_operations() {
        let cfg = RecordStoreConfig {
            max_records_per_publisher: 5,
            max_records_global: 8,
            max_providers_per_hash: 3,
            ttl_us: 1_000,
        };
        let mut s = RecordStore::new(cfg);

        // Phase 1 — new inserts: h(1) reaches the per-hash cap (3 holders),
        // and the store reaches the global cap (8 records).
        for (p, hb, ts) in [
            (1u8, 1u8, 100u64),
            (2, 1, 110),
            (3, 1, 120), // h(1) now at the 3-provider cap
            (1, 2, 130),
            (2, 2, 140),
            (3, 2, 150),
            (1, 3, 160),
            (2, 3, 170), // global now at the 8-record cap
        ] {
            assert_eq!(s.insert_at(nid(p), h(hb), ts), InsertOutcome::Inserted);
        }
        assert_eq!(s.len(), 8);
        assert_tri_index_consistent(&s);

        // Phase 2 — per-hash-cap eviction: a 4th holder for h(1) evicts the
        // oldest h(1) holder (nid(1)@100) and nets the global count out, so
        // the global LRU must NOT also fire.
        assert_eq!(s.insert_at(nid(4), h(1), 180), InsertOutcome::Inserted);
        assert_eq!(s.len(), 8, "per-hash eviction must not change global count");
        assert!(
            !s.providers_at(&h(1), 0).contains(&nid(1)),
            "oldest h(1) holder evicted by per-hash cap"
        );
        assert_tri_index_consistent(&s);

        // Phase 3 — global-cap eviction: h(3) is below its per-hash cap, so
        // this insert grows the store; at the global cap it evicts the
        // globally-oldest record.
        assert_eq!(s.insert_at(nid(4), h(3), 190), InsertOutcome::Inserted);
        assert_eq!(s.len(), 8, "global cap holds the store at 8");
        assert_tri_index_consistent(&s);

        // Phase 4 — refresh (remove + re-add, net-zero on counts).
        assert_eq!(s.insert_at(nid(3), h(1), 200), InsertOutcome::Refreshed);
        assert_eq!(s.len(), 8);
        assert_tri_index_consistent(&s);

        // Phase 5 — GC that genuinely expires the oldest cohort. The five
        // records with receive_us 130..=170 (expiry 1130..=1170) are dropped;
        // 180/190/200 (expiry 1180/1190/1200) survive.
        let removed = s.gc(1_175);
        assert_eq!(removed, 5, "GC must drop the expired cohort, not no-op");
        assert_eq!(s.len(), 3);
        assert_tri_index_consistent(&s);

        // Phase 6 — more inserts after GC, including further per-hash
        // evictions on h(1).
        for (p, ts) in [(7u8, 6_000u64), (8, 6_001), (9, 6_002)] {
            assert_eq!(s.insert_at(nid(p), h(1), ts), InsertOutcome::Inserted);
        }
        assert_tri_index_consistent(&s);
    }

    /// GC must terminate and self-repair if a `global_lru` key has no
    /// matching `by_hash` entry (the "heap corruption" case the deleted
    /// in-place refresh branch used to guard). The front-walk must drop the
    /// orphan directly rather than spin on a key `remove_entry` can't clear.
    #[test]
    fn gc_drops_orphan_global_lru_key_without_spinning() {
        let mut cfg = small_cfg();
        cfg.ttl_us = 100;
        let mut s = RecordStore::new(cfg);
        // One real record (expires at 100).
        s.insert_at(nid(1), h(1), 0);
        // Inject an orphaned global_lru key with no by_hash entry.
        s.global_lru.insert((5, 999, h(0xEE), nid(2)));
        // GC past both expiries: the real record is removed via remove_entry,
        // the orphan via the defensive direct drop — both counted, no spin.
        let removed = s.gc(10_000);
        assert_eq!(removed, 2);
        assert!(s.is_empty());
        assert_tri_index_consistent(&s);
    }

    /// `providers_at` scrubs only the expired holders from a hash whose
    /// bucket has a mix of expired and live records: the live holders
    /// survive, the bucket is NOT pruned, and per-publisher counts drop
    /// only for the expired holders. Guards the scrub rewrite from
    /// in-place index-walk to collect-then-`remove_entry`.
    #[test]
    fn providers_at_scrubs_only_expired_holders_in_mixed_bucket() {
        let mut cfg = small_cfg();
        cfg.ttl_us = 1_000;
        cfg.max_providers_per_hash = 10; // keep per-hash eviction out of it
        let mut s = RecordStore::new(cfg);
        s.insert_at(nid(1), h(1), 0); // expiry 1_000
        s.insert_at(nid(2), h(1), 5_000); // expiry 6_000
        s.insert_at(nid(3), h(1), 5_500); // expiry 6_500
        // now_us between the expiries: nid(1) expired, nid(2)/nid(3) live.
        let live: std::collections::HashSet<_> = s.providers_at(&h(1), 2_000).into_iter().collect();
        assert_eq!(live.len(), 2);
        assert!(!live.contains(&nid(1)), "expired holder scrubbed");
        assert!(live.contains(&nid(2)));
        assert!(live.contains(&nid(3)));
        assert_eq!(
            s.publisher_record_count(&nid(1)),
            0,
            "count drops for expired"
        );
        assert_eq!(s.publisher_record_count(&nid(2)), 1, "live count intact");
        assert_eq!(s.len(), 2);
        assert_tri_index_consistent(&s);
    }
}
