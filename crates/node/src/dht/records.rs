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
//! | Per-publisher records | 100,000 | Hard reject — `StoreAck { accepted: false }` |
//! | Per-hash providers | 50 | Evict oldest holder for that hash |
//! | Per-node global records | 1,000,000 | Evict globally-oldest record (only when inserting publisher is below per-publisher cap) |
//! | Record TTL | 1 hour | Receiver-anchored; refreshed on re-publish |
//!
//! The active-staker filter in the handler is the security boundary: a
//! publisher must be a bonded operator to insert at all, so record spam
//! carries a stake cost. The per-publisher cap is a fairness bound on top
//! of that — it keeps one publisher's footprint from dominating a
//! receiver's store, so the global set stays a broad sample rather than
//! one node's catalogue. With the cap in place, the global LRU only fires
//! when *the inserting publisher is below quota* — i.e. a below-quota
//! publisher is pushing the total over capacity, and evicting the
//! globally-oldest record (likely from a publisher who has stopped
//! re-publishing) is the right call.
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
use decdn_protocol::Coverage;
use decdn_protocol::MAX_PROVIDERS_PER_HASH;
use decdn_protocol::dht::Provider;

/// 32-byte content hash. Re-exported from the protocol crate's
/// [`decdn_protocol::ContentHash`] newtype — distinct from [`NodeId`] at the
/// type level, identical (`[u8; 32]`) on the wire.
pub use decdn_protocol::ContentHash as Hash;

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
///
/// `coverage` holds a `Vec`, so this type is `Clone` rather than `Copy` —
/// every former implicit-copy site now takes an explicit `.clone()`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HolderEntry {
    holder: NodeId,
    receive_us: u64,
    sequence: u64,
    expiry_us: u64,
    coverage: Coverage,
}

/// Configuration for the record store (ADR 022 §Content Records and TTL).
#[derive(Debug, Clone)]
pub struct RecordStoreConfig {
    /// `Max records per publisher (per receiver)`. ADR 022 default: 100,000.
    pub max_records_per_publisher: usize,
    /// `Max records per node`. ADR 022 default: 1,000,000.
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
            max_records_per_publisher: 100_000,
            max_records_global: 1_000_000,
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

    /// Global record capacity — the configured
    /// [`RecordStoreConfig::max_records_global`] ceiling. Paired with
    /// [`Self::len`] to report store utilization in the
    /// `admin_v1_status` health view (issue #741).
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.cfg.max_records_global
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
    pub fn insert_at(
        &mut self,
        holder: NodeId,
        hash: Hash,
        coverage: Coverage,
        receive_us: u64,
    ) -> InsertOutcome {
        // Allocate one monotonic sequence per call. Stored on the entry
        // and used as the second component of the `global_lru` key, so
        // wall-clock collisions don't collapse two records onto one
        // BTreeSet slot. Crucially this is NOT mixed into `receive_us`
        // or `expiry_us` — TTL stays exactly `ttl_us` after wall-clock
        // acceptance per ADR 022 line 122.
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);

        // Refresh path: holder already has a record for this hash. Drop
        // the stale tri-index entry and re-add at the new timestamp (and
        // the freshly-reported coverage). `remove_entry` then `add_entry`
        // is net-zero on the per-publisher count and keeps the tri-index
        // update atomic, so there is no hand-rolled in-place mutation (and
        // no `get_mut` corruption branch) to keep in sync.
        if self.remove_entry(holder, hash).is_some() {
            self.add_entry(holder, hash, coverage, receive_us, sequence);
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

        self.add_entry(holder, hash, coverage, receive_us, sequence);
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
    pub fn providers_at(&mut self, hash: &Hash, now_us: u64) -> Vec<Provider> {
        // Scrub expired entries for this hash before returning.
        self.scrub_hash_expired(hash, now_us);
        self.by_hash
            .get(hash)
            .map(|entries| {
                entries
                    .iter()
                    .map(|e| Provider {
                        node: e.holder,
                        coverage: e.coverage.clone(),
                    })
                    .collect()
            })
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
    fn add_entry(
        &mut self,
        holder: NodeId,
        hash: Hash,
        coverage: Coverage,
        receive_us: u64,
        sequence: u64,
    ) {
        let expiry_us = receive_us.saturating_add(self.cfg.ttl_us);
        self.by_hash.entry(hash).or_default().push(HolderEntry {
            holder,
            receive_us,
            sequence,
            expiry_us,
            coverage,
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
        let Some(removed) = entries.get(idx).cloned() else {
            // `position` just yielded `idx`, so `get(idx)` is `Some`
            // except under heap corruption. The two legitimate
            // "not a record" misses (`?` above) returned already, so a
            // `None` here is never a normal miss — surface it loudly.
            // Surface it at its true source so all
            // callers (refresh, evictions, gc, scrub) inherit the error signal. The
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
        // Source every entry-derived key field from `removed` (it equals
        // `holder` by construction — `position` matched on `e.holder ==
        // holder` — but keeping them uniform avoids a future mismatch if
        // the lookup predicate ever changes).
        self.global_lru
            .remove(&(removed.receive_us, removed.sequence, hash, removed.holder));
        Self::decrement_publisher_count(&mut self.by_publisher_count, &removed.holder);
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
mod tests;
