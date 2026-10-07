use super::*;

fn nid(b: u8) -> NodeId {
    NodeId::from_bytes([b; 32])
}
fn h(b: u8) -> Hash {
    Hash::from_bytes([b; 32])
}
/// A `Provider` for `node` with the same one-block-full `Coverage` used
/// by every `insert_at` call in this module's tests.
fn provider(node: NodeId) -> Provider {
    Provider {
        node,
        coverage: Coverage::full(1),
    }
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
    let out = s.insert_at(nid(1), h(1), Coverage::full(1), 0);
    assert_eq!(out, InsertOutcome::Inserted);
    assert!(out.accepted());
    assert_eq!(s.len(), 1);
    assert_eq!(s.publisher_record_count(&nid(1)), 1);
    assert_eq!(s.providers_at(&h(1), 0), vec![provider(nid(1))]);
}

#[test]
fn re_insert_same_holder_hash_refreshes_not_duplicates() {
    let mut s = RecordStore::new(small_cfg());
    s.insert_at(nid(1), h(1), Coverage::full(1), 100);
    let out = s.insert_at(nid(1), h(1), Coverage::full(1), 200);
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
        assert_eq!(
            s.insert_at(nid(7), h(i), Coverage::full(1), i.into()),
            InsertOutcome::Inserted
        );
    }
    // Fourth distinct hash from publisher 7 must reject.
    let out = s.insert_at(nid(7), h(4), Coverage::full(1), 10);
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
        s.insert_at(nid(7), h(i), Coverage::full(1), i.into());
    }
    // Publisher 8 is below their cap and should be admitted.
    assert_eq!(
        s.insert_at(nid(8), h(4), Coverage::full(1), 100),
        InsertOutcome::Inserted
    );
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
        s.insert_at(nid(p), h(h_byte), Coverage::full(1), ts);
    }
    assert_eq!(s.len(), 6);
    // Insert from publisher 3 (below their cap) — should evict
    // the globally-oldest (publisher 1 / hash 1, receive_us 10).
    let out = s.insert_at(nid(3), h(7), Coverage::full(1), 70);
    assert_eq!(out, InsertOutcome::Inserted);
    assert_eq!(s.len(), 6);
    assert!(s.providers_at(&h(1), 0).is_empty(), "oldest hash evicted");
    assert_eq!(s.providers_at(&h(7), 0), vec![provider(nid(3))]);
    // Publisher 1's count dropped by 1.
    assert_eq!(s.publisher_record_count(&nid(1)), 2);
}

#[test]
fn ttl_expiry_drops_record_on_providers_at() {
    let mut s = RecordStore::new(small_cfg());
    s.insert_at(nid(1), h(1), Coverage::full(1), 1_000);
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
        s.insert_at(nid(i), h(i), Coverage::full(1), 0);
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
        assert_eq!(
            s.insert_at(nid(i), h(1), Coverage::full(1), i.into()),
            InsertOutcome::Inserted
        );
    }
    assert_eq!(s.providers_at(&h(1), 0).len(), 4);
    // Fifth holder for the same hash: oldest (publisher 1) should
    // be evicted.
    assert_eq!(
        s.insert_at(nid(5), h(1), Coverage::full(1), 100),
        InsertOutcome::Inserted
    );
    let providers: std::collections::HashSet<NodeId> = s
        .providers_at(&h(1), 0)
        .into_iter()
        .map(|p| p.node)
        .collect();
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
    assert_eq!(d.max_records_per_publisher, 100_000);
    assert_eq!(d.max_records_global, 1_000_000);
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
        assert_eq!(
            s.insert_at(nid(1), h(i), Coverage::full(1), i.into()),
            InsertOutcome::Inserted
        );
    }
    // Both global (3 records) and per-publisher (3 from nid(1))
    // caps are now at the threshold. A 4th Store from nid(1)
    // must hard-reject without touching the global LRU.
    let len_before = s.len();
    let count_before = s.publisher_record_count(&nid(1));
    let out = s.insert_at(nid(1), h(99), Coverage::full(1), 100);
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
        s.insert_at(nid(1), h(i), Coverage::full(1), i.into());
    }
    assert_eq!(s.publisher_record_count(&nid(1)), 3);
    // Refresh hash 1 — still 3.
    let out = s.insert_at(nid(1), h(1), Coverage::full(1), 100);
    assert_eq!(out, InsertOutcome::Refreshed);
    assert_eq!(s.publisher_record_count(&nid(1)), 3);
    // A NEW hash from publisher 1 still rejects.
    assert_eq!(
        s.insert_at(nid(1), h(99), Coverage::full(1), 200),
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
        s.insert_at(NodeId::from_bytes(holder), h(0xEE), Coverage::full(1), 500);
    }
    // Insert the record under test at wall-clock 500.
    s.insert_at(nid(0xCC), h(0xFF), Coverage::full(1), 500);
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
    s.insert_at(nid(1), h(0xAA), Coverage::full(1), 10);
    s.insert_at(nid(2), h(0xAA), Coverage::full(1), 20);
    // Fill hash B with two unrelated records to take the store to
    // the global cap (4 total).
    s.insert_at(nid(3), h(0xBB), Coverage::full(1), 30);
    s.insert_at(nid(4), h(0xBB), Coverage::full(1), 40);
    assert_eq!(s.len(), 4);
    // Insert into hash A from a new publisher. Per-hash cap fires
    // (evicts oldest A holder = nid(1)). Global cap MUST NOT also
    // fire — otherwise hash B's oldest (nid(3) for hash 0xBB)
    // would be wrongly dropped.
    let out = s.insert_at(nid(5), h(0xAA), Coverage::full(1), 50);
    assert_eq!(out, InsertOutcome::Inserted);
    assert_eq!(s.len(), 4, "global count must remain at the cap");
    // Hash A: nid(2) and nid(5); nid(1) was evicted by per-hash cap.
    let a_holders: std::collections::HashSet<NodeId> = s
        .providers_at(&h(0xAA), 0)
        .into_iter()
        .map(|p| p.node)
        .collect();
    assert!(!a_holders.contains(&nid(1)));
    assert!(a_holders.contains(&nid(2)));
    assert!(a_holders.contains(&nid(5)));
    // Hash B: BOTH original holders survive — global LRU did NOT
    // fire on the same insert.
    let b_holders: std::collections::HashSet<NodeId> = s
        .providers_at(&h(0xBB), 0)
        .into_iter()
        .map(|p| p.node)
        .collect();
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

    let mut counted: std::collections::HashMap<NodeId, usize> = std::collections::HashMap::new();
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
        assert_eq!(
            s.insert_at(nid(p), h(hb), Coverage::full(1), ts),
            InsertOutcome::Inserted
        );
    }
    assert_eq!(s.len(), 8);
    assert_tri_index_consistent(&s);

    // Phase 2 — per-hash-cap eviction: a 4th holder for h(1) evicts the
    // oldest h(1) holder (nid(1)@100) and nets the global count out, so
    // the global LRU must NOT also fire.
    assert_eq!(
        s.insert_at(nid(4), h(1), Coverage::full(1), 180),
        InsertOutcome::Inserted
    );
    assert_eq!(s.len(), 8, "per-hash eviction must not change global count");
    assert!(
        !s.providers_at(&h(1), 0).iter().any(|p| p.node == nid(1)),
        "oldest h(1) holder evicted by per-hash cap"
    );
    assert_tri_index_consistent(&s);

    // Phase 3 — global-cap eviction: h(3) is below its per-hash cap, so
    // this insert grows the store; at the global cap it evicts the
    // globally-oldest record.
    assert_eq!(
        s.insert_at(nid(4), h(3), Coverage::full(1), 190),
        InsertOutcome::Inserted
    );
    assert_eq!(s.len(), 8, "global cap holds the store at 8");
    assert_tri_index_consistent(&s);

    // Phase 4 — refresh (remove + re-add, net-zero on counts).
    assert_eq!(
        s.insert_at(nid(3), h(1), Coverage::full(1), 200),
        InsertOutcome::Refreshed
    );
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
        assert_eq!(
            s.insert_at(nid(p), h(1), Coverage::full(1), ts),
            InsertOutcome::Inserted
        );
    }
    assert_tri_index_consistent(&s);
}

/// GC must terminate and self-repair if a `global_lru` key has no
/// matching `by_hash` entry (the "heap corruption" case). The front-walk
/// must drop the orphan directly rather than spin on a key `remove_entry`
/// can't clear.
#[test]
fn gc_drops_orphan_global_lru_key_without_spinning() {
    let mut cfg = small_cfg();
    cfg.ttl_us = 100;
    let mut s = RecordStore::new(cfg);
    // One real record (expires at 100).
    s.insert_at(nid(1), h(1), Coverage::full(1), 0);
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
/// only for the expired holders. Guards the collect-then-`remove_entry`
/// scrub against regressing to an in-place index-walk.
#[test]
fn providers_at_scrubs_only_expired_holders_in_mixed_bucket() {
    let mut cfg = small_cfg();
    cfg.ttl_us = 1_000;
    cfg.max_providers_per_hash = 10; // keep per-hash eviction out of it
    let mut s = RecordStore::new(cfg);
    s.insert_at(nid(1), h(1), Coverage::full(1), 0); // expiry 1_000
    s.insert_at(nid(2), h(1), Coverage::full(1), 5_000); // expiry 6_000
    s.insert_at(nid(3), h(1), Coverage::full(1), 5_500); // expiry 6_500
    // now_us between the expiries: nid(1) expired, nid(2)/nid(3) live.
    let live: std::collections::HashSet<NodeId> = s
        .providers_at(&h(1), 2_000)
        .into_iter()
        .map(|p| p.node)
        .collect();
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
