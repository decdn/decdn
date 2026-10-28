use super::*;
use crate::dht::staker_set::ConfigStakerSet;

fn id(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

/// A DHT `peer` field and a transport `peer` field render the same
/// string, so a log or span query joins them. Pinned against iroh, whose
/// encoding an upgrade could change.
#[test]
fn node_id_display_matches_iroh_public_key() {
    let pk = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    assert_eq!(
        NodeId::from_bytes(*pk.as_bytes()).to_string(),
        pk.to_string()
    );
}

#[test]
fn xor_distance_to_self_is_zero() {
    let a = id(42);
    let d = xor_distance(&a, &a);
    assert_eq!(d, [0u8; 32]);
}

#[test]
fn xor_distance_is_symmetric() {
    let a = id(1);
    let b = id(2);
    assert_eq!(xor_distance(&a, &b), xor_distance(&b, &a));
}

#[test]
fn bucket_index_zero_is_none() {
    assert_eq!(bucket_index(&[0u8; 32]), None);
}

#[test]
fn bucket_index_max_distance_is_top_bucket() {
    // High bit of first byte set -> bit_from_msb = 0 -> bucket 255 =
    // most-distant peers (zero shared prefix bits with self_id).
    let mut d = [0u8; 32];
    d[0] = 0x80;
    assert_eq!(bucket_index(&d), Some(255));
}

#[test]
fn bucket_index_min_nonzero_is_bucket_zero() {
    // Low bit of last byte set -> bit_from_msb = 255 -> bucket 0 =
    // closest peers (only the lowest bit of distance differs).
    let mut d = [0u8; 32];
    d[31] = 0x01;
    assert_eq!(bucket_index(&d), Some(0));
}

#[test]
fn insert_then_contains() {
    let mut rt = RoutingTable::new(id(0));
    assert!(rt.is_empty());
    let p = id(1);
    assert!(rt.insert(p));
    assert!(rt.contains(&p));
    assert_eq!(rt.len(), 1);
}

#[test]
fn insert_self_id_is_noop() {
    let self_id = id(7);
    let mut rt = RoutingTable::new(self_id);
    assert!(!rt.insert(self_id));
    assert!(rt.is_empty());
    assert!(!rt.contains(&self_id));
}

#[test]
fn duplicate_insert_refreshes_recency_without_growing() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    let p1 = id_in_bucket(&self_id, 100, 1);
    let p2 = id_in_bucket(&self_id, 100, 2);
    assert!(rt.insert(p1));
    assert!(rt.insert(p2));
    // Re-insert p1 — should be a refresh (false), not a new entry.
    assert!(!rt.insert(p1));
    assert_eq!(rt.len(), 2);
}

#[test]
fn bucket_overflow_evicts_least_recently_seen() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    // Fill bucket 100 to K, all distinct salts so each maps to the same
    // bucket-100 prefix but is a distinct NodeId.
    for salt in 1..=K_BUCKET_SIZE {
        let salt_u8 = u8::try_from(salt).unwrap();
        assert!(rt.insert(id_in_bucket(&self_id, 100, salt_u8)));
    }
    assert_eq!(rt.len(), K_BUCKET_SIZE);

    // Inserting one more must evict the LRU (salt=1) and append the
    // newcomer.
    let newcomer = id_in_bucket(&self_id, 100, 99);
    assert!(rt.insert(newcomer));
    assert_eq!(rt.len(), K_BUCKET_SIZE);
    assert!(rt.contains(&newcomer));
    assert!(!rt.contains(&id_in_bucket(&self_id, 100, 1)));
    // salt=2 was second-oldest — should still be present.
    assert!(rt.contains(&id_in_bucket(&self_id, 100, 2)));
}

/// The bucket-100 ids salted `salts`, as a staker set.
fn staked_in_bucket_100(self_id: &NodeId, salts: impl IntoIterator<Item = u8>) -> ConfigStakerSet {
    ConfigStakerSet::new(
        salts
            .into_iter()
            .map(|salt| id_in_bucket(self_id, 100, salt))
            .collect(),
    )
}

/// #2708: the routing pool holds bonded operators only, so a peer the staker
/// set does not hold never enters the table.
#[test]
fn insert_active_refuses_an_unbonded_peer() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    let stakers = staked_in_bucket_100(&self_id, [1]);
    let now = Instant::now();

    assert!(!rt.insert_active(id_in_bucket(&self_id, 100, 2), &stakers, now));
    assert!(rt.is_empty());
    assert!(rt.insert_active(id_in_bucket(&self_id, 100, 1), &stakers, now));
    assert_eq!(rt.len(), 1);
}

/// #2708: unbonded peers cannot displace the bonded entries of a full bucket.
#[test]
fn unbonded_peers_do_not_displace_bonded_entries() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    let k = u8::try_from(K_BUCKET_SIZE).unwrap();
    let stakers = staked_in_bucket_100(&self_id, 1..=k);
    let now = Instant::now();
    for salt in 1..=k {
        assert!(rt.insert_active(id_in_bucket(&self_id, 100, salt), &stakers, now));
    }

    for salt in 100..200 {
        assert!(!rt.insert_active(id_in_bucket(&self_id, 100, salt), &stakers, now));
    }

    assert_eq!(rt.len(), K_BUCKET_SIZE);
    for salt in 1..=k {
        assert!(rt.contains(&id_in_bucket(&self_id, 100, salt)));
    }
}

/// #2708: a full bucket evicts an entry that has unbonded since it entered,
/// then a cooling entry, before the least-recently-seen bonded one.
#[test]
fn a_full_bucket_evicts_an_unbonded_then_a_cooling_entry_first() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    let k = u8::try_from(K_BUCKET_SIZE).unwrap();
    let now = Instant::now();
    for salt in 1..=k {
        rt.insert(id_in_bucket(&self_id, 100, salt));
    }
    // Salt 7 unbonded; salt 9's dial timed out.
    let stakers = staked_in_bucket_100(&self_id, (1..=k).chain([50, 51]).filter(|s| *s != 7));
    rt.mark_unreachable(id_in_bucket(&self_id, 100, 9), now);

    assert!(rt.insert_active(id_in_bucket(&self_id, 100, 50), &stakers, now));
    assert!(!rt.contains(&id_in_bucket(&self_id, 100, 7)));
    assert!(rt.contains(&id_in_bucket(&self_id, 100, 1)));

    assert!(rt.insert_active(id_in_bucket(&self_id, 100, 51), &stakers, now));
    assert!(!rt.contains(&id_in_bucket(&self_id, 100, 9)));
    assert!(rt.contains(&id_in_bucket(&self_id, 100, 1)));
}

/// #2708: the closest-peer reads skip an entry that has unbonded since it
/// entered the table, and still fill `n` from the bonded ones behind it.
#[test]
fn closest_active_skips_unbonded_entries() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    for salt in 1..=5 {
        rt.insert(id_in_bucket(&self_id, 100, salt));
    }
    let stakers = staked_in_bucket_100(&self_id, [2, 4, 5]);
    let target = id_in_bucket(&self_id, 100, 1);

    let closest = rt.closest_active(target.as_bytes(), 2, &stakers);
    assert_eq!(closest.len(), 2);
    assert!(closest.iter().all(|p| stakers.is_active(p)));
    assert_eq!(
        rt.closest_active_unbounded(target.as_bytes(), 10, &stakers)
            .len(),
        3
    );
}

#[test]
fn refresh_then_overflow_preserves_refreshed_peer() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    for salt in 1..=K_BUCKET_SIZE {
        rt.insert(id_in_bucket(&self_id, 100, u8::try_from(salt).unwrap()));
    }
    // Refresh the oldest entry — should move to MRU and be safe from
    // the next overflow.
    rt.insert(id_in_bucket(&self_id, 100, 1));
    let newcomer = id_in_bucket(&self_id, 100, 99);
    rt.insert(newcomer);
    assert!(rt.contains(&id_in_bucket(&self_id, 100, 1)));
    // salt=2 is now the LRU and should have been evicted.
    assert!(!rt.contains(&id_in_bucket(&self_id, 100, 2)));
}

#[test]
fn remove_returns_true_on_hit_false_on_miss() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    let p = id_in_bucket(&self_id, 50, 1);
    rt.insert(p);
    assert!(rt.remove(&p));
    assert!(!rt.remove(&p));
    assert!(rt.is_empty());
}

#[test]
fn remove_self_id_is_noop() {
    let self_id = id(7);
    let mut rt = RoutingTable::new(self_id);
    assert!(!rt.remove(&self_id));
}

#[test]
fn closest_returns_distance_sorted_results() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    let target = id_in_bucket(&self_id, 255, 0);
    // Populate buckets 0, 100, 200, 255 with one peer each.
    for b in [0_usize, 100, 200, 255] {
        rt.insert(id_in_bucket(&self_id, b, 0));
    }
    let closest = rt.closest(target.as_bytes(), 4);
    assert_eq!(closest.len(), 4);
    // Assert the whole vector is monotonically non-decreasing by XOR
    // distance. A bug that returned "first and last correct, middle
    // scrambled" would pass an endpoints-only check but fail this
    // walk over consecutive pairs.
    let dists: Vec<_> = closest.iter().map(|p| xor_distance(p, &target)).collect();
    for window in dists.windows(2) {
        assert!(
            window[0] <= window[1],
            "closest() not distance-sorted: {:?} > {:?} in {dists:?}",
            window[0],
            window[1]
        );
    }
    // And the first is strictly closer than the last (proves the
    // monotonic check above isn't vacuously satisfied by all-equal
    // distances).
    let d_first = dists.first().expect("len >= 1");
    let d_last = dists.last().expect("len >= 1");
    assert!(d_first < d_last, "first = {d_first:?} >= last = {d_last:?}");
}

#[test]
fn closest_returns_no_more_than_n_or_table_size() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    for b in 0..30_usize {
        rt.insert(id_in_bucket(&self_id, b, 0));
    }
    assert_eq!(rt.closest(id(0xFF).as_bytes(), 10).len(), 10);
    // Cap at K_BUCKET_SIZE even when caller asks for more.
    assert_eq!(rt.closest(id(0xFF).as_bytes(), 1000).len(), K_BUCKET_SIZE);
}

#[test]
fn closest_on_empty_table_is_empty() {
    let rt = RoutingTable::new(id(0));
    assert!(rt.closest(id(0xFF).as_bytes(), 10).is_empty());
}

/// ADR 022 §STORE Flow step 1 requires publishing to **K+3 = 23**
/// closest nodes. The wire-capped [`RoutingTable::closest`] tops
/// out at `K_BUCKET_SIZE = 20`; the unbounded variant is what
/// the republish path uses to honour the K+3 contract.
#[test]
fn closest_unbounded_returns_more_than_k_when_requested() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    for b in 0..30_usize {
        rt.insert(id_in_bucket(&self_id, b, 0));
    }
    // The bounded variant caps at K=20 (wire `MAX_CLOSER_NODES`).
    assert_eq!(rt.closest(id(0xFF).as_bytes(), 1000).len(), K_BUCKET_SIZE);
    // The unbounded variant returns up to `n` regardless.
    assert_eq!(rt.closest_unbounded(id(0xFF).as_bytes(), 23).len(), 23);
    // And caps at table size when `n > len`.
    assert_eq!(rt.closest_unbounded(id(0xFF).as_bytes(), 1000).len(), 30);
}

#[test]
fn iter_peers_returns_every_entry() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    let p1 = id_in_bucket(&self_id, 10, 0);
    let p2 = id_in_bucket(&self_id, 200, 0);
    rt.insert(p1);
    rt.insert(p2);
    let collected: std::collections::HashSet<_> = rt.iter_peers().copied().collect();
    assert!(collected.contains(&p1));
    assert!(collected.contains(&p2));
    assert_eq!(collected.len(), 2);
}

#[test]
fn non_empty_bucket_fills_empty_table_is_empty() {
    let rt = RoutingTable::new(id(0));
    assert!(rt.non_empty_bucket_fills().is_empty());
}

#[test]
fn non_empty_bucket_fills_reports_counts_ascending_and_omits_empty() {
    let self_id = id(0);
    let mut rt = RoutingTable::new(self_id);
    // Three distinct peers in bucket 10 (salts vary the last byte;
    // bucket >= 8 keeps the selecting bit out of that byte) and one in
    // bucket 200. All 254 other buckets stay empty and must be omitted.
    rt.insert(id_in_bucket(&self_id, 10, 0));
    rt.insert(id_in_bucket(&self_id, 10, 1));
    rt.insert(id_in_bucket(&self_id, 10, 2));
    rt.insert(id_in_bucket(&self_id, 200, 0));

    let fills = rt.non_empty_bucket_fills();
    // Only the two populated buckets, ascending by index, with real counts.
    assert_eq!(fills, vec![(10, 3), (200, 1)]);
}

#[test]
fn mark_unreachable_cools_a_peer_until_the_cooldown_lapses() {
    let mut t = RoutingTable::new(id(0));
    let now = Instant::now();
    t.insert(id(1));
    assert!(t.mark_unreachable(id(1), now));
    assert!(t.is_cooling(&id(1), now));
    assert!(t.is_cooling(
        &id(1),
        now + DIAL_FAILURE_COOLDOWN.saturating_sub(Duration::from_millis(1))
    ));
    assert!(!t.is_cooling(&id(1), now + DIAL_FAILURE_COOLDOWN));
    assert_eq!(t.cooling_peers(now), HashSet::from([id(1)]));

    // The lapsed snapshot sweeps the entry.
    assert!(t.cooling_peers(now + DIAL_FAILURE_COOLDOWN).is_empty());
    assert!(!t.is_cooling(&id(1), now));
    assert!(
        t.contains(&id(1)),
        "the sweep leaves the peer in its bucket"
    );
}

#[test]
fn a_second_dial_timeout_extends_the_skip_from_its_own_instant() {
    let mut t = RoutingTable::new(id(0));
    t.insert(id(1));
    let first = Instant::now();
    let second = first + Duration::from_secs(100);
    assert!(t.mark_unreachable(id(1), first));
    assert!(t.mark_unreachable(id(1), second));
    assert!(t.is_cooling(&id(1), first + DIAL_FAILURE_COOLDOWN));
    assert!(!t.is_cooling(&id(1), second + DIAL_FAILURE_COOLDOWN));
}

#[test]
fn a_cooling_peer_stays_in_the_table_and_in_closest() {
    let mut t = RoutingTable::new(id(0));
    t.insert(id(1));
    t.insert(id(2));
    let now = Instant::now();
    assert!(t.mark_unreachable(id(1), now));
    assert!(t.contains(&id(1)));
    assert_eq!(t.len(), 2);
    let closest = t.closest(id(1).as_bytes(), 2);
    assert_eq!(
        closest,
        vec![id(1), id(2)],
        "closest() does not apply the skip"
    );
}

#[test]
fn an_insert_or_refresh_of_a_cooling_peer_ends_the_skip() {
    let mut t = RoutingTable::new(id(0));
    t.insert(id(1));
    let now = Instant::now();
    assert!(t.mark_unreachable(id(1), now));
    assert!(!t.insert(id(1)), "a refresh, not a fresh insert");
    assert!(!t.is_cooling(&id(1), now));
}

#[test]
fn a_removed_peer_is_no_longer_cooling() {
    let mut t = RoutingTable::new(id(0));
    t.insert(id(1));
    let now = Instant::now();
    assert!(t.mark_unreachable(id(1), now));
    assert!(t.remove(&id(1)));
    assert!(!t.is_cooling(&id(1), now));
    assert!(t.cooling_peers(now).is_empty());
}

/// A lookup dials peers that a `closer_nodes` answer named, and such a peer
/// need not be in a bucket of this table. Its dial failure is kept all the
/// same, so the next lookup that is referred to it skips it.
#[test]
fn a_referred_peer_outside_the_table_is_marked_too() {
    let mut t = RoutingTable::new(id(0));
    let now = Instant::now();
    assert!(!t.contains(&id(7)));
    assert!(t.mark_unreachable(id(7), now));
    assert!(t.is_cooling(&id(7), now));
    assert_eq!(t.cooling_peers(now), HashSet::from([id(7)]));
    assert!(!t.contains(&id(7)), "marking does not insert the peer");
    assert!(t.cooling_peers(now + DIAL_FAILURE_COOLDOWN).is_empty());
}

#[test]
fn mark_unreachable_ignores_the_self_id() {
    let mut t = RoutingTable::new(id(0));
    let now = Instant::now();
    assert!(!t.mark_unreachable(id(0), now));
    assert!(!t.is_cooling(&id(0), now));
}
