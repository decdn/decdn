use super::*;

fn ob(len: usize) -> Bytes {
    Bytes::from(vec![7u8; len])
}

#[test]
fn a_cached_outboard_is_returned_with_its_origin() {
    let cache = OutboardCache::new(100, 16);
    let h = Hash::new(b"a");
    assert!(cache.get(h, 10).is_none());
    assert!(cache.insert(h, ob(10), 2));
    let hit = cache.get(h, 10).unwrap();
    assert_eq!((hit.bytes.len(), hit.origin_ix), (10, 2));
    assert_eq!(cache.bytes(), 10);
}

#[test]
fn a_hit_of_another_length_is_a_miss_and_drops_the_entry() {
    let cache = OutboardCache::new(100, 16);
    let h = Hash::new(b"a");
    cache.insert(h, ob(10), 0);
    assert!(cache.get(h, 11).is_none());
    assert_eq!(cache.bytes(), 0, "the mismatched entry is dropped");
    assert!(cache.get(h, 10).is_none());
}

#[test]
fn an_insert_past_the_budget_evicts_the_least_recently_used() {
    let cache = OutboardCache::new(100, 16);
    let (a, b, c) = (Hash::new(b"a"), Hash::new(b"b"), Hash::new(b"c"));
    cache.insert(a, ob(40), 0);
    cache.insert(b, ob(40), 0);
    // Touch `a`, so `b` is the least recently used.
    assert!(cache.get(a, 40).is_some());
    cache.insert(c, ob(40), 0);
    assert!(cache.get(a, 40).is_some());
    assert!(
        cache.get(b, 40).is_none(),
        "the least recently used entry goes"
    );
    assert!(cache.get(c, 40).is_some());
    assert_eq!(cache.bytes(), 80);
}

#[test]
fn an_insert_evicts_as_many_entries_as_it_needs() {
    let cache = OutboardCache::new(100, 16);
    let hashes: Vec<Hash> = (0u8..4).map(|i| Hash::new([i])).collect();
    for h in &hashes {
        cache.insert(*h, ob(25), 0);
    }
    let big = Hash::new(b"big");
    assert!(
        cache.insert(big, ob(100), 0),
        "an entry equal to the budget fits"
    );
    assert!(hashes.iter().all(|h| cache.get(*h, 25).is_none()));
    assert_eq!(cache.bytes(), 100);
}

#[test]
fn an_empty_outboard_is_not_cached() {
    let cache = OutboardCache::new(100, 16);
    let h = Hash::new(b"a");
    assert!(cache.insert(h, Bytes::new(), 0));
    assert!(cache.get(h, 0).is_none());
}

#[test]
fn the_entry_count_is_bounded() {
    let cache = OutboardCache::new(1_000, 2);
    let (a, b, c) = (Hash::new(b"a"), Hash::new(b"b"), Hash::new(b"c"));
    cache.insert(a, ob(1), 0);
    cache.insert(b, ob(1), 0);
    cache.insert(c, ob(1), 0);
    assert!(cache.get(a, 1).is_none(), "the oldest entry goes");
    assert!(cache.get(b, 1).is_some() && cache.get(c, 1).is_some());
    assert_eq!(cache.bytes(), 2);
}

#[test]
fn an_outboard_larger_than_the_budget_is_not_cached() {
    let cache = OutboardCache::new(100, 16);
    let (a, big) = (Hash::new(b"a"), Hash::new(b"big"));
    cache.insert(a, ob(40), 0);
    assert!(!cache.insert(big, ob(101), 0));
    assert!(cache.get(big, 101).is_none());
    assert!(
        cache.get(a, 40).is_some(),
        "a refused insert evicts nothing"
    );
}

#[test]
fn a_reinsert_replaces_without_double_counting() {
    let cache = OutboardCache::new(100, 16);
    let h = Hash::new(b"a");
    cache.insert(h, ob(40), 0);
    cache.insert(h, ob(30), 1);
    assert_eq!(cache.bytes(), 30);
    assert_eq!(cache.get(h, 30).unwrap().origin_ix, 1);
}

#[test]
fn evict_if_same_drops_only_the_copy_that_failed() {
    let cache = OutboardCache::new(100, 16);
    let h = Hash::new(b"a");
    let first = ob(40);
    cache.insert(h, first.clone(), 0);
    // Another draw re-read a fresh copy after the first one failed.
    let fresh = ob(40);
    cache.insert(h, fresh.clone(), 1);
    cache.evict_if_same(h, &first);
    assert!(cache.get(h, 40).is_some(), "the fresh copy stays");
    cache.evict_if_same(h, &fresh);
    assert!(cache.get(h, 40).is_none());
    assert_eq!(cache.bytes(), 0);
}
