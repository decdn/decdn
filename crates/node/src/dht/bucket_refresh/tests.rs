use super::*;
use crate::dht::routing::xor_distance;

fn nid(b: u8) -> NodeId {
    NodeId::from_bytes([b; 32])
}

/// `random_target_in_bucket` MUST produce a `NodeId` that XOR-
/// distances to `self_id` into the requested bucket. The exact
/// bucket assignment is what makes the refreshed peer respond with
/// entries in that distance shell.
#[test]
fn random_target_lands_in_requested_bucket() {
    let self_id = nid(0x00);
    for bucket in [0_usize, 1, 50, 100, 200, 254, 255] {
        for _ in 0..32 {
            let target = random_target_in_bucket(&self_id, bucket);
            let d = xor_distance(&self_id, &target);
            let got = bucket_index(&d);
            assert_eq!(
                got,
                Some(bucket),
                "target {target:?} landed in bucket {got:?}, wanted {bucket}"
            );
        }
    }
}

#[test]
fn all_non_empty_picks_is_empty_for_empty_table() {
    let table = RoutingTable::new(nid(0));
    assert!(all_non_empty_picks(&table).is_empty());
}

#[test]
fn all_non_empty_picks_returns_one_entry_per_non_empty_bucket() {
    let self_id = nid(0);
    let mut table = RoutingTable::new(self_id);
    // Insert two peers in two different buckets.
    let p1 = {
        // Force bucket 255 (high-bit set).
        let mut id = [0u8; 32];
        id[0] = 0x80;
        NodeId::from_bytes(id)
    };
    let p2 = {
        // Force bucket 0 (only low-bit differs).
        let mut id = [0u8; 32];
        id[31] = 0x01;
        NodeId::from_bytes(id)
    };
    table.insert(p1);
    table.insert(p2);

    let picks = all_non_empty_picks(&table);
    // One pick per non-empty bucket — exactly two here. ADR 022
    // requires *every* populated bucket be refreshed each tick.
    assert_eq!(picks.len(), 2);
    let mut indexes: Vec<usize> = picks.iter().map(|p| p.bucket_index).collect();
    indexes.sort_unstable();
    assert_eq!(indexes, vec![0, 255]);
    // The pick's peer is the bucket's most-recently-seen entry
    // (the only peer in each single-peer bucket here).
    for pick in &picks {
        assert!(pick.peer == p1 || pick.peer == p2);
    }
}

/// Single-bucket helper from the test-only API still works for the
/// `bucket_iter` / `random_target_in_bucket` integration.
#[test]
fn pick_bucket_for_single_bucket_returns_a_target_in_range() {
    let self_id = nid(0);
    let mut table = RoutingTable::new(self_id);
    let p = {
        let mut id = [0u8; 32];
        id[0] = 0x80;
        NodeId::from_bytes(id)
    };
    table.insert(p);
    let pick = pick_bucket(&table, 255).expect("non-empty bucket 255");
    assert_eq!(pick.bucket_index, 255);
    let d = xor_distance(&self_id, &pick.target);
    assert_eq!(bucket_index(&d), Some(255));
}
