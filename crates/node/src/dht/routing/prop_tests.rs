use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use super::*;

/// Strategy yielding a uniformly-random `NodeId` over the full 256-bit space.
fn node_id() -> impl Strategy<Value = NodeId> {
    proptest::array::uniform32(any::<u8>()).prop_map(NodeId::from_bytes)
}

/// Independent leading-zero-bit count over a big-endian 256-bit value,
/// computed without reference to `bucket_index`'s arithmetic so the two
/// cross-check each other. Returns `KEYSPACE_BITS` for the all-zero input.
fn leading_zero_bits(d: &[u8; NODE_ID_LEN]) -> usize {
    let mut count = 0;
    for byte in d {
        if *byte == 0 {
            count += 8;
        } else {
            count += byte.leading_zeros() as usize;
            break;
        }
    }
    count
}

/// Big-endian 256-bit addition over two distance vectors. Returns the sum
/// truncated to 256 bits plus an overflow flag. Used to check the triangle
/// inequality on the integer interpretation of the XOR metric.
fn be_add(a: &[u8; NODE_ID_LEN], b: &[u8; NODE_ID_LEN]) -> ([u8; NODE_ID_LEN], bool) {
    let mut out = [0u8; NODE_ID_LEN];
    let mut carry: u16 = 0;
    for i in (0..NODE_ID_LEN).rev() {
        let av = a.get(i).copied().unwrap_or_default();
        let bv = b.get(i).copied().unwrap_or_default();
        let sum = u16::from(av) + u16::from(bv) + carry;
        if let Some(o) = out.get_mut(i) {
            *o = u8::try_from(sum & 0xff).unwrap_or_default();
        }
        carry = sum >> 8;
    }
    (out, carry != 0)
}

/// Assert a `closest` / `closest_unbounded` result is correctly sized,
/// distance-sorted, deduplicated, a subset of `held`, and genuinely the
/// nearest peers (every excluded peer is no closer than the farthest
/// returned). Shared by both variants so a regression in either — sorting,
/// dedup, or selection — is caught identically rather than only via a size
/// check on the unbounded path.
fn check_closest_result(
    res: &[NodeId],
    held: &BTreeSet<NodeId>,
    target: &[u8; NODE_ID_LEN],
    expected_len: usize,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(res.len(), expected_len);

    let mut returned: BTreeSet<NodeId> = BTreeSet::new();
    for p in res {
        prop_assert!(held.contains(p), "closest returned a peer not in the table");
        prop_assert!(returned.insert(*p), "closest returned a duplicate");
    }

    // Distance-sorted (non-decreasing) by XOR distance to `target`.
    let dists: Vec<[u8; NODE_ID_LEN]> = res
        .iter()
        .map(|p| xor_distance_bytes(p.as_bytes(), target))
        .collect();
    for w in dists.windows(2) {
        if let (Some(x), Some(y)) = (w.first(), w.get(1)) {
            prop_assert!(x <= y, "closest not distance-sorted");
        }
    }

    // Selection correctness: every peer NOT returned is at least as far as
    // the farthest returned peer. Only constrains when some peers were
    // excluded (the table held more than the cap).
    if let Some(farthest) = dists.last() {
        for p in held {
            if !returned.contains(p) {
                let dp = xor_distance_bytes(p.as_bytes(), target);
                prop_assert!(
                    dp >= *farthest,
                    "an excluded peer was closer than a returned one"
                );
            }
        }
    }
    Ok(())
}

proptest! {
    /// `xor_distance` is a valid metric: reflexive, symmetric, discerns
    /// identity, satisfies the XOR self-cancel identity
    /// `d(a,b) ⊕ d(b,c) = d(a,c)`, and the triangle inequality on the
    /// integer interpretation `d(a,c) <= d(a,b) + d(b,c)`.
    #[test]
    fn xor_distance_is_a_valid_metric(a in node_id(), b in node_id(), c in node_id()) {
        // Reflexivity.
        prop_assert_eq!(xor_distance(&a, &a), [0u8; NODE_ID_LEN]);
        // Symmetry.
        prop_assert_eq!(xor_distance(&a, &b), xor_distance(&b, &a));
        // Identity of indiscernibles: distance is zero iff the ids are equal.
        prop_assert_eq!(xor_distance(&a, &b) == [0u8; NODE_ID_LEN], a == b);

        let dab = xor_distance(&a, &b);
        let dbc = xor_distance(&b, &c);
        let dac = xor_distance(&a, &c);

        // XOR self-cancel: d(a,b) ⊕ d(b,c) == d(a,c).
        let mut cancelled = [0u8; NODE_ID_LEN];
        for i in 0..NODE_ID_LEN {
            if let (Some(o), Some(p), Some(q)) =
                (cancelled.get_mut(i), dab.get(i), dbc.get(i))
            {
                *o = p ^ q;
            }
        }
        prop_assert_eq!(cancelled, dac);

        // Triangle inequality on the big-endian integer interpretation.
        // Array `Ord` compares element-wise from index 0 (most significant),
        // i.e. exactly big-endian integer ordering. If the 256-bit sum
        // overflows it necessarily exceeds the 256-bit `dac`, so the
        // inequality holds trivially.
        let (sum, overflow) = be_add(&dab, &dbc);
        prop_assert!(overflow || dac <= sum);
    }
}

proptest! {
    /// `bucket_index` returns `None` exactly for equal ids and otherwise a
    /// value in `[0, KEYSPACE_BITS)` that agrees with an independent
    /// leading-zero count of the distance.
    #[test]
    fn bucket_index_matches_independent_prefix_count(a in node_id(), b in node_id()) {
        let d = xor_distance(&a, &b);
        match bucket_index(&d) {
            None => prop_assert_eq!(a, b),
            Some(idx) => {
                prop_assert!(idx < KEYSPACE_BITS);
                let lz = leading_zero_bits(&d);
                prop_assert_eq!(idx, KEYSPACE_BITS - 1 - lz);
            }
        }
    }
}

proptest! {
    /// Under an arbitrary insert/remove sequence the table holds its
    /// structural invariants after every operation: no bucket exceeds K,
    /// every held peer sits in the bucket its distance dictates, the own id
    /// is never present, there are no duplicates, and `len()` agrees with
    /// the set of held peers. Finally `contains()` agrees with the held set
    /// for every id the sequence touched.
    ///
    /// Cross-checks the table against an independent reference LRU model
    /// after every operation. An exact state match is strictly stronger
    /// than spot-checking invariants: it pins bucket *placement*, the
    /// within-K fill bound, dedup, AND LRU eviction *ordering* (that the
    /// least-recently-seen entry is the one dropped) — none of which a
    /// `len() <= K` count check alone would catch. The `insert`/`remove`
    /// boolean returns are asserted against the model too.
    #[test]
    fn table_matches_independent_lru_model_under_random_ops(
        self_bytes in proptest::array::uniform32(any::<u8>()),
        // Two buckets (8..10) drawn from a deliberately small salt domain
        // (0..40) so distinct peers routinely collide into the same bucket
        // and drive fill past K=20 — the full `any::<u8>()` salt domain over
        // wider bucket ranges reaches the eviction path in only ~2% of cases
        // (measured), leaving it essentially untested. `bucket >= 8` keeps
        // the selecting bit in byte 30, clear of the salt byte (byte 31), so
        // every peer lands in its intended bucket. Up to 256 ops makes
        // eviction fire in roughly a third of cases.
        ops in prop::collection::vec((any::<bool>(), 8usize..10, 0u8..40), 0..256),
    ) {
        let self_id = NodeId::from_bytes(self_bytes);
        let mut rt = RoutingTable::new(self_id);
        let mut touched: BTreeSet<NodeId> = BTreeSet::new();

        // Reference model: bucket index -> peers in LRU order (front =
        // least-recently-seen, back = most-recently-seen). Keyed by the
        // INDEPENDENT `leading_zero_bits` oracle, never `bucket_index`, so a
        // placement bug in the table surfaces as a state mismatch rather
        // than being mirrored by the model.
        let mut model: BTreeMap<usize, Vec<NodeId>> = BTreeMap::new();

        for (is_insert, bucket, salt) in &ops {
            let peer = id_in_bucket(&self_id, *bucket, *salt);
            touched.insert(peer);

            let idx = KEYSPACE_BITS - 1 - leading_zero_bits(&xor_distance(&self_id, &peer));
            let slot = model.entry(idx).or_default();
            let present = slot.iter().position(|p| p == &peer);

            // Replay the documented insert/remove semantics on the model and
            // capture the boolean it implies.
            let model_ret = if *is_insert {
                if let Some(i) = present {
                    // Refresh: move to MRU, report "not a fresh insert".
                    let p = slot.remove(i);
                    slot.push(p);
                    false
                } else {
                    if slot.len() >= K_BUCKET_SIZE {
                        slot.remove(0); // evict least-recently-seen
                    }
                    slot.push(peer);
                    true
                }
            } else if let Some(i) = present {
                slot.remove(i);
                true
            } else {
                false
            };

            let rt_ret = if *is_insert {
                rt.insert(peer)
            } else {
                rt.remove(&peer)
            };
            prop_assert_eq!(rt_ret, model_ret, "insert/remove return disagreed with model");

            // Own id is never stored.
            prop_assert!(!rt.contains(&self_id));

            // Exact state match. `iter_peers` is bucket-major, MRU-last —
            // identical to flattening the model's `BTreeMap(idx) -> Vec`.
            // This single equality pins placement, within-K fill, dedup, and
            // LRU eviction order.
            let model_flat: Vec<NodeId> = model.values().flatten().copied().collect();
            let rt_flat: Vec<NodeId> = rt.iter_peers().copied().collect();
            prop_assert_eq!(&rt_flat, &model_flat, "table state diverged from LRU model");
        }

        // `contains()` is consistent with the held set for every id the
        // sequence touched (covers still-present and removed/evicted ids).
        let held: BTreeSet<NodeId> = rt.iter_peers().copied().collect();
        for id in &touched {
            prop_assert_eq!(rt.contains(id), held.contains(id));
        }
    }
}

proptest! {
    /// `closest()` returns the genuinely nearest peers to an arbitrary
    /// target: results are distance-sorted, deduplicated, a subset of the
    /// table, correctly bounded (`min(n, K, len)` for the wire-capped
    /// variant and `min(n, len)` for the unbounded one), and every excluded
    /// peer is no closer than the farthest returned one.
    #[test]
    fn closest_is_sorted_bounded_and_truly_nearest(
        self_bytes in proptest::array::uniform32(any::<u8>()),
        peers in prop::collection::vec((8usize..16, any::<u8>()), 0..48),
        target_bytes in proptest::array::uniform32(any::<u8>()),
        n in 0usize..40,
    ) {
        let self_id = NodeId::from_bytes(self_bytes);
        let mut rt = RoutingTable::new(self_id);
        for (bucket, salt) in &peers {
            rt.insert(id_in_bucket(&self_id, *bucket, *salt));
        }
        let held: BTreeSet<NodeId> = rt.iter_peers().copied().collect();
        let len = rt.len();

        // The wire-capped variant: bounded at min(n, K, len).
        let res = rt.closest(&target_bytes, n);
        check_closest_result(&res, &held, &target_bytes, n.min(K_BUCKET_SIZE).min(len))?;

        // The unbounded variant: same ordering/dedup/nearest guarantees,
        // bounded only by the request and table size.
        let res_unbounded = rt.closest_unbounded(&target_bytes, n);
        check_closest_result(&res_unbounded, &held, &target_bytes, n.min(len))?;
    }
}
