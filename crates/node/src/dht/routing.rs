//! Kademlia k-bucket routing table for `cdn/dht/v1` (ADR 022 §Routing Table).
//!
//! 256 buckets indexed by the XOR-distance prefix length to the node's own
//! `NodeId`; each bucket holds up to `K = 20` peers ordered most-recently-seen
//! last. The table is in-memory and rebuilt from the on-chain registry +
//! `FindNode` self-lookup on restart (ADR 022 §Bootstrap).
//!
//! # Self-NodeId handling
//!
//! Inserting the node's own `NodeId` is a no-op: XOR distance is zero, the
//! bucket index would underflow, and a node listing itself as a peer would
//! cause iterative lookup to terminate on its own routing table without ever
//! contacting the network.
//!
//! # Eviction policy
//!
//! On bucket overflow the least-recently-seen entry is evicted and the new
//! `NodeId` appended at the tail. Standard Kademlia pings the LRU first and
//! evicts only if it does not respond; the network-side ping landed in the
//! follow-up PR that introduces `FindNode` driving. The simpler LRU here
//! preserves the "newcomers are reachable" property — the LRU is the one
//! least recently confirmed to be live, so evicting it is the safest bet
//! among entries already in the bucket. ADR 022 §Routing Table does not
//! mandate the ping-then-evict variant.

use decdn_protocol::MAX_CLOSER_NODES;

/// Number of bytes in an iroh `NodeId` (32-byte Ed25519 public key).
/// Matches the `[u8; 32]` representation used on the wire in
/// [`decdn_protocol::dht`].
pub const NODE_ID_LEN: usize = 32;

/// Number of bits in the keyspace — equals `NODE_ID_LEN * 8`. The routing
/// table holds one bucket per possible XOR-prefix length.
pub const KEYSPACE_BITS: usize = NODE_ID_LEN * 8;

/// Kademlia bucket size (ADR 022 §Routing Table). Equal to
/// `MAX_CLOSER_NODES` so a fully-populated bucket can be serialised straight
/// into a `closer_nodes` field without re-truncation.
pub const K_BUCKET_SIZE: usize = MAX_CLOSER_NODES;

/// 32-byte routing-table peer identifier. Distinct from
/// [`iroh::PublicKey`] only so the table can be exercised without an iroh
/// dependency; the handler layer converts at the protocol boundary.
pub type NodeId = [u8; NODE_ID_LEN];

/// Bytewise XOR distance between two `NodeId`s. Cheap (one AVX register on
/// 64-bit) and the only function the bucket index depends on.
#[must_use]
pub fn xor_distance(a: &NodeId, b: &NodeId) -> [u8; NODE_ID_LEN] {
    let mut out = [0u8; NODE_ID_LEN];
    for i in 0..NODE_ID_LEN {
        // Indexing is bounded by the loop range; safe and the alternative
        // (`zip` + collect) materialises a temporary.
        if let (Some(o), Some(x), Some(y)) = (out.get_mut(i), a.get(i), b.get(i)) {
            *o = x ^ y;
        }
    }
    out
}

/// Compare two `NodeId`s by XOR distance to `target`. `Less` means `a` is
/// strictly closer to `target` than `b`. Used to sort candidate sets during
/// iterative lookup and to enforce the "strictly closer" invariant on
/// `closer_nodes` (ADR 022 §Lookup integrity step 1).
#[must_use]
pub fn cmp_by_distance(a: &NodeId, b: &NodeId, target: &NodeId) -> std::cmp::Ordering {
    xor_distance(a, target).cmp(&xor_distance(b, target))
}

/// Index of the bucket that holds peers at XOR distance `d` from the
/// routing-table's own node id. The bucket index is the position of the
/// most-significant set bit in `d` (counted from the high end), so peers
/// that share a long prefix with the local node land in low-index buckets
/// (close in keyspace) and peers with no shared prefix land in bucket 255.
///
/// Returns `None` when `d == 0` — the all-zero distance corresponds to the
/// node's own identity, which is never stored in the table.
#[must_use]
fn bucket_index(d: &[u8; NODE_ID_LEN]) -> Option<usize> {
    for (i, byte) in d.iter().enumerate() {
        if *byte != 0 {
            // Leading-zero count within the byte. `byte.leading_zeros()`
            // returns 0..=8; combined with the byte index, the resulting
            // bit position is unique to one bucket.
            let bit_within = byte.leading_zeros() as usize;
            // KEYSPACE_BITS - 1 - bit_position-from-msb. We invert because
            // bucket 0 holds the most-distant peers (high-bit set, "no shared
            // prefix") and bucket 255 the closest (only the lowest bit differs).
            let bit_from_msb = i * 8 + bit_within;
            return Some(KEYSPACE_BITS - 1 - bit_from_msb);
        }
    }
    None
}

/// In-memory k-bucket routing table (ADR 022 §Routing Table).
///
/// Not internally synchronised — the runtime owns it behind whatever mutex /
/// `RwLock` the handler chooses. A bare `&mut self` interface keeps the
/// happy-path hot loop (one `Vec::iter` + LRU bump) cache-friendly and lets
/// callers compose with their own concurrency strategy.
#[derive(Debug)]
pub struct RoutingTable {
    self_id: NodeId,
    /// 256 buckets, each at most `K_BUCKET_SIZE` entries. Boxed so the
    /// table itself stays a thin pointer on the stack — 256 × 20 × 32B is
    /// 160 KiB and lives on the heap once.
    buckets: Box<[Vec<NodeId>; KEYSPACE_BITS]>,
}

impl RoutingTable {
    /// Construct an empty routing table anchored at `self_id`.
    #[must_use]
    pub fn new(self_id: NodeId) -> Self {
        // `[Vec::new(); KEYSPACE_BITS]` requires `Copy`; build via array-from-fn.
        let buckets: Box<[Vec<NodeId>; KEYSPACE_BITS]> = (0..KEYSPACE_BITS)
            .map(|_| Vec::<NodeId>::new())
            .collect::<Vec<_>>()
            .into_boxed_slice()
            .try_into()
            // The collected Vec has exactly KEYSPACE_BITS elements; the
            // `try_into` is infallible by construction.
            .unwrap_or_else(|_| unreachable_size_mismatch());
        Self { self_id, buckets }
    }

    /// Own `NodeId` (the routing-table anchor).
    #[must_use]
    pub const fn self_id(&self) -> &NodeId {
        &self.self_id
    }

    /// Total entries across all buckets. O(`KEYSPACE_BITS`).
    #[must_use]
    pub fn len(&self) -> usize {
        self.buckets.iter().map(Vec::len).sum()
    }

    /// True iff the table has zero entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(Vec::is_empty)
    }

    /// Insert or refresh `peer`. Returns `true` on a fresh insert, `false`
    /// on a refresh (peer already present — its LRU position is moved to
    /// the tail) or a self-insert (silently ignored).
    ///
    /// On bucket overflow the least-recently-seen entry is evicted to make
    /// room. See the module docs for the LRU rationale vs. classic
    /// ping-then-evict.
    pub fn insert(&mut self, peer: NodeId) -> bool {
        if peer == self.self_id {
            return false;
        }
        let d = xor_distance(&self.self_id, &peer);
        let Some(idx) = bucket_index(&d) else {
            return false;
        };
        let Some(bucket) = self.buckets.get_mut(idx) else {
            // bucket_index returns < KEYSPACE_BITS by construction.
            return false;
        };
        if let Some(pos) = bucket.iter().position(|p| p == &peer) {
            // Already present — move to tail (most-recently-seen).
            let entry = bucket.remove(pos);
            bucket.push(entry);
            return false;
        }
        if bucket.len() >= K_BUCKET_SIZE {
            // Evict least-recently-seen.
            bucket.remove(0);
        }
        bucket.push(peer);
        true
    }

    /// Remove `peer` if present. Returns `true` if a removal occurred.
    pub fn remove(&mut self, peer: &NodeId) -> bool {
        if *peer == self.self_id {
            return false;
        }
        let d = xor_distance(&self.self_id, peer);
        let Some(idx) = bucket_index(&d) else {
            return false;
        };
        let Some(bucket) = self.buckets.get_mut(idx) else {
            return false;
        };
        if let Some(pos) = bucket.iter().position(|p| p == peer) {
            bucket.remove(pos);
            return true;
        }
        false
    }

    /// True if `peer` is currently in the table.
    #[must_use]
    pub fn contains(&self, peer: &NodeId) -> bool {
        if *peer == self.self_id {
            return false;
        }
        let d = xor_distance(&self.self_id, peer);
        let Some(idx) = bucket_index(&d) else {
            return false;
        };
        self.buckets
            .get(idx)
            .is_some_and(|b| b.iter().any(|p| p == peer))
    }

    /// Return up to `n` peers from the table sorted by XOR distance to
    /// `target` (closest first). Capped at `K_BUCKET_SIZE` since the wire
    /// `closer_nodes` field is itself capped at `MAX_CLOSER_NODES`.
    ///
    /// Walks every bucket; with 256 × 20 entries the worst case is 5120
    /// XOR-compares + a partial sort — well under a microsecond on modern
    /// hardware and dwarfed by the QUIC round-trip cost.
    #[must_use]
    pub fn closest(&self, target: &NodeId, n: usize) -> Vec<NodeId> {
        let cap = n.min(K_BUCKET_SIZE);
        let mut candidates: Vec<NodeId> = self.buckets.iter().flatten().copied().collect();
        candidates.sort_unstable_by(|a, b| cmp_by_distance(a, b, target));
        candidates.truncate(cap);
        candidates
    }

    /// Iterator over every peer currently held. Order is bucket-major,
    /// recency within bucket — useful for republish scans and bootstrap
    /// snapshots, but **not** distance-sorted (callers wanting closeness
    /// must use [`Self::closest`]).
    pub fn iter_peers(&self) -> impl Iterator<Item = &NodeId> + '_ {
        self.buckets.iter().flatten()
    }
}

/// Marked `#[cold]` so the unreachable-array-size branch in
/// [`RoutingTable::new`] doesn't pollute the hot-path layout. Calling it is
/// a programming error — the iterator produces exactly `KEYSPACE_BITS`
/// elements by construction.
#[cold]
#[inline(never)]
fn unreachable_size_mismatch() -> Box<[Vec<NodeId>; KEYSPACE_BITS]> {
    // Allocate a fresh boxed array to satisfy the type system without
    // panicking. Returning this means a programming error in `new`; under
    // anti-panic policy we surface a clearly-wrong empty table instead of
    // panicking. In practice this is unreachable.
    let v: Vec<Vec<NodeId>> = (0..KEYSPACE_BITS).map(|_| Vec::new()).collect();
    v.into_boxed_slice()
        .try_into()
        .unwrap_or_else(|_| unreachable_size_mismatch())
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

    fn id(byte: u8) -> NodeId {
        [byte; 32]
    }

    /// Construct a `NodeId` that XORs with `self_id` to a value with exactly
    /// `prefix_bits` leading zeros. Used to drop entries into a specific
    /// bucket deterministically.
    fn id_in_bucket(self_id: &NodeId, bucket_idx: usize, salt: u8) -> NodeId {
        // bucket_idx = KEYSPACE_BITS - 1 - bit_from_msb  =>  bit_from_msb = 255 - bucket_idx
        let bit_from_msb = KEYSPACE_BITS - 1 - bucket_idx;
        let byte_idx = bit_from_msb / 8;
        let bit_within = bit_from_msb % 8;
        let mut out = *self_id;
        // Flip the leading bit so distance bucket index lands on `bucket_idx`.
        if let Some(b) = out.get_mut(byte_idx) {
            *b ^= 1u8 << (7 - bit_within);
        }
        // Bury `salt` in the last byte to vary peers within a bucket without
        // changing the leading prefix.
        if let Some(last) = out.last_mut() {
            *last ^= salt;
        }
        out
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
    fn bucket_index_max_distance_is_zero() {
        // High bit of first byte set -> bit_from_msb = 0 -> bucket 255.
        let mut d = [0u8; 32];
        d[0] = 0x80;
        assert_eq!(bucket_index(&d), Some(255));
    }

    #[test]
    fn bucket_index_min_nonzero_is_max_bucket() {
        // Low bit of last byte set -> bit_from_msb = 255 -> bucket 0.
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
        let closest = rt.closest(&target, 4);
        assert_eq!(closest.len(), 4);
        // First entry must be strictly closer than the last.
        let first = closest.first().expect("len >= 1");
        let last = closest.last().expect("len >= 1");
        let d_first = xor_distance(first, &target);
        let d_last = xor_distance(last, &target);
        assert!(d_first < d_last, "first = {d_first:?} >= last = {d_last:?}");
    }

    #[test]
    fn closest_returns_no_more_than_n_or_table_size() {
        let self_id = id(0);
        let mut rt = RoutingTable::new(self_id);
        for b in 0..30_usize {
            rt.insert(id_in_bucket(&self_id, b, 0));
        }
        assert_eq!(rt.closest(&id(0xFF), 10).len(), 10);
        // Cap at K_BUCKET_SIZE even when caller asks for more.
        assert_eq!(rt.closest(&id(0xFF), 1000).len(), K_BUCKET_SIZE);
    }

    #[test]
    fn closest_on_empty_table_is_empty() {
        let rt = RoutingTable::new(id(0));
        assert!(rt.closest(&id(0xFF), 10).is_empty());
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
    fn cmp_by_distance_orders_closer_first() {
        let target = id(0);
        // a is closer to target than b.
        let a = id(1);
        let b = id(0xFF);
        assert_eq!(cmp_by_distance(&a, &b, &target), std::cmp::Ordering::Less);
        assert_eq!(
            cmp_by_distance(&b, &a, &target),
            std::cmp::Ordering::Greater
        );
        assert_eq!(cmp_by_distance(&a, &a, &target), std::cmp::Ordering::Equal);
    }
}
