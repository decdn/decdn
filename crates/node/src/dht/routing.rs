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
//! On bucket overflow the least-recently-seen entry is evicted and the
//! new `NodeId` appended at the tail. Standard Kademlia would ping the
//! LRU first and evict only if it does not respond; ADR 022 §Routing
//! Table does not mandate the ping-then-evict variant and we do not
//! implement it. The simpler LRU here preserves the "newcomers are
//! reachable" property — the LRU is the one least recently confirmed to
//! be live, so evicting it is the safest bet among entries already in
//! the bucket.

use decdn_protocol::MAX_CLOSER_NODES;
pub use decdn_protocol::NodeId;

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

/// Bytewise XOR distance between two 32-byte keyspace points. Cheap (one AVX
/// register on 64-bit) and the only function the bucket index depends on.
///
/// Operates on raw bytes so it serves the cross-domain case ADR 022 relies on:
/// a [`NodeId`] and a `ContentHash` share the same 256-bit XOR keyspace, so the
/// `FindValue` path measures a content hash's distance to candidate node ids
/// with this primitive. [`xor_distance`] is the `NodeId`-vs-`NodeId` wrapper.
#[must_use]
pub fn xor_distance_bytes(a: &[u8; NODE_ID_LEN], b: &[u8; NODE_ID_LEN]) -> [u8; NODE_ID_LEN] {
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

/// Bytewise XOR distance between two `NodeId`s. Thin wrapper over
/// [`xor_distance_bytes`] for the common same-domain case.
#[must_use]
pub fn xor_distance(a: &NodeId, b: &NodeId) -> [u8; NODE_ID_LEN] {
    xor_distance_bytes(a.as_bytes(), b.as_bytes())
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
pub(crate) fn bucket_index(d: &[u8; NODE_ID_LEN]) -> Option<usize> {
    for (i, byte) in d.iter().enumerate() {
        if *byte != 0 {
            // Leading-zero count within the byte. `byte.leading_zeros()`
            // returns 0..=8; combined with the byte index, the resulting
            // bit position is unique to one bucket.
            let bit_within = byte.leading_zeros() as usize;
            // KEYSPACE_BITS - 1 - bit_from_msb. The inversion lines up the
            // bucket index with shared-prefix length: bucket 255 holds the
            // most-distant peers (high bit set in `d` => zero shared prefix
            // bits => `bit_from_msb == 0`) and bucket 0 holds the closest
            // peers (only the lowest bit of `d` differs => `bit_from_msb ==
            // 255`).
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
        // `std::array::from_fn` builds the fixed-size array in place, so
        // there is no intermediate `Vec` and no fallible length conversion
        // — the type system guarantees we end up with exactly
        // `KEYSPACE_BITS` empty buckets. We `Box` after the fact so the
        // 160 KiB live on the heap rather than the stack.
        let buckets: Box<[Vec<NodeId>; KEYSPACE_BITS]> =
            Box::new(std::array::from_fn(|_| Vec::new()));
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
    /// XOR computes (once each, cached) + a sort over those cached distances —
    /// well under a microsecond on modern hardware and dwarfed by the QUIC
    /// round-trip cost.
    ///
    /// The result is capped at [`K_BUCKET_SIZE`] (= [`MAX_CLOSER_NODES`])
    /// so it can be serialised directly into a wire `closer_nodes` field.
    /// Callers needing more than K candidates (e.g. ADR 022 §STORE Flow
    /// "K+3 closest" republish fanout) MUST use [`Self::closest_unbounded`].
    #[must_use]
    pub fn closest(&self, target: &[u8; NODE_ID_LEN], n: usize) -> Vec<NodeId> {
        let cap = n.min(K_BUCKET_SIZE);
        self.closest_unbounded(target, cap)
    }

    /// Like [`Self::closest`] but without the wire-cap clamp at
    /// [`K_BUCKET_SIZE`]. Used by the republish path which fans out to
    /// K+3 closest peers per ADR 022 §STORE Flow line 128 ("three
    /// positions beyond K are overflow targets — publishing to a wider
    /// set than the minimum required by the routing geometry means an
    /// attacker forcing record expiry by suppressing receivers must
    /// take down K+3 hosts rather than K").
    ///
    /// MUST NOT be used to populate wire responses — those are bound at
    /// [`MAX_CLOSER_NODES`] and a larger result would violate the
    /// ADR 022 wire-cost ceiling. Use [`Self::closest`] for that path.
    #[must_use]
    pub fn closest_unbounded(&self, target: &[u8; NODE_ID_LEN], n: usize) -> Vec<NodeId> {
        // Schwartzian transform: compute `xor_distance_bytes` exactly once per
        // candidate, then sort on the cached distance. `sort_unstable_by_key`
        // does NOT cache its key (that is what `sort_by_cached_key` is for), so
        // sorting directly on the closure would recompute the distance
        // `O(n log n)` times on this hot path. `target` is a raw keyspace point,
        // so a `ContentHash` (FindValue) or a `NodeId` (FindNode) both fit.
        let mut scored: Vec<(NodeId, [u8; NODE_ID_LEN])> = self
            .buckets
            .iter()
            .flatten()
            .map(|p| (*p, xor_distance_bytes(p.as_bytes(), target)))
            .collect();
        // Sorting on the already-computed distance only re-copies the cached
        // 32-byte key during comparisons — `xor_distance_bytes` itself still
        // runs exactly once per element above.
        scored.sort_unstable_by_key(|&(_, dist)| dist);
        scored.truncate(n);
        scored.into_iter().map(|(p, _)| p).collect()
    }

    /// Iterator over every peer currently held. Order is bucket-major,
    /// recency within bucket — useful for republish scans and bootstrap
    /// snapshots, but **not** distance-sorted (callers wanting closeness
    /// must use [`Self::closest`]).
    pub fn iter_peers(&self) -> impl Iterator<Item = &NodeId> + '_ {
        self.buckets.iter().flatten()
    }

    /// Per-bucket fill counts for the **non-empty** buckets only, as
    /// `(index, fill)` pairs ordered by bucket index ascending. Read-only
    /// snapshot for the `admin_v1_status` routing-table health view (issue
    /// #741); empty buckets (the vast majority of the 256-bucket keyspace
    /// on a small network) are omitted so the snapshot stays compact. The
    /// per-bucket capacity is the fixed Kademlia [`K_BUCKET_SIZE`].
    #[must_use]
    pub fn non_empty_bucket_fills(&self) -> Vec<(usize, usize)> {
        self.buckets
            .iter()
            .enumerate()
            .filter(|(_, bucket)| !bucket.is_empty())
            .map(|(idx, bucket)| (idx, bucket.len()))
            .collect()
    }
}

/// Construct a `NodeId` that lands in bucket `bucket_idx` relative to
/// `self_id`, varied within the bucket by `salt`. Shared by both test modules
/// below.
///
/// Flips the single distance bit that selects `bucket_idx` and buries `salt`
/// in the last byte to vary peers within a bucket without changing the leading
/// prefix. Note that for the lowest eight buckets the selecting bit lives in
/// the last byte too, so a non-zero `salt` there can perturb the bucket;
/// callers wanting a guaranteed bucket across all salts use `bucket_idx >= 8`
/// (the selecting bit then sits in byte 30, which `salt` never touches).
#[cfg(test)]
fn id_in_bucket(self_id: &NodeId, bucket_idx: usize, salt: u8) -> NodeId {
    // bucket_idx = KEYSPACE_BITS - 1 - bit_from_msb  =>  bit_from_msb = 255 - bucket_idx
    let bit_from_msb = KEYSPACE_BITS - 1 - bucket_idx;
    let byte_idx = bit_from_msb / 8;
    let bit_within = bit_from_msb % 8;
    let mut out = *self_id.as_bytes();
    if let Some(b) = out.get_mut(byte_idx) {
        *b ^= 1u8 << (7 - bit_within);
    }
    if let Some(last) = out.last_mut() {
        *last ^= salt;
    }
    NodeId::from_bytes(out)
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
}

/// Property-based tests (issue #748).
///
/// The example-based `tests` module above pins specific scenarios; this module
/// asserts the *invariants* hold under random `NodeId` insertion/removal
/// sequences and arbitrary keyspace points — the class of structural bug
/// (a bucket overflowing K, a peer in the wrong bucket, a mis-ordered
/// `closest()`) that silently degrades lookup correctness but is easy to miss
/// with hand-picked cases.
///
/// Note on terminology: issue #748 refers to a "k-bucket *split* invariant",
/// but this table is the fixed-256-bucket variant (see the module docs) that
/// **never splits**. The corresponding invariants tested here are deterministic
/// bucket *placement* by XOR-prefix length and per-bucket fill `<= K_BUCKET_SIZE`
/// enforced by LRU eviction.
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod prop_tests {
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
}
