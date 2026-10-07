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
mod tests;

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
mod prop_tests;
