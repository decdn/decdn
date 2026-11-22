//! Requester-side candidate cache (ADR 001 §Candidate cache).
//!
//! The mirror of [`super::negative_cache`]: where that one remembers which
//! `(NodeId, hash)` pairs answered a discovery open with `NotFound` (or
//! refused a pull), this one remembers which nodes answered it as holders, so
//! a second miss for the same hash inside the TTL skips the DHT lookup AND the
//! discovery fan-out and goes straight to selection. The cache:
//!
//! - is keyed by `hash`;
//! - holds at most 1024 hashes with LRU eviction;
//! - retains at most 10 providers per hash (top 10 by selection score);
//! - retains entries for [`DISCOVERY_ANSWER_TTL`] (30 s), anchored at
//!   insertion;
//! - stores the triple `(NodeId, rate_per_mb, rtt)` plus the answer's
//!   UNSIGNED range-keyed `Coverage` and proven size, never the signed
//!   `StreamResponse`.
//!
//! A hit saves only the lookup and the fan-out. The pull still opens a stream
//! on each candidate it tries, and that open's signed answer re-verifies the
//! quote, the proven size and the coverage before a voucher is paid. So a
//! stale entry costs at most one open: the candidate refuses, or answers on
//! terms the pull checks again. That is why the TTL is the short refusal
//! suppression's, and not something tighter.
//!
//! The cache stores no signature. A `StreamResponse` carries `slash_sig`: a
//! peer's signed statement about one answer at one time. The next pull opens
//! the candidate again and verifies a fresh signed answer before it pays, so a
//! retained signature is never read. A structure that survives one request to
//! speed up the next has no business holding another node's signed
//! statements.
//!
//! `Coverage` and the proven size are exempt from that concern, not an
//! exception to it: both ride the unsigned `StreamResponseExt`, outside the
//! `slash_sig` set, so they carry no author. They ride the entry so a cache-hit candidate can still be
//! range-planned (`plan_covered_runs`) against its coverage; without it a real
//! partial holder would read as covering nothing on the hit path and be
//! excluded from range assignment.
//!
//! `reputation` is likewise NOT stored, for a different reason: it is a local,
//! live value that moves on every pull outcome. Freezing it for the TTL would let a
//! node that just failed three pulls keep the rank it held before them.
//! `node_origin::cached_candidates` recomputes reputation and region on read and
//! re-runs `rank_candidates`: "goes straight to selection" means skipping
//! discovery, not skipping the selection algorithm.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use decdn_protocol::Coverage;
use indexmap::IndexMap;
use tracing::warn;

use crate::dht::routing::NodeId;

pub use crate::dht::records::Hash;

/// How long one discovery answer shapes the misses after it: the TTL of a
/// [`CandidateCache`] entry, and of `node_origin`'s short suppression of a
/// `(peer, hash)` pair in the negative cache after a refusal, a shed or a
/// timestamp refusal.
///
/// One constant, so the positive and the negative view of a peer's answers
/// age on the same clock: a holder the cache still names is never one the
/// short suppression has already forgotten refusing, nor the reverse.
///
/// 30 s, because a stale entry is cheap. A hit skips only the DHT lookup and
/// the discovery fan-out, and the paid open re-verifies the quote, the size and
/// the coverage, so a stale entry costs at most one open.
pub const DISCOVERY_ANSWER_TTL: Duration = Duration::from_secs(30);

const DEFAULT_TTL: Duration = DISCOVERY_ANSWER_TTL;

/// ADR 001 §Candidate cache: max 1024 entries.
const DEFAULT_CAPACITY: usize = 1024;

/// ADR 001 §Candidate cache: "Each hash entry retains at most 10 responses
/// (top 10 by selection score)." With [`DEFAULT_CAPACITY`] this is what bounds the
/// cache's memory at the ADR's stated ~1 MB (1024 × 10 × ~100 bytes).
///
/// Enforced by [`CandidateCache::insert`] rather than by its caller, so the
/// bound is an invariant of the TYPE: a cap a caller can forget is not a cap.
const MAX_PROVIDERS_PER_HASH: usize = 10;

/// One holder a discovery open found: ADR 001 §Candidate cache's entry
/// triple (`hash → Vec<(NodeId, rate_per_mb, rtt)>`) plus the answer's
/// unsigned range-keyed [`Coverage`] and proven size, and nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedCandidate {
    /// The provider that answered as a holder.
    pub node_id: NodeId,
    /// The per-MB rate it signed on that answer.
    pub rate_per_mb: u64,
    /// The path RTT measured on that open.
    pub rtt_ms: u32,
    /// The discovery blocks the provider reported holding on that answer
    /// (`StreamResponseExt.coverage`). UNSIGNED, outside the `slash_sig` set,
    /// so retaining it is not the signature retention the module doc forbids;
    /// it carries no author. A cache-hit candidate is range-planned
    /// against this value rather than reading as covering nothing.
    pub coverage: Coverage,
    /// The blob size the answer proved, when it carried a size proof. Outside
    /// `slash_sig`, like `coverage`, so it carries no author either.
    /// The pull leg cuts its first upstream open from it.
    pub total_bytes_hint: Option<u64>,
}

// The guard against retaining signed statements is the exhaustive struct
// literal at the single write site (no `..`, so any new field must be populated
// there — sending the author back to the module doc). `CachedCandidate` does not
// derive `Copy`: `Coverage` wraps a `Vec<u8>`, which cannot be `Copy`. A
// footprint-ceiling `size_of` assert would only ever bound the fixed head, not
// the heap the coverage bitmap owns, so this struct carries none. A
// `Signature`/`StreamResponse` field is still caught at the write site, which is
// where the invariant actually lives.

#[derive(Debug)]
struct Entry {
    /// Providers in write-time ranked order, best-first, truncated to
    /// [`MAX_PROVIDERS_PER_HASH`].
    providers: Vec<CachedCandidate>,
    /// Absolute expiry, anchored at insert and never refreshed on read.
    expiry: Instant,
}

#[derive(Debug)]
struct Inner {
    /// Hash → entry. `IndexMap` collapses what would otherwise be a
    /// `HashMap` + side `VecDeque` (kept in lockstep to track LRU
    /// order) into a single store: insertion order is the LRU ordering,
    /// index 0 = most-recently-used and `len()-1` = least-recently-used.
    /// Identical to [`super::negative_cache`] — deliberately, so the
    /// two caches' eviction and expiry semantics cannot drift.
    entries: IndexMap<Hash, Entry>,
    /// Hard cap on live hashes. Clamped to ≥ 1 in the constructor.
    cap: usize,
    /// TTL applied on insert. Anchored at insertion, NOT refreshed on read.
    ttl: Duration,
}

/// Bounded LRU cache of `hash → Vec<(NodeId, rate_per_mb, rtt)>`.
#[derive(Debug)]
pub struct CandidateCache {
    inner: Mutex<Inner>,
}

impl Default for CandidateCache {
    fn default() -> Self {
        Self::new()
    }
}

impl CandidateCache {
    /// Build a cache with the ADR 001 §Candidate cache defaults (1024 hashes,
    /// [`DISCOVERY_ANSWER_TTL`], ≤10 providers per hash).
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity_and_ttl(DEFAULT_CAPACITY, DEFAULT_TTL)
    }

    #[cfg(test)]
    #[must_use]
    fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_ttl(cap, DEFAULT_TTL)
    }

    /// Build a cache with an explicit capacity and TTL.
    ///
    /// Production code uses [`Self::new`]; this is the test / tuning seam, for
    /// the same reason
    /// [`super::negative_cache::NegativeCandidateCache::with_capacity_and_ttl`] is:
    /// the TTL is anchored on [`Instant`], so `tokio::time` pause / advance has
    /// no effect on it, and 30 s of wall clock per assertion is not a test suite.
    /// `cap` is clamped to ≥ 1.
    ///
    /// A `ttl` of [`Duration::ZERO`] disables the cache behaviourally: every
    /// entry is already expired the instant it is read, so [`Self::get`] always
    /// misses. That is the supported way to opt out of candidate caching — there
    /// is deliberately no `disabled()` constructor, because a constructor that
    /// turns off an ADR-mandated behaviour is a thing production code will
    /// eventually call.
    #[must_use]
    pub fn with_capacity_and_ttl(cap: usize, ttl: Duration) -> Self {
        let cap = cap.max(1);
        Self {
            inner: Mutex::new(Inner {
                entries: IndexMap::with_capacity(cap),
                cap,
                ttl,
            }),
        }
    }

    /// The cached providers for `hash`, best-first, iff an entry exists and its
    /// TTL hasn't elapsed.
    ///
    /// A live hit bumps the entry to the front of the LRU ordering (index 0)
    /// **without refreshing its expiry** — TTL is anchored at insertion per
    /// ADR 001 §Candidate cache. An expired entry is evicted before returning
    /// `None`.
    ///
    /// Returns a clone rather than a guard-scoped borrow on purpose: the caller
    /// rebuilds `Candidate`s from this, which means an `await` on `region_of(..)`
    /// per provider, and holding a `std::sync::Mutex` guard across an await is
    /// exactly the hazard `clippy::await_holding_lock` exists for. At ≤10 small
    /// providers — each a triple plus a coverage bitmap sized to the blob's
    /// block count — the clone is not worth arguing about.
    #[must_use]
    pub fn get(&self, hash: &Hash) -> Option<Vec<CachedCandidate>> {
        let now = Instant::now();
        let mut guard = self.lock();
        // Remove first: an expired entry is then already evicted (below), and a
        // live one is re-inserted at the front. This drops the redundant `get`
        // that a check-then-remove-then-reinsert shape would cost, and keeps the
        // hit path to two `IndexMap` ops (#1223 review).
        let entry = guard.entries.shift_remove(hash)?;
        if entry.expiry <= now {
            return None;
        }
        // Bump to front of LRU, preserving the original expiry. Unlike
        // `negative_cache`, whose `Instant` value is `Copy` and so can ride a
        // single `shift_insert(0, key, expiry)`, the `Entry` here must be moved
        // out and back.
        let providers = entry.providers.clone();
        guard.entries.shift_insert(0, *hash, entry);
        Some(providers)
    }

    /// Insert / replace the entry for `hash` with `now + TTL` expiry, moving it
    /// to the front of the LRU ordering. Evicts the least-recently-used hash on
    /// cap overflow.
    ///
    /// `providers` MUST already be in selection order, best-first: this keeps the
    /// first `MAX_PROVIDERS_PER_HASH`, which is ADR 001's "top 10 by selection
    /// score" only if the caller ranked first. The cache cannot rank them itself
    /// — the selection score needs `reputation`, which is precisely the field
    /// this cache refuses to store.
    ///
    /// An empty `providers` is a no-op. An empty entry is strictly worse than no
    /// entry: it would occupy an LRU slot, hit on every read, and yield nothing
    /// — a cache of "we found nobody", which is the negative cache's job and not
    /// on the negative cache's terms.
    pub fn insert(&self, hash: Hash, mut providers: Vec<CachedCandidate>) {
        if providers.is_empty() {
            return;
        }
        providers.truncate(MAX_PROVIDERS_PER_HASH);
        let mut guard = self.lock();
        let expiry = Instant::now() + guard.ttl;
        let entry = Entry { providers, expiry };
        // `shift_insert` moves an existing key to the new index and replaces the
        // value — the MRU bump we want on the refresh path.
        guard.entries.shift_insert(0, hash, entry);
        if guard.entries.len() > guard.cap {
            // `pop` removes the last entry — the LRU back.
            guard.entries.pop();
        }
    }

    /// Drop the entry for `hash`, if any.
    ///
    /// ADR 001 §Candidate cache: "if all fail, run a fresh DHT lookup and
    /// discovery." A hit whose every BUDGETED provider failed to deliver has
    /// been disproved by the only evidence that outranks an answer — actual
    /// pulls — so it is removed rather than left to keep hitting for the rest
    /// of its TTL. The caller may not have tried the entry's whole list (the
    /// attempt budget can be smaller than the list); the top-ranked members
    /// failing is disproof enough.
    pub fn invalidate(&self, hash: &Hash) {
        self.lock().entries.shift_remove(hash);
    }

    /// Current hash count. Includes expired entries that haven't been swept yet
    /// — call [`Self::get`] first if a precise live count is needed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Whether the cache holds zero entries (including stale).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().entries.is_empty()
    }

    /// Poison-tolerant lock acquisition — see
    /// [`super::negative_cache::NegativeCandidateCache`] for why we recover rather
    /// than propagate.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!("CandidateCache mutex poisoned; recovering inner state");
                poisoned.into_inner()
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
