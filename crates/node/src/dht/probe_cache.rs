//! Requester-side POSITIVE probe cache (ADR 001 §Probe cache).
//!
//! The mirror of [`super::negative_cache`]: where that one remembers which
//! `(NodeId, hash)` pairs answered `has_blob: false` (or refused a pull), this
//! one remembers which nodes answered `has_blob: true` — so a second miss for the same hash inside
//! the TTL skips the DHT lookup AND the probe fanout entirely and goes straight
//! to selection. Per ADR 001 §Probe cache the cache:
//!
//! - is keyed by `hash`;
//! - holds at most 1024 hashes with LRU eviction;
//! - retains at most 10 providers per hash (top 10 by selection score);
//! - retains entries for `PROBE_SLASH_WINDOW / 2` = 15s (ADR 005 §Derived
//!   constants), anchored at insertion;
//! - stores the ADR triple `(NodeId, rate_per_mb, rtt)` plus the probe's
//!   UNSIGNED range-keyed `Coverage` (#1506) — never the signed `ProbeResponse`.
//!
//! That last point is #1165's "no evidence retention" requirement, and it is
//! not a memory optimisation. A `ProbeResponse` carries `slash_sig`: a peer's
//! signed `has_blob: true`. Paired with a stream response it is on-chain
//! rate-manipulation evidence for `PROBE_SLASH_WINDOW`, and it can also
//! corroborate a blacklist violation — which is bounded by `MAX_EVIDENCE_AGE_US`
//! (5 days), not by the 30s window (ADR 014 §Evidence staleness). So the 15s TTL
//! does not make retention harmless by expiry alone; the point is simply that a
//! structure surviving one request to speed up the next has no business holding
//! another node's slashable statements — retaining them turns an availability
//! cache into an evidence locker.
//!
//! `Coverage` is exempt from that concern, not an exception to it: it is
//! UNSIGNED (`ProbeResponseExt`, outside the `slash_sig` set), so it carries no
//! author to slash and is not evidence of anything. It rides the entry so a
//! cache-hit candidate can still be range-planned (`plan_covered_runs`) against
//! its ≤15s-fresh coverage; without it a real partial holder would read as
//! covering nothing on the hit path and be excluded from range assignment.
//!
//! `reputation` is likewise NOT stored, for a different reason: it is a local,
//! live value that moves on every pull outcome. Freezing it for the TTL would let a
//! node that just failed three pulls keep the rank it held before them.
//! `node_origin::cached_candidates` recomputes reputation and region on read and
//! re-runs `rank_candidates` — ADR 001's "goes straight to selection" means
//! skipping discovery and probing, not skipping the selection algorithm.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use decdn_cache::PROBE_SLASH_WINDOW;
use decdn_protocol::Coverage;
use indexmap::IndexMap;
use tracing::warn;

use crate::dht::routing::NodeId;

pub use crate::dht::records::Hash;

/// ADR 005 §Derived constants: `probe_cache_ttl = PROBE_SLASH_WINDOW / 2` = 15s.
///
/// Derived rather than written as a literal `15s` because ADR 005 says so
/// ("Implementations SHOULD define `PROBE_SLASH_WINDOW` as a named constant and
/// compute the others from it"): a governance change to the slashing window must
/// move this with it, and a literal would silently not move. The 15s ceiling is
/// what keeps a stream opened from a cached entry inside the window during which
/// a misbehaving provider is still slashable — and, per ADR 001, below
/// `probe_hold_duration` ([`decdn_cache::PROBE_HOLD_DURATION`], 35s), so the
/// provider's eviction hold still covers the blob when that stream opens. The
/// second bound is what makes `EvictedSinceProbe` rare on the hit path.
///
/// Expressed through `as_secs` because `Duration: Div<u32>` is not `const` — the
/// same reason [`decdn_cache::PROBE_HOLD_DURATION`] reaches for
/// `saturating_add`. Integer division truncates sub-second remainders; harmless
/// at the current even 30s, and the halving is a policy ratio, not an exact
/// arithmetic requirement.
const DEFAULT_TTL: Duration = Duration::from_secs(PROBE_SLASH_WINDOW.as_secs() / 2);

/// ADR 001 §Probe cache: max 1024 entries.
const DEFAULT_CAPACITY: usize = 1024;

/// ADR 001 §Probe cache: "Each hash entry retains at most 10 responses (top 10
/// by selection score)." With [`DEFAULT_CAPACITY`] this is what bounds the
/// cache's memory at the ADR's stated ~1 MB (1024 × 10 × ~100 bytes).
///
/// Enforced by [`PositiveProbeCache::insert`] rather than by its caller, so the
/// bound is an invariant of the TYPE: a cap a caller can forget is not a cap.
const MAX_PROVIDERS_PER_HASH: usize = 10;

/// One probed provider — ADR 001 §Probe cache's entry triple
/// (`hash → Vec<(NodeId, rate_per_mb, rtt)>`) plus the probe's unsigned
/// range-keyed [`Coverage`] (#1506), and nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbedProvider {
    /// The provider that answered `has_blob: true`.
    pub node_id: NodeId,
    /// Its quoted `ProbeResponse::rate_per_mb`.
    pub rate_per_mb: u64,
    /// The round-trip latency observed on that probe.
    pub rtt_ms: u32,
    /// The discovery blocks the provider reported holding on that probe
    /// (`ProbeResponseExt.coverage`, #1506). UNSIGNED — outside the `slash_sig`
    /// set — so retaining it is not the evidence retention the module doc
    /// forbids; it carries no slashable author. A cache-hit candidate is
    /// range-planned against this ≤15s-fresh value rather than reading as
    /// covering nothing.
    pub coverage: Coverage,
}

// The guard against retaining slashable evidence is the exhaustive struct
// literal at the single write site (no `..`, so any new field must be populated
// there — sending the author back to the module doc). `ProbedProvider` does not
// derive `Copy`: `Coverage` wraps a `Vec<u8>`, which cannot be `Copy`. A
// footprint-ceiling `size_of` assert would only ever bound the fixed head, not
// the heap the coverage bitmap owns, so this struct carries none. A
// `Signature`/`ProbeResponse` field is still caught at the write site, which is
// where the invariant actually lives.

#[derive(Debug)]
struct Entry {
    /// Providers in write-time ranked order, best-first, truncated to
    /// [`MAX_PROVIDERS_PER_HASH`].
    providers: Vec<ProbedProvider>,
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
pub struct PositiveProbeCache {
    inner: Mutex<Inner>,
}

impl Default for PositiveProbeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl PositiveProbeCache {
    /// Build a cache with the ADR 001 §Probe cache defaults (1024 hashes, 15s
    /// TTL, ≤10 providers per hash).
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
    /// [`super::negative_cache::NegativeProbeCache::with_capacity_and_ttl`] is:
    /// the TTL is anchored on [`Instant`], so `tokio::time` pause / advance has
    /// no effect on it, and 15s of wall clock per assertion is not a test suite.
    /// `cap` is clamped to ≥ 1.
    ///
    /// A `ttl` of [`Duration::ZERO`] disables the cache behaviourally: every
    /// entry is already expired the instant it is read, so [`Self::get`] always
    /// misses. That is the supported way to opt out of positive caching — there
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
    /// ADR 001 §Probe cache. An expired entry is evicted before returning
    /// `None`.
    ///
    /// Returns a clone rather than a guard-scoped borrow on purpose: the caller
    /// rebuilds `Candidate`s from this, which means an `await` on `region_of(..)`
    /// per provider, and holding a `std::sync::Mutex` guard across an await is
    /// exactly the hazard `clippy::await_holding_lock` exists for. At ≤10 small
    /// providers — each a triple plus a coverage bitmap sized to the blob's
    /// block count — the clone is not worth arguing about.
    #[must_use]
    pub fn get(&self, hash: &Hash) -> Option<Vec<ProbedProvider>> {
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
    pub fn insert(&self, hash: Hash, mut providers: Vec<ProbedProvider>) {
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
    /// ADR 001 §Probe cache: "if all fail, run a fresh DHT lookup + probe." A hit
    /// whose every BUDGETED provider failed to deliver has been disproved by the
    /// only evidence that outranks a probe — actual pulls — so it is removed
    /// rather than left to keep hitting for the rest of its TTL. The caller may
    /// not have tried the entry's whole list (the attempt budget can be smaller
    /// than the list); the top-ranked members failing is disproof enough.
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
    /// [`super::negative_cache::NegativeProbeCache`] for why we recover rather
    /// than propagate.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!("PositiveProbeCache mutex poisoned; recovering inner state");
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
mod tests {
    use super::*;
    use std::thread;

    fn nid(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 32])
    }
    fn h(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }
    fn p(byte: u8) -> ProbedProvider {
        ProbedProvider {
            node_id: nid(byte),
            rate_per_mb: u64::from(byte),
            rtt_ms: u32::from(byte),
            // A distinct one-block coverage per provider, so a round-trip that
            // dropped or aliased the field fails the `PartialEq` assertions.
            coverage: Coverage::from_block_indices(8, std::iter::once(u32::from(byte % 8))),
        }
    }

    /// ADR 005 §Derived constants: `probe_cache_ttl = PROBE_SLASH_WINDOW / 2`.
    /// Pins the DERIVATION, not the number: a literal `15s` passes an
    /// `== Duration::from_secs(15)` assertion and then silently fails to move
    /// when governance changes the slashing window — the one thing ADR 005
    /// explicitly asks implementations to get right.
    #[test]
    fn ttl_is_derived_from_the_probe_slash_window() {
        assert_eq!(DEFAULT_TTL * 2, PROBE_SLASH_WINDOW);
        assert_eq!(DEFAULT_TTL, Duration::from_secs(15));
    }

    #[test]
    fn absent_hash_returns_none() {
        let c = PositiveProbeCache::new();
        assert!(c.get(&h(1)).is_none());
        assert!(c.is_empty());
    }

    #[test]
    fn insert_then_get_returns_providers_in_order() {
        let c = PositiveProbeCache::new();
        c.insert(h(1), vec![p(1), p(2)]);
        assert_eq!(c.get(&h(1)), Some(vec![p(1), p(2)]));
        assert!(c.get(&h(2)).is_none());
        assert_eq!(c.len(), 1);
    }

    /// ADR 001 §Probe cache: "Each hash entry retains at most 10 responses (top
    /// 10 by selection score)." This truncation is what bounds the cache at the
    /// ADR's stated ~1 MB; without it a hash with a large probe fanout is
    /// unbounded.
    #[test]
    fn insert_keeps_only_the_top_ten_providers() {
        let c = PositiveProbeCache::new();
        let many: Vec<_> = (1..=25u8).map(p).collect();
        c.insert(h(1), many);
        let got = c.get(&h(1)).unwrap();
        assert_eq!(got.len(), MAX_PROVIDERS_PER_HASH);
        // The FIRST ten — the caller ranked best-first, so truncation must drop
        // the tail. Taking the last ten would keep the ten WORST providers.
        assert_eq!(got, (1..=10u8).map(p).collect::<Vec<_>>());
    }

    /// An empty entry would occupy an LRU slot, hit on every read, and yield
    /// nothing — strictly worse than no entry.
    #[test]
    fn inserting_no_providers_is_a_no_op() {
        let c = PositiveProbeCache::new();
        c.insert(h(1), vec![]);
        assert!(c.is_empty());
        assert!(c.get(&h(1)).is_none());
    }

    #[test]
    fn expired_entry_returns_none_and_is_evicted() {
        // Margins kept generous (TTL 500ms, sleep 750ms) so loaded CI runners
        // with cargo-nextest parallelism don't flake on wall-clock checks.
        let c = PositiveProbeCache::with_capacity_and_ttl(8, Duration::from_millis(500));
        c.insert(h(1), vec![p(1)]);
        assert!(c.get(&h(1)).is_some());
        thread::sleep(Duration::from_millis(750));
        assert!(c.get(&h(1)).is_none());
        assert!(c.is_empty(), "expired entry should be evicted on read");
    }

    /// ADR 001 §Probe cache anchors the TTL at insertion. A read that re-stamped
    /// expiry during the LRU bump would keep a hot hash's probe results alive
    /// indefinitely — exactly the staleness the 15s window exists to bound, and
    /// it would ship green without this test.
    #[test]
    fn read_hit_does_not_refresh_ttl() {
        let c = PositiveProbeCache::with_capacity_and_ttl(8, Duration::from_millis(500));
        c.insert(h(1), vec![p(1)]);
        thread::sleep(Duration::from_millis(250));
        assert!(c.get(&h(1)).is_some());
        thread::sleep(Duration::from_millis(500));
        assert!(
            c.get(&h(1)).is_none(),
            "read-hit illegally extended the TTL"
        );
    }

    #[test]
    fn lru_eviction_at_cap_drops_oldest() {
        let c = PositiveProbeCache::with_capacity(2);
        c.insert(h(1), vec![p(1)]);
        c.insert(h(2), vec![p(2)]);
        c.insert(h(3), vec![p(3)]);
        assert!(c.get(&h(1)).is_none());
        assert!(c.get(&h(2)).is_some());
        assert!(c.get(&h(3)).is_some());
        assert_eq!(c.len(), 2);
    }

    /// Pins that `get`'s remove-then-`shift_insert(0, ..)` really is an MRU bump
    /// and not an accidental no-op. The non-`Copy` value forces a different
    /// dance than `negative_cache`'s single `shift_insert`, so its equivalent
    /// test does not cover this one.
    #[test]
    fn read_hit_bumps_lru_so_oldest_eviction_changes() {
        let c = PositiveProbeCache::with_capacity(2);
        c.insert(h(1), vec![p(1)]);
        c.insert(h(2), vec![p(2)]);
        assert!(c.get(&h(1)).is_some()); // bump h(1) → h(2) becomes LRU
        c.insert(h(3), vec![p(3)]);
        assert!(c.get(&h(1)).is_some());
        assert!(c.get(&h(2)).is_none());
        assert!(c.get(&h(3)).is_some());
    }

    #[test]
    fn reinsert_replaces_providers_and_does_not_grow_len() {
        let c = PositiveProbeCache::with_capacity(4);
        c.insert(h(1), vec![p(1), p(2)]);
        c.insert(h(1), vec![p(3)]);
        assert_eq!(c.get(&h(1)), Some(vec![p(3)]));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn invalidate_removes_the_entry() {
        let c = PositiveProbeCache::new();
        c.insert(h(1), vec![p(1)]);
        c.invalidate(&h(1));
        assert!(c.get(&h(1)).is_none());
        assert!(c.is_empty());
    }

    /// The documented "disabled" configuration, so no `disabled()` constructor
    /// needs to exist for production code to eventually call.
    #[test]
    fn a_zero_ttl_cache_never_hits() {
        let c = PositiveProbeCache::with_capacity_and_ttl(8, Duration::ZERO);
        c.insert(h(1), vec![p(1)]);
        assert!(c.get(&h(1)).is_none());
    }
}
