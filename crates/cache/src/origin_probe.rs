//! Live-origin probe memo (#1130 pt3): a bounded, TTL'd cache of
//! `HEAD`/`HeadObject` existence answers so a per-probe origin existence check
//! does not hammer the backend.
//!
//! `rescan_origins` (#1130) builds the zero-I/O `origin_held` index from
//! `enumerate()` ∪ pins, which for http/s3 covers pinned hashes ONLY (neither
//! backend lists). A non-pinned bucket object is therefore invisible to the
//! probe. This memo backs the per-probe fallback that consults the origin
//! directly (`CacheEngine::origin_probe_size`): it caches BOTH a positive answer
//! (`Present(size)`) under a long positive TTL and a negative one (`Absent`)
//! under a short negative TTL, so a flood of random-hash probes turns into at
//! most one `HeadObject` per hash per TTL window rather than one per probe,
//! while a stale `Absent` cannot hide newly-available own content for more
//! than the short negative window.
//!
//! The memo is deliberately a plain data structure with an injected clock
//! (`now: Instant`) so its expiry and capacity behaviour are unit-testable
//! without sleeping or a background sweeper. Expiry is lazy (on lookup); the
//! capacity bound is enforced on insert.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::Hash;

/// Default TTL for a memoised positive origin-probe answer
/// (`cache.origin_probe_ttl_sec`).
pub const DEFAULT_ORIGIN_PROBE_TTL: Duration = Duration::from_secs(15);
/// Default negative TTL (`cache.origin_probe_negative_ttl_sec`). Short on
/// purpose: it bounds how long a stale `Absent` can hide newly-available own
/// content. A random-hash flood never repeats a hash within any window, so a
/// short negative TTL barely changes the flood cost while capping the
/// fresh-content dark window.
pub const DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL: Duration = Duration::from_secs(2);
/// Default per-probe live-`HEAD` ceiling (`cache.origin_probe_timeout_ms`) — a
/// slow origin must never stall the probe hot path.
pub const DEFAULT_ORIGIN_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// Default cap on distinct memoised hashes (`cache.origin_probe_memo_capacity`).
/// Bounds memory under a random-hash probe flood.
pub const DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY: usize = 4096;

/// A memoised existence answer for a hash against the configured origins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    /// The origin holds the blob; carries the total byte size for the probe's
    /// `total_bytes`.
    Present(u64),
    /// No configured origin holds the blob (a `HEAD` 404, an unknown size, a
    /// transport error, or a timeout — all fold to "don't advertise").
    Absent,
}

impl Presence {
    /// The probe answer this presence yields: `Some(size)` advertises
    /// `has_blob: true`, `None` leaves it `false`.
    pub const fn size(self) -> Option<u64> {
        match self {
            Presence::Present(size) => Some(size),
            Presence::Absent => None,
        }
    }
}

/// Bounded, lazily-expiring memo of origin-probe answers.
#[derive(Debug)]
pub struct OriginProbeMemo {
    entries: HashMap<Hash, Entry>,
    positive_ttl: Duration,
    negative_ttl: Duration,
    timeout: Duration,
    capacity: usize,
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    presence: Presence,
    /// Instant at which this entry stops being valid.
    expires_at: Instant,
}

impl OriginProbeMemo {
    /// A memo with the given positive TTL, negative TTL, per-probe `HEAD`
    /// timeout, and capacity. A `capacity` of 0 is treated as 1 so the map can
    /// always hold the entry it just resolved.
    #[must_use]
    pub fn new(
        positive_ttl: Duration,
        negative_ttl: Duration,
        timeout: Duration,
        capacity: usize,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            positive_ttl,
            negative_ttl,
            timeout,
            capacity: capacity.max(1),
        }
    }

    /// The per-probe live-`HEAD` ceiling this memo was configured with.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// A live (non-expired) memoised answer for `hash`, or `None` on a miss.
    /// Expired entries are swept in passing so a stale answer is never returned
    /// and the map does not accrete dead entries on the read path.
    pub fn get(&mut self, hash: Hash, now: Instant) -> Option<Presence> {
        match self.entries.get(&hash) {
            Some(entry) if entry.expires_at > now => Some(entry.presence),
            Some(_) => {
                self.entries.remove(&hash);
                None
            }
            None => None,
        }
    }

    /// Record `presence` for `hash`, valid for one TTL from `now`. Enforces the
    /// capacity bound: expired entries are swept first, and if the map is still
    /// at capacity one arbitrary live entry is dropped to make room, so the memo
    /// can never exceed `capacity` distinct hashes regardless of probe volume.
    pub fn insert(&mut self, hash: Hash, presence: Presence, now: Instant) {
        let ttl = match presence {
            Presence::Present(_) => self.positive_ttl,
            Presence::Absent => self.negative_ttl,
        };
        let expires_at = now + ttl;
        // Refreshing an existing key never grows the map.
        if let std::collections::hash_map::Entry::Occupied(mut occupied) = self.entries.entry(hash)
        {
            occupied.insert(Entry {
                presence,
                expires_at,
            });
            return;
        }
        if self.entries.len() >= self.capacity {
            self.sweep_expired(now);
        }
        if self.entries.len() >= self.capacity {
            // Still full of live entries: evict one arbitrary key. HashMap
            // iteration order is unspecified, which is an acceptable eviction
            // policy for a best-effort existence cache (no LRU dependency).
            if let Some(&victim) = self.entries.keys().next() {
                self.entries.remove(&victim);
            }
        }
        self.entries.insert(
            hash,
            Entry {
                presence,
                expires_at,
            },
        );
    }

    /// Drop every entry whose TTL has passed.
    fn sweep_expired(&mut self, now: Instant) {
        self.entries.retain(|_, e| e.expires_at > now);
    }

    /// Number of memoised entries (test/introspection aid).
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the memo holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for OriginProbeMemo {
    fn default() -> Self {
        Self::new(
            DEFAULT_ORIGIN_PROBE_TTL,
            DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL,
            DEFAULT_ORIGIN_PROBE_TIMEOUT,
            DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{OriginProbeMemo, Presence};
    use crate::Hash;
    use std::time::{Duration, Instant};

    fn hash(seed: u8) -> Hash {
        Hash::from([seed; 32])
    }

    #[test]
    fn positive_answer_is_memoised_within_ttl() {
        let mut memo = OriginProbeMemo::new(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(2),
            16,
        );
        let t0 = Instant::now();
        assert_eq!(memo.get(hash(1), t0), None, "cold lookup misses");
        memo.insert(hash(1), Presence::Present(4096), t0);
        assert_eq!(
            memo.get(hash(1), t0 + Duration::from_secs(9)),
            Some(Presence::Present(4096)),
            "answer is live within the TTL",
        );
    }

    #[test]
    fn negative_answer_is_memoised_too() {
        let mut memo = OriginProbeMemo::new(
            Duration::from_secs(10),
            Duration::from_secs(2),
            Duration::from_secs(2),
            16,
        );
        let t0 = Instant::now();
        memo.insert(hash(2), Presence::Absent, t0);
        assert_eq!(
            memo.get(hash(2), t0 + Duration::from_secs(1)),
            Some(Presence::Absent),
            "a 404 is cached so a random-hash flood does not re-HEAD every probe",
        );
        assert_eq!(
            memo.get(hash(2), t0 + Duration::from_secs(3)),
            None,
            "the negative TTL, not the positive one, bounds how long Absent is honored",
        );
    }

    #[test]
    fn entry_expires_after_ttl_and_is_swept_on_read() {
        let mut memo = OriginProbeMemo::new(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(2),
            16,
        );
        let t0 = Instant::now();
        memo.insert(hash(3), Presence::Present(1), t0);
        assert_eq!(
            memo.get(hash(3), t0 + Duration::from_secs(11)),
            None,
            "expired"
        );
        assert!(memo.is_empty(), "expired entry is swept off the read path");
    }

    #[test]
    fn capacity_is_never_exceeded_under_live_pressure() {
        let cap: usize = 4;
        // TTL far longer than the (instantaneous) test so nothing expires: this
        // exercises the live-eviction arm, not the sweep.
        let mut memo = OriginProbeMemo::new(
            Duration::from_secs(100),
            Duration::from_secs(100),
            Duration::from_secs(2),
            cap,
        );
        let t0 = Instant::now();
        // Insert more distinct live hashes than capacity.
        for i in 0..(cap + 6) {
            let seed = u8::try_from(i).unwrap_or(u8::MAX);
            memo.insert(hash(seed), Presence::Absent, t0);
            assert!(memo.len() <= cap, "memo stays within its capacity bound");
        }
        assert_eq!(memo.len(), cap, "memo saturates exactly at capacity");
    }

    #[test]
    fn expired_entries_are_reclaimed_before_evicting_live_ones() {
        let cap = 2;
        let mut memo = OriginProbeMemo::new(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(2),
            cap,
        );
        let t0 = Instant::now();
        memo.insert(hash(10), Presence::Absent, t0);
        memo.insert(hash(11), Presence::Absent, t0);
        // A later insert past the first two's TTL should reclaim expired space
        // rather than the map growing.
        let t1 = t0 + Duration::from_secs(11);
        memo.insert(hash(12), Presence::Present(9), t1);
        assert!(memo.len() <= cap);
        assert_eq!(
            memo.get(hash(12), t1),
            Some(Presence::Present(9)),
            "the fresh entry is retained",
        );
    }

    #[test]
    fn refreshing_existing_key_does_not_grow_map() {
        let mut memo = OriginProbeMemo::new(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(2),
            2,
        );
        let t0 = Instant::now();
        memo.insert(hash(20), Presence::Absent, t0);
        memo.insert(hash(20), Presence::Present(5), t0);
        assert_eq!(memo.len(), 1);
        assert_eq!(
            memo.get(hash(20), t0),
            Some(Presence::Present(5)),
            "refreshed in place"
        );
    }

    #[test]
    fn negative_entries_expire_on_the_short_ttl_while_positive_ones_persist() {
        // positive 10s, negative 2s.
        let mut memo = OriginProbeMemo::new(
            Duration::from_secs(10),
            Duration::from_secs(2),
            Duration::from_secs(2),
            16,
        );
        let t0 = Instant::now();
        memo.insert(hash(1), Presence::Present(4096), t0);
        memo.insert(hash(2), Presence::Absent, t0);

        // At t0 + 3s the negative entry is gone (fresh content can re-probe),
        // but the positive entry is still live.
        let t = t0 + Duration::from_secs(3);
        assert_eq!(memo.get(hash(2), t), None, "negative expires on the 2s TTL");
        assert_eq!(
            memo.get(hash(1), t),
            Some(Presence::Present(4096)),
            "positive still live on the 10s TTL",
        );
    }

    #[test]
    fn presence_size_maps_to_probe_answer() {
        assert_eq!(Presence::Present(42).size(), Some(42));
        assert_eq!(Presence::Absent.size(), None);
    }
}
