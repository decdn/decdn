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
/// (`cache.origin_probe_ttl_sec`). Canonical seconds value from
/// `decdn-config-types`, wrapped as a `Duration`.
pub const DEFAULT_ORIGIN_PROBE_TTL: Duration =
    Duration::from_secs(decdn_config_types::DEFAULT_ORIGIN_PROBE_TTL_SEC);
/// Default negative TTL (`cache.origin_probe_negative_ttl_sec`). Short on
/// purpose: it bounds how long a stale `Absent` can hide newly-available own
/// content. A random-hash flood never repeats a hash within any window, so a
/// short negative TTL barely changes the flood cost while capping the
/// fresh-content dark window.
pub const DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL: Duration =
    Duration::from_secs(decdn_config_types::DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL_SEC);
/// Default fault TTL (`cache.origin_probe_fault_ttl_sec`). Longer than the
/// negative TTL because a client retrying one hash against a failing origin is
/// the common shape, and this memo is keyed per hash: it collapses the repeats
/// of a hash, not the namespace — during an outage, N distinct hashes still
/// cost N live `HEAD`s per window. Shorter than the positive TTL because a
/// memoised fault costs client-visible availability until it expires, so a
/// recovered origin must be re-probed promptly.
pub const DEFAULT_ORIGIN_PROBE_FAULT_TTL: Duration =
    Duration::from_secs(decdn_config_types::DEFAULT_ORIGIN_PROBE_FAULT_TTL_SEC);
/// Default per-probe live-`HEAD` ceiling (`cache.origin_probe_timeout_ms`) — a
/// slow origin must never stall the probe hot path.
pub const DEFAULT_ORIGIN_PROBE_TIMEOUT: Duration =
    Duration::from_millis(decdn_config_types::DEFAULT_ORIGIN_PROBE_TIMEOUT_MS);
/// Default cap on distinct memoised hashes (`cache.origin_probe_memo_capacity`).
/// Bounds memory under a random-hash probe flood.
#[allow(clippy::cast_possible_truncation)] // 4096 fits usize on every supported target.
pub const DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY: usize =
    decdn_config_types::DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY as usize;

/// A memoised existence answer for a hash against the configured origins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    /// The origin holds the blob; carries the total byte size for the probe's
    /// `total_bytes`.
    Present(u64),
    /// No configured origin holds the blob (a `HEAD` 404, an unknown size,
    /// or no origin — an authoritative "don't advertise").
    Absent,
    /// The probe could not get an authoritative answer: a transport error or
    /// a timeout (a `HEAD` overrun). Distinct from `Absent` on purpose — a
    /// caller that would otherwise sign an authoritative `NotFound` must not
    /// do so on a fault, and a fault is worth memoising under its own TTL so
    /// a failing origin is not re-probed on every request for its namespace.
    Fault,
}

impl Presence {
    /// The probe answer this presence yields: `Some(size)` advertises
    /// `has_blob: true`, `None` leaves it `false`.
    pub const fn size(self) -> Option<u64> {
        match self {
            Presence::Present(size) => Some(size),
            Presence::Absent | Presence::Fault => None,
        }
    }
}

/// Bounded, lazily-expiring memo of origin-probe answers.
#[derive(Debug)]
pub struct OriginProbeMemo {
    entries: HashMap<Hash, Entry>,
    positive_ttl: Duration,
    negative_ttl: Duration,
    fault_ttl: Duration,
    timeout: Duration,
    capacity: usize,
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    presence: Presence,
    /// Instant at which this entry stops being valid.
    expires_at: Instant,
}

/// Live-origin probe policy: the three answer TTLs, the per-probe `HEAD`
/// ceiling, and the memo's capacity, resolved from the `cache.origin_probe_*`
/// knobs.
///
/// Passed as one value rather than five positional arguments because four of
/// them are `Duration`: any transposition of the TTLs and the timeout would
/// compile and pass every test, while quietly changing how long a fault or an
/// absence stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OriginProbePolicy {
    /// TTL for a memoised `Present` answer (`cache.origin_probe_ttl_sec`).
    pub positive_ttl: Duration,
    /// TTL for a memoised `Absent` answer
    /// (`cache.origin_probe_negative_ttl_sec`).
    pub negative_ttl: Duration,
    /// TTL for a memoised `Fault` answer
    /// (`cache.origin_probe_fault_ttl_sec`).
    pub fault_ttl: Duration,
    /// Per-probe ceiling on the live `HEAD`/`HeadObject`
    /// (`cache.origin_probe_timeout_ms`).
    pub timeout: Duration,
    /// Max distinct hashes held in the memo
    /// (`cache.origin_probe_memo_capacity`).
    pub capacity: usize,
}

impl Default for OriginProbePolicy {
    fn default() -> Self {
        Self {
            positive_ttl: DEFAULT_ORIGIN_PROBE_TTL,
            negative_ttl: DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL,
            fault_ttl: DEFAULT_ORIGIN_PROBE_FAULT_TTL,
            timeout: DEFAULT_ORIGIN_PROBE_TIMEOUT,
            capacity: DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY,
        }
    }
}

impl OriginProbeMemo {
    /// A memo under `policy`. A `capacity` of 0 is treated as 1 so the map can
    /// always hold the entry it just resolved.
    #[must_use]
    pub fn new(policy: OriginProbePolicy) -> Self {
        Self {
            entries: HashMap::new(),
            positive_ttl: policy.positive_ttl,
            negative_ttl: policy.negative_ttl,
            fault_ttl: policy.fault_ttl,
            timeout: policy.timeout,
            capacity: policy.capacity.max(1),
        }
    }

    /// How long a memoised `Fault` answer stands before the origin is
    /// re-probed.
    #[must_use]
    pub const fn fault_ttl(&self) -> Duration {
        self.fault_ttl
    }

    /// The per-probe live-`HEAD` ceiling this memo was configured with.
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
            Presence::Fault => self.fault_ttl,
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
        Self::new(OriginProbePolicy::default())
    }
}

#[cfg(test)]
mod tests;
