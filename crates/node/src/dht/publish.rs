//! DHT republish scheduler (ADR 022 §STORE Flow & §Bootstrap).
//!
//! When a hash becomes advertisable — a whole-blob commit, or a ranged
//! admit that completes a discovery block
//! ([`decdn_cache::CacheEngine::subscribe_inserts`]) — and has not announced
//! yet, the scheduler eagerly publishes it to its K+3 closest peers with
//! per-hash `StoreRequest`s (a size-1 batch buys nothing) and schedules its
//! next republish. A hash that has announced waits for its cycle, which
//! carries its widened coverage.
//!
//! On a periodic per-record jittered timer the scheduler:
//!
//!  1. Drains every record whose republish window has come due. On a
//!     tick where one has, it also drains the records due within
//!     [`REPUBLISH_LOOKAHEAD`], in due order, while no receiver's set
//!     passes [`LOOKAHEAD_RECEIVER_CAP`] (ADR 022 §STORE Flow "Drain
//!     cycle"). It keeps those still held in the cache. Per-record jitter spreads due times so thinly that a
//!     1 s tick alone holds about one hash; the look-ahead is what gives
//!     a steady-state batch more than one.
//!  2. Groups the drained hashes by receiver — each hash's K+3 closest
//!     peers — so all hashes bound for one receiver ride a single
//!     `BatchStoreRequest { hashes, holder: self }`, split at the
//!     [`MAX_BATCH_STORE_HASHES`] wire cap. This is one RPC per
//!     publisher-receiver pair per cycle (ADR 022 §STORE Flow Batched
//!     STORE & §DHT Bandwidth Analysis). Every DHT node implements
//!     `BatchStore`, so there is no per-hash fallback.
//!  3. Schedules the next republish at `now + uniform(30 min, 50 min)`
//!     per ADR 022 §STORE Flow step 3. Jitter is drawn independently
//!     per record so the next republish window for a given hash is
//!     not predictable from outside the publisher. Taking a record early
//!     only shortens its refresh gap: the draw keeps it at or below
//!     50 min, and [`REPUBLISH_LOOKAHEAD`] keeps it at or above 25 min.
//!
//! Cold-start (ADR 022 §Bootstrap "Cold-start re-publish scheduling"):
//! a node booting with C cached blobs draws the first republish offset
//! from `uniform(0, 40 min)` per blob, NOT `uniform(30, 50)`. The
//! single-tick "republish everything on the next scheduler tick"
//! pattern is explicitly non-conforming — it saturates the per-peer
//! rate limit at every receiver. The cold-start uniform-0-40-min
//! window matches the steady-state mean cycle so bootstrap rate equals
//! steady-state rate by construction.
//!
//! Lag sweep (ADR 022 §Bootstrap "Re-seed after a lost commit
//! window"): the commit-event channel is bounded and best-effort, and
//! the cache retains nothing it drops, so a `Lagged` receiver cannot
//! backfill. The scheduler instead re-derives the held set
//! (`holder_snapshot`) and seeds whatever is missing. Seeding is
//! idempotent and uses the same `uniform(0, 40 min)` window as cold
//! start, so a sweep costs one cold-start window rather than a burst —
//! the immediate bulk republish is non-conforming here for the same
//! reason it is at boot.
//!
//! Implementation: a single tokio task drives all records via a
//! `BinaryHeap<(due_at, hash)>` and a 1 s ticker. The heap is small (one
//! entry per cached blob, capped by the cache's blob count). Sending does
//! not hold the heap mutex; a drain cycle's batches go out in a spawned
//! task so a slow receiver doesn't stall the rest of the schedule.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::{Endpoint, EndpointAddr, PublicKey};
use rand::RngExt;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::dht::chain_projection::with_lock;
use crate::dht::client;
use crate::dht::routing::{NodeId, RoutingTable};
use decdn_protocol::ContentHash;
use decdn_protocol::Coverage;
use decdn_protocol::dht::MAX_BATCH_STORE_HASHES;

/// Number of receivers per republish (ADR 022 §STORE Flow step 1:
/// "K+3 closest nodes to H"). Three slots beyond `K=20` give the
/// record headroom against an attacker who suppresses up to K
/// receivers.
pub const REPUBLISH_FANOUT: usize = 23;

/// Steady-state minimum republish gap. ADR 022 §STORE Flow step 3.
pub const STEADY_STATE_MIN: Duration = Duration::from_mins(30);

/// Steady-state maximum republish gap. ADR 022 §STORE Flow step 3.
pub const STEADY_STATE_MAX: Duration = Duration::from_mins(50);

/// Cold-start jitter window upper bound (ADR 022 §Bootstrap "Cold-
/// start re-publish scheduling"). The lower bound is 0 — a record
/// drawn at 0 fires on the next tick.
pub const COLD_START_MAX: Duration = Duration::from_mins(40);

/// How far ahead a drain cycle reaches for records not yet due (ADR 022
/// §STORE Flow "Drain cycle"). Sending early only shortens a record's refresh
/// gap, which the [`STEADY_STATE_MAX`] draw bound keeps inside the
/// receiver-anchored 1 h TTL. This bound keeps the gap at or above 25 min,
/// which caps the extra refresh load the look-ahead adds.
pub const REPUBLISH_LOOKAHEAD: Duration = Duration::from_mins(5);

/// The most hashes one receiver's set holds once the look-ahead has pulled
/// records forward (ADR 022 §STORE Flow "Drain cycle"). A full batch fits the
/// 40-token per-peer burst of ADR 022 §DHT Rate Limiting with 8 tokens to
/// spare for the same publisher's lookups and eager `Store`s. Records already
/// due are not held to it.
pub const LOOKAHEAD_RECEIVER_CAP: usize = 32;

/// The shortest time between a drain cycle and a later cycle that pulls
/// records forward (ADR 022 §STORE Flow "Drain cycle"). The per-receiver cap
/// bounds one cycle, not two a second apart: after a backlog empties a
/// receiver's per-peer bucket, a look-ahead batch on the next tick would find
/// about 20 tokens and have its tail refused. At the 20-token/s per-peer
/// refill, 2 s refill the whole 40-token burst.
pub const LOOKAHEAD_MIN_SPACING: Duration = Duration::from_secs(2);

// A record rescheduled at `now + STEADY_STATE_MIN` must fall outside the
// look-ahead, or every cycle would pull it forward again.
const _: () = assert!(REPUBLISH_LOOKAHEAD.as_secs() < STEADY_STATE_MIN.as_secs());
// A look-ahead set fits in one `BatchStore`.
const _: () = assert!(LOOKAHEAD_RECEIVER_CAP <= MAX_BATCH_STORE_HASHES);

/// Per-record scheduler entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    /// Wall-clock-anchored due time in microseconds since `UNIX_EPOCH`.
    due_us: u64,
    /// Hash being republished.
    hash: ContentHash,
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Default `BinaryHeap` is a max-heap; we want the *earliest*
        // due time at the top. The driver wraps with `Reverse` so
        // this `Ord` is just the natural `(due_us, hash)` order.
        self.due_us
            .cmp(&other.due_us)
            .then_with(|| self.hash.cmp(&other.hash))
    }
}

/// Schedule a single republish for `hash`. Used by the
/// `subscribe_inserts` event loop and by cold-start startup.
fn schedule_at(heap: &mut BinaryHeap<Reverse<Entry>>, hash: ContentHash, due_us: u64) {
    heap.push(Reverse(Entry { due_us, hash }));
}

/// Wall-clock now in microseconds since `UNIX_EPOCH`. Matches the
/// `now_us` helper in [`crate::handlers::dht`] — falls back to 0 if
/// the host clock is mis-set. The handler-side helper additionally
/// logs the fallback at error level; the scheduler only needs the
/// numeric value so we don't re-log here.
fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// Draw a uniform jitter offset (in microseconds) in `[lo, hi]`.
fn jitter_us(lo: Duration, hi: Duration) -> u64 {
    let lo_us = u64::try_from(lo.as_micros()).unwrap_or(u64::MAX);
    let hi_us = u64::try_from(hi.as_micros()).unwrap_or(u64::MAX);
    let mut rng = rand::rng();
    if hi_us <= lo_us {
        return lo_us;
    }
    rng.random_range(lo_us..=hi_us)
}

/// Republish-scheduler handle. Owns the per-hash heap and the abort
/// signal so the runtime can stop it on shutdown.
///
/// # The authoritative-due invariant
///
/// A `BinaryHeap` cannot remove or reschedule an interior entry, so any
/// rescheduling leaves the old entry behind as a tombstone. `scheduled` is
/// therefore the authority: it maps each live hash to the ONE `due_us` that
/// counts, and `drain_cycle` discards a popped entry whose `due_us`
/// disagrees. Without that check a tombstone is resurrected the moment its
/// hash is scheduled again — it pops already-overdue, passes a
/// membership-only test, and buys a spurious republish. Two call patterns hit
/// that: a re-seed racing the tick path's drain-then-reschedule, and
/// [`Self::unschedule`] followed by a later re-seed.
#[allow(missing_debug_implementations)]
pub struct RepublishScheduler {
    heap: Arc<Mutex<BinaryHeap<Reverse<Entry>>>>,
    /// Live hash -> its authoritative `due_us`. A hash absent here has no
    /// live entry, whatever the heap still holds.
    scheduled: Arc<Mutex<HashMap<ContentHash, u64>>>,
    /// Hashes a peer accepted a `Store` with non-empty coverage for, from an
    /// eager publish or a drain cycle's `BatchStore`. This, not
    /// `scheduled`, is what dedupes the eager publish on cache events. A bulk
    /// seed schedules hashes that have published nothing yet — a partial that
    /// covered no block when the seed walked it — and the block it completes
    /// before its due time must still publish at once. [`Self::unschedule`]
    /// clears the mark.
    announced: Arc<Mutex<HashSet<ContentHash>>>,
}

impl RepublishScheduler {
    /// Construct an empty scheduler.
    #[must_use]
    pub fn new() -> Self {
        Self {
            heap: Arc::new(Mutex::new(BinaryHeap::new())),
            scheduled: Arc::new(Mutex::new(HashMap::new())),
            announced: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Number of hashes currently scheduled.
    #[must_use]
    pub fn len(&self) -> usize {
        with_lock(&self.scheduled, "dht republish scheduled set", |s| s.len())
    }

    /// Whether the scheduler is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        with_lock(&self.scheduled, "dht republish scheduled set", |s| {
            s.is_empty()
        })
    }

    /// Schedule `hash` with the steady-state jitter window (30–50 min),
    /// superseding any due time it already has.
    ///
    /// A fresh draw per cycle is ADR 022 §STORE Flow step 3. Superseding is
    /// safe because the displaced heap entry no longer matches the
    /// authoritative due time and `drain_cycle` discards it.
    pub fn schedule_steady(&self, hash: ContentHash) {
        let offset = jitter_us(STEADY_STATE_MIN, STEADY_STATE_MAX);
        self.schedule_with_offset(hash, offset);
    }

    /// Schedule `hash` with the steady-state jitter window (30–50 min) only if
    /// it has no live entry. Returns whether it was added.
    ///
    /// The cache-event path uses this so that a repeated event (a ranged fill
    /// announces once per completed block) keeps the due time the hash already
    /// has rather than pushing it back.
    pub fn schedule_steady_if_absent(&self, hash: ContentHash) -> bool {
        let offset = jitter_us(STEADY_STATE_MIN, STEADY_STATE_MAX);
        self.schedule_if_absent(hash, offset)
    }

    /// Record that a peer accepted a `Store` with non-empty coverage for `hash`.
    pub fn mark_announced(&self, hash: ContentHash) {
        with_lock(&self.announced, "dht republish announced set", |a| {
            a.insert(hash);
        });
    }

    /// Whether a peer accepted a `Store` with non-empty coverage for `hash`
    /// since it was last unscheduled.
    #[must_use]
    pub fn is_announced(&self, hash: &ContentHash) -> bool {
        with_lock(&self.announced, "dht republish announced set", |a| {
            a.contains(hash)
        })
    }

    /// Batch-schedule every hash in `iter` with an independent
    /// cold-start jitter draw (uniform(0, 40 min) per hash, NOT a
    /// shared timestamp). Callers pass a `holder_snapshot` result,
    /// or `origin_held_snapshot` for the origin-only rescan — never
    /// `access_times_snapshot`, which is eviction-recency state rather
    /// than the on-disk set; see the `iter_hashes` rustdoc.
    ///
    /// Seeding leaves an already-scheduled hash alone: it keeps its existing
    /// due time rather than being pulled earlier by a re-seed. The lag sweep
    /// in [`run_republish`] and the periodic origin rescan both run against a
    /// populated scheduler, so without this a burst of re-seeds would keep
    /// dragging every due time toward the present.
    ///
    /// Returns the number of hashes *newly* scheduled — useful for
    /// the startup log line so operators can see how big the
    /// cold-start queue is, and for the sweep so a no-op sweep is
    /// distinguishable from a repair.
    pub fn seed_cold_start<I>(&self, iter: I) -> usize
    where
        I: IntoIterator<Item = ContentHash>,
    {
        let mut count = 0usize;
        for hash in iter {
            let offset = jitter_us(Duration::ZERO, COLD_START_MAX);
            if self.schedule_if_absent(hash, offset) {
                count = count.saturating_add(1);
            }
        }
        count
    }

    /// Set `hash`'s authoritative due time to `now + offset_us`, replacing any
    /// existing one.
    fn schedule_with_offset(&self, hash: ContentHash, offset_us: u64) {
        self.with_slots(offset_us, |heap, scheduled, due_us| {
            scheduled.insert(hash, due_us);
            schedule_at(heap, hash, due_us);
            true
        });
    }

    /// Schedule `hash` at `now + offset_us` only if it has no live entry.
    /// Returns whether it was added.
    ///
    /// "Already scheduled" means "has a live entry that will drain", never
    /// "was ever scheduled" — a hash the tick path just drained is absent
    /// again and re-seeds normally.
    fn schedule_if_absent(&self, hash: ContentHash, offset_us: u64) -> bool {
        self.with_slots(offset_us, |heap, scheduled, due_us| {
            if scheduled.contains_key(&hash) {
                return false;
            }
            scheduled.insert(hash, due_us);
            schedule_at(heap, hash, due_us);
            true
        })
    }

    /// Run `f` under both locks with a freshly computed due time.
    ///
    /// Both guards are held across the read-and-push so a concurrent
    /// `drain_cycle` cannot act on a half-applied change. Acquired
    /// heap-then-scheduled to match `drain_cycle`'s order — the reverse would
    /// risk deadlocking against it, which is why the [`with_lock`] calls nest
    /// in that order rather than running side by side.
    fn with_slots<F>(&self, offset_us: u64, f: F) -> bool
    where
        F: FnOnce(&mut BinaryHeap<Reverse<Entry>>, &mut HashMap<ContentHash, u64>, u64) -> bool,
    {
        let due_us = now_us().saturating_add(offset_us);
        with_lock(&self.heap, "dht republish heap", |heap| {
            with_lock(
                &self.scheduled,
                "dht republish scheduled set",
                |scheduled| f(heap, scheduled, due_us),
            )
        })
    }

    /// Drain one republish cycle: every hash whose authoritative due time has
    /// passed, then — only if at least one has — the hashes due by
    /// `horizon_us`, in due order, while `admit` accepts them.
    ///
    /// `admit(hash, mandatory)` sees every live hash the cycle takes, plus the
    /// one look-ahead hash it refuses. Every due hash reaches `admit` before
    /// any look-ahead hash. `mandatory` is `true` for a hash already due: it
    /// always drains, so `admit` must record it, and its answer is ignored.
    /// For a look-ahead hash `admit` decides, and the first refusal ends the
    /// cycle with that hash and every later one still scheduled. The
    /// look-ahead never runs on its own: pulling hashes forward on a tick with
    /// nothing due would only shift the whole schedule earlier and batch
    /// nothing. A `horizon_us` at or below `now_us` turns it off.
    ///
    /// A popped entry counts only if `scheduled` still names that hash at
    /// exactly that `due_us`. Anything else is a tombstone left by a
    /// supersede or an unschedule, and is dropped without reaching `admit`.
    fn drain_cycle<F>(&self, now_us: u64, horizon_us: u64, mut admit: F) -> Vec<ContentHash>
    where
        F: FnMut(&ContentHash, bool) -> bool,
    {
        let mut out = Vec::new();
        with_lock(&self.heap, "dht republish heap", |heap| {
            with_lock(
                &self.scheduled,
                "dht republish scheduled set",
                |scheduled| {
                    while let Some(Reverse(Entry { due_us, hash })) = heap.peek().copied() {
                        if due_us > now_us.max(horizon_us) {
                            break;
                        }
                        if scheduled.get(&hash) != Some(&due_us) {
                            heap.pop();
                            continue;
                        }
                        let mandatory = due_us <= now_us;
                        if mandatory {
                            admit(&hash, true);
                        } else if out.is_empty() || !admit(&hash, false) {
                            break;
                        }
                        heap.pop();
                        out.push(hash);
                        // Drop the live entry so the next `schedule_*` for this hash
                        // re-adds it. The caller re-schedules after publishing.
                        scheduled.remove(&hash);
                    }
                },
            );
        });
        out
    }

    /// Drain every hash whose authoritative due time has passed, with no
    /// look-ahead.
    #[cfg(test)]
    fn drain_due(&self, now_us: u64) -> Vec<ContentHash> {
        self.drain_cycle(now_us, now_us, |_, _| true)
    }

    /// Unschedule `hash` — used when the cache evicts the blob, or holds no
    /// whole block of it. Clears its announced mark, so the next cache event
    /// for it publishes eagerly again.
    pub fn unschedule(&self, hash: &ContentHash) {
        with_lock(&self.scheduled, "dht republish scheduled set", |s| {
            s.remove(hash);
        });
        with_lock(&self.announced, "dht republish announced set", |a| {
            a.remove(hash);
        });
        // The heap entry is left in place; `drain_cycle` discards it, and a
        // later re-schedule cannot resurrect it because its `due_us` will no
        // longer match the authoritative one.
    }
}

impl Default for RepublishScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Every hash this node holds and may advertise, as one snapshot.
///
/// The union of the origin-held index and — when the node relays foreign
/// namespaces — the blobs in the local store, complete or partial. The set is
/// a superset of what gets announced: the due-time gate ([`cache_still_holds`])
/// drops a partial that covers no discovery block. This is the input to both
/// *full* seeds of the republish scheduler: bring-up cold start and the
/// lag sweep in [`run_republish`] need the same set, and computing it in one
/// place is what keeps them from drifting apart. The periodic origin rescan is
/// not one of them — it seeds the origin-held half alone and deliberately does
/// not walk the store.
#[derive(Debug)]
pub(crate) struct HolderSnapshot {
    /// The union. Deduplicated, so a hash that is both stored and origin-held
    /// draws one jitter offset rather than two.
    pub(crate) hashes: HashSet<decdn_cache::Hash>,
    /// `Some` when the store walk failed. The origin-held half is still in
    /// `hashes`; the caller decides how loudly to report the degradation, since
    /// bring-up and the sweep phrase it differently.
    pub(crate) store_error: Option<decdn_cache::CacheError>,
    /// Size probes that faulted on the rescan the origin-held half comes from.
    ///
    /// Carried from the index rather than measured here, so it describes a pass
    /// that may be as old as the rescan cadence — kept separate from
    /// `ownership_probe_faults` for that reason, and because it is already
    /// metered on the cache's own counter.
    pub(crate) rescan_probe_faults: u64,
    /// Origins whose listing failed on that rescan. More severe than a probe
    /// fault: no candidate was produced, so nothing could be carried forward.
    pub(crate) rescan_enumerate_failures: u64,
    /// Stored hashes this walk could not put to the origin — the ownership test
    /// applied under the origin-only policy, answered neither way.
    ///
    /// Measured by this snapshot, so it describes right now. Zero when the node
    /// relays foreign namespaces, which asks the origin nothing.
    pub(crate) ownership_probe_faults: u64,
}

impl HolderSnapshot {
    /// Whether this snapshot is short of what the node actually holds.
    pub(crate) const fn is_degraded(&self) -> bool {
        self.store_error.is_some()
            || self.rescan_probe_faults > 0
            || self.rescan_enumerate_failures > 0
            || self.ownership_probe_faults > 0
    }

    /// Meter and log every way this snapshot came up short, returning
    /// [`Self::is_degraded`].
    ///
    /// One place, because both full seeds — bring-up cold start and the lag
    /// sweep — degrade identically and differ only in what they call themselves.
    /// `context` names the caller in each line.
    pub(crate) fn report_degradation(
        &self,
        metrics: &crate::metrics::Metrics,
        context: &str,
    ) -> bool {
        if let Some(err) = &self.store_error {
            metrics.dht_republish_seed_store_walk_failure();
            tracing::warn!(
                context,
                error = %err.display_chain(),
                "dht republish: seed could not walk the store and covered the \
                 origin-held half only. Blobs held only in the store stay \
                 un-republished until a later seed walks it successfully"
            );
        }
        // Only this walk's own faults. The rescan's are already on
        // `decdn_cache_origin_probe_failures_total`, and re-counting them here
        // would move the seed's counter on a node that asks the origin nothing.
        if self.ownership_probe_faults > 0 {
            metrics.dht_republish_seed_origin_probe_failures(self.ownership_probe_faults);
            tracing::warn!(
                context,
                faults = self.ownership_probe_faults,
                "dht republish: seed could not put every stored hash to its \
                 origin; each one it could not confirm is left out of the \
                 announce set rather than advertised"
            );
        }
        if self.rescan_probe_faults > 0 || self.rescan_enumerate_failures > 0 {
            tracing::warn!(
                context,
                faults = self.rescan_probe_faults,
                enumerate_failures = self.rescan_enumerate_failures,
                "dht republish: the origin-held half of this seed comes from a \
                 rescan that could not resolve everything; entries it carried \
                 forward may name content the origin has dropped, and an origin \
                 it could not list contributed nothing at all"
            );
        }
        self.is_degraded()
    }
}

/// Collect the [`HolderSnapshot`] for `cache`.
///
/// The store half is every blob the store holds, complete or partial
/// ([`decdn_cache::CacheEngine::iter_hashes`]); the due-time gate
/// ([`cache_still_holds`]) later drops a partial that covers no discovery block.
///
/// Under the origin-only policy (`relay_foreign_namespaces == false`) the store
/// half is not taken wholesale — the store may hold leftover foreign content
/// from before the toggle was set, and announcing it would advertise blobs the
/// serve gate now declines. Each stored hash is instead put to
/// `origin_probe_presence`, the same question the origin-only serve gate asks,
/// so the snapshot advertises exactly what this node would serve.
///
/// That check is what recovers a node's *own* store-only content. The
/// origin-held index covers enumerable origins plus present pins, and a remote
/// origin (S3/R2/HTTP) deliberately does not enumerate — so an unpinned object
/// this node owns, committed but with its insert event dropped, is in neither
/// half without it. `Fault` is skipped rather than admitted: a transport blip
/// must not turn into an advertisement for content the serve path might then
/// refuse. A fault is memoised for `cache.origin_probe_fault_ttl_sec`, so a
/// sweep landing inside that window re-reads the same fault rather than
/// re-probing — the hash stays out of the announce set until the memo expires,
/// which bounds how long a blip suppresses it and is why the fault TTL is
/// capped below the positive TTL. Skipping is still a hash this node holds and
/// does not announce, so each one counts toward `origin_probe_faults` —
/// otherwise an origin-only node drops its own store-only content on a blip and
/// reports a healthy snapshot.
///
/// Never fails: a store-walk error degrades to the origin-held half rather than
/// yielding nothing, because a partial announce strictly beats none. Every way
/// the set can come up short reports itself: `store_error` when the walk failed
/// outright, `rescan_probe_faults` and `ownership_probe_faults` for questions
/// the origin would not answer — the last rescan's and this walk's own
/// ownership tests — and `rescan_enumerate_failures` for an origin that could
/// not be listed at all.
pub(crate) async fn holder_snapshot(
    cache: &decdn_cache::CacheEngine,
    relay_foreign_namespaces: bool,
) -> HolderSnapshot {
    let report = cache.origin_held_snapshot();
    let mut hashes = report.hashes;
    let mut ownership_probe_faults = 0u64;
    let mut store_error = None;
    match cache.iter_hashes().await {
        Ok(stored) => {
            for hash in stored {
                if hashes.contains(&hash) {
                    continue;
                }
                if relay_foreign_namespaces {
                    hashes.insert(hash);
                    continue;
                }
                match cache.origin_probe_presence(hash).await {
                    decdn_cache::OriginPresence::Present(_) => {
                        hashes.insert(hash);
                    }
                    decdn_cache::OriginPresence::Absent => {}
                    decdn_cache::OriginPresence::Fault => {
                        ownership_probe_faults = ownership_probe_faults.saturating_add(1);
                    }
                }
            }
        }
        Err(err) => store_error = Some(err),
    }
    HolderSnapshot {
        hashes,
        store_error,
        rescan_probe_faults: report.probe_faults,
        rescan_enumerate_failures: report.enumerate_failures,
        ownership_probe_faults,
    }
}

/// What one lag sweep did, for the completion log.
///
/// `degraded` rides along so the completion line names the outcome: the warnings
/// come from inside [`lag_sweep`], several frames away, and an unqualified
/// "complete" would read as a clean run to anyone grepping for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SweepOutcome {
    /// Hashes this sweep newly scheduled.
    reseeded: usize,
    /// Whether this sweep could not resolve everything the node holds — a failed
    /// store walk, a faulted origin probe, or an origin that could not be listed.
    degraded: bool,
}

/// Re-seed the republish scheduler from local state after the cache-commit
/// event window was lost.
///
/// Seeding draws `uniform(0, 40 min)` per record, so a sweep of C blobs spreads
/// over the steady-state mean cycle instead of firing as one burst — ADR 022
/// §Bootstrap makes the single-tick bulk republish non-conforming for exactly
/// this reason, which is why the sweep goes through the scheduler and never
/// calls [`publish_batch`] directly.
async fn lag_sweep(
    cache: &decdn_cache::CacheEngine,
    relay_foreign_namespaces: bool,
    scheduler: &RepublishScheduler,
    metrics: &crate::metrics::Metrics,
) -> SweepOutcome {
    let snapshot = holder_snapshot(cache, relay_foreign_namespaces).await;
    let degraded = snapshot.report_degradation(metrics, "lag sweep");
    let reseeded = scheduler.seed_cold_start(
        snapshot
            .hashes
            .into_iter()
            .map(|h| ContentHash::from_bytes(*h.as_bytes())),
    );
    metrics.dht_republish_sweep_reseeded(u64::try_from(reseeded).unwrap_or(u64::MAX));
    SweepOutcome { reseeded, degraded }
}

/// Sweep slot: no worker, and none requested.
const SWEEP_IDLE: u8 = 0;
/// A worker owns the slot; nothing queued behind it.
const SWEEP_RUNNING: u8 = 1;
/// A worker owns the slot and a further sweep is queued behind it.
const SWEEP_QUEUED: u8 = 2;

/// RAII release for the sweep slot, for the abandon paths only.
///
/// The clean path releases through a `SWEEP_RUNNING -> SWEEP_IDLE`
/// compare-exchange and then disarms this, because releasing and re-checking
/// the queue MUST be one atomic step — a plain store would clobber a request
/// published between the check and the release. On the abandon paths —
/// shutdown, and a walk task cancelled under the worker at runtime teardown —
/// there is nothing to re-check: no worker survives them, so an unconditional
/// release is right.
struct SweepSlotGuard<'a> {
    state: &'a std::sync::atomic::AtomicU8,
    armed: bool,
}

impl Drop for SweepSlotGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.state
                .store(SWEEP_IDLE, std::sync::atomic::Ordering::Release);
        }
    }
}

/// Everything one lag-sweep worker borrows from [`run_republish`].
///
/// A struct rather than six positional parameters because four of the six
/// are `&Arc<_>` / `&_` and would otherwise be transposable at the call site.
#[derive(Clone, Copy)]
struct SweepSlot<'a> {
    /// Three-state slot: see `SWEEP_IDLE` / `SWEEP_RUNNING` / `SWEEP_QUEUED`.
    state: &'a Arc<std::sync::atomic::AtomicU8>,
    cache: &'a decdn_cache::CacheEngine,
    scheduler: &'a Arc<RepublishScheduler>,
    metrics: &'a Arc<crate::metrics::Metrics>,
    /// Cancelled by [`run_republish`] on its way out, so a walk in flight at
    /// shutdown is abandoned rather than classified as a degradation.
    shutdown: &'a CancellationToken,
    relay_foreign_namespaces: bool,
}

/// Request a lag sweep, starting a worker if the slot is free.
///
/// Detached, like the batch publish: the store walk costs one `status()` per
/// blob, and running it on the `select!` loop would stall shutdown and the
/// eager per-insert publishes. Cancelled on shutdown — the next lag, or the
/// next boot, re-seeds.
///
/// The cancellation is what keeps a clean restart quiet. The runtime cancels
/// before it flushes the store, and the `select!` below is `biased` with
/// cancellation first, so a worker polled any time after the flush takes the
/// cancel arm rather than observing the store it was walking disappear and
/// reporting that as a degradation. Nothing joins the worker, so it may outlive
/// the flush by a poll — it just cannot report anything once cancelled.
///
/// Coalescing re-runs rather than drops. Each pass re-derives the advertised
/// set once, at its start, so a lag observed mid-walk concerns commits that
/// snapshot cannot contain; skipping it would strand exactly the blobs the
/// sweep exists to recover. A request therefore survives every ordinary
/// outcome: it either starts a worker or moves the slot to `SWEEP_QUEUED`, and
/// the running worker's release is a compare-exchange that fails if a request
/// landed first.
///
/// Only shutdown discards a queued request instead of running it, and does so
/// deliberately — the next boot re-seeds. A panic inside the walk does not:
/// the walk runs in its own task, so the worker observes the panic as a failed
/// join rather than unwinding through it, takes its normal release path, and a
/// queued request earns its further pass.
fn spawn_lag_sweep(slot: &SweepSlot<'_>) {
    use std::sync::atomic::Ordering;
    loop {
        match slot.state.compare_exchange_weak(
            SWEEP_IDLE,
            SWEEP_RUNNING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            // A worker is mid-walk: queue behind it. Its release CAS will fail
            // and it will take another pass.
            Err(SWEEP_RUNNING) => {
                if slot
                    .state
                    .compare_exchange_weak(
                        SWEEP_RUNNING,
                        SWEEP_QUEUED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    slot.metrics.dht_republish_lag_sweep_coalesced();
                    tracing::debug!(
                        "dht republish: lag sweep already running; queued a further pass"
                    );
                    return;
                }
            }
            // Already queued — one further pass covers this lag too, because
            // that pass has not taken its snapshot yet.
            Err(SWEEP_QUEUED) => {
                slot.metrics.dht_republish_lag_sweep_coalesced();
                tracing::debug!("dht republish: lag sweep already queued");
                return;
            }
            // Spurious failure or a state change under us; re-read and retry.
            Err(_) => {}
        }
    }

    let cache = slot.cache.clone();
    let scheduler = Arc::clone(slot.scheduler);
    let metrics = Arc::clone(slot.metrics);
    let state = Arc::clone(slot.state);
    let shutdown = slot.shutdown.clone();
    let relay_foreign_namespaces = slot.relay_foreign_namespaces;
    tokio::spawn(run_sweep_worker(
        state,
        cache,
        scheduler,
        metrics,
        shutdown,
        relay_foreign_namespaces,
    ));
}

/// One sweep worker: walk passes until a clean release or an abandon path.
/// Owns the `SWEEP_RUNNING` slot its spawner claimed; split from
/// [`spawn_lag_sweep`] so the slot-claim protocol and the worker loop each
/// stay readable on their own.
//
// Linear "spawn walk → join → release-or-repeat" loop. Splitting further
// would scatter the slot protocol's release invariant across helpers; the
// function body is one state machine.
#[allow(clippy::cognitive_complexity)]
async fn run_sweep_worker(
    state: Arc<std::sync::atomic::AtomicU8>,
    cache: decdn_cache::CacheEngine,
    scheduler: Arc<RepublishScheduler>,
    metrics: Arc<crate::metrics::Metrics>,
    shutdown: CancellationToken,
    relay_foreign_namespaces: bool,
) {
    use std::sync::atomic::Ordering;
    // RAII for the paths that leave without a successful release CAS:
    // shutdown, and a walk task cancelled under this worker at runtime
    // teardown. Either would otherwise strand the slot and disable every
    // later sweep for the process lifetime, while `lag_sweeps_total` kept
    // climbing — a wedge that reads as health.
    let mut guard = SweepSlotGuard {
        state: &state,
        armed: true,
    };
    loop {
        // The walk runs in its own task so a panic inside it (`iter_hashes`
        // goes through iroh-blobs, which sits outside the workspace
        // anti-panic lints) surfaces below as `JoinError::is_panic` rather
        // than unwinding through this worker. The worker then takes its
        // normal release path, which consumes a queued request instead of
        // discarding it. Same join-and-branch pattern as the runtime,
        // admin, and buyer-channel layers (`Cargo.toml`'s
        // `panic = "unwind"` note).
        let mut walk = tokio::spawn({
            let cache = cache.clone();
            let scheduler = Arc::clone(&scheduler);
            let metrics = Arc::clone(&metrics);
            async move { lag_sweep(&cache, relay_foreign_namespaces, &scheduler, &metrics).await }
        });
        let joined = tokio::select! {
            biased;
            // Shutdown wins: the store is about to be flushed and closed,
            // so a walk that continues here reports its own teardown as a
            // store-walk failure. Abort it — nothing awaits it after this
            // — and let the slot guard release on the way out.
            () = shutdown.cancelled() => {
                walk.abort();
                tracing::debug!(
                    "dht republish: shutdown during a lag sweep; abandoning the walk"
                );
                return;
            }
            joined = &mut walk => joined,
        };
        match joined {
            Ok(outcome) => {
                tracing::info!(
                    reseeded = outcome.reseeded,
                    degraded = outcome.degraded,
                    "dht republish: lag sweep complete"
                );
            }
            Err(err) if err.is_panic() => {
                metrics.dht_republish_lag_sweep_panicked();
                tracing::error!(
                    error = %err,
                    "dht republish: lag sweep panicked; whatever the walk \
                     seeded before it died stays scheduled, and a queued \
                     request still runs"
                );
            }
            // Cancelled without this worker aborting it: the runtime is
            // tearing down. Leave through the guard, as on shutdown.
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    "dht republish: lag sweep walk cancelled; abandoning"
                );
                return;
            }
        }
        if state
            .compare_exchange(
                SWEEP_RUNNING,
                SWEEP_IDLE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            // Released cleanly with nothing queued. The guard must not
            // store again: a producer may already have claimed the slot.
            guard.armed = false;
            return;
        }
        // The CAS can only fail because a request arrived, so consume it
        // and take another pass.
        state.store(SWEEP_RUNNING, Ordering::Release);
    }
}

/// Long-running task: consumes a [`broadcast::Receiver<Hash>`] from
/// the cache, drives the scheduler heap, and publishes each hash that has not
/// announced yet with a `Store` to its K+3 closest peers and each drain cycle
/// with one `BatchStore` per receiver. Exits on `stop`, or on a closed cache
/// subscribe channel — which this task's own `CacheEngine`
/// clone makes unreachable in practice.
///
/// `stop` is owned by the runtime, which cancels it before flushing the cache
/// store. That ordering is what a detached lag sweep needs: the sweep selects
/// against this same token, so its walk is abandoned ahead of the flush rather
/// than on whatever tick this task next happens to be polled — a signal this
/// task had to forward would give no such guarantee.
///
/// Returns on shutdown so the runtime's `JoinSet` can drain it.
//
// Linear "shutdown? new commit? tick?" select loop. Splitting would
// scatter the three-way priority across helpers; the function body
// is a flat dispatcher.
#[allow(clippy::cognitive_complexity, clippy::too_many_arguments)]
pub async fn run_republish(
    endpoint: Endpoint,
    self_id: PublicKey,
    routing: Arc<Mutex<RoutingTable>>,
    scheduler: Arc<RepublishScheduler>,
    cache: decdn_cache::CacheEngine,
    relay_foreign_namespaces: bool,
    metrics: Arc<crate::metrics::Metrics>,
    mut cache_inserts: broadcast::Receiver<iroh_blobs::Hash>,
    stop: CancellationToken,
) {
    let self_node_id = NodeId::from_bytes(*self_id.as_bytes());
    // One sweep at a time, with a queued re-run rather than a dropped request.
    // A lag is a symptom of sustained commit pressure, so `Lagged` commonly
    // repeats; see `spawn_lag_sweep` for the slot protocol.
    let sweep_state = Arc::new(std::sync::atomic::AtomicU8::new(SWEEP_IDLE));
    // Use a relatively short polling interval — the driver wakes on
    // cache-insert events, on the scheduler ticking, OR on the
    // shutdown signal. A 1-second poll keeps the worst-case latency
    // between "hash became due" and "Store sent" bounded.
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut last_cycle_us = None;
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // burn first tick

    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => {
                tracing::debug!("dht republish: shutdown signal received");
                return;
            }
            insert = cache_inserts.recv() => {
                match insert {
                    Ok(hash) => {
                        let hash_bytes = ContentHash::from_bytes(*hash.as_bytes());
                        // ADR 022 §STORE Flow steps 2–3: publish
                        // *immediately* once the hash becomes advertisable
                        // (step 2), with the next republish at `T +
                        // uniform(30, 50) min` (step 3). Without the eager
                        // publish a freshly-cached blob isn't discoverable
                        // for up to 50 minutes.
                        //
                        // Only a hash that has not announced yet publishes: a
                        // ranged fill announces once per completed block, and
                        // an announced hash's coverage widens on its next
                        // cycle (ADR 022 §Content Records and TTL). A hash
                        // whose publish reached no peer stays unannounced, so
                        // its next event retries.
                        scheduler.schedule_steady_if_absent(hash_bytes);
                        if !scheduler.is_announced(&hash_bytes) {
                            let accepted =
                                publish_hash(&endpoint, self_node_id, &routing, &cache, hash_bytes)
                                    .await;
                            metrics.dht_store_published(accepted);
                            if accepted > 0 {
                                scheduler.mark_announced(hash_bytes);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // ADR 022 §STORE Flow & the `subscribe_inserts`
                        // contract: the cache does not retain the missed
                        // hashes, so a lagged consumer MUST re-derive the
                        // held set rather than try to backfill. Without the
                        // sweep a hash announced inside the lag window stays
                        // undiscoverable until an operator restarts.
                        tracing::warn!(
                            missed = n,
                            "dht republish: cache-insert channel lagged; \
                             re-seeding the scheduler from local state"
                        );
                        metrics.dht_republish_lag_sweep();
                        spawn_lag_sweep(&SweepSlot {
                            state: &sweep_state,
                            cache: &cache,
                            scheduler: &scheduler,
                            metrics: &metrics,
                            shutdown: &stop,
                            relay_foreign_namespaces,
                        });
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Unreachable while this task holds a `CacheEngine`
                        // clone, which owns the sender. Terminal rather than
                        // `continue` because `recv` on a closed channel
                        // resolves instantly and forever: re-polling it in
                        // this biased `select!` starves the ticker and spins
                        // a core.
                        tracing::error!(
                            "dht republish: cache insert channel closed; stopping the republisher"
                        );
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                let (due, budget) =
                    plan_cycle(&scheduler, &routing, now_us(), &mut last_cycle_us);
                // ADR 022 §Content Records and TTL: "A node
                // stops re-publishing when it evicts the blob." Keep only
                // the still-held hashes; drop evicted ones from the
                // scheduler instead of re-adding them.
                let mut held = Vec::with_capacity(due.len());
                let mut store_faults = 0usize;
                for hash in due {
                    match cache_still_holds(&cache, &hash).await {
                        Some(true) => held.push(hash),
                        Some(false) => scheduler.unschedule(&hash),
                        // A store fault is not eviction evidence: keep the
                        // hash scheduled with a fresh steady-state draw and
                        // skip this cycle's publish, rather than advertising
                        // content the serve path cannot confirm — or worse,
                        // unscheduling it for the process lifetime.
                        None => {
                            store_faults = store_faults.saturating_add(1);
                            scheduler.schedule_steady(hash);
                        }
                    }
                }
                if store_faults > 0 {
                    tracing::warn!(
                        faults = store_faults,
                        "dht republish: store queries faulted at the due-time \
                         gate; the affected hashes stay scheduled and retry on \
                         their next cycle"
                    );
                }
                if !held.is_empty() {
                    // Re-schedule with the steady-state jitter window first
                    // (ADR 022 §STORE Flow step 3 — fresh jitter draw per record per
                    // cycle). The reschedule is independent of publish
                    // outcome, so doing it before the publish lets the
                    // publish run detached below.
                    for hash in &held {
                        scheduler.schedule_steady(*hash);
                    }
                    // One BatchStore per receiver (ADR 022 §STORE Flow
                    // Batched STORE) rather than a per-hash fan-out — the
                    // cycle's hashes share receiver sets, which is exactly
                    // what batching folds into a single RPC. The sets are the
                    // ones the drain admitted, so each look-ahead batch stays
                    // within the cap. Fire it off the select loop: a slow
                    // or unreachable receiver would otherwise hold the loop
                    // for up to (chunks × DHT_CLIENT_TIMEOUT), delaying the
                    // shutdown signal and eager cache-insert publishes. The
                    // sweep is best-effort — abandoned on shutdown, retried
                    // next cycle.
                    //
                    // A hash counts as announced only once a peer accepts it
                    // with non-empty coverage. One that no peer took — no
                    // routing peers yet, or every receiver refused — stays
                    // eligible for the eager publish on its next cache event.
                    let groups = budget.into_groups(&held.into_iter().collect());
                    let ep = endpoint.clone();
                    let cache_cloned = cache.clone();
                    let metrics = Arc::clone(&metrics);
                    let scheduler = Arc::clone(&scheduler);
                    tokio::spawn(async move {
                        let outcome =
                            publish_batch(&ep, self_node_id, &cache_cloned, groups).await;
                        metrics.dht_store_published(outcome.accepted);
                        for hash in outcome.announced {
                            scheduler.mark_announced(hash);
                        }
                    });
                }
            }
        }
    }
}

/// Plan one drain cycle at `now_us`: drain it from `scheduler` and group it by
/// receiver (ADR 022 §STORE Flow "Drain cycle").
///
/// The cycle takes the records due now and, while no receiver's set passes
/// [`LOOKAHEAD_RECEIVER_CAP`], the ones due within [`REPUBLISH_LOOKAHEAD`].
/// Per-record jitter spreads due times so thinly that a tick alone holds about
/// one hash per receiver. The look-ahead is off when the previous non-empty
/// cycle, recorded in `last_cycle_us`, is less than [`LOOKAHEAD_MIN_SPACING`]
/// old.
///
/// Lock order is heap → scheduled → routing: nothing takes a scheduler lock
/// while it holds the routing table.
fn plan_cycle(
    scheduler: &RepublishScheduler,
    routing: &Mutex<RoutingTable>,
    now_us: u64,
    last_cycle_us: &mut Option<u64>,
) -> (Vec<ContentHash>, ReceiverBudget) {
    let micros = |d: Duration| u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
    let spaced = last_cycle_us
        .is_none_or(|last| now_us.saturating_sub(last) >= micros(LOOKAHEAD_MIN_SPACING));
    let horizon_us = if spaced {
        now_us.saturating_add(micros(REPUBLISH_LOOKAHEAD))
    } else {
        now_us
    };
    let mut budget = ReceiverBudget::default();
    let drained = scheduler.drain_cycle(now_us, horizon_us, |hash, mandatory| {
        let receivers = with_lock(routing, "dht routing table", |table| {
            table.closest_unbounded(hash.as_bytes(), REPUBLISH_FANOUT)
        });
        budget.admit(*hash, &receivers, mandatory)
    });
    if !drained.is_empty() {
        *last_cycle_us = Some(now_us);
    }
    (drained, budget)
}

/// Whether this node would still advertise `hash` — `Some(true)` when it
/// holds at least one verified discovery block (ADR 039-adjacent partial-
/// holder discovery; a partial holder is advertise-eligible, not just a
/// `Complete` one), `Some(false)` when it holds none, `None` when the store
/// query faulted and the question has no answer. The due-time gate that
/// stops the scheduler from re-publishing content LRU drift or an
/// operator-evict already removed.
///
/// Origin-held counts, not just the local store. `CacheEngine::coverage`
/// derives its bitmap from cached blocks only, but the probe path
/// advertises origin-held content through `origin_held_size` — so a
/// filesystem or pinned-origin hash that was never imported would be
/// advertised at probe time and yet dropped here at its first due time,
/// publishing no `Store` at all. Both bulk seeds feed exactly that content
/// in, so a cache-only check silently discards what they schedule.
///
/// A store error is `None`, never `Some(false)`: eviction is the only honest
/// reason to stop republishing, and a fault is not eviction evidence. Folding
/// it into "no longer held" would have the tick path `unschedule` the hash —
/// dropping it from the announce set for the process lifetime on a transient
/// store blip, with every signal green.
///
/// Both reads are in-memory or local; the live origin probe is deliberately not
/// used, because this runs per hash per republish cycle.
///
/// This gate is also what drops a partial the bulk seeds scheduled without
/// reading its coverage ([`decdn_cache::CacheEngine::iter_hashes`]): one that
/// covers no discovery block answers `Some(false)` and leaves the schedule
/// until a later completed block announces it or a later seed re-adds it.
async fn cache_still_holds(cache: &decdn_cache::CacheEngine, hash: &ContentHash) -> Option<bool> {
    let h = iroh_blobs::Hash::from_bytes(*hash.as_bytes());
    if cache.refuses(h) {
        return Some(false);
    }
    // `origin_held_size` applies the live refusal filter itself, so a
    // blacklisted or operator-evicted hash cannot re-enter through it. It is
    // an in-memory index read that cannot fault, so it answers first.
    if cache.origin_held_size(h).is_some() {
        return Some(true);
    }
    match cache.coverage(h).await {
        Ok(coverage) => Some(!coverage.is_empty()),
        Err(err) => {
            tracing::debug!(
                hash = ?hash,
                error = %err.display_chain(),
                "dht republish: store query faulted at the due-time gate"
            );
            None
        }
    }
}

/// Send a `Store` to the K+3 closest peers for `hash` in parallel.
/// Failures are logged at debug level — a missed receiver in this cycle
/// will be retried on the next cycle (or on the next cold start).
///
/// `coverage` is derived once, up front — every receiver gets the same
/// snapshot of what this node can currently serve for `hash` rather than a
/// per-RPC re-derivation that could drift mid-fan-out. An empty coverage sends
/// nothing: the record would advertise no block a requester can fetch. That
/// covers a cache event for a hash the store no longer holds, a partial with no
/// whole block, and a coverage query that faulted.
///
/// Returns how many peers accepted the record.
async fn publish_hash(
    endpoint: &Endpoint,
    self_node_id: NodeId,
    routing: &Arc<Mutex<RoutingTable>>,
    cache: &decdn_cache::CacheEngine,
    hash: ContentHash,
) -> u64 {
    let coverage = fetch_coverage(cache, hash).await;
    if coverage.is_empty() {
        return 0;
    }
    let targets: Vec<NodeId> = with_lock(routing, "dht routing table", |table| {
        // ADR 022 §STORE Flow step 1 specifies K+3 (= 23) closest
        // nodes — the three positions beyond K are overflow targets
        // so an attacker suppressing receivers has to take down K+3
        // hosts rather than K. The default `closest` caps at K (=
        // wire `MAX_CLOSER_NODES`), which we MUST NOT use here;
        // `closest_unbounded` returns up to `n` peers regardless of
        // the wire cap.
        table.closest_unbounded(hash.as_bytes(), REPUBLISH_FANOUT)
    });
    if targets.is_empty() {
        // No routing-table entries yet (e.g. boot before bootstrap).
        // Nothing to do this cycle; the scheduler will retry.
        return 0;
    }
    let mut handles = Vec::with_capacity(targets.len());
    for peer in targets {
        let endpoint_cloned = endpoint.clone();
        let coverage_cloned = coverage.clone();
        handles.push(tokio::spawn(async move {
            let target_pk = match PublicKey::from_bytes(peer.as_bytes()) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!(
                        peer = %peer,
                        error = %e,
                        "dht republish: routing-table peer not a valid public key"
                    );
                    return 0;
                }
            };
            let addr = EndpointAddr::new(target_pk);
            match client::store(&endpoint_cloned, addr, hash, self_node_id, coverage_cloned).await {
                Ok(ack) if ack.accepted => 1,
                Ok(_) => {
                    tracing::debug!(
                        peer = %peer,
                        hash = ?hash,
                        "dht republish: peer rejected Store (not staked, over quota, etc)"
                    );
                    0
                }
                Err(e) => {
                    tracing::debug!(
                        peer = %peer,
                        hash = ?hash,
                        error = %e,
                        "dht republish: Store request failed"
                    );
                    0
                }
            }
        }));
    }
    let mut accepted = 0;
    for h in handles {
        accepted += h.await.unwrap_or(0);
    }
    accepted
}

/// Derive `hash`'s current [`Coverage`] from the cache, folding any store
/// fault into [`Coverage::empty`] — a republish that can't answer the
/// coverage question advertises nothing for this cycle rather than
/// blocking on it; the hash stays scheduled and retries next cycle via the
/// `cache_still_holds` due-time gate.
async fn fetch_coverage(cache: &decdn_cache::CacheEngine, hash: ContentHash) -> Coverage {
    let h = iroh_blobs::Hash::from_bytes(*hash.as_bytes());
    match cache.coverage(h).await {
        Ok(coverage) => coverage,
        Err(err) => {
            tracing::debug!(
                hash = ?hash,
                error = %err.display_chain(),
                "dht republish: coverage query faulted; publishing empty coverage this cycle"
            );
            Coverage::empty()
        }
    }
}

/// One republish cycle's `receiver → hashes` grouping, which is also the
/// admission rule behind the look-ahead in
/// [`RepublishScheduler::drain_cycle`].
///
/// The grouping that admits a hash is the grouping that publishes it:
/// [`publish_batch`] sends exactly these sets rather than asking the routing
/// table again. A peer that joins the table between the two steps cannot then
/// collect uncharged hashes and push its batch past the cap.
///
/// A set holds at most `max(cap, due hashes)` because `drain_cycle` presents
/// every due hash before any look-ahead hash; the type itself does not track
/// the order.
#[derive(Debug, Default)]
struct ReceiverBudget {
    groups: HashMap<NodeId, Vec<ContentHash>>,
}

impl ReceiverBudget {
    /// Add `hash` to the set of each of `receivers` — its K+3 closest peers
    /// (ADR 022 §STORE Flow step 1). A `mandatory` hash is always added. A
    /// look-ahead hash is added only if it has a receiver and no receiver's
    /// set is already at [`LOOKAHEAD_RECEIVER_CAP`]; otherwise nothing is
    /// added and the answer is `false`. With no receivers (boot before
    /// bootstrap) the cycle publishes nothing, so pulling a record forward
    /// would only spend its refresh.
    fn admit(&mut self, hash: ContentHash, receivers: &[NodeId], mandatory: bool) -> bool {
        let full =
            |peer: &NodeId| self.groups.get(peer).map_or(0, Vec::len) >= LOOKAHEAD_RECEIVER_CAP;
        if !mandatory && (receivers.is_empty() || receivers.iter().any(full)) {
            return false;
        }
        for peer in receivers {
            self.groups.entry(*peer).or_default().push(hash);
        }
        true
    }

    /// The per-receiver sets, keeping only the hashes in `held`. A hash the
    /// due-time gate dropped (evicted, or a store fault) leaves every set, and
    /// a receiver left with nothing gets no batch.
    fn into_groups(self, held: &HashSet<ContentHash>) -> HashMap<NodeId, Vec<ContentHash>> {
        self.groups
            .into_iter()
            .filter_map(|(peer, mut hashes)| {
                hashes.retain(|h| held.contains(h));
                (!hashes.is_empty()).then_some((peer, hashes))
            })
            .collect()
    }
}

/// Publish a cycle's `receiver → hashes` sets ([`ReceiverBudget::into_groups`])
/// as one `BatchStore` per receiver, split at the [`MAX_BATCH_STORE_HASHES`]
/// wire cap (ADR 022 §STORE Flow Batched STORE). Each receiver's batches run
/// in their own task so a
/// slow peer doesn't stall the rest of the sweep. A rejected hash (peer
/// not staked, over quota) or a failed exchange is logged at debug — the
/// record retries on the next cycle. Every DHT node implements
/// `BatchStore`, so there is no per-hash fallback.
///
/// Returns how many `(peer, hash)` records the peers accepted, and which
/// hashes at least one peer accepted with non-empty coverage.
async fn publish_batch(
    endpoint: &Endpoint,
    self_node_id: NodeId,
    cache: &decdn_cache::CacheEngine,
    groups: HashMap<NodeId, Vec<ContentHash>>,
) -> BatchOutcome {
    if groups.is_empty() {
        // No routing-table entries yet (e.g. boot before bootstrap).
        // Nothing to do this cycle; the scheduler will retry.
        return BatchOutcome::default();
    }
    // Derive each hash's coverage once, up front, and share it across every
    // receiver's batch — the same rationale as `publish_hash`'s single
    // snapshot: every receiver of a given hash this cycle sees the same
    // coverage rather than a per-batch re-derivation that could drift.
    let hashes: HashSet<ContentHash> = groups.values().flatten().copied().collect();
    let mut coverage_by_hash = HashMap::with_capacity(hashes.len());
    for hash in hashes {
        coverage_by_hash.insert(hash, fetch_coverage(cache, hash).await);
    }
    let coverage_by_hash = Arc::new(coverage_by_hash);
    let mut handles = Vec::with_capacity(groups.len());
    for (peer, peer_hashes) in groups {
        let endpoint_cloned = endpoint.clone();
        let coverage_by_hash = Arc::clone(&coverage_by_hash);
        handles.push(tokio::spawn(async move {
            let target_pk = match PublicKey::from_bytes(peer.as_bytes()) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!(
                        peer = %peer,
                        error = %e,
                        "dht republish: routing-table peer not a valid public key"
                    );
                    return (0, Vec::new());
                }
            };
            let addr = EndpointAddr::new(target_pk);
            let mut accepted: u64 = 0;
            let mut announced = Vec::new();
            for chunk in peer_hashes.chunks(MAX_BATCH_STORE_HASHES) {
                let entries: Vec<(ContentHash, Coverage)> = chunk
                    .iter()
                    .map(|h| {
                        let coverage = coverage_by_hash.get(h).cloned().unwrap_or_else(Coverage::empty);
                        (*h, coverage)
                    })
                    .collect();
                match client::batch_store(
                    &endpoint_cloned,
                    addr.clone(),
                    entries.clone(),
                    self_node_id,
                )
                .await
                {
                    Ok(ack) => {
                        announced.extend(announced_in_batch(&entries, &ack.results));
                        let ok = ack.results.iter().filter(|accepted| **accepted).count();
                        accepted += u64::try_from(ok).unwrap_or(u64::MAX);
                        let rejected = ack.results.iter().filter(|accepted| !**accepted).count();
                        if rejected > 0 {
                            tracing::debug!(
                                peer = %peer,
                                rejected,
                                batch = chunk.len(),
                                "dht republish: peer rejected some batched Stores (not staked, over quota, etc)"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::debug!(
                            peer = %peer,
                            batch = chunk.len(),
                            error = %e,
                            "dht republish: BatchStore request failed"
                        );
                    }
                }
            }
            (accepted, announced)
        }));
    }
    let mut outcome = BatchOutcome::default();
    for h in handles {
        let (accepted, announced) = h.await.unwrap_or_default();
        outcome.accepted += accepted;
        outcome.announced.extend(announced);
    }
    outcome
}

/// What one drain cycle's [`publish_batch`] achieved.
#[derive(Debug, Default)]
struct BatchOutcome {
    /// `(peer, hash)` records the peers accepted.
    accepted: u64,
    /// Hashes at least one peer accepted with non-empty coverage.
    announced: HashSet<ContentHash>,
}

/// The hashes of `entries` that a `BatchStoreAck` accepted and whose entry
/// advertised at least one block. `results` answers `entries` in request
/// order; an entry it does not answer is not accepted.
fn announced_in_batch<'a>(
    entries: &'a [(ContentHash, Coverage)],
    results: &'a [bool],
) -> impl Iterator<Item = ContentHash> + 'a {
    entries
        .iter()
        .zip(results)
        .filter(|((_, coverage), accepted)| **accepted && !coverage.is_empty())
        .map(|((hash, _), _)| *hash)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
