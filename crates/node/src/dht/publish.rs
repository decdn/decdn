//! DHT republish scheduler (ADR 022 §STORE Flow & §Bootstrap).
//!
//! On every successful blob commit ([`decdn_cache::CacheEngine::subscribe_inserts`])
//! the scheduler eagerly publishes the single new hash to its K+3
//! closest peers with per-hash `StoreRequest`s (a size-1 batch buys
//! nothing), then schedules its next republish.
//!
//! On a periodic per-record jittered timer the scheduler:
//!
//!  1. Drains every record whose republish window has come due and
//!     keeps those still held in the cache.
//!  2. Groups the due hashes by receiver — each hash's K+3 closest
//!     peers — so all hashes bound for one receiver ride a single
//!     `BatchStoreRequest { hashes, holder: self }`, split at the
//!     [`MAX_BATCH_STORE_HASHES`] wire cap. This collapses the
//!     concentration of overlapping republish windows into one RPC per
//!     publisher-receiver pair (ADR 022 §STORE Flow Batched STORE &
//!     §DHT Bandwidth Analysis). Every DHT node implements `BatchStore`,
//!     so there is no per-hash fallback.
//!  3. Schedules the next republish at `now + uniform(30 min, 50 min)`
//!     per ADR 022 §STORE Flow step 3. Jitter is drawn independently
//!     per record so the next republish window for a given hash is
//!     not predictable from outside the publisher.
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
//! `BinaryHeap<(due_at, hash)>`. The heap is small (one entry per
//! cached blob, capped by the cache's blob count) and the next
//! `due_at` drives the task's sleep duration. Sending a `Store` does
//! not hold the heap mutex; the K+3 fan-out happens in spawned tasks
//! so a slow receiver doesn't stall the rest of the schedule.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::{Endpoint, EndpointAddr, PublicKey};
use rand::RngExt;
use tokio::sync::{broadcast, oneshot};
use tokio_util::sync::CancellationToken;

use crate::dht::chain_projection::with_lock;
use crate::dht::client;
use crate::dht::routing::{NodeId, RoutingTable};
use decdn_protocol::ContentHash;
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
/// counts, and `drain_due` discards a popped entry whose `due_us`
/// disagrees. Without that check a tombstone is resurrected the moment its
/// hash is scheduled again — it pops already-overdue, passes a
/// membership-only test, and buys a spurious republish. Three call patterns
/// hit that: a re-seed racing the eager per-insert publish, a re-seed racing
/// the tick path's drain-then-reschedule, and [`Self::unschedule`] followed by
/// a later re-seed.
#[allow(missing_debug_implementations)]
pub struct RepublishScheduler {
    heap: Arc<Mutex<BinaryHeap<Reverse<Entry>>>>,
    /// Live hash -> its authoritative `due_us`. Also gives `O(1)` dedupe on
    /// cache-insert events without walking the heap. A hash absent here has no
    /// live entry, whatever the heap still holds.
    scheduled: Arc<Mutex<HashMap<ContentHash, u64>>>,
}

impl RepublishScheduler {
    /// Construct an empty scheduler.
    #[must_use]
    pub fn new() -> Self {
        Self {
            heap: Arc::new(Mutex::new(BinaryHeap::new())),
            scheduled: Arc::new(Mutex::new(HashMap::new())),
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
    /// authoritative due time and `drain_due` discards it.
    pub fn schedule_steady(&self, hash: ContentHash) {
        let offset = jitter_us(STEADY_STATE_MIN, STEADY_STATE_MAX);
        self.schedule_with_offset(hash, offset);
    }

    /// Batch-schedule every hash in `iter` with an independent
    /// cold-start jitter draw (uniform(0, 40 min) per hash, NOT a
    /// shared timestamp). Callers pass a `holder_snapshot` result,
    /// or `origin_held_hashes` for the origin-only rescan — never
    /// `access_times_snapshot`, which maps `Hash -> Instant` and is
    /// empty on cold start; see the `iter_hashes` rustdoc.
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
    /// `drain_due` cannot act on a half-applied change. Acquired
    /// heap-then-scheduled to match `drain_due`'s order — the reverse would
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

    /// Drain every hash whose authoritative due time has passed.
    ///
    /// A popped entry counts only if `scheduled` still names that hash at
    /// exactly that `due_us`. Anything else is a tombstone left by a
    /// supersede or an unschedule, and is dropped.
    fn drain_due(&self, now_us: u64) -> Vec<ContentHash> {
        let mut out = Vec::new();
        with_lock(&self.heap, "dht republish heap", |heap| {
            with_lock(
                &self.scheduled,
                "dht republish scheduled set",
                |scheduled| {
                    while let Some(Reverse(Entry { due_us, hash })) = heap.peek().copied() {
                        if due_us > now_us {
                            break;
                        }
                        heap.pop();
                        if scheduled.get(&hash) != Some(&due_us) {
                            continue;
                        }
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

    /// Unschedule `hash` — used when the cache evicts the blob.
    pub fn unschedule(&self, hash: &ContentHash) {
        with_lock(&self.scheduled, "dht republish scheduled set", |s| {
            s.remove(hash);
        });
        // The heap entry is left in place; `drain_due` discards it, and a
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
/// namespaces — the committed blobs in the local store. This is the input to
/// both *full* seeds of the republish scheduler: bring-up cold start and the
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
    /// Origin size probes that faulted on the rescan the origin-held half comes
    /// from. The mirror of `store_error` for the other half: nonzero means the
    /// origin-held set is running on carried-forward sizes, and any candidate
    /// first seen inside the fault window is missing from it entirely.
    pub(crate) origin_probe_faults: u64,
}

/// Collect the [`HolderSnapshot`] for `cache`.
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
/// refuse, and faults are not memoised, so the next sweep retries.
///
/// Never fails: a store-walk error degrades to the origin-held half rather than
/// yielding nothing, because a partial announce strictly beats none. Both
/// halves report their own degradation — `store_error` for the store walk,
/// `origin_probe_faults` for the last origin rescan — so neither can truncate
/// the announce set while the snapshot reads healthy.
pub(crate) async fn holder_snapshot(
    cache: &decdn_cache::CacheEngine,
    relay_foreign_namespaces: bool,
) -> HolderSnapshot {
    let mut hashes: HashSet<decdn_cache::Hash> = cache.origin_held_hashes().into_iter().collect();
    let mut store_error = None;
    match cache.iter_hashes().await {
        Ok(stored) => {
            for hash in stored {
                if hashes.contains(&hash) {
                    continue;
                }
                if relay_foreign_namespaces || is_own_origin_content(cache, hash).await {
                    hashes.insert(hash);
                }
            }
        }
        Err(err) => store_error = Some(err),
    }
    HolderSnapshot {
        hashes,
        store_error,
        origin_probe_faults: cache.last_rescan_origin_probe_faults(),
    }
}

/// Whether a configured origin backend confirms it holds `hash` — the
/// origin-only node's ownership test, matching the serve gate in
/// `handlers::client::dispatch`.
///
/// Memoised inside the cache under the positive TTL, so a sweep re-walking the
/// same store does not re-issue a `HEAD` per hash.
async fn is_own_origin_content(cache: &decdn_cache::CacheEngine, hash: decdn_cache::Hash) -> bool {
    matches!(
        cache.origin_probe_presence(hash).await,
        decdn_cache::OriginPresence::Present(_)
    )
}

/// What one lag sweep did, for the completion log.
///
/// `degraded` rides along so the completion line names the outcome: a
/// store-walk failure warns from inside [`lag_sweep`], several frames away, and
/// an unqualified "complete" would read as a clean run to anyone grepping for
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SweepOutcome {
    /// Hashes this sweep newly scheduled.
    reseeded: usize,
    /// Whether the store walk failed, leaving only the origin-held half.
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
    let degraded = snapshot.store_error.is_some() || snapshot.origin_probe_faults > 0;
    if let Some(err) = &snapshot.store_error {
        metrics.dht_republish_seed_store_walk_failure();
        tracing::warn!(
            error = %err,
            "dht republish: lag sweep could not walk the store; re-seeding the \
             origin-held half only. Blobs held only in the store stay \
             un-republished until the next sweep or restart"
        );
    }
    if snapshot.origin_probe_faults > 0 {
        tracing::warn!(
            faults = snapshot.origin_probe_faults,
            "dht republish: the last origin rescan could not resolve every \
             candidate; the origin-held half of this sweep runs on carried-forward \
             sizes and omits anything first seen inside the fault window"
        );
    }
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

/// RAII release for the sweep slot, for the panic path only.
///
/// The clean path releases through a `SWEEP_RUNNING -> SWEEP_IDLE`
/// compare-exchange and then disarms this, because releasing and re-checking
/// the queue MUST be one atomic step — a plain store would clobber a request
/// published between the check and the release. On panic there is nothing to
/// re-check: no worker survives, so an unconditional release is right.
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
/// next boot, re-seeds. Cancelling rather than letting the walk run out
/// matters twice: the worker drops its `CacheEngine` clone before the runtime
/// flushes the store, and a walk that ends because the store went away under
/// it never reaches `lag_sweep`'s degradation arm, which would otherwise fire
/// an alertable counter on an ordinary restart.
///
/// Coalescing re-runs rather than drops. Each pass re-derives the advertised
/// set once, at its start, so a lag observed mid-walk concerns commits that
/// snapshot cannot contain; skipping it would strand exactly the blobs the
/// sweep exists to recover. A request is therefore never lost: it either
/// starts a worker or moves the slot to `SWEEP_QUEUED`, and the running
/// worker's release is a compare-exchange that fails if a request landed
/// first.
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
    tokio::spawn(async move {
        // RAII for the panic path: a panic inside the walk (iroh-blobs is
        // outside the workspace anti-panic lints) would otherwise strand the
        // slot and disable every later sweep for the process lifetime, while
        // `lag_sweeps_total` kept climbing — a wedge that reads as health.
        let mut guard = SweepSlotGuard {
            state: &state,
            armed: true,
        };
        loop {
            let outcome = tokio::select! {
                biased;
                // Shutdown wins: the store is about to be flushed and closed,
                // so a walk that continues here reports its own teardown as a
                // store-walk failure. The slot guard releases on the way out.
                () = shutdown.cancelled() => {
                    tracing::debug!(
                        "dht republish: shutdown during a lag sweep; abandoning the walk"
                    );
                    return;
                }
                outcome = lag_sweep(&cache, relay_foreign_namespaces, &scheduler, &metrics) => outcome,
            };
            tracing::info!(
                reseeded = outcome.reseeded,
                degraded = outcome.degraded,
                "dht republish: lag sweep complete"
            );
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
    });
}

/// Long-running task: consumes a [`broadcast::Receiver<Hash>`] from
/// the cache, drives the scheduler heap, and fans out `Store` requests
/// to the K+3 closest peers per due hash. Exits on `stop_rx`, or on a
/// closed cache subscribe channel — which this task's own `CacheEngine`
/// clone makes unreachable in practice.
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
    mut stop_rx: oneshot::Receiver<()>,
) {
    let self_node_id = NodeId::from_bytes(*self_id.as_bytes());
    // One sweep at a time, with a queued re-run rather than a dropped request.
    // A lag is a symptom of sustained commit pressure, so `Lagged` commonly
    // repeats; see `spawn_lag_sweep` for the slot protocol.
    let sweep_state = Arc::new(std::sync::atomic::AtomicU8::new(SWEEP_IDLE));
    // Cancelled before this task returns, so a detached sweep abandons its walk
    // instead of racing the runtime's cache flush.
    let sweep_shutdown = CancellationToken::new();
    let _sweep_shutdown_guard = sweep_shutdown.clone().drop_guard();
    // Use a relatively short polling interval — the driver wakes on
    // cache-insert events, on the scheduler ticking, OR on the
    // shutdown signal. A 1-second poll keeps the worst-case latency
    // between "hash became due" and "Store sent" bounded.
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // burn first tick

    loop {
        tokio::select! {
            biased;
            _ = &mut stop_rx => {
                tracing::debug!("dht republish: shutdown signal received");
                return;
            }
            insert = cache_inserts.recv() => {
                match insert {
                    Ok(hash) => {
                        let hash_bytes = ContentHash::from_bytes(*hash.as_bytes());
                        // ADR 022 §STORE Flow steps 2–3: publish
                        // *immediately* on cache insertion (step 2),
                        // then schedule the next republish at `T +
                        // uniform(30, 50) min` where T is the local
                        // wall-clock at step 2. Without the initial
                        // publish a freshly-cached blob isn't
                        // discoverable for up to 50 minutes — the
                        // exact failure mode the eager publish
                        // closes.
                        publish_hash(&endpoint, self_node_id, &routing, hash_bytes).await;
                        scheduler.schedule_steady(hash_bytes);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // ADR 022 §STORE Flow & the `subscribe_inserts`
                        // contract: the cache does not retain the missed
                        // hashes, so a lagged consumer MUST re-derive the
                        // held set rather than try to backfill. Without the
                        // sweep a blob committed inside the lag window stays
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
                            shutdown: &sweep_shutdown,
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
                let due = scheduler.drain_due(now_us());
                // ADR 022 §Content Records and TTL line 122: "A node
                // stops re-publishing when it evicts the blob." Keep only
                // the still-held hashes; drop evicted ones from the
                // scheduler instead of re-adding them.
                let mut held = Vec::with_capacity(due.len());
                for hash in due {
                    if cache_still_holds(&cache, &hash).await {
                        held.push(hash);
                    } else {
                        scheduler.unschedule(&hash);
                    }
                }
                if !held.is_empty() {
                    // Re-schedule with the steady-state jitter window first
                    // (ADR 022 line 130 — fresh jitter draw per record per
                    // cycle). The reschedule is independent of publish
                    // outcome, so doing it before the publish lets the
                    // publish run detached below.
                    for hash in &held {
                        scheduler.schedule_steady(*hash);
                    }
                    // One BatchStore per receiver (ADR 022 §STORE Flow
                    // Batched STORE) rather than a per-hash fan-out — the
                    // overlapping republish windows concentrate on shared
                    // receiver sets, which is exactly what batching folds
                    // into a single RPC. Fire it off the select loop: a slow
                    // or unreachable receiver would otherwise hold the loop
                    // for up to (chunks × DHT_CLIENT_TIMEOUT), delaying the
                    // shutdown signal and eager cache-insert publishes. The
                    // sweep is best-effort — abandoned on shutdown, retried
                    // next cycle.
                    let ep = endpoint.clone();
                    let routing = Arc::clone(&routing);
                    tokio::spawn(async move {
                        publish_batch(&ep, self_node_id, &routing, &held).await;
                    });
                }
            }
        }
    }
}

/// True iff this node would still answer `has_blob` for `hash`. The due-time
/// gate that stops the scheduler from re-publishing content LRU drift or an
/// operator-evict already removed.
///
/// Origin-held counts, not just the local store. `CacheEngine::has` consults
/// only the iroh-blobs store, but the probe path advertises origin-held content
/// through `origin_held_size` — so a filesystem or pinned-origin hash that was
/// never imported would be advertised at probe time and yet dropped here at its
/// first due time, publishing no `Store` at all. Both bulk seeds feed exactly
/// that content in, so a store-only check silently discards what they schedule.
///
/// Both reads are in-memory or local; the live origin probe is deliberately not
/// used, because this runs per hash per republish cycle.
async fn cache_still_holds(cache: &decdn_cache::CacheEngine, hash: &ContentHash) -> bool {
    let h = iroh_blobs::Hash::from_bytes(*hash.as_bytes());
    if cache.refuses(h) {
        return false;
    }
    // `origin_held_size` applies the live refusal filter itself, so a
    // blacklisted or operator-evicted hash cannot re-enter through it.
    cache.has(h).await.unwrap_or(false) || cache.origin_held_size(h).is_some()
}

/// Send a `Store` to the K+3 closest peers for `hash` in parallel.
/// Failures are logged at debug level — a missed receiver in this cycle
/// will be retried on the next cycle (or on the next cold start).
async fn publish_hash(
    endpoint: &Endpoint,
    self_node_id: NodeId,
    routing: &Arc<Mutex<RoutingTable>>,
    hash: ContentHash,
) {
    let targets: Vec<NodeId> = with_lock(routing, "dht routing table", |table| {
        // ADR 022 §STORE Flow line 128 specifies K+3 (= 23) closest
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
        return;
    }
    let mut handles = Vec::with_capacity(targets.len());
    for peer in targets {
        let endpoint_cloned = endpoint.clone();
        handles.push(tokio::spawn(async move {
            let target_pk = match PublicKey::from_bytes(peer.as_bytes()) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!(
                        peer = ?peer,
                        error = %e,
                        "dht republish: routing-table peer not a valid public key"
                    );
                    return;
                }
            };
            let addr = EndpointAddr::new(target_pk);
            match client::store(&endpoint_cloned, addr, hash, self_node_id).await {
                Ok(ack) if ack.accepted => {}
                Ok(_) => {
                    tracing::debug!(
                        peer = ?peer,
                        hash = ?hash,
                        "dht republish: peer rejected Store (not staked, over quota, etc)"
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        peer = ?peer,
                        hash = ?hash,
                        error = %e,
                        "dht republish: Store request failed"
                    );
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// Group `hashes` by receiver: for each hash, its K+3 closest peers
/// (ADR 022 §STORE Flow line 128), inverted into `receiver → hashes`.
/// A pure function over a locked snapshot of the routing table so the
/// receiver-grouping logic is unit-testable without a network. Returns
/// an empty map when the table has no peers (e.g. boot before
/// bootstrap) — the caller then publishes nothing this cycle.
fn group_by_receiver(
    table: &RoutingTable,
    hashes: &[ContentHash],
) -> HashMap<NodeId, Vec<ContentHash>> {
    let mut groups: HashMap<NodeId, Vec<ContentHash>> = HashMap::new();
    for &hash in hashes {
        for peer in table.closest_unbounded(hash.as_bytes(), REPUBLISH_FANOUT) {
            groups.entry(peer).or_default().push(hash);
        }
    }
    groups
}

/// Publish a set of due hashes as one `BatchStore` per receiver, split
/// at the [`MAX_BATCH_STORE_HASHES`] wire cap (ADR 022 §STORE Flow
/// Batched STORE). Each receiver's batches run in their own task so a
/// slow peer doesn't stall the rest of the sweep. A rejected hash (peer
/// not staked, over quota) or a failed exchange is logged at debug — the
/// record retries on the next cycle. Every DHT node implements
/// `BatchStore`, so there is no per-hash fallback.
async fn publish_batch(
    endpoint: &Endpoint,
    self_node_id: NodeId,
    routing: &Arc<Mutex<RoutingTable>>,
    hashes: &[ContentHash],
) {
    let groups = with_lock(routing, "dht routing table", |table| {
        group_by_receiver(table, hashes)
    });
    if groups.is_empty() {
        // No routing-table entries yet (e.g. boot before bootstrap).
        // Nothing to do this cycle; the scheduler will retry.
        return;
    }
    let mut handles = Vec::with_capacity(groups.len());
    for (peer, peer_hashes) in groups {
        let endpoint_cloned = endpoint.clone();
        handles.push(tokio::spawn(async move {
            let target_pk = match PublicKey::from_bytes(peer.as_bytes()) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!(
                        peer = ?peer,
                        error = %e,
                        "dht republish: routing-table peer not a valid public key"
                    );
                    return;
                }
            };
            let addr = EndpointAddr::new(target_pk);
            for chunk in peer_hashes.chunks(MAX_BATCH_STORE_HASHES) {
                match client::batch_store(&endpoint_cloned, addr.clone(), chunk.to_vec(), self_node_id)
                    .await
                {
                    Ok(ack) => {
                        let rejected = ack.results.iter().filter(|accepted| !**accepted).count();
                        if rejected > 0 {
                            tracing::debug!(
                                peer = ?peer,
                                rejected,
                                batch = chunk.len(),
                                "dht republish: peer rejected some batched Stores (not staked, over quota, etc)"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::debug!(
                            peer = ?peer,
                            batch = chunk.len(),
                            error = %e,
                            "dht republish: BatchStore request failed"
                        );
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
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

    fn h(b: u8) -> ContentHash {
        ContentHash::from_bytes([b; 32])
    }

    fn nid(b: u8) -> NodeId {
        NodeId::from_bytes([b; 32])
    }

    #[test]
    fn group_by_receiver_sends_every_due_hash_to_each_of_its_closest_peers() {
        // A table with fewer than REPUBLISH_FANOUT (23) peers means every
        // peer is within the K+3 closest set of every hash, so each peer's
        // batch must carry all due hashes exactly once.
        let mut table = RoutingTable::new(nid(0x01));
        for b in [0x10, 0x20, 0x30] {
            table.insert(nid(b));
        }
        let hashes = [h(0xA0), h(0xB0)];
        let groups = group_by_receiver(&table, &hashes);
        assert_eq!(
            groups.len(),
            3,
            "all three peers are closest to both hashes"
        );
        for b in [0x10, 0x20, 0x30] {
            let mut got = groups
                .get(&nid(b))
                .cloned()
                .unwrap_or_else(|| panic!("peer {b:#x} missing a batch"));
            got.sort();
            let mut want = hashes.to_vec();
            want.sort();
            assert_eq!(got, want, "peer {b:#x} must receive every due hash once");
        }
    }

    #[test]
    fn group_by_receiver_empty_table_yields_no_batches() {
        // No routing-table peers (e.g. boot before bootstrap): nothing to
        // publish, so the grouping is empty rather than a panic.
        let table = RoutingTable::new(nid(0x01));
        assert!(group_by_receiver(&table, &[h(0xA0)]).is_empty());
    }

    #[test]
    fn schedule_steady_adds_one_entry() {
        let s = RepublishScheduler::new();
        assert!(s.is_empty());
        s.schedule_steady(h(1));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn unschedule_removes_from_set() {
        let s = RepublishScheduler::new();
        s.schedule_steady(h(1));
        s.unschedule(&h(1));
        assert!(s.is_empty());
    }

    #[test]
    fn drain_due_returns_only_past_entries() {
        let s = RepublishScheduler::new();
        // Steady-state min is 30 min — schedule one steady-state and
        // one explicit "now" entry; the explicit one should drain,
        // the steady-state one should not.
        s.schedule_with_offset(h(1), 0); // due immediately
        s.schedule_steady(h(2));
        let drained = s.drain_due(now_us());
        assert_eq!(drained, vec![h(1)]);
        // h(2) still scheduled.
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn drain_due_filters_via_scheduled_set() {
        // Schedule h(1), then unschedule before drain — the heap entry
        // should be dropped silently rather than republished.
        let s = RepublishScheduler::new();
        s.schedule_with_offset(h(1), 0);
        s.unschedule(&h(1));
        let drained = s.drain_due(now_us());
        assert!(drained.is_empty());
    }

    #[test]
    fn cold_start_jitter_bounded_by_window() {
        // Drawing 100 cold-start offsets must all land in [0, COLD_START_MAX].
        for _ in 0..100 {
            let j = jitter_us(Duration::ZERO, COLD_START_MAX);
            assert!(j <= u64::try_from(COLD_START_MAX.as_micros()).unwrap());
        }
    }

    #[test]
    fn seed_cold_start_schedules_every_hash_within_cold_start_window() {
        // Seeding N hashes returns N, and every hash drains within the
        // cold-start window plus a small slack. Behavioural check via
        // `drain_due` rather than peeking at the heap so the test
        // survives a future switch to a different scheduling primitive.
        let s = RepublishScheduler::new();
        let hashes: Vec<ContentHash> = (1u8..=5).map(h).collect();
        let n = s.seed_cold_start(hashes.iter().copied());
        assert_eq!(n, 5);
        assert_eq!(s.len(), 5);
        let deadline_us = now_us()
            .saturating_add(u64::try_from(COLD_START_MAX.as_micros()).unwrap())
            .saturating_add(1_000); // 1 ms slack for drift between seed and check
        let drained: HashSet<ContentHash> = s.drain_due(deadline_us).into_iter().collect();
        let expected: HashSet<ContentHash> = hashes.into_iter().collect();
        assert_eq!(drained, expected);
        assert!(
            s.is_empty(),
            "drain_due at deadline should empty the scheduler"
        );
    }

    /// Single-blob stub origin.
    ///
    /// Three independent axes, because the real backends differ on exactly
    /// these: `fetch` is what puts a hash in the store; `size` is what
    /// `origin_probe_presence` asks (the ownership test); `enumerate` plus
    /// `size` is what puts it in the origin-held index. A filesystem origin
    /// does all three; a remote S3/R2/HTTP origin answers `size` but
    /// deliberately does not enumerate.
    #[derive(Debug)]
    struct StubOrigin {
        data: bytes::Bytes,
        hash: decdn_cache::Hash,
        enumerable: bool,
        answers_size: bool,
    }

    impl decdn_cache::Origin for StubOrigin {
        fn kind(&self) -> decdn_cache::OriginKind {
            decdn_cache::OriginKind::Filesystem
        }

        fn fetch(
            &self,
            hash: decdn_cache::Hash,
            _max_bytes: u64,
        ) -> std::pin::Pin<
            Box<
                dyn Future<Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>>
                    + Send
                    + '_,
            >,
        > {
            let result = if hash == self.hash {
                Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
            } else {
                Ok(decdn_cache::OriginFetch::NotFound)
            };
            Box::pin(async move { result })
        }

        fn size(
            &self,
            hash: decdn_cache::Hash,
        ) -> std::pin::Pin<
            Box<dyn Future<Output = Result<Option<u64>, decdn_cache::OriginPullError>> + Send + '_>,
        > {
            let n = (self.answers_size && hash == self.hash)
                .then(|| u64::try_from(self.data.len()).unwrap_or(u64::MAX));
            Box::pin(async move { Ok(n) })
        }

        fn enumerate(
            &self,
        ) -> std::pin::Pin<
            Box<
                dyn Future<Output = Result<Vec<decdn_cache::Hash>, decdn_cache::OriginPullError>>
                    + Send
                    + '_,
            >,
        > {
            let out = if self.enumerable {
                vec![self.hash]
            } else {
                Vec::new()
            };
            Box::pin(async move { Ok(out) })
        }
    }

    /// A filesystem-shaped origin: enumerates and answers `size`, so its blob
    /// lands in the origin-held index.
    fn stub_origin(payload: &'static [u8], enumerable: bool) -> Arc<dyn decdn_cache::Origin> {
        Arc::new(StubOrigin {
            data: bytes::Bytes::from_static(payload),
            hash: decdn_cache::Hash::new(payload),
            enumerable,
            answers_size: enumerable,
        })
    }

    /// A remote-shaped origin: answers `size` (so it is this node's own
    /// content) but never enumerates, so nothing puts it in the origin-held
    /// index. Unpinned objects on S3/R2/HTTP have exactly this shape.
    fn remote_stub_origin(payload: &'static [u8]) -> Arc<dyn decdn_cache::Origin> {
        Arc::new(StubOrigin {
            data: bytes::Bytes::from_static(payload),
            hash: decdn_cache::Hash::new(payload),
            enumerable: false,
            answers_size: true,
        })
    }

    /// An origin that holds nothing: no enumerate, no size. Content fetched
    /// through it is foreign relay traffic, not this node's own.
    fn foreign_stub_origin(payload: &'static [u8]) -> Arc<dyn decdn_cache::Origin> {
        Arc::new(StubOrigin {
            data: bytes::Bytes::from_static(payload),
            hash: decdn_cache::Hash::new(payload),
            enumerable: false,
            answers_size: false,
        })
    }

    /// With relay on, the snapshot is the union of both halves — and a hash
    /// present in both appears once, so it draws one jitter offset rather
    /// than two.
    #[tokio::test]
    async fn holder_snapshot_unions_store_and_origin_held() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        // `stored` is fetchable but not enumerable (store half only);
        // `origin` is enumerable and also fetched below, so it lands in both.
        let stored = decdn_cache::Hash::new(b"holder-stored");
        let origin_held = decdn_cache::Hash::new(b"holder-origin");
        let cache = decdn_cache::CacheEngine::open(
            tmp.path(),
            vec![
                foreign_stub_origin(b"holder-stored"),
                stub_origin(b"holder-origin", true),
            ],
            16,
        )
        .await?;
        cache.get(stored).await?;
        cache.get(origin_held).await?;
        cache.rescan_origins().await;

        let snap = holder_snapshot(&cache, true).await;

        assert!(snap.store_error.is_none(), "the store walk must succeed");
        assert!(snap.hashes.contains(&stored), "store half missing");
        assert!(
            snap.hashes.contains(&origin_held),
            "origin-held half missing"
        );
        assert_eq!(snap.hashes.len(), 2, "the overlap must dedupe: {snap:?}");
        Ok(())
    }

    /// Origin-only nodes must not announce foreign store content: the store can
    /// hold what was relayed before the toggle was set, and the serve gate now
    /// declines it.
    #[tokio::test]
    async fn holder_snapshot_skips_foreign_store_content_under_origin_only() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let foreign = decdn_cache::Hash::new(b"holder-foreign");
        let own = decdn_cache::Hash::new(b"holder-own");
        let cache = decdn_cache::CacheEngine::open(
            tmp.path(),
            vec![
                foreign_stub_origin(b"holder-foreign"),
                stub_origin(b"holder-own", true),
            ],
            16,
        )
        .await?;
        cache.get(foreign).await?;
        cache.get(own).await?;
        cache.rescan_origins().await;

        let snap = holder_snapshot(&cache, false).await;

        assert!(
            !snap.hashes.contains(&foreign),
            "an origin-only node must not announce store-only content"
        );
        assert!(
            snap.hashes.contains(&own),
            "own (origin-held) content is announced regardless of the toggle"
        );
        assert!(
            snap.store_error.is_none(),
            "a skipped store walk is not a failed one"
        );
        Ok(())
    }

    /// ...but it MUST still announce its own store-only content.
    ///
    /// The origin-held index covers enumerable origins plus present pins, and a
    /// remote origin does not enumerate. An unpinned object this node owns,
    /// committed to the store with its insert event dropped, is in neither half
    /// unless the ownership probe puts it back — which is the whole recovery
    /// this sweep promises for an origin-only node.
    #[tokio::test]
    async fn holder_snapshot_recovers_own_store_only_content_under_origin_only()
    -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let mine = decdn_cache::Hash::new(b"policy-remote-own");
        let theirs = decdn_cache::Hash::new(b"policy-relayed");
        let cache = decdn_cache::CacheEngine::open(
            tmp.path(),
            vec![
                remote_stub_origin(b"policy-remote-own"),
                foreign_stub_origin(b"policy-relayed"),
            ],
            16,
        )
        .await?;
        cache.get(mine).await?;
        cache.get(theirs).await?;
        cache.rescan_origins().await;

        // Neither hash is in the origin-held index: the remote origin does not
        // enumerate, and neither is pinned. Without the ownership probe the
        // snapshot would be empty.
        assert!(
            cache.origin_held_hashes().is_empty(),
            "fixture precondition: nothing is enumerable or pinned"
        );

        let snap = holder_snapshot(&cache, false).await;

        assert!(
            snap.hashes.contains(&mine),
            "an origin-only node must recover its own store-only content"
        );
        assert!(
            !snap.hashes.contains(&theirs),
            "relayed content is not this node's to announce"
        );
        Ok(())
    }

    /// `cache_still_holds` gates every due entry. Origin-held content is never
    /// imported into the iroh-blobs store, so a store-only check would drop it
    /// at its first due time — advertised at probe time, never published.
    #[tokio::test]
    async fn cache_still_holds_accepts_origin_held_content() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin_only = decdn_cache::Hash::new(b"held-origin-only");
        let cache = decdn_cache::CacheEngine::open(
            tmp.path(),
            vec![stub_origin(b"held-origin-only", true)],
            16,
        )
        .await?;
        // Deliberately no `get`: the blob stays out of the store.
        cache.rescan_origins().await;
        assert!(
            !cache.has(origin_only).await?,
            "fixture precondition: the blob is not in the store"
        );

        let hash = ContentHash::from_bytes(*origin_only.as_bytes());
        assert!(
            cache_still_holds(&cache, &hash).await,
            "origin-held content must survive the due-time gate"
        );

        let absent = ContentHash::from_bytes(*decdn_cache::Hash::new(b"held-nowhere").as_bytes());
        assert!(
            !cache_still_holds(&cache, &absent).await,
            "content held nowhere must still be dropped"
        );
        Ok(())
    }

    /// An empty node yields an empty snapshot rather than an error — the
    /// sweep's no-op case.
    #[tokio::test]
    async fn holder_snapshot_on_an_empty_cache_is_empty() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let cache = decdn_cache::CacheEngine::open(tmp.path(), vec![], 16).await?;

        let snap = holder_snapshot(&cache, true).await;

        assert!(snap.hashes.is_empty(), "{snap:?}");
        assert!(snap.store_error.is_none());
        Ok(())
    }

    #[test]
    fn seed_cold_start_skips_already_scheduled_hashes() {
        // The lag sweep and the periodic origin rescan both seed a
        // scheduler that is already populated. A second entry for an
        // already-scheduled hash would drain twice — once at the earlier
        // due time and again on a later tick, once the tick path has
        // re-added the hash — buying a spurious republish per re-seed.
        let s = RepublishScheduler::new();
        let hashes: Vec<ContentHash> = (1u8..=5).map(h).collect();
        assert_eq!(s.seed_cold_start(hashes.iter().copied()), 5);

        assert_eq!(
            s.seed_cold_start(hashes.iter().copied()),
            0,
            "re-seeding an unchanged held set must schedule nothing new"
        );
        assert_eq!(s.len(), 5, "and must not grow the scheduled set");

        // Assert the heap directly. `len()` / `is_empty()` read the
        // `scheduled` set, which a duplicate entry does not grow, and a
        // single `drain_due` past every due time swallows duplicates via
        // its own `seen` filter — so neither can observe the invariant
        // that actually matters here.
        assert_eq!(
            s.heap.lock().map_or(usize::MAX, |h| h.len()),
            5,
            "a re-seed must push no second heap entry"
        );

        let deadline_us = now_us()
            .saturating_add(u64::try_from(COLD_START_MAX.as_micros()).unwrap())
            .saturating_add(1_000);
        let drained = s.drain_due(deadline_us);
        assert_eq!(drained.len(), 5, "each hash drains once: {drained:?}");
        assert!(s.is_empty());

        // A hash the tick path just drained is absent again, so the next
        // seed re-adds it — "already scheduled" must mean "has a live
        // entry", not "was ever scheduled".
        assert_eq!(
            s.seed_cold_start(hashes.iter().copied()),
            5,
            "a drained hash must be re-seedable"
        );
    }

    #[test]
    fn seed_cold_start_returns_only_newly_scheduled() {
        // Partial overlap is the sweep's real shape: some hashes were
        // committed inside the lag window and are missing, the rest are
        // already scheduled. The count is what the operator log reports,
        // so it must name the repair, not the walk.
        let s = RepublishScheduler::new();
        assert_eq!(s.seed_cold_start((1u8..=3).map(h)), 3);

        assert_eq!(
            s.seed_cold_start((1u8..=5).map(h)),
            2,
            "only the two hashes not already scheduled count"
        );
        assert_eq!(s.len(), 5);
        assert_eq!(
            s.heap.lock().map_or(usize::MAX, |h| h.len()),
            5,
            "the three overlapping hashes must not gain a second heap entry"
        );
    }

    /// The sweep reports the repair, not the walk: a scheduler that already
    /// holds some of the swept hashes counts only what it added. The
    /// `reseeded` count is what the operator log and
    /// `decdn_dht_republish_sweep_reseeded_total` report, so the distinction
    /// is load-bearing.
    #[tokio::test]
    async fn lag_sweep_counts_only_newly_scheduled_hashes() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let already = decdn_cache::Hash::new(b"sweep-already");
        let missing = decdn_cache::Hash::new(b"sweep-missing");
        let cache = decdn_cache::CacheEngine::open(
            tmp.path(),
            vec![
                foreign_stub_origin(b"sweep-already"),
                foreign_stub_origin(b"sweep-missing"),
            ],
            16,
        )
        .await?;
        cache.get(already).await?;
        cache.get(missing).await?;

        let scheduler = RepublishScheduler::new();
        scheduler.schedule_steady(ContentHash::from_bytes(*already.as_bytes()));
        let metrics = crate::metrics::Metrics::new();

        let outcome = lag_sweep(&cache, true, &scheduler, &metrics).await;

        assert_eq!(outcome.reseeded, 1, "only the unscheduled hash is a repair");
        assert!(!outcome.degraded, "a healthy store walk is not degraded");
        assert_eq!(scheduler.len(), 2);
        let text = metrics.encode()?;
        assert!(
            text.lines()
                .any(|l| l == "decdn_dht_republish_sweep_reseeded_total 1"),
            "the counter must report the repair, not the walk:\n{text}"
        );
        Ok(())
    }

    /// A lag that lands while a sweep owns the slot folds into that sweep and
    /// bumps the coalesced sibling, so `lag_sweeps_total` stays readable: the
    /// difference between the two is the number of walks actually started.
    ///
    /// Drives the slot protocol directly with the state pre-claimed. A
    /// concurrency-observing variant would have to win a race against a real
    /// worker's store walk to assert the same two arms.
    #[tokio::test]
    async fn a_lag_arriving_mid_sweep_coalesces_and_counts() -> anyhow::Result<()> {
        use std::sync::atomic::Ordering;

        let tmp = tempfile::tempdir()?;
        let cache =
            decdn_cache::CacheEngine::open(tmp.path(), vec![foreign_stub_origin(b"coalesce")], 16)
                .await?;
        let scheduler = Arc::new(RepublishScheduler::new());
        let metrics = Arc::new(crate::metrics::Metrics::new());
        // Pre-claimed: stand in for a worker mid-walk without racing one.
        let state = Arc::new(std::sync::atomic::AtomicU8::new(SWEEP_RUNNING));
        let shutdown = CancellationToken::new();
        let slot = SweepSlot {
            state: &state,
            cache: &cache,
            scheduler: &scheduler,
            metrics: &metrics,
            shutdown: &shutdown,
            relay_foreign_namespaces: true,
        };

        // First lag: claims the queue slot behind the running worker.
        spawn_lag_sweep(&slot);
        assert_eq!(state.load(Ordering::Acquire), SWEEP_QUEUED);
        // Second lag: the queued pass already covers it.
        spawn_lag_sweep(&slot);
        assert_eq!(state.load(Ordering::Acquire), SWEEP_QUEUED);

        let text = metrics.encode()?;
        assert!(
            text.lines()
                .any(|l| l == "decdn_dht_republish_lag_sweeps_coalesced_total 2"),
            "both folded lags must count, so a slot that never releases is \
             visible as a coalesced rate tracking the lag rate:\n{text}"
        );
        assert!(
            text.lines()
                .any(|l| l == "decdn_dht_republish_sweep_reseeded_total 0"),
            "coalescing must not walk the store:\n{text}"
        );
        Ok(())
    }

    /// A sweep must abandon its walk once shutdown starts.
    ///
    /// The runtime signals the republisher and then flushes and closes the
    /// cache store. A walk that keeps going sees the store disappear, takes the
    /// store-walk failure arm, and fires an alertable counter plus a
    /// degradation warning on an ordinary restart.
    #[tokio::test]
    async fn a_sweep_abandons_its_walk_once_shutdown_starts() -> anyhow::Result<()> {
        use std::sync::atomic::Ordering;

        let tmp = tempfile::tempdir()?;
        let held = decdn_cache::Hash::new(b"shutdown-sweep");
        let cache = decdn_cache::CacheEngine::open(
            tmp.path(),
            vec![foreign_stub_origin(b"shutdown-sweep")],
            16,
        )
        .await?;
        cache.get(held).await?;

        let scheduler = Arc::new(RepublishScheduler::new());
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let state = Arc::new(std::sync::atomic::AtomicU8::new(SWEEP_IDLE));
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        spawn_lag_sweep(&SweepSlot {
            state: &state,
            cache: &cache,
            scheduler: &scheduler,
            metrics: &metrics,
            shutdown: &shutdown,
            relay_foreign_namespaces: true,
        });

        // The worker is detached; poll for its guard releasing the slot.
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            if state.load(Ordering::Acquire) == SWEEP_IDLE {
                break;
            }
        }
        assert_eq!(
            state.load(Ordering::Acquire),
            SWEEP_IDLE,
            "the slot guard must release on the cancellation path"
        );
        assert_eq!(
            scheduler.len(),
            0,
            "a cancelled sweep must not have walked the store"
        );
        let text = metrics.encode()?;
        assert!(
            text.lines()
                .any(|l| l == "decdn_dht_republish_seed_store_walk_failures_total 0"),
            "a shutdown is not a store-walk degradation:\n{text}"
        );
        Ok(())
    }

    #[test]
    fn superseded_entry_does_not_drain_twice() {
        // The sweep-vs-eager and sweep-vs-tick races both end in this shape:
        // one hash, two heap entries, the superseded one already overdue. Only
        // the authoritative due time may drain; resurrecting the stale entry
        // buys a spurious republish.
        //
        // The state is built directly rather than through
        // seed-then-reschedule: both due times are drawn from jitter, so the
        // natural path cannot guarantee the stale entry is the overdue one,
        // and a test that only sometimes reaches the check proves nothing.
        let s = RepublishScheduler::new();
        let hash = h(1);
        let now = now_us();
        let stale_due = now.saturating_sub(1_000_000);
        let live_due = now.saturating_add(3_600_000_000);
        {
            let (mut heap, mut scheduled) = (s.heap.lock().unwrap(), s.scheduled.lock().unwrap());
            scheduled.insert(hash, live_due);
            schedule_at(&mut heap, hash, stale_due);
            schedule_at(&mut heap, hash, live_due);
        }

        assert!(
            s.drain_due(now).is_empty(),
            "the superseded entry must not drain"
        );
        assert_eq!(s.len(), 1, "and must not clear the live entry either");
        assert_eq!(
            s.drain_due(live_due),
            vec![hash],
            "the live entry still drains at its own due time"
        );
        assert!(s.is_empty());
    }

    #[test]
    fn unscheduled_tombstone_is_not_resurrected_by_a_later_seed() {
        // Evict-then-re-cache. `unschedule` cannot remove the heap entry, so
        // the tombstone must not drain the freshly scheduled hash ahead of its
        // own due time.
        let s = RepublishScheduler::new();
        let hash = h(2);
        s.seed_cold_start(std::iter::once(hash));
        let tombstone_due = s
            .scheduled
            .lock()
            .unwrap()
            .get(&hash)
            .copied()
            .expect("seeded hash has a due time");
        s.unschedule(&hash);
        assert!(s.is_empty(), "unschedule clears the live entry");

        // Re-cached, scheduled strictly later than the tombstone.
        let fresh_due = tombstone_due.saturating_add(1);
        {
            let (mut heap, mut scheduled) = (s.heap.lock().unwrap(), s.scheduled.lock().unwrap());
            scheduled.insert(hash, fresh_due);
            schedule_at(&mut heap, hash, fresh_due);
        }

        assert!(
            s.drain_due(tombstone_due).is_empty(),
            "the tombstone must not drain the re-scheduled hash early"
        );
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn seed_cold_start_empty_input_is_noop() {
        let s = RepublishScheduler::new();
        let n = s.seed_cold_start(std::iter::empty::<ContentHash>());
        assert_eq!(n, 0);
        assert!(s.is_empty());
    }

    #[test]
    fn steady_state_jitter_in_window() {
        // Drawing 100 steady-state offsets must all land in
        // [STEADY_STATE_MIN, STEADY_STATE_MAX].
        let lo = u64::try_from(STEADY_STATE_MIN.as_micros()).unwrap();
        let hi = u64::try_from(STEADY_STATE_MAX.as_micros()).unwrap();
        for _ in 0..100 {
            let j = jitter_us(STEADY_STATE_MIN, STEADY_STATE_MAX);
            assert!((lo..=hi).contains(&j), "offset {j} outside [{lo}, {hi}]");
        }
    }

    /// A panic under either scheduler guard poisons a sticky `Mutex`. The
    /// scheduler must keep scheduling and draining afterwards: an inert
    /// scheduler stops advertising every hash the node holds while every
    /// liveness signal stays green.
    #[test]
    fn a_poisoned_lock_does_not_wedge_the_scheduler() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let s = RepublishScheduler::new();
        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _heap = s.heap.lock().unwrap();
            let _scheduled = s.scheduled.lock().unwrap();
            panic!("poison both scheduler locks while holding the guards");
        }));
        assert!(poisoned.is_err());
        assert!(s.heap.is_poisoned());
        assert!(s.scheduled.is_poisoned());

        // Schedule in the past so the drain is deterministic.
        s.schedule_with_offset(h(1), 0);
        assert_eq!(s.len(), 1);
        assert!(!s.is_empty());
        assert_eq!(s.drain_due(now_us().saturating_add(1)), vec![h(1)]);
        assert_eq!(s.len(), 0);

        // Unschedule still reaches the map through the poisoned guard.
        s.schedule_with_offset(h(2), 0);
        s.unschedule(&h(2));
        assert!(s.drain_due(now_us().saturating_add(1)).is_empty());
    }

    #[test]
    fn entry_ordering_is_due_us_first() {
        // BinaryHeap with Reverse wrapper should yield earliest due_us
        // first. Verify the underlying Ord directly.
        let a = Entry {
            due_us: 100,
            hash: h(2),
        };
        let b = Entry {
            due_us: 50,
            hash: h(1),
        };
        assert!(b < a);
    }
}
