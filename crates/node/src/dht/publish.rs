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
//! ([`holder_snapshot`]) and seeds whatever is missing. Seeding is
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
#[allow(missing_debug_implementations)]
pub struct RepublishScheduler {
    heap: Arc<Mutex<BinaryHeap<Reverse<Entry>>>>,
    /// Set of hashes currently scheduled — `O(1)` dedupe on cache
    /// insert events without walking the heap. Stays in sync with the
    /// heap via every push / pop path.
    scheduled: Arc<Mutex<HashSet<ContentHash>>>,
}

impl RepublishScheduler {
    /// Construct an empty scheduler.
    #[must_use]
    pub fn new() -> Self {
        Self {
            heap: Arc::new(Mutex::new(BinaryHeap::new())),
            scheduled: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Number of hashes currently scheduled.
    #[must_use]
    pub fn len(&self) -> usize {
        self.scheduled.lock().map_or(0, |s| s.len())
    }

    /// Whether the scheduler is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.scheduled.lock().is_ok_and(|s| s.is_empty())
    }

    /// Schedule `hash` with the steady-state jitter window (30–50 min).
    /// Re-scheduling a hash that's already scheduled pushes a fresh
    /// heap entry; the prior entry isn't removed. `drain_due` dedupes
    /// on pop via the `scheduled` set, so a hash drains at most once
    /// per due window regardless of how many stale heap entries it
    /// has.
    pub fn schedule_steady(&self, hash: ContentHash) {
        let offset = jitter_us(STEADY_STATE_MIN, STEADY_STATE_MAX);
        self.schedule_with_offset(hash, offset);
    }

    /// Batch-schedule every hash in `iter` with an independent
    /// cold-start jitter draw (uniform(0, 40 min) per hash, NOT a
    /// shared timestamp). Callers should pass
    /// [`decdn_cache::CacheEngine::iter_hashes`] results (NOT
    /// `access_times_snapshot`) — see the `iter_hashes` rustdoc for
    /// the rationale.
    ///
    /// Seeding is idempotent: a hash already scheduled keeps its
    /// existing due time and gets no second heap entry. That matters
    /// wherever the seed runs against a non-empty scheduler — the
    /// lag sweep in [`run_republish`] and the periodic origin
    /// rescan — because a duplicate entry drains a second time on a
    /// later tick and buys a spurious republish per re-seed.
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

    fn schedule_with_offset(&self, hash: ContentHash, offset_us: u64) {
        let due_us = now_us().saturating_add(offset_us);
        // Add to the scheduled set first; if the hash was already
        // scheduled, the prior heap entry is left in place and
        // `drain_due` will resolve via the scheduled set when it pops.
        if let Ok(mut s) = self.scheduled.lock() {
            s.insert(hash);
        }
        if let Ok(mut heap) = self.heap.lock() {
            schedule_at(&mut heap, hash, due_us);
        }
    }

    /// Schedule `hash` at `now + offset_us` only if it is not already
    /// scheduled. Returns whether it was added.
    ///
    /// The `scheduled` set is the dedupe key, and it is also what
    /// [`Self::drain_due`] pops from — so "already scheduled" means
    /// "has a live entry that will drain", never "was ever scheduled".
    /// A hash the tick path just drained is absent again and re-seeds
    /// normally.
    fn schedule_if_absent(&self, hash: ContentHash, offset_us: u64) -> bool {
        let due_us = now_us().saturating_add(offset_us);
        // Both guards are held across the test-and-push so a concurrent
        // `drain_due` cannot pop the hash out of `scheduled` between the
        // membership test and the heap push, which would strand the new
        // entry as a tombstone. Acquired heap-then-scheduled to match
        // `drain_due`'s order — the reverse would deadlock against it.
        let (Ok(mut heap), Ok(mut s)) = (self.heap.lock(), self.scheduled.lock()) else {
            return false;
        };
        if !s.insert(hash) {
            return false;
        }
        schedule_at(&mut heap, hash, due_us);
        true
    }

    /// Drain all entries whose `due_us <= now_us`. Returns the popped
    /// hashes after deduplicating via the `scheduled` set (a hash
    /// removed from `scheduled` since being heap-pushed is ignored).
    fn drain_due(&self, now_us: u64) -> Vec<ContentHash> {
        let mut out = Vec::new();
        let mut seen = HashSet::<ContentHash>::new();
        if let (Ok(mut heap), Ok(mut s)) = (self.heap.lock(), self.scheduled.lock()) {
            while let Some(Reverse(Entry { due_us, hash })) = heap.peek().copied() {
                if due_us > now_us {
                    break;
                }
                heap.pop();
                // The scheduled set is the truth — if a hash was
                // unscheduled (e.g. evicted from cache) but the heap
                // still carries an entry, drop it.
                if !s.contains(&hash) {
                    continue;
                }
                if seen.insert(hash) {
                    out.push(hash);
                }
                // Pop from the scheduled set so the next `schedule_*`
                // for this hash can re-add it. The caller is
                // responsible for re-scheduling after a successful
                // publish (steady-state window).
                s.remove(&hash);
            }
        }
        out
    }

    /// Unschedule `hash` — used when the cache evicts the blob.
    pub fn unschedule(&self, hash: &ContentHash) {
        if let Ok(mut s) = self.scheduled.lock() {
            s.remove(hash);
        }
        // The heap entry is left in place; `drain_due` filters via the
        // scheduled set when it eventually pops.
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
/// every bulk seed of the republish scheduler: bring-up cold start and the lag
/// sweep in [`run_republish`] both need the same set, and computing it in one
/// place is what keeps them from drifting apart.
#[derive(Debug)]
pub struct HolderSnapshot {
    /// The union. Deduplicated, so a hash that is both stored and origin-held
    /// draws one jitter offset rather than two.
    pub hashes: HashSet<decdn_cache::Hash>,
    /// `Some` when the store walk failed. The origin-held half is still in
    /// `hashes`; the caller decides how loudly to report the degradation, since
    /// bring-up and the sweep phrase it differently.
    pub store_error: Option<decdn_cache::CacheError>,
}

/// Collect the [`HolderSnapshot`] for `cache`.
///
/// Under the origin-only policy (`relay_foreign_namespaces == false`) the store
/// half is skipped: the store may hold leftover foreign content from before the
/// toggle was set, and announcing it would advertise blobs the serve gate now
/// declines. Own content is unaffected — it lives in the origin-held index
/// regardless of the toggle.
///
/// Never fails: a store-walk error degrades to the origin-held half rather than
/// yielding nothing, because a partial announce strictly beats none.
pub async fn holder_snapshot(
    cache: &decdn_cache::CacheEngine,
    relay_foreign_namespaces: bool,
) -> HolderSnapshot {
    let mut hashes: HashSet<decdn_cache::Hash> = cache.origin_held_hashes().into_iter().collect();
    let mut store_error = None;
    if relay_foreign_namespaces {
        match cache.iter_hashes().await {
            Ok(stored) => hashes.extend(stored),
            Err(err) => store_error = Some(err),
        }
    }
    HolderSnapshot {
        hashes,
        store_error,
    }
}

/// Re-seed the republish scheduler from local state after the cache-commit
/// event window was lost.
///
/// Returns the number of hashes newly scheduled. Seeding draws
/// `uniform(0, 40 min)` per record, so a sweep of C blobs spreads over the
/// steady-state mean cycle instead of firing as one burst — ADR 022 §Bootstrap
/// makes the single-tick bulk republish non-conforming for exactly this reason,
/// which is why the sweep goes through the scheduler and never calls
/// [`publish_batch`] directly.
async fn lag_sweep(
    cache: &decdn_cache::CacheEngine,
    relay_foreign_namespaces: bool,
    scheduler: &RepublishScheduler,
    metrics: &crate::metrics::Metrics,
) -> usize {
    let snapshot = holder_snapshot(cache, relay_foreign_namespaces).await;
    if let Some(err) = &snapshot.store_error {
        metrics.dht_republish_sweep_failure();
        tracing::warn!(
            error = %err,
            "dht republish: lag sweep could not walk the store; re-seeding the \
             origin-held half only. Blobs held only in the store stay \
             un-republished until the next sweep or restart"
        );
    }
    let reseeded = scheduler.seed_cold_start(
        snapshot
            .hashes
            .into_iter()
            .map(|h| ContentHash::from_bytes(*h.as_bytes())),
    );
    metrics.dht_republish_sweep_reseeded(u64::try_from(reseeded).unwrap_or(u64::MAX));
    reseeded
}

/// Long-running task: consumes a [`broadcast::Receiver<Hash>`] from
/// the cache, drives the scheduler heap, and fans out `Store` requests
/// to the K+3 closest peers per due hash. Exits on `stop_rx` or when
/// the cache subscribe channel closes.
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
    // One sweep at a time. A lag is a symptom of sustained commit pressure, so
    // `Lagged` commonly repeats; without this each one would start another full
    // store walk on top of the last.
    let sweep_running = Arc::new(std::sync::atomic::AtomicBool::new(false));
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
                        // Detached, like the batch publish below: the store
                        // walk costs one `status()` per blob, and running it
                        // on the select loop would stall shutdown and the
                        // eager per-insert publishes. Abandoned on shutdown
                        // — the next lag, or the next boot, re-seeds.
                        if sweep_running
                            .compare_exchange(
                                false,
                                true,
                                std::sync::atomic::Ordering::AcqRel,
                                std::sync::atomic::Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            let cache = cache.clone();
                            let scheduler = Arc::clone(&scheduler);
                            let metrics = Arc::clone(&metrics);
                            let running = Arc::clone(&sweep_running);
                            tokio::spawn(async move {
                                let reseeded = lag_sweep(
                                    &cache,
                                    relay_foreign_namespaces,
                                    &scheduler,
                                    &metrics,
                                )
                                .await;
                                running.store(false, std::sync::atomic::Ordering::Release);
                                tracing::info!(
                                    reseeded,
                                    "dht republish: lag sweep complete"
                                );
                            });
                        } else {
                            tracing::debug!(
                                "dht republish: lag sweep already running; \
                                 the in-flight one covers this lag too"
                            );
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Cache dropped — nothing more to schedule.
                        // We don't exit yet; the heap may still have
                        // due entries to publish.
                        tracing::debug!("dht republish: cache insert channel closed");
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

/// True iff the cache still holds `hash` (and the operator hasn't
/// explicitly evicted it). The blob-presence check is what stops the
/// scheduler from re-publishing content that LRU drift or an
/// operator-evict already removed from the local store.
async fn cache_still_holds(cache: &decdn_cache::CacheEngine, hash: &ContentHash) -> bool {
    let h = iroh_blobs::Hash::from_bytes(*hash.as_bytes());
    if cache.refuses(h) {
        return false;
    }
    cache.has(h).await.unwrap_or(false)
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
    let targets: Vec<NodeId> = if let Ok(table) = routing.lock() {
        // ADR 022 §STORE Flow line 128 specifies K+3 (= 23) closest
        // nodes — the three positions beyond K are overflow targets
        // so an attacker suppressing receivers has to take down K+3
        // hosts rather than K. The default `closest` caps at K (=
        // wire `MAX_CLOSER_NODES`), which we MUST NOT use here;
        // `closest_unbounded` returns up to `n` peers regardless of
        // the wire cap.
        table.closest_unbounded(hash.as_bytes(), REPUBLISH_FANOUT)
    } else {
        tracing::error!("dht republish: routing-table mutex poisoned");
        return;
    };
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
    let groups = {
        let Ok(table) = routing.lock() else {
            tracing::error!("dht republish: routing-table mutex poisoned");
            return;
        };
        group_by_receiver(&table, hashes)
    };
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

    /// Single-blob stub origin. `enumerate` + `size` are what put a hash in
    /// the origin-held index; `fetch` is what puts it in the store. Which of
    /// the two a test wants is set per-instance, so one type covers the
    /// store half, the origin-held half, and the overlap between them.
    #[derive(Debug)]
    struct StubOrigin {
        data: bytes::Bytes,
        hash: decdn_cache::Hash,
        enumerable: bool,
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
            let n = (self.enumerable && hash == self.hash)
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

    fn stub_origin(payload: &'static [u8], enumerable: bool) -> Arc<dyn decdn_cache::Origin> {
        Arc::new(StubOrigin {
            data: bytes::Bytes::from_static(payload),
            hash: decdn_cache::Hash::new(payload),
            enumerable,
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
                stub_origin(b"holder-stored", false),
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

    /// Origin-only nodes must not announce the store half: it can hold
    /// foreign content left over from before the toggle was set, which the
    /// serve gate now declines. Own content is in the origin-held index and
    /// is unaffected.
    #[tokio::test]
    async fn holder_snapshot_skips_store_when_relay_foreign_namespaces_is_false()
    -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let foreign = decdn_cache::Hash::new(b"holder-foreign");
        let own = decdn_cache::Hash::new(b"holder-own");
        let cache = decdn_cache::CacheEngine::open(
            tmp.path(),
            vec![
                stub_origin(b"holder-foreign", false),
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

        // Drain far past the window: every hash appears exactly once, so
        // no duplicate entry survived. `drain_due` dedupes within one
        // call, so also assert the scheduler is empty afterwards — a
        // duplicate would still be sitting in the heap.
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
