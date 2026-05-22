//! DHT republish scheduler (ADR 022 §STORE Flow & §Bootstrap).
//!
//! On every successful blob commit ([`decdn_cache::CacheEngine::subscribe_inserts`])
//! and on a periodic per-record jittered timer the scheduler:
//!
//!  1. Computes the K+3 closest peers to the blob's hash from the
//!     local routing table.
//!  2. Sends `StoreRequest { hash, holder: self }` to each in
//!     parallel.
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
//! Implementation: a single tokio task drives all records via a
//! `BinaryHeap<(due_at, hash)>`. The heap is small (one entry per
//! cached blob, capped by the cache's blob count) and the next
//! `due_at` drives the task's sleep duration. Sending a `Store` does
//! not hold the heap mutex; the K+3 fan-out happens in spawned tasks
//! so a slow receiver doesn't stall the rest of the schedule.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::{Endpoint, EndpointAddr, PublicKey};
use rand::RngExt;
use tokio::sync::{broadcast, oneshot};

use crate::dht::client;
use crate::dht::routing::{NodeId, RoutingTable};

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
    hash: [u8; 32],
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
fn schedule_at(heap: &mut BinaryHeap<Reverse<Entry>>, hash: [u8; 32], due_us: u64) {
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
    scheduled: Arc<Mutex<HashSet<[u8; 32]>>>,
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
    pub fn schedule_steady(&self, hash: [u8; 32]) {
        let offset = jitter_us(STEADY_STATE_MIN, STEADY_STATE_MAX);
        self.schedule_with_offset(hash, offset);
    }

    /// Schedule `hash` with the cold-start jitter window (0–40 min).
    /// Used at boot for every blob already in the cache.
    pub fn schedule_cold_start(&self, hash: [u8; 32]) {
        let offset = jitter_us(Duration::ZERO, COLD_START_MAX);
        self.schedule_with_offset(hash, offset);
    }

    /// Batch-schedule every hash in `iter` with an independent
    /// cold-start jitter draw (uniform(0, 40 min) per hash, NOT a
    /// shared timestamp). Consumed by the runtime at startup over
    /// [`decdn_cache::CacheEngine::access_times_snapshot`] so any
    /// blob already on disk gets a republish entry without waiting
    /// for `subscribe_inserts` (which only fires on fresh
    /// pull-through commits, not on cache reuse across restarts).
    /// Returns the number of hashes scheduled — useful for the
    /// startup log line so operators can see how big the cold-start
    /// queue is.
    pub fn seed_cold_start<I>(&self, iter: I) -> usize
    where
        I: IntoIterator<Item = [u8; 32]>,
    {
        let mut count = 0usize;
        for hash in iter {
            self.schedule_cold_start(hash);
            count = count.saturating_add(1);
        }
        count
    }

    fn schedule_with_offset(&self, hash: [u8; 32], offset_us: u64) {
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

    /// Drain all entries whose `due_us <= now_us`. Returns the popped
    /// hashes after deduplicating via the `scheduled` set (a hash
    /// removed from `scheduled` since being heap-pushed is ignored).
    fn drain_due(&self, now_us: u64) -> Vec<[u8; 32]> {
        let mut out = Vec::new();
        let mut seen = HashSet::<[u8; 32]>::new();
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
    pub fn unschedule(&self, hash: &[u8; 32]) {
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
    mut cache_inserts: broadcast::Receiver<iroh_blobs::Hash>,
    mut stop_rx: oneshot::Receiver<()>,
) {
    let self_id_bytes = *self_id.as_bytes();
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
                        let hash_bytes = *hash.as_bytes();
                        // ADR 022 §STORE Flow steps 2–3: publish
                        // *immediately* on cache insertion (step 2),
                        // then schedule the next republish at `T +
                        // uniform(30, 50) min` where T is the local
                        // wall-clock at step 2. Without the initial
                        // publish a freshly-cached blob isn't
                        // discoverable for up to 50 minutes — the
                        // exact failure mode the eager publish
                        // closes.
                        publish_hash(&endpoint, self_id_bytes, &routing, hash_bytes).await;
                        scheduler.schedule_steady(hash_bytes);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // ADR 022 §STORE Flow & doc on
                        // `subscribe_inserts`: lagged consumers force a
                        // full republish sweep. We don't have a cache
                        // `iter_hashes` API yet (filed for follow-up);
                        // log so the operator sees the gap and can
                        // restart the node if the cache changed
                        // unpredictably during the lag.
                        tracing::warn!(
                            missed = n,
                            "dht republish: cache-insert channel lagged; \
                             missed hashes will republish on the next \
                             cold-start (or operator restart)"
                        );
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
                for hash in due {
                    // ADR 022 §Content Records and TTL line 122: "A
                    // node stops re-publishing when it evicts the
                    // blob." Verify the blob is still held before
                    // each republish; if it's been evicted, drop the
                    // scheduler entry instead of re-adding it.
                    if !cache_still_holds(&cache, &hash).await {
                        scheduler.unschedule(&hash);
                        continue;
                    }
                    publish_hash(&endpoint, self_id_bytes, &routing, hash).await;
                    // Re-schedule with the steady-state jitter window;
                    // ADR 022 line 130 — fresh jitter draw per record
                    // per cycle.
                    scheduler.schedule_steady(hash);
                }
            }
        }
    }
}

/// True iff the cache still holds `hash` (and the operator hasn't
/// explicitly evicted it). The blob-presence check is what stops the
/// scheduler from re-publishing content that LRU drift or an
/// operator-evict already removed from the local store.
async fn cache_still_holds(cache: &decdn_cache::CacheEngine, hash: &[u8; 32]) -> bool {
    let h = iroh_blobs::Hash::from_bytes(*hash);
    if cache.is_evicted(h) {
        return false;
    }
    cache.has(h).await.unwrap_or(false)
}

/// Send a `Store` to the K+3 closest peers for `hash` in parallel.
/// Failures are logged at debug level — a missed receiver in this cycle
/// will be retried on the next cycle (or on the next cold start).
async fn publish_hash(
    endpoint: &Endpoint,
    self_id_bytes: [u8; 32],
    routing: &Arc<Mutex<RoutingTable>>,
    hash: [u8; 32],
) {
    let targets: Vec<NodeId> = if let Ok(table) = routing.lock() {
        // ADR 022 §STORE Flow line 128 specifies K+3 (= 23) closest
        // nodes — the three positions beyond K are overflow targets
        // so an attacker suppressing receivers has to take down K+3
        // hosts rather than K. The default `closest` caps at K (=
        // wire `MAX_CLOSER_NODES`), which we MUST NOT use here;
        // `closest_unbounded` returns up to `n` peers regardless of
        // the wire cap.
        table.closest_unbounded(&hash, REPUBLISH_FANOUT)
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
            let target_pk = match PublicKey::from_bytes(&peer) {
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
            match client::store(&endpoint_cloned, addr, hash, self_id_bytes).await {
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    fn h(b: u8) -> [u8; 32] {
        [b; 32]
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
        let hashes: Vec<[u8; 32]> = (1u8..=5).map(h).collect();
        let n = s.seed_cold_start(hashes.iter().copied());
        assert_eq!(n, 5);
        assert_eq!(s.len(), 5);
        let deadline_us = now_us()
            .saturating_add(u64::try_from(COLD_START_MAX.as_micros()).unwrap())
            .saturating_add(1_000); // 1 ms slack for drift between seed and check
        let drained: HashSet<[u8; 32]> = s.drain_due(deadline_us).into_iter().collect();
        let expected: HashSet<[u8; 32]> = hashes.into_iter().collect();
        assert_eq!(drained, expected);
        assert!(
            s.is_empty(),
            "drain_due at deadline should empty the scheduler"
        );
    }

    #[test]
    fn seed_cold_start_empty_input_is_noop() {
        let s = RepublishScheduler::new();
        let n = s.seed_cold_start(std::iter::empty::<[u8; 32]>());
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
