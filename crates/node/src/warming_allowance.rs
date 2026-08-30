//! Per-source serve-vindicated warming allowance (ADR 041).
//!
//! Speculative "warming" buys pull a blob before any client asks for it. The buy
//! can be wrong: a dud that never gets a second serve. This module bounds the
//! resulting loss **per upstream source node** (a bonded seller identity), so a
//! flood of at-market one-hit blobs from one attacker source can only drain that
//! source's small allowance, never the operational deposit.
//!
//! Each source holds a net profit-and-loss ledger, zero-centered and clamped to
//! `[-budget, +budget]`. A never-touched source has no ledger entry and reads as
//! available. A speculative buy debits the source's ledger by the buy cost and
//! tags the blob's hash with its source. Every serve of a tagged hash credits
//! the realized margin back to that source's ledger. A blob served at least
//! twice nets positive (vindicated, still warming); a dud served once nets
//! negative (the skim loss) and blocks further speculative buys from that
//! source until it recovers. Eviction forgets the tag so a stale hash can never
//! credit a ledger again. A slow time-refill on top of the serve credits
//! forgives a transient bad patch.
//!
//! # Seam
//!
//! [`WarmingAllowance`] is the ledger; [`WarmingCreditSink`] is the serve-path
//! boundary in front of it, mirroring [`crate::receipt_log::ReceiptSink`]. A
//! stream task ends its serve with a non-blocking
//! [`WarmingCreditSink::credit`], and the single background task from
//! [`spawn_warming_creditor`] applies the credit to the ledger. Tests use the
//! inline [`DirectWarmingCreditSink`] to keep the credit observable
//! synchronously.
//!
//! The bucket ledger is a plain `Mutex<HashMap>` and every operation on it is
//! O(1) — nothing iterates under the lock. The seam is not there to escape a
//! long hold; it is there so a serve's final step joins no queue at all behind
//! the buy loop and the eviction driver, which take that lock on their own
//! cadences while a stream is still holding its lane slot and floor
//! reservation.
//!
//! The hash-to-source tags live outside that lock in a [`DashMap`], so the
//! serve path resolves the source itself and enqueues `(source, units)`. That
//! is what keeps a deferred credit honest: the source is bound at serve time,
//! so an eviction and a re-warm from a different source between enqueue and
//! apply cannot redirect the credit (see [`WarmingCreditSink`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use dashmap::DashMap;
use decdn_cache::Hash;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::metrics::Metrics;
use crate::stop_handle::StopHandle;

/// The bonded upstream seller node a speculative warming buy is charged to.
///
/// A 32-byte node identity, kept distinct at the type level from a content
/// [`Hash`] so the serve-vindicated accounting can never transpose the two: the
/// seam passes a source and a hash side by side, and both are 32 bytes wide.
/// [`WarmingAllowance`] keys every ledger and tag on this type, so a caller that
/// swaps the arguments of [`WarmingAllowance::debit_speculative`] no longer
/// compiles.
#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct SourceId([u8; 32]);

impl SourceId {
    /// Wrap a raw node id. `const` so callers can build one in const contexts.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the inner bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for SourceId {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// One source node's warming ledger. `remaining` is a signed, zero-centered net
/// P&L: negative means the source is in the hole (blocked), positive means it
/// has banked surplus (capped at `budget`).
#[derive(Debug)]
struct Bucket {
    remaining: i64,
    last: Instant,
}

impl Bucket {
    fn fresh() -> Self {
        Self {
            remaining: 0,
            last: Instant::now(),
        }
    }
}

#[derive(Debug)]
struct State {
    buckets: HashMap<SourceId, Bucket>,
}

/// Bounds speculative warming losses per upstream source node.
///
/// See the module docs for the serve-vindicated accounting model.
#[derive(Debug)]
pub struct WarmingAllowance {
    budget: i64,
    refill_per_sec: u64,
    state: Mutex<State>,
    /// Which source speculatively bought each hash. Kept outside `state` so the
    /// serve path can resolve a hash's source without taking the bucket lock —
    /// see [`Self::source_for`], the first half of every deferred credit.
    source_of: DashMap<Hash, SourceId>,
}

impl WarmingAllowance {
    /// Creates a new allowance tracker. A source with no recorded activity
    /// reads as fully available.
    pub fn new(budget: u64, refill_per_sec: u64) -> Self {
        Self {
            budget: i64::try_from(budget).unwrap_or(i64::MAX),
            refill_per_sec,
            state: Mutex::new(State {
                buckets: HashMap::new(),
            }),
            source_of: DashMap::new(),
        }
    }

    /// Applies the time-based refill to one bucket, capped at `+budget`.
    fn refill(&self, bucket: &mut Bucket) {
        let elapsed_secs = bucket.last.elapsed().as_secs();
        let refill = self.refill_per_sec.saturating_mul(elapsed_secs);
        let refill = i64::try_from(refill).unwrap_or(i64::MAX);
        bucket.remaining = bucket.remaining.saturating_add(refill).min(self.budget);
        bucket.last = Instant::now();
    }

    /// Reports whether `source` currently has any warming allowance left. A
    /// source with no recorded activity has never spent anything and is
    /// available; an existing ledger is available only while its net P&L is
    /// still positive.
    pub fn available(&self, source: SourceId) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match state.buckets.get_mut(&source) {
            Some(bucket) => {
                self.refill(bucket);
                bucket.remaining > 0
            }
            None => true,
        }
    }

    /// Debits `source`'s ledger for a speculative buy of `hash` and tags the
    /// hash with its source so a later serve can credit the right ledger. The
    /// loss is floored at `-budget`, bounding the total possible drain from
    /// one source to one grief-cap's worth.
    ///
    /// The tag lands before the debit. The tag map and the bucket ledger are
    /// separate locks, so the two writes are not one atomic step; ordering them
    /// this way makes the only observable interleaving the harmless one. A
    /// serve that resolves the tag between the two sees the source and credits
    /// it, which is what the accounting wants. The reverse order would let a
    /// serve see a committed debit with no tag yet and silently drop its
    /// credit.
    pub fn debit_speculative(&self, source: SourceId, hash: Hash, units: u64) {
        self.source_of.insert(hash, source);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let bucket = state.buckets.entry(source).or_insert_with(Bucket::fresh);
        self.refill(bucket);
        let units = i64::try_from(units).unwrap_or(i64::MAX);
        bucket.remaining = bucket
            .remaining
            .saturating_sub(units)
            .max(self.budget.saturating_neg());
    }

    /// The source that speculatively bought `hash`, or `None` if the hash was
    /// never warmed or has since been forgotten.
    ///
    /// One lock-free shard read. The serve path calls this to bind a credit to
    /// the source that was tagged at serve time, so that a credit applied later
    /// cannot follow a tag that changed in between.
    #[must_use]
    pub fn source_for(&self, hash: Hash) -> Option<SourceId> {
        self.source_of.get(&hash).map(|e| *e.value())
    }

    /// Credits `units` of realized margin directly to `source`, capped at
    /// `+budget`. The half of [`Self::credit_serve`] that touches the ledger,
    /// split out so a deferred credit can resolve its source first.
    pub fn credit_source(&self, source: SourceId, units: u64) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let bucket = state.buckets.entry(source).or_insert_with(Bucket::fresh);
        self.refill(bucket);
        let units = i64::try_from(units).unwrap_or(i64::MAX);
        bucket.remaining = bucket.remaining.saturating_add(units).min(self.budget);
    }

    /// Credits the realized margin of a serve back to the source that
    /// speculatively bought `hash`. No-op if `hash` has no known source
    /// (never warmed, or forgotten since). The gain is capped at `+budget`.
    pub fn credit_serve(&self, hash: Hash, units: u64) {
        if let Some(source) = self.source_for(hash) {
            self.credit_source(source, units);
        }
    }

    /// Drops the hash-to-source tag, e.g. on cache eviction, so a stale hash
    /// can never credit a ledger again.
    pub fn forget(&self, hash: Hash) {
        self.source_of.remove(&hash);
    }
}

/// Capacity of the warming-credit queue. One credit is produced per completed
/// serve, and the aggregator applies each with a couple of in-memory map
/// operations, so this absorbs a burst far larger than the drain can fall
/// behind on.
pub const WARMING_CREDIT_CAPACITY: usize = 1024;

/// Non-blocking enqueue boundary for warming serve credits on the serve path.
///
/// A stream task calls [`Self::credit`] as its last act on a clean completion,
/// so the implementation MUST NOT take the bucket lock. Every ledger operation
/// is O(1), so the hazard is not a long hold — it is that the buy loop and the
/// eviction driver take that lock on their own cadences, and a serve completion
/// queued behind either keeps the stream's lane slot and floor reservation
/// alive for no reason. The runtime uses the channel sink from
/// [`spawn_warming_creditor`]; tests use [`DirectWarmingCreditSink`].
///
/// An implementation that defers the credit MUST resolve the hash's source
/// before it hands the credit off, and carry the source rather than the hash.
/// Resolving at apply time instead would let an eviction ([`WarmingAllowance::forget`])
/// and a re-warm from a different source land in between, and the credit would
/// follow the new tag — paying one source for another's serve.
pub trait WarmingCreditSink: Send + Sync {
    /// Best-effort, non-blocking credit of `units` for a clean serve of `hash`.
    /// Never blocks the caller on the bucket lock and never fails the serve.
    fn credit(&self, hash: Hash, units: u64);
}

/// Production [`WarmingCreditSink`]: resolves the hash's source, then enqueues
/// `(source, units)` to the background aggregator over a bounded channel.
///
/// A `Full` queue drops the credit and counts it
/// ([`Metrics::warming_credit_dropped`]). The drop is conservative in the
/// direction that matters: an un-applied credit leaves the source's ledger more
/// negative than reality, so at worst it blocks speculative buys from a source
/// that had earned its margin back, and the time refill forgives it. A
/// sustained non-zero rate means the aggregator is not keeping up and warming
/// is being throttled by bookkeeping loss rather than by real losses.
///
/// A `Closed` channel means the aggregator is gone. That is expected exactly
/// once, at shutdown; any other time it means the task died and *every*
/// subsequent credit is lost for the process lifetime, which drives every
/// source to blocked. The two are indistinguishable from here, so the first
/// occurrence warns and the rest stay quiet.
struct ChannelWarmingCreditSink {
    tx: mpsc::Sender<(SourceId, u64)>,
    allowance: Arc<WarmingAllowance>,
    metrics: Arc<Metrics>,
    /// Latches on the first `Closed`, so a dead aggregator warns once instead of
    /// once per completed serve.
    closed_logged: AtomicBool,
}

impl WarmingCreditSink for ChannelWarmingCreditSink {
    fn credit(&self, hash: Hash, units: u64) {
        // Bind the credit to the source tagged right now: the aggregator applies
        // it later, by which time an eviction and a re-warm could have retagged
        // the hash to someone else.
        let Some(source) = self.allowance.source_for(hash) else {
            return; // never warmed, or forgotten since — nothing to credit
        };
        match self.tx.try_send((source, units)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.warming_credit_dropped();
                tracing::debug!(
                    event = "warming_credit_dropped",
                    "warming serve credit dropped: aggregator queue full \
                     (the source ledger stays conservative and refills on time)"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.warming_credit_dropped();
                if !self.closed_logged.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        event = "warming_credit_sink_closed",
                        "warming serve credit dropped: aggregator gone. Expected \
                         during shutdown; at any other time every later credit is \
                         lost too and speculative warming will stop node-wide."
                    );
                }
            }
        }
    }
}

/// Synchronous [`WarmingCreditSink`] that applies each credit inline to a
/// [`WarmingAllowance`]. Test and loopback support only: it lets a test read the
/// ledger right after a serve instead of polling for a background drain.
///
/// It deliberately *violates* the [`WarmingCreditSink`] non-blocking contract
/// (it takes the ledger lock on the caller's thread), so it must never be wired
/// onto the serve path. The type and constructor stay `pub` only because the
/// cross-crate integration tests in `tests/` cannot see `#[cfg(test)]` items;
/// the field is private and the type is `#[doc(hidden)]` so it does not read as
/// a production knob.
#[doc(hidden)]
#[derive(Debug)]
pub struct DirectWarmingCreditSink(Arc<WarmingAllowance>);

impl DirectWarmingCreditSink {
    /// Wrap a ledger as an inline-crediting sink. Test and loopback use only —
    /// see the type docs; never wire this onto the serve path.
    #[must_use]
    pub const fn new(allowance: Arc<WarmingAllowance>) -> Self {
        Self(allowance)
    }
}

impl WarmingCreditSink for DirectWarmingCreditSink {
    fn credit(&self, hash: Hash, units: u64) {
        self.0.credit_serve(hash, units);
    }
}

/// Spawn the single background task that owns the credit queue and applies each
/// credit to `allowance`, returning the [`WarmingCreditSink`] the serve path
/// enqueues through and the [`StopHandle`] that stops the task.
///
/// This is what takes the bucket lock off the stream task (see the module
/// Seam): a serve completion resolves the source from a lock-free tag map and
/// does one bounded `try_send`, and this task takes the lock instead, behind
/// whichever pass of the buy loop or the eviction driver holds it.
///
/// The handle is returned rather than detached so shutdown can await the drain
/// and a task that died is visible as a join error instead of as credits that
/// quietly stop landing. [`StopHandle::shutdown`] ends the loop after it
/// flushes whatever is already queued (the handle owns the stop token beside
/// the task, so the join cannot hang on the sinks the client handler holds);
/// the loop also ends if every sink is dropped first.
pub fn spawn_warming_creditor(
    allowance: Arc<WarmingAllowance>,
    metrics: Arc<Metrics>,
) -> (Arc<dyn WarmingCreditSink>, StopHandle) {
    let (tx, rx) = mpsc::channel(WARMING_CREDIT_CAPACITY);
    let handle = {
        let allowance = Arc::clone(&allowance);
        StopHandle::spawn(move |shutdown| warming_creditor_loop(rx, allowance, shutdown))
    };
    (
        Arc::new(ChannelWarmingCreditSink {
            tx,
            allowance,
            metrics,
            closed_logged: AtomicBool::new(false),
        }),
        handle,
    )
}

/// Drain loop for the background aggregator (see [`spawn_warming_creditor`]).
/// Applies each credit FIFO until `shutdown` fires or every sink is dropped,
/// then flushes the already-enqueued tail so a credit that made it into the
/// queue before teardown still lands.
async fn warming_creditor_loop(
    mut rx: mpsc::Receiver<(SourceId, u64)>,
    allowance: Arc<WarmingAllowance>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            maybe = rx.recv() => match maybe {
                Some((source, units)) => allowance.credit_source(source, units),
                None => break,
            },
            () = shutdown.cancelled() => break,
        }
    }
    rx.close();
    while let Some((source, units)) = rx.recv().await {
        allowance.credit_source(source, units);
    }
    tracing::debug!("warming-credit aggregator drained and stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Spawn an aggregator with a throwaway metrics registry and stop token,
    /// for the tests that only care about the credit landing.
    fn creditor(
        allowance: &Arc<WarmingAllowance>,
    ) -> (Arc<dyn WarmingCreditSink>, StopHandle, Arc<Metrics>) {
        let metrics = Arc::new(Metrics::new());
        let (sink, handle) = spawn_warming_creditor(Arc::clone(allowance), Arc::clone(&metrics));
        (sink, handle, metrics)
    }

    /// Poll `cond` until it holds or the bound expires. The aggregator applies
    /// credits on its own task, so a test that reads the ledger straight after
    /// enqueueing would race the drain.
    async fn eventually(mut cond: impl FnMut() -> bool) -> bool {
        for _ in 0..500 {
            if cond() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    const S1: SourceId = SourceId::from_bytes([1u8; 32]);
    const S2: SourceId = SourceId::from_bytes([2u8; 32]);
    const H1: Hash = Hash::from_bytes([10u8; 32]);

    #[test]
    fn dud_drains_then_blocks() {
        let a = WarmingAllowance::new(1000, 0); // no time refill
        a.debit_speculative(S1, H1, 1000); // bought at full P_buy·mb
        a.credit_serve(H1, 600); // one serve (the requester): 0.6·P_sell·mb
        assert!(!a.available(S1)); // net -400: spent -> cut off (dud)
    }

    #[test]
    fn re_served_blob_refunds_and_keeps_warming() {
        let a = WarmingAllowance::new(1000, 0);
        a.debit_speculative(S1, H1, 1000);
        a.credit_serve(H1, 600); // serve #1
        a.credit_serve(H1, 600); // serve #2 -> net +200, capped at budget
        assert!(a.available(S1)); // vindicated
    }

    #[test]
    fn credit_is_capped_at_budget() {
        let a = WarmingAllowance::new(1000, 0);
        a.debit_speculative(S1, H1, 100);
        for _ in 0..100 {
            a.credit_serve(H1, 600);
        }
        // remaining never exceeds budget; a fresh source still reads full
        assert!(a.available(S2));
    }

    /// The background aggregator applies what the serve path enqueues: the same
    /// two-serve vindication the inline ledger records, reached through the
    /// channel instead.
    #[tokio::test]
    async fn channel_sink_credits_through_the_background_aggregator() {
        let allowance = Arc::new(WarmingAllowance::new(1000, 0));
        let (sink, _handle, _metrics) = creditor(&allowance);
        allowance.debit_speculative(S1, H1, 1000);
        sink.credit(H1, 600); // serve #1
        sink.credit(H1, 600); // serve #2 -> net +200, vindicated

        assert!(
            eventually(|| allowance.available(S1)).await,
            "the aggregator must apply the enqueued credits"
        );
    }

    /// A serve completion's credit must not wait on the ledger lock.
    ///
    /// The buy loop and the eviction driver take that lock for whole passes of
    /// their own bookkeeping. The credit goes over a bounded channel, so it
    /// lands while a pass holds the ledger; applying it inline on the stream
    /// task would make the serve's final step wait for that pass to finish.
    #[tokio::test]
    async fn credit_does_not_wait_on_the_ledger_lock() {
        let allowance = Arc::new(WarmingAllowance::new(1000, 0));
        let (sink, _handle, _metrics) = creditor(&allowance);
        allowance.debit_speculative(S1, H1, 1000);

        // Pin the ledger the way a buy-loop or eviction pass does.
        let held = allowance
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        // Credit from another thread so a wait is observable as a timeout
        // rather than a hung test.
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let serving = Arc::clone(&sink);
        let crediter = std::thread::spawn(move || {
            serving.credit(H1, 600);
            let _ = tx.send(());
        });
        let landed = rx.recv_timeout(Duration::from_secs(10)).is_ok();

        // Release the ledger and reap the thread before asserting, so a failure
        // reports rather than leaks the thread.
        drop(held);
        let joined = crediter.join().is_ok();
        assert!(landed, "a serve credit waited on the ledger lock");
        assert!(joined, "the crediting thread panicked");
    }

    /// A deferred credit is bound to the source tagged at serve time, not at
    /// apply time.
    ///
    /// Eviction forgets a hash's tag so a stale hash can never credit a ledger
    /// again, and a re-warm from a different source retags it. With the credit
    /// applied on a background task, both can happen between the serve and the
    /// apply — so resolving the tag at apply time would pay `S2` for a serve of
    /// `S1`'s blob. The sink resolves before it enqueues, which is what closes
    /// that window.
    #[tokio::test]
    async fn a_queued_credit_cannot_follow_a_retagged_hash() {
        let allowance = Arc::new(WarmingAllowance::new(1000, 0));
        let (sink, _handle, _metrics) = creditor(&allowance);

        // S1 warms H1 and serves it twice: enough to go net-positive.
        allowance.debit_speculative(S1, H1, 1000);
        sink.credit(H1, 600);
        sink.credit(H1, 600);

        // Eviction forgets the tag, then S2 warms the same hash — and spends
        // its whole budget doing so, so any credit landing on S2 shows up as
        // S2 becoming available again.
        allowance.forget(H1);
        allowance.debit_speculative(S2, H1, 1000);

        assert!(
            eventually(|| allowance.available(S1)).await,
            "the credits must land on the source that was tagged at serve time"
        );
        assert!(
            !allowance.available(S2),
            "a credit for S1's serve must never vindicate S2's speculative buy"
        );
    }

    /// A serve of a hash with no tag is not enqueued at all. Keeps the queue
    /// (and the drop counter) for credits that can actually be applied — the
    /// common case is an own-namespace or non-speculative hash, on every serve.
    #[tokio::test]
    async fn an_untagged_hash_never_reaches_the_queue() {
        let allowance = Arc::new(WarmingAllowance::new(1000, 0));
        let (sink, _handle, metrics) = creditor(&allowance);
        for _ in 0..(WARMING_CREDIT_CAPACITY * 4) {
            sink.credit(H1, 600); // never warmed -> no tag
        }
        assert!(
            !metrics_line_nonzero(&metrics, "decdn_warming_credits_dropped_total"),
            "untagged serves must not fill the queue or count as drops"
        );
    }

    /// A full queue drops the credit, counts it, and never blocks the serve.
    ///
    /// The drop must also be conservative: an un-applied credit leaves the
    /// source more negative than reality, which can only *block* speculative
    /// buys. The opposite direction — a credit applied twice, or a debit lost —
    /// would be a grief-cap bypass, so pin the direction rather than just the
    /// fact of the drop.
    #[tokio::test]
    async fn a_full_queue_drops_conservatively_and_counts_it() {
        let allowance = Arc::new(WarmingAllowance::new(1_000_000, 0));
        let metrics = Arc::new(Metrics::new());
        // No aggregator: nothing drains the queue, so it fills and stays full.
        let (tx, _rx) = mpsc::channel(WARMING_CREDIT_CAPACITY);
        let sink = ChannelWarmingCreditSink {
            tx,
            allowance: Arc::clone(&allowance),
            metrics: Arc::clone(&metrics),
            closed_logged: AtomicBool::new(false),
        };
        allowance.debit_speculative(S1, H1, 1_000_000);
        assert!(!allowance.available(S1), "the speculative buy drains S1");

        for _ in 0..(WARMING_CREDIT_CAPACITY + 64) {
            sink.credit(H1, 10_000); // returns promptly even once full
        }

        assert!(
            metrics_line_nonzero(&metrics, "decdn_warming_credits_dropped_total"),
            "a dropped credit must be counted, not only logged"
        );
        assert!(
            !allowance.available(S1),
            "a dropped credit must never move the ledger in the crediting direction"
        );
    }

    /// A dead or shut-down aggregator drops the credit, counts it, and does not
    /// fail the serve. This is the arm that means every later credit is lost
    /// too, which is why it warns rather than staying at `debug`.
    #[tokio::test]
    async fn a_closed_queue_drops_and_counts_without_failing_the_serve() {
        let allowance = Arc::new(WarmingAllowance::new(1000, 0));
        let metrics = Arc::new(Metrics::new());
        let (sink, handle) = spawn_warming_creditor(Arc::clone(&allowance), Arc::clone(&metrics));
        allowance.debit_speculative(S1, H1, 1000);

        // Stop the aggregator and let it finish, so the channel is closed.
        assert!(
            handle.shutdown().await.is_ok(),
            "the aggregator must exit cleanly"
        );

        sink.credit(H1, 600);
        assert!(
            metrics_line_nonzero(&metrics, "decdn_warming_credits_dropped_total"),
            "a credit into a closed queue must be counted"
        );
        assert!(
            !allowance.available(S1),
            "the dropped credit stays unapplied"
        );
    }

    /// The aggregator flushes what is already queued when it is asked to stop,
    /// so a credit from a serve that completed just before shutdown still lands.
    #[tokio::test]
    async fn shutdown_flushes_the_queued_tail() {
        let allowance = Arc::new(WarmingAllowance::new(1000, 0));
        let metrics = Arc::new(Metrics::new());
        let (sink, handle) = spawn_warming_creditor(Arc::clone(&allowance), Arc::clone(&metrics));
        allowance.debit_speculative(S1, H1, 1000);
        sink.credit(H1, 600);
        sink.credit(H1, 600);

        assert!(
            handle.shutdown().await.is_ok(),
            "the aggregator must exit cleanly"
        );
        assert!(
            allowance.available(S1),
            "credits enqueued before the stop signal must still be applied"
        );
    }

    /// Read one counter out of the `OpenMetrics` encoding. The registry is the
    /// operator-visible surface, so assert through it rather than the field.
    fn metrics_line_nonzero(metrics: &Metrics, name: &str) -> bool {
        let Ok(text) = metrics.encode() else {
            return false;
        };
        text.lines().any(|l| {
            l.split_once(' ')
                .is_some_and(|(k, v)| k == name && v.trim() != "0")
        })
    }

    #[test]
    fn sources_are_independent_and_forget_stops_credit() {
        let a = WarmingAllowance::new(1000, 0);
        a.debit_speculative(S1, H1, 1000);
        a.forget(H1);
        a.credit_serve(H1, 600); // no-op: tag gone
        assert!(!a.available(S1)); // still drained
        assert!(a.available(S2));
    }
}
