//! Per-source serve-vindicated warming allowance (ADR 041).
//!
//! Speculative "warming" buys pull a blob before any client asks for it. The buy
//! can be wrong: a dud that never gets a second serve. This module bounds the
//! resulting loss **per upstream source node** (a bonded seller identity), so a
//! flood of at-market one-hit blobs from one attacker source can only drain that
//! source's small allowance, never the operational deposit.
//!
//! Each source holds an allowance that starts full at `budget` and is clamped to
//! `[-budget, +budget]`. A never-touched source has no ledger entry and reads as
//! full. A speculative buy debits the source's ledger by the buy cost and tags
//! the blob's hash with its source. Every serve of a tagged hash credits the
//! realized margin back to that source's ledger. A blob re-served often enough
//! refunds its buy (vindicated; at a market price and the default operator
//! share, two serves do); a dud served once keeps the fee skim as a loss. The
//! source keeps warming while its allowance is positive, so its duds
//! spend the budget down, and once they exhaust it the source is blocked from
//! further speculative buys until it recovers. Eviction forgets the tag so a
//! stale hash can never credit a ledger again. A slow time-refill on top of the
//! serve credits forgives a transient bad patch.
//!
//! # Seam
//!
//! [`WarmingAllowance`] is the ledger; [`WarmingCreditSink`] is the serve-path
//! boundary in front of it, mirroring [`crate::receipt_log::ReceiptSink`]. A
//! stream task ends its serve with a non-blocking
//! [`WarmingCreditSink::credit`], and the single background task from
//! [`spawn_warming_creditor`] applies the credit to the ledger. Tests use the
//! inline `DirectWarmingCreditSink` (a `test-support` type) to keep the credit
//! observable synchronously.
//!
//! The bucket ledger is a plain `Mutex<HashMap>` and every operation on it is
//! O(1) — nothing iterates under the lock. The seam is not there to escape a
//! long hold. It is there so a serve's final step joins no queue at all: a
//! stream that has finished its bytes still holds its lane slot and its floor
//! reservation until that step returns, so waiting on a lock somebody else
//! happens to hold costs a serve slot for the length of the wait.
//!
//! Two things take that lock in the shipped wiring. The buy loop takes it on
//! the cache-miss path — [`WarmingAllowance::available`] to gate a speculative
//! pull, then [`WarmingAllowance::debit_speculative`] when one goes ahead. The
//! aggregator takes it to apply each queued credit. Eviction does not: its only
//! contact with the allowance is [`WarmingAllowance::forget`], which touches the
//! tag map alone.
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
/// [`struct@Hash`] so the serve-vindicated accounting can never transpose the two: the
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

    /// Consume into the inner bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl From<[u8; 32]> for SourceId {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<SourceId> for [u8; 32] {
    fn from(value: SourceId) -> Self {
        value.0
    }
}

/// One source node's warming ledger. `remaining` is the signed allowance left:
/// it starts at `budget`, stays warm while positive, and blocks the source once
/// speculative losses drive it to zero or below.
#[derive(Debug)]
struct Bucket {
    remaining: i64,
    last: Instant,
}

impl Bucket {
    /// A source's first ledger entry holds the full allowance (ADR 041 §The
    /// per-source warming allowance), the same state a never-touched source
    /// reads as.
    fn full(budget: i64) -> Self {
        Self {
            remaining: budget,
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
///
/// # Poisoning
///
/// Every acquisition of `state` recovers the inner value rather than
/// propagating the poison. The whole of each critical section is a `HashMap`
/// entry insert, an `Instant` read (`Instant::elapsed` saturates rather than
/// panicking), and saturating `i64` arithmetic, so there is no reachable unwind
/// and recovered state is never torn.
///
/// Recovering is also the safe direction on each of the three paths that take
/// the lock, which is why it is uniform here rather than split per method.
/// Skipping a [`Self::debit_speculative`] would let a source buy speculatively
/// without paying the grief cap for it, and skipping the aggregator's apply
/// would strand a serve's realized margin. Refusing on [`Self::available`] is
/// the conservative direction for that one call, but it blocks every
/// speculative buy node-wide for the process lifetime with no signal, which is
/// a worse operational failure than the unreachable one it guards against.
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
    /// source with no recorded activity holds the full budget and is
    /// available; an existing ledger is available only while its allowance is
    /// still positive.
    ///
    /// A zero budget disables warming ([`Self::enabled`]): every source reads as
    /// unavailable, including never-seen ones, so `economic_ceiling` never
    /// selects the warming regime and no speculative buy is authorized. Without
    /// this short-circuit a fresh source would read available (`None => true`)
    /// even with warming off, and buy at the market price for its first hash —
    /// the "zero budget disables warming" semantics would hold only after each
    /// source's first buy created its bucket.
    pub fn available(&self, source: SourceId) -> bool {
        if !self.enabled() {
            return false;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match state.buckets.get_mut(&source) {
            Some(bucket) => {
                self.refill(bucket);
                bucket.remaining > 0
            }
            None => true,
        }
    }

    /// Whether speculative warming is configured at all. A zero budget disables
    /// it: every above-floor buy is refused and no source ever has a positive
    /// allowance to spend. Callers gate the "source blocked" telemetry on this
    /// so a node with warming deliberately off does not report every source as
    /// griefed down.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.budget > 0
    }

    /// How many sources currently hold no warming allowance — a spent ledger at
    /// or below zero once its time refill is projected forward. Read-only: it
    /// projects each bucket's refill without persisting it (unlike
    /// [`Self::available`], which refills in place), so a metrics scrape never
    /// mutates the ledger. Sampled onto `decdn_warming_sources_blocked`.
    ///
    /// Reads zero when warming is disabled ([`Self::enabled`]), even if buckets
    /// linger from a config that previously ran warming on: with the budget at
    /// zero no source is being griefed down, so the gauge stays at zero across
    /// the reload rather than reporting every spent ledger as blocked.
    #[must_use]
    pub fn blocked_source_count(&self) -> usize {
        if !self.enabled() {
            return 0;
        }
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .buckets
            .values()
            .filter(|bucket| self.projected_remaining(bucket) <= 0)
            .count()
    }

    /// `bucket.remaining` with its time refill projected forward to now, capped
    /// at `+budget`, without writing it back. The read-only twin of the refill
    /// [`Self::available`] applies in place.
    fn projected_remaining(&self, bucket: &Bucket) -> i64 {
        let elapsed_secs = bucket.last.elapsed().as_secs();
        let refill = self.refill_per_sec.saturating_mul(elapsed_secs);
        let refill = i64::try_from(refill).unwrap_or(i64::MAX);
        bucket.remaining.saturating_add(refill).min(self.budget)
    }

    /// Debits `source`'s ledger for a speculative buy of `hash` and tags the
    /// hash with its source so a later serve can credit the right ledger.
    ///
    /// The buy loop checks [`Self::available`] when it selects a candidate and
    /// debits only after the pull completes, so concurrent pulls from one source
    /// can each pass the check and drive the ledger below zero: the real spend
    /// past the allowance is every speculative buy still in flight when it
    /// crossed zero. The ledger is floored at `-budget`, so however far those
    /// buys overshoot, the source needs at most `budget + 1` units of credit or
    /// refill to warm again. The floor bounds that recovery debt, not the spend.
    ///
    /// The tag is published while the bucket lock is held, before the debit.
    /// The serve path reads the tag map without that lock, but every credit
    /// reaches the ledger through `credit_source`, which takes it. So a
    /// serve that sees the tag has its credit applied only after this debit, and
    /// a serve that misses the tag also precedes the debit. Publishing the tag
    /// outside the lock would let a credit land on a bucket this debit is about
    /// to create full, where the `+budget` cap discards it; publishing it after
    /// the debit would let a serve see a committed debit with no tag and drop
    /// its credit.
    pub fn debit_speculative(&self, source: SourceId, hash: Hash, units: u64) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.source_of.insert(hash, source);
        let bucket = state
            .buckets
            .entry(source)
            .or_insert_with(|| Bucket::full(self.budget));
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
    /// One shard-scoped read on the tag map, never the bucket lock. The serve
    /// path calls this to bind a credit to the source that was tagged at serve
    /// time, so that a credit applied later cannot follow a tag that changed in
    /// between.
    #[must_use]
    fn source_for(&self, hash: Hash) -> Option<SourceId> {
        self.source_of.get(&hash).map(|e| *e.value())
    }

    /// Credits `units` of realized margin directly to `source`, capped at
    /// `+budget`. This is the ledger half of a serve credit; resolving the
    /// hash's source is a separate step the caller takes first, so a credit
    /// applied later still pays the source its tag named at serve time.
    fn credit_source(&self, source: SourceId, units: u64) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let bucket = state
            .buckets
            .entry(source)
            .or_insert_with(|| Bucket::full(self.budget));
        self.refill(bucket);
        let units = i64::try_from(units).unwrap_or(i64::MAX);
        bucket.remaining = bucket.remaining.saturating_add(units).min(self.budget);
    }

    /// Credits the realized margin of a serve back to the source that
    /// speculatively bought `hash`. No-op if `hash` has no known source
    /// (never warmed, or forgotten since). The gain is capped at `+budget`.
    ///
    /// Resolving the source and applying the credit in one step is what the
    /// shipped serve path must not do — it binds the source at apply time, so
    /// an eviction and a re-warm in between would pay the wrong source. The
    /// production path splits the two across the sink and the aggregator
    /// instead, which leaves this used only by `DirectWarmingCreditSink` and
    /// the tests that assert against it. It carries their gate for that reason.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn credit_serve(&self, hash: Hash, units: u64) {
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
/// is O(1), so the hazard is not a long hold — it is that a serve completion
/// which waits for the lock at all keeps the stream's lane slot and floor
/// reservation alive for the length of the wait. The buy loop holds that lock
/// on the cache-miss path and the aggregator holds it to apply credits, so a
/// serve that took the lock and arrived in either window would pay for it. That
/// is the cost this contract exists to refuse. The runtime uses the channel sink from
/// [`spawn_warming_creditor`]; tests use the `test-support`
/// `DirectWarmingCreditSink`.
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
/// direction that matters: an un-applied credit leaves the source's ledger lower
/// than reality, so at worst it blocks speculative buys from a source
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

/// A [`WarmingCreditSink`] that drops every credit. It is the inert default a
/// fresh [`crate::handlers::client::ClientHandlerDeps`] carries before the
/// runtime overwrites it with the channel sink from [`spawn_warming_creditor`].
/// A handler left with this default applies no warming credit, which only keeps
/// each source's ledger more conservative — the same safe direction a full or
/// closed aggregator queue takes.
#[derive(Debug, Default)]
pub struct NoopWarmingCreditSink;

impl WarmingCreditSink for NoopWarmingCreditSink {
    fn credit(&self, _hash: Hash, _units: u64) {}
}

/// Synchronous [`WarmingCreditSink`] that applies each credit inline to a
/// [`WarmingAllowance`]. Test and loopback support only: it lets a test read the
/// ledger right after a serve instead of polling for a background drain.
///
/// It deliberately *violates* the [`WarmingCreditSink`] non-blocking contract
/// (it takes the ledger lock on the caller's thread), so it must never be wired
/// onto the serve path. The `test-support` feature gate keeps it out of every
/// production build: it compiles only for this crate's own tests and for the
/// cross-crate integration tests in `tests/`, which cannot see `#[cfg(test)]`
/// items. The field is private and the type is `#[doc(hidden)]` so it does not
/// read as a production knob.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
#[derive(Debug)]
pub struct DirectWarmingCreditSink(Arc<WarmingAllowance>);

#[cfg(any(test, feature = "test-support"))]
impl DirectWarmingCreditSink {
    /// Wrap a ledger as an inline-crediting sink. Test and loopback use only —
    /// see the type docs; never wire this onto the serve path.
    #[must_use]
    pub const fn new(allowance: Arc<WarmingAllowance>) -> Self {
        Self(allowance)
    }
}

#[cfg(any(test, feature = "test-support"))]
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
/// Seam): a serve completion resolves the source from the tag map — one shard
/// read, never the bucket lock — and does one bounded `try_send`. This task
/// takes the bucket lock instead, where waiting behind the buy loop costs a
/// queued credit rather than a held lane slot.
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
        let metrics = Arc::clone(&metrics);
        StopHandle::spawn(move |shutdown| warming_creditor_loop(rx, allowance, metrics, shutdown))
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
///
/// Each applied credit bumps [`Metrics::warming_credit_applied`]. That is the
/// counter an operator reads beside the drop counter: a serve path that never
/// reaches the ledger drops nothing, so zero drops only means something next to
/// a non-zero apply count.
async fn warming_creditor_loop(
    mut rx: mpsc::Receiver<(SourceId, u64)>,
    allowance: Arc<WarmingAllowance>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) {
    let apply = |source, units| {
        allowance.credit_source(source, units);
        metrics.warming_credit_applied();
    };
    loop {
        tokio::select! {
            biased;
            maybe = rx.recv() => match maybe {
                Some((source, units)) => apply(source, units),
                None => break,
            },
            () = shutdown.cancelled() => break,
        }
    }
    rx.close();
    while let Some((source, units)) = rx.recv().await {
        apply(source, units);
    }
    tracing::debug!("warming-credit aggregator drained and stopped");
}

#[cfg(test)]
mod tests;
