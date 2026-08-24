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
//! [`spawn_warming_creditor`] applies the credit to the ledger. So the buy loop
//! and the eviction driver, which take the ledger lock for their own
//! bookkeeping, cannot add latency to a serve's final step. Tests use the
//! inline [`DirectWarmingCreditSink`] to keep the credit observable
//! synchronously.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::mpsc;

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
    buckets: HashMap<[u8; 32], Bucket>,
    source_of: HashMap<[u8; 32], [u8; 32]>,
}

/// Bounds speculative warming losses per upstream source node.
///
/// See the module docs for the serve-vindicated accounting model.
#[derive(Debug)]
pub struct WarmingAllowance {
    budget: i64,
    refill_per_sec: u64,
    state: Mutex<State>,
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
                source_of: HashMap::new(),
            }),
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
    pub fn available(&self, source: [u8; 32]) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
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
    pub fn debit_speculative(&self, source: [u8; 32], hash: [u8; 32], units: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let bucket = state.buckets.entry(source).or_insert_with(Bucket::fresh);
        self.refill(bucket);
        let units = i64::try_from(units).unwrap_or(i64::MAX);
        bucket.remaining = bucket
            .remaining
            .saturating_sub(units)
            .max(self.budget.saturating_neg());
        state.source_of.insert(hash, source);
    }

    /// Credits the realized margin of a serve back to the source that
    /// speculatively bought `hash`. No-op if `hash` has no known source
    /// (never warmed, or forgotten since). The gain is capped at `+budget`.
    pub fn credit_serve(&self, hash: [u8; 32], units: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(&source) = state.source_of.get(&hash) else {
            return;
        };
        let bucket = state.buckets.entry(source).or_insert_with(Bucket::fresh);
        self.refill(bucket);
        let units = i64::try_from(units).unwrap_or(i64::MAX);
        bucket.remaining = bucket.remaining.saturating_add(units).min(self.budget);
    }

    /// Drops the hash-to-source tag, e.g. on cache eviction, so a stale hash
    /// can never credit a ledger again.
    pub fn forget(&self, hash: [u8; 32]) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.source_of.remove(&hash);
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
/// so the implementation MUST NOT take the ledger lock: the buy loop and the
/// eviction driver hold that lock for their own passes, and a serve completion
/// waiting on them would hold the stream's lane slot and floor reservation for
/// the duration. The runtime uses the channel sink from
/// [`spawn_warming_creditor`]; tests use [`DirectWarmingCreditSink`].
pub trait WarmingCreditSink: Send + Sync {
    /// Best-effort, non-blocking credit of `units` for a clean serve of `hash`.
    /// Never blocks the caller on the ledger and never fails the serve.
    fn credit(&self, hash: [u8; 32], units: u64);
}

/// Production [`WarmingCreditSink`]: enqueues to the background aggregator over
/// a bounded channel.
///
/// A `Full` queue drops the credit. The drop is conservative in the direction
/// that matters: an un-applied credit leaves the source's ledger more negative
/// than reality, so at worst it blocks speculative buys from a source that had
/// earned its margin back, and the time refill forgives that within seconds.
/// A `Closed` channel means the aggregator stopped, which happens at shutdown
/// once the last sink is gone.
struct ChannelWarmingCreditSink {
    tx: mpsc::Sender<([u8; 32], u64)>,
}

impl WarmingCreditSink for ChannelWarmingCreditSink {
    fn credit(&self, hash: [u8; 32], units: u64) {
        match self.tx.try_send((hash, units)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::debug!(
                    event = "warming_credit_dropped",
                    "warming serve credit dropped: aggregator queue full \
                     (the source ledger stays conservative and refills on time)"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!(
                    event = "warming_credit_sink_closed",
                    "warming serve credit dropped: aggregator gone \
                     (expected only during shutdown)"
                );
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
    fn credit(&self, hash: [u8; 32], units: u64) {
        self.0.credit_serve(hash, units);
    }
}

/// Spawn the single background task that owns the credit queue and applies each
/// credit to `allowance`, returning the [`WarmingCreditSink`] the serve path
/// enqueues through.
///
/// This is what takes the ledger lock off the stream task (see the module
/// Seam): a serve completion does one bounded `try_send`, and this task waits
/// on the lock instead, behind whichever pass of the buy loop or the eviction
/// driver holds it.
///
/// The task ends when the last sink is dropped, which is the router teardown at
/// shutdown, so it carries no separate stop signal. Credits still queued at
/// that point are lost with the ledger itself: the allowance is in-memory
/// bookkeeping that a restart rebuilds from a clean slate either way.
pub fn spawn_warming_creditor(allowance: Arc<WarmingAllowance>) -> Arc<dyn WarmingCreditSink> {
    let (tx, rx) = mpsc::channel(WARMING_CREDIT_CAPACITY);
    tokio::spawn(warming_creditor_loop(rx, allowance));
    Arc::new(ChannelWarmingCreditSink { tx })
}

/// Drain loop for the background aggregator (see [`spawn_warming_creditor`]).
/// Applies each credit FIFO until every sink is dropped and the queue closes.
async fn warming_creditor_loop(
    mut rx: mpsc::Receiver<([u8; 32], u64)>,
    allowance: Arc<WarmingAllowance>,
) {
    while let Some((hash, units)) = rx.recv().await {
        allowance.credit_serve(hash, units);
    }
    tracing::debug!("warming-credit aggregator drained and stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::PoisonError;
    use std::time::Duration;

    const S1: [u8; 32] = [1u8; 32];
    const S2: [u8; 32] = [2u8; 32];
    const H1: [u8; 32] = [10u8; 32];

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
        let sink = spawn_warming_creditor(Arc::clone(&allowance));
        allowance.debit_speculative(S1, H1, 1000);
        sink.credit(H1, 600); // serve #1
        sink.credit(H1, 600); // serve #2 -> net +200, vindicated

        let mut applied = false;
        for _ in 0..500 {
            if allowance.available(S1) {
                applied = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(applied, "the aggregator must apply the enqueued credits");
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
        let sink = spawn_warming_creditor(Arc::clone(&allowance));
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
