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
const H2: Hash = Hash::from_bytes([11u8; 32]);

#[test]
fn source_id_round_trips_through_its_accessors() {
    let raw = [7u8; 32];
    let id = SourceId::from_bytes(raw);
    assert_eq!(id.as_bytes(), &raw);
    assert_eq!(id.to_bytes(), raw);
    assert_eq!(<[u8; 32]>::from(id), raw);
    assert_eq!(SourceId::from(raw), id);
}

#[test]
fn a_fresh_source_starts_with_the_full_budget() {
    let a = WarmingAllowance::new(1000, 0); // no time refill
    a.debit_speculative(S1, H1, 999);
    assert!(a.available(S1)); // 1 unit left of the full budget
    a.debit_speculative(S1, H2, 1);
    assert!(!a.available(S1)); // budget spent
}

#[test]
fn duds_spend_the_budget_then_block() {
    let a = WarmingAllowance::new(1000, 0); // no time refill
    a.debit_speculative(S1, H1, 1000); // bought at full P_buy·mb
    a.credit_serve(H1, 600); // one serve (the requester): 0.6·P_sell·mb
    assert!(a.available(S1)); // one dud costs the skim, not the source
    a.debit_speculative(S1, H2, 1000); // a second dud, never served
    assert!(!a.available(S1)); // 600 - 1000: budget spent -> cut off
}

/// Only the SECOND serve vindicates the buy: an unserved dud first spends
/// the budget, so the vindicated buy starts the ledger at the floor and one
/// serve's margin alone leaves the source spent.
#[test]
fn re_served_blob_refunds_and_keeps_warming() {
    let a = WarmingAllowance::new(1000, 0);
    a.debit_speculative(S1, H2, 1000); // an unserved dud: budget spent
    a.debit_speculative(S1, H1, 1000); // the buy to vindicate: -1000
    a.credit_serve(H1, 600); // serve #1 -> -400
    assert!(!a.available(S1));
    a.credit_serve(H1, 600); // serve #2 -> +200
    assert!(a.available(S1)); // vindicated
}

#[test]
fn credit_is_capped_at_budget() {
    let a = WarmingAllowance::new(1000, 0);
    a.debit_speculative(S1, H1, 100);
    for _ in 0..100 {
        a.credit_serve(H1, 600);
    }
    // The credits bank no more than the budget, so one budget-sized debit
    // spends the source.
    a.debit_speculative(S1, H2, 1000);
    assert!(!a.available(S1));
}

#[test]
fn debt_is_floored_at_minus_budget() {
    let a = WarmingAllowance::new(1000, 0);
    a.debit_speculative(S1, H1, 10_000); // floored at -1000
    a.credit_serve(H1, 1000); // back to 0: still spent
    assert!(!a.available(S1));
    a.credit_serve(H1, 1); // budget + 1 units of credit warm it again
    assert!(a.available(S1));
}

/// The background aggregator applies what the serve path enqueues: the same
/// two-serve vindication the inline ledger records, reached through the
/// channel instead.
#[tokio::test]
async fn channel_sink_credits_through_the_background_aggregator() {
    let allowance = Arc::new(WarmingAllowance::new(1000, 0));
    let (sink, _handle, _metrics) = creditor(&allowance);
    allowance.debit_speculative(S1, H2, 1000); // an unserved dud: budget spent
    allowance.debit_speculative(S1, H1, 1000); // the buy to vindicate: -1000
    sink.credit(H1, 600); // serve #1 -> -400
    sink.credit(H1, 600); // serve #2 -> +200: only both credits vindicate

    assert!(
        eventually(|| allowance.available(S1)).await,
        "the aggregator must apply the enqueued credits"
    );
}

/// A speculative buy's tag is invisible to the serve path until its debit
/// can land.
///
/// A credit resolved from a tag published ahead of the debit could reach a
/// bucket the debit then creates full, where the `+budget` cap discards the
/// credit before the debit subtracts from it. Publishing the tag under the
/// ledger lock closes that: a debit blocked on the lock has not published
/// its tag, so no serve can resolve it yet.
#[test]
fn a_blocked_debit_has_not_published_its_tag() {
    let allowance = Arc::new(WarmingAllowance::new(1000, 0));
    let held = allowance
        .state
        .lock()
        .unwrap_or_else(PoisonError::into_inner);

    let buyer = Arc::clone(&allowance);
    let debit = std::thread::spawn(move || buyer.debit_speculative(S1, H1, 1000));
    // Give the debit time to run up to the lock it blocks on.
    std::thread::sleep(Duration::from_millis(100));
    let tag_while_blocked = allowance.source_for(H1);

    drop(held);
    let joined = debit.join().is_ok();
    assert!(joined, "the debiting thread panicked");
    assert_eq!(
        tag_while_blocked, None,
        "a debit blocked on the ledger lock must not have published its tag"
    );
    assert_eq!(allowance.source_for(H1), Some(S1));
    assert!(!allowance.available(S1), "the debit landed");
}

/// A serve completion's credit must not wait on the ledger lock.
///
/// The buy loop takes that lock on the cache-miss path, so a serve can
/// finish while it is held. The credit goes over a bounded channel and lands
/// anyway; applying it inline on the stream task would make the serve's
/// final step wait for the holder to be done, with the stream's lane slot
/// and floor reservation still charged for the wait.
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

    // S1 spends its whole budget warming H1, then serves it: any credit makes
    // it available again.
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
/// source's ledger lower than reality, which can only *block* speculative
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
fn enabled_tracks_a_nonzero_budget() {
    assert!(WarmingAllowance::new(1000, 0).enabled());
    assert!(!WarmingAllowance::new(0, 0).enabled());
}

#[test]
fn a_zero_budget_disables_warming_for_every_source() {
    // Budget 0 means warming is off. A never-seen source must read as
    // unavailable (not `None => true`), so `economic_ceiling` never picks the
    // warming regime, and the blocked-source gauge stays at zero even if a
    // spent bucket lingers from a prior config.
    let a = WarmingAllowance::new(0, 0);
    assert!(
        !a.available(S1),
        "a fresh source is not available with warming off"
    );
    a.debit_speculative(S1, H1, 10); // a lingering bucket from a prior config
    assert!(!a.available(S1));
    assert_eq!(
        a.blocked_source_count(),
        0,
        "warming off reports no griefing, even with a spent bucket present"
    );
}

#[test]
fn blocked_source_count_tracks_spent_ledgers() {
    let a = WarmingAllowance::new(1000, 0); // no time refill
    assert_eq!(a.blocked_source_count(), 0, "no ledger entries yet");
    a.debit_speculative(S1, H1, 1000); // spends S1's whole budget
    assert_eq!(a.blocked_source_count(), 1, "S1 is now blocked");
    a.debit_speculative(S2, H2, 1); // S2 still has headroom
    assert_eq!(a.blocked_source_count(), 1, "only S1 is blocked");
    a.credit_serve(H1, 1); // S1 back above zero
    assert_eq!(a.blocked_source_count(), 0, "S1 recovered");
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
