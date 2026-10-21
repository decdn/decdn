use std::sync::atomic::AtomicBool;

use alloy::primitives::Address;
use sponsord_api::time::FixedClock;

use super::*;
use crate::test_support::{
    CapturedLog, FAKE_RESENT_TOPUP_TX, FAKE_TOPUP_TX, FakePool, ResendFailure, TEST_POOL_ID,
};

const CFG: KeeperConfig = KeeperConfig {
    low_water: MicroUsdc(20_000_000),
    refill: MicroUsdc(100_000_000),
    interval: Duration::from_hours(1),
};

#[test]
fn refills_only_below_low_water() {
    let low = MicroUsdc(20_000_000);
    assert!(needs_refill(MicroUsdc(19_999_999), low));
    assert!(!needs_refill(MicroUsdc(20_000_000), low));
    assert!(!needs_refill(MicroUsdc(50_000_000), low));
}

#[tokio::test]
async fn sweep_tops_up_below_low_water_and_records_it() {
    let pool = FakePool::new(Address::repeat_byte(1), 5_000_000);
    let status = KeeperStatus::default();
    let clock = FixedClock::new(1_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut None).await;
    let s = status.snapshot();
    assert_eq!(s.topups, 1);
    assert_eq!(s.last_check_unix, 1_000);
    assert_eq!(s.last_topup_unix, 1_000);
    assert_eq!(s.remaining, MicroUsdc(105_000_000));
    assert_eq!(pool.remaining_now(), MicroUsdc(105_000_000));

    // Now above low water: checked, not topped up.
    clock.set(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut None).await;
    let s = status.snapshot();
    assert_eq!(
        (s.topups, s.last_check_unix, s.last_topup_unix),
        (1, 2_000, 1_000)
    );
}

#[tokio::test]
async fn failures_are_counted_not_fatal() {
    let pool = FakePool::new(Address::repeat_byte(1), 0);
    pool.fail_top_ups(true);
    let status = KeeperStatus::default();
    let log = CapturedLog::default();
    {
        let _guard = log.install();
        sweep(
            &pool,
            TEST_POOL_ID,
            &CFG,
            &status,
            &FixedClock::new(1),
            &mut None,
        )
        .await;
    }
    let s = status.snapshot();
    assert_eq!((s.topups, s.failures), (0, 1));

    // The warning keeps the error text but not the RPC URL (decdn/sponsord#38).
    let text = log.text();
    assert!(text.contains("pool top-up failed"), "{text}");
    assert!(text.contains("error sending request"), "{text}");
    assert!(!text.contains("FAKE-RPC-KEY"), "{text}");
    assert!(!text.contains("rpc.example"), "{text}");
}

#[tokio::test]
async fn a_failed_remaining_read_is_counted_and_logged_without_the_url() {
    let pool = FakePool::new(Address::repeat_byte(1), 0);
    pool.fail_remaining_reads(true);
    let status = KeeperStatus::default();
    let log = CapturedLog::default();
    {
        let _guard = log.install();
        sweep(
            &pool,
            TEST_POOL_ID,
            &CFG,
            &status,
            &FixedClock::new(1),
            &mut None,
        )
        .await;
    }
    let s = status.snapshot();
    assert_eq!((s.topups, s.failures, s.last_check_unix), (0, 1, 0));
    assert_eq!(pool.remaining_now(), MicroUsdc(0), "no top-up was sent");

    // The warning keeps the error text but not the RPC URL (decdn/sponsord#38).
    let text = log.text();
    assert!(text.contains("pool remaining read failed"), "{text}");
    assert!(text.contains("error sending request"), "{text}");
    assert!(!text.contains("FAKE-RPC-KEY"), "{text}");
    assert!(!text.contains("rpc.example"), "{text}");
}

/// A pool below low water whose top-up, sent at nonce 10, came back
/// unconfirmed at t=1000; later top-ups would succeed.
async fn held_pool() -> (FakePool, KeeperStatus, Option<Unconfirmed>) {
    let pool = FakePool::new(Address::repeat_byte(1), 0);
    pool.set_confirmed_tx_count(10);
    pool.unconfirm_top_ups(true);
    let status = KeeperStatus::default();
    let mut held = None;
    let clock = FixedClock::new(1_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    pool.unconfirm_top_ups(false);
    (pool, status, held)
}

/// A pool below low water whose top-up, sent at nonce 10 at t=1000, failed
/// in transport, so the hold has no hash; later top-ups would succeed.
async fn lost_pool() -> (FakePool, KeeperStatus, Option<Unconfirmed>) {
    let pool = FakePool::new(Address::repeat_byte(1), 0);
    pool.set_confirmed_tx_count(10);
    pool.lose_top_up_submits(true);
    let status = KeeperStatus::default();
    let mut held = None;
    let clock = FixedClock::new(1_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    pool.lose_top_up_submits(false);
    let h = held.as_ref().unwrap();
    assert_eq!((h.tx, h.nonce, h.since_unix), (None, 10, 1_000));
    (pool, status, held)
}

#[tokio::test]
async fn an_unconfirmed_top_up_holds_further_top_ups_while_pending() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = held_pool().await;
    let s = status.snapshot();
    assert_eq!(
        (s.topups, s.failures, s.topup_unconfirmed_since_unix),
        (0, 1, 1_000)
    );
    let h = held.as_ref().unwrap();
    assert_eq!((h.tx, h.nonce), (Some(FAKE_TOPUP_TX), 10));
    let text = log.text();
    assert!(text.contains("pool top-up unconfirmed"), "{text}");
    assert!(text.contains(&FAKE_TOPUP_TX.to_string()), "{text}");
    assert!(!text.contains("rpc.example"), "{text}");

    // Still pending an hour later, and the pool still below low water:
    // no second top-up.
    pool.set_tx_state(TxState::Pending { nonce: 10 });
    let clock = FixedClock::new(4_600);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_calls(), 1);
    assert!(held.is_some());
    let s = status.snapshot();
    assert_eq!(
        (
            s.failures,
            s.last_check_unix,
            s.topup_unconfirmed_since_unix
        ),
        (1, 4_600, 1_000)
    );
    assert!(log.text().contains("still pending"), "{}", log.text());
}

#[tokio::test]
async fn a_mined_top_up_clears_the_hold_and_counts_as_a_top_up() {
    let (pool, status, mut held) = held_pool().await;
    pool.set_tx_state(TxState::Mined);
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    // It refilled the pool, so this sweep sends no other top-up, even
    // though the fake's balance did not move.
    assert_eq!(pool.top_up_calls(), 1);
    let s = status.snapshot();
    assert_eq!(
        (s.topups, s.last_topup_unix, s.topup_unconfirmed_since_unix),
        (1, 2_000, 0)
    );

    // The next sweep tops up as usual.
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_calls(), 2);
    assert_eq!(status.snapshot().topups, 2);
}

#[tokio::test]
async fn a_reverted_top_up_clears_the_hold_and_the_next_sweep_tops_up() {
    let (pool, status, mut held) = held_pool().await;
    pool.set_tx_state(TxState::Reverted);
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    assert_eq!(pool.top_up_calls(), 1);
    let s = status.snapshot();
    assert_eq!((s.topups, s.topup_unconfirmed_since_unix), (0, 0));

    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_calls(), 2);
    assert_eq!(pool.remaining_now(), CFG.refill);
}

#[tokio::test]
async fn a_top_up_whose_nonce_was_used_by_another_tx_clears_the_hold() {
    let (pool, status, mut held) = held_pool().await;
    let clock = FixedClock::new(2_000);
    pool.set_tx_state(TxState::Pending { nonce: 10 });
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;

    // Gone from the node, whose mempool holds a transaction at nonce 10 that
    // is not used yet: it may come back.
    pool.set_tx_state(TxState::Unknown);
    pool.set_pending_tx_count(11);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_some());
    assert!(pool.top_up_resends().is_empty());

    // Nonce 10 mined with another transaction: this one never can. The
    // sweep that learns it still sends nothing; the next one tops up.
    pool.set_confirmed_tx_count(11);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    assert_eq!(pool.top_up_calls(), 1);
    let s = status.snapshot();
    assert_eq!((s.topups, s.topup_unconfirmed_since_unix), (0, 0));
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_calls(), 2);
}

/// A stale or load-balanced node can keep reporting a replaced `topUp`
/// as pending. Its used nonce still clears the hold, or the documented
/// self-transfers could never clear it.
#[tokio::test]
async fn a_top_up_still_reported_pending_clears_once_its_nonce_is_used() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = held_pool().await;
    let clock = FixedClock::new(2_000);
    pool.set_tx_state(TxState::Pending { nonce: 10 });
    pool.set_confirmed_tx_count(11);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    let s = status.snapshot();
    assert_eq!((s.topups, s.topup_unconfirmed_since_unix), (0, 0));
    assert_eq!(pool.top_up_calls(), 1);
    let text = log.text();
    assert!(text.contains("nonce 10 is used"), "{text}");

    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_calls(), 2);
}

#[tokio::test]
async fn a_top_up_the_node_never_returned_clears_once_its_nonce_is_used() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = held_pool().await;
    let clock = FixedClock::new(2_000);
    // Unknown from the start, while the node's mempool holds a transaction
    // at its nonce (the top-up, through another backend, or a replacement):
    // only its nonce settles it. The log says how many self-transfers would
    // clear it.
    pool.set_pending_tx_count(11);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_some());
    assert!(pool.top_up_resends().is_empty());
    let text = log.text();
    assert!(text.contains("mempool holds"), "{text}");
    assert!(text.contains("send a 0-value transaction"), "{text}");
    assert!(text.contains("at nonce 10 with a higher fee"), "{text}");

    pool.set_confirmed_tx_count(11);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    assert_eq!(status.snapshot().topup_unconfirmed_since_unix, 0);
    assert_eq!(pool.top_up_calls(), 1);
}

/// The RPC node may have broadcast a `topUp` whose submit failed in
/// transport, so it is held too, with no hash to check. While the node's
/// mempool holds a transaction at its nonce (the pending count is above the
/// confirmed one), that transaction may be the top-up, so nothing is re-sent.
#[tokio::test]
async fn a_top_up_lost_in_transport_is_held_while_a_transaction_holds_its_nonce() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = lost_pool().await;
    let clock = FixedClock::new(1_000);
    assert_eq!(status.snapshot().topup_unconfirmed_since_unix, 1_000);
    let text = log.text();
    assert!(text.contains("pool top-up unconfirmed"), "{text}");
    assert!(!text.contains("rpc.example"), "{text}");

    // With no hash, a receipt cannot settle it: only the nonce does.
    pool.set_pending_tx_count(11);
    pool.set_tx_state(TxState::Mined);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(held.as_ref().map(|h| h.tx), Some(None));
    assert_eq!(pool.top_up_calls(), 1);
    assert!(pool.top_up_resends().is_empty());
    let text = log.text();
    assert!(text.contains("mempool holds"), "{text}");
    assert!(text.contains("at nonce 10 with a higher fee"), "{text}");
    assert_eq!(
        status.snapshot().failures,
        1,
        "a held top-up is not a failure"
    );

    // Nonce 10 is used: it mined or never will. Unknown which, so it is
    // not counted; the sweep that learns it sends nothing, the next tops
    // up on a fresh balance.
    pool.set_confirmed_tx_count(11);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    let s = status.snapshot();
    assert_eq!((s.topups, s.topup_unconfirmed_since_unix), (0, 0));
    assert_eq!(pool.top_up_calls(), 1);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_calls(), 2);
}

/// Once the confirmed and pending counts both equal the nonce, no
/// transaction holds it, so the keeper re-sends the `topUp` there; the
/// receipt of the re-send then settles the hold (#2572).
#[tokio::test]
async fn a_top_up_lost_in_transport_is_re_sent_once_its_nonce_is_vacant_and_clears_on_its_receipt()
{
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = lost_pool().await;
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_resends(), vec![10]);
    let h = held.as_ref().unwrap();
    assert_eq!(
        (h.tx, h.nonce, h.since_unix),
        (Some(FAKE_RESENT_TOPUP_TX), 10, 1_000)
    );
    assert_eq!(
        pool.top_up_calls(),
        1,
        "the sweep that re-sends tops up nothing else"
    );
    let s = status.snapshot();
    assert_eq!(
        (s.topups, s.failures, s.topup_unconfirmed_since_unix),
        (0, 1, 1_000),
        "a re-send is not a failure"
    );
    let text = log.text();
    assert!(text.contains("re-sent topUp"), "{text}");
    assert!(text.contains(&FAKE_RESENT_TOPUP_TX.to_string()), "{text}");

    // The re-send is pending: still held, and not re-sent again.
    pool.set_tx_state(TxState::Pending { nonce: 10 });
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_some());
    assert_eq!(pool.top_up_resends(), vec![10]);
    assert!(log.text().contains("still pending"), "{}", log.text());

    // It mines: counted as a top-up, the hold clears, the next sweep tops up
    // on a fresh balance.
    pool.set_tx_state(TxState::Mined);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    let s = status.snapshot();
    assert_eq!(
        (s.topups, s.last_topup_unix, s.topup_unconfirmed_since_unix),
        (1, 2_000, 0)
    );
    assert_eq!(pool.top_up_calls(), 1);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_calls(), 2);
}

/// A re-send that reverts `PoolNotOpen` says the pool accepts no funds, so
/// neither it nor the lost top-up can escrow: the hold ends.
#[tokio::test]
async fn a_re_send_that_reverts_pool_not_open_ends_the_hold() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = lost_pool().await;
    pool.fail_resends(Some(ResendFailure::PoolNotOpen));
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    assert_eq!(pool.top_up_resends(), vec![10]);
    let s = status.snapshot();
    assert_eq!((s.failures, s.topup_unconfirmed_since_unix), (2, 0));
    let text = log.text();
    assert!(text.contains("PoolNotOpen"), "{text}");
    assert!(!text.contains("rpc.example"), "{text}");
}

/// A held top-up whose hash the node does not know is re-sent under the
/// same rule as a hashless one: its nonce is next and nothing holds it.
#[tokio::test]
async fn a_top_up_the_node_does_not_know_is_re_sent_once_its_nonce_is_vacant() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = held_pool().await;
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_resends(), vec![10]);
    assert_eq!(
        held.as_ref().map(|h| h.tx),
        Some(Some(FAKE_RESENT_TOPUP_TX))
    );
    assert_eq!(status.snapshot().failures, 1);
    let text = log.text();
    assert!(text.contains("re-sent topUp"), "{text}");
    assert!(
        text.contains(&FAKE_TOPUP_TX.to_string()),
        "names the first hash: {text}"
    );

    pool.set_tx_state(TxState::Mined);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    assert_eq!(status.snapshot().topups, 1);
}

/// A re-sent top-up the node drops in turn is re-sent again, not held for
/// an operator.
#[tokio::test]
async fn a_re_sent_top_up_the_node_drops_is_re_sent_again() {
    let (pool, status, mut held) = lost_pool().await;
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_resends(), vec![10]);

    // The node does not know the re-send either, and nothing holds nonce 10.
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_resends(), vec![10, 10]);
    assert_eq!(
        held.as_ref().map(|h| h.tx),
        Some(Some(FAKE_RESENT_TOPUP_TX))
    );

    pool.set_tx_state(TxState::Mined);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    assert_eq!(status.snapshot().topups, 1);
    assert_eq!(pool.top_up_calls(), 1);
}

/// A re-send that fails in transport leaves the hold as it was; the next
/// sweep finds the nonce still vacant and re-sends again.
#[tokio::test]
async fn a_re_send_that_fails_keeps_the_hold() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = lost_pool().await;
    pool.fail_resends(Some(ResendFailure::Transport));
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(held.as_ref().map(|h| h.tx), Some(None));
    assert_eq!(pool.top_up_resends(), vec![10]);
    assert_eq!(status.snapshot().failures, 2);
    let text = log.text();
    assert!(text.contains("re-sending it failed"), "{text}");
    assert!(text.contains("error sending request"), "{text}");
    assert!(!text.contains("FAKE-RPC-KEY"), "{text}");
    assert!(!text.contains("rpc.example"), "{text}");

    pool.fail_resends(None);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_resends(), vec![10, 10]);
    assert_eq!(
        held.as_ref().map(|h| h.tx),
        Some(Some(FAKE_RESENT_TOPUP_TX))
    );
}

/// With an earlier nonce still unfilled, nothing at the top-up's nonce can
/// mine yet and the pending count says nothing about it: held, not re-sent.
#[tokio::test]
async fn a_queued_hashless_top_up_is_held_without_a_re_send() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = lost_pool().await;
    pool.set_confirmed_tx_count(9);
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(held.as_ref().map(|h| h.tx), Some(None));
    assert!(pool.top_up_resends().is_empty());
    assert_eq!(status.snapshot().failures, 1);
    let text = log.text();
    assert!(text.contains("cannot mine yet"), "{text}");
    assert!(text.contains("2 at most"), "{text}");
}

/// The confirmed read works and the pending read fails: nothing is known
/// about the mempool, so nothing is re-sent and the hold stays as it was.
#[tokio::test]
async fn a_failed_pending_nonce_read_keeps_the_hold_without_a_re_send() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = lost_pool().await;
    pool.fail_pending_reads(true);
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(held.as_ref().map(|h| h.tx), Some(None));
    assert!(pool.top_up_resends().is_empty());
    assert_eq!(pool.top_up_calls(), 1);
    let s = status.snapshot();
    assert_eq!((s.failures, s.topup_unconfirmed_since_unix), (2, 1_000));
    let text = log.text();
    assert!(text.contains("pending-nonce check"), "{text}");
    assert!(text.contains("error sending request"), "{text}");
    assert!(!text.contains("rpc.example"), "{text}");

    // Once the read works the nonce is still vacant: re-sent.
    pool.fail_pending_reads(false);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(pool.top_up_resends(), vec![10]);
}

/// A re-send moves the first hash aside, but the first send may still be
/// the one that mines: its receipt counts the top-up.
#[tokio::test]
async fn a_first_send_that_mines_after_a_re_send_is_counted() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = held_pool().await;
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(
        held.as_ref().map(|h| h.tx),
        Some(Some(FAKE_RESENT_TOPUP_TX))
    );

    // The first send mines; the node never learns the re-send.
    pool.set_tx_state_of(FAKE_TOPUP_TX, TxState::Mined);
    pool.set_confirmed_tx_count(11);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    let s = status.snapshot();
    assert_eq!(
        (s.topups, s.last_topup_unix, s.topup_unconfirmed_since_unix),
        (1, 2_000, 0)
    );
    let text = log.text();
    assert!(text.contains("an earlier send"), "{text}");
    assert!(text.contains(&FAKE_TOPUP_TX.to_string()), "{text}");
}

/// A re-send the node answers with `already known` may be in a mempool
/// without its hash: the hold keeps no hash, and the nonce settles it.
#[tokio::test]
async fn a_re_send_answered_as_possibly_broadcast_keeps_the_hold_without_a_hash() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = lost_pool().await;
    pool.fail_resends(Some(ResendFailure::Unknown));
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(held.as_ref().map(|h| h.tx), Some(None));
    assert_eq!(pool.top_up_resends(), vec![10]);
    assert_eq!(status.snapshot().failures, 2);
    let text = log.text();
    assert!(text.contains("broadcast unknown"), "{text}");
    assert!(!text.contains("re-sending it failed"), "{text}");

    pool.set_confirmed_tx_count(11);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
}

/// A re-send short of allowance keeps the hold and names the remedy.
#[tokio::test]
async fn a_re_send_short_of_allowance_keeps_the_hold_and_names_the_remedy() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = lost_pool().await;
    pool.fail_resends(Some(ResendFailure::AllowanceShort));
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(held.as_ref().map(|h| h.tx), Some(None));
    assert_eq!(pool.top_up_resends(), vec![10]);
    assert_eq!(status.snapshot().failures, 2);
    let text = log.text();
    assert!(text.contains("allowance"), "{text}");
    assert!(text.contains("approve it again"), "{text}");
}

/// Read in the other order, a lost top-up that mines between the reads
/// would look vacant and be re-sent at a nonce it has used.
#[tokio::test]
async fn a_hashless_top_up_that_mines_between_the_reads_is_not_re_sent() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (inner, status, mut held) = lost_pool().await;
    let pool = MinesBetweenReads {
        inner,
        mined: AtomicBool::new(false),
    };
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert_eq!(held.as_ref().map(|h| h.tx), Some(None));
    assert!(pool.inner.top_up_resends().is_empty());
    assert!(log.text().contains("mempool holds"), "{}", log.text());

    // The next sweep sees the nonce used and clears the hold.
    sweep(&pool.inner, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    assert!(pool.inner.top_up_resends().is_empty());
}

#[tokio::test]
async fn a_failed_check_of_the_held_top_up_keeps_the_hold() {
    let log = CapturedLog::default();
    let _guard = log.install();
    let (pool, status, mut held) = held_pool().await;
    pool.fail_tx_reads(true);
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_some());
    assert_eq!(pool.top_up_calls(), 1);
    assert_eq!(status.snapshot().failures, 2);
    let text = log.text();
    assert!(text.contains("check failed"), "{text}");
    assert!(!text.contains("rpc.example"), "{text}");
}

/// A pool whose held top-up mines right after the first of the keeper's
/// two reads (nonce and transaction), whichever comes first.
struct MinesBetweenReads {
    inner: FakePool,
    mined: AtomicBool,
}

impl MinesBetweenReads {
    fn mine_once(&self) {
        if !self.mined.swap(true, Ordering::SeqCst) {
            self.inner.set_tx_state(TxState::Mined);
            let n = self.inner.confirmed_nonce.load(Ordering::SeqCst);
            self.inner.set_confirmed_tx_count(n + 1);
        }
    }
}

#[async_trait::async_trait]
impl PoolChain for MinesBetweenReads {
    fn owner_address(&self) -> Address {
        self.inner.owner_address()
    }
    async fn remaining(&self, id: B256) -> anyhow::Result<MicroUsdc> {
        self.inner.remaining(id).await
    }
    async fn top_up(&self, id: B256, amount: MicroUsdc) -> anyhow::Result<MicroUsdc> {
        self.inner.top_up(id, amount).await
    }
    async fn pool_owner(&self, id: B256) -> anyhow::Result<Address> {
        self.inner.pool_owner(id).await
    }
    async fn authorization(
        &self,
        id: B256,
        signer: Address,
    ) -> anyhow::Result<Option<crate::pool::Authorization>> {
        self.inner.authorization(id, signer).await
    }
    async fn transaction(&self, tx: TxHash) -> anyhow::Result<TxState> {
        let state = self.inner.transaction(tx).await;
        self.mine_once();
        state
    }
    async fn confirmed_nonce(&self) -> anyhow::Result<u64> {
        let nonce = self.inner.confirmed_nonce().await;
        self.mine_once();
        nonce
    }
    async fn pending_nonce(&self) -> anyhow::Result<u64> {
        self.inner.pending_nonce().await
    }
    async fn send_top_up_at(
        &self,
        id: B256,
        amount: MicroUsdc,
        nonce: u64,
    ) -> anyhow::Result<TxHash> {
        self.inner.send_top_up_at(id, amount, nonce).await
    }
}

/// Read in the other order, a top-up that mines between the reads would
/// look unknown with its nonce used, and be taken for replaced.
#[tokio::test]
async fn a_top_up_that_mines_between_the_reads_counts_as_mined() {
    let (inner, status, mut held) = held_pool().await;
    let pool = MinesBetweenReads {
        inner,
        mined: AtomicBool::new(false),
    };
    let clock = FixedClock::new(2_000);
    sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
    assert!(held.is_none());
    assert_eq!(status.snapshot().topups, 1, "counted as mined");
}

#[tokio::test(start_paused = true)]
async fn run_keeps_the_hold_across_ticks() {
    let pool = Arc::new(FakePool::new(Address::repeat_byte(1), 0));
    pool.unconfirm_top_ups(true);
    // The node's mempool holds a transaction at the top-up's nonce 0.
    pool.set_pending_tx_count(1);
    let status = Arc::new(KeeperStatus::default());
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(
        pool.clone(),
        TEST_POOL_ID,
        CFG,
        status.clone(),
        shutdown.clone(),
    ));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(pool.top_up_calls(), 1);
    assert_ne!(status.snapshot().topup_unconfirmed_since_unix, 0);

    // Three more ticks: the top-up is unknown, its nonce held and unused.
    pool.unconfirm_top_ups(false);
    tokio::time::sleep(CFG.interval * 3).await;
    assert_eq!(pool.top_up_calls(), 1);
    assert!(pool.top_up_resends().is_empty());
    assert_ne!(status.snapshot().topup_unconfirmed_since_unix, 0);

    // It mines: the next tick clears the hold, the one after tops up.
    pool.set_tx_state(TxState::Mined);
    tokio::time::sleep(CFG.interval).await;
    assert_eq!(status.snapshot().topup_unconfirmed_since_unix, 0);
    tokio::time::sleep(CFG.interval).await;
    assert_eq!(pool.top_up_calls(), 2);
    shutdown.cancel();
    task.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_keeper_starts_no_sweep_even_with_a_tick_ready() {
    let pool = Arc::new(FakePool::new(Address::repeat_byte(1), 0));
    let status = Arc::new(KeeperStatus::default());
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    // The interval's first tick is ready at once, as is the cancellation.
    run(pool.clone(), TEST_POOL_ID, CFG, status.clone(), shutdown).await;
    assert_eq!(status.snapshot(), KeeperSnapshot::default());
    assert_eq!(pool.remaining_now(), MicroUsdc(0), "no top-up was sent");
}

#[tokio::test(start_paused = true)]
async fn run_checks_at_once_and_stops_on_shutdown() {
    let pool = Arc::new(FakePool::new(Address::repeat_byte(1), 50_000_000));
    let status = Arc::new(KeeperStatus::default());
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(
        pool,
        TEST_POOL_ID,
        CFG,
        status.clone(),
        shutdown.clone(),
    ));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(status.snapshot().remaining, MicroUsdc(50_000_000));
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("keeper stops on shutdown")
        .unwrap();
}
