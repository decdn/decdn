//! The pool keeper: every tick, read the pool's remaining balance and, if it
//! is below the low-water mark, top it up from the treasury by the refill
//! amount. Its progress is published in [`KeeperStatus`] for metrics.
//!
//! A top-up whose `topUp` was broadcast but not confirmed may still mine, and
//! a second one would escrow the refill twice. The keeper holds further
//! top-ups until that transaction has a receipt, or the RPC node no longer
//! knows it and the treasury's confirmed nonce has passed every nonce it could
//! have (#40). The hold is kept in memory, so a restart clears it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::primitives::{B256, TxHash};
use decdn_client::buyer_pool::TopUpUnconfirmed;
use decdn_common::redact::sanitize_err_chain;
use sponsord_api::MicroUsdc;
use sponsord_api::time::{Clock, SystemClock};
use tokio_util::sync::CancellationToken;

use crate::pool::{PoolChain, TxState};

/// When to top the pool up, and by how much.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeeperConfig {
    /// Top up once the remaining balance drops below this.
    pub low_water: MicroUsdc,
    /// Amount each top-up adds.
    pub refill: MicroUsdc,
    /// How often the balance is checked.
    pub interval: Duration,
}

/// What the keeper has seen and done, updated as it runs. Every value starts
/// at 0; a timestamp of 0 means "never".
#[derive(Debug, Default)]
pub struct KeeperStatus {
    remaining: AtomicU64,
    last_check_unix: AtomicU64,
    last_topup_unix: AtomicU64,
    topups: AtomicU64,
    failures: AtomicU64,
    topup_unconfirmed_since_unix: AtomicU64,
}

/// A point-in-time copy of [`KeeperStatus`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeeperSnapshot {
    /// The pool's remaining balance at the last successful read.
    pub remaining: MicroUsdc,
    /// Unix time of the last successful balance read.
    pub last_check_unix: u64,
    /// Unix time of the last successful top-up. For a top-up that came back
    /// unconfirmed, the time the keeper found its receipt.
    pub last_topup_unix: u64,
    /// Successful top-ups so far.
    pub topups: u64,
    /// Failed chain reads and top-ups so far.
    pub failures: u64,
    /// Unix time a top-up came back unconfirmed, while further top-ups are
    /// held for it; 0 when nothing is held.
    pub topup_unconfirmed_since_unix: u64,
}

impl KeeperStatus {
    #[must_use]
    pub fn snapshot(&self) -> KeeperSnapshot {
        KeeperSnapshot {
            remaining: MicroUsdc(self.remaining.load(Ordering::Relaxed)),
            last_check_unix: self.last_check_unix.load(Ordering::Relaxed),
            last_topup_unix: self.last_topup_unix.load(Ordering::Relaxed),
            topups: self.topups.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            topup_unconfirmed_since_unix: self.topup_unconfirmed_since_unix.load(Ordering::Relaxed),
        }
    }
}

/// Whether `remaining` has dropped below `low_water`.
#[must_use]
pub fn needs_refill(remaining: MicroUsdc, low_water: MicroUsdc) -> bool {
    remaining < low_water
}

/// Run the keeper until `shutdown` is cancelled, checking first right away.
/// A failed tick is logged, counted, and retried next interval; the loop
/// never exits on its own. An unconfirmed top-up is the exception: no top-up
/// is sent until it is settled (see the module docs).
pub async fn run(
    pool: Arc<dyn PoolChain>,
    pool_id: B256,
    cfg: KeeperConfig,
    status: Arc<KeeperStatus>,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(cfg.interval);
    let mut unconfirmed = None;
    loop {
        // Biased: when a tick and the cancellation are both ready, stop rather
        // than start another sweep.
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        // A top-up already sent is allowed to finish: cancelling between
        // the approve and the top-up would leave a dangling allowance.
        sweep(
            pool.as_ref(),
            pool_id,
            &cfg,
            &status,
            &SystemClock,
            &mut unconfirmed,
        )
        .await;
    }
}

/// How many nonces past the treasury's pending nonce a top-up's `topUp` can
/// land: an `approve` may take the pending nonce first, and decdn's `top_up`
/// re-sends a `topUp` that lost its nonce to a concurrent transaction up to
/// three times (its private `TOPUP_NONCE_RETRIES`; decdn/decdn#2319 asks for
/// the nonce on `TopUpUnconfirmed`, which would replace this bound).
const TOPUP_NONCE_SPAN: u64 = 4;

/// A top-up whose `topUp` was broadcast but whose receipt was not read. It
/// may still mine, so no other top-up is sent until it is settled (#40).
#[derive(Clone, Copy, Debug)]
struct Unconfirmed {
    tx: TxHash,
    /// The highest nonce the `topUp` can have: its own once the RPC node has
    /// returned it, until then the treasury's pending nonce before the top-up
    /// plus [`TOPUP_NONCE_SPAN`].
    max_nonce: u64,
    since_unix: u64,
}

/// Set or clear the hold, and publish it for metrics.
fn set_hold(slot: &mut Option<Unconfirmed>, status: &KeeperStatus, hold: Option<Unconfirmed>) {
    *slot = hold;
    status
        .topup_unconfirmed_since_unix
        .store(hold.map_or(0, |h| h.since_unix), Ordering::Relaxed);
}

async fn sweep(
    pool: &dyn PoolChain,
    pool_id: B256,
    cfg: &KeeperConfig,
    status: &KeeperStatus,
    clock: &dyn Clock,
    unconfirmed: &mut Option<Unconfirmed>,
) {
    // A sweep that finds a hold sends no top-up, even when it settles it: the
    // balance it reads may come from a node that has not yet seen the held
    // top-up mine. The next sweep decides on a fresh balance. Settling before
    // the balance read makes the `remaining` recorded here include a held
    // top-up that has mined.
    let mut may_top_up = true;
    if let Some(held) = unconfirmed.as_mut() {
        may_top_up = false;
        if !settle(pool, pool_id, held, status, clock).await {
            set_hold(unconfirmed, status, None);
        }
    }
    let remaining = match pool.remaining(pool_id).await {
        Ok(r) => r,
        Err(e) => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                pool = %pool_id,
                "pool remaining read failed: {}",
                sanitize_err_chain(&e)
            );
            return;
        }
    };
    status.remaining.store(remaining.0, Ordering::Relaxed);
    status
        .last_check_unix
        .store(clock.now_unix(), Ordering::Relaxed);
    if !may_top_up || !needs_refill(remaining, cfg.low_water) {
        return;
    }
    // Bound the nonces this top-up can take, so that if it comes back
    // unconfirmed it can be settled even if the RPC node never returns it.
    let pending = match pool.pending_nonce().await {
        Ok(n) => n,
        Err(e) => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                pool = %pool_id,
                "treasury nonce read failed, so the top-up waits for the next check: {}",
                sanitize_err_chain(&e)
            );
            return;
        }
    };
    match pool.top_up(pool_id, cfg.refill).await {
        Ok(credited) => {
            status.topups.fetch_add(1, Ordering::Relaxed);
            status
                .last_topup_unix
                .store(clock.now_unix(), Ordering::Relaxed);
            status
                .remaining
                .store(remaining.saturating_add(credited).0, Ordering::Relaxed);
            tracing::info!(
                pool = %pool_id, remaining = remaining.0, credited = credited.0,
                "pool topped up"
            );
        }
        Err(e) => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            if let Some(m) = e.downcast_ref::<TopUpUnconfirmed>() {
                let hold = Unconfirmed {
                    tx: m.tx,
                    max_nonce: pending.saturating_add(TOPUP_NONCE_SPAN),
                    since_unix: clock.now_unix(),
                };
                set_hold(unconfirmed, status, Some(hold));
                tracing::error!(
                    pool = %pool_id, tx = %m.tx, max_nonce = hold.max_nonce,
                    "pool top-up unconfirmed; holding further top-ups until it has a \
                     receipt or every nonce it could have is used: {}",
                    sanitize_err_chain(&e)
                );
            } else {
                tracing::warn!(
                    pool = %pool_id,
                    "pool top-up failed: {}",
                    sanitize_err_chain(&e)
                );
            }
        }
    }
}

/// Check the held top-up; returns whether it may still mine.
async fn settle(
    pool: &dyn PoolChain,
    pool_id: B256,
    held: &mut Unconfirmed,
    status: &KeeperStatus,
    clock: &dyn Clock,
) -> bool {
    let tx = held.tx;
    // The nonce is read first: a transaction that mines between the two
    // reads then shows as mined, never as replaced.
    let read = async { anyhow::Ok((pool.confirmed_nonce().await?, pool.transaction(tx).await?)) };
    let (confirmed, state) = match read.await {
        Ok(r) => r,
        Err(e) => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                pool = %pool_id, %tx,
                "unconfirmed pool top-up check failed; still holding top-ups: {}",
                sanitize_err_chain(&e)
            );
            return true;
        }
    };
    let held_secs = clock.now_unix().saturating_sub(held.since_unix);
    match state {
        TxState::Mined => {
            status.topups.fetch_add(1, Ordering::Relaxed);
            status
                .last_topup_unix
                .store(clock.now_unix(), Ordering::Relaxed);
            tracing::info!(pool = %pool_id, %tx, "unconfirmed pool top-up mined");
            false
        }
        TxState::Reverted => {
            tracing::warn!(
                pool = %pool_id, %tx,
                "unconfirmed pool top-up reverted, so nothing was escrowed; resuming top-ups"
            );
            false
        }
        TxState::Pending { nonce } => {
            held.max_nonce = nonce;
            tracing::warn!(
                pool = %pool_id, %tx, nonce, held_secs,
                "pool top-up still pending; holding further top-ups"
            );
            true
        }
        TxState::Unknown if confirmed > held.max_nonce => {
            tracing::warn!(
                pool = %pool_id, %tx, max_nonce = held.max_nonce, confirmed,
                "the RPC node does not know pool top-up {tx} and every nonce it could \
                 have is used, so it can never mine; resuming top-ups"
            );
            false
        }
        TxState::Unknown => {
            let max_nonce = held.max_nonce;
            tracing::error!(
                pool = %pool_id, %tx, max_nonce, confirmed, held_secs,
                "the RPC node does not know pool top-up {tx}, which may still be pending \
                 elsewhere; holding further top-ups. If it was dropped, send 0-value \
                 transactions from the treasury to itself until its confirmed nonce \
                 passes {max_nonce} ({} at most); the hold then clears",
                max_nonce.saturating_add(1).saturating_sub(confirmed)
            );
            true
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use alloy::primitives::Address;
    use sponsord_api::time::FixedClock;

    use super::*;
    use crate::test_support::{CapturedLog, FAKE_TOPUP_TX, FakePool, TEST_POOL_ID};

    const CFG: KeeperConfig = KeeperConfig {
        low_water: MicroUsdc(20_000_000),
        refill: MicroUsdc(100_000_000),
        interval: Duration::from_secs(3600),
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

        // The warning keeps the error text but not the RPC URL (#38).
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

        // The warning keeps the error text but not the RPC URL (#38).
        let text = log.text();
        assert!(text.contains("pool remaining read failed"), "{text}");
        assert!(text.contains("error sending request"), "{text}");
        assert!(!text.contains("FAKE-RPC-KEY"), "{text}");
        assert!(!text.contains("rpc.example"), "{text}");
    }

    /// A pool below low water whose top-up, sent at pending nonce 10, came
    /// back unconfirmed at t=1000; later top-ups would succeed.
    async fn held_pool() -> (FakePool, KeeperStatus, Option<Unconfirmed>) {
        let pool = FakePool::new(Address::repeat_byte(1), 0);
        pool.set_pending_tx_count(10);
        pool.set_confirmed_tx_count(10);
        pool.unconfirm_top_ups(true);
        let status = KeeperStatus::default();
        let mut held = None;
        let clock = FixedClock::new(1_000);
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
        pool.unconfirm_top_ups(false);
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
        let h = held.unwrap();
        assert_eq!((h.tx, h.max_nonce), (FAKE_TOPUP_TX, 10 + TOPUP_NONCE_SPAN));
        let text = log.text();
        assert!(text.contains("pool top-up unconfirmed"), "{text}");
        assert!(text.contains(&FAKE_TOPUP_TX.to_string()), "{text}");
        assert!(!text.contains("rpc.example"), "{text}");

        // Still pending an hour later, and the pool still below low water:
        // no second top-up, and the bound tightens to the tx's own nonce.
        pool.set_tx_state(TxState::Pending { nonce: 11 });
        let clock = FixedClock::new(4_600);
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
        assert_eq!(pool.top_up_calls(), 1);
        assert_eq!(held.unwrap().max_nonce, 11);
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
        pool.set_tx_state(TxState::Pending { nonce: 11 });
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;

        // Gone from the node, but nonce 11 is not used yet: it may come back.
        pool.set_tx_state(TxState::Unknown);
        pool.set_confirmed_tx_count(11);
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
        assert!(held.is_some());

        // Nonce 11 mined with another transaction: this one never can. The
        // sweep that learns it still sends nothing; the next one tops up.
        pool.set_confirmed_tx_count(12);
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
        assert!(held.is_none());
        assert_eq!(pool.top_up_calls(), 1);
        let s = status.snapshot();
        assert_eq!((s.topups, s.topup_unconfirmed_since_unix), (0, 0));
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
        assert_eq!(pool.top_up_calls(), 2);
    }

    #[tokio::test]
    async fn a_top_up_the_node_never_returned_clears_once_its_bound_is_passed() {
        let log = CapturedLog::default();
        let _guard = log.install();
        let (pool, status, mut held) = held_pool().await;
        let max = 10 + TOPUP_NONCE_SPAN;
        let clock = FixedClock::new(2_000);
        // Unknown from the start, so only the bound is known. The log says
        // how many self-transfers would clear it.
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
        assert!(held.is_some());
        let text = log.text();
        assert!(text.contains("send 0-value transactions"), "{text}");
        assert!(
            text.contains(&format!("passes {max} (5 at most)")),
            "{text}"
        );

        pool.set_confirmed_tx_count(max);
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
        assert!(held.is_some(), "nonce {max} may still be the top-up's");
        assert_eq!(pool.top_up_calls(), 1);

        pool.set_confirmed_tx_count(max + 1);
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock, &mut held).await;
        assert!(held.is_none());
        assert_eq!(status.snapshot().topup_unconfirmed_since_unix, 0);
        assert_eq!(pool.top_up_calls(), 1);
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

    #[tokio::test]
    async fn a_failed_pending_nonce_read_sends_no_top_up() {
        let pool = FakePool::new(Address::repeat_byte(1), 0);
        pool.fail_tx_reads(true);
        let status = KeeperStatus::default();
        let mut held = None;
        sweep(
            &pool,
            TEST_POOL_ID,
            &CFG,
            &status,
            &FixedClock::new(1),
            &mut held,
        )
        .await;
        assert_eq!(pool.top_up_calls(), 0);
        assert!(held.is_none());
        assert_eq!(status.snapshot().failures, 1);
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
    }

    /// Read in the other order, a top-up that mines between the reads would
    /// look unknown with its nonce used, and be taken for replaced.
    #[tokio::test]
    async fn a_top_up_that_mines_between_the_reads_counts_as_mined() {
        let (inner, status, mut held) = held_pool().await;
        held.as_mut().unwrap().max_nonce = 10;
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

        // Three more ticks: the top-up is unknown and its nonces unused.
        pool.unconfirm_top_ups(false);
        tokio::time::sleep(CFG.interval * 3).await;
        assert_eq!(pool.top_up_calls(), 1);
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
}
