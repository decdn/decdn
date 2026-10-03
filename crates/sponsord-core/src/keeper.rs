//! The pool keeper: every tick, read the pool's remaining balance and, if it
//! is below the low-water mark, top it up from the treasury by the refill
//! amount. Its progress is published in [`KeeperStatus`] for metrics.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::primitives::B256;
use sponsord_api::MicroUsdc;
use sponsord_api::time::{Clock, SystemClock};
use tokio_util::sync::CancellationToken;

use crate::pool::PoolChain;

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
}

/// A point-in-time copy of [`KeeperStatus`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeeperSnapshot {
    /// The pool's remaining balance at the last successful read.
    pub remaining: MicroUsdc,
    /// Unix time of the last successful balance read.
    pub last_check_unix: u64,
    /// Unix time of the last successful top-up.
    pub last_topup_unix: u64,
    /// Successful top-ups so far.
    pub topups: u64,
    /// Failed balance reads and top-ups so far.
    pub failures: u64,
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
/// never exits on its own.
pub async fn run(
    pool: Arc<dyn PoolChain>,
    pool_id: B256,
    cfg: KeeperConfig,
    status: Arc<KeeperStatus>,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(cfg.interval);
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
        sweep(pool.as_ref(), pool_id, &cfg, &status, &SystemClock).await;
    }
}

async fn sweep(
    pool: &dyn PoolChain,
    pool_id: B256,
    cfg: &KeeperConfig,
    status: &KeeperStatus,
    clock: &dyn Clock,
) {
    let remaining = match pool.remaining(pool_id).await {
        Ok(r) => r,
        Err(e) => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(pool = %pool_id, "pool remaining read failed: {e}");
            return;
        }
    };
    status.remaining.store(remaining.0, Ordering::Relaxed);
    status
        .last_check_unix
        .store(clock.now_unix(), Ordering::Relaxed);
    if !needs_refill(remaining, cfg.low_water) {
        return;
    }
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
            tracing::warn!(pool = %pool_id, "pool top-up failed: {e}");
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use alloy::primitives::Address;
    use sponsord_api::time::FixedClock;

    use super::*;
    use crate::test_support::{FakePool, TEST_POOL_ID};

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
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock).await;
        let s = status.snapshot();
        assert_eq!(s.topups, 1);
        assert_eq!(s.last_check_unix, 1_000);
        assert_eq!(s.last_topup_unix, 1_000);
        assert_eq!(s.remaining, MicroUsdc(105_000_000));
        assert_eq!(pool.remaining_now(), MicroUsdc(105_000_000));

        // Now above low water: checked, not topped up.
        clock.set(2_000);
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &clock).await;
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
        sweep(&pool, TEST_POOL_ID, &CFG, &status, &FixedClock::new(1)).await;
        let s = status.snapshot();
        assert_eq!((s.topups, s.failures), (0, 1));
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
