//! `NodeFunder` — the node's [`Funder`] adapter over [`PoolOpener::top_up_pool`].
//!
//! [`client-pull`](decdn_client_pull)'s gap-driven `drive()` reactively tops up
//! the buyer deposit through the injected [`Funder`] seam (`source.rs`) rather
//! than naming a chain handle directly, so the node's upstream cache-miss pull
//! leg (B2) shares that driver instead of running its own copy of the top-up
//! loop. `NodeFunder` is the bridge.
//!
//! `Funder::top_up(additional)` means "add exactly `additional` to the deposit"
//! — the same contract the CLI's `CliFunder` honours by calling `topUp` with
//! `additional` directly. A `topUp` leaves the pool's committed spend untouched,
//! so adding `additional` to the deposit adds exactly `additional` to the
//! spendable headroom too. [`PoolOpener::top_up_pool`] takes the same amount and
//! reports how much of it landed, so `NodeFunder` passes `additional` through and
//! grades the landing on that report. The driver sizes it from the pull's live
//! ledger; the buyer service's persisted lane progress lags a running pull and
//! cannot size it.
//!
//! The pool has no expiry, so a `topUp` strands nothing time-bound — the node
//! funds its own pool freely, and there is no near-expiry refusal to derive.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::U256;
use async_trait::async_trait;
use decdn_client_pull::source::SourceFuture;
use decdn_client_pull::{Funder, LocalPullFault, PoolContext};
use decdn_incentive::DepositOutcome;
use tracing::warn;

use crate::buyer_channel::PoolOpener;

/// How many times ONE call of the node's miss-pull driver answers a genuine
/// mid-pull `SpendingCapExhausted` with an on-chain `topUp` before giving up.
///
/// Deliberately **1**, where the CLI's [`MAX_TOPUP_ATTEMPTS`] is 3. The node tops
/// up from the initial deposit straight to the working deposit — 0.5 USDC to 10
/// USDC under the shipped defaults, a 20x jump — so one top-up covers any blob
/// inside `cache.max_blob_size_mb` at any sane rate. Needing a second means the
/// upstream's quoted rate is wrong for the working deposit, which is a pricing
/// problem no amount of funding fixes; each extra attempt costs a transaction plus
/// a settle wait, both of which land on a client that is waiting.
///
/// [`MAX_TOPUP_ATTEMPTS`]: decdn_client_pull::MAX_TOPUP_ATTEMPTS
///
/// Reported through [`Funder::max_topups`] below, so [`NodeFunder`] is the one
/// source of the node's reactive-top-up budget.
pub(crate) const MAX_REACTIVE_TOPUPS: u32 = 1;

/// One step of the post-top-up settle wait. Small enough that the common case
/// (the upstream's watcher was already close to its next poll) costs little.
///
/// `pub(crate)` so the gap-driven pull leg ([`super::pull_leg::run_pull_leg`]) and
/// the node origin's own [`decdn_client_pull::driver::DriveConfig`] reuse it as
/// `settle_backoff`, keeping one source of the node's settle cadence.
pub(crate) const SETTLE_POLL_STEP: Duration = Duration::from_millis(500);

/// How many [`SETTLE_POLL_STEP`]s to spend waiting for the UPSTREAM's chain watcher
/// to observe our just-landed `ChannelToppedUp` before treating its refusal as real.
///
/// The upstream gates serving on the deposit it has observed, so between our receipt
/// and its next poll it correctly refuses a resume for a channel it still believes is
/// empty. Waiting that out is money-safe: no voucher is sent and `byte_offset` does
/// not move, so the worst case is wasted wall clock.
///
/// Sized at **two poll intervals** (14 s at the 7 s default), not a hard-coded
/// constant. One interval is the bare minimum and leaves no room for the watcher's
/// own head TTL, and anything derived from our own timeouts would drift the moment an
/// operator retunes the chain lane. Two clears a full poll plus the head cache in the
/// ordinary case, and with [`MAX_REACTIVE_TOPUPS`] at 1 it is spent at most once per
/// candidate.
///
/// Then capped at [`MAX_SETTLE_WAITS`], because `event_poll_interval_ms` has a config
/// floor but NO ceiling: at a 60 s chain lane the derived budget would be 240 steps —
/// two minutes of a foreground client's pull spent sleeping, well past the
/// per-candidate share of `outer_pull_deadline` (45 s at defaults) that this wait has
/// to fit inside (#1600 review).
///
/// Note what the budget bounds: the SLEEPS. Each step also costs a re-open round trip
/// (dial, signed request, verified response), so the true wall clock is
/// `steps × (sleep + open RTT)`. Both halves are charged to the pull's paid-wait
/// accounting and excluded from the peer's delivery-speed score — none of it is the
/// upstream serving slowly.
pub(crate) fn settle_wait_budget(event_poll_interval: Duration) -> u32 {
    let budget = event_poll_interval.saturating_mul(2).as_millis();
    let step = SETTLE_POLL_STEP.as_millis().max(1);
    u32::try_from(budget / step)
        .unwrap_or(u32::MAX)
        .min(MAX_SETTLE_WAITS)
}

/// Hard ceiling on the settle wait, whatever the configured chain cadence: 30 s of
/// sleeps. Past this the top-up is better treated as not-yet-visible and the pull
/// ended, than kept alive on a client's clock.
const MAX_SETTLE_WAITS: u32 = 60;

/// The node's [`Funder`]: reactively tops up ITS OWN pool through an injected
/// [`PoolOpener`].
///
/// # Fields
///
/// - `opener`: where the top-up lands. `Arc<dyn PoolOpener>` because the node
///   origin already stores its buyer service behind that same object-safe seam.
/// - `ctx`: the driver's live [`PoolContext`], shared (not copied) because its
///   `deposit` field grows across the fetch as earlier top-ups land. `top_up`
///   reads it as a floor, so a landing never lowers the deposit the driver
///   credits. Locked only to copy fields out; the guard is never held across
///   `.await` (`top_up_pool` is a network+chain round trip).
/// - `metrics`: the node's metrics handle. `top_up` records
///   `node_pull_reactive_topup` on a landing that adds the full requested amount
///   and `node_pull_reactive_topup_refused` on a short landing or a
///   `top_up_pool` error — the one seam both the window-paced serve leg and the
///   gap-driven pull leg fund through, so metering here covers both.
pub(crate) struct NodeFunder {
    opener: Arc<dyn PoolOpener>,
    ctx: Arc<Mutex<PoolContext>>,
    metrics: Arc<crate::metrics::Metrics>,
    /// Set the first time a top-up escrows any headroom for this pull, so the
    /// caller can tell a pull that never funded itself (an extortion refusal to
    /// meter) from one that did. Shared with `pull_from_candidate`, which reads it after the drive
    /// thread joins.
    funded: Arc<AtomicBool>,
}

impl std::fmt::Debug for NodeFunder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeFunder").finish_non_exhaustive()
    }
}

impl NodeFunder {
    pub(crate) fn new(
        opener: Arc<dyn PoolOpener>,
        ctx: Arc<Mutex<PoolContext>>,
        metrics: Arc<crate::metrics::Metrics>,
        funded: Arc<AtomicBool>,
    ) -> Self {
        Self {
            opener,
            ctx,
            metrics,
            funded,
        }
    }
}

#[async_trait]
impl Funder for NodeFunder {
    fn max_topups(&self) -> u32 {
        MAX_REACTIVE_TOPUPS
    }

    fn top_up(&self, additional: U256) -> SourceFuture<'_, DepositOutcome> {
        Box::pin(async move {
            // The deposit the driver holds before the call: a floor for the deposit it
            // credits, so a landing that reports a lower total cannot shrink it.
            let (pool_id, current_deposit) = {
                let guard = self
                    .ctx
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (guard.pool_id, guard.deposit)
            };
            match self.opener.top_up_pool(additional).await {
                Ok(landed) => {
                    let new_deposit = landed.new_deposit.max(current_deposit);
                    if !landed.added.is_zero() {
                        // This pull escrowed headroom, so a later terminal
                        // `SpendingCapExhausted` is NOT metered as an extortion
                        // refusal: a pull that funded itself is not being extorted,
                        // and a short landing is already metered below.
                        self.funded.store(true, Ordering::Relaxed);
                    }
                    if landed.added >= additional {
                        self.metrics.node_pull_reactive_topup();
                    } else {
                        // The driver still credits the new deposit and spends a unit
                        // of its top-up budget on any `Added`, so the short landing is
                        // logged and metered here. `BuyerPoolService::top_up_pool`
                        // funds the remainder of a short join itself, so this arm sees
                        // only what is left when its funding calls run out.
                        warn!(
                            %pool_id,
                            requested = %additional,
                            landed = %landed.added,
                            %new_deposit,
                            "reactive top-up landed less than requested"
                        );
                        self.metrics.node_pull_reactive_topup_refused();
                    }
                    Ok(DepositOutcome::Added(new_deposit))
                }
                // A funding failure is ours (allowance, RPC, a row we cannot credit),
                // never the upstream's. Typed `LocalPullFault` so the pull verdict does
                // not score an honest provider `Unreachable` for it.
                Err(err) => {
                    self.metrics.node_pull_reactive_topup_refused();
                    Err(err.context(LocalPullFault))
                }
            }
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::duration_suboptimal_units,
    reason = "workspace anti-panic policy targets runtime code; the settle-cadence \
              test asserts against the raw config unit, not the most readable one"
)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use alloy::dyn_abi::Eip712Domain;
    use alloy::primitives::{Address, B256};
    use alloy::signers::local::PrivateKeySigner;
    use anyhow::Result;
    use decdn_incentive::PoolId;

    use super::*;
    use crate::buyer_channel::{PoolOpener, TopUpLanded};

    /// A configurable [`PoolOpener`] double: `top_up_pool` records the
    /// `additional` it was called with (and how many times) and returns a
    /// fixed outcome. Only `top_up_pool` is exercised by `NodeFunder`; the rest of
    /// the trait is required by its signature but unreachable from these tests.
    #[derive(Debug)]
    struct MockOpener {
        top_up_result: Result<TopUpLanded, String>,
        top_up_calls: AtomicU32,
        last_additional: StdMutex<Option<U256>>,
    }

    impl MockOpener {
        fn new(top_up_result: Result<TopUpLanded, String>) -> Self {
            Self {
                top_up_result,
                top_up_calls: AtomicU32::new(0),
                last_additional: StdMutex::new(None),
            }
        }

        /// An opener whose top-up lands with `new_deposit` and adds `added`.
        fn landing(new_deposit: u64, added: u64) -> Self {
            Self::new(Ok(TopUpLanded {
                new_deposit: U256::from(new_deposit),
                added: U256::from(added),
            }))
        }

        fn call_count(&self) -> u32 {
            self.top_up_calls.load(Ordering::SeqCst)
        }

        fn last_additional(&self) -> Option<U256> {
            *self
                .last_additional
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    #[async_trait]
    impl PoolOpener for MockOpener {
        async fn open_or_reuse_pool(
            &self,
            _provider_addr: Address,
            _budget: std::time::Duration,
        ) -> Result<PoolContext> {
            unreachable!("not exercised by NodeFunder tests")
        }

        fn record_progress(
            &self,
            _provider_addr: Address,
            _pool_id: PoolId,
            _bytes_delivered: U256,
            _amount: U256,
        ) -> Result<()> {
            unreachable!("not exercised by NodeFunder tests")
        }

        async fn top_up_pool(&self, additional: U256) -> Result<TopUpLanded> {
            self.top_up_calls.fetch_add(1, Ordering::SeqCst);
            *self
                .last_additional
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(additional);
            self.top_up_result.clone().map_err(|e| anyhow::anyhow!(e))
        }
    }

    fn test_ctx(deposit: U256) -> Arc<Mutex<PoolContext>> {
        let signer = PrivateKeySigner::random();
        Arc::new(Mutex::new(PoolContext {
            pool_id: B256::ZERO,
            provider: Address::repeat_byte(9),
            deposit,
            client_signer: Arc::new(signer),
            voucher_domain: Eip712Domain::default(),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        }))
    }

    fn test_funded() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    /// The driver path's contract: `top_up(additional)` asks the opener to add
    /// exactly `additional`. The driver sizes it from the pull's live ledger; the
    /// funder never re-sizes it from a deposit or committed-spend view of its own,
    /// which would drift from what the driver saw (#1893).
    #[tokio::test]
    async fn top_up_asks_the_opener_for_exactly_additional() {
        let deposit = U256::from(1_000u64);
        let additional = U256::from(250u64);
        let opener = Arc::new(MockOpener::landing(1_250, 250));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let f = NodeFunder::new(opener.clone(), test_ctx(deposit), metrics, test_funded());

        let outcome = f.top_up(additional).await.expect("top-up should succeed");

        assert_eq!(opener.last_additional(), Some(additional));
        assert_eq!(outcome, DepositOutcome::Added(U256::from(1_250u64)));
    }

    /// A funding failure is the node's own fault, so it carries `LocalPullFault`
    /// and the pull verdict does not score the upstream `Unreachable` for it.
    #[tokio::test]
    async fn top_up_pool_error_propagates_as_a_local_fault() {
        let opener = Arc::new(MockOpener::new(Err("chain rejected".to_string())));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let f = NodeFunder::new(
            opener.clone(),
            test_ctx(U256::from(100u64)),
            metrics,
            test_funded(),
        );

        let err = f.top_up(U256::from(50u64)).await.unwrap_err();

        assert!(format!("{err:#}").contains("chain rejected"), "{err:#}");
        assert!(err.downcast_ref::<LocalPullFault>().is_some());
        assert_eq!(
            super::super::pull_verdict(&err),
            super::super::PullVerdict::OurLocalFault
        );
        assert_eq!(opener.call_count(), 1);
    }

    #[test]
    fn max_topups_reports_the_reactive_budget() {
        let opener = Arc::new(MockOpener::landing(0, 0));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let f = NodeFunder::new(opener, test_ctx(U256::ZERO), metrics, test_funded());

        assert_eq!(f.max_topups(), MAX_REACTIVE_TOPUPS);
        assert_eq!(f.max_topups(), 1);
    }

    /// A top-up that adds the full request bumps `node_pull_reactive_topup_total`
    /// and marks the pull funded; a `top_up_pool` failure bumps
    /// `node_pull_reactive_topup_refused_total` instead (and still propagates the
    /// error) — the seam both pull paths share for metering.
    #[tokio::test]
    async fn top_up_meters_success_and_refusal() {
        let metrics = Arc::new(crate::metrics::Metrics::new());

        let funded = test_funded();
        let f = NodeFunder::new(
            Arc::new(MockOpener::landing(1_250, 250)),
            test_ctx(U256::from(1_000u64)),
            Arc::clone(&metrics),
            Arc::clone(&funded),
        );
        let _ = f
            .top_up(U256::from(250u64))
            .await
            .expect("top-up should succeed");
        assert!(funded.load(Ordering::Relaxed));
        let text = metrics.encode().expect("metrics should encode");
        assert!(
            text.contains("decdn_node_pull_reactive_topup_total 1"),
            "expected a full top-up to bump the success counter: {text}"
        );

        let err_opener = Arc::new(MockOpener::new(Err("chain rejected".to_string())));
        let funded = test_funded();
        let f = NodeFunder::new(
            err_opener,
            test_ctx(U256::from(1_000u64)),
            Arc::clone(&metrics),
            Arc::clone(&funded),
        );
        let _ = f.top_up(U256::from(250u64)).await;
        assert!(!funded.load(Ordering::Relaxed));
        let text = metrics.encode().expect("metrics should encode");
        assert!(
            text.contains("decdn_node_pull_reactive_topup_refused_total 1"),
            "expected a failing top-up to bump the refused counter: {text}"
        );
    }

    /// An opener that adds nothing is a refusal, not a success, and leaves the pull
    /// unfunded. A zero `new_deposit` (the trait default) never lowers the deposit
    /// the driver credits.
    #[tokio::test]
    async fn top_up_that_adds_nothing_is_refused() {
        let deposit = U256::from(1_000u64);
        let opener = Arc::new(MockOpener::landing(0, 0));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let funded = test_funded();
        let f = NodeFunder::new(
            opener,
            test_ctx(deposit),
            Arc::clone(&metrics),
            Arc::clone(&funded),
        );

        let outcome = f
            .top_up(U256::from(250u64))
            .await
            .expect("a landing that adds nothing is not an error");

        assert_eq!(outcome, DepositOutcome::Added(deposit));
        assert!(!funded.load(Ordering::Relaxed));
        let text = metrics.encode().expect("metrics should encode");
        assert!(
            text.contains("decdn_node_pull_reactive_topup_refused_total 1"),
            "expected an empty landing to bump the refused counter: {text}"
        );
        assert!(
            !text.contains("decdn_node_pull_reactive_topup_total 1"),
            "expected an empty landing NOT to bump the success counter: {text}"
        );
    }

    /// A landing that adds less than `additional` is metered as refused (#2012),
    /// even when the pool's total deposit grew by more — another funder's escrow is
    /// not this pull's headroom. It still marks the pull funded, because it escrowed
    /// something, so the extortion metering does not count it a second time.
    #[tokio::test]
    async fn top_up_short_landing_is_refused() {
        let deposit = U256::from(1_000u64);
        let opener = Arc::new(MockOpener::landing(1_400, 100));
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let funded = test_funded();
        let f = NodeFunder::new(
            opener,
            test_ctx(deposit),
            Arc::clone(&metrics),
            Arc::clone(&funded),
        );

        let outcome = f
            .top_up(U256::from(250u64))
            .await
            .expect("a short landing still credits the deposit");

        assert_eq!(outcome, DepositOutcome::Added(U256::from(1_400u64)));
        assert!(funded.load(Ordering::Relaxed));
        let text = metrics.encode().expect("metrics should encode");
        assert!(
            text.contains("decdn_node_pull_reactive_topup_refused_total 1"),
            "expected a short landing to bump the refused counter: {text}"
        );
        assert!(
            !text.contains("decdn_node_pull_reactive_topup_total 1"),
            "expected a short landing NOT to bump the success counter: {text}"
        );
    }

    /// Two poll intervals of 500 ms steps, so the upstream clears a full poll plus
    /// its head cache — 28 steps (14 s) at the 7 s default.
    #[test]
    fn settle_budget_tracks_the_chain_poll_cadence() {
        assert_eq!(settle_wait_budget(Duration::from_secs(7)), 28);
        assert_eq!(settle_wait_budget(Duration::from_secs(1)), 4);
        // A chain lane tuned faster than one step still gets a real, if tiny, budget
        // rather than zero — a zero budget would make the top-up a coin flip.
        assert_eq!(settle_wait_budget(Duration::from_millis(250)), 1);
        // ...and a SLOW chain lane cannot buy unbounded foreground sleep.
        // `event_poll_interval_ms` has a config floor but no ceiling, so without
        // the cap a 60 s lane would sleep a client's pull for two minutes.
        assert_eq!(
            settle_wait_budget(Duration::from_secs(60)),
            MAX_SETTLE_WAITS
        );
        assert_eq!(
            settle_wait_budget(Duration::from_secs(3600)),
            MAX_SETTLE_WAITS
        );
    }
}
