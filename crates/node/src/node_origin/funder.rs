//! `NodeFunder` — the node's [`Funder`] adapter over [`PoolOpener::top_up_pool_by`].
//!
//! [`decdn-client`](decdn_client)'s gap-driven `drive()` reactively tops up
//! the buyer deposit through the injected [`Funder`] seam (`source.rs`) rather
//! than naming a chain handle directly, so the node's upstream cache-miss pull
//! leg shares that driver instead of running its own copy of the top-up
//! loop. `NodeFunder` is the bridge.
//!
//! `Funder::top_up(additional)` asks to add `additional` to the deposit — the same
//! request the CLI's `CliFunder` makes by calling `topUp` with `additional`
//! directly. A `topUp` leaves the pool's committed spend untouched, so what lands
//! on the deposit lands on the spendable headroom too. What lands can be less than
//! `additional`. [`PoolOpener::top_up_pool_by`] takes the same amount and reports how
//! much of it landed, so `NodeFunder` passes `additional` through and grades the
//! landing on that report: a full landing is a success, and a short one is still
//! returned as `Added` so the driver keeps the headroom that did land. The driver sizes it from the pull's live
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
use decdn_client::source::SourceFuture;
use decdn_client::{Funder, LocalPullFault, PoolContext};
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
/// [`MAX_TOPUP_ATTEMPTS`]: decdn_client::MAX_TOPUP_ATTEMPTS
///
/// Reported through [`Funder::max_topups`] below, so [`NodeFunder`] is the one
/// source of the node's reactive-top-up budget.
pub(crate) const MAX_REACTIVE_TOPUPS: u32 = 1;

/// One step of the post-top-up settle wait. Small enough that the common case
/// (the upstream's watcher was already close to its next poll) costs little.
///
/// `pub(crate)` so the gap-driven pull leg ([`super::pull_leg::run_pull_leg`]) and
/// the node origin's own [`decdn_client::driver::DriveConfig`] reuse it as
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
///   `.await` (`top_up_pool_by` is a network+chain round trip).
/// - `metrics`: the node's metrics handle. `top_up` records
///   `node_pull_reactive_topup` on a landing that adds the full requested amount
///   and `node_pull_reactive_topup_refused` on a short landing or a
///   `top_up_pool_by` error — the one seam both the window-paced serve leg and the
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
            match self.opener.top_up_pool_by(additional).await {
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
                        // logged and metered here. `BuyerPoolService::top_up_pool_by`
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
mod tests;
