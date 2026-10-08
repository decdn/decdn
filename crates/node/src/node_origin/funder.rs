//! `NodeFunder`: the node's [`Funder`] adapter over [`PoolOpener::recover_pool`].
//!
//! The node's upstream cache-miss pull adds funds only through the funding
//! recovery step (ADR 003 § Funding recovery): when a fill's candidate walk is
//! exhausted and at least one candidate refused this node's funding, the fill
//! runs one step under its [`decdn_client::RecoveryGate`]. `NodeFunder` is the
//! bridge from that step to the node's buyer pool service, which sizes the
//! top-up from its own pool row and opens a replacement pool when the current
//! one no longer accepts funds.
//!
//! The node's own low-water refill of its buyer pool is a separate, proactive
//! node policy ([`crate::buyer_channel`]); it reacts to no upstream.

use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::U256;
use decdn_client::source::SourceFuture;
use decdn_client::{Funder, LocalPullFault, Recovery};

use crate::buyer_channel::PoolOpener;

/// One step of the settle wait after a funding recovery step's top-up. Small
/// enough that the common case (the upstream's watcher was already close to
/// its next poll) costs little.
pub(crate) const SETTLE_POLL_STEP: Duration = Duration::from_millis(500);

/// How many [`SETTLE_POLL_STEP`]s a fill waits, after its funding recovery
/// step topped the pool up, for the UPSTREAM's chain watcher to observe the
/// new deposit before treating its refusal as real.
///
/// The upstream gates serving on the deposit it has observed, so between our
/// receipt and its next poll it correctly refuses a stream for a pool it still
/// believes is short. Waiting that out is money-safe: an open sends no voucher.
///
/// Sized at **two poll intervals** (14 s at the 7 s default), not a hard-coded
/// constant. One interval is the bare minimum and leaves no room for the
/// watcher's own head TTL, and anything derived from our own timeouts would
/// drift the moment an operator retunes the chain lane. Two clears a full poll
/// plus the head cache in the ordinary case.
///
/// Then capped at [`MAX_SETTLE_WAITS`], because `event_poll_interval_ms` has a
/// config floor but NO ceiling: at a 60 s chain lane the derived budget would
/// be 240 steps: two minutes of a foreground client's pull spent waiting.
pub(crate) fn settle_wait_budget(event_poll_interval: Duration) -> u32 {
    let budget = event_poll_interval.saturating_mul(2).as_millis();
    let step = SETTLE_POLL_STEP.as_millis().max(1);
    u32::try_from(budget / step)
        .unwrap_or(u32::MAX)
        .min(MAX_SETTLE_WAITS)
}

/// The settle window ([`decdn_client::RecoveryGate::with_settle`]) a fill's
/// funding recovery gate keeps after a top-up: [`settle_wait_budget`] steps of
/// [`SETTLE_POLL_STEP`].
pub(crate) fn settle_window(event_poll_interval: Duration) -> Duration {
    SETTLE_POLL_STEP.saturating_mul(settle_wait_budget(event_poll_interval))
}

/// Hard ceiling on the settle wait, whatever the configured chain cadence: 30 s
/// of waits. Past this the top-up is better treated as not-yet-visible and the
/// fill ended, than kept alive on a client's clock.
const MAX_SETTLE_WAITS: u32 = 60;

/// The node's [`Funder`]: runs the funding recovery step on ITS OWN buyer pool
/// through an injected [`PoolOpener`].
///
/// The step sizes itself from the pool row the opener holds, so the
/// `remaining` deposit [`Funder::recover`] receives is not read. `seen` is the
/// deposit the fill saw: a row above it is a sibling fill's step, which this
/// fill shares instead of funding its own ([`PoolOpener::recover_pool`]). It
/// records `node_pull_reactive_topup` on a step that funds or settles and
/// `node_pull_reactive_topup_refused` on a step that cannot, or fails.
pub(crate) struct NodeFunder {
    opener: Arc<dyn PoolOpener>,
    metrics: Arc<crate::metrics::Metrics>,
    seen: U256,
}

impl std::fmt::Debug for NodeFunder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeFunder")
            .field("seen", &self.seen)
            .finish_non_exhaustive()
    }
}

impl NodeFunder {
    pub(crate) fn new(
        opener: Arc<dyn PoolOpener>,
        metrics: Arc<crate::metrics::Metrics>,
        seen: U256,
    ) -> Self {
        Self {
            opener,
            metrics,
            seen,
        }
    }
}

impl Funder for NodeFunder {
    fn recover(&self, _remaining: U256) -> SourceFuture<'_, Recovery> {
        Box::pin(async move {
            match self.opener.recover_pool(self.seen).await {
                Ok(Recovery::ToppedUp(deposit)) => {
                    self.metrics.node_pull_reactive_topup();
                    Ok(Recovery::ToppedUp(deposit))
                }
                Ok(Recovery::Replaced(replaced)) => {
                    self.metrics.node_pull_reactive_topup();
                    Ok(Recovery::Replaced(replaced))
                }
                Ok(other) => {
                    self.metrics.node_pull_reactive_topup_refused();
                    Ok(other)
                }
                // A funding failure is ours (allowance, RPC, a row we cannot
                // credit), never the upstream's. Typed `LocalPullFault` so the
                // fill does not score an honest provider for it.
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
