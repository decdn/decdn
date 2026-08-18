//! `PaymentPool.RateBoundsUpdated` watcher (#1172, ADR 019 §3.1 / ADR 003).
//!
//! The node seeds its live delivery-rate clamp ([`RateBounds`]) from an
//! authoritative `getRateBounds()` read at startup (see `runtime::mod`). This
//! watcher keeps that clamp current: it follows `RateBoundsUpdated` events off
//! the same shared-head `eth_getLogs` poller every other chain watcher uses, and
//! — as a safety net against a missed log — periodically re-reads
//! `getRateBounds()` authoritatively at `rate_bounds_poll_interval` (default
//! 1h). Both paths `store` into the shared [`RateBounds`], so a governance
//! retune reaches the running probe/client handlers without a restart.
//!
//! The sink is read-only and holds no durable cursor, and it scans no history:
//! the tail starts AT head. There is nothing for a lookback to recover — the
//! startup `getRateBounds()` read is authoritative and already reflects every
//! event ever emitted, so re-scanning blocks below it can only re-derive a value
//! the node already holds. The hourly re-read covers anything the tail drops.

use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::eth::Log;
use alloy::sol_types::SolEvent;
use anyhow::Result;
use decdn_incentive::payment_pool::PaymentPool;

use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{CursorStart, LogSink};
use crate::rate_bounds::RateBounds;

/// Projection sink for `RateBoundsUpdated`: decodes each event into the shared
/// clamp, and re-reads `getRateBounds()` authoritatively once per
/// `poll_interval` at the end of a clean tick.
struct RateBoundsSink<P: Provider + Clone> {
    contract: PaymentPool::PaymentPoolInstance<P>,
    bounds: RateBounds,
    /// Authoritative-re-read cadence (`rate_bounds_poll_interval`).
    poll_interval: Duration,
    /// When the last authoritative re-read ran; `None` until the first.
    last_poll: Option<Instant>,
}

impl<P: Provider + Clone> RateBoundsSink<P> {
    /// Convert the on-chain `uint256` floor to the node's `u64` clamp and store
    /// it. A value beyond `u64::MAX` cannot be applied — unlike startup (which
    /// refuses to boot), a running node cannot bail, so it logs and keeps the
    /// current floor rather than truncating to a wrong clamp. The floor is
    /// enforced at settlement, so it must be represented exactly or not at all:
    /// an out-of-range floor means we cannot know what we are obliged to
    /// charge, and keeping the previous one is the only safe move.
    fn store_bounds(&self, floor: U256, source: &str) {
        let Ok(floor) = u64::try_from(floor) else {
            tracing::error!(
                %floor,
                source,
                "rate-bounds watcher: on-chain delivery floor exceeds u64::MAX; \
                 keeping current floor — governance must lower it"
            );
            return;
        };
        // Same bound the startup read enforces (`rate_bounds::on_chain_floor_to_u64`).
        // Without it the value that refuses to boot is installed silently at
        // runtime one event later: the node would raise every quote above the
        // wire cap, so no peer could decode its responses and every voucher
        // would revert at settlement — while it looked healthy locally. Keeping
        // the previous floor is the lesser evil, but it is NOT safe: the node is
        // now quoting under a floor the chain will not honour, so anything it
        // serves accrues vouchers that revert at redemption. `error!` because an
        // operator must escalate to governance, not because it is self-healing.
        if floor > decdn_protocol::MAX_RATE_PER_MB {
            tracing::error!(
                floor,
                max = decdn_protocol::MAX_RATE_PER_MB,
                source,
                "rate-bounds watcher: on-chain delivery floor exceeds the wire cap \
                 MAX_RATE_PER_MB; keeping the previous floor, but this node is now \
                 quoting under a floor the chain will not honour and its vouchers will \
                 revert at settlement — governance must lower the floor"
            );
            return;
        }
        self.bounds.store(floor);
        tracing::info!(floor, source, "delivery-rate floor updated from chain");
    }
}

impl<P: Provider + Clone + 'static> LogSink for RateBoundsSink<P> {
    async fn apply(&mut self, log: Log) -> Result<()> {
        // Filter is scoped to the single RateBoundsUpdated topic; match
        // defensively so an unexpected log is skipped, not misdecoded.
        if log.topic0() == Some(&PaymentPool::RateBoundsUpdated::SIGNATURE_HASH) {
            match PaymentPool::RateBoundsUpdated::decode_log_data(&log.inner.data) {
                Ok(ev) => {
                    self.store_bounds(ev.newDeliveryFloor, "event");
                }
                Err(err) => {
                    // Undecodable log: log-and-skip (never Err — a deterministic
                    // re-scan would hot-loop the cursor).
                    tracing::warn!(%err, "rate-bounds watcher: undecodable RateBoundsUpdated log; skipping");
                }
            }
        }
        Ok(())
    }

    async fn on_tick_complete(&mut self) -> Result<()> {
        let now = Instant::now();
        let due = self
            .last_poll
            .is_none_or(|last| now.duration_since(last) >= self.poll_interval);
        if !due {
            return Ok(());
        }
        // Stamp BEFORE the call, not only on success. Stamping in the `Ok` arm
        // meant a persistently failing `getRateBounds()` retried on every
        // watcher tick (`event_poll_interval`, ~7s) instead of hourly — exactly
        // the RPC hammering the `rate_bounds_poll_interval_sec != 0` validator
        // exists to prevent, plus a warn line each time.
        self.last_poll = Some(now);
        match self.contract.getRateBounds().call().await {
            Ok(floor) => {
                self.store_bounds(floor, "poll");
            }
            Err(err) => {
                // Best-effort safety net: the event path is primary, so a failed
                // re-read logs and keeps the current floor rather than backing
                // off the whole watcher (which would also stall event pickup).
                // Returning Ok keeps the cursor advancing.
                tracing::warn!(%err, "rate-bounds watcher: authoritative getRateBounds() poll failed; keeping current floor");
            }
        }
        Ok(())
    }
}

/// Build the rate-bounds [`Route`] for the shared multiplexed poller.
/// `poll_interval` is the slower authoritative-re-read safety net
/// (`rate_bounds_poll_interval`, default 1h) that rides the sink's
/// `on_tick_complete`; the merged getLogs cadence is the poller's.
pub(crate) fn route<P>(
    provider: P,
    payment_pool_addr: Address,
    bounds: RateBounds,
    poll_interval: Duration,
    metrics: &Arc<crate::metrics::Metrics>,
) -> Route
where
    P: Provider + Clone + 'static,
{
    let contract = PaymentPool::new(payment_pool_addr, provider);
    let sink = RateBoundsSink {
        contract,
        bounds,
        poll_interval,
        // Seeded to "just polled": the runtime performed the authoritative
        // startup `getRateBounds()` read moments ago, so leaving this `None`
        // would fire a redundant re-read on the very first tick. The first
        // safety-net poll is due one `poll_interval` from now.
        last_poll: Some(Instant::now()),
    };
    Route {
        addresses: vec![payment_pool_addr],
        topic0s: vec![PaymentPool::RateBoundsUpdated::SIGNATURE_HASH],
        // Start the tail at head — no historical scan at all. `window_blocks: 0`
        // resolves to head exactly. The startup `getRateBounds()` read is the
        // authoritative baseline and already folds in every past event, so a
        // lookback would re-derive a value the node holds; the hourly re-read is
        // the backstop for anything the tail drops. No durable cursor needed.
        start: CursorStart::HeadMinusWindow { window_blocks: 0 },
        sink: SinkSource::Ready(Box::new(sink)),
        label: "rate-bounds",
        on_established: None,
        on_backoff: None,
        // Liveness + panic signals. Load-bearing here more than for most
        // watchers: this sink's poll-failure and undecodable-log paths both
        // return `Ok` by design, so without these a route wedged in RPC backoff
        // (or dead) looks identical to a healthy one while the node signs quotes
        // against stale bounds.
        on_tick_success: Some(crate::metrics::metric_hook(
            metrics,
            crate::metrics::Metrics::rate_bounds_watcher_tick,
        )),
        on_task_panic: Some(crate::metrics::metric_hook(
            metrics,
            crate::metrics::Metrics::rate_bounds_watcher_task_panicked,
        )),
    }
}

#[cfg(test)]
mod tests {
    use alloy::providers::ProviderBuilder;

    use super::*;
    use crate::metrics::Metrics;

    /// `route()` never calls the chain (it only constructs a contract handle),
    /// so a mocked client with no scripted responses is sufficient here.
    fn mock_provider() -> impl Provider + Clone + 'static {
        ProviderBuilder::new().connect_mocked_client(alloy::providers::mock::Asserter::new())
    }

    /// The rate-bounds watcher's `Route` must carry exactly the
    /// `RateBoundsUpdated` topic0 and start at head (`HeadMinusWindow { 0 }`) —
    /// this fails if the topic0 were ever dropped or swapped for another event.
    #[test]
    fn route_watches_rate_bounds_updated_from_head() {
        let metrics = Arc::new(Metrics::new());
        let route = route(
            mock_provider(),
            Address::repeat_byte(0x11),
            RateBounds::new(0),
            Duration::from_hours(1),
            &metrics,
        );

        assert_eq!(route.addresses, vec![Address::repeat_byte(0x11)]);
        assert_eq!(
            route.topic0s,
            vec![PaymentPool::RateBoundsUpdated::SIGNATURE_HASH],
            "must watch exactly RateBoundsUpdated — no more, no fewer"
        );
        assert!(
            matches!(
                route.start,
                CursorStart::HeadMinusWindow { window_blocks: 0 }
            ),
            "must start at head with no lookback window (HeadMinusWindow{{ 0 }})"
        );
    }
}
