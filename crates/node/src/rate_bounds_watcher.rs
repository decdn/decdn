//! `PaymentChannel.RateBoundsUpdated` watcher (#1172, ADR 019 §3.1 / ADR 003).
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
//! The sink is read-only and holds no durable cursor: a bounded recent
//! lookback (`CursorStart::HeadMinusWindow`) is sufficient because the startup
//! read already established the authoritative baseline, and the periodic
//! re-read reconciles anything the event tail missed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::eth::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::Result;
use decdn_incentive::payment_channel::PaymentChannel;

use crate::chain_events::MAX_BACKFILL_BLOCK_SPAN;
use crate::chain_events::resumable_watcher::{
    self, CursorStart, LogSink, WatcherConfig, WatcherHandle,
};
use crate::chain_events::shared_head::HeadSource;
use crate::rate_bounds::RateBounds;

/// Projection sink for `RateBoundsUpdated`: decodes each event into the shared
/// clamp, and re-reads `getRateBounds()` authoritatively once per
/// `poll_interval` at the end of a clean tick.
struct RateBoundsSink<P: Provider + Clone> {
    contract: PaymentChannel::PaymentChannelInstance<P>,
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
        if log.topic0() == Some(&PaymentChannel::RateBoundsUpdated::SIGNATURE_HASH) {
            match PaymentChannel::RateBoundsUpdated::decode_log_data(&log.inner.data) {
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

/// Spawn the rate-bounds watcher. `event_poll_interval` is the shared getLogs
/// cadence (events picked up promptly); `poll_interval` is the slower
/// authoritative-re-read safety net (`rate_bounds_poll_interval`, default 1h).
pub(crate) fn spawn<P>(
    provider: P,
    payment_channel_addr: Address,
    bounds: RateBounds,
    event_poll_interval: Duration,
    poll_interval: Duration,
    head: Arc<dyn HeadSource>,
    metrics: &Arc<crate::metrics::Metrics>,
) -> WatcherHandle
where
    P: Provider + Clone + 'static,
{
    let contract = PaymentChannel::new(payment_channel_addr, provider.clone());
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
    let cfg = WatcherConfig::new(
        head,
        Filter::new()
            .address(payment_channel_addr)
            .event_signature(PaymentChannel::RateBoundsUpdated::SIGNATURE_HASH),
        // Bounded recent lookback each boot; the startup getRateBounds() read is
        // the authoritative baseline, and the periodic re-read reconciles the
        // tail. No durable cursor needed.
        CursorStart::HeadMinusWindow {
            window_blocks: MAX_BACKFILL_BLOCK_SPAN,
        },
        event_poll_interval.max(Duration::from_secs(1)),
        "rate-bounds",
    )
    // Liveness + panic signals. Load-bearing here more than for most watchers:
    // this sink's poll-failure and undecodable-log paths both return `Ok` by
    // design, so without these a watcher wedged in RPC backoff (or dead) looks
    // identical to a healthy one while the node signs quotes against stale
    // bounds.
    .on_tick_success(crate::metrics::metric_hook(
        metrics,
        crate::metrics::Metrics::rate_bounds_watcher_tick,
    ))
    .on_task_panic(crate::metrics::metric_hook(
        metrics,
        crate::metrics::Metrics::rate_bounds_watcher_task_panicked,
    ));
    // The sink observes no shutdown token; the runtime drives graceful stop via
    // the returned handle's `shutdown()`.
    resumable_watcher::spawn(provider, cfg, move |_| sink)
}
