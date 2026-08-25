//! `FeeRouter.SharesUpdated` watcher (ADR 041 / ADR 016 § Tunable Economics).
//!
//! The node seeds its live operator fee-share cell ([`OperatorShares`]) from an
//! authoritative `getShares()` read at startup (see `runtime::mod`). This
//! watcher keeps that cell current: it follows `SharesUpdated` events off the
//! same shared-head `eth_getLogs` poller every other chain watcher uses, and —
//! as a safety net against a missed log — periodically re-reads `getShares()`
//! authoritatively at `fee_shares_poll_interval`. Both paths `store` into the
//! shared [`OperatorShares`], so a governance retune of the fee split reaches
//! the running serve-economics margin calculation without a restart.
//!
//! The sink is read-only and holds no durable cursor, and it scans no history:
//! the tail starts AT head. There is nothing for a lookback to recover — the
//! startup `getShares()` read is authoritative and already reflects every event
//! ever emitted, so re-scanning blocks below it can only re-derive a value the
//! node already holds. The periodic re-read covers anything the tail drops.

use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::eth::Log;
use alloy::sol_types::SolEvent;
use anyhow::Result;
use decdn_incentive::payment_pool::FeeRouter;

use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{CursorStart, LogSink, clear_cadence_on_recovery};
use crate::fee_shares::{OperatorShares, operator_bps_from_shares};

/// Projection sink for `SharesUpdated`: decodes each event into the shared
/// operator-fee-share cell, and re-reads `getShares()` authoritatively once
/// per `poll_interval` at the end of a clean tick.
struct FeeSharesSink<P: Provider + Clone> {
    contract: FeeRouter::FeeRouterInstance<P>,
    shares: OperatorShares,
    /// Authoritative-re-read cadence (`fee_shares_poll_interval`).
    poll_interval: Duration,
    /// When the last authoritative re-read ran; `None` until the first.
    last_poll: Option<Instant>,
}

impl<P: Provider + Clone> FeeSharesSink<P> {
    /// Narrow the on-chain `[operator, buyback, treasury]` split to the
    /// operator's bps and store it. Fail-closed at the narrowing step
    /// (`operator_bps_from_shares` rejects an out-of-range split), so unlike
    /// `rate_bounds_watcher`'s clamp-and-keep, a bad split here simply never
    /// overwrites the current cell: it is logged and the previous share is
    /// kept, because signing quotes against a value we cannot validate is
    /// worse than signing against the last-known-good one.
    fn store_shares(&self, raw_shares: [U256; 3], source: &str) {
        match operator_bps_from_shares(raw_shares, source) {
            Ok(bps) => {
                self.shares.store(bps);
                tracing::info!(bps, source, "operator fee share updated from chain");
            }
            Err(err) => {
                tracing::error!(
                    %err,
                    source,
                    "fee-shares watcher: on-chain FeeRouter split could not be narrowed to \
                     operator bps; keeping the current share — governance must fix the split"
                );
            }
        }
    }
}

impl<P: Provider + Clone + 'static> LogSink for FeeSharesSink<P> {
    async fn apply(&mut self, log: Log) -> Result<()> {
        // Filter is scoped to the single SharesUpdated topic; match
        // defensively so an unexpected log is skipped, not misdecoded.
        if log.topic0() == Some(&FeeRouter::SharesUpdated::SIGNATURE_HASH) {
            match FeeRouter::SharesUpdated::decode_log_data(&log.inner.data) {
                Ok(ev) => {
                    self.store_shares(ev.newShares, "event");
                }
                Err(err) => {
                    // Undecodable log: log-and-skip (never Err — a deterministic
                    // re-scan would hot-loop the cursor).
                    tracing::warn!(%err, "fee-shares watcher: undecodable SharesUpdated log; skipping");
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
        // Stamp BEFORE the call, not only on success — see
        // `rate_bounds_watcher::RateBoundsSink::on_tick_complete` for why: a
        // persistently failing `getShares()` must retry on the slow cadence,
        // not on every watcher tick.
        self.last_poll = Some(now);
        match self.contract.getShares().call().await {
            Ok(shares) => {
                self.store_shares(shares, "poll");
            }
            Err(err) => {
                // Best-effort safety net: the event path is primary, so a failed
                // re-read logs and keeps the current share rather than backing
                // off the whole watcher (which would also stall event pickup).
                // Returning Ok keeps the cursor advancing.
                tracing::warn!(%err, "fee-shares watcher: authoritative getShares() poll failed; keeping current share");
            }
        }
        Ok(())
    }

    /// Force the safety-net re-read on the tick the watcher recovers.
    ///
    /// [`Self::on_tick_complete`] stamps its clock before the call, so an outage
    /// spanning a due poll defers the re-read a further `poll_interval` — hours,
    /// at the default. The node meanwhile prices its share of every settlement
    /// off a split the chain may have moved.
    fn on_recovered(&mut self) {
        clear_cadence_on_recovery(&mut self.last_poll, self.poll_interval);
    }
}

/// Build the fee-shares [`Route`] for the shared multiplexed poller.
/// `poll_interval` is the slower authoritative-re-read safety net
/// (`fee_shares_poll_interval`) that rides the sink's `on_tick_complete`; the
/// merged getLogs cadence is the poller's.
pub fn route<P>(
    provider: P,
    fee_router_addr: Address,
    shares: OperatorShares,
    poll_interval: Duration,
    metrics: &Arc<crate::metrics::Metrics>,
) -> Route
where
    P: Provider + Clone + 'static,
{
    let contract = FeeRouter::new(fee_router_addr, provider);
    let sink = FeeSharesSink {
        contract,
        shares,
        poll_interval,
        // Seeded to "just polled": the runtime performed the authoritative
        // startup `getShares()` read moments ago, so leaving this `None`
        // would fire a redundant re-read on the very first tick. The first
        // safety-net poll is due one `poll_interval` from now.
        last_poll: Some(Instant::now()),
    };
    Route {
        addresses: vec![fee_router_addr],
        topic0s: vec![FeeRouter::SharesUpdated::SIGNATURE_HASH],
        // Start the tail at head — no historical scan at all. `window_blocks: 0`
        // resolves to head exactly. The startup `getShares()` read is the
        // authoritative baseline and already folds in every past event, so a
        // lookback would re-derive a value the node holds; the periodic re-read
        // is the backstop for anything the tail drops. No durable cursor needed.
        start: CursorStart::HeadMinusWindow { window_blocks: 0 },
        sink: SinkSource::Ready(Box::new(sink)),
        label: "fee-shares",
        on_established: None,
        on_backoff: None,
        // Liveness + panic signals. Load-bearing here more than for most
        // watchers: this sink's poll-failure and undecodable-log paths both
        // return `Ok` by design, so without these a route wedged in RPC backoff
        // (or dead) looks identical to a healthy one while the node signs quotes
        // against a stale operator fee share.
        on_tick_success: Some(crate::metrics::metric_hook(
            metrics,
            crate::metrics::Metrics::fee_shares_watcher_tick,
        )),
        on_task_panic: Some(crate::metrics::metric_hook(
            metrics,
            crate::metrics::Metrics::fee_shares_watcher_task_panicked,
        )),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use alloy::primitives::LogData;
    use alloy::providers::ProviderBuilder;

    use super::*;
    use crate::metrics::Metrics;

    /// `route()` never calls the chain (it only constructs a contract handle),
    /// so a mocked client with no scripted responses is sufficient here.
    fn mock_provider() -> impl Provider + Clone + 'static {
        ProviderBuilder::new().connect_mocked_client(alloy::providers::mock::Asserter::new())
    }

    /// Build a `FeeSharesSink` over a mocked, never-called provider. No live
    /// contract: `on_tick_complete`'s `getShares()` call is never exercised by
    /// tests that only drive `apply`, and the mocked client panics loudly if a
    /// test path did reach it unscripted.
    fn for_test(shares: OperatorShares) -> FeeSharesSink<impl Provider + Clone + 'static> {
        FeeSharesSink {
            contract: FeeRouter::new(Address::ZERO, mock_provider()),
            shares,
            poll_interval: Duration::from_secs(u64::MAX),
            last_poll: Some(Instant::now()),
        }
    }

    /// Encode a synthetic `SharesUpdated { newShares }` log, the same way the
    /// live poller would hand one to `apply`.
    fn synthetic_shares_updated_log(new_shares: [U256; 3]) -> Log {
        let event = FeeRouter::SharesUpdated {
            newShares: new_shares,
        };
        Log {
            inner: alloy::primitives::Log {
                address: Address::ZERO,
                data: event.encode_log_data(),
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn shares_updated_log_updates_cell() {
        let shares = OperatorShares::new(6000);
        let mut sink = for_test(shares.clone());
        let log = synthetic_shares_updated_log([
            U256::from(4000u64),
            U256::from(5000u64),
            U256::from(1000u64),
        ]);
        sink.apply(log).await.expect("apply ok");
        assert_eq!(shares.bps(), 4000);
    }

    /// A log whose topic0 doesn't match `SharesUpdated` is skipped, not
    /// errored, and never touches the cell.
    #[tokio::test]
    async fn foreign_topic_log_is_skipped() {
        let shares = OperatorShares::new(6000);
        let mut sink = for_test(shares.clone());
        let mut log = synthetic_shares_updated_log([
            U256::from(4000u64),
            U256::from(5000u64),
            U256::from(1000u64),
        ]);
        log.inner.data = LogData::empty();
        sink.apply(log)
            .await
            .expect("apply ok even for a foreign topic");
        assert_eq!(
            shares.bps(),
            6000,
            "cell must be untouched by a non-matching log"
        );
    }

    /// The fee-shares watcher's `Route` must carry exactly the
    /// `SharesUpdated` topic0 and start at head (`HeadMinusWindow { 0 }`) —
    /// this fails if the topic0 were ever dropped or swapped for another event.
    #[test]
    fn route_watches_shares_updated_from_head() {
        let metrics = Arc::new(Metrics::new());
        let route = route(
            mock_provider(),
            Address::repeat_byte(0x22),
            OperatorShares::new(0),
            Duration::from_hours(1),
            &metrics,
        );

        assert_eq!(route.addresses, vec![Address::repeat_byte(0x22)]);
        assert_eq!(
            route.topic0s,
            vec![FeeRouter::SharesUpdated::SIGNATURE_HASH],
            "must watch exactly SharesUpdated — no more, no fewer"
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
