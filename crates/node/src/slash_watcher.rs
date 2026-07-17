//! Daemon slash-detection watcher (#1032, G-NODE-05).
//!
//! Follows `SlashJudge.Slashed` for this node's own operator and records each
//! slash into a shared in-memory store, so the operator can see it over the
//! admin RPC (`admin_v1_slashes`) and file `decdn appeal slash` within the
//! 30-day window without watching the chain directly. A counter metric
//! (`decdn_slashes_detected_total`) mirrors it.
//!
//! Shape mirrors [`crate::payment_settlement`] but far leaner (read-only, no
//! settlement path):
//!
//! - **Server-side operator filter.** `Slashed` indexes `operator` as `topic2`,
//!   so every `eth_getLogs` window constrains on it — a node never downloads or
//!   decodes other operators' slashes.
//! - **Rebuild from a bounded floor on every start.** The detected-slash store
//!   is in-memory, so it is empty on each process start and must be rebuilt by
//!   scanning history — a durable resume cursor could not skip this (resuming
//!   from it would drop still-appealable slashes at/below the cursor). But only
//!   the still-appealable tail matters, so the boot scan is bounded to the
//!   `appeal_window_blocks` lookback (`head - ~30 days`, clamped `>= from_block`
//!   the `SlashJudge` deploy floor) rather than genesis (#1108) — turning an
//!   O(chain-age) boot scan into O(appeal window). This still re-surfaces a slash
//!   mined while the node was down on the next restart, within its appeal window.
//! - **Unified getLogs poller.** Backfill and the live tail are one
//!   `resumable_watcher` cursor loop (#1092/#1106): each poll tick scans
//!   `[cursor, head]` in `MAX_BACKFILL_BLOCK_SPAN` windows via `eth_getLogs`
//!   (no `eth_newFilter`), advancing the cursor. The store is deduped by
//!   `slashId`, so the re-scan overlap each boot is harmless.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::Result;
use decdn_incentive::slash_judge::SlashJudge;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use decdn_common::redact::sanitize_err_chain;

use crate::chain_events::resumable_watcher::{
    self, CursorStart, LogSink, WatcherConfig, WatcherHook,
};
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::{AbortOnDrop, MAX_BACKFILL_BLOCK_SPAN, WATCHER_INITIAL_BACKOFF, timed};
use crate::metrics::Metrics;

/// Nominal appeal filing window (ADR 028: 30 days from the slash timestamp).
const APPEAL_FILING_WINDOW_SECS: u64 = 30 * 24 * 60 * 60;

/// Nominal Arbitrum block time. Used only to convert the 30-day appeal window
/// into a block-count lookback for the boot re-scan (below); it does not need to
/// be exact, only a *lower* bound on the true block time so the derived
/// block-count is an *upper* bound and the re-scan never covers less than the
/// appeal window. Arbitrum Sepolia observes ~0.25–0.3s/block.
const ARBITRUM_BLOCK_TIME_MS: u64 = 250;

/// Block-count lookback that covers at least the [`APPEAL_FILING_WINDOW_SECS`]
/// appeal window (#1108). The slash watcher re-scans `[head - this, head]` each
/// boot (clamped `>= from_block`) instead of `[from_block, head]`, because the
/// in-memory store must be rebuilt every start but only the still-appealable tail
/// matters — bounding an O(chain-age) boot scan to O(appeal window). `div_ceil`
/// keeps it an upper bound so the window is never under-covered.
const fn appeal_window_blocks() -> u64 {
    (APPEAL_FILING_WINDOW_SECS * 1_000).div_ceil(ARBITRUM_BLOCK_TIME_MS)
}

/// Ceiling for the poll-retry backoff — a deliberate override of the shared
/// [`crate::chain_events::WATCHER_MAX_BACKOFF`]: it is lower than the other
/// watchers' 1-minute cap because a missed `Slashed` event burns the operator's
/// fixed 30-day appeal window, so recovery is prioritized. Named distinctly from
/// the shared constant so the divergence is obvious at the `WatcherConfig` site.
/// The floor is the shared [`crate::chain_events::WATCHER_INITIAL_BACKOFF`].
const SLASH_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// One slash detected against this node's operator.
#[derive(Debug, Clone)]
pub struct DetectedSlash {
    /// Globally-monotonic on-chain `slashId` (`CapacityBond.slash`).
    pub slash_id: U256,
    /// Offense taxonomy index (ADR 014): 0=Phantom, 1=RateManipulation, 2=Blacklist.
    pub offense_type: u8,
    /// Bond amount slashed, in TOKEN base units.
    pub amount: U256,
    /// `keccak256` evidence digest from the `Slashed` event.
    pub evidence_hash: B256,
    /// Block the `Slashed` log was mined in (`None` if pending when observed).
    pub block_number: Option<u64>,
    /// **Nominal** appeal-window close: the `Slashed` block timestamp + 30 days
    /// (`None` if the block read failed). This is a cheap client-side hint, not
    /// the authoritative deadline — it does NOT read the `CapacityBond` slash
    /// record and ignores protocol-pause extensions, which only ever move the
    /// real deadline *later* (`markAppealOpen` adds `pausedTotal`). Safe to file
    /// before this; a keeper must not treat a just-past value as final.
    pub appeal_window_close: Option<u64>,
}

/// Shared, in-memory detected-slash store: appended in detection order and
/// deduped by `slashId`. Cloned into [`crate::admin::AdminState`] for the
/// read-only `admin_v1_slashes` surface.
pub type SlashStore = Arc<RwLock<Vec<DetectedSlash>>>;

/// A running slash-detection watcher. Holds the shared store and owns the
/// background task (aborted on drop).
pub struct SlashWatcher {
    store: SlashStore,
    _task: AbortOnDrop,
}

impl std::fmt::Debug for SlashWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlashWatcher").finish_non_exhaustive()
    }
}

impl SlashWatcher {
    /// Spawn the slash-detection watcher. Infallible: every RPC (head read,
    /// `get_logs`) happens inside the poll loop, so a transient
    /// bring-up failure retries with backoff rather than disabling detection for
    /// the daemon's lifetime.
    ///
    /// `from_block` is the deploy floor the boot re-scan is clamped to. The
    /// in-memory store is rebuilt on **every** process start, but bounded to the
    /// `appeal_window_blocks` lookback (`head - ~30 days`, clamped `>=
    /// from_block`) rather than genesis (#1108), so a slash mined while the node
    /// was down is re-surfaced on restart within its appeal window without an
    /// O(chain-age) scan — the store is not durable, so a resume cursor could not
    /// skip this rebuild.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn bootstrap<P: Provider + Clone + 'static>(
        provider: P,
        slash_judge_addr: Address,
        self_address: Address,
        from_block: u64,
        event_poll_interval: Duration,
        head: Arc<dyn HeadSource>,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
    ) -> Self {
        info!(%slash_judge_addr, %self_address, from_block, "slash-detection watcher started");
        let store: SlashStore = Arc::new(RwLock::new(Vec::new()));
        let on_established = Some(established_hook(&metrics));
        let on_backoff = Some(backoff_hook(&metrics));
        let sink = SlashSink {
            provider: provider.clone(),
            self_address,
            store: Arc::clone(&store),
            metrics,
        };
        let cfg = WatcherConfig {
            head,
            filter: operator_filter(slash_judge_addr, self_address),
            from_block,
            poll_interval: event_poll_interval,
            max_backfill_span: MAX_BACKFILL_BLOCK_SPAN,
            // No durable resume cursor (the in-memory store is rebuilt each boot);
            // re-scan the bounded appeal-window lookback, clamped to the deploy
            // floor (`from_block`), so a slash mined while down is re-surfaced (#1108).
            start: CursorStart::HeadMinusWindow {
                window_blocks: appeal_window_blocks(),
            },
            initial_backoff: WATCHER_INITIAL_BACKOFF,
            max_backoff: SLASH_MAX_BACKOFF,
            rpc_call_timeout: None,
            shutdown,
            label: "slash",
            on_established,
            on_backoff,
        };
        let task = tokio::spawn(resumable_watcher::run(provider, cfg, sink));
        Self {
            store,
            _task: AbortOnDrop(task),
        }
    }

    /// A clone of the shared detected-slash store for the admin surface.
    #[must_use]
    pub fn store(&self) -> SlashStore {
        Arc::clone(&self.store)
    }
}

/// Applies operator-filtered `Slashed` logs to the in-memory detected-slash
/// store (#1092). `apply` never returns `Err`: [`record_log`] decodes, skips a
/// reorged-out/undecodable/foreign log, and dedupes by `slashId`, so a bad log
/// neither tears down the poll cycle nor hot-loops the deterministic re-scan. The
/// best-effort appeal-window block-timestamp read fails soft (logged, `None`).
struct SlashSink<P: Provider + Clone> {
    provider: P,
    self_address: Address,
    store: SlashStore,
    metrics: Arc<Metrics>,
}

impl<P: Provider + Clone> LogSink for SlashSink<P> {
    async fn apply(&mut self, log: Log) -> Result<()> {
        record_log(
            &self.provider,
            self.self_address,
            &self.store,
            &self.metrics,
            &log,
        )
        .await;
        Ok(())
    }
}

/// Wire the watcher's healthy-cycle transition to `slash_watcher_cycle_established`
/// (down-seconds → 0).
fn established_hook(metrics: &Arc<Metrics>) -> WatcherHook {
    let metrics = Arc::clone(metrics);
    Box::new(move || metrics.slash_watcher_cycle_established())
}

/// Wire a tick failure to `slash_watcher_backoff_started` (opens the downtime
/// window `slash_watcher_down_seconds` reads).
fn backoff_hook(metrics: &Arc<Metrics>) -> WatcherHook {
    let metrics = Arc::clone(metrics);
    Box::new(move || metrics.slash_watcher_backoff_started())
}

/// The address + `Slashed`-signature + `topic2 == operator` filter applied to
/// every `eth_getLogs` poll window, so the RPC only ever returns this operator's
/// slashes.
fn operator_filter(slash_judge_addr: Address, self_address: Address) -> Filter {
    Filter::new()
        .address(slash_judge_addr)
        .event_signature(SlashJudge::Slashed::SIGNATURE_HASH)
        .topic2(self_address.into_word())
}

/// Decode one `Slashed` log, or `None` for a log that must not be recorded:
/// reorged-out (`removed == true` — not expected from `eth_getLogs`, which
/// returns only canonical logs, so this is defensive against a nonconforming
/// provider), undecodable, or another operator's. The `topic2` filter already
/// constrains to this operator, but the defensive operator check guards against a
/// provider that ignores the topic. Pure (no provider) so the skip policy is
/// unit-testable.
fn decode_slashed(
    self_address: Address,
    log: &alloy::rpc::types::Log,
) -> Option<SlashJudge::Slashed> {
    if log.removed {
        debug!("skipping reorged-out (removed) Slashed log");
        return None;
    }
    let Ok(event) = SlashJudge::Slashed::decode_log_data(&log.inner.data) else {
        warn!(
            block_number = ?log.block_number,
            tx = ?log.transaction_hash,
            "skipping undecodable Slashed log"
        );
        return None;
    };
    if event.operator != self_address {
        return None;
    }
    Some(event)
}

/// Decode one `Slashed` log and record it. Skipped logs (see
/// [`decode_slashed`]) neither tear down the cycle nor record a phantom slash.
async fn record_log<P: Provider>(
    provider: &P,
    self_address: Address,
    store: &SlashStore,
    metrics: &Arc<Metrics>,
    log: &alloy::rpc::types::Log,
) {
    let Some(event) = decode_slashed(self_address, log) else {
        return;
    };
    let window_close = appeal_window_close(provider, log.block_number).await;
    record_slash(
        store,
        metrics,
        DetectedSlash {
            slash_id: event.slashId,
            offense_type: event.offenseType as u8,
            amount: event.amount,
            evidence_hash: event.evidenceHash,
            block_number: log.block_number,
            appeal_window_close: window_close,
        },
    );
}

/// Nominal appeal-window close = block timestamp + 30 days, or `None` when the
/// block number is absent (pending log) or the block read fails (best-effort).
///
/// The read is bounded by [`timed`], and a timeout takes the same soft-degrade
/// path as any other read failure: the slash is still recorded, only its appeal
/// deadline is unknown. That is strictly better than the unbounded alternative,
/// where one stalled `get_block` wedges the whole tick and *no* slash surfaces.
async fn appeal_window_close<P: Provider>(provider: &P, block_number: Option<u64>) -> Option<u64> {
    let block_number = block_number?;
    match timed(
        None,
        "get_block",
        provider.get_block(alloy::eips::BlockId::from(block_number)),
    )
    .await
    {
        Ok(Some(block)) => Some(block.header.timestamp + APPEAL_FILING_WINDOW_SECS),
        Ok(None) => None,
        Err(err) => {
            // Scrubbed, not `%err`: `timed` folds in the transport leg, whose
            // Display carries reqwest's ` for url (…)` tail — i.e. the raw
            // `rpc_url`, credentials and all. This was the one chain-error log
            // in the tree rendering an alloy error unscrubbed.
            warn!(
                err = %sanitize_err_chain(&err),
                block_number,
                "failed to read slash block timestamp for appeal window"
            );
            None
        }
    }
}

/// Append `slash` to the store if its `slashId` is not already present, bumping
/// the detection metric only on a genuinely new slash (backfill/live overlap is
/// deduped). Poison-tolerant so a panicked reader can't wedge the writer.
fn record_slash(store: &SlashStore, metrics: &Arc<Metrics>, slash: DetectedSlash) {
    let mut guard = store
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(existing) = guard.iter_mut().find(|s| s.slash_id == slash.slash_id) {
        // Re-sighting (backfill/live overlap or a re-scanned window): don't
        // re-record or re-count, but do backfill the best-effort deadline hint
        // if the original sighting's block-timestamp read failed — the dedup
        // would otherwise leave a one-off RPC error's `None` in place forever.
        if existing.appeal_window_close.is_none() {
            existing.appeal_window_close = slash.appeal_window_close;
        }
        return;
    }
    info!(
        slash_id = %slash.slash_id,
        offense_type = slash.offense_type,
        amount = %slash.amount,
        "slash detected against this operator; file `decdn appeal slash` within 30 days"
    );
    guard.push(slash);
    drop(guard);
    metrics.slash_detected();
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A stalled provider must not wedge the tick: the block read is bounded, and
    /// a timeout takes the same soft-degrade path as any other read failure.
    ///
    /// This drives the real `appeal_window_close` against a real provider, so it
    /// fails if the `timed` wrap is ever dropped from the call site — asserting
    /// `timed` in isolation would prove the mechanism but not this wiring.
    /// `start_paused` auto-advances to the deadline, so it costs no wall-clock.
    #[tokio::test(start_paused = true)]
    async fn hanging_block_read_degrades_to_no_deadline_rather_than_wedging() {
        use crate::chain_events::test_support::{bounded, hanging_provider};
        let provider = hanging_provider();
        assert_eq!(
            bounded(
                "appeal_window_close",
                appeal_window_close(&provider, Some(100))
            )
            .await,
            None,
            "a stalled get_block must degrade to an unknown appeal deadline"
        );
    }

    /// A `DetectedSlash` distinguished only by `slash_id` (the dedup key).
    fn slash(id: u64) -> DetectedSlash {
        DetectedSlash {
            slash_id: U256::from(id),
            offense_type: 0,
            amount: U256::from(42u64),
            evidence_hash: B256::repeat_byte(7),
            block_number: Some(100),
            appeal_window_close: None,
        }
    }

    /// A well-formed `Slashed` RPC log for `operator`, with the `removed`
    /// reorg flag under test control.
    fn slashed_log(operator: Address, removed: bool) -> alloy::rpc::types::Log {
        let event = SlashJudge::Slashed {
            slashId: U256::from(1u64),
            operator,
            offenseType: SlashJudge::OffenseType::Phantom,
            amount: U256::from(42u64),
            evidenceHash: B256::repeat_byte(7),
        };
        alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: Address::repeat_byte(0xAA),
                data: event.encode_log_data(),
            },
            removed,
            ..Default::default()
        }
    }

    /// Exact `<name> <value>` line match against the Prometheus text encoding,
    /// so `..._total 1` can't accidentally match `..._total 10`.
    fn has_metric_line(text: &str, name: &str, value: u64) -> bool {
        let needle = format!("{name} {value}");
        text.lines().any(|l| l.trim_end() == needle)
    }

    #[test]
    fn record_slash_dedupes_by_slash_id() {
        let store: SlashStore = Arc::new(RwLock::new(Vec::new()));
        let metrics = Arc::new(Metrics::new());

        // Backfill/live overlap re-delivers the same slashId: no double insert,
        // no double count.
        record_slash(&store, &metrics, slash(1));
        record_slash(&store, &metrics, slash(1));
        record_slash(&store, &metrics, slash(2));

        let guard = store.read().unwrap();
        assert_eq!(guard.len(), 2, "duplicate slashId must not double-insert");
        assert!(guard.iter().any(|s| s.slash_id == U256::from(1u64)));
        assert!(guard.iter().any(|s| s.slash_id == U256::from(2u64)));
        drop(guard);
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_slashes_detected_total", 2),
            "duplicate slashId must not double-count:\n{text}"
        );
    }

    #[test]
    fn decode_slashed_skips_removed_and_foreign_logs() {
        let operator = Address::repeat_byte(0x11);

        // The same log decodes when live but is skipped once reorged out
        // (`removed == true`), so a reorg can't record a phantom slash.
        assert!(decode_slashed(operator, &slashed_log(operator, false)).is_some());
        assert!(decode_slashed(operator, &slashed_log(operator, true)).is_none());
        // Defensive operator check: another operator's slash is never recorded
        // even if a provider ignores the `topic2` filter.
        assert!(
            decode_slashed(Address::repeat_byte(0x22), &slashed_log(operator, false)).is_none()
        );
    }

    /// The scan-floor policy (cursor rewind, deploy-floor clamp, head clamp) now
    /// lives on the resumable watcher's `resolve_head_window_start` /
    /// `resolve_persisted_start`; its unit tests live in
    /// `crate::chain_events::resumable_watcher`.
    #[test]
    fn appeal_window_covers_thirty_days() {
        // The boot lookback must cover the full 30-day appeal window even at the
        // fastest plausible block time. Pinned to the concrete expected count
        // (30 days of 250ms Arbitrum blocks) so an accidental unit slip in the
        // formula (secs vs ms) fails loudly rather than restating itself.
        assert_eq!(appeal_window_blocks(), 10_368_000);
        // At least one block per second of the window (block time < 1s).
        assert!(appeal_window_blocks() >= APPEAL_FILING_WINDOW_SECS);
    }
}
