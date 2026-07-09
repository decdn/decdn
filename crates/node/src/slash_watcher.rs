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
//!   so both the backfill and the live filter constrain on it — a node never
//!   downloads or decodes other operators' slashes.
//! - **Filter-first, then head.** Each cycle installs the live filter *before*
//!   reading the head block used as the backfill bound, so no block mined
//!   between the two falls into a gap (same ordering as the settlement watcher).
//! - **Cursor across resubscribes.** A last-scanned-block cursor is carried
//!   across every resubscribe (filter TTL expiry / RPC error), so a slash mined
//!   during an outage window is recovered by the next cycle's backfill rather
//!   than silently lost until a full restart. The first cycle seeds the cursor
//!   at the head (no genesis scan — an operator isn't slashed before its node
//!   first runs, and older slashes are past the appeal window anyway).
//! - **Bounded backfill.** The `[cursor, head]` range is walked in
//!   [`crate::payment_settlement::MAX_BACKFILL_BLOCK_SPAN`]-block windows (one
//!   `eth_getLogs` each) via the shared [`backfill_windows`], so a single call
//!   never exceeds a provider's range cap.
//!
//! The store is deduped by `slashId`, so the backfill/live overlap is harmless.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use decdn_incentive::slash_judge::SlashJudge;
use futures_util::StreamExt;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::chain_events::watch_filter;
use crate::metrics::Metrics;
use crate::payment_settlement::{MAX_BACKFILL_BLOCK_SPAN, backfill_windows};

/// Nominal appeal filing window (ADR 028: 30 days from the slash timestamp).
const APPEAL_FILING_WINDOW_SECS: u64 = 30 * 24 * 60 * 60;

/// Backoff bounds for the resubscribe loop (mirrors the settlement watcher).
const WATCHER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const WATCHER_MAX_BACKOFF: Duration = Duration::from_secs(30);

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

/// Abort the background task when the owning [`SlashWatcher`] is dropped, so a
/// runtime teardown doesn't leak the poller.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

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
    /// `get_logs`, subscribe) happens inside the retrying loop, so a transient
    /// bring-up failure retries with backoff rather than disabling detection for
    /// the daemon's lifetime.
    #[must_use]
    pub fn bootstrap<P: Provider + Clone + 'static>(
        provider: P,
        slash_judge_addr: Address,
        self_address: Address,
        metrics: Arc<Metrics>,
    ) -> Self {
        info!(%slash_judge_addr, %self_address, "slash-detection watcher started");
        let store: SlashStore = Arc::new(RwLock::new(Vec::new()));
        let task = tokio::spawn(watcher_loop(
            provider,
            slash_judge_addr,
            self_address,
            Arc::clone(&store),
            metrics,
        ));
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

/// Background watcher: install the operator-filtered `Slashed` stream, backfill
/// `[cursor, head]`, then drain the stream — resubscribing with exponential
/// backoff on error and re-arming the backfill from the retained cursor so an
/// outage window is recovered on the next cycle.
async fn watcher_loop<P: Provider + Clone>(
    provider: P,
    slash_judge_addr: Address,
    self_address: Address,
    store: SlashStore,
    metrics: Arc<Metrics>,
) {
    // Last-scanned-block cursor, carried across resubscribes. `None` until the
    // first cycle seeds it at the head (no genesis scan).
    let mut cursor: Option<u64> = None;
    let mut backoff = WATCHER_INITIAL_BACKOFF;
    loop {
        match run_once(
            &provider,
            slash_judge_addr,
            self_address,
            &mut cursor,
            &store,
            &metrics,
        )
        .await
        {
            Ok(()) => {
                // A "clean" end is often a provider-side filter TTL expiry
                // (common on polling RPCs). Pace the resubscribe by at least the
                // initial backoff so a stream that keeps ending immediately
                // can't spin into a zero-delay resubscribe storm.
                debug!("slash watcher stream ended cleanly; resubscribing after backoff");
                tokio::time::sleep(WATCHER_INITIAL_BACKOFF).await;
                backoff = WATCHER_INITIAL_BACKOFF;
            }
            Err(err) => {
                warn!(
                    %err,
                    backoff_secs = backoff.as_secs(),
                    "slash watcher error; restarting after backoff"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(WATCHER_MAX_BACKOFF);
            }
        }
    }
}

/// One watcher cycle: install the operator-filtered live stream, read the head
/// as the backfill bound, backfill `[cursor, head]` in bounded windows, advance
/// the cursor, then drain the live stream until it ends or errors.
async fn run_once<P: Provider + Clone>(
    provider: &P,
    slash_judge_addr: Address,
    self_address: Address,
    cursor: &mut Option<u64>,
    store: &SlashStore,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    // Install the live filter first so any slash mined between the head read
    // below and the filter install still buffers in the stream (the backfill
    // covers up to head; the overlap is deduped by `slashId`).
    let mut events = watch_filter(provider, operator_filter(slash_judge_addr, self_address))
        .await
        .context("watch SlashJudge Slashed for operator")?;

    let head = provider
        .get_block_number()
        .await
        .context("read head block for slash backfill bound")?;
    // First cycle seeds the cursor at head (scan only the head block — no
    // genesis history). Later cycles resume from the cursor so an outage
    // `[cursor, head]` window is recovered.
    let from = cursor.unwrap_or(head).min(head);
    for (start, end) in backfill_windows(from, head, MAX_BACKFILL_BLOCK_SPAN) {
        let filter = operator_filter(slash_judge_addr, self_address)
            .from_block(start)
            .to_block(end);
        let logs = provider
            .get_logs(&filter)
            .await
            .with_context(|| format!("get_logs Slashed backfill [{start}, {end}]"))?;
        for log in logs {
            record_log(provider, self_address, store, metrics, &log).await;
        }
    }
    // Persist progress: the next resubscribe backfills from here, covering any
    // gap. `head + 1` never regresses (head only grows).
    *cursor = Some(head + 1);

    while let Some(log) = events.next().await {
        record_log(provider, self_address, store, metrics, &log).await;
    }
    Ok(())
}

/// The address + `Slashed`-signature + `topic2 == operator` filter shared by the
/// live subscription and every backfill window, so the RPC only ever returns
/// this operator's slashes.
fn operator_filter(slash_judge_addr: Address, self_address: Address) -> Filter {
    Filter::new()
        .address(slash_judge_addr)
        .event_signature(SlashJudge::Slashed::SIGNATURE_HASH)
        .topic2(self_address.into_word())
}

/// Decode one `Slashed` log and record it. Skips reorged-out logs
/// (`removed == true`, re-delivered by `eth_getFilterChanges` per the JSON-RPC
/// spec) and undecodable logs, so neither tears down the cycle nor records a
/// phantom slash. The `topic2` filter already constrains to this operator, but
/// a defensive operator check guards against a provider that ignores the topic.
async fn record_log<P: Provider>(
    provider: &P,
    self_address: Address,
    store: &SlashStore,
    metrics: &Arc<Metrics>,
    log: &alloy::rpc::types::Log,
) {
    if log.removed {
        debug!("skipping reorged-out (removed) Slashed log");
        return;
    }
    let Ok(event) = SlashJudge::Slashed::decode_log_data(&log.inner.data) else {
        warn!("skipping undecodable Slashed log");
        return;
    };
    if event.operator != self_address {
        return;
    }
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
async fn appeal_window_close<P: Provider>(provider: &P, block_number: Option<u64>) -> Option<u64> {
    let block_number = block_number?;
    match provider
        .get_block(alloy::eips::BlockId::from(block_number))
        .await
    {
        Ok(Some(block)) => Some(block.header.timestamp + APPEAL_FILING_WINDOW_SECS),
        Ok(None) => None,
        Err(err) => {
            warn!(%err, block_number, "failed to read slash block timestamp for appeal window");
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
    if guard.iter().any(|s| s.slash_id == slash.slash_id) {
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
