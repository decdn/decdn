//! Daemon slash-detection watcher (#1032, G-NODE-05).
//!
//! Follows `SlashJudge.Slashed` and records every slash whose `operator` is
//! this node's own Ethereum address into a shared in-memory store, so the
//! operator can see the slash over the admin RPC (`admin_v1_slashes`) and file
//! `decdn appeal slash` within the 30-day window without watching the chain
//! directly. A counter metric (`decdn_slashes_detected_total`) mirrors it.
//!
//! Shape mirrors [`crate::payment_settlement`] but far leaner: slashes are rare
//! and the surface is read-only, so there is no redemption / settlement / write
//! path. On bootstrap the first watcher cycle backfills `Slashed` from genesis
//! (`get_logs [0, head]`) so a restart re-populates the in-memory store, then
//! streams forward with exponential-backoff resubscription. The store is deduped
//! by `slashId`, so the backfill/live overlap is harmless.

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

use crate::chain_events::watch_contract_events;
use crate::metrics::Metrics;

/// Nominal appeal filing window (ADR 028: 30 days from the slash timestamp).
/// The on-chain deadline can be extended by protocol-pause time; this is the
/// un-paused nominal close surfaced to the operator as a "file by" hint.
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
    /// Unix seconds after which the 30-day filing window nominally closes
    /// (block timestamp + 30 days), or `None` if the block read failed.
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
    /// Bootstrap the watcher: self-check the `SlashJudge` address by reading the
    /// head block, then spawn the backfill + live watcher task.
    ///
    /// # Errors
    ///
    /// Returns an error if the head-block read fails — an unreachable RPC or a
    /// bad `slash_judge_address` is fatal at bring-up, matching the settlement
    /// service's fail-fast posture.
    pub async fn bootstrap<P: Provider + Clone + 'static>(
        provider: P,
        slash_judge_addr: Address,
        self_address: Address,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let head_block = provider
            .get_block_number()
            .await
            .context("read head block for slash-watcher backfill at bootstrap")?;
        info!(
            %slash_judge_addr,
            %self_address,
            head_block,
            "slash-detection watcher bootstrap complete"
        );
        let store: SlashStore = Arc::new(RwLock::new(Vec::new()));
        let task = tokio::spawn(watcher_loop(
            provider,
            slash_judge_addr,
            self_address,
            head_block,
            Arc::clone(&store),
            metrics,
        ));
        Ok(Self {
            store,
            _task: AbortOnDrop(task),
        })
    }

    /// A clone of the shared detected-slash store for the admin surface.
    #[must_use]
    pub fn store(&self) -> SlashStore {
        Arc::clone(&self.store)
    }
}

/// Background watcher: backfill `Slashed` from genesis on the first cycle, then
/// stream forward, resubscribing with exponential backoff on stream error.
async fn watcher_loop<P: Provider + Clone>(
    provider: P,
    slash_judge_addr: Address,
    self_address: Address,
    backfill_to: u64,
    store: SlashStore,
    metrics: Arc<Metrics>,
) {
    // Backfill from genesis on the first cycle. `Some` until it succeeds, so a
    // `get_logs` failure retries on the next resubscription; once `None`, later
    // resubscriptions skip it (the in-memory store already holds prior slashes).
    let mut backfill = Some(backfill_to);
    let mut backoff = WATCHER_INITIAL_BACKOFF;
    loop {
        match run_once(
            &provider,
            slash_judge_addr,
            self_address,
            &mut backfill,
            &store,
            &metrics,
        )
        .await
        {
            Ok(()) => {
                debug!("slash watcher stream ended cleanly; resubscribing");
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

/// One watcher cycle: install the live filter, run the pending genesis backfill,
/// then drain the live stream until it ends or a log fails to decode.
async fn run_once<P: Provider + Clone>(
    provider: &P,
    slash_judge_addr: Address,
    self_address: Address,
    backfill: &mut Option<u64>,
    store: &SlashStore,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    // Install the live filter before the backfill so any slash emitted during
    // the backfill RPC buffers in the stream rather than falling into a gap.
    let mut events = watch_contract_events(
        provider,
        slash_judge_addr,
        [SlashJudge::Slashed::SIGNATURE_HASH],
    )
    .await
    .context("watch SlashJudge events")?;

    if let Some(to) = *backfill {
        backfill_slashes(provider, slash_judge_addr, self_address, store, metrics, to).await?;
        *backfill = None;
    }

    while let Some(log) = events.next().await {
        let event =
            SlashJudge::Slashed::decode_log_data(&log.inner.data).context("decode Slashed")?;
        if event.operator != self_address {
            continue;
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
    Ok(())
}

/// Backfill `Slashed` for this operator over `[0, to]` via a single `get_logs`.
/// Sufficient for the initial network scale (short chains / tens of nodes); a
/// windowed scan against a long-lived L2 is a later refinement.
async fn backfill_slashes<P: Provider>(
    provider: &P,
    slash_judge_addr: Address,
    self_address: Address,
    store: &SlashStore,
    metrics: &Arc<Metrics>,
    to: u64,
) -> Result<()> {
    let filter = Filter::new()
        .address(slash_judge_addr)
        .event_signature(SlashJudge::Slashed::SIGNATURE_HASH)
        .from_block(0u64)
        .to_block(to);
    let logs = provider
        .get_logs(&filter)
        .await
        .context("get_logs Slashed backfill")?;
    for log in logs {
        let Ok(event) = SlashJudge::Slashed::decode_log_data(&log.inner.data) else {
            warn!("skipping undecodable Slashed log during backfill");
            continue;
        };
        if event.operator != self_address {
            continue;
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
    Ok(())
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
