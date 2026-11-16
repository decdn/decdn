//! Daemon slash detection (#1032, G-NODE-05).
//!
//! Surfaces every still-appealable slash minted against this node's own operator
//! so the operator can see it over the admin RPC (`admin_v1_slashes`) and file
//! `decdn appeal slash` within the 30-day window without watching the chain
//! directly. A counter metric (`decdn_slashes_detected_total`) mirrors it.
//!
//! The store is read from the authoritative `CapacityBond.operatorSlash*`
//! enumeration (ADR 019) by `eth_call`, never from an event log. Walking the
//! append-only index backwards and stopping at the first closed record reads
//! only the still-appealable tail. Offense and evidence come straight off the
//! record; the enforced deadline is the record's base `appealWindowClose` plus
//! the global `pausedTotal`, all read at one pinned block.
//!
//! The node re-reads every `SLASH_REFRESH_INTERVAL`. A re-read starts with
//! `operatorSlashCount(self)`: when the count has not moved and no slash is
//! still appealable, that one read is the whole refresh. A transient boot
//! failure retries on the shared boot budget; a failed re-read keeps the current
//! set and counts `decdn_slash_resync_failures_total`.

use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use anyhow::{Context, Result};
use decdn_incentive::capacity_bond::CapacityBond;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use decdn_common::redact::sanitize_err_chain;

use crate::chain_events::boot_retry::BootRetry;
use crate::chain_events::refresh::run_refresh_loop;
use crate::chain_events::snapshot::snapshot_block;
use crate::chain_events::timed;
use crate::dht::capacity_bond_registry::RefreshTask;
use crate::metrics::Metrics;

/// How often this operator's slashes are re-read. Fifteen minutes is a small
/// slice of the 30-day appeal window, at one `operatorSlashCount` read per
/// interval for an operator with no open slash.
const SLASH_REFRESH_INTERVAL: Duration = Duration::from_mins(15);

/// One slash detected against this node's operator.
#[derive(Debug, Clone)]
pub struct DetectedSlash {
    /// Globally-monotonic on-chain `slashId` (`CapacityBond.slash`).
    pub slash_id: U256,
    /// Offense taxonomy index (ADR 014): 0=RateManipulation, 1=Blacklist.
    pub offense_type: u8,
    /// Bond amount slashed, in TOKEN base units.
    pub amount: U256,
    /// `keccak256` evidence digest, from the `CapacityBond` slash record.
    pub evidence_hash: B256,
    /// **Effective** appeal-window close the contract enforces: the record's base
    /// `appealWindowClose` plus the global `pausedTotal` at the read, so
    /// protocol-pause extensions are reflected.
    pub appeal_window_close: Option<u64>,
}

/// Shared, in-memory detected-slash store, newest first as the enumeration
/// walks it, deduped by `slashId`. Cloned into [`crate::admin::AdminState`] for
/// the read-only `admin_v1_slashes` surface.
pub type SlashStore = Arc<RwLock<Vec<DetectedSlash>>>;

/// The authoritative `CapacityBond.getSlashRecord` fields the enumeration
/// consumes, decoupled from the ABI struct so the read path is unit-testable
/// with a scripted stub.
#[derive(Debug, Clone)]
struct SlashRecordView {
    /// Offense taxonomy index (ADR 014).
    offense_type: u8,
    /// Bond amount slashed, in TOKEN base units.
    amount: U256,
    /// `keccak256` evidence digest.
    evidence_hash: B256,
    /// **Base** appeal-window close (`slashedAt + 30d`), fixed at mint. The
    /// deadline the contract enforces adds the global `pausedTotal`; callers must
    /// fold that in (see [`bootstrap_slashes`]).
    appeal_window_close: u64,
}

/// The chain reads the enumeration performs, behind a trait so the paging and
/// still-appealable filter are unit-testable without a provider.
///
/// RPITIT with an explicit `+ Send`, because the refresh runs on a spawned
/// task.
trait SlashChainReads: Send + Sync {
    /// The block an enumeration pins its reads to (`snapshot::snapshot_block`).
    fn snapshot_block(&self) -> impl Future<Output = Result<u64>> + Send;
    /// How many slashes have ever been minted against `operator`
    /// (`operatorSlashCount`).
    fn operator_slash_count(
        &self,
        operator: Address,
        at: BlockId,
    ) -> impl Future<Output = Result<U256>> + Send;
    /// The `slashId` at `index` in `operator`'s append-only slash list
    /// (`operatorSlashIdAt`).
    fn operator_slash_id_at(
        &self,
        operator: Address,
        index: U256,
        at: BlockId,
    ) -> impl Future<Output = Result<U256>> + Send;
    /// The authoritative record for `slashId` (`getSlashRecord`).
    fn get_slash_record(
        &self,
        slash_id: U256,
        at: BlockId,
    ) -> impl Future<Output = Result<SlashRecordView>> + Send;
    /// The global `pausedTotal` offset added to every record's base
    /// `appealWindowClose` to get the deadline the contract enforces.
    fn paused_total(&self, at: BlockId) -> impl Future<Output = Result<u64>> + Send;
}

/// Enumerate this operator's still-appealable slashes from the authoritative
/// `CapacityBond` records (ADR 019).
///
/// Walks the append-only index backwards from newest and stops at the first
/// record whose appeal window has already closed against `now_secs`: earlier
/// records were minted earlier (`slashedAt` order), so their windows closed too.
/// The list is append-only with stable indices, so — unlike the swap-and-pop
/// enumeration views — no count re-check is required; a slash minted mid-scan
/// lands at an index past the old count and the next refresh reads it.
///
/// Every read runs at `at`, so a load-balanced provider cannot answer the count
/// from one backend and the index or record from a lagging one.
async fn bootstrap_slashes<R: SlashChainReads>(
    reads: &R,
    operator: Address,
    now_secs: u64,
    at: BlockId,
) -> Result<(U256, Vec<DetectedSlash>)> {
    let count = reads.operator_slash_count(operator, at).await?;
    // The enforced deadline is the record's base `appealWindowClose` plus this
    // global offset; read once at the same block. It only ever grows and
    // applies uniformly, so base-close ordering equals effective-close ordering
    // and the backward-walk early-stop below stays valid.
    let paused_total = reads.paused_total(at).await?;
    let mut out = Vec::new();
    let mut index = count;
    while index > U256::ZERO {
        index -= U256::from(1u8);
        let slash_id = reads.operator_slash_id_at(operator, index, at).await?;
        let record = reads.get_slash_record(slash_id, at).await?;
        // The deadline the contract enforces (`SlashEscrowLib`): base + pause.
        let effective_close = record.appeal_window_close.saturating_add(paused_total);
        if effective_close <= now_secs {
            // Appended in `slashedAt` order, so every older record is closed too.
            break;
        }
        out.push(DetectedSlash {
            slash_id,
            offense_type: record.offense_type,
            amount: record.amount,
            evidence_hash: record.evidence_hash,
            appeal_window_close: Some(effective_close),
        });
    }
    Ok((count, out))
}

/// Production [`SlashChainReads`] over the live `CapacityBond` contract.
#[derive(Clone)]
struct ContractReads<P: Provider + Clone> {
    bond: CapacityBond::CapacityBondInstance<P>,
}

impl<P: Provider + Clone> SlashChainReads for ContractReads<P> {
    async fn snapshot_block(&self) -> Result<u64> {
        snapshot_block(self.bond.provider(), *self.bond.address()).await
    }

    async fn operator_slash_count(&self, operator: Address, at: BlockId) -> Result<U256> {
        timed(
            None,
            "operatorSlashCount",
            self.bond.operatorSlashCount(operator).block(at).call(),
        )
        .await
        .with_context(|| format!("operatorSlashCount({operator})"))
    }

    async fn operator_slash_id_at(
        &self,
        operator: Address,
        index: U256,
        at: BlockId,
    ) -> Result<U256> {
        timed(
            None,
            "operatorSlashIdAt",
            self.bond
                .operatorSlashIdAt(operator, index)
                .block(at)
                .call(),
        )
        .await
        .with_context(|| format!("operatorSlashIdAt({operator}, {index})"))
    }

    async fn get_slash_record(&self, slash_id: U256, at: BlockId) -> Result<SlashRecordView> {
        let record = timed(
            None,
            "getSlashRecord",
            self.bond.getSlashRecord(slash_id).block(at).call(),
        )
        .await
        .with_context(|| format!("getSlashRecord({slash_id})"))?;
        Ok(SlashRecordView {
            offense_type: record.offenseType,
            amount: record.slashAmount,
            evidence_hash: record.evidenceHash,
            appeal_window_close: record.appealWindowClose,
        })
    }

    async fn paused_total(&self, at: BlockId) -> Result<u64> {
        timed(
            None,
            "pausedTotal",
            self.bond.pausedTotal().block(at).call(),
        )
        .await
        .context("pausedTotal")
    }
}

/// Current wall-clock UNIX seconds, the clock the still-appealable filter tests
/// each record's `appealWindowClose` against. Wall time tracks chain time within
/// seconds on an L2; the filter only decides whether a slash is *shown*, so a
/// boundary-second skew is immaterial. A pre-epoch clock degrades to `0`, which
/// shows every slash.
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The store, the count it was read at, and the reads that refresh it.
struct SlashSync<R> {
    reads: R,
    self_address: Address,
    store: SlashStore,
    metrics: Arc<Metrics>,
    /// `operatorSlashCount` at the last successful read.
    count: std::sync::Mutex<U256>,
}

impl<R: SlashChainReads> SlashSync<R> {
    /// The count at the last successful read.
    fn count(&self) -> U256 {
        *self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// One refresh. A count that has not moved, with no slash still
    /// appealable, ends at that one read; otherwise the enumeration re-runs at
    /// a pinned block and replaces the store, which also prunes slashes whose
    /// appeal window has closed.
    async fn refresh(&self) {
        let unchanged = self.store_is_empty()
            && match self
                .reads
                .operator_slash_count(self.self_address, BlockId::latest())
                .await
            {
                Ok(count) => count == self.count(),
                Err(err) => {
                    self.metrics.slash_resync_failure();
                    warn!(
                        error = %sanitize_err_chain(&err),
                        "slash count read failed; keeping the current detected-slash set"
                    );
                    return;
                }
            };
        if unchanged {
            return;
        }
        let read = async {
            let at = self.reads.snapshot_block().await?;
            bootstrap_slashes(
                &self.reads,
                self.self_address,
                unix_now(),
                BlockId::number(at),
            )
            .await
        };
        match read.await {
            Ok((count, fresh)) => {
                *self
                    .count
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = count;
                replace_store(&self.store, &self.metrics, fresh);
            }
            Err(err) => {
                self.metrics.slash_resync_failure();
                warn!(
                    error = %sanitize_err_chain(&err),
                    "slash re-read failed; keeping the current detected-slash set"
                );
            }
        }
    }

    fn store_is_empty(&self) -> bool {
        self.store
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }
}

/// Enumerate this operator's still-appealable slashes from `CapacityBond`, then
/// return the shared detected-slash store (for the admin surface) and the
/// refresh loop that keeps it current until `shutdown` is cancelled.
///
/// The enumeration retries transient failures on `boot`'s budget; a
/// deterministic fault or an exhausted budget fails the bootstrap.
///
/// # Errors
///
/// The boot enumeration's error once the boot budget is spent, or at once on a
/// deterministic fault.
pub async fn bootstrap<P: Provider + Clone + 'static>(
    provider: P,
    capacity_bond_addr: Address,
    self_address: Address,
    metrics: Arc<Metrics>,
    boot: &BootRetry,
    shutdown: CancellationToken,
) -> Result<(SlashStore, RefreshTask)> {
    let reads = ContractReads {
        bond: CapacityBond::new(capacity_bond_addr, provider),
    };
    let (snapshot_block, (count, initial)) = boot
        .run("slash enumeration snapshot", || async {
            let snapshot_block = reads
                .snapshot_block()
                .await
                .context("read the slash enumeration snapshot block")?;
            let read = bootstrap_slashes(
                &reads,
                self_address,
                unix_now(),
                BlockId::number(snapshot_block),
            )
            .await
            .with_context(|| {
                format!("enumerate operator slashes from CapacityBond at {capacity_bond_addr}")
            })?;
            Ok((snapshot_block, read))
        })
        .await?;
    info!(
        slash_count = initial.len(),
        snapshot_block, %self_address, "slash enumeration complete"
    );
    let store: SlashStore = Arc::new(RwLock::new(Vec::new()));
    replace_store(&store, &metrics, initial);
    let sync = Arc::new(SlashSync {
        reads,
        self_address,
        store: Arc::clone(&store),
        metrics,
        count: std::sync::Mutex::new(count),
    });
    let task: RefreshTask = Box::pin(run_refresh_loop(
        SLASH_REFRESH_INTERVAL,
        SLASH_REFRESH_INTERVAL,
        None,
        shutdown,
        move || {
            let sync = Arc::clone(&sync);
            async move { sync.refresh().await }
        },
    ));
    Ok((store, task))
}

/// Replace the store with `fresh`, logging and counting each slash the store
/// did not hold yet.
fn replace_store(store: &SlashStore, metrics: &Arc<Metrics>, fresh: Vec<DetectedSlash>) {
    let mut guard = store
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for slash in &fresh {
        if guard.iter().any(|s| s.slash_id == slash.slash_id) {
            continue;
        }
        info!(
            slash_id = %slash.slash_id,
            offense_type = slash.offense_type,
            amount = %slash.amount,
            "slash detected against this operator; file `decdn appeal slash` within 30 days"
        );
        metrics.slash_detected();
    }
    let count = fresh.len();
    *guard = fresh;
    drop(guard);
    debug!(slash_count = count, "detected-slash set read from chain");
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests;
