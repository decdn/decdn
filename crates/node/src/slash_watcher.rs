//! Daemon slash-detection watcher (#1032, G-NODE-05).
//!
//! Surfaces every still-appealable slash minted against this node's own operator
//! so the operator can see it over the admin RPC (`admin_v1_slashes`) and file
//! `decdn appeal slash` within the 30-day window without watching the chain
//! directly. A counter metric (`decdn_slashes_detected_total`) mirrors it.
//!
//! Shape follows the `CapacityBond` registry watcher
//! ([`crate::dht::capacity_bond_registry`]) — **enumerate at head, follow the
//! tail, re-read periodically as the backstop** — rather than replaying logs:
//!
//! - **Enumerate at boot.** The in-memory store is rebuilt on every start from
//!   the authoritative `CapacityBond.operatorSlash*` enumeration (ADR 019), not
//!   by re-scanning the `Slashed` log tail from a block floor (#1108). Walking
//!   the append-only index backwards and stopping at the first closed record
//!   reads only the still-appealable tail. Offense and evidence come straight off
//!   the record; the enforced deadline is the record's base `appealWindowClose`
//!   plus the global `pausedTotal`, all read at the snapshot block. A transient
//!   RPC failure retries on the shared boot budget, like the registry bootstrap
//!   on the same contract; a deterministic fault or an exhausted budget fails
//!   startup.
//! - **Follow `SlashRecorded` at head.** The route follows `SlashRecorded` on the
//!   shared multiplexed poller, seeded at the enumeration snapshot block, so there
//!   is no historical scan on any boot. `SlashRecorded` indexes `operator` as
//!   `topic2`, but the merged poller filter cannot scope `topic2`, so the route
//!   receives every operator's `SlashRecorded` and `decode_recorded`'s
//!   `operator == self` guard filters out the rest. The event carries only the
//!   `slashId`; the offense/evidence/deadline are point-read from
//!   `getSlashRecord`.
//! - **Resync as the backstop.** A missed tail event (reorg at the unstable tip,
//!   or a tick lost to RPC backoff) never self-heals otherwise, so the store is
//!   re-enumerated every `SLASH_RESYNC_INTERVAL`; this also prunes slashes
//!   whose appeal window has since closed. The store is deduped by `slashId`, so
//!   the enumeration/tail overlap is harmless.

use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use decdn_incentive::capacity_bond::CapacityBond;
use tracing::{debug, info, warn};

use decdn_common::redact::sanitize_err_chain;

use crate::chain_events::boot_retry::BootRetry;
use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{CursorStart, LogSink, clear_cadence_on_recovery};
use crate::chain_events::shared_head::{HeadSource, snapshot_block};
use crate::chain_events::timed;
use crate::metrics::{Metrics, metric_hook};

/// How often the store is re-enumerated from `CapacityBond` as a drift backstop.
///
/// Mirrors the `CapacityBond` registry watcher's cadence and rationale: a missed
/// live-tail event never self-heals short of a restart, and one enumeration of a
/// single operator's (tiny) slash list every quarter hour is negligible.
/// Deliberately a constant, not a config knob — a correctness backstop, not a
/// tuning surface.
const SLASH_RESYNC_INTERVAL: Duration = Duration::from_mins(15);

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
    /// Block the `SlashRecorded` log was mined in — `Some` for a slash seen live
    /// on the tail, `None` for one recovered by the boot/resync enumeration
    /// (which reads records, not logs).
    pub block_number: Option<u64>,
    /// **Effective** appeal-window close the contract enforces: the record's base
    /// `appealWindowClose` plus the global `pausedTotal` snapshot, so protocol-pause
    /// extensions are reflected. Always `Some` because the deadline comes off the
    /// record, not a best-effort block read.
    pub appeal_window_close: Option<u64>,
}

/// Shared, in-memory detected-slash store: appended in detection order and
/// deduped by `slashId`. Cloned into [`crate::admin::AdminState`] for the
/// read-only `admin_v1_slashes` surface.
pub type SlashStore = Arc<RwLock<Vec<DetectedSlash>>>;

/// The authoritative `CapacityBond.getSlashRecord` fields the enumeration and
/// live tail consume, decoupled from the ABI struct so the boot path is
/// unit-testable with a scripted stub.
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

/// The chain reads the boot enumeration and the live tail perform, behind a
/// trait so the paging and still-appealable filter are unit-testable without a
/// provider.
///
/// Spelled RPITIT with an explicit `+ Send` (rather than `async fn`, whose
/// futures carry no `Send` bound) because the periodic resync calls these from
/// inside the spawned watcher's `on_tick_complete`, whose future `tokio::spawn`
/// requires to be `Send`. Production monomorphizes to the alloy contract impl.
trait SlashChainReads: Send + Sync {
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
/// `CapacityBond` records — the boot path that replaces re-scanning the
/// `Slashed` log tail from a block floor on every start (#1108, ADR 019).
///
/// Walks the append-only index backwards from newest and stops at the first
/// record whose appeal window has already closed against `now_secs`: earlier
/// records were minted earlier (`slashedAt` order), so their windows closed too.
/// The list is append-only with stable indices, so — unlike the swap-and-pop
/// enumeration views — no count re-check is required; a slash minted mid-scan
/// lands at an index past the old count (unseen here) and is picked up by the
/// live `SlashRecorded` tail instead.
///
/// Every read runs at `at`. The boot enumeration pins it to the snapshot block,
/// so a load-balanced provider cannot answer the count from one backend and the
/// index or record from a lagging one: a backend behind `at` answers "header not
/// found", which retries, rather than reverting on an index it has not seen,
/// which is deterministic and fails boot. The periodic resync reads `latest`; a
/// failed resync keeps the current set.
async fn bootstrap_slashes<R: SlashChainReads>(
    reads: &R,
    operator: Address,
    now_secs: u64,
    at: BlockId,
) -> Result<Vec<DetectedSlash>> {
    let count = reads.operator_slash_count(operator, at).await?;
    // The enforced deadline is the record's base `appealWindowClose` plus this
    // global offset; read once at the same snapshot. It only ever grows and
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
            // No log is read on this path; the deadline is the pause-extended one
            // the contract enforces, not a block-timestamp derivation.
            block_number: None,
            appeal_window_close: Some(effective_close),
        });
    }
    Ok(out)
}

/// Production [`SlashChainReads`] over the live `CapacityBond` contract.
#[derive(Clone)]
struct ContractReads<P: Provider + Clone> {
    bond: CapacityBond::CapacityBondInstance<P>,
}

impl<P: Provider + Clone> SlashChainReads for ContractReads<P> {
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
/// boundary-second skew is immaterial (the authoritative deadline is stored
/// regardless). A pre-epoch clock degrades to `0`, which shows every slash.
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Enumerate this operator's still-appealable slashes from `CapacityBond`, then
/// return the shared detected-slash store (for the admin surface) and the
/// `SlashRecorded` [`Route`] the shared poller drives to keep it current.
///
/// The enumeration retries transient failures on `boot`'s budget, like the
/// `CapacityBond` registry bootstrap that runs on the same contract immediately
/// before this one; a deterministic fault or an exhausted budget fails the
/// bootstrap. Once running, a transient tail RPC blip retries with the shared
/// poller's backoff, and the periodic resync repairs any drift — so detection is
/// never disabled for the daemon's lifetime.
pub async fn bootstrap<P: Provider + Clone + 'static>(
    provider: P,
    capacity_bond_addr: Address,
    self_address: Address,
    head: Arc<dyn HeadSource>,
    metrics: Arc<Metrics>,
    boot: &BootRetry,
) -> Result<(SlashStore, Route)> {
    info!(%capacity_bond_addr, %self_address, "slash-detection watcher started");
    let reads = ContractReads {
        bond: CapacityBond::new(capacity_bond_addr, provider),
    };

    // Snapshot block BEFORE the enumeration, then pin every read and seed the tail
    // cursor there. The reverse order would lose a `SlashRecorded` landing between
    // enumeration and the head read — neither in the snapshot nor above the
    // cursor. The block sits `SNAPSHOT_LAG_MARGIN_BLOCKS` below the reported head
    // so every upstream behind a load-balanced RPC can serve the pinned reads;
    // the tail replays the margin, and re-applying a snapshot event is a deduped
    // no-op, so overlap is safe but a gap is not.
    let (snapshot_block, initial) = boot
        .run("slash enumeration snapshot", || async {
            let snapshot_block = snapshot_block(&*head)
                .await
                .context("read the slash enumeration snapshot block")?;
            let initial = bootstrap_slashes(
                &reads,
                self_address,
                unix_now(),
                BlockId::number(snapshot_block),
            )
            .await
            .with_context(|| {
                format!("enumerate operator slashes from CapacityBond at {capacity_bond_addr}")
            })?;
            Ok((snapshot_block, initial))
        })
        .await?;
    info!(
        slash_count = initial.len(),
        snapshot_block, %self_address, "slash enumeration complete"
    );

    // Seed through `record_slash` so the boot set is logged, counted, and deduped
    // on the same path the live tail uses.
    let store: SlashStore = Arc::new(RwLock::new(Vec::new()));
    for slash in initial {
        record_slash(&store, &metrics, slash);
    }

    let sink = SlashSink {
        reads,
        self_address,
        store: Arc::clone(&store),
        metrics: Arc::clone(&metrics),
        resync_interval: SLASH_RESYNC_INTERVAL,
        // The bootstrap enumeration just ran, so the first backstop resync is due
        // one interval from now rather than on the first tick.
        last_resync: Some(Instant::now()),
    };
    let route = Route {
        addresses: vec![capacity_bond_addr],
        topic0s: slash_route_topic0s(),
        start: slash_cursor_start(snapshot_block),
        sink: SinkSource::Ready(Box::new(sink)),
        label: "slash",
        on_established: Some(metric_hook(
            &metrics,
            Metrics::slash_watcher_cycle_established,
        )),
        on_backoff: Some(metric_hook(
            &metrics,
            Metrics::slash_watcher_backoff_started,
        )),
        on_tick_success: Some(metric_hook(&metrics, Metrics::slash_watcher_tick)),
        on_task_panic: Some(metric_hook(&metrics, Metrics::slash_watcher_task_panicked)),
    };
    Ok((store, route))
}

/// The slash route's demux key: `SlashRecorded` only.
///
/// `SlashRecorded` indexes `operator` as `topic2`, but the merged poller filter
/// cannot scope `topic2` to only this event (it ORs many events across
/// contracts), so this route receives EVERY operator's `SlashRecorded`.
/// [`decode_recorded`]'s `operator == self` guard filters them, which is what
/// makes dropping the wire-level `topic2` safe. Split out from [`bootstrap`] so
/// the exact topic0 set is unit-testable without a provider.
fn slash_route_topic0s() -> Vec<B256> {
    vec![CapacityBond::SlashRecorded::SIGNATURE_HASH]
}

/// The slash route's cursor start: seed the tail at the enumeration snapshot
/// head. The enumeration rebuilt the store from all of history, so there is no
/// durable cursor and no historical scan. Split out from [`bootstrap`] so the
/// cursor shape is unit-testable without a provider.
const fn slash_cursor_start(snapshot_block: u64) -> CursorStart {
    CursorStart::Seeded { at: snapshot_block }
}

/// Applies operator-filtered `SlashRecorded` logs to the in-memory store and
/// re-enumerates it periodically as the drift backstop.
///
/// `apply` never returns `Err`: [`record_recorded_log`] decodes, skips a
/// reorged-out/undecodable/foreign log, soft-skips on a failed record read (the
/// resync recovers it), and dedupes by `slashId`, so a bad log neither tears
/// down the poll cycle nor hot-loops.
struct SlashSink<R: SlashChainReads> {
    reads: R,
    self_address: Address,
    store: SlashStore,
    metrics: Arc<Metrics>,
    resync_interval: Duration,
    last_resync: Option<Instant>,
}

impl<R: SlashChainReads> LogSink for SlashSink<R> {
    async fn apply(&mut self, log: Log) -> Result<()> {
        record_recorded_log(
            &self.reads,
            self.self_address,
            &self.store,
            &self.metrics,
            &log,
        )
        .await;
        Ok(())
    }

    /// Re-enumerate the operator's still-appealable slashes on the resync cadence
    /// and replace the store wholesale (build-then-swap: a failed read keeps the
    /// current set). This repairs any tail event lost to a reorg or RPC backoff
    /// and prunes slashes whose appeal window has since closed. Returns `Ok` on
    /// failure — the event tail is the primary path and is still working.
    async fn on_tick_complete(&mut self) -> Result<()> {
        let now = Instant::now();
        if self
            .last_resync
            .is_some_and(|last| now.duration_since(last) < self.resync_interval)
        {
            return Ok(());
        }
        // Stamp before the call, not only on success, so a persistently failing
        // read retries on the resync cadence rather than on every watcher tick.
        self.last_resync = Some(now);

        let fresh = match bootstrap_slashes(
            &self.reads,
            self.self_address,
            unix_now(),
            BlockId::latest(),
        )
        .await
        {
            Ok(fresh) => fresh,
            Err(err) => {
                warn!(
                    error = %sanitize_err_chain(&err),
                    "slash resync failed; keeping the current detected-slash set"
                );
                return Ok(());
            }
        };
        let count = fresh.len();
        let mut guard = self
            .store
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = fresh;
        drop(guard);
        debug!(
            slash_count = count,
            "detected-slash set resynced from chain"
        );
        Ok(())
    }

    /// Force the backstop re-read on the tick the watcher recovers, rather than
    /// waiting out the cadence.
    ///
    /// [`Self::on_tick_complete`] stamps its clock before the read, so an outage
    /// that spans a due tick defers the re-read a further `resync_interval` —
    /// and the reconcile does not run at all while the route is errored. A slash
    /// recorded during the outage would otherwise sit unseen for up to that long,
    /// which eats into the appeal window.
    fn on_recovered(&mut self) {
        clear_cadence_on_recovery(&mut self.last_resync, self.resync_interval);
    }
}

/// Decode one `SlashRecorded` log to its `slashId`, or `None` for a log that
/// must not be recorded: reorged-out (`removed == true` — not expected from
/// `eth_getLogs`, so this is defensive against a nonconforming provider),
/// undecodable, or another operator's. The merged poller filter cannot scope
/// `topic2` to this operator, so this route receives every operator's
/// `SlashRecorded` and the `operator == self` check below is what filters them.
/// Pure (no provider) so the skip policy is unit-testable.
fn decode_recorded(self_address: Address, log: &alloy::rpc::types::Log) -> Option<U256> {
    if log.removed {
        debug!("skipping reorged-out (removed) SlashRecorded log");
        return None;
    }
    let Ok(event) = CapacityBond::SlashRecorded::decode_log_data(&log.inner.data) else {
        warn!(
            block_number = ?log.block_number,
            tx = ?log.transaction_hash,
            "skipping undecodable SlashRecorded log"
        );
        return None;
    };
    if event.operator != self_address {
        return None;
    }
    Some(event.slashId)
}

/// Decode one `SlashRecorded` log, point-read its authoritative record, and
/// record it. Skipped logs (see [`decode_recorded`]) and a failed record read
/// neither tear down the cycle nor record a phantom slash — a record read that
/// fails is recovered by the next resync.
async fn record_recorded_log<R: SlashChainReads>(
    reads: &R,
    self_address: Address,
    store: &SlashStore,
    metrics: &Arc<Metrics>,
    log: &alloy::rpc::types::Log,
) {
    let Some(slash_id) = decode_recorded(self_address, log) else {
        return;
    };
    // Read the record and the global pause offset together; a failure on either
    // soft-skips (the resync recovers it) rather than recording a wrong deadline.
    let record = match reads.get_slash_record(slash_id, BlockId::latest()).await {
        Ok(record) => record,
        Err(err) => {
            warn!(
                error = %sanitize_err_chain(&err),
                slash_id = %slash_id,
                "failed to read slash record for a live SlashRecorded log; the resync will recover it"
            );
            return;
        }
    };
    let paused_total = match reads.paused_total(BlockId::latest()).await {
        Ok(paused_total) => paused_total,
        Err(err) => {
            warn!(
                error = %sanitize_err_chain(&err),
                slash_id = %slash_id,
                "failed to read pausedTotal for a live SlashRecorded log; the resync will recover it"
            );
            return;
        }
    };
    record_slash(
        store,
        metrics,
        DetectedSlash {
            slash_id,
            offense_type: record.offense_type,
            amount: record.amount,
            evidence_hash: record.evidence_hash,
            block_number: log.block_number,
            // The deadline the contract enforces: base close + global pause offset.
            appeal_window_close: Some(record.appeal_window_close.saturating_add(paused_total)),
        },
    );
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
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// A transient provider error during the boot enumeration is retried on the
    /// boot budget, not fatal.
    #[tokio::test(start_paused = true)]
    async fn a_transient_boot_enumeration_error_is_retried() {
        use crate::chain_events::boot_retry::{BOOT_CHAIN_RETRY_BUDGET, BootRetry};
        use crate::chain_events::shared_head::SharedHead;
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;
        use alloy::sol_types::SolValue;

        let head_asserter = Asserter::new();
        head_asserter.push_success(&alloy::primitives::U64::from(100));
        let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(
            ProviderBuilder::new().connect_mocked_client(head_asserter),
            Duration::from_hours(1),
            None,
        ));
        let asserter = Asserter::new();
        asserter.push_failure(
            serde_json::from_value(serde_json::json!({
                "code": 19,
                "message": "Temporary internal error. Please retry",
            }))
            .unwrap(),
        );
        // The retry: no slashes, then the pause offset.
        for _ in 0..2 {
            asserter.push_success(&alloy::primitives::Bytes::from(U256::ZERO.abi_encode()));
        }
        let metrics = Arc::new(Metrics::new());

        let (store, _route) = bootstrap(
            ProviderBuilder::new().connect_mocked_client(asserter.clone()),
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            head,
            Arc::clone(&metrics),
            &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
        )
        .await
        .unwrap();

        assert!(store.read().unwrap().is_empty());
        assert!(asserter.read_q().is_empty(), "both attempts ran");
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_chain_boot_read_retries_total 1"),
            "{text}"
        );
    }

    /// The slash route watches exactly `SlashRecorded` — no more, no fewer —
    /// and there is deliberately no `topic2` (operator) constraint on the route
    /// (see [`slash_route_topic0s`]'s doc for why filtering happens in
    /// [`decode_recorded`] instead).
    #[test]
    fn route_topic0s_is_exactly_slash_recorded() {
        assert_eq!(
            slash_route_topic0s(),
            vec![CapacityBond::SlashRecorded::SIGNATURE_HASH]
        );
    }

    /// The slash route seeds its cursor at the enumeration snapshot block, with
    /// no durable persistence (the store is rebuilt from enumeration each boot).
    #[test]
    fn cursor_start_seeds_at_snapshot_with_no_persistence() {
        let start = slash_cursor_start(12_345);
        assert_eq!(start.seed(), Some(12_345));
        assert!(
            matches!(start, CursorStart::Seeded { .. }),
            "must not carry a durable checkpoint"
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

    /// A well-formed `SlashRecorded` RPC log for `operator`, with the `removed`
    /// reorg flag under test control.
    fn recorded_log(operator: Address, removed: bool) -> alloy::rpc::types::Log {
        let event = CapacityBond::SlashRecorded {
            slashId: U256::from(1u64),
            operator,
            slashedAt: 0,
            slashAmount: U256::from(42u64),
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
    fn decode_recorded_skips_removed_and_foreign_logs() {
        let operator = Address::repeat_byte(0x11);

        // The same log decodes when live but is skipped once reorged out
        // (`removed == true`), so a reorg can't record a phantom slash.
        assert_eq!(
            decode_recorded(operator, &recorded_log(operator, false)),
            Some(U256::from(1u64))
        );
        assert!(decode_recorded(operator, &recorded_log(operator, true)).is_none());
        // Defensive operator check: another operator's slash is never recorded
        // even if a provider ignores the `topic2` filter.
        assert!(
            decode_recorded(Address::repeat_byte(0x22), &recorded_log(operator, false)).is_none()
        );
    }

    /// Scripted [`SlashChainReads`]: no provider, no chain. Slashes are supplied
    /// oldest-first (append order), matching `operatorSlashIdAt`'s stable indices.
    struct StubSlashReads {
        operator: Address,
        /// (slashId, record) in append order (index 0 = oldest).
        slashes: Vec<(U256, SlashRecordView)>,
        /// Global `CapacityBond.pausedTotal` the enumeration adds to each base close.
        paused_total: u64,
        /// slashIds whose record was point-read, for the early-stop assertion.
        reads: std::sync::Mutex<Vec<U256>>,
        /// The block every read ran at, for the pinned-read assertion.
        blocks: std::sync::Mutex<Vec<BlockId>>,
    }

    impl StubSlashReads {
        fn new(operator: Address, slashes: Vec<(U256, SlashRecordView)>) -> Self {
            Self {
                operator,
                slashes,
                paused_total: 0,
                reads: std::sync::Mutex::new(Vec::new()),
                blocks: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn blocks_read(&self) -> Vec<BlockId> {
            self.blocks.lock().unwrap().clone()
        }

        fn with_paused_total(mut self, paused_total: u64) -> Self {
            self.paused_total = paused_total;
            self
        }

        fn records_read(&self) -> Vec<U256> {
            self.reads.lock().unwrap().clone()
        }
    }

    impl SlashChainReads for StubSlashReads {
        async fn operator_slash_count(&self, operator: Address, at: BlockId) -> Result<U256> {
            self.blocks.lock().unwrap().push(at);
            assert_eq!(operator, self.operator, "unexpected operator");
            Ok(U256::from(self.slashes.len()))
        }

        async fn operator_slash_id_at(
            &self,
            operator: Address,
            index: U256,
            at: BlockId,
        ) -> Result<U256> {
            self.blocks.lock().unwrap().push(at);
            assert_eq!(operator, self.operator, "unexpected operator");
            let i: usize = index.to();
            self.slashes
                .get(i)
                .map(|(id, _)| *id)
                .ok_or_else(|| anyhow::anyhow!("SlashIndexOutOfRange"))
        }

        async fn get_slash_record(&self, slash_id: U256, at: BlockId) -> Result<SlashRecordView> {
            self.blocks.lock().unwrap().push(at);
            self.reads.lock().unwrap().push(slash_id);
            self.slashes
                .iter()
                .find(|(id, _)| *id == slash_id)
                .map(|(_, rec)| rec.clone())
                .ok_or_else(|| anyhow::anyhow!("no such slash"))
        }

        async fn paused_total(&self, at: BlockId) -> Result<u64> {
            self.blocks.lock().unwrap().push(at);
            Ok(self.paused_total)
        }
    }

    fn record(
        offense_type: u8,
        amount: u64,
        evidence: u8,
        appeal_window_close: u64,
    ) -> SlashRecordView {
        SlashRecordView {
            offense_type,
            amount: U256::from(amount),
            evidence_hash: B256::repeat_byte(evidence),
            appeal_window_close,
        }
    }

    /// Every enumeration read runs at the block it is given: the count, the
    /// pause offset, each index and each record.
    #[tokio::test]
    async fn every_enumeration_read_runs_at_the_given_block() {
        let op = Address::repeat_byte(0xAB);
        let now = 1_000;
        let reads = StubSlashReads::new(
            op,
            vec![
                (U256::from(20u64), record(1, 200, 0x22, now + 50)),
                (U256::from(30u64), record(0, 300, 0x33, now + 99)),
            ],
        );

        bootstrap_slashes(&reads, op, now, BlockId::number(100))
            .await
            .unwrap();

        let blocks = reads.blocks_read();
        // Count, pause offset, then an index and a record per slash.
        assert_eq!(blocks.len(), 6);
        assert!(
            blocks.iter().all(|b| *b == BlockId::number(100)),
            "{blocks:?}"
        );
    }

    /// A JSON-RPC responder that answers `eth_blockNumber` with block 100 and
    /// every `eth_call` with a zero word, recording each call's block tag.
    struct BlockTagRpc {
        tags: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl wiremock::Respond for BlockTagRpc {
        fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
            let id = body.get("id").cloned().unwrap_or(serde_json::Value::Null);
            let result = if body.get("method").and_then(serde_json::Value::as_str)
                == Some("eth_blockNumber")
            {
                serde_json::json!("0x3e8")
            } else {
                let tag = body
                    .pointer("/params/1")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                self.tags.lock().unwrap().push(tag);
                serde_json::json!(format!("0x{}", "00".repeat(32)))
            };
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": id, "result": result,
            }))
        }
    }

    /// The boot enumeration pins its reads to one snapshot block, so a lagging
    /// load-balanced backend cannot answer the count and the records from
    /// different blocks. The pin sits the lag margin below the reported head
    /// (1000), so an upstream behind that head can still serve it.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_boot_enumeration_reads_at_the_snapshot_block() {
        use crate::chain_events::boot_retry::BootRetry;
        use crate::chain_events::shared_head::{SNAPSHOT_LAG_MARGIN_BLOCKS, SharedHead};
        use alloy::providers::ProviderBuilder;

        let tags = Arc::new(std::sync::Mutex::new(Vec::new()));
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(BlockTagRpc {
                tags: Arc::clone(&tags),
            })
            .mount(&server)
            .await;
        let url: reqwest::Url = server.uri().parse().unwrap();
        let provider = ProviderBuilder::new().connect_http(url);
        let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(
            provider.clone(),
            Duration::from_hours(1),
            None,
        ));
        let metrics = Arc::new(Metrics::new());

        bootstrap(
            provider,
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            head,
            Arc::clone(&metrics),
            &BootRetry::single_attempt(Arc::clone(&metrics)),
        )
        .await
        .unwrap();

        // No slashes: the count and the pause offset.
        let tags = tags.lock().unwrap().clone();
        let pinned = format!("{:#x}", 1_000 - SNAPSHOT_LAG_MARGIN_BLOCKS);
        assert_eq!(tags, vec![serde_json::json!(pinned); 2]);
    }

    /// A stalled provider cannot wedge boot: every read is bounded by the
    /// per-call timeout, so the boot read fails into its budget.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_provider_fails_the_boot_enumeration() {
        use crate::chain_events::boot_retry::BootRetry;
        use crate::chain_events::shared_head::SharedHead;
        use crate::chain_events::test_support::{bounded, hanging_provider};
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;

        let head_asserter = Asserter::new();
        head_asserter.push_success(&alloy::primitives::U64::from(100));
        let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(
            ProviderBuilder::new().connect_mocked_client(head_asserter),
            Duration::from_hours(1),
            None,
        ));
        let metrics = Arc::new(Metrics::new());

        let err = bounded(
            "slash boot enumeration",
            bootstrap(
                hanging_provider(),
                Address::repeat_byte(0x11),
                Address::repeat_byte(0x22),
                head,
                Arc::clone(&metrics),
                &BootRetry::single_attempt(Arc::clone(&metrics)),
            ),
        )
        .await
        .err()
        .map(|e| format!("{e:#}"))
        .unwrap();

        assert!(err.contains("operatorSlashCount timed out after"), "{err}");
    }

    /// A deterministic head-read failure keeps its typed cause through the
    /// `SharedHead` cache, so boot fails at once rather than retrying.
    #[tokio::test(start_paused = true)]
    async fn a_permanent_head_error_fails_the_boot_enumeration_at_once() {
        use crate::chain_events::boot_retry::{BOOT_CHAIN_RETRY_BUDGET, BootRetry};
        use crate::chain_events::shared_head::SharedHead;
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;

        let head_asserter = Asserter::new();
        head_asserter.push_failure(
            serde_json::from_value(serde_json::json!({
                "code": -32601,
                "message": "method not found",
            }))
            .unwrap(),
        );
        let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(
            ProviderBuilder::new().connect_mocked_client(head_asserter),
            Duration::from_hours(1),
            None,
        ));
        let metrics = Arc::new(Metrics::new());
        let start = tokio::time::Instant::now();

        let err = bootstrap(
            ProviderBuilder::new().connect_mocked_client(Asserter::new()),
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            head,
            Arc::clone(&metrics),
            &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
        )
        .await
        .unwrap_err();

        assert!(format!("{err:#}").contains("not retried"), "{err:#}");
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    /// Enumeration surfaces the still-appealable slashes newest-first, carrying
    /// the authoritative record fields, and excludes those whose window closed.
    #[tokio::test]
    async fn bootstrap_slashes_surfaces_only_still_appealable_slashes_newest_first() {
        let op = Address::repeat_byte(0xAB);
        let now = 1_000;
        // Append order (oldest→newest): id 10 already closed, 20 and 30 still open.
        let reads = StubSlashReads::new(
            op,
            vec![
                (U256::from(10u64), record(1, 100, 0x11, now - 1)),
                (U256::from(20u64), record(1, 200, 0x22, now + 50)),
                (U256::from(30u64), record(0, 300, 0x33, now + 99)),
            ],
        );

        let slashes = bootstrap_slashes(&reads, op, now, BlockId::latest())
            .await
            .unwrap();

        assert_eq!(slashes.len(), 2, "closed slash 10 must be excluded");
        // Newest-first backward walk: 30 then 20.
        assert_eq!(slashes[0].slash_id, U256::from(30u64));
        assert_eq!(slashes[1].slash_id, U256::from(20u64));
        // Authoritative appeal-window close carried from the record, not derived.
        assert_eq!(slashes[0].appeal_window_close, Some(now + 99));
        assert_eq!(slashes[0].offense_type, 0);
        assert_eq!(slashes[1].evidence_hash, B256::repeat_byte(0x22));
        // No log, so no block number.
        assert_eq!(slashes[0].block_number, None);
    }

    /// The backward walk stops at the first closed record and never reads the
    /// older ones (they are closed too — appended in `slashedAt` order).
    #[tokio::test]
    async fn bootstrap_slashes_stops_at_the_first_closed_record() {
        let op = Address::repeat_byte(0xCD);
        let now = 1_000;
        let reads = StubSlashReads::new(
            op,
            vec![
                (U256::from(1u64), record(0, 1, 0x01, now - 100)),
                (U256::from(2u64), record(0, 1, 0x02, now - 50)),
                (U256::from(3u64), record(0, 1, 0x03, now + 10)),
            ],
        );

        let slashes = bootstrap_slashes(&reads, op, now, BlockId::latest())
            .await
            .unwrap();

        assert_eq!(slashes.len(), 1);
        assert_eq!(slashes[0].slash_id, U256::from(3u64));
        // Only the newest was point-read; the walk stopped at slash 2 without
        // reading slash 1.
        assert_eq!(
            reads.records_read(),
            vec![U256::from(3u64), U256::from(2u64)]
        );
    }

    /// The enforced deadline is the base `appealWindowClose` plus the global
    /// `pausedTotal`. A slash whose base window has closed but whose pause-extended
    /// window is still open MUST stay visible — and the backward walk must not stop
    /// early on it and hide older still-appealable slashes.
    #[tokio::test]
    async fn bootstrap_slashes_honors_the_pause_extended_deadline() {
        let op = Address::repeat_byte(0x5A);
        let now = 1_000;
        // Every base window has already closed (all <= now)…
        let reads = StubSlashReads::new(
            op,
            vec![
                (U256::from(10u64), record(0, 1, 0x10, now - 50)),
                (U256::from(20u64), record(0, 1, 0x20, now - 10)),
                (U256::from(30u64), record(0, 1, 0x30, now - 5)),
            ],
        )
        // …but a 100s protocol pause moves every effective deadline past `now`.
        .with_paused_total(100);

        let slashes = bootstrap_slashes(&reads, op, now, BlockId::latest())
            .await
            .unwrap();

        assert_eq!(
            slashes.len(),
            3,
            "all three are still appealable once pausedTotal is added; ignoring it \
             would surface none (base windows closed) and stop the walk early"
        );
        // The surfaced deadline is the pause-extended one the contract enforces.
        assert_eq!(slashes[0].appeal_window_close, Some(now + 95)); // 30: (now-5)+100
        assert_eq!(slashes[2].appeal_window_close, Some(now + 50)); // 10: (now-50)+100
    }

    /// An operator with no slashes enumerates to an empty set without error.
    #[tokio::test]
    async fn bootstrap_slashes_of_a_clean_operator_is_empty() {
        let op = Address::repeat_byte(0xEF);
        let reads = StubSlashReads::new(op, vec![]);
        assert!(
            bootstrap_slashes(&reads, op, 1_000, BlockId::latest())
                .await
                .unwrap()
                .is_empty()
        );
    }
}
