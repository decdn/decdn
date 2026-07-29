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
//!   reads only the still-appealable tail, and every field — offense, evidence,
//!   and the pause-aware `appealWindowClose` — comes straight off the record.
//!   Fatal on failure, like the registry bootstrap on the same contract that
//!   already gates startup: an RPC that fails here fails there first.
//! - **Follow `SlashRecorded` at head.** `SlashRecorded` indexes `operator` as
//!   `topic2`, so every `eth_getLogs` window constrains on it — a node never
//!   decodes other operators' slashes — and the tail is seeded at the
//!   enumeration snapshot head, so there is no historical scan on any boot. The
//!   event carries only the `slashId`; the offense/evidence/deadline are
//!   point-read from `getSlashRecord`.
//! - **Resync as the backstop.** A missed tail event (reorg at the unstable tip,
//!   or a tick lost to RPC backoff) never self-heals otherwise, so the store is
//!   re-enumerated every `SLASH_RESYNC_INTERVAL`; this also prunes slashes
//!   whose appeal window has since closed. The store is deduped by `slashId`, so
//!   the enumeration/tail overlap is harmless.

use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use decdn_incentive::capacity_bond::CapacityBond;
use tracing::{debug, info, warn};

use decdn_common::redact::sanitize_err_chain;

use crate::chain_events::resumable_watcher::{
    self, CursorStart, LogSink, WatcherConfig, WatcherHandle,
};
use crate::chain_events::shared_head::HeadSource;
use crate::metrics::{Metrics, metric_hook};

/// How often the store is re-enumerated from `CapacityBond` as a drift backstop.
///
/// Mirrors the `CapacityBond` registry watcher's cadence and rationale: a missed
/// live-tail event never self-heals short of a restart, and one enumeration of a
/// single operator's (tiny) slash list every quarter hour is negligible.
/// Deliberately a constant, not a config knob — a correctness backstop, not a
/// tuning surface.
const SLASH_RESYNC_INTERVAL: Duration = Duration::from_mins(15);

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
    /// `keccak256` evidence digest, from the `CapacityBond` slash record.
    pub evidence_hash: B256,
    /// Block the `SlashRecorded` log was mined in — `Some` for a slash seen live
    /// on the tail, `None` for one recovered by the boot/resync enumeration
    /// (which reads records, not logs).
    pub block_number: Option<u64>,
    /// **Authoritative** appeal-window close, read from `getSlashRecord`
    /// (`appealWindowClose`): it carries protocol-pause extensions, which a
    /// deadline derived from a log's block timestamp does not. Always `Some` now
    /// the deadline comes off the record rather than a best-effort block read.
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
    /// **Authoritative** appeal-window close: carries protocol-pause extensions,
    /// unlike a deadline derived from a log's block timestamp.
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
    fn operator_slash_count(&self, operator: Address) -> impl Future<Output = Result<U256>> + Send;
    /// The `slashId` at `index` in `operator`'s append-only slash list
    /// (`operatorSlashIdAt`); indices are stable, so no pinned block is needed.
    fn operator_slash_id_at(
        &self,
        operator: Address,
        index: U256,
    ) -> impl Future<Output = Result<U256>> + Send;
    /// The authoritative record for `slashId` (`getSlashRecord`).
    fn get_slash_record(
        &self,
        slash_id: U256,
    ) -> impl Future<Output = Result<SlashRecordView>> + Send;
}

/// Enumerate this operator's still-appealable slashes from the authoritative
/// `CapacityBond` records — the boot path that replaces re-scanning the
/// `Slashed` log tail from a block floor on every start (#1108, ADR 019).
///
/// Walks the append-only index backwards from newest and stops at the first
/// record whose appeal window has already closed against `now_secs`: earlier
/// records were minted earlier (`slashedAt` order), so their windows closed too.
/// The list is append-only with stable indices, so — unlike the swap-and-pop
/// enumeration views — no pinned-block read or count re-check is required; a
/// slash minted mid-scan lands at an index past the old count (unseen here) and
/// is picked up by the live `SlashRecorded` tail instead.
async fn bootstrap_slashes<R: SlashChainReads>(
    reads: &R,
    operator: Address,
    now_secs: u64,
) -> Result<Vec<DetectedSlash>> {
    let count = reads.operator_slash_count(operator).await?;
    let mut out = Vec::new();
    let mut index = count;
    while index > U256::ZERO {
        index -= U256::from(1u8);
        let slash_id = reads.operator_slash_id_at(operator, index).await?;
        let record = reads.get_slash_record(slash_id).await?;
        if record.appeal_window_close <= now_secs {
            // Appended in `slashedAt` order, so every older record is closed too.
            break;
        }
        out.push(DetectedSlash {
            slash_id,
            offense_type: record.offense_type,
            amount: record.amount,
            evidence_hash: record.evidence_hash,
            // No log is read on this path; the record carries the authoritative
            // deadline rather than a block-timestamp derivation.
            block_number: None,
            appeal_window_close: Some(record.appeal_window_close),
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
    async fn operator_slash_count(&self, operator: Address) -> Result<U256> {
        self.bond
            .operatorSlashCount(operator)
            .call()
            .await
            .with_context(|| format!("operatorSlashCount({operator})"))
    }

    async fn operator_slash_id_at(&self, operator: Address, index: U256) -> Result<U256> {
        self.bond
            .operatorSlashIdAt(operator, index)
            .call()
            .await
            .with_context(|| format!("operatorSlashIdAt({operator}, {index})"))
    }

    async fn get_slash_record(&self, slash_id: U256) -> Result<SlashRecordView> {
        let record = self
            .bond
            .getSlashRecord(slash_id)
            .call()
            .await
            .with_context(|| format!("getSlashRecord({slash_id})"))?;
        Ok(SlashRecordView {
            offense_type: record.offenseType,
            amount: record.slashAmount,
            evidence_hash: record.evidenceHash,
            appeal_window_close: record.appealWindowClose,
        })
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

/// A running slash-detection watcher. Holds the shared store and owns its
/// `WatcherHandle` — graceful [`shutdown`](Self::shutdown) first (the runtime
/// calls it in the ordered stop sequence), with the handle's `AbortOnDrop` as
/// the backstop.
pub struct SlashWatcher {
    store: SlashStore,
    watcher: WatcherHandle,
}

impl std::fmt::Debug for SlashWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlashWatcher").finish_non_exhaustive()
    }
}

impl SlashWatcher {
    /// Enumerate this operator's still-appealable slashes from `CapacityBond`,
    /// then spawn the `SlashRecorded` tail that keeps the store current.
    ///
    /// Fatal on an enumeration failure, matching the `CapacityBond` registry
    /// bootstrap that runs on the same contract immediately before this one and
    /// already gates node startup: an RPC that fails here fails there first, so
    /// this adds no new startup-failure mode. Once running, a transient tail RPC
    /// blip retries with backoff, and the periodic resync repairs any drift — so
    /// detection is never disabled for the daemon's lifetime.
    pub async fn bootstrap<P: Provider + Clone + 'static>(
        provider: P,
        capacity_bond_addr: Address,
        self_address: Address,
        event_poll_interval: Duration,
        head: Arc<dyn HeadSource>,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        info!(%capacity_bond_addr, %self_address, "slash-detection watcher started");
        let reads = ContractReads {
            bond: CapacityBond::new(capacity_bond_addr, provider.clone()),
        };

        // Head BEFORE the enumeration, then seed the tail cursor there. The
        // reverse order would lose a `SlashRecorded` landing between enumeration
        // and the head read — neither in the snapshot nor above the cursor.
        // Re-applying a snapshot event is a deduped no-op, so overlap is safe but
        // a gap is not.
        let snapshot_block = head
            .head()
            .await
            .context("read head block for the slash enumeration snapshot")?;
        let initial = bootstrap_slashes(&reads, self_address, unix_now())
            .await
            .with_context(|| {
                format!("enumerate operator slashes from CapacityBond at {capacity_bond_addr}")
            })?;
        info!(
            slash_count = initial.len(),
            snapshot_block, %self_address, "slash enumeration complete"
        );

        // Seed through `record_slash` so the boot set is logged, counted, and
        // deduped on the same path the live tail uses.
        let store: SlashStore = Arc::new(RwLock::new(Vec::new()));
        for slash in initial {
            record_slash(&store, &metrics, slash);
        }

        let on_established = metric_hook(&metrics, Metrics::slash_watcher_cycle_established);
        let on_backoff = metric_hook(&metrics, Metrics::slash_watcher_backoff_started);
        let on_tick_success = metric_hook(&metrics, Metrics::slash_watcher_tick);
        let on_task_panic = metric_hook(&metrics, Metrics::slash_watcher_task_panicked);
        let sink = SlashSink {
            reads,
            self_address,
            store: Arc::clone(&store),
            metrics: Arc::clone(&metrics),
            resync_interval: SLASH_RESYNC_INTERVAL,
            // The bootstrap enumeration just ran, so the first backstop resync is
            // due one interval from now rather than on the first tick.
            last_resync: Some(Instant::now()),
        };
        let cfg = WatcherConfig::new(
            head,
            operator_filter(capacity_bond_addr, self_address),
            // The enumeration rebuilt the store from all of history, so seed the
            // tail at that snapshot head — no durable cursor, no historical scan.
            CursorStart::Seeded {
                at: snapshot_block,
                persist: None,
            },
            event_poll_interval,
            "slash",
        )
        .max_backoff(SLASH_MAX_BACKOFF)
        .on_established(on_established)
        .on_backoff(on_backoff)
        .on_tick_success(on_tick_success)
        .on_task_panic(on_task_panic);
        // This sink observes no shutdown token, so it ignores the one `spawn`
        // mints (`|_| sink`); the runtime drives graceful stop via `shutdown`.
        let watcher = resumable_watcher::spawn(provider, cfg, move |_| sink);
        Ok(Self { store, watcher })
    }

    /// Signal the watcher to stop its poll loop and return. Called by the runtime
    /// on graceful shutdown; the `WatcherHandle`'s `AbortOnDrop` is the backstop.
    pub fn shutdown(&self) {
        self.watcher.shutdown();
    }

    /// A clone of the shared detected-slash store for the admin surface.
    #[must_use]
    pub fn store(&self) -> SlashStore {
        Arc::clone(&self.store)
    }
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

        let fresh = match bootstrap_slashes(&self.reads, self.self_address, unix_now()).await {
            Ok(fresh) => fresh,
            Err(err) => {
                warn!(
                    err = %sanitize_err_chain(&err),
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
}

/// The address + `SlashRecorded`-signature + `topic2 == operator` filter applied
/// to every `eth_getLogs` poll window, so the RPC only ever returns this
/// operator's slashes.
fn operator_filter(capacity_bond_addr: Address, self_address: Address) -> Filter {
    Filter::new()
        .address(capacity_bond_addr)
        .event_signature(CapacityBond::SlashRecorded::SIGNATURE_HASH)
        .topic2(self_address.into_word())
}

/// Decode one `SlashRecorded` log to its `slashId`, or `None` for a log that
/// must not be recorded: reorged-out (`removed == true` — not expected from
/// `eth_getLogs`, so this is defensive against a nonconforming provider),
/// undecodable, or another operator's. The `topic2` filter already constrains to
/// this operator, but the defensive operator check guards against a provider that
/// ignores the topic. Pure (no provider) so the skip policy is unit-testable.
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
    let record = match reads.get_slash_record(slash_id).await {
        Ok(record) => record,
        Err(err) => {
            warn!(
                err = %sanitize_err_chain(&err),
                slash_id = %slash_id,
                "failed to read slash record for a live SlashRecorded log; the resync will recover it"
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
            appeal_window_close: Some(record.appeal_window_close),
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
        /// slashIds whose record was point-read, for the early-stop assertion.
        reads: std::sync::Mutex<Vec<U256>>,
    }

    impl StubSlashReads {
        fn new(operator: Address, slashes: Vec<(U256, SlashRecordView)>) -> Self {
            Self {
                operator,
                slashes,
                reads: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn records_read(&self) -> Vec<U256> {
            self.reads.lock().unwrap().clone()
        }
    }

    impl SlashChainReads for StubSlashReads {
        async fn operator_slash_count(&self, operator: Address) -> Result<U256> {
            assert_eq!(operator, self.operator, "unexpected operator");
            Ok(U256::from(self.slashes.len()))
        }

        async fn operator_slash_id_at(&self, operator: Address, index: U256) -> Result<U256> {
            assert_eq!(operator, self.operator, "unexpected operator");
            let i: usize = index.to();
            self.slashes
                .get(i)
                .map(|(id, _)| *id)
                .ok_or_else(|| anyhow::anyhow!("SlashIndexOutOfRange"))
        }

        async fn get_slash_record(&self, slash_id: U256) -> Result<SlashRecordView> {
            self.reads.lock().unwrap().push(slash_id);
            self.slashes
                .iter()
                .find(|(id, _)| *id == slash_id)
                .map(|(_, rec)| rec.clone())
                .ok_or_else(|| anyhow::anyhow!("no such slash"))
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
                (U256::from(10u64), record(2, 100, 0x11, now - 1)),
                (U256::from(20u64), record(1, 200, 0x22, now + 50)),
                (U256::from(30u64), record(0, 300, 0x33, now + 99)),
            ],
        );

        let slashes = bootstrap_slashes(&reads, op, now).await.unwrap();

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

        let slashes = bootstrap_slashes(&reads, op, now).await.unwrap();

        assert_eq!(slashes.len(), 1);
        assert_eq!(slashes[0].slash_id, U256::from(3u64));
        // Only the newest was point-read; the walk stopped at slash 2 without
        // reading slash 1.
        assert_eq!(
            reads.records_read(),
            vec![U256::from(3u64), U256::from(2u64)]
        );
    }

    /// An operator with no slashes enumerates to an empty set without error.
    #[tokio::test]
    async fn bootstrap_slashes_of_a_clean_operator_is_empty() {
        let op = Address::repeat_byte(0xEF);
        let reads = StubSlashReads::new(op, vec![]);
        assert!(
            bootstrap_slashes(&reads, op, 1_000)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
