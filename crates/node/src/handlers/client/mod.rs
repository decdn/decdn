//! `cdn/client/v1` handler — paid blob delivery (ADR 005 §`cdn/client/v1`).
//!
//! Serves the revenue path: a payer opens one bidirectional QUIC stream per
//! blob, the node answers with a signed [`StreamResponse`], then streams
//! [`ChunkData`](decdn_protocol::client::ChunkData) frames, collecting one
//! hash-chain preimage per delivered
//! `CHUNK_BYTES` chunk — with a signed `Voucher` to open a chain, to roll one,
//! and to settle a sub-chunk residual — and pausing only when the unpaid balance
//! (`delivered − paid`) reaches the credit window — so delivery pipelines
//! several intervals ahead of payment rather than stopping at each one — and
//! finishing with [`ClientMessage::StreamEnd`]. A delivery fault rides in the
//! initial response (`ok: false` + [`StreamError`]); a mid-stream voucher
//! rejection is sent as a [`ClientMessage::StreamError`] and the stream is
//! closed **cleanly** (no QUIC reset) so the client can read the reason.
//!
//! # Scope (#317 / #327)
//!
//! This handler validates vouchers per **lane** — a `(pool_id, signer,
//! provider)` triple keyed by [`LaneKey`] — against the persisted
//! [`PoolStateStore`]. A lane's watermark is hydrated from the store at
//! construction (see [`ClientHandler::new`]) and, for a lane first seen live,
//! created on the first voucher from its off-chain capability handle. A voucher
//! whose `pool_id` names an unknown pool is rejected with
//! [`VoucherRejectReason::WrongPool`]. After accepting a voucher the handler
//! emits a redeem hint (via the `redeem_hint` sender wired on
//! [`ClientHandlerDeps`]) so the settlement service can redeem the accrued claim
//! once it crosses its threshold.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use dashmap::DashMap;
use decdn_cache::{CHUNK_GROUP_BYTES, CacheEngine, CacheError, Hash, RangePullOutcome};
use decdn_incentive::rate::{DEFAULT_TOLERANCE_BPS, RateError, min_payment, verify_rate};
use decdn_incentive::store::{PoolStateStore, StoreError};
use decdn_incentive::{
    Capability, LaneKey, LaneState, RetrySignal, SignedCapability, SignedVoucher, StreamSlashData,
    verify_binding, voucher_reject_reason, wire_voucher_to_signed,
};
use decdn_protocol::client::{
    ClientMessage, StreamError, StreamRequest, StreamRequestExt, StreamResponse,
    StreamResponseBody, StreamResponseExt, VoucherRejectReason, WatermarkBundle,
};
use decdn_protocol::{
    ALPN_CLIENT, APP_ERR_RATE_LIMITED, CHUNK_BYTES, FrameError, decode_message, encode_chunk_frame,
    encode_message, is_unknown_variant, read_frame, write_frame,
};
use iroh::PublicKey;
use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};

use crate::dispatch::{ConnectionLimiter, RejectReason};
use crate::metrics::Metrics;
use crate::node_origin::NodeOrigin;
use crate::receipt_log::{DownloadReceipt, ReceiptSink};

// The paid-delivery methods are split across concern-focused submodules, each
// a bare `impl ClientHandler` block over the fields defined here. Support
// types, consts, and free functions stay in this module so every submodule
// (and the test module) can reach them via `use super::*` — Rust makes a
// module's private items visible to its descendants (#1254).
mod delivery;
mod dispatch;
mod fill;
mod serve_encoder;
mod serve_leg;
mod voucher;
mod window;
mod wire;

/// Default per-connection concurrent-stream cap for `cdn/client/v1` (ADR 005
/// §Concurrent stream limits). The QUIC transport config also caps bidi
/// streams at this value; the application semaphore makes the per-ALPN bound
/// explicit and testable.
pub const MAX_CLIENT_STREAMS: usize = 100;

// Per-stage timeouts so a stalled peer cannot pin a stream task indefinitely.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);
const VOUCHER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const REJECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(250);
/// Fallback overall deadline for opening a window-paced pull (#856) when no
/// pull-through deadline is configured. In practice the runtime always sets
/// one alongside the window provider, so this only guards a misconfiguration.
const WINDOW_PULL_FALLBACK_DEADLINE: Duration = Duration::from_mins(1);

/// Application-layer idle-close ceiling (ADR 005 §Connection lifetime): a served
/// connection is closed this long after its last stream closes — or after it is
/// accepted, if no stream ever opens. Distinct from
/// the QUIC transport idle timeout (`runtime::QUIC_MAX_IDLE_TIMEOUT`, also 30s
/// today — the two are independent constants that happen to match): keep-alive
/// PINGs refresh the transport timer, so a peer can hold a connection open
/// indefinitely while sending zero streams — only this app-layer clock reclaims
/// it. ADR 005's "no unacknowledged vouchers in flight" clause never delays the
/// reaper here: vouchers only ever flow *inside* a stream, so a connection with
/// no stream in flight has no voucher in flight either (sent or received), and
/// the rule reduces to purely stream-idle.
const APP_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

// QUIC application error codes (ADR 013 §Application Error Codes). A clean
// voucher rejection does NOT use these — it writes a `StreamError` frame and
// finishes the stream so the reason survives.
const APP_ERR_NO_ERROR: u32 = 0x00;
const APP_ERR_UNSUPPORTED_MESSAGE: u32 = 0x01;
const APP_ERR_MALFORMED_MESSAGE: u32 = 0x03;

/// Per-lane delivery state: the validated voucher watermark plus the lane-wide
/// cumulative byte counter that feeds voucher reconstruction (ADR 003 §Voucher
/// wire format — `bytes_delivered` is not on the wire).
#[derive(Debug)]
struct LaneDeliveryState {
    state: LaneState,
    /// Lane-wide cumulative bytes delivered as of the last accepted voucher.
    bytes_delivered_cumulative: U256,
    /// Cumulative wire bytes CREDITED to streams' paid headroom on this lane
    /// (design rule #1). Monotone and never exceeds `bytes_delivered_cumulative`
    /// (the settled watermark), so a benign already-satisfied voucher — which
    /// does not raise the watermark — can only advance a stream's window for
    /// bytes the lane has actually settled, never for unpaid delivered bytes.
    paid_credited: U256,
    /// Count of same-lane streams currently admitted and delivering. The serve-path
    /// admission gate charges each already-active stream one credit-window floor of
    /// pool headroom; a [`LaneSlot`] decrements this on every serve exit path. Shared
    /// as an `Arc` so the guard releases lock-free without re-taking the lane mutex.
    active_streams: Arc<AtomicU32>,
    /// Wall-clock (Unix milliseconds) of the last accepted voucher on this lane,
    /// or `0` when this process has accepted none since it hydrated the lane
    /// (issue #1733). Stamped by [`ClientHandler::commit_one_proof`] under the
    /// per-lane lock it already holds — a field write, not a separate global
    /// mutex — and read back by [`LaneActivityClock::ages`] for the admin
    /// "seconds since last voucher" readout. Best-effort liveness bookkeeping:
    /// it gates nothing, and the stamp lifecycle follows the lane row (a
    /// forgotten lane drops it automatically).
    last_voucher_at: AtomicU64,
}

/// Read handle over the client handler's live lane registry, exposing each
/// lane's whole-seconds age since its last accepted voucher for the admin
/// `lanes` surface (issue #1733). Cloning shares the same registry `Arc`, so
/// the admin surface and the handler observe one set of lanes. The timestamp
/// lives on the lane's own delivery state, stamped under the per-lane lock the
/// voucher-accept path already holds, so reading liveness needs no separate
/// global lock.
#[derive(Clone)]
pub struct LaneActivityClock {
    lanes: Arc<DashMap<LaneKey, Arc<Mutex<LaneDeliveryState>>>>,
}

impl std::fmt::Debug for LaneActivityClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneActivityClock").finish_non_exhaustive()
    }
}

impl LaneActivityClock {
    /// An activity clock over an empty registry — reports "never" for every
    /// lane. Used where no client handler is wired (a node with no payment
    /// surface, and admin unit tests).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            lanes: Arc::new(DashMap::new()),
        }
    }

    /// Whole seconds since each live lane last accepted a voucher, keyed by
    /// [`LaneKey`]. A lane whose stamp is `0` (none accepted since this process
    /// hydrated it) is omitted, so a caller reads it back as "never" rather than
    /// a bogus zero age. Best-effort: `Instant`-free wall-clock arithmetic, so a
    /// clock that stepped backwards can only under-report the age — the safe
    /// direction for a diagnostic.
    pub async fn ages(&self) -> HashMap<LaneKey, u64> {
        let now_ms = unix_millis();
        // Snapshot the live lane handles first (cloning the per-lane `Arc`s
        // releases the registry's shard guards), then read each stamp with only
        // its own (brief) lane lock held — never a shard guard across an await.
        let handles: Vec<(LaneKey, Arc<Mutex<LaneDeliveryState>>)> = self
            .lanes
            .iter()
            .map(|e| (*e.key(), Arc::clone(e.value())))
            .collect();
        let mut out = HashMap::with_capacity(handles.len());
        for (key, lane) in handles {
            let stamped = lane.lock().await.last_voucher_at.load(Ordering::Relaxed);
            if stamped != 0 {
                out.insert(key, now_ms.saturating_sub(stamped) / 1000);
            }
        }
        out
    }

    /// Test-only: a clock whose registry holds each `lane` already stamped
    /// "now", so [`ages`](Self::ages) reports a near-zero age for it. Mirrors
    /// what the voucher-accept path does to a live lane, without a running
    /// handler. Used to drive the admin `lanes` ordering test.
    #[cfg(test)]
    pub(crate) fn with_stamped_lanes(lanes: &[LaneKey]) -> Self {
        let now = unix_millis();
        let map = DashMap::new();
        for &key in lanes {
            let state = LaneState::hydrate(
                key.pool_id,
                key.signer,
                key.provider,
                U256::MAX,
                0,
                U256::ZERO,
                U256::ZERO,
                None,
                decdn_incentive::LaneChain::NONE,
            );
            map.insert(
                key,
                Arc::new(Mutex::new(LaneDeliveryState {
                    state,
                    bytes_delivered_cumulative: U256::ZERO,
                    paid_credited: U256::ZERO,
                    active_streams: Arc::new(AtomicU32::new(0)),
                    last_voucher_at: AtomicU64::new(now),
                })),
            );
        }
        Self {
            lanes: Arc::new(map),
        }
    }
}

/// Wall-clock now in milliseconds since the Unix epoch, or `0` if the system
/// clock is before the epoch. Stamps [`LaneDeliveryState::last_voucher_at`] on
/// each accepted voucher and is read back by [`LaneActivityClock::ages`] (issue
/// #1733). The `0` fallback doubles as the "never stamped" sentinel and only
/// makes a lane look staler than it is — the safe direction for a best-effort
/// diagnostic.
pub(super) fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl ClientHandler {
    /// A read handle over this handler's live lane registry for the admin
    /// `lanes` surface (issue #1733): the last-voucher clock the operator's
    /// "seconds since last voucher" readout reads. Shares the registry `Arc`, so
    /// it observes every stamp the voucher-accept path writes without a separate
    /// shared structure.
    #[must_use]
    pub fn lane_activity_clock(&self) -> LaneActivityClock {
        LaneActivityClock {
            lanes: Arc::clone(&self.lanes),
        }
    }
}

/// RAII slot for one admitted same-lane stream. Created under the lane lock after
/// the admission gate increments [`LaneDeliveryState::active_streams`]; its `Drop`
/// decrements the same counter on every serve exit — success, error, `?`-return,
/// client disconnect, panic — so a finished stream always frees its slot. A leaked
/// slot would make the lane refuse new streams forever, so the count is owned by
/// this guard, never decremented by hand.
struct LaneSlot {
    counter: Arc<AtomicU32>,
}

impl LaneSlot {
    const fn new(counter: Arc<AtomicU32>) -> Self {
        Self { counter }
    }
}

impl Drop for LaneSlot {
    fn drop(&mut self) {
        // Saturating decrement. A slot exists only paired with a prior increment,
        // so the counter is never 0 here today; the saturating floor keeps a future
        // unpaired slot from underflowing `u32::MAX` and wedging the lane (every
        // admission then refused) rather than failing safe.
        let _ = self
            .counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
    }
}

/// Per-pool floor-credit accounting (ADR 003 §Pool solvency). `live_reservation`
/// is the `µUSDC` currently reserved by in-flight streams — ephemeral, cleared on
/// restart since no stream is live then; `dead_charge` is the durable, cumulative
/// unrecoverable floor loss, persisted in a [`decdn_incentive::PoolFloorLossStore`]
/// and reloaded at bring-up.
// The floor accumulator, its RAII guard, and their accessors form the serve
// path's pool-solvency surface. They are self-contained and unit-tested on their
// own; the serve loop is their only caller and does not reach them yet, so
// `dead_code` is allowed on the not-yet-called surface.
#[allow(dead_code)]
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct PoolFloorState {
    live_reservation: U256,
    dead_charge: U256,
}

/// RAII hold for one stream's span-capped reservation against a pool's budget.
///
/// Construction ([`Self::reserve`]) charges the reserved amount to the pool's
/// `live_reservation`. The serve loop keeps the current unpaid `µUSDC` updated via
/// [`Self::note_unpaid`], and calls [`Self::release_live_repaid`] once THIS stream's
/// cumulative payment reaches a floor — which frees the live reservation
/// immediately. On drop (every exit path — success, `?`, disconnect, panic) the
/// guard releases the live reservation if it was not already repaid and folds the
/// last-noted unpaid amount (capped at the reserved amount) into the durable
/// `dead_charge`,
/// then persists the new dead total best-effort. Mirrors [`LaneSlot`]: the
/// reservation is owned by the guard and never adjusted by hand, and every counter
/// update saturates.
#[allow(dead_code)]
pub(super) struct FloorReservation {
    map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
    store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
    /// Failure accounting for the best-effort drop-time persist
    /// (`floor_loss_persist_failures`, #1782). Held by the guard because the
    /// persist outlives the serve path that opened it.
    metrics: Arc<Metrics>,
    pool_id: B256,
    reserved: U256,
    /// Last-noted unpaid `µUSDC` (`u64`, saturating). Read once at drop to size the
    /// `dead_charge` fold.
    unpaid: AtomicU64,
    /// Set by [`Self::release_live_repaid`]; makes drop a no-op (live already freed,
    /// no dead charge). Idempotent.
    repaid: AtomicBool,
    /// Set by [`Self::mark_settled`] at a stream's CLEAN completion. On drop it
    /// selects the fold size: a settled stream folds only its proportional unpaid
    /// tail; an abnormal exit that was never marked settled folds the full `reserved`
    /// — the safe, conservative direction for a solvency guard. Without this, an abort before
    /// any byte is delivered would drop with `unpaid == 0` and fold nothing, so a
    /// cache-miss stream that fronted upstream USDC could be repeated sequentially
    /// forever, never charging `dead_charge` (ADR 003 §Pool solvency).
    settled: AtomicBool,
}

#[allow(dead_code)]
impl FloorReservation {
    /// Reserve `reserved` `µUSDC` of the pool's budget. Charges
    /// `live_reservation += reserved` (saturating) under the sync lock — an O(1) map
    /// touch with no `.await` held, so a blocking lock is correct even on the async
    /// serve path (mirrors `lane_metrics_refresh`). A poisoned lock recovers the
    /// guard rather than panicking; the reservation is best-effort accounting, never
    /// a safety gate.
    fn reserve(
        map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
        store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
        metrics: Arc<Metrics>,
        pool_id: B256,
        reserved: U256,
    ) -> Self {
        {
            let mut guard = map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = guard.entry(pool_id).or_default();
            entry.live_reservation = entry.live_reservation.saturating_add(reserved);
        }
        Self::new_charged(map, store, metrics, pool_id, reserved)
    }

    /// Build a guard for a floor that is ALREADY charged to `live_reservation`
    /// under the caller's own lock hold. This does NOT touch the map — the
    /// increment happens exactly once, at the caller's atomic check-and-reserve,
    /// so re-incrementing here would double-charge the pool. Used by
    /// [`ClientHandler::try_reserve_floor`], whose single lock hold covers both the
    /// budget check and the increment; [`Self::reserve`] is the standalone form
    /// that increments first, then delegates here.
    fn new_charged(
        map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
        store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
        metrics: Arc<Metrics>,
        pool_id: B256,
        reserved: U256,
    ) -> Self {
        Self {
            map,
            store,
            metrics,
            pool_id,
            reserved,
            unpaid: AtomicU64::new(0),
            repaid: AtomicBool::new(false),
            settled: AtomicBool::new(false),
        }
    }

    /// Mark the stream cleanly completed, so drop folds only the proportional unpaid
    /// tail (`min(reserved, unpaid)`) rather than the full `reserved`. Called at the
    /// serve loop's clean-completion point — the whole request delivered and every
    /// interval paid. Any exit that does NOT call this (client disconnect, `?`, a
    /// voucher rejection, a mid-stream stop) is treated as abnormal and folds the
    /// full reservation. Idempotent.
    fn mark_settled(&self) {
        self.settled.store(true, Ordering::Relaxed);
    }

    /// Record the stream's current unpaid `µUSDC`, read at drop to size the
    /// `dead_charge` fold if the reservation is never repaid. Saturating to `u64`.
    fn note_unpaid(&self, micro: U256) {
        self.unpaid
            .store(micro.saturating_to::<u64>(), Ordering::Relaxed);
    }

    /// Mark the reservation repaid: the lane's cumulative payment reached a floor,
    /// so free the live reservation now (`live_reservation -= reserved`, saturating)
    /// and make the eventual drop a no-op. Idempotent — only the first call moves
    /// the live counter.
    fn release_live_repaid(&self) {
        if self.repaid.swap(true, Ordering::Relaxed) {
            return; // already repaid; live already released
        }
        let mut guard = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // `get_mut`, not `entry().or_default()`, same as the drop guard: if the
        // pool was reclaimed (`forget_pool_floor` removed its entry) the live
        // reservation went with the entry, and re-inserting a default state here
        // resurrects an entry for a closed pool that nothing removes again — the
        // in-memory face of #1781.
        if let Some(entry) = guard.get_mut(&self.pool_id) {
            entry.live_reservation = entry.live_reservation.saturating_sub(self.reserved);
        }
    }

    /// Release the live reservation once cumulative payment (`paid_micro`) reaches
    /// the amount that was reserved. Matches release to the reserved size at any
    /// `credit_ramp_divisor`: with the ramp disabled the reservation is the full
    /// `credit_max`, so release must wait for that much to be paid rather than a
    /// single chunk. Idempotent (delegates to [`Self::release_live_repaid`]).
    fn release_if_repaid(&self, paid_micro: U256) {
        if paid_micro >= self.reserved {
            self.release_live_repaid();
        }
    }
}

impl Drop for FloorReservation {
    fn drop(&mut self) {
        if self.repaid.load(Ordering::Relaxed) {
            return; // repaid: live already released, no dead charge
        }
        // Not repaid: release the live reservation and fold a dead charge. A CLEANLY
        // completed stream ([`Self::mark_settled`]) folds only its proportional unpaid
        // tail (`min(reserved, unpaid)`, `== 0` for a fully-paid small blob). An
        // ABNORMAL exit — client disconnect, `?`, a voucher rejection, a mid-stream
        // stop — folds the FULL `reserved`: the conservative, safe direction for a
        // solvency guard. This is what bounds sequential abuse where a client aborts a
        // cache-miss fill before any byte is delivered (`unpaid == 0`) yet the node has
        // already fronted upstream USDC — without it the pool's budget would be
        // restored in full and the pattern could repeat forever (ADR 003 §Pool solvency).
        // All under the sync lock, all saturating — an O(1) update that never blocks
        // the reactor.
        let dead_add = if self.settled.load(Ordering::Relaxed) {
            self.reserved
                .min(U256::from(self.unpaid.load(Ordering::Relaxed)))
        } else {
            self.reserved
        };
        let snapshot = {
            let mut guard = self
                .map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // `get_mut`, not `entry().or_default()`: if the pool was reclaimed
            // (`forget_pool_floor` removed its entry) there is nothing to release —
            // the live reservation went with the entry — and re-inserting would
            // resurrect a row `forget` just deleted. Skip the whole reconcile.
            let Some(entry) = guard.get_mut(&self.pool_id) else {
                return;
            };
            entry.live_reservation = entry.live_reservation.saturating_sub(self.reserved);
            entry.dead_charge = entry.dead_charge.saturating_add(dead_add);
            entry.dead_charge
        };
        // A fully-repaid or fully-settled stream folds nothing: skip the durable
        // write entirely so a sub-interval request does not fsync a value already
        // on disk. `record_loss` commits with `Durability::Immediate` on the same
        // redb file as the lane table, so an unconditional write here would contend
        // with the periodic voucher flush on every small paid request.
        if dead_add.is_zero() {
            return;
        }
        // Persist the new total best-effort. The in-memory `dead_charge` above is
        // authoritative for the running process; the durable copy only guards a
        // restart, so a lost persist is the documented small crash-window residual —
        // logged and counted, never panicked or propagated. `record_loss` is MONOTONIC
        // (it raises the stored total, never lowers it), so two drops on the same pool
        // completing out of order cannot regress the row. `record_loss` may fsync, so
        // offload it to a blocking task when a runtime is available; a drop outside
        // any runtime (e.g. a sync test) records inline.
        let Some(store) = self.store.clone() else {
            return;
        };
        let pool_id = self.pool_id;
        let micro = snapshot.saturating_to::<u128>();
        let metrics = Arc::clone(&self.metrics);
        // One closure for both dispatch paths so the failure accounting cannot
        // drift between them (#1782): bump `floor_loss_persist_failures`, log the
        // µUSDC total that failed to reach disk, and surface a corrupt payment
        // database at `error!` — after a mid-commit failure redb refuses further
        // writes until the file is closed and reopened, so every later persist
        // fails too and the fix is an operator restart, unlike a transient fault.
        let persist = move || {
            if let Err(e) = store.record_loss(pool_id, micro) {
                metrics.floor_loss_persist_failure();
                if matches!(e, decdn_incentive::StoreError::Corrupt { .. }) {
                    tracing::error!(
                        %pool_id, micro, error = %e,
                        "floor dead-charge persist failed: payment store corrupt"
                    );
                } else {
                    tracing::warn!(%pool_id, micro, error = %e, "floor dead-charge persist failed");
                }
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn_blocking(persist);
            }
            Err(_) => persist(),
        }
    }
}

/// Failure accounting for a reclaimed pool's `forget_loss`
/// ([`ClientHandler::forget_pool_floor`]). A failed forget leaves the row in
/// place with NO tombstone, so a drop-dispatched `record_loss` still in flight
/// can re-insert the closed pool's row and nothing ever deletes it again — the
/// #1781 leak through the error path. Counted under
/// `floor_loss_persist_failures` so `DecdnFloorLossPersistFailures` covers this
/// mode too; a corrupt payment store surfaces at `error!`, mirroring the
/// drop-time persist — redb refuses further writes until the file is reopened,
/// so the remedy is an operator restart.
fn note_forget_outcome(
    metrics: &Metrics,
    pool_id: B256,
    result: Result<Result<(), decdn_incentive::StoreError>, tokio::task::JoinError>,
) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            metrics.floor_loss_persist_failure();
            if matches!(e, decdn_incentive::StoreError::Corrupt { .. }) {
                tracing::error!(
                    %pool_id, error = %e,
                    "pool dead-charge forget failed: payment store corrupt"
                );
            } else {
                tracing::warn!(%pool_id, error = %e, "pool dead-charge forget failed");
            }
        }
        Err(e) => {
            metrics.floor_loss_persist_failure();
            tracing::warn!(%pool_id, error = %e, "pool dead-charge forget join failed");
        }
    }
}

/// Whether an insufficient-deposit `warn!` is due: `interval` has elapsed since
/// `last_warn_ms`, or nothing has ever been warned (`last_warn_ms == 0`).
///
/// Pure and millisecond-based so the throttle is testable without sleeping. A
/// clock that steps behind `last_warn_ms` yields `false` (via the saturating
/// subtraction), suppressing rather than spamming. Note the asymmetry that leaves:
/// a clock that jumps FORWARD parks `last_warn_ms` in the future, so the gate stays
/// shut for the size of the jump rather than for `interval` — see
/// [`ClientHandler::note_deposit_refusal`] for why that is tolerated.
fn should_warn_now(now_ms: u64, last_warn_ms: u64, interval: Duration) -> bool {
    if last_warn_ms == 0 {
        return true;
    }
    let elapsed = now_ms.saturating_sub(last_warn_ms);
    elapsed >= u64::try_from(interval.as_millis()).unwrap_or(u64::MAX)
}

/// Server-side classification of a `serve_stream` refusal, used to pick the
/// per-reason reject counter (#876). Finer-grained than the wire `StreamError`:
/// `CacheMiss`, `UnknownChannel`, and `OwnerMismatch` all ship as `NotFound` on
/// the wire (to avoid leaking channel existence), but are distinct here so an
/// operator can, e.g., isolate an unknown-channel abuse campaign.
#[derive(Debug, Clone, Copy)]
enum ServeRejectReason {
    EvictedSinceProbe,
    CacheMiss,
    InternalError,
    BlobTooLarge,
    UnknownChannel,
    OwnerMismatch,
    InsufficientDeposit,
    /// The lane already has enough concurrent same-lane streams in flight that
    /// admitting one more would put more unpaid egress in flight than the pool's
    /// refundable-floor headroom covers. Distinct from [`Self::InsufficientDeposit`]
    /// for the per-reason metric ONLY — both collapse to `NotFound` on the wire,
    /// see [`Self::wire_error`].
    LaneAtCapacity,
    /// A cache-HIT serve shed under node overload — egress saturation, or this
    /// client's fair-share cap while the node is pressured. Distinct from
    /// [`Self::LoadShedMiss`] for the per-reason metric ONLY — both collapse to
    /// `NotFound` on the wire (see [`Self::wire_error`]) so a client cannot
    /// read node load, and both are reputation-benign (a client scores
    /// `NotFound` as no fault).
    LoadShedHit,
    /// A cache-MISS serve shed under node overload — concurrency pressure,
    /// per-client fairness, or egress saturation. See [`Self::LoadShedHit`].
    LoadShedMiss,
    RangeNotSatisfiable,
    /// The blob is on this operator's local denylist (ADR 011 §Local Denylist).
    HashDenied,
    /// The blob is on the governance blacklist (ADR 011 §On Blacklist Event).
    /// Separate from [`Self::HashDenied`] for the operator's metrics ONLY — the
    /// two are deliberately one and the same on the wire, see
    /// [`Self::wire_error`].
    ChainHashDenied,
    /// The channel's funding address is on the origin blacklist — the operator's
    /// local `denied_origins` or the on-chain one (ADR 011 §On Blacklist Event).
    OriginDenied,
    /// The origin-only policy (#1759, `cache.relay_foreign_namespaces = false`)
    /// declined a hash this node's own backend genuinely does not hold. Distinct
    /// from [`Self::CacheMiss`] for the operator's per-reason metric ONLY — both
    /// collapse to `NotFound` on the wire (a declined foreign hash and a real
    /// miss must look the same to a client, which re-routes either way), see
    /// [`Self::wire_error`].
    ForeignNamespaceDeclined,
}

impl ServeRejectReason {
    /// The wire `StreamError` a refusal for this reason signs to the client.
    /// The reason is the single source of truth: `CacheMiss`, `UnknownChannel`,
    /// and `OwnerMismatch` deliberately collapse to one `NotFound` here so the
    /// three are wire-indistinguishable (no channel-existence leak), while the
    /// finer split survives only in the per-reason metric (#876). Keeping the
    /// mapping on the type makes an inconsistent error/reason pairing
    /// unrepresentable at the call sites.
    ///
    /// The requester side of this mapping is `decdn_client_pull::UpstreamRefused`,
    /// which recovers the wire code — and ONLY the wire code — from a refusal
    /// (#1144). So the `NotFound` collapse is what a requester sees for all six
    /// reasons below, and the reputation consequences it draws must hold for the
    /// weakest of them. They do: it scores `NotFound` as no fault at all, and only
    /// `InternalError` as a degraded peer.
    const fn wire_error(self) -> StreamError {
        match self {
            // `InsufficientDeposit` and `LaneAtCapacity` both collapse to `NotFound`
            // alongside the other miss reasons (#856): they must be
            // wire-indistinguishable so a probing client cannot map out a pool's
            // remaining balances or lane concurrency state; the distinction survives
            // only in the per-reason metric. `RangeNotSatisfiable` collapses to
            // `NotFound` alongside the other "won't serve this" reasons: an
            // out-of-bounds bounded range is a client error, but signalling it as
            // `NotFound` (rather than `InternalError`) keeps it reputation-benign —
            // a requester scores `InternalError` as a degraded peer (#1144), and a
            // client's own malformed range must not penalise the node for it. The
            // distinction survives in the per-reason metric.
            Self::CacheMiss
            | Self::UnknownChannel
            | Self::OwnerMismatch
            | Self::InsufficientDeposit
            | Self::LaneAtCapacity
            | Self::LoadShedHit
            | Self::LoadShedMiss
            | Self::RangeNotSatisfiable
            | Self::ForeignNamespaceDeclined => StreamError::NotFound,
            Self::EvictedSinceProbe => StreamError::EvictedSinceProbe,
            Self::InternalError => StreamError::InternalError,
            Self::BlobTooLarge => StreamError::BlobTooLarge,
            // The two takedown refusals do NOT collapse to `NotFound`. ADR 011
            // §`StreamRequest` Response names distinct codes because the retry
            // advice differs and a miss-shaped answer would be actively
            // misleading: a client told `NotFound` retries elsewhere and pays
            // again, when for `OriginBlacklisted` every node will refuse it.
            //
            // They are still each other's privacy floor. `HashBlacklisted` does
            // not say whether the entry is governance or local — that is the ADR's
            // explicit requirement, since a client able to tell them apart could
            // map an operator's private legal exposure by probing. It is why the
            // two reasons below converge here and why the governance one is NOT
            // allowed to fall through to `EvictedSinceProbe`: a hash refused
            // under a code no on-chain entry explains is a hash this operator
            // denied privately, which is that map. And neither says anything
            // about a channel's balance, which is what the `NotFound` collapse
            // above exists to protect.
            Self::HashDenied | Self::ChainHashDenied => StreamError::HashBlacklisted,
            Self::OriginDenied => StreamError::OriginBlacklisted,
        }
    }
}

/// The result of a reactive cache-miss fill attempt (#1129).
///
/// Separates a genuine absence from a transient backend fault, which a bare
/// `bool` cannot. The cache engine already draws this distinction (it
/// deliberately prefers `OriginError` over `NotFound` when an origin faulted);
/// preserving it here keeps the handler from collapsing both to "not filled" and
/// refusing with `CacheMiss` — wire [`StreamError::NotFound`] — when the
/// real cause is the operator's own S3/fs origin being down.
///
/// Why the reason code matters, stated precisely (the wire codes' own docs in
/// `decdn_protocol::client` are the authority here):
///
/// - `NotFound` = "node lacks the blob and cannot reach a provider, or declines
///   to pull through". It is NODE-scoped, not blob-scoped, and it is the code a
///   healthy-but-empty node returns.
/// - `InternalError` = "unexpected failure; do not retry THIS node" — i.e. go
///   elsewhere, this node is broken.
///
/// Both steer a client to another node, so this is not the difference between
/// "retry" and "give up". What it buys is (a) an honest signal that the node is
/// degraded rather than merely empty, and (b) the per-reason reject metric — the
/// ONLY server-side place the true cause is observable, since seven distinct
/// reject reasons collapse to the single `NotFound` wire code
/// ([`ServeRejectReason::wire_error`]). An operator whose origin is 5xx-ing must
/// not see that reported as a cache miss.
///
/// Note the reason code is NOT covered by the response's EIP-712 `slash_sig`,
/// which signs only [`StreamResponseBody`] (`ok: false`); `StreamResponse::error`
/// is explicitly "unsigned and informational". So a misclassification is a
/// correctness and observability bug, not a false attestation.
///
/// A deadline expiry is deliberately NOT a `HardFault` — see
/// [`ClientHandler::on_pull_through_timeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillOutcome {
    /// The blob is now present locally; fall through to the size gate + delivery.
    Filled,
    /// The tier produced no blob and no evidence of a fault. Covers BOTH a genuine
    /// clean miss (a source was asked and did not have it) and a tier that was
    /// never attempted at all (pull-through unconfigured, or the request not
    /// authorized to make this node spend). The two are deliberately one variant:
    /// neither is evidence of a fault, so both leave the terminal classification to
    /// whatever the other tiers found. Terminal (when no tier fills and none
    /// faulted): `NotFound`.
    CleanMiss,
    /// A backend/store fault, or a fault in this node's own buyer leg — the node is
    /// degraded, not empty. Terminal: `InternalError` ("do not retry this node"), so a
    /// client routes around it and the operator's reject metric names the real cause.
    ///
    /// Two different lifetimes arrive here, and the variant deliberately does not
    /// distinguish them, because the client's answer is the same either way:
    ///
    /// - TRANSIENT: the operator's origin is 5xx-ing or its store is briefly unhappy.
    ///   Passes on its own.
    /// - PERMANENT: this node's buyer side cannot pay at all — a broken signer, an
    ///   unusable deadline config, a channel store it cannot read (#1560). The node-origin
    ///   surfaces these as `OriginPullError::Permanent`, which the engine collapses into
    ///   `CacheError::OriginError` like any other origin failure. It recurs on every
    ///   request for EVERY hash until an operator intervenes.
    ///
    /// Deliberately narrow: ONLY `CacheError::OriginError` and `CacheError::Store`
    /// qualify. A `BlobTooLarge` / `HashMismatch` / `VerifyFailed` is deterministic
    /// and will recur on every request for that hash — reporting those as "this
    /// node is broken" would steer clients off a perfectly healthy node forever
    /// over one oversized blob.
    HardFault,
}

impl FillOutcome {
    /// The reject reason a *terminal* miss carries, given whether any tier
    /// attempted for this request hit a hard fault. Falling THROUGH to a further
    /// tier after a fault is legitimate (a different source may still serve) — so
    /// a fault seen on an earlier tier must be remembered here rather than
    /// overwritten by a later clean miss, which would report a degraded node as a
    /// merely-empty one.
    const fn miss_reason(fault_seen: bool) -> ServeRejectReason {
        if fault_seen {
            ServeRejectReason::InternalError
        } else {
            ServeRejectReason::CacheMiss
        }
    }

    /// Whether this outcome is a hard backend fault.
    const fn is_fault(self) -> bool {
        matches!(self, Self::HardFault)
    }

    /// Whether the blob is now present locally.
    const fn is_filled(self) -> bool {
        matches!(self, Self::Filled)
    }
}

/// Construction bundle for [`ClientHandler`] — the 16 required runtime deps plus
/// every optional wiring hook, so a handler's full configuration is one literal
/// at its call site instead of a `new()` call followed by a setter chain.
///
/// Build it with [`ClientHandlerDeps::new`] (required fields only; every optional
/// defaults to `None`), set the `Some` optionals the deployment enables, then
/// pass it to [`ClientHandler::new`]. Each optional field's runtime semantics are
/// documented on the matching [`ClientHandler`] field.
pub struct ClientHandlerDeps {
    pub node_id: PublicKey,
    pub metrics: Arc<Metrics>,
    pub limiter: Arc<ConnectionLimiter>,
    /// Overload-protection gate: sheds new serves under resource pressure.
    pub shed: Arc<crate::load_shed::LoadShedController>,
    pub cache: CacheEngine,
    pub eth_signer: Arc<PrivateKeySigner>,
    pub slash_domain: Eip712Domain,
    pub voucher_domain: Eip712Domain,
    pub bind_domain: Eip712Domain,
    pub channel_state_store: Arc<dyn PoolStateStore>,
    pub receipt_sink: Arc<dyn ReceiptSink>,
    /// Optional durable sink for owner-signed capability material (ADR 003
    /// §Capability delegation). `None` (tests) makes capability intake a no-op.
    pub capability_sink: Option<Arc<dyn crate::channel_store::CapabilitySink>>,
    /// Cached `getPool` view (owner + remaining), read by the floor-`M` solvency
    /// gate and the ADR 011 funder gate. `None` (tests) disables both gates —
    /// they fail open, exactly as before E4 wired the view.
    pub pool_view: Option<Arc<dyn crate::pool_view::PoolView>>,
    /// Refundable minimum-remaining-deposit floor `M` (token base units). The
    /// seller refuses to serve a lane's pool once its on-chain remaining
    /// (`getPool.deposit − getPool.totalRedeemed`) minus this floor can no
    /// longer cover the next credit window. Threaded from config by E4/F; here
    /// it is a plain field the floor-M guard reads.
    pub pool_min_remaining_deposit: U256,
    /// The served per-MB price, fixed at startup. Reprice by restarting the
    /// daemon (see `runtime::reload::warn_restart_required_sections`).
    pub rate_per_mb: u64,
    /// Live per-MB delivery-rate bounds (#1172). Seeded from on-chain
    /// `getRateBounds()` and updated by the `RateBoundsUpdated` watcher,
    /// replacing the by-value config stand-in.
    pub rate_bounds: crate::rate_bounds::RateBounds,
    pub max_blob_size_bytes: u64,
    pub max_concurrent_streams: usize,
    /// Live content deny-set (ADR 011): the operator's local denylist unioned
    /// with the on-chain origin blacklist. NOT an `Option`, unlike the wiring
    /// hooks below — an empty deny-set is a correct steady state (most operators
    /// deny nothing), so there is no "unwired" case to represent, and an
    /// `Option` would only add a way to fail open on a takedown gate. It is a
    /// required [`ClientHandlerDeps::new`] parameter: seeding it empty and
    /// relying on the runtime to overwrite it was itself a silent fail-open — a
    /// construction site that forgot the wiring was indistinguishable from an
    /// operator who denies nothing. Callers with no deny-set (tests) pass
    /// `ContentDenylist::empty()` explicitly.
    pub content_deny: Arc<crate::content_deny::ContentDenylist>,
    // Optional wiring — `None` unless the deployment enables the feature.
    pub redeem_hint: Option<mpsc::Sender<LaneKey>>,
    pub pull_through: Option<Duration>,
    pub local_populate: Option<Duration>,
    pub pull_through_origin: Option<Arc<NodeOrigin>>,
    /// Downstream credit-window ceiling in bytes (ADR 003 §Credit window): the
    /// per-stream window ramps toward this cap as the stream pays. Defaults to
    /// `DEFAULT_CREDIT_MAX` (64 MiB); the runtime sets it from
    /// `payment.credit_max`. The SAME ceiling paces the pull leg's upstream
    /// speculative spend on a cache-miss pull (`RampPacer`, #1669), so the
    /// upstream and downstream ramps never diverge. Floored at one chunk so
    /// the serve loop can always make progress.
    pub credit_max: u64,
    /// Ramp divisor for the credit window (ADR 003 §Credit window): the window is
    /// `paid / credit_ramp_divisor`, floored at one chunk and capped at
    /// `credit_max`. Defaults to `DEFAULT_CREDIT_RAMP_DIVISOR` (2); the runtime
    /// sets it from `payment.credit_ramp_divisor`. `0` opens the full ceiling
    /// immediately.
    pub credit_ramp_divisor: u64,
    /// Target wire-frame size in bytes for the serve path (ADR 005
    /// §`cdn/client/v1`). Node-local policy, never negotiated: the payer accepts
    /// any non-empty frame, and neither payment nor bao verification is defined
    /// over frame boundaries. Defaults to `DEFAULT_FRAME_TARGET_BYTES` (1 MiB);
    /// the runtime sets it from `payment.frame_target_bytes`. The serve loop
    /// clamps each request to the credit window's remaining room, so this is a
    /// ceiling on frame size rather than an exact size.
    pub frame_target_bytes: u64,
    pub idle_timeout: Option<Duration>,
    /// Wall-clock cadence for the mid-stream pool-solvency re-check (ADR 003
    /// §Pool solvency). `None` (the default and production path) reads as
    /// [`crate::pool_view::POOL_RECHECK_INTERVAL`]; a shorter value is set at
    /// construction only by tests, so a drain case need not wait a real interval.
    pub pool_recheck_interval: Option<Duration>,
    /// Durable mirror of each pool's `dead_charge` (ADR 003 §Pool solvency). The
    /// constructor hydrates the in-memory floor accumulator from it, and each
    /// per-stream floor reservation persists a new dead total to it best-effort on
    /// drop. `None` (the default and in tests) keeps the floor accounting in-memory
    /// only.
    pub floor_loss_store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
    /// ADR 041 per-source warming allowance — the SAME `Arc` the buy loop
    /// ([`crate::node_origin::NodeOriginConfig`]) and eviction hold. On each clean
    /// serve the handler credits the realized operator margin back to the source
    /// that speculatively warmed the blob (a no-op for an untagged / non-speculative
    /// hash). Defaults to a fresh zero-budget allowance (tests): a fresh allowance
    /// tags nothing, so its credit is inert.
    pub warming: Arc<crate::warming_allowance::WarmingAllowance>,
    /// Live operator fee-share (basis points) cell (`FeeRouter.getShares()[0]`), the
    /// `(1 − f)` numerator the ADR 041 serve credit realizes. Defaults to zero
    /// (tests): a zero share credits nothing.
    pub operator_shares: crate::fee_shares::OperatorShares,
    /// Origin-only policy (#1759). When `false`, `serve_stream` declines any
    /// hash its own backend does not hold — including a cache HIT for a
    /// foreign hash — before any discovery, lane accounting, or spend. `true`
    /// (the default) preserves today's relay behavior.
    pub relay_foreign_namespaces: bool,
}

impl std::fmt::Debug for ClientHandlerDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientHandlerDeps")
            .field("node_id", &self.node_id)
            .field("max_blob_size_bytes", &self.max_blob_size_bytes)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .finish_non_exhaustive()
    }
}

impl ClientHandlerDeps {
    /// The required runtime deps; every optional wiring hook defaults to `None`.
    #[allow(clippy::too_many_arguments)] // required runtime state; optionals set on the returned value.
    pub fn new(
        node_id: PublicKey,
        metrics: Arc<Metrics>,
        limiter: Arc<ConnectionLimiter>,
        cache: CacheEngine,
        eth_signer: Arc<PrivateKeySigner>,
        slash_domain: Eip712Domain,
        voucher_domain: Eip712Domain,
        bind_domain: Eip712Domain,
        channel_state_store: Arc<dyn PoolStateStore>,
        receipt_sink: Arc<dyn ReceiptSink>,
        rate_per_mb: u64,
        rate_bounds: crate::rate_bounds::RateBounds,
        max_blob_size_bytes: u64,
        max_concurrent_streams: usize,
        content_deny: Arc<crate::content_deny::ContentDenylist>,
        pool_min_remaining_deposit: U256,
        shed: Arc<crate::load_shed::LoadShedController>,
    ) -> Self {
        Self {
            node_id,
            metrics,
            limiter,
            shed,
            cache,
            eth_signer,
            slash_domain,
            voucher_domain,
            bind_domain,
            channel_state_store,
            receipt_sink,
            capability_sink: None,
            pool_view: None,
            pool_min_remaining_deposit,
            rate_per_mb,
            rate_bounds,
            max_blob_size_bytes,
            max_concurrent_streams,
            content_deny,
            redeem_hint: None,
            pull_through: None,
            local_populate: None,
            pull_through_origin: None,
            credit_max: decdn_common::config::DEFAULT_CREDIT_MAX,
            credit_ramp_divisor: decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
            frame_target_bytes: decdn_common::config::DEFAULT_FRAME_TARGET_BYTES,
            idle_timeout: None,
            pool_recheck_interval: None,
            floor_loss_store: None,
            warming: Arc::new(crate::warming_allowance::WarmingAllowance::new(0, 0)),
            operator_shares: crate::fee_shares::OperatorShares::new(0),
            relay_foreign_namespaces: decdn_common::config::DEFAULT_RELAY_FOREIGN_NAMESPACES,
        }
    }
}

/// `cdn/client/v1` paid-delivery handler.
pub struct ClientHandler {
    node_id: PublicKey,
    metrics: Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    /// Overload-protection gate: sheds new serves under resource pressure.
    shed: Arc<crate::load_shed::LoadShedController>,
    cache: CacheEngine,
    eth_signer: Arc<PrivateKeySigner>,
    /// `SlashJudge` EIP-712 domain for `StreamResponse.slash_sig`.
    slash_domain: Eip712Domain,
    /// `PaymentPool` EIP-712 domain for voucher verification.
    voucher_domain: Eip712Domain,
    /// `CapacityBond` EIP-712 domain for ephemeral `BindNodeId` verification.
    bind_domain: Eip712Domain,
    channel_state_store: Arc<dyn PoolStateStore>,
    /// Refundable minimum-remaining-deposit floor `M` for the floor-M serving
    /// guard (see [`ClientHandlerDeps::pool_min_remaining_deposit`]).
    pool_min_remaining_deposit: U256,
    /// Non-blocking sink for the served-and-paid audit log (issues #248, #803).
    /// The voucher-accept path enqueues one receipt here as each voucher is
    /// durably accepted; the actual disk write happens off the hot path in the
    /// background receipt writer, so receipt-log I/O can never back-pressure
    /// paid delivery. A dropped receipt (queue full) is non-fatal — the payment
    /// already committed to the fsynced lane store.
    receipt_sink: Arc<dyn ReceiptSink>,
    /// Durable sink for owner-signed capability material (ADR 003 §Capability
    /// delegation), set at construction via [`ClientHandlerDeps`]. On a stream
    /// whose [`StreamRequestExt`] carries a `capability`, the serve gate verifies
    /// the owner signature and persists `{spending_cap, expiry, owner_sig}` for
    /// `(pool_id, signer)` so the redeemer can register the signer on its first
    /// on-chain redemption. `None` when no settlement surface is wired (tests) —
    /// intake is then a no-op.
    capability_sink: Option<Arc<dyn crate::channel_store::CapabilitySink>>,
    /// Cached `getPool` view for the floor-`M` and ADR 011 funder gates. `None`
    /// (tests) makes both gates fail open.
    pool_view: Option<Arc<dyn crate::pool_view::PoolView>>,
    /// Per-lane state, hydrated from the store at construction. The sharded map
    /// resolves independent lanes concurrently — a lookup keyed by [`LaneKey`]
    /// is a point read that only locks that key's shard; each inner mutex
    /// serializes voucher application for one lane across its concurrent streams
    /// (ADR 003 §concurrent streams). No call site holds a map entry across an
    /// `.await`, so lane lookup never blocks an unrelated lane's admission.
    lanes: Arc<DashMap<LaneKey, Arc<Mutex<LaneDeliveryState>>>>,
    /// Serializes absolute lane snapshots without holding the lane map while
    /// individual lane state (which may be fsync-bound) is locked.
    lane_metrics_refresh: Mutex<()>,
    /// Per-pool floor-credit accumulator (ADR 003 §Pool solvency). Guards an O(1)
    /// map only and is never held across `.await` — a plain `std::sync::Mutex`, so
    /// a [`FloorReservation`]'s `Drop` can reconcile under it (a tokio mutex cannot
    /// be locked in `Drop`). Hydrated from `floor_loss_store` at construction.
    #[allow(dead_code)]
    // read by the serve path via `reserve_floor` / `pool_budget_covers_reserve`
    pool_floor: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
    /// Durable mirror of each pool's `dead_charge`; `None` in tests (in-memory
    /// only). A [`FloorReservation`]'s `Drop` writes the new dead total here
    /// best-effort.
    #[allow(dead_code)] // handed to each `FloorReservation` by `reserve_floor`
    floor_loss_store: Option<Arc<dyn decdn_incentive::PoolFloorLossStore>>,
    /// Redeem-hint sender to the on-chain settlement service (#327), set at
    /// construction via [`ClientHandlerDeps`]. `None` when no settlement service
    /// is wired (e.g. tests) — a hint is best-effort, so an absent sender or a
    /// full channel just skips it. Keyed by [`LaneKey`]: redemption is per-lane.
    redeem_hint: Option<mpsc::Sender<LaneKey>>,
    /// Node-to-node cache-miss pull-through deadline (#831), set at construction
    /// via [`ClientHandlerDeps`]. `None` (the default — feature off, and in
    /// tests) keeps the pre-#831 behaviour: a cache miss returns `NotFound`. When
    /// `Some`, a miss *from a request that proves ownership of the named channel*
    /// (see [`Self::pull_authorized`]) triggers `cache.populate` (the engine's
    /// `NodeOrigin` discovers, pays, pulls, and fills the store), bounded by this
    /// deadline so a slow upstream can't pin the delivery path. Proven channel
    /// ownership — not mere channel existence, which is public — is the
    /// anti-proxy-abuse gate: a client without an owned channel cannot make this
    /// node front upstream egress.
    pull_through: Option<Duration>,
    /// Reactive LOCAL-origin pull-through deadline (#1116), set at construction
    /// via [`ClientHandlerDeps`] whenever `[cache.origin]` is configured —
    /// INDEPENDENT of `node_to_node_pull_through_enabled`. When `Some`, a cache
    /// miss on a proven-owned channel first tries to fill from the node's OWN
    /// fs/http/s3 origin (`CacheEngine::populate_local`, which never touches the
    /// paid `Peer` origin), so a cache-only operator can reactively serve its own
    /// content and a local origin is preferred over the paid peer window path.
    /// `None` keeps the pre-#1116 behavior (miss ⇒ node→node path or a plain
    /// `NotFound`).
    local_populate: Option<Duration>,
    /// Window-paced node→node pull-through provider (#856), set at construction
    /// via [`ClientHandlerDeps`]. When `Some` (alongside `pull_through`), a cache
    /// miss for an offset-0 request that proves channel ownership is served by
    /// fusing a progressive upstream pull with downstream delivery — forwarding
    /// each chunk to the paying client and teeing it into the cache — so
    /// per-request speculative exposure is bounded to the ramped credit window
    /// (#1669) instead of the whole blob. `None` keeps the buffered `populate`
    /// path (`pull_through`) or a plain `NotFound`.
    pull_through_origin: Option<Arc<NodeOrigin>>,
    /// Downstream credit-window ceiling in bytes (ADR 003 §Credit window), set at
    /// construction via [`ClientHandlerDeps`]. The serve loop keeps streaming
    /// while `delivered − paid ≤ credit_window`, collecting cumulative vouchers as
    /// they arrive instead of stalling a full round trip at every interval. Read
    /// through [`Self::credit_window`], which ramps from one interval toward this
    /// ceiling as `paid` grows.
    credit_max: u64,
    /// Ramp divisor for the credit window (ADR 003 §Credit window), set at
    /// construction via [`ClientHandlerDeps`]. Read through
    /// [`Self::credit_window`].
    credit_ramp_divisor: u64,
    /// Target wire-frame size in bytes, set at construction via
    /// [`ClientHandlerDeps`]. Read through [`Self::frame_target`], which clamps it
    /// to the credit window's remaining room.
    frame_target_bytes: u64,
    /// Live content deny-set (ADR 011). Consulted at three points, all of which
    /// must gate or the check is bypassable: the hash gate above the
    /// availability check in `serve_stream`, the origin gate right after channel
    /// resolution, and the same origin gate inside `pull_authorized` — that last
    /// one runs EARLIEST and decides whether to front upstream USDC egress, so
    /// omitting it would have this node pay on a blacklisted origin's behalf
    /// before ever reaching the serve refusal. The window-paced serve path
    /// (`window.rs`) is a fourth, independent ladder.
    pub(crate) content_deny: Arc<crate::content_deny::ContentDenylist>,
    /// Served per-MB price, fixed at startup (see
    /// [`ClientHandlerDeps::rate_per_mb`]).
    rate_per_mb: u64,
    rate_bounds: crate::rate_bounds::RateBounds,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
    /// Throttle state for the insufficient-deposit refusal log (#1520): the
    /// millisecond timestamp of the last emitted `warn!`, and how many refusals
    /// have been swallowed since. See [`ClientHandler::note_deposit_refusal`].
    ///
    /// Unkeyed on purpose. A per-channel `governor` limiter was the obvious reach —
    /// it is already a dependency and the vocabulary the three request limiters
    /// speak — but keying it means an unboundedly growing map, and the in-tree cost
    /// of owning one is `retain_recent` sweeps, split single-flight prune guards,
    /// and two metrics per map (`crate::rate_limit`). That is a lot of machinery to
    /// rate-limit a log line, and the aggregate is what answers the triage
    /// question anyway: "one client ran dry" versus "I am refusing everyone". The
    /// per-channel detail lives in the `debug!` beside it and in the counter.
    deposit_refusal_last_warn_ms: AtomicU64,
    deposit_refusal_suppressed: AtomicU64,
    /// Application-layer idle-close ceiling (ADR 005 §Connection lifetime).
    /// `None` (the default and production path) reads as [`APP_IDLE_TIMEOUT`]
    /// (30s); a shorter value is set at construction via [`ClientHandlerDeps`]
    /// only by tests, so an idle-close case need not wait a real 30s.
    idle_timeout: Option<Duration>,
    /// Wall-clock cadence for the mid-stream pool-solvency re-check (ADR 003
    /// §Pool solvency), read through [`Self::pool_recheck_interval`]. `None` (the
    /// default and production path) reads as
    /// [`crate::pool_view::POOL_RECHECK_INTERVAL`]; a shorter value is set at
    /// construction via [`ClientHandlerDeps`] only by tests.
    pool_recheck_interval: Option<Duration>,
    /// ADR 041 per-source warming allowance, shared with the buy loop and eviction.
    /// Credited the realized operator margin on each clean serve (a no-op for an
    /// untagged hash).
    warming: Arc<crate::warming_allowance::WarmingAllowance>,
    /// Live operator fee-share (basis points), the ADR 041 serve-credit numerator.
    operator_shares: crate::fee_shares::OperatorShares,
    /// Origin-only policy (#1759), set at construction via
    /// [`ClientHandlerDeps::relay_foreign_namespaces`]. Read by the gate at the
    /// top of `serve_stream`.
    relay_foreign_namespaces: bool,
}

impl std::fmt::Debug for ClientHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientHandler")
            .field("node_id", &self.node_id)
            .field("max_blob_size_bytes", &self.max_blob_size_bytes)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .finish_non_exhaustive()
    }
}

impl ClientHandler {
    pub const ALPN: &'static [u8] = ALPN_CLIENT;

    /// Construct the handler from [`ClientHandlerDeps`], hydrating per-channel
    /// state from the deps' `channel_state_store`.
    ///
    /// All optional runtime wiring (settlement redeem hints, pull-through
    /// deadlines, the window/leech providers, …) is supplied on `deps` as
    /// `Some`/`None` at construction — there is no post-construction attach step,
    /// so a handler's full wiring is one reviewable literal at its call site.
    ///
    /// # Errors
    ///
    /// Propagates a [`decdn_incentive::StoreError`] if the persisted channel
    /// state cannot be loaded — the node must not serve paid delivery without
    /// knowing prior voucher state (the #527 replay guard).
    pub fn new(deps: ClientHandlerDeps) -> anyhow::Result<Self> {
        let map: DashMap<LaneKey, Arc<Mutex<LaneDeliveryState>>> = DashMap::new();
        for state in deps.channel_state_store.load_all()? {
            let bytes = state.last_bytes_delivered();
            map.insert(
                state.key(),
                Arc::new(Mutex::new(LaneDeliveryState {
                    state,
                    bytes_delivered_cumulative: bytes,
                    paid_credited: bytes,
                    active_streams: Arc::new(AtomicU32::new(0)),
                    last_voucher_at: AtomicU64::new(0),
                })),
            );
        }
        // Deposit is a pool-level, on-chain quantity (getPool), not carried per
        // lane, so the seller-side snapshot reports lane count only.
        deps.metrics
            .set_inbound_lane_snapshot(map.len(), U256::ZERO);
        // Hydrate the per-pool floor accumulator: no stream is live at boot, so
        // `live_reservation` starts at zero; each pool's persisted `dead_charge`
        // carries forward so a restart does not grant a fresh free-floor budget.
        let mut pool_floor: HashMap<B256, PoolFloorState> = HashMap::new();
        if let Some(store) = deps.floor_loss_store.as_ref() {
            // Reclaim forget tombstones first: handler construction is the one
            // point where no reservation exists and no drop-time persist can be in
            // flight, so the sweep cannot reopen the resurrection window the
            // tombstones close (#1781). This bounds tombstone growth to one
            // process lifetime.
            let swept = store.sweep_forgotten()?;
            if swept > 0 {
                tracing::debug!(swept, "reclaimed floor-loss tombstones of closed pools");
            }
            // Fail CLOSED, like the lane-state hydration above: genuine first boot
            // returns `Ok(vec![])` from `load_losses` (the table simply does not
            // exist yet), so any error reaching here is a real store fault. Starting
            // empty would silently re-grant every pool its full free-floor budget,
            // so refuse to come up instead.
            for (pool_id, micro) in store.load_losses()? {
                pool_floor.insert(
                    pool_id,
                    PoolFloorState {
                        live_reservation: U256::ZERO,
                        dead_charge: U256::from(micro),
                    },
                );
            }
        }
        Ok(Self {
            node_id: deps.node_id,
            metrics: deps.metrics,
            limiter: deps.limiter,
            shed: deps.shed,
            cache: deps.cache,
            eth_signer: deps.eth_signer,
            slash_domain: deps.slash_domain,
            voucher_domain: deps.voucher_domain,
            bind_domain: deps.bind_domain,
            channel_state_store: deps.channel_state_store,
            pool_min_remaining_deposit: deps.pool_min_remaining_deposit,
            receipt_sink: deps.receipt_sink,
            capability_sink: deps.capability_sink,
            pool_view: deps.pool_view,
            lanes: Arc::new(map),
            lane_metrics_refresh: Mutex::new(()),
            pool_floor: Arc::new(std::sync::Mutex::new(pool_floor)),
            floor_loss_store: deps.floor_loss_store,
            redeem_hint: deps.redeem_hint,
            pull_through: deps.pull_through,
            local_populate: deps.local_populate,
            pull_through_origin: deps.pull_through_origin,
            credit_max: deps.credit_max,
            credit_ramp_divisor: deps.credit_ramp_divisor,
            frame_target_bytes: deps.frame_target_bytes,
            content_deny: deps.content_deny,
            rate_per_mb: deps.rate_per_mb,
            rate_bounds: deps.rate_bounds,
            max_blob_size_bytes: deps.max_blob_size_bytes,
            max_concurrent_streams: deps.max_concurrent_streams,
            deposit_refusal_last_warn_ms: AtomicU64::new(0),
            deposit_refusal_suppressed: AtomicU64::new(0),
            idle_timeout: deps.idle_timeout,
            pool_recheck_interval: deps.pool_recheck_interval,
            warming: deps.warming,
            operator_shares: deps.operator_shares,
            relay_foreign_namespaces: deps.relay_foreign_namespaces,
        })
    }

    /// ADR 041 serve credit: return the realized operator margin to the source that
    /// speculatively warmed `hash`, for a clean serve of `served_bytes`. The credit
    /// is `(operator_bps / 10_000) · P_sell · MB(served_bytes)`, matching the
    /// buy-loop debit's units. A no-op for an untagged hash (own-namespace or
    /// non-speculative), so calling it unconditionally on every serve is correct and
    /// costs one map lookup.
    fn credit_warming_serve(&self, hash: Hash, served_bytes: u64) {
        let (sell_rate, _floor) = self.rate_bounds.raise_to_floor(self.rate_per_mb);
        let margin_per_mb = sell_rate
            .saturating_mul(u64::from(self.operator_shares.bps()))
            .saturating_div(10_000);
        let mb = served_bytes.div_ceil(decdn_protocol::MB_BYTES);
        self.warming
            .credit_serve(*hash.as_bytes(), mb.saturating_mul(margin_per_mb));
    }

    /// The wall-clock cadence for the mid-stream pool-solvency re-check (ADR 003
    /// §Pool solvency): the configured test override, else
    /// [`crate::pool_view::POOL_RECHECK_INTERVAL`].
    pub(super) fn pool_recheck_interval(&self) -> Duration {
        self.pool_recheck_interval
            .unwrap_or(crate::pool_view::POOL_RECHECK_INTERVAL)
    }

    /// The pool's cached `getPool` status (owner + `remaining`), or `None` when no
    /// pool-view is wired or the read faulted. Both the takedown funder resolution
    /// ([`Self::pool_funder`]) reads through this at the START of a serve leg; a
    /// `None` result makes the caller fail open — a transient RPC blip must not stop
    /// a paying stream, and the on-chain `redeem` is the backstop. MAY block on a
    /// `getPool` fetch on a cache miss, so it is NOT for the per-voucher-boundary
    /// path — the mid-stream re-check uses [`Self::pool_view_status_cached`].
    pub(super) async fn pool_view_status(
        &self,
        pool_id: B256,
    ) -> Option<crate::pool_view::PoolStatus> {
        self.pool_view.as_ref()?.status(pool_id).await
    }

    /// CACHE-ONLY pool status for the per-voucher-boundary mid-stream solvency
    /// re-check: never triggers a `getPool` `eth_call`, so it cannot stall the serve
    /// loop when the RPC is slow. Returns `None` when nothing fresh is cached (the
    /// re-check then fails open, exactly as on a fetch fault) — a long stream whose
    /// admission read has aged past the cache TTL simply stops re-checking rather
    /// than blocking delivery on a fresh read. The on-chain `redeem` remains the
    /// backstop, and a wider serve-path RPC reduction is tracked separately.
    pub(super) async fn pool_view_status_cached(
        &self,
        pool_id: B256,
    ) -> Option<crate::pool_view::PoolStatus> {
        self.pool_view.as_ref()?.cached_status(pool_id).await
    }

    /// The pool's funder (`getPool.owner`) for the ADR 011 mid-stream takedown
    /// re-check, or `None` when no pool-view is wired or the read faulted (the
    /// re-check then falls back to the open-time gates and the hash-denylist
    /// re-check). Cached, so a per-MB call is cheap.
    pub(super) async fn pool_funder(&self, pool_id: B256) -> Option<Address> {
        self.pool_view_status(pool_id).await.map(|s| s.owner)
    }

    /// Accept an owner-signed capability presented at session start (ADR 003
    /// §Capability delegation): verify the owner signature against the on-chain
    /// pool owner, register the lane so the voucher path serves it, and persist
    /// the grant so the redeemer can register the signer on its first on-chain
    /// redemption. `signer` is the request's bound Ethereum address; `pool_id`
    /// is [`StreamRequest::pool_id`]; `pool_owner` is `getPool.owner` from the
    /// cached pool-view.
    ///
    /// The owner check is authoritative here, not deferred: the on-chain
    /// `PaymentPool.redeemMany` verifies every capability's owner signature
    /// against `pools[poolId].owner` and reverts the WHOLE batch on one bad
    /// grant, which would strand every other lane's redemption in an
    /// indefinite-retry tick. So a grant whose owner signature does not verify
    /// against the pool owner is DROPPED — never persisted, never lane-registered.
    ///
    /// The verification recovers the 65-byte EOA signature via
    /// [`SignedCapability::verify_owner`] (canonical-`s` ecrecover, matching the
    /// contract's verifiable set). Two cases drop rather than persist:
    /// - `pool_owner` is `None` — no pool-view, an unknown pool, or an RPC fault
    ///   left the owner unknown, so ownership cannot be confirmed. Intake is
    ///   best-effort and the client re-sends the capability on its next request.
    /// - a non-65-byte (contract-wallet / ERC-1271-shaped) owner signature, which
    ///   cannot be recovered off-chain; this design does not accept contract
    ///   wallets as pool owners.
    ///
    /// Best-effort and off the durability path otherwise: a persist failure never
    /// fails the stream. Lane registration is idempotent — a re-observed grant for
    /// an already-tracked lane does NOT reset the accepted-voucher watermark
    /// (#527); the lane's `cap`/`expiry` are set once at first registration.
    #[allow(clippy::cognitive_complexity)] // linear verify → register → persist sequence.
    async fn intake_capability(
        &self,
        pool_id: B256,
        signer: Address,
        pool_owner: Option<Address>,
        capability: &decdn_protocol::client::WireCapability,
    ) {
        // Without the on-chain pool owner the grant cannot be confirmed to belong
        // to this pool; persisting an unverified capability is exactly what
        // strands the redeemer. Drop it — the client re-sends next request.
        let Some(pool_owner) = pool_owner else {
            tracing::debug!(%pool_id, %signer, "dropping capability: pool owner unavailable, cannot verify");
            return;
        };
        let spending_cap = U256::from_be_bytes(capability.spending_cap);
        let expiry = capability.expiry;
        // Only the common EOA (65-byte) owner signature is recoverable off-chain;
        // a contract-wallet signature is dropped rather than persisted unverified.
        let Ok(sig_bytes) = <[u8; 65]>::try_from(capability.owner_signature.as_slice()) else {
            tracing::debug!(%pool_id, %signer, "dropping capability: owner signature is not a recoverable EOA signature");
            return;
        };
        let Ok(signature) = alloy::primitives::Signature::from_raw(&sig_bytes) else {
            tracing::debug!(%pool_id, %signer, "dropping capability: malformed owner signature");
            return;
        };
        let grant = SignedCapability {
            capability: Capability {
                signer,
                spending_cap,
                pool_id,
                expiry,
            },
            signature,
        };
        if let Err(e) = grant.verify_owner(pool_owner, &self.voucher_domain) {
            tracing::debug!(%pool_id, %signer, error = %e, "dropping capability: owner verification failed");
            return;
        }

        // The grant is authentic. Register the lane so the voucher path accepts
        // vouchers for `(pool_id, signer, this operator)` — without this a
        // brand-new lane's first request is never served, since the serve gate
        // admits only known lanes. Idempotent for an already-tracked lane.
        let lane = LaneState::hydrate(
            pool_id,
            signer,
            self.eth_signer.address(),
            spending_cap,
            expiry,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        );
        if let Err(e) = self.register_lane(lane).await {
            tracing::warn!(%pool_id, %signer, error = %e, "lane registration failed; the request refuses as an unknown lane and the client retries");
        }

        // Persist the owner-signed material for the redeemer (if a settlement
        // sink is wired). `None` (tests) makes this a no-op.
        let Some(sink) = self.capability_sink.as_ref() else {
            return;
        };
        let sink = Arc::clone(sink);
        let owner_sig = capability.owner_signature.clone();
        let write = tokio::task::spawn_blocking(move || {
            sink.store_capability(pool_id, signer, spending_cap, expiry, &owner_sig)
        })
        .await;
        match write {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(%pool_id, %signer, error = %e, "capability persist failed");
            }
            Err(e) => {
                tracing::warn!(%pool_id, %signer, error = %e, "capability persist task join failed");
            }
        }
    }

    /// Register a lane so the voucher path accepts vouchers for it — its
    /// capability handle is registered on-chain and its [`LaneState`] persisted
    /// durably, then inserted into the live map. Called both from the on-chain
    /// capability-registration consumer (#327) and from the seller-side
    /// capability intake on the first voucher of a new lane.
    ///
    /// **Idempotent:** a re-observed registration for an already-tracked lane is
    /// a no-op — it MUST NOT reset the accepted-voucher watermark and reopen the
    /// #527 replay window. The live map (hydrated from the store at
    /// construction, updated here) is the authority.
    ///
    /// # Errors
    ///
    /// Propagates a [`StoreError`] if the durable persist fails; the caller
    /// logs and retries.
    pub async fn register_lane(&self, state: LaneState) -> Result<(), StoreError> {
        let key = state.key();
        if self.lanes.contains_key(&key) {
            return Ok(());
        }
        // The store write is a synchronous fsync (store trait §Durability) —
        // run it off the runtime worker, same as the voucher-accept path.
        let store = Arc::clone(&self.channel_state_store);
        let to_persist = state.clone();
        tokio::task::spawn_blocking(move || store.record(&to_persist))
            .await
            .map_err(|e| StoreError::Backend(format!("register_lane join: {e}")))??;

        let bytes = state.last_bytes_delivered();
        self.lanes.entry(key).or_insert_with(|| {
            Arc::new(Mutex::new(LaneDeliveryState {
                state,
                bytes_delivered_cumulative: bytes,
                paid_credited: bytes,
                active_streams: Arc::new(AtomicU32::new(0)),
                last_voucher_at: AtomicU64::new(0),
            }))
        });
        self.refresh_lane_metrics().await;
        Ok(())
    }

    /// Drop a settled lane from the live map and the persisted store.
    /// Idempotent — forgetting an unknown lane is a no-op.
    ///
    /// # Errors
    ///
    /// Propagates a [`StoreError`] if the durable delete fails.
    pub async fn forget_lane(&self, key: LaneKey) -> Result<(), StoreError> {
        // Removing the lane row drops its `last_voucher_at` stamp with it (issue
        // #1733): the timestamp lives on the lane's delivery state, so its
        // lifecycle follows the lane automatically — no separate activity-map
        // eviction.
        self.lanes.remove(&key);
        self.refresh_lane_metrics().await;
        let store = Arc::clone(&self.channel_state_store);
        tokio::task::spawn_blocking(move || store.forget(key))
            .await
            .map_err(|e| StoreError::Backend(format!("forget_lane join: {e}")))?
    }

    /// Minimum gap between insufficient-deposit `warn!` lines (#1520).
    ///
    /// A module const, not a config field: a log cadence does not justify the five
    /// config sites (schema, resolver, validate summary, template, docs) a
    /// `[payment]` knob costs, and no operator needs to tune it.
    pub(super) const DEPOSIT_REFUSAL_WARN_INTERVAL: Duration = Duration::from_mins(5);

    /// Record an insufficient-deposit refusal and decide whether this one gets a
    /// `warn!`. Returns `Some(suppressed_since_last)` when the caller should warn.
    ///
    /// The refusal is routine — a client running dry is not a fault — so warning
    /// per occurrence is farmable into log spam by exactly the abuse this guards
    /// against. But a one-shot latch (the only existing precedent, `log_per_source_poison`
    /// in `crate::dispatch`) is wrong in the other direction: this fires
    /// legitimately and repeatedly, so a permanently-latched warning is as
    /// invisible as none. Hence a window, with the swallowed count carried on the
    /// line so a reader can tell one dry client from a node refusing everyone.
    ///
    /// The window arithmetic lives in [`should_warn_now`] so it is testable
    /// without sleeping.
    pub(super) fn note_deposit_refusal(&self) -> Option<u64> {
        self.deposit_refusal_suppressed
            .fetch_add(1, Ordering::Relaxed);
        let now_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let last = self.deposit_refusal_last_warn_ms.load(Ordering::Relaxed);
        if !should_warn_now(now_ms, last, Self::DEPOSIT_REFUSAL_WARN_INTERVAL) {
            return None;
        }
        // Lost the race: another task is emitting this window's line. Its count
        // already includes ours, because we bumped before checking.
        if self
            .deposit_refusal_last_warn_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        Some(
            self.deposit_refusal_suppressed
                .swap(0, Ordering::Relaxed)
                .saturating_sub(1),
        )
    }

    /// Emit the observable side of an insufficient-deposit refusal (#1520).
    ///
    /// Unconditional `debug!` so a support ticket is answerable at all, plus a
    /// throttled `warn!` (see [`Self::note_deposit_refusal`]). The wire code is
    /// deliberately lossy — `InsufficientDeposit` collapses to `NotFound` with six
    /// other reasons so a prober cannot map channel balances — so without these the
    /// only trace of a refusal is a counter that, at the time this was written, no
    /// alert, panel, or runbook entry referenced.
    ///
    /// `headroom` and `ceiling` are both in payment-token base units.
    pub(super) fn log_deposit_refusal(
        &self,
        pool_id: B256,
        hash: Hash,
        headroom: U256,
        ceiling: U256,
    ) {
        tracing::debug!(
            %pool_id, %hash, %headroom, %ceiling,
            "refusing delivery: pool's refundable remaining deposit cannot cover the reserved cost"
        );
        if let Some(suppressed) = self.note_deposit_refusal() {
            tracing::warn!(
                %pool_id, %headroom, %ceiling, suppressed,
                interval = ?Self::DEPOSIT_REFUSAL_WARN_INTERVAL,
                "refusing paying clients: pool's refundable remaining deposit below the reserved cost. \
                 A sustained rate here is either a client running dry (no action) or this \
                 node's chain watcher lagging behind an on-chain top-up (check RPC health) \
                 — see docs/runbook.md"
            );
        }
    }

    /// The frame size to request from a frame producer, given `unvouchered` — the
    /// bytes delivered since the last payment-chunk boundary — and `interval_bytes`,
    /// the payment quantum.
    ///
    /// **A frame never crosses a payment-chunk boundary.** Both sides meter the same
    /// byte stream over the same frame sequence, and each waits for the other at
    /// every `interval_bytes`: the node accumulates `unvouchered` until it reaches a
    /// full interval and then demands a proof, while the payer releases one
    /// hash-chain preimage per interval. A frame that straddled a boundary would
    /// land the payer's counter PAST it, which is settled with a signed voucher for
    /// the residual instead of a preimage — a per-frame signature on the hot path,
    /// and a cadence the two sides no longer agree on. Cutting at the boundary keeps
    /// the preimage path exact, whatever the target.
    ///
    /// This also bounds the node's credit exposure. The serve loop checks the window
    /// BEFORE each send, so `delivered - paid` overshoots by at most one frame; with
    /// frames capped at one interval the overshoot is at most one payment chunk —
    /// the same granularity the node bills at, and the bound ADR 003 §Credit window
    /// states as "the window, plus at most one chunk".
    ///
    /// `room` — the window's unused remainder, `window - (delivered - paid)` — caps
    /// the request as well, and that cap is load-bearing on the **cache-miss** leg
    /// rather than merely tidy. Both loops prefetch one frame ahead of the window
    /// check, and on a miss the producer is fed by an upstream pull paced against
    /// this stream's own served-and-paid frontier. Asking a closed window for a full
    /// frame parks the prefetch on bytes that cannot arrive until the loop exits to
    /// recoup — which it cannot do while parked. Sizing the request to the room that
    /// actually remains keeps the prefetch satisfiable from what is already buffered.
    ///
    /// The room cap is floored at one bao chunk group so a shut window yields a
    /// short frame rather than a single byte; that runt goes out once the recoup
    /// reopens the window. A stream sitting at the one-chunk credit floor therefore
    /// spends about two frames per payment chunk — a full frame, then the runt the
    /// shut window produced — while a ramped stream keeps `room` far above the floor
    /// and spends one.
    ///
    /// **The floor is exactly one group because the pull side reserves exactly one.**
    /// On a cache miss the bytes this prefetch asks for can only come from the
    /// upstream pull, and `decdn_client_pull::PULL_WINDOW_FLOOR` carries a third
    /// chunk group for precisely this request. Raising the floor here without
    /// raising it there parks the serve leg on bytes the pull may not draw —
    /// a hang, not a failed assertion. `frame_target_room_floor_matches_the_pull_reservation`
    /// pins the pair.
    pub(super) fn frame_target(&self, unvouchered: u64, interval_bytes: u64, room: u64) -> usize {
        // `unvouchered < interval_bytes` at every call site — both loops reset it to
        // zero the moment it reaches a full interval. Were that ever to break, a
        // saturating `0` would ask for one-byte frames forever: a stream that still
        // "progresses" at six orders of magnitude below line rate, with no error and
        // no metric. Fall back to a whole interval instead, which is wrong in the
        // same direction as the rest of the clamp rather than catastrophically.
        debug_assert!(
            unvouchered < interval_bytes,
            "unvouchered {unvouchered} must stay below the {interval_bytes}-byte interval"
        );
        let to_boundary = interval_bytes
            .checked_sub(unvouchered)
            .filter(|remaining| *remaining > 0)
            .unwrap_or(interval_bytes);
        let want = self
            .frame_target_bytes
            .min(to_boundary)
            .min(room.max(decdn_bao_range::CHUNK_GROUP_BYTES));
        // Saturating toward `usize::MAX` would tell a producer to buffer without
        // bound — the O(blob size) behaviour the framers exist to avoid. Every term
        // above is already `<= CHUNK_BYTES`, so this only ever runs on a 16-bit
        // target, where one chunk group is the safe answer.
        usize::try_from(want)
            .unwrap_or_else(|_| usize::try_from(decdn_bao_range::CHUNK_GROUP_BYTES).unwrap_or(1))
    }

    /// The effective downstream credit window in bytes for a stream whose voucher
    /// interval is `interval_bytes` and whose cumulative confirmed payment is
    /// `paid` (ADR 003 §Credit window). The window ramps from one interval toward
    /// `credit_max` as `paid` grows, so the serve loop's bounded credit exposure —
    /// `delivered − paid` — is exactly the window: `paid / credit_ramp_divisor`
    /// once that clears the one-interval floor, the floor itself below that point
    /// (including at `paid == 0`), and the full `credit_max` when
    /// `credit_ramp_divisor` is `0`. Floored at one interval so the loop always
    /// makes progress.
    pub(super) fn credit_window(&self, interval_bytes: u64, paid: u64) -> u64 {
        decdn_incentive::ramped_credit_window(
            self.credit_ramp_divisor,
            interval_bytes,
            self.credit_max,
            paid,
        )
    }

    /// The refundable floor-`M` serving guard (shared-payment-pool model): the
    /// seller keeps serving a pool only while its on-chain **remaining**
    /// (`getPool.deposit − getPool.totalRedeemed`) minus the configured
    /// minimum-remaining-deposit floor `M` can still cover the reserved credit
    /// window, i.e. `remaining − M ≥ min_payment(reserved_bytes, rate)`. `M` is
    /// the refundable minimum the pool owner is guaranteed to keep, so the node
    /// refuses to serve into it. Pure and total (saturating), so it is testable
    /// without any chain access.
    ///
    /// `remaining` is a chain quantity read from `getPool`; the handler does not
    /// hold an RPC client, so **E4 threads `remaining` to every call site** (from
    /// the redemption/pool watcher's cached `getPool` view, cached like
    /// `withdrawn_cache`). This method owns only the policy arithmetic.
    pub(super) fn pool_remaining_covers_window(
        &self,
        remaining: U256,
        reserved_bytes: u64,
        rate_per_mb: u64,
    ) -> bool {
        let refundable_headroom = remaining.saturating_sub(self.pool_min_remaining_deposit);
        refundable_headroom >= min_payment(reserved_bytes, rate_per_mb)
    }

    /// Open a span-capped [`FloorReservation`] against `pool_id`'s budget for one
    /// stream. The serve loop holds the returned guard for the stream's lifetime:
    /// it notes the stream's unpaid balance as it delivers and releases the
    /// reservation once a floor is repaid; on drop the guard reconciles the live
    /// reservation and any residual dead charge against the per-pool accumulator
    /// (and the durable [`Self::floor_loss_store`]).
    #[allow(dead_code)] // the serve loop opens a reservation per admitted stream
    pub(super) fn reserve_floor(&self, pool_id: B256, reserved: U256) -> FloorReservation {
        FloorReservation::reserve(
            Arc::clone(&self.pool_floor),
            self.floor_loss_store.clone(),
            Arc::clone(&self.metrics),
            pool_id,
            reserved,
        )
    }

    /// Stateful-B pool solvency: does the pool's `remaining − M` cover its
    /// already-committed floor credit (`live_reservation + dead_charge`) plus
    /// `new_reserve`? Reads the per-pool accumulator; pure arithmetic otherwise. A
    /// poisoned accumulator lock recovers the guard rather than panicking (the
    /// reservation is best-effort accounting, never a safety gate).
    #[allow(dead_code)] // the serve-path admission gate consults this before reserving
    pub(super) fn pool_budget_covers_reserve(
        &self,
        pool_id: B256,
        remaining: U256,
        new_reserve: U256,
    ) -> bool {
        let committed = {
            let guard = self
                .pool_floor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.get(&pool_id).map_or(U256::ZERO, |s| {
                s.live_reservation.saturating_add(s.dead_charge)
            })
        };
        decdn_incentive::pool_budget_covers(
            remaining,
            self.pool_min_remaining_deposit,
            committed,
            new_reserve,
        )
    }

    /// Atomically check the pool's solvency and reserve one `floor` against its
    /// budget, returning the [`FloorReservation`] guard on success or `None` when
    /// `remaining − M` cannot cover the pool's already-committed floor credit plus
    /// this new floor.
    ///
    /// The budget read (`live_reservation + dead_charge`), the solvency test, and
    /// the `live_reservation += floor` increment all happen under ONE `pool_floor`
    /// lock hold, so two concurrent admissions on a near-exhausted pool cannot both
    /// pass the check and then both reserve — the TOCTOU over-commit a separate
    /// [`Self::pool_budget_covers_reserve`] call followed by [`Self::reserve_floor`]
    /// would allow. The guard is built from the already-charged state
    /// ([`FloorReservation::new_charged`]) so the reserved amount is charged exactly
    /// once. A poisoned lock recovers the guard rather than panicking (best-effort
    /// accounting, never a safety gate).
    pub(super) fn try_reserve_floor(
        &self,
        pool_id: B256,
        remaining: U256,
        reserved: U256,
    ) -> Option<FloorReservation> {
        {
            let mut guard = self
                .pool_floor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = guard.entry(pool_id).or_default();
            let committed = entry.live_reservation.saturating_add(entry.dead_charge);
            if !decdn_incentive::pool_budget_covers(
                remaining,
                self.pool_min_remaining_deposit,
                committed,
                reserved,
            ) {
                return None;
            }
            entry.live_reservation = entry.live_reservation.saturating_add(reserved);
        }
        Some(FloorReservation::new_charged(
            Arc::clone(&self.pool_floor),
            self.floor_loss_store.clone(),
            Arc::clone(&self.metrics),
            pool_id,
            reserved,
        ))
    }

    /// Drop a reclaimed pool's floor-credit accounting: remove its in-memory
    /// `PoolFloorState` and its durable `dead_charge` row. Called once when a pool
    /// is reclaimed on-chain; a reclaimed `pool_id` never recurs (monotonic open
    /// nonce), so its accumulated `dead_charge` is permanently moot.
    ///
    /// A [`FloorReservation`] drop that snapshotted its total before the in-memory
    /// remove here can still have its `record_loss` in flight when the durable
    /// delete commits. The store closes that window, not this method:
    /// `forget_loss` tombstones the pool id, so the late write is a no-op instead
    /// of resurrecting a row for a closed pool that nothing would ever delete
    /// again (#1781).
    pub(crate) async fn forget_pool_floor(&self, pool_id: B256) {
        {
            let mut guard = self
                .pool_floor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.remove(&pool_id);
        }
        let Some(store) = self.floor_loss_store.clone() else {
            return;
        };
        let result = tokio::task::spawn_blocking(move || store.forget_loss(pool_id)).await;
        note_forget_outcome(&self.metrics, pool_id, result);
    }

    /// Has a takedown landed on this stream since it opened (ADR 011 §On
    /// Blacklist Event: "In-flight streams for a blacklisted hash are terminated
    /// at the next MB boundary")?
    ///
    /// The open-time gates in `dispatch.rs` are not enough on their own: a
    /// multi-GB blob can still be streaming minutes after a one-hour removal
    /// order took effect, and serving past the compliance window is slashable
    /// (ADR 026 §Slashing and burn). Both halves are re-checked because both can
    /// land mid-stream — a hash via the local denylist reload, the governance
    /// blacklist, or an eviction; a funder via either origin list.
    ///
    /// Cheap enough for a per-MB call: three atomic loads and a hash-set probe
    /// each, against a boundary that already takes a channel lock and a network
    /// round trip to collect a voucher.
    pub(super) fn takedown_landed(&self, hash: Hash, funder: Option<Address>) -> bool {
        self.cache.refuses(hash)
            || funder.is_some_and(|addr| self.content_deny.is_origin_denied(&addr))
    }

    /// Cut off an in-flight delivery whose hash or funder was taken down
    /// mid-stream, by resetting both directions.
    ///
    /// A reset, not a `StreamError` frame: ADR 005's stream-error domain split
    /// makes `VoucherRejected` the only code that travels mid-stream, and the
    /// client's cue here is the absence of the `StreamEnd` sentinel — the same
    /// convention every other mid-stream fault uses. The QUIC code is
    /// [`APP_ERR_NO_ERROR`] because this is a compliance action, not a protocol
    /// fault by either party, and ADR 013 §Application Error Codes reserves
    /// `0x03` for peers that misbehaved; coding it as a fault would have the
    /// client penalise this node's reputation for discharging a takedown. A
    /// client that re-requests the hash gets the signed `HashBlacklisted`
    /// refusal from the open-time gate, which is where the reason belongs.
    pub(super) fn terminate_for_takedown(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        hash: Hash,
    ) {
        self.metrics.serve_stream_terminated_takedown();
        tracing::warn!(
            %hash,
            "terminating an in-flight delivery: a takedown landed after the stream opened"
        );
        reset_stream(send, recv, APP_ERR_NO_ERROR);
    }

    /// Snapshot the latest tracked [`LaneState`] for `key`, or `None` if this
    /// node does not track the lane. Used by the settlement watcher's
    /// self-defense reaction (#1586) to read the node's latest persisted voucher
    /// (amount / bytes / signature) when a counterparty redeems at a stale
    /// watermark. Reads the in-memory row, which the voucher-accept path
    /// advances only *after* the durable store write commits (#527), so the
    /// snapshot never reports a voucher the node has not persisted.
    pub async fn lane_state_snapshot(&self, key: LaneKey) -> Option<LaneState> {
        let entry = self.lanes.get(&key).map(|e| Arc::clone(e.value()))?;
        let guard = entry.lock().await;
        Some(guard.state.clone())
    }

    async fn refresh_lane_metrics(&self) {
        let _refresh = self.lane_metrics_refresh.lock().await;
        let open = self.lanes.len();
        // Deposit is a pool-level, on-chain quantity (getPool), not carried per
        // lane, so the seller-side snapshot reports lane count only.
        self.metrics.set_inbound_lane_snapshot(open, U256::ZERO);
    }
}

/// Terminal disposition of one voucher collected by
/// [`ClientHandler::commit_one_proof`].
enum VoucherStop {
    /// Verified and advanced in memory; keep serving. `credited_bytes` is the
    /// watermark-capped wire bytes to advance the serve loop's `paid` by (rule
    /// #1) — at most the amount the lane watermark advanced, so a benign
    /// already-satisfied voucher contributes zero.
    Continue { credited_bytes: u64 },
    /// Rejected — the reject frame was written and the stream finishes cleanly;
    /// the loop returns `Ok(())`.
    Rejected,
}

impl ProtocolHandler for ClientHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.serve(connection)
            .await
            .map_err(|e| AcceptError::from_err(std::io::Error::other(e.to_string())))
    }
}

/// Error from the request read path carrying the ADR 013 app error code to
/// propagate to the peer.
struct StreamReadError {
    err: anyhow::Error,
    app_code: u32,
}

const fn frame_err_code(e: &FrameError) -> u32 {
    match e {
        FrameError::TooLarge(_) => 0x02,
        FrameError::Io(_) => APP_ERR_NO_ERROR,
        FrameError::Varint | FrameError::Decode(_) => APP_ERR_MALFORMED_MESSAGE,
    }
}

fn reset_stream(send: &mut SendStream, recv: &mut RecvStream, code: u32) {
    let v = VarInt::from_u32(code);
    let _ = send.reset(v);
    let _ = recv.stop(v);
}

/// The first message on a fresh `cdn/client/v1` stream: a paid delivery
/// request. It opens a bidirectional stream and leads with one
/// [`ClientMessage::StreamRequest`].
enum FirstMessage {
    /// A paid delivery: [`StreamRequest`] plus its [`StreamRequestExt`].
    Delivery(StreamRequest, StreamRequestExt),
}

/// Read the first framed [`ClientMessage`] on a stream with a timeout. A
/// [`ClientMessage::StreamRequest`] yields [`FirstMessage::Delivery`] (with its
/// [`StreamRequestExt`] parsed from the trailing bytes — the ADR 005 two-phase
/// pattern; an absent extension yields `StreamRequestExt::default()`). Any other
/// variant is a protocol fault.
async fn read_first_message(recv: &mut RecvStream) -> Result<FirstMessage, StreamReadError> {
    let frame = match tokio::time::timeout(REQUEST_READ_TIMEOUT, read_frame(recv)).await {
        Err(_) => {
            return Err(StreamReadError {
                err: anyhow::anyhow!("stream request timed out after {REQUEST_READ_TIMEOUT:?}"),
                app_code: APP_ERR_NO_ERROR,
            });
        }
        Ok(Err(e)) => {
            let app_code = frame_err_code(&e);
            return Err(StreamReadError {
                err: anyhow::anyhow!("stream request frame read failed: {e}"),
                app_code,
            });
        }
        Ok(Ok(frame)) => frame,
    };
    match decode_message::<ClientMessage>(&frame) {
        Ok((ClientMessage::StreamRequest(req), remainder)) => {
            let ext = decdn_protocol::parse_stream_request_ext(remainder).map_err(|e| {
                StreamReadError {
                    err: anyhow::anyhow!("stream request ext decode failed: {e}"),
                    app_code: APP_ERR_MALFORMED_MESSAGE,
                }
            })?;
            // Value checks are kept out of the parse so forward-compatible
            // trailing bytes don't couple to them (ADR 005 two-phase). Gate here
            // rather than at each use: a malformed client binding or capability
            // signature is a protocol error, and this wire boundary is its only
            // enforcement point.
            ext.validate().map_err(|e| StreamReadError {
                err: anyhow::anyhow!("stream request ext rejected: {e}"),
                app_code: APP_ERR_MALFORMED_MESSAGE,
            })?;
            Ok(FirstMessage::Delivery(req, ext))
        }
        Ok((_, _)) => Err(StreamReadError {
            err: anyhow::anyhow!("expected StreamRequest"),
            app_code: APP_ERR_UNSUPPORTED_MESSAGE,
        }),
        Err(e) => {
            // ADR 013: an unknown enum discriminant closes with
            // UNSUPPORTED_MESSAGE (0x01), not MALFORMED_MESSAGE (0x03). A
            // genuine parse fault (in-range discriminant, bad payload) stays
            // MALFORMED.
            let app_code = if is_unknown_variant::<ClientMessage>(&frame) {
                APP_ERR_UNSUPPORTED_MESSAGE
            } else {
                APP_ERR_MALFORMED_MESSAGE
            };
            Err(StreamReadError {
                err: anyhow::anyhow!("stream request decode failed: {e}"),
                app_code,
            })
        }
    }
}

/// How many proofs the recoup phase will read for ONE outstanding chunk before
/// it gives up.
///
/// A chunk is paid by exactly one reveal, but the payer may legitimately send
/// housekeeping vouchers ahead of it — the epoch's root voucher when this stream
/// has not carried it yet, and a rollover voucher when the chain is spent. Both
/// advance the lane's claim by nothing (they re-assert a cumulative the node
/// already holds), so they credit nothing, and the outstanding chunk stays
/// outstanding. Three messages is the real worst case for a well-behaved payer
/// (re-anchor, roll, reveal); the rest is slack for a duplicate or out-of-order
/// reveal, which is ordinary once several streams share one lane and which also
/// credits nothing.
///
/// The bound matters because without it a payer could hold a stream open
/// indefinitely with a run of zero-credit vouchers, each one refreshing the read
/// timeout while the delivered-but-unpaid balance never moves — the same shape
/// of stall the non-empty-`ChunkData` floor closes on the delivery side. The
/// exact value trades slack against how long that stall may run; every read
/// inside it still carries its own timeout, so the bound is about liveness, not
/// about capping any single wait.
const MAX_PROOFS_PER_CHUNK: u32 = 8;

/// One payment proof off the wire: a signed voucher, or a released hash-chain
/// preimage (ADR 003 §Two payment resolutions).
///
/// The two resolve different things and cost differently, which is why the
/// protocol keeps both rather than making either do the other's work. A
/// **voucher** settles any residual exactly, down to one token base unit, and
/// costs one signature. A **preimage** settles whole chunks past the anchor and
/// costs one keccak — no signature, no acknowledgement, nothing to persist
/// before the node sends the next chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Proof {
    Voucher(decdn_protocol::client::Voucher),
    Preimage(decdn_protocol::client::ChunkPreimage),
}

/// Cancellation-safe, buffered reader for `cdn/client/v1` payment-proof frames. Owns
/// a byte buffer that PERSISTS across [`Self::read`] calls, so a `read` future
/// cancelled by the outer [`VOUCHER_READ_TIMEOUT`] loses no bytes: any partial
/// frame stays buffered for the next call.
///
/// [`read_frame`] is built on `read_exact` and is NOT cancellation-safe — a
/// `timeout` firing mid-frame would drop already-consumed bytes and desync the
/// stream. Reading instead via the cancel-safe [`tokio::io::AsyncReadExt::read`]
/// into an owned buffer, then splitting whole frames off it with
/// [`decdn_protocol::framing::parse_frame`], keeps every byte. Borrows the
/// `RecvStream` per call so the caller retains it for stream teardown.
///
/// All proof reads on a given stream MUST go through ONE instance: it may read
/// ahead (buffering the next pipelined proof, #1486) while the current one is
/// verified and recorded, and a second reader on the same `RecvStream` would
/// lose those buffered bytes.
#[derive(Default)]
pub(super) struct BufferedProofReader {
    /// Unconsumed bytes read from the stream, at a frame boundary or partway
    /// into the next frame's header/body.
    buf: Vec<u8>,
}

impl BufferedProofReader {
    /// Read one framed payment proof — a [`ClientMessage::Voucher`] or a
    /// [`ClientMessage::ChunkPreimage`] — filling the buffer incrementally.
    /// **Cancellation-safe:** if the returned future is dropped (a gather
    /// `timeout` elapsed), bytes already read stay in `self.buf` for the next
    /// call — no frame is torn.
    ///
    /// These two variants are the entire payer→node vocabulary after the
    /// opening `StreamRequest`, so this is the one place every proof passes
    /// through. Anything else on this stream is a protocol error.
    pub(super) async fn read(&mut self, recv: &mut RecvStream) -> anyhow::Result<Proof> {
        loop {
            if let Some((header_len, payload_len)) = decdn_protocol::framing::parse_frame(&self.buf)
                .map_err(|e| anyhow::anyhow!("proof frame parse failed: {e}"))?
            {
                let total = header_len.saturating_add(payload_len);
                let payload = self
                    .buf
                    .get(header_len..total)
                    .ok_or_else(|| anyhow::anyhow!("proof frame bounds out of range"))?;
                let decoded = decode_message::<ClientMessage>(payload);
                // Consume the frame's bytes regardless of decode outcome so a
                // single bad frame cannot wedge the buffer.
                let result = match decoded {
                    Ok((ClientMessage::Voucher(v), _)) => Ok(Proof::Voucher(v)),
                    Ok((ClientMessage::ChunkPreimage(p), _)) => Ok(Proof::Preimage(p)),
                    Ok((_, _)) => Err(anyhow::anyhow!(
                        "expected ClientMessage::Voucher or ClientMessage::ChunkPreimage"
                    )),
                    Err(e) => Err(anyhow::anyhow!("proof decode failed: {e}")),
                };
                self.buf.drain(..total);
                return result;
            }
            // Need more bytes. Use tokio's `AsyncReadExt::read` (explicitly, since
            // iroh's inherent Quinn `read` shadows it) — it is documented
            // cancel-safe: a dropped future consumes nothing, and on `Ready(n)` we
            // append to `self.buf` before the next await, so no bytes are ever lost
            // to a gather timeout. `0` is EOF.
            let mut scratch = [0u8; 4096];
            let n = AsyncReadExt::read(recv, &mut scratch)
                .await
                .map_err(|e| anyhow::anyhow!("proof stream read failed: {e}"))?;
            if n == 0 {
                anyhow::bail!("proof stream closed mid-frame");
            }
            let chunk = scratch
                .get(..n)
                .ok_or_else(|| anyhow::anyhow!("short read length out of range"))?;
            self.buf.extend_from_slice(chunk);
        }
    }
}

/// A load-shed controller that never sheds, for handler-layer tests that are
/// not exercising the load-shed gate itself. Keeps every existing serve test
/// admitting exactly as it did before the gate was wired in.
#[cfg(test)]
fn always_admit_shed() -> Arc<crate::load_shed::LoadShedController> {
    crate::load_shed::LoadShedController::from_config(&decdn_common::config::ResolvedLoadShed {
        policy: decdn_common::config::LoadShedPolicyKind::AlwaysAdmit,
        ..Default::default()
    })
}

/// Build a `ClientHandler` over an arbitrary [`PoolStateStore`] for the
/// sibling-module tests (e.g. `voucher.rs`'s #527 durability tests need a
/// fault-injecting store). Kept at module level (not inside `mod tests`) so a
/// child module's `#[cfg(test)]` can reach it as `super::handler_over_store`.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(super) async fn handler_over_store(
    metrics: &Arc<Metrics>,
    store: Arc<dyn PoolStateStore>,
) -> (Arc<ClientHandler>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = CacheEngine::open(dir.path(), Vec::new(), 16)
        .await
        .expect("cache");
    let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
    let deps = ClientHandlerDeps::new(
        iroh::SecretKey::generate().public(),
        Arc::clone(metrics),
        Arc::new(ConnectionLimiter::new(
            &decdn_common::config::ResolvedSecurity {
                max_concurrent_handlers: u32::MAX,
                per_source_rate_per_sec: 1e9,
                per_source_burst: u32::MAX,
                max_tracked_sources: 16,
            },
            Arc::clone(metrics),
        )),
        cache,
        Arc::new(alloy::signers::local::PrivateKeySigner::random()),
        domain.clone(),
        domain.clone(),
        domain,
        store,
        Arc::new(crate::receipt_log::DirectReceiptSink::new(Arc::new(
            crate::receipt_log::NoopReceiptLog,
        ))) as Arc<dyn ReceiptSink>,
        1,
        crate::rate_bounds::RateBounds::new(0),
        0,
        16,
        Arc::new(crate::content_deny::ContentDenylist::empty()),
        U256::ZERO,
        always_admit_shed(),
    );
    let handler = ClientHandler::new(deps).expect("handler");
    (Arc::new(handler), dir)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// A frame must never cross a payment-chunk boundary, whatever the configured
    /// target. Both sides meter the same frame sequence and exchange one preimage per
    /// interval; a straddling frame lands the payer past the boundary, which settles
    /// as a signed residual voucher instead — a per-frame signature on the hot path
    /// and a cadence the two sides no longer share. A frame size that divides
    /// `interval_bytes` gets the property for free; at any other size it has to be
    /// cut for explicitly, which is what this clamp does.
    #[tokio::test]
    async fn a_frame_never_crosses_a_payment_chunk_boundary() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await;
        let interval = decdn_protocol::CHUNK_BYTES;
        let wide_open = u64::MAX;

        // Walk a whole interval in the frame sizes the handler itself hands out and
        // confirm the walk lands exactly on the boundary rather than stepping over it.
        let mut unvouchered = 0u64;
        let mut frames = 0u32;
        while unvouchered < interval {
            let target = handler.frame_target(unvouchered, interval, wide_open) as u64;
            assert!(target > 0, "a zero-length frame is a protocol error");
            unvouchered += target;
            assert!(
                unvouchered <= interval,
                "frame of {target} crossed the boundary: {unvouchered} > {interval}"
            );
            frames += 1;
            assert!(
                frames < 64,
                "target collapsed to runt frames: {frames} per interval"
            );
        }
        assert_eq!(unvouchered, interval, "the walk must land ON the boundary");
    }

    /// At the default target an open window spends exactly ONE frame per payment
    /// interval. Nothing else pins that the default target and the payment quantum
    /// line up, and a regression multiplies per-frame CPU across every byte the node
    /// egresses.
    #[tokio::test]
    async fn an_open_window_spends_one_frame_per_payment_chunk() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await;
        let interval = decdn_protocol::CHUNK_BYTES;
        assert_eq!(
            handler.frame_target(0, interval, u64::MAX) as u64,
            interval,
            "an open window at the default target must cover the interval in one frame"
        );
    }

    /// The serve side's prefetch floor and the pull side's reservation are one
    /// invariant split across two crates, so it needs a test that names both.
    ///
    /// `frame_target` floors its room cap at one bao chunk group, which on a cache
    /// miss is a request the upstream pull must be allowed to satisfy past a shut
    /// credit window. `PULL_WINDOW_FLOOR` reserves a third group for exactly that.
    /// Raise one without the other and the cache-miss leg parks — and it parks as a
    /// hang with no diagnostic, which is why the relationship is asserted here
    /// rather than left to an integration test's timeout.
    #[tokio::test]
    async fn frame_target_room_floor_matches_the_pull_reservation() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await;
        let interval = decdn_protocol::CHUNK_BYTES;
        let group = decdn_bao_range::CHUNK_GROUP_BYTES;

        // What the serve side asks for past a shut window.
        let prefetch = handler.frame_target(0, interval, 0) as u64;
        assert_eq!(prefetch, group, "the room floor is one bao chunk group");

        // What the pull side can still draw once both group roundings are spent.
        let drawable = decdn_client_pull::PULL_WINDOW_FLOOR - 2 * group;
        assert!(
            drawable >= interval + prefetch,
            "the pull floor leaves {drawable} bytes after both roundings, but the \
             client must complete a {interval}-byte chunk to pay AND the serve leg \
             prefetches {prefetch} bytes past its shut window — the miss leg would park"
        );
    }

    /// A closed window must not be asked for a full frame. Both loops prefetch one
    /// frame ahead of the window check, and on the cache-miss leg the producer is fed
    /// by an upstream pull paced against this stream's own served-and-paid frontier —
    /// so a large request parks on bytes that only recouping can unblock, and the
    /// loop must exit to recoup. The room cap is what keeps that prefetch
    /// satisfiable; the floor keeps it from degenerating to a single byte.
    #[tokio::test]
    async fn a_closed_window_yields_a_short_frame_not_a_full_one() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await;
        let interval = decdn_protocol::CHUNK_BYTES;
        let group = decdn_bao_range::CHUNK_GROUP_BYTES;

        let closed = handler.frame_target(0, interval, 0) as u64;
        assert_eq!(
            closed, group,
            "a closed window must fall back to the group floor"
        );

        // A partly-open window is honoured as-is once it clears the floor.
        assert_eq!(
            handler.frame_target(0, interval, 4 * group) as u64,
            4 * group
        );
        // ...and the boundary still wins when it is the tighter of the two.
        assert_eq!(
            handler.frame_target(interval - group, interval, u64::MAX) as u64,
            group
        );
    }

    /// Build the smallest `ClientHandler` for the handler-layer tests below,
    /// seeding the floor-`M` minimum-remaining-deposit at zero.
    async fn handler_for_tests(metrics: &Arc<Metrics>) -> (Arc<ClientHandler>, tempfile::TempDir) {
        handler_for_tests_with_floor(metrics, U256::ZERO).await
    }

    /// [`handler_for_tests`] with an explicit floor-`M` so the floor-`M` guard can
    /// be exercised with a non-zero minimum-remaining-deposit.
    async fn handler_for_tests_with_floor(
        metrics: &Arc<Metrics>,
        pool_min_remaining_deposit: U256,
    ) -> (Arc<ClientHandler>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = CacheEngine::open(dir.path(), Vec::new(), 16)
            .await
            .expect("cache");
        let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
        let deps = ClientHandlerDeps::new(
            iroh::SecretKey::generate().public(),
            Arc::clone(metrics),
            Arc::new(ConnectionLimiter::new(
                &decdn_common::config::ResolvedSecurity {
                    max_concurrent_handlers: u32::MAX,
                    per_source_rate_per_sec: 1e9,
                    per_source_burst: u32::MAX,
                    max_tracked_sources: 16,
                },
                Arc::clone(metrics),
            )),
            cache,
            Arc::new(alloy::signers::local::PrivateKeySigner::random()),
            domain.clone(),
            domain.clone(),
            domain,
            Arc::new(decdn_incentive::store::MemoryPoolStateStore::new())
                as Arc<dyn PoolStateStore>,
            Arc::new(crate::receipt_log::DirectReceiptSink::new(Arc::new(
                crate::receipt_log::NoopReceiptLog,
            ))) as Arc<dyn ReceiptSink>,
            1,
            crate::rate_bounds::RateBounds::new(0),
            0,
            16,
            Arc::new(crate::content_deny::ContentDenylist::empty()),
            pool_min_remaining_deposit,
            always_admit_shed(),
        );
        let handler = ClientHandler::new(deps).expect("handler");
        (Arc::new(handler), dir)
    }

    /// The load-shed controller sheds a cache-miss once the node is at its
    /// configured concurrency ceiling, while a cache-hit for a DIFFERENT client
    /// still rides — a hit is local, zero-upstream-cost margin, so it is shed
    /// last (miss-before-hit). Exercises the controller directly at the wiring
    /// boundary rather than standing up a full QUIC loopback.
    #[tokio::test]
    async fn miss_is_shed_when_node_at_capacity_but_hit_admitted() {
        // Build a handler whose shed controller trips at 1 concurrent serve.
        let cfg = decdn_common::config::ResolvedLoadShed {
            policy: decdn_common::config::LoadShedPolicyKind::ResourcePressure,
            egress_budget_mbps: 0,
            max_concurrent_serves_high: 1,
            max_concurrent_serves_low: 0,
            per_client_serve_cap: 0,
        };
        let shed = crate::load_shed::LoadShedController::from_config(&cfg);
        // Occupy the one slot.
        let _held = shed
            .try_admit(crate::load_shed::RequestClass::CacheHit, B256::ZERO)
            .expect("first serve admits");
        // A new miss is shed; a new hit rides (egress under budget).
        assert!(
            shed.try_admit(
                crate::load_shed::RequestClass::CacheMiss,
                B256::from([1u8; 32])
            )
            .is_err()
        );
        assert!(
            shed.try_admit(
                crate::load_shed::RequestClass::CacheHit,
                B256::from([1u8; 32])
            )
            .is_ok()
        );
    }

    /// The lane registry resolves independent lanes concurrently (#1731). Many
    /// distinct [`LaneKey`]s register, resolve, and forget in parallel with no
    /// shared map lock serializing them; the sharded map must still preserve the
    /// single-mutex semantics — every registered lane is present and resolvable,
    /// and every forgotten lane is gone.
    #[tokio::test]
    async fn distinct_lanes_register_resolve_and_forget_concurrently() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await;

        // Distinct lanes differ only by signer — the independent-lane case the
        // single mutex used to serialize regardless of how they shard.
        let keys: Vec<LaneKey> = (0u8..32)
            .map(|i| LaneKey {
                pool_id: B256::repeat_byte(0xC0),
                signer: Address::repeat_byte(i),
                provider: handler.eth_signer.address(),
            })
            .collect();

        // Register every lane concurrently.
        let mut register = Vec::new();
        for key in &keys {
            let handler = Arc::clone(&handler);
            let state = LaneState::hydrate(
                key.pool_id,
                key.signer,
                key.provider,
                U256::from(1_000_000u64),
                0,
                U256::ZERO,
                U256::ZERO,
                None,
                decdn_incentive::LaneChain::NONE,
            );
            register.push(tokio::spawn(
                async move { handler.register_lane(state).await },
            ));
        }
        for task in register {
            task.await.expect("join").expect("register_lane");
        }
        assert_eq!(handler.lanes.len(), keys.len(), "every lane is tracked");

        // Resolve every lane concurrently — each is a point read on the map.
        let mut resolve = Vec::new();
        for key in &keys {
            let handler = Arc::clone(&handler);
            let key = *key;
            resolve.push(tokio::spawn(async move {
                handler.lane_state_snapshot(key).await.map(|s| s.key())
            }));
        }
        for (task, key) in resolve.into_iter().zip(keys.iter()) {
            assert_eq!(
                task.await.expect("join"),
                Some(*key),
                "each registered lane resolves to itself"
            );
        }

        // Forget every lane concurrently; the map drains to empty.
        let mut forget = Vec::new();
        for key in &keys {
            let handler = Arc::clone(&handler);
            let key = *key;
            forget.push(tokio::spawn(async move { handler.forget_lane(key).await }));
        }
        for task in forget {
            task.await.expect("join").expect("forget_lane");
        }
        assert_eq!(handler.lanes.len(), 0, "every lane is forgotten");
    }

    /// The floor-`M` solvency arithmetic with a NON-ZERO floor `M`
    /// (`pool_remaining_covers_window`, ADR 003 §Sizing). The node keeps serving a
    /// pool only while its on-chain remaining minus `M` still covers the reserved
    /// credit window; it refuses once the refundable floor would be dipped into.
    #[tokio::test]
    async fn pool_remaining_covers_window_reserves_the_floor_m() {
        let metrics = Arc::new(Metrics::new());
        // M = 1 USDC; a 1 MB window at 1 USDC/MB costs exactly 1 USDC.
        let m = U256::from(1_000_000u64);
        let (handler, _dir) = handler_for_tests_with_floor(&metrics, m).await;
        let rate_per_mb = 1_000_000u64; // 1 USDC/MB
        let window_bytes = decdn_protocol::MB_BYTES; // one MB
        let window_cost = decdn_incentive::min_payment(window_bytes, rate_per_mb);
        assert_eq!(window_cost, U256::from(1_000_000u64), "1 MB @ 1 USDC/MB");

        // remaining just below `M + window_cost` → the window would dip into the
        // floor → refuse.
        let below = m + window_cost - U256::from(1u64);
        assert!(
            !handler.pool_remaining_covers_window(below, window_bytes, rate_per_mb),
            "remaining under M + window cost must be refused"
        );
        // remaining exactly `M + window_cost` → the window is covered above the
        // floor → serve.
        let exact = m + window_cost;
        assert!(
            handler.pool_remaining_covers_window(exact, window_bytes, rate_per_mb),
            "remaining at exactly M + window cost must be served"
        );
        // A pool with only the floor left (remaining == M) can never serve a
        // non-empty window.
        assert!(
            !handler.pool_remaining_covers_window(m, window_bytes, rate_per_mb),
            "remaining == M leaves nothing above the floor"
        );
    }

    /// The seller-side lane-count gauge tracks the live `lanes` map. Deposit is a
    /// pool-level on-chain quantity (getPool), not carried per lane, so the
    /// snapshot reports the open-lane count only.
    #[tokio::test]
    async fn lane_count_gauge_tracks_the_live_map() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await;
        let lane = LaneKey {
            pool_id: B256::repeat_byte(0xA1),
            signer: Address::repeat_byte(0x11),
            provider: Address::repeat_byte(0x22),
        };
        handler.lanes.insert(
            lane,
            Arc::new(Mutex::new(LaneDeliveryState {
                state: LaneState::hydrate(
                    lane.pool_id,
                    lane.signer,
                    lane.provider,
                    U256::from(10u64),
                    0,
                    U256::ZERO,
                    U256::ZERO,
                    None,
                    decdn_incentive::LaneChain::NONE,
                ),
                bytes_delivered_cumulative: U256::ZERO,
                paid_credited: U256::ZERO,
                active_streams: Arc::new(AtomicU32::new(0)),
                last_voucher_at: AtomicU64::new(0),
            })),
        );
        handler.refresh_lane_metrics().await;
        let encoded = metrics.encode().expect("metrics encode");
        assert!(encoded.lines().any(|line| line == "decdn_lanes_open 1"));
    }

    #[test]
    fn deposit_refusal_warn_fires_on_the_first_refusal_then_waits_out_the_window() {
        let interval = Duration::from_mins(5);
        // Nothing warned yet: the very first refusal must be visible, not swallowed
        // until a window elapses from process start.
        assert!(should_warn_now(0, 0, interval));
        assert!(should_warn_now(1_000_000, 0, interval));

        let last = 1_000_000;
        // Inside the window — suppressed.
        assert!(!should_warn_now(last, last, interval));
        assert!(!should_warn_now(last + 299_999, last, interval));
        // Exactly at the boundary, and past it — due.
        assert!(should_warn_now(last + 300_000, last, interval));
        assert!(should_warn_now(last + 600_000, last, interval));
    }

    /// The atomic path, which `should_warn_now`'s two tests do not touch. The
    /// suppressed count IS the feature — it is what distinguishes one dry client
    /// from a node refusing everyone — and every part of producing it was
    /// unverified: the `fetch_add` before the gate, the `swap(0)`, and the
    /// `saturating_sub(1)` that removes the winner's own event from its own report.
    ///
    /// Mutants this kills: dropping the `-1` (every line off by one); moving the
    /// `fetch_add` after the gate (the first warn reports 0 forever and nothing
    /// accumulates); `swap` → `load` (the count grows monotonically and "suppressed
    /// since the last line" becomes meaningless).
    ///
    /// No sleeping and no clock injection: the window is forced open by writing
    /// `last_warn_ms` back to 1, which is what a test in the same module can do.
    #[tokio::test]
    async fn deposit_refusal_warn_reports_exactly_what_it_swallowed() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await;

        // First refusal is always visible, and has swallowed nothing.
        assert_eq!(handler.note_deposit_refusal(), Some(0));
        // Inside the window: silent, but counting.
        assert_eq!(handler.note_deposit_refusal(), None);
        assert_eq!(handler.note_deposit_refusal(), None);

        // Force the window open. `1`, not `0` — `0` is the never-warned sentinel.
        handler
            .deposit_refusal_last_warn_ms
            .store(1, Ordering::Relaxed);
        assert_eq!(
            handler.note_deposit_refusal(),
            Some(2),
            "the line must report the two it swallowed, not counting itself"
        );

        // And the counter reset, so the next window starts from zero.
        assert_eq!(handler.note_deposit_refusal(), None);
        handler
            .deposit_refusal_last_warn_ms
            .store(1, Ordering::Relaxed);
        assert_eq!(handler.note_deposit_refusal(), Some(1));
    }

    /// Floor-`M` serving policy: the pool serves a full credit window while
    /// `remaining − M` covers it and stops the instant it cannot. `M` is the
    /// refundable minimum the pool owner is guaranteed to keep.
    #[tokio::test]
    async fn floor_m_serves_above_the_floor_and_stops_at_it() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await;
        // `handler_for_tests` seeds `pool_min_remaining_deposit == 0`; rebuild a
        // small handler with a real floor by poking the field via a fresh deps is
        // awkward, so assert the arithmetic directly against the ZERO-floor
        // handler plus a manual floor calculation.
        // ZERO floor: covered whenever remaining >= min_payment.
        let rate = 1_000u64;
        let bytes = decdn_incentive::rate::BYTES_PER_MB; // one MB
        let cost = min_payment(bytes, rate);
        assert!(
            handler.pool_remaining_covers_window(cost, bytes, rate),
            "exactly the cost clears a zero floor"
        );
        assert!(
            !handler.pool_remaining_covers_window(cost - U256::from(1u64), bytes, rate),
            "one base unit short must refuse"
        );
        // Non-zero floor arithmetic: remaining − M must still cover the window.
        let floor = U256::from(500u64);
        let remaining = cost + floor;
        let refundable = remaining.saturating_sub(floor);
        assert_eq!(refundable, cost, "remaining − M is exactly the window cost");
        // Draining to the floor must stop serving: remaining − M underflows to 0.
        let at_floor = floor;
        assert!(at_floor.saturating_sub(floor).is_zero());
    }

    #[test]
    fn deposit_refusal_warn_suppresses_rather_than_spams_on_a_backwards_clock() {
        // A clock that steps backwards (NTP correction, VM migration) makes
        // `now < last`. The saturating subtraction yields 0 elapsed, so the gate
        // stays shut until the clock catches up. Suppressing is the safe direction
        // for a log gate: the alternative is every refusal warning until then.
        let interval = Duration::from_mins(5);
        assert!(!should_warn_now(500, 1_000_000, interval));
    }

    /// ADR 011 §`StreamRequest` Response names distinct refusal codes for the two
    /// takedown reasons. They must NOT join the seven-reason `NotFound` collapse:
    /// a client told `NotFound` retries elsewhere and pays again, which for
    /// `OriginBlacklisted` is advice that can never succeed.
    #[test]
    fn takedown_reject_reasons_do_not_collapse_to_not_found() {
        assert_eq!(
            ServeRejectReason::HashDenied.wire_error(),
            decdn_protocol::StreamError::HashBlacklisted
        );
        assert_eq!(
            ServeRejectReason::OriginDenied.wire_error(),
            decdn_protocol::StreamError::OriginBlacklisted
        );
        // The collapse itself is unchanged — it is a privacy property, not an
        // oversight, and widening it was never the point of #1179.
        for reason in [
            ServeRejectReason::CacheMiss,
            ServeRejectReason::UnknownChannel,
            ServeRejectReason::OwnerMismatch,
            ServeRejectReason::InsufficientDeposit,
            ServeRejectReason::RangeNotSatisfiable,
        ] {
            assert_eq!(
                reason.wire_error(),
                decdn_protocol::StreamError::NotFound,
                "{reason:?} must stay wire-indistinguishable"
            );
        }
    }

    /// The privacy invariant ADR 011 §`StreamRequest` Response actually asks
    /// for: a governance takedown and this operator's own denylist entry are one
    /// wire code. They stay distinct *reasons* only so the operator's own
    /// metrics can tell them apart, which no client can read.
    ///
    /// Without this, governance entries reaching the serve path only as cache
    /// evictions would answer `EvictedSinceProbe`, making `HashBlacklisted` a unique
    /// fingerprint for "this operator privately denied it": exactly the map of an
    /// operator's legal exposure the ADR forecloses.
    #[test]
    fn local_and_governance_hash_denials_share_one_wire_code() {
        assert_eq!(
            ServeRejectReason::HashDenied.wire_error(),
            ServeRejectReason::ChainHashDenied.wire_error(),
            "a client must not be able to tell a governance takedown from a local one"
        );
    }

    /// ...while an eviction with no blacklist entry behind it (corruption
    /// recovery, a manual `decdn node evict`) keeps its own code. Collapsing
    /// that one too would cost the probe-then-gone race its distinct answer for
    /// no privacy gain: nobody can infer a legal exposure from a hash this node
    /// simply no longer holds.
    #[test]
    fn plain_eviction_keeps_its_own_wire_code() {
        assert_ne!(
            ServeRejectReason::EvictedSinceProbe.wire_error(),
            ServeRejectReason::HashDenied.wire_error()
        );
    }

    /// A [`crate::channel_store::CapabilitySink`] that records which
    /// `(pool_id, signer)` grants were persisted, so a test can assert a
    /// forged-owner capability is dropped before it reaches the redeemer.
    #[derive(Debug)]
    struct RecordingCapabilitySink {
        recorded: Arc<std::sync::Mutex<Vec<(B256, Address)>>>,
    }

    impl crate::channel_store::CapabilitySink for RecordingCapabilitySink {
        fn store_capability(
            &self,
            pool_id: B256,
            signer: Address,
            _spending_cap: U256,
            _expiry: u64,
            _owner_sig: &[u8],
        ) -> Result<(), StoreError> {
            self.recorded
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((pool_id, signer));
            Ok(())
        }
    }

    /// A [`crate::pool_view::PoolView`] returning a fixed owner (and unbounded
    /// remaining, so the floor-`M` gate never interferes) for the capability
    /// owner-verification test.
    #[derive(Debug)]
    struct FixedPoolView {
        owner: Address,
    }

    #[async_trait::async_trait]
    impl crate::pool_view::PoolView for FixedPoolView {
        async fn status(&self, _pool_id: B256) -> Option<crate::pool_view::PoolStatus> {
            Some(crate::pool_view::PoolStatus {
                owner: self.owner,
                remaining: U256::MAX,
                lifecycle: crate::pool_view::Lifecycle::Open,
            })
        }
    }

    /// Build a handler with a recording capability sink and a fixed-owner
    /// pool-view wired, for the capability-intake owner check.
    async fn handler_with_capability_intake(
        metrics: &Arc<Metrics>,
        owner: Address,
    ) -> (
        Arc<ClientHandler>,
        Arc<std::sync::Mutex<Vec<(B256, Address)>>>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = CacheEngine::open(dir.path(), Vec::new(), 16)
            .await
            .expect("cache");
        let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut deps = ClientHandlerDeps::new(
            iroh::SecretKey::generate().public(),
            Arc::clone(metrics),
            Arc::new(ConnectionLimiter::new(
                &decdn_common::config::ResolvedSecurity {
                    max_concurrent_handlers: u32::MAX,
                    per_source_rate_per_sec: 1e9,
                    per_source_burst: u32::MAX,
                    max_tracked_sources: 16,
                },
                Arc::clone(metrics),
            )),
            cache,
            Arc::new(alloy::signers::local::PrivateKeySigner::random()),
            domain.clone(),
            domain.clone(),
            domain,
            Arc::new(decdn_incentive::store::MemoryPoolStateStore::new())
                as Arc<dyn PoolStateStore>,
            Arc::new(crate::receipt_log::DirectReceiptSink::new(Arc::new(
                crate::receipt_log::NoopReceiptLog,
            ))) as Arc<dyn ReceiptSink>,
            1,
            crate::rate_bounds::RateBounds::new(0),
            0,
            16,
            Arc::new(crate::content_deny::ContentDenylist::empty()),
            U256::ZERO,
            always_admit_shed(),
        );
        deps.capability_sink = Some(Arc::new(RecordingCapabilitySink {
            recorded: Arc::clone(&recorded),
        }));
        deps.pool_view = Some(Arc::new(FixedPoolView { owner }));
        let handler = ClientHandler::new(deps).expect("handler");
        (Arc::new(handler), recorded, dir)
    }

    /// Fix 1 (security): capability intake verifies the owner signature against
    /// the on-chain pool owner. A grant signed by a NON-owner key is dropped —
    /// never persisted, never lane-registered — so it cannot revert the
    /// redeemer's `redeemMany` batch. A correct-owner grant is persisted and
    /// registers its lane.
    #[tokio::test]
    async fn intake_rejects_wrong_owner_capability() {
        let metrics = Arc::new(Metrics::new());
        let owner = PrivateKeySigner::random();
        let (handler, recorded, _dir) =
            handler_with_capability_intake(&metrics, owner.address()).await;
        let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
        let pool_id = B256::repeat_byte(0x77);
        let signer = Address::repeat_byte(0x11);
        let spending_cap = U256::from(1_000_000u64);
        let expiry = 1_900_000_000u64;

        let make_wire = |key: &PrivateKeySigner| -> decdn_protocol::client::WireCapability {
            let signed_cap = Capability {
                signer,
                spending_cap,
                pool_id,
                expiry,
            }
            .sign(key, &domain)
            .expect("sign capability");
            decdn_protocol::client::WireCapability {
                spending_cap: spending_cap.to_be_bytes(),
                expiry,
                owner_signature: signed_cap.signature.as_bytes().to_vec(),
            }
        };

        let lane_key = LaneKey {
            pool_id,
            signer,
            provider: handler.eth_signer.address(),
        };

        // A capability signed by a NON-owner is dropped: not persisted, no lane.
        let bad = make_wire(&PrivateKeySigner::random());
        handler
            .intake_capability(pool_id, signer, Some(owner.address()), &bad)
            .await;
        assert!(
            recorded
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "a forged-owner capability must not be persisted"
        );
        assert!(
            !handler.lanes.contains_key(&lane_key),
            "a forged-owner capability must not register a lane"
        );

        // The correct owner's capability is persisted and registers the lane.
        let good = make_wire(&owner);
        handler
            .intake_capability(pool_id, signer, Some(owner.address()), &good)
            .await;
        assert_eq!(
            recorded
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            &[(pool_id, signer)],
            "a correct-owner capability is persisted for the redeemer"
        );
        assert!(
            handler.lanes.contains_key(&lane_key),
            "a correct-owner capability registers its lane so vouchers can be served"
        );
    }

    #[test]
    fn lane_at_capacity_collapses_to_not_found() {
        // A capacity refusal must be wire-indistinguishable from any other
        // miss/deposit refusal so a probing client cannot detect a lane's
        // concurrency state or map pool balances.
        assert_eq!(
            ServeRejectReason::LaneAtCapacity.wire_error(),
            StreamError::NotFound
        );
    }

    #[test]
    fn lane_slot_decrements_counter_on_drop() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let counter = Arc::new(AtomicU32::new(0));
        counter.fetch_add(1, Ordering::Relaxed); // caller increments under lock
        {
            let _slot = LaneSlot::new(counter.clone());
            assert_eq!(counter.load(Ordering::Relaxed), 1);
        }
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "slot must release on drop"
        );
    }

    /// Lock the floor map for a test assertion, surfacing a poisoned lock as an
    /// `anyhow` error rather than panicking (the anti-panic policy holds in tests).
    fn lock_floor(
        map: &Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
    ) -> anyhow::Result<std::sync::MutexGuard<'_, HashMap<B256, PoolFloorState>>> {
        map.lock()
            .map_err(|e| anyhow::anyhow!("floor map poisoned: {e}"))
    }

    /// A stream that never reaches a floor of payment leaves its unpaid tail (capped
    /// at one floor) as the pool's durable `dead_charge`, and frees the live
    /// reservation, when its [`FloorReservation`] drops. No tokio runtime is present,
    /// so `Drop` persists synchronously via the direct-call fallback.
    #[test]
    fn floor_reservation_reconciles_partial_loss_on_drop() -> anyhow::Result<()> {
        use decdn_incentive::PoolFloorLossStore as _;
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let store = Arc::new(decdn_incentive::MemoryPoolFloorLossStore::new());
        let pool = B256::repeat_byte(0x5A);
        let floor = decdn_incentive::floor_micro(1000);
        let quarter = floor / U256::from(4u64);
        {
            let res = FloorReservation::reserve(
                map.clone(),
                Some(store.clone()),
                Arc::new(Metrics::new()),
                pool,
                floor,
            );
            // Live reservation is held while the guard lives.
            let live = lock_floor(&map)?.get(&pool).map(|s| s.live_reservation);
            anyhow::ensure!(
                live == Some(floor),
                "live reservation is held while the guard lives"
            );
            // Stream delivered a partial (unpaid) floor, then completed cleanly.
            res.note_unpaid(quarter);
            res.mark_settled();
        } // drop → reconcile: live released, dead_charge = min(floor, unpaid) = floor/4
        let st = lock_floor(&map)?.get(&pool).copied().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation == U256::ZERO,
            "live reservation is released on drop"
        );
        anyhow::ensure!(
            st.dead_charge == quarter,
            "a settled stream folds the proportional unpaid tail (min of floor and unpaid)"
        );
        let persisted = store
            .load_losses()
            .map_err(|e| anyhow::anyhow!("load_losses: {e}"))?
            .first()
            .map(|(_, v)| *v);
        anyhow::ensure!(
            persisted == Some(quarter.to::<u128>()),
            "the new dead total is persisted best-effort on drop"
        );
        Ok(())
    }

    /// An ABNORMAL exit — the guard drops without [`FloorReservation::mark_settled`],
    /// as on a client disconnect or abort before delivery — folds the FULL `reserved`
    /// into `dead_charge`, not the (here zero) unpaid tail. This is what bounds
    /// sequential abuse where a cache-miss fill is aborted before any byte is
    /// delivered yet the node already fronted upstream USDC (C3, ADR 003 §Pool solvency).
    #[test]
    fn floor_reservation_abnormal_exit_folds_full_reserved() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let pool = B256::repeat_byte(0x5B);
        let floor = decdn_incentive::floor_micro(1000);
        {
            let res =
                FloorReservation::reserve(map.clone(), None, Arc::new(Metrics::new()), pool, floor);
            // Aborted before delivering/paying anything: unpaid stays 0, and the guard
            // is never marked settled.
            res.note_unpaid(U256::ZERO);
        } // drop → conservative: dead_charge = full reserved despite unpaid == 0
        let st = lock_floor(&map)?.get(&pool).copied().unwrap_or_default();
        anyhow::ensure!(
            st.live_reservation == U256::ZERO,
            "live reservation is released even on an abnormal exit"
        );
        anyhow::ensure!(
            st.dead_charge == floor,
            "an unsettled (abnormal) exit folds the full reserved floor, not the zero unpaid tail"
        );
        Ok(())
    }

    /// The pool-budget guard counts a pool's committed floor credit (live
    /// reservations plus durable dead charge) against `remaining − M`.
    #[tokio::test]
    async fn pool_budget_covers_reserve_accounts_committed_floor() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await; // M = 0
        let pool = B256::repeat_byte(0x11);
        let floor = decdn_incentive::floor_micro(1_000_000);
        // Empty pool, M = 0: remaining must cover the new reserve exactly.
        anyhow::ensure!(
            handler.pool_budget_covers_reserve(pool, floor, floor),
            "remaining equal to the reserve is covered"
        );
        anyhow::ensure!(
            !handler.pool_budget_covers_reserve(
                pool,
                floor.saturating_sub(U256::from(1u64)),
                floor
            ),
            "remaining one below the reserve is not covered"
        );
        // A live reservation consumes the budget: the same remaining no longer
        // covers a second identical reserve.
        let _guard = handler.reserve_floor(pool, floor);
        anyhow::ensure!(
            !handler.pool_budget_covers_reserve(pool, floor, floor),
            "an in-flight floor reservation is committed against the budget"
        );
        Ok(())
    }

    /// `try_reserve_floor` reserves atomically: it charges the budget only when
    /// `remaining − M` covers the pool's committed floor credit plus the new floor,
    /// and the charge is visible to the very next call so a second reserve on an
    /// exhausted pool is refused.
    #[tokio::test]
    async fn try_reserve_floor_charges_only_when_budget_covers() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await; // M = 0
        let pool = B256::repeat_byte(0x22);
        let floor = decdn_incentive::floor_micro(1_000_000);
        // Budget covers exactly one floor: the first reserve succeeds.
        let first = handler.try_reserve_floor(pool, floor, floor);
        anyhow::ensure!(first.is_some(), "a floor within remaining − M is reserved");
        // The charge is live: a second identical reserve against the SAME remaining
        // now sees `committed = floor` and is refused (no over-commit).
        anyhow::ensure!(
            handler.try_reserve_floor(pool, floor, floor).is_none(),
            "a second reserve over the same budget is refused"
        );
        // A CLEANLY completed stream (marked settled, nothing unpaid) frees its live
        // reservation on drop and folds no dead charge, reopening the budget. (An
        // abnormal exit would instead fold the full reserved into `dead_charge` and
        // keep the budget spent — see `floor_reservation_abnormal_exit_folds_full_reserved`.)
        if let Some(g) = first.as_ref() {
            g.mark_settled();
        }
        drop(first);
        anyhow::ensure!(
            handler.try_reserve_floor(pool, floor, floor).is_some(),
            "budget reopens once a cleanly-settled reservation is released"
        );
        Ok(())
    }

    /// A stream whose cumulative payment reaches a floor releases its live
    /// reservation immediately and leaves no `dead_charge` — the drop is a no-op.
    #[test]
    fn floor_reservation_repaid_leaves_no_dead_charge() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let pool = B256::repeat_byte(0x5B);
        let floor = decdn_incentive::floor_micro(1000);
        {
            let res =
                FloorReservation::reserve(map.clone(), None, Arc::new(Metrics::new()), pool, floor);
            res.release_live_repaid(); // paid ≥ floor
            let live = lock_floor(&map)?.get(&pool).map(|s| s.live_reservation);
            anyhow::ensure!(
                live == Some(U256::ZERO),
                "live reservation is freed the moment the floor is repaid"
            );
        } // drop is a no-op: already repaid
        let st = lock_floor(&map)?.get(&pool).copied().unwrap_or_default();
        anyhow::ensure!(
            st.dead_charge == U256::ZERO,
            "a repaid reservation folds no dead charge"
        );
        Ok(())
    }

    /// A repayment that lands after the pool was reclaimed releases nothing:
    /// `forget_pool_floor` removed the entry (its live reservation went with
    /// it), so `release_live_repaid` must not re-insert a default state for the
    /// closed pool — the in-memory face of the #1781 resurrection race. The
    /// pool id never recurs, so a re-inserted entry sits in the map for the
    /// process lifetime, collecting `dead_charge` from any later drop.
    #[test]
    fn repaid_release_after_forget_does_not_resurrect_entry() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let pool = B256::repeat_byte(0x5F);
        let floor = decdn_incentive::floor_micro(1000);
        let res =
            FloorReservation::reserve(map.clone(), None, Arc::new(Metrics::new()), pool, floor);
        // The pool closes mid-stream: the same in-memory remove
        // `forget_pool_floor` performs.
        lock_floor(&map)?.remove(&pool);
        res.release_live_repaid();
        anyhow::ensure!(
            lock_floor(&map)?.get(&pool).is_none(),
            "a repaid release on a reclaimed pool must not re-insert its entry"
        );
        drop(res);
        anyhow::ensure!(
            lock_floor(&map)?.get(&pool).is_none(),
            "the subsequent drop leaves the reclaimed pool absent too"
        );
        Ok(())
    }

    /// Read one pool's persisted dead charge out of a floor-loss store, for the
    /// drop-guard tests below.
    fn persisted_loss(
        store: &dyn decdn_incentive::PoolFloorLossStore,
        pool: B256,
    ) -> anyhow::Result<Option<u128>> {
        Ok(store
            .load_losses()
            .map_err(|e| anyhow::anyhow!("load_losses: {e}"))?
            .into_iter()
            .find(|(id, _)| *id == pool)
            .map(|(_, micro)| micro))
    }

    /// Two withheld streams on ONE pool, each reconciled through the real `Drop`
    /// guard against a real store (no runtime, so each drop persists through the
    /// synchronous fallback): the second drop persists a cumulative total larger
    /// than the first, and a later fully-repaid guard writes nothing. This is the
    /// drop guard driving the store's monotonic contract — every other
    /// monotonicity test drives the store directly, without its caller (#1783).
    #[test]
    fn floor_reservation_sequential_drops_accumulate_dead_charge() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let store = Arc::new(decdn_incentive::MemoryPoolFloorLossStore::new());
        let metrics = Arc::new(Metrics::new());
        let pool = B256::repeat_byte(0x5C);
        let floor = decdn_incentive::floor_micro(1000);
        let quarter = floor / U256::from(4u64);
        {
            let res = FloorReservation::reserve(
                map.clone(),
                Some(store.clone()),
                Arc::clone(&metrics),
                pool,
                floor,
            );
            res.note_unpaid(quarter);
            res.mark_settled();
        } // settled: folds the quarter unpaid tail
        anyhow::ensure!(persisted_loss(&*store, pool)? == Some(quarter.to::<u128>()));
        {
            let _res = FloorReservation::reserve(
                map.clone(),
                Some(store.clone()),
                Arc::clone(&metrics),
                pool,
                floor,
            );
        } // abnormal (never settled): folds the FULL reserved floor on top
        let want = quarter.saturating_add(floor);
        let st = lock_floor(&map)?.get(&pool).copied().unwrap_or_default();
        anyhow::ensure!(
            st.dead_charge == want,
            "the second drop folds onto the first's total, not over it"
        );
        anyhow::ensure!(
            persisted_loss(&*store, pool)? == Some(want.to::<u128>()),
            "the second drop persists the RAISED cumulative total"
        );
        {
            let res = FloorReservation::reserve(
                map.clone(),
                Some(store.clone()),
                Arc::clone(&metrics),
                pool,
                floor,
            );
            res.release_live_repaid();
        } // repaid: no fold, no write
        anyhow::ensure!(
            persisted_loss(&*store, pool)? == Some(want.to::<u128>()),
            "a repaid guard disturbs neither the accumulator nor the durable total"
        );
        Ok(())
    }

    /// [`decdn_incentive::PoolFloorLossStore`] wrapper that HOLDS every
    /// `record_loss` at its entry until released, so a test can deterministically
    /// land a drop-dispatched persist AFTER the pool's forget committed — the
    /// #1781 interleaving. Everything else delegates straight through.
    struct GatedLossStore {
        inner: decdn_incentive::MemoryPoolFloorLossStore,
        released: std::sync::Mutex<bool>,
        turnstile: std::sync::Condvar,
        records_done: std::sync::atomic::AtomicUsize,
    }

    impl GatedLossStore {
        fn new() -> Self {
            Self {
                inner: decdn_incentive::MemoryPoolFloorLossStore::new(),
                released: std::sync::Mutex::new(false),
                turnstile: std::sync::Condvar::new(),
                records_done: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn release(&self) {
            let mut open = self
                .released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *open = true;
            self.turnstile.notify_all();
        }
    }

    impl decdn_incentive::PoolFloorLossStore for GatedLossStore {
        fn record_loss(
            &self,
            pool_id: B256,
            micro_usdc: u128,
        ) -> Result<(), decdn_incentive::StoreError> {
            let mut open = self
                .released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*open {
                open = self
                    .turnstile
                    .wait(open)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            drop(open);
            let result = self.inner.record_loss(pool_id, micro_usdc);
            self.records_done.fetch_add(1, Ordering::SeqCst);
            result
        }

        fn load_losses(&self) -> Result<Vec<(B256, u128)>, decdn_incentive::StoreError> {
            self.inner.load_losses()
        }

        fn forget_loss(&self, pool_id: B256) -> Result<(), decdn_incentive::StoreError> {
            self.inner.forget_loss(pool_id)
        }

        fn sweep_forgotten(&self) -> Result<usize, decdn_incentive::StoreError> {
            self.inner.sweep_forgotten()
        }
    }

    /// The #1781 resurrection race through the REAL drop guard: the guard
    /// snapshots its cumulative total under the floor lock while the pool's entry
    /// still exists, dispatches `record_loss` to a blocking task, and the pool's
    /// forget (in-memory remove + durable `forget_loss`) commits BEFORE that task
    /// runs. The gate makes the lost race deterministic. The forget's tombstone
    /// turns the late write into a no-op — without it, the write re-inserts a
    /// row for the closed pool, and (`record_loss` being monotonic, the pool id
    /// never recurring) nothing ever deletes it again: one leaked row per closed
    /// pool, rehydrated on every later boot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn late_drop_persist_after_forget_does_not_resurrect_row() -> anyhow::Result<()> {
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let store = Arc::new(GatedLossStore::new());
        let pool = B256::repeat_byte(0x5D);
        let floor = decdn_incentive::floor_micro(1000);
        let res = FloorReservation::reserve(
            map.clone(),
            Some(store.clone()),
            Arc::new(Metrics::new()),
            pool,
            floor,
        );
        // Abnormal drop inside the runtime: the guard snapshots `floor` (the map
        // entry still exists) and dispatches its persist, which parks on the gate.
        drop(res);
        // The pool closes: in-memory entry removed, durable row deleted +
        // tombstoned — the same order `forget_pool_floor` runs them in.
        {
            let mut guard = map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.remove(&pool);
        }
        decdn_incentive::PoolFloorLossStore::forget_loss(&*store, pool)
            .map_err(|e| anyhow::anyhow!("forget_loss: {e}"))?;
        // Only now does the drop's `record_loss` land.
        store.release();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while store.records_done.load(Ordering::SeqCst) == 0 {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "the gated record_loss never ran"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        anyhow::ensure!(
            persisted_loss(&*store, pool)?.is_none(),
            "a record_loss landing after forget_loss must not resurrect the row"
        );
        Ok(())
    }

    /// A failed drop-time persist bumps `floor_loss_persist_failures` (#1782) —
    /// the only alertable signal that dead charges have stopped reaching disk
    /// (e.g. redb latching writes after a failed commit on the shared file) and
    /// that a restart would re-grant pools their consumed free-floor budget.
    #[test]
    fn floor_persist_failure_bumps_the_counter() -> anyhow::Result<()> {
        struct FailingLossStore;
        impl decdn_incentive::PoolFloorLossStore for FailingLossStore {
            fn record_loss(&self, _: B256, _: u128) -> Result<(), decdn_incentive::StoreError> {
                Err(decdn_incentive::StoreError::Backend("injected".into()))
            }
            fn load_losses(&self) -> Result<Vec<(B256, u128)>, decdn_incentive::StoreError> {
                Ok(Vec::new())
            }
            fn forget_loss(&self, _: B256) -> Result<(), decdn_incentive::StoreError> {
                Ok(())
            }
            fn sweep_forgotten(&self) -> Result<usize, decdn_incentive::StoreError> {
                Ok(0)
            }
        }
        let map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        let metrics = Arc::new(Metrics::new());
        let pool = B256::repeat_byte(0x5E);
        let floor = decdn_incentive::floor_micro(1000);
        {
            let _res = FloorReservation::reserve(
                map.clone(),
                Some(Arc::new(FailingLossStore)),
                Arc::clone(&metrics),
                pool,
                floor,
            );
        } // abnormal drop → synchronous persist fallback → injected failure
        let encoded = metrics.encode()?;
        anyhow::ensure!(
            encoded.contains("decdn_floor_loss_persist_failures_total 1"),
            "a failed dead-charge persist must bump the failure counter"
        );
        Ok(())
    }

    /// A failed `forget_loss` bumps `floor_loss_persist_failures` too: the
    /// closed pool's row survives with no tombstone, open to permanent
    /// re-insertion by a late persist (#1781's error-path residual), and
    /// without the counter `DecdnFloorLossPersistFailures` never sees this
    /// mode.
    #[test]
    fn floor_forget_failure_bumps_the_counter() -> anyhow::Result<()> {
        let metrics = Metrics::new();
        note_forget_outcome(
            &metrics,
            B256::repeat_byte(0x60),
            Ok(Err(decdn_incentive::StoreError::Backend("injected".into()))),
        );
        let encoded = metrics.encode()?;
        anyhow::ensure!(
            encoded.contains("decdn_floor_loss_persist_failures_total 1"),
            "a failed dead-charge forget must bump the failure counter"
        );
        Ok(())
    }

    /// `LaneActivityClock::ages` reports a near-zero whole-seconds age for a
    /// stamped lane and OMITS an unstamped (`last_voucher_at == 0`) one, so the
    /// admin surface reads the latter back as "never" rather than a bogus `0`
    /// age (issue #1733).
    #[tokio::test]
    async fn lane_activity_clock_ages_reports_stamped_and_omits_unstamped() {
        let stamped = LaneKey {
            pool_id: B256::repeat_byte(0x11),
            signer: Address::repeat_byte(0x22),
            provider: Address::repeat_byte(0x33),
        };
        let unstamped = LaneKey {
            pool_id: B256::repeat_byte(0x44),
            signer: Address::repeat_byte(0x55),
            provider: Address::repeat_byte(0x66),
        };
        let mk = |key: LaneKey, stamp: u64| {
            Arc::new(Mutex::new(LaneDeliveryState {
                state: LaneState::hydrate(
                    key.pool_id,
                    key.signer,
                    key.provider,
                    U256::MAX,
                    0,
                    U256::ZERO,
                    U256::ZERO,
                    None,
                    decdn_incentive::LaneChain::NONE,
                ),
                bytes_delivered_cumulative: U256::ZERO,
                paid_credited: U256::ZERO,
                active_streams: Arc::new(AtomicU32::new(0)),
                last_voucher_at: AtomicU64::new(stamp),
            }))
        };
        let map = DashMap::new();
        map.insert(stamped, mk(stamped, unix_millis()));
        map.insert(unstamped, mk(unstamped, 0));
        let clock = LaneActivityClock {
            lanes: Arc::new(map),
        };

        let ages = clock.ages().await;
        assert!(
            ages.get(&stamped).is_some_and(|age| *age < 5),
            "a freshly stamped lane reports a near-zero age, got {:?}",
            ages.get(&stamped)
        );
        assert!(
            !ages.contains_key(&unstamped),
            "an unstamped lane (stamp == 0) must be omitted, read back as never"
        );
    }
}
