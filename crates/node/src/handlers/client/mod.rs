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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use dashmap::DashMap;
use decdn_cache::{CHUNK_GROUP_BYTES, CacheEngine, CacheError, Hash};
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
    ALPN_CLIENT, APP_ERR_RATE_LIMITED, CHUNK_BYTES, FrameError, decode_message, encode_message,
    is_unknown_variant, read_frame, write_frame,
};
use iroh::PublicKey;
use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};

use crate::dispatch::{ConnectionLimiter, RejectReason};
use crate::metrics::Metrics;
use crate::node_origin::NodeOrigin;
use crate::receipt_log::{RawReceipt, ReceiptSink};
use crate::warn_throttle::WarnThrottle;

// The paid-delivery methods are split across concern-focused submodules, each
// a bare `impl ClientHandler` block over the fields defined here. Support
// types, consts, and free functions stay in this module so every submodule
// (and the test module) can reach them via `use super::*` — Rust makes a
// module's private items visible to its descendants (#1254).
mod delivery;
mod dispatch;
mod fill;
mod outcome;
mod proof_wait;
mod ramp;
mod serve_encoder;
mod serve_leg;
mod serve_waits;
mod voucher;
mod window;
mod wire;

pub(crate) use proof_wait::PROOF_WAIT_CEILING;

/// Default per-connection concurrent-stream cap for `cdn/client/v1` (ADR 005
/// §Concurrent stream limits). The QUIC transport config also caps bidi
/// streams at this value; the application semaphore makes the per-ALPN bound
/// explicit and testable.
pub const MAX_CLIENT_STREAMS: usize = 100;

// Per-stage timeouts so a stalled peer cannot pin a stream task indefinitely.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the serve loop waits for a proof while the connection sends no new
/// STREAM frame. The proofs of one owed chunk never wait past
/// [`proof_wait::PROOF_WAIT_CEILING`] in total, from the chunk's first proof
/// wait (see [`proof_wait`]). Traffic on sibling streams of the same
/// connection counts as progress, so it can hold a wait up to that ceiling.
const VOUCHER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const REJECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(250);
/// After a clean `StreamEnd`, how long the serve waits for the client's FIN while
/// draining its send half (see [`drain_recv_to_fin`]). A conforming client
/// finishes its send right after its last voucher, so the FIN lands within a round
/// trip; this only bounds a client that completes delivery but never FINs from
/// pinning the serve task.
const POST_END_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
/// Fallback overall deadline for opening a window-paced pull (#856) when no
/// pull-through deadline is configured. In practice the runtime always sets
/// one alongside the window provider, so this only guards a misconfiguration.
const WINDOW_PULL_FALLBACK_DEADLINE: Duration = Duration::from_mins(1);
/// The largest first frame a serve writes ([`ClientHandler::first_frame_target`]):
/// four bao chunk groups. It sits above the one-group floor the pull side
/// reserves for a prefetch past a shut window (`decdn_client::PULL_WINDOW_FLOOR`),
/// so the miss leg's pairing holds.
const FIRST_FRAME_BYTES: u64 = 4 * decdn_bao_range::CHUNK_GROUP_BYTES;

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
    /// Lane-wide cumulative bytes the lane's claim covers: the signed bytes
    /// extended by the chunks the chain has proved.
    bytes_delivered_cumulative: U256,
    /// Cumulative wire bytes CREDITED to streams' paid headroom on this lane
    /// (design rule #1). Monotone and never exceeds the chain-extended watermark
    /// the lane has settled, so a benign already-satisfied proof — which does
    /// not raise the watermark — can only advance a stream's window for bytes
    /// the lane has actually settled, never for unpaid delivered bytes.
    paid_credited: U256,
    /// Wall-clock (Unix milliseconds) of the last accepted voucher on this lane,
    /// or `0` when this process has accepted none since it hydrated the lane
    /// (issue #1733). Stamped by [`ClientHandler::commit_one_proof`] under the
    /// per-lane lock it already holds — a field write, not a separate global
    /// mutex — and read back by [`LaneActivityClock::ages`] for the admin
    /// "seconds since last voucher" readout. Best-effort liveness bookkeeping:
    /// it gates nothing, and the stamp lifecycle follows the lane row (a
    /// forgotten lane drops it automatically).
    last_voucher_at: AtomicU64,
    /// Paid wire bytes that finished streams on this lane returned for the next
    /// stream's credit ramp (ADR 003 §Credit window). In memory only: a lane this
    /// process loads or registers starts with an empty pool.
    ramp_pool: Arc<ramp::RampPool>,
}

impl LaneDeliveryState {
    /// The delivery state for a lane this process loads or registers, with its
    /// whole claim — signed bytes and proved chain frontier — already credited.
    ///
    /// Every byte the lane's claim covers was delivered before this entry
    /// existed — by an earlier process, or before the lane was last forgotten —
    /// to a stream that either took the credit or ended with it unclaimed. No
    /// stream outlives the entry it drew on. A new stream delivers
    /// only new bytes, so none of that claim is headroom it may draw on.
    /// Crediting only the signed half would hand each restart the proved
    /// frontier — up to a whole chain — as free credit for proofs that pay
    /// nothing.
    fn hydrated(state: LaneState) -> Self {
        let owed_bytes = state.owed_bytes();
        Self {
            state,
            bytes_delivered_cumulative: owed_bytes,
            paid_credited: owed_bytes,
            last_voucher_at: AtomicU64::new(0),
            ramp_pool: Arc::default(),
        }
    }
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
                    last_voucher_at: AtomicU64::new(now),
                    ramp_pool: Arc::default(),
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

/// One capability signer's slice of a pool's floor accounting (ADR 003 §Pool
/// solvency, per-signer floor isolation). Two independent concerns live here.
///
/// `live_reservation` is the `µUSDC` this signer's in-flight streams currently
/// reserve — un-vouchered floor delivered ahead of payment, released as each stream
/// pays. It is the concurrency/solvency dimension: it carries no memory and never
/// penalizes a signer for quitting. Bounding it per signer (`k · one_window`) is
/// what stops one capability key from taking a shared pool's whole live floor
/// budget from its co-tenants (ADR 003 §Pool solvency, per-signer floor isolation).
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct SignerFloorState {
    live_reservation: U256,
}

impl SignerFloorState {
    /// A row that carries no information: no live reservation. An unseen signer reads
    /// the same, so such a row can be pruned.
    fn is_empty(self) -> bool {
        self.live_reservation.is_zero()
    }
}

/// One pool's floor accounting (ADR 003 §Pool solvency). `live_reservation` is the
/// `µUSDC` currently reserved by in-flight streams — the hard money envelope, summed
/// across every signer and bounded by `remaining − M`. It is ephemeral: no stream is
/// live at restart, so it clears.
///
/// `signers` holds each capability signer's [`SignerFloorState`]: its slice of
/// `live_reservation`, so admission can bound one signer's concurrent un-vouchered
/// exposure. The pool's `live_reservation` stays the sum of the signer slices —
/// every charge and release touches both levels under one lock hold.
#[derive(Debug, Clone)]
pub(super) struct PoolFloorState {
    live_reservation: U256,
    signers: HashMap<Address, SignerFloorState>,
    /// Generation stamp, unique across every entry this process creates. A
    /// [`FloorReservation`] copies it at charge time and both reconcile paths
    /// compare it, so a guard whose pool was reclaimed
    /// ([`ClientHandler::forget_pool_floor`] removed the entry) and whose `pool_id`
    /// a later admission then re-entered reconciles against nothing, rather than
    /// decrementing counters it never contributed to against a signer row belonging
    /// to a different pool generation.
    epoch: u64,
}

/// Source of [`PoolFloorState::epoch`]. Process-wide and monotonic, so no two
/// entries share a stamp — including an entry re-created under a `pool_id` a
/// removed entry previously held.
static POOL_FLOOR_EPOCH: AtomicU64 = AtomicU64::new(0);

impl Default for PoolFloorState {
    fn default() -> Self {
        Self {
            live_reservation: U256::ZERO,
            signers: HashMap::new(),
            epoch: POOL_FLOOR_EPOCH.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl PoolFloorState {
    /// Live floor credit committed across every signer on the pool — the pool
    /// solvency quantity, bounded by `remaining − M`.
    const fn live_committed(&self) -> U256 {
        self.live_reservation
    }

    /// Drop a signer's entry once it carries no information — no live reservation. An
    /// unseen signer reads the same way (zero live reservation), so keeping such a row
    /// changes no decision.
    ///
    /// Without this the map only ever grows. ADR 003 §Revocation makes short-expiry
    /// session keys the intended usage, so a busy publisher mints signer identities
    /// steadily, and every one that reserves and pays cleanly would leave an empty row
    /// alive until the pool is reclaimed on-chain — inside a map locked on every
    /// admission.
    ///
    /// Safe against a live guard: a repaid guard's `Drop` returns before touching
    /// the map at all, and an unrepaid guard holds `live_reservation > 0` — every
    /// admission reserves at least one chunk at the on-chain-floored rate, so a
    /// reservation is never zero — and it cannot have its row pruned out from
    /// under it.
    fn prune_spent(&mut self, signer: Address) {
        if self
            .signers
            .get(&signer)
            .is_some_and(|lane| lane.is_empty())
        {
            self.signers.remove(&signer);
        }
    }
}

/// RAII hold for one stream's span-capped reservation against a pool's budget.
///
/// Construction charges the reserved amount to the pool's `live_reservation` (and
/// the signer's slice of it). [`Self::release_if_repaid`] frees the reservation
/// once THIS stream's cumulative payment reaches the reserved floor. On drop (every
/// exit path — success, `?`, disconnect, panic) the guard releases the live
/// reservation if it was not already repaid, so an abandoned stream never holds a
/// pool's floor headroom past its own lifetime. The reservation is owned by the
/// guard and never adjusted by hand, and every counter update saturates.
#[must_use = "dropping the guard at once releases its live reservation"]
pub(super) struct FloorReservation {
    map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
    pool_id: B256,
    /// Generation of the [`PoolFloorState`] this reservation is charged against
    /// (see [`PoolFloorState::epoch`]). Both reconcile paths skip an entry whose
    /// stamp differs: such an entry belongs to a later pool generation, and this
    /// guard's reservation went with the one that was removed.
    epoch: u64,
    /// The capability signer this reservation belongs to, so its live slice is
    /// released on the same signer row it was charged to.
    signer: Address,
    reserved: U256,
    /// Set by [`Self::release_live_repaid`]; makes drop a no-op (live already freed).
    /// Idempotent.
    repaid: AtomicBool,
}

impl FloorReservation {
    /// Reserve `reserved` `µUSDC` of the pool's budget. Charges
    /// `live_reservation += reserved` (saturating) under the sync lock — an O(1) map
    /// touch with no `.await` held, so a blocking lock is correct even on the async
    /// serve path (mirrors `lane_gauge_publish`). A poisoned lock recovers the
    /// guard rather than panicking; the reservation is best-effort accounting, never
    /// a safety gate.
    ///
    /// The standalone increment-then-build form, used only by the accumulator's
    /// own unit tests. The serve path admits through
    /// [`ClientHandler::try_reserve_floor`] instead, which needs the increment to
    /// happen inside its own check-and-reserve lock hold and so builds the guard
    /// via [`Self::new_charged`].
    #[cfg(test)]
    fn reserve(
        map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
        pool_id: B256,
        signer: Address,
        reserved: U256,
    ) -> Self {
        let epoch = {
            let mut guard = map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = guard.entry(pool_id).or_default();
            entry.live_reservation = entry.live_reservation.saturating_add(reserved);
            let lane = entry.signers.entry(signer).or_default();
            lane.live_reservation = lane.live_reservation.saturating_add(reserved);
            entry.epoch
        };
        Self::new_charged(map, pool_id, signer, reserved, epoch)
    }

    /// Build a guard for a floor that is ALREADY charged to `live_reservation`
    /// under the caller's own lock hold. This does NOT touch the map — the
    /// increment happens exactly once, at the caller's atomic check-and-reserve,
    /// so re-incrementing here would double-charge the pool. Used by
    /// [`ClientHandler::try_reserve_floor`], whose single lock hold covers both the
    /// budget check and the increment; `FloorReservation::reserve` is the test-only
    /// standalone form that increments first, then delegates here.
    const fn new_charged(
        map: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
        pool_id: B256,
        signer: Address,
        reserved: U256,
        epoch: u64,
    ) -> Self {
        Self {
            map,
            pool_id,
            epoch,
            signer,
            reserved,
            repaid: AtomicBool::new(false),
        }
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
        // in-memory face of #1781. The epoch test covers the other half of that
        // window: an entry present under this `pool_id` but stamped differently is
        // a LATER generation, re-entered by an admission that ran after the remove,
        // and subtracting from it would report a reservation this guard never
        // charged to it.
        if let Some(entry) = guard
            .get_mut(&self.pool_id)
            .filter(|entry| entry.epoch == self.epoch)
        {
            entry.live_reservation = entry.live_reservation.saturating_sub(self.reserved);
            // The signer entry moves with the pool total — the two levels are one
            // accumulator, so a release that touched only one would leave the
            // signer's share permanently consumed by a stream that paid for it.
            if let Some(lane) = entry.signers.get_mut(&self.signer) {
                lane.live_reservation = lane.live_reservation.saturating_sub(self.reserved);
            }
            entry.prune_spent(self.signer);
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

    /// Release the reservation for a serve that was REFUSED before the serve loop
    /// ran — no upstream USDC fronted, no downstream byte delivered. Frees the live
    /// reservation promptly (rather than at drop) and marks the guard repaid so the
    /// drop is a clean no-op. Mechanically a refused-unspent serve and a fully-repaid
    /// one both owe nothing, so this delegates to [`Self::release_live_repaid`]; the
    /// distinct name states the intent at the refusal call sites.
    fn release_unspent(&self) {
        self.release_live_repaid();
    }
}

impl Drop for FloorReservation {
    fn drop(&mut self) {
        // Release the live reservation on every exit path (success, `?`, disconnect,
        // panic) unless a paying stream already released it via
        // [`Self::release_live_repaid`]. An abandoned stream must not hold a pool's
        // floor headroom past its own lifetime, so an unrepaid drop frees the
        // reservation at both the pool and signer levels. No abandonment charge
        // survives the drop: bounding un-vouchered floor is the admission-time job of
        // the pool ceiling and the per-signer live cap (ADR 003 §Pool solvency).
        self.release_live_repaid();
    }
}

/// Which floor gate refused an admission
/// ([`ClientHandler::try_reserve_floor`]). They stay distinct here so the
/// per-reason metric separates "this pool cannot pay" from the node-local
/// per-signer live cap, and they part on the wire too: the pool arm speaks the
/// owner-only [`StreamError::InsufficientDeposit`] (both floor gates run past the
/// lane-ownership proof — option 2 / #2013), while the signer arm stays a plain
/// `NotFound`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FloorRefusal {
    /// The pool-wide ceiling: `remaining − M` cannot cover the live floor
    /// reservation already committed across every signer on the pool plus this new
    /// floor. This is the hard money envelope; it clears with a top-up.
    PoolExhausted,
    /// The per-signer LIVE concurrency cap: the pool can still pay, but THIS signer
    /// already holds `k` windows of live un-vouchered reservation, so admitting
    /// another would let one signer front more concurrent credit than its share of
    /// the pool covers. Carries the cap that refused, read under the same lock hold
    /// as the decision, so the operator line reports the number that actually
    /// applied. Clears as the signer's in-flight streams pay.
    SignerAtCap { signer_cap: U256 },
}

/// What a floor-admission refusal needs to describe itself
/// ([`ClientHandler::log_floor_refusal`]). Grouped rather than passed positionally
/// because the two messages read different fields: the pool arm wants the pool's
/// headroom against the reserved `ceiling`, the signer arm wants the signer. The
/// signer's cap is NOT here — it rides on [`FloorRefusal::SignerAtCap`], read under
/// the lock hold that made the decision. `remaining` is the raw `getPool` value, so
/// the callee derives the headroom rather than every call site deriving it.
#[derive(Debug, Clone, Copy)]
pub(super) struct FloorRefusalSite {
    pub pool_id: B256,
    pub signer: Address,
    pub hash: Hash,
    pub remaining: U256,
    /// The `µUSDC` this admission asked to reserve.
    pub ceiling: U256,
}

impl From<FloorRefusal> for ServeRejectReason {
    /// The refusal reason a serve-path admission gate reports for this cap. Keeping
    /// the mapping on the conversion makes an inconsistent cap/reason pairing
    /// unrepresentable at the call sites, exactly as
    /// [`ServeRejectReason::wire_error`] does for the wire codes.
    fn from(refusal: FloorRefusal) -> Self {
        match refusal {
            FloorRefusal::PoolExhausted => Self::InsufficientDeposit,
            FloorRefusal::SignerAtCap { .. } => Self::SignerFloorAtCap,
        }
    }
}

/// Server-side classification of a `serve_stream` refusal, used to pick the
/// per-reason reject counter (#876). Finer-grained than the wire `StreamError`:
/// `CacheMiss`, `UnknownChannel`, and `OwnerMismatch` all ship as `NotFound` on
/// the wire (to avoid leaking channel existence), but are distinct here so an
/// operator can, e.g., isolate an unknown-channel abuse campaign.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServeRejectReason {
    EvictedSinceProbe,
    CacheMiss,
    InternalError,
    UnknownChannel,
    OwnerMismatch,
    /// The pool's on-chain remaining deposit, minus the node's refundable floor
    /// `M`, can no longer cover a credit window (ADR 003 §Pool solvency). UNLIKE
    /// its siblings below, this one does NOT collapse to `NotFound`: it is
    /// reachable only past the lane-ownership proof (a known lane keyed to a
    /// verified signer holding an owner-signed capability), so its audience is the
    /// proven pool owner — never an unauthenticated prober — and it ships the true
    /// wire [`StreamError::InsufficientDeposit`] so that owner's reactive top-up
    /// loop can fund past the node's `M` and re-open (option 2 / #2013, see
    /// [`Self::wire_error`]).
    InsufficientDeposit,
    /// A wired `PoolView` could not confirm the request's pool on-chain: the pool
    /// has no on-chain record, is closed/reclaimed, or the admit-path `getPool`
    /// faulted; or the admit-path `getAuthorization` faulted for a signer the node
    /// holds no cached registered read of. The node refuses rather than serve a
    /// pool it cannot confirm is live and solvent, or a signer it cannot confirm
    /// can pay (ADR 003 §Pool solvency). Collapses to `NotFound` on the wire
    /// (see [`Self::wire_error`]) — unlike [`Self::InsufficientDeposit`], this
    /// refusal can precede any lane-ownership proof, so it must stay a plain miss:
    /// a client cannot tell an unconfirmed pool from a drained one, while an
    /// operator can tell a chain/RPC problem from real deposit exhaustion.
    PoolUnconfirmed,
    /// The request's voucher signer is registered on-chain with `cap − spent` below
    /// a serve floor or with an expired registration. A signer's `cap`
    /// is shared across every provider (ADR 003 §Pool solvency), so a "spent"
    /// capability — one whose signer has already drawn its full `cap` at other nodes
    /// — is uncashable here: the node would serve for vouchers it could never
    /// redeem. Collapses to `NotFound` on the wire (see [`Self::wire_error`]) — it
    /// keys on a per-signer quantity distinct from the pool floor
    /// [`Self::InsufficientDeposit`] names, and stays a plain miss so no signer
    /// state leaks.
    SignerCapExhausted,
    /// One capability signer's live un-vouchered reservation already fills its
    /// `k`-window concurrency cap (ADR 003 §Pool solvency, per-signer floor
    /// isolation). The pool itself can still pay and co-tenants are unaffected; the
    /// cap is signer-scoped and clears as that signer's in-flight streams pay.
    /// Collapses to `NotFound` on the wire (see [`Self::wire_error`]) — a node-local
    /// concurrency stop, cleared by waiting rather than by a top-up, so unlike
    /// [`Self::InsufficientDeposit`] it names no owner-actionable pool state.
    SignerFloorAtCap,
    /// A whole-blob request from an active staker, refused while this node's own
    /// upstream open for the blob is in progress and no live fill exists to
    /// coalesce onto (#2224). Answering it would let partial holders pull the
    /// blob from each other in a loop. Collapses to `NotFound` on the wire (see
    /// [`Self::wire_error`]) so the requester tries its next candidate and scores
    /// no fault: the refusal is a loop guard, never a sign of a degraded node,
    /// whatever an earlier tier of the request saw.
    PullLoopGuard,
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
    /// A pull-through request whose start sits at or past this node's
    /// buyer-side `max_blob_size` ceiling (ADR 005 §`BlobTooLarge` enforcement).
    /// The gate reads only the requester's `byte_offset`, never the upstream's
    /// unverified `total_bytes` claim (#1895), and fires before the node commits
    /// to the stream. Ships the true wire [`StreamError::BlobTooLarge`] (see
    /// [`Self::wire_error`]), so the requester drops this node for the blob.
    BlobTooLarge,
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
    /// The node has been unable to reach the chain for longer than
    /// `blockchain.chain_staleness_grace_sec` (ADR 011 § Serving while
    /// chain-stale), so its deny-set, pool-solvency, and signer-cap guards are
    /// all reading stale state. It refuses rather than sign a serve it can no
    /// longer vouch for. Collapses to `NotFound` on the wire (see
    /// [`Self::wire_error`]): the refusal is reputation-benign and the client
    /// should re-route to a peer whose chain reads are live, exactly as for a
    /// miss.
    ChainStale,
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
    /// The requester side of this mapping is `decdn_client::UpstreamRefused`,
    /// which recovers the wire code — and ONLY the wire code — from a refusal
    /// (#1144). So the `NotFound` collapse is what a requester sees for the miss
    /// reasons below, and the reputation consequences it draws must hold for the
    /// weakest of them. They do: it scores `NotFound` as no fault at all, and only
    /// `InternalError` as a degraded peer. `InsufficientDeposit` is the lone
    /// non-collapsing floor refusal — reachable only past the lane-ownership proof,
    /// so it is spoken to the proven owner (option 2 / #2013) and likewise scored
    /// as no peer fault.
    const fn wire_error(self) -> StreamError {
        match self {
            // `InsufficientDeposit` is the one floor-`M` refusal that does NOT
            // collapse to `NotFound` (ADR 003 §Pool solvency, option 2 / #2013). It is
            // reachable only past the lane-ownership proof — the floor gate fires behind
            // a known lane keyed to a verified signer that holds an owner-signed
            // capability — so its audience is never an unauthenticated prober but the
            // proven pool owner, which already reads the pool's on-chain `remaining` and
            // so learns no balance off the wire it could not compute. Speaking the true
            // reason lets the owner's reactive top-up loop fund past the node's
            // larger-than-estimated `M` and re-open, instead of dead-ending on an
            // ambiguous `NotFound`. `SignerCapExhausted`, `SignerFloorAtCap`, and
            // `PoolUnconfirmed` stay collapsed below: each keys on a different quantity
            // than the pool floor this signal names.
            Self::InsufficientDeposit => StreamError::InsufficientDeposit,
            // `RangeNotSatisfiable` collapses to
            // `NotFound` alongside the other "won't serve this" reasons: an
            // out-of-bounds bounded range is a client error, but signalling it as
            // `NotFound` (rather than `InternalError`) keeps it reputation-benign —
            // a requester scores `InternalError` as a degraded peer (#1144), and a
            // client's own malformed range must not penalise the node for it. The
            // distinction survives in the per-reason metric.
            Self::CacheMiss
            | Self::UnknownChannel
            | Self::OwnerMismatch
            | Self::PoolUnconfirmed
            | Self::SignerCapExhausted
            | Self::SignerFloorAtCap
            | Self::PullLoopGuard
            | Self::LoadShedHit
            | Self::LoadShedMiss
            | Self::RangeNotSatisfiable
            | Self::ForeignNamespaceDeclined
            | Self::ChainStale => StreamError::NotFound,
            Self::EvictedSinceProbe => StreamError::EvictedSinceProbe,
            Self::InternalError => StreamError::InternalError,
            // `BlobTooLarge` does not collapse to `NotFound`. ADR 005 gives it its
            // own retry rule: the ceiling is a stable node policy, so the requester
            // must not ask this node for the same blob again. A `NotFound` would
            // send the requester back here for each range. The code reveals only
            // this node's own ceiling, which is no secret.
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
/// ONLY server-side place the true cause is observable, since the distinct
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
    /// The `outcome` value a `pull_through` span records.
    const fn as_str(self) -> &'static str {
        match self {
            Self::Filled => "filled",
            Self::CleanMiss => "clean_miss",
            Self::HardFault => "hard_fault",
        }
    }
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

/// The serve-miss exit that refused a request. It names the cause in the
/// refusal's log line only: the wire answer depends on the refusal's reason,
/// never on its path ([`ServeRejectReason::wire_error`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissPath {
    /// The pull leg opened, but its ranked candidates do not cover part of the
    /// range this node does not hold (#2195).
    CoverageGate(CoverageShortfall),
    /// The pull leg open ended without a target, for the reason the
    /// [`PullMiss`](crate::node_origin::PullMiss) names: no reachable provider
    /// or every candidate declined (`clean_miss`), every candidate quoted above
    /// the buy ceiling (`below_margin`), or a fault in this node (`local_fault`).
    PullLegMiss(crate::node_origin::PullMiss),
    /// The pull leg open ran past its deadline: the configured pull-through
    /// timeout, or [`WINDOW_PULL_FALLBACK_DEADLINE`] when none is set.
    PullLegTimeout,
    /// The buffered pull-through ran and did not fill the blob. A deadline
    /// expiry lands here too: this tier has no separate timeout path.
    BufferedMiss,
    /// The request may make this node spend, but no node-to-node pull-through
    /// is configured. The local-origin tiers may have run and missed first.
    NoPullThrough,
    /// The request may not make this node spend: it carries no verified
    /// binding, or this node holds no lane for its signer. No fill tier ran.
    /// A remote peer reaches this path at will, so its line is throttled.
    NoLane,
}

impl MissPath {
    /// The `path` value the refusal's log line records.
    const fn as_str(self) -> &'static str {
        match self {
            Self::CoverageGate(_) => "coverage_gate",
            Self::PullLegMiss(_) => "pull_leg_miss",
            Self::PullLegTimeout => "pull_leg_timeout",
            Self::BufferedMiss => "buffered_miss",
            Self::NoPullThrough => "no_pull_through",
            Self::NoLane => "no_lane",
        }
    }
}

/// Why the coverage gate refused: the part of the pull range no ranked
/// candidate covers (#2195).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CoverageShortfall {
    /// Start of the range the pull would fetch. It differs from the request's
    /// start when this serve attaches to a live fill for the front of it.
    pull_offset: u64,
    /// Length of the range the pull would fetch.
    pull_len: u64,
    /// How many ranked candidates the pull leg holds.
    candidates: usize,
    /// The first 1 KiB chunk no candidate covers.
    first_uncovered_chunk: Option<u64>,
}

/// A terminal serve-miss refusal: the reject reason
/// ([`FillOutcome::miss_reason`]) and the exit that refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MissRefusal {
    /// The reason the refusal carries: `CacheMiss`, or `InternalError` when a
    /// tier this request attempted hit a hard fault.
    reason: ServeRejectReason,
    /// The exit that refused.
    path: MissPath,
}

impl MissRefusal {
    /// The refusal `path` sends, given whether any tier attempted for this
    /// request hit a hard fault.
    const fn new(fault_seen: bool, path: MissPath) -> Self {
        Self {
            reason: FillOutcome::miss_reason(fault_seen),
            path,
        }
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
    /// This node's iroh identity. Carried for diagnostics — the handler does
    /// not put it on the wire; `StreamResponseBody` has no node-id field.
    pub node_id: PublicKey,
    /// Shared metrics registry the serve path records into.
    pub metrics: Arc<Metrics>,
    /// Per-source connection-rate gate, applied before any work is done.
    pub limiter: Arc<ConnectionLimiter>,
    /// Overload-protection gate: sheds new serves under resource pressure.
    pub shed: Arc<crate::load_shed::LoadShedController>,
    /// Blob store the serve path reads from, and the pull-through target on a
    /// miss.
    pub cache: CacheEngine,
    /// The operator's Ethereum key. Signs the `slash_sig` on each
    /// `StreamResponse`; every other use reads it only for its address.
    /// Client bindings are verified here, never signed.
    pub eth_signer: Arc<PrivateKeySigner>,
    /// EIP-712 domain for slash attestations.
    pub slash_domain: Eip712Domain,
    /// EIP-712 domain for payment vouchers.
    pub voucher_domain: Eip712Domain,
    /// EIP-712 domain for lane bindings.
    pub bind_domain: Eip712Domain,
    /// Durable per-lane cumulative state. Fences voucher replay across a
    /// restart (ADR 003 §Off-chain voucher state persistence).
    pub channel_state_store: Arc<dyn PoolStateStore>,
    /// Non-blocking sink for the served-and-paid audit log. One receipt per
    /// accepted voucher, so a single delivery emits several.
    pub receipt_sink: Arc<dyn ReceiptSink>,
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
    /// Cap on concurrently served streams within one connection — the
    /// semaphore is built per accepted connection, not per node.
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
    /// Liveness of the node's chain reads (ADR 011 § Serving while chain-stale).
    /// `Some` only when the blacklist watcher is wired; the admit path refuses a
    /// serve once this reads stale. `None` (dev/test, no chain) disables the
    /// gate — the same fail-open shape as an unwired [`Self::pool_view`].
    pub chain_freshness: Option<crate::chain_freshness::ChainFreshness>,
    // Optional wiring — `None` unless the deployment enables the feature.
    /// Best-effort nudge to the settlement service that a lane's accrued
    /// claim advanced; sent on every accepted voucher, against no threshold.
    /// `None` when no settlement service is wired.
    pub redeem_hint: Option<mpsc::Sender<LaneKey>>,
    /// Deadline for the node-to-node cache-miss pull, so a slow upstream
    /// cannot pin the delivery path. `None` disables the node-to-node leg
    /// only; a miss can still fill from the local origin via
    /// [`Self::local_populate`].
    pub pull_through: Option<Duration>,
    /// Deadline for filling a miss from this node's OWN configured fs/http/s3
    /// origin, tried ahead of any node-to-node path and independent of
    /// [`Self::pull_through`]. `None` skips the local-origin tier.
    pub local_populate: Option<Duration>,
    /// Selects the window-paced node-to-node pull leg, which bounds
    /// speculative spend to the ramped credit window. `None` falls back to the
    /// buffered `populate` path when [`Self::pull_through`] is set.
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
    /// Application-layer idle-close ceiling for a whole connection (ADR 005
    /// §Connection lifetime). `None` — the production path — reads as
    /// `APP_IDLE_TIMEOUT` (30s); tests set a shorter one.
    pub idle_timeout: Option<Duration>,
    /// Wall-clock cadence for the mid-stream pool-solvency re-check (ADR 003
    /// §Pool solvency). `None` (the default and production path) reads as
    /// [`crate::pool_view::POOL_RECHECK_INTERVAL`]; a shorter value is set at
    /// construction only by tests, so a drain case need not wait a real interval.
    pub pool_recheck_interval: Option<Duration>,
    /// Per-signer LIVE concurrency cap `k`, in credit windows (ADR 003 §Pool solvency,
    /// per-signer floor isolation): the most live un-vouchered reservation one signer
    /// may hold, `k · one_window`. The runtime sets it from
    /// `blockchain.pool_floor_signer_live_windows`; the default of `u64::MAX` leaves it
    /// effectively unbounded, which is what unit tests that only exercise the pool-wide
    /// bound want. Lower-clamped to one window at use.
    pub pool_floor_signer_live_windows: u64,
    /// ADR 041 serve-credit boundary in front of the per-source warming
    /// allowance — the ledger the buy loop
    /// ([`crate::node_origin::NodeOriginConfig`]) debits and eviction forgets. On
    /// each clean serve the handler enqueues the realized operator margin for the
    /// source that speculatively warmed the blob (a no-op for an untagged /
    /// non-speculative hash), so the stream task never waits on the ledger lock.
    /// Defaults to the inert [`crate::warming_allowance::NoopWarmingCreditSink`];
    /// the runtime overwrites it with the channel sink from
    /// [`crate::warming_allowance::spawn_warming_creditor`], and a handler left
    /// with the default simply applies no warming credit.
    pub warming_credit: Arc<dyn crate::warming_allowance::WarmingCreditSink>,
    /// Live operator fee-share (basis points) cell (`FeeRouter.getShares()[0]`), the
    /// `(1 − f)` numerator the ADR 041 serve credit realizes. Defaults to zero
    /// (tests): a zero share credits nothing.
    pub operator_shares: crate::fee_shares::OperatorShares,
    /// Origin-only policy (#1759). When `false`, `serve_stream` declines any
    /// hash its own backend does not hold — including a cache HIT for a
    /// foreign hash — before any discovery, lane accounting, or spend. `true`
    /// (the default) serves and relays foreign hashes as well.
    pub relay_foreign_namespaces: bool,
    /// Coarse wall clock for the two reads the voucher-accept path takes under
    /// the per-lane lock — the capability-expiry gate and the `last_voucher_at`
    /// stamp (issue #1792 item 4). `None` (the default and every test) means the
    /// handler builds its own unrefreshed clock, which reads the live wall clock
    /// on every call. The runtime sets `Some` with a refresher running, so each
    /// of those two reads becomes a relaxed atomic load instead of a
    /// `SystemTime::now()` syscall in the critical section.
    pub coarse_clock: Option<Arc<crate::coarse_clock::CoarseClock>>,
    /// The redeemer's self-tick interval in seconds
    /// (`blockchain.redeem_interval_secs`). The voucher-accept path stops
    /// accepting vouchers this interval plus the redeemer's landing slack before
    /// a capability expires ([`decdn_common::config::capability_expiry_margin_secs`],
    /// ADR 003 §Revocation). Defaults to the config default.
    pub redeem_interval_secs: u64,
}

impl std::fmt::Debug for ClientHandlerDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientHandlerDeps")
            .field("node_id", &self.node_id)
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
            pool_view: None,
            pool_min_remaining_deposit,
            rate_per_mb,
            max_concurrent_streams,
            content_deny,
            chain_freshness: None,
            redeem_hint: None,
            pull_through: None,
            local_populate: None,
            pull_through_origin: None,
            credit_max: decdn_common::config::DEFAULT_CREDIT_MAX,
            credit_ramp_divisor: decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
            frame_target_bytes: decdn_common::config::DEFAULT_FRAME_TARGET_BYTES,
            idle_timeout: None,
            pool_recheck_interval: None,
            // No-op per-signer live cap by default: the runtime overrides it from
            // config, and a caller that never sets it keeps the pool-wide ceiling as
            // the only floor bound (an unbounded per-signer live cap).
            pool_floor_signer_live_windows: u64::MAX,
            warming_credit: Arc::new(crate::warming_allowance::NoopWarmingCreditSink),
            operator_shares: crate::fee_shares::OperatorShares::new(0),
            relay_foreign_namespaces: decdn_common::config::DEFAULT_RELAY_FOREIGN_NAMESPACES,
            coarse_clock: None,
            redeem_interval_secs: decdn_common::config::DEFAULT_REDEEM_INTERVAL_SECS,
        }
    }
}

/// Capacity of the capability-verification cache (#1789 item 2). Bounded so a
/// flood of distinct capability sends cannot grow it without limit; `ecrecover`
/// is expensive enough that a 1024-entry cache still pays for itself across a
/// client that re-sends the same capability on every request.
///
/// The bound is on memory, not on attacker CPU: a flood of distinct garbage
/// signatures thrashes the entries and still pays one `ecrecover` per miss, so
/// the cache is a hit-path saving rather than an admission control.
const CAPABILITY_VERIFY_CACHE_CAPACITY: usize = 1024;

/// Cached outcome of a capability owner recovery: the recovered owner, or
/// `Invalid` for a signature that does not recover (high-`s`, bad recovery
/// id). Both are deterministic per `(digest, signature)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapabilityVerifyOutcome {
    /// `ecrecover` succeeded and recovered this owner.
    Owner(Address),
    /// The signature is malformed and recovers nothing (deterministic).
    Invalid,
}

/// Bounded LRU cache of capability owner-recovery outcomes, keyed by the full
/// signed material `(EIP-712 signing hash, signature bytes)`.
///
/// Ownership verification in [`ClientHandler::intake_capability`] runs
/// `ecrecover` on every capability-carrying request; `ecrecover` is a pure
/// function of `(digest, signature)`, so the same signed material always
/// recovers the same owner and the result can be cached deterministically.
/// Entry is only ever made for the canonical 65-byte EOA signature shape the
/// intake path already accepts; an `Invalid` value means "this signature is
/// malformed" — also deterministic, also cached, so a repeated malformed
/// capability is not re-recovered either.
///
/// Holds no I/O, and a hit is one hash lookup under one short lock, so the
/// cache never lengthens the open path it exists to shorten. It carries no TTL — unlike the TTL-anchored `dht::negative_cache`, an entry
/// lives until the capacity evicts it. That is safe because the value is the
/// recovered ADDRESS, not a verdict: [`ClientHandler::intake_capability`]
/// re-compares it against the live pool owner on every call, so an on-chain
/// owner transfer takes effect immediately.
#[derive(Debug)]
struct CapabilityVerifyCache {
    /// `(signing_hash, signature bytes)` → recovered outcome and the tick at
    /// which it was last used. Recency lives in the value rather than in the
    /// map's order, so a hit is one hash lookup and a field write — no
    /// reordering, no memmove.
    entries: HashMap<(B256, [u8; 65]), (CapabilityVerifyOutcome, u64)>,
    /// Monotonic use counter. Only the ORDER of these values matters, and
    /// `u64` at one tick per capability verification does not wrap.
    tick: u64,
    cap: usize,
}

impl Default for CapabilityVerifyCache {
    fn default() -> Self {
        Self::with_capacity(CAPABILITY_VERIFY_CACHE_CAPACITY)
    }
}

impl CapabilityVerifyCache {
    fn with_capacity(cap: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(cap),
            tick: 0,
            cap: cap.max(1),
        }
    }

    /// The cached recovery outcome for `(signing_hash, signature)`, or `None`
    /// on a miss. A hit re-stamps the entry as most-recently-used.
    fn get(&mut self, signing_hash: B256, signature: [u8; 65]) -> Option<CapabilityVerifyOutcome> {
        self.tick = self.tick.wrapping_add(1);
        let tick = self.tick;
        let (outcome, last_used) = self.entries.get_mut(&(signing_hash, signature))?;
        *last_used = tick;
        Some(*outcome)
    }

    /// Record `outcome` for `(signing_hash, signature)`, evicting the
    /// least-recently-used entry when full.
    ///
    /// The eviction scan is linear in `cap`, but it runs only on an insert that
    /// would overflow — i.e. on a path that has just paid an `ecrecover`, which
    /// costs orders of magnitude more than a 1024-entry scan. The hit path,
    /// which is the one this cache exists to shorten, stays O(1).
    fn insert(
        &mut self,
        signing_hash: B256,
        signature: [u8; 65],
        outcome: CapabilityVerifyOutcome,
    ) {
        self.tick = self.tick.wrapping_add(1);
        let tick = self.tick;
        let key = (signing_hash, signature);
        if self.entries.insert(key, (outcome, tick)).is_some() {
            return;
        }
        while self.entries.len() > self.cap {
            let Some(least_recent) = self
                .entries
                .iter()
                .min_by_key(|(_, (_, last_used))| *last_used)
                .map(|(k, _)| *k)
            else {
                break;
            };
            self.entries.remove(&least_recent);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
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
    /// accepted; the actual disk write happens off the hot path in the background
    /// receipt writer, so receipt-log I/O can never back-pressure paid delivery.
    /// A dropped receipt (queue full) is non-fatal — the payment already advanced
    /// the lane watermark.
    receipt_sink: Arc<dyn ReceiptSink>,
    /// Cached `getPool` view for the floor-`M` and ADR 011 funder gates. `None`
    /// (tests) makes both gates fail open.
    pool_view: Option<Arc<dyn crate::pool_view::PoolView>>,
    /// Liveness of the node's chain reads (ADR 011 § Serving while chain-stale).
    /// `Some` only when the blacklist watcher is wired; the admit path refuses
    /// once it reads stale. `None` (tests) disables the gate.
    chain_freshness: Option<crate::chain_freshness::ChainFreshness>,
    /// Per-lane state, hydrated from the store at construction. The sharded map
    /// resolves independent lanes concurrently — a lookup keyed by [`LaneKey`]
    /// is a point read that only locks that key's shard; each inner mutex
    /// serializes voucher application for one lane across its concurrent streams
    /// (ADR 003 §concurrent streams). No call site holds a map entry across an
    /// `.await`, so lane lookup never blocks an unrelated lane's admission.
    lanes: Arc<DashMap<LaneKey, Arc<Mutex<LaneDeliveryState>>>>,
    /// Live-lane count backing `decdn_lanes_open` (#1789 item 3): `fetch_add`
    /// on a real insert, `fetch_sub` on a real remove, seeded once at
    /// construction from the hydrated map. Holding the count in an atomic keeps
    /// the gauge off an ordered walk of the (possibly large) lane map on the
    /// registration path.
    lane_count: AtomicUsize,
    /// Test-only: a lane [`ClientHandler::register_lane_after_next_resolution`]
    /// armed, registered right after the next request resolves its lane.
    #[cfg(any(test, feature = "test-support"))]
    lane_after_resolution: std::sync::Mutex<Option<LaneState>>,
    /// Serializes the read-and-publish half of [`Self::tune_lane_gauge`], so
    /// two concurrent lane-lifecycle calls cannot publish `decdn_lanes_open`
    /// out of order. The atomic alone fixes the count, not the publication:
    /// without this the slower task's `set_lanes_open` overwrites a
    /// fresher lane count with its own staler one, and the gauge stays wrong
    /// until the next lifecycle event. Held across one atomic RMW and one gauge
    /// store, never across an `.await` or a map walk.
    lane_gauge_publish: std::sync::Mutex<()>,
    /// Bounded cache of capability owner-recovery outcomes keyed by the full
    /// signed material (#1789 item 2), so a client that re-sends the same
    /// capability on every request skips the per-request `ecrecover` in
    /// [`ClientHandler::intake_capability`].
    capability_verify_cache: std::sync::Mutex<CapabilityVerifyCache>,
    /// Floor accounting (ADR 003 §Pool solvency): per pool `live_reservation` and, per
    /// signer within each pool, that signer's live slice. Guards an O(1) map only and
    /// is never held across `.await` — a plain `std::sync::Mutex`, so a
    /// [`FloorReservation`]'s `Drop` can reconcile under it (a tokio mutex cannot be
    /// locked in `Drop`). Purely in-memory: no stream is live at restart, so it clears.
    pool_floor: Arc<std::sync::Mutex<HashMap<B256, PoolFloorState>>>,
    /// Per-signer LIVE concurrency cap `k`, in credit windows
    /// (see [`ClientHandlerDeps::pool_floor_signer_live_windows`]). Read by
    /// [`Self::signer_floor_cap`] on every admission
    /// ([`Self::try_reserve_floor`]). The mid-stream and direct-serve re-checks
    /// leave it out — see [`Self::pool_budget_covers_reserve`].
    pool_floor_signer_live_windows: u64,
    /// Redeem-hint sender to the on-chain settlement service (#327), set at
    /// construction via [`ClientHandlerDeps`]. `None` when no settlement service
    /// is wired (e.g. tests) — a hint is best-effort, so an absent sender or a
    /// full channel just skips it. Keyed by [`LaneKey`]: redemption is per-lane.
    redeem_hint: Option<mpsc::Sender<LaneKey>>,
    /// Node-to-node cache-miss pull-through deadline (#831), set at construction
    /// via [`ClientHandlerDeps`]. `None` (the default — feature off, and in
    /// tests) disables pull-through: a cache miss returns `NotFound`. When
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
    /// `None` disables local populate (miss ⇒ node→node path or a plain
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
    /// Live content deny-set (ADR 011), keyed on the pool FUNDER. Consulted at
    /// two points, both of which must gate or the check is bypassable: the
    /// open-time funder gate in `serve_stream`, right after the pool status
    /// resolves, and [`Self::takedown_landed`], the per-interval in-flight
    /// re-check every serve leg runs. The open-time gate precedes every fill
    /// tier, so omitting it would have this node front upstream USDC or origin
    /// egress on a blacklisted funder's behalf. The hash half of ADR 011 is the
    /// cache's own deny state (`CacheEngine::is_denied` / `refuses`).
    pub(crate) content_deny: Arc<crate::content_deny::ContentDenylist>,
    /// Served per-MB price, fixed at startup (see
    /// [`ClientHandlerDeps::rate_per_mb`]).
    rate_per_mb: u64,
    max_concurrent_streams: usize,
    /// Throttle for the insufficient-deposit refusal `warn!` (#1520). Unkeyed:
    /// the aggregate answers the triage question — "one client ran dry" versus
    /// "I am refusing everyone" — and the per-channel detail lives in the
    /// `debug!` beside it and in the counter.
    deposit_refusal_warn: WarnThrottle,
    /// The same window for the per-signer LIVE-cap arm. Kept separate from the
    /// pool arm because the two carry different remedies — a pool-wide shortfall
    /// clears with a top-up, a signer at its share does not — so one must not
    /// starve the other's line or pollute its `suppressed` count.
    signer_cap_refusal_warn: WarnThrottle,
    /// Throttle for the `warn!` on a client binding whose signature is invalid
    /// or recovers a different address. A remote peer triggers it at will. The
    /// two causes share one window on purpose: both are the same client fault
    /// with the same remedy (the client signs its binding wrongly), and each
    /// event also counts into `decdn_serve_stream_rejected_bad_binding_total`.
    binding_warn: WarnThrottle,
    /// Throttle for the `warn!` on a stream request that carries no verified
    /// binding. A remote peer triggers it at will.
    unbound_request_warn: WarnThrottle,
    /// Throttle for the `warn!` on a stream request that names no known lane. A
    /// remote peer triggers it at will.
    unknown_lane_warn: WarnThrottle,
    /// Throttle for the serve-miss refusal line on a request with no lane here
    /// ([`MissPath::NoLane`]). A remote peer triggers it at will.
    no_lane_miss_log: WarnThrottle,
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
    /// ADR 041 serve-credit sink in front of the per-source warming allowance the
    /// buy loop and eviction share. Enqueues the realized operator margin on each
    /// clean serve (a no-op for an untagged hash) without taking the ledger lock.
    warming_credit: Arc<dyn crate::warming_allowance::WarmingCreditSink>,
    /// Live operator fee-share (basis points), the ADR 041 serve-credit numerator.
    operator_shares: crate::fee_shares::OperatorShares,
    /// Origin-only policy (#1759), set at construction via
    /// [`ClientHandlerDeps::relay_foreign_namespaces`]. Read by the gate at the
    /// top of `serve_stream`.
    relay_foreign_namespaces: bool,
    /// Coarse wall clock for the voucher-accept path's two under-lock reads —
    /// the capability-expiry gate and the `last_voucher_at` stamp (issue #1792
    /// item 4). The runtime supplies one with a background refresher; a handler
    /// built without one (tests) gets a fresh [`CoarseClock`](crate::coarse_clock::CoarseClock)
    /// that reads the live wall clock on every call, so behavior is unchanged
    /// there.
    coarse_clock: Arc<crate::coarse_clock::CoarseClock>,
    /// How long before a capability's expiry the voucher-accept path stops
    /// accepting vouchers, in seconds: one redeem interval plus the redeemer's
    /// landing slack ([`decdn_common::config::capability_expiry_margin_secs`]).
    capability_expiry_margin_secs: u64,
}

impl std::fmt::Debug for ClientHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientHandler")
            .field("node_id", &self.node_id)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .finish_non_exhaustive()
    }
}

impl ClientHandler {
    /// The ALPN this handler answers on: `cdn/client/v1`, the single paid
    /// delivery protocol for both client-to-node and node-to-node transfers.
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
            map.insert(
                state.key(),
                Arc::new(Mutex::new(LaneDeliveryState::hydrated(state))),
            );
        }
        // Seed the atomic lane counter once at hydrate; `register_lane` /
        // `forget_lane` tune it from then on (#1789 item 3). The pool deposit
        // behind these lanes is published separately, by the redeemer tick that
        // can actually see it (#2072).
        let open_lanes = map.len();
        let lane_count = AtomicUsize::new(open_lanes);
        deps.metrics.set_lanes_open(open_lanes);
        // The floor accumulator is purely in-memory: no stream is live at boot, so
        // every `live_reservation` starts at zero and nothing is loaded from disk.
        let pool_floor: HashMap<B256, PoolFloorState> = HashMap::new();
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
            pool_view: deps.pool_view,
            chain_freshness: deps.chain_freshness,
            lanes: Arc::new(map),
            lane_count,
            #[cfg(any(test, feature = "test-support"))]
            lane_after_resolution: std::sync::Mutex::new(None),
            lane_gauge_publish: std::sync::Mutex::new(()),
            capability_verify_cache: std::sync::Mutex::new(CapabilityVerifyCache::default()),
            pool_floor: Arc::new(std::sync::Mutex::new(pool_floor)),
            pool_floor_signer_live_windows: deps.pool_floor_signer_live_windows,
            redeem_hint: deps.redeem_hint,
            pull_through: deps.pull_through,
            local_populate: deps.local_populate,
            pull_through_origin: deps.pull_through_origin,
            credit_max: deps.credit_max,
            credit_ramp_divisor: deps.credit_ramp_divisor,
            frame_target_bytes: deps.frame_target_bytes,
            content_deny: deps.content_deny,
            rate_per_mb: deps.rate_per_mb,
            max_concurrent_streams: deps.max_concurrent_streams,
            deposit_refusal_warn: WarnThrottle::new(Self::DEPOSIT_REFUSAL_WARN_INTERVAL),
            signer_cap_refusal_warn: WarnThrottle::new(Self::DEPOSIT_REFUSAL_WARN_INTERVAL),
            binding_warn: WarnThrottle::new(Self::PEER_FAULT_WARN_INTERVAL),
            unbound_request_warn: WarnThrottle::new(Self::PEER_FAULT_WARN_INTERVAL),
            unknown_lane_warn: WarnThrottle::new(Self::PEER_FAULT_WARN_INTERVAL),
            no_lane_miss_log: WarnThrottle::new(Self::PEER_FAULT_WARN_INTERVAL),
            idle_timeout: deps.idle_timeout,
            pool_recheck_interval: deps.pool_recheck_interval,
            warming_credit: deps.warming_credit,
            operator_shares: deps.operator_shares,
            relay_foreign_namespaces: deps.relay_foreign_namespaces,
            // A handler with no clock wired (tests) reads the live wall clock on
            // every call, exactly as before #1792 item 4.
            coarse_clock: deps
                .coarse_clock
                .unwrap_or_else(|| Arc::new(crate::coarse_clock::CoarseClock::new())),
            capability_expiry_margin_secs: decdn_common::config::capability_expiry_margin_secs(
                deps.redeem_interval_secs,
            ),
        })
    }

    /// ADR 041 serve credit: return the realized operator margin to the source that
    /// speculatively warmed `hash`, for a clean serve of `served_bytes`. The credit
    /// is `(operator_bps / 10_000) · P_sell · MB(served_bytes)`, matching the
    /// buy-loop debit's units. A no-op for an untagged hash (own-namespace or
    /// non-speculative), so calling it unconditionally on every serve is correct.
    ///
    /// The arithmetic runs here and the ledger update is handed to the wired
    /// [`crate::warming_allowance::WarmingCreditSink`], so this — the stream
    /// task's last act on a clean completion — costs whatever that sink costs.
    /// The runtime's channel sink is one tag-map shard read and one bounded
    /// `try_send`, and never waits on the bucket lock the buy loop takes, which
    /// is what the sink contract requires of anything on this path.
    fn credit_warming_serve(&self, hash: Hash, served_bytes: u64) {
        let sell_rate = self.rate_per_mb;
        let margin_per_mb = sell_rate
            .saturating_mul(u64::from(self.operator_shares.bps()))
            .saturating_div(10_000);
        let mb = served_bytes.div_ceil(decdn_protocol::MB_BYTES);
        self.warming_credit
            .credit(hash, mb.saturating_mul(margin_per_mb));
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

    /// The signer's total on-chain `spent` across every provider in `pool_id`, read
    /// from the event-fed projection ONLY (never a `getAuthorization`), for the
    /// mid-stream signer cap-headroom re-check. `None` when no pool-view is wired
    /// (dev/test, no chain) or the view holds no projection — the re-check then skips
    /// and delivery continues, matching how [`Self::pool_view_status_cached`] fails
    /// open. See [`crate::pool_view::PoolView::signer_spent_cached`].
    pub(super) async fn signer_spent_cached(&self, pool_id: B256, signer: Address) -> Option<u64> {
        self.pool_view
            .as_ref()?
            .signer_spent_cached(pool_id, signer)
            .await
    }

    /// Whether a live stream must stop because its voucher `signer` has drained its
    /// shared on-chain `cap` at other nodes since admission (ADR 003 §Pool solvency,
    /// mid-stream re-check). A signer's `cap` is shared across every provider, so a
    /// signer that spends it elsewhere leaves `held_cap − spent` unable to cover a
    /// serve floor here, and further vouchers redeem `min(desired, cap − spent) ≈ 0`
    /// — the node would eat the delivered bytes, unbounded for a large blob.
    ///
    /// `held_cap` is the `cap` the node already holds on the lane (the admit-time
    /// capability), and `spent` is read from the event-fed projection. The floor is
    /// one ramp-start credit window, the SAME quantity the admit-time gate confirms
    /// (`ClientHandler::serve_stream`), so the mid-stream threshold matches admission.
    ///
    /// Fails toward SERVING: `signer_spent_cached` returning `None` (no projection)
    /// skips the stop, and a projection that has not yet folded the signer's
    /// pre-admission spend only UNDER-counts `spent`, over-stating headroom. That is
    /// acceptable — the admit-time `getAuthorization` (#1958) already caught an
    /// already-exhausted signer authoritatively, so this only catches drain SINCE
    /// admit, and the on-chain `redeemMany` `min(desired, cap − spent)` is the
    /// backstop; this re-check only BOUNDS over-delivery, it is not a correctness
    /// gate. The caller ends the stream as `Stopped { SignerCapExhausted }`, which
    /// the dispatch sink counts on the mid-stream metric.
    pub(super) async fn signer_cap_drained_midstream(
        &self,
        pool_id: B256,
        signer: Address,
        held_cap: U256,
        rate_per_mb: u64,
    ) -> bool {
        let Some(spent) = self.signer_spent_cached(pool_id, signer).await else {
            return false;
        };
        let headroom = held_cap.saturating_sub(U256::from(spent));
        let floor_micro = U256::from(decdn_incentive::min_payment(
            self.credit_window(CHUNK_BYTES, 0),
            rate_per_mb,
        ));
        headroom < floor_micro
    }

    /// The pool's funder (`getPool.owner`) for the ADR 011 mid-stream takedown
    /// re-check, or `None` when no pool-view is wired or the read faulted (the
    /// re-check then falls back to the open-time gates and the hash-denylist
    /// re-check). Cached, so a per-MB call is cheap.
    pub(super) async fn pool_funder(&self, pool_id: B256) -> Option<Address> {
        self.pool_view_status(pool_id).await.map(|s| s.owner)
    }

    /// Recover an owner-signed capability's EIP-712 signer (ADR 003
    /// §Capability delegation), short-circuiting on the capability-verification
    /// cache (#1789 item 2). The caller compares the result against the pool
    /// owner; recovery says who signed, not whether the grant is accepted.
    ///
    /// `ecrecover` is a pure function of `(digest, signature)`, so the
    /// recovered owner for a given signed material is deterministic and can be
    /// cached safely: [`Self::capability_verify_cache`] maps the full
    /// `(signing hash, signature bytes)` to the recovered owner, and a client
    /// that re-sends the same capability (the documented recovery path) hits
    /// the cache instead of paying a fresh `ecrecover` per request. A
    /// tampered signature is a different key, so it can never be served a
    /// stale cached owner. Caching the recovered ADDRESS rather than an
    /// accept/reject verdict is what keeps this sound across an on-chain owner
    /// transfer: the comparison is re-made against the live owner every call.
    fn recover_capability_owner(&self, grant: &SignedCapability) -> CapabilityVerifyOutcome {
        let domain = &self.voucher_domain;
        let signing_hash = grant.capability.signing_hash(domain);
        let signature = grant.signature.as_bytes();
        // The lock is held only for the cache lookup/insert — a hash lookup plus
        // an `IndexMap` shift. The expensive `recover_owner` (secp256k1
        // ecrecover) runs OUTSIDE the lock, so a burst of distinct
        // capabilities at session open does not serialize on the cache mutex
        // and block a Tokio worker for the crypto. Concurrent races on the
        // same key re-run the recovery and overwrite the entry — duplicate
        // work under races is acceptable for a deterministic result.
        let recovered = {
            let mut cache = self
                .capability_verify_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cache.get(signing_hash, signature)
        };
        if let Some(outcome) = recovered {
            return outcome;
        }
        let outcome = match grant.recover_owner(domain) {
            Ok(owner) => CapabilityVerifyOutcome::Owner(owner),
            Err(_) => CapabilityVerifyOutcome::Invalid,
        };
        self.capability_verify_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(signing_hash, signature, outcome);
        outcome
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
    /// [`Self::recover_capability_owner`] (canonical-`s` ecrecover, matching the
    /// contract's verifiable set) and compares it to `pool_owner` — the confirmed
    /// on-chain owner the admit path already resolved (the dispatcher calls this
    /// only past the refuse-on-unknown-pool gate). A non-65-byte (contract-wallet /
    /// ERC-1271-shaped) owner signature drops rather than persists: it cannot be
    /// recovered off-chain, and this design does not accept contract wallets as
    /// pool owners.
    ///
    /// Best-effort and off the durability path otherwise: a persist failure never
    /// fails the stream. Lane registration is idempotent — a re-observed grant for
    /// an already-tracked lane does NOT reset the accepted-voucher watermark
    /// (#527); the lane's `cap`/`expiry` are set once at first registration.
    #[allow(clippy::cognitive_complexity)] // linear verify → register → persist sequence.
    fn intake_capability(
        &self,
        pool_id: B256,
        signer: Address,
        pool_owner: Address,
        capability: &decdn_protocol::client::WireCapability,
    ) {
        let spending_cap = capability.spending_cap;
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
        // #1789 item 2: the ecrecover is cached keyed by the full signed
        // material, so a client that re-sends the same capability on every
        // request (the documented recovery path for a lost lane) skips it. The
        // two drop reasons stay distinct in the log: a malformed signature is a
        // client-side signing bug, while a well-formed signature recovering to
        // the wrong address is operator-actionable — usually a client still
        // signing against an owner the pool has since transferred away.
        match self.recover_capability_owner(&grant) {
            CapabilityVerifyOutcome::Owner(owner) if owner == pool_owner => {}
            CapabilityVerifyOutcome::Owner(recovered) => {
                tracing::warn!(
                    %pool_id,
                    %signer,
                    error = %decdn_incentive::capability::CapabilityError::WrongOwner {
                        expected: pool_owner,
                        recovered,
                    },
                    "dropping capability: owner verification failed"
                );
                return;
            }
            CapabilityVerifyOutcome::Invalid => {
                tracing::debug!(
                    %pool_id,
                    %signer,
                    error = %decdn_incentive::capability::CapabilityError::InvalidSignature,
                    "dropping capability: owner verification failed"
                );
                return;
            }
        }

        // The grant is authentic. Register the lane so the voucher path accepts
        // vouchers for `(pool_id, signer, this operator)` — without this a
        // brand-new lane's first request is never served, since the serve gate
        // admits only known lanes. Idempotent for an already-tracked lane.
        //
        // The verified owner signature rides the lane record itself, alongside
        // the `cap`/`expiry` it authorizes: it is the `ownerSig` the redeemer
        // submits as a `CapabilityReg` on the signer's first on-chain redemption.
        // Kept ON the lane — not in a side table — so it is written in the same
        // durable transaction as the voucher frontier and can never be lost while
        // the frontier survives (#1906). A buffered in-memory insert like every
        // lane `record`; the row lands on disk in the periodic lane flush's
        // fsynced commit, not a per-request fsync on the intake path.
        let mut lane = LaneState::hydrate(
            pool_id,
            signer,
            self.eth_signer.address(),
            // `LaneState.cap` stays a `U256`; the capability's `u64` cap
            // zero-extends into it losslessly.
            U256::from(spending_cap),
            expiry,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        );
        lane.owner_sig = Some(sig_bytes);
        if let Err(e) = self.register_lane(lane) {
            tracing::warn!(%pool_id, %signer, error = %e, "lane registration failed; the request refuses as an unknown lane and the client retries");
        }
    }

    /// Register a lane so the voucher path accepts vouchers for it — its
    /// capability handle is registered on-chain and its [`LaneState`] recorded
    /// in the lane store, then inserted into the live map. Called both from the
    /// on-chain capability-registration consumer (#327) and from the
    /// seller-side capability intake on the first voucher of a new lane.
    ///
    /// **Idempotent:** a re-observed registration for an already-tracked lane is
    /// a no-op — it MUST NOT reset the accepted-voucher watermark and reopen the
    /// #527 replay window. The live map (hydrated from the store at
    /// construction, updated here) is the authority.
    ///
    /// # Errors
    ///
    /// Propagates a [`StoreError`] if the store `record` fails.
    pub fn register_lane(&self, state: LaneState) -> Result<(), StoreError> {
        let key = state.key();
        if self.lanes.contains_key(&key) {
            return Ok(());
        }
        // #1789 item 4: call the store's `record` inline. `register_lane` sits
        // ON the per-stream open path (the first capability of every stream
        // reaches it), so it cannot afford a `spawn_blocking` hop — and does
        // not need one: `PoolStateStore::record` is contractually non-blocking,
        // a buffered insert into the store's in-memory working set, with the
        // periodic lane flush doing the disk work.
        self.channel_state_store.record(&state)?;

        // Only a REAL insertion (a vacant slot) tunes the lane gauge — the
        // entry guard is atomic, so racing first-streams on one lane still
        // count it once.
        if let dashmap::mapref::entry::Entry::Vacant(entry) = self.lanes.entry(key) {
            entry.insert(Arc::new(Mutex::new(LaneDeliveryState::hydrated(state))));
            self.tune_lane_gauge(1);
        }
        Ok(())
    }

    /// Test-only: register `state` right after the next `serve_stream` resolves
    /// its lane. It stands in for a sibling stream whose capability intake lands
    /// while this request is past its lane resolution, the window in which the
    /// request must keep the answer it resolved.
    #[cfg(any(test, feature = "test-support"))]
    pub fn register_lane_after_next_resolution(&self, state: LaneState) {
        if let Ok(mut armed) = self.lane_after_resolution.lock() {
            *armed = Some(state);
        }
    }

    /// Test-only: register the lane [`Self::register_lane_after_next_resolution`]
    /// armed, if any. `serve_stream` calls it once its lane is resolved.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn fire_lane_after_resolution(&self) {
        let armed = self
            .lane_after_resolution
            .lock()
            .ok()
            .and_then(|mut armed| armed.take());
        if let Some(state) = armed
            && let Err(e) = self.register_lane(state)
        {
            tracing::warn!(error = %e, "test seam: armed lane registration failed");
        }
    }

    /// Clamp a live lane to its signer's on-chain registration (ADR 003
    /// §Capability delegation): the lane holds `min(held, registered)` for `cap`
    /// and `expiry` and records the registered expiry as `registered_until`. On-chain
    /// registration is write-once, so the clamp is final for this `(pool, signer)`
    /// and the lane store persists it with the lane. A change is recorded to the
    /// buffered store under the per-lane lock, the same path as a voucher advance.
    /// A store fault is logged and leaves the in-memory clamp in place: the live
    /// lane is what the voucher path reads.
    async fn clamp_lane_to_registration(
        &self,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        cap: u64,
        expiry: u64,
    ) {
        let mut guard = lane.lock().await;
        if !guard.state.clamp_to_registration(cap, expiry) {
            return;
        }
        tracing::debug!(
            pool_id = %guard.state.pool_id,
            signer = %guard.state.signer,
            held_cap = %guard.state.cap,
            held_expiry = guard.state.expiry,
            "lane clamped to the signer's on-chain registration"
        );
        if let Err(error) = self.channel_state_store.record(&guard.state) {
            tracing::warn!(
                pool_id = %guard.state.pool_id,
                signer = %guard.state.signer,
                %error,
                "failed to record a lane clamped to its signer's registration"
            );
        }
    }

    /// Drop a settled lane from the live map and the lane store, where the removal
    /// is a buffered tombstone that the next flush applies to disk. Idempotent —
    /// forgetting an unknown lane is a no-op.
    ///
    /// # Errors
    ///
    /// Propagates a [`StoreError`] if the store cannot take the tombstone (its
    /// buffer mutex is poisoned), or if the blocking store task fails to join —
    /// a panic inside the store, or the runtime shutting down mid-call.
    pub async fn forget_lane(&self, key: LaneKey) -> Result<(), StoreError> {
        // Removing the lane row drops its `last_voucher_at` stamp with it (issue
        // #1733): the timestamp lives on the lane's delivery state, so its
        // lifecycle follows the lane automatically — no separate activity-map
        // eviction. Only a real removal tunes the lane gauge down.
        if self.lanes.remove(&key).is_some() {
            self.tune_lane_gauge(-1);
        }
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

    /// Minimum gap between `warn!` lines a remote peer can trigger at will: a bad
    /// binding, a request with no binding, a request on an unknown lane. Each
    /// line carries the offending `peer` and the `suppressed` count.
    pub(super) const PEER_FAULT_WARN_INTERVAL: Duration = Duration::from_mins(1);

    /// Emit the observable side of a floor-admission refusal, picking the message
    /// the refusing gate actually justifies.
    ///
    /// The gates need different words and different remedies.
    /// [`FloorRefusal::PoolExhausted`] is the pool running dry, which
    /// [`Self::log_deposit_refusal`] already describes. The per-signer live cap is the
    /// opposite situation: `try_reserve_floor` tests the pool ceiling FIRST, so
    /// reaching it proves the pool can pay. Reporting it as a deposit shortfall
    /// would print a headroom that visibly exceeds the ceiling beside a sentence
    /// denying it, and would send the operator to check RPC health while the real cause
    /// is one signer's node-local live cap.
    fn log_floor_refusal(&self, refusal: FloorRefusal, at: FloorRefusalSite) {
        let FloorRefusalSite {
            pool_id,
            signer,
            hash,
            remaining,
            ceiling,
        } = at;
        let headroom = remaining.saturating_sub(self.pool_min_remaining_deposit);
        match refusal {
            FloorRefusal::PoolExhausted => {
                self.log_deposit_refusal(pool_id, hash, headroom, ceiling);
            }
            FloorRefusal::SignerAtCap { signer_cap } => {
                self.log_signer_at_cap(pool_id, signer, hash, signer_cap, ceiling, headroom);
            }
        }
    }

    /// The observable side of a per-signer LIVE-cap refusal
    /// ([`FloorRefusal::SignerAtCap`]).
    fn log_signer_at_cap(
        &self,
        pool_id: B256,
        signer: Address,
        hash: Hash,
        signer_cap: U256,
        ceiling: U256,
        headroom: U256,
    ) {
        tracing::debug!(
            %pool_id, %signer, %hash, %signer_cap, %ceiling, %headroom,
            "refusing delivery: this capability signer already holds its live concurrency cap \
             of un-vouchered floor reservation; the pool itself can still pay"
        );
        if let Some(suppressed) = self.signer_cap_refusal_warn.admit() {
            tracing::warn!(
                %pool_id, %signer, %signer_cap, %headroom, suppressed,
                interval = ?self.signer_cap_refusal_warn.interval(),
                "refusing a paying client: one capability signer is running its whole live \
                 concurrency cap of un-vouchered streams at once while the pool is solvent. This \
                 clears as those streams pay; a sustained rate means that signer runs more \
                 concurrent un-vouchered streams than its cap covers. Rotate the session key or \
                 widen pool_floor_signer_live_windows — see docs/runbook.md, which covers why \
                 widening it needs a restart"
            );
        }
    }

    /// Emit the observable side of an insufficient-deposit refusal (#1520).
    ///
    /// Unconditional `debug!` so a support ticket is answerable at all, plus a
    /// throttled `warn!` ([`WarnThrottle`]). The wire code is
    /// deliberately lossy — `InsufficientDeposit` collapses to `NotFound` with the
    /// other miss reasons so a prober cannot map channel balances — so without these the
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
        if let Some(suppressed) = self.deposit_refusal_warn.admit() {
            tracing::warn!(
                %pool_id, %headroom, %ceiling, suppressed,
                interval = ?self.deposit_refusal_warn.interval(),
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
    /// upstream pull, and `decdn_client::PULL_WINDOW_FLOOR` carries a third
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

    /// The target size of a stream's first frame: [`Self::frame_target`] at the
    /// start of an interval, capped at [`FIRST_FRAME_BYTES`]. A serve writes no
    /// byte until its first frame is full, so the cap is what the first byte
    /// waits for: a few chunk groups of store read on a hit, or of upstream pull
    /// on a miss, rather than a whole interval. Every later frame keeps the
    /// full target, so the frames still land on the interval boundary.
    pub(super) fn first_frame_target(&self, interval_bytes: u64, opening_window: u64) -> usize {
        let cap = usize::try_from(FIRST_FRAME_BYTES).unwrap_or(usize::MAX);
        self.frame_target(0, interval_bytes, opening_window)
            .min(cap)
    }

    /// The effective downstream credit window in bytes for a stream whose voucher
    /// interval is `interval_bytes` and whose ramp input is `paid` (ADR 003
    /// §Credit window): the stream's own confirmed payment plus the credit it
    /// carries from its lane ([`RampCarry::ramp_paid`](ramp::RampCarry::ramp_paid)).
    /// The window ramps from one interval toward `credit_max` as `paid` grows:
    /// `paid / credit_ramp_divisor` once that clears the one-interval floor, the
    /// floor itself below that point (including at `paid == 0`), and the full
    /// `credit_max` when `credit_ramp_divisor` is `0`. Floored at one interval so
    /// the loop always makes progress.
    pub(super) fn credit_window(&self, interval_bytes: u64, paid: u64) -> u64 {
        decdn_incentive::ramped_credit_window(
            self.credit_ramp_divisor,
            interval_bytes,
            self.credit_max,
            paid,
        )
    }

    /// The most ramp credit a lane banks: the paid bytes that open one window at
    /// `credit_max`. `0` when the ramp is disabled, because the window then starts
    /// at the ceiling.
    const fn ramp_credit_cap(&self) -> u64 {
        self.credit_max.saturating_mul(self.credit_ramp_divisor)
    }

    /// Take `lane`'s banked ramp credit for one new stream (ADR 003 §Credit
    /// window). A stream with no tracked lane carries nothing.
    async fn take_ramp_carry(
        &self,
        lane: Option<&Arc<Mutex<LaneDeliveryState>>>,
    ) -> ramp::RampCarry {
        let Some(lane) = lane else {
            return ramp::RampCarry::detached();
        };
        let pool = Arc::clone(&lane.lock().await.ramp_pool);
        ramp::RampCarry::take(pool, self.ramp_credit_cap())
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
    /// stream. The serve loop holds the returned guard for the stream's lifetime and
    /// releases the reservation once a floor is repaid; on any other drop the guard
    /// releases the live reservation, freeing the pool's floor headroom.
    // Test-only: the serve path admits through `try_reserve_floor`, which checks
    // every floor gate and reserves under one lock hold.
    #[cfg(test)]
    pub(super) fn reserve_floor(
        &self,
        pool_id: B256,
        signer: Address,
        reserved: U256,
    ) -> FloorReservation {
        FloorReservation::reserve(Arc::clone(&self.pool_floor), pool_id, signer, reserved)
    }

    /// One credit window in `µUSDC` priced at `rate_per_mb`: the ramp-start credit
    /// window (one chunk normally, the full `credit_max` when
    /// `credit_ramp_divisor == 0`), which is exactly what a fresh stream reserves. It
    /// is the unit the per-signer live cap is denominated in.
    pub(super) fn one_window(&self, rate_per_mb: u64) -> U256 {
        min_payment(self.credit_window(CHUNK_BYTES, 0), rate_per_mb)
    }

    /// The per-signer LIVE concurrency cap on un-vouchered floor reservation (ADR 003
    /// §Pool solvency, per-signer floor isolation): the most `live_reservation` any
    /// ONE capability signer may hold against this pool, `k · one_window`.
    ///
    /// **The window is the unit being rationed.** A signer's honest need for
    /// un-vouchered floor credit does not scale with the pool's size: every admission
    /// reserves at most one window, and a paying stream releases its reservation as
    /// soon as it covers it, so honest need is `concurrent un-vouchered streams × one
    /// window` whether the pool holds ten dollars or a hundred thousand. An ABSOLUTE
    /// `k`-window cap therefore fits the need directly, rather than a fraction of the
    /// deposit that would be loose in both directions on a large pool.
    ///
    /// `k` is lower-clamped to one window, so a lone signer's first stream is always
    /// admissible on any solvent pool. This carries no permanent memory: it is pure
    /// concurrency, released as each stream pays, so it never penalizes a signer for
    /// quitting.
    ///
    /// Pure and total (saturating), so it is testable without any chain access.
    pub(super) fn signer_floor_cap(&self, rate_per_mb: u64) -> U256 {
        let one_window = self.one_window(rate_per_mb);
        // Lower-clamp `k` to one window: `k = 0` (or an unset field) still admits a
        // lone signer's first stream rather than wedging it.
        let k = self.pool_floor_signer_live_windows.max(1);
        one_window.saturating_mul(U256::from(k))
    }

    /// POOL solvency: does the pool's `remaining − M` cover its already-committed LIVE
    /// floor reservation across every signer, plus `new_reserve`? Reads the floor
    /// accumulator; pure arithmetic otherwise. A poisoned
    /// accumulator lock recovers the guard rather than panicking: every critical
    /// section over this mutex is panic-free by construction — saturating `U256`
    /// arithmetic and infallible map operations only, no indexing and no `unwrap` — so
    /// a poison cannot originate from a holder of this lock, and a recovered guard
    /// cannot observe a torn pool/signer split. Do NOT add a fallible or panicking
    /// call under this lock; that argument is what the recovery rests on, because the
    /// pool total and the signer entries must agree.
    ///
    /// (The recovery is not justified by the accumulator being "best-effort": it
    /// gates whether the node fronts upstream USDC.)
    ///
    /// Deliberately the pool level ONLY, which is what makes it the right check for a
    /// stream already in flight. The per-signer gates are ADMISSION controls; an
    /// admitted stream's reservation is already counted in the pool total, so leaving
    /// them out of the mid-stream re-check bounds nothing less.
    /// [`Self::try_reserve_floor`] is where every gate applies.
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
            guard
                .get(&pool_id)
                .map_or(U256::ZERO, PoolFloorState::live_committed)
        };
        decdn_incentive::pool_budget_covers(
            remaining,
            self.pool_min_remaining_deposit,
            committed,
            new_reserve,
        )
    }

    /// Atomically check both floor gates and reserve one `floor` against the pool's
    /// budget, returning the [`FloorReservation`] guard on success or the gate that
    /// refused it.
    ///
    /// Two gates apply, both under ONE `pool_floor` lock hold so two concurrent
    /// admissions cannot both pass a check and then both reserve (the TOCTOU
    /// over-commit a separate check-then-reserve would allow):
    /// 1. **Pool solvency** — `remaining − M` must cover the pool's committed LIVE
    ///    reservation plus this floor. The hard money envelope.
    /// 2. **Per-signer live cap** — this signer's live reservation plus this floor must
    ///    stay within `k · one_window`.
    ///
    /// Only on success is the row inserted and the reservation charged, so probing a
    /// full pool with fresh signer keys cannot grow the accumulator. The guard is built
    /// from the already-charged state ([`FloorReservation::new_charged`]) so the
    /// reserved amount is charged exactly once. A poisoned lock recovers the guard
    /// rather than panicking (best-effort accounting, never a safety gate).
    pub(super) fn try_reserve_floor(
        &self,
        pool_id: B256,
        signer: Address,
        remaining: U256,
        rate_per_mb: u64,
        reserved: U256,
    ) -> Result<FloorReservation, FloorRefusal> {
        let signer_cap = self.signer_floor_cap(rate_per_mb);
        let epoch;
        {
            let mut guard = self
                .pool_floor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Read the pool's live total and this signer's live slice, without creating
            // a row: a refused admission leaves no new pool or signer row behind, so a
            // client probing a full pool with fresh signer keys cannot grow the map.
            let (pool_live, signer_live) = match guard.get(&pool_id) {
                Some(entry) => {
                    let signer_live = entry
                        .signers
                        .get(&signer)
                        .map_or(U256::ZERO, |lane| lane.live_reservation);
                    (entry.live_reservation, signer_live)
                }
                None => (U256::ZERO, U256::ZERO),
            };
            // Pool ceiling first: it is the solvency bound, and it is what an
            // operator reads as "this pool cannot pay".
            if !decdn_incentive::pool_budget_covers(
                remaining,
                self.pool_min_remaining_deposit,
                pool_live,
                reserved,
            ) {
                return Err(FloorRefusal::PoolExhausted);
            }
            // Per-signer live concurrency cap: one signer cannot hold more than `k`
            // windows of live un-vouchered reservation at once.
            if signer_live.saturating_add(reserved) > signer_cap {
                return Err(FloorRefusal::SignerAtCap { signer_cap });
            }
            let entry = guard.entry(pool_id).or_default();
            entry.live_reservation = entry.live_reservation.saturating_add(reserved);
            let lane = entry.signers.entry(signer).or_default();
            lane.live_reservation = lane.live_reservation.saturating_add(reserved);
            epoch = entry.epoch;
        }
        Ok(FloorReservation::new_charged(
            Arc::clone(&self.pool_floor),
            pool_id,
            signer,
            reserved,
            epoch,
        ))
    }

    /// Drop a reclaimed pool's floor accounting: remove its in-memory
    /// `PoolFloorState`, which carries every signer's live slice with it. Called once
    /// when a pool is reclaimed on-chain; a reclaimed `pool_id` never recurs (monotonic
    /// open nonce), so the entry is permanently moot.
    pub(crate) fn forget_pool_floor(&self, pool_id: B256) {
        let mut guard = self
            .pool_floor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.remove(&pool_id);
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
    /// refusal from the open-time gate, which is where the reason belongs. The
    /// caller returns `Stopped { Takedown }`, which the dispatch sink counts on
    /// `decdn_serve_stream_terminated_takedown_total`.
    pub(super) fn terminate_for_takedown(send: &mut SendStream, recv: &mut RecvStream, hash: Hash) {
        tracing::warn!(
            %hash,
            "terminating an in-flight delivery: a takedown landed after the stream opened"
        );
        reset_stream(send, recv, APP_ERR_NO_ERROR);
    }

    /// Apply one lane-lifecycle `delta` to the live-lane count and publish the
    /// resulting `decdn_lanes_open` gauge.
    ///
    /// The count comes from the atomic RMW's own result rather than a re-read,
    /// and [`Self::lane_gauge_publish`] orders the publish, so the gauge always
    /// reflects the most recent lifecycle event. The pool deposit behind these
    /// lanes is a pool-level on-chain quantity no lane carries, so it is
    /// published by the redeemer tick instead (#2072).
    fn tune_lane_gauge(&self, delta: isize) {
        let _publish = self
            .lane_gauge_publish
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let open = if delta >= 0 {
            self.lane_count
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1)
        } else {
            self.lane_count
                .fetch_sub(1, Ordering::Relaxed)
                .saturating_sub(1)
        };
        self.metrics.set_lanes_open(open);
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

/// Router-facing `ProtocolHandler` for `cdn/client/v1`.
///
/// Wraps the shared `Arc<ClientHandler>` so the serve loop can hand every
/// per-stream task a `'static` clone of the handler and `tokio::spawn` it
/// (#1788). The iroh [`ProtocolHandler::accept`] signature borrows `&self`, so
/// the handler itself cannot spawn tasks that outlive the borrow; owning the
/// `Arc` here and cloning it into `ClientHandler::serve` bridges that gap. The
/// wrapper is cheap to clone (one `Arc` bump) and the router holds one for the
/// process lifetime.
#[derive(Clone, Debug)]
pub struct ClientProtocol(Arc<ClientHandler>);

impl ClientProtocol {
    /// The `cdn/client/v1` ALPN this handler answers.
    pub const ALPN: &'static [u8] = ALPN_CLIENT;

    /// Wrap a shared handler for registration on the iroh `Router`.
    #[must_use]
    pub const fn new(handler: Arc<ClientHandler>) -> Self {
        Self(handler)
    }
}

impl ProtocolHandler for ClientProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        Arc::clone(&self.0)
            .serve(connection)
            .await
            .map_err(|e| AcceptError::from_boxed(e.into()))
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

/// After a clean completion (`StreamEnd` written, send half finished), read the
/// client's send half to its FIN — bounded by [`POST_END_DRAIN_TIMEOUT`] —
/// discarding whatever is left, then return so `recv` drops cleanly.
///
/// Dropping an un-finished [`RecvStream`] issues an implicit `STOP_SENDING(0)` to
/// the peer. `StreamEnd` is written only once every interval, the closing voucher
/// included, is credited, but the client finishes its send half after that. Without
/// this drain the node's drop can stop the client's send before its FIN — surfacing
/// to the client as an opaque write failure at the very end of an otherwise
/// complete, fully-paid fetch. Draining to FIN first lets both halves close cleanly.
/// The bound keeps a client that finishes delivery but never FINs from pinning the
/// serve task here.
pub(super) async fn drain_recv_to_fin(recv: &mut RecvStream) {
    // Read to FIN and discard. Every proof is already read, so only the FIN is
    // expected; the cap leaves room for a few stray frames. A client that keeps
    // sending past it makes
    // `read_to_end` error, which — like the timeout — just ends the drain and lets
    // `recv` drop. Buffering onto the heap keeps this future small (no large stack
    // scratch array to inflate the serve future — `clippy::large_futures`).
    let _ = tokio::time::timeout(POST_END_DRAIN_TIMEOUT, recv.read_to_end(64 * 1024)).await;
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
/// The budget counts every proof that leaves the chunk unsettled. That includes
/// a proof that credits nothing and a proof that pays only part of the chunk.
/// A proof pays part of a chunk when the lane headroom it finds is short — for
/// example, a sealed voucher that moves the watermark by less than the chunk. The
/// remainder stays owed, and the next proof answers it.
///
/// A whole chunk is paid by exactly one reveal, but the payer may legitimately
/// send metering vouchers ahead of it — the epoch's root voucher when this
/// stream has not carried it yet, and a rollover voucher when the chain is
/// spent. A metering voucher never credits a whole chunk, even when its
/// rollover advances the lane's watermark, so the outstanding chunk stays
/// outstanding until its reveal lands (see `voucher_credit_delta`). Three
/// messages is the real worst case for a well-behaved payer: two zero-credit
/// vouchers (re-anchor, roll) and then the reveal. The rest of the budget of 8
/// is slack for a duplicate or out-of-order reveal, which is ordinary once
/// several streams share one lane and which credits nothing when the lane holds
/// no headroom for it.
///
/// The bound matters because without it a payer could hold a stream open
/// indefinitely with a run of zero-credit vouchers, each one refreshing the
/// no-progress timeout while the delivered-but-unpaid balance never moves — the
/// same shape of stall the non-empty-`ChunkData` floor closes on the delivery
/// side. This budget bounds the number of proofs for one owed chunk, not the
/// time. [`proof_wait::ChunkProofs`] counts them, and it also holds the chunk's
/// time bound: every proof wait for the chunk ends at
/// [`proof_wait::PROOF_WAIT_CEILING`] after the chunk's first wait. So one owed
/// chunk holds the stream for at most that long, however many proofs it takes.
/// A stream that owes several chunks waits up to that long for each of them.
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
/// dropped mid-frame loses no bytes: any partial frame stays buffered for the
/// next call.
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
    /// **Cancellation-safe:** if the returned future is dropped (the proof wait
    /// faulted, or the stream is torn down), bytes already read stay in
    /// `self.buf` for the next call — no frame is torn.
    ///
    /// These two variants are the entire payer→node vocabulary after the
    /// opening `StreamRequest`, so this is the one place every proof passes
    /// through. Anything else on this stream is a protocol error.
    ///
    /// # Errors
    ///
    /// Everything the peer controls — a reset stream, a hang-up mid-frame, a
    /// malformed frame, an undecodable or unexpected message — carries
    /// [`PeerFault`](wire::PeerFault), so the dispatch sink logs it at `debug!`.
    /// The two bounds checks do not: those are this node's own invariants.
    pub(super) async fn read(&mut self, recv: &mut RecvStream) -> anyhow::Result<Proof> {
        loop {
            if let Some(result) = self.take_buffered()? {
                return result;
            }
            // Need more bytes. Use tokio's `AsyncReadExt::read` (explicitly, since
            // iroh's inherent Quinn `read` shadows it) — it is documented
            // cancel-safe: a dropped future consumes nothing, and on `Ready(n)` we
            // append to `self.buf` before the next await, so no bytes are ever lost
            // when the caller drops the read. `0` is EOF.
            let mut scratch = [0u8; 4096];
            let n = AsyncReadExt::read(recv, &mut scratch).await.map_err(|e| {
                anyhow::Error::new(wire::PeerFault)
                    .context(format!("proof stream read failed: {e}"))
            })?;
            if n == 0 {
                // The dominant abandon shape: the client finished its send side
                // and walked away between intervals.
                return Err(
                    anyhow::Error::new(wire::PeerFault).context("proof stream closed mid-frame")
                );
            }
            let chunk = scratch
                .get(..n)
                .ok_or_else(|| anyhow::anyhow!("short read length out of range"))?;
            self.buf.extend_from_slice(chunk);
        }
    }

    /// Take the next whole frame out of the buffer, if one is there.
    ///
    /// `Ok(None)` means the buffer holds less than a frame and the caller must
    /// read more. The inner `Result` is the frame's own outcome: its bytes are
    /// consumed either way, so one bad frame cannot wedge the buffer.
    ///
    /// A malformed length prefix, an undecodable body, and a message that is
    /// neither a `Voucher` nor a `ChunkPreimage` are all the peer's doing and
    /// carry [`PeerFault`](wire::PeerFault). The bounds check between them is this
    /// node's own invariant and carries no marker.
    fn take_buffered(&mut self) -> anyhow::Result<Option<anyhow::Result<Proof>>> {
        let Some((header_len, payload_len)) = decdn_protocol::framing::parse_frame(&self.buf)
            .map_err(|e| {
                anyhow::Error::new(wire::PeerFault)
                    .context(format!("proof frame parse failed: {e}"))
            })?
        else {
            return Ok(None);
        };
        let total = header_len.saturating_add(payload_len);
        let payload = self
            .buf
            .get(header_len..total)
            .ok_or_else(|| anyhow::anyhow!("proof frame bounds out of range"))?;
        let decoded = decode_message::<ClientMessage>(payload);
        let result =
            match decoded {
                Ok((ClientMessage::Voucher(v), _)) => Ok(Proof::Voucher(v)),
                Ok((ClientMessage::ChunkPreimage(p), _)) => Ok(Proof::Preimage(p)),
                Ok((_, _)) => Err(anyhow::Error::new(wire::PeerFault)
                    .context("expected ClientMessage::Voucher or ClientMessage::ChunkPreimage")),
                Err(e) => Err(anyhow::Error::new(wire::PeerFault)
                    .context(format!("proof decode failed: {e}"))),
            };
        self.buf.drain(..total);
        Ok(Some(result))
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
mod tests;
