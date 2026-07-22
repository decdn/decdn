//! `cdn/client/v1` handler — paid blob delivery (ADR 005 §`cdn/client/v1`).
//!
//! Serves the revenue path: a payer opens one bidirectional QUIC stream per
//! blob, the node answers with a signed [`StreamResponse`], then streams
//! [`ChunkData`] in `voucher_interval_mb`-sized batches, pausing at each batch
//! boundary to collect a cumulative payment `Voucher` before continuing, and
//! finishing with [`ClientMessage::StreamEnd`]. A delivery fault rides in the
//! initial response (`ok: false` + [`StreamError`]); a mid-stream voucher
//! rejection is sent as a [`ClientMessage::StreamError`] and the stream is
//! closed **cleanly** (no QUIC reset) so the client can read the reason.
//!
//! # Scope (#317 / #327)
//!
//! This handler validates vouchers only for channels present in the persisted
//! [`ChannelStateStore`]. Channels enter that set two ways: hydrated from the
//! store at construction (see [`ClientHandler::new`]), and live as the
//! on-chain `ChannelOpened` consumer in [`crate::payment_settlement`] (#327)
//! calls [`ClientHandler::register_open_channel`]. A voucher for a
//! still-unknown `channel_id` is rejected with
//! [`VoucherRejectReason::WrongChannel`] — the closest existing reason. After
//! accepting a voucher the handler emits a redeem hint (via the `redeem_hint`
//! sender wired on [`ClientHandlerDeps`]) so the settlement service can
//! withdraw the accrued claim once it crosses its threshold.
//!
//! # 0-RTT
//!
//! Unlike `cdn/probe/v1`, `cdn/client/v1` **rejects** 0-RTT (ADR 015): paid
//! accounting must not run on replayable early data, so `on_accepting` always
//! takes the full handshake.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{
    Bytes, CacheEngine, CacheError, Hash, RangePullOutcome, TeeOpen, TeeReservation, TeeSink,
};
use decdn_incentive::rate::{DEFAULT_TOLERANCE_BPS, RateError, min_payment, verify_rate};
use decdn_incentive::store::StoreError;
use decdn_incentive::{
    ChannelId, ChannelState, ChannelStateStore, CooperativeClose, StreamSlashData, VoucherActivity,
    verify_binding, voucher_reject_reason, wire_voucher_to_signed,
};
use decdn_protocol::client::{
    ChunkData, ClientMessage, CooperativeCloseAuth, CooperativeCloseRequest, StreamError,
    StreamRequest, StreamRequestExt, StreamResponse, StreamResponseBody, VoucherRejectReason,
};
use decdn_protocol::{
    ALPN_CLIENT, APP_ERR_RATE_LIMITED, FrameError, MB_BYTES, decode_message, encode_message,
    is_unknown_variant, read_frame, write_frame,
};
use iroh::PublicKey;
use iroh::endpoint::{Accepting, Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::dht::origin::OriginDirectory;
use crate::dispatch::{ConnectionLimiter, RejectReason};
use crate::leech_governor::LeechGovernor;
use crate::metrics::Metrics;
use crate::node_origin::{NodeOrigin, NodeProgressivePull, TeeVerdict};
use crate::receipt_log::{DownloadReceipt, ReceiptSink};
use crate::region_accounting::RegionAccountant;

// The paid-delivery methods are split across concern-focused submodules, each
// a bare `impl ClientHandler` block over the fields defined here. Support
// types, consts, and free functions stay in this module so every submodule
// (and the test module) can reach them via `use super::*` — Rust makes a
// module's private items visible to its descendants (#1254).
mod delivery;
mod dispatch;
mod fill;
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

/// Per-channel delivery state: the validated voucher state plus the channel-wide
/// cumulative byte counter that feeds voucher reconstruction (ADR 003 §Voucher
/// wire format — `bytes_delivered` is not on the wire).
#[derive(Debug)]
struct ChannelDeliveryState {
    state: ChannelState,
    /// Channel-wide cumulative bytes delivered as of the last accepted voucher.
    bytes_delivered_cumulative: U256,
}

/// Absolute backstop on a detached background cache-fill (#1134 review).
///
/// This is a LEAK GUARD, not a health signal, and the distinction is why it is an
/// hour rather than a minute. The warm's streaming stage is bounded by inactivity,
/// which resets on any byte received — so an upstream that trickles one byte per
/// stall-window keeps the task alive indefinitely without ever tripping the stall
/// bound. That is not merely a leaked task: `arm_background_fill` claims the hash
/// for the task's lifetime, so a warm that never ends means the node can never warm
/// that blob again for the life of the process.
///
/// Sized ~21.5× the derived foreground deadline ([`crate::selection::outer_pull_deadline`] —
/// `(5 + 20 + 20) × 3 + 32.5 s = 167.5 s` at defaults), so it bounds no honest transfer the
/// node's `max_blob_size_mb` ceiling permits — a 1 GiB blob would have to average under
/// 300 KB/s to hit it — while still guaranteeing every warm terminates. Not config-tunable
/// (YAGNI): an operator who needs to tune this wants `node_pull_stall_timeout_sec`, which is
/// the actual health knob.
///
/// The ratio is stated against the deadline rather than restating its arithmetic, because
/// restating it is how this comment went stale: it said `+ 10 s` and `~25×` after the slack
/// became derived (500 ms probe + 4 × 8 s lookup = 32.5 s, not 10 s), and 3600/167.5 is
/// ~21.5, not 25.
pub const BACKGROUND_FILL_HARD_CAP: Duration = Duration::from_hours(1);

/// Ceiling on the memory background warms may hold at once, in MiB (#1145 review).
///
/// The per-hash `inflight` claim dedups warms for the SAME blob; it says nothing about
/// how many DISTINCT blobs can be warming. Each warm runs the buffered path, which
/// accumulates the whole blob into memory and pays vouchers for every byte — and
/// [`BACKGROUND_FILL_HARD_CAP`] extends a warm's life from the old ~70 s to an hour, so
/// slow upstreams now accumulate roughly **50×** more concurrent warms for the same miss
/// rate (3600/70 ≈ 51). Unbounded, a burst of misses against a slow peer is a memory and
/// spend amplifier.
///
/// (This said `~25×` — the same figure as `BACKGROUND_FILL_HARD_CAP`'s ratio against the
/// FOREGROUND deadline, which is a different denominator entirely. The two cannot both be
/// right, and the copy understated this one by half.)
///
/// # Why bytes, not tasks
///
/// A task COUNT is the wrong quantity to bound: what a warm costs is its blob, and a blob
/// is bounded only by `max_blob_size_mb` (1 GiB by default). Eight concurrent warms of
/// near-max blobs is ~8 GiB resident for up to an hour each — while eight concurrent warms
/// of 4 KiB blobs is nothing at all, and a count-based ceiling throttles those just as hard.
///
/// So the permits are MiB, and each warm reserves `max_blob_size_mb` of them up front —
/// the blob's size is not known until it has been fetched, so the ceiling is what has to
/// be reserved. Small-blob nodes now run many warms at once; large-blob nodes run few.
///
/// **At the defaults this means TWO concurrent warms** (a 1 GiB `max_blob_size_mb` against
/// this 2 GiB pool), down from the old fixed 8 — but bounded at 2 GiB resident instead of
/// 8 GiB. An operator who wants more concurrency lowers `max_blob_size_mb`; one who raises
/// it is explicitly trading warm concurrency for blob size, which is the honest trade the
/// task-count ceiling hid.
///
/// A miss that finds no room is simply not warmed: the hash stays unclaimed, so the next
/// miss retries it. Shedding is the right failure mode — a warm is speculative work for
/// a FUTURE request, and dropping it costs a later cache miss, never a live one.
pub const MAX_BACKGROUND_FILL_MB: u64 = 2048;

/// Detached background cache-fill state (#859), built by [`BackgroundFill::new`]
/// and supplied to the handler at construction via [`ClientHandlerDeps`]. When
/// the foreground delivery deadline fires on a node-to-node miss, the handler
/// spawns a task to keep warming the cache from a slow-but-available upstream for
/// future requests.
pub(crate) struct BackgroundFill {
    /// Cancelled on node shutdown so in-flight warm tasks stop cooperatively at
    /// their next await rather than being left to run past drain.
    cancel: CancellationToken,
    /// Optional overall wall-clock cap on a background fill. The runtime passes
    /// [`BACKGROUND_FILL_HARD_CAP`]; `None` (tests) runs to completion.
    ///
    /// It used to reuse the derived outer pull-through deadline, and that made the
    /// background fill useless for exactly the content it matters most for: the warm
    /// re-pulls from scratch, so capping it at the same deadline the *foreground* gave
    /// up on meant a blob that could not be pulled in one deadline could not be
    /// warmed in one either. The node simply could not acquire any blob needing more
    /// than a deadline's worth of transfer (#1134).
    ///
    /// The fix is a *bigger* cap, not the absence of one. It is tempting to argue
    /// the cap is unnecessary because the pull's streaming stage is inactivity-bounded
    /// — but inactivity is not liveness: the deadline resets on ANY byte, so a peer
    /// trickling one byte per stall-window keeps a warm running forever, and the
    /// per-hash `inflight` claim then blocks that blob from ever being warmed again.
    /// See [`BACKGROUND_FILL_HARD_CAP`].
    budget: Option<Duration>,
    /// Hashes with a background fill currently running, so repeated foreground
    /// misses on the same hash don't spawn duplicate warming tasks. A std mutex
    /// (no await held); a poisoned lock is recovered rather than disabling the
    /// feature — the dedup set holds no torn state to fear (only `insert`/`remove`
    /// ever take it).
    inflight: Arc<std::sync::Mutex<HashSet<Hash>>>,
    /// Memory ceiling across DISTINCT hashes, denominated in MiB
    /// ([`MAX_BACKGROUND_FILL_MB`]). `inflight` dedups warms for one blob; this bounds
    /// how much those warms can hold at once. Each warm reserves `reserve_mb` permits.
    slots: Arc<tokio::sync::Semaphore>,
    /// MiB each warm reserves from `slots` — the node's `max_blob_size_mb`, since a
    /// blob's true size is unknown until it has been fetched, so the ceiling is what
    /// must be reserved. Clamped to the pool size, so a node whose `max_blob_size_mb`
    /// exceeds [`MAX_BACKGROUND_FILL_MB`] still runs ONE warm at a time rather than
    /// shedding every warm forever.
    reserve_mb: u32,
}

impl BackgroundFill {
    /// Build the background-fill state from the runtime knobs, sizing the memory
    /// pool via [`warm_budget_mb`]. Called by the runtime wiring (and the
    /// warm-budget tests) to populate [`ClientHandlerDeps::background_fill`].
    ///
    /// `budget` is an OPTIONAL overall wall-clock cap: the runtime passes
    /// `Some(`[`BACKGROUND_FILL_HARD_CAP`]`)`; `None` (tests only) runs to
    /// completion. It must be far LARGER than the foreground deadline, not equal
    /// to it — the warm re-pulls from scratch, so capping it at the budget the
    /// foreground just exhausted means a blob too large to fetch in one deadline
    /// can never be warmed either (#1134) — but it cannot be absent, since
    /// inactivity is not liveness (see [`BACKGROUND_FILL_HARD_CAP`]).
    ///
    /// `max_blob_size_mb` is the node's blob ceiling — what each warm reserves
    /// from the [`MAX_BACKGROUND_FILL_MB`] memory pool, since a blob's size is not
    /// known until it has been fetched. It is clamped to the pool, so an operator
    /// who raises `max_blob_size_mb` above the pool gets one warm at a time rather
    /// than none.
    pub(crate) fn new(
        cancel: CancellationToken,
        budget: Option<Duration>,
        max_blob_size_mb: u64,
    ) -> Self {
        let (pool_mb, reserve_mb) = warm_budget_mb(max_blob_size_mb);
        Self {
            cancel,
            budget,
            inflight: Arc::new(std::sync::Mutex::new(HashSet::new())),
            slots: Arc::new(tokio::sync::Semaphore::new(pool_mb as usize)),
            reserve_mb,
        }
    }
}

/// RAII guard that clears a hash from [`BackgroundFill::inflight`] when its warm
/// task ends (success, failure, or shutdown cancel), re-arming future misses.
struct BgInflightGuard {
    hash: Hash,
    inflight: Arc<std::sync::Mutex<HashSet<Hash>>>,
}

impl Drop for BgInflightGuard {
    fn drop(&mut self) {
        // Recover a poisoned lock so the claim is ALWAYS released — otherwise a
        // panic elsewhere would strand this hash in the set, permanently
        // disabling its background fill.
        let mut set = self
            .inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set.remove(&self.hash);
    }
}

/// Whether the reactive pull-through authorized-origin gate (#821) refuses to
/// initiate a pull for `hash`. Returns `true` (refuse) only when the operator
/// opted in — a directory is wired on [`ClientHandlerDeps`] — AND that directory
/// holds no
/// authorized origin for the hash's namespace. An unset gate (the default)
/// always returns `false`, preserving the permissionless cache role. Free
/// function so the branch is unit-testable without a full handler / QUIC stream
/// (the wire `NotFound` it produces is indistinguishable from a plain miss, so
/// an end-to-end test cannot observe it).
fn pull_origin_gate_blocks(
    gate: Option<&Arc<dyn OriginDirectory>>,
    hash: &crate::dht::origin::Hash,
) -> bool {
    gate.is_some_and(|dir| !dir.has_origin(hash))
}

/// Claim `hash` for a background fill (#859), returning a [`BgInflightGuard`]
/// the caller must hold for the lifetime of the spawned warm task — dropping it
/// releases the claim. Returns `None` only if a fill is already in flight for
/// `hash` (a poisoned lock is recovered, not treated as a claim failure). Fusing
/// the claim with its releaser makes "armed" and "holds a guard" the same fact: a
/// caller cannot arm without receiving the releaser (an unreleasable leak), nor
/// release without arming.
fn arm_background_fill(
    inflight: &Arc<std::sync::Mutex<HashSet<Hash>>>,
    hash: Hash,
) -> Option<BgInflightGuard> {
    let claimed = inflight
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(hash);
    claimed.then(|| BgInflightGuard {
        hash,
        inflight: Arc::clone(inflight),
    })
}

/// Size the background-warm memory budget: `(pool_mb, reserve_mb)`, both in MiB.
///
/// Split out from [`BackgroundFill::new`] so the SIZING DECISION — the
/// only part with any judgement in it — is a pure function that can be asserted on
/// directly. A test that stands up its own semaphore and reserves from it proves only that
/// tokio's semaphore works; this is the thing that can actually be got wrong.
///
/// Each warm reserves the node's whole blob ceiling, because a blob's true size is not
/// known until it has been fetched. So the reserve is a PROXY for the worst-case blob, and
/// the only question this function answers is what that proxy should be:
///
/// - **`0` is the "unlimited" sentinel**, not "a zero-byte ceiling". Every blob-size gate in
///   the node reads it that way (`max_blob_size_bytes > 0 && …`, five sites), and config
///   resolution accepts it — it only enforces `max_blob_size_mb < cache_size_mb`. So at `0`
///   the worst-case blob is UNBOUNDED, and the honest proxy for an unbounded blob is the
///   whole pool: one warm at a time (#1145 review).
///
///   This used to clamp to a 1 MiB FLOOR instead, on the reasoning that "a zero ceiling must
///   not make warms free and unbounded" — which inverted the guard at the one value it
///   named. A 1 MiB reserve against a 2 GiB pool admits 2048 concurrent warms, each
///   buffering an arbitrarily large blob into memory for up to `BACKGROUND_FILL_HARD_CAP`.
///   The byte-based pool was then strictly WORSE than the fixed task-count ceiling it
///   replaced.
/// - a ceiling of [`MAX_BACKGROUND_FILL_MB`], so an operator who sets a blob ceiling larger
///   than the whole pool gets ONE warm at a time rather than none. Without it the
///   reservation could never be granted — a semaphore cannot hand out more permits than it
///   holds — and every warm would shed forever, silently disabling the feature.
///
/// The two land in the same place, which is the tell that it is the right answer: an
/// unlimited ceiling IS a ceiling larger than the pool.
fn warm_budget_mb(max_blob_size_mb: u64) -> (u32, u32) {
    let pool = u32::try_from(MAX_BACKGROUND_FILL_MB).unwrap_or(u32::MAX);
    let reserve = if max_blob_size_mb == 0 {
        MAX_BACKGROUND_FILL_MB
    } else {
        max_blob_size_mb.min(MAX_BACKGROUND_FILL_MB)
    };
    (pool, u32::try_from(reserve).unwrap_or(u32::MAX))
}

/// Is this populate error a routine MISS rather than a fault (#1145 review)?
///
/// The distinction the warm counters used to lack. A warm that finds no provider, or has no
/// origin configured, has done nothing wrong and there is nothing to fix. A warm that failed
/// because the store is corrupt, the disk is full, or an upstream served bytes that do not
/// verify is an operator's problem. Folding the two into one counter — as
/// `node_pull_through_background_failed` did — meant no threshold on it could distinguish
/// them, so it could never fire *for* the emergency it would need to signal.
///
/// Exhaustive on purpose: a new `CacheError` must break this build and be classified, rather
/// than silently inheriting "fault" (noisy) or "miss" (a swallowed emergency).
const fn is_clean_miss(err: &CacheError) -> bool {
    match err {
        // Nothing to warm. The blob is not out there, or we have nowhere to look.
        CacheError::NotFound { .. } | CacheError::NoOrigin { .. } => true,
        // Every one of these is a fault someone must act on: a corrupt or full store, an
        // upstream serving bytes that do not verify, a blob over our ceiling, a broken
        // origin, or an eviction sweep that could not free space.
        CacheError::Store(_)
        | CacheError::HashMismatch { .. }
        | CacheError::VerifyFailed { .. }
        | CacheError::BlobTooLarge { .. }
        | CacheError::OriginError { .. }
        | CacheError::EvictionLimitExceeded { .. } => false,
    }
}

/// How a background warm ended. Every variant is a TERMINAL outcome, so exactly one is
/// recorded per spawn and the books balance:
/// `spawned == succeeded + missed + failed + cancelled + panicked`.
#[derive(Clone, Copy, Debug)]
enum WarmVerdict {
    Succeeded,
    /// Routine: nothing to warm (see [`is_clean_miss`]).
    Missed,
    /// A fault an operator must act on.
    Failed,
    Cancelled,
}

/// Meters a background warm's outcome — including the one outcome the task itself cannot
/// report (#1145 review).
///
/// The warm is `tokio::spawn`ed and its `JoinHandle` dropped on the spot, so nothing awaits
/// it. A panic inside `cache.populate`, the tee, or the decoder therefore reaches *nobody*:
/// no counter moves, nothing is logged, and `spawned` sits permanently one above the sum of
/// its outcomes — a gap that reads like an in-flight warm rather than a crash.
///
/// `Drop` runs on the unwind, which makes it the only thing that can still see it. The same
/// hole was already fixed for the detached channel-open task, which got a supervisor for
/// exactly this reason; the warm task has no caller at all, so it is strictly worse off, and
/// it got nothing.
struct WarmOutcome<'a> {
    hash: Hash,
    metrics: &'a Metrics,
    recorded: bool,
}

impl<'a> WarmOutcome<'a> {
    const fn new(hash: Hash, metrics: &'a Metrics) -> Self {
        Self {
            hash,
            metrics,
            recorded: false,
        }
    }

    fn record(&mut self, verdict: WarmVerdict) {
        self.recorded = true;
        match verdict {
            WarmVerdict::Succeeded => self.metrics.node_pull_through_background_succeeded(),
            WarmVerdict::Missed => self.metrics.node_pull_through_background_missed(),
            WarmVerdict::Failed => self.metrics.node_pull_through_background_failed(),
            WarmVerdict::Cancelled => self.metrics.node_pull_through_background_cancelled(),
        }
    }
}

impl Drop for WarmOutcome<'_> {
    fn drop(&mut self) {
        if self.recorded {
            return;
        }
        // A verdict was never recorded — two very different reasons, told apart by
        // `thread::panicking()`: a real panic unwinds THROUGH this Drop (so it is true), whereas
        // a task dropped UNPOLLED at runtime teardown does not (#1145 review). Conflating them
        // fired a false `error!("PANICKED")` on every clean shutdown against a counter whose
        // doc says any non-zero value is a bug.
        if std::thread::panicking() {
            // Nothing awaits the warm's `JoinHandle`, so this counter is the panic's only trace.
            self.metrics.node_pull_through_background_panicked();
            tracing::error!(
                hash = %self.hash,
                "background cache-fill PANICKED; nothing awaits this task, so this counter is \
                 the only trace it leaves"
            );
        } else {
            // Cancelled / dropped unpolled at shutdown — expected, not a crash.
            self.metrics.node_pull_through_background_cancelled();
        }
    }
}

/// Run `fut` under an optional wall-clock deadline, keeping the `Result<_,
/// Elapsed>` shape of [`tokio::time::timeout`] so callers branch identically
/// whether or not a cap is set. `None` never elapses.
async fn with_optional_deadline<F: std::future::Future>(
    deadline: Option<Duration>,
    fut: F,
) -> Result<F::Output, tokio::time::error::Elapsed> {
    match deadline {
        Some(d) => tokio::time::timeout(d, fut).await,
        None => Ok(fut.await),
    }
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
    UnauthorizedOrigin,
    CooperativeCloseSigned,
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
    /// (#1144). So the `NotFound` collapse is what a requester sees for all seven
    /// reasons below, and the reputation consequences it draws must hold for the
    /// weakest of them. They do: it scores `NotFound` as no fault at all, and only
    /// `InternalError` as a degraded peer.
    const fn wire_error(self) -> StreamError {
        match self {
            // `InsufficientDeposit` collapses to `NotFound` alongside the other
            // miss reasons (#856): it must be wire-indistinguishable so a probing
            // client cannot map out other clients' channel balances; the
            // distinction survives only in the per-reason metric.
            // `CooperativeCloseSigned` collapses to `NotFound` with the other
            // miss reasons: a channel being cooperatively settled is no longer
            // serving, and the refusal stays wire-indistinguishable from an
            // unknown channel (no leak that a waiver was signed).
            // `RangeNotSatisfiable` collapses to `NotFound` alongside the other
            // "won't serve this" reasons: an out-of-bounds bounded range is a
            // client error, but signalling it as `NotFound` (rather than
            // `InternalError`) keeps it reputation-benign — a requester scores
            // `InternalError` as a degraded peer (#1144), and a client's own
            // malformed range must not penalise the node for it. The distinction
            // survives in the per-reason metric.
            Self::CacheMiss
            | Self::UnknownChannel
            | Self::OwnerMismatch
            | Self::InsufficientDeposit
            | Self::UnauthorizedOrigin
            | Self::CooperativeCloseSigned
            | Self::RangeNotSatisfiable => StreamError::NotFound,
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
/// the handler used to throw it away, collapsing both to "not filled" and
/// refusing with `CacheMiss` — wire [`StreamError::NotFound`] — even when the
/// real cause was the operator's own S3/fs origin being down.
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
    /// A TRANSIENT backend/store fault — the node is degraded, not empty.
    /// Terminal: `InternalError` ("do not retry this node"), so a client routes
    /// around a node whose origin is down and the operator's reject metric names
    /// the real cause.
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
    pub cache: CacheEngine,
    pub eth_signer: Arc<PrivateKeySigner>,
    pub slash_domain: Eip712Domain,
    pub voucher_domain: Eip712Domain,
    pub bind_domain: Eip712Domain,
    pub channel_state_store: Arc<dyn ChannelStateStore>,
    pub receipt_sink: Arc<dyn ReceiptSink>,
    pub rate_per_mb: Arc<AtomicU64>,
    /// Live per-MB delivery-rate bounds (#1172). Seeded from on-chain
    /// `getRateBounds()` and updated by the `RateBoundsUpdated` watcher,
    /// replacing the by-value config stand-in.
    pub rate_bounds: crate::rate_bounds::RateBounds,
    pub voucher_interval_mb: u64,
    pub max_blob_size_bytes: u64,
    pub max_concurrent_streams: usize,
    /// Live content deny-set (ADR 011): the operator's local denylist unioned
    /// with the on-chain origin blacklist. NOT an `Option`, unlike the wiring
    /// hooks below — an empty deny-set is a correct steady state (most operators
    /// deny nothing), so there is no "unwired" case to represent, and an
    /// `Option` would only add a way to fail open on a takedown gate.
    /// [`ClientHandlerDeps::new`] seeds it empty; the runtime overwrites it with
    /// the resolved one.
    pub content_deny: Arc<crate::content_deny::ContentDenylist>,
    // Optional wiring — `None` unless the deployment enables the feature.
    pub redeem_hint: Option<mpsc::Sender<ChannelId>>,
    pub voucher_activity: Option<Arc<VoucherActivity>>,
    pub region_accountant: Option<Arc<RegionAccountant>>,
    pub pull_through: Option<Duration>,
    pub local_populate: Option<Duration>,
    /// `pub(crate)` — the inner [`BackgroundFill`] is a crate-internal type, so
    /// only in-crate wiring/tests set it (via [`BackgroundFill::new`]); external
    /// callers leave it `None`.
    pub(crate) background_fill: Option<BackgroundFill>,
    pub pull_through_origin: Option<Arc<NodeOrigin>>,
    pub pull_ahead_bytes: Option<Bytes>,
    pub leech_governor: Option<Arc<LeechGovernor>>,
    pub pull_origin_gate: Option<Arc<dyn OriginDirectory>>,
    pub prefetch_engine: Option<Arc<crate::prefetch::PrefetchEngine>>,
    pub idle_timeout: Option<Duration>,
}

impl std::fmt::Debug for ClientHandlerDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientHandlerDeps")
            .field("node_id", &self.node_id)
            .field("voucher_interval_mb", &self.voucher_interval_mb)
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
        channel_state_store: Arc<dyn ChannelStateStore>,
        receipt_sink: Arc<dyn ReceiptSink>,
        rate_per_mb: Arc<AtomicU64>,
        rate_bounds: crate::rate_bounds::RateBounds,
        voucher_interval_mb: u64,
        max_blob_size_bytes: u64,
        max_concurrent_streams: usize,
    ) -> Self {
        Self {
            node_id,
            metrics,
            limiter,
            cache,
            eth_signer,
            slash_domain,
            voucher_domain,
            bind_domain,
            channel_state_store,
            receipt_sink,
            rate_per_mb,
            rate_bounds,
            voucher_interval_mb,
            max_blob_size_bytes,
            max_concurrent_streams,
            content_deny: Arc::new(crate::content_deny::ContentDenylist::empty()),
            redeem_hint: None,
            voucher_activity: None,
            region_accountant: None,
            pull_through: None,
            local_populate: None,
            background_fill: None,
            pull_through_origin: None,
            pull_ahead_bytes: None,
            leech_governor: None,
            pull_origin_gate: None,
            prefetch_engine: None,
            idle_timeout: None,
        }
    }

    /// Wire the window-paced pull-through provider and its companion window size
    /// together (#856). The two are only meaningful as a pair — the serve path
    /// gates the fused pull-and-forward on `pull_through_origin` being `Some` and
    /// reads `pull_ahead_bytes` as the pipeline window — so setting them through
    /// one call keeps a caller from half-wiring the window path.
    pub fn set_window_pull_through(&mut self, origin: Arc<NodeOrigin>, pull_ahead_bytes: Bytes) {
        self.pull_through_origin = Some(origin);
        self.pull_ahead_bytes = Some(pull_ahead_bytes);
    }
}

/// `cdn/client/v1` paid-delivery handler.
pub struct ClientHandler {
    node_id: PublicKey,
    metrics: Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    eth_signer: Arc<PrivateKeySigner>,
    /// `SlashJudge` EIP-712 domain for `StreamResponse.slash_sig`.
    slash_domain: Eip712Domain,
    /// `PaymentChannel` EIP-712 domain for voucher verification.
    voucher_domain: Eip712Domain,
    /// `CapacityBond` EIP-712 domain for ephemeral `BindNodeId` verification.
    bind_domain: Eip712Domain,
    channel_state_store: Arc<dyn ChannelStateStore>,
    /// Non-blocking sink for the served-and-paid audit log (issues #248, #803).
    /// The voucher-accept path enqueues one receipt here *before* `VoucherAck`;
    /// the actual disk write happens off the hot path in the background receipt
    /// writer, so receipt-log I/O can never back-pressure paid delivery. A
    /// dropped receipt (queue full) is non-fatal — the payment already committed
    /// to the fsynced channel store.
    receipt_sink: Arc<dyn ReceiptSink>,
    /// Per-channel state, hydrated from the store at construction. Outer mutex
    /// guards the map; each inner mutex serializes voucher application for one
    /// channel across its concurrent streams (ADR 003 §concurrent streams).
    channels: Arc<Mutex<HashMap<ChannelId, Arc<Mutex<ChannelDeliveryState>>>>>,
    /// Serializes absolute channel snapshots without holding the channel map
    /// while individual channel state (which may be fsync-bound) is locked.
    channel_metrics_refresh: Mutex<()>,
    /// Redeem-hint sender to the on-chain settlement service (#327), set at
    /// construction via [`ClientHandlerDeps`]. `None` when no settlement service
    /// is wired (e.g. tests) — a hint is best-effort, so an absent sender or a
    /// full channel just skips it.
    redeem_hint: Option<mpsc::Sender<ChannelId>>,
    /// In-memory last-voucher clock shared with `admin_v1_channels`
    /// (issue #749), set at construction via [`ClientHandlerDeps`]. `None` when
    /// no admin surface is wired (e.g. tests) — stamping is best-effort, so the
    /// handler just skips it and the channel reports "no activity since restart"
    /// to the operator.
    voucher_activity: Option<Arc<VoucherActivity>>,
    /// Per-region bandwidth accountant (issue #750), set at construction via
    /// [`ClientHandlerDeps`]. `None` when no admin surface is wired (tests) —
    /// recording is best-effort, so the handler simply skips it.
    region_accountant: Option<Arc<RegionAccountant>>,
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
    /// Background cache-fill state (#859), set at construction via
    /// [`ClientHandlerDeps`]. `None` (the default — feature off, and in tests)
    /// means a foreground pull-through deadline simply returns `NotFound` with no
    /// warming. When `Some`, the deadline additionally spawns a detached task to
    /// keep filling the cache from a slow upstream.
    background_fill: Option<BackgroundFill>,
    /// Window-paced node→node pull-through provider (#856), set at construction
    /// via [`ClientHandlerDeps`]. When `Some` (alongside `pull_through`), a cache
    /// miss for an offset-0 request that proves channel ownership is served by
    /// fusing a progressive upstream pull with downstream delivery — forwarding
    /// each chunk to the paying client and teeing it into the cache — so
    /// per-request speculative exposure is bounded to `pull_ahead_bytes` instead
    /// of the whole blob. `None` keeps the buffered `populate` path
    /// (`pull_through`) or a plain `NotFound`.
    pull_through_origin: Option<Arc<NodeOrigin>>,
    /// Per-request pipeline window in bytes (#856, ADR 037 `pull_ahead_bytes`),
    /// set at construction via [`ClientHandlerDeps`] alongside
    /// `pull_through_origin`. The window-paced loop pulls at most this many bytes
    /// ahead of cleared downstream payment.
    pull_ahead_bytes: Option<Bytes>,
    /// Node-wide seed-leech caps (#856, ADR 037), set at construction via
    /// [`ClientHandlerDeps`]. Consulted before/while a speculative pull-through
    /// proceeds and credited from the voucher path. `None` (tests / feature off)
    /// leaves only the per-request window.
    leech_governor: Option<Arc<LeechGovernor>>,
    /// Optional content-authorization gate on the reactive pull-through path
    /// (#821, ADR 037 §Seed-leech caps / ADR 022 §Scope and limits), set at
    /// construction via [`ClientHandlerDeps`] only when the operator sets
    /// `cache.pull_through_require_authorized_origin = true`. `None` (the default
    /// and in tests) keeps the permissionless cache role: misses pull through
    /// unconditionally. When `Some`, a cache miss whose hash has no authorized
    /// origin in this directory (`has_origin == false`) is refused with
    /// `NotFound` before any upstream pull or cache-warming write — a
    /// pull-*initiation* gate only, never consulted for a range already held.
    /// Shares the same `OriginDirectory` the prefetch gate uses, so the namespace
    /// / default-open (`namespaceId == 0`) / fail-closed-on-RPC-loss semantics
    /// are identical.
    pull_origin_gate: Option<Arc<dyn OriginDirectory>>,
    /// Live content deny-set (ADR 011). Consulted at three points, all of which
    /// must gate or the check is bypassable: the hash gate above the
    /// availability check in `serve_stream`, the origin gate right after channel
    /// resolution, and the same origin gate inside `pull_authorized` — that last
    /// one runs EARLIEST and decides whether to front upstream USDC egress, so
    /// omitting it would have this node pay on a blacklisted origin's behalf
    /// before ever reaching the serve refusal. The window-paced serve path
    /// (`window.rs`) is a fourth, independent ladder.
    pub(crate) content_deny: Arc<crate::content_deny::ContentDenylist>,
    /// Speculative-prefetch engine (#820), set at construction via
    /// [`ClientHandlerDeps`]. `None` in tests / when prefetch is off. When
    /// `Some`, the serve path credits bytes served from prefetch-acquired blobs
    /// to the demand-quality numerator.
    prefetch_engine: Option<Arc<crate::prefetch::PrefetchEngine>>,
    rate_per_mb: Arc<AtomicU64>,
    rate_bounds: crate::rate_bounds::RateBounds,
    voucher_interval_mb: u64,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
    /// Application-layer idle-close ceiling (ADR 005 §Connection lifetime).
    /// `None` (the default and production path) reads as [`APP_IDLE_TIMEOUT`]
    /// (30s); a shorter value is set at construction via [`ClientHandlerDeps`]
    /// only by tests, so an idle-close case need not wait a real 30s.
    idle_timeout: Option<Duration>,
}

impl std::fmt::Debug for ClientHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientHandler")
            .field("node_id", &self.node_id)
            .field("voucher_interval_mb", &self.voucher_interval_mb)
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
        let mut map = HashMap::new();
        let mut channel_deposit = U256::ZERO;
        for state in deps.channel_state_store.load_all()? {
            channel_deposit = channel_deposit.saturating_add(state.deposit);
            let bytes = state.last_bytes_delivered();
            map.insert(
                state.channel_id,
                Arc::new(Mutex::new(ChannelDeliveryState {
                    state,
                    bytes_delivered_cumulative: bytes,
                })),
            );
        }
        deps.metrics
            .set_inbound_channel_snapshot(map.len(), channel_deposit);
        Ok(Self {
            node_id: deps.node_id,
            metrics: deps.metrics,
            limiter: deps.limiter,
            cache: deps.cache,
            eth_signer: deps.eth_signer,
            slash_domain: deps.slash_domain,
            voucher_domain: deps.voucher_domain,
            bind_domain: deps.bind_domain,
            channel_state_store: deps.channel_state_store,
            receipt_sink: deps.receipt_sink,
            channels: Arc::new(Mutex::new(map)),
            channel_metrics_refresh: Mutex::new(()),
            redeem_hint: deps.redeem_hint,
            voucher_activity: deps.voucher_activity,
            region_accountant: deps.region_accountant,
            pull_through: deps.pull_through,
            local_populate: deps.local_populate,
            background_fill: deps.background_fill,
            pull_through_origin: deps.pull_through_origin,
            pull_ahead_bytes: deps.pull_ahead_bytes,
            leech_governor: deps.leech_governor,
            pull_origin_gate: deps.pull_origin_gate,
            content_deny: deps.content_deny,
            prefetch_engine: deps.prefetch_engine,
            rate_per_mb: deps.rate_per_mb,
            rate_bounds: deps.rate_bounds,
            voucher_interval_mb: deps.voucher_interval_mb,
            max_blob_size_bytes: deps.max_blob_size_bytes,
            max_concurrent_streams: deps.max_concurrent_streams,
            idle_timeout: deps.idle_timeout,
        })
    }

    /// Register a channel observed on-chain via `ChannelOpened` (#327) so the
    /// voucher path accepts vouchers for it. Persists a fresh [`ChannelState`]
    /// durably, then inserts it into the live map.
    ///
    /// **Idempotent:** a re-observed `ChannelOpened` (e.g. from a poll-tick
    /// window re-scan — reorg rewind or mid-backfill retry) for an
    /// already-tracked channel is a no-op — it MUST
    /// NOT reset the accepted-voucher watermark and reopen the #527 replay
    /// window. The live map (hydrated from the store at construction, updated
    /// here) is the authority.
    ///
    /// # Errors
    ///
    /// Propagates a [`StoreError`] if the durable persist fails; the watcher
    /// logs and retries on the next `ChannelOpened` observation.
    pub async fn register_open_channel(&self, state: ChannelState) -> Result<(), StoreError> {
        if self.channels.lock().await.contains_key(&state.channel_id) {
            return Ok(());
        }
        // The store write is a synchronous fsync (store trait §Durability) —
        // run it off the runtime worker, same as the voucher-accept path.
        let store = Arc::clone(&self.channel_state_store);
        let to_persist = state.clone();
        tokio::task::spawn_blocking(move || store.record(&to_persist))
            .await
            .map_err(|e| StoreError::Backend(format!("register_open_channel join: {e}")))??;

        let bytes = state.last_bytes_delivered();
        self.channels
            .lock()
            .await
            .entry(state.channel_id)
            .or_insert_with(|| {
                Arc::new(Mutex::new(ChannelDeliveryState {
                    state,
                    bytes_delivered_cumulative: bytes,
                }))
            });
        self.refresh_channel_metrics().await;
        Ok(())
    }

    /// Drop a settled channel (observed via `ChannelSettled`, #327) from the
    /// live map and the persisted store. Idempotent — forgetting an unknown
    /// channel is a no-op.
    ///
    /// # Errors
    ///
    /// Propagates a [`StoreError`] if the durable delete fails.
    pub async fn forget_channel(&self, channel_id: ChannelId) -> Result<(), StoreError> {
        self.channels.lock().await.remove(&channel_id);
        self.refresh_channel_metrics().await;
        // Drop the in-memory last-voucher stamp too (issue #749 review):
        // `touch` inserts per-channel with no eviction, so without this a
        // settled channel's `Instant` would linger for the whole process
        // lifetime — a slow leak on a high-churn node. Best-effort, mirroring
        // the live-map removal: an unset clock just skips.
        if let Some(activity) = self.voucher_activity.as_ref() {
            activity.forget(channel_id);
        }
        let store = Arc::clone(&self.channel_state_store);
        tokio::task::spawn_blocking(move || store.forget(channel_id))
            .await
            .map_err(|e| StoreError::Backend(format!("forget_channel join: {e}")))?
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

    /// Raise a tracked channel's on-chain deposit after a `ChannelToppedUp`
    /// event (#327). Without this, [`decdn_incentive::ChannelState::apply_voucher`]
    /// keeps enforcing the original (lower) deposit and rejects the
    /// otherwise-valid vouchers a client signs *after* topping up. No-op for
    /// channels this node does not track; idempotent for a non-increasing
    /// `new_deposit` (deposits only ever grow).
    ///
    /// # Errors
    ///
    /// Propagates a [`StoreError`] if the durable persist fails.
    pub async fn update_channel_deposit(
        &self,
        channel_id: ChannelId,
        new_deposit: U256,
    ) -> Result<(), StoreError> {
        let entry = self.channels.lock().await.get(&channel_id).cloned();
        let Some(entry) = entry else { return Ok(()) };
        let mut guard = entry.lock().await;
        if guard.state.deposit >= new_deposit {
            return Ok(());
        }
        // Persist the raised deposit before advancing in-memory, mirroring the
        // voucher-accept commit discipline (#527): record a clone durably and
        // swap it in only on success. The per-channel guard is held across the
        // blocking write so a concurrent voucher on this channel serializes.
        let mut next = guard.state.clone();
        next.deposit = new_deposit;
        let to_persist = next.clone();
        let store = Arc::clone(&self.channel_state_store);
        tokio::task::spawn_blocking(move || store.record(&to_persist))
            .await
            .map_err(|e| StoreError::Backend(format!("update_channel_deposit join: {e}")))??;
        guard.state = next;
        drop(guard);
        self.refresh_channel_metrics().await;
        Ok(())
    }

    async fn refresh_channel_metrics(&self) {
        let _refresh = self.channel_metrics_refresh.lock().await;
        let (open, channels) = {
            let channels = self.channels.lock().await;
            (
                channels.len(),
                channels.values().cloned().collect::<Vec<_>>(),
            )
        };
        let mut deposit = U256::ZERO;
        for channel in channels {
            deposit = deposit.saturating_add(channel.lock().await.state.deposit);
        }
        self.metrics.set_inbound_channel_snapshot(open, deposit);
    }
}

/// Outcome of a single batch-boundary voucher exchange.
enum VoucherOutcome {
    Accepted,
    Rejected,
}

impl ProtocolHandler for ClientHandler {
    /// ADR 015: `cdn/client/v1` MUST reject 0-RTT — always take the full
    /// handshake, never `into_0rtt()`. This is the inverse of `ProbeHandler`.
    async fn on_accepting(&self, accepting: Accepting) -> Result<Connection, AcceptError> {
        accepting.await.map_err(AcceptError::from_err)
    }

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

/// The first message on a fresh `cdn/client/v1` stream: either a paid delivery
/// request or a standalone cooperative-close request (ADR 003 §Cooperative
/// close). Both open a bidirectional stream and lead with one [`ClientMessage`].
enum FirstMessage {
    /// A paid delivery: [`StreamRequest`] plus its [`StreamRequestExt`].
    Delivery(StreamRequest, StreamRequestExt),
    /// A request for the node's cooperative-close waiver.
    CooperativeClose(CooperativeCloseRequest),
}

/// Read the first framed [`ClientMessage`] on a stream with a timeout. A
/// [`ClientMessage::StreamRequest`] yields [`FirstMessage::Delivery`] (with its
/// [`StreamRequestExt`] parsed from the trailing bytes — the ADR 005 two-phase
/// pattern; an absent extension yields `StreamRequestExt::default()`); a
/// [`ClientMessage::CooperativeCloseRequest`] yields
/// [`FirstMessage::CooperativeClose`]. Any other variant is a protocol fault.
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
            Ok(FirstMessage::Delivery(req, ext))
        }
        Ok((ClientMessage::CooperativeCloseRequest(req), _)) => {
            Ok(FirstMessage::CooperativeClose(req))
        }
        Ok((_, _)) => Err(StreamReadError {
            err: anyhow::anyhow!("expected StreamRequest or CooperativeCloseRequest"),
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

/// Read one framed [`ClientMessage::Voucher`] with a timeout.
async fn read_voucher(recv: &mut RecvStream) -> anyhow::Result<decdn_protocol::client::Voucher> {
    let frame = tokio::time::timeout(VOUCHER_READ_TIMEOUT, read_frame(recv))
        .await
        .map_err(|_| anyhow::anyhow!("voucher read timed out after {VOUCHER_READ_TIMEOUT:?}"))?
        .map_err(|e| anyhow::anyhow!("voucher frame read failed: {e}"))?;
    match decode_message::<ClientMessage>(&frame) {
        Ok((ClientMessage::Voucher(v), _)) => Ok(v),
        Ok((_, _)) => anyhow::bail!("expected ClientMessage::Voucher"),
        Err(e) => anyhow::bail!("voucher decode failed: {e}"),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    // #859: the handler-layer dedup set spawns at most one background warm per
    // in-flight hash, and re-arms once the spawned task's guard releases.
    #[test]
    fn background_fill_dedup_and_rearm() {
        let inflight = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let hash = Hash::new(b"background-fill-dedup");

        // The first foreground timeout claims the hash → caller receives a guard.
        let guard = arm_background_fill(&inflight, hash);
        assert!(guard.is_some(), "first claim must succeed");
        // A concurrent timeout for the same hash is deduped while the guard lives.
        assert!(
            arm_background_fill(&inflight, hash).is_none(),
            "a second claim while the first is in flight must be deduped"
        );
        // A different hash is independent.
        let other_guard = arm_background_fill(&inflight, Hash::new(b"other-hash"));
        assert!(
            other_guard.is_some(),
            "a distinct hash claims independently"
        );

        // The spawned task's guard releases the claim when it (and the task) ends.
        drop(guard);
        // A later miss on the released hash re-arms and spawns again.
        assert!(
            arm_background_fill(&inflight, hash).is_some(),
            "the hash re-arms once its guard releases"
        );
        drop(other_guard);
    }

    /// The warm memory budget is denominated in BYTES, not tasks (#1145 review).
    ///
    /// A task-count ceiling bounds the wrong quantity: what a warm costs is its blob, and
    /// a blob is bounded only by `max_blob_size_mb`. Eight concurrent warms of near-max
    /// (1 GiB) blobs is ~8 GiB resident for up to `BACKGROUND_FILL_HARD_CAP` — an hour —
    /// while eight concurrent warms of 4 KiB blobs cost nothing and were throttled just as
    /// hard.
    ///
    /// Each warm reserves `max_blob_size_mb` MiB up front, since a blob's true size is not
    /// known until it has been fetched. So the concurrency a node actually gets is
    /// `MAX_BACKGROUND_FILL_MB / max_blob_size_mb` — many for small blobs, few for large.
    /// Concurrency a node actually gets under the real sizing function: pool ÷ reservation.
    fn concurrent_warms(max_blob_size_mb: u64) -> u32 {
        let (pool_mb, reserve_mb) = warm_budget_mb(max_blob_size_mb);
        pool_mb / reserve_mb
    }

    /// The warm memory budget is denominated in BYTES, not tasks (#1145 review).
    ///
    /// A task-count ceiling bounds the wrong quantity: what a warm costs is its blob, and a
    /// blob is bounded only by `max_blob_size_mb`. The old fixed ceiling of 8 meant eight
    /// concurrent warms of near-max (1 GiB) blobs — ~8 GiB resident for up to
    /// `BACKGROUND_FILL_HARD_CAP`, an hour — while eight concurrent warms of 4 MiB blobs
    /// cost 32 MiB and were throttled exactly as hard.
    ///
    /// Asserted on `warm_budget_mb`, the real function `BackgroundFill::new` calls. (An
    /// earlier draft of this test stood up its own semaphore and reserved from it, which
    /// proves only that tokio's semaphore works — the same "assert against a copy of the
    /// logic" mistake this review round exists to remove.)
    #[test]
    fn the_warm_budget_admits_by_bytes_not_by_task_count() {
        // A node serving 1 GiB blobs: each warm reserves the whole ceiling, so two fit the
        // 2 GiB pool and the third sheds. Memory in flight is bounded, by construction.
        assert_eq!(warm_budget_mb(1024), (2048, 1024));
        assert_eq!(concurrent_warms(1024), 2);

        // A node serving 4 MiB blobs runs 512 at once — the count-based ceiling stopped it
        // at 8, for no memory reason at all.
        assert_eq!(concurrent_warms(4), 512);
        assert!(
            concurrent_warms(4) > 8,
            "small-blob nodes must not inherit the old fixed task-count ceiling"
        );

        // Whatever the ceiling, the memory in flight never exceeds the pool.
        for ceiling in [1u64, 4, 64, 512, 1024, 2048] {
            let (pool_mb, reserve_mb) = warm_budget_mb(ceiling);
            assert!(
                concurrent_warms(ceiling) * reserve_mb <= pool_mb,
                "ceiling {ceiling} MiB admits more warms than the pool can hold"
            );
        }
    }

    /// Both ends of `warm_budget_mb`, each of which silently disables the feature if dropped.
    ///
    /// The zero case is asserted on the PROPERTY — how many warms it admits — and not on the
    /// mechanism, because the version this replaces pinned the mechanism and mistook it for
    /// the property (#1145 review). It asserted `warm_budget_mb(0).1 == 1` under the comment
    /// "a zero ceiling must not make warms free and unbounded", which is exactly backwards:
    /// `0` is the UNLIMITED sentinel, so reserving 1 MiB for an unbounded blob is what makes
    /// warms free and unbounded. The assertion passed while the property it named was false,
    /// and 2048 concurrent warms of arbitrarily large blobs were admitted at that setting.
    #[test]
    fn the_warm_reservation_is_bounded_at_both_ends() {
        // Above the pool: the reservation is capped, so ONE warm still runs. Uncapped, the
        // reservation could never be granted — a semaphore cannot hand out more permits
        // than it holds — and every warm would shed forever, with the feature looking
        // configured and doing nothing.
        let (pool_mb, reserve_mb) = warm_budget_mb(MAX_BACKGROUND_FILL_MB * 4);
        assert_eq!(reserve_mb, pool_mb, "capped at the pool");
        assert_eq!(concurrent_warms(MAX_BACKGROUND_FILL_MB * 4), 1);

        // Unlimited (`0`): the worst-case blob is unbounded, so exactly one warm may run —
        // the same answer as a ceiling above the pool, because that is the same situation.
        assert_eq!(
            concurrent_warms(0),
            1,
            "an unlimited blob ceiling must admit ONE warm at a time: the reserve is a proxy \
             for the worst-case blob, and here that blob is unbounded. Reserving anything \
             smaller lets N warms each buffer an arbitrarily large blob into memory."
        );
        let (pool_mb, reserve_mb) = warm_budget_mb(0);
        assert_eq!(
            reserve_mb, pool_mb,
            "an unlimited ceiling IS a ceiling larger than the pool, and must reserve like one"
        );
    }

    /// `BACKGROUND_FILL_HARD_CAP` is a leak guard, and the guard has to actually fire: an
    /// upstream trickling one byte per stall window never trips the inactivity bound, and
    /// because the warm's `inflight` claim is held for the task's life, a warm that never
    /// ends means that blob can never be warmed again for the life of the process.
    ///
    /// `with_optional_deadline` is the seam. `None` (tests) runs to completion; `Some`
    /// terminates a future that otherwise never would.
    #[tokio::test]
    async fn a_background_warm_cannot_outlive_its_cap() {
        let capped = with_optional_deadline(
            Some(Duration::from_millis(20)),
            std::future::pending::<()>(),
        )
        .await;
        assert!(
            capped.is_err(),
            "a warm that never completes must be abandoned at its cap — inactivity is not \
             liveness, and an uncapped warm poisons its hash for the life of the process"
        );

        let uncapped = with_optional_deadline(None, std::future::ready(7u8)).await;
        assert_eq!(uncapped.ok(), Some(7), "no cap: the future runs to its end");
    }

    /// An origin whose fetch NEVER returns, so a warm launched against it holds its memory
    /// reservation for as long as the test needs it to.
    ///
    /// Required to make the shed observable at all: with any completing origin the first
    /// warm finishes and hands its permit back, and the second one then always fits.
    #[derive(Debug)]
    struct HangingOrigin;

    impl decdn_cache::Origin for HangingOrigin {
        fn fetch(
            &self,
            _hash: Hash,
            _max_bytes: u64,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>,
                    > + Send
                    + '_,
            >,
        > {
            Box::pin(std::future::pending())
        }

        fn kind(&self) -> decdn_cache::OriginKind {
            decdn_cache::OriginKind::Filesystem
        }
    }

    /// Build the smallest `ClientHandler` that can spawn a background warm.
    /// `background_fill` is the (construction-time) warm state the test needs, or
    /// `None` when the test does not exercise the warm path.
    async fn handler_for_warm_tests(
        metrics: &Arc<Metrics>,
        background_fill: Option<BackgroundFill>,
    ) -> (Arc<ClientHandler>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = CacheEngine::open(
            dir.path(),
            vec![Arc::new(HangingOrigin) as Arc<dyn decdn_cache::Origin>],
            16,
        )
        .await
        .expect("cache");
        let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
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
            Arc::new(decdn_incentive::store::MemoryChannelStateStore::new())
                as Arc<dyn ChannelStateStore>,
            Arc::new(crate::receipt_log::DirectReceiptSink::new(Arc::new(
                crate::receipt_log::NoopReceiptLog,
            ))) as Arc<dyn ReceiptSink>,
            Arc::new(AtomicU64::new(1)),
            crate::rate_bounds::RateBounds::new(0, u64::MAX),
            1,
            0,
            16,
        );
        deps.background_fill = background_fill;
        let handler = ClientHandler::new(deps).expect("handler");
        (Arc::new(handler), dir)
    }

    #[tokio::test]
    async fn channel_metric_refresh_releases_map_and_serializes_snapshots() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_warm_tests(&metrics, None).await;
        let old_id = B256::repeat_byte(0xA1);
        let old = Arc::new(Mutex::new(ChannelDeliveryState {
            state: ChannelState::new(
                old_id,
                Address::repeat_byte(0x11),
                Address::repeat_byte(0x22),
                U256::from(10u64),
            ),
            bytes_delivered_cumulative: U256::ZERO,
        }));
        handler
            .channels
            .lock()
            .await
            .insert(old_id, Arc::clone(&old));

        let held = old.lock().await;
        let first = tokio::spawn({
            let handler = Arc::clone(&handler);
            async move { handler.refresh_channel_metrics().await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !first.is_finished(),
            "refresh must be waiting on the channel"
        );

        let new_id = B256::repeat_byte(0xB2);
        let mut channels =
            tokio::time::timeout(Duration::from_millis(100), handler.channels.lock())
                .await
                .expect("refresh must release the global map before awaiting a channel");
        channels.clear();
        channels.insert(
            new_id,
            Arc::new(Mutex::new(ChannelDeliveryState {
                state: ChannelState::new(
                    new_id,
                    Address::repeat_byte(0x33),
                    Address::repeat_byte(0x44),
                    U256::from(20u64),
                ),
                bytes_delivered_cumulative: U256::ZERO,
            })),
        );
        drop(channels);

        let mut second = tokio::spawn({
            let handler = Arc::clone(&handler);
            async move { handler.refresh_channel_metrics().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut second)
                .await
                .is_err(),
            "a newer refresh must wait so it publishes after the older snapshot"
        );

        drop(held);
        first.await.expect("first refresh task");
        second.await.expect("second refresh task");
        let encoded = metrics.encode().expect("metrics encode");
        assert!(encoded.lines().any(|line| line == "decdn_channels_open 1"));
        assert!(
            encoded
                .lines()
                .any(|line| line == "decdn_channel_deposit_usdc 20")
        );
    }

    /// The warm memory budget must SHED, not just be computed (#1145 review).
    ///
    /// `MAX_BACKGROUND_FILL_MB` is the bound that stops the hour-long warm lifetime from
    /// being a memory amplifier: 8 concurrent warms × a 1 GiB blob ceiling was up to 8 GiB
    /// resident, for up to an hour each, and the old ~70 s deadline used to make that
    /// self-limiting. Three unit tests covered `warm_budget_mb` — the SIZING function — and
    /// nothing at all covered the code that acts on it. Deleting the whole
    /// `try_acquire_many_owned` block left the suite green.
    ///
    /// So this drives the real `maybe_spawn_background_fill`, against an origin that never
    /// returns: the first warm therefore HOLDS its reservation, and with a ceiling sized to
    /// the whole pool there is nothing left for a second. The shed is observed on the real
    /// counters, and the hash is left unclaimed so a later miss can retry it.
    #[tokio::test]
    async fn a_warm_that_cannot_reserve_its_memory_is_shed_rather_than_spawned() {
        let metrics = Arc::new(Metrics::new());
        // A ceiling at (or above) the pool means each warm reserves the ENTIRE budget, so
        // exactly one can be in flight. `warm_budget_mb` clamps it, which is what makes an
        // operator who over-raises `max_blob_size_mb` get one warm rather than none.
        let (handler, _dir) = handler_for_warm_tests(
            &metrics,
            Some(BackgroundFill::new(
                CancellationToken::new(),
                None,
                MAX_BACKGROUND_FILL_MB,
            )),
        )
        .await;

        handler.maybe_spawn_background_fill(Hash::new(b"first"));
        // The first warm is parked in `HangingOrigin::fetch`, holding its reservation. Wait
        // for it to actually be spawned rather than sleeping on a guess.
        for _ in 0..200u32 {
            if metrics
                .encode()
                .is_ok_and(|e| e.contains("decdn_node_pull_through_background_spawned_total 1"))
            {
                break;
            }
            tokio::task::yield_now().await;
        }

        // A DIFFERENT hash, so the dedup claim is not what stops it — only the memory budget
        // can be.
        handler.maybe_spawn_background_fill(Hash::new(b"second"));

        let encoded = metrics.encode().expect("metrics encode");
        assert!(
            encoded.contains("decdn_node_pull_through_background_shed_total 1"),
            "the second warm must be SHED: the first still holds the whole memory budget. \
             Without the reservation, an hour-long warm lifetime is a memory amplifier — up \
             to 8 GiB resident against a 1 GiB blob ceiling. Got:\n{encoded}"
        );
        assert!(
            encoded.contains("decdn_node_pull_through_background_spawned_total 1"),
            "a shed warm must never be spawned — `shed` is disjoint from `spawned`, which is \
             why it is NOT a term in `spawned == succeeded + missed + failed + cancelled + \
             panicked`. Got:\n{encoded}"
        );
    }

    /// A clean MISS is not a fault, and the two must not share a counter (#1145 review).
    ///
    /// `node_pull_through_background_failed_total` used to count both, so no threshold on it
    /// could distinguish "the network does not have this blob" (routine) from "our store is
    /// corrupt" (an emergency) — and both logged at `debug!`, below the project's default
    /// `RUST_LOG=info`. There was no alert an operator could write.
    #[test]
    fn a_clean_miss_is_not_a_fault() {
        let hash = Hash::new(b"warm-miss");
        assert!(
            is_clean_miss(&CacheError::NotFound { hash }),
            "no provider had it: routine"
        );
        assert!(
            is_clean_miss(&CacheError::NoOrigin { hash }),
            "nowhere to look: routine"
        );
        assert!(
            !is_clean_miss(&CacheError::Store(anyhow::anyhow!("disk full"))),
            "a store fault is an EMERGENCY and must never be filed as a miss — that is what \
             made the failure counter unalertable"
        );
        assert!(
            !is_clean_miss(&CacheError::VerifyFailed { expected: hash }),
            "an upstream serving bytes that do not verify is a fault"
        );
    }

    /// A warm that PANICS must still be counted — it is the one outcome the task cannot
    /// report for itself (#1145 review) — but a warm merely DROPPED unpolled at shutdown is
    /// NOT a panic and must not fire the `PANICKED` error.
    ///
    /// Nothing awaits a warm's `JoinHandle`; it is spawned and forgotten. So a panic inside
    /// `populate`, the tee, or the decoder reached nobody: no counter moved, nothing was
    /// logged, and `spawned` sat permanently one above the sum of its outcomes — a gap that
    /// reads like an in-flight warm rather than a crash. `Drop` runs on the unwind, which is
    /// what makes the guard able to see it. But it also runs on a clean teardown drop, so the
    /// guard must distinguish the two via `thread::panicking()`, or every restart cries wolf.
    ///
    /// Asserted on the REAL `Metrics`, across three cases: a genuine unwind → `panicked`; a
    /// drop-unpolled (no panic) → `cancelled`, NOT `panicked`; a recorded verdict → neither.
    #[test]
    #[allow(clippy::panic, clippy::expect_used)] // deliberately panics to test the unwind path
    fn a_panicking_warm_is_counted_but_a_dropped_one_is_not() {
        let hash = Hash::new(b"warm-panic");

        // (1) A GENUINE unwind: the guard is dropped while `thread::panicking()` is true.
        let metrics = Metrics::new();
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {})); // silence the expected panic's backtrace
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _outcome = WarmOutcome::new(hash, &metrics);
            panic!("warm body exploded");
        }));
        std::panic::set_hook(prev_hook);
        assert!(unwound.is_err(), "the closure must have panicked");
        let encoded = metrics.encode().expect("metrics encode");
        assert!(
            encoded.contains("decdn_node_pull_through_background_panicked_total 1"),
            "a warm that unwound must be metered as a panic. Got:\n{encoded}"
        );

        // (2) A DROP WITHOUT A PANIC — a task cancelled/dropped unpolled at teardown. This is
        // the false-alarm case: it must count `cancelled`, not `panicked`. Fails on revert.
        let metrics = Metrics::new();
        drop(WarmOutcome::new(hash, &metrics));
        let encoded = metrics.encode().expect("metrics encode");
        assert!(
            encoded.contains("decdn_node_pull_through_background_panicked_total 0"),
            "a clean drop must NOT report a panic. Got:\n{encoded}"
        );
        assert!(
            encoded.contains("decdn_node_pull_through_background_cancelled_total 1"),
            "a clean drop-unpolled must count as cancelled. Got:\n{encoded}"
        );

        // (3) A warm that recorded a verdict is neither a panic nor a cancel.
        let metrics = Metrics::new();
        let mut outcome = WarmOutcome::new(hash, &metrics);
        outcome.record(WarmVerdict::Succeeded);
        drop(outcome);
        let encoded = metrics.encode().expect("metrics encode");
        assert!(
            encoded.contains("decdn_node_pull_through_background_panicked_total 0")
                && encoded.contains("decdn_node_pull_through_background_cancelled_total 0"),
            "a warm that recorded a verdict must not also report a panic or a cancel. Got:\n{encoded}"
        );
        assert!(
            encoded.contains("decdn_node_pull_through_background_succeeded_total 1"),
            "…and must record the verdict it was given. Got:\n{encoded}"
        );
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
            ServeRejectReason::UnauthorizedOrigin,
            ServeRejectReason::CooperativeCloseSigned,
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
    /// The failure this pins is not hypothetical — it shipped. Governance
    /// entries used to reach the serve path only as cache evictions and answered
    /// `EvictedSinceProbe`, which made `HashBlacklisted` a unique fingerprint for
    /// "this operator privately denied it": exactly the map of an operator's
    /// legal exposure the ADR forecloses.
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

    // #821: the reactive pull-through authorized-origin gate refuses a pull only
    // when the operator opted in (a directory is wired) AND the hash has no
    // authorized origin; an unset gate keeps the permissionless default.
    #[test]
    fn pull_origin_gate_decision() {
        use crate::dht::origin::{
            EmptyOriginDirectory, Hash as OriginHash, OriginDirectory, StaticOriginDirectory,
        };
        use crate::dht::routing::NodeId;

        let h = OriginHash::from_bytes([7u8; 32]);

        // Unset gate (default): never blocks — cache role stays permissionless.
        assert!(!pull_origin_gate_blocks(None, &h));

        // Opted in on a node with no chain addresses — the runtime's fallback
        // shape. Blocks every hash, which is the #1292 hazard: on the wire this
        // is indistinguishable from a plain miss.
        let empty: Arc<dyn OriginDirectory> = Arc::new(EmptyOriginDirectory);
        assert!(pull_origin_gate_blocks(Some(&empty), &h));

        // Opted in, directory holds an authorized origin: allows the pull.
        let mut m = HashMap::new();
        m.insert(h, vec![NodeId::from_bytes([1u8; 32])]);
        let authorized: Arc<dyn OriginDirectory> = Arc::new(StaticOriginDirectory::new(m));
        assert!(!pull_origin_gate_blocks(Some(&authorized), &h));
        // A different, unclaimed hash through the same directory is still blocked.
        assert!(pull_origin_gate_blocks(
            Some(&authorized),
            &OriginHash::from_bytes([9u8; 32])
        ));
    }
}
