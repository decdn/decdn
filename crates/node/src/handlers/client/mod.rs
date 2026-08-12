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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{Bytes, CHUNK_GROUP_BYTES, CacheEngine, CacheError, Hash, RangePullOutcome};
use decdn_incentive::rate::{DEFAULT_TOLERANCE_BPS, RateError, min_payment, verify_rate};
use decdn_incentive::store::StoreError;
use decdn_incentive::{
    ChannelId, ChannelState, ChannelStateStore, CooperativeClose, SignedVoucher, StreamSlashData,
    VoucherActivity, verify_binding, voucher_reject_reason, wire_voucher_to_signed,
};
use decdn_protocol::client::{
    ChunkData, ClientMessage, CooperativeCloseAuth, CooperativeCloseRequest, StreamError,
    StreamRequest, StreamRequestExt, StreamResponse, StreamResponseBody, VoucherRejectReason,
    WatermarkBundle,
};
use decdn_protocol::{
    ALPN_CLIENT, APP_ERR_RATE_LIMITED, FrameError, MB_BYTES, decode_message, encode_message,
    is_unknown_variant, read_frame, write_frame,
};
use iroh::PublicKey;
use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};

use crate::dispatch::{ConnectionLimiter, RejectReason};
use crate::leech_governor::LeechGovernor;
use crate::metrics::Metrics;
use crate::node_origin::NodeOrigin;
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
/// Group-commit interval when the handler is built without an explicit one
/// (`voucher_commit_interval == None`, i.e. tests and any construction that does
/// not thread `payment.voucher_commit_interval_ms`). Mirrors the config default
/// [`decdn_common::config::DEFAULT_VOUCHER_COMMIT_INTERVAL_MS`]; the runtime
/// always sets an explicit value from resolved config, so this only backs the
/// `None` case. See [`ClientHandler::commit_interval`] (#1483).
const DEFAULT_COMMIT_INTERVAL: Duration =
    Duration::from_millis(decdn_common::config::DEFAULT_VOUCHER_COMMIT_INTERVAL_MS);
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
    /// `Option` would only add a way to fail open on a takedown gate. It is a
    /// required [`ClientHandlerDeps::new`] parameter: seeding it empty and
    /// relying on the runtime to overwrite it was itself a silent fail-open — a
    /// construction site that forgot the wiring was indistinguishable from an
    /// operator who denies nothing. Callers with no deny-set (tests) pass
    /// `ContentDenylist::empty()` explicitly.
    pub content_deny: Arc<crate::content_deny::ContentDenylist>,
    // Optional wiring — `None` unless the deployment enables the feature.
    pub redeem_hint: Option<mpsc::Sender<ChannelId>>,
    pub voucher_activity: Option<Arc<VoucherActivity>>,
    pub region_accountant: Option<Arc<RegionAccountant>>,
    pub pull_through: Option<Duration>,
    pub local_populate: Option<Duration>,
    pub pull_through_origin: Option<Arc<NodeOrigin>>,
    pub pull_ahead_bytes: Option<Bytes>,
    /// Downstream paid-delivery credit window in bytes (ADR 003 §Credit window):
    /// how far past cleared payment the serve loop keeps streaming before it must
    /// collect a voucher. `None` (the default, and in tests) reads as one voucher
    /// interval — stop-and-wait, the pre-credit-window cadence. The runtime sets
    /// it from `payment.credit_window_bytes`. Independent of `pull_ahead_bytes`
    /// (which bounds the *upstream* speculative spend on a cache-miss pull): this
    /// bounds the *downstream* unbilled-egress exposure. Both are floored at one
    /// interval so the serve loop can always make progress.
    pub credit_window_bytes: Option<Bytes>,
    /// Group-commit interval (ADR 003 §Off-chain voucher state persistence,
    /// #1483): how long the serve loop waits to gather more vouchers into one
    /// fsynced commit before committing what it has. `None` (the default, and in
    /// tests) reads as the `DEFAULT_VOUCHER_COMMIT_INTERVAL_MS` config default
    /// (5 ms). The runtime sets it from `payment.voucher_commit_interval_ms`.
    pub voucher_commit_interval: Option<Duration>,
    pub leech_governor: Option<Arc<LeechGovernor>>,
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
        content_deny: Arc<crate::content_deny::ContentDenylist>,
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
            content_deny,
            redeem_hint: None,
            voucher_activity: None,
            region_accountant: None,
            pull_through: None,
            local_populate: None,
            pull_through_origin: None,
            pull_ahead_bytes: None,
            credit_window_bytes: None,
            voucher_commit_interval: None,
            leech_governor: None,
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
    /// Downstream paid-delivery credit window in bytes (ADR 003 §Credit window),
    /// set at construction via [`ClientHandlerDeps`]. The serve loop keeps
    /// streaming while `delivered − paid ≤ credit_window`, collecting cumulative
    /// vouchers as they arrive instead of stalling a full round trip at every
    /// interval. `None` reads as one voucher interval (stop-and-wait). Read
    /// through [`Self::credit_window`], which applies the one-interval floor.
    credit_window_bytes: Option<Bytes>,
    /// Group-commit interval (ADR 003 §Off-chain voucher state persistence,
    /// #1483), set at construction via [`ClientHandlerDeps`]. The serve loop
    /// waits at most this long to gather additional vouchers into one fsynced
    /// commit before committing the batch it has, amortizing the per-voucher
    /// fsync while still acknowledging each voucher only after it is durable.
    /// `None` reads as [`DEFAULT_COMMIT_INTERVAL`]. Read through
    /// [`Self::commit_interval`].
    voucher_commit_interval: Option<Duration>,
    /// Node-wide seed-leech caps (#856, ADR 037), set at construction via
    /// [`ClientHandlerDeps`]. Consulted before/while a speculative pull-through
    /// proceeds and credited from the voucher path. `None` (tests / feature off)
    /// leaves only the per-request window.
    leech_governor: Option<Arc<LeechGovernor>>,
    /// Live content deny-set (ADR 011). Consulted at three points, all of which
    /// must gate or the check is bypassable: the hash gate above the
    /// availability check in `serve_stream`, the origin gate right after channel
    /// resolution, and the same origin gate inside `pull_authorized` — that last
    /// one runs EARLIEST and decides whether to front upstream USDC egress, so
    /// omitting it would have this node pay on a blacklisted origin's behalf
    /// before ever reaching the serve refusal. The window-paced serve path
    /// (`window.rs`) is a fourth, independent ladder.
    pub(crate) content_deny: Arc<crate::content_deny::ContentDenylist>,
    rate_per_mb: Arc<AtomicU64>,
    rate_bounds: crate::rate_bounds::RateBounds,
    voucher_interval_mb: u64,
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
            pull_through_origin: deps.pull_through_origin,
            pull_ahead_bytes: deps.pull_ahead_bytes,
            credit_window_bytes: deps.credit_window_bytes,
            voucher_commit_interval: deps.voucher_commit_interval,
            leech_governor: deps.leech_governor,
            content_deny: deps.content_deny,
            rate_per_mb: deps.rate_per_mb,
            rate_bounds: deps.rate_bounds,
            voucher_interval_mb: deps.voucher_interval_mb,
            max_blob_size_bytes: deps.max_blob_size_bytes,
            max_concurrent_streams: deps.max_concurrent_streams,
            deposit_refusal_last_warn_ms: AtomicU64::new(0),
            deposit_refusal_suppressed: AtomicU64::new(0),
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
        channel_id: ChannelId,
        hash: Hash,
        headroom: U256,
        ceiling: U256,
    ) {
        tracing::debug!(
            %channel_id, %hash, %headroom, %ceiling,
            "refusing delivery: remaining channel deposit cannot cover the reserved cost"
        );
        if let Some(suppressed) = self.note_deposit_refusal() {
            tracing::warn!(
                %channel_id, %headroom, %ceiling, suppressed,
                interval = ?Self::DEPOSIT_REFUSAL_WARN_INTERVAL,
                "refusing paying clients: remaining channel deposit below the reserved cost. \
                 A sustained rate here is either a client running dry (no action) or this \
                 node's chain watcher lagging behind an on-chain top-up (check RPC health) \
                 — see docs/runbook.md"
            );
        }
    }

    /// The effective downstream credit window in bytes for a stream whose
    /// negotiated voucher interval is `interval_bytes` (ADR 003 §Credit window).
    ///
    /// The serve loop keeps `delivered − paid` within this bound before it must
    /// collect a voucher, so it is exactly the node's bounded credit exposure:
    /// unbilled egress already on the wire, capped here and nowhere else. Floored
    /// at one interval so the loop can always make progress (deliver a full
    /// interval, then recoup it) — a configured window below one interval, or the
    /// unconfigured `None`, both collapse to the interval, which reproduces the
    /// pre-credit-window stop-and-wait cadence exactly. The floor is also what
    /// rules out a deadlock: whenever the window blocks further delivery, at least
    /// one full interval is unpaid, so there is always a voucher to collect.
    pub(super) fn credit_window(&self, interval_bytes: u64) -> u64 {
        self.credit_window_bytes
            .as_ref()
            .map_or(0, |b| b.get())
            .max(interval_bytes)
    }

    /// The group-commit interval for this handler (ADR 003 §Off-chain voucher
    /// state persistence, #1483): the most the recoup phase waits to gather
    /// another voucher into the current fsynced batch before committing what it
    /// has. `None` reads as [`DEFAULT_COMMIT_INTERVAL`]. Zero is a valid setting
    /// (commit each blocking-read batch immediately) and is preserved. The batch
    /// is bounded above by the credit window regardless — at most
    /// `credit_window / interval` vouchers are ever outstanding — so this only
    /// governs the *wait* for a straggler, never grows the batch past the window.
    pub(super) fn commit_interval(&self) -> Duration {
        self.voucher_commit_interval
            .unwrap_or(DEFAULT_COMMIT_INTERVAL)
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

    /// Snapshot the latest tracked [`ChannelState`] for `channel_id`, or `None`
    /// if this node does not track the channel. Used by the settlement watcher's
    /// self-defense-dispute reaction (#1586) to read the node's latest persisted
    /// voucher (nonce / amount / signature) when a counterparty closes a channel
    /// at a stale watermark. Reads the in-memory row, which the voucher-accept
    /// path advances only *after* the durable store write commits (#527), so the
    /// snapshot never reports a voucher the node has not persisted.
    pub async fn channel_state_snapshot(&self, channel_id: ChannelId) -> Option<ChannelState> {
        let entry = self.channels.lock().await.get(&channel_id).cloned()?;
        let guard = entry.lock().await;
        Some(guard.state.clone())
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

/// How far a group-commit voucher batch got, returned by
/// [`ClientHandler::collect_voucher_batch`] (#1483).
struct BatchOutcome {
    /// Number of vouchers durably committed AND acknowledged this call. The
    /// serve loop advances its `paid` counter by the sum of the corresponding
    /// deltas and re-queues any deltas beyond this — a *short* batch, meaning the
    /// client had not sent those vouchers yet — for the next recoup.
    committed: usize,
    /// Whether the stream must end now.
    stop: BatchStop,
}

/// Terminal disposition of a voucher batch.
enum BatchStop {
    /// Every gathered voucher committed and acked; keep serving.
    Continue,
    /// A voucher was rejected, or the batch commit failed (`RetryLater`). The
    /// rejection frame was already written and the stream finished cleanly (any
    /// valid prefix was committed + acked first, reflected in
    /// [`BatchOutcome::committed`]); the loop returns `Ok(())`.
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
            // Value checks are kept out of the parse so forward-compatible
            // trailing bytes don't couple to them (ADR 005 two-phase). Gate here
            // rather than at each use: an out-of-range `voucher_interval_mb` is a
            // protocol error per ADR 003 §Voucher Interval Negotiation, and this
            // wire boundary is its only enforcement point — `PaymentChannel`
            // holds no cadence parameter to check it against.
            ext.validate().map_err(|e| StreamReadError {
                err: anyhow::anyhow!("stream request ext rejected: {e}"),
                app_code: APP_ERR_MALFORMED_MESSAGE,
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

/// Cancellation-safe, buffered reader for `cdn/client/v1` voucher frames
/// (#1483 group commit). Owns a byte buffer that PERSISTS across [`Self::read`]
/// calls, so a `read` future cancelled by an outer `timeout` — the batch gather
/// waits on stragglers under [`ClientHandler::commit_interval`] — loses no bytes:
/// any partial frame stays buffered for the next call.
///
/// This is what makes the group-commit gather safe. [`read_frame`] is built on
/// `read_exact` and is NOT cancellation-safe — a `timeout` firing mid-frame
/// would drop already-consumed bytes and desync the stream. Reading instead via
/// the cancel-safe [`tokio::io::AsyncReadExt::read`] into an owned buffer, then
/// splitting whole frames off it with [`decdn_protocol::framing::parse_frame`],
/// keeps every byte. Borrows the `RecvStream` per call so the caller retains it
/// for stream teardown.
///
/// All voucher reads on a given stream MUST go through ONE instance: it may read
/// ahead (buffering the next pipelined voucher, #1486) while a batch commits, and
/// a second reader on the same `RecvStream` would lose those buffered bytes.
#[derive(Default)]
pub(super) struct BufferedVoucherReader {
    /// Unconsumed bytes read from the stream, at a frame boundary or partway
    /// into the next frame's header/body.
    buf: Vec<u8>,
}

impl BufferedVoucherReader {
    /// Read one framed [`ClientMessage::Voucher`], filling the buffer
    /// incrementally. **Cancellation-safe:** if the returned future is dropped
    /// (a gather `timeout` elapsed), bytes already read stay in `self.buf` for
    /// the next call — no frame is torn.
    pub(super) async fn read(
        &mut self,
        recv: &mut RecvStream,
    ) -> anyhow::Result<decdn_protocol::client::Voucher> {
        loop {
            if let Some((header_len, payload_len)) = decdn_protocol::framing::parse_frame(&self.buf)
                .map_err(|e| anyhow::anyhow!("voucher frame parse failed: {e}"))?
            {
                let total = header_len.saturating_add(payload_len);
                let payload = self
                    .buf
                    .get(header_len..total)
                    .ok_or_else(|| anyhow::anyhow!("voucher frame bounds out of range"))?;
                let decoded = decode_message::<ClientMessage>(payload);
                // Consume the frame's bytes regardless of decode outcome so a
                // single bad frame cannot wedge the buffer.
                let result = match decoded {
                    Ok((ClientMessage::Voucher(v), _)) => Ok(v),
                    Ok((_, _)) => Err(anyhow::anyhow!("expected ClientMessage::Voucher")),
                    Err(e) => Err(anyhow::anyhow!("voucher decode failed: {e}")),
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
                .map_err(|e| anyhow::anyhow!("voucher stream read failed: {e}"))?;
            if n == 0 {
                anyhow::bail!("voucher stream closed mid-frame");
            }
            let chunk = scratch
                .get(..n)
                .ok_or_else(|| anyhow::anyhow!("short read length out of range"))?;
            self.buf.extend_from_slice(chunk);
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// Build the smallest `ClientHandler` for the handler-layer tests below.
    async fn handler_for_tests(metrics: &Arc<Metrics>) -> (Arc<ClientHandler>, tempfile::TempDir) {
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
            Arc::new(decdn_incentive::store::MemoryChannelStateStore::new())
                as Arc<dyn ChannelStateStore>,
            Arc::new(crate::receipt_log::DirectReceiptSink::new(Arc::new(
                crate::receipt_log::NoopReceiptLog,
            ))) as Arc<dyn ReceiptSink>,
            Arc::new(AtomicU64::new(1)),
            crate::rate_bounds::RateBounds::new(0),
            1,
            0,
            16,
            Arc::new(crate::content_deny::ContentDenylist::empty()),
        );
        let handler = ClientHandler::new(deps).expect("handler");
        (Arc::new(handler), dir)
    }

    #[tokio::test]
    async fn channel_metric_refresh_releases_map_and_serializes_snapshots() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await;
        let old_id = B256::repeat_byte(0xA1);
        let old = Arc::new(Mutex::new(ChannelDeliveryState {
            state: ChannelState::new(
                old_id,
                Address::repeat_byte(0x11),
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

    /// #1382 / #1388: the hard per-byte voucher floor must track the LIVE
    /// delivery floor, NOT the floor snapshotted when the quote was signed.
    ///
    /// The on-chain `PaymentChannel._advanceClaimWatermark` enforces the floor
    /// against the live `deliveryFloor` storage slot at settlement — there is no
    /// per-channel floor snapshot (the `Channel` struct carries none), and
    /// `setRateBounds` overwrites it globally. So a node that accepted a voucher
    /// priced below the live floor could never redeem it (`RateFloorViolation`).
    /// When a governance floor raise lands mid-stream, a voucher paying the old
    /// quoted rate MUST therefore be rejected: the buyer did nothing wrong, but
    /// the node cannot get paid for those bytes on-chain, so accepting would be
    /// serving for free. Pinning the quote-time floor here (the reverted #1388
    /// approach) would make the node countersign an unredeemable voucher.
    #[tokio::test]
    async fn voucher_floor_tracks_live_bounds_not_the_quote() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_for_tests(&metrics).await;

        // Quote-time band: floor == F, a generous ceiling. The advertised
        // `rate_per_mb` atomic is seeded to 1 in `handler_for_tests`, so
        // set the band's floor to F and clamp will raise the quote to F.
        let f: u64 = 500;
        handler.rate_bounds.store(f);

        // Quote the stream at rate F (the value signed into the `StreamResponse`).
        let quoted_rate = handler.clamped_rate();
        assert_eq!(quoted_rate, f, "quote clamps up to the floor");

        // A well-formed cumulative voucher paying exactly the quoted rate for a
        // one-MB interval.
        let bytes_per_mb = decdn_incentive::rate::BYTES_PER_MB;
        let bytes = U256::from(bytes_per_mb);
        let amount = min_payment(bytes_per_mb, quoted_rate);

        // While the floor is still F, the voucher clears the live floor — this is
        // the exact call `collect_voucher` makes (`verify_rate(amount, new_bytes,
        // self.rate_bounds.floor(), 0)`), and on-chain redemption would succeed.
        assert!(
            verify_rate(amount, bytes, handler.rate_bounds.floor(), 0).is_ok(),
            "at the quoted floor the voucher is redeemable"
        );

        // Governance raises the floor above F mid-stream (the ~1s watcher cadence
        // spanning multiple voucher intervals — the reachable race from #1382).
        // The chain now enforces 2F at settlement, so a voucher priced at F is
        // unredeemable and the live-floor acceptance check MUST reject it.
        handler.rate_bounds.store(f * 2);
        assert_eq!(
            handler.rate_bounds.floor(),
            f * 2,
            "live floor moved above the quoted rate"
        );
        assert!(
            matches!(
                verify_rate(amount, bytes, handler.rate_bounds.floor(), 0),
                Err(RateError::Underpayment { .. })
            ),
            "after the live floor rises above the quoted rate the voucher must be \
             rejected — it would revert on-chain with RateFloorViolation"
        );
    }
}
