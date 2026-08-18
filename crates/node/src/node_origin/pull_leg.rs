//! The gap-driven, range-minimized **pull leg** of the node serve-miss (ADR 037).
//!
//! This module drives the shared
//! [`decdn_client_pull::drive`] loop over a node sink, so a serve-miss pulls and
//! pays UPSTREAM for only the ranges the cache is missing — held ranges are read
//! locally, never re-pulled or re-paid. It is the buyer half of the two concurrent
//! legs the orchestration (`serve_via_window_pull_through`) runs on the one serve
//! task; the seller half is [`super::super::handlers::client`]'s `serve_leg`.
//!
//! # Two entry points
//!
//! - [`NodeOrigin::open_pull_leg`] — discovery + candidate walk + channel open + a
//!   free header handshake. Returns the bound [`PullLegTarget`] (provider, channel
//!   context, ledger) AND the upstream `total_bytes`, so the orchestration can sign
//!   its `StreamResponse` before either leg streams a byte. Discovery happens ONCE
//!   here; the pull leg does not re-discover. Open-time candidate fallback walks
//!   the ranked candidates until one opens; mid-pull candidate switch is deferred,
//!   consistent with the resumable-pull design (#1530).
//! - [`run_pull_leg`] — builds the driver axes ([`NodeAdmitStore`] sink,
//!   [`PeerSource`], [`RampPacer`], [`NodeFunder`]) and runs the drive, then scores
//!   the provider and records its terminal outcome via the shared
//!   [`decdn_cache::FillSession::mark_ended`]. A [`SettleOnDrop`] guard persists the buyer
//!   watermark (#852) on EVERY exit — including a mid-drive cancellation when the
//!   serve leg finishes first and drops this future (client disconnect / shutdown).
//!
//! # Reputation, region accounting, watermark
//!
//! The pull leg handles three concerns explicitly around the `drive`: the
//! [`SettleOnDrop`] guard for #852, a post-drive `record_outcome` scoring the
//! discovered provider, and `region_accountant.record_pulled` on the clean path.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use bao_tree::ChunkRanges;
use decdn_bao_range::RangedStore;
use decdn_cache::{CacheEngine, CacheError, FillError, FillSession, Hash};
use decdn_client_pull::driver::DriveConfig;
use decdn_client_pull::source::{Funder, SourceFuture};
use decdn_client_pull::{
    HashMismatch as ClientPullHashMismatch, PacingWait, PeerSource, RampPacer, drive,
};
use decdn_incentive::DepositOutcome;

use decdn_reputation::Outcome;
use iroh::{EndpointAddr, PublicKey};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::admit_store::NodeAdmitStore;
use super::backend_source::BackendSource;
use super::funder::NodeFunder;
use super::funder::{SETTLE_POLL_STEP, settle_wait_budget};
use super::{
    NodeOrigin, NodeOriginDeps, PullMiss, PullOutcome, SettleOnDrop, bind_upstream_ctx,
    cached_candidates, classify_pull_failure, discover, lane_ledger, now_micros, probe_and_rank,
    record_outcome, record_pool_open_failure,
};
use crate::client_requester::{
    PoolContext, PoolLedger, PullDeadlines, effective_rate_ceiling,
    open_progressive_pull as open_progressive_upstream,
};
use crate::dht::negative_cache::Hash as DhtHash;
use crate::dht::routing::NodeId as DhtNodeId;
use crate::selection::{CHANNEL_OPEN_CALLER_BUDGET, Candidate, MAX_PROVIDER_ATTEMPTS};

/// Bytes per [`bao_tree::ChunkNum`] — a 1 KiB bao chunk. A chunk-range's byte span
/// is its boundaries scaled by this (twin of the driver's private constant).
const CHUNK_BYTES: u64 = 1024;

/// How long an ABANDONED pull leg yields its runtime so the upstream connection can
/// flush its `CONNECTION_CLOSE` and drain before the current-thread runtime that owns
/// it is dropped (see the drain in [`run_pull_leg`]). iroh bounds a
/// close handshake to roughly three seconds on bad connectivity and returns much
/// faster in the usual case (instant on loopback), so this comfortably covers the
/// drain without holding the pull thread for longer than a real close would take.
pub(super) const ABANDON_DRAIN: Duration = Duration::from_secs(3);

/// A discovered, channel-open upstream ready to be pulled from by [`drive`], plus
/// the `total_bytes` the caller needs to sign its `StreamResponse`. Everything a
/// [`PeerSource`] and a [`NodeFunder`] need is bundled here so the orchestration can
/// do discovery ONCE and hand the result to [`run_pull_leg`].
#[allow(
    dead_code,
    reason = "wired by the orchestration in handlers::client::window"
)]
pub(crate) struct PullLegTarget {
    /// The provider's iroh identity — dialled by [`PeerSource`] and scored on
    /// completion.
    pk: PublicKey,
    /// Its bonded operator address: the channel counterparty and the `PeerSource`
    /// `expected_signer`.
    provider_addr: alloy::primitives::Address,
    /// The dial target (`EndpointAddr::new(pk)`).
    target: EndpointAddr,
    /// The buyer channel context, shared behind `Arc<Mutex<..>>` with the
    /// [`NodeFunder`] so a mid-pull top-up's new deposit is visible to the next
    /// [`PeerSource`].
    ctx: Arc<std::sync::Mutex<PoolContext>>,
    /// The channel's shared voucher ledger (`BuyerLedgers::get_or_seed`).
    ledger: Arc<PoolLedger>,
    /// The upstream-claimed content length, from the header handshake.
    pub(crate) total_bytes: u64,
    /// The candidate's DHT id, for region accounting on the clean path.
    node_id: [u8; 32],
    /// The served client's namespace, threaded onto the pull (ADR 005), big-endian.
    namespace_id: [u8; 32],
    /// The effective per-MB rate ceiling (lower of the probe rate and the config
    /// ceiling), enforced by [`PeerSource`] before paying.
    rate_ceiling: u64,
    /// The open/stall stage bounds for each [`PeerSource`].
    deadlines: PullDeadlines,
}

/// The injected wait for [`RampPacer`]'s `Wait`: resolve once the serve leg's paid
/// frontier advances. Awaits the shared `served_paid_advanced` notify so a parked
/// pull re-decides exactly when a downstream voucher clears.
struct ServedPaidWait {
    served_paid_advanced: Arc<Notify>,
    /// The live shared served-paid frontier (the SAME atomic the pacer reads through
    /// `served_paid`). Re-read AFTER the wakeup is registered so an advance that raced
    /// the pacer's `Wait` decision is not waited on forever — `Notify::notify_waiters`
    /// stores no permit, so without this re-check the window-paused pull wedges (the
    /// #1673 CI-starvation hang).
    served_paid: Arc<AtomicU64>,
    /// Bumps `node_pull_through_window_paused` on each pause — the pull hit its ADR
    /// 037 window and is waiting for downstream payment to clear.
    metrics: Arc<crate::metrics::Metrics>,
}

impl PacingWait for ServedPaidWait {
    fn wait(&self, observed: u64) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        // The pull hit its ADR 037 window: count the pause (the decision was `Wait`),
        // independent of whether we then park or short-circuit on a raced advance.
        self.metrics.node_pull_through_window_paused();
        Box::pin(async move {
            // Register the wakeup FIRST, then re-read the frontier. `notify_waiters`
            // wakes only waiters registered at the moment it fires and stores no
            // permit, so the serve leg's `served_advanced().notify_waiters()` that
            // lands between the pacer reading `observed` and this park would be lost —
            // wedging the pull (#1673). Arm the waiter, THEN check: if the frontier
            // already moved past `observed`, the advance we would wait for has already
            // happened, so re-decide at once instead of parking on a notify that will
            // never repeat. Any advance AFTER this arm wakes the registered waiter.
            let mut notified = Box::pin(self.served_paid_advanced.notified());
            notified.as_mut().enable();
            if self.served_paid.load(Ordering::Relaxed) > observed {
                return;
            }
            notified.await;
        })
    }
}

/// Total content bytes a chunk-unit [`ChunkRanges`] covers, clamped to `total`.
///
/// A boundary with no matching close is an OPEN-ENDED run `[a, ∞)`: a fully-present
/// blob observes as `ChunkRanges{0..}` (a single unpaired boundary). Pairing `(a, b)`
/// alone would drop that final run and undercount a complete blob to 0, skewing
/// provider scoring and region accounting. Clamp the open end to `total`.
fn content_len(ranges: &ChunkRanges, total: u64) -> u64 {
    let boundaries = ranges.boundaries();
    let mut it = boundaries.iter();
    let mut sum = 0u64;
    while let Some(a) = it.next() {
        let start = a.0.saturating_mul(CHUNK_BYTES).min(total);
        let end = match it.next() {
            Some(b) => b.0.saturating_mul(CHUNK_BYTES).min(total),
            None => total,
        };
        sum = sum.saturating_add(end.saturating_sub(start));
    }
    sum
}

/// Whether `err` (or anything in its chain) is a paid-but-corrupt UPSTREAM
/// delivery — one that must be scored `Corruption` against the provider and
/// metered `upstream_verify_failed`, as distinct from a transport/refusal fault or
/// a buyer-side (local) fault. Honest providers never trip any arm here.
///
/// Three shapes reach the decoupled serve-miss pull leg, all provider corruption:
///
/// - [`CacheError::VerifyFailed`] / [`CacheError::HashMismatch`] — the node's cache
///   decoder ([`NodeAdmitStore`] → `admit_bao_stream`) rejected a chunk group or
///   the whole-blob root. This is how a wire-complete lie surfaces.
/// - [`decdn_client_pull::HashMismatch`] — the client-pull decoder's typed
///   content-addressing sentinel, matched defensively for the paths that surface it
///   directly (it is also what [`super::pull_verdict`] downcasts to).
/// - The client-pull streaming OVER-DELIVERY guards — the upstream sent more wire
///   than the signed `total_bytes` promised ("… more than the … promised", "… after
///   the promised total"). An honest upstream sends exactly the promised wire then
///   `StreamEnd`, so over-delivery is unambiguous provider misbehaviour; a corrupt
///   upstream desyncs the wire/plaintext accounting and trips it. These are bare
///   `anyhow` strings (no typed sentinel to downcast), so they are matched by their
///   stable text.
///
/// A SHORT/truncated stream ("… of … promised wire bytes before `StreamEnd`") is the
/// deliberate NON-match: a stream that ends early is a transport fault, not
/// corruption, and stays out of this predicate.
fn is_bao_corruption(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        if cause.downcast_ref::<CacheError>().is_some_and(|e| {
            matches!(
                e,
                CacheError::VerifyFailed { .. } | CacheError::HashMismatch { .. }
            )
        }) || cause.downcast_ref::<ClientPullHashMismatch>().is_some()
        {
            return true;
        }
        let msg = cause.to_string();
        msg.contains("more than the") || msg.contains("after the promised total")
    })
}

impl NodeOrigin {
    /// Discover, probe, rank, and open a channel to the best available provider for
    /// `hash`, returning the bound [`PullLegTarget`] (with the upstream `total_bytes`)
    /// the orchestration hands to [`run_pull_leg`]. The header handshake is a
    /// free open — no bytes pulled, no vouchers — so the abandoned probe pull costs
    /// only one round trip.
    ///
    /// Shares [`Self::open_progressive_pull`]'s cached-first discover → probe → rank
    /// pipeline and its open-time candidate fallback, but stops at channel-open +
    /// header instead of returning a live progressive pull.
    ///
    /// # Errors
    ///
    /// The [`PullMiss`] this failure is — unprovisioned, no reachable provider, or
    /// every candidate declined. A [`PullMiss::LocalFault`] must not be signed to the
    /// client as a clean `NotFound` about the content (#1560).
    pub(crate) async fn open_pull_leg(
        &self,
        hash: Hash,
        namespace_id: U256,
    ) -> Result<PullLegTarget, PullMiss> {
        let deps = self.deps.get().ok_or(PullMiss::Clean)?;
        let hash_bytes = *hash.as_bytes();
        let target = DhtHash::from_bytes(hash_bytes);
        let mut budget = MAX_PROVIDER_ATTEMPTS;
        let mut attempt_metered = false;
        let mut miss = PullMiss::Clean;

        if let Some(cached) = cached_candidates(deps, target).await {
            deps.metrics.probe_cache_hit();
            deps.metrics.node_pull_attempt();
            attempt_metered = true;
            let outcome = self
                .open_leg_from_candidates(deps, &cached, hash_bytes, namespace_id, budget)
                .await;
            match outcome.payload {
                Ok(opened) => return Ok(opened),
                Err(failed) => miss = miss.or(failed),
            }
            budget = budget.saturating_sub(outcome.attempts);
            deps.probe_cache.invalidate(&target);
            if budget == 0 {
                debug!(%hash, "node-origin: probe-cache candidates exhausted the pull-leg budget");
                return Err(miss);
            }
        } else {
            deps.metrics.probe_cache_miss();
        }

        let providers = discover(deps, hash_bytes, namespace_id).await;
        if providers.is_empty() {
            if !attempt_metered {
                deps.metrics.node_pull_no_providers();
            }
            debug!(%hash, "node-origin: no providers discovered for the pull leg");
            return Err(miss);
        }
        if !attempt_metered {
            deps.metrics.node_pull_attempt();
        }
        let ranked = probe_and_rank(deps, providers, hash_bytes).await;
        self.open_leg_from_candidates(deps, &ranked, hash_bytes, namespace_id, budget)
            .await
            .payload
            .map_err(|failed| miss.or(failed))
    }

    /// Walk `ranked` (bounded by `budget`), opening a pull-leg target from each until
    /// one succeeds. The open-time-only fallback twin of `open_from_candidates`.
    async fn open_leg_from_candidates(
        &self,
        deps: &NodeOriginDeps,
        ranked: &[Candidate],
        hash_bytes: [u8; 32],
        namespace_id: U256,
        budget: usize,
    ) -> PullOutcome<PullLegTarget> {
        let mut attempts = 0;
        let mut miss = PullMiss::Clean;
        for candidate in ranked.iter().take(budget) {
            attempts += 1;
            match self
                .open_leg_target_from_candidate(deps, candidate, hash_bytes, namespace_id)
                .await
            {
                Ok(target) => {
                    return PullOutcome {
                        payload: Ok(target),
                        attempts,
                    };
                }
                Err(failed) => miss = miss.or(failed),
            }
        }
        PullOutcome {
            payload: Err(miss),
            attempts,
        }
    }

    /// Resolve, open/reuse a channel, bind (#1117), and open a free header handshake
    /// from one candidate; return the bound [`PullLegTarget`]. Classifies every
    /// failure into the [`PullMiss`] it is, exactly like `open_from_candidate`.
    async fn open_leg_target_from_candidate(
        &self,
        deps: &NodeOriginDeps,
        candidate: &Candidate,
        hash_bytes: [u8; 32],
        namespace_id: U256,
    ) -> Result<PullLegTarget, PullMiss> {
        let Ok(pk) = PublicKey::from_bytes(&candidate.node_id) else {
            return Err(PullMiss::Clean);
        };
        let Some(provider_addr) = deps
            .addr_resolver
            .address_of(&DhtNodeId::from_bytes(candidate.node_id))
        else {
            debug!("node-origin: pull-leg candidate has no resolvable operator address; skipping");
            return Err(PullMiss::Clean);
        };
        let ctx = match deps
            .buyer
            .open_or_reuse_pool(provider_addr, CHANNEL_OPEN_CALLER_BUDGET)
            .await
        {
            Ok(ctx) => ctx,
            Err(err) => return Err(record_pool_open_failure(deps, provider_addr, &err)),
        };
        let ctx = match bind_upstream_ctx(deps, ctx) {
            Ok(ctx) => ctx,
            Err(err) => {
                let verdict =
                    classify_pull_failure(deps, pk, provider_addr, hash_bytes, None, &err);
                return Err(PullMiss::for_verdict(verdict));
            }
        };
        let deadlines = match deps.config.deadlines() {
            Ok(deadlines) => deadlines,
            Err(err) => {
                let verdict =
                    classify_pull_failure(deps, pk, provider_addr, hash_bytes, None, &err);
                return Err(PullMiss::for_verdict(verdict));
            }
        };
        let ledger = lane_ledger(deps, provider_addr, &ctx);
        let rate_ceiling =
            effective_rate_ceiling(candidate.rate_per_mb, deps.config.max_rate_per_mb);
        let namespace_bytes = namespace_id.to_be_bytes::<32>();

        // The free header handshake: whole-tail open (`byte_offset == 0`,
        // `byte_len == 0`) to read the committed `total_bytes`, then abort — no
        // `next_chunk`, so no bytes are pulled and no voucher is paid, and the ledger
        // watermark is unchanged. The actual range-minimized pull re-opens per gap via
        // `PeerSource` on this same channel.
        let stream_guard = deps.metrics.outbound_stream_guard();
        let (header, probe) = match open_progressive_upstream(
            &deps.endpoint,
            EndpointAddr::new(pk),
            &ctx,
            Arc::clone(&ledger),
            &deps.slash_domain,
            provider_addr,
            hash_bytes,
            namespace_bytes,
            0,
            now_micros(),
            deps.config.max_blob_size_bytes,
            rate_ceiling,
            deadlines,
            0,
        )
        .await
        {
            Ok(pair) => pair,
            Err(err) => {
                drop(stream_guard);
                let verdict = classify_pull_failure(
                    deps,
                    pk,
                    provider_addr,
                    hash_bytes,
                    Some(ctx.pool_id),
                    &err,
                );
                return Err(PullMiss::for_verdict(verdict));
            }
        };
        let total_bytes = header.total_bytes;
        let _ = probe.abort();
        drop(stream_guard);

        Ok(PullLegTarget {
            pk,
            provider_addr,
            target: EndpointAddr::new(pk),
            ctx: Arc::new(std::sync::Mutex::new(ctx)),
            ledger,
            total_bytes,
            node_id: candidate.node_id,
            namespace_id: namespace_bytes,
            rate_ceiling,
            deadlines,
        })
    }
}

/// Run the range-minimized upstream pull for `[offset, offset + len)` of `hash`
/// into `engine`'s cache via [`drive`], paying only for the missing ranges and
/// pacing the pull with a [`RampPacer`] built from `credit_ramp_divisor`,
/// `credit_floor`, and `credit_max` — the same ramped credit window the serve leg
/// computes from its own paid frontier (ADR 003 §Credit window / ADR 037), so the
/// pull never runs further ahead of the downstream serve leg's paid frontier than
/// that window allows. Under coalescing that paid frontier is the shared, MAX-over-
/// live-observers `served_paid` on `session` — each attached observer's serve leg
/// advances it by `fetch_max`, so the pull is bounded by whichever observer has
/// paid furthest (DECISION-B); a solo pull is the N=1 case. Records the terminal
/// outcome via the shared [`FillSession::mark_ended`]
/// on completion; a [`SettleOnDrop`] guard persists the buyer watermark (#852) on
/// EVERY exit — clean completion, a terminal drive error, and a cooperative
/// `cancel` (the serve leg finished, so the client no longer waits: stop the
/// upstream spend, #1610).
///
/// # Off the accept task, on its own runtime
///
/// This is a FREE function, not a `NodeOrigin` method: `drive`'s future is
/// non-`Send` (its [`decdn_client_pull::IngestStore`] fill is deliberately
/// non-`Send`), which the iroh `ProtocolHandler::accept` bound forbids on
/// the serve task. So the orchestration spawns a dedicated OS thread with its OWN
/// current-thread tokio runtime and `block_on`s this. All inputs are therefore
/// OWNED + `'static` (no borrow crosses the thread): `deps_lock` is a clone of
/// [`NodeOrigin::deps_arc`], read via `get()` HERE so `&deps.endpoint` /
/// `&deps.slash_domain` are borrowed only within this runtime's scope. The shared
/// coordination state (the shared [`FillSession`]'s [`AtomicU64`] / [`Notify`]) crosses runtimes
/// safely — atomics and `Notify` wakers are runtime-agnostic — and the
/// [`CacheEngine`] store actor and iroh [`Endpoint`](iroh::Endpoint) are reached
/// through their own channels, so a second runtime talking to them is fine.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn run_pull_leg(
    deps_lock: Arc<OnceLock<NodeOriginDeps>>,
    target: PullLegTarget,
    engine: CacheEngine,
    hash: Hash,
    offset: u64,
    len: u64,
    credit_ramp_divisor: u64,
    credit_floor: u64,
    credit_max: u64,
    session: Arc<FillSession>,
    cancel: CancellationToken,
) {
    let hash_bytes = *hash.as_bytes();
    let PullLegTarget {
        pk,
        provider_addr,
        target: endpoint_target,
        ctx,
        ledger,
        total_bytes,
        node_id,
        namespace_id,
        rate_ceiling,
        deadlines,
    } = target;

    // Read the pool id + lane seed once (quick std-lock, never held across
    // await). `prior_amount` is the cumulative the lane started from, so the
    // settle-on-drop can tell whether this stream advanced the watermark past
    // its seed before persisting.
    let (pool_id, prior_amount) = {
        let guard = ctx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (guard.pool_id, guard.prior_amount)
    };

    let Some(deps) = deps_lock.get() else {
        // Unprovisioned under us (cannot happen — we discovered via deps): still
        // record a terminal outcome so the serve leg does not hang.
        session.mark_ended(Err(FillError::new(
            "node-origin pull leg lost its dependencies mid-serve",
        )));
        return;
    };

    // #852: persist the buyer watermark on EVERY exit — clean completion, a terminal
    // drive error, or a `cancel` (the serve leg finished first, e.g. client
    // disconnect / done). Held as a local so its `Drop` runs on all three, on THIS
    // pull-thread runtime.
    let _settle = SettleOnDrop {
        deps: Arc::clone(&deps_lock),
        provider_addr,
        pool_id,
        prior_amount,
        ledger: Arc::clone(&ledger),
    };

    let admit_store = NodeAdmitStore::new(engine, hash, total_bytes, Some(Arc::clone(&session)));
    // Content bytes this provider will actually serve (the gaps), for scoring —
    // held ranges are not re-pulled, so this is below `total_bytes` on an interior
    // hold.
    let gap_bytes = match admit_store.missing_ranges(offset, len).await {
        Ok(ranges) => content_len(&ranges, total_bytes),
        Err(_) => total_bytes,
    };

    let peer_source = PeerSource::new(
        &deps.endpoint,
        endpoint_target,
        Arc::clone(&ctx),
        Arc::clone(&ledger),
        &deps.slash_domain,
        provider_addr,
        namespace_id,
        deps.config.max_blob_size_bytes,
        rate_ceiling,
        deadlines,
    );
    // The ramped credit-window pacer (ADR 003 §Credit window / ADR 037): the pull
    // never runs further ahead of the downstream served-paid frontier than the
    // ramped window allows, in lockstep with the serve leg's own ramp.
    let pacer = RampPacer {
        divisor: credit_ramp_divisor,
        floor: credit_floor,
        credit_max,
    };
    let node_funder = NodeFunder::new(
        Arc::clone(&deps.buyer),
        Arc::clone(&ctx),
        Arc::clone(&ledger),
        Arc::clone(&deps.metrics),
        // The window-paced serve-miss leg does not derive a refuse-metering signal
        // from this flag; `NodeFunder` still records its own success/refusal metrics.
        Arc::new(AtomicBool::new(false)),
    );
    let config = DriveConfig {
        working_deposit: deps.config.working_deposit,
        max_settle_waits: settle_wait_budget(deps.config.event_poll_interval),
        settle_backoff: SETTLE_POLL_STEP,
    };
    let served_paid_reader = {
        let served_paid = Arc::clone(session.served_frontier());
        move || served_paid.load(Ordering::Relaxed)
    };
    let pacing_wait = ServedPaidWait {
        served_paid_advanced: Arc::clone(session.served_advanced()),
        served_paid: Arc::clone(session.served_frontier()),
        metrics: Arc::clone(&deps.metrics),
    };

    let started = Instant::now();
    // Cooperative cancellation: the serve leg finishing cancels the token. On cancel
    // the `drive` future is dropped (aborting the in-flight upstream pull), and the
    // `_settle` guard below still persists the buyer watermark (#852). No score on
    // cancel — an abandoned pull is neither a clean delivery nor a provider fault.
    let cancelled;
    let result = tokio::select! {
        biased;
        r = drive(
            &admit_store,
            &peer_source,
            &pacer,
            &node_funder,
            &ctx,
            &ledger,
            hash_bytes,
            offset,
            len,
            &config,
            None,
            Some(&pacing_wait),
            Some(&served_paid_reader),
        ) => {
            cancelled = false;
            r
        }
        () = cancel.cancelled() => {
            cancelled = true;
            Ok(())
        }
    };
    let elapsed = started.elapsed();

    // Abandon drain. On cancel the `drive` future above is dropped
    // mid-transfer, which queues an upstream `Connection::close` (via
    // `UpstreamPull::drop`) but does NOT drive it to completion. That connection's
    // QUIC driver lives on THIS pull-thread current-thread runtime, which the caller
    // (`window.rs`) drops the instant we return. Without a drain the close frame is
    // never flushed: the connection is stranded with no driver, so it can never reach
    // "closed or timed out", and the node's own `Endpoint::close()` — which iroh
    // otherwise bounds to a few seconds — would wait forever (a hang that surfaces
    // whenever the endpoint is closed while an abandoned pull is in flight). Yield the
    // runtime briefly so the abandoned connection flushes its CONNECTION_CLOSE and
    // drains on the runtime that owns it. A drive that returns `Err` strands its
    // upstream connection the SAME way a cancel does — it returns without a graceful
    // cooperative close (unlike the clean `Ok` path, which closes inside `drive`) —
    // so the drain must cover it too, or `Endpoint::close()` hangs whenever a pull
    // failed mid-serve (e.g. a corrupt upstream). Only the clean `Ok` path skips the
    // drain and stays on the hot path with no added latency.
    if cancelled || result.is_err() {
        tokio::time::sleep(ABANDON_DRAIN).await;
    }

    // Reputation + region accounting, skipped on cancel — an abandoned pull is
    // neither a clean delivery nor a provider fault. A ramp `Wait` never reaches
    // here as a terminal state: `drive` only returns once the fetch completes,
    // errors, or is cancelled, so a pacer pause is not a fault to skip scoring for.
    if !cancelled {
        match &result {
            Ok(()) => {
                record_outcome(
                    deps,
                    pk,
                    &Outcome::Delivered {
                        bytes: gap_bytes,
                        elapsed,
                    },
                );
                deps.region_accountant
                    .record_pulled(&node_id, gap_bytes)
                    .await;
            }
            Err(err) => {
                if is_bao_corruption(err) {
                    tracing::warn!(
                        provider = %pk, %provider_addr,
                        "node pull leg: upstream served bao-corrupt bytes; scoring Corruption"
                    );
                    record_outcome(deps, pk, &Outcome::Corruption);
                    deps.metrics.node_pull_through_upstream_verify_failed();
                } else {
                    let _ = classify_pull_failure(
                        deps,
                        pk,
                        provider_addr,
                        hash_bytes,
                        Some(pool_id),
                        err,
                    );
                }
            }
        }
    }

    // Record the terminal outcome so the serve leg can decide a gap it is waiting on
    // (`mark_ended` sets the outcome then wakes waiters). On cancel the serve leg has
    // already finished and nobody reads this, but set it regardless. `anyhow::Error`
    // is not `Clone`, so flatten it into a `FillError` message.
    session.mark_ended(result.map_err(|e| FillError::new(format!("{e:#}"))));
    // `_settle` drops here, persisting the buyer watermark (#852).
}

// ===========================================================================
// The UNPAID own-origin twin of the pull leg.
// ===========================================================================

/// The [`Funder`] for the UNPAID local leg. There is no channel to fund, so it
/// permits zero reactive top-ups and its [`Funder::top_up`] is unreachable.
///
/// Rate 0 keeps `next_voucher_cost` at 0 and `DriveConfig::working_deposit` is
/// [`U256::ZERO`], so the pacer's exhaustion arm never fires and never returns a
/// top-up decision — the only thing that would call `top_up`. It errs
/// defensively (rather than escrow anything, which it could not do anyway) so a
/// hypothetical future regression that reached it fails loudly instead of hanging.
#[allow(dead_code, reason = "wired by the own-origin serve-miss orchestration")]
struct NullFunder;

impl Funder for NullFunder {
    fn max_topups(&self) -> u32 {
        0
    }

    fn top_up(&self, _additional: U256) -> SourceFuture<'_, DepositOutcome> {
        Box::pin(async {
            Err(anyhow::anyhow!(
                "local own-origin pull leg has no channel to top up \
                 (unreachable: rate 0, working_deposit 0)"
            ))
        })
    }
}

/// Build the benign LOCAL [`PoolContext`] the driver carries as pure
/// bookkeeping for the unpaid leg (THE CRUX).
///
/// It signs NOTHING: the [`BackendSource`] quotes rate 0, so `drive` never prices,
/// issues, or sends a voucher, and the throwaway signer is never touched. The
/// large `deposit` keeps the pacer's `remaining_deposit` (`deposit −
/// committed.amount`, and `committed.amount` stays 0 at rate 0) permanently above
/// `next_voucher_cost` (also 0), so [`crate::pacer` `BudgetPacer`] never reaches
/// its exhaustion/top-up arm. Fresh priors — there is no prior pool state to
/// resume. `U256::MAX` is used, not a merely-large value, so no blob size can ever
/// bring the gap headroom below the (zero) voucher cost.
#[allow(dead_code, reason = "wired by the own-origin serve-miss orchestration")]
fn local_bookkeeping_ctx() -> PoolContext {
    PoolContext {
        pool_id: B256::ZERO,
        // No provider is paid: rate 0 means no voucher is ever signed, so the
        // ZERO-provider signing guard is never reached on this local leg.
        provider: Address::ZERO,
        deposit: U256::MAX,
        client_signer: Arc::new(PrivateKeySigner::random()),
        // Chain 0 / zero contract: this domain is never used to sign, because rate
        // 0 means no voucher is ever produced. It exists only to satisfy the struct.
        voucher_domain: decdn_incentive::voucher_domain(0, Address::ZERO),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
        capability: None,
    }
}

/// Run the range-minimized OWN-ORIGIN pull for `[offset, offset + len)` of `hash`
/// into `engine`'s cache via [`drive`] over an UNPAID [`BackendSource`], pacing the
/// pull with the same [`RampPacer`] the paid leg uses, built from
/// `credit_ramp_divisor`, `credit_floor`, and `credit_max` (ADR 003 §Credit window
/// / ADR 037). Records the terminal outcome via the shared [`FillSession::mark_ended`]. The
/// local twin of [`run_pull_leg`].
///
/// # What drops out relative to the paid [`run_pull_leg`]
///
/// This leg pulls from THIS node's own configured origin, reached through
/// [`CacheEngine::origin_encode_range`] behind the [`BackendSource`]. There is no
/// counterparty, so every paid-path axis is absent — and each absence is load-bearing,
/// not an omission:
///
/// - **No discovery / channel open / [`PeerSource`] / [`NodeFunder`].** The bytes
///   are already reachable locally, so there is nothing to dial, no channel to open,
///   and nothing to pay. The source is handed in by the orchestration, already built.
/// - **No provider scoring, no region accounting.** There is no provider and no
///   remote region: a fault here is OUR own origin, never a peer to score or a
///   region to credit. On a [`drive`] error we meter it as a LOCAL fault
///   ([`crate::metrics::Metrics::node_pull_local_fault`]) and NEVER touch reputation.
/// - **No [`SettleOnDrop`].** That guard persists a BUYER voucher watermark (#852);
///   this leg issues no vouchers, so there is nothing to settle.
///
/// # Why the driver still needs a "ledger" — THE CRUX
///
/// [`drive`]'s per-gap completion is paid-frontier gated: a gap is `Done` only once
/// the ledger's committed `bytes` reach the gap end. An unpaid source that never
/// advanced a ledger would leave that frontier at zero and the gap loop would
/// re-draw forever. So the [`BackendSource`] carries a LOCAL bookkeeping
/// [`PoolLedger`] and, on `finish`, advances its `bytes` by exactly the leg's
/// drained wire (at amount 0). We hand `drive` that SAME ledger ([`BackendSource::ledger`])
/// plus a benign [`local_bookkeeping_ctx`] and a [`NullFunder`], so the completion
/// counter the source moves is the one the gap loop reads. This is NOT payment — no
/// channel, no voucher, no chain, no counterparty; see the [`BackendSource`] module
/// docs.
///
/// The downstream [`RampPacer`] is KEPT (bound on `served_paid`): the pull still
/// never runs further ahead of the real downstream client's paid frontier than the
/// ramped credit window allows (#1610 — ingest only behind a waiting, paying client
/// — and the storage/egress exposure bound).
///
/// # Off the accept task, on its own runtime
///
/// Like [`run_pull_leg`], `drive`'s future is non-`Send`, so the orchestration
/// `block_on`s this on a dedicated current-thread runtime. All inputs are
/// therefore owned + `'static`; the shared coordination state
/// (the shared [`FillSession`]'s [`AtomicU64`] / [`Notify`]) crosses runtimes safely.
#[allow(
    clippy::too_many_arguments,
    dead_code,
    reason = "wired by the own-origin serve-miss orchestration"
)]
pub(crate) async fn run_local_pull_leg(
    metrics: Arc<crate::metrics::Metrics>,
    engine: CacheEngine,
    source: BackendSource,
    hash: Hash,
    offset: u64,
    len: u64,
    credit_ramp_divisor: u64,
    credit_floor: u64,
    credit_max: u64,
    total_bytes: u64,
    session: Arc<FillSession>,
    cancel: CancellationToken,
) {
    let hash_bytes = *hash.as_bytes();

    let admit_store = NodeAdmitStore::new(engine, hash, total_bytes, Some(Arc::clone(&session)));

    // The LOCAL bookkeeping axes (THE CRUX). The `ledger` is the SAME `Arc` the
    // source advances on `finish`, so the paid-frontier the gap loop reads for
    // completion tracks the wire this leg actually drained. The `ctx` is a benign
    // large-deposit / throwaway-signer context (never used to sign, rate 0), and the
    // funder is a no-op — there is no channel to top up.
    let ledger = source.ledger();
    let ctx = Arc::new(std::sync::Mutex::new(local_bookkeeping_ctx()));
    let null_funder = NullFunder;

    // The ramped credit-window pacer (ADR 003 §Credit window / ADR 037), IDENTICAL
    // to the paid leg's: bound on `served_paid` (#1610 + storage/egress exposure),
    // so the unpaid pull is still throttled to the real client's paid frontier.
    let pacer = RampPacer {
        divisor: credit_ramp_divisor,
        floor: credit_floor,
        credit_max,
    };
    // `working_deposit == ZERO` disables the pacer's reactive top-up arm entirely, so
    // the settle-wait budget is inert here; keep the smallest sane values.
    let config = DriveConfig {
        working_deposit: U256::ZERO,
        max_settle_waits: 0,
        settle_backoff: SETTLE_POLL_STEP,
    };
    let served_paid_reader = {
        let served_paid = Arc::clone(session.served_frontier());
        move || served_paid.load(Ordering::Relaxed)
    };
    let pacing_wait = ServedPaidWait {
        served_paid_advanced: Arc::clone(session.served_advanced()),
        served_paid: Arc::clone(session.served_frontier()),
        metrics: Arc::clone(&metrics),
    };

    // Cooperative cancellation exactly as the paid leg: the serve leg finishing
    // cancels the token, dropping the `drive` future. There is no provider to score
    // and no watermark to persist, so a cancel simply stops the local pull.
    let cancelled;
    let result = tokio::select! {
        biased;
        r = drive(
            &admit_store,
            &source,
            &pacer,
            &null_funder,
            &ctx,
            &ledger,
            hash_bytes,
            offset,
            len,
            &config,
            None,
            Some(&pacing_wait),
            Some(&served_paid_reader),
        ) => {
            cancelled = false;
            r
        }
        () = cancel.cancelled() => {
            cancelled = true;
            Ok(())
        }
    };

    // Abandon drain, for the same reason as the paid leg: a cancelled or
    // errored `drive` returns without a graceful cooperative close, and the
    // orchestration drops this pull-thread runtime the instant we return. There is no
    // UPSTREAM iroh connection here (the origin fetch is an HTTP/S3/fs call inside the
    // cache engine, which does not strand a QUIC driver on this runtime), so the drain
    // is strictly a belt-and-braces yield; keep it identical to the paid twin so the
    // two teardown shapes do not drift. Only the clean `Ok` path skips it.
    if cancelled || result.is_err() {
        tokio::time::sleep(ABANDON_DRAIN).await;
    }

    // Classify a terminal error. There is no upstream, so a fault is ALWAYS local
    // (our own origin is corrupt/misconfigured, or a transport fault reaching it):
    // meter it as a local fault and NEVER score a provider or a bao-corruption against
    // an upstream that does not exist. Skipped on cancel (nobody waits).
    if !cancelled && let Err(err) = &result {
        metrics.node_pull_local_fault();
        tracing::warn!(
            %hash,
            error = %err,
            "own-origin pull leg failed; local-origin fault (no upstream to score)"
        );
    }

    // Record the terminal outcome so the serve leg can decide a gap it is waiting on
    // (`mark_ended` sets the outcome then wakes waiters). `anyhow::Error` is not
    // `Clone`, so flatten it into a `FillError` message.
    session.mark_ended(result.map_err(|e| FillError::new(format!("{e:#}"))));
}

#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "tests"
)]
#[cfg(test)]
mod local_pull_leg_tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use decdn_bao_range::IROH_BLOCK_SIZE;
    use decdn_cache::{
        CacheEngine, FillError, FillSession, Hash, Origin, OriginFetch, OriginKind,
        OriginPullError, OriginRangeFetch, OriginRangeRequest, OutboardFetch,
    };
    use decdn_client_pull::{Cumulative, PoolLedger};
    use tokio_util::sync::CancellationToken;

    use super::BackendSource;
    use super::run_local_pull_leg;

    /// What a [`FakeOrigin`] does when its range is fetched.
    #[derive(Clone, Copy)]
    enum Mode {
        /// Serve the genuine bytes for `H` — a healthy own origin.
        Serve,
        /// Return a transport error from `fetch_range` — an origin the node cannot
        /// reach (the no-hang-on-fault case).
        Fault,
        /// Serve length-matching bytes that do NOT hash to `H` — a
        /// corrupt/misconfigured own origin (the local-verify case).
        Corrupt,
    }

    /// A minimal own-origin double serving one blob's aligned ranges + its
    /// `{H}.obao4` outboard, parameterized by [`Mode`]. Twin of the `FakeOrigin` in
    /// `backend_source.rs`'s tests (kept local — test doubles don't cross module
    /// test boundaries).
    #[derive(Debug)]
    struct FakeOrigin {
        hash: Hash,
        data: Bytes,
        outboard: Bytes,
        size: u64,
        mode: FakeMode,
    }

    // A non-Copy Debug shim so `FakeOrigin` can derive `Debug` (Mode is internal).
    #[derive(Debug, Clone, Copy)]
    enum FakeMode {
        Serve,
        Fault,
    }

    impl FakeOrigin {
        fn new(hash: Hash, data: &[u8], outboard: Bytes, mode: Mode) -> Self {
            // `Corrupt` is expressed by feeding mismatched `data` under `Serve`; only
            // `Fault` needs distinct fetch behaviour, so the stored mode is binary.
            let fake_mode = match mode {
                Mode::Serve | Mode::Corrupt => FakeMode::Serve,
                Mode::Fault => FakeMode::Fault,
            };
            Self {
                hash,
                data: Bytes::from(data.to_vec()),
                outboard,
                size: data.len() as u64,
                mode: fake_mode,
            }
        }
    }

    impl Origin for FakeOrigin {
        fn kind(&self) -> OriginKind {
            OriginKind::Http
        }

        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>>
        {
            let result = if hash == self.hash {
                Ok(OriginFetch::found_one_shot(self.data.clone()))
            } else {
                Ok(OriginFetch::NotFound)
            };
            Box::pin(async move { result })
        }

        fn size(
            &self,
            hash: Hash,
        ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, OriginPullError>> + Send + '_>>
        {
            let out = (hash == self.hash).then_some(self.size);
            Box::pin(async move { Ok(out) })
        }

        fn fetch_outboard(
            &self,
            hash: Hash,
            _outboard_max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>>
        {
            let result = if hash == self.hash {
                OutboardFetch::Found(self.outboard.clone())
            } else {
                OutboardFetch::NotFound
            };
            Box::pin(async move { Ok(result) })
        }

        fn fetch_range(
            &self,
            hash: Hash,
            req: OriginRangeRequest,
            _outboard_max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>>
        {
            if matches!(self.mode, FakeMode::Fault) {
                return Box::pin(async {
                    Err(OriginPullError::Permanent(anyhow::anyhow!(
                        "simulated own-origin transport fault"
                    )))
                });
            }
            let result = if hash == self.hash {
                let s = req.fetch_start as usize;
                let e = req.fetch_end as usize;
                match self.data.get(s..e) {
                    Some(span) => OriginRangeFetch::Ranged {
                        data: Bytes::copy_from_slice(span),
                        outboard: self.outboard.clone(),
                    },
                    None => OriginRangeFetch::NotFound,
                }
            } else {
                OriginRangeFetch::Unsupported
            };
            Box::pin(async move { Ok(result) })
        }
    }

    /// A blob spanning several chunk groups plus a partial final group, so the bao
    /// tree has real interior nodes.
    fn test_blob() -> Vec<u8> {
        let size = 5 * decdn_cache::CHUNK_GROUP_BYTES as usize + 123;
        (0..size).map(|i| (i % 251) as u8).collect()
    }

    fn fresh_ledger() -> Arc<PoolLedger> {
        Arc::new(PoolLedger::new(Cumulative::default()))
    }

    /// Build an engine over one `FakeOrigin` in `mode`, plus the root/outboard/total
    /// for the genuine blob. In `Corrupt` mode the origin serves `corrupt` bytes
    /// (length-matched, different content) under the genuine `H`.
    async fn engine_with_origin(
        mode: Mode,
    ) -> anyhow::Result<(CacheEngine, [u8; 32], u64, tempfile::TempDir)> {
        let data = test_blob();
        let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data.clone());
        let hash = Hash::from(root);
        let total = data.len() as u64;

        let served: Vec<u8> = match mode {
            Mode::Serve | Mode::Fault => data,
            Mode::Corrupt => data.iter().map(|b| b ^ 0xFF).collect(),
        };
        let origin = FakeOrigin::new(hash, &served, outboard, mode);
        let tmp = tempfile::tempdir()?;
        let engine =
            CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;
        Ok((engine, root, total, tmp))
    }

    /// Drive `run_local_pull_leg` to termination under a hard timeout — a hang (the
    /// failure mode THE CRUX must rule out) surfaces as the timeout error rather
    /// than wedging the test runner. `served_paid` is pre-advanced to `total` so the
    /// downstream `RampPacer` never gates the pull (this test exercises the
    /// completion path, not the window).
    ///
    /// Returns the recorded `pull_result`. The leg sets `pull_result`
    /// UNCONDITIONALLY on the line immediately before it fires `pull_ended` and
    /// returns, so — because we await the leg to full completion — a `Some`
    /// `pull_result` is the non-racy proof that the leg both terminated and fired
    /// `pull_ended` (a fresh `notified()` here would miss the already-sent
    /// `notify_waiters`, which stores no permit, so we do not watch the notify).
    async fn run_to_termination(
        engine: &CacheEngine,
        root: [u8; 32],
        total: u64,
    ) -> anyhow::Result<Option<Result<(), FillError>>> {
        let hash = Hash::from(root);
        let source = BackendSource::new(engine.clone(), root, total, fresh_ledger());
        let session = FillSession::new(bao_tree::blake3::Hash::from(root), total);
        // Pre-advance the served frontier to `total` so the downstream `RampPacer`
        // never gates the pull (this test exercises the completion path, not the
        // window).
        session.served_frontier().store(total, Ordering::Relaxed);

        let cancel = CancellationToken::new();
        let metrics = Arc::new(crate::metrics::Metrics::new());
        // A floor comfortably larger than the blob: `ramped_credit_window` never
        // returns below `floor`, so with `served_paid == total` the pull never waits,
        // regardless of divisor/credit_max — this only has to admit the whole gap.
        let credit_floor = total.saturating_mul(4).max(decdn_cache::CHUNK_GROUP_BYTES);

        tokio::time::timeout(
            Duration::from_secs(45),
            run_local_pull_leg(
                metrics,
                engine.clone(),
                source,
                hash,
                0,
                0,
                2,
                credit_floor,
                credit_floor,
                total,
                Arc::clone(&session),
                cancel,
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("run_local_pull_leg HUNG — the CRUX failed to terminate"))?;

        // The leg records its terminal outcome unconditionally on its single exit, and
        // we awaited it to completion, so `outcome()` is the non-racy proof it ran.
        Ok(session.outcome())
    }

    /// (a) THE CRUX: a full-miss whole-blob local pull TERMINATES, fills the cache
    /// byte-exact, and records `pull_result == Some(Ok(()))`.
    #[tokio::test]
    async fn local_pull_leg_full_miss_terminates_and_fills() -> anyhow::Result<()> {
        let (engine, root, total, _tmp) = engine_with_origin(Mode::Serve).await?;
        let hash = Hash::from(root);

        let result = run_to_termination(&engine, root, total)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("pull_result must be recorded (leg fired pull_ended)")
            })?;
        result.map_err(|e| anyhow::anyhow!("expected Ok, got Err: {e}"))?;

        // The cache now holds the whole blob, byte-exact.
        let want = test_blob();
        assert_eq!(
            engine.get(hash).await?.as_ref(),
            want.as_slice(),
            "the local pull must fill the cache byte-exact"
        );
        Ok(())
    }

    /// (b) A transport fault reaching the own origin records `pull_result ==
    /// Some(Err(_))` and does NOT hang.
    #[tokio::test]
    async fn local_pull_leg_origin_fault_fails_without_hang() -> anyhow::Result<()> {
        let (engine, root, total, _tmp) = engine_with_origin(Mode::Fault).await?;

        let result = run_to_termination(&engine, root, total)
            .await?
            .ok_or_else(|| anyhow::anyhow!("pull_result must be recorded even on fault"))?;
        assert!(
            result.is_err(),
            "an origin transport fault must terminate the leg with Err"
        );
        Ok(())
    }

    /// (c) A corrupt own origin (bytes don't hash to `H`) terminates with `Err` —
    /// classified LOCAL (there is no upstream to score) — and does NOT hang.
    #[tokio::test]
    async fn local_pull_leg_corrupt_origin_fails_local_without_hang() -> anyhow::Result<()> {
        let (engine, root, total, _tmp) = engine_with_origin(Mode::Corrupt).await?;

        let result = run_to_termination(&engine, root, total)
            .await?
            .ok_or_else(|| anyhow::anyhow!("pull_result must be recorded even on corruption"))?;
        assert!(
            result.is_err(),
            "a corrupt own origin must terminate the leg with a local Err"
        );
        Ok(())
    }
}

/// Regression coverage for the window-pause lost-wakeup that wedged
/// [`run_pull_leg`] / [`run_local_pull_leg`] under CI scheduling gaps (#1673).
/// [`ServedPaidWait`] is edge-triggered on a [`Notify`], which stores no permit
/// across `notify_waiters`, so a serve-leg advance that races the pacer's `Wait`
/// decision must be caught by re-reading the frontier AFTER arming the waiter — not
/// waited on forever.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod served_paid_wait_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use tokio::sync::Notify;

    use super::ServedPaidWait;
    use crate::metrics::Metrics;
    use decdn_client_pull::PacingWait;

    fn wait_hook(advanced: &Arc<Notify>, frontier: &Arc<AtomicU64>) -> ServedPaidWait {
        ServedPaidWait {
            served_paid_advanced: Arc::clone(advanced),
            served_paid: Arc::clone(frontier),
            metrics: Arc::new(Metrics::new()),
        }
    }

    /// The #1673 race: the serve leg advances the frontier and fires
    /// `notify_waiters()` in the gap between the pacer reading `observed` and the
    /// pull parking. The notify wakes nobody (no waiter registered, no permit
    /// stored). `wait` must re-read the frontier after arming and return at once;
    /// an edge-triggered wait wedges here forever.
    #[tokio::test]
    async fn a_racing_advance_before_the_park_is_not_lost() {
        let advanced = Arc::new(Notify::new());
        let frontier = Arc::new(AtomicU64::new(0));
        let hook = wait_hook(&advanced, &frontier);

        // The advance + notify land BEFORE `wait` is polled — the lost-wakeup window.
        frontier.store(64 * 1024, Ordering::Relaxed);
        advanced.notify_waiters();

        tokio::time::timeout(Duration::from_secs(5), hook.wait(0))
            .await
            .expect("wait must observe the raced advance, not wedge on a lost notify");
    }

    /// The ordinary path still parks and wakes: with no advance yet, `wait` blocks,
    /// then resolves on a later `notify_waiters()` from the serve leg.
    #[tokio::test]
    async fn a_later_advance_wakes_the_parked_wait() {
        let advanced = Arc::new(Notify::new());
        let frontier = Arc::new(AtomicU64::new(0));
        let hook = wait_hook(&advanced, &frontier);

        let advance = {
            let advanced = Arc::clone(&advanced);
            let frontier = Arc::clone(&frontier);
            async move {
                // Let `wait` arm + park first, then advance and notify.
                tokio::task::yield_now().await;
                frontier.store(64 * 1024, Ordering::Relaxed);
                advanced.notify_waiters();
            }
        };
        tokio::join!(
            async {
                tokio::time::timeout(Duration::from_secs(5), hook.wait(0))
                    .await
                    .expect("a later advance must wake the parked wait");
            },
            advance,
        );
    }
}
