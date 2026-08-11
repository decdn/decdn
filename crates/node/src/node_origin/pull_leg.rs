//! The gap-driven, range-minimized **pull leg** of the node serve-miss (#1621 B2
//! part 2, ADR 037).
//!
//! The fused `window_forward_loop` (retained for the local-outboard twin) pulls
//! the WHOLE blob and tees it. This module instead drives the shared
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
//!   here; the pull leg does not re-discover. Open-time candidate fallback is
//!   preserved (walk the ranked candidates until one opens); mid-pull candidate
//!   switch is deferred, consistent with the resumable-pull design (#1530).
//! - [`run_pull_leg`] — builds the driver axes ([`NodeAdmitStore`] sink,
//!   [`PeerSource`], [`WindowPacer`], [`NodeFunder`]) and runs the drive, then scores
//!   the provider and records its terminal outcome into the shared `pull_result`
//!   before firing `pull_ended`. A [`SettleOnDrop`] guard persists the buyer
//!   watermark (#852) on EVERY exit — including a mid-drive cancellation when the
//!   serve leg finishes first and drops this future (client disconnect / shutdown).
//!
//! # Reputation, region accounting, watermark — re-homed from `NodeProgressivePull`
//!
//! The fused path's scoring lived on [`super::NodeProgressivePull`]
//! (`finish(TeeVerdict)` → `Delivered`/`Corruption`, `SettleOnDrop`, region
//! accounting). The gap-driven `PeerSource`/`drive` path bypasses that type, so this
//! module re-homes the same three concerns explicitly (accepted duplication for B2;
//! unifying the two is a later concern): the [`SettleOnDrop`] guard for #852, a
//! post-drive `record_outcome` scoring the discovered provider, and
//! `region_accountant.record_pulled` on the clean path.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use alloy::primitives::U256;
use bao_tree::ChunkRanges;
use decdn_bao_range::RangedStore;
use decdn_cache::{CacheEngine, CacheError, Hash};
use decdn_client_pull::driver::DriveConfig;
use decdn_client_pull::{
    HashMismatch as ClientPullHashMismatch, PacingWait, PeerSource, WindowPacer, drive,
};
use decdn_reputation::Outcome;
use iroh::{EndpointAddr, PublicKey};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::admit_store::NodeAdmitStore;
use super::funder::NodeFunder;
use super::resume::{SETTLE_POLL_STEP, settle_wait_budget};
use super::{
    NodeOrigin, NodeOriginDeps, PullMiss, PullOutcome, SettleDeps, SettleOnDrop, bind_upstream_ctx,
    cached_candidates, channel_ledger, classify_pull_failure, discover, now_micros, probe_and_rank,
    record_channel_open_failure, record_outcome,
};
use crate::client_requester::{
    ChannelContext, ChannelLedger, PullDeadlines, effective_rate_ceiling,
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
const ABANDON_DRAIN: Duration = Duration::from_secs(3);

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
    ctx: Arc<std::sync::Mutex<ChannelContext>>,
    /// The channel's shared voucher ledger (`BuyerLedgers::get_or_seed`).
    ledger: Arc<ChannelLedger>,
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

/// The injected wait for [`WindowPacer`]'s `Wait`: resolve once the serve leg's paid
/// frontier advances. Awaits the shared `served_paid_advanced` notify so a parked
/// pull re-decides exactly when a downstream voucher clears.
struct ServedPaidWait {
    served_paid_advanced: Arc<Notify>,
    /// Bumps `node_pull_through_window_paused` on each pause — the pull hit its ADR
    /// 037 window and is waiting for downstream payment to clear. The fused
    /// `window_forward_loop` bumped this at the same point; re-emitted here so the
    /// pause is still observable now that the pause lives in the pull leg.
    metrics: Arc<crate::metrics::Metrics>,
}

impl PacingWait for ServedPaidWait {
    fn wait(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.metrics.node_pull_through_window_paused();
        Box::pin(async move { self.served_paid_advanced.notified().await })
    }
}

/// Total content bytes a chunk-unit [`ChunkRanges`] covers, clamped to `total`.
fn content_len(ranges: &ChunkRanges, total: u64) -> u64 {
    let boundaries = ranges.boundaries();
    let mut it = boundaries.iter();
    let mut sum = 0u64;
    while let (Some(a), Some(b)) = (it.next(), it.next()) {
        let start = a.0.saturating_mul(CHUNK_BYTES).min(total);
        let end = b.0.saturating_mul(CHUNK_BYTES).min(total);
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
    /// Mirrors [`Self::open_progressive_pull`]'s cached-first discover → probe → rank
    /// pipeline and its open-time candidate fallback, but stops at channel-open +
    /// header instead of returning a live fused pull.
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
            .open_or_reuse_channel(
                provider_addr,
                deps.config.deposit_hint,
                CHANNEL_OPEN_CALLER_BUDGET,
            )
            .await
        {
            Ok(ctx) => ctx,
            Err(err) => return Err(record_channel_open_failure(deps, provider_addr, &err)),
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
        let ledger = channel_ledger(deps, provider_addr, &ctx);
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
                    Some(ctx.channel_id),
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
/// pacing the pull to within `window` of the downstream serve leg's paid frontier
/// (ADR 037). Records the terminal outcome into `pull_result` and fires `pull_ended`
/// on completion; a [`SettleOnDrop`] guard persists the buyer watermark (#852) on
/// EVERY exit — clean completion, a terminal drive error, and a cooperative
/// `cancel` (the serve leg finished, so the client no longer waits: stop the
/// upstream spend, #1610).
///
/// # Off the accept task, on its own runtime (#1621 B2 part 2, Strategy B)
///
/// This is a FREE function, not a `NodeOrigin` method: `drive`'s future is
/// non-`Send` (its [`decdn_client_pull::IngestStore`] fill is deliberately
/// non-`Send`, Task 6), which the iroh `ProtocolHandler::accept` bound forbids on
/// the serve task. So the orchestration spawns a dedicated OS thread with its OWN
/// current-thread tokio runtime and `block_on`s this. All inputs are therefore
/// OWNED + `'static` (no borrow crosses the thread): `deps_lock` is a clone of
/// [`NodeOrigin::deps_arc`], read via `get()` HERE so `&deps.endpoint` /
/// `&deps.slash_domain` are borrowed only within this runtime's scope. The shared
/// coordination state ([`AtomicU64`] / [`Notify`] / [`StdMutex`]) crosses runtimes
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
    window: u64,
    served_paid: Arc<AtomicU64>,
    served_paid_advanced: Arc<Notify>,
    pull_ended: Arc<Notify>,
    pull_result: Arc<StdMutex<Option<anyhow::Result<()>>>>,
    outboard_writer: super::serve_outboard::OutboardWriter,
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

    // Read the channel identifiers once (quick std-lock, never held across await).
    let (channel_id, _token) = {
        let guard = ctx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (guard.channel_id, guard.token)
    };
    let prior_nonce = ctx
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .prior_nonce;

    let Some(deps) = deps_lock.get() else {
        // Unprovisioned under us (cannot happen — we discovered via deps): still
        // record a terminal outcome so the serve leg does not hang.
        {
            let mut guard = pull_result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard = Some(Err(anyhow::anyhow!(
                "node-origin pull leg lost its dependencies mid-serve"
            )));
        }
        pull_ended.notify_waiters();
        return;
    };

    // #852: persist the buyer watermark on EVERY exit — clean completion, a terminal
    // drive error, or a `cancel` (the serve leg finished first, e.g. client
    // disconnect / done). Held as a local so its `Drop` runs on all three, on THIS
    // pull-thread runtime. Mirrors `NodeProgressivePull`'s `SettleOnDrop`.
    let _settle = SettleOnDrop {
        deps: SettleDeps::Shared(Arc::clone(&deps_lock)),
        provider_addr,
        channel_id,
        prior_nonce,
        ledger: Arc::clone(&ledger),
    };

    let admit_store = NodeAdmitStore::new(engine, hash, total_bytes, Some(outboard_writer));
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
    let window_pacer = WindowPacer::new(window);
    let node_funder = NodeFunder::new(
        Arc::clone(&deps.buyer),
        provider_addr,
        Arc::clone(&ctx),
        deps.config.reactive_topup_min_ttl,
        Arc::new(crate::payment_settlement::unix_now),
    );
    let config = DriveConfig {
        working_deposit: deps.config.working_deposit,
        max_settle_waits: settle_wait_budget(deps.config.event_poll_interval),
        settle_backoff: SETTLE_POLL_STEP,
    };
    let served_paid_reader = {
        let served_paid = Arc::clone(&served_paid);
        move || served_paid.load(Ordering::Relaxed)
    };
    let pacing_wait = ServedPaidWait {
        served_paid_advanced: Arc::clone(&served_paid_advanced),
        metrics: Arc::clone(&deps.metrics),
    };

    let started = Instant::now();
    // Cooperative cancellation: the serve leg finishing cancels the token. On cancel
    // the `drive` future is dropped (aborting the in-flight upstream pull), and the
    // `_settle` guard below still persists the buyer watermark (#852). No score on
    // cancel — an abandoned pull is neither a clean delivery nor a provider fault
    // (mirrors `NodeProgressivePull::abandon(None)`).
    let cancelled;
    let result = tokio::select! {
        biased;
        r = drive(
            &admit_store,
            &peer_source,
            &window_pacer,
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

    // Abandon drain (#1621 B2). On cancel the `drive` future above is dropped
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

    // Re-homed reputation + region accounting (from `NodeProgressivePull::finish`),
    // skipped on cancel.
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
                        Some(channel_id),
                        err,
                    );
                }
            }
        }
    }

    // Record the terminal outcome BEFORE firing `pull_ended` (the serve leg reads
    // `pull_result` after the notify to decide a gap it is waiting on). On cancel the
    // serve leg has already finished and nobody reads this, but set it regardless.
    {
        let mut guard = pull_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(result);
    }
    pull_ended.notify_waiters();
    // `_settle` drops here, persisting the buyer watermark (#852).
}
