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
//! - [`NodeOrigin::open_pull_leg`] — discovery + probe + rank + a free header
//!   handshake. Returns the ranked [`PullLegTarget`] (the upstream `total_bytes`
//!   plus the ranked candidates, each with its probe-fresh range-keyed coverage),
//!   so the orchestration can sign its `StreamResponse` before either leg streams a
//!   byte. Discovery happens ONCE here; the pull leg does not re-discover. Any
//!   holder — partial or whole — reports the same `total_bytes`, so the handshake
//!   walks candidates until one answers.
//! - [`run_pull_leg`] — assembles the blob across the partial holders via the
//!   ranged-drive loop (#1506): it plans the missing range into runs by coverage
//!   ([`decdn_client_pull::plan_covered_runs`]) and drives them in offset order,
//!   opening ONE payment lane ([`NodeAdmitStore`] sink, [`PeerSource`],
//!   [`RampPacer`], [`NodeFunder`]) per run and re-planning a non-terminal run
//!   fault onto the survivors. It scores each provider as its run ends and records
//!   the assembly's terminal outcome via the shared
//!   [`decdn_cache::FillSession::mark_ended`]. A per-lane [`SettleOnDrop`] guard
//!   persists that lane's buyer watermark (#852) on EVERY exit — including a
//!   mid-drive cancellation when the serve leg finishes first and drops this future
//!   (client disconnect / shutdown).
//!
//! # Reputation, watermark
//!
//! The pull leg handles two concerns explicitly around the `drive`: the
//! [`SettleOnDrop`] guard for #852, and a post-drive `record_outcome` scoring
//! the discovered provider on the clean path.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use bao_tree::ChunkRanges;
use decdn_bao_range::RangedStore;
use decdn_cache::{CacheEngine, CacheError, DownstreamWatch, FillError, FillSession, Hash};
use decdn_client_pull::driver::DriveConfig;
use decdn_client_pull::source::{Funder, SourceFuture};
use decdn_client_pull::{
    CoveredRun, DownstreamFrontier, HashMismatch as ClientPullHashMismatch, PacingWait, PeerSource,
    PoolExhausted, RampPacer, RetryDisposition, SharedPool, drive, retry_disposition,
};
use decdn_incentive::DepositOutcome;

use decdn_reputation::Outcome;
use iroh::{EndpointAddr, PublicKey};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::abandon_drain::{ConnDrain, as_observer, drain_abandoned};
use super::admit_store::NodeAdmitStore;
use super::backend_source::BackendSource;
use super::funder::NodeFunder;
use super::funder::{SETTLE_POLL_STEP, settle_wait_budget};
use super::ranged_pull::{AssembleOutcome, RunOutcome, RunSink, assemble};
use super::{
    EconGate, NodeOrigin, NodeOriginDeps, ProbeGather, PullMiss, PullOutcome, SettleOnDrop,
    bind_upstream_ctx, cached_candidates, classify_pull_failure, discover, economic_ceiling,
    heat_of, lane_ledger, mb_of, now_micros, probe_and_rank, record_outcome,
    record_pool_open_failure,
};
use crate::client_requester::{
    PoolContext, PullDeadlines, open_progressive_pull as open_progressive_upstream,
};
use crate::dht::negative_cache::Hash as DhtHash;
use crate::dht::routing::NodeId as DhtNodeId;
use crate::selection::{CHANNEL_OPEN_CALLER_BUDGET, Candidate, MAX_PROVIDER_ATTEMPTS};

/// Bytes per [`bao_tree::ChunkNum`] — a 1 KiB bao chunk. A chunk-range's byte span
/// is its boundaries scaled by this (twin of the driver's private constant).
const CHUNK_BYTES: u64 = 1024;

/// A discovered, ranked pull plan for one serve-miss: the `total_bytes` the caller
/// needs to sign its `StreamResponse`, plus the RANKED candidate list (each
/// carrying its probe-fresh range-keyed `coverage`, #1506) the ranged-drive loop
/// assembles the blob from. Discovery + probe + rank happen ONCE, here, and the
/// result is handed to [`run_pull_leg`], which opens a lane per planned run — a
/// blob spread across partial holders is pulled from each in turn.
pub(crate) struct PullLegTarget {
    /// The upstream-claimed content length, from the header handshake against the
    /// first openable candidate. Any holder — partial or whole — reports the same
    /// blob geometry, so it is authoritative regardless of which candidate answered.
    pub(crate) total_bytes: u64,
    /// The served client's namespace, threaded onto every lane (ADR 005), big-endian.
    namespace_id: [u8; 32],
    /// The ranked candidates (best-first), each with its probe-confirmed
    /// `coverage`. The loop plans the missing range across these
    /// ([`decdn_client_pull::plan_covered_runs`]) and opens one payment lane per
    /// run.
    candidates: Vec<Candidate>,
}

/// The injected wait for [`RampPacer`]'s `Wait`: resolve once the serve leg's paid
/// frontier or demand frontier moves past what the decision read
/// ([`DownstreamWatch::past`], which owns the #1673 arm-then-recheck). Also the
/// reader `drive` paces against, so the decision and the wait always read the same
/// frontiers.
struct DownstreamWait {
    /// The session's downstream frontiers.
    watch: DownstreamWatch,
    /// Bumps `node_pull_through_window_paused` on each pause — the pull hit its ADR
    /// 037 window and is waiting for downstream payment to clear or a serve leg to
    /// park at its frontier.
    metrics: Arc<crate::metrics::Metrics>,
}

impl DownstreamWait {
    /// A wait over `session`'s downstream frontiers.
    fn for_session(session: &FillSession, metrics: Arc<crate::metrics::Metrics>) -> Self {
        Self {
            watch: session.downstream_watch(),
            metrics,
        }
    }

    /// The live downstream frontiers, as the pacer reads them.
    fn frontier(&self) -> DownstreamFrontier {
        DownstreamFrontier {
            served_paid: self.watch.served_paid(),
            serve_demand: self.watch.serve_demand(),
        }
    }
}

impl PacingWait for DownstreamWait {
    fn wait(&self, observed: DownstreamFrontier) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        // The pull hit its ADR 037 window: count the pause (the decision was `Wait`),
        // independent of whether we then park or short-circuit on a raced advance.
        self.metrics.node_pull_through_window_paused();
        Box::pin(self.watch.past(observed.served_paid, observed.serve_demand))
    }
}

/// Total content bytes a chunk-unit [`ChunkRanges`] covers, clamped to `total`.
///
/// A boundary with no matching close is an OPEN-ENDED run `[a, ∞)`: a fully-present
/// blob observes as `ChunkRanges{0..}` (a single unpaired boundary). Pairing `(a, b)`
/// alone would drop that final run and undercount a complete blob to 0, skewing
/// provider scoring. Clamp the open end to `total`.
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
    /// Shares the buffered [`decdn_cache::Origin::fetch`] path's cached-first discover → probe →
    /// rank pipeline and its open-time candidate fallback, but stops at channel-open +
    /// header instead of pulling any bytes.
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
        let namespace_bytes = namespace_id.to_be_bytes::<32>();
        let mut budget = MAX_PROVIDER_ATTEMPTS;
        let mut attempt_metered = false;
        let mut miss = PullMiss::Clean;

        if let Some(cached) = cached_candidates(deps, target).await {
            deps.metrics.probe_cache_hit();
            deps.metrics.node_pull_attempt();
            attempt_metered = true;
            let outcome = self
                .handshake_from_candidates(deps, &cached, hash_bytes, namespace_id, budget)
                .await;
            match outcome.payload {
                Ok(total_bytes) => {
                    return Ok(PullLegTarget {
                        total_bytes,
                        namespace_id: namespace_bytes,
                        candidates: cached,
                    });
                }
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
        // The ranged-drive assembly (#1506) plans over the coverage UNION of these
        // candidates, so the probe round must gather holders until their union spans
        // the blob — not stop at a fixed count that could miss the holders of the
        // still-uncovered blocks.
        let ranked = probe_and_rank(deps, providers, hash_bytes, ProbeGather::CoverageUnion).await;
        let outcome = self
            .handshake_from_candidates(deps, &ranked, hash_bytes, namespace_id, budget)
            .await;
        match outcome.payload {
            Ok(total_bytes) => Ok(PullLegTarget {
                total_bytes,
                namespace_id: namespace_bytes,
                candidates: ranked,
            }),
            Err(failed) => Err(miss.or(failed)),
        }
    }

    /// Walk `ranked` (bounded by `budget`), running the free header handshake against
    /// each until one reports `total_bytes`. Discovery already ranked the candidates;
    /// this only learns the blob geometry the serve response commits to. The ranged
    /// pull opens its own per-run lanes later — this handshake pays nothing.
    async fn handshake_from_candidates(
        &self,
        deps: &NodeOriginDeps,
        ranked: &[Candidate],
        hash_bytes: [u8; 32],
        namespace_id: U256,
        budget: usize,
    ) -> PullOutcome<u64> {
        let mut attempts = 0;
        let mut miss = PullMiss::Clean;
        for candidate in ranked.iter().take(budget) {
            attempts += 1;
            match self
                .handshake_total_bytes(deps, candidate, hash_bytes, namespace_id)
                .await
            {
                Ok(total_bytes) => {
                    return PullOutcome {
                        payload: Ok(total_bytes),
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

    /// Resolve, open/reuse a channel, bind (#1117), and run a free header handshake
    /// against one candidate; return the committed `total_bytes`. Any holder —
    /// partial or whole — signs the same whole-blob geometry, so the caller can commit
    /// its `StreamResponse` from whichever candidate answers first. Classifies every
    /// failure into the [`PullMiss`] it is. The channel it opens is cached by
    /// `open_or_reuse_pool`, so the ranged pull's first lane to this provider reuses it.
    async fn handshake_total_bytes(
        &self,
        deps: &NodeOriginDeps,
        candidate: &Candidate,
        hash_bytes: [u8; 32],
        namespace_id: U256,
    ) -> Result<u64, PullMiss> {
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
        // ADR 041 buy-side gate: refuse a candidate quoting above this node's buy
        // ceiling BEFORE opening a channel. A skip folds into the walk as `BelowMargin`.
        let heat = heat_of(deps, hash_bytes);
        let rate_ceiling =
            match economic_ceiling(deps, candidate.node_id.into(), heat, candidate.rate_per_mb) {
                EconGate::Allow { rate_ceiling, .. } => rate_ceiling,
                EconGate::Skip => return Err(PullMiss::BelowMargin),
            };
        let ctx = match deps
            .buyer
            .open_or_reuse_pool(provider_addr, CHANNEL_OPEN_CALLER_BUDGET)
            .await
        {
            Ok(ctx) => ctx,
            Err(err) => return Err(record_pool_open_failure(deps, provider_addr, &err)),
        };
        // Every setup fault from here to the header handshake classifies the
        // same way: it is ours, not the candidate's, so it folds into the walk
        // as a miss without scoring the peer. Deriving this lane's chain master
        // is one of them — it is a signing operation, so a remote or hardware
        // signer can refuse it.
        let local_miss = |err: &anyhow::Error| {
            PullMiss::for_verdict(classify_pull_failure(
                deps,
                pk,
                provider_addr,
                hash_bytes,
                None,
                err,
            ))
        };
        let ctx = bind_upstream_ctx(deps, ctx).map_err(|err| local_miss(&err))?;
        let deadlines = deps.config.deadlines().map_err(|err| local_miss(&err))?;
        let ledger = lane_ledger(deps, provider_addr, &ctx);
        let namespace_bytes = namespace_id.to_be_bytes::<32>();

        // The free header handshake: whole-tail open (`byte_offset == 0`,
        // `byte_len == 0`) to read the committed `total_bytes`, then abort — no
        // `next_chunk`, so no bytes are pulled and no voucher is paid, and the ledger
        // watermark is unchanged. The actual range-minimized pull re-opens per gap via
        // `PeerSource` on this same (now cached) channel.
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
            // The pre-flight handshake runs on the OUTER runtime, which keeps
            // living, so its driver is never stranded and needs no observer.
            None,
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
        Ok(total_bytes)
    }
}

/// Assemble `[offset, offset + len)` of `hash` into `engine`'s cache across the
/// ranked partial holders in `target` via the ranged-drive loop (#1506, ADR 039),
/// paying only for the missing ranges and pacing each lane with a [`RampPacer`]
/// built from `credit_ramp_divisor`, `credit_floor`, and `credit_max` — the same
/// ramped credit window the serve leg computes from its own paid frontier (ADR 003
/// §Credit window / ADR 037), so the pull never runs further ahead of the
/// downstream serve leg's paid frontier than that window allows, plus the one
/// window floor a serve leg parked at the pull's frontier demands.
///
/// # Ranged assembly across partial holders
///
/// A blob can live spread across nodes — one holds discovery block 0, another
/// block 1. The loop derives the still-missing gap from the shared
/// [`NodeAdmitStore`], plans it into contiguous runs by each candidate's
/// probe-fresh coverage ([`decdn_client_pull::plan_covered_runs`], concentrate +
/// sticky), and drives the runs in offset order. Each run opens ONE buyer lane to
/// its source — one `(signer, provider)` payment lane — and runs are SEQUENTIAL,
/// so two lanes never pay at once. The [`NodeAdmitStore`], [`RampPacer`], the
/// downstream frontier reader and wait ([`DownstreamWait`]) are SHARED across
/// every run, so the demand window is continuous: it is keyed on the downstream
/// paid frontier, not on the run, and a later run's lane still `Wait`s on the same
/// frontier the earlier one did.
///
/// A run whose `drive` returns a NON-terminal fault ([`decdn_client_pull::retry_disposition`]
/// `== RetryElsewhere`) drops that source and re-plans the still-missing remainder
/// against the survivors — the loop-level reassign-only tail. The store keeps the
/// verified bytes, so the replacement lane resumes at the gap and re-pays nothing
/// (#1682). A TERMINAL fault (a shared-pool voucher rejection, an origin blacklist,
/// an over-cap blob) ends the whole assembly. A still-missing range no surviving
/// candidate covers ends it as an `Unavailable` outcome (origins advertise all-ones
/// coverage, so an admitted origin candidate makes this unreachable). This function
/// runs on the pull thread the serve leg spawns AFTER it has already signed and sent
/// `ok: true`, so none of these failure outcomes reaches the client as a signed
/// `NotFound` — they surface as a truncated stream (`session.mark_ended(Err(..))`
/// stops the fill and the serve encoder ends short).
///
/// Records each provider's terminal outcome to reputation as its run ends, and the
/// whole assembly's terminal outcome via the shared [`FillSession::mark_ended`]. A
/// per-lane [`SettleOnDrop`] guard persists that lane's buyer watermark (#852) on
/// EVERY exit — clean run, terminal drive error, or a cooperative `cancel` (the
/// serve leg finished, so the client no longer waits: stop the upstream spend,
/// #1610).
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
/// coordination state (the shared [`FillSession`]'s [`DownstreamWatch`]) crosses
/// runtimes safely — atomics and `Notify` wakers are runtime-agnostic — and the
/// [`CacheEngine`] store actor and iroh [`Endpoint`](iroh::Endpoint) are reached
/// through their own channels, so a second runtime talking to them is fine.
#[allow(clippy::too_many_arguments)]
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
        total_bytes,
        namespace_id,
        candidates,
    } = target;

    let Some(deps) = deps_lock.get() else {
        // Unprovisioned under us (cannot happen — we discovered via deps): still
        // record a terminal outcome so the serve leg does not hang.
        session.mark_ended(Err(FillError::new(
            "node-origin pull leg lost its dependencies mid-serve",
        )));
        return;
    };
    // A zero/unusable deadline config is OUR fault (metered as such where it is
    // raised); fail the serve rather than open a lane that cannot legally run.
    let deadlines = match deps.config.deadlines() {
        Ok(deadlines) => deadlines,
        Err(err) => {
            session.mark_ended(Err(FillError::new(format!(
                "node-origin ranged pull has unusable deadlines: {err:#}"
            ))));
            return;
        }
    };

    let admit_store = NodeAdmitStore::new(engine, hash, total_bytes, Some(Arc::clone(&session)))
        .counting_received(Arc::clone(&deps.metrics));
    // The ramped credit-window pacer (ADR 003 §Credit window / ADR 037), SHARED
    // across every run so the pull never runs further ahead of the downstream
    // served-paid frontier than the window allows, in lockstep with the serve leg.
    let pacer = RampPacer {
        divisor: credit_ramp_divisor,
        floor: credit_floor,
        credit_max,
        paid_base: session.served_start(),
    };
    let config = DriveConfig {
        working_deposit: deps.config.working_deposit,
        seller_reserve: deps.config.seller_reserve,
        max_settle_waits: settle_wait_budget(deps.config.event_poll_interval),
        settle_backoff: SETTLE_POLL_STEP,
    };
    // The downstream pacing wait and frontier reader, SHARED across every run's lane
    // so the window is continuous — keyed on the session's downstream frontiers, not
    // on the run.
    let pacing_wait = DownstreamWait::for_session(&session, Arc::clone(&deps.metrics));
    // Index-aligned with `candidates`: `SourceCoverage.source_ix` is a position here.
    let coverages: Vec<decdn_protocol::Coverage> =
        candidates.iter().map(|c| c.coverage.clone()).collect();

    let sink = PeerRunSink {
        deps,
        deps_lock: &deps_lock,
        candidates: &candidates,
        hash_bytes,
        namespace_id,
        total_bytes,
        deadlines,
        admit_store: &admit_store,
        pacer: &pacer,
        config: &config,
        pacing_wait: &pacing_wait,
        topups_used: AtomicU32::new(0),
        cancel: &cancel,
    };

    let outcome = assemble(&sink, &coverages, offset, len, total_bytes).await;
    // One outbound outcome per assembled range. A cancelled assembly (the serve
    // leg finished first) is neither.
    match &outcome {
        AssembleOutcome::Complete => deps.metrics.outbound_stream_ended(true),
        AssembleOutcome::Unavailable | AssembleOutcome::Terminal(_) => {
            deps.metrics.outbound_stream_ended(false);
        }
        AssembleOutcome::Cancelled => {}
    }
    // On cancel the serve leg has already finished and nobody reads this, but set a
    // terminal outcome regardless. A cancelled assembly is not a fault (Ok), matching
    // a single-source pull's own Cancelled outcome; a range no holder covers is the
    // existing miss.
    let result = match outcome {
        AssembleOutcome::Complete | AssembleOutcome::Cancelled => Ok(()),
        AssembleOutcome::Unavailable => Err(FillError::new(
            "node-origin ranged pull: a still-missing range is held by no reachable provider",
        )),
        AssembleOutcome::Terminal(err) => Err(err),
    };
    session.mark_ended(result);
    // Each run's per-lane `SettleOnDrop` already persisted its buyer watermark (#852)
    // as that run ended.
}

/// The real [`RunSink`]: opens one buyer lane per planned run, drives it with
/// [`drive`], scores the provider, and persists the lane watermark. All the
/// per-run buyer state (channel context, voucher ledger, funder, source) is built
/// INSIDE `drive_run` — one lane per run — while the store, pacer, and demand
/// window it borrows from the fields are SHARED across every run.
struct PeerRunSink<'a> {
    deps: &'a NodeOriginDeps,
    /// For the per-lane [`SettleOnDrop`], which holds the deps `Arc` so it can
    /// persist the watermark after the run ends.
    deps_lock: &'a Arc<OnceLock<NodeOriginDeps>>,
    /// Ranked candidates, index-aligned with the coverage `assemble` plans over;
    /// `CoveredRun.source_ix` indexes here.
    candidates: &'a [Candidate],
    hash_bytes: [u8; 32],
    namespace_id: [u8; 32],
    total_bytes: u64,
    deadlines: PullDeadlines,
    admit_store: &'a NodeAdmitStore,
    pacer: &'a RampPacer,
    config: &'a DriveConfig,
    /// The shared downstream wait, which is also the frontier reader every run's
    /// `drive` paces against, so the demand window is continuous across runs.
    pacing_wait: &'a DownstreamWait,
    /// Reactive top-ups this assembly has escrowed, across EVERY run's lane
    /// (#1506). One pool deposit backs the whole set, so [`Funder::max_topups`]
    /// bounds the assembly, not each run — counting per-run would let a K-source
    /// miss escrow K on-chain `topUp` txs for one serve. Built once here and
    /// shared into every run's [`SharedPool`].
    topups_used: AtomicU32,
    cancel: &'a CancellationToken,
}

impl RunSink for PeerRunSink<'_> {
    async fn missing(&self, offset: u64, len: u64) -> ChunkRanges {
        // A store read fault is OUR fault; surface it by reporting the whole range
        // as still-missing so a run is attempted and `drive`'s own read raises and
        // classifies it, rather than falsely reporting the range complete.
        self.admit_store
            .missing_ranges(offset, len)
            .await
            .unwrap_or_else(|_| whole_range_chunks(offset, len))
    }

    // Sequential resolve → econ-gate → open → bind → drive → classify pipeline; the
    // tracing macros and the success/failure classification inflate the
    // cognitive-complexity + line metrics past threshold, exactly as the buffered
    // `pull_from_candidate` twin does. Splitting it would scatter one linear flow.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn drive_run(&self, run: CoveredRun) -> RunOutcome {
        // Cancellation before the lane open (#1506). Each run now opens its OWN lane
        // — `open_or_reuse_pool` can escrow a fresh `openChannel` or fire a proactive
        // `topUp`, and `bind_upstream_ctx` / `missing_ranges` run before the `drive`
        // `select!` that watches `cancel`. A serve leg that already finished (client
        // gone, shutdown) must not land an on-chain tx for nobody, so stop the loop
        // cleanly here rather than after opening.
        if self.cancel.is_cancelled() {
            return RunOutcome::Cancelled;
        }
        let Some(candidate) = self.candidates.get(run.source_ix) else {
            // Cannot happen — `source_ix` is a live index into `candidates` — but
            // drop the source rather than panic (anti-panic policy).
            return RunOutcome::Reassign;
        };
        let Ok(pk) = PublicKey::from_bytes(&candidate.node_id) else {
            return RunOutcome::Reassign;
        };
        let Some(provider_addr) = self
            .deps
            .addr_resolver
            .address_of(&DhtNodeId::from_bytes(candidate.node_id))
        else {
            debug!("node-origin: ranged-pull run candidate has no resolvable operator address");
            return RunOutcome::Reassign;
        };
        // ADR 041 buy-side gate: refuse a source quoting above this node's buy
        // ceiling BEFORE opening a lane; drop it and re-plan onto another holder.
        let heat = heat_of(self.deps, self.hash_bytes);
        let (rate_ceiling, speculative) = match economic_ceiling(
            self.deps,
            candidate.node_id.into(),
            heat,
            candidate.rate_per_mb,
        ) {
            EconGate::Allow {
                rate_ceiling,
                speculative,
            } => (rate_ceiling, speculative),
            EconGate::Skip => return RunOutcome::Reassign,
        };
        let ctx = match self
            .deps
            .buyer
            .open_or_reuse_pool(provider_addr, CHANNEL_OPEN_CALLER_BUDGET)
            .await
        {
            Ok(ctx) => ctx,
            // A channel-open failure is OUR payment-side problem, not the source's
            // fault. If it is a node-wide LOCAL fault (a broken buyer key, an
            // unreadable store), no other lane can fix it — terminal. Otherwise it
            // is per-provider (a revert, an RPC blip): drop this source and re-plan.
            Err(err) => {
                return match record_pool_open_failure(self.deps, provider_addr, &err) {
                    PullMiss::LocalFault => RunOutcome::Terminal(FillError::new(format!(
                        "node-origin ranged pull: local buyer fault opening a lane: {err:#}"
                    ))),
                    _ => RunOutcome::Reassign,
                };
            }
        };
        // #1117: bind the request so the upstream can chain a reactive pull. A bind
        // fault is our signer's — node-wide, so terminal.
        let ctx = match bind_upstream_ctx(self.deps, ctx) {
            Ok(ctx) => ctx,
            Err(err) => {
                let verdict = classify_pull_failure(
                    self.deps,
                    pk,
                    provider_addr,
                    self.hash_bytes,
                    None,
                    &err,
                );
                return match PullMiss::for_verdict(verdict) {
                    PullMiss::LocalFault => RunOutcome::Terminal(FillError::new(format!(
                        "node-origin ranged pull: local bind fault: {err:#}"
                    ))),
                    _ => RunOutcome::Reassign,
                };
            }
        };
        let pool_id = ctx.pool_id;
        let prior_amount = ctx.prior_amount;
        let ledger = lane_ledger(self.deps, provider_addr, &ctx);
        let ctx = Arc::new(std::sync::Mutex::new(ctx));

        // Content bytes this source will actually serve on THIS run (its gap,
        // interior holds excluded), for scoring and the speculative warming debit.
        let run_bytes = match self.admit_store.missing_ranges(run.offset, run.len).await {
            Ok(ranges) => content_len(&ranges, self.total_bytes),
            Err(_) => run.len,
        };

        // Per-lane #852 settle: persist THIS channel's watermark on every exit —
        // clean run, terminal drive error, or a cooperative cancel — after `drive`
        // below stops advancing the shared ledger.
        let _settle = SettleOnDrop {
            deps: Arc::clone(self.deps_lock),
            provider_addr,
            pool_id,
            prior_amount,
            ledger: Arc::clone(&ledger),
        };
        let abandoned = ConnDrain::default();
        let observer = abandoned.observer();
        let peer_source = PeerSource::new(
            &self.deps.endpoint,
            EndpointAddr::new(pk),
            Arc::clone(&ctx),
            Arc::clone(&ledger),
            &self.deps.slash_domain,
            provider_addr,
            self.namespace_id,
            self.deps.config.max_blob_size_bytes,
            rate_ceiling,
            self.deadlines,
        )
        .with_dial_observer(as_observer(&observer));
        let node_funder = NodeFunder::new(
            Arc::clone(&self.deps.buyer),
            Arc::clone(&ctx),
            Arc::clone(&self.deps.metrics),
            // The window-paced serve-miss leg does not derive a refuse-metering
            // signal from this flag; `NodeFunder` records its own metrics.
            Arc::new(AtomicBool::new(false)),
        );
        // The SAME shared downstream frontiers every run reads, so the demand
        // window is continuous across the sequential lanes.
        let pacing_wait = self.pacing_wait;
        let downstream_reader = move || pacing_wait.frontier();

        // The shared-pool view this run's `drive` gates on (#1506). One deposit
        // backs every run's lane, so the three pool facts are read across ALL of
        // them, not this one lane:
        // - `spent`: the whole-pool committed spend — the sum over every live
        //   lane ledger for `pool_id`, INCLUDING this run's own (seeded above via
        //   `lane_ledger`). Without it, a later run whose lane has spent nothing
        //   sees `committed == 0` and believes the whole deposit is unspent, then
        //   signs a voucher the pool cannot back → mid-stream `SpendingCapExhausted`.
        // - `topups_used`: the assembly-wide reactive-top-up budget, shared so K
        //   runs cannot each escrow `Funder::max_topups` on-chain `topUp` txs.
        // - `credit`: a landed top-up's new deposit, written to THIS run's `ctx`
        //   so its own gate stops reading the stale pre-top-up value; a later run
        //   re-reads the deposit from the persisted pool row `open_or_reuse_pool`
        //   already refreshed.
        let ledgers = &self.deps.ledgers;
        let spent = move || ledgers.pool_committed(pool_id);
        let credit_ctx = Arc::clone(&ctx);
        let credit = move |new_deposit: U256| -> anyhow::Result<()> {
            credit_ctx
                .lock()
                .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?
                .deposit = new_deposit;
            Ok(())
        };
        let pool = SharedPool {
            spent: &spent,
            topups_used: &self.topups_used,
            credit: &credit,
        };

        let started = Instant::now();
        // Cooperative cancellation: the serve leg finishing cancels the token,
        // dropping the `drive` future; the `_settle` guard still persists the
        // watermark. No score on cancel — an abandoned pull is neither a clean
        // delivery nor a provider fault.
        let cancelled;
        let result = tokio::select! {
            biased;
            r = drive(
                self.admit_store,
                &peer_source,
                self.pacer,
                &node_funder,
                &ctx,
                &ledger,
                self.hash_bytes,
                run.offset,
                run.len,
                self.config,
                None,
                Some(self.pacing_wait),
                Some(&downstream_reader),
                Some(&pool),
            ) => {
                cancelled = false;
                r
            }
            () = self.cancel.cancelled() => {
                cancelled = true;
                Ok(())
            }
        };
        let elapsed = started.elapsed();

        // Abandon drain on the cancel/`Err` paths — a dropped or errored `drive`
        // strands its upstream connection on this pull-thread runtime, which the
        // caller drops the instant this leg returns (see the single-source leg and
        // `abandon_drain` for why the wait is on the transition, not a fixed span).
        if cancelled || result.is_err() {
            drain_abandoned(&abandoned, provider_addr, &self.deps.metrics).await;
        }
        if cancelled {
            return RunOutcome::Cancelled;
        }

        match result {
            Ok(()) => {
                // ADR 041: a clean speculative run debits the source's warming
                // allowance by the full buy cost of the bytes it pulled (its gap),
                // in whole MB at the candidate's buy rate.
                if speculative {
                    self.deps.config.warming.debit_speculative(
                        (*pk.as_bytes()).into(),
                        Hash::from_bytes(self.hash_bytes),
                        candidate.rate_per_mb.saturating_mul(mb_of(run_bytes)),
                    );
                }
                record_outcome(
                    self.deps,
                    pk,
                    &Outcome::Delivered {
                        bytes: run_bytes,
                        elapsed,
                    },
                );
                RunOutcome::Filled
            }
            Err(err) => {
                if is_bao_corruption(&err) {
                    tracing::warn!(
                        provider = %pk, %provider_addr,
                        "node ranged pull: upstream served bao-corrupt bytes; scoring Corruption"
                    );
                    record_outcome(self.deps, pk, &Outcome::Corruption);
                    self.deps.metrics.node_pull_through_upstream_verify_failed();
                } else {
                    let _ = classify_pull_failure(
                        self.deps,
                        pk,
                        provider_addr,
                        self.hash_bytes,
                        Some(pool_id),
                        &err,
                    );
                }
                // A terminal fault (shared-pool voucher rejection or exhaustion,
                // origin blacklist, over-cap blob) cannot be fixed by another lane;
                // anything else is a property of THIS source's delivery — drop it
                // and re-plan.
                if run_fault_is_terminal(&err) {
                    RunOutcome::Terminal(FillError::new(format!("{err:#}")))
                } else {
                    RunOutcome::Reassign
                }
            }
        }
    }
}

/// The chunk-range span of the byte range `[offset, offset + len)`, chunk-aligned
/// outward — the whole-range gap a store-read fault reports so the range is
/// re-attempted rather than falsely reported complete.
fn whole_range_chunks(offset: u64, len: u64) -> ChunkRanges {
    let start = offset / CHUNK_BYTES;
    let end = offset.saturating_add(len).div_ceil(CHUNK_BYTES);
    ChunkRanges::from(bao_tree::ChunkNum(start)..bao_tree::ChunkNum(end))
}

/// Whether a run's [`drive`] error ends the whole assembly rather than reassigning
/// its range to another holder.
///
/// It is the shared [`retry_disposition`] verdict, PLUS one node-specific override:
/// a [`PoolExhausted`] is terminal here even though `retry_disposition` calls it
/// `RetryElsewhere`. The classifier keeps pool exhaustion retryable for the
/// single-source failover, where a cheaper provider's next voucher may fit a
/// deposit the current one's did not. The node's ranged loop is the opposite case:
/// every lane draws the ONE shared buyer pool (ADR 003), so no surviving holder can
/// pay from a dry pool — reassigning would only churn each remaining candidate
/// (a fresh dial + a first-voucher attempt) before the same failure. This mirrors
/// the multi-source scheduler, which special-cases exactly [`PoolExhausted`].
fn run_fault_is_terminal(err: &anyhow::Error) -> bool {
    retry_disposition(err) == RetryDisposition::Terminal
        || err.downcast_ref::<PoolExhausted>().is_some()
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
        // No chain either: rate 0 means nothing is ever metered on this leg.
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
/// [`CacheEngine::origin_range_wire`] behind the [`BackendSource`]. There is no
/// counterparty, so every paid-path axis is absent — and each absence is load-bearing,
/// not an omission:
///
/// - **No discovery / channel open / [`PeerSource`] / [`NodeFunder`].** The bytes
///   are already reachable locally, so there is nothing to dial, no channel to open,
///   and nothing to pay. The source is handed in by the orchestration, already built.
/// - **No provider scoring.** There is no provider: a fault here is OUR own
///   origin, never a peer to score. On a [`drive`] error we meter it as a LOCAL
///   fault ([`crate::metrics::Metrics::node_pull_local_fault`]) and NEVER touch
///   reputation.
/// - **No [`SettleOnDrop`].** That guard persists a BUYER voucher watermark (#852);
///   this leg issues no vouchers, so there is nothing to settle.
///
/// # Why the driver still needs a "ledger" — THE CRUX
///
/// [`drive`]'s per-gap completion is paid-frontier gated: a gap is `Done` only once
/// the ledger's committed `bytes` reach the gap end. An unpaid source that never
/// advanced a ledger would leave that frontier at zero and the gap loop would
/// re-draw forever. So the [`BackendSource`] carries a LOCAL bookkeeping
/// [`PoolLedger`](decdn_client_pull::PoolLedger) and, on `finish`, advances its `bytes` by exactly the leg's
/// drained wire (at amount 0). We hand `drive` that SAME ledger ([`BackendSource::ledger`])
/// plus a benign [`local_bookkeeping_ctx`] and a [`NullFunder`], so the completion
/// counter the source moves is the one the gap loop reads. This is NOT payment — no
/// channel, no voucher, no chain, no counterparty; see the [`BackendSource`] module
/// docs.
///
/// The downstream [`RampPacer`] is KEPT (bound on `served_paid`): the pull still
/// never runs further ahead of the real downstream client's paid frontier than the
/// ramped credit window allows, plus one serve-demand floor (#1610 — ingest only behind a waiting, paying client
/// — and the storage/egress exposure bound).
///
/// # Off the accept task, on its own runtime
///
/// Like [`run_pull_leg`], `drive`'s future is non-`Send`, so the orchestration
/// `block_on`s this on a dedicated current-thread runtime. All inputs are
/// therefore owned + `'static`; the shared coordination state
/// (the shared [`FillSession`]'s [`DownstreamWatch`]) crosses runtimes safely.
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
        paid_base: session.served_start(),
    };
    // `working_deposit == ZERO` disables the pacer's reactive top-up arm entirely, so
    // the settle-wait budget is inert here; keep the smallest sane values.
    let config = DriveConfig {
        working_deposit: U256::ZERO,
        seller_reserve: U256::ZERO,
        max_settle_waits: 0,
        settle_backoff: SETTLE_POLL_STEP,
    };
    let pacing_wait = DownstreamWait::for_session(&session, Arc::clone(&metrics));
    let downstream_reader = || pacing_wait.frontier();

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
            Some(&downstream_reader),
            // Single-source leg: one lane IS the pool, so no shared view.
            None,
        ) => {
            cancelled = false;
            r
        }
        () = cancel.cancelled() => {
            cancelled = true;
            Ok(())
        }
    };

    // No abandon drain here, unlike the paid twin. This leg fetches from the node's
    // OWN origin — an HTTP/S3/fs call inside the cache engine — so a cancelled or
    // errored `drive` strands no QUIC driver on this pull-thread runtime, and there
    // is nothing for the orchestration's immediate drop of that runtime to break.

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
        /// Return a transport error from `fetch_range_data` — an origin the node cannot
        /// reach (the no-hang-on-fault case).
        Fault,
        /// Serve a multi-window blob's first window, then return a transport error
        /// — an origin that fails after the wire has started streaming.
        FaultMidStream,
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
        /// Windows starting at or past this offset fail.
        FaultFrom(u64),
    }

    impl FakeOrigin {
        fn new(hash: Hash, data: &[u8], outboard: Bytes, mode: Mode) -> Self {
            // `Corrupt` is expressed by feeding mismatched `data` under `Serve`; only
            // `Fault` needs distinct fetch behaviour, so the stored mode is binary.
            let fake_mode = match mode {
                Mode::Serve | Mode::Corrupt => FakeMode::Serve,
                Mode::Fault => FakeMode::Fault,
                Mode::FaultMidStream => FakeMode::FaultFrom(decdn_cache::RANGE_PULL_WINDOW_BYTES),
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

        fn fetch_range_data(
            &self,
            hash: Hash,
            req: OriginRangeRequest,
        ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>>
        {
            let faults = match self.mode {
                FakeMode::Serve => false,
                FakeMode::Fault => true,
                FakeMode::FaultFrom(from) => req.fetch_start >= from,
            };
            if faults {
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
        let data = match mode {
            // Past one window, so the fault lands after the wire has started.
            Mode::FaultMidStream => {
                let size = decdn_cache::RANGE_PULL_WINDOW_BYTES as usize
                    + 5 * decdn_cache::CHUNK_GROUP_BYTES as usize
                    + 123;
                (0..size).map(|i| (i % 251) as u8).collect()
            }
            Mode::Serve | Mode::Fault | Mode::Corrupt => test_blob(),
        };
        let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data.clone());
        let hash = Hash::from(root);
        let total = data.len() as u64;

        let served: Vec<u8> = match mode {
            Mode::Serve | Mode::Fault | Mode::FaultMidStream => data,
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
        let outboard = engine
            .origin_fetch_outboard_bytes(hash, total)
            .await?
            .ok_or_else(|| anyhow::anyhow!("fixture origin must publish the outboard"))?;
        let source = BackendSource::new(engine.clone(), root, total, outboard, fresh_ledger());
        // Start the served frontier at `total` so the downstream `RampPacer` never
        // gates the pull (this test exercises the completion path, not the window).
        let session = FillSession::starting_at(bao_tree::blake3::Hash::from(root), total, total);

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

    /// (b2) A transport fault AFTER the wire has started streaming also records
    /// `pull_result == Some(Err(_))` and does NOT hang.
    #[tokio::test]
    async fn local_pull_leg_mid_stream_origin_fault_fails_without_hang() -> anyhow::Result<()> {
        let (engine, root, total, _tmp) = engine_with_origin(Mode::FaultMidStream).await?;

        let result = run_to_termination(&engine, root, total)
            .await?
            .ok_or_else(|| anyhow::anyhow!("pull_result must be recorded even on fault"))?;
        assert!(
            result.is_err(),
            "a mid-stream origin transport fault must terminate the leg with Err"
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
/// [`DownstreamWait`] parks on an edge-triggered wakeup that stores no permit, so a
/// serve-leg advance that races the pacer's `Wait` decision must be caught by
/// re-reading the frontiers AFTER arming the waiter — not waited on forever.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod downstream_wait_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use decdn_cache::FillSession;

    use super::DownstreamWait;
    use crate::metrics::Metrics;
    use decdn_client_pull::{DownstreamFrontier, PacingWait};

    /// A standalone session and a wait over its downstream frontiers.
    fn session_and_hook() -> (Arc<FillSession>, DownstreamWait) {
        let session = FillSession::new(bao_tree::blake3::Hash::from([7; 32]), 1 << 20);
        let hook = DownstreamWait::for_session(&session, Arc::new(Metrics::new()));
        (session, hook)
    }

    /// The #1673 race on the demand frontier: a serve encoder parks and raises the
    /// serve demand between the pacer's `Wait` decision and the pull parking, with no
    /// served-paid advance at all. `wait` must see the demand move and return at
    /// once, or the pull waits for a payment the parked encoder blocks (#1893).
    #[tokio::test]
    async fn a_racing_demand_advance_before_the_park_is_not_lost() {
        let (session, hook) = session_and_hook();

        session.demand_up_to(64 * 1024);

        tokio::time::timeout(
            Duration::from_secs(5),
            hook.wait(DownstreamFrontier::default()),
        )
        .await
        .expect("wait must observe the raced demand advance, not wedge on a lost notify");
    }

    /// A pull already parked on its window wakes when a serve leg raises the demand,
    /// with no payment at all — and a demand that does not move the frontier leaves
    /// it parked. Pins that `FillSession::demand_up_to` notifies the same wakeup
    /// `DownstreamWait::for_session` arms (#1893).
    #[tokio::test]
    async fn a_demand_raise_wakes_a_parked_pull_and_a_stale_one_does_not() {
        let (session, hook) = session_and_hook();
        session.demand_up_to(64 * 1024);
        let observed = hook.frontier();

        let wait = hook.wait(observed);
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), wait.as_mut())
                .await
                .is_err(),
            "with no advance the wait stays parked"
        );

        session.demand_up_to(32 * 1024);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), wait.as_mut())
                .await
                .is_err(),
            "a demand below the frontier moves nothing and wakes nothing"
        );

        session.demand_up_to(80 * 1024);
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("a demand raise must wake the parked pull");
    }

    /// The #1673 race: the serve leg advances the frontier and fires its wakeup in
    /// the gap between the pacer reading `observed` and the pull parking. The
    /// notify wakes nobody (no waiter registered, no permit stored). `wait` must
    /// re-read the frontier after arming and return at once; an edge-triggered wait
    /// wedges here forever.
    #[tokio::test]
    async fn a_racing_advance_before_the_park_is_not_lost() {
        let (session, hook) = session_and_hook();

        // The advance + notify land BEFORE `wait` is polled — the lost-wakeup window.
        session.advance_served(64 * 1024);

        tokio::time::timeout(
            Duration::from_secs(5),
            hook.wait(DownstreamFrontier::default()),
        )
        .await
        .expect("wait must observe the raced advance, not wedge on a lost notify");
    }

    /// The ordinary path still parks and wakes: with no advance yet, `wait` blocks,
    /// then resolves on a later advance from the serve leg.
    #[tokio::test]
    async fn a_later_advance_wakes_the_parked_wait() {
        let (session, hook) = session_and_hook();

        let advance = async {
            // Let `wait` arm + park first, then advance.
            tokio::task::yield_now().await;
            session.advance_served(64 * 1024);
        };
        tokio::join!(
            async {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    hook.wait(DownstreamFrontier::default()),
                )
                .await
                .expect("a later advance must wake the parked wait");
            },
            advance,
        );
    }
}

/// The ranged-drive loop's run-fault terminal classification (#1506): a
/// shared-pool exhaustion must STOP the assembly, not reassign the range to
/// another holder that draws the same dry pool.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod run_fault_terminal_tests {
    use decdn_client_pull::PoolExhausted;
    use decdn_protocol::client::VoucherRejectReason;

    use super::run_fault_is_terminal;
    use crate::client_requester::UpstreamVoucherRejected;

    /// A `PoolExhausted` is TERMINAL in the node loop even though the shared
    /// `retry_disposition` classifier calls it `RetryElsewhere`: the node draws one
    /// shared buyer pool, so no surviving holder can pay from a dry pool. Without
    /// this override the loop churns every remaining candidate before giving up.
    #[test]
    fn pool_exhaustion_is_terminal_in_the_node_loop() {
        let err = anyhow::Error::new(PoolExhausted {
            gap_start: 0,
            gap_len: 1 << 20,
        });
        assert!(
            run_fault_is_terminal(&err),
            "a dry shared pool must fail fast, not reassign onto another lane"
        );
    }

    /// The classifier's own `Terminal` verdicts still flow through: a shared-pool
    /// voucher rejection ends the assembly.
    #[test]
    fn voucher_rejection_stays_terminal() {
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
        });
        assert!(run_fault_is_terminal(&err));
    }

    /// An ordinary delivery fault (a stall, a transport reset) is NOT terminal —
    /// it faults one source and reassigns the range to another holder.
    #[test]
    fn a_transport_fault_reassigns() {
        let err = anyhow::anyhow!("connect failed: timed out");
        assert!(
            !run_fault_is_terminal(&err),
            "a per-source delivery fault must reassign, not stop the assembly"
        );
    }
}
