//! The gap-driven, range-minimized **pull leg** of the node serve-miss (ADR 037).
//!
//! This module drives the shared
//! [`decdn_client::drive`] loop over a node sink, so a serve-miss pulls and
//! pays UPSTREAM for only the ranges the cache is missing — held ranges are read
//! locally, never re-pulled or re-paid. It is the buyer half of the two concurrent
//! legs the orchestration (`serve_via_window_pull_through`) runs on the one serve
//! task; the seller half is [`super::super::handlers::client`]'s `serve_leg`.
//!
//! # Two entry points
//!
//! - [`NodeOrigin::open_pull_leg`] — discovery + probe + rank + a header
//!   handshake. Returns the ranked [`PullLegTarget`] (the upstream `total_bytes`
//!   plus the ranked candidates, each with its probe-fresh range-keyed coverage),
//!   so the orchestration can sign its `StreamResponse` before either leg streams a
//!   byte. Discovery happens ONCE here; the pull leg does not re-discover. Any
//!   holder — partial or whole — reports the same `total_bytes`, so the handshake
//!   walks candidates until one answers. With a prime and a candidate that
//!   reported its size on the probe, the handshake opens the pull leg's own first
//!   leg and the target carries that pull for the first run to adopt (#2063).
//! - [`run_pull_leg`] — assembles the blob across the partial holders via the
//!   ranged-drive loop (#1506): it plans the missing range into runs by coverage
//!   ([`decdn_client::plan_covered_runs`]) and drives them in offset order,
//!   opening ONE payment lane ([`NodeAdmitStore`] sink, [`PeerSource`],
//!   [`RampPacer`], [`super::funder::NodeFunder`]) per run and re-planning a non-terminal run
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
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use bao_tree::ChunkRanges;
use decdn_bao_range::RangedStore;
use decdn_bao_range::{AlignedRange, CHUNK_GROUP_BYTES, align_range};
use decdn_cache::{CacheEngine, CacheError, DownstreamWatch, FillError, FillSession, Hash};
use decdn_client::sink::PullReader;
use decdn_client::source::BlobSource as _;
use decdn_client::{
    CoveredRun, DownstreamFrontier, Fault, HashMismatch as ClientPullHashMismatch, LegNoProgress,
    PacingWait, PeerSource, PoolLedger, PrimedSource, RampPacer, RecoveryGate, SharedPool,
    UpstreamPullHeader, WaitReason, classify, drive, first_leg,
};

use decdn_reputation::Outcome;
use iroh::{EndpointAddr, PublicKey};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, debug, warn};

use super::admit_store::NodeAdmitStore;
use super::backend_source::BackendSource;
use super::funder::{SETTLE_POLL_STEP, settle_window};
use super::ranged_pull::{AssembleOutcome, RecoveryEnd, RunOutcome, RunSink, assemble};
use super::timed_source::{TimedReader, TimedSource, timed_open};
use super::{
    EconGate, NodeOrigin, NodeOriginDeps, ProbeGather, PullMiss, PullOutcome, PullVerdict,
    SettleOnDrop, bind_upstream_ctx, cached_candidates, classify_pull_failure, coverage_spans,
    discover, economic_ceiling, heat_of, lane_ledger, mb_of, now_micros, probe_and_rank,
    record_backpressure_exhausted, record_backpressure_refusal, record_outcome,
    record_pool_open_failure,
};
use crate::dht::negative_cache::Hash as DhtHash;
use crate::dht::routing::NodeId as DhtNodeId;
use crate::handlers::client::PROOF_WAIT_CEILING;
use crate::runtime::QUIC_MAX_IDLE_TIMEOUT;
use crate::selection::{Candidate, MAX_PROVIDER_ATTEMPTS, POOL_OPEN_CALLER_BUDGET};
use decdn_client::{
    PoolContext, PullDeadlines, open_progressive_pull as open_progressive_upstream,
};

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
    /// ([`decdn_client::plan_covered_runs`]) and opens one payment lane per
    /// run.
    candidates: Vec<Candidate>,
    /// The handshake's pull, opened at the first leg the pull leg is expected to
    /// open, for that leg to adopt (#2063). `None` when the handshake kept no
    /// first-leg pull: no prime or size hint, no predictable leg, a bounded open
    /// past the blob's end, or a signed size that differs from the hint.
    primed: Option<PrimedHandshake>,
}

impl PullLegTarget {
    /// How many ranked candidates the pull plans over.
    pub(crate) const fn candidate_count(&self) -> usize {
        self.candidates.len()
    }

    /// The part of `gap` no candidate covers ([`super::ranged_pull::uncovered`]),
    /// judged against the signed `total_bytes` rather than any candidate's
    /// unsigned size hint.
    pub(crate) fn uncovered(&self, gap: &ChunkRanges) -> ChunkRanges {
        let coverages: Vec<decdn_protocol::Coverage> =
            self.candidates.iter().map(|c| c.coverage.clone()).collect();
        super::ranged_pull::uncovered(gap, self.total_bytes, &coverages)
    }

    /// Keep the handshake's primed pull only when this serve drives the pull it
    /// was cut for: an owning claim over exactly `prime`'s range. Any other claim
    /// (an attach, or a mixed remainder) starts its pull elsewhere, so the pull
    /// is closed now rather than left idle on the upstream.
    pub(crate) fn keep_prime_for(&mut self, owns: bool, pull_range: Option<(u64, u64)>) {
        let keep = self.primed.as_ref().is_some_and(|primed| {
            owns && pull_range == Some((primed.prime.offset, primed.prime.len))
        });
        if !keep && self.primed.take().is_some() {
            debug!("node-origin: the serve does not own the primed pull's range; closing it");
        }
    }
}

/// What the pull leg is expected to open first, known before the upstream
/// handshake: the request it pulls and the first pacing window. The handshake
/// opens exactly that leg rather than a whole-blob open it drops (#2063). The
/// upstream treats every open as real (it signs, claims a fill, and on a miss
/// starts its own origin draw), so a dropped handshake open costs it a
/// throwaway fill and delays the real one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrimeLeg {
    /// The pull's content offset.
    pub(crate) offset: u64,
    /// The pull's content length; `0` is to the blob's end.
    pub(crate) len: u64,
    /// The first draw [`RampPacer`] allows: the ramped credit window at the
    /// ramp credit the owning stream carries and no payment of its own yet,
    /// floored to whole chunk groups as the window pacer floors it.
    pub(crate) window: u64,
}

impl PrimeLeg {
    /// The prime for a pull of `[offset, +len)` paced by a [`RampPacer`] built
    /// from `divisor`, `floor`, `credit_max`, and `paid_carried`.
    pub(crate) fn new(
        offset: u64,
        len: u64,
        divisor: u64,
        floor: u64,
        credit_max: u64,
        paid_carried: u64,
    ) -> Self {
        let window =
            decdn_incentive::ramped_credit_window(divisor, floor, credit_max, paid_carried);
        Self {
            offset,
            len,
            window: window - window % CHUNK_GROUP_BYTES,
        }
    }

    /// The first leg the pull leg's first run opens on a node holding none of
    /// the range, if that run goes to a source with `coverage` and the blob is
    /// `total_bytes` long, or `None` when the source does not cover the pull's
    /// first block. The run is the source's contiguous covered span from that
    /// block, cut to the request; the leg is the run cut to the window and to
    /// the received-byte ceiling `max_blob_size_bytes` (`0` = none), as
    /// [`decdn_client::first_leg`] cuts it.
    fn predicted_leg(
        &self,
        total_bytes: u64,
        coverage: &decdn_protocol::Coverage,
        max_blob_size_bytes: u64,
    ) -> Option<AlignedRange> {
        let block_bytes = decdn_protocol::discovery_block_bytes();
        let start = self.offset - self.offset % CHUNK_GROUP_BYTES;
        let request_end = if self.len == 0 {
            total_bytes
        } else {
            // `saturating_add`, not `checked_add`: an overflowing end (a grown
            // size claim can widen `self.len` toward `u64::MAX`) primes a
            // first leg too; the `.min(total_bytes)` below clamps it back down
            // regardless of how far the raw sum overshot.
            self.offset
                .saturating_add(self.len)
                .div_ceil(CHUNK_GROUP_BYTES)
                .saturating_mul(CHUNK_GROUP_BYTES)
                .min(total_bytes)
        };
        let first_block = u32::try_from(start / block_bytes).ok()?;
        if start >= request_end || !coverage.covers(first_block) {
            return None;
        }
        let blocks = decdn_protocol::num_blocks(total_bytes);
        let mut last_block = first_block;
        while last_block.saturating_add(1) < blocks && coverage.covers(last_block + 1) {
            last_block += 1;
        }
        let span_end = (u64::from(last_block) + 1)
            .saturating_mul(block_bytes)
            .min(request_end);
        let mut draw = (span_end - start).min(self.window);
        if max_blob_size_bytes > 0 {
            let cap_end = max_blob_size_bytes.saturating_add(CHUNK_GROUP_BYTES);
            draw = draw.min(cap_end.saturating_sub(start));
        }
        align_range(start, draw, total_bytes).ok()
    }

    /// The index into `coverages` of the source the pull leg's first run uses,
    /// planned as [`super::ranged_pull::assemble`] plans its first round over
    /// the ranked candidates' `coverages` of a `total_bytes` blob, or `None`
    /// when no candidate covers the pull's first block. It assumes the node
    /// holds none of the range, as [`Self::predicted_leg`] does.
    fn first_run_source(
        &self,
        total_bytes: u64,
        coverages: &[decdn_protocol::Coverage],
    ) -> Option<usize> {
        let end = if self.len == 0 {
            total_bytes
        } else {
            self.offset.saturating_add(self.len).min(total_bytes)
        };
        if self.offset >= end {
            return None;
        }
        let gap = whole_range_chunks(self.offset, end - self.offset);
        let rank: Vec<usize> = (0..coverages.len()).collect();
        let (runs, _) = super::ranged_pull::plan_over(&gap, total_bytes, coverages, &rank);
        let run = runs.first()?;
        let block_bytes = decdn_protocol::discovery_block_bytes();
        (run.offset / block_bytes == self.offset / block_bytes).then_some(run.source_ix)
    }
}

/// The order the header handshake visits the ranked candidates, as indices
/// into them: with a `prime` and a blob size, the candidate the pull leg's
/// first run uses ([`PrimeLeg::first_run_source`]) comes first, so the
/// handshake's primed pull is the one that run adopts; the rest follow in rank
/// order. Without a prime, a size, or a candidate covering the prime's first
/// block, the order is the rank order.
fn handshake_order(
    coverages: &[decdn_protocol::Coverage],
    total_bytes: Option<u64>,
    prime: Option<PrimeLeg>,
) -> Vec<usize> {
    let mut order: Vec<usize> = (0..coverages.len()).collect();
    let first = prime
        .zip(total_bytes)
        .and_then(|(prime, total)| prime.first_run_source(total, coverages));
    if let Some(first) = first {
        order.retain(|&ix| ix != first);
        order.insert(0, first);
    }
    order
}

/// The header handshake's range for a candidate advertising `coverage` of a
/// `total_bytes` blob: the first chunk group of the first discovery block it
/// covers, as `(offset, len)`, or `None` when it covers none of the blob.
fn covered_handshake_range(
    coverage: &decdn_protocol::Coverage,
    total_bytes: u64,
) -> Option<(u64, u64)> {
    let blocks = decdn_protocol::num_blocks(total_bytes);
    let first = coverage.covered_blocks().find(|&block| block < blocks)?;
    let start = u64::from(first).checked_mul(decdn_protocol::discovery_block_bytes())?;
    (start < total_bytes).then(|| (start, CHUNK_GROUP_BYTES.min(total_bytes - start)))
}

/// Which run may adopt a primed pull, apart from the pull itself: the facts a
/// run checks before it hands the pull to its drive.
#[derive(Debug, Clone)]
struct PrimeKey {
    /// The index, in the ranked candidates, of the source the pull is open to.
    candidate_ix: usize,
    /// The pool the pull pays from.
    pool_id: B256,
    /// The lane ledger the pull pays through.
    ledger: Arc<PoolLedger>,
    /// The range the pull covers.
    range: AlignedRange,
    /// When the pull opened. [`PrimedSource`] measures the pull's age from it.
    opened_at: tokio::time::Instant,
}

impl PrimeKey {
    /// Whether a run may adopt the pull: it runs on the same source, pays the
    /// same pool through the same lane ledger, and opens exactly `range` first
    /// (`first_leg`). A pull on another lane would be paid against a watermark
    /// the run's drive does not read, so it would pay twice. The pull's age is
    /// [`PrimedSource`]'s to check, from `opened_at`.
    fn answers(
        &self,
        source_ix: usize,
        pool_id: B256,
        ledger: &Arc<PoolLedger>,
        first_leg: Option<&AlignedRange>,
    ) -> bool {
        self.candidate_ix == source_ix
            && self.pool_id == pool_id
            && Arc::ptr_eq(&self.ledger, ledger)
            && first_leg == Some(&self.range)
    }
}

/// The handshake's live pull, opened at the pull leg's predicted first leg and
/// parked for the first run to adopt.
pub(crate) struct PrimedHandshake {
    /// What a run checks before it adopts the pull.
    key: PrimeKey,
    /// The pull leg the pull was cut for.
    prime: PrimeLeg,
    /// The pull's signed response header.
    header: UpstreamPullHeader,
    /// The live pull, timed from the start of the handshake open.
    reader: TimedReader<PullReader>,
}

/// The injected wait for [`RampPacer`]'s `Wait` and `WaitForMinDraw`: resolve once the serve leg's paid
/// frontier moves past, or the nearest serve demand differs from, what the decision read
/// ([`DownstreamWatch::past`], which owns the #1673 arm-then-recheck). Also the
/// reader `drive` paces against, so the decision and the wait always read the same
/// frontiers.
struct DownstreamWait {
    /// The blob the pull fills, for the long-pause warning.
    hash: Hash,
    /// The session's downstream frontiers.
    watch: DownstreamWatch,
    /// Bumps `node_pull_through_window_paused` on each window-full pause — the pull
    /// hit its ADR 037 window and is waiting for downstream payment to clear or a
    /// serve leg to park at its frontier — and `node_pull_through_min_draw_waits` on
    /// each pause for the minimum draw. Records each pause's length in
    /// `node_pull_through_wait_seconds`, a cancelled pause included.
    metrics: Arc<crate::metrics::Metrics>,
}

/// How long one window-paced pause runs before [`DownstreamWait`] logs a warning.
/// The pause keeps waiting after the warning: a payer that stalls is the serve
/// leg's to end, not the pull's.
///
/// The threshold exceeds the time a serve leg needs to notice a vanished payer.
/// The serve leg of a payer that vanishes without a `CONNECTION_CLOSE` ends at the
/// QUIC idle timeout ([`QUIC_MAX_IDLE_TIMEOUT`]) or at the proof-wait ceiling
/// ([`PROOF_WAIT_CEILING`]). The end of the last serve leg then cancels the pull.
/// So a pause that reaches the warning means a live serve leg holds the pull.
const PULL_WAIT_WARN_AFTER: Duration = Duration::from_secs(45);

/// The least time [`PULL_WAIT_WARN_AFTER`] keeps above each of its two ceilings: the
/// QUIC idle timeout and the proof-wait ceiling. A serve leg ends a few seconds after
/// its ceiling passes, and its end must then reach the pull.
const PULL_WAIT_WARN_MARGIN: Duration = Duration::from_secs(10);

const _: () = assert!(
    PULL_WAIT_WARN_AFTER.as_secs()
        >= QUIC_MAX_IDLE_TIMEOUT.as_secs() + PULL_WAIT_WARN_MARGIN.as_secs()
        && PULL_WAIT_WARN_AFTER.as_secs()
            >= PROOF_WAIT_CEILING.as_secs() + PULL_WAIT_WARN_MARGIN.as_secs(),
    "PULL_WAIT_WARN_AFTER must exceed the QUIC idle timeout and the proof-wait ceiling \
     by PULL_WAIT_WARN_MARGIN, or a vanished payer trips the warning"
);

impl DownstreamWait {
    /// A wait over `session`'s downstream frontiers, for a pull of `hash`.
    fn for_session(
        session: &FillSession,
        hash: Hash,
        metrics: Arc<crate::metrics::Metrics>,
    ) -> Self {
        Self {
            hash,
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
    fn wait(
        &self,
        observed: DownstreamFrontier,
        reason: WaitReason,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        // Count the pause by its reason, independent of whether we then park or
        // short-circuit on a raced advance.
        match reason {
            WaitReason::WindowFull => self.metrics.node_pull_through_window_paused(),
            WaitReason::MinDraw => self.metrics.node_pull_through_min_draw_waits(),
        }
        Box::pin(async move {
            // Records on drop, so a pause its leg cancels is observed too.
            let _timer = PauseTimer {
                started: tokio::time::Instant::now(),
                metrics: &self.metrics,
            };
            let mut past =
                std::pin::pin!(self.watch.past(observed.served_paid, observed.serve_demand));
            if tokio::time::timeout(PULL_WAIT_WARN_AFTER, &mut past)
                .await
                .is_err()
            {
                warn!(
                    hash = %self.hash,
                    ?reason,
                    served_paid = observed.served_paid,
                    serve_demand = observed.serve_demand,
                    waited_secs = PULL_WAIT_WARN_AFTER.as_secs(),
                    "pull still paused on its downstream frontiers; a stalled payer or a \
                     pacing regression holds it",
                );
                past.await;
            }
        })
    }
}

/// Records one pause's length in `node_pull_through_wait_seconds` when it drops:
/// at the end of the wait, or when the leg cancels the wait.
struct PauseTimer<'a> {
    /// When the pause started.
    started: tokio::time::Instant,
    /// The histogram's home.
    metrics: &'a crate::metrics::Metrics,
}

impl Drop for PauseTimer<'_> {
    fn drop(&mut self) {
        self.metrics.node_pull_through_wait(self.started.elapsed());
    }
}

/// Whether an own-origin leg's terminal error is a code bug
/// ([`CacheError::Internal`] anywhere in its chain): an encode panic or a broken
/// reader invariant. A store fault ([`CacheError::Store`], such as a full disk)
/// is not one.
fn is_internal_fault(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<CacheError>(),
            Some(CacheError::Internal(_))
        )
    })
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
/// - [`decdn_client::HashMismatch`] — the `decdn-client` decoder's typed
///   content-addressing sentinel, matched defensively for the paths that surface it
///   directly (it is also what [`super::pull_verdict`] downcasts to).
/// - The `decdn-client` streaming OVER-DELIVERY guards — the upstream sent more wire
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

/// The probe-cache candidates for `target`, when their coverage spans the blob.
///
/// A cached list that cannot span the blob is no hit for the ranged assembly: a
/// partial holder can win the handshake, and the blocks no cached holder covers
/// would then stay unfilled (#2195). The caller's cold path re-runs discovery, and
/// its probe round adds the directory's origins when the discovered holders do not
/// span the blob. The entry is not invalidated here: that probe round overwrites
/// it.
async fn spanning_cached_candidates(
    deps: &NodeOriginDeps,
    target: DhtHash,
    hash: Hash,
    requester: [u8; 32],
) -> Option<Vec<Candidate>> {
    let mut cached = cached_candidates(deps, target).await?;
    cached.retain(|candidate| candidate.node_id != requester);
    if coverage_spans(&cached) {
        return Some(cached);
    }
    debug!(
        %hash,
        cached = cached.len(),
        "node-origin: probe-cache entry does not span the blob; running a fresh lookup"
    );
    None
}

impl NodeOrigin {
    /// Discover, probe, rank, and open a channel to the best available provider for
    /// `hash`, returning the bound [`PullLegTarget`] (with the upstream `total_bytes`)
    /// the orchestration hands to [`run_pull_leg`].
    ///
    /// The handshake that reads `total_bytes` is a real open on the upstream: it
    /// signs, claims a fill, and on a miss starts its own origin draw. With a
    /// `prime` and a candidate that reported its size on the probe, the handshake
    /// therefore opens the pull leg's own first leg and parks the pull in the
    /// target for that leg to adopt (#2063). Otherwise it opens one chunk group
    /// of a block the candidate advertised, reads the header, and drops the pull;
    /// only without a size hint it can trust does it open the whole blob.
    ///
    /// `requester` is the node the serve answers. It is never a candidate: pulling
    /// from it would hand it back its own bytes, and two nodes that each lack part
    /// of the blob would pull from each other in a loop. While this open runs, the
    /// serve path refuses a whole-blob request for `hash` from an active staker
    /// ([`NodeOrigin::refuses_whole_blob`]), which stops longer loops.
    ///
    /// Shares the buffered [`decdn_cache::Origin::fetch`] path's cached-first discover → probe →
    /// rank pipeline and its open-time candidate fallback, but stops at pool-open +
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
        prime: Option<PrimeLeg>,
        requester: [u8; 32],
    ) -> Result<PullLegTarget, PullMiss> {
        let deps = self.deps.get().ok_or(PullMiss::Clean)?;
        let _pending = self.enter_open(hash);
        let hash_bytes = *hash.as_bytes();
        let target = DhtHash::from_bytes(hash_bytes);
        let namespace_bytes = namespace_id.to_be_bytes::<32>();
        let mut budget = MAX_PROVIDER_ATTEMPTS;
        let mut attempt_metered = false;
        let mut miss = PullMiss::Clean;

        if let Some(cached) = spanning_cached_candidates(deps, target, hash, requester).await {
            deps.metrics.probe_cache_hit();
            deps.metrics.node_pull_attempt();
            attempt_metered = true;
            let outcome = self
                .handshake_from_candidates(deps, &cached, hash_bytes, namespace_id, budget, prime)
                .await;
            match outcome.payload {
                Ok((total_bytes, primed)) => {
                    return Ok(PullLegTarget {
                        total_bytes,
                        namespace_id: namespace_bytes,
                        candidates: cached,
                        primed,
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

        let mut providers = discover(deps, hash_bytes, namespace_id).await;
        let requester_id = DhtNodeId::from_bytes(requester);
        providers.retain(|provider| *provider != requester_id);
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
        // still-uncovered blocks — and adds the origin-directory candidates when the
        // discovered holders cannot span it (#2195).
        // The origin-directory supplement inside the probe round can list the
        // requester too.
        let mut ranked = probe_and_rank(
            deps,
            providers,
            hash_bytes,
            ProbeGather::CoverageUnion,
            namespace_id,
        )
        .await;
        ranked.retain(|candidate| candidate.node_id != requester);
        let outcome = self
            .handshake_from_candidates(deps, &ranked, hash_bytes, namespace_id, budget, prime)
            .await;
        match outcome.payload {
            Ok((total_bytes, primed)) => Ok(PullLegTarget {
                total_bytes,
                namespace_id: namespace_bytes,
                candidates: ranked,
                primed,
            }),
            Err(failed) => Err(miss.or(failed)),
        }
    }

    /// Walk `ranked` (bounded by `budget`), running the header handshake against
    /// each until one reports `total_bytes`, and return it with the handshake's
    /// primed pull, if any. Discovery already ranked the candidates; this learns
    /// the blob geometry the serve response commits to. The ranged pull opens its
    /// own per-run lanes later, and its first run may adopt the primed pull.
    ///
    /// The walk visits first the candidate the pull's first run uses for
    /// `prime`, then the rest in rank order ([`handshake_order`]), so a
    /// top-ranked holder that lacks the request's first block does not take the
    /// handshake from the holder whose primed pull that run adopts. Each
    /// candidate keeps its index into `ranked` as its `candidate_ix`.
    async fn handshake_from_candidates(
        &self,
        deps: &NodeOriginDeps,
        ranked: &[Candidate],
        hash_bytes: [u8; 32],
        namespace_id: U256,
        budget: usize,
        prime: Option<PrimeLeg>,
    ) -> PullOutcome<(u64, Option<PrimedHandshake>)> {
        let mut attempts = 0;
        let mut miss = PullMiss::Clean;
        let coverages: Vec<decdn_protocol::Coverage> =
            ranked.iter().map(|c| c.coverage.clone()).collect();
        // The size the order plans over is a probe hint; each candidate's own
        // hint cuts its primed leg in `handshake_total_bytes`.
        let total_hint = ranked.iter().find_map(|c| c.total_bytes_hint);
        let visits = handshake_order(&coverages, total_hint, prime)
            .into_iter()
            .filter_map(|ix| ranked.get(ix).map(|candidate| (ix, candidate)));
        for (candidate_ix, candidate) in visits.take(budget) {
            attempts += 1;
            match self
                .handshake_total_bytes(
                    deps,
                    candidate,
                    candidate_ix,
                    hash_bytes,
                    namespace_id,
                    prime,
                )
                .await
            {
                Ok(answer) => {
                    return PullOutcome {
                        payload: Ok(answer),
                        attempts,
                        unfunded: Vec::new(),
                    };
                }
                Err(failed) => miss = miss.or(failed),
            }
        }
        // The handshake pays nothing, so it runs no funding recovery step; the
        // assembly that follows runs its own ([`assemble`]).
        PullOutcome {
            payload: Err(miss),
            attempts,
            unfunded: Vec::new(),
        }
    }

    /// Resolve, open/reuse a channel, bind (#1117), and run the header handshake
    /// against one candidate, the `candidate_ix`-th ranked; return the committed
    /// `total_bytes` and, when the handshake opened the pull leg's first leg, that
    /// live pull. Any holder — partial or whole — signs the same whole-blob
    /// geometry, so the caller can commit its `StreamResponse` from whichever
    /// candidate answers first. Classifies every failure into the [`PullMiss`] it
    /// is. The pool it opens is cached by `open_or_reuse_pool`, so the ranged
    /// pull's first lane to this provider reuses it.
    ///
    /// With a `prime` and the candidate's probe-reported size, the handshake opens
    /// [`PrimeLeg::predicted_leg`]. The size is unsigned, so a bounded open that
    /// fails (a range past a smaller blob's end) is asked again for the whole blob
    /// before the candidate counts as failed, and a pull whose signed size differs
    /// from the hint is dropped: its range was cut from the wrong size.
    // Straight-line resolve → gate → open → bind → handshake; the bounded first
    // attempt adds one branch to a flow that reads best in one piece.
    #[allow(clippy::too_many_lines)]
    async fn handshake_total_bytes(
        &self,
        deps: &NodeOriginDeps,
        candidate: &Candidate,
        candidate_ix: usize,
        hash_bytes: [u8; 32],
        namespace_id: U256,
        prime: Option<PrimeLeg>,
    ) -> Result<(u64, Option<PrimedHandshake>), PullMiss> {
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
        // ceiling BEFORE opening or reusing the pool. A skip folds into the walk as
        // `BelowMargin`.
        let heat = heat_of(deps, hash_bytes);
        let rate_ceiling =
            match economic_ceiling(deps, candidate.node_id.into(), heat, candidate.rate_per_mb) {
                EconGate::Allow { rate_ceiling, .. } => rate_ceiling,
                EconGate::Skip => return Err(PullMiss::BelowMargin),
            };
        let ctx = match deps
            .buyer
            .open_or_reuse_pool(provider_addr, POOL_OPEN_CALLER_BUDGET)
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
        let stream_guard = deps.metrics.outbound_stream_guard();

        // The pull leg's own first leg, when the probe reported the size to cut it
        // from. Its dial runs on the node's main runtime, so its connection's
        // driver lives there and outlives the pull thread that adopts it.
        let hint = candidate.total_bytes_hint;
        // Set when the primed open runs past the blob's end: the probed size is
        // wrong, so no range cut from it can be trusted.
        let mut hint_wrong = false;
        let predicted = prime.zip(hint).and_then(|(prime, total)| {
            prime
                .predicted_leg(total, &candidate.coverage, deps.config.max_blob_size_bytes)
                .map(|range| (prime, range))
        });
        if let Some((prime, range)) = predicted {
            let source = PeerSource::new(
                &deps.endpoint,
                EndpointAddr::new(pk),
                Arc::new(std::sync::Mutex::new(ctx.clone())),
                Arc::clone(&ledger),
                &deps.slash_domain,
                provider_addr,
                namespace_bytes,
                deps.config.max_blob_size_bytes,
                rate_ceiling,
                deadlines,
                // Per-serve runtime: dial per leg (#1675).
                None,
            )
            .with_dial_runtime(deps.dial_runtime.clone());
            let handshake = timed_open(
                source.open(hash_bytes, range.clone()),
                Arc::clone(&deps.metrics),
            );
            match handshake.await {
                Ok((header, reader)) => {
                    drop(stream_guard);
                    let total_bytes = header.total_bytes;
                    if hint != Some(total_bytes) {
                        warn!(
                            peer = %pk,
                            hash = %Hash::from(hash_bytes),
                            signed = total_bytes,
                            ?hint,
                            "node-origin: the provider signs another size than it probed; \
                             closing the first-leg handshake pull"
                        );
                        return Ok((total_bytes, None));
                    }
                    let primed = PrimedHandshake {
                        key: PrimeKey {
                            candidate_ix,
                            pool_id: ctx.pool_id,
                            ledger,
                            range,
                            opened_at: tokio::time::Instant::now(),
                        },
                        prime,
                        header,
                        reader,
                    };
                    return Ok((total_bytes, Some(primed)));
                }
                // A range past the blob's end means the probed size was wrong, not
                // the provider: ask it for the whole blob instead.
                Err(err) if decdn_client::is_range_past_end(&err) => {
                    hint_wrong = true;
                    debug!(
                        peer = %pk,
                        hash = %Hash::from(hash_bytes),
                        "node-origin: the first-leg handshake runs past the blob's end ({err:#}); \
                         opening the whole blob, as the probed size is wrong"
                    );
                }
                // Any other failure is the handshake's own outcome, classified as the
                // whole-blob open's would be.
                Err(err) => {
                    drop(stream_guard);
                    let verdict =
                        handshake_verdict(deps, pk, provider_addr, hash_bytes, ctx.pool_id, &err);
                    return Err(PullMiss::for_verdict(verdict));
                }
            }
        }

        // The header handshake: open a range to read the committed `total_bytes`,
        // then abort: no `next_chunk`, so no bytes are pulled and no voucher is
        // paid, and the ledger watermark is unchanged. The actual range-minimized
        // pull re-opens per gap via `PeerSource` on this same (now cached) channel.
        // The range lies in a block the candidate advertised, so a partial holder
        // answers from what it holds; asked for bytes it lacks, a holder that pulls
        // through starts its own pull for them, and partial holders asking each
        // other form a loop. Only without a size to cut that range from is the
        // whole blob (`byte_offset == 0`, `byte_len == 0`) the range asked for.
        let (byte_offset, byte_len) = hint
            .filter(|_| !hint_wrong)
            .and_then(|total| covered_handshake_range(&candidate.coverage, total))
            .unwrap_or((0, 0));
        let (header, probe) = match open_progressive_upstream(
            &deps.endpoint,
            EndpointAddr::new(pk),
            &ctx,
            Arc::clone(&ledger),
            &deps.slash_domain,
            provider_addr,
            hash_bytes,
            namespace_bytes,
            byte_offset,
            now_micros(),
            deps.config.max_blob_size_bytes,
            rate_ceiling,
            deadlines,
            byte_len,
            // Every dial this node makes runs on its main runtime, wherever the
            // caller runs.
            Some(&deps.dial_runtime),
        )
        .await
        {
            Ok(pair) => pair,
            Err(err) => {
                drop(stream_guard);
                let verdict =
                    handshake_verdict(deps, pk, provider_addr, hash_bytes, ctx.pool_id, &err);
                return Err(PullMiss::for_verdict(verdict));
            }
        };
        let total_bytes = header.total_bytes;
        let _ = probe.abort();
        drop(stream_guard);
        Ok((total_bytes, None))
    }
}

/// The verdict for a failed first-leg or header handshake. A backpressure
/// refusal is metered without the `(peer, hash)` suppression (#2178): the
/// handshake is a real admission at the seller, so this node's other pulls on
/// the hash can fill the seller's per-signer cap and refuse it, and suppressing
/// the peer would take it out of those pulls' candidate walks while the cap
/// clears. Every other failure is classified as usual.
fn handshake_verdict(
    deps: &NodeOriginDeps,
    pk: PublicKey,
    provider_addr: Address,
    hash_bytes: [u8; 32],
    pool_id: B256,
    err: &anyhow::Error,
) -> PullVerdict {
    record_backpressure_refusal(deps, provider_addr, err).unwrap_or_else(|| {
        classify_pull_failure(deps, pk, provider_addr, hash_bytes, Some(pool_id), err)
    })
}

/// Assemble `[offset, offset + len)` of `hash` into `engine`'s cache across the
/// ranked partial holders in `target` via the ranged-drive loop (#1506, ADR 039),
/// paying only for the missing ranges and pacing each lane with a [`RampPacer`]
/// built from `credit_ramp_divisor`, `credit_floor`, `credit_max`, and the ramp
/// credit `paid_carried` the owning stream carries from its lane — the same
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
/// probe-fresh coverage ([`decdn_client::plan_covered_runs`], concentrate +
/// sticky), and drives the runs in offset order. Each run opens ONE buyer lane to
/// its source — one `(signer, provider)` payment lane — and runs are SEQUENTIAL,
/// so two lanes never pay at once. The [`NodeAdmitStore`], [`RampPacer`], the
/// downstream frontier reader and wait ([`DownstreamWait`]) are SHARED across
/// every run, so the demand window is continuous: it is keyed on the downstream
/// paid frontier, not on the run, and a later run's lane still `Wait`s on the same
/// frontier the earlier one did.
///
/// A run whose `drive` returns a fault that does not end the assembly
/// ([`ends_the_assembly`]) drops that source and re-plans the still-missing
/// remainder against the survivors — the loop-level reassign-only tail. The
/// store keeps the verified bytes, so the replacement lane resumes at the gap
/// and re-pays nothing (#1682). A fatal fault (an over-cap blob, a fault of
/// this node's own store) ends the whole assembly. A funding refusal drops
/// only its source, and the assembly's funding recovery step decides once no
/// other holder serves. The assembly also ends `Unavailable`
/// when every covering candidate faulted, when a round made no progress, when the
/// reassign budget ran out, or when no surviving candidate covers a still-missing
/// range ([`super::ranged_pull::UnavailableCause`]). The serve leg refuses before
/// `ok: true` when the candidates cannot cover the missing range at all, so these
/// are faults that arise after the commit. This function
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
/// non-`Send` (its [`decdn_client::IngestStore`] fill is deliberately
/// non-`Send`), which the iroh `ProtocolHandler::accept` bound forbids on
/// the serve task. So the orchestration spawns a dedicated OS thread with its OWN
/// current-thread tokio runtime and `block_on`s this. All inputs are therefore
/// OWNED + `'static` (no borrow crosses the thread): `deps_lock` is a clone of
/// [`NodeOrigin::deps_arc`], read via `get()` HERE so `&deps.endpoint` /
/// `&deps.slash_domain` are borrowed only within this runtime's scope. The shared
/// coordination state (the shared [`FillSession`]'s [`DownstreamWatch`]) crosses
/// runtimes safely — atomics and `Notify` wakers are runtime-agnostic — and the
/// [`CacheEngine`] store actor is reached through its own channel. Upstream dials
/// are the exception: a connection's QUIC driver runs on the runtime that dials
/// it, and this runtime drops when the leg returns, so every dial goes through
/// [`NodeOriginDeps::dial_runtime`].
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
    paid_carried: u64,
    session: Arc<FillSession>,
    cancel: CancellationToken,
) {
    let hash_bytes = *hash.as_bytes();
    let PullLegTarget {
        total_bytes,
        namespace_id,
        candidates,
        primed,
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
        paid_carried,
    };
    // The downstream pacing wait and frontier reader, SHARED across every run's lane
    // so the window is continuous — keyed on the session's downstream frontiers, not
    // on the run.
    let pacing_wait = DownstreamWait::for_session(&session, hash, Arc::clone(&deps.metrics));
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
        pacing_wait: &pacing_wait,
        recovery: RecoveryGate::with_settle(settle_window(deps.config.event_poll_interval)),
        gap_seen: std::sync::Mutex::new(None),
        seen_deposit: std::sync::Mutex::new(deps.buyer.pool_deposit().unwrap_or(U256::ZERO)),
        cancel: &cancel,
        primed: std::sync::Mutex::new(primed),
    };

    let outcome = assemble(&sink, &coverages, offset, len, total_bytes).await;
    // One outbound outcome per assembled range. A cancelled assembly (the serve
    // leg finished first) is neither.
    match &outcome {
        AssembleOutcome::Complete => deps.metrics.outbound_stream_ended(true),
        AssembleOutcome::Unavailable(_)
        | AssembleOutcome::Backpressured
        | AssembleOutcome::FundingNeeded
        | AssembleOutcome::RecoveryFailed
        | AssembleOutcome::Terminal(_) => {
            deps.metrics.outbound_stream_ended(false);
        }
        AssembleOutcome::Cancelled => {}
    }
    session.mark_ended(assembly_result(
        outcome,
        hash,
        offset,
        len,
        candidates.len(),
    ));
    // Each run's per-lane `SettleOnDrop` already persisted its buyer watermark (#852)
    // as that run ended.
}

/// The fill session's end for an assembly `outcome` over `[offset, offset+len)`
/// of `hash`, from `candidates` candidates. A cancelled assembly (the serve leg
/// finished first, so nobody reads the end) is not a fault, matching a
/// single-source pull's own `Cancelled` outcome; a range no holder covers is the
/// existing miss.
fn assembly_result(
    outcome: AssembleOutcome,
    hash: Hash,
    offset: u64,
    len: u64,
    candidates: usize,
) -> Result<(), FillError> {
    match outcome {
        AssembleOutcome::Complete | AssembleOutcome::Cancelled => Ok(()),
        AssembleOutcome::Unavailable(cause) => {
            // Signed `ok: true` is already out, so this is a truncated stream. Name
            // the cause for the operator: the downstream fault line carries only the
            // fill error.
            warn!(
                %hash,
                offset,
                len,
                candidates,
                cause = cause.as_str(),
                "node-origin ranged pull ended before the range was filled"
            );
            Err(FillError::new(format!(
                "node-origin ranged pull: {}",
                cause.as_str()
            )))
        }
        AssembleOutcome::Backpressured => Err(FillError::new(
            "node-origin ranged pull: the only holder of a still-missing range kept refusing \
         for backpressure",
        )),
        AssembleOutcome::FundingNeeded => {
            warn!(
                %hash,
                offset,
                len,
                "node-origin ranged pull ended funding needed: the holders of a still-missing \
                 range refuse this node's funding and no recovery step can raise it"
            );
            Err(FillError::new(
                "node-origin ranged pull: funding needed (this node's buyer pool)",
            ))
        }
        // `node_step` logged the step's cause.
        AssembleOutcome::RecoveryFailed => Err(FillError::new(
            "node-origin ranged pull: the funding recovery step failed (this node's buyer pool)",
        )),
        AssembleOutcome::Terminal(err) => Err(err),
    }
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
    /// The shared downstream wait, which is also the frontier reader every run's
    /// `drive` paces against, so the demand window is continuous across runs.
    pacing_wait: &'a DownstreamWait,
    /// The assembly's funding recovery state (ADR 003 § Funding recovery). One
    /// assembly is one fill, so its runs share one progress rule.
    recovery: RecoveryGate,
    /// The still-missing chunk count at the last recovery check, so the
    /// gap's shrink since then counts as verified progress ([`RunSink::recover`]).
    gap_seen: std::sync::Mutex<Option<u64>>,
    /// The buyer pool deposit the assembly last saw: at its start, then after
    /// each recovery step. A pool row above it is a sibling fill's step.
    seen_deposit: std::sync::Mutex<U256>,
    cancel: &'a CancellationToken,
    /// The handshake's primed pull, taken by the first run (#2063). That run
    /// adopts it when it opens exactly its leg on its lane; otherwise it closes.
    primed: std::sync::Mutex<Option<PrimedHandshake>>,
}

impl PeerRunSink<'_> {
    /// Hand the handshake's primed pull to `source` when `run` opens exactly its
    /// leg, on its source, through its lane (`pool_id`, `ledger`) — see
    /// [`PrimeKey::answers`]. Only the first run gets the chance: the pull is taken
    /// either way, and closes when it does not answer.
    async fn adopt_primed(
        &self,
        source: &PrimedSource<TimedSource<PeerSource<'_>>>,
        run: &CoveredRun,
        pool_id: B256,
        ledger: &Arc<PoolLedger>,
    ) {
        let taken = self
            .primed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(primed) = taken else {
            return;
        };
        let leg = match first_leg(
            self.admit_store,
            &[(run.offset, run.len)],
            primed.prime.window,
            self.deps.config.max_blob_size_bytes,
        )
        .await
        {
            Ok(leg) => leg,
            Err(err) => {
                // The drive reads the same store and meets the same fault; this
                // only closes the pull it can no longer match.
                warn!(
                    error = %format!("{err:#}"),
                    "node-origin: the store read to match the primed handshake leg failed; \
                     closing it"
                );
                return;
            }
        };
        if primed
            .key
            .answers(run.source_ix, pool_id, ledger, leg.as_ref())
        {
            source.prime(
                self.hash_bytes,
                primed.key.range,
                primed.header,
                primed.reader,
                primed.key.opened_at,
            );
        } else {
            debug!(
                hash = %Hash::from(self.hash_bytes),
                source_ix = run.source_ix,
                primed_ix = primed.key.candidate_ix,
                "node-origin: the first run does not open the primed handshake leg on its \
                 lane; closing it"
            );
        }
    }
}

/// The first backpressure wait (#2178). Each later wait doubles, up to
/// [`BACKPRESSURE_BACKOFF_MAX`].
const BACKPRESSURE_BACKOFF_BASE: Duration = Duration::from_millis(250);

/// The longest single backpressure wait (#2178). With the base and
/// `MAX_BACKPRESSURE_RETRIES`, a source that refuses every retry holds the
/// assembly for 11.75 s before the assembly gives up on it — long enough for
/// this node's other pulls on that source to pay their first window and release
/// its per-signer cap.
const BACKPRESSURE_BACKOFF_MAX: Duration = Duration::from_secs(4);

/// The wait before the `attempt`-th (1-based) backpressure re-drive.
fn backpressure_backoff(attempt: u32) -> Duration {
    let doublings = attempt.saturating_sub(1).min(8);
    BACKPRESSURE_BACKOFF_BASE
        .saturating_mul(1 << doublings)
        .min(BACKPRESSURE_BACKOFF_MAX)
}

impl RunSink for PeerRunSink<'_> {
    async fn backoff(&self, attempt: u32) -> bool {
        tokio::select! {
            () = self.cancel.cancelled() => false,
            () = tokio::time::sleep(backpressure_backoff(attempt)) => {
                self.deps.metrics.node_pull_backpressure_backoffs();
                true
            }
        }
    }

    fn backpressure_exhausted(&self, source_ix: usize, waits: u32) {
        let Some(pk) = self
            .candidates
            .get(source_ix)
            .and_then(|c| PublicKey::from_bytes(&c.node_id).ok())
        else {
            return;
        };
        record_backpressure_exhausted(self.deps, pk, self.hash_bytes, waits);
    }

    /// The assembly's funding recovery step, under its gate. The gap's shrink
    /// since the last check is the verified progress the gate counts: the store
    /// admits only bao-verified bytes. Inside the settle window after a top-up,
    /// a source that still refuses is the upstream's chain watcher lagging the
    /// new deposit, so the assembly waits a [`SETTLE_POLL_STEP`] and asks again
    /// without a step.
    async fn recover(&self, gap_chunks: u64) -> Result<(), RecoveryEnd> {
        {
            let mut seen = self
                .gap_seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(prev) = *seen
                && gap_chunks < prev
            {
                self.recovery
                    .record_verified((prev - gap_chunks).saturating_mul(CHUNK_BYTES));
            }
            *seen = Some(gap_chunks);
        }
        if self.recovery.settling(tokio::time::Instant::now()) {
            return tokio::select! {
                () = self.cancel.cancelled() => Err(RecoveryEnd::FundingNeeded),
                () = tokio::time::sleep(SETTLE_POLL_STEP) => Ok(()),
            };
        }
        let seen = *self
            .seen_deposit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match super::node_step(&self.recovery, self.deps, seen, self.candidates.len()).await {
            Ok(deposit) => {
                *self
                    .seen_deposit
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = deposit;
                Ok(())
            }
            Err(super::PullMiss::LocalFault) => Err(RecoveryEnd::Failed),
            Err(_) => Err(RecoveryEnd::FundingNeeded),
        }
    }

    async fn missing(&self, offset: u64, len: u64) -> ChunkRanges {
        // A store read fault is OUR fault; surface it by reporting the whole range
        // as still-missing so a run is attempted and `drive`'s own read raises and
        // classifies it, rather than falsely reporting the range complete.
        self.admit_store
            .missing_ranges(offset, len)
            .await
            .unwrap_or_else(|_| whole_range_chunks(offset, len))
    }

    /// One attempt to pull `run` from one candidate, in an `upstream_stream`
    /// span: the ranged twin of the span `pull_from_candidate` opens on the
    /// buffered path, with the same fields in the same renderings. The span
    /// records the run's `outcome` once, when the run ends. A run that ends
    /// before its lane binds has no `pool_id`, and a first run that adopts the
    /// primed handshake pull has no `open_progressive_pull` child for that leg.
    async fn drive_run(&self, run: CoveredRun) -> RunOutcome {
        let span = tracing::info_span!(
            "upstream_stream",
            otel.kind = "client",
            direction = "outbound",
            peer = tracing::field::Empty,
            local_node_id = %self.deps.endpoint.id(),
            hash = %DhtHash::from_bytes(self.hash_bytes),
            pool_id = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        if let Some(candidate) = self.candidates.get(run.source_ix) {
            span.record(
                "peer",
                tracing::field::display(DhtNodeId::from_bytes(candidate.node_id)),
            );
        }
        let outcome = self.drive_run_in_span(run).instrument(span.clone()).await;
        span.record("outcome", outcome.as_str());
        outcome
    }
}

impl PeerRunSink<'_> {
    // Sequential resolve → econ-gate → open → bind → drive → classify pipeline; the
    // tracing macros and the success/failure classification inflate the
    // cognitive-complexity + line metrics past threshold, exactly as the buffered
    // `pull_from_candidate_in_span` twin does. Splitting it would scatter one linear flow.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn drive_run_in_span(&self, run: CoveredRun) -> RunOutcome {
        // Cancellation before the lane open (#1506). Each run opens its OWN lane
        // — `open_or_reuse_pool` can escrow a fresh `openPool` or fire a proactive
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
            .open_or_reuse_pool(provider_addr, POOL_OPEN_CALLER_BUDGET)
            .await
        {
            Ok(ctx) => ctx,
            // A pool-open failure is OUR payment-side problem, not the source's
            // fault. If it is a node-wide LOCAL fault (a broken buyer key, an
            // unreadable store), no other lane can fix it — terminal. Otherwise it
            // is per-provider (a contract revert): drop this source and re-plan.
            Err(err) => {
                return match record_pool_open_failure(self.deps, provider_addr, &err) {
                    PullMiss::LocalFault | PullMiss::FundingNeeded => {
                        RunOutcome::Terminal(FillError::new(format!(
                            "node-origin ranged pull: local buyer fault opening a lane: {err:#}"
                        )))
                    }
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
        tracing::Span::current().record("pool_id", tracing::field::display(pool_id));
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
            // Per-serve runtime: dial per leg (#1675).
            None,
        )
        .with_dial_runtime(self.deps.dial_runtime.clone());
        let peer_source = PrimedSource::new(TimedSource::new(
            peer_source,
            Arc::clone(&self.deps.metrics),
        ));
        self.adopt_primed(&peer_source, &run, pool_id, &ledger)
            .await;
        // The SAME shared downstream frontiers every run reads, so the demand
        // window is continuous across the sequential lanes.
        let pacing_wait = self.pacing_wait;
        let downstream_reader = move || pacing_wait.frontier();

        // The shared-pool spend this run's `drive` gates on (#1506): the
        // whole-pool committed spend: the sum over every live lane ledger for
        // `pool_id`, INCLUDING this run's own (seeded above via `lane_ledger`).
        // Without it, a later run whose lane has spent nothing sees
        // `committed == 0` and believes the whole deposit is unspent, then signs
        // a voucher the pool cannot back. The deposit itself rises only through
        // the assembly's funding recovery step, which a later run's lane reads
        // from the pool row `open_or_reuse_pool` refreshes.
        let ledgers = &self.deps.ledgers;
        let spent = move || ledgers.pool_committed(pool_id);
        let pool = SharedPool {
            spent: &spent,
            quotes: None,
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
                &ctx,
                &ledger,
                self.hash_bytes,
                run.offset,
                run.len,
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
        // A primed pull the drive never opened closes now, not when the run ends.
        peer_source.clear();
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
                // A clean leg that moved neither frontier (#2194) was served and
                // paid for, so a speculative run spends its warming allowance on it
                // as a clean run does. `classify_pull_failure` below meters and logs
                // it and suppresses the pair without scoring the source.
                if speculative && let Some(stuck) = err.downcast_ref::<LegNoProgress>() {
                    self.deps.config.warming.debit_speculative(
                        (*pk.as_bytes()).into(),
                        Hash::from_bytes(self.hash_bytes),
                        candidate.rate_per_mb.saturating_mul(mb_of(stuck.len)),
                    );
                }
                if is_bao_corruption(&err) {
                    tracing::warn!(
                        provider = %pk, %provider_addr,
                        "node ranged pull: upstream served bao-corrupt bytes; scoring Corruption"
                    );
                    record_outcome(self.deps, pk, &Outcome::Corruption);
                    self.deps.metrics.node_pull_through_upstream_verify_failed();
                } else if record_backpressure_refusal(self.deps, provider_addr, &err).is_some() {
                    // A refusal that usually clears with time (#2178): `assemble`
                    // decides whether to wait on this source or reassign.
                    return RunOutcome::Backpressure;
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
                if ends_the_assembly(&err) {
                    RunOutcome::Terminal(FillError::new(format!("{err:#}")))
                } else if classify(&err) == Fault::Unaffordable {
                    RunOutcome::Unfunded
                } else {
                    RunOutcome::Reassign
                }
            }
        }
    }
}

/// Whether a run's fault ends the whole assembly rather than moving its range
/// to another holder.
///
/// A fatal fault ([`classify`]: an over-cap blob, a local fault) cannot be
/// fixed by another lane, and neither can a fault of this node's own store
/// ([`super::is_local_store_fault`]): every holder's bytes land in it. A
/// funding refusal never ends the assembly: an open-time `Unfunded`, a funding
/// rejection and the pacer's [`decdn_client::PoolExhausted`] scope to the
/// source that met them ([`RunOutcome::Unfunded`]), and the assembly's funding
/// recovery step decides once no other holder serves. A rejection that healed
/// the lane ledger after the lane spent its resume budget
/// ([`decdn_client::HealExhausted`]) moves on too. Anything else is a property
/// of this source's delivery.
fn ends_the_assembly(err: &anyhow::Error) -> bool {
    if super::is_local_store_fault(err) {
        return true;
    }
    match classify(err) {
        Fault::Fatal(_) => true,
        Fault::Unaffordable | Fault::Source | Fault::Transient => false,
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

// ===========================================================================
// The UNPAID own-origin twin of the pull leg.
// ===========================================================================

/// Build the benign LOCAL [`PoolContext`] the driver carries as pure
/// bookkeeping for the unpaid leg (THE CRUX).
///
/// It signs NOTHING: the [`BackendSource`] quotes rate 0, so `drive` never prices,
/// issues, or sends a voucher, and the throwaway signer is never touched. The
/// large `deposit` keeps the pacer's `remaining_deposit` (`deposit −
/// committed.amount`, and `committed.amount` stays 0 at rate 0) permanently above
/// `next_voucher_cost` (also 0), so [`crate::pacer` `BudgetPacer`] never reaches
/// its refuse arm. Fresh priors: there is no prior pool state to
/// resume. `U256::MAX` is used, not a merely-large value, so no blob size can ever
/// bring the gap headroom below the (zero) voucher cost.
fn local_bookkeeping_ctx() -> PoolContext {
    // No provider is paid: rate 0 means no voucher is ever signed, so the
    // ZERO-provider signing guard is never reached on this local leg. Chain 0 /
    // zero contract: the domain is never used to sign either. It exists only to
    // build the context.
    PoolContext::new(
        B256::ZERO,
        U256::MAX,
        Arc::new(PrivateKeySigner::random()),
        decdn_incentive::voucher_domain(0, Address::ZERO),
    )
}

/// Run the range-minimized OWN-ORIGIN pull for `[offset, offset + len)` of `hash`
/// into `engine`'s cache via [`drive`] over an UNPAID [`BackendSource`], pacing the
/// pull with the same [`RampPacer`] the paid leg uses, built from
/// `credit_ramp_divisor`, `credit_floor`, `credit_max`, and `paid_carried` (ADR
/// 003 §Credit window / ADR 037). Records the terminal outcome via the shared [`FillSession::mark_ended`]. The
/// local twin of [`run_pull_leg`].
///
/// # What drops out relative to the paid [`run_pull_leg`]
///
/// This leg pulls from THIS node's own configured origin, reached through
/// [`CacheEngine::origin_range_wire`] behind the [`BackendSource`]. There is no
/// counterparty, so every paid-path axis is absent — and each absence is load-bearing,
/// not an omission:
///
/// - **No discovery / pool open / [`PeerSource`] / [`super::funder::NodeFunder`].** The bytes
///   are already reachable locally, so there is nothing to dial, no pool to open,
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
/// [`PoolLedger`] and, on `finish`, advances its `bytes` by exactly the leg's
/// drained wire (at amount 0). We hand `drive` that SAME ledger ([`BackendSource::ledger`])
/// plus a benign [`local_bookkeeping_ctx`], so the completion
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
    reason = "the pull leg's inputs are all owned so it can run on its own runtime"
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
    paid_carried: u64,
    total_bytes: u64,
    session: Arc<FillSession>,
    cancel: CancellationToken,
) {
    let hash_bytes = *hash.as_bytes();

    let admit_store = NodeAdmitStore::new(engine, hash, total_bytes, Some(Arc::clone(&session)));

    // The LOCAL bookkeeping axes (THE CRUX). The `ledger` is the SAME `Arc` the
    // source advances on `finish`, so the paid-frontier the gap loop reads for
    // completion tracks the wire this leg actually drained. The `ctx` is a benign
    // large-deposit / throwaway-signer context (never used to sign, rate 0).
    let ledger = source.ledger();
    let ctx = Arc::new(std::sync::Mutex::new(local_bookkeeping_ctx()));

    // The ramped credit-window pacer (ADR 003 §Credit window / ADR 037), IDENTICAL
    // to the paid leg's: bound on `served_paid` (#1610 + storage/egress exposure),
    // so the unpaid pull is still throttled to the real client's paid frontier.
    let pacer = RampPacer {
        divisor: credit_ramp_divisor,
        floor: credit_floor,
        credit_max,
        paid_base: session.served_start(),
        paid_carried,
    };
    let pacing_wait = DownstreamWait::for_session(&session, hash, Arc::clone(&metrics));
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
            &ctx,
            &ledger,
            hash_bytes,
            offset,
            len,
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

    // Classify a terminal error. There is no upstream, so a fault is ALWAYS local
    // (our own origin is corrupt/misconfigured, or a transport fault reaching it):
    // meter it as a local fault and NEVER score a provider or a bao-corruption against
    // an upstream that does not exist. Skipped on cancel (nobody waits).
    // An internal fault (an encode panic or a broken invariant) is a code bug, not
    // the origin's: say so, so the operator does not audit a healthy origin. A
    // clean leg that moved neither frontier gets its own counter and log for the
    // same reason.
    if !cancelled && let Err(err) = &result {
        if let Some(stuck) = err.downcast_ref::<LegNoProgress>() {
            // Not the origin's fault: the store or the bookkeeping ledger did not
            // record a leg that finished cleanly (#2194).
            super::warn_leg_no_progress(&metrics, *hash.as_bytes(), None, stuck);
        } else if is_internal_fault(err) {
            metrics.node_pull_local_fault();
            tracing::error!(
                %hash,
                error = %format_args!("{err:#}"),
                "own-origin pull leg failed on an internal fault (code bug), not the origin"
            );
        } else {
            metrics.node_pull_local_fault();
            tracing::warn!(
                %hash,
                error = %format_args!("{err:#}"),
                "own-origin pull leg failed; local-origin fault (no upstream to score)"
            );
        }
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
mod local_pull_leg_tests;

/// Regression coverage for the window-pause lost-wakeup that wedged
/// [`run_pull_leg`] / [`run_local_pull_leg`] under CI scheduling gaps (#1673).
/// [`DownstreamWait`] parks on an edge-triggered wakeup that stores no permit, so a
/// serve-leg advance that races the pacer's `Wait` decision must be caught by
/// re-reading the frontiers AFTER arming the waiter — not waited on forever.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod downstream_wait_tests;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod prime_tests;

#[cfg(test)]
mod backpressure_backoff_tests;

#[cfg(test)]
mod assembly_fault_tests;
