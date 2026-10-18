//! Where a fetch finds a blob's holders (#936, #1174, ADR 037): the pinned
//! node, the peer store's fast path, or a probe round over a registry sample,
//! ordered for failover with proxy-warming candidates first.
//!
//! Every probe answer is verified before it can influence the order (ADR 014
//! §1). A streamed round returns at the first verified holder and keeps its
//! pending probes as a tail, so a holder that answers later joins the running
//! fetch (`LateProbes`). The probe samples are harvested into the peer store
//! off the fetch's critical path (`spawn_harvest`).

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, Bytes};
use decdn_incentive::slash_judge_domain;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};
use tokio_util::task::AbortOnDropHandle;

use crate::UpstreamRateLimited;
use crate::discovery::{self, NodeCandidate};
use crate::probe::probe_once;
use crate::session::{ChainConfig, DiscoveryOptions, FetchMarks, ProxyWarmingParams, PullSite};

/// Per-candidate probe timeout during auto-discovery (#936). The K probes run
/// concurrently, so this bounds selection latency rather than the overall fetch
/// (`--timeout-ms`); a dead candidate falls out of selection after this.
const SELECT_PROBE_TIMEOUT_MS: u64 = 5_000;

/// Current unix time in microseconds (the requester-echoed `timestamp_us`).
pub(crate) fn micros_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros()),
    )
    .unwrap_or(u64::MAX)
}

/// The terminal "nothing can serve this blob" error for [`probe_and_order`],
/// naming the failure that actually happened (#1911). A candidate leaves the
/// selection loop four ways: it never answered (unreachable), it kept shedding
/// the probe with `APP_ERR_RATE_LIMITED` (rate-limited — reachable, but refusing
/// this client's probe rate), it answered but its `slash_sig` did not recover
/// (unverifiable), or it answered but was unusable — a `has_blob`/coverage
/// mismatch that no honest responder produces. When any probe was unverifiable
/// the cause is almost always local configuration rather than missing content,
/// so that case gets its own message; otherwise the parenthetical accounts for
/// each of the other three separately. Reached only when there is neither a
/// cache holder nor a reachable non-holder to pull through — a `has_blob:false`
/// answer is a serve target, not a failure, so it never lands here.
fn no_serve_target_error(
    probe_count: usize,
    unreachable: usize,
    rate_limited: usize,
    unverifiable: usize,
) -> anyhow::Error {
    if unverifiable > 0 {
        return anyhow::Error::new(ResolveConfigFault(format!(
            "{unverifiable} of {probe_count} probed node(s) answered, but their probe \
             signatures did not recover to the operator address each is registered \
             under. That is usually local configuration rather than missing content: \
             check that blockchain.slash_judge_address and blockchain.chain_id match \
             the deployment these nodes registered against"
        )));
    }
    // With no unverifiable candidate, every candidate that neither stayed silent
    // nor shed the probe answered but was dropped as unusable (has_blob/coverage
    // mismatch) — name each count so the message never implies a silent,
    // wholly-unreachable set when some replied.
    let unusable = probe_count
        .saturating_sub(unreachable)
        .saturating_sub(rate_limited);
    let shed = if rate_limited > 0 {
        format!(", {rate_limited} rate-limited this client's probes")
    } else {
        String::new()
    };
    anyhow::anyhow!(
        "none of the {probe_count} probed node(s) could serve the blob \
         ({unreachable} did not answer{shed}, {unusable} answered but were unusable)"
    )
}

/// The waits between probe attempts on a candidate that sheds the probe with
/// `APP_ERR_RATE_LIMITED`. ADR 005's per-peer limit refills one probe every
/// 200 ms, so these clear the burst that concurrent entries of one bundle pull
/// send at its start. A candidate that still sheds after the last wait counts
/// as rate-limited, never as silent.
const PROBE_SHED_BACKOFFS_MS: [u64; 3] = [250, 500, 1000];

/// Probe one candidate, probing it again after each [`PROBE_SHED_BACKOFFS_MS`]
/// wait while it sheds the probe. Any other result — an answer, a timeout, a
/// transport failure — returns at once.
async fn probe_candidate(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hash: [u8; 32],
    timestamp_us: u64,
) -> anyhow::Result<(
    decdn_protocol::message::ProbeResponse,
    decdn_protocol::ProbeResponseExt,
    crate::probe::ProbeRtt,
)> {
    let mut backoffs = PROBE_SHED_BACKOFFS_MS.iter();
    loop {
        let res = probe_once(
            endpoint,
            target.clone(),
            hash,
            timestamp_us,
            Duration::from_millis(SELECT_PROBE_TIMEOUT_MS),
        )
        .await;
        match (&res, backoffs.next()) {
            (Err(e), Some(&ms)) if probe_shed(e) => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            (Err(e), None) if probe_shed(e) => {
                tracing::info!(
                    "{} kept rate-limiting this client's probe: {e:#}",
                    target.id
                );
                return res;
            }
            _ => return res,
        }
    }
}

/// Whether a failed probe was the node shedding it with `APP_ERR_RATE_LIMITED`.
fn probe_shed(err: &anyhow::Error) -> bool {
    err.downcast_ref::<UpstreamRateLimited>().is_some()
}

/// How long a probe round keeps collecting answers after the first verified
/// holder answers. A holder that answers later is about 125 ms farther away,
/// still connecting, or still waiting for hole punching to select a direct
/// path (`probe_once` waits up to 500 ms for one after its exchange), and would
/// not lead the order. One slow connect would otherwise hold the whole round
/// for up to the probe timeout.
const PROBE_SETTLE_AFTER_HOLDER: Duration = Duration::from_millis(250);

/// Run `probes` concurrently and collect their outcomes until every probe has
/// answered or the round's deadline passes. The first outcome `is_holder`
/// accepts sets the deadline `grace` later. With `cold_grace` set and no
/// holder yet, the first outcome `is_answer` accepts sets it `cold_grace`
/// later, and a holder inside that window still ends the round `grace` after
/// it. Returns the collected outcomes, in answer order, and the probes still
/// pending. A zero `grace` returns at the first holder with every outcome
/// already ready: `timeout_at` polls the stream before it checks the deadline.
/// With no holder and no `cold_grace`, the round waits for every probe and the
/// tail is empty.
async fn settle_probes<T, F>(
    probes: impl IntoIterator<Item = F>,
    grace: Duration,
    cold_grace: Option<Duration>,
    is_holder: impl Fn(&T) -> bool,
    is_answer: impl Fn(&T) -> bool,
) -> (Vec<T>, futures_util::stream::FuturesUnordered<F>)
where
    F: std::future::Future<Output = T>,
{
    use futures_util::StreamExt as _;
    let mut pending: futures_util::stream::FuturesUnordered<F> = probes.into_iter().collect();
    let mut settled = Vec::with_capacity(pending.len());
    let mut deadline: Option<tokio::time::Instant> = None;
    let mut holder_seen = false;
    loop {
        let next = match deadline {
            None => pending.next().await,
            Some(at) => match tokio::time::timeout_at(at, pending.next()).await {
                Ok(next) => next,
                Err(_) => break,
            },
        };
        let Some(outcome) = next else { break };
        let now = tokio::time::Instant::now();
        if !holder_seen && is_holder(&outcome) {
            holder_seen = true;
            let at = now + grace;
            deadline = Some(deadline.map_or(at, |cold| cold.min(at)));
        } else if deadline.is_none()
            && let Some(cold) = cold_grace
            && is_answer(&outcome)
        {
            deadline = Some(now + cold);
        }
        settled.push(outcome);
    }
    if !pending.is_empty() {
        tracing::debug!(
            pending = pending.len(),
            holder = holder_seen,
            "probe round settled; the rest continue as its tail"
        );
    }
    (settled, pending)
}

/// Log one probe's raw result at `debug`, with the time since the round started.
fn log_probe_result(
    cand: &NodeCandidate,
    res: &anyhow::Result<(
        decdn_protocol::message::ProbeResponse,
        decdn_protocol::ProbeResponseExt,
        crate::probe::ProbeRtt,
    )>,
    started: std::time::Instant,
) {
    match res {
        Ok((resp, _, rtt)) => tracing::debug!(
            node = %cand.node_id,
            elapsed_ms = started.elapsed().as_millis(),
            rtt_ms = rtt.ms,
            rtt_direct = rtt.direct,
            has_blob = resp.body.has_blob,
            "probe answered"
        ),
        Err(error) => tracing::debug!(
            node = %cand.node_id,
            elapsed_ms = started.elapsed().as_millis(),
            error = %format_args!("{error:#}"),
            "probe failed"
        ),
    }
}

/// One probed candidate, verified and classified ([`classify_probe`]).
pub(crate) enum ProbeOutcome {
    /// A verified answer that holds the blob, with its peer-store sample.
    Holder(discovery::Probed, (PublicKey, f64, u64)),
    /// A verified answer that does not hold the blob, with its peer-store
    /// sample.
    NonHolder(discovery::WarmingCandidate, (PublicKey, f64, u64)),
    /// No answer.
    Unreachable,
    /// The candidate kept shedding the probe.
    RateLimited,
    /// The answer's `slash_sig` did not recover to the candidate's operator.
    Unverifiable,
    /// The answer's `has_blob` and coverage disagree.
    Unusable,
}

impl ProbeOutcome {
    const fn is_holder(&self) -> bool {
        matches!(self, Self::Holder(..))
    }

    /// A verified answer, holder or not.
    const fn is_answer(&self) -> bool {
        matches!(self, Self::Holder(..) | Self::NonHolder(..))
    }

    /// The peer-store sample of a verified answer.
    pub(crate) const fn sample(&self) -> Option<(PublicKey, f64, u64)> {
        match self {
            Self::Holder(_, sample) | Self::NonHolder(_, sample) => Some(*sample),
            Self::Unreachable | Self::RateLimited | Self::Unverifiable | Self::Unusable => None,
        }
    }
}

/// How a probe round ends once a verified holder answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeRound {
    /// Return at the first verified holder and keep the pending probes as
    /// the round's tail (`LateProbes`): the first round of a fetch, whose
    /// late holders join the running fetch.
    Stream,
    /// Collect for `PROBE_SETTLE_AFTER_HOLDER` after the first verified
    /// holder and drop the rest: a rediscovery.
    Settle,
}

impl ProbeRound {
    /// How long the round collects after the first verified holder.
    const fn grace(self) -> Duration {
        match self {
            Self::Stream => Duration::ZERO,
            Self::Settle => PROBE_SETTLE_AFTER_HOLDER,
        }
    }

    /// How long the round collects after its first verified answer while no
    /// holder has answered. A streamed round then starts on the non-holders
    /// in hand as pull-through targets (#1911), and a holder that answers
    /// later joins the running fetch. A settled round waits for every probe.
    const fn cold_grace(self) -> Option<Duration> {
        match self {
            Self::Stream => Some(PROBE_SETTLE_AFTER_HOLDER),
            Self::Settle => None,
        }
    }
}

/// The probes a streamed round left pending, with what deciding a late
/// answer needs.
pub(crate) struct LateProbes {
    /// The pending probes, each yielding its classified outcome.
    pub(crate) tail: std::pin::Pin<Box<dyn futures_util::Stream<Item = ProbeOutcome> + Send>>,
    /// The lowest RTT among the holders the round returned.
    pub(crate) best_holder_rtt_ms: f64,
    /// The fetch's proxy-warming knobs.
    pub(crate) warming: ProxyWarmingParams,
}

/// How a resolve probes: where it records its marks, and how its probe round
/// ends.
#[derive(Clone, Copy)]
pub struct ProbeOpts<'a> {
    /// Where the round reports its start; `None` reports nothing.
    pub marks: Option<&'a dyn FetchMarks>,
    /// How the probe round ends.
    pub round: ProbeRound,
}

impl std::fmt::Debug for ProbeOpts<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeOpts")
            .field("marks", &self.marks.is_some())
            .field("round", &self.round)
            .finish()
    }
}

/// A streamed round's [`LateProbes`], taken once by whoever adopts them.
#[derive(Default)]
pub(crate) struct LateSlot(std::sync::Mutex<Option<LateProbes>>);

impl LateSlot {
    /// A slot holding `late`.
    pub(crate) const fn new(late: LateProbes) -> Self {
        Self(std::sync::Mutex::new(Some(late)))
    }

    /// Take the late probes, leaving the slot empty.
    pub(crate) fn take(&self) -> Option<LateProbes> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

/// A probe round's outcomes, split by what [`probe_and_order`] does with each.
struct ProbeTally {
    /// The verified holders.
    holders: Vec<discovery::Probed>,
    /// Probed bonded nodes that answered `has_blob:false` — reachable, with a
    /// measured RTT, but not holding the blob in their cache store. They serve
    /// two roles: the proxy-warming candidate pool (ADR 037 § Candidate pool)
    /// when a holder exists but is distant, and — when NO holder answers — the
    /// pull-through serve targets a cold blob's first fetch bootstraps from
    /// (#1911), since `has_blob:false` from the cache store does not mean the
    /// node cannot serve via its own origin. Always collected, because the
    /// second role does not depend on warming being on.
    non_holders: Vec<discovery::WarmingCandidate>,
    /// `(node_id, rtt_ms, rate_per_mb)` for each verified responder, harvested
    /// into the peer store off the critical path (`spawn_harvest`, called from
    /// `discover_provider`).
    probed_samples: Vec<(PublicKey, f64, u64)>,
    /// The ways a candidate drops out, counted separately: the terminal error
    /// has to name the one that actually happened. A wrong
    /// `slash_judge_address` or `chain_id` makes EVERY honest node fail
    /// verification, and reporting that as "nobody holds the blob" sends the
    /// operator hunting for missing content instead of a local
    /// misconfiguration; a node shedding this client's probe rate is
    /// reachable, and reporting it as silent points at the network instead.
    unreachable: usize,
    rate_limited: usize,
    unverifiable: usize,
}

impl ProbeTally {
    fn of(outcomes: Vec<ProbeOutcome>) -> Self {
        let mut tally = Self {
            holders: Vec::new(),
            non_holders: Vec::new(),
            probed_samples: Vec::new(),
            unreachable: 0,
            rate_limited: 0,
            unverifiable: 0,
        };
        for outcome in outcomes {
            match outcome {
                ProbeOutcome::Holder(holder, sample) => {
                    tally.probed_samples.push(sample);
                    tally.holders.push(holder);
                }
                ProbeOutcome::NonHolder(candidate, sample) => {
                    tally.probed_samples.push(sample);
                    tally.non_holders.push(candidate);
                }
                ProbeOutcome::Unreachable => tally.unreachable += 1,
                ProbeOutcome::RateLimited => tally.rate_limited += 1,
                ProbeOutcome::Unverifiable => tally.unverifiable += 1,
                ProbeOutcome::Unusable => {}
            }
        }
        tally
    }
}

/// Verify `cand`'s probe result and classify it. Every response is verified
/// before it can influence the order (ADR 014 §1). A failure is requester-local
/// policy, with no reputation effect, because an unrecovered signature
/// attributes nothing to anyone.
fn classify_probe(
    cand: &NodeCandidate,
    res: anyhow::Result<(
        decdn_protocol::message::ProbeResponse,
        decdn_protocol::ProbeResponseExt,
        crate::probe::ProbeRtt,
    )>,
    hash: [u8; 32],
    timestamp_us: u64,
    slash_domain: &alloy::sol_types::Eip712Domain,
) -> ProbeOutcome {
    let (resp, resp_ext, rtt) = match res {
        Ok(answer) => answer,
        Err(e) if probe_shed(&e) => return ProbeOutcome::RateLimited,
        Err(_) => return ProbeOutcome::Unreachable,
    };
    if let Err(e) = crate::probe::verify_probe_response(
        &resp,
        cand.eth_address,
        slash_domain,
        hash,
        timestamp_us,
    ) {
        // A candidate silently vanishing from selection is exactly what the
        // operator needs told, so log why at `warn`.
        tracing::warn!(
            "dropping an unverifiable probe response from {}: {e}",
            cand.node_id
        );
        return ProbeOutcome::Unverifiable;
    }
    // #1506: `has_blob` and `coverage.is_empty()` are a biconditional by
    // construction on an honest responder, except for the empty root, whose
    // coverage is empty either way (#2471). `coverage` is not in the signed
    // set, and an inconsistency has no attributable author, so drop the
    // candidate rather than score it.
    if !resp_ext.consistent_with(&resp.body) {
        tracing::warn!(
            "dropping a probe response from {} with has_blob/coverage mismatch",
            cand.node_id
        );
        return ProbeOutcome::Unusable;
    }
    let rtt_ms = rtt.ms;
    // Every verified responder contributes a probe sample, holder or not.
    let sample = (cand.node_id, rtt_ms, resp.body.rate_per_mb);
    if resp.body.has_blob {
        ProbeOutcome::Holder(
            discovery::Probed {
                candidate: cand.clone(),
                rtt_ms,
                total_bytes: resp_ext.total_bytes,
                coverage: resp_ext.coverage,
            },
            sample,
        )
    } else {
        ProbeOutcome::NonHolder(
            discovery::WarmingCandidate {
                node_id: cand.node_id,
                eth_address: cand.eth_address,
                rtt_ms,
                multiaddrs: cand.multiaddrs.clone(),
            },
            sample,
        )
    }
}

/// Probe `candidates` for `hash` over `endpoint` and return the ordered
/// provider-failover list (#1174, ADR 037 § Fallback): the sequence `fetch`
/// tries in turn, each entry a fallback for the one before it, until one
/// delivers the blob. Errors only when no probed candidate is reachable to serve
/// at all — a cache holder OR a bonded non-holder that can pull through (#1911).
///
/// Every response is verified before it can influence the order: value
/// invariants, echoed-field correlation, and `slash_sig` recovery to the
/// candidate's on-chain operator address (ADR 014 §1). Selection reads
/// `has_blob` and coverage off the response and pairs them with the measured
/// RTT; `rate_per_mb` is harvested into `probed_samples` for the peer store
/// only, never ranked on — the fetch pays the stream's own signed quote, gated
/// by `--max-rate-per-mb`. `has_blob` and the harvested rate are only
/// meaningful once the signature attributes them to the peer (coverage is
/// unsigned; the consistency check below ties it to `has_blob`): an unverified
/// quote is a claim no one is accountable for, so a node could seed the store
/// with a rate it never committed to. A response that fails is
/// dropped and its candidate skipped, exactly as for a timeout; it is
/// requester-local policy and never scored against the peer, since a signature
/// that does not recover attributes nothing to anyone.
///
/// Every candidate is probed on equal footing: opening cost is
/// provider-independent, because the caller has ONE pool that fans out to
/// every provider (ADR 003), so there is no per-provider "already funded"
/// distinction to prefer.
///
/// The round ends `PROBE_SETTLE_AFTER_HOLDER` after the first verified
/// holder answers, or when every probe has answered. A holder the cutoff drops
/// stays reachable through the fetch's later discovery.
///
/// The order is proxy-warming candidates first (nearest RTT first, ADR 037 §
/// Client selection policy) when warming is enabled and engages, then the
/// holders nearest RTT first. A caller that walks it therefore gets ADR 037's
/// exact fallback shape: the chosen proxy, then the next candidate, and finally
/// the direct holder — routing around a proxy that declines or stalls without
/// ever surfacing an error while a holder remains.
///
/// When no candidate holds the blob, the order is instead the reachable
/// non-holders, nearest RTT first, as pull-through serve targets — see
/// `failover_order` for why an empty holder set bootstraps rather than fails.
pub async fn probe_and_order(
    endpoint: &Endpoint,
    candidates: &[NodeCandidate],
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
    warming: ProxyWarmingParams,
    slash_domain: &alloy::sol_types::Eip712Domain,
    round: ProbeRound,
) -> anyhow::Result<ResolvedTargets> {
    let timestamp_us = micros_now();
    // Probe concurrently in one task. `probe_once`'s internal timeout bounds
    // each leg; a candidate that sheds the probe is probed again after a short
    // wait (`probe_candidate`). Each probe verifies and classifies its own
    // answer, so the round ends only on a verified holder. Each probe owns its
    // inputs, so a streamed round's pending probes outlive this call.
    let started = std::time::Instant::now();
    let probes = candidates.iter().cloned().map(|cand| {
        let target = probe_target(&cand, relay_hint.cloned());
        let endpoint = endpoint.clone();
        let slash_domain = slash_domain.clone();
        async move {
            let res = probe_candidate(&endpoint, target, hash, timestamp_us).await;
            log_probe_result(&cand, &res, started);
            classify_probe(&cand, res, hash, timestamp_us, &slash_domain)
        }
    });
    let (outcomes, tail) = settle_probes(
        probes,
        round.grace(),
        round.cold_grace(),
        ProbeOutcome::is_holder,
        ProbeOutcome::is_answer,
    )
    .await;

    let ProbeTally {
        holders,
        non_holders,
        probed_samples,
        unreachable,
        rate_limited,
        unverifiable,
    } = ProbeTally::of(outcomes);

    // Terminal only when NOTHING can serve — no holder AND no reachable
    // non-holder to pull through. An empty holder set alone is not terminal: a
    // cold blob is origin-only with zero cache holders, the normal first-fetch
    // state (#1911), so as long as one bonded node answered it is a serve target.
    if holders.is_empty() && non_holders.is_empty() {
        return Err(no_serve_target_error(
            candidates.len(),
            unreachable,
            rate_limited,
            unverifiable,
        ));
    }

    if holders.is_empty() {
        // No cache holder, but reachable non-holders can serve via pull-through.
        // Log why the fetch is talking to nodes that answered `has_blob:false`.
        tracing::info!(
            "no node that has answered holds the blob in cache; starting on {} reachable \
             bonded non-holder(s) as pull-through serve targets — a node serves an authorized \
             miss from its own origin (#1911), and a holder that answers later joins the fetch",
            non_holders.len()
        );
    }

    let size_hint = nearest_size_hint(&holders);
    let late = late_slot(round, tail, &holders, warming);
    let ordered = failover_order(holders, &non_holders, warming);
    if let Some((node_id, proxy_rtt, best_holder_rtt)) = ordered.warming_lead {
        tracing::info!(
            "proxy-warming: routing through nearer non-holder {node_id} ({proxy_rtt:.1}ms) \
             instead of the best holder ({best_holder_rtt:.1}ms) to seed a regional copy, \
             falling back to the holder if it declines (ADR 037)",
        );
    }
    Ok(ResolvedTargets {
        candidates: ordered.order,
        coverage_by_node: ordered.coverage_by_node,
        probed_samples,
        pinned: false,
        size_hint,
        late,
    })
}

/// A streamed round's pending probes as its [`LateSlot`], with the best RTT
/// among the `holders` it returned. Empty for a settled round or an empty
/// tail.
///
/// A task drives the probes from here on, so they keep running and timing
/// while the fetch unlocks its signer and opens its pool before anything reads
/// the tail. Their answers wait in a channel. The task stops when the tail
/// stream is dropped, so no probe outlives the fetch that adopted it.
fn late_slot<F>(
    round: ProbeRound,
    tail: futures_util::stream::FuturesUnordered<F>,
    holders: &[discovery::Probed],
    warming: ProxyWarmingParams,
) -> LateSlot
where
    F: std::future::Future<Output = ProbeOutcome> + Send + 'static,
{
    use futures_util::StreamExt as _;
    if round != ProbeRound::Stream || tail.is_empty() {
        return LateSlot::default();
    }
    let best_holder_rtt_ms = holders
        .iter()
        .map(|h| h.rtt_ms)
        .fold(f64::INFINITY, f64::min);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let task = AbortOnDropHandle::new(tokio::spawn(tail.for_each(move |outcome| {
        // A send fails only once the tail stream is gone, which aborts this
        // task too.
        let _ = tx.send(outcome);
        std::future::ready(())
    })));
    let answers = futures_util::stream::unfold((rx, task), |(mut rx, task)| async move {
        rx.recv().await.map(|outcome| (outcome, (rx, task)))
    });
    LateSlot::new(LateProbes {
        tail: Box::pin(answers),
        best_holder_rtt_ms,
        warming,
    })
}

/// The size hint of the nearest holder that gave one: the fetch's first size
/// claim ([`ResolvedTargets::size_hint`]).
fn nearest_size_hint(holders: &[discovery::Probed]) -> Option<u64> {
    holders
        .iter()
        .filter(|h| h.total_bytes.is_some())
        .min_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms))
        .and_then(|h| h.total_bytes)
}

/// The ordered provider-failover list plus, when a proxy leads it, that proxy's
/// identity for the operator log line. Split from [`probe_and_order`] as a pure
/// function so the ordering is unit-tested without live probing.
struct FailoverOrder {
    /// The candidates to try in turn: proxy-warming non-holders first (nearest
    /// RTT first) when warming engages, then the holders nearest RTT first — or,
    /// when no holder answered, the reachable non-holders nearest RTT first as
    /// pull-through serve targets (#1911).
    order: Vec<NodeCandidate>,
    /// `Some((proxy_node_id, proxy_rtt_ms, best_holder_rtt_ms))` when a warming
    /// proxy is prepended; `None` when the list is just the holders.
    warming_lead: Option<(PublicKey, f64, f64)>,
    /// Each probed holder's measured [`decdn_protocol::Coverage`] (#1506),
    /// keyed by `node_id`. Proxy-warming candidates never appear here — they
    /// are non-holders by definition, so a lookup miss on them (and on any
    /// node this map otherwise has no entry for) means "no measured
    /// coverage", which the multi-source lane builder treats as a full
    /// holder rather than as a gap in the data.
    coverage_by_node: HashMap<PublicKey, decdn_protocol::Coverage>,
}

/// Assemble the failover order (#1174, ADR 037 § Client selection policy) from
/// the probed `holders` and the `non_holders` — probed bonded nodes that
/// answered `has_blob:false`.
///
/// The holders form the backbone, nearest RTT first — and, absent proxy warming,
/// the whole list. When warming is enabled and the best holder is distant, the
/// non-holders that beat it by the margin are PREPENDED nearest first, so the
/// request routes through the nearest one (it serves via window-paced
/// pull-through and becomes the first regional copy) and falls over through the
/// remaining proxies to the direct holder. RTT-only ranking; never a gamble (an
/// empty proxy order leaves the list as just the holders).
///
/// When `holders` is empty the blob is cache-cold — its normal first-fetch state,
/// since every object is born origin-only with zero cache holders (#1911). The
/// order is then the `non_holders`, nearest RTT first, as real pull-through serve
/// targets: a node serves an authorized miss from its own origin (or node-to-node)
/// even when its cache store answers `has_blob:false`. This fallback is
/// independent of proxy warming — it is how a caching network bootstraps cold
/// content, not a latency optimization — so it applies whether warming is on or
/// off. `warming_lead` and `coverage_by_node` are both empty here,
/// since no holder answered.
fn failover_order(
    mut holders: Vec<discovery::Probed>,
    non_holders: &[discovery::WarmingCandidate],
    warming: ProxyWarmingParams,
) -> FailoverOrder {
    if holders.is_empty() {
        let mut cold = non_holders.iter().collect::<Vec<_>>();
        cold.sort_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms));
        let order = cold
            .iter()
            .map(|c| NodeCandidate {
                node_id: c.node_id,
                eth_address: c.eth_address,
                // As for a proxy lead: `region_hint` never rides along on a
                // candidate the client assembled from probe RTTs alone.
                region_hint: None,
                // Registry addresses ride along so a cold-order target dials
                // directly (ADR 001 § Node Discovery).
                multiaddrs: c.multiaddrs.clone(),
            })
            .collect();
        return FailoverOrder {
            order,
            warming_lead: None,
            coverage_by_node: HashMap::new(),
        };
    }
    holders.sort_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms));
    // Captured before `holders` is consumed into `order` below — each probed
    // holder's real coverage, for the multi-source lane builder (#1506).
    let coverage_by_node: HashMap<PublicKey, decdn_protocol::Coverage> = holders
        .iter()
        .map(|h| (h.candidate.node_id, h.coverage.clone()))
        .collect();
    let best_holder_rtt = holders
        .iter()
        .map(|h| h.rtt_ms)
        .fold(f64::INFINITY, f64::min);
    let proxy_order = if warming.enabled {
        discovery::proxy_warming_order(
            best_holder_rtt,
            warming.rtt_threshold_ms,
            warming.margin_ms,
            non_holders,
        )
    } else {
        Vec::new()
    };
    let warming_lead = proxy_order
        .first()
        .map(|nearest| (nearest.node_id, nearest.rtt_ms, best_holder_rtt));
    let proxies = proxy_order.iter().map(|proxy| NodeCandidate {
        node_id: proxy.node_id,
        eth_address: proxy.eth_address,
        // Region deliberately dropped rather than carried over: ADR 037
        // §"Ranking key is measured RTT only" forbids region from influencing
        // warming, and `region_hint` is only ever read by `select_candidates`'
        // pre-probe shortlist and operator logging. Leaving it unset keeps a
        // spoofed region from riding along.
        region_hint: None,
        // Registry addresses ride along so a warming proxy dials directly
        // (ADR 001 § Node Discovery).
        multiaddrs: proxy.multiaddrs.clone(),
    });
    let order = proxies
        .chain(holders.iter().map(|h| h.candidate.clone()))
        .collect();
    FailoverOrder {
        order,
        warming_lead,
        coverage_by_node,
    }
}

/// Auto-discover the failover order to fetch `hash` from (#936): read the
/// active set from `CapacityBond`, take a random, region-first sample of
/// [`discovery::SELECT_K`] candidates, and [`probe_and_order`] them. Returns the
/// ordered candidate list `fetch` tries in turn (#1174).
/// Callers must have already unwrapped `chain.capacity_bond` into the
/// "auto-discovery needs `capacity_bond_address`" error, which is why the
/// address is a separate parameter rather than read back off `chain`.
async fn discover_provider(
    endpoint: &Endpoint,
    (chain, site): (&ChainConfig, &PullSite),
    capacity_bond: Address,
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
    opts: &DiscoveryOptions,
    probe: ProbeOpts<'_>,
) -> anyhow::Result<ResolvedTargets> {
    let mark_probe_start = || {
        if let Some(marks) = probe.marks {
            marks.probe_started();
        }
    };
    // Probe-less fast path: when the store already holds enough fresh,
    // unsuppressed candidates, skip the network probe round entirely and rank
    // by the store's own EWMA latency. Falls through to today's bootstrap +
    // probe path whenever the store can't back a full candidate set — never a
    // new failure mode, only a possible extra probe round.
    let peer_store = crate::PeerStore::open(&site.data_dir);
    let store_cfg = crate::StoreConfig::default();
    if !opts.rediscover
        && let Some(targets) =
            store_fast_path(&peer_store, &store_cfg, opts.max_sources, now_secs())
    {
        return Ok(targets);
    }
    // Registry read-skip (identity-fresh, latency-stale): when the store still
    // holds enough recently-confirmed identities, build the candidate set from
    // them and re-probe for fresh RTT, issuing NO `getRegisteredNodes`. This
    // decouples "skip the registry read" (bounded by the 24h identity horizon)
    // from "skip the probe" (bounded by the 10-min latency TTL, the fast path
    // above). Falls through to the registry read when too few identities are
    // fresh. `--rediscover` forces the read (checked above alongside the fast
    // path).
    if !opts.rediscover {
        let cached = identity_fresh_candidates(&peer_store, &store_cfg, now_secs());
        let selected = select_with_widening(
            cached,
            site.region.as_deref(),
            site.region_allowlist.as_slice(),
            &store_cfg,
        );
        if selected.len() >= store_cfg.min_fresh_candidates {
            let warming = opts.proxy_warming;
            let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
            mark_probe_start();
            let targets = probe_and_order(
                endpoint,
                &selected,
                relay_hint,
                hash,
                warming,
                &slash_dom,
                probe.round,
            )
            .await?;
            // Stats-only harvest, never identity — identity refreshes ONLY on a
            // real registry read (same reasoning as the `Bootstrap::Cached`
            // path): re-`upsert_identity` here would "confirm" identity against
            // the store itself, reset `identity_seen_at_secs`, and keep the 24h
            // refresh horizon from ever expiring.
            drop(spawn_harvest(
                &site.data_dir,
                Vec::new(),
                targets.probed_samples.clone(),
            ));
            return Ok(targets);
        }
    }
    let bootstrap = discovery::bootstrap_nodes(
        &chain.rpc_url,
        capacity_bond,
        &site.data_dir,
        opts.registry_timeout,
    )
    .await?;
    // Captured before `bootstrap` is consumed: the registry-outage fallback
    // (`Bootstrap::Cached`, the peer store's surviving identities) must
    // IGNORE `site.region_allowlist` — the client is already
    // degraded to whatever the store still has, and narrowing that further
    // by region risks starving the fetch entirely over data that is already
    // possibly stale.
    let is_live_registry = matches!(bootstrap, discovery::Bootstrap::Live { .. });
    // `decdn-client` returns the provenance rather than logging it itself; a
    // silently stale peer list is exactly what the operator needs told, so
    // surface it here at `warn`.
    if let Some(warning) = bootstrap.warning() {
        tracing::warn!("{warning}");
    }
    let all = bootstrap.into_peers();
    if all.is_empty() {
        anyhow::bail!("no active nodes in the CapacityBond registry at {capacity_bond}");
    }
    let allow: &[decdn_protocol::Region] = if is_live_registry {
        site.region_allowlist.as_slice()
    } else {
        &[]
    };
    let selected = select_with_widening(all, site.region.as_deref(), allow, &store_cfg);
    let warming = opts.proxy_warming;
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
    mark_probe_start();
    let targets = probe_and_order(
        endpoint,
        &selected,
        relay_hint,
        hash,
        warming,
        &slash_dom,
        probe.round,
    )
    .await?;
    // Identity is harvested ONLY against a live registry read, and
    // `resolve_bootstrap` already does that: on a `Bootstrap::Live` read it
    // upserts+prunes identity for every returned node. So the harvest here
    // writes stats only and never identity. Two reasons:
    //   - On the `Bootstrap::Cached` outage path the returned peer set (`all`) IS
    //     the store's own surviving identities; re-`upsert_identity`-ing them would
    //     "confirm" identity against the store itself and reset
    //     `identity_seen_at_secs` — contradicting its meaning ("last confirmed
    //     against the registry") and keeping a departed node from ever becoming
    //     `identity_prunable` while outages recur.
    //   - On the live path it would be a redundant second identity write over
    //     what `resolve_bootstrap` already wrote.
    // The probed stats are harvested on both paths.
    //
    // Off the fetch's critical path: the handle is intentionally dropped,
    // never awaited here (tests await it directly for determinism).
    drop(spawn_harvest(
        &site.data_dir,
        Vec::new(),
        targets.probed_samples.clone(),
    ));
    Ok(targets)
}

/// Build a probe-less [`ResolvedTargets`] straight from the peer store: rank
/// every [`crate::PeerRecord::selectable`] record by its
/// EWMA `latency_ms` (ascending, `None` sorts last), project each back to a
/// [`NodeCandidate`], and admit per-operator via
/// [`discovery::admit_sources`]. Returns `None` when fewer than
/// `cfg.min_fresh_candidates` records are selectable, or when admission still
/// leaves the set below that floor — either way the caller falls back to the
/// probe path unchanged. Takes no [`Endpoint`], so it structurally issues no
/// network probe.
fn store_fast_path(
    store: &crate::PeerStore,
    cfg: &crate::StoreConfig,
    max_sources: usize,
    now_secs: u64,
) -> Option<ResolvedTargets> {
    // Require BOTH a fresh, unsuppressed latency sample AND a non-prunable
    // identity — symmetric with `resolve_bootstrap`'s outage-fallback filter. A
    // stats-only placeholder that `record_sample` created for an unknown peer
    // carries `eth_address: Address::ZERO` and `identity_seen_at_secs == 0`, so
    // it is always `identity_prunable` and can never project a `0x0` payment
    // lane into a fetch here.
    let mut fresh: Vec<_> = store
        .load_all()
        .into_iter()
        .filter(|r| r.selectable(now_secs, cfg) && !r.identity_prunable(now_secs, cfg))
        .collect();
    if fresh.len() < cfg.min_fresh_candidates {
        return None;
    }
    fresh.sort_by(|a, b| {
        a.latency_ms
            .unwrap_or(f64::MAX)
            .total_cmp(&b.latency_ms.unwrap_or(f64::MAX))
    });
    let ordered: Vec<NodeCandidate> = fresh.iter().map(crate::PeerRecord::as_candidate).collect();
    let candidates = discovery::admit_sources(ordered, max_sources);
    if candidates.len() < cfg.min_fresh_candidates {
        return None;
    }
    Some(ResolvedTargets {
        candidates,
        coverage_by_node: HashMap::new(),
        probed_samples: Vec::new(),
        pinned: false,
        size_hint: None,
        late: LateSlot::default(),
    })
}

/// Gather the cached peers whose identity is fresh enough to build a candidate
/// set from without re-reading the registry: identity confirmed within
/// [`crate::StoreConfig::identity_refresh_secs`], not
/// failure-suppressed, and not past the prune horizon. Latency freshness is NOT
/// required — this is exactly the identity-fresh/latency-stale case that
/// `store_fast_path` rejects; the caller re-probes these records for a fresh
/// RTT. Returns them projected to [`NodeCandidate`], unranked (the probe orders
/// them); empty when none qualify, letting the caller fall through to the
/// registry read. A bundle pull reads its run-wide candidate list from the same
/// set.
pub fn identity_fresh_candidates(
    store: &crate::PeerStore,
    cfg: &crate::StoreConfig,
    now_secs: u64,
) -> Vec<NodeCandidate> {
    store
        .load_all()
        .into_iter()
        .filter(|r| {
            r.identity_fresh(now_secs, cfg)
                && !r.identity_prunable(now_secs, cfg)
                && !r.failure_suppressed(now_secs, cfg)
        })
        .map(|r| r.as_candidate())
        .collect()
}

/// Region-filter a candidate set to at most [`discovery::SELECT_K`], then
/// progressively widen: a too-thin region allowlist must never starve the fetch,
/// so re-run unfiltered over the same set when the filtered pool falls below the
/// store's freshness floor. Shared by the live registry path and the
/// registry-read-skip path so both rank identically.
fn select_with_widening(
    candidates: Vec<NodeCandidate>,
    region: Option<&str>,
    allow: &[decdn_protocol::Region],
    cfg: &crate::StoreConfig,
) -> Vec<NodeCandidate> {
    // Only a non-empty allowlist can trigger the widening re-run, so keep the
    // unfiltered copy only in that case — the common empty-allowlist path (no
    // filtering, no widening possible) does no extra allocation.
    let widen_fallback = (!allow.is_empty()).then(|| candidates.clone());
    let selected =
        discovery::select_candidates_filtered(candidates, region, discovery::SELECT_K, allow);
    if let Some(widened) = widen_fallback
        && selected.len() < cfg.min_fresh_candidates
    {
        return discovery::select_candidates_filtered(widened, region, discovery::SELECT_K, &[]);
    }
    selected
}

/// Persist a discovery session's identity + probe stats off the fetch's
/// critical path: `upsert_identity` for every registry candidate (whether or
/// not it was probed), `record_sample` for each probed triple, then
/// `prune_and_cap` to bound store growth. Never awaited on the fetch path —
/// the returned handle exists so tests can await it for determinism.
pub(crate) fn spawn_harvest(
    data_dir: &Path,
    registry: Vec<NodeCandidate>,
    probed: Vec<(PublicKey, f64, u64)>,
) -> tokio::task::JoinHandle<()> {
    let dir = data_dir.to_path_buf();
    tokio::spawn(async move {
        let store = crate::PeerStore::open(&dir);
        let cfg = crate::StoreConfig::default();
        harvest(&store, &registry, probed, now_secs(), &cfg);
    })
}

/// How many per-peer record writes one [`harvest`] made, and how many failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HarvestTally {
    /// Identity and sample writes attempted.
    writes: usize,
    /// Of those, the writes the store refused.
    failed: usize,
}

/// The body of [`spawn_harvest`]. Probe samples are the only input to the
/// peer store's latency ranking, so a refused write is logged, not dropped:
/// each one at `debug` with its peer, and once at `warn` when the store
/// refuses every record write, because the ranking then stays frozen.
fn harvest(
    store: &crate::PeerStore,
    registry: &[NodeCandidate],
    probed: Vec<(PublicKey, f64, u64)>,
    now: u64,
    cfg: &crate::StoreConfig,
) -> HarvestTally {
    let mut tally = HarvestTally {
        writes: 0,
        failed: 0,
    };
    let mut file = |node_id: &PublicKey, what: &str, written: anyhow::Result<()>| {
        tally.writes = tally.writes.saturating_add(1);
        if let Err(e) = written {
            tally.failed = tally.failed.saturating_add(1);
            tracing::debug!(%node_id, "could not record {what} in the peer store: {e:#}");
        }
    };
    for cand in registry {
        file(
            &cand.node_id,
            "an identity",
            store.upsert_identity(cand, now),
        );
    }
    for (node_id, rtt_ms, rate) in probed {
        let written = store.record_sample(&node_id, rtt_ms, rate, now, cfg);
        file(&node_id, "a probe sample", written);
    }
    if let Err(e) = store.prune_and_cap(now, cfg) {
        tracing::debug!("could not prune the peer store: {e:#}");
    }
    if tally.writes > 0 && tally.failed == tally.writes {
        tracing::warn!(
            writes = tally.writes,
            "the peer store refused every write of this probe round; \
             its latency ranking stays at its last state"
        );
    }
    tally
}

/// Seconds since the Unix epoch, saturating to 0 on a clock before the epoch
/// (never on this platform in practice) rather than panicking.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A holder resolution that only the user's configuration can fix: a missing
/// `capacity_bond_address`, a malformed `--node-id` or `--provider-address`,
/// or probe signatures that do not recover to their operators.
#[derive(Debug)]
pub struct ResolveConfigFault(pub String);

impl std::fmt::Display for ResolveConfigFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ResolveConfigFault {}

/// The holders a command starts from. A resolution the configuration breaks
/// ([`ResolveConfigFault`]) ends the command. Any other failure, such as a
/// sole holder that is down at start, starts with no holder: the acquire loop
/// then discovers with backoff under the stop policy.
///
/// # Errors
///
/// A [`ResolveConfigFault`].
pub fn holders_or_none(
    resolved: anyhow::Result<ResolvedTargets>,
) -> anyhow::Result<ResolvedTargets> {
    match resolved {
        Ok(targets) => Ok(targets),
        Err(err) if err.downcast_ref::<ResolveConfigFault>().is_some() => Err(err),
        Err(err) => {
            tracing::warn!(
                error = %decdn_common::redact::sanitize_err_chain(&err),
                "no holder resolved yet; the fetch keeps looking"
            );
            Ok(ResolvedTargets {
                candidates: Vec::new(),
                coverage_by_node: HashMap::new(),
                probed_samples: Vec::new(),
                pinned: false,
                size_hint: None,
                late: LateSlot::default(),
            })
        }
    }
}

/// The ordered holder list plus what discovery learned about each holder.
pub struct ResolvedTargets {
    /// The candidates, nearest first (#1174).
    pub(crate) candidates: Vec<NodeCandidate>,
    /// Each probed holder's measured [`decdn_protocol::Coverage`] (#1506),
    /// keyed by `node_id` — what a holder's lane is planned against instead of
    /// assuming every candidate is a full holder. Empty on the pinned
    /// `--node-id` path, where nothing was probed; a lookup miss there (as
    /// everywhere else) reads as "no measured coverage", a full holder.
    pub(crate) coverage_by_node: HashMap<PublicKey, decdn_protocol::Coverage>,
    /// `(node_id, rtt_ms, rate_per_mb)` for each holder that answered a probe
    /// this fetch — harvested into the peer store.
    pub(crate) probed_samples: Vec<(PublicKey, f64, u64)>,
    /// The one candidate is the `--node-id` the user pinned. It counts as a
    /// holder, as a probe-reported holder does.
    pub(crate) pinned: bool,
    /// The blob size the nearest probed holder reported, an unsigned hint
    /// ([`discovery::Probed::total_bytes`]). `None` when nothing was probed
    /// (a pinned `--node-id`, the peer-store fast path) or no holder knew it.
    pub(crate) size_hint: Option<u64>,
    /// The probes a streamed round left pending ([`ProbeRound::Stream`]).
    /// Empty on every other path.
    pub(crate) late: LateSlot,
}

impl std::fmt::Debug for ResolvedTargets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedTargets")
            .field("candidates", &self.candidates.len())
            .field("pinned", &self.pinned)
            .field("size_hint", &self.size_hint)
            .finish_non_exhaustive()
    }
}

/// The one holder of a pinned `--node-id` at `provider`. Nothing was probed,
/// so it carries no size hint.
fn pinned_targets(node_id: PublicKey, provider: Address) -> ResolvedTargets {
    ResolvedTargets {
        candidates: vec![NodeCandidate {
            node_id,
            eth_address: provider,
            region_hint: None,
            // A `--node-id`-pinned target takes its direct address from
            // `--addr` at the dial site, not from the registry.
            multiaddrs: Bytes::new(),
        }],
        // Nothing was probed on this path, so no holder coverage was
        // measured: the pinned node is a full holder.
        coverage_by_node: HashMap::new(),
        // Nothing was probed on this path, so there is nothing to harvest.
        probed_samples: Vec::new(),
        pinned: true,
        // Nothing was probed, so the first claim comes from a header.
        size_hint: None,
        late: LateSlot::default(),
    }
}

/// Resolve the holders to fetch from (#1174): the pinned node as the one
/// holder, or auto-discovery (#936) when nothing is pinned (deriving each
/// provider from its node's registry entry), nearest first. The acquire loop
/// stripes across them; a set that goes stale is refreshed by a later
/// discovery with [`DiscoveryOptions::rediscover`] forced, which reads the
/// registry rather than the peer store. `probe` says where the probe round's
/// start is recorded and how the round ends.
///
/// # Errors
///
/// A [`ResolveConfigFault`] when discovery needs a `CapacityBond` the chain
/// does not name, or when every probe answer fails its signature check; any
/// other error when no probed node can serve the blob.
pub async fn resolve_target_node(
    opts: &DiscoveryOptions,
    (chain, site): (&ChainConfig, &PullSite),
    endpoint: &Endpoint,
    relays: &[RelayUrl],
    hash: [u8; 32],
    probe: ProbeOpts<'_>,
) -> anyhow::Result<ResolvedTargets> {
    // No reachability pre-check: the endpoint is discovery-enabled, so a
    // node id resolves through its discovery and default relays even with no
    // direct address or configured relay. A pinned node is its own only
    // holder: a fault cools it and the fetch waits for it to return.
    if let Some(pinned) = &opts.pinned {
        return Ok(pinned_targets(pinned.node_id, pinned.provider));
    }

    let capacity_bond = chain.capacity_bond.ok_or_else(|| {
        anyhow::Error::new(ResolveConfigFault(
            "auto-discovery needs capacity_bond_address (--capacity-bond-address or \
             blockchain.capacity_bond_address), or pass --node-id to dial directly"
                .to_string(),
        ))
    })?;
    // `--timeout-ms` bounds the registry read (#1349). Nothing did before: its
    // retry schedule alone can burn 36 s, so `decdn get --timeout-ms 5000`
    // could sit far longer than 5 s before the transfer it caps had started.
    //
    // The bound is applied INSIDE `bootstrap_nodes` (around the read alone)
    // rather than wrapped around discovery from out here. Wrapping from out
    // here also cancels the ADR 012 § Bootstrap step 4 peer-store fallback,
    // so a client with a usable peer store would be handed a hard failure
    // instead of the degraded-but-working fetch the store exists to provide.
    let order = discover_provider(
        endpoint,
        (chain, site),
        capacity_bond,
        relays.first(),
        hash,
        opts,
        probe,
    )
    .await?;
    if !order.candidates.is_empty() {
        // A header event, then one event per candidate — the log-side shape of
        // the previous per-line stderr listing.
        tracing::info!("discovered {} node(s):", order.candidates.len());
        for c in &order.candidates {
            tracing::info!(
                "  * {} / {} / {}",
                c.region_hint.as_ref().map_or("??", |r| r.as_str()),
                node_id_tail(&c.node_id),
                short_address(&c.eth_address),
            );
        }
    }
    Ok(order)
}

/// Last 4 hex characters of a node id, for a compact operator-facing listing.
///
/// The full 64-hex id is unwieldy in a per-line summary; the tail is enough to
/// tell candidates apart at a glance. Falls back to the full string only if it
/// is somehow shorter than 4 characters (it never is for a `PublicKey`).
fn node_id_tail(node_id: &PublicKey) -> String {
    let s = node_id.to_string();
    s.get(s.len().saturating_sub(4)..).unwrap_or(&s).to_owned()
}

/// Shorten an Ethereum address to `0x` + first 4 + `...` + last 4 hex
/// characters (e.g. `0xa43d...fdCe`), preserving the checksummed casing.
///
/// Returns the full string unchanged if it is unexpectedly short.
fn short_address(addr: &Address) -> String {
    let s = addr.to_string();
    match (s.get(..6), s.get(s.len().saturating_sub(4)..)) {
        (Some(head), Some(tail)) if s.len() > 10 => format!("{head}...{tail}"),
        _ => s,
    }
}

/// Build a probe target for a discovered candidate: its `node_id`, an optional
/// relay hint, and its registry multiaddrs as direct-address hints (ADR 001 §
/// Node Discovery). See [`crate::lanes::lane_target`] for why both ride the first packet.
fn probe_target(cand: &NodeCandidate, relay: Option<RelayUrl>) -> EndpointAddr {
    let mut target = EndpointAddr::new(cand.node_id);
    if let Some(url) = relay {
        target = target.with_relay_url(url);
    }
    discovery::with_dial_addrs(target, cand)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;

/// Probe-shaped fixtures shared by the target and source tests.
#[cfg(test)]
pub(crate) mod tests_support;
