//! Where a fetch finds a blob's holders (#936, #1174, ADR 037): the pinned
//! node, the peer store's fast path, or a round of voucher-less opens over a
//! registry sample, ordered for failover with proxy-warming candidates first.
//!
//! A discovery open is a `cdn/client/v1` stream that stops after its signed
//! answer ([`crate::discovery_open`]): it carries the node's quote, its proven
//! size of the blob, and its coverage, and costs the node nothing before the
//! first voucher. Every answer is verified before it can influence the order
//! (ADR 014 §1). A streamed round returns at the first verified holder and
//! keeps its pending opens as a tail, so a holder that answers later joins the
//! running fetch (`LateOpens`). The holders' streams stay open in
//! [`ResolvedTargets`], so the fetch adopts the winner's stream with no second
//! round trip and cancels the rest. The answers' samples are harvested into
//! the peer store off the fetch's critical path (`spawn_harvest`).

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, Bytes};
use decdn_incentive::slash_judge_domain;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};
use tokio_util::task::AbortOnDropHandle;

use crate::discovery::{self, NodeCandidate};
use crate::probe::probe_once;
use crate::session::{
    ChainConfig, DiscoveryOptions, FetchMarks, PinnedNode, ProxyWarmingParams, PullSite,
};
use crate::{
    Connections, OpenedStream, PoolContext, UnverifiedAnswer, UpstreamRateLimited,
    UpstreamTimestampRejected,
};

/// Per-candidate probe timeout of a probe round (#936). The K probes run
/// concurrently, so this bounds selection latency rather than the overall fetch
/// (`--timeout-ms`); a dead candidate falls out of selection after this.
const SELECT_PROBE_TIMEOUT_MS: u64 = 5_000;

/// Per-candidate bound on a discovery open: the dial and the exchange. The K
/// opens run concurrently, so this bounds selection latency rather than the
/// fetch; a dead candidate falls out of selection after this.
const SELECT_OPEN_TIMEOUT_MS: u64 = 5_000;

/// Current unix time in microseconds (the requester-echoed `timestamp_us`).
pub(crate) fn micros_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros()),
    )
    .unwrap_or(u64::MAX)
}

/// How each asked node of a round dropped out, for [`no_serve_target_error`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Dropouts {
    /// Nodes asked.
    pub(crate) asked: usize,
    /// Nodes that never answered.
    pub(crate) unreachable: usize,
    /// Nodes that shed the request with `APP_ERR_RATE_LIMITED`.
    pub(crate) rate_limited: usize,
    /// Nodes whose answer did not verify.
    pub(crate) unverifiable: usize,
    /// Nodes that refused this machine's request timestamp.
    pub(crate) clock_skewed: usize,
    /// Nodes that declined the blob for this fetch.
    pub(crate) declined: usize,
    /// Nodes that quoted above the fetch's rate ceiling.
    pub(crate) over_ceiling: usize,
}

/// The terminal "nothing can serve this blob" error for [`open_and_order`],
/// naming the failure that actually happened (#1911). A candidate leaves the
/// selection loop seven ways: it never answered (unreachable), it shed the
/// request with `APP_ERR_RATE_LIMITED` (rate-limited — reachable, but refusing
/// this client's request rate), it refused the request's timestamp with
/// `APP_ERR_TIMESTAMP_SKEW` (clock-skewed), it answered but its answer did not
/// verify (unverifiable), it declined the blob, it quoted above the fetch's
/// `--max-rate-per-mb` (over the ceiling), or it answered but was unusable.
///
/// When at least [`CLOCK_SKEW_FATAL_MIN`] asked nodes, and most of them,
/// refused the timestamp, the cause is this machine's clock, so that case is a
/// [`ResolveConfigFault`] that names the clock and ends the command. Fewer
/// refusals do not end the command that way: any node can send the unsigned
/// code at will, and a refusing node's own clock may be the wrong one. They
/// still put the clock hint in the ordinary error's text. When any answer was
/// unverifiable the cause is almost always local configuration rather than
/// missing content, so that case gets its own message too. Otherwise the
/// parenthetical accounts for each of the other counts separately. Reached
/// only when there is neither a cache holder nor a reachable non-holder to
/// pull through — a `NotFound` answer is a serve target, not a failure, so it
/// never lands here.
pub(crate) fn no_serve_target_error(d: Dropouts) -> anyhow::Error {
    let Dropouts {
        asked,
        unreachable,
        rate_limited,
        unverifiable,
        clock_skewed,
        declined,
        over_ceiling,
    } = d;
    if clock_skewed >= CLOCK_SKEW_FATAL_MIN && clock_skewed.saturating_mul(2) > asked {
        return anyhow::Error::new(ResolveConfigFault(format!(
            "{clock_skewed} of {asked} asked node(s) refused this machine's request \
             timestamp as more than 30 seconds from their clocks. Check this machine's \
             clock: set it from a time server (NTP)"
        )));
    }
    if unverifiable > 0 {
        return anyhow::Error::new(ResolveConfigFault(format!(
            "{unverifiable} of {asked} asked node(s) answered, but their answers did not \
             verify: their signatures did not recover to the operator address each is \
             registered under, or their coverage disagreed with the signed answer. That is \
             usually local configuration rather than missing content: check that \
             blockchain.slash_judge_address and blockchain.chain_id match the deployment \
             these nodes registered against"
        )));
    }
    // With no unverifiable candidate, every candidate that neither stayed silent
    // nor shed the request nor was dropped for a named reason answered but was
    // unusable — name each count so the message never implies a silent,
    // wholly-unreachable set when some replied.
    let unusable = asked
        .saturating_sub(unreachable)
        .saturating_sub(rate_limited)
        .saturating_sub(clock_skewed)
        .saturating_sub(declined)
        .saturating_sub(over_ceiling);
    let named = [
        (rate_limited, "rate-limited this client's requests"),
        (clock_skewed, "refused this machine's request timestamp"),
        (declined, "declined the blob"),
        (over_ceiling, "quoted above the --max-rate-per-mb ceiling"),
    ]
    .into_iter()
    .filter(|&(n, _)| n > 0)
    .fold(String::new(), |mut named, (n, what)| {
        use std::fmt::Write as _;
        let _ = write!(named, ", {n} {what}");
        named
    });
    let hint = if clock_skewed > 0 {
        ". If this machine's clock is more than 30 seconds off, set it from a time server \
         (NTP); if only one node refuses, that node's clock is wrong"
    } else {
        ""
    };
    anyhow::anyhow!(
        "none of the {asked} asked node(s) could serve the blob \
         ({unreachable} did not answer{named}, {unusable} answered but were unusable)\
         {hint}"
    )
}

/// The fewest distinct asked nodes whose `TIMESTAMP_SKEW` refusals, when they
/// are also a majority of the asked nodes, make [`no_serve_target_error`] a
/// [`ResolveConfigFault`] that ends the command. Any node can send the
/// unsigned code, so one or two hostile candidates must not be able to end it.
pub(crate) const CLOCK_SKEW_FATAL_MIN: usize = 3;

/// The waits between probe attempts on a candidate that sheds the probe with
/// `APP_ERR_RATE_LIMITED`. ADR 005's per-peer limit refills one probe every
/// 200 ms, so these clear the burst that concurrent entries of one bundle pull
/// send at its start. A candidate that still sheds after the last wait counts
/// as rate-limited, never as silent.
const PROBE_SHED_BACKOFFS_MS: [u64; 3] = [250, 500, 1000];

/// One probe attempt's result.
pub(crate) type ProbeResult = anyhow::Result<(
    decdn_protocol::message::ProbeResponse,
    decdn_protocol::ProbeResponseExt,
    crate::probe::ProbeRtt,
)>;

/// Probe one candidate, probing it again after each [`PROBE_SHED_BACKOFFS_MS`]
/// wait while it sheds the probe. Any other result — an answer, a timeout, a
/// transport failure — returns at once.
///
/// Each attempt stamps its own `timestamp_us` from the wall clock just before
/// it sends: a node refuses a timestamp more than 30 seconds from its clock,
/// and the returned timestamp is the one the last attempt sent, which is the
/// one its answer echoes and [`classify_probe`] verifies against.
pub(crate) async fn probe_candidate(
    endpoint: &Endpoint,
    target: EndpointAddr,
    hash: [u8; 32],
) -> (u64, ProbeResult) {
    let mut backoffs = PROBE_SHED_BACKOFFS_MS.iter();
    loop {
        let timestamp_us = micros_now();
        let res = probe_once(
            endpoint,
            target.clone(),
            hash,
            timestamp_us,
            Duration::from_millis(SELECT_PROBE_TIMEOUT_MS),
        )
        .await;
        match (&res, backoffs.next()) {
            (Err(e), Some(&ms)) if is_shed(e) => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            (Err(e), None) if is_shed(e) => {
                tracing::info!(
                    "{} kept rate-limiting this client's probe: {e:#}",
                    target.id
                );
                return (timestamp_us, res);
            }
            _ => return (timestamp_us, res),
        }
    }
}

/// Whether a failed request was the node shedding it with
/// `APP_ERR_RATE_LIMITED`.
fn is_shed(err: &anyhow::Error) -> bool {
    err.downcast_ref::<UpstreamRateLimited>().is_some()
}

/// Whether a failed request was the node refusing its timestamp with
/// `APP_ERR_TIMESTAMP_SKEW`.
fn is_timestamp_rejected(err: &anyhow::Error) -> bool {
    err.downcast_ref::<UpstreamTimestampRejected>().is_some()
}

/// Set once this process has warned about a `TIMESTAMP_SKEW` refusal.
static CLOCK_SKEW_WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Log a `TIMESTAMP_SKEW` refusal: at `warn` the first time in this process,
/// and at `debug` after that. When this machine's clock is wrong, every node
/// refuses every request, and a bundle asks each node once per entry, so a
/// line per refusal would repeat thousands of times. The count reaches the
/// terminal error ([`no_serve_target_error`]) instead.
fn log_timestamp_rejected(cand: &NodeCandidate, err: &anyhow::Error) {
    if CLOCK_SKEW_WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::debug!(
            node = %cand.node_id,
            error = %format_args!("{err:#}"),
            "request refused for clock skew"
        );
    } else {
        tracing::warn!(
            "a request to {} failed: the node refused this machine's request timestamp as \
             more than 30 seconds from its clock. Check this machine's clock. If only one \
             node refuses, that node's clock is wrong. Later refusals in this run log at debug",
            cand.node_id
        );
    }
}

/// How long a round keeps collecting answers after the first verified holder
/// answers. A holder that answers later is about 125 ms farther away, still
/// connecting, or still waiting for hole punching to select a direct path (an
/// open waits up to 500 ms for one after its answer), and would not lead the
/// order. One slow connect would otherwise hold the whole round for up to the
/// open timeout.
const SETTLE_AFTER_HOLDER: Duration = Duration::from_millis(250);

/// Run `opens` concurrently and collect their outcomes until every open has
/// answered or the round's deadline passes. The first outcome `is_holder`
/// accepts sets the deadline `grace` later. With `cold_grace` set and no
/// holder yet, the first outcome `is_answer` accepts sets it `cold_grace`
/// later, and a holder inside that window still ends the round `grace` after
/// it. Returns the collected outcomes, in answer order, and the opens still
/// pending. A zero `grace` returns at the first holder with every outcome
/// already ready: `timeout_at` polls the stream before it checks the deadline.
/// With no holder and no `cold_grace`, the round waits for every open and the
/// tail is empty.
async fn settle_opens<T, F>(
    opens: impl IntoIterator<Item = F>,
    grace: Duration,
    cold_grace: Option<Duration>,
    is_holder: impl Fn(&T) -> bool,
    is_answer: impl Fn(&T) -> bool,
) -> (Vec<T>, futures_util::stream::FuturesUnordered<F>)
where
    F: std::future::Future<Output = T>,
{
    use futures_util::StreamExt as _;
    let mut pending: futures_util::stream::FuturesUnordered<F> = opens.into_iter().collect();
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
            "discovery round settled; the rest continue as its tail"
        );
    }
    (settled, pending)
}

/// Log one probe's raw result at `debug`, with the time since the round started.
fn log_probe_result(cand: &NodeCandidate, res: &ProbeResult, started: std::time::Instant) {
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

/// Log one discovery open's raw result at `debug`, with the time since the
/// round started.
fn log_open_result(
    cand: &NodeCandidate,
    res: &anyhow::Result<OpenedStream>,
    started: std::time::Instant,
) {
    match res {
        Ok(opened) => tracing::debug!(
            node = %cand.node_id,
            elapsed_ms = started.elapsed().as_millis(),
            rtt_ms = opened.rtt().map(|(ms, _)| ms),
            rtt_direct = opened.rtt().is_some_and(|(_, direct)| direct),
            ok = opened.served(),
            rate_per_mb = opened.rate_per_mb(),
            "discovery open answered"
        ),
        Err(error) => tracing::debug!(
            node = %cand.node_id,
            elapsed_ms = started.elapsed().as_millis(),
            error = %format_args!("{error:#}"),
            "discovery open failed"
        ),
    }
}

/// A peer-store sample: `(node_id, rtt_ms, rate_per_mb)` of one verified
/// answer.
pub(crate) type Sample = (PublicKey, f64, u64);

/// One asked candidate, verified and classified ([`classify_open`],
/// [`classify_probe`]).
pub(crate) enum OpenOutcome {
    /// A verified answer that serves the span, with its peer-store sample.
    Holder(discovery::Probed, Sample),
    /// A verified answer that does not hold the blob (a signed `NotFound`),
    /// with its peer-store sample: a proxy-warming or pull-through candidate.
    NonHolder(discovery::WarmingCandidate, Sample),
    /// No answer.
    Unreachable,
    /// The candidate shed the request.
    RateLimited,
    /// The candidate refused the request's timestamp
    /// (`APP_ERR_TIMESTAMP_SKEW`): its clock and this machine's differ by more
    /// than 30 seconds.
    ClockSkewed,
    /// The answer did not verify: its `slash_sig` did not recover to the
    /// candidate's operator, or its coverage disagreed with its signed `ok`.
    Unverifiable,
    /// The answer was verified but unusable: a size with no valid proof, or a
    /// refusal class discovery cannot act on.
    Unusable,
    /// The candidate declined the blob for this fetch (a signed `Declined`).
    Declined,
    /// The candidate's signed quote is above the fetch's `--max-rate-per-mb`.
    OverCeiling,
}

impl OpenOutcome {
    const fn is_holder(&self) -> bool {
        matches!(self, Self::Holder(..))
    }

    /// A verified answer, holder or not.
    const fn is_answer(&self) -> bool {
        matches!(self, Self::Holder(..) | Self::NonHolder(..))
    }

    /// The peer-store sample of a verified answer.
    pub(crate) const fn sample(&self) -> Option<Sample> {
        match self {
            Self::Holder(_, sample) | Self::NonHolder(_, sample) => Some(*sample),
            Self::Unreachable
            | Self::RateLimited
            | Self::ClockSkewed
            | Self::Unverifiable
            | Self::Unusable
            | Self::Declined
            | Self::OverCeiling => None,
        }
    }
}

/// How a discovery round ends once a verified holder answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenRound {
    /// Return at the first verified holder and keep the pending opens as the
    /// round's tail (`LateOpens`): the first round of a fetch, whose late
    /// holders join the running fetch.
    Stream,
    /// Collect for `SETTLE_AFTER_HOLDER` after the first verified holder and
    /// drop the rest: a rediscovery.
    Settle,
}

impl OpenRound {
    /// How long the round collects after the first verified holder.
    const fn grace(self) -> Duration {
        match self {
            Self::Stream => Duration::ZERO,
            Self::Settle => SETTLE_AFTER_HOLDER,
        }
    }

    /// How long the round collects after its first verified answer while no
    /// holder has answered. A streamed round then starts on the non-holders
    /// in hand as pull-through targets (#1911), and a holder that answers
    /// later joins the running fetch. A settled round waits for every open.
    const fn cold_grace(self) -> Option<Duration> {
        match self {
            Self::Stream => Some(SETTLE_AFTER_HOLDER),
            Self::Settle => None,
        }
    }
}

/// The opens a streamed round left pending, with what deciding a late answer
/// needs. A late answer keeps its verified facts (coverage, quote, size), and
/// its stream is cancelled as it arrives: the fetch has adopted its first
/// stream by then, and a lane to a late holder opens its own with pull-through.
pub(crate) struct LateOpens {
    /// The pending opens, each yielding its classified outcome.
    pub(crate) tail: std::pin::Pin<Box<dyn futures_util::Stream<Item = OpenOutcome> + Send>>,
    /// The lowest RTT among the holders the round returned.
    pub(crate) best_holder_rtt_ms: f64,
    /// The fetch's proxy-warming knobs.
    pub(crate) warming: ProxyWarmingParams,
}

/// What a discovery open sends: the connections it opens on, the payer, the
/// span, and the rate ceiling it holds each quote to.
///
/// The request carries `ctx`'s pool, binding and capability like any open. The
/// node defers funding admission to the first voucher, so the pool may be one
/// the payer has yet to open ([`crate::funding::discovery_ctx`]).
#[derive(Clone, Copy)]
pub struct OpenSpec<'a> {
    /// The command's connection per node: a discovery open and the lane that
    /// adopts its stream share one connection.
    pub conns: &'a Connections,
    /// The payer's context: pool, signer, binding and capability.
    pub ctx: &'a PoolContext,
    /// The namespace routing hint (ADR 005 § Namespace routing).
    pub namespace_id: [u8; 32],
    /// The span every open asks for, `(byte_offset, byte_len)`: the fetch's
    /// first leg, so the winner's stream is that leg.
    pub span: (u64, u64),
    /// The most the fetch pays per MiB; `0` for no ceiling. A node that quotes
    /// above it is left out.
    pub max_rate_per_mb: u64,
}

impl std::fmt::Debug for OpenSpec<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenSpec")
            .field("pool_id", &self.ctx.pool_id)
            .field("span", &self.span)
            .field("max_rate_per_mb", &self.max_rate_per_mb)
            .finish_non_exhaustive()
    }
}

/// How a resolve discovers: where it records its marks, how its round ends,
/// the blob size the caller already knows, and what each open sends.
#[derive(Clone, Copy)]
pub struct OpenOpts<'a> {
    /// Where the round reports its start; `None` reports nothing.
    pub marks: Option<&'a dyn FetchMarks>,
    /// How the round ends.
    pub round: OpenRound,
    /// A size of the blob an earlier fetch proved or verified: the fetch's
    /// first size when no answer proves one. A pinned node is not opened for
    /// it. A declared size (a bundle manifest's) is never one.
    pub known_size: Option<crate::ProvenSize>,
    /// What each discovery open sends.
    pub spec: OpenSpec<'a>,
}

impl std::fmt::Debug for OpenOpts<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenOpts")
            .field("marks", &self.marks.is_some())
            .field("round", &self.round)
            .field("known_size", &self.known_size)
            .field("spec", &self.spec)
            .finish()
    }
}

/// A streamed round's [`LateOpens`], taken once by whoever adopts them.
#[derive(Default)]
pub(crate) struct LateSlot(std::sync::Mutex<Option<LateOpens>>);

impl LateSlot {
    /// A slot holding `late`.
    pub(crate) const fn new(late: LateOpens) -> Self {
        Self(std::sync::Mutex::new(Some(late)))
    }

    /// Take the late opens, leaving the slot empty.
    pub(crate) fn take(&self) -> Option<LateOpens> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

/// The holders' streams a round left open, keyed by node, taken once by
/// whoever adopts them. A stream still here when the slot drops is cancelled.
#[derive(Default)]
pub(crate) struct OpenedSlot(std::sync::Mutex<HashMap<PublicKey, OpenedStream>>);

impl OpenedSlot {
    /// A slot holding `opened`.
    pub(crate) const fn new(opened: HashMap<PublicKey, OpenedStream>) -> Self {
        Self(std::sync::Mutex::new(opened))
    }

    /// Take every stream, leaving the slot empty.
    pub(crate) fn take(&self) -> HashMap<PublicKey, OpenedStream> {
        std::mem::take(&mut *self.lock())
    }

    /// How many streams the slot holds.
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    /// Take `node`'s stream.
    pub(crate) fn remove(&self, node: &PublicKey) -> Option<OpenedStream> {
        self.lock().remove(node)
    }

    /// Hold `opened` in place of the streams held now, which are cancelled.
    pub(crate) fn replace(&self, opened: HashMap<PublicKey, OpenedStream>) {
        let replaced = std::mem::replace(&mut *self.lock(), opened);
        cancel_all(replaced);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PublicKey, OpenedStream>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for OpenedSlot {
    fn drop(&mut self) {
        cancel_all(self.take());
    }
}

/// Cancel every stream in `opened` ([`OpenedStream::cancel`]).
pub(crate) fn cancel_all(opened: HashMap<PublicKey, OpenedStream>) {
    for stream in opened.into_values() {
        stream.cancel();
    }
}

/// A round's outcomes, split by what [`open_and_order`] does with each.
struct OpenTally {
    /// The verified holders.
    holders: Vec<discovery::Probed>,
    /// Asked bonded nodes that answered a signed `NotFound` — reachable, with
    /// a measured RTT, but not holding the blob in their cache store. They
    /// serve two roles: the proxy-warming candidate pool (ADR 037 § Candidate
    /// pool) when a holder exists but is distant, and — when NO holder answers
    /// — the pull-through serve targets a cold blob's first fetch bootstraps
    /// from (#1911), since a cache miss does not mean the node cannot serve
    /// via its own origin. Always collected, because the second role does not
    /// depend on warming being on.
    non_holders: Vec<discovery::WarmingCandidate>,
    /// One sample for each verified responder, harvested into the peer store
    /// off the critical path (`spawn_harvest`, called from
    /// `discover_provider`).
    samples: Vec<Sample>,
    /// The ways a candidate drops out, counted separately: the terminal error
    /// has to name the one that actually happened. A wrong
    /// `slash_judge_address` or `chain_id` makes EVERY honest node fail
    /// verification, and reporting that as "nobody holds the blob" sends the
    /// operator hunting for missing content instead of a local
    /// misconfiguration; a node shedding this client's request rate is
    /// reachable, and reporting it as silent points at the network instead.
    /// A node refusing this machine's timestamp is reachable too, and the
    /// remedy is this machine's clock.
    dropouts: Dropouts,
}

impl OpenTally {
    fn of(asked: usize, outcomes: Vec<OpenOutcome>) -> Self {
        let mut tally = Self {
            holders: Vec::new(),
            non_holders: Vec::new(),
            samples: Vec::new(),
            dropouts: Dropouts {
                asked,
                ..Dropouts::default()
            },
        };
        for outcome in outcomes {
            let d = &mut tally.dropouts;
            match outcome {
                OpenOutcome::Holder(holder, sample) => {
                    tally.samples.push(sample);
                    tally.holders.push(holder);
                }
                OpenOutcome::NonHolder(candidate, sample) => {
                    tally.samples.push(sample);
                    tally.non_holders.push(candidate);
                }
                OpenOutcome::Unreachable => d.unreachable += 1,
                OpenOutcome::RateLimited => d.rate_limited += 1,
                OpenOutcome::ClockSkewed => d.clock_skewed += 1,
                OpenOutcome::Unverifiable => d.unverifiable += 1,
                OpenOutcome::Declined => d.declined += 1,
                OpenOutcome::OverCeiling => d.over_ceiling += 1,
                OpenOutcome::Unusable => {}
            }
        }
        tally
    }
}

/// The size `ext` states for `hash`, proven: `Ok(None)` when it states none,
/// `Err(())` when it states one with no valid size proof. A stated size is a
/// fact only with its proof (ADR 005 § Bounded byte ranges), and a size with
/// no valid proof is a fault of the node: the caller drops the answer before
/// the size can reach a plan or a price.
fn stated_size(
    cand: &NodeCandidate,
    hash: [u8; 32],
    ext: &decdn_protocol::ProbeResponseExt,
) -> Result<Option<crate::ProvenSize>, ()> {
    let Some(proof) = ext.size_proof.as_ref() else {
        return Ok(None);
    };
    crate::check_size_proof(hash, proof)
        .map(Some)
        .map_err(|e| tracing::warn!("dropping a probe response from {}: {e}", cand.node_id))
}

/// Verify `cand`'s probe result and classify it. Every response is verified
/// before it can influence the order (ADR 014 §1). A failure is requester-local
/// policy, with no reputation effect, because an unrecovered signature
/// attributes nothing to anyone.
/// `timestamp_us` is the timestamp the probe that produced `res` sent.
pub(crate) fn classify_probe(
    cand: &NodeCandidate,
    res: ProbeResult,
    hash: [u8; 32],
    timestamp_us: u64,
    slash_domain: &alloy::sol_types::Eip712Domain,
) -> OpenOutcome {
    let (resp, resp_ext, rtt) = match res {
        Ok(answer) => answer,
        Err(e) if is_shed(&e) => return OpenOutcome::RateLimited,
        Err(e) if is_timestamp_rejected(&e) => {
            log_timestamp_rejected(cand, &e);
            return OpenOutcome::ClockSkewed;
        }
        Err(_) => return OpenOutcome::Unreachable,
    };
    if let Err(e) = crate::probe::verify_probe_response(
        &resp,
        cand.eth_address,
        slash_domain,
        hash,
        timestamp_us,
    ) {
        // The answer counts as none: discovery drops the candidate from
        // selection, and a pinned node gives no size. The operator needs
        // told why, so log it at `warn`.
        tracing::warn!(
            "ignoring an unverifiable probe response from {}: {e}",
            cand.node_id
        );
        return OpenOutcome::Unverifiable;
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
        return OpenOutcome::Unusable;
    }
    let Ok(total_bytes) = stated_size(cand, hash, &resp_ext) else {
        return OpenOutcome::Unusable;
    };
    let rtt_ms = rtt.ms;
    let rate_per_mb = resp.body.rate_per_mb;
    // Every verified responder contributes a sample, holder or not.
    let sample = (cand.node_id, rtt_ms, rate_per_mb);
    if resp.body.has_blob {
        OpenOutcome::Holder(
            discovery::Probed {
                candidate: cand.clone(),
                rtt_ms,
                rate_per_mb,
                total_bytes,
                coverage: resp_ext.coverage,
            },
            sample,
        )
    } else {
        OpenOutcome::NonHolder(warming_candidate(cand, rtt_ms), sample)
    }
}

/// `cand` as a proxy-warming or pull-through candidate at `rtt_ms`.
fn warming_candidate(cand: &NodeCandidate, rtt_ms: f64) -> discovery::WarmingCandidate {
    discovery::WarmingCandidate {
        node_id: cand.node_id,
        eth_address: cand.eth_address,
        rtt_ms,
        multiaddrs: cand.multiaddrs.clone(),
    }
}

/// Open `hash` on `cand` for discovery ([`crate::discovery_open`]) as `spec`
/// says, with no pull-through unless `pull_through` is set. A span that starts
/// past the blob's end serves nothing, so it is opened again for the whole
/// blob: the answer still proves the size and names the node a holder.
pub(crate) async fn open_candidate(
    cand: &NodeCandidate,
    relay: Option<RelayUrl>,
    spec: OpenSpec<'_>,
    slash_domain: &alloy::sol_types::Eip712Domain,
    hash: [u8; 32],
    pull_through: bool,
) -> anyhow::Result<OpenedStream> {
    let target = probe_target(cand, relay);
    let open = |span| {
        crate::discovery_open(
            spec.conns,
            &target,
            cand.eth_address,
            spec.ctx,
            slash_domain,
            hash,
            spec.namespace_id,
            span,
            pull_through,
            Duration::from_millis(SELECT_OPEN_TIMEOUT_MS),
        )
    };
    match open(spec.span).await {
        Err(e) if spec.span != (0, 0) && crate::is_range_past_end(&e) => open((0, 0)).await,
        other => other,
    }
}

/// Classify `cand`'s discovery open result `res`. The answer was verified
/// before `res` holds it (ADR 014 §1); a verification fault is
/// [`OpenOutcome::Unverifiable`], requester-local policy with no reputation
/// effect, because an answer that does not verify attributes nothing to
/// anyone. A served answer above `max_rate_per_mb` (`0` for no ceiling) is
/// [`OpenOutcome::OverCeiling`]. Returns the outcome and, for a holder, its
/// stream, still open; every other stream is cancelled here.
pub(crate) fn classify_open(
    cand: &NodeCandidate,
    res: anyhow::Result<OpenedStream>,
    max_rate_per_mb: u64,
) -> (OpenOutcome, Option<OpenedStream>) {
    let opened = match res {
        Ok(opened) => opened,
        Err(e) if is_shed(&e) => return (OpenOutcome::RateLimited, None),
        Err(e) if is_timestamp_rejected(&e) => {
            log_timestamp_rejected(cand, &e);
            return (OpenOutcome::ClockSkewed, None);
        }
        Err(e) if e.downcast_ref::<UnverifiedAnswer>().is_some() => {
            // The answer counts as none: discovery drops the candidate from
            // selection, and a pinned node gives no size. The operator needs
            // told why, so log it at `warn`.
            tracing::warn!(
                "ignoring an unverifiable answer from {}: {e:#}",
                cand.node_id
            );
            return (OpenOutcome::Unverifiable, None);
        }
        Err(_) => return (OpenOutcome::Unreachable, None),
    };
    // An open with no RTT has no open path: the connection closed under it,
    // so its stream is gone too.
    let Some((rtt_ms, _)) = opened.rtt() else {
        opened.cancel();
        return (OpenOutcome::Unreachable, None);
    };
    let rate_per_mb = opened.rate_per_mb();
    let sample = (cand.node_id, rtt_ms, rate_per_mb);
    if !opened.served() {
        let error = opened.ext().error().cloned();
        opened.cancel();
        return match error {
            // `Unfunded` speaks of the payer's funding, not the blob: the node
            // is a serve target, and the lane's open meets the refusal again
            // with a funding recovery behind it.
            Some(
                decdn_protocol::client::StreamError::NotFound
                | decdn_protocol::client::StreamError::Unfunded,
            ) => (
                OpenOutcome::NonHolder(warming_candidate(cand, rtt_ms), sample),
                None,
            ),
            Some(decdn_protocol::client::StreamError::Declined) => (OpenOutcome::Declined, None),
            Some(decdn_protocol::client::StreamError::VoucherRejected { .. }) | None => {
                (OpenOutcome::Unusable, None)
            }
        };
    }
    if max_rate_per_mb > 0 && rate_per_mb > max_rate_per_mb {
        tracing::info!(
            node = %cand.node_id,
            rate_per_mb,
            max_rate_per_mb,
            "leaving out a holder that quoted above --max-rate-per-mb"
        );
        opened.cancel();
        return (OpenOutcome::OverCeiling, None);
    }
    let holder = discovery::Probed {
        candidate: cand.clone(),
        rtt_ms,
        rate_per_mb,
        total_bytes: opened.proven_size(),
        coverage: opened.coverage().clone(),
    };
    (OpenOutcome::Holder(holder, sample), Some(opened))
}

/// Open `hash` on every one of `candidates` and return the ordered
/// provider-failover list (#1174, ADR 037 § Fallback): the sequence `fetch`
/// tries in turn, each entry a fallback for the one before it, until one
/// delivers the blob. Errors only when no asked candidate is reachable to
/// serve at all — a cache holder OR a bonded non-holder that can pull through
/// (#1911).
///
/// Every open is voucher-less and asks no pull-through (`open_candidate`):
/// a holder answers its signed quote, its proven size and its coverage, and a
/// node that misses answers a signed `NotFound` at once. Every answer is
/// verified before it can influence the order: value invariants, echoed-field
/// correlation, `slash_sig` recovery to the candidate's on-chain operator
/// address (ADR 014 §1), and the coverage against the signed `ok`. A
/// candidate whose answer fails is dropped, exactly as for a timeout; it is
/// requester-local policy and never scored against the peer. A holder that
/// quotes above `spec.max_rate_per_mb` is dropped too.
///
/// Every candidate is asked on equal footing: opening cost is
/// provider-independent, because the caller has ONE pool that fans out to
/// every provider (ADR 003), so there is no per-provider "already funded"
/// distinction to prefer.
///
/// The round ends `SETTLE_AFTER_HOLDER` after the first verified holder
/// answers, or when every open has answered. A holder the cutoff drops stays
/// reachable through the fetch's later discovery. The returned holders'
/// streams stay open in [`ResolvedTargets`]; a streamed round's later answers
/// cancel theirs (`LateOpens`).
///
/// The order is `failover_order`'s: proxy-warming candidates first when
/// warming engages, then the holders by `holder_rank`; or, when no candidate
/// holds the blob, the reachable non-holders nearest RTT first as
/// pull-through serve targets.
#[allow(clippy::too_many_arguments)]
pub async fn open_and_order(
    candidates: &[NodeCandidate],
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
    spec: OpenSpec<'_>,
    warming: ProxyWarmingParams,
    slash_domain: &alloy::sol_types::Eip712Domain,
    round: OpenRound,
) -> anyhow::Result<ResolvedTargets> {
    // Open concurrently in one task. Each open is bounded by its own timeout
    // and stamps its own timestamp, verifies and classifies its own answer,
    // so the round ends only on a verified holder. Each open owns its
    // inputs, so a streamed round's pending opens outlive this call.
    let started = std::time::Instant::now();
    let opens = candidates.iter().cloned().map(|cand| {
        let relay = relay_hint.cloned();
        let slash_domain = slash_domain.clone();
        let conns = spec.conns.clone();
        let ctx = spec.ctx.clone();
        let (namespace_id, span, max_rate_per_mb) =
            (spec.namespace_id, spec.span, spec.max_rate_per_mb);
        async move {
            let spec = OpenSpec {
                conns: &conns,
                ctx: &ctx,
                namespace_id,
                span,
                max_rate_per_mb,
            };
            let res = open_candidate(&cand, relay, spec, &slash_domain, hash, false).await;
            log_open_result(&cand, &res, started);
            let (outcome, opened) = classify_open(&cand, res, max_rate_per_mb);
            (cand.node_id, outcome, opened)
        }
    });
    let (answers, tail) = settle_opens(
        opens,
        round.grace(),
        round.cold_grace(),
        |(_, outcome, _)| outcome.is_holder(),
        |(_, outcome, _)| outcome.is_answer(),
    )
    .await;

    let mut opened = HashMap::new();
    let outcomes: Vec<OpenOutcome> = answers
        .into_iter()
        .map(|(node_id, outcome, stream)| {
            if let Some(stream) = stream {
                opened.insert(node_id, stream);
            }
            outcome
        })
        .collect();
    let OpenTally {
        holders,
        non_holders,
        samples,
        dropouts,
    } = OpenTally::of(candidates.len(), outcomes);

    // Terminal only when NOTHING can serve — no holder AND no reachable
    // non-holder to pull through. An empty holder set alone is not terminal: a
    // cold blob is origin-only with zero cache holders, the normal first-fetch
    // state (#1911), so as long as one bonded node answered it is a serve target.
    if holders.is_empty() && non_holders.is_empty() {
        return Err(no_serve_target_error(dropouts));
    }

    if holders.is_empty() {
        // No cache holder, but reachable non-holders can serve via pull-through.
        // Log why the fetch is talking to nodes that answered `NotFound`.
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
        samples,
        pinned: false,
        size_hint,
        late,
        opened: OpenedSlot::new(opened),
    })
}

/// Probe `candidates` for `hash` over `endpoint` and return the ordered
/// provider-failover list, as [`open_and_order`] does from opens. A bundle
/// entry resolves its holders this way.
pub async fn probe_and_order(
    endpoint: &Endpoint,
    candidates: &[NodeCandidate],
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
    warming: ProxyWarmingParams,
    slash_domain: &alloy::sol_types::Eip712Domain,
    round: OpenRound,
) -> anyhow::Result<ResolvedTargets> {
    // Probe concurrently in one task. `probe_once`'s internal timeout bounds
    // each leg; a candidate that sheds the probe is probed again after a short
    // wait (`probe_candidate`). Each attempt stamps its own timestamp, because
    // a streamed round's tail can outlive a node's 30-second clock-skew bound.
    let started = std::time::Instant::now();
    let probes = candidates.iter().cloned().map(|cand| {
        let target = probe_target(&cand, relay_hint.cloned());
        let endpoint = endpoint.clone();
        let slash_domain = slash_domain.clone();
        async move {
            let (timestamp_us, res) = probe_candidate(&endpoint, target, hash).await;
            log_probe_result(&cand, &res, started);
            classify_probe(&cand, res, hash, timestamp_us, &slash_domain)
        }
    });
    let (outcomes, tail) = settle_opens(
        probes,
        round.grace(),
        round.cold_grace(),
        OpenOutcome::is_holder,
        OpenOutcome::is_answer,
    )
    .await;

    let OpenTally {
        holders,
        non_holders,
        samples,
        dropouts,
    } = OpenTally::of(candidates.len(), outcomes);
    if holders.is_empty() && non_holders.is_empty() {
        return Err(no_serve_target_error(dropouts));
    }
    let size_hint = nearest_size_hint(&holders);
    let late = late_probe_slot(round, tail, &holders, warming);
    let ordered = failover_order(holders, &non_holders, warming);
    Ok(ResolvedTargets {
        candidates: ordered.order,
        coverage_by_node: ordered.coverage_by_node,
        samples,
        pinned: false,
        size_hint,
        late,
        opened: OpenedSlot::default(),
    })
}

/// A streamed round's pending opens as its [`LateSlot`], with the best RTT
/// among the `holders` it returned. Empty for a settled round or an empty
/// tail. A late answer's stream is cancelled as it arrives, and its outcome
/// keeps the verified facts.
fn late_slot<F>(
    round: OpenRound,
    tail: futures_util::stream::FuturesUnordered<F>,
    holders: &[discovery::Probed],
    warming: ProxyWarmingParams,
) -> LateSlot
where
    F: std::future::Future<Output = (PublicKey, OpenOutcome, Option<OpenedStream>)>
        + Send
        + 'static,
{
    use futures_util::StreamExt as _;
    let empty = tail.is_empty();
    let outcomes = tail.map(|(_, outcome, stream)| {
        if let Some(stream) = stream {
            stream.cancel();
        }
        outcome
    });
    late_stream_slot(round, empty, outcomes, holders, warming)
}

/// [`late_slot`] for a probe round's pending probes.
fn late_probe_slot<F>(
    round: OpenRound,
    tail: futures_util::stream::FuturesUnordered<F>,
    holders: &[discovery::Probed],
    warming: ProxyWarmingParams,
) -> LateSlot
where
    F: std::future::Future<Output = OpenOutcome> + Send + 'static,
{
    let empty = tail.is_empty();
    late_stream_slot(round, empty, tail, holders, warming)
}

/// The [`LateSlot`] of a streamed round's pending `tail`; `empty` says the
/// tail holds nothing.
///
/// A task drives the tail from here on, so its requests keep running and
/// timing while the fetch builds its lanes before anything reads it. Their
/// answers wait in a channel. The task stops when the tail stream is dropped,
/// so no request outlives the fetch that adopted it.
fn late_stream_slot<S>(
    round: OpenRound,
    empty: bool,
    tail: S,
    holders: &[discovery::Probed],
    warming: ProxyWarmingParams,
) -> LateSlot
where
    S: futures_util::Stream<Item = OpenOutcome> + Send + 'static,
{
    use futures_util::StreamExt as _;
    if round != OpenRound::Stream || empty {
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
    LateSlot::new(LateOpens {
        tail: Box::pin(answers),
        best_holder_rtt_ms,
        warming,
    })
}

/// The proven size of the nearest holder that gave one: the fetch's first
/// size ([`ResolvedTargets::size_hint`]).
fn nearest_size_hint(holders: &[discovery::Probed]) -> Option<crate::ProvenSize> {
    holders
        .iter()
        .filter(|h| h.total_bytes.is_some())
        .min_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms))
        .and_then(|h| h.total_bytes)
}

/// The width of the RTT bands [`holder_rank`] compares holders in. Two holders
/// in one band are near enough that the cheaper quote, then the wider
/// coverage, decides between them.
pub(crate) const RANK_RTT_BAND_MS: f64 = 20.0;

/// The rank key of one holder; the lower key leads the failover order. The
/// key compares, in turn:
///
/// 1. the holder's RTT band, its RTT in whole [`RANK_RTT_BAND_MS`] steps, so a
///    holder in a nearer band always leads;
/// 2. its signed quote (`rate_per_mb`), so within a band the cheaper holder
///    leads;
/// 3. its coverage, the number of discovery blocks it holds, wider first, so
///    within a band and a quote the holder that can serve more of the blob
///    leads;
/// 4. its exact RTT.
///
/// A quote above the fetch's `--max-rate-per-mb` never reaches the order
/// ([`OpenOutcome::OverCeiling`]).
pub(crate) fn holder_rank(holder: &discovery::Probed) -> (u64, u64, std::cmp::Reverse<usize>, f64) {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "an RTT is finite and non-negative, and an RTT band past u64 saturates"
    )]
    let band = (holder.rtt_ms / RANK_RTT_BAND_MS).floor() as u64;
    (
        band,
        holder.rate_per_mb,
        std::cmp::Reverse(holder.coverage.covered_blocks().count()),
        holder.rtt_ms,
    )
}

/// The ordered provider-failover list plus, when a proxy leads it, that proxy's
/// identity for the operator log line. Split from [`open_and_order`] as a pure
/// function so the ordering is unit-tested without live opens.
pub(crate) struct FailoverOrder {
    /// The candidates to try in turn: proxy-warming non-holders first (nearest
    /// RTT first) when warming engages, then the holders by [`holder_rank`] —
    /// or, when no holder answered, the reachable non-holders nearest RTT
    /// first as pull-through serve targets (#1911).
    pub(crate) order: Vec<NodeCandidate>,
    /// `Some((proxy_node_id, proxy_rtt_ms, best_holder_rtt_ms))` when a warming
    /// proxy is prepended; `None` when the list is just the holders.
    warming_lead: Option<(PublicKey, f64, f64)>,
    /// Each holder's [`decdn_protocol::Coverage`] (#1506), keyed by `node_id`.
    /// Proxy-warming candidates never appear here — they are non-holders by
    /// definition, so a lookup miss on them (and on any node this map
    /// otherwise has no entry for) means "no measured coverage", which the
    /// multi-source lane builder treats as a full holder rather than as a gap
    /// in the data.
    coverage_by_node: HashMap<PublicKey, decdn_protocol::Coverage>,
}

/// Assemble the failover order (#1174, ADR 037 § Client selection policy) from
/// the verified `holders` and the `non_holders` — asked bonded nodes that
/// answered a signed `NotFound`.
///
/// The holders form the backbone, ranked by [`holder_rank`] (RTT band, then
/// quote, then coverage) — and, absent proxy warming, the whole list. When
/// warming is enabled and the best holder is distant, the non-holders that
/// beat it by the margin are PREPENDED nearest first, so the request routes
/// through the nearest one (it serves via window-paced pull-through and
/// becomes the first regional copy) and falls over through the remaining
/// proxies to the direct holder. Proxies rank on RTT only; never a gamble (an
/// empty proxy order leaves the list as just the holders).
///
/// When `holders` is empty the blob is cache-cold — its normal first-fetch
/// state, since every object is born origin-only with zero cache holders
/// (#1911). The order is then the `non_holders`, nearest RTT first, as real
/// pull-through serve targets: a node serves an authorized miss from its own
/// origin (or node-to-node) even when its cache store misses. This fallback is
/// independent of proxy warming — it is how a caching network bootstraps cold
/// content, not a latency optimization — so it applies whether warming is on
/// or off. `warming_lead` and `coverage_by_node` are both empty here, since no
/// holder answered.
pub(crate) fn failover_order(
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
                // candidate the client assembled from measured RTTs alone.
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
    holders.sort_by(|a, b| {
        let (a, b) = (holder_rank(a), holder_rank(b));
        a.0.cmp(&b.0)
            .then(a.1.cmp(&b.1))
            .then(a.2.cmp(&b.2))
            .then(a.3.total_cmp(&b.3))
    });
    // Captured before `holders` is consumed into `order` below — each
    // holder's coverage, for the multi-source lane builder (#1506).
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
        // shortlist and operator logging. Leaving it unset keeps a spoofed
        // region from riding along.
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
/// [`discovery::SELECT_K`] candidates, and [`open_and_order`] them. Returns the
/// ordered candidate list `fetch` tries in turn (#1174).
/// Callers must have already unwrapped `chain.capacity_bond` into the
/// "auto-discovery needs `capacity_bond_address`" error, which is why the
/// address is a separate parameter rather than read back off `chain`.
async fn discover_provider(
    (chain, site): (&ChainConfig, &PullSite),
    capacity_bond: Address,
    relay_hint: Option<&RelayUrl>,
    hash: [u8; 32],
    opts: &DiscoveryOptions,
    open: OpenOpts<'_>,
) -> anyhow::Result<ResolvedTargets> {
    let mark_round_start = || {
        if let Some(marks) = open.marks {
            marks.probe_started();
        }
    };
    // Open-less fast path: when the store already holds enough fresh,
    // unsuppressed candidates, skip the discovery round entirely and rank
    // by the store's own EWMA latency. Each lane then opens with
    // pull-through. Falls through to the bootstrap + open path whenever the
    // store can't back a full candidate set — never a new failure mode, only
    // a possible extra round.
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
    // them and open each for a fresh RTT, issuing NO `getRegisteredNodes`.
    // This decouples "skip the registry read" (bounded by the 24h identity
    // horizon) from "skip the round" (bounded by the 10-min latency TTL, the
    // fast path above). Falls through to the registry read when too few
    // identities are fresh. `--rediscover` forces the read (checked above
    // alongside the fast path).
    if !opts.rediscover {
        let cached = identity_fresh_candidates(&peer_store, &store_cfg, now_secs());
        let selected = select_with_widening(
            cached,
            site.region.as_deref(),
            site.region_allowlist.as_slice(),
            &store_cfg,
        );
        if selected.len() >= store_cfg.min_fresh_candidates {
            let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
            mark_round_start();
            let targets = open_and_order(
                &selected,
                relay_hint,
                hash,
                open.spec,
                opts.proxy_warming,
                &slash_dom,
                open.round,
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
                targets.samples.clone(),
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
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
    mark_round_start();
    let targets = open_and_order(
        &selected,
        relay_hint,
        hash,
        open.spec,
        opts.proxy_warming,
        &slash_dom,
        open.round,
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
    // The answers' stats are harvested on both paths.
    //
    // Off the fetch's critical path: the handle is intentionally dropped,
    // never awaited here (tests await it directly for determinism).
    drop(spawn_harvest(
        &site.data_dir,
        Vec::new(),
        targets.samples.clone(),
    ));
    Ok(targets)
}

/// Build an open-less [`ResolvedTargets`] straight from the peer store: rank
/// every [`crate::PeerRecord::selectable`] record by its
/// EWMA `latency_ms` (ascending, `None` sorts last), project each back to a
/// [`NodeCandidate`], and admit per-operator via
/// [`discovery::admit_sources`]. Returns `None` when fewer than
/// `cfg.min_fresh_candidates` records are selectable, or when admission still
/// leaves the set below that floor — either way the caller falls back to a
/// discovery round. Takes no connection, so it structurally opens nothing:
/// each lane opens its own stream, with pull-through.
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
        samples: Vec::new(),
        pinned: false,
        size_hint: None,
        late: LateSlot::default(),
        opened: OpenedSlot::default(),
    })
}

/// Gather the cached peers whose identity is fresh enough to build a candidate
/// set from without re-reading the registry: identity confirmed within
/// [`crate::StoreConfig::identity_refresh_secs`], not
/// failure-suppressed, and not past the prune horizon. Latency freshness is NOT
/// required — this is exactly the identity-fresh/latency-stale case that
/// `store_fast_path` rejects; the caller opens on these records for a fresh
/// RTT. Returns them projected to [`NodeCandidate`], unranked (the round orders
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
/// store's freshness floor. Shared by the live registry path, the
/// registry-read-skip path and a bundle pull's per-entry sample
/// ([`crate::bundle`]), so all three rank identically.
pub(crate) fn select_with_widening(
    candidates: Vec<NodeCandidate>,
    region: Option<&str>,
    allow: &[decdn_protocol::Region],
    cfg: &crate::StoreConfig,
) -> Vec<NodeCandidate> {
    select_up_to(candidates, region, allow, cfg, discovery::SELECT_K)
}

/// [`select_with_widening`] capped at `k` candidates instead of
/// [`discovery::SELECT_K`]. A price census ([`crate::quote::census`]) passes
/// `usize::MAX`, so it keeps every node the allowlist keeps.
pub(crate) fn select_up_to(
    candidates: Vec<NodeCandidate>,
    region: Option<&str>,
    allow: &[decdn_protocol::Region],
    cfg: &crate::StoreConfig,
    k: usize,
) -> Vec<NodeCandidate> {
    // Only a non-empty allowlist can trigger the widening re-run, so keep the
    // unfiltered copy only in that case — the common empty-allowlist path (no
    // filtering, no widening possible) does no extra allocation.
    let widen_fallback = (!allow.is_empty()).then(|| candidates.clone());
    let selected = discovery::select_candidates_filtered(candidates, region, k, allow);
    if let Some(widened) = widen_fallback
        && selected.len() < cfg.min_fresh_candidates
    {
        return discovery::select_candidates_filtered(widened, region, k, &[]);
    }
    selected
}

/// Persist a discovery session's identity + answer stats off the fetch's
/// critical path: `upsert_identity` for every registry candidate (whether or
/// not it was asked), `record_sample` for each answer's triple, then
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

/// The body of [`spawn_harvest`]. Answer samples are the only input to the
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
        file(&node_id, "an answer sample", written);
    }
    if let Err(e) = store.prune_and_cap(now, cfg) {
        tracing::debug!("could not prune the peer store: {e:#}");
    }
    if tally.writes > 0 && tally.failed == tally.writes {
        tracing::warn!(
            writes = tally.writes,
            "the peer store refused every write of this discovery round; \
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
/// or answers whose signatures do not recover to their operators.
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
                samples: Vec::new(),
                pinned: false,
                size_hint: None,
                late: LateSlot::default(),
                opened: OpenedSlot::default(),
            })
        }
    }
}

/// The ordered holder list plus what discovery learned about each holder.
pub struct ResolvedTargets {
    /// The candidates, in failover order (#1174).
    pub(crate) candidates: Vec<NodeCandidate>,
    /// Each holder's [`decdn_protocol::Coverage`] (#1506), keyed by `node_id` —
    /// what a holder's lane is planned against instead of assuming every
    /// candidate is a full holder. Empty on the pinned `--node-id` path: a
    /// pinned node is a full holder whatever it answers, and a lookup miss
    /// there (as everywhere else) reads as "no measured coverage", a full
    /// holder.
    pub(crate) coverage_by_node: HashMap<PublicKey, decdn_protocol::Coverage>,
    /// `(node_id, rtt_ms, rate_per_mb)` for each node whose answer verified
    /// this fetch — harvested into the peer store.
    pub(crate) samples: Vec<Sample>,
    /// The one candidate is the `--node-id` the user pinned. It counts as a
    /// holder, as a node that answers `ok` does.
    pub(crate) pinned: bool,
    /// The blob size the nearest holder proved
    /// ([`discovery::Probed::total_bytes`]), or else a size an earlier fetch
    /// proved or verified ([`OpenOpts::known_size`]). `None` when nothing was
    /// asked and no size is known (the peer-store fast path, a pinned
    /// rediscovery), when no holder stated it, or when a pinned `--node-id`'s
    /// open failed or was refused.
    pub(crate) size_hint: Option<crate::ProvenSize>,
    /// The opens a streamed round left pending ([`OpenRound::Stream`]).
    /// Empty on every other path.
    pub(crate) late: LateSlot,
    /// The holders' streams the round left open, keyed by node: the fetch
    /// adopts the winner's and cancels the rest. Empty when no holder
    /// answered, on the fast path, and for a probe round.
    pub(crate) opened: OpenedSlot,
}

impl std::fmt::Debug for ResolvedTargets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedTargets")
            .field("candidates", &self.candidates.len())
            .field("pinned", &self.pinned)
            .field("size_hint", &self.size_hint)
            .field("opened", &self.opened.len())
            .finish_non_exhaustive()
    }
}

impl ResolvedTargets {
    /// Each candidate's signed per-MiB rate, in failover order: the rate its
    /// answer in this resolve signed, else the rate the peer store last
    /// recorded for it ([`crate::quote::store_rate`]). A candidate with
    /// neither is left out. `decdn get` prints its estimate from these
    /// before the pull starts (#2600).
    #[doc(hidden)]
    #[must_use]
    pub fn rate_hints(&self, store: &crate::PeerStore) -> Vec<crate::quote::RateHint> {
        self.candidates
            .iter()
            .filter_map(|c| {
                let answered = self
                    .samples
                    .iter()
                    .find(|(node_id, _, _)| *node_id == c.node_id)
                    .map(|&(_, _, rate)| rate);
                let rate_per_mb = answered.or_else(|| {
                    crate::quote::store_rate(store, &c.node_id, now_secs()).map(|(_, rate)| rate)
                })?;
                Some(crate::quote::RateHint {
                    provider: c.eth_address,
                    rate_per_mb,
                })
            })
            .collect()
    }
}

/// The one holder of a pinned `--node-id` at `provider`, with no size.
/// A fetch that knows the blob's size ([`OpenOpts::known_size`]) takes that
/// as its first size; any other fetch takes one from an open of the node
/// ([`open_pinned`]).
pub(crate) fn pinned_targets(node_id: PublicKey, provider: Address) -> ResolvedTargets {
    ResolvedTargets {
        candidates: vec![NodeCandidate {
            node_id,
            eth_address: provider,
            region_hint: None,
            // A `--node-id`-pinned target takes its direct address from
            // `--addr` at the dial site, not from the registry.
            multiaddrs: Bytes::new(),
        }],
        // A pinned node is a full holder whatever it answers.
        coverage_by_node: HashMap::new(),
        // A pinned fetch harvests no peer-store sample.
        samples: Vec::new(),
        pinned: true,
        size_hint: None,
        late: LateSlot::default(),
        opened: OpenedSlot::default(),
    }
}

/// Whether `opts` can resolve holders on `chain` at all: a pinned node, or a
/// `CapacityBond` to discover them from. A fetch checks this before it
/// unlocks its keys, so a configuration fault never prompts for a password.
///
/// # Errors
///
/// A [`ResolveConfigFault`] when discovery needs a `CapacityBond` the chain
/// does not name.
pub fn check_resolvable(opts: &DiscoveryOptions, chain: &ChainConfig) -> anyhow::Result<()> {
    if opts.pinned.is_some() || chain.capacity_bond.is_some() {
        return Ok(());
    }
    Err(anyhow::Error::new(ResolveConfigFault(
        "auto-discovery needs capacity_bond_address (--capacity-bond-address or \
         blockchain.capacity_bond_address), or pass --node-id to dial directly"
            .to_string(),
    )))
}

/// Resolve the holders to fetch from (#1174): the pinned node as the one
/// holder, with the size its open proves as the fetch's size, or
/// auto-discovery (#936) when nothing is pinned (deriving each provider from
/// its node's registry entry), in failover order. The acquire loop stripes
/// across them; a set that goes stale is refreshed by a later discovery with
/// [`DiscoveryOptions::rediscover`] forced, which reads the registry rather
/// than the peer store. `open` says where the round's start is recorded, how
/// the round ends, and what each open sends.
///
/// # Errors
///
/// A [`ResolveConfigFault`] when discovery needs a `CapacityBond` the chain
/// does not name, or when every answer fails its verification; any other
/// error when no asked node can serve the blob.
pub async fn resolve_target_node(
    opts: &DiscoveryOptions,
    (chain, site): (&ChainConfig, &PullSite),
    relays: &[RelayUrl],
    hash: [u8; 32],
    open: OpenOpts<'_>,
) -> anyhow::Result<ResolvedTargets> {
    // The pinned open reads a proven size and is no reachability pre-check: a
    // failed open ends nothing, and a known size needs no open. The endpoint
    // is discovery-enabled, so a node id resolves through its discovery and
    // default relays even with no direct address or configured relay. A
    // pinned node is its own only holder: a fault cools it and the fetch
    // waits for it to return.
    if let Some(pinned) = opts.pinned {
        if let Some(size) = open.known_size {
            let mut targets = pinned_targets(pinned.node_id, pinned.provider);
            targets.size_hint = Some(size);
            return Ok(targets);
        }
        return Ok(open_pinned(chain, relays.first().cloned(), pinned, hash, open).await);
    }

    check_resolvable(opts, chain)?;
    let capacity_bond = chain.capacity_bond.ok_or_else(|| {
        anyhow::Error::new(ResolveConfigFault(
            "auto-discovery needs capacity_bond_address".to_string(),
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
    let mut order = discover_provider(
        (chain, site),
        capacity_bond,
        relays.first(),
        hash,
        opts,
        open,
    )
    .await?;
    if order.size_hint.is_none() {
        order.size_hint = open.known_size;
    }
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

/// The pinned node as a candidate and its dial target, with its `--addr` as
/// a direct address when given.
pub(crate) fn pinned_target(
    pinned: PinnedNode,
    relay: Option<RelayUrl>,
) -> (NodeCandidate, EndpointAddr) {
    let cand = NodeCandidate {
        node_id: pinned.node_id,
        eth_address: pinned.provider,
        region_hint: None,
        multiaddrs: Bytes::new(),
    };
    let mut target = probe_target(&cand, relay);
    if let Some(addr) = pinned.addr {
        target = target.with_ip_addr(addr);
    }
    (cand, target)
}

/// How long the size probe of a pinned node may take. The size saves at most
/// the credit window a header-only open makes the node write, so a slow probe
/// is not worth the wait: the fetch takes its size from that open instead.
const PINNED_PROBE_TIMEOUT_MS: u64 = 2_000;

/// The pinned node as its own only holder, opened with pull-through for the
/// fetch's first leg (`open.spec.span`): the size its answer proves is the
/// fetch's size, and its stream is the first leg's, which the fetch adopts.
/// An open that fails, that does not verify, or that the node refuses leaves
/// the size to the lane's own open: the node stays the fetch's only holder
/// either way. The open waits for no direct path beyond the grace every
/// discovery open takes.
pub(crate) async fn open_pinned(
    chain: &ChainConfig,
    relay: Option<RelayUrl>,
    pinned: PinnedNode,
    hash: [u8; 32],
    open: OpenOpts<'_>,
) -> ResolvedTargets {
    let (cand, target) = pinned_target(pinned, relay);
    if let Some(marks) = open.marks {
        marks.probe_started();
    }
    let started = std::time::Instant::now();
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
    let res = crate::discovery_open(
        open.spec.conns,
        &target,
        cand.eth_address,
        open.spec.ctx,
        &slash_dom,
        hash,
        open.spec.namespace_id,
        open.spec.span,
        true,
        Duration::from_millis(SELECT_OPEN_TIMEOUT_MS),
    )
    .await;
    log_open_result(&cand, &res, started);
    let (outcome, opened) = classify_open(&cand, res, open.spec.max_rate_per_mb);
    if matches!(outcome, OpenOutcome::NonHolder(..)) {
        if chain.capacity_bond.is_none() {
            tracing::warn!(
                node = %pinned.node_id,
                "the pinned node does not hold this blob, and with no CapacityBond \
                 configured this client sends no identity binding, so the node \
                 refuses to fill the miss"
            );
        } else {
            tracing::info!(
                node = %pinned.node_id,
                "the pinned node did not serve this blob at its open; the lane asks it \
                 again once it pays from its pool"
            );
        }
    }
    let mut targets = pinned_targets(pinned.node_id, pinned.provider);
    targets.size_hint = answered_size_hint(&outcome);
    if let Some(opened) = opened {
        targets.opened = OpenedSlot::new(HashMap::from([(pinned.node_id, opened)]));
    }
    targets
}

/// The pinned node as its own only holder, with the size its probe proves as
/// the fetch's size: [`open_pinned`] over a probe, for a bundle entry.
/// Without a size, the fetch reads one from the header of a paid open, and the node can write up to a credit window into that
/// stream before the fetch drops it. A probe that fails, that does not verify,
/// or whose node does not hold the blob or names no size leaves the size to
/// that header: the node stays the fetch's only holder either way. One
/// attempt, with no backoff on a shed probe: a pinned client sends every
/// probe to the one node, and a shed one costs only the header open. The
/// probe waits for no direct path, since its RTT goes unused.
pub(crate) async fn probe_pinned(
    endpoint: &Endpoint,
    chain: &ChainConfig,
    relay: Option<RelayUrl>,
    pinned: PinnedNode,
    hash: [u8; 32],
    marks: Option<&dyn FetchMarks>,
) -> ResolvedTargets {
    let (cand, target) = pinned_target(pinned, relay);
    if let Some(marks) = marks {
        marks.probe_started();
    }
    let started = std::time::Instant::now();
    let timestamp_us = micros_now();
    let res = crate::probe::probe_answer_once(
        endpoint,
        target,
        hash,
        timestamp_us,
        Duration::from_millis(PINNED_PROBE_TIMEOUT_MS),
    )
    .await;
    log_probe_result(&cand, &res, started);
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
    let outcome = classify_probe(&cand, res, hash, timestamp_us, &slash_dom);
    if matches!(outcome, OpenOutcome::NonHolder(..)) {
        if chain.capacity_bond.is_none() {
            tracing::warn!(
                node = %pinned.node_id,
                "the pinned node does not hold this blob, and with no CapacityBond \
                 configured this client sends no identity binding, so the node \
                 refuses to fill the miss"
            );
        } else {
            tracing::info!(
                node = %pinned.node_id,
                "the pinned node does not hold this blob; it serves it only if it can \
                 fill the miss"
            );
        }
    }
    let mut targets = pinned_targets(pinned.node_id, pinned.provider);
    targets.size_hint = answered_size_hint(&outcome);
    targets
}

/// The size an outcome offers as the fetch's size: the size a holder proves
/// in an answer that verifies, else none.
const fn answered_size_hint(outcome: &OpenOutcome) -> Option<crate::ProvenSize> {
    match outcome {
        OpenOutcome::Holder(probed, _) => probed.total_bytes,
        _ => None,
    }
}

/// Build a dial target for a discovered candidate: its `node_id`, an optional
/// relay hint, and its registry multiaddrs as direct-address hints (ADR 001 §
/// Node Discovery). See [`crate::lanes::lane_target`] for why both ride the first packet.
pub(crate) fn probe_target(cand: &NodeCandidate, relay: Option<RelayUrl>) -> EndpointAddr {
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

/// Discovery fixtures shared by the target and source tests.
#[cfg(test)]
pub(crate) mod tests_support;
