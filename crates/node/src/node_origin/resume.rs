//! The node-to-node buffered miss pull, as a **resumable** progressive pull with
//! a reactive mid-pull deposit top-up (#1530).
//!
//! # What this replaces, and why
//!
//! [`Origin::fetch`](decdn_cache::origin::Origin::fetch) used to reach the wire
//! through `stream_fetch_shared` at a fixed `byte_offset == 0`. That call buffers
//! the whole blob and has no resume point, which made the reactive half of the
//! two-tier deposit (ADR 003 § Two-tier deposit) impossible on this leg: answering
//! a genuine `InsufficientDeposit` there would mean retrying from zero and
//! **re-paying for every delivered byte**. Worse, after real exhaustion the
//! from-zero retry re-spends the fresh working deposit on bytes it already bought
//! and re-exhausts at the same offset, so it cannot make progress at all. The
//! daemon therefore shipped with the proactive low-water refill only, and a single
//! miss pull larger than the initial deposit simply failed.
//!
//! So this module drives the same loop the CLI streaming fetch drives
//! (`crates/cli/src/commands/fetch.rs`): `open_progressive_pull` at a live
//! `byte_offset`, [`pull_to_sink`] into a sink, and on a genuine ceiling hit an
//! on-chain top-up followed by a resume at the **paid frontier** — the largest
//! chunk-group boundary the accepted vouchers provably cover
//! ([`content_paid_frontier`]). No delivered byte is skipped or paid for twice.
//!
//! # Why the loop is here and not in `client-pull`
//!
//! The pure decisions are already shared and stay shared: [`genuine_exhaustion`],
//! [`resumable_watermark`], [`content_paid_frontier`], [`MAX_RESUME_ATTEMPTS`].
//! The loops around them are not the same shape. The CLI rewinds a FILE and needs
//! a stale-partial restart branch, because its prefix is an unverified artifact of
//! an earlier process; this one rewinds an in-memory `Vec` whose every byte was
//! chunk-group-verified in this very frame, so that branch would be a way to throw
//! away paid bytes on the first `NotFound`. The CLI funder-gates; the node is
//! always its own funder. The CLI prints; the node meters and classifies. The CLI
//! persists its watermark by hand; the node persists through a `Drop` guard. Five
//! differences, all of them injected dependencies — pushing the body down would
//! buy one shared `loop` keyword behind six trait objects.
//!
//! # The sink is a `Vec`, on purpose
//!
//! [`Origin::fetch`](decdn_cache::origin::Origin::fetch) hands the cache engine an
//! `OriginFetch::found_one_shot(bytes)`, so this path owes its caller one
//! contiguous buffer either way. Keeping it lets a retry rewind with a
//! `Vec::truncate` — the in-memory twin of the CLI's `set_len` + `seek` — and it
//! is still a strict improvement on what it replaces: the buffered requester held
//! the bao wire form in a `BytesMut` AND the decoded content in a `Vec`, so peak
//! memory drops from roughly 2x the blob to 1x.

use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use decdn_protocol::MB_BYTES;
use decdn_protocol::client::NO_NAMESPACE;
use iroh::{EndpointAddr, PublicKey};
use tracing::{debug, info, warn};

use crate::client_requester::sink::{content_paid_frontier, pull_to_sink};
use crate::client_requester::{
    ChannelContext, ChannelLedger, Cumulative, LocalPullFault, MAX_RESUME_ATTEMPTS, PullDeadlines,
    ResumeOffsetPastEnd, UpstreamPull, UpstreamRefused, VoucherProgress, effective_rate_ceiling,
    genuine_exhaustion, open_progressive_pull as open_progressive_upstream, resumable_watermark,
};
use decdn_protocol::client::StreamError;

use super::{NodeOriginDeps, now_micros, persist_buyer_progress};
use crate::selection::Candidate;

/// How many times ONE call of [`pull_blob`] answers a genuine mid-pull
/// `InsufficientDeposit` with an on-chain `topUp` before giving up.
///
/// Deliberately **1**, where the CLI's [`MAX_TOPUP_ATTEMPTS`] is 3. The node tops
/// up from the initial deposit straight to the working deposit — 0.5 USDC to 10
/// USDC under the shipped defaults, a 20x jump — so one top-up covers any blob
/// inside `cache.max_blob_size_mb` at any sane rate. Needing a second means the
/// upstream's quoted rate is wrong for the working deposit, which is a pricing
/// problem no amount of funding fixes; each extra attempt costs a transaction plus
/// a settle wait, both of which land on a client that is waiting.
///
/// [`MAX_TOPUP_ATTEMPTS`]: decdn_client_pull::MAX_TOPUP_ATTEMPTS
const MAX_REACTIVE_TOPUPS: u32 = 1;

/// One step of the post-top-up settle wait. Small enough that the common case
/// (the upstream's watcher was already close to its next poll) costs little.
const SETTLE_POLL_STEP: Duration = Duration::from_millis(500);

/// How many [`SETTLE_POLL_STEP`]s to spend waiting for the UPSTREAM's chain watcher
/// to observe our just-landed `ChannelToppedUp` before treating its refusal as real.
///
/// The upstream gates serving on the deposit it has observed, so between our receipt
/// and its next poll it correctly refuses a resume for a channel it still believes is
/// empty. Waiting that out is money-safe: no voucher is sent and `byte_offset` does
/// not move, so the worst case is wasted wall clock.
///
/// Sized at **two poll intervals** (14 s at the 7 s default), not a hard-coded
/// constant. One interval is the bare minimum and leaves no room for the watcher's
/// own head TTL, and anything derived from our own timeouts would drift the moment an
/// operator retunes the chain lane. Two clears a full poll plus the head cache in the
/// ordinary case, and with [`MAX_REACTIVE_TOPUPS`] at 1 it is spent at most once per
/// candidate — comfortably inside the per-candidate share of `outer_pull_deadline`
/// (45 s at defaults), so it cannot starve the fallback walk.
fn settle_wait_budget(event_poll_interval: Duration) -> u32 {
    let budget = event_poll_interval.saturating_mul(2).as_millis();
    let step = SETTLE_POLL_STEP.as_millis().max(1);
    u32::try_from(budget / step).unwrap_or(u32::MAX)
}

/// What a failed attempt means for the loop. Pure, so the policy is testable
/// without a network or a chain.
#[derive(Debug, PartialEq, Eq)]
enum ResumeAction {
    /// The upstream has not observed our top-up yet — sleep and re-open at the
    /// same offset. Costs nothing but time.
    SettleWait,
    /// A genuine ceiling hit, corroborated by our own ledger: raise the channel
    /// toward this target and resume at the paid frontier.
    TopUp(U256),
    /// The upstream holds a voucher we do not — reseed the ledger and retry.
    Reseed,
    /// Nothing left to try; hand the error to the classifier.
    Terminal,
}

/// The loop's budgets, so [`decide`] stays pure and the caller owns the counters.
#[derive(Debug, Clone, Copy)]
struct ResumeBudgets {
    /// Top-ups already spent on this pull.
    topups: u32,
    /// Reseed retries already spent on this pull.
    attempts: u32,
    /// Settle-wait steps already slept since the last top-up.
    settle_waits: u32,
    /// Settle-wait steps allowed in total.
    max_settle_waits: u32,
    /// Whether a top-up has landed whose settlement we are still waiting on.
    awaiting_topup_settle: bool,
    /// The graduation target, or `U256::ZERO` to disable reactive top-up.
    working_deposit: U256,
}

/// Whether `err` is the shape an upstream produces when it has not yet seen our
/// top-up: it either refuses the resume offset outright, or answers `NotFound` —
/// which is what the pre-serve deposit gate collapses to, deliberately
/// indistinguishable from an absent blob.
///
/// The CLI's twin of this also treats it as "the partial may belong to another
/// blob" and restarts from zero. This one must not: our prefix was verified in
/// this frame, so there is no wrong-blob case to recover from — only watcher lag
/// or an eviction, and neither is helped by throwing paid bytes away.
fn awaiting_settle(err: &anyhow::Error) -> bool {
    if err.downcast_ref::<ResumeOffsetPastEnd>().is_some() {
        return true;
    }
    err.downcast_ref::<UpstreamRefused>()
        .is_some_and(|refused| matches!(refused.error(), StreamError::NotFound))
}

/// The cost of the voucher that was refused: `ceil(interval_bytes * rate / MiB)`,
/// the same arithmetic `ChannelLedger` itself prices with.
///
/// Priced off the upstream's QUOTED rate and cadence from the last successful
/// open — never the buyer's `max_rate_per_mb` ceiling, which bounds what we are
/// willing to pay, not what the next voucher actually costs.
fn next_voucher_cost(interval_bytes: u64, rate_per_mb: u64) -> U256 {
    U256::from(interval_bytes)
        .saturating_mul(U256::from(rate_per_mb))
        .div_ceil(U256::from(MB_BYTES))
}

/// Classify a failed attempt. See [`ResumeAction`].
fn decide(
    err: &anyhow::Error,
    ctx: &ChannelContext,
    committed: Cumulative,
    interval_bytes: u64,
    rate_per_mb: u64,
    budgets: ResumeBudgets,
) -> ResumeAction {
    // Ahead of everything: a refusal we PROVOKED by topping up a moment ago is not
    // evidence about the blob or the peer, and the remedy is to wait, not to spend.
    if budgets.awaiting_topup_settle
        && budgets.settle_waits < budgets.max_settle_waits
        && awaiting_settle(err)
    {
        return ResumeAction::SettleWait;
    }

    let remaining = ctx.deposit.saturating_sub(committed.amount);
    if budgets.topups < MAX_REACTIVE_TOPUPS
        && !budgets.working_deposit.is_zero()
        && genuine_exhaustion(
            err,
            ctx,
            committed,
            remaining,
            next_voucher_cost(interval_bytes, rate_per_mb),
        )
    {
        return ResumeAction::TopUp(budgets.working_deposit);
    }

    // `genuine_exhaustion` already routed an ADVANCING watermark bundle here rather
    // than to the top-up: the upstream knows about a voucher we do not, and
    // reseeding — not funding — is what heals that.
    if budgets.attempts < MAX_RESUME_ATTEMPTS && resumable_watermark(err, ctx).is_some() {
        return ResumeAction::Reseed;
    }

    ResumeAction::Terminal
}

/// Who we are pulling from. Bundled because the four identifiers travel together
/// through every stage of the loop and none of them changes across a resume.
#[derive(Debug, Clone, Copy)]
pub(super) struct PullTarget<'a> {
    /// The candidate's iroh identity, for dialling and for scoring.
    pub pk: PublicKey,
    /// Its bonded operator address — the channel counterparty and the expected
    /// `slash_sig` signer.
    pub provider_addr: Address,
    /// The ranked candidate, for its probe-relative rate bound.
    pub candidate: &'a Candidate,
    /// The blob.
    pub hash_bytes: [u8; 32],
}

/// The per-leg accounting a resume needs, re-anchored on every successful open.
///
/// Kept together because using one without the other is the bug: the paid frontier
/// is `content_paid_frontier(offset, total, committed.bytes - baseline)`, and a
/// baseline from a DIFFERENT leg sums two independent bao range encodings — it
/// re-bills the first leg's span plus its root->offset proof path, and the inflated
/// budget maps to a frontier PAST the true paid one, skipping content unbilled.
/// That is the exact under-pay `content_paid_frontier` exists to prevent.
#[derive(Debug, Clone, Copy)]
struct LegAnchor {
    /// Content offset this leg started at.
    offset: u64,
    /// Channel-cumulative WIRE watermark at that moment.
    committed_bytes: U256,
}

/// A completed resumable pull: the blob, and the wall clock spent NOT pulling it.
pub(super) struct PulledBlob {
    /// The decoded content, verified per chunk group as it landed.
    pub bytes: Vec<u8>,
    /// Time spent waiting on chain settlement and the `topUp` receipt.
    ///
    /// Subtracted from the pull's elapsed time before it reaches the delivery-speed
    /// reputation signal. Blocking on a transaction says nothing about how fast the
    /// upstream serves, and folding it in would defame a peer for our funding — the
    /// same reasoning that already keeps the watermark fsync out of `elapsed`.
    pub paid_wait: Duration,
}

/// Pull the whole blob from one candidate, resuming across a reactive top-up.
///
/// `ctx` is `&mut` because a landed top-up changes `deposit`, and the resumed
/// leg's headroom arithmetic — the thing that decides whether the NEXT rejection
/// is genuine — reads it.
///
/// # Errors
///
/// The last attempt's error, typed exactly as the buffered path used to return it
/// so [`super::classify_pull_failure`] still sees `PullStalled` / `PullTimeout` /
/// `UpstreamRefused` / `UpstreamVoucherRejected` / `HashMismatch` /
/// `LocalPullFault` / `BlobTooLargeClaim` / `RateAboveCeiling`.
pub(super) async fn pull_blob(
    deps: &NodeOriginDeps,
    target: PullTarget<'_>,
    ctx: &mut ChannelContext,
    ledger: &Arc<ChannelLedger>,
    deadlines: PullDeadlines,
) -> anyhow::Result<PulledBlob> {
    let mut state = LoopState::new(deps, ledger);

    loop {
        let Err(err) = stream_leg(deps, target, ctx, ledger, deadlines, &mut state).await else {
            return Ok(PulledBlob {
                bytes: state.buf,
                paid_wait: state.paid_wait,
            });
        };

        let committed = ledger.committed();
        match decide(
            &err,
            ctx,
            committed,
            state.voucher_interval_bytes,
            state.quoted_rate_per_mb,
            state.budgets,
        ) {
            ResumeAction::SettleWait => {
                state.budgets.settle_waits += 1;
                debug!(
                    provider = %target.provider_addr,
                    wait = state.budgets.settle_waits,
                    of = state.budgets.max_settle_waits,
                    "node-origin: waiting for the upstream's chain watcher to observe our top-up"
                );
                let started = Instant::now();
                tokio::time::sleep(SETTLE_POLL_STEP).await;
                state.paid_wait = state.paid_wait.saturating_add(started.elapsed());
            }
            ResumeAction::TopUp(want) => {
                let started = Instant::now();
                let funded = fund(deps, target.provider_addr, ctx, ledger, want).await;
                state.paid_wait = state.paid_wait.saturating_add(started.elapsed());
                if !funded {
                    // No headroom was added, so the original exhaustion stands at the
                    // same offset. Return THAT error rather than a funding one, so the
                    // classifier's channel remedy still keys on the voucher rejection.
                    return Err(err);
                }
                state.budgets.topups += 1;
                state.byte_offset = resume_frontier(ledger, state.anchor, state.total_bytes);
                info!(
                    provider = %target.provider_addr,
                    deposit = %ctx.deposit,
                    byte_offset = state.byte_offset,
                    total_bytes = state.total_bytes,
                    "node-origin: channel exhausted mid-pull; topped up and resuming at the paid frontier (#1530)"
                );
                state.budgets.awaiting_topup_settle = true;
                state.budgets.settle_waits = 0;
            }
            ResumeAction::Reseed => {
                // Only an ADVANCING bundle is a desync worth retrying; `reseed` reports
                // that and refuses to regress. A bundle merely echoing our own committed
                // watermark — which the upstream attaches to every watermark-gated
                // rejection once a voucher has been accepted — would otherwise burn the
                // whole budget re-sending vouchers it has already refused.
                let Some(bundle) = resumable_watermark(&err, ctx) else {
                    return Err(err);
                };
                if !ledger.reseed(Cumulative::from(bundle)) {
                    return Err(err);
                }
                state.budgets.attempts += 1;
                debug!(
                    provider = %target.provider_addr,
                    attempt = state.budgets.attempts,
                    "node-origin: reseeded the voucher watermark from the upstream's signed bundle"
                );
            }
            ResumeAction::Terminal => return Err(err),
        }
    }
}

/// Everything one call of [`pull_blob`] carries across its legs.
///
/// A struct rather than a fistful of `let mut`s so [`stream_leg`] can own the
/// post-open bookkeeping — which is the half that must not drift, since the anchor,
/// the quoted terms, and the settle flag all have to be updated by the SAME
/// successful open or the resume arithmetic silently reads a previous leg's numbers.
struct LoopState {
    /// The verified content decoded so far. Truncated back to the resume offset at
    /// the start of each leg, so no byte is ever appended twice.
    ///
    /// Deliberately NOT `with_capacity(total_bytes)`: `max_blob_size_bytes == 0` is
    /// the "unlimited" sentinel, so a hostile upstream could claim `u64::MAX` and we
    /// would abort on the allocation before the size gate ever ran. Growing costs a
    /// few reallocs on a path already bounded by the network.
    buf: Vec<u8>,
    /// Content offset the next leg starts at.
    byte_offset: u64,
    /// Wall clock spent on chain settlement rather than on the transfer.
    paid_wait: Duration,
    anchor: LegAnchor,
    /// The upstream's promised blob size, from the most recent successful open.
    total_bytes: u64,
    /// Its quoted rate and voucher cadence, used to price the voucher a rejection
    /// refused. Only ever read after at least one successful open, which is exactly
    /// when an `InsufficientDeposit` can occur.
    quoted_rate_per_mb: u64,
    voucher_interval_bytes: u64,
    budgets: ResumeBudgets,
}

impl LoopState {
    fn new(deps: &NodeOriginDeps, ledger: &ChannelLedger) -> Self {
        Self {
            buf: Vec::new(),
            byte_offset: 0,
            paid_wait: Duration::ZERO,
            anchor: LegAnchor {
                offset: 0,
                committed_bytes: ledger.committed().bytes,
            },
            total_bytes: 0,
            quoted_rate_per_mb: 0,
            voucher_interval_bytes: 0,
            budgets: ResumeBudgets {
                topups: 0,
                attempts: 0,
                settle_waits: 0,
                max_settle_waits: settle_wait_budget(deps.config.event_poll_interval),
                awaiting_topup_settle: false,
                working_deposit: deps.config.working_deposit,
            },
        }
    }
}

/// Open one leg at `state.byte_offset` and stream it into `state.buf`.
///
/// # Errors
///
/// The open's or the stream's error, typed for the classifier.
async fn stream_leg(
    deps: &NodeOriginDeps,
    target: PullTarget<'_>,
    ctx: &ChannelContext,
    ledger: &Arc<ChannelLedger>,
    deadlines: PullDeadlines,
    state: &mut LoopState,
) -> anyhow::Result<()> {
    let (header, pull) = open_leg(deps, target, ctx, ledger, deadlines, state.byte_offset).await?;

    state.total_bytes = header.total_bytes;
    state.quoted_rate_per_mb = header.rate_per_mb;
    state.voucher_interval_bytes = header.interval_bytes;
    // The upstream accepted this open, so its watcher has caught up (or never
    // lagged) — stop reading refusals as settlement lag.
    state.budgets.awaiting_topup_settle = false;
    state.budgets.settle_waits = 0;
    // A new leg begins here and nowhere else. See [`LegAnchor`].
    state.anchor = LegAnchor {
        offset: state.byte_offset,
        committed_bytes: ledger.committed().bytes,
    };
    // Rewind to exactly the verified prefix we are resuming behind, so a re-fetched
    // span is not appended twice. `pull_to_sink` writes only from `byte_offset`
    // onward. A blob past this machine's address space cannot live in a `Vec` at
    // all, and the size gate inside the open should already have refused it — so
    // this is our fault, not the peer's.
    let Ok(prefix_len) = usize::try_from(state.byte_offset) else {
        return Err(anyhow::Error::new(LocalPullFault)
            .context("resume offset exceeds this platform's addressable range"));
    };
    state.buf.truncate(prefix_len);

    pull_to_sink(
        pull,
        target.hash_bytes,
        header.total_bytes,
        state.byte_offset,
        &mut state.buf,
        None,
    )
    .await
    .map(|_| ())
}

/// Open one leg at `byte_offset`.
///
/// Split out so the caller reads as a loop rather than as an argument list: the only
/// thing that varies between legs is the offset.
async fn open_leg(
    deps: &NodeOriginDeps,
    target: PullTarget<'_>,
    ctx: &ChannelContext,
    ledger: &Arc<ChannelLedger>,
    deadlines: PullDeadlines,
    byte_offset: u64,
) -> anyhow::Result<(crate::client_requester::UpstreamPullHeader, UpstreamPull)> {
    // Open BEFORE the caller touches its buffer. The truncate there is destructive,
    // and an open fails for plenty of reasons that say nothing about the prefix (the
    // upstream evicted the blob, the link dropped, the channel is unknown) —
    // rewinding first would destroy a verified, paid-for prefix and then fail anyway.
    open_progressive_upstream(
        &deps.endpoint,
        EndpointAddr::new(target.pk),
        ctx,
        Arc::clone(ledger),
        &deps.slash_domain,
        target.provider_addr,
        target.hash_bytes,
        // `NO_NAMESPACE`, which is what `stream_fetch_shared` hard-coded for this leg.
        // `Origin::fetch` is the hash-only whole-blob path and carries no served-client
        // namespace; passing one would change upstream routing, since a
        // directory-discovered cold origin resolves its authorized-origin gate on it.
        // The window path, which DOES have a namespace, threads its own.
        NO_NAMESPACE,
        byte_offset,
        now_micros(),
        deps.config.max_blob_size_bytes,
        // Refuse a stream quote above the lower of the candidate's probe rate and the
        // configured absolute ceiling, before paying (#1375). `candidate.rate_per_mb >= 1`
        // always (`ProbeResponse::validate` rejects a zero rate and `probe_candidate`
        // drops it), so `effective_rate_ceiling` never treats the probe bound as
        // unbounded here.
        effective_rate_ceiling(target.candidate.rate_per_mb, deps.config.max_rate_per_mb),
        deadlines,
    )
    .await
}

/// Raise the channel toward `want` and update `ctx.deposit`. Returns whether the
/// resumed leg actually has more headroom than the one that just exhausted — the
/// only question the loop needs answered, since retrying without it would exhaust at
/// the same offset.
async fn fund(
    deps: &NodeOriginDeps,
    provider_addr: Address,
    ctx: &mut ChannelContext,
    ledger: &Arc<ChannelLedger>,
    want: U256,
) -> bool {
    // Persist BEFORE the funding await — the longest and most cancellation-prone
    // point in the loop. The `SettleOnDrop` guard the caller holds still covers a
    // drop, but a concurrent pull sharing this channel's ledger wants an accurate row
    // across the whole transaction, not only after it. Idempotent against a monotonic
    // store.
    let progress = VoucherProgress::from_cumulative(ledger.settlement(), ctx.prior_nonce);
    persist_buyer_progress(deps, provider_addr, ctx.channel_id, &progress);

    let new_deposit = match deps.buyer.top_up_channel(provider_addr, want).await {
        Ok(new_deposit) => new_deposit,
        Err(err) => {
            warn!(
                provider = %provider_addr,
                error = %format!("{err:#}"),
                "node-origin: reactive top-up failed; the pull ends on the original exhaustion"
            );
            deps.metrics.node_pull_reactive_topup_refused();
            return false;
        }
    };
    if new_deposit <= ctx.deposit {
        // An opener that does not fund, or a race where a concurrent proactive refill
        // already held this provider's slot.
        debug!(
            provider = %provider_addr,
            %new_deposit,
            "node-origin: reactive top-up added no headroom; ending the pull"
        );
        deps.metrics.node_pull_reactive_topup_refused();
        return false;
    }
    ctx.deposit = new_deposit;
    deps.metrics.node_pull_reactive_topup();
    true
}

/// Where to resume: the paid frontier of the leg that just exhausted.
///
/// NOT the buffer length. The credit window (ADR 003 § Credit window) lets the
/// upstream stream — and the decoder verify and flush — a full interval's worth of
/// content before the voucher paying for it is due, so the buffer runs AHEAD of
/// payment. Resuming at the buffer length would skip billing for that
/// credited-but-unpaid tail; leaving the offset unmoved would re-pay for what was
/// already accepted. The frontier is neither: `[frontier, buf.len())` is re-fetched
/// and paid exactly once, a bounded over-pay of strictly under one chunk group.
///
/// Derived in CONTENT bytes from a WIRE watermark. Vouchers pay for bao-encoded
/// content plus interleaved proof (ADR 038), so the delta against this leg's baseline
/// is wire, and `content_paid_frontier` maps it back to the largest chunk-group
/// content boundary provably inside it.
fn resume_frontier(ledger: &ChannelLedger, anchor: LegAnchor, total_bytes: u64) -> u64 {
    let paid_wire_this_leg = u64::try_from(
        ledger
            .committed()
            .bytes
            .saturating_sub(anchor.committed_bytes),
    )
    .unwrap_or(u64::MAX);
    content_paid_frontier(anchor.offset, total_bytes, paid_wire_this_leg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;
    use alloy::signers::local::PrivateKeySigner;
    use decdn_protocol::client::VoucherRejectReason;

    use crate::client_requester::UpstreamVoucherRejected;

    fn ctx_with_deposit(deposit: U256) -> ChannelContext {
        ChannelContext {
            channel_id: B256::ZERO,
            token: Address::ZERO,
            deposit,
            client_signer: Arc::new(PrivateKeySigner::random()),
            voucher_domain: alloy::dyn_abi::Eip712Domain::default(),
            prior_nonce: U256::ZERO,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
        }
    }

    fn budgets(working_deposit: U256) -> ResumeBudgets {
        ResumeBudgets {
            topups: 0,
            attempts: 0,
            settle_waits: 0,
            max_settle_waits: 28,
            awaiting_topup_settle: false,
            working_deposit,
        }
    }

    fn insufficient_deposit() -> anyhow::Error {
        anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::InsufficientDeposit,
            bundle: None,
        })
    }

    /// Two poll intervals of 500 ms steps, so the upstream clears a full poll plus
    /// its head cache — 28 steps (14 s) at the 7 s default.
    #[test]
    fn settle_budget_tracks_the_chain_poll_cadence() {
        assert_eq!(settle_wait_budget(Duration::from_secs(7)), 28);
        assert_eq!(settle_wait_budget(Duration::from_secs(1)), 4);
        // A chain lane tuned faster than one step still gets a real, if tiny, budget
        // rather than zero — a zero budget would make the top-up a coin flip.
        assert_eq!(settle_wait_budget(Duration::from_millis(250)), 1);
    }

    /// The buyer's own ledger has no headroom left, so the upstream's complaint is
    /// corroborated and funding it is the right answer.
    #[test]
    fn a_corroborated_ceiling_hit_funds_the_channel() {
        let ctx = ctx_with_deposit(U256::from(10u64));
        let committed = Cumulative {
            nonce: U256::from(1u64),
            bytes: U256::from(4096u64),
            amount: U256::from(10u64),
        };
        let working = U256::from(1000u64);
        assert_eq!(
            decide(
                &insufficient_deposit(),
                &ctx,
                committed,
                MB_BYTES,
                1_000,
                budgets(working)
            ),
            ResumeAction::TopUp(working)
        );
    }

    /// An upstream crying poverty while our own ledger still covers the next
    /// voucher is lying or buggy. We refuse to fund it — the whole point of
    /// validating the claim against our own accounting.
    #[test]
    fn a_bogus_exhaustion_claim_is_not_funded() {
        let ctx = ctx_with_deposit(U256::from(1_000_000u64));
        let committed = Cumulative {
            nonce: U256::from(1u64),
            bytes: U256::from(4096u64),
            amount: U256::from(10u64),
        };
        assert_eq!(
            decide(
                &insufficient_deposit(),
                &ctx,
                committed,
                MB_BYTES,
                1_000,
                budgets(U256::from(1000u64))
            ),
            ResumeAction::Terminal
        );
    }

    /// `buyer_working_deposit_micro_usdc = 0` disables the reactive leg, the same
    /// sentinel the proactive refill honours.
    #[test]
    fn a_zero_working_deposit_disables_the_reactive_leg() {
        let ctx = ctx_with_deposit(U256::from(10u64));
        let committed = Cumulative {
            nonce: U256::from(1u64),
            bytes: U256::from(4096u64),
            amount: U256::from(10u64),
        };
        assert_eq!(
            decide(
                &insufficient_deposit(),
                &ctx,
                committed,
                MB_BYTES,
                1_000,
                budgets(U256::ZERO)
            ),
            ResumeAction::Terminal
        );
    }

    /// One top-up per pull. A second exhaustion is a pricing problem, not a
    /// funding one, and each attempt costs a transaction a client waits through.
    #[test]
    fn reactive_topups_are_bounded() {
        let ctx = ctx_with_deposit(U256::from(10u64));
        let committed = Cumulative {
            nonce: U256::from(1u64),
            bytes: U256::from(4096u64),
            amount: U256::from(10u64),
        };
        let mut spent = budgets(U256::from(1000u64));
        spent.topups = MAX_REACTIVE_TOPUPS;
        assert_eq!(
            decide(
                &insufficient_deposit(),
                &ctx,
                committed,
                MB_BYTES,
                1_000,
                spent
            ),
            ResumeAction::Terminal
        );
    }

    /// A `NotFound` right after a top-up is the upstream's deposit gate, not a
    /// verdict about the blob — and it outranks every branch that would spend.
    #[test]
    fn a_refusal_right_after_a_topup_is_waited_out_not_funded() {
        let ctx = ctx_with_deposit(U256::from(10u64));
        let committed = Cumulative {
            nonce: U256::from(1u64),
            bytes: U256::from(4096u64),
            amount: U256::from(10u64),
        };
        let mut waiting = budgets(U256::from(1000u64));
        waiting.awaiting_topup_settle = true;
        let err = anyhow::Error::new(ResumeOffsetPastEnd {
            total_bytes: 100,
            byte_offset: 200,
        });
        assert_eq!(
            decide(&err, &ctx, committed, MB_BYTES, 1_000, waiting),
            ResumeAction::SettleWait
        );
    }

    /// The settle wait is bounded: once the budget is spent the refusal is taken
    /// at face value rather than slept on forever.
    #[test]
    fn the_settle_wait_is_bounded() {
        let ctx = ctx_with_deposit(U256::from(10u64));
        let committed = Cumulative {
            nonce: U256::from(1u64),
            bytes: U256::from(4096u64),
            amount: U256::from(10u64),
        };
        let mut spent = budgets(U256::from(1000u64));
        spent.awaiting_topup_settle = true;
        spent.settle_waits = spent.max_settle_waits;
        spent.topups = MAX_REACTIVE_TOPUPS;
        let err = anyhow::Error::new(ResumeOffsetPastEnd {
            total_bytes: 100,
            byte_offset: 200,
        });
        assert_eq!(
            decide(&err, &ctx, committed, MB_BYTES, 1_000, spent),
            ResumeAction::Terminal
        );
    }

    /// A rejection that is not about the deposit is nobody's funding problem.
    #[test]
    fn an_unrelated_failure_is_terminal() {
        let ctx = ctx_with_deposit(U256::from(10u64));
        let committed = Cumulative {
            nonce: U256::ZERO,
            bytes: U256::ZERO,
            amount: U256::ZERO,
        };
        let err = anyhow::Error::new(crate::client_requester::PullStalled {
            after: Duration::from_secs(1),
        });
        assert_eq!(
            decide(
                &err,
                &ctx,
                committed,
                MB_BYTES,
                1_000,
                budgets(U256::from(1000u64))
            ),
            ResumeAction::Terminal
        );
    }
}
