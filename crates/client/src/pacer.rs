//! The pure pacing axis of the gap-driven pull driver (#1608).
//!
//! A [`Pacer`] answers one question at each gap boundary: given a snapshot of the
//! fetch's budget state ([`PaceState`]), may the driver draw the next paid
//! segment, wait, stop, or refuse? It is a total function of the snapshot (**no
//! I/O, no async**), so the policy is unit-testable in isolation, independent of
//! any network or chain fixture.
//!
//! [`BudgetPacer`] is the client policy: it gates on the buyer's OWN deposit
//! (order-free: it never waits on the counterparty's state). A lane whose
//! deposit cannot cover the next voucher refuses; the buyer adds funds only
//! through the funding recovery step at the exhausted candidate set (ADR 003
//! § Funding recovery), never inside a lane. The node's window pacer (ADR 037)
//! is a separate impl handed to the same driver.

use alloy::primitives::U256;
use decdn_bao_range::CHUNK_GROUP_BYTES;
use decdn_protocol::client::CHUNK_BYTES;

/// The smallest pull window that keeps the fused serve-miss loop live.
///
/// [`RampPacer`] paces the pull in CONTENT bytes against
/// [`DownstreamFrontier::served_paid`], which
/// [`content_paid_frontier`](crate::sink::content_paid_frontier) derives from PAID
/// WIRE by flooring to a chunk-group boundary; [`WindowPacer`] then floors its own
/// room to whole groups. Two group-sized roundings therefore sit between what the
/// client has paid for and what the pull may fetch next, while the client releases
/// its next proof only once a whole [`CHUNK_BYTES`] of WIRE has verified, which
/// takes up to [`VERIFY_LAG_BYTES`] more past the chunk.
///
/// A window of exactly one chunk loses more to those roundings than the wire's
/// interleaved proof bytes hand back, so the pull parks with the client short of
/// the chunk it must verify to pay — a payment that can then never come. Carrying
/// the verify lag and both roundings on top of the chunk closes that gap at every
/// window size, because the ramp only ever widens the window above this floor.
///
/// A **third** group covers the serving node's prefetch. A serve leg reads one frame
/// ahead of its own credit-window check, so once that window shuts it still asks its
/// producer for up to one bao chunk group more wire — bytes the producer can only
/// get from this pull. Without reserved headroom that request parks on data the pull
/// may not draw until a payment that the parked serve leg is the one blocked from
/// collecting. The two rounding groups cannot pay for it: the liveness invariant
/// below spends them in full.
///
/// Past the floor the ramp carries no such padding, and the two windows diverge:
/// the serve leg ramps on paid wire, this pacer on the smaller paid content
/// frontier. There, liveness rests on [`DownstreamFrontier::serve_demand`], which
/// lets the pull fetch one more floor when a serve leg is parked at its frontier.
pub const PULL_WINDOW_FLOOR: u64 = CHUNK_BYTES + VERIFY_LAG_BYTES + 3 * CHUNK_GROUP_BYTES;

/// The most wire bytes a payer receives past a chunk boundary before it has
/// verified that boundary and pays for the chunk: one bao chunk group, the
/// largest item (a leaf) the decoder checks in one step. A payer pays only for
/// verified bytes, so the node's credit window floors at one chunk plus this
/// lag (ADR 003 § Credit Window): a stream at the floor then delivers the leaf
/// that straddles the boundary, and the payer can verify it and pay.
pub const VERIFY_LAG_BYTES: u64 = CHUNK_GROUP_BYTES;

/// The node's downstream content frontiers the pull leg paces against (ADR
/// 037): what the downstream client has paid for, and how far the serve leg waits
/// on bytes. The node hands `drive` a reader of these; the client path has no
/// downstream leg and passes none. [`BudgetPacer`] ignores both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DownstreamFrontier {
    /// Content bytes the node has already served AND been paid for on its
    /// downstream (serve) leg. Paired with [`PaceState::pulled_frontier`] for
    /// [`WindowPacer`]'s window check. Inert on the client path, same as
    /// `pulled_frontier`.
    pub served_paid: u64,
    /// Content end of the nearest span a starved downstream serve leg waits on for
    /// this pull's content, or `0` when none waits. Each serve leg stands its own
    /// demand and withdraws it once it moves again, so a leg parked further down
    /// the blob never hides one parked at this pull's frontier. When it lies within
    /// one chunk group past [`PaceState::pulled_frontier`], a serve leg is parked on
    /// this pull, collecting no payment; [`WindowPacer`] then draws one
    /// [`PULL_WINDOW_FLOOR`] even with its window full, or neither leg moves again.
    /// A demand further out is ignored. `0` on the client path.
    pub serve_demand: u64,
}

/// A snapshot of one fetch's budget state at a gap boundary, everything a
/// [`Pacer`] needs and nothing it must fetch. Plain `Copy` data so a decision is
/// trivially reproducible in a test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaceState {
    /// Content bytes of the requested range already PAID FOR — the driver's
    /// per-leg [`content_paid_frontier`](crate::sink::content_paid_frontier)
    /// progress, NOT the store's delivered frontier. Completion must track
    /// PAYMENT, not delivery: the node streams a full credit window ahead of the
    /// voucher that pays for it (ADR 003), and the store checkpoints that tail
    /// payment-agnostically, so a delivered-frontier gate would return `Done`
    /// before the delivered-but-unpaid tail is billed (under-pay). When this
    /// reaches [`requested_bytes`](Self::requested_bytes) the range is fully paid.
    pub cleared_bytes: u64,
    /// Total content bytes the caller requested.
    pub requested_bytes: u64,
    /// Spendable deposit left on the channel: `deposit - committed.amount`.
    pub remaining_deposit: U256,
    /// Cost of the next voucher at the upstream's quoted rate/cadence
    /// (`ceil(interval_bytes * rate_per_mb / MiB)`), priced by the driver from the
    /// [`crate::UpstreamPullHeader`] of the last open.
    pub next_voucher_cost: U256,
    /// Content bytes the node's upstream pull leg has drawn so far (the pull-side
    /// frontier). [`BudgetPacer`] never reads this field — it exists for
    /// [`WindowPacer`] (ADR 037), which bounds `pulled_frontier -
    /// downstream.served_paid` by its window. On the client path (which always uses
    /// `BudgetPacer`, never `WindowPacer`) this field is inert; callers may set it
    /// to `0` or to the already-computed delivered frontier — either is safe.
    pub pulled_frontier: u64,
    /// Content bytes of the current gap not yet pulled: from
    /// [`pulled_frontier`](Self::pulled_frontier) to the gap's end. [`WindowPacer`]
    /// clamps its minimum draw to this, so the last short piece of a gap still
    /// draws; `0` turns the minimum off. [`BudgetPacer`] never reads it.
    pub gap_remaining: u64,
    /// The downstream serve leg's paid and demand frontiers. Ignored by
    /// [`BudgetPacer`]; on the client path the driver fills in the leg's own paid
    /// frontier and a `0` demand, both inert.
    pub downstream: DownstreamFrontier,
}

/// What the driver should do next for the current gap. See [`Pacer::decide`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaceDecision {
    /// Draw up to `up_to_bytes` more content bytes of this gap (the deposit covers
    /// the next voucher). For [`BudgetPacer`] this is the whole remainder of the
    /// range — the deposit is the only cap; a window pacer would cap it tighter.
    Draw {
        /// Upper bound on content bytes to draw before the next pacing decision.
        up_to_bytes: u64,
    },
    /// The requested range is fully present: finalize and stop.
    Done,
    /// The remaining deposit cannot cover the next voucher. Terminal for the
    /// lane: the driver ends it with [`crate::PoolExhausted`], which marks its
    /// source priced out at the current deposit.
    Refuse,
    /// The pull leg has run its full window ahead of the downstream paid frontier
    /// ([`WindowPacer`], ADR 037) and no serve leg is parked at its frontier: pause
    /// and re-decide once either [`PaceState::downstream`] frontier advances.
    /// [`BudgetPacer`] never returns this — only a window-bounded pacer does, so it
    /// only appears on the node's pull leg, never on the client path.
    Wait,
    /// The window has room, but less than [`WindowPacer`]'s minimum draw, and no
    /// serve leg is parked at the pull's frontier: pause and re-decide once
    /// either [`PaceState::downstream`] frontier advances, as for
    /// [`Self::Wait`]. A separate variant so the caller can meter the two
    /// pauses apart. Like `Wait`, only a window-bounded pacer returns it.
    WaitForMinDraw,
}

/// The pacing policy handed to the gap-driven driver. Pure: no I/O, no async.
pub trait Pacer: Send + Sync {
    /// Decide the next action for the current gap from `state`. Total and
    /// side-effect-free.
    fn decide(&self, state: &PaceState) -> PaceDecision;
}

/// The client pacing policy: gate on the buyer's own deposit, refuse once it
/// cannot cover the next voucher, and stop when the range is satisfied.
/// Carries no configuration — every input arrives in the [`PaceState`] — so it is
/// a zero-sized, order-free decision function.
#[derive(Debug, Clone, Copy, Default)]
pub struct BudgetPacer;

impl BudgetPacer {
    /// Construct the client pacer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Pacer for BudgetPacer {
    fn decide(&self, s: &PaceState) -> PaceDecision {
        // 1. The range is fully PAID — nothing left to pull or pay for. Gating on
        //    paid (not delivered) progress is what bills the credit-window tail the
        //    store checkpointed ahead of payment: an exhaustion leaves paid < the
        //    delivered frontier, so this stays below `requested` and the draw
        //    branch below re-opens the unpaid tail until payment catches up.
        if s.cleared_bytes >= s.requested_bytes {
            return PaceDecision::Done;
        }
        // 2. Gating on our OWN deposit: the remaining balance cannot cover the
        //    next voucher. The lane refuses; the funding recovery step at the
        //    exhausted candidate set is the only place the buyer adds funds.
        if s.remaining_deposit < s.next_voucher_cost {
            return PaceDecision::Refuse;
        }
        // 3. The deposit covers the next voucher and the range is not fully paid:
        //    keep drawing the UNPAID remainder (the driver re-opens it at the paid
        //    frontier, re-delivering any delivered-but-unpaid span idempotently).
        PaceDecision::Draw {
            up_to_bytes: s.requested_bytes.saturating_sub(s.cleared_bytes),
        }
    }
}

/// The node's pull-leg pacing policy (ADR 037): reuse [`BudgetPacer`]'s
/// money logic VERBATIM — a window pacer never overrides a money decision — and,
/// only on a `Draw`, clamp `up_to_bytes` so the pull never runs more than
/// `window_bytes` ahead of the downstream serve leg's paid frontier — plus one
/// [`PULL_WINDOW_FLOOR`] when [`DownstreamFrontier::serve_demand`] shows a serve leg parked
/// at the pull's frontier. When the window is already full and no serve leg is
/// parked there, wait instead of drawing zero bytes.
///
/// Each draw opens a new upstream request, so a window that reopens a chunk at
/// a time would cost one request round trip per chunk. A window of at least
/// [`MIN_DRAW_WINDOW`] therefore waits ([`PaceDecision::WaitForMinDraw`]) until
/// half of it is open (the minimum draw) and then draws the open room, while
/// the serve leg drains what it already holds. Three cases draw below the
/// minimum, so the rule never stalls the pull:
/// - a window below [`MIN_DRAW_WINDOW`], where half the window may be more
///   room than the payments are sure to release;
/// - a serve leg parked at the pull's frontier (the serve-demand floor);
/// - the end of the gap, where less than the minimum is left to pull.
///
/// The minimum only turns some draws into waits. It never widens the room, so
/// the exposure bound is the same.
///
/// Composition, not reimplementation: `WindowPacer::decide` calls
/// `BudgetPacer::decide` and only touches the `Draw` arm. `Done` and `Refuse`
/// pass through unchanged, so the two pacers can never disagree about whether
/// to pay, only about how much to pull in one pass.
#[derive(Debug, Clone, Copy)]
pub struct WindowPacer {
    /// Maximum content bytes the pull frontier may run ahead of the served-paid
    /// frontier before the pacer waits.
    window_bytes: u64,
}

/// Smallest window on which [`WindowPacer`] enforces its minimum draw of half
/// the window. Half the window must stay clear of the lag between the pull and
/// the downstream paid frontier — one chunk plus two group roundings (see
/// [`PULL_WINDOW_FLOOR`]) — or the minimum could wait on room the payments
/// never release. That holds from about two floors; four floors keeps half the
/// window about a chunk clear of the lag, including the serve leg's one-group
/// prefetch.
pub const MIN_DRAW_WINDOW: u64 = 4 * PULL_WINDOW_FLOOR;

impl WindowPacer {
    /// Construct a window pacer bounded to `window_bytes`.
    #[must_use]
    pub const fn new(window_bytes: u64) -> Self {
        Self { window_bytes }
    }
}

impl Pacer for WindowPacer {
    fn decide(&self, s: &PaceState) -> PaceDecision {
        match BudgetPacer.decide(s) {
            PaceDecision::Draw { up_to_bytes } => {
                let ahead = s.pulled_frontier.saturating_sub(s.downstream.served_paid);
                // Floor the room to whole chunk groups. The driver's `align_range`
                // rounds a draw's END up to a chunk-group boundary, so a room that is
                // not group-aligned would let the open overshoot the window by up to
                // one group. Flooring keeps `pulled_frontier - downstream.served_paid <=
                // window_bytes` EXACT (ADR 037) whenever the window, not the serve
                // demand below, sets the draw. `window_bytes` is always at least one
                // chunk — orders of magnitude larger than a 16 KiB group —
                // so a healthy window never floors to zero; only a sub-group remainder
                // (the window all but full) floors to 0 -> `Wait`, which is correct:
                // never draw a fraction that `align_range` would round past the window.
                let room = self.window_bytes.saturating_sub(ahead);
                let room = room - room % CHUNK_GROUP_BYTES;
                // A serve leg parked AT this pull's frontier overrides a full window
                // by one pull-window floor. Its encoder waits on the first byte the
                // pull has not fetched, so the demand lands within one group past
                // `pulled_frontier`: a leaf read demands its end (at most one group
                // out), and a proof read demands its node's first byte plus one. The
                // encoder walks the tree in pre-order, so it loads a node's pair only
                // after reading every leaf before the node — a node starting a whole
                // group past the pull cannot be the one a parked encoder waits on. A serve leg parks only while its own credit
                // window has room, so a client that stops paying stops raising
                // demand after at most one floor past what it may receive. A floor,
                // not one group, keeps each unpark to one upstream open instead of
                // one per group. A demand further out came from a serve leg that is
                // not blocked on this pull — honouring it would let an unpaid request
                // far down the blob drag the pull across the whole gap.
                let demand_ahead = s.downstream.serve_demand.saturating_sub(s.pulled_frontier);
                let demanded = if demand_ahead > 0 && demand_ahead <= CHUNK_GROUP_BYTES {
                    PULL_WINDOW_FLOOR
                } else {
                    0
                };
                // The minimum draw (see the type docs): with no parked serve leg,
                // wait until half the window is open, or until what the gap still
                // lacks, whichever is smaller. Rounded up to whole groups, like the
                // draw's end, so a sub-group gap tail still meets it.
                let min_draw = if self.window_bytes >= MIN_DRAW_WINDOW {
                    let half = self.window_bytes / 2;
                    half - half % CHUNK_GROUP_BYTES
                } else {
                    0
                };
                let gap_left = s
                    .gap_remaining
                    .div_ceil(CHUNK_GROUP_BYTES)
                    .saturating_mul(CHUNK_GROUP_BYTES);
                let below_min_draw = demanded == 0 && room < min_draw.min(gap_left);
                let room = room.max(demanded);
                if room == 0 {
                    PaceDecision::Wait
                } else if below_min_draw {
                    PaceDecision::WaitForMinDraw
                } else {
                    PaceDecision::Draw {
                        up_to_bytes: up_to_bytes.min(room),
                    }
                }
            }
            other => other,
        }
    }
}

/// The node pull-leg's ramped pacing policy (ADR 003 §Credit window / ADR 037):
/// compose [`WindowPacer`] over a window that itself ramps with the downstream
/// served-paid frontier, so on the fused serve-miss path the upstream pull never
/// runs further ahead of cleared client payment than the ramped credit window
/// allows, plus the one serve-demand floor [`WindowPacer`] may add. With a nonzero
/// divisor a non-paying client's request therefore fronts at most about two floors
/// of speculative upstream spend, and the window
/// widens only as the client pays; a divisor of `0` opens the full `credit_max`
/// from the first byte, the same instant-ceiling behavior as the downstream window.
#[derive(Debug, Clone, Copy)]
pub struct RampPacer {
    /// Ramp divisor: the window is `paid / divisor`. `0` opens the full
    /// `credit_max` immediately.
    pub divisor: u64,
    /// Smallest window the ramp may produce, so the pull can always make
    /// progress. In practice [`PULL_WINDOW_FLOOR`] — one chunk plus three
    /// chunk groups; see that constant for why one chunk alone deadlocks.
    pub floor: u64,
    /// Ceiling the ramp climbs toward.
    pub credit_max: u64,
    /// The ABSOLUTE content offset the downstream stream starts paying from —
    /// the fill session's served start. `served_paid` is an absolute frontier,
    /// so the stream's own payment is `served_paid − paid_base`: what THIS
    /// stream has paid, not where in the blob it happens to sit.
    pub paid_base: u64,
    /// Paid content bytes the owning downstream stream carries from earlier
    /// streams on its lane (ADR 003 §Credit window), in the same content units
    /// as `served_paid`. Added to the stream's own payment, so
    /// the ramp input is `served_paid − paid_base + paid_carried`: a request
    /// resuming at a multi-GiB offset ramps from its lane's credit, not from its
    /// position in the blob.
    pub paid_carried: u64,
}

impl Pacer for RampPacer {
    fn decide(&self, s: &PaceState) -> PaceDecision {
        let paid = s
            .downstream
            .served_paid
            .saturating_sub(self.paid_base)
            .saturating_add(self.paid_carried);
        let window =
            decdn_incentive::ramped_credit_window(self.divisor, self.floor, self.credit_max, paid);
        WindowPacer::new(window).decide(s)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
