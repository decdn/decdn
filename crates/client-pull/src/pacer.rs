//! The pure pacing axis of the gap-driven pull driver (#1608).
//!
//! A [`Pacer`] answers one question at each gap boundary: given a snapshot of the
//! fetch's budget state ([`PaceState`]), may the driver draw the next paid
//! segment, must it top up, wait, stop, or refuse? It is a total function of the
//! snapshot — **no I/O, no async** — so the policy is unit-testable in isolation,
//! independent of any network or chain fixture.
//!
//! [`BudgetPacer`] is the client policy: it gates on the buyer's OWN deposit
//! (order-free — it never waits on the counterparty's state) and folds in the
//! reactive top-up / resume-at-paid-frontier logic. The node's window pacer
//! (ADR 037) is a separate impl handed to the same driver.
//!
//! # Where the error classification lives
//!
//! The reactive exhaustion predicates that need the typed pull error —
//! [`crate::genuine_exhaustion`] and [`crate::resumable_watermark`] — stay at the
//! DRIVER boundary, not inside `decide`: the driver runs
//! [`crate::genuine_exhaustion`] against the error and feeds its boolean result in
//! as [`PaceState::exhaustion_confirmed`], and owns the reseed (desync) path
//! itself. That keeps the pacer a pure function of numbers while still REUSING the
//! shipped predicates rather than reimplementing them.

use alloy::primitives::U256;
use decdn_bao_range::CHUNK_GROUP_BYTES;
use decdn_protocol::client::CHUNK_BYTES;

/// The smallest pull window that keeps the fused serve-miss loop live.
///
/// [`RampPacer`] paces the pull in CONTENT bytes against
/// [`PaceState::served_paid_frontier`], which
/// [`content_paid_frontier`](crate::sink::content_paid_frontier) derives from PAID
/// WIRE by flooring to a chunk-group boundary; [`WindowPacer`] then floors its own
/// room to whole groups. Two group-sized roundings therefore sit between what the
/// client has paid for and what the pull may fetch next, while the client releases
/// its next proof only once a whole [`CHUNK_BYTES`] of WIRE has arrived.
///
/// A window of exactly one chunk loses more to those roundings than the wire's
/// interleaved proof bytes hand back, so the pull parks with the client short of
/// the chunk it must complete to pay — a payment that can then never come. Carrying
/// both roundings on top of the chunk closes that gap at every window size, because
/// the ramp only ever widens the window above this floor.
///
/// A **third** group covers the serving node's prefetch. A serve leg reads one frame
/// ahead of its own credit-window check, so once that window shuts it still asks its
/// producer for up to one bao chunk group more wire — bytes the producer can only
/// get from this pull. Without reserved headroom that request parks on data the pull
/// may not draw until a payment that the parked serve leg is the one blocked from
/// collecting. The two rounding groups cannot pay for it: the liveness invariant
/// below spends them in full.
pub const PULL_WINDOW_FLOOR: u64 = CHUNK_BYTES + 3 * CHUNK_GROUP_BYTES;

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
    /// The reactive top-up target. `U256::ZERO` disables reactive top-up (the
    /// pacer then refuses on exhaustion rather than funding).
    pub working_deposit: U256,
    /// Reactive top-ups already spent on this fetch.
    pub topups_used: u32,
    /// Reactive top-ups allowed in total, from
    /// [`crate::source::Funder::max_topups`] (CLI 3, node 1).
    pub max_topups: u32,
    /// Whether the last draw failed with an exhaustion the DRIVER confirmed as
    /// genuine via [`crate::genuine_exhaustion`] — a real ceiling hit corroborated
    /// by our own ledger, not a healable desync or a lying peer. `false` on the
    /// proactive (happy-path) query; a non-genuine reactive fault is handled by
    /// the driver's own reseed/terminal path, not here.
    pub exhaustion_confirmed: bool,
    /// Content bytes the node's upstream pull leg has drawn so far (the pull-side
    /// frontier). [`BudgetPacer`] never reads this field — it exists for
    /// [`WindowPacer`] (ADR 037), which bounds `pulled_frontier -
    /// served_paid_frontier` by its window. On the client path (which always uses
    /// `BudgetPacer`, never `WindowPacer`) this field is inert; callers may set it
    /// to `0` or to the already-computed delivered frontier — either is safe.
    pub pulled_frontier: u64,
    /// Content bytes the node has already served AND been paid for on its
    /// downstream (serve) leg. Paired with [`Self::pulled_frontier`] for
    /// [`WindowPacer`]'s window check; ignored by [`BudgetPacer`]. Inert on the
    /// client path, same as `pulled_frontier`.
    pub served_paid_frontier: u64,
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
    /// A genuine, corroborated exhaustion with budget remaining: add this many
    /// micro-USDC via [`crate::source::Funder::top_up`], then resume at the paid
    /// frontier.
    TopUp(U256),
    /// The requested range is fully present: finalize and stop.
    Done,
    /// Out of budget or attempts (deposit cannot cover the next voucher and either
    /// top-up is disabled/exhausted, or there is nothing left to add). Terminal.
    Refuse,
    /// The pull leg has run its full window ahead of the downstream paid frontier
    /// ([`WindowPacer`], ADR 037): pause and re-decide once `served_paid_frontier`
    /// advances. [`BudgetPacer`] never returns this — only a window-bounded pacer
    /// does, so it only appears on the node's pull leg, never on the client path.
    Wait,
}

/// The pacing policy handed to the gap-driven driver. Pure: no I/O, no async.
pub trait Pacer: Send + Sync {
    /// Decide the next action for the current gap from `state`. Total and
    /// side-effect-free.
    fn decide(&self, state: &PaceState) -> PaceDecision;
}

/// The client pacing policy: gate on the buyer's own deposit, top up reactively
/// on a genuine mid-fetch ceiling hit, and stop when the range is satisfied.
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
        //    delivered frontier, so this stays below `requested` and the fund/draw
        //    branch below re-opens the unpaid tail until payment catches up.
        if s.cleared_bytes >= s.requested_bytes {
            return PaceDecision::Done;
        }
        // 2. Exhaustion. Two ways to reach it, both order-free: the driver
        //    confirmed a genuine reactive ceiling hit (`genuine_exhaustion`), or —
        //    gating on our OWN deposit — the remaining balance cannot even cover
        //    the next voucher. Either way, fund it if a top-up is enabled, budget
        //    remains, and there is something to add; otherwise refuse.
        let unaffordable = s.remaining_deposit < s.next_voucher_cost;
        if s.exhaustion_confirmed || unaffordable {
            let additional = s.working_deposit.saturating_sub(s.remaining_deposit);
            let can_topup = s.topups_used < s.max_topups
                && !s.working_deposit.is_zero()
                && !additional.is_zero();
            return if can_topup {
                PaceDecision::TopUp(additional)
            } else {
                PaceDecision::Refuse
            };
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
/// `window_bytes` ahead of the downstream serve leg's paid frontier. When the
/// window is already full, wait instead of drawing zero bytes.
///
/// Composition, not reimplementation: `WindowPacer::decide` calls
/// `BudgetPacer::decide` and only touches the `Draw` arm. `Done` / `TopUp` /
/// `Refuse` pass through unchanged, so the two pacers can never disagree about
/// whether/how to pay — only about how much to pull in one pass.
#[derive(Debug, Clone, Copy)]
pub struct WindowPacer {
    /// Maximum content bytes the pull frontier may run ahead of the served-paid
    /// frontier before the pacer waits.
    window_bytes: u64,
}

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
                let ahead = s.pulled_frontier.saturating_sub(s.served_paid_frontier);
                // Floor the room to whole chunk groups. The driver's `align_range`
                // rounds a draw's END up to a chunk-group boundary, so a room that is
                // not group-aligned would let the open overshoot the window by up to
                // one group. Flooring keeps `pulled_frontier - served_paid_frontier <=
                // window_bytes` EXACT (ADR 037). `window_bytes` is always at least one
                // chunk — orders of magnitude larger than a 16 KiB group —
                // so a healthy window never floors to zero; only a sub-group remainder
                // (the window all but full) floors to 0 -> `Wait`, which is correct:
                // never draw a fraction that `align_range` would round past the window.
                let room = self.window_bytes.saturating_sub(ahead);
                let room = room - room % CHUNK_GROUP_BYTES;
                if room == 0 {
                    PaceDecision::Wait
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
/// allows. With a nonzero divisor a non-paying client's request therefore fronts
/// at most one chunk (the floor) of speculative upstream spend, and the window
/// widens only as the client pays; a divisor of `0` opens the full `credit_max`
/// from the first byte, the same instant-ceiling behavior as the downstream window.
#[derive(Debug, Clone, Copy)]
pub struct RampPacer {
    pub divisor: u64,
    pub floor: u64,
    pub credit_max: u64,
}

impl Pacer for RampPacer {
    fn decide(&self, s: &PaceState) -> PaceDecision {
        let window = decdn_incentive::ramped_credit_window(
            self.divisor,
            self.floor,
            self.credit_max,
            s.served_paid_frontier,
        );
        WindowPacer::new(window).decide(s)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::{
        BudgetPacer, CHUNK_BYTES, CHUNK_GROUP_BYTES, PULL_WINDOW_FLOOR, PaceDecision, PaceState,
        Pacer, RampPacer, WindowPacer,
    };
    use alloy::primitives::U256;

    /// A healthy mid-fetch snapshot: deposit covers the next voucher, range
    /// incomplete, no top-up pending. Each test tweaks one axis.
    fn healthy() -> PaceState {
        PaceState {
            cleared_bytes: 16 * 1024,
            requested_bytes: 1_000_000,
            remaining_deposit: U256::from(1_000u64),
            next_voucher_cost: U256::from(10u64),
            working_deposit: U256::from(5_000u64),
            topups_used: 0,
            max_topups: 3,
            exhaustion_confirmed: false,
            pulled_frontier: 0,
            served_paid_frontier: 0,
        }
    }

    #[test]
    fn deposit_covers_the_next_voucher_so_draw() {
        let s = healthy();
        assert_eq!(
            BudgetPacer::new().decide(&s),
            PaceDecision::Draw {
                up_to_bytes: s.requested_bytes - s.cleared_bytes
            }
        );
    }

    #[test]
    fn fully_cleared_range_is_done() {
        let mut s = healthy();
        s.cleared_bytes = s.requested_bytes;
        assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Done);
        // Overshoot (a final partial group can push cleared past requested) is
        // still Done, never a spurious extra draw.
        s.cleared_bytes = s.requested_bytes + 5;
        assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Done);
    }

    #[test]
    fn exhausted_with_attempts_left_tops_up_the_shortfall() {
        let mut s = healthy();
        // Deposit can no longer cover the next voucher.
        s.remaining_deposit = U256::from(4u64);
        s.next_voucher_cost = U256::from(10u64);
        assert_eq!(
            BudgetPacer::new().decide(&s),
            // additional = working_deposit - remaining = 5000 - 4.
            PaceDecision::TopUp(U256::from(4_996u64))
        );
    }

    #[test]
    fn a_confirmed_reactive_exhaustion_tops_up_even_if_numbers_look_affordable() {
        let mut s = healthy();
        // The driver corroborated a genuine ceiling hit against its own ledger.
        s.exhaustion_confirmed = true;
        match BudgetPacer::new().decide(&s) {
            PaceDecision::TopUp(_) => {}
            other => panic!("a confirmed exhaustion must top up, got {other:?}"),
        }
    }

    #[test]
    fn exhausted_with_no_attempts_left_refuses() {
        let mut s = healthy();
        s.remaining_deposit = U256::from(4u64);
        s.topups_used = s.max_topups;
        assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Refuse);
    }

    #[test]
    fn exhausted_with_topup_disabled_refuses() {
        let mut s = healthy();
        s.remaining_deposit = U256::from(4u64);
        s.working_deposit = U256::ZERO; // reactive top-up disabled
        assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Refuse);
    }

    #[test]
    fn exhausted_but_nothing_left_to_add_refuses() {
        let mut s = healthy();
        // Below the next voucher, but the deposit already sits at the working
        // target, so there is no shortfall to fund.
        s.remaining_deposit = U256::from(4u64);
        s.next_voucher_cost = U256::from(10u64);
        s.working_deposit = U256::from(4u64);
        assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Refuse);
    }

    #[test]
    fn a_top_up_healed_deposit_draws_immediately() {
        // After a top-up lands, `remaining_deposit` covers the next voucher again
        // and the range is incomplete: the pacer must retry the open right away —
        // no proactive settle-wait. The bounded settle-wait only fires in the
        // driver, and only on an ACTUAL stale-resume refusal (see driver.rs).
        let s = healthy();
        assert_eq!(
            BudgetPacer::new().decide(&s),
            PaceDecision::Draw {
                up_to_bytes: s.requested_bytes - s.cleared_bytes
            }
        );
    }

    #[test]
    fn window_room_clamps_the_draw() {
        // ahead = 3 groups, window = 4 groups -> room = 1 group. BudgetPacer alone
        // would draw the whole remainder (far more than one group); WindowPacer must
        // clamp to the group-aligned window room.
        let mut s = healthy();
        s.pulled_frontier = 5 * CHUNK_GROUP_BYTES;
        s.served_paid_frontier = 2 * CHUNK_GROUP_BYTES;
        assert_eq!(
            BudgetPacer::new().decide(&s),
            PaceDecision::Draw {
                up_to_bytes: s.requested_bytes - s.cleared_bytes
            },
            "sanity: BudgetPacer would draw far more than the window room"
        );
        assert_eq!(
            WindowPacer::new(4 * CHUNK_GROUP_BYTES).decide(&s),
            PaceDecision::Draw {
                up_to_bytes: CHUNK_GROUP_BYTES
            }
        );
    }

    #[test]
    fn window_room_floors_to_whole_groups() {
        // ahead = 3 groups, window = 3 groups + a sub-group remainder -> room is a
        // fraction of a group, which floors to 0 -> Wait. Never draw a sub-group that
        // `align_range` would round up past the window (ADR 037 bound stays exact).
        let mut s = healthy();
        s.pulled_frontier = 5 * CHUNK_GROUP_BYTES;
        s.served_paid_frontier = 2 * CHUNK_GROUP_BYTES;
        assert_eq!(
            WindowPacer::new(3 * CHUNK_GROUP_BYTES + 5_000).decide(&s),
            PaceDecision::Wait
        );
        // One byte over a full group of room -> still exactly one group is drawable.
        assert_eq!(
            WindowPacer::new(4 * CHUNK_GROUP_BYTES + 1).decide(&s),
            PaceDecision::Draw {
                up_to_bytes: CHUNK_GROUP_BYTES
            }
        );
    }

    #[test]
    fn window_full_waits() {
        // pulled - served_paid == window -> no room left, wait.
        let mut s = healthy();
        s.pulled_frontier = 5 * CHUNK_GROUP_BYTES;
        s.served_paid_frontier = 2 * CHUNK_GROUP_BYTES;
        assert_eq!(
            WindowPacer::new(3 * CHUNK_GROUP_BYTES).decide(&s),
            PaceDecision::Wait
        );
    }

    #[test]
    fn window_pacer_passes_through_done_topup_refuse() {
        // Fully paid -> Done, identical to BudgetPacer, regardless of window state.
        let mut s = healthy();
        s.cleared_bytes = s.requested_bytes;
        s.pulled_frontier = 1_000_000;
        s.served_paid_frontier = 0;
        assert_eq!(WindowPacer::new(10).decide(&s), PaceDecision::Done);
        assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Done);

        // Exhausted with attempts left -> TopUp, exactly BudgetPacer's amount; the
        // window never overrides the money decision.
        let mut s = healthy();
        s.remaining_deposit = U256::from(4u64);
        s.next_voucher_cost = U256::from(10u64);
        s.pulled_frontier = 1_000_000;
        s.served_paid_frontier = 0;
        assert_eq!(
            WindowPacer::new(10).decide(&s),
            BudgetPacer::new().decide(&s)
        );
        match WindowPacer::new(10).decide(&s) {
            PaceDecision::TopUp(_) => {}
            other => panic!("expected TopUp, got {other:?}"),
        }

        // Exhausted with no attempts left -> Refuse, exactly BudgetPacer's.
        let mut s = healthy();
        s.remaining_deposit = U256::from(4u64);
        s.topups_used = s.max_topups;
        s.pulled_frontier = 1_000_000;
        s.served_paid_frontier = 0;
        assert_eq!(WindowPacer::new(10).decide(&s), PaceDecision::Refuse);
        assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Refuse);
    }

    #[test]
    fn ramp_pacer_paces_pull_on_the_ramped_window() {
        // Unpaid: served_paid_frontier = 0 -> window = floor. The pull may run at
        // most `floor` ahead of the served-paid frontier, then Wait.
        let floor = 4 * CHUNK_GROUP_BYTES;
        let pacer = RampPacer {
            divisor: 2,
            floor,
            credit_max: 64 * CHUNK_GROUP_BYTES,
        };
        let mut s = healthy();
        s.served_paid_frontier = 0;
        s.pulled_frontier = floor; // already floor ahead
        assert_eq!(pacer.decide(&s), PaceDecision::Wait);
    }

    #[test]
    fn ramp_pacer_widens_the_pull_window_as_served_paid_advances() {
        // served_paid_frontier = 32 groups, divisor 2 -> window 16 groups > floor,
        // so a pull only `floor` ahead may Draw again.
        let floor = 4 * CHUNK_GROUP_BYTES;
        let pacer = RampPacer {
            divisor: 2,
            floor,
            credit_max: 64 * CHUNK_GROUP_BYTES,
        };
        let mut s = healthy();
        s.served_paid_frontier = 32 * CHUNK_GROUP_BYTES;
        s.pulled_frontier = floor;
        assert!(matches!(pacer.decide(&s), PaceDecision::Draw { .. }));
    }

    /// The liveness invariant `PULL_WINDOW_FLOOR` exists to hold: a pull window
    /// must clear one payment chunk by BOTH group roundings that separate paid
    /// wire from drawable content — `content_paid_frontier`'s floor to a group
    /// boundary, and `WindowPacer`'s floor of its own room to whole groups — AND
    /// still leave a group for the serving node's one-frame prefetch.
    ///
    /// Modelled at the worst case for each: the served-paid frontier lags the
    /// client's true paid position by a full group, and the room the pacer grants
    /// loses another. What survives must be a whole chunk plus the group the serve
    /// leg reads ahead, or one side or the other parks — the client short of the
    /// chunk whose payment would widen the window, or the serve leg short of the
    /// frame it prefetches past its own shut window.
    ///
    /// The prefetch term is what ties this constant to
    /// `ClientHandler::frame_target`'s room floor on the serving side. Lowering
    /// this floor, or raising that floor, breaks liveness on the cache-miss leg —
    /// and it breaks it as a hang, not a failed assertion, so it is pinned here
    /// rather than left to an integration test to discover by timeout.
    #[test]
    fn the_pull_window_floor_clears_one_chunk_and_a_prefetch_after_both_roundings() {
        let survives = PULL_WINDOW_FLOOR - 2 * CHUNK_GROUP_BYTES;
        let needed = CHUNK_BYTES + CHUNK_GROUP_BYTES;
        assert!(
            survives >= needed,
            "a {PULL_WINDOW_FLOOR}-byte floor leaves only {survives} bytes after both \
             roundings, short of the {needed} bytes the client must draw — one \
             {CHUNK_BYTES}-byte chunk to complete a payment, plus the \
             {CHUNK_GROUP_BYTES}-byte group the serve leg prefetches past a shut \
             window. One side or the other would park forever"
        );
    }

    /// The same invariant, exercised through the pacer rather than by arithmetic:
    /// with the window at the floor and the pull parked exactly one chunk past a
    /// group-lagged paid frontier, the pacer must still grant a draw. A window of
    /// exactly one chunk does not, which is the deadlock this floor removes.
    #[test]
    fn at_the_floor_a_pull_one_chunk_ahead_may_still_draw() {
        let pacer = RampPacer {
            divisor: 2,
            floor: PULL_WINDOW_FLOOR,
            credit_max: 64 * CHUNK_BYTES,
        };
        let mut s = healthy();
        s.requested_bytes = 64 * CHUNK_BYTES;
        // The frontier the serve leg publishes trails the client's real paid
        // position by up to one group (`content_paid_frontier` floors to one).
        s.served_paid_frontier = 0;
        s.pulled_frontier = CHUNK_BYTES;
        assert!(
            matches!(pacer.decide(&s), PaceDecision::Draw { .. }),
            "the pull must be able to feed the client past the chunk boundary it \
             pays at, or neither side can move"
        );
    }
}
