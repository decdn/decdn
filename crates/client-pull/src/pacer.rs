//! The pure pacing axis of the gap-driven pull driver (#1608).
//!
//! A [`Pacer`] answers one question at each gap boundary: given a snapshot of the
//! fetch's budget state ([`PaceState`]), may the driver draw the next paid
//! segment, must it top up, wait, stop, or refuse? It is a total function of the
//! snapshot — **no I/O, no async** — so the policy is unit-testable in isolation,
//! exactly like the node's `node_origin::resume::decide`.
//!
//! [`BudgetPacer`] is the client policy: it gates on the buyer's OWN deposit
//! (order-free — it never waits on the counterparty's state) and folds in the
//! reactive top-up / resume-at-paid-frontier logic that lived inline in the CLI's
//! `fetch_blob_streaming`. The node's window pacer (ADR 037, Phase B) is a
//! separate impl handed to the same driver.
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::{BudgetPacer, PaceDecision, PaceState, Pacer};
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
}
