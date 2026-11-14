use super::{
    BudgetPacer, CHUNK_BYTES, CHUNK_GROUP_BYTES, CreditPacer, DownstreamFrontier, MIN_DRAW_WINDOW,
    PULL_AHEAD_BYTES, PULL_WINDOW_FLOOR, PaceDecision, PaceState, Pacer, WindowPacer,
};
use alloy::primitives::U256;
use std::assert_matches;

/// A healthy mid-fetch snapshot: deposit covers the next voucher, range
/// incomplete. Each test tweaks one axis.
fn healthy() -> PaceState {
    PaceState {
        cleared_bytes: 16 * 1024,
        requested_bytes: 1_000_000,
        remaining_deposit: U256::from(1_000u64),
        next_voucher_cost: U256::from(10u64),
        pulled_frontier: 0,
        gap_remaining: 1_000_000,
        downstream: DownstreamFrontier::default(),
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

/// A lane never adds funds: a deposit short of the next voucher refuses, and
/// the acquire loop's funding recovery step is the only place funds rise.
#[test]
fn a_deposit_short_of_the_next_voucher_refuses_instead_of_topping_up() {
    let mut s = healthy();
    s.remaining_deposit = U256::from(4u64);
    s.next_voucher_cost = U256::from(10u64);
    assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Refuse);
}

#[test]
fn a_deposit_that_exactly_covers_the_next_voucher_draws() {
    let mut s = healthy();
    s.remaining_deposit = U256::from(10u64);
    s.next_voucher_cost = U256::from(10u64);
    assert_matches!(BudgetPacer::new().decide(&s), PaceDecision::Draw { .. });
}

#[test]
fn the_first_draw_before_any_quote_always_proceeds() {
    // No open yet prices the voucher at zero, so an empty deposit still draws:
    // an exhaustion can only follow an open.
    let mut s = healthy();
    s.remaining_deposit = U256::ZERO;
    s.next_voucher_cost = U256::ZERO;
    assert_matches!(BudgetPacer::new().decide(&s), PaceDecision::Draw { .. });
}

#[test]
fn window_room_clamps_the_draw() {
    // ahead = 3 groups, window = 4 groups -> room = 1 group. BudgetPacer alone
    // would draw the whole remainder (far more than one group); WindowPacer must
    // clamp to the group-aligned window room.
    let mut s = healthy();
    s.pulled_frontier = 5 * CHUNK_GROUP_BYTES;
    s.downstream.served_paid = 2 * CHUNK_GROUP_BYTES;
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
    s.downstream.served_paid = 2 * CHUNK_GROUP_BYTES;
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

/// A 64 MiB window with a large gap and no demand: the snapshot is
/// `pulled - served_paid = ahead`, and `requested` is far past the window.
fn big_window_state(ahead: u64) -> (WindowPacer, PaceState) {
    const WINDOW: u64 = 64 * 1024 * 1024;
    let mut s = healthy();
    s.requested_bytes = 1 << 40;
    s.gap_remaining = 1 << 40;
    s.pulled_frontier = 100 * CHUNK_BYTES + ahead;
    s.downstream.served_paid = 100 * CHUNK_BYTES;
    (WindowPacer::new(WINDOW), s)
}

#[test]
fn a_ramped_window_waits_until_half_the_window_opens() {
    const WINDOW: u64 = 64 * 1024 * 1024;
    // Room of a few groups on a 64 MiB window: without the minimum this would
    // open one upstream request per payment. Wait instead.
    let (pacer, s) = big_window_state(WINDOW - 4 * CHUNK_GROUP_BYTES);
    assert_eq!(pacer.decide(&s), PaceDecision::WaitForMinDraw);
    // One group short of half open: still wait.
    let (pacer, s) = big_window_state(WINDOW / 2 + CHUNK_GROUP_BYTES);
    assert_eq!(pacer.decide(&s), PaceDecision::WaitForMinDraw);
    // Exactly half open: draw the half.
    let (pacer, s) = big_window_state(WINDOW / 2);
    assert_eq!(
        pacer.decide(&s),
        PaceDecision::Draw {
            up_to_bytes: WINDOW / 2
        }
    );
}

#[test]
fn min_draw_is_clamped_to_the_gap_remainder() {
    // Two groups and a bit left in the gap, and three groups of room: the
    // minimum shrinks to the (group-rounded) remainder, so the tail draws.
    let (pacer, mut s) = big_window_state(64 * 1024 * 1024 - 3 * CHUNK_GROUP_BYTES);
    s.gap_remaining = 2 * CHUNK_GROUP_BYTES + 100;
    assert_matches!(pacer.decide(&s), PaceDecision::Draw { .. });
    // Two groups of room for the same remainder is below it: wait.
    let (pacer, mut s) = big_window_state(64 * 1024 * 1024 - 2 * CHUNK_GROUP_BYTES);
    s.gap_remaining = 2 * CHUNK_GROUP_BYTES + 100;
    assert_eq!(pacer.decide(&s), PaceDecision::WaitForMinDraw);
}

#[test]
fn serve_demand_bypasses_the_min_draw() {
    // A serve leg parked at the pull's frontier gets its floor at once, even
    // with the window nearly full.
    let (pacer, mut s) = big_window_state(64 * 1024 * 1024 - 2 * CHUNK_GROUP_BYTES);
    s.downstream.serve_demand = s.pulled_frontier + 1;
    assert_eq!(
        pacer.decide(&s),
        PaceDecision::Draw {
            up_to_bytes: PULL_WINDOW_FLOOR
        }
    );
}

#[test]
fn min_draw_is_off_below_the_min_draw_window() {
    // A window just below `MIN_DRAW_WINDOW` keeps drawing whatever room opens.
    let window = MIN_DRAW_WINDOW - CHUNK_GROUP_BYTES;
    let mut s = healthy();
    s.requested_bytes = 1 << 40;
    s.gap_remaining = 1 << 40;
    s.pulled_frontier = 100 * CHUNK_BYTES + window - CHUNK_GROUP_BYTES;
    s.downstream.served_paid = 100 * CHUNK_BYTES;
    assert_eq!(
        WindowPacer::new(window).decide(&s),
        PaceDecision::Draw {
            up_to_bytes: CHUNK_GROUP_BYTES
        }
    );
    // At `MIN_DRAW_WINDOW` itself the same one-group room waits.
    s.pulled_frontier = 100 * CHUNK_BYTES + MIN_DRAW_WINDOW - CHUNK_GROUP_BYTES;
    assert_eq!(
        WindowPacer::new(MIN_DRAW_WINDOW).decide(&s),
        PaceDecision::WaitForMinDraw
    );
}

#[test]
fn half_the_window_always_opens_once_the_client_catches_up() {
    // A caught-up, paying client leaves the pull at most one chunk plus two
    // group roundings ahead of the paid frontier (see `PULL_WINDOW_FLOOR`).
    // At every window from `MIN_DRAW_WINDOW` up, that lag must leave the
    // minimum draw open, or the minimum could wait on room that never comes.
    let lag = CHUNK_BYTES + 2 * CHUNK_GROUP_BYTES;
    let mut window = MIN_DRAW_WINDOW;
    while window <= 64 * 1024 * 1024 {
        let mut s = healthy();
        s.requested_bytes = 1 << 40;
        s.gap_remaining = 1 << 40;
        s.pulled_frontier = 100 * CHUNK_BYTES + lag;
        s.downstream.served_paid = 100 * CHUNK_BYTES;
        assert_matches!(
            WindowPacer::new(window).decide(&s),
            PaceDecision::Draw { .. },
            "window {window} must draw with a caught-up client"
        );
        window += CHUNK_GROUP_BYTES;
    }
}

#[test]
fn window_full_waits() {
    // pulled - served_paid == window -> no room left, wait.
    let mut s = healthy();
    s.pulled_frontier = 5 * CHUNK_GROUP_BYTES;
    s.downstream.served_paid = 2 * CHUNK_GROUP_BYTES;
    assert_eq!(
        WindowPacer::new(3 * CHUNK_GROUP_BYTES).decide(&s),
        PaceDecision::Wait
    );
}

#[test]
fn serve_demand_at_a_full_window_draws_one_floor() {
    // Window full (ahead == window), but a serve leg is parked on the next group
    // past the pull — a proof node (demand = frontier + 1) or a whole leaf
    // (demand = frontier + one group). Both draw one pull-window floor.
    let mut s = healthy();
    s.requested_bytes = 64 * CHUNK_BYTES;
    s.pulled_frontier = 5 * CHUNK_GROUP_BYTES;
    s.downstream.served_paid = 2 * CHUNK_GROUP_BYTES;
    for demand in [5 * CHUNK_GROUP_BYTES + 1, 6 * CHUNK_GROUP_BYTES] {
        s.downstream.serve_demand = demand;
        assert_eq!(
            WindowPacer::new(3 * CHUNK_GROUP_BYTES).decide(&s),
            PaceDecision::Draw {
                up_to_bytes: PULL_WINDOW_FLOOR
            },
            "demand {demand}"
        );
    }
}

#[test]
fn serve_demand_far_past_the_pull_is_ignored() {
    // A demand more than one group past the pull comes from a serve leg that is
    // not parked on this pull (an attached request far down the blob). It must
    // not open a full window, or an unpaid request would pull the whole gap.
    let mut s = healthy();
    s.pulled_frontier = 5 * CHUNK_GROUP_BYTES;
    s.downstream.served_paid = 2 * CHUNK_GROUP_BYTES;
    s.downstream.serve_demand = 6 * CHUNK_GROUP_BYTES + 1;
    assert_eq!(
        WindowPacer::new(3 * CHUNK_GROUP_BYTES).decide(&s),
        PaceDecision::Wait
    );
    s.downstream.serve_demand = 1 << 30;
    assert_eq!(
        WindowPacer::new(3 * CHUNK_GROUP_BYTES).decide(&s),
        PaceDecision::Wait
    );
}

#[test]
fn serve_demand_already_pulled_still_waits() {
    // The demanded span is present, so it grants no room: a full window waits.
    let mut s = healthy();
    s.pulled_frontier = 5 * CHUNK_GROUP_BYTES;
    s.downstream.served_paid = 2 * CHUNK_GROUP_BYTES;
    s.downstream.serve_demand = 5 * CHUNK_GROUP_BYTES;
    assert_eq!(
        WindowPacer::new(3 * CHUNK_GROUP_BYTES).decide(&s),
        PaceDecision::Wait
    );
}

#[test]
fn serve_demand_inside_a_wider_window_changes_nothing() {
    // Window room already exceeds the demand's floor, so the window decides.
    let mut s = healthy();
    s.requested_bytes = 64 * CHUNK_BYTES;
    s.pulled_frontier = 2 * CHUNK_GROUP_BYTES;
    s.downstream.served_paid = 2 * CHUNK_GROUP_BYTES;
    s.downstream.serve_demand = 3 * CHUNK_GROUP_BYTES;
    assert_eq!(
        WindowPacer::new(4 * CHUNK_BYTES).decide(&s),
        PaceDecision::Draw {
            up_to_bytes: 4 * CHUNK_BYTES
        }
    );
}

#[test]
fn window_pacer_passes_through_done_and_refuse() {
    // Fully paid -> Done, identical to BudgetPacer, regardless of window state.
    let mut s = healthy();
    s.cleared_bytes = s.requested_bytes;
    s.pulled_frontier = 1_000_000;
    s.downstream.served_paid = 0;
    assert_eq!(WindowPacer::new(10).decide(&s), PaceDecision::Done);
    assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Done);

    // A deposit short of the next voucher -> Refuse, exactly BudgetPacer's; the
    // window never overrides the money decision.
    let mut s = healthy();
    s.remaining_deposit = U256::from(4u64);
    s.next_voucher_cost = U256::from(10u64);
    s.pulled_frontier = 1_000_000;
    s.downstream.served_paid = 0;
    assert_eq!(WindowPacer::new(10).decide(&s), PaceDecision::Refuse);
    assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Refuse);
}

/// A lane that holds no credit for the stream draws nothing: the downstream
/// frontier has not left the stream's start, so the pull waits. A payer that
/// never pays costs the node no upstream payment.
#[test]
fn credit_pacer_draws_nothing_while_the_lane_holds_no_credit() {
    let start = 32 * CHUNK_GROUP_BYTES;
    let pacer = CreditPacer { paid_base: start };
    let mut s = healthy();
    s.downstream.served_paid = start;
    s.pulled_frontier = start;
    assert_eq!(pacer.decide(&s), PaceDecision::Wait);
}

/// The pull runs at most `PULL_AHEAD_BYTES` past the credit frontier, then
/// waits for the lane to pay for more.
#[test]
fn credit_pacer_stops_the_pull_at_the_credit_frontier_plus_the_ahead_slack() {
    let pacer = CreditPacer { paid_base: 0 };
    let mut s = healthy();
    s.requested_bytes = 64 * CHUNK_BYTES;
    s.gap_remaining = 64 * CHUNK_BYTES;
    s.downstream.served_paid = CHUNK_BYTES;
    s.pulled_frontier = 0;
    assert_eq!(
        pacer.decide(&s),
        PaceDecision::Draw {
            up_to_bytes: CHUNK_BYTES + PULL_AHEAD_BYTES
        }
    );
    s.pulled_frontier = CHUNK_BYTES + PULL_AHEAD_BYTES;
    assert_eq!(pacer.decide(&s), PaceDecision::Wait);
}

/// A draw below the minimum waits for more credit, so each upstream request
/// carries a useful amount. The end of the gap draws whatever is left.
#[test]
fn credit_pacer_waits_for_a_minimum_draw_except_at_the_gap_end() {
    let pacer = CreditPacer { paid_base: 0 };
    let mut s = healthy();
    s.requested_bytes = 64 * CHUNK_BYTES;
    s.gap_remaining = 64 * CHUNK_BYTES;
    s.downstream.served_paid = 4 * CHUNK_GROUP_BYTES;
    s.pulled_frontier = 4 * CHUNK_GROUP_BYTES;
    assert_eq!(pacer.decide(&s), PaceDecision::WaitForMinDraw);
    s.gap_remaining = 2 * CHUNK_GROUP_BYTES;
    assert_matches!(pacer.decide(&s), PaceDecision::Draw { .. });
}

/// A serve leg parked within one group past the pull's frontier is waiting on
/// bytes its credit covers, so it draws one `PULL_AHEAD_BYTES` even with no
/// credit frontier movement.
#[test]
fn credit_pacer_serves_a_parked_leg_one_ahead_slack() {
    let pacer = CreditPacer { paid_base: 0 };
    let mut s = healthy();
    s.requested_bytes = 64 * CHUNK_BYTES;
    s.gap_remaining = 64 * CHUNK_BYTES;
    s.downstream.served_paid = 0;
    s.pulled_frontier = 0;
    s.downstream.serve_demand = 1;
    assert_eq!(
        pacer.decide(&s),
        PaceDecision::Draw {
            up_to_bytes: PULL_AHEAD_BYTES
        }
    );
}

/// Money decisions pass through the credit pacer unchanged.
#[test]
fn credit_pacer_passes_through_done_and_refuse() {
    let pacer = CreditPacer { paid_base: 0 };
    let mut s = healthy();
    s.cleared_bytes = s.requested_bytes;
    assert_eq!(pacer.decide(&s), PaceDecision::Done);
    let mut s = healthy();
    s.remaining_deposit = U256::from(4u64);
    s.next_voucher_cost = U256::from(10u64);
    assert_eq!(pacer.decide(&s), PaceDecision::Refuse);
}

/// The liveness invariant `PULL_AHEAD_BYTES` exists to hold: the pull draws
/// past the credit frontier by both group roundings that separate paid wire
/// from drawable content (`content_paid_frontier` floors to a group boundary,
/// and `CreditPacer` floors its room to whole groups) plus the leaf the serve
/// leg's encoder reads to finish a frame cut at `max(credit, one group)`.
///
/// The leaf term ties this constant to `ClientHandler::frame_target`'s room
/// floor on the serving side. Raising that floor, or lowering this constant,
/// breaks liveness on the cache-miss leg. It breaks it as a hang, not a failed
/// assertion, so it is pinned here.
#[test]
fn the_pull_ahead_slack_clears_both_roundings_and_a_frame_leaf() {
    let rounding_groups = 2;
    let leaf_groups = 1;
    assert!(
        PULL_AHEAD_BYTES >= (rounding_groups + leaf_groups) * CHUNK_GROUP_BYTES,
        "a {PULL_AHEAD_BYTES}-byte slack is short of two rounding groups plus the \
         frame's leaf group. The serve leg would park on bytes the pull may not draw"
    );
    assert_eq!(
        decdn_bao_range::CHUNK_GROUP_BYTES,
        CHUNK_GROUP_BYTES,
        "the room floor in `frame_target` is one chunk group"
    );
}

/// With credit for one chunk and the pull at the credit frontier, a serve leg
/// that cuts one group more than the credit (the room floor) still finds the
/// bytes it needs inside the pull's reach.
#[test]
fn a_frame_one_group_past_the_credit_is_inside_the_pull_reach() {
    let pacer = CreditPacer { paid_base: 0 };
    let mut s = healthy();
    s.requested_bytes = 64 * CHUNK_BYTES;
    s.gap_remaining = 64 * CHUNK_BYTES;
    // The frontier the serve leg publishes trails the payer's real position by
    // up to one group (`content_paid_frontier` floors to one).
    s.downstream.served_paid = CHUNK_BYTES - CHUNK_GROUP_BYTES;
    s.pulled_frontier = 0;
    let PaceDecision::Draw { up_to_bytes } = pacer.decide(&s) else {
        panic!("the pull must draw while the lane holds credit");
    };
    assert!(
        up_to_bytes >= CHUNK_BYTES + CHUNK_GROUP_BYTES,
        "the pull reaches {up_to_bytes} bytes, short of the credit plus one frame group"
    );
}
