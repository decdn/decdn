use super::{
    BudgetPacer, CHUNK_BYTES, CHUNK_GROUP_BYTES, DownstreamFrontier, MIN_DRAW_WINDOW,
    PULL_WINDOW_FLOOR, PaceDecision, PaceState, Pacer, RampPacer, WindowPacer,
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
        seller_reserve: U256::ZERO,
        topups_used: 0,
        max_topups: 3,
        exhaustion_confirmed: false,
        pulled_frontier: 0,
        gap_remaining: 1_000_000,
        downstream: DownstreamFrontier::default(),
    }
}

#[test]
fn a_deposit_inside_the_seller_floor_band_tops_up_before_the_voucher_is_short() {
    // The voucher (10) is affordable at 500, but 500 < 10 + a 1_000 floor: a
    // serving peer would refuse the next open. Top up to the working deposit now.
    let mut s = healthy();
    s.remaining_deposit = U256::from(500u64);
    s.seller_reserve = U256::from(1_000u64);
    assert_eq!(
        BudgetPacer::new().decide(&s),
        PaceDecision::TopUp(U256::from(4_500u64))
    );
}

#[test]
fn a_deposit_above_the_seller_floor_band_draws() {
    // 1_000 >= 10 + a 900 floor: the peer still admits new streams.
    let mut s = healthy();
    s.seller_reserve = U256::from(900u64);
    assert!(matches!(
        BudgetPacer::new().decide(&s),
        PaceDecision::Draw { .. }
    ));
}

#[test]
fn the_seller_floor_never_refuses_on_its_own() {
    // Inside the band with no top-up left (or none enabled), the voucher is still
    // affordable: keep drawing and let the peer decide, never refuse.
    let mut s = healthy();
    s.remaining_deposit = U256::from(500u64);
    s.seller_reserve = U256::from(1_000u64);
    s.topups_used = s.max_topups;
    assert!(matches!(
        BudgetPacer::new().decide(&s),
        PaceDecision::Draw { .. }
    ));
    let mut s = healthy();
    s.remaining_deposit = U256::from(500u64);
    s.seller_reserve = U256::from(1_000u64);
    s.working_deposit = U256::ZERO;
    assert!(matches!(
        BudgetPacer::new().decide(&s),
        PaceDecision::Draw { .. }
    ));
}

#[test]
fn a_working_deposit_inside_the_band_draws_after_its_top_up() {
    // Topped up to a working deposit (600) that still sits inside the band
    // (< 10 + 1_000): nothing more to add, so draw rather than loop on top-ups.
    let mut s = healthy();
    s.remaining_deposit = U256::from(600u64);
    s.working_deposit = U256::from(600u64);
    s.seller_reserve = U256::from(1_000u64);
    assert!(matches!(
        BudgetPacer::new().decide(&s),
        PaceDecision::Draw { .. }
    ));
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

/// #2296: right after a refill restored the deposit to the working target,
/// a stale refusal confirmed an exhaustion whose top-up would add a few
/// micro-USDC. That top-up costs two transactions and changes nothing.
#[test]
fn a_confirmed_exhaustion_below_the_low_water_shortfall_refuses() {
    let mut s = healthy();
    s.exhaustion_confirmed = true;
    // additional = 5000 - 4998 = 2, far below the 1000 low water.
    s.remaining_deposit = U256::from(4_998u64);
    assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Refuse);
    // At the low water exactly, the top-up is worth sending.
    s.remaining_deposit = U256::from(4_000u64);
    assert_eq!(
        BudgetPacer::new().decide(&s),
        PaceDecision::TopUp(U256::from(1_000u64))
    );
}

/// The floor applies only while the deposit still covers the next voucher:
/// a deposit that cannot pay for it tops up whatever the shortfall.
#[test]
fn an_unaffordable_voucher_tops_up_even_a_small_shortfall() {
    let mut s = healthy();
    s.next_voucher_cost = U256::from(4_999u64);
    s.remaining_deposit = U256::from(4_998u64);
    assert_eq!(
        BudgetPacer::new().decide(&s),
        PaceDecision::TopUp(U256::from(2u64))
    );
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
    assert!(matches!(pacer.decide(&s), PaceDecision::Draw { .. }));
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
        assert!(
            matches!(
                WindowPacer::new(window).decide(&s),
                PaceDecision::Draw { .. }
            ),
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
fn window_pacer_passes_through_done_topup_refuse() {
    // Fully paid -> Done, identical to BudgetPacer, regardless of window state.
    let mut s = healthy();
    s.cleared_bytes = s.requested_bytes;
    s.pulled_frontier = 1_000_000;
    s.downstream.served_paid = 0;
    assert_eq!(WindowPacer::new(10).decide(&s), PaceDecision::Done);
    assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Done);

    // Exhausted with attempts left -> TopUp, exactly BudgetPacer's amount; the
    // window never overrides the money decision.
    let mut s = healthy();
    s.remaining_deposit = U256::from(4u64);
    s.next_voucher_cost = U256::from(10u64);
    s.pulled_frontier = 1_000_000;
    s.downstream.served_paid = 0;
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
    s.downstream.served_paid = 0;
    assert_eq!(WindowPacer::new(10).decide(&s), PaceDecision::Refuse);
    assert_eq!(BudgetPacer::new().decide(&s), PaceDecision::Refuse);
}

#[test]
fn ramp_pacer_paces_pull_on_the_ramped_window() {
    // Unpaid: downstream.served_paid = 0 -> window = floor. The pull may run at
    // most `floor` ahead of the served-paid frontier, then Wait.
    let floor = 4 * CHUNK_GROUP_BYTES;
    let pacer = RampPacer {
        divisor: 2,
        floor,
        credit_max: 64 * CHUNK_GROUP_BYTES,
        paid_base: 0,
        paid_carried: 0,
    };
    let mut s = healthy();
    s.downstream.served_paid = 0;
    s.pulled_frontier = floor; // already floor ahead
    assert_eq!(pacer.decide(&s), PaceDecision::Wait);
}

#[test]
fn ramp_pacer_widens_the_pull_window_as_served_paid_advances() {
    // downstream.served_paid = 32 groups, divisor 2 -> window 16 groups > floor,
    // so a pull only `floor` ahead may Draw again.
    let floor = 4 * CHUNK_GROUP_BYTES;
    let pacer = RampPacer {
        divisor: 2,
        floor,
        credit_max: 64 * CHUNK_GROUP_BYTES,
        paid_base: 0,
        paid_carried: 0,
    };
    let mut s = healthy();
    s.downstream.served_paid = 32 * CHUNK_GROUP_BYTES;
    s.pulled_frontier = floor;
    assert!(matches!(pacer.decide(&s), PaceDecision::Draw { .. }));
}

#[test]
fn ramp_pacer_measures_paid_from_the_session_start() {
    // A request resuming at 32 groups has an ABSOLUTE served-paid frontier of
    // 32 groups before it pays a byte. The ramp must read that as "0 paid",
    // so the window stays at the floor and a pull already `floor` ahead Waits.
    let floor = 4 * CHUNK_GROUP_BYTES;
    let start = 32 * CHUNK_GROUP_BYTES;
    let pacer = RampPacer {
        divisor: 2,
        floor,
        credit_max: 64 * CHUNK_GROUP_BYTES,
        paid_base: start,
        paid_carried: 0,
    };
    let mut s = healthy();
    s.downstream.served_paid = start;
    s.pulled_frontier = start + floor;
    assert_eq!(pacer.decide(&s), PaceDecision::Wait);

    // Once the stream has paid 32 groups PAST its start, the window is 16
    // groups (> floor), so the same pull may Draw again.
    s.downstream.served_paid = start + 32 * CHUNK_GROUP_BYTES;
    assert!(matches!(pacer.decide(&s), PaceDecision::Draw { .. }));
}

#[test]
fn ramp_pacer_adds_the_carried_credit_to_the_streams_own_payment() {
    // A stream that carries 32 groups of lane credit opens at a 16-group
    // window before it pays a byte, even though it starts at an offset
    // smaller than the carry: the carry is a term of the ramp, not an offset.
    let floor = 4 * CHUNK_GROUP_BYTES;
    let start = 8 * CHUNK_GROUP_BYTES;
    let pacer = RampPacer {
        divisor: 2,
        floor,
        credit_max: 64 * CHUNK_GROUP_BYTES,
        paid_base: start,
        paid_carried: 32 * CHUNK_GROUP_BYTES,
    };
    let mut s = healthy();
    s.downstream.served_paid = start;
    s.pulled_frontier = start + floor;
    assert!(matches!(pacer.decide(&s), PaceDecision::Draw { .. }));
    s.pulled_frontier = start + 16 * CHUNK_GROUP_BYTES;
    assert_eq!(pacer.decide(&s), PaceDecision::Wait);

    // Its own payment adds to the carry: 16 more paid groups widen the
    // window to 24 groups.
    s.downstream.served_paid = start + 16 * CHUNK_GROUP_BYTES;
    s.pulled_frontier = start + 16 * CHUNK_GROUP_BYTES + 24 * CHUNK_GROUP_BYTES;
    assert_eq!(pacer.decide(&s), PaceDecision::Wait);
    s.pulled_frontier -= CHUNK_GROUP_BYTES;
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
        paid_base: 0,
        paid_carried: 0,
    };
    let mut s = healthy();
    s.requested_bytes = 64 * CHUNK_BYTES;
    // The frontier the serve leg publishes trails the client's real paid
    // position by up to one group (`content_paid_frontier` floors to one).
    s.downstream.served_paid = 0;
    s.pulled_frontier = CHUNK_BYTES;
    assert!(
        matches!(pacer.decide(&s), PaceDecision::Draw { .. }),
        "the pull must be able to feed the client past the chunk boundary it \
         pays at, or neither side can move"
    );
}
