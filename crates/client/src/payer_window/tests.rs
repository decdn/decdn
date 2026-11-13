use alloy::primitives::Address;

use proptest::prelude::*;

use super::{PayAheadPolicy, PayerWindows, WINDOW_FLOOR_BYTES, due};

const MIB: u64 = 1 << 20;

#[test]
fn a_cold_lane_pays_one_chunk_at_the_floor_window() {
    assert_eq!(due(WINDOW_FLOOR_BYTES, 10 * MIB, 0, u64::MAX), (1, 0));
}

#[test]
fn a_stream_on_a_funded_lane_pays_nothing_of_its_own() {
    // The lane holds a full window: the node sends against it with no proof.
    assert_eq!(due(8 * MIB, 20 * MIB, 8 * MIB, u64::MAX), (0, 0));
    assert_eq!(due(8 * MIB, 20 * MIB, 9 * MIB, u64::MAX), (0, 0));
}

#[test]
fn the_window_is_refilled_as_credit_runs_down() {
    assert_eq!(due(8 * MIB, 20 * MIB, 5 * MIB, u64::MAX), (3, 0));
}

#[test]
fn the_tail_pays_a_closing_residual_and_never_past_the_end() {
    let half = MIB / 2;
    assert_eq!(due(17 * MIB, MIB + half, 0, u64::MAX), (1, half));
    // The transfer is smaller than one chunk: one closing voucher, exact.
    assert_eq!(due(17 * MIB, half, 0, u64::MAX), (0, half));
    // Nothing left to receive, nothing to pay.
    assert_eq!(due(17 * MIB, 0, 0, u64::MAX), (0, 0));
}

#[test]
fn a_full_per_node_cap_stalls_a_lane_that_holds_credit_but_not_one_that_holds_none() {
    assert_eq!(due(8 * MIB, 20 * MIB, MIB, 0), (0, 0));
    assert_eq!(due(8 * MIB, 20 * MIB, 0, 0), (1, 0));
}

proptest! {
    /// A lane never pays for one byte range twice: whatever the window, the
    /// frame sizes and the order streams draw credit in, the bytes a lane pays
    /// for never exceed the bytes it is asked to deliver, and every delivered
    /// byte was paid before it was sent.
    #[test]
    fn a_lane_never_pays_twice_for_the_same_bytes(
        total in 1u64..(40 * MIB),
        window in WINDOW_FLOOR_BYTES..(70 * MIB),
        frames in proptest::collection::vec(1u64..(2 * MIB), 1..64),
        streams in 1usize..4,
    ) {
        // `held` is the lane's credit, shared by every stream; `paid` and
        // `received` are lane totals. The streams split `total` evenly.
        let per_stream = total / streams as u64;
        let mut need: Vec<u64> = vec![per_stream; streams];
        if let Some(first) = need.first_mut() {
            *first += total - per_stream * streams as u64;
        }
        let mut held = 0u64;
        let mut paid = 0u64;
        let mut received = 0u64;
        let mut frame = frames.iter().cycle();
        for _ in 0..4096 {
            if need.iter().all(|&n| n == 0) {
                break;
            }
            for left in &mut need {
                // The stream pays before it reads on.
                let (chunks, residual) = due(window, *left, held, u64::MAX);
                let pay = chunks * MIB + residual;
                paid += pay;
                held += pay;
                // The node sends only while the lane's credit covers the frame.
                let len = (*frame.next().unwrap()).min(*left).min(held);
                held -= len;
                received += len;
                *left -= len;
            }
        }
        prop_assert!(received <= total);
        // Paid ahead of delivery, but never beyond the bytes still owed: the
        // credit left over is at most what a stream was still to receive.
        prop_assert!(paid >= received);
        prop_assert!(paid - received <= held);
        prop_assert!(paid <= total + MIB, "paid {paid} for a {total}-byte transfer");
    }
}

#[test]
fn a_node_the_payer_has_not_used_starts_at_the_floor() {
    let policy = PayAheadPolicy::new();
    assert_eq!(policy.window_bytes(0, 0, 10), WINDOW_FLOOR_BYTES);
}

#[test]
fn the_window_ramps_with_verified_bytes_up_to_the_cap() {
    let policy = PayAheadPolicy::new();
    assert_eq!(policy.window_bytes(40 * MIB, 0, 10), 20 * MIB);
    // 640 base units at 10 per MiB is 64 MiB.
    assert_eq!(policy.window_bytes(1000 * MIB, 0, 10), 64 * MIB);
}

#[test]
fn a_higher_quoted_rate_shrinks_the_window_in_bytes() {
    let policy = PayAheadPolicy::new();
    let cheap = policy.window_bytes(10_000 * MIB, 0, 10);
    let dear = policy.window_bytes(10_000 * MIB, 0, 20);
    assert_eq!(dear * 2, cheap);
}

#[test]
fn one_lost_byte_cancels_loss_weight_verified_bytes() {
    let policy = PayAheadPolicy::new();
    let before = policy.window_bytes(100 * MIB, 0, 10);
    let after = policy.window_bytes(100 * MIB, MIB, 10);
    assert!(after < before);
    // 100 MiB verified minus 20 MiB cancelled, halved.
    assert_eq!(after, 40 * MIB);
}

#[test]
fn a_cap_below_the_floor_still_leaves_the_floor() {
    let policy = PayAheadPolicy {
        cap_micro_usdc: 1,
        ..PayAheadPolicy::new()
    };
    assert_eq!(policy.window_bytes(u64::MAX, 0, 1000), WINDOW_FLOOR_BYTES);
}

#[test]
fn a_zero_divisor_reads_as_one() {
    let policy = PayAheadPolicy {
        divisor: 0,
        ..PayAheadPolicy::new()
    };
    assert_eq!(policy.window_bytes(10 * MIB, 0, 10), 10 * MIB);
}

#[test]
fn the_per_node_cap_is_shared_across_every_holder_of_the_record() {
    let windows = PayerWindows::new(PayAheadPolicy::new());
    let address = Address::repeat_byte(7);
    let a = windows.node(address);
    let b = windows.node(address);
    a.note_prepaid(60 * MIB);
    assert_eq!(b.outstanding(), 60 * MIB);
    assert_eq!(b.cap_room_bytes(10), 4 * MIB);
    b.note_spent(60 * MIB);
    assert_eq!(a.cap_room_bytes(10), 64 * MIB);
}

#[test]
fn a_write_off_shrinks_the_next_window_and_spends_the_budget() {
    let windows = PayerWindows::new(PayAheadPolicy::new());
    let node = windows.node(Address::repeat_byte(1));
    node.note_verified(100 * MIB);
    let before = node.window_bytes(10);
    node.note_prepaid(8 * MIB);
    node.write_off(8 * MIB, 0, 10);
    assert_eq!(node.lost(), 8 * MIB);
    assert_eq!(node.outstanding(), 0);
    assert!(node.window_bytes(10) < before);
    assert_eq!(windows.written_off_micro_usdc(), 80);
}

#[test]
fn a_spent_run_budget_pins_every_node_to_the_floor() {
    let windows = PayerWindows::new(PayAheadPolicy {
        write_off_budget_micro_usdc: 10,
        ..PayAheadPolicy::new()
    });
    let liar = windows.node(Address::repeat_byte(1));
    let honest = windows.node(Address::repeat_byte(2));
    honest.note_verified(1000 * MIB);
    assert!(honest.window_bytes(10) > WINDOW_FLOOR_BYTES);
    liar.note_prepaid(2 * MIB);
    liar.write_off(2 * MIB, 0, 10);
    assert!(honest.budget_spent());
    assert_eq!(honest.window_bytes(10), WINDOW_FLOOR_BYTES);
}

#[test]
fn seeding_keeps_the_larger_count() {
    let windows = PayerWindows::new(PayAheadPolicy::new());
    let address = Address::repeat_byte(3);
    windows.node(address).note_verified(5 * MIB);
    windows.seed(address, 2 * MIB, 3);
    windows.seed(address, 9 * MIB, 1);
    let node = windows.node(address);
    assert_eq!(node.verified(), 9 * MIB);
    assert_eq!(node.lost(), 3);
    assert_eq!(windows.snapshot(), vec![(address, 9 * MIB, 3)]);
}
