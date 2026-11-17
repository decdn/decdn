use alloy::primitives::Address;

use proptest::prelude::*;

use decdn_bao_range::CHUNK_GROUP_BYTES;

use super::{PayAheadPolicy, PayerWindows, WINDOW_FLOOR_BYTES, due, room};

const MIB: u64 = 1 << 20;

#[test]
fn a_new_stream_pays_one_chunk_at_the_floor_window() {
    assert_eq!(due(WINDOW_FLOOR_BYTES, 10 * MIB, 0, u64::MAX), (1, 0));
}

#[test]
fn a_stream_holding_its_window_pays_nothing_more() {
    assert_eq!(due(8 * MIB, 20 * MIB, 8 * MIB, u64::MAX), (0, 0));
    assert_eq!(due(8 * MIB, 20 * MIB, 9 * MIB, u64::MAX), (0, 0));
}

#[test]
fn the_window_is_refilled_as_credit_runs_down() {
    assert_eq!(due(8 * MIB, 20 * MIB, 5 * MIB, u64::MAX), (3, 0));
}

#[test]
fn a_shortfall_below_one_chunk_pays_nothing_yet() {
    assert_eq!(
        due(
            WINDOW_FLOOR_BYTES,
            10 * MIB,
            WINDOW_FLOOR_BYTES - MIB / 2,
            u64::MAX
        ),
        (0, 0)
    );
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
fn a_full_per_node_cap_stalls_only_a_stream_whose_credit_still_feeds_a_frame() {
    // Credit for at least one frame: the node keeps sending, so the stream
    // waits for cap room.
    assert_eq!(due(8 * MIB, 20 * MIB, MIB, room(0, MIB)), (0, 0));
    // A stream with no credit takes one chunk past the cap.
    assert_eq!(due(8 * MIB, 20 * MIB, 0, room(0, 0)), (1, 0));
    // A sliver below one chunk group feeds no frame, so it takes one chunk too.
    let sliver = CHUNK_GROUP_BYTES / 2;
    assert_eq!(due(8 * MIB, 20 * MIB, sliver, room(0, sliver)), (1, 0));
}

proptest! {
    /// A stream never pays for one byte twice: whatever the window and the
    /// frame sizes, it pays ahead of delivery, never beyond the bytes it is
    /// asked to deliver, and every delivered byte was paid before it was sent.
    #[test]
    fn a_stream_never_pays_twice_for_the_same_bytes(
        total in 1u64..(40 * MIB),
        window in WINDOW_FLOOR_BYTES..(70 * MIB),
        frames in proptest::collection::vec(1u64..(2 * MIB), 1..64),
    ) {
        let mut left = total;
        let mut held = 0u64;
        let mut paid = 0u64;
        let mut received = 0u64;
        let mut frame = frames.iter().cycle();
        for _ in 0..4096 {
            if left == 0 {
                break;
            }
            // The stream pays before it reads on.
            let (chunks, residual) = due(window, left, held, u64::MAX);
            let pay = chunks * MIB + residual;
            paid += pay;
            held += pay;
            // The node sends only while the stream's credit covers the frame.
            let len = (*frame.next().unwrap()).min(left).min(held);
            held -= len;
            received += len;
            left -= len;
        }
        prop_assert_eq!(received, total);
        prop_assert_eq!(paid, total, "paid {} for a {}-byte transfer", paid, total);
        prop_assert_eq!(held, 0);
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
fn a_write_off_shrinks_only_that_nodes_window() {
    let windows = PayerWindows::new(PayAheadPolicy::new());
    let liar = windows.node(Address::repeat_byte(1));
    let honest = windows.node(Address::repeat_byte(2));
    liar.note_verified(100 * MIB);
    honest.note_verified(100 * MIB);
    let before = liar.window_bytes(10);
    liar.write_off(MIB);
    assert_eq!(liar.lost(), MIB);
    assert!(liar.window_bytes(10) < before);
    assert_eq!(honest.window_bytes(10), before);
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
}

#[test]
fn only_this_processs_progress_is_unsaved() {
    let windows = PayerWindows::new(PayAheadPolicy::new());
    let address = Address::repeat_byte(4);
    windows.seed(address, 9 * MIB, 1);
    windows.node(address).note_verified(MIB);
    windows.node(address).write_off(2);
    assert_eq!(windows.take_unsaved(), vec![(address, MIB, 2)]);
    assert!(windows.take_unsaved().is_empty(), "taken once");
    windows.return_unsaved(address, MIB, 2);
    assert_eq!(windows.take_unsaved(), vec![(address, MIB, 2)]);
}
