use super::*;

/// The closed-form [`aligned_wire_size`] must equal the encoder walk
/// byte-for-byte for every group-aligned contiguous range: it is what the
/// receiver's overrun / short-delivery / ceiling bounds and the voucher
/// cadence are computed from, so a one-byte drift is a money bug. Swept over
/// blob sizes that exercise every tree shape up to several groups (full,
/// ragged, half-group tails, exact multiples) and every aligned `[offset, end)`
/// window inside each.
#[test]
fn closed_form_wire_size_matches_the_encoder_walk() {
    let g = CHUNK_GROUP_BYTES;
    let mut sizes = vec![1, 1023, 1024, 1025, g / 2, g / 2 + 1, g - 1, g, g + 1];
    for groups in 2..=9u64 {
        for tail in [0, 1, g / 2, g / 2 + 1, g - 1] {
            sizes.push((groups - 1) * g + tail.max(1));
            sizes.push(groups * g + tail);
        }
    }
    for total in sizes {
        let groups = total.div_ceil(g);
        for first in 0..groups {
            for last in first + 1..=groups {
                let offset = first * g;
                let len = (last * g).min(total) - offset;
                let aligned = align_range(offset, len, total).expect("aligned");
                let closed = aligned_wire_size(total, aligned.chunk_ranges())
                    .expect("aligned ranges take the closed form");
                let walked = walked_wire_size(total, aligned.chunk_ranges());
                assert_eq!(
                    closed, walked,
                    "total={total} offset={offset} len={len}: closed form {closed} != walk {walked}"
                );
            }
        }
    }
}

/// A group-aligned end cuts the pre-order encoding of a range between two
/// items: the encoding of `[offset, split)` is a byte prefix of the
/// encoding of `[offset, end)`. A client that stops a leg at a steal's
/// split relies on it, so the bytes it read verify on their own and are
/// exactly what it pays for.
#[test]
fn a_shorter_range_encodes_as_a_prefix_of_a_longer_one() {
    let g = CHUNK_GROUP_BYTES;
    for total in [5 * g, 9 * g + 321, 16 * g, 17 * g - 1] {
        let data: Vec<u8> = (0..total)
            .map(|i| u8::try_from(i * 31 % 251).expect("below 251"))
            .collect();
        let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let root = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data);
        let encode = |offset: u64, end: u64| {
            let aligned = align_range(offset, end - offset, total).expect("aligned");
            let window = usize::try_from(aligned.fetch_start()).expect("fits")
                ..usize::try_from(aligned.fetch_end()).expect("fits");
            let slice = data.get(window).expect("in the blob");
            let combined =
                encode_verified_range(root, &aligned, slice, outboard.clone()).expect("encodes");
            combined.slice(8..)
        };
        let groups = total.div_ceil(g);
        for first in 0..groups {
            let longer = encode(first * g, total);
            for split in first + 1..groups {
                let shorter = encode(first * g, split * g);
                assert_eq!(
                    longer.get(..shorter.len()),
                    Some(&shorter[..]),
                    "total={total} offset={} split={}",
                    first * g,
                    split * g
                );
            }
        }
    }
}

/// Large trees, small windows: the walk is `O(window)` so it stays testable,
/// and the closed form must agree on offsets deep inside multi-level trees —
/// including the ragged final group of a blob in the gigabytes.
#[test]
fn closed_form_wire_size_matches_the_encoder_walk_deep_in_large_trees() {
    let g = CHUNK_GROUP_BYTES;
    for total in [
        1_000_000_007u64,
        (1u64 << 30) + 12_345,
        (1u64 << 33) - g / 2,
        (1u64 << 34) + 1,
    ] {
        let groups = total.div_ceil(g);
        for first in [
            0u64,
            1,
            2,
            3,
            1000,
            groups / 3,
            groups / 2,
            groups - 4,
            groups - 1,
        ] {
            for span in [1u64, 2, 3, 5] {
                let last = (first + span).min(groups);
                let offset = first * g;
                let len = (last * g).min(total) - offset;
                let aligned = align_range(offset, len, total).expect("aligned");
                let closed = aligned_wire_size(total, aligned.chunk_ranges())
                    .expect("aligned ranges take the closed form");
                let walked = walked_wire_size(total, aligned.chunk_ranges());
                assert_eq!(
                    closed, walked,
                    "total={total} offset={offset} len={len}: closed form {closed} != walk {walked}"
                );
            }
        }
    }
}

/// A range the closed form does not cover (a sub-group start) falls back to the
/// walk rather than answering wrongly.
#[test]
fn closed_form_declines_sub_group_ranges() {
    let total = 3 * CHUNK_GROUP_BYTES;
    let ranges = ChunkRanges::from(ChunkNum(3)..ChunkNum(40));
    assert!(aligned_wire_size(total, &ranges).is_none());
    assert_eq!(
        bao_encoded_size(total, &ranges),
        walked_wire_size(total, &ranges)
    );
}

// A blob spanning several chunk groups so the aligned fetch window has a
// non-trivial length we can over- and under-shoot.
const BLOB_SIZE: u64 = 5 * CHUNK_GROUP_BYTES + 123;

// `encode_verified_range` rejects a `range_data` longer than the aligned fetch
// window before any verification, so the surplus tail can never be silently
// dropped. The bogus outboard is never reached — the size guard fires first.
#[test]
fn rejects_overlong_range_data() {
    let aligned = align_range(0, CHUNK_GROUP_BYTES, BLOB_SIZE).expect("align");
    let fetch_len = aligned.fetch_len();
    let too_long = usize::try_from(fetch_len).expect("fits usize") + 1;
    let range_data = vec![0u8; too_long];
    let err = encode_verified_range([0u8; 32], &aligned, &range_data, Bytes::new())
        .expect_err("overlong range_data must be rejected");
    assert!(matches!(
        err,
        RangeVerifyError::RangeDataSize {
            expected,
            got,
            blob_size,
        } if expected == fetch_len && got == too_long && blob_size == BLOB_SIZE
    ));
}

// The mirror case: a truncated 206 body is a typed error, not an opaque
// verification short-read.
#[test]
fn rejects_too_short_range_data() {
    let aligned = align_range(0, CHUNK_GROUP_BYTES, BLOB_SIZE).expect("align");
    let fetch_len = aligned.fetch_len();
    let too_short = usize::try_from(fetch_len).expect("fits usize") - 1;
    let range_data = vec![0u8; too_short];
    let err = encode_verified_range([0u8; 32], &aligned, &range_data, Bytes::new())
        .expect_err("short range_data must be rejected");
    assert!(matches!(
        err,
        RangeVerifyError::RangeDataSize { expected, got, .. }
            if expected == fetch_len && got == too_short
    ));
}

/// A request whose end runs past the blob clamps to the blob's end instead
/// of erroring: the planner's bound can overshoot a claimed size, and a
/// leg that reaches the true end proves it.
#[test]
fn align_range_clamped_clamps_an_end_past_the_blob() {
    let total = 5 * CHUNK_GROUP_BYTES;
    let clamped = align_range_clamped(0, total + CHUNK_GROUP_BYTES, total).expect("clamps");
    let whole = align_range(0, 0, total).expect("whole blob aligns");
    assert_eq!(clamped.fetch_end(), whole.fetch_end());
    assert_eq!(clamped.chunk_ranges(), whole.chunk_ranges());
    assert_eq!(clamped.wire_len(), whole.wire_len());

    // An interior start whose end overshoots also clamps to the blob end.
    let interior = align_range_clamped(2 * CHUNK_GROUP_BYTES, total, total).expect("clamps");
    assert_eq!(interior.fetch_end(), total);
}

/// A start at or past the blob end (for a non-empty blob) is refused: the
/// mirror case to the clamp above.
#[test]
fn align_range_clamped_refuses_an_offset_at_or_past_the_end() {
    let total = 5 * CHUNK_GROUP_BYTES;
    assert!(matches!(
        align_range_clamped(total, 0, total),
        Err(RangeVerifyError::RangeOutOfBounds { .. })
    ));
    assert!(matches!(
        align_range_clamped(total + 1, 1, total),
        Err(RangeVerifyError::RangeOutOfBounds { .. })
    ));
}

/// `(0, 0)` stays the whole blob under the clamped rule, same as
/// [`align_range`].
#[test]
fn align_range_clamped_whole_blob_request_is_unchanged() {
    let total = 5 * CHUNK_GROUP_BYTES + 123;
    let clamped = align_range_clamped(0, 0, total).expect("whole blob");
    let plain = align_range(0, 0, total).expect("whole blob");
    assert_eq!(clamped.fetch_end(), plain.fetch_end());
    assert_eq!(clamped.chunk_ranges(), plain.chunk_ranges());
}

/// The empty blob is addressable only as `(0, 0)`, the same as
/// [`align_range`]: any positive offset or length is refused, never
/// clamped to nothing.
#[test]
fn align_range_clamped_empty_blob_is_whole_blob_only() {
    assert!(align_range_clamped(0, 0, 0).is_ok());
    assert!(matches!(
        align_range_clamped(1, 0, 0),
        Err(RangeVerifyError::RangeOutOfBounds { .. })
    ));
    assert!(matches!(
        align_range_clamped(0, 1, 0),
        Err(RangeVerifyError::RangeOutOfBounds { .. })
    ));
}

/// Edges [`align_range_clamped`] must get right beyond the group-scale cases
/// above: a 1-byte blob, an end landing exactly at the size (no clamp
/// needed), a start and end that both sit mid-group (unaligned either way),
/// and an offset paired with `u64::MAX` (the widest possible overflowing
/// end).
#[test]
fn align_range_clamped_sub_group_and_overflow_edges() {
    // A 1-byte blob: the whole blob is addressable, and any end past it
    // (here `byte_len` overflowing entirely) still clamps to the 1 byte.
    let one_byte = align_range_clamped(0, 0, 1).expect("1-byte whole blob");
    assert_eq!(one_byte.fetch_end(), 1);
    let one_byte_overflow = align_range_clamped(0, u64::MAX, 1).expect("clamps to 1 byte");
    assert_eq!(one_byte_overflow.fetch_end(), 1);
    assert_eq!(one_byte_overflow.chunk_ranges(), one_byte.chunk_ranges());

    // An end landing EXACTLY at the size: not a clamp case (the request was
    // already in bounds), and must equal the explicit whole-blob request.
    let total = 5 * CHUNK_GROUP_BYTES + 123;
    let exact_end = align_range_clamped(0, total, total).expect("exact end aligns");
    let whole = align_range_clamped(0, 0, total).expect("whole blob");
    assert_eq!(exact_end.fetch_end(), whole.fetch_end());
    assert_eq!(exact_end.chunk_ranges(), whole.chunk_ranges());

    // A start AND end that both sit mid-group, still fully in bounds: no
    // clamp fires, and the result must match `align_range`'s own answer.
    let (offset, len) = (CHUNK_GROUP_BYTES / 2, CHUNK_GROUP_BYTES);
    let unaligned_clamped = align_range_clamped(offset, len, total).expect("in bounds");
    let unaligned_plain = align_range(offset, len, total).expect("in bounds");
    assert_eq!(
        unaligned_clamped.fetch_start(),
        unaligned_plain.fetch_start()
    );
    assert_eq!(unaligned_clamped.fetch_end(), unaligned_plain.fetch_end());
    assert_eq!(
        unaligned_clamped.chunk_ranges(),
        unaligned_plain.chunk_ranges()
    );

    // `(1, u64::MAX)`: an in-bounds offset paired with the widest possible
    // overflowing end clamps to the blob's end, same as the whole tail.
    let widest = align_range_clamped(1, u64::MAX, total).expect("clamps");
    let tail = align_range_clamped(1, 0, total).expect("whole tail");
    assert_eq!(widest.fetch_end(), tail.fetch_end());
    assert_eq!(widest.chunk_ranges(), tail.chunk_ranges());
}
