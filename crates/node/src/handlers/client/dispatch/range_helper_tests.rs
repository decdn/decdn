use super::{CHUNK_GROUP_BYTES, aligned_span, range_out_of_bounds};

#[test]
fn range_out_of_bounds_mirrors_align_range_clamped() {
    let total = 100 * 1024;
    // Whole blob and in-bounds tails / bounds are satisfiable.
    assert!(!range_out_of_bounds(0, 0, total));
    assert!(!range_out_of_bounds(16 * 1024, 0, total));
    assert!(!range_out_of_bounds(16 * 1024, 32 * 1024, total));
    assert!(!range_out_of_bounds(0, total, total));
    // An end past the blob, or an overflowing end, clamps to the blob end
    // rather than refusing; only a start at or past the end refuses.
    assert!(!range_out_of_bounds(16 * 1024, total, total));
    assert!(!range_out_of_bounds(1, u64::MAX, total));
    assert!(range_out_of_bounds(total, 0, total));
    assert!(range_out_of_bounds(total + 1, 0, total));
    assert!(range_out_of_bounds(u64::MAX, 1, total));
    // The empty blob is addressable only as (0, 0).
    assert!(!range_out_of_bounds(0, 0, 0));
    assert!(range_out_of_bounds(0, 1, 0));
}

/// The clamped rule the serve tiers apply (ADR 005 §Bounded byte ranges): a
/// request whose end runs past the blob is served, not refused.
#[test]
fn a_request_whose_end_runs_past_the_blob_is_served_clamped() {
    let total = 100 * 1024;
    assert!(!range_out_of_bounds(0, total + 1, total));
    // Its billed span is exactly the aligned whole blob, same as (0, 0).
    assert_eq!(aligned_span(0, total + 1, total), aligned_span(0, 0, total));
}

const G: u64 = CHUNK_GROUP_BYTES;

#[test]
fn whole_blob_is_the_blob() {
    assert_eq!(aligned_span(0, 0, 5 * G), 5 * G);
    // A partial final group is clamped to the blob, not rounded past it.
    assert_eq!(aligned_span(0, 0, 5 * G + 1), 5 * G + 1);
}

#[test]
fn zero_length_blob_prices_nothing() {
    // #1054: must yield a zero ceiling so an empty blob still serves.
    assert_eq!(aligned_span(0, 0, 0), 0);
}

#[test]
fn a_tiny_range_is_priced_as_the_group_it_touches() {
    // The regression this helper exists for: pricing `byte_len` directly
    // would reserve 2 bytes for a request that bills a full 16 KiB group.
    assert_eq!(aligned_span(0, 2, 10 * G), G);
    assert_eq!(aligned_span(1, 1, 10 * G), G);
}

#[test]
fn a_range_straddling_a_boundary_pays_both_groups() {
    // Worst case: one byte either side of a boundary spans two whole groups.
    assert_eq!(aligned_span(G - 1, 2, 10 * G), 2 * G);
}

#[test]
fn an_already_aligned_range_gains_nothing() {
    assert_eq!(aligned_span(G, G, 10 * G), G);
    assert_eq!(aligned_span(2 * G, 3 * G, 10 * G), 3 * G);
}

#[test]
fn a_whole_tail_runs_from_its_group_start_to_the_blob_end() {
    // `byte_len == 0` with a non-zero offset is the resume shape.
    assert_eq!(aligned_span(3 * G, 0, 10 * G), 7 * G);
    // Mid-group offset floors back to the group start.
    assert_eq!(aligned_span(3 * G + 5, 0, 10 * G), 7 * G);
}

#[test]
fn a_range_past_the_blob_end_clamps_to_the_blob() {
    // The caller's bounds check rejects these, but the helper must not
    // over-price if it is ever reached with a partial final group.
    assert_eq!(aligned_span(0, u64::MAX, 3 * G + 7), 3 * G + 7);
}
