use super::*;

#[test]
fn full_covers_every_block_in_range_and_nothing_past_it() {
    let cov = Coverage::full(3);
    for i in 0..=2 {
        assert!(cov.covers(i), "block {i} should be covered by full(3)");
    }
    assert!(!cov.covers(3), "full(3) must not cover block 3");
}

#[test]
fn from_block_indices_covers_exactly_the_given_set() {
    let cov = Coverage::from_block_indices(4, [0, 2].into_iter());
    assert!(cov.covers(0));
    assert!(!cov.covers(1));
    assert!(cov.covers(2));
    assert!(!cov.covers(3));
}

#[test]
fn empty_is_empty() {
    assert!(Coverage::empty().is_empty());
    assert!(Coverage::default().is_empty());
    assert!(!Coverage::full(1).is_empty());
}

#[test]
fn num_blocks_zero_for_empty_blob() {
    assert_eq!(num_blocks(0), 0);
    assert_eq!(num_blocks(1), 1);
    assert_eq!(num_blocks(DISCOVERY_BLOCK_BYTES), 1);
    assert_eq!(num_blocks(DISCOVERY_BLOCK_BYTES + 1), 2);
}

#[test]
fn covered_blocks_walks_set_bits_in_order() {
    let cov = Coverage::from_block_indices(20, [3, 5, 17].into_iter());
    assert_eq!(cov.covered_blocks().collect::<Vec<_>>(), vec![3, 5, 17]);
}

#[test]
fn out_of_range_indices_in_the_input_set_are_dropped() {
    let cov = Coverage::from_block_indices(4, [0, 4, 100].into_iter());
    assert!(cov.covers(0));
    assert!(!cov.covers(4), "block 4 is out of range for num_blocks=4");
    assert_eq!(cov.covered_blocks().collect::<Vec<_>>(), vec![0]);
}

#[test]
fn postcard_round_trips() {
    let cov = Coverage::from_block_indices(20, [3, 5, 17].into_iter());
    let bytes = postcard::to_allocvec(&cov).expect("postcard serialize");
    let back: Coverage = postcard::from_bytes(&bytes).expect("postcard deserialize");
    assert_eq!(cov, back);

    let empty = Coverage::empty();
    let bytes = postcard::to_allocvec(&empty).expect("postcard serialize");
    let back: Coverage = postcard::from_bytes(&bytes).expect("postcard deserialize");
    assert_eq!(empty, back);
}

#[test]
fn max_coverage_bytes_matches_max_discoverable_blob() {
    // 100 GiB / 64 MiB = 1600 blocks, bit-packed at 8/byte = 200 bytes.
    assert_eq!(MAX_COVERAGE_BYTES, 200);
}

#[test]
fn deserialize_accepts_bitmap_at_the_cap() {
    // A single-field struct postcard-encodes identically to its `Vec<u8>`
    // field, so an at-cap byte vector is a valid at-cap `Coverage`.
    let at_cap = vec![0xFFu8; MAX_COVERAGE_BYTES];
    let bytes = postcard::to_allocvec(&at_cap).expect("postcard serialize");
    let back: Coverage = postcard::from_bytes(&bytes).expect("at-cap coverage must decode");
    assert_eq!(back.blocks.len(), MAX_COVERAGE_BYTES);
}

#[test]
fn deserialize_rejects_oversized_bitmap() {
    // One byte past the cap describes a blob larger than
    // MAX_DISCOVERABLE_BLOB_BYTES and must fail at decode rather than be
    // stored — this is the memory-amplification guard.
    let oversized = vec![0u8; MAX_COVERAGE_BYTES + 1];
    let bytes = postcard::to_allocvec(&oversized).expect("postcard serialize");
    let decoded: Result<Coverage, _> = postcard::from_bytes(&bytes);
    assert!(
        decoded.is_err(),
        "oversized coverage bitmap must be rejected"
    );
}
