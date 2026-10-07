use super::{
    B256, CHUNK_BYTES, MAX_CHAIN_LENGTH, U256, keccak256, pack_chain_meter, preimage_at,
    random_seed, root_from_seed, verify_forward,
};

fn seed() -> B256 {
    B256::repeat_byte(0x42)
}

/// ADR 003 §Chunk Cadence: `CHUNK_BYTES == BYTES_PER_MB` by identity, which
/// is what makes a chunk cost exactly the advertised `rate_per_mb` with no
/// rounding at any rate. Two constants, one number — pin them together so a
/// drift is a test failure and not a silent repricing of every tick.
#[test]
fn a_chunk_is_exactly_one_mb() {
    assert_eq!(CHUNK_BYTES, crate::rate::BYTES_PER_MB);
}

/// The whole verification contract, at both ends of the index space and in
/// the middle: a preimage released at `k` reaches the root in exactly `k`
/// steps.
#[test]
fn a_released_preimage_reaches_the_root_in_index_steps() {
    let root = root_from_seed(seed());
    for index in [1u8, 2, 128, MAX_CHAIN_LENGTH] {
        assert!(
            verify_forward(preimage_at(seed(), index), index, root),
            "index {index} must reach the root"
        );
    }
}

/// Index 0 IS the root, so settling a real-root voucher at its own `amount`
/// submits a value the payer already holds and walks nothing.
#[test]
fn index_zero_is_the_root_itself() {
    assert_eq!(preimage_at(seed(), 0), root_from_seed(seed()));
    assert!(verify_forward(
        root_from_seed(seed()),
        0,
        root_from_seed(seed())
    ));
}

/// The incremental step: a preimage at `k` reaches the one at `k − 1` in a
/// single hash. This is what makes the node's per-tick cost one keccak
/// rather than a walk from the root.
#[test]
fn each_step_is_one_hash_from_the_previous_tip() {
    let deeper = preimage_at(seed(), 130);
    let tip = preimage_at(seed(), 129);
    assert_eq!(keccak256(deeper), tip);
    assert!(verify_forward(deeper, 1, tip));
}

/// A fast stream may skip indices a slower one has not reached, so the walk
/// must span an arbitrary gap, not only one step.
#[test]
fn a_skipped_gap_verifies_in_one_walk() {
    let verified = 10u8;
    let tip = preimage_at(seed(), verified);
    let index = 37u8;
    assert!(verify_forward(
        preimage_at(seed(), index),
        index - verified,
        tip
    ));
}

/// A value from the wrong chain never verifies, which is the whole basis of
/// `BadPreimage`.
#[test]
fn a_foreign_preimage_never_verifies() {
    let other = B256::repeat_byte(0x43);
    assert!(!verify_forward(
        preimage_at(other, 5),
        5,
        root_from_seed(seed())
    ));
}

/// A shallower value cannot be passed off as a deeper one: reaching the tip
/// takes the hashes it takes, and offering the wrong count fails.
#[test]
fn a_shallower_preimage_cannot_claim_a_deeper_index() {
    let root = root_from_seed(seed());
    assert!(!verify_forward(preimage_at(seed(), 4), 5, root));
}

/// Nothing hashes to zero, so a sealed voucher (`chain_root == 0`) is
/// sealed at exactly its `amount` — this is what lets the zero root be a
/// sentinel with no branch anywhere (ADR 003 §The sealed voucher).
#[test]
fn no_index_above_zero_redeems_against_a_zero_root() {
    assert!(verify_forward(B256::ZERO, 0, B256::ZERO));
    for index in [1u8, 2, MAX_CHAIN_LENGTH] {
        assert!(!verify_forward(B256::ZERO, index, B256::ZERO));
        assert!(!verify_forward(
            preimage_at(seed(), index),
            index,
            B256::ZERO
        ));
    }
}

/// ADR 003 §One chain per lane: successive chains never share a root. Two
/// draws colliding is a 2⁻²⁵⁶ event, so this is the whole of the reuse
/// defence — there is no derivation input to get wrong and no counter to
/// have persisted.
#[test]
fn successive_draws_commit_disjoint_roots() {
    let mut roots = std::collections::HashSet::new();
    for _ in 0..64 {
        let seed = random_seed();
        assert_ne!(seed, B256::ZERO, "a draw of all zeros is not a real seed");
        assert!(
            roots.insert(root_from_seed(seed)),
            "two draws committed the same root"
        );
    }
}

/// The packed word's layout, at the boundaries that matter: the index is
/// the low byte as itself, the price occupies bytes 23..=30, and the
/// reserved span above stays zero. This is the representation the wire
/// index and the on-chain `uint8` extraction have to agree on exactly.
#[test]
fn the_packed_meter_puts_the_index_in_the_low_byte() {
    let word = pack_chain_meter(U256::from(10u64), MAX_CHAIN_LENGTH).unwrap();
    let bytes = word.to_be_bytes::<32>();
    assert_eq!(bytes[31], 255, "index is the low byte, as itself");
    assert_eq!(u64::from_be_bytes(bytes[23..31].try_into().unwrap()), 10);
    assert!(
        bytes[..23].iter().all(|b| *b == 0),
        "reserved span must be zero"
    );
}

/// The full-width price still leaves the reserved span clear — that is what
/// makes the span a real guarantee rather than an accident of small numbers.
#[test]
fn a_full_width_price_does_not_spill_into_the_reserved_span() {
    let word = pack_chain_meter(U256::from(u64::MAX), 1).unwrap();
    let bytes = word.to_be_bytes::<32>();
    assert!(bytes[..23].iter().all(|b| *b == 0));
    assert_eq!(bytes[31], 1);
}

/// A price wider than the word gives it is refused, never truncated —
/// truncation would silently re-price every metered chunk.
#[test]
fn an_over_wide_price_is_refused_not_truncated() {
    assert_eq!(
        pack_chain_meter(U256::from(u64::MAX) + U256::from(1u64), 0),
        Err(super::ChainMeterError::PriceExceedsWireWidth)
    );
}

/// The sealed shape packs to a zero word, which is what makes a cooperative
/// redemption almost entirely zero calldata.
#[test]
fn a_sealed_meter_is_the_zero_word() {
    assert_eq!(pack_chain_meter(U256::ZERO, 0).unwrap(), U256::ZERO);
}
