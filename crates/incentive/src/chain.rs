//! `PayWord` hash chains: the per-lane meter that advances a voucher's
//! settlement anchor without a signature (ADR 003 §Hash-chain metering
//! (`PayWord`)).
//!
//! # The primitive
//!
//! The payer draws a random seed `s` for a lane and commits
//! `chain_root = keccak^N(s)`, where `N` is [`MAX_CHAIN_LENGTH`]. It signs that
//! root into a voucher. After the node delivers chunk `k` — and after the payer
//! has verified those bytes against the BLAKE3 root — the payer releases
//! `keccak^(N−k)(s)`. The node hashes the released value forward until it
//! reaches a value it already trusts.
//!
//! Preimage resistance makes a release self-proving: nobody computes
//! `keccak^(N−k−1)(s)` from `keccak^(N−k)(s)` without the seed, so a deeper
//! preimage **is** the receipt for every chunk below it. Accepting one costs a
//! single keccak — no signature, no round trip, and nothing to persist before
//! the node sends the next chunk.
//!
//! Redemption resolves the two axes with one formula:
//! `claimed = amount + chain_index × chunk_price`.
//!
//! # Index convention
//!
//! `chain_root` sits at index `0` and index `k` reveals `keccak^(N−k)(s)`,
//! which redemption checks as `keccak^k(preimage) == chain_root`. The wire byte
//! carries the index as itself — no offset on send, no increment on receipt —
//! and the same byte is the low byte of the on-chain packed `chainMeter` word,
//! so no off-by-one is possible anywhere along the path.
//!
//! # One chain per lane
//!
//! A released preimage is a **bearer proof**: it names no payee, and its
//! binding to one comes entirely from the voucher whose `chain_root` it
//! satisfies. So a payer MUST NOT commit one root on two lanes — reuse lets a
//! second node claim chunks it never delivered, and the payer pays twice for
//! one tick (ADR 003 §Cross-lane preimage spend).
//!
//! [`random_seed`] is that defence, and it keeps no state to get wrong: every
//! chain draws 32 fresh bytes from the OS, so two chains share a root only with
//! probability 2⁻²⁵⁶ — on one lane or across every lane a payer holds. Reuse
//! stops being a rule to enforce and becomes an outcome the draw does not
//! produce.
//!
//! A seed lives in memory for the life of its chain and is never written down,
//! never derived from the signing key, and never reproduced. Nothing needs to
//! reproduce one: a chain is only ever extended by the process that drew it,
//! because crossing a process or device boundary FOLDS the frontier the node
//! proved into a fresh signed amount and opens a fresh chain (ADR 003
//! §Resumption folds).
//!
//! There is deliberately **no node-side check** for root reuse: the reuse pays
//! the node, so a rejection rule protects nobody who would choose to run it,
//! and enforcing it would need an unbounded, never-expiring set of every root
//! the node has ever seen.

use alloy::primitives::{B256, U256, keccak256};
use rand::Rng;

pub use decdn_protocol::client::{CHUNK_BYTES, MAX_CHAIN_LENGTH};

/// Hash `value` forward exactly `steps` times.
///
/// `steps` is a `u8`, so the walk is bounded at [`MAX_CHAIN_LENGTH`] by the
/// type rather than by a runtime comparison — the same bound the one-byte wire
/// index and the on-chain `uint8` extraction carry.
#[must_use]
fn hash_forward(value: B256, steps: u8) -> B256 {
    let mut acc = value;
    for _ in 0..steps {
        acc = keccak256(acc);
    }
    acc
}

/// The chain head committed in a voucher: `keccak^MAX_CHAIN_LENGTH(seed)`,
/// the value at index 0.
///
/// A real root is a keccak output, so it is all-zero only with probability
/// 2⁻²⁵⁶; a payer that draws such a seed redraws, which is what makes all-zero
/// a safe sentinel for the sealed voucher (ADR 003 §The sealed voucher).
#[must_use]
pub fn root_from_seed(seed: B256) -> B256 {
    hash_forward(seed, MAX_CHAIN_LENGTH)
}

/// The preimage to release at `index`: `keccak^(MAX_CHAIN_LENGTH − index)(seed)`.
///
/// At `index == 0` this returns the root itself, which is the value a payer
/// submits when it settles a real-root voucher at exactly its `amount` without
/// walking anything.
#[must_use]
pub fn preimage_at(seed: B256, index: u8) -> B256 {
    hash_forward(seed, MAX_CHAIN_LENGTH - index)
}

/// Verify a released preimage against a value the receiver already trusts:
/// `keccak^steps(preimage) == tip`.
///
/// `tip` is the deepest preimage the receiver has verified on this chain (or
/// the `chain_root` itself when it has verified none), and `steps` is
/// `index − verified`. Hashing forward from the *submitted* value — never from
/// a stored intermediate — is what lets both the node and the contract keep no
/// chain state at all.
#[must_use]
pub fn verify_forward(preimage: B256, steps: u8, tip: B256) -> bool {
    hash_forward(preimage, steps) == tip
}

/// Pack `chunk_price` and `chain_index` into the single `chainMeter` word the
/// contract's `LaneVoucher` carries (ADR 003 §Voucher signatures are compact).
///
/// ```text
///  byte  0                     22 23            30 31
///       +------------------------+----------------+--+
///       |  reserved — MUST be 0  |   chunkPrice   |ci|
///       +------------------------+----------------+--+
///          23 bytes                 8 bytes (u64)  1 byte (u8)
/// ```
///
/// Packing two fields into one word is what keeps `LaneVoucher` at 8 static
/// words instead of 9. The reserved span stays zero so a later field can claim
/// those bits without any voucher signed today becoming reinterpretable — the
/// contract rejects a non-zero reserved span rather than masking it away.
///
/// # Errors
///
/// [`ChainMeterError::PriceExceedsWireWidth`] when `chunk_price` does not fit
/// the 8 bytes the word gives it. The on-chain pool caps every USDC counter at
/// `uint64`, so such a price is unredeemable; it is refused rather than
/// truncated into a *different, cheaper* price the node would then be paid at.
pub fn pack_chain_meter(chunk_price: U256, chain_index: u8) -> Result<U256, ChainMeterError> {
    let price = u64::try_from(chunk_price).map_err(|_| ChainMeterError::PriceExceedsWireWidth)?;
    Ok((U256::from(price) << 8) | U256::from(chain_index))
}

/// Why [`pack_chain_meter`] refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChainMeterError {
    /// `chunk_price` exceeds the `uint64` the packed word gives it.
    #[error("chunk_price exceeds the uint64 width of the packed chainMeter word")]
    PriceExceedsWireWidth,
}

/// Draw the seed for one chain: 32 fresh bytes from the OS CSPRNG.
///
/// A seed is drawn when the chain opens, held in memory for as long as the
/// chain meters, and dropped with it. Two draws collide only with probability
/// 2⁻²⁵⁶, so the one-root-per-lane rule the scheme rests on holds by the draw
/// rather than by any state kept correct between draws — across a payer's
/// lanes, and across the successive chains of one lane (ADR 003 §One chain per
/// lane).
///
/// Nothing reproduces a seed, and nothing needs to. Re-opening a chain across a
/// process or device boundary is never how a payer resumes: it folds the
/// frontier the node proved into a signed amount and opens a fresh chain
/// instead, which needs the signing key and no secret at rest at all (ADR 003
/// §Resumption folds).
#[must_use]
pub fn random_seed() -> B256 {
    let mut seed = [0u8; 32];
    rand::rng().fill_bytes(&mut seed);
    B256::from(seed)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
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
}
