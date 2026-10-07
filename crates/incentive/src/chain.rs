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
mod tests;
