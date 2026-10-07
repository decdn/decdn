//! Canonical-`s` enforcement for off-chain ECDSA signature verification (#836).
//!
//! On-chain `OpenZeppelin` `ECDSA.tryRecover` rejects high-`s` signatures with
//! `InvalidSignatureS`; alloy's `recover_address_from_prehash` silently
//! normalizes and accepts them. That asymmetry lets a signer produce a voucher
//! (or slash signature) that verifies off-chain but reverts on-chain, leaving
//! accrued value unsettleable. We reject high-`s` at acceptance time so the
//! off-chain accept-set matches the on-chain verifiable-set.

use alloy::primitives::Signature;

/// `true` when `s` is non-canonical (high-`s`) and would revert on-chain.
///
/// [`Signature::normalize_s`] returns `Some(_)` exactly when `s` exceeds
/// `secp256k1n / 2`, i.e. the signature is in malleable high-`s` form.
///
/// Note this is strictly stronger than "the top bit of `s` is set": `n / 2` is
/// itself below `2^255`, so roughly `2^128` values carry a clear top bit and are
/// still high-`s`. A caller folding a recovery bit into that top bit must check
/// canonicality here rather than infer it from the bit being free.
pub fn is_high_s(sig: &Signature) -> bool {
    sig.normalize_s().is_some()
}

/// secp256k1 group order `n`. Test-only; used to build malleable high-`s`
/// twins of valid signatures across the crate's verification-site tests.
#[cfg(test)]
pub(crate) const SECP256K1N: alloy::primitives::U256 = alloy::primitives::U256::from_be_bytes([
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
    0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36, 0x41, 0x41,
]);

/// The malleable high-`s` twin of a valid low-`s` signature: `(r, n - s, !v)`.
///
/// Both signatures recover the *same* signer, so asserting the twin is rejected
/// proves the rejection is specifically about canonicalization (#836), not a
/// wrong signer. Test-only.
#[cfg(test)]
pub(crate) fn high_s_twin(sig: &Signature) -> Signature {
    debug_assert!(!is_high_s(sig), "twin helper expects a canonical low-s sig");
    Signature::new(sig.r(), SECP256K1N - sig.s(), !sig.v())
}

#[cfg(test)]
mod tests;
