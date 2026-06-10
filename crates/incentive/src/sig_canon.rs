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
pub(crate) fn is_high_s(sig: &Signature) -> bool {
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
mod tests {
    use super::*;
    use alloy::primitives::U256;

    #[test]
    fn low_s_is_canonical() {
        // s = 1 is well below n/2.
        let sig = Signature::new(U256::from(1u8), U256::from(1u8), false);
        assert!(!is_high_s(&sig));
    }

    #[test]
    fn high_s_is_detected() {
        // s = n - 1 is the maximal high-s value.
        let sig = Signature::new(U256::from(1u8), SECP256K1N - U256::from(1u8), false);
        assert!(is_high_s(&sig));
    }

    #[test]
    fn high_s_twin_is_high_and_recovers_same_signer() -> anyhow::Result<()> {
        use alloy::primitives::B256;
        use alloy::signers::SignerSync;
        use alloy::signers::local::PrivateKeySigner;

        // Anchors the `high_s_twin` helper that every verification-site test
        // depends on: the twin must be canonically *high* yet recover the
        // *same* signer as the low-`s` original. Without this, a subtly-broken
        // twin (wrong `v`-flip / off-by-one on `n - s`) would still make the
        // site tests pass green — but for the wrong reason.
        let signer = PrivateKeySigner::random();
        let hash = B256::repeat_byte(0x42);
        let sig = signer.sign_hash_sync(&hash)?;
        assert!(!is_high_s(&sig), "k256 signer must emit low-s");

        let twin = high_s_twin(&sig);
        assert!(is_high_s(&twin), "twin must be high-s");
        anyhow::ensure!(
            sig.recover_address_from_prehash(&hash)? == signer.address(),
            "sanity: original recovers the signer"
        );
        anyhow::ensure!(
            twin.recover_address_from_prehash(&hash)? == signer.address(),
            "twin must recover the SAME signer (proves canonicalization, not garbage)"
        );
        Ok(())
    }

    #[test]
    fn half_order_boundary_is_canonical() {
        // s == n/2 is the largest still-canonical value (strictly-greater test).
        let half = SECP256K1N >> 1;
        assert!(!is_high_s(&Signature::new(U256::from(1u8), half, false)));
        assert!(is_high_s(&Signature::new(
            U256::from(1u8),
            half + U256::from(1u8),
            false
        )));
    }
}
