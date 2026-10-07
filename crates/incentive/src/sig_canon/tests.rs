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
