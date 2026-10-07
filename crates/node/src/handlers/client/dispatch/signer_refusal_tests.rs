use super::{ServeRejectReason, SignerAuthorization, U256, signer_refusal};

/// A `getAuthorization` fault with no earlier read refuses as an unconfirmed
/// pool, so `signer_cap_exhausted` counts only signers whose cap is spent
/// (#2220).
#[test]
fn an_unconfirmed_signer_is_not_counted_as_cap_exhausted() {
    let floor = U256::from(1_000u64);
    assert_eq!(
        signer_refusal(None, floor, 0),
        Some(ServeRejectReason::PoolUnconfirmed)
    );
    let spent = SignerAuthorization::Registered {
        cap: 1_000,
        expiry: u64::MAX,
        spent: 500,
    };
    assert_eq!(
        signer_refusal(Some(spent), floor, 0),
        Some(ServeRejectReason::SignerCapExhausted)
    );
    let live = SignerAuthorization::Registered {
        cap: 10_000,
        expiry: u64::MAX,
        spent: 500,
    };
    assert_eq!(signer_refusal(Some(live), floor, 0), None);
    assert_eq!(
        signer_refusal(Some(SignerAuthorization::Unregistered), floor, 0),
        None
    );
}
