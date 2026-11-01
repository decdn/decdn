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

/// A registration at or past its expiry refuses as `CapabilityExpired`, even
/// with headroom left, so `signer_cap_exhausted` counts only a cap shortfall
/// (#2631). A spent cap on a live registration stays `SignerCapExhausted`, and
/// an expired registration that is also spent reports the expiry: a larger
/// cap does not cure it.
#[test]
fn an_expired_registration_refuses_as_capability_expired() {
    let floor = U256::from(1_000u64);
    let registered = |cap, expiry, spent| SignerAuthorization::Registered { cap, expiry, spent };
    assert_eq!(
        signer_refusal(Some(registered(10_000, 100, 0)), floor, 100),
        Some(ServeRejectReason::CapabilityExpired),
        "now == expiry is expired: the chain reads `block.timestamp >= expiry`"
    );
    assert_eq!(
        signer_refusal(Some(registered(10_000, 100, 0)), floor, 101),
        Some(ServeRejectReason::CapabilityExpired)
    );
    assert_eq!(
        signer_refusal(Some(registered(1_000, 100, 500)), floor, 100),
        Some(ServeRejectReason::CapabilityExpired)
    );
    assert_eq!(
        signer_refusal(Some(registered(1_000, 100, 500)), floor, 99),
        Some(ServeRejectReason::SignerCapExhausted)
    );
    assert_eq!(
        signer_refusal(Some(registered(10_000, 100, 0)), floor, 99),
        None
    );
}
