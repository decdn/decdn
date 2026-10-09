use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::voucher_domain;

use super::*;

fn issuer(max_cap: u64, max_ttl: u64) -> (Issuer, Address, Eip712Domain) {
    let signer = PrivateKeySigner::random();
    let owner = signer.address();
    let domain = voucher_domain(421_614, Address::repeat_byte(0x22));
    let issuer = Issuer::new(
        signer,
        domain.clone(),
        B256::repeat_byte(0x11),
        Limits {
            max_spending_cap: MicroUsdc(max_cap),
            max_ttl_secs: max_ttl,
        },
    );
    (issuer, owner, domain)
}

fn req(cap: Option<u64>, ttl: Option<u64>) -> TermsRequest {
    TermsRequest {
        spending_cap: cap.map(MicroUsdc),
        ttl_secs: ttl,
    }
}

#[tokio::test]
async fn signed_token_decodes_and_recovers_owner_and_fields() {
    let (issuer, owner, domain) = issuer(10_000_000, 2_592_000);
    let delegate = Address::repeat_byte(0xaa);
    let signed = issuer
        .sign(delegate, MicroUsdc(7_000_000), 1_769_904_000)
        .await
        .unwrap();

    assert!(signed.token.starts_with("dcap1:"));
    assert_eq!(signed.spending_cap, MicroUsdc(7_000_000));
    assert_eq!(signed.expiry, 1_769_904_000);

    let grant = CapabilityGrant::from_token(&signed.token).unwrap();
    assert_eq!(grant.signer, delegate);
    assert_eq!(grant.pool_id, B256::repeat_byte(0x11));
    assert_eq!(grant.spending_cap, 7_000_000);
    assert_eq!(grant.expiry, 1_769_904_000);
    assert_eq!(grant.owner(&domain).unwrap(), owner);
}

#[tokio::test]
async fn local_signing_is_deterministic() {
    let (issuer, _, _) = issuer(10_000_000, 2_592_000);
    let delegate = Address::repeat_byte(0xaa);
    let a = issuer
        .sign(delegate, MicroUsdc(5_000_000), 1_769_904_000)
        .await
        .unwrap();
    let b = issuer
        .sign(delegate, MicroUsdc(5_000_000), 1_769_904_000)
        .await
        .unwrap();
    assert_eq!(a, b);
}

#[test]
fn terms_default_to_max_and_honour_lower_values() {
    let (issuer, _, _) = issuer(5_000_000, 172_800);
    let terms = |cap, ttl| issuer.terms(&req(cap, ttl));
    let t = |cap, ttl_secs| Terms {
        spending_cap: MicroUsdc(cap),
        ttl_secs,
    };
    assert_eq!(terms(None, None), Ok(t(5_000_000, 172_800)));
    assert_eq!(terms(Some(1), Some(60)), Ok(t(1, 60)));
    assert_eq!(
        terms(Some(5_000_000), Some(172_800)),
        Ok(t(5_000_000, 172_800))
    );
}

#[test]
fn terms_reject_zero_and_above_max() {
    let (issuer, _, _) = issuer(5_000_000, 172_800);
    let terms = |cap, ttl| issuer.terms(&req(cap, ttl));
    assert_eq!(
        terms(Some(5_000_001), None),
        Err(TermsError::ExceedsMax {
            field: "spending_cap",
            requested: 5_000_001,
            max: 5_000_000
        })
    );
    assert_eq!(
        terms(None, Some(172_801)),
        Err(TermsError::ExceedsMax {
            field: "ttl_secs",
            requested: 172_801,
            max: 172_800
        })
    );
    assert_eq!(
        terms(Some(0), None),
        Err(TermsError::Zero {
            field: "spending_cap"
        })
    );
    assert_eq!(
        terms(None, Some(0)),
        Err(TermsError::Zero { field: "ttl_secs" })
    );
}
