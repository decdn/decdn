use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::CapabilityGrant;
use sponsord_api::MicroUsdc;

use super::*;
use crate::pool::Authorization;
use crate::test_support::{FakePool, TEST_CHAIN_ID, TEST_PAYMENT_POOL, TEST_POOL_ID, fake_sponsor};

const NOW: u64 = 1_769_904_000;
const SIGNER: Address = Address::repeat_byte(0xaa);

fn req(cap: Option<u64>, ttl: Option<u64>) -> TermsRequest {
    TermsRequest {
        spending_cap: cap.map(MicroUsdc),
        ttl_secs: ttl,
    }
}

fn auth(cap: u64, expiry: u64) -> Authorization {
    Authorization {
        spending_cap: MicroUsdc(cap),
        expiry,
    }
}

#[tokio::test]
async fn new_rejects_a_pool_owned_by_another_key() {
    let issuer = Issuer::new(
        PrivateKeySigner::random(),
        voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL),
        TEST_POOL_ID,
        Limits {
            max_spending_cap: MicroUsdc(5_000_000),
            max_ttl_secs: 172_800,
        },
    );
    let pool = Arc::new(FakePool::new(Address::repeat_byte(0x99), 0));
    let err = Sponsor::new(issuer, pool, TEST_CHAIN_ID, TEST_PAYMENT_POOL)
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("owned on-chain by"), "{err}");
}

#[tokio::test]
async fn unregistered_signer_gets_requested_terms() {
    let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
    let issued = sponsor
        .issue(SIGNER, &req(Some(1_000_000), Some(3_600)), NOW)
        .await
        .unwrap();
    assert!(!issued.registered);
    assert_eq!(issued.capability.spending_cap, MicroUsdc(1_000_000));
    assert_eq!(issued.capability.expiry, NOW + 3_600);
    let grant = CapabilityGrant::from_token(&issued.capability.token).unwrap();
    assert_eq!(grant.signer, SIGNER);
    assert_eq!(grant.spending_cap, 1_000_000);
    assert_eq!(
        grant
            .owner(&voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL))
            .unwrap(),
        pool.owner_address()
    );
}

#[tokio::test]
async fn omitted_terms_default_to_the_maximum() {
    let (sponsor, _) = fake_sponsor(5_000_000, 172_800).await;
    let issued = sponsor.issue(SIGNER, &req(None, None), NOW).await.unwrap();
    assert_eq!(issued.capability.spending_cap, MicroUsdc(5_000_000));
    assert_eq!(issued.capability.expiry, NOW + 172_800);
}

#[tokio::test]
async fn registered_signer_gets_its_existing_capability_back() {
    let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
    let first = sponsor
        .issue(SIGNER, &req(Some(2_000_000), None), NOW)
        .await
        .unwrap()
        .capability;
    pool.register(SIGNER, auth(first.spending_cap.0, first.expiry));

    // Asking again, for other terms, later: the registered terms win and
    // the token is byte-identical to the one first issued.
    let again = sponsor
        .issue(SIGNER, &req(Some(4_000_000), Some(60)), NOW + 100)
        .await
        .unwrap();
    assert!(again.registered);
    assert_eq!(again.capability, first);
}

#[tokio::test]
async fn expired_registration_is_an_error() {
    let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
    pool.register(SIGNER, auth(5_000_000, NOW));
    // expiry == now counts as expired.
    let err = sponsor
        .issue(SIGNER, &req(None, None), NOW)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, SponsorError::SignerExpired { expiry } if expiry == NOW));
}

#[tokio::test]
async fn bad_terms_are_rejected_even_for_a_registered_signer() {
    let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
    pool.register(SIGNER, auth(5_000_000, NOW + 1_000));
    let err = sponsor
        .issue(SIGNER, &req(Some(5_000_001), None), NOW)
        .await
        .err()
        .unwrap();
    assert!(matches!(
        err,
        SponsorError::Terms(TermsError::ExceedsMax { .. })
    ));
    let err = sponsor
        .issue(SIGNER, &req(Some(0), None), NOW)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, SponsorError::Terms(TermsError::Zero { .. })));
}

#[tokio::test]
async fn failed_registration_read_is_a_chain_error() {
    let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
    pool.fail_authorization_reads(true);
    let err = sponsor
        .issue(SIGNER, &req(None, None), NOW)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, SponsorError::Chain(_)));
}

#[tokio::test]
async fn info_reports_chain_and_maximums() {
    let (sponsor, _) = fake_sponsor(5_000_000, 172_800).await;
    assert_eq!(
        sponsor.info(),
        Info {
            chain_id: TEST_CHAIN_ID,
            payment_pool: TEST_PAYMENT_POOL,
            max_spending_cap: MicroUsdc(5_000_000),
            max_ttl_secs: 172_800,
        }
    );
}
