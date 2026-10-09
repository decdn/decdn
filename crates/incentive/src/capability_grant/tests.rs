use super::*;
use alloy::primitives::{address, b256};
use alloy::signers::local::PrivateKeySigner;
use std::assert_matches;

fn sample_domain() -> Eip712Domain {
    crate::capability::voucher_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    )
}

fn sample_capability() -> Capability {
    Capability {
        signer: address!("00000000000000000000000000000000000000a1"),
        spending_cap: 10_000_000u64,
        pool_id: b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
        expiry: 1_900_000_000,
    }
}

fn signed_grant() -> (SignedCapability, CapabilityGrant, Address) {
    let owner = PrivateKeySigner::random();
    let signed = sample_capability().sign(&owner, &sample_domain()).unwrap();
    let grant = CapabilityGrant::from_signed_capability(&signed);
    (signed, grant, owner.address())
}

#[test]
fn token_round_trip_is_lossless() {
    let (_, grant, _) = signed_grant();
    let token = grant.to_token();
    assert!(
        token.starts_with("dcap1:"),
        "token carries the prefix: {token}"
    );
    let decoded = CapabilityGrant::from_token(&token).expect("round-trips");
    assert_eq!(decoded, grant, "decode must reproduce the grant exactly");
}

#[test]
fn reconstructed_capability_recovers_the_same_owner() {
    let (signed, grant, owner) = signed_grant();
    let domain = sample_domain();
    // The token's reconstructed capability recovers to the SAME owner as the
    // original, so a node validating either sees one pool owner.
    let rebuilt = grant.to_signed_capability().expect("rebuild");
    assert_eq!(
        rebuilt, signed,
        "rebuilt SignedCapability must equal the original"
    );
    assert_eq!(rebuilt.recover_owner(&domain).unwrap(), owner);
    assert_eq!(grant.owner(&domain).unwrap(), owner);
}

/// Fixed-vector: a deterministic owner key + capability must recover a stable
/// owner through the whole token path, catching a codec drift before a node
/// does.
#[test]
fn fixed_vector_token_recovers_expected_owner() {
    let pk_hex = "2222222222222222222222222222222222222222222222222222222222222222";
    let owner: PrivateKeySigner = pk_hex.parse().unwrap();
    let domain = sample_domain();
    let signed = sample_capability().sign(&owner, &domain).unwrap();
    let grant = CapabilityGrant::from_signed_capability(&signed);
    let token = grant.to_token();
    let decoded = CapabilityGrant::from_token(&token).unwrap();
    assert_eq!(decoded.owner(&domain).unwrap(), owner.address());
}

#[test]
fn bad_prefix_is_rejected() {
    let (_, grant, _) = signed_grant();
    let body = grant.to_token();
    let no_prefix = body.trim_start_matches("dcap1:");
    assert_eq!(
        CapabilityGrant::from_token(no_prefix),
        Err(GrantError::BadPrefix)
    );
    assert_eq!(
        CapabilityGrant::from_token("dcap2:whatever"),
        Err(GrantError::BadPrefix)
    );
}

#[test]
fn truncated_body_is_rejected_without_panicking() {
    let (_, grant, _) = signed_grant();
    let token = grant.to_token();
    // Lop off the trailing half of the base64 body: still valid-ish base64url
    // characters, but a truncated postcard payload.
    let cut = token.len() - (token.len() - "dcap1:".len()) / 2;
    let truncated = &token[..cut];
    let err = CapabilityGrant::from_token(truncated)
        .expect_err("a truncated token must be rejected, not panic");
    assert_matches!(
        err,
        GrantError::BadPayload | GrantError::BadBase64,
        "unexpected error for truncated token: {err:?}"
    );
}

#[test]
fn non_base64_body_is_rejected() {
    // `*` is outside the base64url alphabet.
    assert_eq!(
        CapabilityGrant::from_token("dcap1:not*base64*"),
        Err(GrantError::BadBase64)
    );
}

#[test]
fn malformed_signature_bytes_fail_reconstruction() {
    let (_, mut grant, _) = signed_grant();
    grant.owner_signature = vec![0u8; 10]; // not 65 bytes
    assert_eq!(grant.to_signed_capability(), Err(GrantError::BadSignature));
    assert_matches!(
        grant.owner(&sample_domain()),
        Err(GrantOwnerError::Decode(GrantError::BadSignature))
    );
}
