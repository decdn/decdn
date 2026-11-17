use super::*;

use alloy::signers::local::PrivateKeySigner;

fn domain() -> Eip712Domain {
    scope_domain(421_614, Address::repeat_byte(0x35))
}

#[test]
fn a_scope_recovers_the_owner_that_signed_it() {
    let owner = PrivateKeySigner::random();
    let signer = Address::repeat_byte(0xab);
    let pool = B256::repeat_byte(1);
    let scope = SignedScope::sign(&owner, signer, pool, vec![7, 9], &domain()).unwrap();
    assert_eq!(
        scope.owner(signer, pool, &domain()).unwrap(),
        owner.address()
    );
}

#[test]
fn a_scope_binds_its_key_its_pool_its_namespaces_and_its_deployment() {
    let owner = PrivateKeySigner::random();
    let signer = Address::repeat_byte(0xab);
    let pool = B256::repeat_byte(1);
    let scope = SignedScope::sign(&owner, signer, pool, vec![7], &domain()).unwrap();
    let other_key = scope.owner(Address::repeat_byte(0xcd), pool, &domain());
    let other_pool = scope.owner(signer, B256::repeat_byte(2), &domain());
    let other_deployment = scope.owner(signer, pool, &scope_domain(1, Address::repeat_byte(0x35)));
    let mut widened = scope.clone();
    widened.namespaces.push(8);
    for recovered in [
        other_key,
        other_pool,
        other_deployment,
        widened.owner(signer, pool, &domain()),
    ] {
        assert_ne!(recovered.ok(), Some(owner.address()));
    }
}

#[test]
fn a_malformed_signature_is_refused() {
    let scope = SignedScope {
        namespaces: vec![7],
        signature: Bytes::from(vec![0u8; 12]),
    };
    assert_eq!(
        scope.owner(Address::ZERO, B256::ZERO, &domain()),
        Err(MalformedSignature)
    );
}

#[test]
fn a_registration_scope_is_any_or_a_signed_scope() {
    assert_eq!(
        serde_json::from_str::<Scope>(r#""any""#).unwrap(),
        Scope::Any
    );
    let signed: Scope = serde_json::from_str(r#"{"namespaces":[7],"signature":"0x01"}"#).unwrap();
    assert!(matches!(signed, Scope::Namespaces(ref s) if s.namespaces == [7]));
    for refused in [r#""all""#, r#""*""#, "null", "[]", r#"{"namespaces":[7]}"#] {
        assert!(serde_json::from_str::<Scope>(refused).is_err(), "{refused}");
    }
    assert_eq!(serde_json::to_string(&Scope::Any).unwrap(), r#""any""#);
    assert_eq!(
        serde_json::to_value(&signed).unwrap(),
        serde_json::json!({"namespaces": [7], "signature": "0x01"})
    );
}

#[test]
fn a_scope_reads_its_json_shape() {
    let scope: SignedScope =
        serde_json::from_str(r#"{"namespaces":[7,9],"signature":"0x01"}"#).unwrap();
    assert_eq!(scope.namespaces, vec![7, 9]);
    assert!(
        serde_json::from_str::<SignedScope>(r#"{"namespaces":[7],"signature":"0x01","x":1}"#)
            .is_err()
    );
}
