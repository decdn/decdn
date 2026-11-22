use alloy::primitives::{Address, keccak256};
use alloy::signers::local::PrivateKeySigner;

use super::*;
use crate::capacity_bond_domain;

fn domain() -> Eip712Domain {
    capacity_bond_domain(421_614, Address::repeat_byte(0x44))
}

#[test]
fn typehash_matches_the_type_string() {
    assert_eq!(
        ORIGIN_ATTESTATION_TYPEHASH,
        keccak256(
            "OriginAttestation(bytes32 hash,uint256 namespaceId,uint64 timestampUs,uint32 ttlSec)"
        )
    );
    assert_eq!(
        sol_types::OriginAttestation::eip712_type_hash(&to_sol(
            [0; 32],
            &OriginAttestation {
                namespace_id: [0; 32],
                timestamp_us: 0,
                ttl_sec: 0,
                sig: Vec::new(),
            }
        )),
        ORIGIN_ATTESTATION_TYPEHASH
    );
}

#[test]
fn a_signed_attestation_recovers_its_signer() {
    let signer = PrivateKeySigner::random();
    let att = sign_origin_attestation(&signer, &domain(), [1; 32], [2; 32], 3, 3_600).unwrap();
    assert_eq!(
        recover_origin_attestation_signer([1; 32], &att, &domain()),
        Ok(signer.address())
    );
}

#[test]
fn every_signed_field_binds_the_signature() {
    let signer = PrivateKeySigner::random();
    let att = sign_origin_attestation(&signer, &domain(), [1; 32], [2; 32], 3, 3_600).unwrap();
    let recovers = |hash: [u8; 32], a: &OriginAttestation| {
        recover_origin_attestation_signer(hash, a, &domain()) == Ok(signer.address())
    };
    assert!(!recovers([9; 32], &att), "hash");
    assert!(
        !recovers(
            [1; 32],
            &OriginAttestation {
                namespace_id: [3; 32],
                ..att.clone()
            }
        ),
        "namespace"
    );
    assert!(
        !recovers(
            [1; 32],
            &OriginAttestation {
                timestamp_us: 4,
                ..att.clone()
            }
        ),
        "timestamp"
    );
    assert!(
        !recovers(
            [1; 32],
            &OriginAttestation {
                ttl_sec: 7_200,
                ..att.clone()
            }
        ),
        "ttl"
    );
    let other_domain = capacity_bond_domain(1, Address::repeat_byte(0x44));
    assert_ne!(
        recover_origin_attestation_signer([1; 32], &att, &other_domain),
        Ok(signer.address()),
        "domain"
    );
}

#[test]
fn a_stream_response_signature_is_no_attestation() {
    let signer = PrivateKeySigner::random();
    let body = decdn_protocol::StreamResponseBody {
        hash: [1; 32],
        ok: true,
        rate_per_mb: 1,
        timestamp_us: 3,
    };
    let sig = crate::StreamSlashData::from_response_body(&body)
        .sign(&signer, &domain())
        .unwrap();
    let att = OriginAttestation {
        namespace_id: [0; 32],
        timestamp_us: 3,
        ttl_sec: 3_600,
        sig: sig.as_bytes().to_vec(),
    };
    assert_ne!(
        recover_origin_attestation_signer([1; 32], &att, &domain()),
        Ok(signer.address())
    );
}
