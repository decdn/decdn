use super::*;
use crate::capacity_bond_domain;
use alloy::primitives::address;
use alloy::signers::local::PrivateKeySigner;

fn sample_data() -> StreamSigData {
    StreamSigData {
        hash: B256::repeat_byte(0x7A),
        ok: true,
        rate_per_mb: 10_000,
        timestamp_us: 1_700_000_000_000_000,
    }
}

fn sample_domain() -> Eip712Domain {
    // Arbitrum Sepolia chain id; address is a deterministic test fixture.
    capacity_bond_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    )
}

fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
    r.err()
        .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
}

#[test]
fn round_trip_sign_verify() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let address = signer.address();
    let domain = sample_domain();
    let data = sample_data();

    let sig = data.sign(&signer, &domain)?;
    data.verify_signer(&sig, address, &domain)?;
    Ok(())
}

#[test]
fn high_s_stream_signature_rejected() -> anyhow::Result<()> {
    // Reject the high-`s` twin so each response has one accepted signature
    // encoding (#836).
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let data = sample_data();
    let sig = data.sign(&signer, &domain)?;
    data.verify_signer(&sig, signer.address(), &domain)?; // low-s ok

    let twin = crate::sig_canon::high_s_twin(&sig);
    anyhow::ensure!(
        data.recover_signer(&twin, &domain) == Err(StreamSigError::InvalidSignature),
        "high-s stream signature twin must be rejected as InvalidSignature"
    );
    Ok(())
}

#[test]
fn wrong_signer_address_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let other = PrivateKeySigner::random().address();
    let domain = sample_domain();
    let data = sample_data();

    let sig = data.sign(&signer, &domain)?;
    let err = err_of(data.verify_signer(&sig, other, &domain))?;
    anyhow::ensure!(
        matches!(err, StreamSigError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn different_chain_id_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain_a = capacity_bond_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    );
    let domain_b = capacity_bond_domain(1, address!("0000000000000000000000000000000000001234"));
    let data = sample_data();

    let sig = data.sign(&signer, &domain_a)?;
    let err = err_of(data.verify_signer(&sig, signer.address(), &domain_b))?;
    anyhow::ensure!(
        matches!(err, StreamSigError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn different_verifying_contract_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain_a = capacity_bond_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    );
    let domain_b = capacity_bond_domain(
        421_614,
        address!("0000000000000000000000000000000000005678"),
    );
    let data = sample_data();

    let sig = data.sign(&signer, &domain_a)?;
    let err = err_of(data.verify_signer(&sig, signer.address(), &domain_b))?;
    anyhow::ensure!(
        matches!(err, StreamSigError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

/// Each signed field must be covered: tampering any one must flip the
/// recovered signer.
#[test]
fn tampered_fields_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let data = sample_data();
    let sig = data.sign(&signer, &domain)?;

    let mutations: [StreamSigData; 4] = [
        StreamSigData {
            hash: B256::ZERO,
            ..data
        },
        StreamSigData { ok: false, ..data },
        StreamSigData {
            rate_per_mb: data.rate_per_mb + 1,
            ..data
        },
        StreamSigData {
            timestamp_us: data.timestamp_us + 1,
            ..data
        },
    ];
    for (i, m) in mutations.into_iter().enumerate() {
        let err = err_of(m.verify_signer(&sig, signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, StreamSigError::WrongSigner { .. }),
            "mutation {i} should flip the signer, got {err:?}"
        );
    }
    Ok(())
}

/// Lock the EIP-712 type hash to the exact ADR 005 wording and to the
/// pinned [`STREAM_RESPONSE_TYPEHASH`] digest. If this breaks, either the ADR
/// changed or the `sol!` macro's canonical encoding shifted — both break
/// every verifier that runs the other version.
#[test]
fn stream_response_type_hash_matches_adr_005() -> anyhow::Result<()> {
    use alloy::primitives::keccak256;
    let canonical: &[u8] =
        b"StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 timestampUs)";
    anyhow::ensure!(
        keccak256(canonical) == STREAM_RESPONSE_TYPEHASH,
        "pinned STREAM_RESPONSE_TYPEHASH does not match the ADR 005 type string"
    );
    let actual = StreamResponseSol::eip712_type_hash(&sample_data().to_sol());
    anyhow::ensure!(
        actual == STREAM_RESPONSE_TYPEHASH,
        "StreamResponse type hash drifted: actual={actual} expected={STREAM_RESPONSE_TYPEHASH}"
    );
    Ok(())
}

/// Independently reconstruct the EIP-712 signing digest from the canonical
/// `0x1901 || domainSeparator || hashStruct` preimage and pin it against
/// `signing_hash`. Unlike a self-consistent sign→recover, this proves the
/// struct field encoding and domain binding match the canonical EIP-712
/// computation — a real drift guard, not a tautology.
#[test]
fn signing_hash_matches_eip712_canonical() -> anyhow::Result<()> {
    use alloy::primitives::keccak256;
    use alloy::sol_types::SolValue;

    let data = sample_data();
    let domain = sample_domain();

    // EIP-712 encodeData: each field as a 32-byte ABI word, prefixed by the
    // pinned type hash. All fields here are static, so abi_encode of the
    // tuple is exactly 5 × 32 bytes.
    let struct_hash = keccak256(
        (
            STREAM_RESPONSE_TYPEHASH,
            data.hash,
            data.ok,
            data.rate_per_mb,
            data.timestamp_us,
        )
            .abi_encode(),
    );
    let mut preimage = Vec::with_capacity(2 + 32 + 32);
    preimage.extend_from_slice(&[0x19, 0x01]);
    preimage.extend_from_slice(domain.separator().as_slice());
    preimage.extend_from_slice(struct_hash.as_slice());
    let expected = keccak256(&preimage);

    anyhow::ensure!(
        data.signing_hash(&domain) == expected,
        "signing_hash drifted from canonical EIP-712: actual={} expected={expected}",
        data.signing_hash(&domain)
    );
    Ok(())
}

/// `from_response_body` maps the wire body into the signed-field view.
#[test]
fn from_response_body_maps_fields() {
    use decdn_protocol::StreamResponseBody;

    let body = StreamResponseBody {
        hash: [0x7Au8; 32],
        ok: true,
        rate_per_mb: 10_000,
        timestamp_us: 1_700_000_000_000_000,
    };
    assert_eq!(StreamSigData::from_response_body(&body), sample_data());
}

/// Lock the `CapacityBond` domain separator to the canonical EIP-712 form.
#[test]
fn domain_separator_matches_eip712_canonical() -> anyhow::Result<()> {
    use alloy::primitives::{U256, keccak256};
    use alloy::sol_types::SolValue;

    let domain_typehash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let chain_id: u64 = 421_614;
    let verifying = address!("0000000000000000000000000000000000001234");

    let domain = capacity_bond_domain(chain_id, verifying);
    let actual = domain.separator();

    let expected = keccak256(
        (
            domain_typehash,
            keccak256(crate::CAPACITY_BOND_DOMAIN_NAME.as_bytes()),
            keccak256(crate::CAPACITY_BOND_DOMAIN_VERSION.as_bytes()),
            U256::from(chain_id),
            verifying,
        )
            .abi_encode(),
    );
    anyhow::ensure!(
        actual == expected,
        "domain separator drift: actual={actual} expected={expected}"
    );
    Ok(())
}
