use super::*;
use crate::slash_judge_domain;
use alloy::primitives::address;
use alloy::signers::local::PrivateKeySigner;

fn sample_data() -> StreamSlashData {
    StreamSlashData {
        hash: B256::repeat_byte(0x7A),
        ok: true,
        rate_per_mb: 10_000,
        total_bytes: 1_048_576,
        pool_id: B256::repeat_byte(0x33),
        timestamp_us: 1_700_000_000_000_000,
    }
}

fn sample_domain() -> Eip712Domain {
    // Arbitrum Sepolia chain id; address is a deterministic test fixture.
    slash_judge_domain(
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
fn high_s_stream_slash_signature_rejected() -> anyhow::Result<()> {
    // High-`s` slash evidence verifies off-chain but fails at the on-chain
    // `SlashJudge`; reject it so collected evidence stays settleable (#836).
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let data = sample_data();
    let sig = data.sign(&signer, &domain)?;
    data.verify_signer(&sig, signer.address(), &domain)?; // low-s ok

    let twin = crate::sig_canon::high_s_twin(&sig);
    anyhow::ensure!(
        data.recover_signer(&twin, &domain) == Err(StreamSlashError::InvalidSignature),
        "high-s stream slash twin must be rejected as InvalidSignature"
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
        matches!(err, StreamSlashError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn different_chain_id_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain_a = slash_judge_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    );
    let domain_b = slash_judge_domain(1, address!("0000000000000000000000000000000000001234"));
    let data = sample_data();

    let sig = data.sign(&signer, &domain_a)?;
    let err = err_of(data.verify_signer(&sig, signer.address(), &domain_b))?;
    anyhow::ensure!(
        matches!(err, StreamSlashError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn different_verifying_contract_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain_a = slash_judge_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    );
    let domain_b = slash_judge_domain(
        421_614,
        address!("0000000000000000000000000000000000005678"),
    );
    let data = sample_data();

    let sig = data.sign(&signer, &domain_a)?;
    let err = err_of(data.verify_signer(&sig, signer.address(), &domain_b))?;
    anyhow::ensure!(
        matches!(err, StreamSlashError::WrongSigner { .. }),
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

    let mutations: [StreamSlashData; 6] = [
        StreamSlashData {
            hash: B256::ZERO,
            ..data
        },
        StreamSlashData { ok: false, ..data },
        StreamSlashData {
            rate_per_mb: data.rate_per_mb + 1,
            ..data
        },
        StreamSlashData {
            total_bytes: data.total_bytes + 1,
            ..data
        },
        StreamSlashData {
            pool_id: B256::ZERO,
            ..data
        },
        StreamSlashData {
            timestamp_us: data.timestamp_us + 1,
            ..data
        },
    ];
    for (i, m) in mutations.into_iter().enumerate() {
        let err = err_of(m.verify_signer(&sig, signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, StreamSlashError::WrongSigner { .. }),
            "mutation {i} should flip the signer, got {err:?}"
        );
    }
    Ok(())
}

/// Lock the EIP-712 type hash to the exact ADR 014 wording and to the
/// pinned [`STREAM_RESPONSE_TYPEHASH`] digest the Solidity suite also
/// asserts. If this breaks, either the ADR changed or the `sol!` macro's
/// canonical encoding shifted — both warrant a coordinated update with the
/// `SlashJudge` contract and its deployment manifests.
#[test]
fn stream_response_type_hash_matches_adr_014() -> anyhow::Result<()> {
    use alloy::primitives::keccak256;
    let canonical: &[u8] = b"StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,bytes32 poolId,uint64 timestampUs)";
    anyhow::ensure!(
        keccak256(canonical) == STREAM_RESPONSE_TYPEHASH,
        "pinned STREAM_RESPONSE_TYPEHASH does not match the ADR 014 type string"
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
/// struct field encoding and domain binding match the on-chain `SlashJudge`
/// computation — a real drift guard, not a tautology.
#[test]
fn signing_hash_matches_eip712_canonical() -> anyhow::Result<()> {
    use alloy::primitives::keccak256;
    use alloy::sol_types::SolValue;

    let data = sample_data();
    let domain = sample_domain();

    // EIP-712 encodeData: each field as a 32-byte ABI word, prefixed by the
    // pinned type hash. All fields here are static, so abi_encode of the
    // tuple is exactly 7 × 32 bytes.
    let struct_hash = keccak256(
        (
            STREAM_RESPONSE_TYPEHASH,
            data.hash,
            data.ok,
            data.rate_per_mb,
            data.total_bytes,
            data.pool_id,
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
        total_bytes: 1_048_576,
        pool_id: [0x33u8; 32],
        timestamp_us: 1_700_000_000_000_000,
    };
    assert_eq!(StreamSlashData::from_response_body(&body), sample_data());
}
