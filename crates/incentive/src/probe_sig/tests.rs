use super::*;
use alloy::primitives::address;
use alloy::signers::local::PrivateKeySigner;

fn sample_data() -> ProbeSlashData {
    ProbeSlashData {
        hash: B256::repeat_byte(0x7A),
        has_blob: true,
        rate_per_mb: 10_000,
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
fn wrong_signer_address_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let other = PrivateKeySigner::random().address();
    let domain = sample_domain();
    let data = sample_data();

    let sig = data.sign(&signer, &domain)?;
    let err = err_of(data.verify_signer(&sig, other, &domain))?;
    anyhow::ensure!(
        matches!(err, ProbeSlashError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn high_s_probe_slash_signature_rejected() -> anyhow::Result<()> {
    // High-`s` slash evidence verifies off-chain but fails at the on-chain
    // `SlashJudge`; reject it so collected evidence stays settleable (#836).
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let data = sample_data();
    let sig = data.sign(&signer, &domain)?;
    data.verify_signer(&sig, signer.address(), &domain)?; // low-s ok

    let twin = crate::sig_canon::high_s_twin(&sig);
    anyhow::ensure!(
        data.recover_signer(&twin, &domain) == Err(ProbeSlashError::InvalidSignature),
        "high-s probe slash twin must be rejected as InvalidSignature"
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
        matches!(err, ProbeSlashError::WrongSigner { .. }),
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
        matches!(err, ProbeSlashError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn tampered_hash_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let data = sample_data();

    let sig = data.sign(&signer, &domain)?;
    let tampered = ProbeSlashData {
        hash: B256::ZERO,
        ..data
    };
    let err = err_of(tampered.verify_signer(&sig, signer.address(), &domain))?;
    anyhow::ensure!(
        matches!(err, ProbeSlashError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn tampered_has_blob_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let data = sample_data();

    let sig = data.sign(&signer, &domain)?;
    let tampered = ProbeSlashData {
        has_blob: false,
        ..data
    };
    let err = err_of(tampered.verify_signer(&sig, signer.address(), &domain))?;
    anyhow::ensure!(
        matches!(err, ProbeSlashError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn tampered_rate_per_mb_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let data = sample_data();

    let sig = data.sign(&signer, &domain)?;
    let tampered = ProbeSlashData {
        rate_per_mb: data.rate_per_mb + 1,
        ..data
    };
    let err = err_of(tampered.verify_signer(&sig, signer.address(), &domain))?;
    anyhow::ensure!(
        matches!(err, ProbeSlashError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn tampered_timestamp_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let data = sample_data();

    let sig = data.sign(&signer, &domain)?;
    let tampered = ProbeSlashData {
        timestamp_us: data.timestamp_us + 1,
        ..data
    };
    let err = err_of(tampered.verify_signer(&sig, signer.address(), &domain))?;
    anyhow::ensure!(
        matches!(err, ProbeSlashError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

/// Lock the EIP-712 type hash to the exact ADR 014 wording and to the
/// pinned [`PROBE_RESPONSE_TYPEHASH`] digest the Solidity suite also
/// asserts. If this breaks, either the ADR changed or the `sol!` macro's
/// canonical encoding shifted — both warrant a coordinated update with the
/// `SlashJudge` contract and its deployment manifests.
#[test]
fn probe_response_type_hash_matches_adr_014() -> anyhow::Result<()> {
    use alloy::primitives::keccak256;
    let canonical: &[u8] =
        b"ProbeResponse(bytes32 hash,bool hasBlob,uint64 ratePerMb,uint64 timestampUs)";
    anyhow::ensure!(
        keccak256(canonical) == PROBE_RESPONSE_TYPEHASH,
        "pinned PROBE_RESPONSE_TYPEHASH does not match the ADR 014 type string"
    );
    let actual = ProbeResponseSol::eip712_type_hash(&sample_data().to_sol());
    anyhow::ensure!(
        actual == PROBE_RESPONSE_TYPEHASH,
        "ProbeResponse type hash drifted: actual={actual} expected={PROBE_RESPONSE_TYPEHASH}"
    );
    Ok(())
}

/// Lock the domain separator to the canonical EIP-712 form.
#[test]
fn domain_separator_matches_eip712_canonical() -> anyhow::Result<()> {
    use alloy::primitives::{U256, keccak256};
    use alloy::sol_types::SolValue;

    let domain_typehash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let chain_id: u64 = 421_614;
    let verifying = address!("0000000000000000000000000000000000001234");

    let domain = slash_judge_domain(chain_id, verifying);
    let actual = domain.separator();

    let expected = keccak256(
        (
            domain_typehash,
            keccak256(SLASH_JUDGE_DOMAIN_NAME.as_bytes()),
            keccak256(SLASH_JUDGE_DOMAIN_VERSION.as_bytes()),
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

/// Fixed-vector regression: a deterministic key + data must recover a
/// specific address. Catches digest drift before the contract does.
#[test]
fn fixed_vector_recovers_expected_signer() -> anyhow::Result<()> {
    let pk_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    let signer: PrivateKeySigner = pk_hex
        .parse()
        .map_err(|e| anyhow::anyhow!("parse test key: {e}"))?;
    let address = signer.address();

    let data = ProbeSlashData {
        hash: B256::repeat_byte(0xAA),
        has_blob: true,
        rate_per_mb: 1_000_000,
        timestamp_us: 1_700_000_000_000_000,
    };
    let domain = slash_judge_domain(
        421_614,
        address!("00000000000000000000000000000000deadbeef"),
    );

    // Pin the EIP-712 signing digest itself, not just self-consistent
    // sign→recover. Recovery alone passes even if the struct-hash/value
    // encoding drifts (it only proves the sig matches whatever digest
    // this code produced); a fixed digest locks on-chain `SlashJudge`
    // compatibility.
    let expected_hash = alloy::primitives::b256!(
        "67405dde5244ffc7846bda4886ef62ef29bcbb8ab34723314d63df181eabb3d0"
    );
    anyhow::ensure!(
        data.signing_hash(&domain) == expected_hash,
        "signing_hash drifted: actual={} expected={expected_hash}",
        data.signing_hash(&domain)
    );

    let sig = data.sign(&signer, &domain)?;
    let recovered = data.recover_signer(&sig, &domain)?;
    anyhow::ensure!(
        recovered == address,
        "recovered {recovered}, expected {address}"
    );
    Ok(())
}
