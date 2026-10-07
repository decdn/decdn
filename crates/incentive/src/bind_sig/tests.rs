use super::*;
use alloy::primitives::address;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;

fn sample_domain() -> Eip712Domain {
    bind_node_id_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    )
}

fn sign_binding(
    signer: &PrivateKeySigner,
    node_id: B256,
    nonce: u64,
    domain: &Eip712Domain,
) -> anyhow::Result<Vec<u8>> {
    let hash = binding_signing_hash(node_id, nonce, domain);
    Ok(signer.sign_hash_sync(&hash)?.as_bytes().to_vec())
}

#[test]
fn round_trip_recovers_signer() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let node_id = B256::repeat_byte(0xAB);

    let sig = sign_binding(&signer, node_id, EPHEMERAL_BINDING_NONCE, &domain)?;
    let recovered = verify_binding(node_id, EPHEMERAL_BINDING_NONCE, &sig, &domain)?;
    anyhow::ensure!(recovered == signer.address(), "recovered {recovered}");
    Ok(())
}

/// Lock the `RegisterNode` EIP-712 type hash to the contract's
/// `REGISTER_NODE_TYPEHASH` (ADR 019 § Terms Acceptance / ADR 003 § Binding
/// Message Format). A drift here means the off-chain registration signature
/// no longer verifies on-chain.
#[test]
fn register_node_type_hash_matches_contract() {
    use alloy::primitives::keccak256;
    let canonical: &[u8] = b"RegisterNode(bytes32 nodeId,uint64 nonce,bytes32 termsHash)";
    assert_eq!(
        keccak256(RegisterNodeSol::eip712_root_type().as_bytes()),
        keccak256(canonical),
        "RegisterNode type hash drifted from CapacityBond.REGISTER_NODE_TYPEHASH"
    );
}

#[test]
fn high_s_binding_signature_rejected() -> anyhow::Result<()> {
    // The high-`s` twin of a valid binding recovers the same signer
    // off-chain but reverts on-chain; reject it to keep the accept-set
    // aligned with the on-chain verifiable-set (#836).
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let node_id = B256::repeat_byte(0xAB);
    let hash = binding_signing_hash(node_id, EPHEMERAL_BINDING_NONCE, &domain);
    let sig = signer.sign_hash_sync(&hash)?;
    verify_binding(node_id, EPHEMERAL_BINDING_NONCE, &sig.as_bytes(), &domain)?; // low-s ok

    let twin = crate::sig_canon::high_s_twin(&sig).as_bytes().to_vec();
    anyhow::ensure!(
        verify_binding(node_id, EPHEMERAL_BINDING_NONCE, &twin, &domain)
            == Err(BindError::Malformed),
        "high-s binding twin must be rejected as Malformed"
    );
    Ok(())
}

#[test]
fn tampered_node_id_recovers_different_address() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let sig = sign_binding(
        &signer,
        B256::repeat_byte(0x01),
        EPHEMERAL_BINDING_NONCE,
        &domain,
    )?;
    // Verify against a different node_id — recovers some other address, not
    // the real signer (the digest changed).
    let recovered = verify_binding(
        B256::repeat_byte(0x02),
        EPHEMERAL_BINDING_NONCE,
        &sig,
        &domain,
    )?;
    anyhow::ensure!(recovered != signer.address(), "tamper must change recovery");
    Ok(())
}

#[test]
fn tampered_nonce_recovers_different_address() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let node_id = B256::repeat_byte(0xAB);
    let sig = sign_binding(&signer, node_id, EPHEMERAL_BINDING_NONCE, &domain)?;
    let recovered = verify_binding(node_id, 1, &sig, &domain)?;
    anyhow::ensure!(
        recovered != signer.address(),
        "nonce change must alter recovery"
    );
    Ok(())
}

#[test]
fn wrong_length_rejected() -> anyhow::Result<()> {
    let domain = sample_domain();
    let err = verify_binding(B256::ZERO, 0, &[0u8; 64], &domain)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected BadLength, got Ok"))?;
    anyhow::ensure!(matches!(err, BindError::BadLength { len: 64 }), "{err:?}");
    Ok(())
}

#[test]
fn type_hash_matches_adr_003() {
    use alloy::primitives::keccak256;
    let canonical: &[u8] = b"BindNodeId(bytes32 nodeId,uint64 nonce)";
    let expected = keccak256(canonical);
    let actual = BindNodeIdSol::eip712_type_hash(&BindNodeIdSol {
        nodeId: B256::ZERO,
        nonce: 0,
    });
    assert_eq!(actual, expected, "BindNodeId type hash drifted");
}
