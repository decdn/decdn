use super::*;
use alloy::primitives::{address, b256, keccak256};
use alloy::signers::local::PrivateKeySigner;

fn sample_capability() -> Capability {
    Capability {
        signer: address!("00000000000000000000000000000000000000a1"),
        spending_cap: 10_000_000u64,
        pool_id: b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
        expiry: 1_900_000_000,
    }
}

fn sample_domain() -> Eip712Domain {
    voucher_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    )
}

fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
    r.err()
        .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
}

#[test]
fn sign_then_recover_is_the_owner() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let domain = sample_domain();
    let signed = sample_capability().sign(&owner, &domain)?;
    let recovered = signed.recover_owner(&domain)?;
    anyhow::ensure!(recovered == owner.address(), "must recover the owner");
    signed.verify_owner(owner.address(), &domain)?;
    Ok(())
}

#[test]
fn wrong_owner_rejected() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let other = PrivateKeySigner::random().address();
    let domain = sample_domain();
    let signed = sample_capability().sign(&owner, &domain)?;
    let err = err_of(signed.verify_owner(other, &domain))?;
    anyhow::ensure!(
        matches!(err, CapabilityError::WrongOwner { .. }),
        "expected WrongOwner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn tampered_signer_rejected() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let domain = sample_domain();
    let mut signed = sample_capability().sign(&owner, &domain)?;
    signed.capability.signer = address!("000000000000000000000000000000000000dead");
    let err = err_of(signed.verify_owner(owner.address(), &domain))?;
    anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
    Ok(())
}

#[test]
fn tampered_spending_cap_rejected() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let domain = sample_domain();
    let mut signed = sample_capability().sign(&owner, &domain)?;
    signed.capability.spending_cap += 1;
    let err = err_of(signed.verify_owner(owner.address(), &domain))?;
    anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
    Ok(())
}

#[test]
fn tampered_pool_id_rejected() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let domain = sample_domain();
    let mut signed = sample_capability().sign(&owner, &domain)?;
    signed.capability.pool_id = B256::ZERO;
    let err = err_of(signed.verify_owner(owner.address(), &domain))?;
    anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
    Ok(())
}

#[test]
fn tampered_expiry_rejected() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let domain = sample_domain();
    let mut signed = sample_capability().sign(&owner, &domain)?;
    signed.capability.expiry += 1;
    let err = err_of(signed.verify_owner(owner.address(), &domain))?;
    anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
    Ok(())
}

#[test]
fn high_s_capability_signature_rejected() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let domain = sample_domain();
    let signed = sample_capability().sign(&owner, &domain)?;
    let twin = SignedCapability {
        signature: crate::sig_canon::high_s_twin(&signed.signature),
        ..signed
    };
    anyhow::ensure!(
        twin.recover_owner(&domain) == Err(CapabilityError::InvalidSignature),
        "high-s capability twin must be rejected"
    );
    Ok(())
}

#[test]
fn different_chain_id_rejected() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let domain_a = sample_domain();
    let domain_b = voucher_domain(1, address!("0000000000000000000000000000000000001234"));
    let signed = sample_capability().sign(&owner, &domain_a)?;
    let err = err_of(signed.verify_owner(owner.address(), &domain_b))?;
    anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
    Ok(())
}

#[test]
fn different_verifying_contract_rejected() -> anyhow::Result<()> {
    let owner = PrivateKeySigner::random();
    let domain_a = sample_domain();
    let domain_b = voucher_domain(
        421_614,
        address!("0000000000000000000000000000000000005678"),
    );
    let signed = sample_capability().sign(&owner, &domain_a)?;
    let err = err_of(signed.verify_owner(owner.address(), &domain_b))?;
    anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
    Ok(())
}

/// Lock the EIP-712 type hash to the exact `PaymentPool.CAPABILITY_TYPEHASH`
/// wording. If this breaks, the contract typehash or the `sol!` canonical
/// encoding drifted — a coordinated update with the Solidity contract.
#[test]
fn capability_type_hash_matches_adr_003() -> anyhow::Result<()> {
    let canonical: &[u8] =
        b"Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)";
    let expected = keccak256(canonical);
    let actual = CapabilitySol::eip712_type_hash(&sample_capability().to_sol());
    anyhow::ensure!(
        actual == expected,
        "capability type hash drifted: actual={actual} expected={expected}"
    );
    Ok(())
}

/// Fixed-vector regression: a deterministic key + capability must recover
/// the owner's address. Catches a Rust-side digest drift before the
/// contract does.
#[test]
fn fixed_vector_recovers_expected_owner() -> anyhow::Result<()> {
    let pk_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    let owner: PrivateKeySigner = pk_hex
        .parse()
        .map_err(|e| anyhow::anyhow!("parse test key: {e}"))?;
    let address = owner.address();
    let signed = sample_capability().sign(&owner, &sample_domain())?;
    let recovered = signed.recover_owner(&sample_domain())?;
    anyhow::ensure!(
        recovered == address,
        "recovered {recovered}, expected {address}"
    );
    Ok(())
}

/// A capability and a voucher over overlapping fields must produce
/// *different* digests — the distinct type-string is what stops one
/// standing in for the other.
#[test]
fn capability_digest_differs_from_voucher_digest() -> anyhow::Result<()> {
    use crate::voucher::Voucher;
    let domain = sample_domain();
    let cap = sample_capability();
    let voucher = Voucher {
        pool_id: cap.pool_id,
        signer: cap.signer,
        provider: address!("00000000000000000000000000000000000000b2"),
        amount: U256::from(cap.spending_cap),
        bytes_delivered: U256::from(1u64),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    };
    anyhow::ensure!(
        cap.signing_hash(&domain) != voucher.signing_hash(&domain),
        "capability and voucher digests must differ (distinct typehash)"
    );
    Ok(())
}
