use super::*;
use alloy::primitives::{address, b256};
use alloy::signers::local::PrivateKeySigner;

fn sample_voucher() -> Voucher {
    Voucher {
        pool_id: b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
        signer: address!("00000000000000000000000000000000000000a1"),
        provider: address!("00000000000000000000000000000000000000b2"),
        amount: U256::from(10_000_000u64), // 10 USDC at 6 decimals
        bytes_delivered: U256::from(1_048_576u64), // 1 MB
        chain_root: B256::repeat_byte(0xA7),
        chunk_price: U256::from(10u64), // one chunk == one MB at 10 µUSDC/MB
    }
}

fn sample_domain() -> Eip712Domain {
    // Arbitrum Sepolia chain id; address is a deterministic test fixture.
    voucher_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    )
}

/// Helper: extract the error from a `Result`, returning a typed error if
/// the value was unexpectedly `Ok`.
fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
    r.err()
        .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
}

#[test]
fn round_trip_sign_verify() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let address = signer.address();
    let domain = sample_domain();

    let signed = sample_voucher().sign(&signer, &domain)?;
    signed.verify_signer(address, &domain)?;
    Ok(())
}

#[test]
fn wrong_signer_address_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let other = PrivateKeySigner::random().address();
    let domain = sample_domain();

    let signed = sample_voucher().sign(&signer, &domain)?;
    let err = err_of(signed.verify_signer(other, &domain))?;
    anyhow::ensure!(
        matches!(err, VoucherError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn unrecoverable_signature_is_invalid_signature() -> anyhow::Result<()> {
    // A structurally well-formed signature (valid parity byte) whose `r`/`s`
    // do not recover to any point surfaces as `InvalidSignature` — the only
    // producer of that variant. `r = s = 0` is the canonical unrecoverable
    // case (ECDSA recovery requires `r, s ∈ [1, n)`). This is the one
    // negative path the tamper tests can't reach: they all start from a
    // valid signature and only perturb the digest, yielding `WrongSigner`.
    let signed = SignedVoucher {
        voucher: sample_voucher(),
        signature: Signature::new(U256::ZERO, U256::ZERO, false),
    };
    let err = err_of(signed.recover_signer(&sample_domain()))?;
    anyhow::ensure!(
        matches!(err, VoucherError::InvalidSignature),
        "expected InvalidSignature, got: {err:?}"
    );
    Ok(())
}

#[test]
fn high_s_voucher_signature_rejected() -> anyhow::Result<()> {
    // A malicious client can flip a valid voucher signature to its
    // non-canonical high-`s` twin. alloy's recovery would normalize and
    // accept it, but the on-chain `PaymentPool` reverts (#836). The
    // off-chain accept-set must match: reject the twin, accept the original.
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let signed = sample_voucher().sign(&signer, &domain)?;
    signed.verify_signer(signer.address(), &domain)?; // low-s original still valid

    let twin = SignedVoucher {
        signature: crate::sig_canon::high_s_twin(&signed.signature),
        ..signed
    };
    anyhow::ensure!(
        twin.recover_signer(&domain) == Err(VoucherError::InvalidSignature),
        "high-s voucher twin must be rejected as InvalidSignature"
    );
    Ok(())
}

#[test]
fn different_chain_id_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain_a = voucher_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    );
    let domain_b = voucher_domain(1, address!("0000000000000000000000000000000000001234"));

    let signed = sample_voucher().sign(&signer, &domain_a)?;
    // Recovered signer differs from the actual one because the digest
    // changed, so we get a WrongSigner — not InvalidSignature.
    let err = err_of(signed.verify_signer(signer.address(), &domain_b))?;
    anyhow::ensure!(
        matches!(err, VoucherError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn different_verifying_contract_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain_a = voucher_domain(
        421_614,
        address!("0000000000000000000000000000000000001234"),
    );
    let domain_b = voucher_domain(
        421_614,
        address!("0000000000000000000000000000000000005678"),
    );

    let signed = sample_voucher().sign(&signer, &domain_a)?;
    let err = err_of(signed.verify_signer(signer.address(), &domain_b))?;
    anyhow::ensure!(
        matches!(err, VoucherError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn tampered_amount_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let mut signed = sample_voucher().sign(&signer, &domain)?;

    signed.voucher.amount += U256::from(1u64);

    let err = err_of(signed.verify_signer(signer.address(), &domain))?;
    anyhow::ensure!(
        matches!(err, VoucherError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn tampered_signer_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let mut signed = sample_voucher().sign(&signer, &domain)?;

    signed.voucher.signer = address!("000000000000000000000000000000000000dead");

    let err = err_of(signed.verify_signer(signer.address(), &domain))?;
    anyhow::ensure!(
        matches!(err, VoucherError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn tampered_provider_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let mut signed = sample_voucher().sign(&signer, &domain)?;

    signed.voucher.provider = address!("000000000000000000000000000000000000beef");

    let err = err_of(signed.verify_signer(signer.address(), &domain))?;
    anyhow::ensure!(
        matches!(err, VoucherError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn tampered_bytes_delivered_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let mut signed = sample_voucher().sign(&signer, &domain)?;

    signed.voucher.bytes_delivered += U256::from(1u64);

    let err = err_of(signed.verify_signer(signer.address(), &domain))?;
    anyhow::ensure!(
        matches!(err, VoucherError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

#[test]
fn tampered_pool_id_rejected() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = sample_domain();
    let mut signed = sample_voucher().sign(&signer, &domain)?;

    signed.voucher.pool_id = B256::ZERO;

    let err = err_of(signed.verify_signer(signer.address(), &domain))?;
    anyhow::ensure!(
        matches!(err, VoucherError::WrongSigner { .. }),
        "expected WrongSigner, got: {err:?}"
    );
    Ok(())
}

/// Lock the EIP-712 type hash to the exact ADR 003 wording. If this
/// breaks, either the ADR changed or the `sol!` macro's canonical
/// encoding shifted — both warrant a coordinated update with the
/// Solidity contract.
#[test]
fn voucher_type_hash_matches_adr_003() -> anyhow::Result<()> {
    use alloy::primitives::keccak256;
    // Canonical EIP-712 type-string per ADR 003 §EIP-712 Voucher
    // Signature. Single space between Solidity type and field name; no
    // other whitespace; fields in declaration order.
    let canonical: &[u8] = b"Voucher(bytes32 poolId,address signer,address provider,uint256 amount,uint256 bytesDelivered,bytes32 chainRoot,uint256 chunkPrice)";
    let expected = keccak256(canonical);
    let actual = VoucherSol::eip712_type_hash(&sample_voucher().to_sol());
    anyhow::ensure!(
        actual == expected,
        "voucher type hash drifted: actual={actual} expected={expected}"
    );
    Ok(())
}

/// Lock the domain separator to the canonical EIP-712 form. If the
/// alloy `Eip712Domain` ever changes its separator computation, this
/// surfaces it before signatures stop being recoverable on-chain.
#[test]
fn domain_separator_matches_eip712_canonical() -> anyhow::Result<()> {
    use alloy::primitives::keccak256;
    use alloy::sol_types::SolValue;

    let domain_typehash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let chain_id: u64 = 421_614;
    let verifying = address!("0000000000000000000000000000000000001234");

    let domain = voucher_domain(chain_id, verifying);
    let actual = domain.separator();

    // separator = keccak256(abi.encode(typehash, hash(name), hash(version), chainId, addr))
    let expected = keccak256(
        (
            domain_typehash,
            keccak256(DOMAIN_NAME.as_bytes()),
            keccak256(DOMAIN_VERSION.as_bytes()),
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

/// Fixed-vector regression: a deterministic key + voucher must produce a
/// specific recovered address. If the digest computation drifts, this
/// catches it before the contract does.
#[test]
fn fixed_vector_recovers_expected_signer() -> anyhow::Result<()> {
    // Test key — never used for anything but this fixture.
    let pk_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    let signer: PrivateKeySigner = pk_hex
        .parse()
        .map_err(|e| anyhow::anyhow!("parse test key: {e}"))?;
    let address = signer.address();

    let voucher = Voucher {
        pool_id: B256::repeat_byte(0xAA),
        signer: address,
        provider: address!("00000000000000000000000000000000000000b2"),
        amount: U256::from(1_000_000u64),
        bytes_delivered: U256::from(1_048_576u64),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    };
    let domain = voucher_domain(
        421_614,
        address!("00000000000000000000000000000000deadbeef"),
    );

    let signed = voucher.sign(&signer, &domain)?;
    let recovered = signed.recover_signer(&domain)?;
    anyhow::ensure!(
        recovered == address,
        "recovered {recovered}, expected {address}"
    );
    Ok(())
}

/// Pin the EIP-712 domain `name`/`version` to the on-chain contract's
/// constructor args — `PaymentPool.sol` calls `EIP712("PaymentPool",
/// "1")`. A divergence here makes every `redeem` revert with the
/// contract's `InvalidVoucherSignature` even though off-chain
/// `verify_signer` passes, so the literal is asserted directly rather than
/// derived from `DOMAIN_NAME` (which would let a regression slip through).
#[test]
fn domain_matches_payment_pool_contract() {
    assert_eq!(
        DOMAIN_NAME, "PaymentPool",
        "voucher domain name must match PaymentPool.sol EIP712 ctor"
    );
    assert_eq!(
        DOMAIN_VERSION, "1",
        "voucher domain version must match PaymentPool.sol EIP712 ctor"
    );
}
