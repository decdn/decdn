use super::*;
use crate::store::StoreError;
use crate::voucher::voucher_domain;
use alloy::primitives::address;
use alloy::signers::local::PrivateKeySigner;

const PROVIDER: Address = address!("00000000000000000000000000000000000000b2");
const VERIFYING: Address = address!("0000000000000000000000000000000000001234");

/// Round-trip: sign an incentive voucher, encode to the wire, decode back
/// via the bridge with lane context, and confirm the signature still
/// verifies against the original signer.
#[test]
fn wire_voucher_roundtrip() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = voucher_domain(421_614, VERIFYING);
    let pool_id = B256::repeat_byte(0xC1);
    let bytes_delivered = U256::from(1_048_576u64);

    let signed = Voucher {
        pool_id,
        signer: signer.address(),
        provider: PROVIDER,
        amount: U256::from(10_000u64),
        bytes_delivered,
        chain_root: B256::repeat_byte(0xA7),
        chunk_price: U256::from(10u64),
    }
    .sign(&signer, &domain)?;

    let wire = signed_to_wire_voucher(&signed)?;
    anyhow::ensure!(wire.signature.len() == decdn_protocol::VOUCHER_SIG_LEN);

    let rebuilt = wire_voucher_to_signed(&wire, pool_id, signer.address(), PROVIDER)?;
    anyhow::ensure!(rebuilt == signed, "bridge must preserve the signed voucher");
    rebuilt.verify_signer(signer.address(), &domain)?;
    Ok(())
}

#[test]
fn bad_signature_length_rejected() -> anyhow::Result<()> {
    let wire = WireVoucher {
        signature: vec![0u8; 10],
        amount: 0u64,
        bytes_delivered: 0u64,
        chain_root: [0u8; 32],
        chunk_price: 0u64,
    };
    let err = wire_voucher_to_signed(&wire, B256::ZERO, PROVIDER, PROVIDER)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected BadSignature, got Ok"))?;
    anyhow::ensure!(matches!(err, WireVoucherError::BadSignature));
    Ok(())
}

#[test]
fn valid_length_invalid_parity_rejected() -> anyhow::Result<()> {
    // A correctly-sized (65-byte) signature whose recovery-id byte is not a
    // valid parity is rejected by `Signature::from_raw`, exercising the
    // bridge's `from_raw` error arm — distinct from the length check above.
    let mut signature = vec![0u8; decdn_protocol::VOUCHER_SIG_LEN];
    if let Some(v) = signature.last_mut() {
        *v = 2;
    }
    let wire = WireVoucher {
        signature,
        amount: 0u64,
        bytes_delivered: 0u64,
        chain_root: [0u8; 32],
        chunk_price: 0u64,
    };
    let err = wire_voucher_to_signed(&wire, B256::ZERO, PROVIDER, PROVIDER)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected BadSignature, got Ok"))?;
    anyhow::ensure!(matches!(err, WireVoucherError::BadSignature));
    Ok(())
}

#[test]
fn amount_narrows_to_u64() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = voucher_domain(421_614, VERIFYING);
    let signed = Voucher {
        pool_id: B256::ZERO,
        signer: signer.address(),
        provider: PROVIDER,
        amount: U256::from(0x0102_0304u64),
        bytes_delivered: U256::ZERO,
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(&signer, &domain)?;

    let wire = signed_to_wire_voucher(&signed)?;
    anyhow::ensure!(wire.amount == 0x0102_0304u64);
    anyhow::ensure!(wire.bytes_delivered == 0u64);
    Ok(())
}

/// A cumulative amount above `u64::MAX` — the on-chain pool caps at
/// `uint64`, so such a voucher can never be redeemed — is refused rather
/// than truncated.
#[test]
fn amount_above_u64_max_is_refused() -> anyhow::Result<()> {
    let signer = PrivateKeySigner::random();
    let domain = voucher_domain(421_614, VERIFYING);
    let signed = Voucher {
        pool_id: B256::ZERO,
        signer: signer.address(),
        provider: PROVIDER,
        amount: U256::from(u64::MAX) + U256::from(1u64),
        bytes_delivered: U256::ZERO,
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(&signer, &domain)?;

    let err = signed_to_wire_voucher(&signed)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected ValueExceedsWireWidth, got Ok"))?;
    anyhow::ensure!(matches!(err, WireVoucherError::ValueExceedsWireWidth));
    Ok(())
}

/// Every `PoolError` variant maps to a fixed reject reason — except the
/// transient `Store`, which signals retry. Exhaustive by construction; if a
/// new `PoolError` variant lands, `voucher_reject_reason` fails to compile
/// and this test is the reminder to extend the wire enum.
#[test]
fn voucher_reject_reason_is_exhaustive() {
    let cases = [
        (
            PoolError::WrongPool {
                expected: B256::ZERO,
                got: B256::ZERO,
            },
            Ok(VoucherRejectReason::WrongPool),
        ),
        (
            PoolError::WrongProvider {
                expected: PROVIDER,
                got: PROVIDER,
            },
            Ok(VoucherRejectReason::WrongProvider),
        ),
        (
            PoolError::AmountRegression {
                last: U256::ZERO,
                got: U256::ZERO,
            },
            Ok(VoucherRejectReason::AmountRegression),
        ),
        (
            PoolError::UnderFold {
                axis: crate::FoldAxis::Amount,
                owed: U256::ZERO,
                got: U256::ZERO,
            },
            Ok(VoucherRejectReason::UnderFold),
        ),
        (
            PoolError::BytesRegression {
                last: U256::ZERO,
                got: U256::ZERO,
            },
            Ok(VoucherRejectReason::BytesRegression),
        ),
        (
            PoolError::CapExceeded {
                cap: U256::ZERO,
                got: U256::ZERO,
            },
            Ok(VoucherRejectReason::SpendingCapExhausted),
        ),
        (
            PoolError::Signature(VoucherError::InvalidSignature),
            Ok(VoucherRejectReason::BadSignature),
        ),
        (
            PoolError::Signature(VoucherError::WrongSigner {
                expected: PROVIDER,
                recovered: PROVIDER,
            }),
            Ok(VoucherRejectReason::WrongSigner),
        ),
    ];
    for (err, expected) in cases {
        assert_eq!(voucher_reject_reason(&err), expected, "{err:?}");
    }

    // Store is transient — signals retry, not a wire rejection.
    let store_err = PoolError::Store(StoreError::Io(std::io::Error::other("x")));
    assert_eq!(voucher_reject_reason(&store_err), Err(RetrySignal));
}
