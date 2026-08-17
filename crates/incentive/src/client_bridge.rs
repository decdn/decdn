//! Bridge between `cdn/client/v1` wire vouchers and the incentive voucher
//! state machine.
//!
//! The protocol crate ([`decdn_protocol::Voucher`]) is crypto-free: a wire
//! voucher carries `{signature, amount, bytes_delivered}`, with `amount` and
//! `bytes_delivered` as `u64` cumulative totals matching the contract's
//! on-chain `uint64` storage. Validating it against a lane requires the full
//! EIP-712 typed data `{pool_id, signer, provider, amount, bytes_delivered}`
//! — and `pool_id`, `signer`, and `provider` are **not** on the wire (ADR 005
//! §Voucher wire format). They come from stream context: `pool_id` from the
//! signed `StreamRequest`, `signer` the capability key, and `provider` this
//! node. [`wire_voucher_to_signed`] reconstructs the [`SignedVoucher`] from a
//! wire voucher plus that context, widening `u64 → U256` (infallible);
//! [`signed_to_wire_voucher`] is the requester-side inverse, narrowing
//! `U256 → u64` (fallible — a value above the on-chain `uint64` cap is
//! refused, never truncated).
//!
//! [`voucher_reject_reason`] maps an off-chain [`PoolError`] to the wire
//! [`VoucherRejectReason`] a node returns mid-stream. It is exhaustive with no
//! wildcard so that adding a `PoolError` variant fails to compile until the wire
//! enum and ADR 005 §Mirror obligation are updated.

use alloy::primitives::{Address, B256, Signature, U256};

use decdn_protocol::client::{Voucher as WireVoucher, VoucherRejectReason};

use crate::lane::PoolError;
use crate::voucher::{SignedVoucher, Voucher, VoucherError};

/// Reconstruct a [`SignedVoucher`] from a wire voucher plus lane context.
///
/// `pool_id`, `signer`, and `provider` are not carried on the wire — the node
/// supplies `pool_id` from the originating `StreamRequest`, `signer` from the
/// registered capability, and `provider` from its own identity. `amount` and
/// `bytes_delivered` ride the wire.
///
/// # Errors
///
/// Returns [`WireVoucherError::BadSignature`] if `wire.signature` is not a
/// well-formed 65-byte (`r‖s‖v`) secp256k1 signature.
pub fn wire_voucher_to_signed(
    wire: &WireVoucher,
    pool_id: B256,
    signer: Address,
    provider: Address,
) -> Result<SignedVoucher, WireVoucherError> {
    // Length is checked by the protocol's own `Voucher::validate` (it pins
    // `VOUCHER_SIG_LEN`); `Signature::from_raw` then enforces well-formedness.
    wire.validate()
        .map_err(|_| WireVoucherError::BadSignature)?;
    let signature =
        Signature::from_raw(&wire.signature).map_err(|_| WireVoucherError::BadSignature)?;
    Ok(SignedVoucher {
        voucher: Voucher {
            pool_id,
            signer,
            provider,
            amount: U256::from(wire.amount),
            bytes_delivered: U256::from(wire.bytes_delivered),
        },
        signature,
    })
}

/// Encode a [`SignedVoucher`] to its wire form (requester side). The
/// lane-context fields (`pool_id`, `signer`, `provider`) are dropped — the
/// receiver reconstructs them.
///
/// # Errors
///
/// Returns [`WireVoucherError::ValueExceedsWireWidth`] if `amount` or
/// `bytes_delivered` exceeds `u64::MAX` — the on-chain pool caps both at
/// `uint64`, so such a voucher is unredeemable; it is refused rather than
/// truncated.
pub fn signed_to_wire_voucher(signed: &SignedVoucher) -> Result<WireVoucher, WireVoucherError> {
    let amount = u64::try_from(signed.voucher.amount)
        .map_err(|_| WireVoucherError::ValueExceedsWireWidth)?;
    let bytes_delivered = u64::try_from(signed.voucher.bytes_delivered)
        .map_err(|_| WireVoucherError::ValueExceedsWireWidth)?;
    Ok(WireVoucher {
        signature: signed.signature.as_bytes().to_vec(),
        amount,
        bytes_delivered,
    })
}

/// Map an off-chain [`PoolError`] to the wire [`VoucherRejectReason`] a node
/// returns mid-stream, or [`RetrySignal`] when the failure is transient.
///
/// **Exhaustive, no wildcard (ADR 005 §Mirror obligation).** A new `PoolError`
/// variant breaks this match at compile time, forcing a coordinated update of
/// [`VoucherRejectReason`] and the retry-semantics table.
///
/// [`PoolError::Store`] is the only transient failure: in-memory state did not
/// advance, so it returns [`RetrySignal`] rather than a permanent reason. The
/// `cdn/client/v1` handler surfaces that signal on the wire as
/// [`VoucherRejectReason::RetryLater`] (sent in-band, then the stream is finished
/// cleanly), so the client resends the **same** voucher on a fresh stream instead
/// of seeing an opaque connection drop (ADR 003 §Off-chain voucher state
/// persistence). `RetryLater` has no `PoolError` counterpart here — it is produced
/// by the handler from the `Err(RetrySignal)` arm, not by this mapping. Every
/// other variant is a permanent rejection.
pub const fn voucher_reject_reason(err: &PoolError) -> Result<VoucherRejectReason, RetrySignal> {
    match err {
        PoolError::WrongPool { .. } => Ok(VoucherRejectReason::WrongPool),
        PoolError::WrongProvider { .. } => Ok(VoucherRejectReason::WrongProvider),
        PoolError::AmountRegression { .. } => Ok(VoucherRejectReason::AmountRegression),
        PoolError::BytesRegression { .. } => Ok(VoucherRejectReason::BytesRegression),
        PoolError::CapExceeded { .. } => Ok(VoucherRejectReason::CapExceeded),
        PoolError::Signature(VoucherError::InvalidSignature) => {
            Ok(VoucherRejectReason::BadSignature)
        }
        PoolError::Signature(VoucherError::WrongSigner { .. }) => {
            Ok(VoucherRejectReason::WrongSigner)
        }
        // Transient — in-memory state unchanged. The handler emits this on the
        // wire as `VoucherRejectReason::RetryLater`; the client resends the same
        // voucher (#527, ADR 003 §Off-chain voucher state persistence).
        PoolError::Store(_) => Err(RetrySignal),
    }
}

/// Failure mode for [`wire_voucher_to_signed`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireVoucherError {
    /// `wire.signature` is not a well-formed 65-byte secp256k1 signature.
    #[error("wire voucher signature is malformed or wrong length")]
    BadSignature,
    /// A cumulative amount or byte count exceeds the `u64` wire width. The
    /// on-chain pool caps both at `uint64`, so such a voucher is unredeemable;
    /// it is refused rather than truncated.
    #[error("voucher amount or bytes_delivered exceeds the u64 wire width")]
    ValueExceedsWireWidth,
}

/// Signals that a [`PoolError`] was transient ([`PoolError::Store`]): in-memory
/// state did not advance, so the caller surfaces it as a
/// [`VoucherRejectReason::RetryLater`] in-band rejection (no ack) and the client
/// resends the **same** voucher on a fresh stream (#527, ADR 003) —
/// distinguishable from a permanent rejection or a network drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrySignal;

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed pair up clearly here
mod tests {
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
                Ok(VoucherRejectReason::CapExceeded),
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
}

/// Property-based tests for the wire bridge (#740). The bridge is where the
/// `U256` money field crosses to the `u64` wire form and back — the exact
/// spot a truncation would corrupt a payment. These sweep the full `u64`
/// keyspace and confirm the round-trip is lossless and that malformed wire
/// input never panics. The narrowing failure path (`U256` above `u64::MAX`)
/// is covered separately by `amount_above_u64_max_is_refused`.
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod prop_tests {
    use super::*;
    use crate::voucher::voucher_domain;
    use alloy::primitives::address;
    use alloy::signers::local::PrivateKeySigner;
    use proptest::prelude::*;

    const PROVIDER: Address = address!("00000000000000000000000000000000000000b2");
    const VERIFYING: Address = address!("0000000000000000000000000000000000001234");

    fn any_signer() -> impl Strategy<Value = PrivateKeySigner> {
        proptest::array::uniform32(any::<u8>())
            .prop_filter_map("scalar must be a valid, non-zero secp256k1 key", |bytes| {
                PrivateKeySigner::from_slice(&bytes).ok()
            })
    }

    proptest! {
        /// `signed → wire → signed` reproduces the signed voucher exactly when
        /// the off-wire context (`pool_id`, `signer`, `provider`,
        /// `bytes_delivered`) is re-supplied — proving the `U256 ↔ u64`
        /// narrowing/widening is lossless across the whole `u64` range.
        #[test]
        fn wire_round_trip_is_lossless(
            pool_id in proptest::array::uniform32(any::<u8>()).prop_map(B256::from),
            amount in any::<u64>().prop_map(U256::from),
            bytes_delivered in any::<u64>().prop_map(U256::from),
            signer in any_signer(),
        ) {
            let domain = voucher_domain(421_614, VERIFYING);
            let signed = Voucher {
                pool_id,
                signer: signer.address(),
                provider: PROVIDER,
                amount,
                bytes_delivered,
            }
                .sign(&signer, &domain)
                .unwrap();

            let wire = signed_to_wire_voucher(&signed).unwrap();
            prop_assert_eq!(wire.amount, u64::try_from(amount).unwrap());
            prop_assert_eq!(wire.bytes_delivered, u64::try_from(bytes_delivered).unwrap());

            let rebuilt =
                wire_voucher_to_signed(&wire, pool_id, signer.address(), PROVIDER)
                    .unwrap();
            prop_assert_eq!(&rebuilt, &signed, "bridge must preserve the signed voucher");
            prop_assert!(rebuilt.verify_signer(signer.address(), &domain).is_ok());
        }

        /// Decoding never panics on arbitrary input, and any signature whose
        /// length is not `VOUCHER_SIG_LEN` is rejected as `BadSignature` rather
        /// than parsed or crashed.
        #[test]
        fn malformed_wire_signature_is_rejected_not_panicked(
            signature in prop::collection::vec(any::<u8>(), 0..200),
            amount in any::<u64>(),
        ) {
            let wire = WireVoucher {
                signature: signature.clone(),
                amount,
                bytes_delivered: 0u64,
            };
            let result = wire_voucher_to_signed(&wire, B256::ZERO, PROVIDER, PROVIDER);
            if signature.len() != decdn_protocol::VOUCHER_SIG_LEN {
                prop_assert_eq!(result.err(), Some(WireVoucherError::BadSignature));
            }
        }
    }
}
