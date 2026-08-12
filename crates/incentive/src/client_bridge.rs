//! Bridge between `cdn/client/v1` wire vouchers and the incentive voucher
//! state machine.
//!
//! The protocol crate ([`decdn_protocol::Voucher`]) is crypto-free: a wire
//! voucher carries only `{signature, amount}` as raw bytes. Validating it
//! against a lane requires the full EIP-712 typed data
//! `{pool_id, signer, provider, amount, bytes_delivered}` — and `pool_id`,
//! `signer`, `provider`, and `bytes_delivered` are **not** on the wire (ADR 005
//! §Voucher wire format). They come from stream context: `pool_id` from the
//! signed `StreamRequest`, `signer` the capability key, `provider` this node,
//! and `bytes_delivered` the node's per-lane cumulative byte counter.
//! [`wire_voucher_to_signed`] reconstructs the [`SignedVoucher`] from a wire
//! voucher plus that context; [`signed_to_wire_voucher`] is the requester-side
//! inverse.
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
/// `pool_id`, `signer`, `provider`, and `bytes_delivered` are not carried on the
/// wire — the node supplies `pool_id` from the originating `StreamRequest`,
/// `signer` from the registered capability, `provider` from its own identity,
/// and `bytes_delivered` from its per-lane cumulative byte counter.
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
    bytes_delivered: U256,
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
            amount: U256::from_be_bytes(wire.amount),
            bytes_delivered,
        },
        signature,
    })
}

/// Encode a [`SignedVoucher`] to its wire form (requester side). The
/// lane-context fields (`pool_id`, `signer`, `provider`, `bytes_delivered`) are
/// dropped — the receiver reconstructs them.
#[must_use]
pub fn signed_to_wire_voucher(signed: &SignedVoucher) -> WireVoucher {
    WireVoucher {
        signature: signed.signature.as_bytes().to_vec(),
        amount: signed.voucher.amount.to_be_bytes::<32>(),
    }
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

        let wire = signed_to_wire_voucher(&signed);
        anyhow::ensure!(wire.signature.len() == decdn_protocol::VOUCHER_SIG_LEN);

        let rebuilt =
            wire_voucher_to_signed(&wire, pool_id, signer.address(), PROVIDER, bytes_delivered)?;
        anyhow::ensure!(rebuilt == signed, "bridge must preserve the signed voucher");
        rebuilt.verify_signer(signer.address(), &domain)?;
        Ok(())
    }

    #[test]
    fn bad_signature_length_rejected() -> anyhow::Result<()> {
        let wire = WireVoucher {
            signature: vec![0u8; 10],
            amount: [0u8; 32],
        };
        let err = wire_voucher_to_signed(&wire, B256::ZERO, PROVIDER, PROVIDER, U256::ZERO)
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
            amount: [0u8; 32],
        };
        let err = wire_voucher_to_signed(&wire, B256::ZERO, PROVIDER, PROVIDER, U256::ZERO)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected BadSignature, got Ok"))?;
        anyhow::ensure!(matches!(err, WireVoucherError::BadSignature));
        Ok(())
    }

    #[test]
    fn amount_big_endian_preserved() -> anyhow::Result<()> {
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

        let wire = signed_to_wire_voucher(&signed);
        // Big-endian: most-significant byte first, low byte last.
        anyhow::ensure!(wire.amount[31] == 0x04);
        anyhow::ensure!(wire.amount[30] == 0x03);
        anyhow::ensure!(wire.amount[0] == 0x00);
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
/// `U256` money field crosses to the fixed 32-byte big-endian wire form and
/// back — the exact spot a truncation would corrupt a payment. These sweep the
/// full keyspace (boundaries `0`, `u64::MAX`, `U256::MAX` heavily over-sampled,
/// see [`any_u256`]) and confirm the round-trip is lossless and that malformed
/// wire input never panics.
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

    /// `U256` sweep with the truncation-prone boundaries heavily over-sampled
    /// (weight 1 each against the random arm's 8).
    fn any_u256() -> impl Strategy<Value = U256> {
        prop_oneof![
            8 => proptest::array::uniform32(any::<u8>()).prop_map(U256::from_be_bytes),
            1 => Just(U256::ZERO),
            1 => Just(U256::from(1u64)),
            1 => Just(U256::from(u64::MAX)),
            1 => Just(U256::MAX),
        ]
    }

    fn any_signer() -> impl Strategy<Value = PrivateKeySigner> {
        proptest::array::uniform32(any::<u8>())
            .prop_filter_map("scalar must be a valid, non-zero secp256k1 key", |bytes| {
                PrivateKeySigner::from_slice(&bytes).ok()
            })
    }

    proptest! {
        /// `signed → wire → signed` reproduces the signed voucher exactly when
        /// the off-wire context (`pool_id`, `signer`, `provider`,
        /// `bytes_delivered`) is re-supplied — proving the big-endian
        /// `U256 ↔ [u8; 32]` encoding is lossless across the whole range.
        #[test]
        fn wire_round_trip_is_lossless(
            pool_id in proptest::array::uniform32(any::<u8>()).prop_map(B256::from),
            amount in any_u256(),
            bytes_delivered in any_u256(),
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

            let wire = signed_to_wire_voucher(&signed);
            prop_assert_eq!(wire.amount, amount.to_be_bytes::<32>());

            let rebuilt =
                wire_voucher_to_signed(&wire, pool_id, signer.address(), PROVIDER, bytes_delivered)
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
            amount in proptest::array::uniform32(any::<u8>()),
        ) {
            let wire = WireVoucher { signature: signature.clone(), amount };
            let result =
                wire_voucher_to_signed(&wire, B256::ZERO, PROVIDER, PROVIDER, U256::ZERO);
            if signature.len() != decdn_protocol::VOUCHER_SIG_LEN {
                prop_assert_eq!(result.err(), Some(WireVoucherError::BadSignature));
            }
        }
    }
}
