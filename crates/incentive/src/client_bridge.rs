//! Bridge between `cdn/client/v1` wire vouchers and the incentive voucher
//! state machine.
//!
//! The protocol crate ([`decdn_protocol::Voucher`]) is crypto-free: a wire
//! voucher carries only `{signature, amount, nonce}` as raw bytes. Validating
//! it against a channel requires the full EIP-712 typed data
//! `{channelId, amount, nonce, bytesDelivered, token}` — and `channelId`,
//! `token`, and `bytesDelivered` are **not** on the wire (ADR 005 §Voucher wire
//! format). They come from stream context: `channel_id` from the
//! `StreamRequest`, `token` fixed at channel open, and `bytesDelivered` the
//! node's per-channel cumulative byte counter. [`wire_voucher_to_signed`]
//! reconstructs the [`SignedVoucher`] from a wire voucher plus that context;
//! [`signed_to_wire_voucher`] is the requester-side inverse.
//!
//! [`voucher_reject_reason`] maps an off-chain [`ChannelError`] to the wire
//! [`VoucherRejectReason`] a node returns mid-stream. It is exhaustive with no
//! wildcard so that adding a `ChannelError` variant fails to compile until the
//! wire enum and ADR 005 §Mirror obligation are updated.

use alloy::primitives::{Address, B256, Signature, U256};

use decdn_protocol::client::{Voucher as WireVoucher, VoucherRejectReason};

use crate::channel::ChannelError;
use crate::voucher::{SignedVoucher, Voucher, VoucherError};

/// Reconstruct a [`SignedVoucher`] from a wire voucher plus channel context.
///
/// `channel_id`, `token`, and `bytes_delivered` are not carried on the wire —
/// the node supplies them from the channel's hydrated [`crate::ChannelState`]
/// (`token`) and its per-channel cumulative byte counter (`bytes_delivered`),
/// and `channel_id` from the originating `StreamRequest`.
///
/// # Errors
///
/// Returns [`WireVoucherError::BadSignature`] if `wire.signature` is not a
/// well-formed 65-byte (`r‖s‖v`) secp256k1 signature.
pub fn wire_voucher_to_signed(
    wire: &WireVoucher,
    channel_id: B256,
    token: Address,
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
            channel_id,
            amount: U256::from_be_bytes(wire.amount),
            nonce: U256::from_be_bytes(wire.nonce),
            bytes_delivered,
            token,
        },
        signature,
    })
}

/// Encode a [`SignedVoucher`] to its wire form (requester side). The
/// channel-context fields (`channel_id`, `token`, `bytes_delivered`) are
/// dropped — the receiver reconstructs them.
#[must_use]
pub fn signed_to_wire_voucher(signed: &SignedVoucher) -> WireVoucher {
    WireVoucher {
        signature: signed.signature.as_bytes().to_vec(),
        amount: signed.voucher.amount.to_be_bytes::<32>(),
        nonce: signed.voucher.nonce.to_be_bytes::<32>(),
    }
}

/// Map an off-chain [`ChannelError`] to the wire [`VoucherRejectReason`] a node
/// returns mid-stream, or [`RetrySignal`] when the failure is transient.
///
/// **Exhaustive, no wildcard (ADR 005 §Mirror obligation).** A new
/// `ChannelError` variant breaks this match at compile time, forcing a
/// coordinated update of [`VoucherRejectReason`] and the retry-semantics table.
///
/// [`ChannelError::Store`] is the only transient failure: in-memory state did
/// not advance, so the node keeps the stream open and the client retries the
/// **same** voucher — it is not a `VoucherRejected` (which has no `Store`
/// counterpart). Every other variant is a permanent rejection. "Unknown
/// channel" is surfaced by the caller as [`ChannelError::WrongChannel`] →
/// [`VoucherRejectReason::WrongChannel`].
pub const fn voucher_reject_reason(err: &ChannelError) -> Result<VoucherRejectReason, RetrySignal> {
    match err {
        ChannelError::WrongChannel { .. } => Ok(VoucherRejectReason::WrongChannel),
        ChannelError::WrongToken { .. } => Ok(VoucherRejectReason::WrongToken),
        ChannelError::NonceNotIncreasing { .. } => Ok(VoucherRejectReason::StaleNonce),
        ChannelError::AmountDecreasing { .. } => Ok(VoucherRejectReason::AmountRegression),
        ChannelError::BytesDecreasing { .. } => Ok(VoucherRejectReason::BytesRegression),
        ChannelError::AmountExceedsDeposit { .. } => Ok(VoucherRejectReason::InsufficientDeposit),
        ChannelError::Signature(VoucherError::InvalidSignature) => {
            Ok(VoucherRejectReason::BadSignature)
        }
        ChannelError::Signature(VoucherError::WrongSigner { .. }) => {
            Ok(VoucherRejectReason::WrongSigner)
        }
        // Transient — in-memory state unchanged; client retries the same
        // voucher. No `VoucherRejected` reason corresponds (#527).
        ChannelError::Store(_) => Err(RetrySignal),
    }
}

/// Failure mode for [`wire_voucher_to_signed`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireVoucherError {
    /// `wire.signature` is not a well-formed 65-byte secp256k1 signature.
    #[error("wire voucher signature is malformed or wrong length")]
    BadSignature,
}

/// Signals that a [`ChannelError`] was transient ([`ChannelError::Store`]): the
/// caller must keep the stream open and let the client retry the same voucher
/// rather than sending a `VoucherRejected` (#527).
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

    const TOKEN: Address = address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");
    const VERIFYING: Address = address!("0000000000000000000000000000000000001234");

    /// Round-trip: sign an incentive voucher, encode to the wire, decode back
    /// via the bridge with channel context, and confirm the signature still
    /// verifies against the original signer.
    #[test]
    fn wire_voucher_roundtrip() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = voucher_domain(421_614, VERIFYING);
        let channel_id = B256::repeat_byte(0xC1);
        let bytes_delivered = U256::from(1_048_576u64);

        let signed = Voucher {
            channel_id,
            amount: U256::from(10_000u64),
            nonce: U256::from(3u64),
            bytes_delivered,
            token: TOKEN,
        }
        .sign(&signer, &domain)?;

        let wire = signed_to_wire_voucher(&signed);
        anyhow::ensure!(wire.signature.len() == decdn_protocol::VOUCHER_SIG_LEN);

        let rebuilt = wire_voucher_to_signed(&wire, channel_id, TOKEN, bytes_delivered)?;
        anyhow::ensure!(rebuilt == signed, "bridge must preserve the signed voucher");
        rebuilt.verify_signer(signer.address(), &domain)?;
        Ok(())
    }

    #[test]
    fn bad_signature_length_rejected() -> anyhow::Result<()> {
        let wire = WireVoucher {
            signature: vec![0u8; 10],
            amount: [0u8; 32],
            nonce: [0u8; 32],
        };
        let err = wire_voucher_to_signed(&wire, B256::ZERO, TOKEN, U256::ZERO)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected BadSignature, got Ok"))?;
        anyhow::ensure!(matches!(err, WireVoucherError::BadSignature));
        Ok(())
    }

    #[test]
    fn amount_nonce_big_endian_preserved() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = voucher_domain(421_614, VERIFYING);
        let signed = Voucher {
            channel_id: B256::ZERO,
            amount: U256::from(0x0102_0304u64),
            nonce: U256::from(0xABu64),
            bytes_delivered: U256::ZERO,
            token: TOKEN,
        }
        .sign(&signer, &domain)?;

        let wire = signed_to_wire_voucher(&signed);
        // Big-endian: most-significant byte first, low byte last.
        anyhow::ensure!(wire.amount[31] == 0x04);
        anyhow::ensure!(wire.amount[30] == 0x03);
        anyhow::ensure!(wire.amount[0] == 0x00);
        anyhow::ensure!(wire.nonce[31] == 0xAB);
        Ok(())
    }

    /// Every `ChannelError` variant maps to a fixed reject reason — except the
    /// transient `Store`, which signals retry. Exhaustive by construction; if a
    /// new `ChannelError` variant lands, `voucher_reject_reason` fails to
    /// compile and this test is the reminder to extend the wire enum.
    #[test]
    fn voucher_reject_reason_is_exhaustive() {
        let cases = [
            (
                ChannelError::WrongChannel {
                    expected: B256::ZERO,
                    got: B256::ZERO,
                },
                Ok(VoucherRejectReason::WrongChannel),
            ),
            (
                ChannelError::WrongToken {
                    expected: TOKEN,
                    got: TOKEN,
                },
                Ok(VoucherRejectReason::WrongToken),
            ),
            (
                ChannelError::NonceNotIncreasing {
                    last: U256::ZERO,
                    got: U256::ZERO,
                },
                Ok(VoucherRejectReason::StaleNonce),
            ),
            (
                ChannelError::AmountDecreasing {
                    last: U256::ZERO,
                    got: U256::ZERO,
                },
                Ok(VoucherRejectReason::AmountRegression),
            ),
            (
                ChannelError::BytesDecreasing {
                    last: U256::ZERO,
                    got: U256::ZERO,
                },
                Ok(VoucherRejectReason::BytesRegression),
            ),
            (
                ChannelError::AmountExceedsDeposit {
                    deposit: U256::ZERO,
                    got: U256::ZERO,
                },
                Ok(VoucherRejectReason::InsufficientDeposit),
            ),
            (
                ChannelError::Signature(VoucherError::InvalidSignature),
                Ok(VoucherRejectReason::BadSignature),
            ),
            (
                ChannelError::Signature(VoucherError::WrongSigner {
                    expected: TOKEN,
                    recovered: TOKEN,
                }),
                Ok(VoucherRejectReason::WrongSigner),
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(voucher_reject_reason(&err), expected, "{err:?}");
        }

        // Store is transient — signals retry, not a wire rejection.
        let store_err = ChannelError::Store(StoreError::Io(std::io::Error::other("x")));
        assert_eq!(voucher_reject_reason(&store_err), Err(RetrySignal));
    }
}
