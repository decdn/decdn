//! Bridge between `cdn/client/v1` wire vouchers and the incentive voucher
//! state machine.
//!
//! The protocol crate ([`decdn_protocol::Voucher`]) is crypto-free: a wire
//! voucher carries `{signature, amount, bytes_delivered, chain_root,
//! chunk_price}`, with `amount` and
//! `bytes_delivered` as `u64` cumulative totals matching the contract's
//! on-chain `uint64` storage. Validating it against a lane requires the full
//! EIP-712 typed data `{pool_id, signer, provider, amount, bytes_delivered,
//! chain_root, chunk_price}`
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
/// registered capability, and `provider` from its own identity. `amount`,
/// `bytes_delivered`, `chain_root` and `chunk_price` ride the wire.
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
            chain_root: B256::from(wire.chain_root),
            chunk_price: U256::from(wire.chunk_price),
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
/// Returns [`WireVoucherError::ValueExceedsWireWidth`] if `amount`,
/// `bytes_delivered` or `chunk_price` exceeds `u64::MAX` — the on-chain pool
/// caps all three at `uint64` (`chunk_price` rides inside the packed
/// `chainMeter` word), so such a voucher is unredeemable; it is refused rather
/// than truncated.
pub fn signed_to_wire_voucher(signed: &SignedVoucher) -> Result<WireVoucher, WireVoucherError> {
    let amount = u64::try_from(signed.voucher.amount)
        .map_err(|_| WireVoucherError::ValueExceedsWireWidth)?;
    let bytes_delivered = u64::try_from(signed.voucher.bytes_delivered)
        .map_err(|_| WireVoucherError::ValueExceedsWireWidth)?;
    let chunk_price = u64::try_from(signed.voucher.chunk_price)
        .map_err(|_| WireVoucherError::ValueExceedsWireWidth)?;
    Ok(WireVoucher {
        signature: signed.signature.as_bytes().to_vec(),
        amount,
        bytes_delivered,
        chain_root: signed.voucher.chain_root.into(),
        chunk_price,
    })
}

/// Map an off-chain [`PoolError`] to the wire [`VoucherRejectReason`] a node
/// returns mid-stream, or [`RetrySignal`] when the failure is transient.
///
/// **Exhaustive, no wildcard (ADR 005 §Mirror obligation).** A new `PoolError`
/// variant breaks this match at compile time, forcing a coordinated update of
/// [`VoucherRejectReason`] and the retry-semantics table.
///
/// `BadPreimage` bridges too — the hash-chain walk is a lane-state check like
/// the monotonicity guards, so it belongs in the same taxonomy. The other three
/// chain reasons have no `PoolError` counterpart by design:
/// `ChainIndexZero` and `UnanchoredPreimage` are decided against the
/// *per-stream* anchor, and `ChunkPriceMismatch` against the node's own quoted
/// rate — none of which the lane-scoped validation enum can see, so the
/// `cdn/client/v1` handler raises those directly.
///
/// [`PoolError::Store`] is the only transient failure: in-memory state did not
/// advance, so it returns [`RetrySignal`] rather than a permanent reason. The
/// `cdn/client/v1` handler surfaces that signal by aborting the stream (no
/// in-band wire reason), so the client resends the **same** voucher on a
/// fresh stream (ADR 003 §Off-chain voucher state persistence). Every other
/// variant is a permanent rejection.
pub const fn voucher_reject_reason(err: &PoolError) -> Result<VoucherRejectReason, RetrySignal> {
    match err {
        PoolError::WrongPool { .. } => Ok(VoucherRejectReason::WrongPool),
        PoolError::WrongProvider { .. } => Ok(VoucherRejectReason::WrongProvider),
        PoolError::AmountRegression { .. } => Ok(VoucherRejectReason::AmountRegression),
        PoolError::UnderFold { .. } => Ok(VoucherRejectReason::UnderFold),
        PoolError::BytesRegression { .. } => Ok(VoucherRejectReason::BytesRegression),
        PoolError::CapExceeded { .. } => Ok(VoucherRejectReason::SpendingCapExhausted),
        PoolError::BadPreimage { .. } => Ok(VoucherRejectReason::BadPreimage),
        PoolError::Signature(VoucherError::InvalidSignature) => {
            Ok(VoucherRejectReason::BadSignature)
        }
        PoolError::Signature(VoucherError::WrongSigner { .. }) => {
            Ok(VoucherRejectReason::WrongSigner)
        }
        // Transient — in-memory state unchanged. The handler aborts the stream
        // rather than emitting an in-band wire reason; the client resends the
        // same voucher (ADR 003 §Off-chain voucher state persistence).
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
/// state did not advance, so the caller aborts the stream (no in-band wire
/// reason) and the client resends the **same** voucher on a fresh stream (ADR
/// 003) — distinguishable from a permanent rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrySignal;

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed pair up clearly here
mod tests;

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
mod prop_tests;
