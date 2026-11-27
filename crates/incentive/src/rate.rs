//! Rate enforcement for incremental voucher payments.
//!
//! Each voucher carries cumulative `amount` and `bytes_delivered`. Between
//! two vouchers on the same lane, the increments must satisfy
//! `amount_delta / bytes_delta >= rate_per_mb`: the client paid at least the
//! advertised rate for the bytes delivered. The check is exact. The client
//! prices each span as `ceil(bytes * rate_per_mb / 1_048_576)`, so an honest
//! voucher never falls short, and any shortfall marks a faulty or hostile
//! client.
//!
//! Rate units, all matching ADR 005 / ADR 003:
//! - `amount` is in token base units (`µUSDC` = 10⁻⁶ `USDC`)
//! - `bytes_delivered` is in bytes
//! - `rate_per_mb` is in token base units per **megabyte**, where 1 `MB` =
//!   1,048,576 bytes (binary `MB`, ADR 005)

use alloy::primitives::U256;

/// 1 `MB` in bytes: the unit every `rate_per_mb` prices. deCDN's `MB` is a
/// mebibyte (2^20 bytes).
pub const BYTES_PER_MB: u64 = 1_048_576;

/// Verify that paying `amount_delta` for `bytes_delta` bytes meets
/// `rate_per_mb` exactly, with no shortfall.
///
/// # Errors
///
/// - [`RateError::ZeroBytes`] — `bytes_delta == 0`. A voucher that adds no
///   bytes cannot be priced and is silently invalid.
/// - [`RateError::Underpayment`] — effective rate is below `rate_per_mb`.
/// - [`RateError::Overflow`] — `amount_delta * BYTES_PER_MB` would exceed
///   `U256::MAX`. Will not occur in practice (would require ~10⁷¹ `µUSDC`,
///   far more than total `USDC` supply).
pub fn verify_rate(
    amount_delta: U256,
    bytes_delta: U256,
    rate_per_mb: u64,
) -> Result<(), RateError> {
    if bytes_delta.is_zero() {
        return Err(RateError::ZeroBytes);
    }

    // Required micro-USDC for `bytes_delta` at exactly `rate_per_mb`:
    //     required = bytes_delta * rate_per_mb / BYTES_PER_MB
    // Cross-multiplied to stay in integers:
    //     amount_delta * BYTES_PER_MB >= bytes_delta * rate_per_mb
    let lhs = amount_delta
        .checked_mul(U256::from(BYTES_PER_MB))
        .ok_or(RateError::Overflow)?;
    let rhs = bytes_delta
        .checked_mul(U256::from(rate_per_mb))
        .ok_or(RateError::Overflow)?;

    if lhs >= rhs {
        Ok(())
    } else {
        Err(RateError::Underpayment {
            paid: amount_delta,
            bytes: bytes_delta,
            rate_per_mb,
        })
    }
}

/// Minimum payment (in token base units) required to cover `bytes` delivered
/// at `rate_per_mb`, i.e. `ceil(bytes * rate_per_mb / BYTES_PER_MB)`.
///
/// This is the exact lower bound [`verify_rate`] enforces and the same
/// arithmetic the buyer's voucher signer uses to price an interval
/// (`bytes * rate / MB`, rounded up). It is intended for pre-flight cost
/// estimation — e.g. gating a speculative pull on the requesting lane's
/// remaining deposit covering the worst-case blob cost (#856).
///
/// Saturating on the (practically unreachable) multiply overflow: a saturated
/// `U256::MAX` ceiling makes any finite deposit look insufficient, which is the
/// safe, conservative direction for a guard.
#[must_use]
pub fn min_payment(bytes: u64, rate_per_mb: u64) -> U256 {
    U256::from(bytes)
        .saturating_mul(U256::from(rate_per_mb))
        .div_ceil(U256::from(BYTES_PER_MB))
}

/// Failure modes for [`verify_rate`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RateError {
    /// Voucher delta included zero bytes — cannot be priced. The node should
    /// reject it before it advances lane state.
    #[error("voucher carries zero bytes_delta — cannot enforce rate")]
    ZeroBytes,
    /// Effective rate is below the advertised rate. The node should pause the
    /// stream and (per ADR 003 §Voucher withholding) refuse further delivery.
    #[error("underpayment: paid {paid} for {bytes} bytes against rate {rate_per_mb}/MB")]
    Underpayment {
        /// The voucher's amount delta.
        paid: U256,
        /// The voucher's bytes delta, priced against that amount.
        bytes: U256,
        /// The advertised per-MB rate the payment is judged against.
        rate_per_mb: u64,
    },
    /// Arithmetic would overflow U256. Will not occur for realistic values
    /// but surfaced as an error rather than panicking, per the workspace
    /// anti-panic policy.
    #[error("rate enforcement arithmetic overflowed U256")]
    Overflow,
}

#[cfg(test)]
mod tests;
