//! Rate enforcement for incremental voucher payments.
//!
//! Each voucher carries cumulative `amount` and `bytes_delivered`. Between
//! two vouchers on the same channel, the increments must satisfy
//! `amount_delta / bytes_delta >= rate_per_mb` — i.e. the client paid at
//! least the advertised rate for the bytes delivered. Underpayment beyond a
//! tolerance threshold (rounding error, edge-MB padding) indicates a faulty
//! or hostile client.
//!
//! Rate units, all matching ADR 005 / ADR 003:
//! - `amount` is in token base units (`µUSDC` = 10⁻⁶ `USDC`)
//! - `bytes_delivered` is in bytes
//! - `rate_per_mb` is in token base units per **megabyte**, where 1 `MB` =
//!   1,048,576 bytes (binary `MB`, ADR 005)
//!
//! Tolerance is expressed in basis points (bps); 100 bps = 1%. The default
//! [`DEFAULT_TOLERANCE_BPS`] of 100 leaves headroom for the rounding the
//! client does when computing `amount = ceil(bytes / 1_048_576) * rate`.

use alloy::primitives::U256;

/// 1 `MB` in bytes per ADR 005 §Probe-Triggered Eviction Hold's `MB`
/// definition.
pub const BYTES_PER_MB: u64 = 1_048_576;

/// Basis-points scale: 100% = `10_000` `bps`.
pub const BPS_SCALE: u64 = 10_000;

/// Default underpayment tolerance — 1%. Calibrated against the client-side
/// rounding `amount = ceil(bytes / 1_048_576) * rate_per_mb`: at 1 MB
/// granularity the worst-case underpayment is < 1 byte's-worth of rate, so
/// 100 bps leaves >>10× headroom.
pub const DEFAULT_TOLERANCE_BPS: u64 = 100;

/// Verify that paying `amount_delta` for `bytes_delta` bytes meets
/// `rate_per_mb` within `tolerance_bps`.
///
/// # Errors
///
/// - [`RateError::ZeroBytes`] — `bytes_delta == 0`. A voucher that adds no
///   bytes cannot be priced and is silently invalid.
/// - [`RateError::Underpayment`] — effective rate is below `rate_per_mb` by
///   more than `tolerance_bps` basis points.
/// - [`RateError::Overflow`] — `amount_delta * BYTES_PER_MB * BPS_SCALE`
///   would exceed `U256::MAX`. Will not occur in practice (would require
///   ~10⁶³ `µUSDC`, far more than total `USDC` supply).
pub fn verify_rate(
    amount_delta: U256,
    bytes_delta: U256,
    rate_per_mb: u64,
    tolerance_bps: u64,
) -> Result<(), RateError> {
    if bytes_delta.is_zero() {
        return Err(RateError::ZeroBytes);
    }

    // Required micro-USDC for `bytes_delta` at exactly `rate_per_mb`:
    //     required = bytes_delta * rate_per_mb / BYTES_PER_MB
    // Allowed slack:
    //     min_paid = required * (BPS_SCALE - tolerance_bps) / BPS_SCALE
    //
    // We avoid the float divide by cross-multiplying:
    //     amount_delta * BYTES_PER_MB * BPS_SCALE
    //         >= bytes_delta * rate_per_mb * (BPS_SCALE - tolerance_bps)
    //
    // Both sides fit comfortably in U256 for any plausible network values.
    let tolerance = tolerance_bps.min(BPS_SCALE);
    let allowed_bps = BPS_SCALE - tolerance;

    let lhs = amount_delta
        .checked_mul(U256::from(BYTES_PER_MB))
        .and_then(|v| v.checked_mul(U256::from(BPS_SCALE)))
        .ok_or(RateError::Overflow)?;
    let rhs = bytes_delta
        .checked_mul(U256::from(rate_per_mb))
        .and_then(|v| v.checked_mul(U256::from(allowed_bps)))
        .ok_or(RateError::Overflow)?;

    if lhs >= rhs {
        Ok(())
    } else {
        Err(RateError::Underpayment {
            paid: amount_delta,
            bytes: bytes_delta,
            rate_per_mb,
            tolerance_bps: tolerance,
        })
    }
}

/// Failure modes for [`verify_rate`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RateError {
    /// Voucher delta included zero bytes — cannot be priced. The node should
    /// reject it before it advances channel state.
    #[error("voucher carries zero bytes_delta — cannot enforce rate")]
    ZeroBytes,
    /// Effective rate is below the advertised rate by more than the
    /// configured tolerance. The node should pause the stream and (per
    /// ADR 003 §Voucher withholding) refuse further delivery.
    #[error(
        "underpayment: paid {paid} for {bytes} bytes against rate {rate_per_mb}/MB \
         with {tolerance_bps} bps tolerance"
    )]
    Underpayment {
        paid: U256,
        bytes: U256,
        rate_per_mb: u64,
        tolerance_bps: u64,
    },
    /// Arithmetic would overflow U256. Will not occur for realistic values
    /// but surfaced as an error rather than panicking, per the workspace
    /// anti-panic policy.
    #[error("rate enforcement arithmetic overflowed U256")]
    Overflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: 1 `MB` delivered at the given rate is exactly `rate` `µUSDC`.
    fn one_mb() -> U256 {
        U256::from(BYTES_PER_MB)
    }

    fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
        r.err()
            .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
    }

    #[test]
    fn exact_rate_accepted() -> anyhow::Result<()> {
        // 1 MB at rate 100 µUSDC/MB → 100 µUSDC
        verify_rate(U256::from(100u64), one_mb(), 100, DEFAULT_TOLERANCE_BPS)?;
        Ok(())
    }

    #[test]
    fn overpayment_accepted() -> anyhow::Result<()> {
        // 1 MB at rate 100 → 200 µUSDC paid (client overpaid; that's fine)
        verify_rate(U256::from(200u64), one_mb(), 100, DEFAULT_TOLERANCE_BPS)?;
        Ok(())
    }

    #[test]
    fn within_tolerance_accepted() -> anyhow::Result<()> {
        // 1 MB at rate 1000 → required 1000 µUSDC, paid 991 (0.9% under)
        verify_rate(U256::from(991u64), one_mb(), 1000, DEFAULT_TOLERANCE_BPS)?;
        Ok(())
    }

    #[test]
    fn underpayment_beyond_tolerance_rejected() -> anyhow::Result<()> {
        // 1 MB at rate 1000 → required 1000 µUSDC, paid 980 (2% under, default tol 1%)
        let err = err_of(verify_rate(
            U256::from(980u64),
            one_mb(),
            1000,
            DEFAULT_TOLERANCE_BPS,
        ))?;
        anyhow::ensure!(matches!(err, RateError::Underpayment { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn zero_bytes_rejected() -> anyhow::Result<()> {
        let err = err_of(verify_rate(
            U256::from(1u64),
            U256::ZERO,
            100,
            DEFAULT_TOLERANCE_BPS,
        ))?;
        anyhow::ensure!(matches!(err, RateError::ZeroBytes), "{err:?}");
        Ok(())
    }

    #[test]
    fn zero_amount_with_bytes_rejected_unless_rate_is_zero() -> anyhow::Result<()> {
        // Free serving (rate 0) makes any payment legal.
        verify_rate(U256::ZERO, one_mb(), 0, DEFAULT_TOLERANCE_BPS)?;

        // Rate > 0 with amount 0 underpays.
        let err = err_of(verify_rate(
            U256::ZERO,
            one_mb(),
            100,
            DEFAULT_TOLERANCE_BPS,
        ))?;
        anyhow::ensure!(matches!(err, RateError::Underpayment { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn sub_megabyte_proportional() -> anyhow::Result<()> {
        // 0.5 MB at rate 1000 → required 500 µUSDC, paid 500.
        verify_rate(
            U256::from(500u64),
            U256::from(BYTES_PER_MB / 2),
            1000,
            DEFAULT_TOLERANCE_BPS,
        )?;
        Ok(())
    }

    #[test]
    fn multi_mb_proportional() -> anyhow::Result<()> {
        // 10 MB at rate 1000 → required 10_000.
        verify_rate(
            U256::from(10_000u64),
            U256::from(10 * BYTES_PER_MB),
            1000,
            DEFAULT_TOLERANCE_BPS,
        )?;
        Ok(())
    }

    #[test]
    fn zero_tolerance_strict() -> anyhow::Result<()> {
        // With 0 bps tolerance, 999/1000 is rejected.
        let err = err_of(verify_rate(U256::from(999u64), one_mb(), 1000, 0))?;
        anyhow::ensure!(matches!(err, RateError::Underpayment { .. }), "{err:?}");
        // Exact match still passes.
        verify_rate(U256::from(1_000u64), one_mb(), 1000, 0)?;
        Ok(())
    }

    #[test]
    fn full_tolerance_accepts_anything() -> anyhow::Result<()> {
        // 10_000 bps tolerance == 100% — any non-zero delta passes the rate
        // check (the underpayment lower bound becomes zero).
        verify_rate(
            U256::from(1u64),
            U256::from(10u64.pow(15)),
            1_000_000,
            10_000,
        )?;
        Ok(())
    }

    #[test]
    fn tolerance_above_100_percent_clamped() -> anyhow::Result<()> {
        // Tolerance values above 10_000 bps should be clamped to 100%
        // tolerance, never wrap or trigger spurious overflow.
        verify_rate(U256::ZERO, one_mb(), 1_000, 50_000)?;
        Ok(())
    }
}
