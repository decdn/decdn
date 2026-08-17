//! Rate enforcement for incremental voucher payments.
//!
//! Each voucher carries cumulative `amount` and `bytes_delivered`. Between
//! two vouchers on the same lane, the increments must satisfy
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
use decdn_protocol::client::VOUCHER_INTERVAL_BYTES;

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

/// Minimum payment (in token base units) required to cover `bytes` delivered
/// at `rate_per_mb`, i.e. `ceil(bytes * rate_per_mb / BYTES_PER_MB)`.
///
/// This is the exact lower bound [`verify_rate`] enforces at zero tolerance and
/// the same arithmetic the buyer's voucher signer uses to price an interval
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

/// One voucher-interval floor priced in `µUSDC` — the un-self-funded credit a fresh
/// lane draws before its first voucher (ADR 003 §Credit window / §Pool solvency).
#[must_use]
pub fn floor_micro(rate_per_mb: u64) -> U256 {
    min_payment(VOUCHER_INTERVAL_BYTES, rate_per_mb)
}

/// The unrecoverable floor loss a lane leaves at stream end: the value of its
/// still-unpaid delivered bytes, capped at one floor (everything above the floor is
/// self-funded and bounded by the ramped credit window). Saturating.
#[must_use]
pub fn dead_charge_micro(rate_per_mb: u64, delivered: u64, paid_bytes: u64) -> U256 {
    let unpaid = delivered.saturating_sub(paid_bytes);
    min_payment(unpaid, rate_per_mb).min(floor_micro(rate_per_mb))
}

/// Stateful-B pool solvency: the pool's remaining deposit minus the refundable
/// floor `M` must cover its already-committed concurrent floor credit plus this
/// stream's new reservation. Saturating (a `remaining` below `M` yields no
/// headroom rather than underflowing).
#[must_use]
pub fn pool_budget_covers(remaining: U256, m: U256, committed: U256, new_reserve: U256) -> bool {
    remaining.saturating_sub(m) >= committed.saturating_add(new_reserve)
}

/// Failure modes for [`verify_rate`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RateError {
    /// Voucher delta included zero bytes — cannot be priced. The node should
    /// reject it before it advances lane state.
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

    /// `verify_rate` at zero tolerance is the hard price floor the serving node
    /// applies after the advertised-rate check (#846): a positive `floor`
    /// rejects byte inflation with no slack, while a `floor` of `0` is inert
    /// (the default / free-serving config, where the always-`>= 1` on-chain
    /// floor is the authoritative enforcement instead).
    #[test]
    fn price_floor_rejects_byte_inflation() -> anyhow::Result<()> {
        let floor = 1u64;

        // The headline attack shape: 1 µUSDC stamped against 2^200 bytes. A zero
        // floor accepts it (inert); a positive floor at zero tolerance rejects.
        let inflated = U256::ONE << 200;
        verify_rate(U256::ONE, inflated, 0, 0)?; // floor 0: inert, passes
        let err = err_of(verify_rate(U256::ONE, inflated, floor, 0))?; // floor 1: rejects
        anyhow::ensure!(matches!(err, RateError::Underpayment { .. }), "{err:?}");

        // Exactly at the floor (1 µUSDC for 1 MB) passes with zero tolerance.
        verify_rate(U256::ONE, one_mb(), floor, 0)?;
        // One byte over the per-µUSDC budget for that amount is rejected — no
        // tolerance headroom, unlike the advertised-rate check's 1%.
        let err = err_of(verify_rate(U256::ONE, one_mb() + U256::ONE, floor, 0))?;
        anyhow::ensure!(matches!(err, RateError::Underpayment { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn min_payment_exact_megabyte() {
        // 1 MB at 100 µUSDC/MB costs exactly 100.
        assert_eq!(min_payment(BYTES_PER_MB, 100), U256::from(100u64));
        // 10 MB at 1000 costs 10_000.
        assert_eq!(min_payment(10 * BYTES_PER_MB, 1000), U256::from(10_000u64));
    }

    #[test]
    fn min_payment_sub_megabyte_rounds_up() {
        // 0.5 MB at 1000 → 500 exactly (divides evenly).
        assert_eq!(min_payment(BYTES_PER_MB / 2, 1000), U256::from(500u64));
        // 1 byte at rate 1 → ceil(1/1_048_576) = 1, never zero for a priced byte.
        assert_eq!(min_payment(1, 1), U256::ONE);
        // One byte over a whole MB rounds the partial MB up.
        assert_eq!(min_payment(BYTES_PER_MB + 1, 1), U256::from(2u64));
    }

    #[test]
    fn min_payment_zero_rate_is_free() {
        assert_eq!(min_payment(u64::MAX, 0), U256::ZERO);
    }

    #[test]
    fn min_payment_zero_bytes_is_zero() {
        assert_eq!(min_payment(0, 1_000_000), U256::ZERO);
    }

    #[test]
    fn min_payment_large_blob_no_overflow() {
        // A 64 GiB blob at a high rate stays well within U256 and does not panic.
        let bytes = 64u64 * 1024 * 1024 * 1024;
        let got = min_payment(bytes, 1_000_000);
        // ceil(bytes * 1e6 / MB) == bytes/MB * 1e6 for an exact-MB blob.
        assert_eq!(
            got,
            U256::from(bytes / BYTES_PER_MB) * U256::from(1_000_000u64)
        );
    }

    /// The deposit-guard contract (#856): `min_payment` is the lower bound a
    /// zero-tolerance `verify_rate` accepts, so paying exactly `min_payment`
    /// for the bytes always clears the rate floor.
    #[test]
    fn min_payment_clears_zero_tolerance_rate() -> anyhow::Result<()> {
        for &(bytes, rate) in &[
            (BYTES_PER_MB, 1000u64),
            (BYTES_PER_MB / 3, 777),
            (3 * BYTES_PER_MB + 17, 100),
        ] {
            let amount = min_payment(bytes, rate);
            verify_rate(amount, U256::from(bytes), rate, 0)?;
        }
        Ok(())
    }

    #[test]
    fn floor_micro_is_one_interval() {
        // 4 MiB interval at 100 µUSDC/MB = 4 * 100 = 400 µUSDC (4 MiB = 4 MB-units here per min_payment rounding).
        let f = floor_micro(100);
        assert_eq!(
            f,
            min_payment(decdn_protocol::client::VOUCHER_INTERVAL_BYTES, 100)
        );
        assert!(f > U256::ZERO);
    }

    #[test]
    fn dead_charge_is_proportional_and_capped_at_floor() {
        let rate = 1000;
        let floor = floor_micro(rate);
        // Fully repaid (delivered == paid) → zero dead charge.
        assert_eq!(
            dead_charge_micro(rate, 4 * 1024 * 1024, 4 * 1024 * 1024),
            U256::ZERO
        );
        // Full withhold of a whole interval → exactly the floor.
        assert_eq!(
            dead_charge_micro(rate, decdn_protocol::client::VOUCHER_INTERVAL_BYTES, 0),
            floor
        );
        // Early abort after a fraction of an interval → proportional, below the floor.
        let partial = dead_charge_micro(rate, 1024 * 1024, 0);
        assert!(partial > U256::ZERO && partial < floor);
        // Delivered beyond an interval but only a floor unpaid stays capped at the floor.
        let capped = dead_charge_micro(rate, 100 * 1024 * 1024, 96 * 1024 * 1024);
        assert!(capped <= floor);
    }

    #[test]
    fn pool_budget_covers_saturates_and_reserves_m() {
        let m = U256::from(1_000_000u64);
        let remaining = U256::from(1_000_400u64); // M + 400
        // committed 0, reserve 400 → exactly covered.
        assert!(pool_budget_covers(
            remaining,
            m,
            U256::ZERO,
            U256::from(400u64)
        ));
        // committed 400, another 1 → over budget.
        assert!(!pool_budget_covers(
            remaining,
            m,
            U256::from(400u64),
            U256::from(1u64)
        ));
        // remaining below M saturates to zero headroom, never panics.
        assert!(!pool_budget_covers(
            U256::from(500_000u64),
            m,
            U256::ZERO,
            U256::from(1u64)
        ));
    }
}
