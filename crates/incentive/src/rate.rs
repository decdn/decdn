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
use decdn_protocol::client::CHUNK_BYTES;

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

/// One-chunk floor priced in `µUSDC` — the un-self-funded credit a fresh lane
/// draws before its first proof (ADR 003 §Credit window / §Pool solvency).
///
/// The credit window is floored at one chunk so a stream can always make
/// progress: deliver a full chunk, then recoup it. Because `CHUNK_BYTES ==
/// BYTES_PER_MB`, this is exactly `rate_per_mb` — one chunk costs one MB of
/// price, by identity (ADR 003 §Chunk Cadence).
#[must_use]
pub fn floor_micro(rate_per_mb: u64) -> U256 {
    min_payment(CHUNK_BYTES, rate_per_mb)
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
        /// The voucher's amount delta.
        paid: U256,
        /// The voucher's bytes delta, priced against that amount.
        bytes: U256,
        /// The advertised per-MB rate the payment is judged against.
        rate_per_mb: u64,
        /// Allowed shortfall, in basis points of the expected amount.
        tolerance_bps: u64,
    },
    /// Arithmetic would overflow U256. Will not occur for realistic values
    /// but surfaced as an error rather than panicking, per the workspace
    /// anti-panic policy.
    #[error("rate enforcement arithmetic overflowed U256")]
    Overflow,
}

#[cfg(test)]
mod tests;
