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

/// One chunk prices to exactly the advertised per-MB rate with no rounding
/// at any rate, because `CHUNK_BYTES == BYTES_PER_MB` — the identity ADR 003
/// §Chunk sizing rests on.
#[test]
fn one_chunk_is_exactly_the_per_mb_rate() {
    let f = min_payment(decdn_protocol::client::CHUNK_BYTES, 100);
    assert_eq!(f, U256::from(100u64));
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
