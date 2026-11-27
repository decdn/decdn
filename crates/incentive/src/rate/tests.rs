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
    verify_rate(U256::from(100u64), one_mb(), 100)?;
    Ok(())
}

#[test]
fn overpayment_accepted() -> anyhow::Result<()> {
    // 1 MB at rate 100 → 200 µUSDC paid (client overpaid; that's fine)
    verify_rate(U256::from(200u64), one_mb(), 100)?;
    Ok(())
}

/// A voucher 1% short of the quoted rate is refused (#2851): the check has no
/// slack, so a payer cannot shave a percent off every span.
#[test]
fn one_percent_short_rejected() -> anyhow::Result<()> {
    // 1 MB at rate 1000 → required 1000 µUSDC, paid 990 (1% under).
    let err = err_of(verify_rate(U256::from(990u64), one_mb(), 1000))?;
    anyhow::ensure!(matches!(err, RateError::Underpayment { .. }), "{err:?}");
    // One base unit short is refused too.
    let err = err_of(verify_rate(U256::from(999u64), one_mb(), 1000))?;
    anyhow::ensure!(matches!(err, RateError::Underpayment { .. }), "{err:?}");
    Ok(())
}

#[test]
fn zero_bytes_rejected() -> anyhow::Result<()> {
    let err = err_of(verify_rate(U256::from(1u64), U256::ZERO, 100))?;
    anyhow::ensure!(matches!(err, RateError::ZeroBytes), "{err:?}");
    Ok(())
}

#[test]
fn zero_amount_with_bytes_rejected_unless_rate_is_zero() -> anyhow::Result<()> {
    // Free serving (rate 0) makes any payment legal.
    verify_rate(U256::ZERO, one_mb(), 0)?;

    // Rate > 0 with amount 0 underpays.
    let err = err_of(verify_rate(U256::ZERO, one_mb(), 100))?;
    anyhow::ensure!(matches!(err, RateError::Underpayment { .. }), "{err:?}");
    Ok(())
}

#[test]
fn sub_megabyte_proportional() -> anyhow::Result<()> {
    // 0.5 MB at rate 1000 → required 500 µUSDC, paid 500.
    verify_rate(U256::from(500u64), U256::from(BYTES_PER_MB / 2), 1000)?;
    Ok(())
}

#[test]
fn multi_mb_proportional() -> anyhow::Result<()> {
    // 10 MB at rate 1000 → required 10_000.
    verify_rate(U256::from(10_000u64), U256::from(10 * BYTES_PER_MB), 1000)?;
    Ok(())
}

/// Byte inflation is refused: 1 `µUSDC` stamped against 2^200 bytes passes only
/// at rate 0, and one byte over the per-µUSDC budget is refused at rate 1.
#[test]
fn byte_inflation_rejected() -> anyhow::Result<()> {
    let inflated = U256::ONE << 200;
    verify_rate(U256::ONE, inflated, 0)?;
    let err = err_of(verify_rate(U256::ONE, inflated, 1))?;
    anyhow::ensure!(matches!(err, RateError::Underpayment { .. }), "{err:?}");

    verify_rate(U256::ONE, one_mb(), 1)?;
    let err = err_of(verify_rate(U256::ONE, one_mb() + U256::ONE, 1))?;
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

/// The honest-payer contract: `min_payment` is the round-up price the client
/// signs, so paying exactly `min_payment` for any byte count clears the exact
/// `verify_rate`.
#[test]
fn min_payment_clears_rate_check() -> anyhow::Result<()> {
    for &(bytes, rate) in &[
        (BYTES_PER_MB, 1000u64),
        (BYTES_PER_MB / 3, 777),
        (3 * BYTES_PER_MB + 17, 100),
        (1, 1),
        (BYTES_PER_MB - 1, 3),
    ] {
        let amount = min_payment(bytes, rate);
        verify_rate(amount, U256::from(bytes), rate)?;
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
