use super::*;

fn approx(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
#[allow(clippy::cast_precision_loss)]
fn speed_score_is_log_scaled_monotonic_and_bounded() {
    let r = 1024 * 1024 * 1024; // 1 GiB/s reference → ~1.0
    let a = speed_score_from_bps(1.0 * 1024.0 * 1024.0, r);
    let b = speed_score_from_bps(10.0 * 1024.0 * 1024.0, r);
    let c = speed_score_from_bps(100.0 * 1024.0 * 1024.0, r);
    assert!(0.0 < a && a < b && b < c && c < 1.0, "a={a} b={b} c={c}");
    // The log curve keeps rising well below the reference: a 10× faster node
    // scores strictly higher, by a real margin.
    assert!(c > b + 0.05, "no real differentiation: b={b} c={c}");
    // Reference throughput scores ~1.0; above it clamps to 1.0.
    assert!(approx(speed_score_from_bps(r as f64, r), 1.0));
    assert!(approx(speed_score_from_bps(2.0 * r as f64, r), 1.0));
    assert!(approx(speed_score_from_bps(0.0, r), 0.0));
    assert!(approx(speed_score_from_bps(100.0, 0), 0.0)); // zero reference
    assert!(approx(
        speed_score_from_transfer(r, Duration::from_secs(1), r),
        1.0
    ));
    assert!(approx(
        speed_score_from_transfer(0, Duration::from_secs(1), r),
        0.0
    ));
    assert!(approx(speed_score_from_transfer(1, Duration::ZERO, r), 0.0));
    assert!(approx(
        speed_score_from_transfer(1, Duration::from_secs(1), 0),
        0.0
    ));
}
