use super::*;

fn approx(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
#[allow(clippy::cast_precision_loss)]
fn speed_score_does_not_saturate_above_the_old_baseline() {
    // Old curve pinned everything above 10 MiB/s to 1.0. The log curve must
    // keep rising: a 10× faster node scores strictly higher, by a real margin.
    let ref_bps = 1024 * 1024 * 1024; // 1 GiB/s reference → ~1.0
    let slow = speed_score_from_bps(10.0 * 1024.0 * 1024.0, ref_bps); // 10 MiB/s
    let fast = speed_score_from_bps(100.0 * 1024.0 * 1024.0, ref_bps); // 100 MiB/s
    assert!(
        fast > slow + 0.05,
        "no real differentiation: slow={slow} fast={fast}"
    );
    assert!(
        slow > 0.0 && fast < 1.0,
        "both should be interior: slow={slow} fast={fast}"
    );
    // Reference throughput scores ~1.0; above it clamps to 1.0.
    assert!(approx(speed_score_from_bps(ref_bps as f64, ref_bps), 1.0));
    assert!(approx(
        speed_score_from_bps(2.0 * ref_bps as f64, ref_bps),
        1.0
    ));
    // Monotonic and bounded at the bottom.
    assert!(
        speed_score_from_bps(1.0 * 1024.0 * 1024.0, ref_bps)
            < speed_score_from_bps(10.0 * 1024.0 * 1024.0, ref_bps)
    );
    assert!(approx(speed_score_from_bps(0.0, ref_bps), 0.0));
    assert!(approx(speed_score_from_bps(100.0, 0), 0.0)); // zero reference
}

#[test]
fn speed_score_is_monotonic_and_bounded() {
    let r = 1024 * 1024 * 1024; // 1 GiB/s
    let a = speed_score_from_bps(1.0 * 1024.0 * 1024.0, r);
    let b = speed_score_from_bps(10.0 * 1024.0 * 1024.0, r);
    let c = speed_score_from_bps(100.0 * 1024.0 * 1024.0, r);
    assert!(0.0 < a && a < b && b < c && c < 1.0, "a={a} b={b} c={c}");
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
