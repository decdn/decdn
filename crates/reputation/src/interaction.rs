//! Shared interaction-score arithmetic (ADR 008 §Local Score Calculation).
//!
//! The local scorer ([`crate::local`]) scores a single interaction as
//! `0.4 * speed + 0.4 * correctness + 0.2 * reachability`. This module is the
//! one place that arithmetic lives.

use std::time::Duration;

/// The three component weights for an interaction score. Each is in `[0, 1]`
/// and the three sum to `1.0` (validated by the owning config).
#[derive(Debug, Clone, Copy)]
pub(crate) struct InteractionWeights {
    pub speed: f64,
    pub correctness: f64,
    pub reachability: f64,
}

/// Log-normalized delivery-speed score in `[0, 1]` (ADR 008 §Local Score
/// Calculation), computed from a byte count and the time it took to transfer
/// them. `reference_bps` is the throughput that scores ~1.0. Returns `0.0`
/// for non-positive or non-finite inputs (ADR 008 §Local Score Calculation
/// normalization).
// u64→f64 loses precision above 2^53 (~9 PB) — far above any plausible single
// transfer; the result is clamped to [0, 1] so any drift rounds within the
// saturated range and cannot escape the score envelope.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn speed_score_from_transfer(bytes: u64, elapsed: Duration, reference_bps: u64) -> f64 {
    let secs = elapsed.as_secs_f64();
    if !secs.is_finite() || secs <= 0.0 || reference_bps == 0 {
        return 0.0;
    }
    speed_score_from_bps(bytes as f64 / secs, reference_bps)
}

/// Log-normalized delivery-speed score in `[0, 1]` (ADR 008 §Local Score
/// Calculation). `reference_bps` is the throughput that scores ~1.0. The log
/// shape keeps throughput from saturating — a materially faster node scores
/// strictly higher. Returns
/// `0.0` for non-positive / non-finite input or a zero reference.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn speed_score_from_bps(bytes_per_sec: f64, reference_bps: u64) -> f64 {
    if !bytes_per_sec.is_finite() || bytes_per_sec <= 0.0 || reference_bps == 0 {
        return 0.0;
    }
    // `ln_1p(x)` computes `ln(1 + x)` without the catastrophic cancellation a
    // literal `(1.0 + x).ln()` suffers for very small `x` — so a near-zero-rate
    // transfer is scored accurately rather than swamped by float error.
    let denom = (reference_bps as f64).ln_1p();
    (bytes_per_sec.ln_1p() / denom).clamp(0.0, 1.0)
}

/// `w.speed * speed + w.correctness * correctness + w.reachability *
/// reachability` for component scores each already in `[0, 1]`.
pub(crate) fn interaction_score(
    w: InteractionWeights,
    speed: f64,
    correctness: f64,
    reachability: f64,
) -> f64 {
    w.speed * speed + w.correctness * correctness + w.reachability * reachability
}

#[cfg(test)]
mod tests {
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
}
