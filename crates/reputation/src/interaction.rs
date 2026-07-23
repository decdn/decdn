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

/// `min(1, actual_bps / expected_bps)` computed from a byte count and the time
/// it took to transfer them. Returns `0.0` for non-positive or non-finite
/// inputs (ADR 008 §Local Score Calculation normalization).
// u64→f64 loses precision above 2^53 (~9 PB) — far above any plausible single
// transfer; the result is clamped to [0, 1] so any drift rounds within the
// saturated range and cannot escape the score envelope.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn speed_score_from_transfer(bytes: u64, elapsed: Duration, expected_bps: u64) -> f64 {
    let secs = elapsed.as_secs_f64();
    if !secs.is_finite() || secs <= 0.0 || expected_bps == 0 {
        return 0.0;
    }
    speed_score_from_bps(bytes as f64 / secs, expected_bps)
}

/// `min(1, bytes_per_sec / expected_bps)` from an already-computed delivery
/// rate. Returns `0.0` for non-positive or non-finite inputs.
// expected_bps fits f64 exactly for any realistic baseline; the ratio is
// clamped to [0, 1].
#[allow(clippy::cast_precision_loss)]
pub(crate) fn speed_score_from_bps(bytes_per_sec: f64, expected_bps: u64) -> f64 {
    if !bytes_per_sec.is_finite() || bytes_per_sec <= 0.0 || expected_bps == 0 {
        return 0.0;
    }
    (bytes_per_sec / expected_bps as f64).clamp(0.0, 1.0)
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

    const BPS: u64 = 10 * 1024 * 1024;

    #[test]
    fn speed_from_transfer_matches_expected() {
        let one = Duration::from_secs(1);
        assert!(approx(speed_score_from_transfer(BPS, one, BPS), 1.0));
        assert!(approx(speed_score_from_transfer(2 * BPS, one, BPS), 1.0));
        assert!(approx(speed_score_from_transfer(BPS / 10, one, BPS), 0.1));
        assert!(approx(speed_score_from_transfer(0, one, BPS), 0.0));
        assert!(approx(
            speed_score_from_transfer(1, Duration::ZERO, BPS),
            0.0
        ));
        assert!(approx(speed_score_from_transfer(1, one, 0), 0.0));
    }
}
