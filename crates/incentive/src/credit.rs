//! Ramped delivery credit window (ADR 003 §Credit window).

/// The ramped delivery credit window (ADR 003 §Credit window): the unbilled
/// egress a node fronts on a stream grows in proportion to what the stream has
/// already paid, floored at one chunk (`floor`) so the loop can always deliver
/// a full chunk and recoup it, and capped at `credit_max`. A `divisor`
/// of `0` opens the full `credit_max` from the first byte. The window is a pure
/// function of this stream's own `paid` bytes, so a non-paying stream stays pinned
/// at the floor and a paying stream ramps to the ceiling. The node's unbilled
/// exposure is exactly the returned window: `paid / divisor` once that clears the
/// `floor`, the `floor` itself below that point (including at `paid == 0`), and the
/// full `credit_max` when `divisor` is `0`.
#[must_use]
pub fn ramped_credit_window(divisor: u64, floor: u64, credit_max: u64, paid: u64) -> u64 {
    let ceiling = credit_max.max(floor);
    if divisor == 0 {
        return ceiling;
    }
    (paid / divisor).clamp(floor, ceiling)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::ramped_credit_window;

    const FLOOR: u64 = 1024 * 1024; // one 1 MiB chunk
    const MAX: u64 = 64 * 1024 * 1024;

    #[test]
    fn unpaid_stream_sits_at_the_floor() {
        assert_eq!(ramped_credit_window(2, FLOOR, MAX, 0), FLOOR);
    }

    #[test]
    fn window_is_paid_over_divisor_once_it_clears_the_floor() {
        // paid 32 MiB, divisor 2 -> 16 MiB, above the 1 MiB floor.
        assert_eq!(
            ramped_credit_window(2, FLOOR, MAX, 32 * 1024 * 1024),
            16 * 1024 * 1024
        );
    }

    #[test]
    fn window_is_pinned_to_floor_until_paid_exceeds_divisor_times_floor() {
        // paid 1 MiB, divisor 2 -> 512 KiB, below the floor -> floor.
        assert_eq!(ramped_credit_window(2, FLOOR, MAX, 1024 * 1024), FLOOR);
        // And exactly AT `divisor × floor` the ramp still ties the floor, so
        // the clamp — not the ratio — is what decides the boundary.
        assert_eq!(ramped_credit_window(2, FLOOR, MAX, 2 * 1024 * 1024), FLOOR);
    }

    #[test]
    fn window_caps_at_credit_max() {
        // paid 1 GiB, divisor 2 -> 512 MiB, capped to 64 MiB.
        assert_eq!(ramped_credit_window(2, FLOOR, MAX, 1024 * 1024 * 1024), MAX);
    }

    #[test]
    fn divisor_zero_is_instant_full_credit_max() {
        assert_eq!(ramped_credit_window(0, FLOOR, MAX, 0), MAX);
    }

    #[test]
    fn a_credit_max_below_the_floor_never_drops_below_the_floor() {
        // Misconfiguration: ceiling < floor. Progress floor wins.
        assert_eq!(ramped_credit_window(2, FLOOR, 1024, 1_000_000_000), FLOOR);
        assert_eq!(ramped_credit_window(0, FLOOR, 1024, 0), FLOOR);
    }
}
