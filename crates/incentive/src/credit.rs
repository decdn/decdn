//! Ramped delivery credit window (ADR 003 §Credit window).

/// The ramped delivery credit window (ADR 003 §Credit window): the unbilled
/// egress a node fronts on a stream grows in proportion to `paid`, floored at one
/// chunk (`floor`) so the loop can always deliver a full chunk and recoup it, and
/// capped at `credit_max`. A `divisor` of `0` opens the full `credit_max` from the
/// first byte. `paid` is the ramp input the caller supplies: the stream's own
/// confirmed payment plus the ramp credit it carries from earlier streams on its
/// lane. A non-paying stream on a lane with no credit stays pinned at the floor,
/// and a paying stream ramps to the ceiling. The node's unbilled exposure is
/// exactly the returned window: `paid / divisor` once that clears the
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
mod tests;
