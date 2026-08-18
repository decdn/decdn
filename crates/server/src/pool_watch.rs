//! Low-water pool top-up sweep (replaces the channel reclaim sweep). Every
//! tick, read the pool's remaining balance and, if it is below the low-water
//! mark, top it up from the treasury by the configured refill amount.

use std::time::Duration;

use alloy::primitives::B256;

use crate::money::MicroUsdc;
use crate::state::AppState;

/// Whether `remaining` has dropped below `low_water`.
#[must_use]
pub fn needs_refill(remaining: u64, low_water: u64) -> bool {
    remaining < low_water
}

/// Run the sweep forever on `interval`. Never panics the loop — a failed tick
/// is logged and retried next interval.
pub async fn run(
    state: AppState,
    interval: Duration,
    low_water: MicroUsdc,
    refill: MicroUsdc,
    pool_id: B256,
) {
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        match state.treasury.remaining(pool_id).await {
            Ok(remaining) => {
                if needs_refill(remaining.0, low_water.0) {
                    match state.treasury.top_up(pool_id, refill).await {
                        Ok(credited) => tracing::info!(
                            pool = %pool_id, remaining = remaining.0, credited = credited.0,
                            "pool topped up"
                        ),
                        Err(e) => tracing::warn!(pool = %pool_id, "pool top-up failed: {e}"),
                    }
                }
            }
            Err(e) => tracing::warn!(pool = %pool_id, "pool remaining read failed: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::needs_refill;

    #[test]
    fn refills_only_below_low_water() {
        assert!(needs_refill(19_999_999, 20_000_000));
        assert!(!needs_refill(20_000_000, 20_000_000));
        assert!(!needs_refill(50_000_000, 20_000_000));
    }
}
