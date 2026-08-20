//! Live operator fee-share (basis points), read from `FeeRouter.getShares()[0]`.
//! Mirrors `rate_bounds.rs`: a single `Arc<AtomicU16>` shared by all clones,
//! seeded at startup and refreshed by the multiplexed log poller (see
//! `fee_shares_watcher.rs`). `(1 - f)` numerator for the serve-economics margin.

use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};

use alloy::primitives::U256;

const BPS_DENOMINATOR: u64 = 10_000;

/// Cloneable live operator fee share. All clones share one atomic cell.
#[derive(Clone, Debug)]
pub struct OperatorShares {
    bps: Arc<AtomicU16>,
}

impl OperatorShares {
    /// Seed the cell, typically from the startup `getShares()` read.
    #[must_use]
    pub fn new(bps: u16) -> Self {
        Self {
            bps: Arc::new(AtomicU16::new(bps)),
        }
    }

    /// Current operator fee share, in basis points.
    #[must_use]
    pub fn bps(&self) -> u16 {
        self.bps.load(Ordering::Relaxed)
    }

    /// Publish a new operator share — called by the fee-shares watcher on a
    /// `SharesUpdated` event and by the periodic authoritative re-read.
    pub fn store(&self, bps: u16) {
        self.bps.store(bps, Ordering::Relaxed);
    }
}

/// Narrow the on-chain `[operator, buyback, treasury]` shares to operator bps.
///
/// Fail-closed: an out-of-range or unnarrowable operator share is a hard
/// error, not a clamp, so a malformed read never silently understates the
/// skim.
pub fn operator_bps_from_shares(shares: [U256; 3], addr: &str) -> anyhow::Result<u16> {
    let operator = shares.first().copied().unwrap_or(U256::ZERO);
    let raw = u64::try_from(operator).map_err(|_| {
        anyhow::anyhow!("FeeRouter operator share {operator} at {addr} exceeds u64")
    })?;
    anyhow::ensure!(
        raw <= BPS_DENOMINATOR,
        "FeeRouter operator share {raw} at {addr} exceeds {BPS_DENOMINATOR} bps"
    );
    // raw <= BPS_DENOMINATOR (10_000), so this narrowing is always lossless.
    u16::try_from(raw)
        .map_err(|_| anyhow::anyhow!("FeeRouter operator share {raw} at {addr} exceeds u16"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloy::primitives::U256;

    use super::*;

    #[test]
    fn shares_narrow_to_operator_bps() {
        let shares = [
            U256::from(6000u64),
            U256::from(3000u64),
            U256::from(1000u64),
        ];
        assert_eq!(operator_bps_from_shares(shares, "0xrouter").unwrap(), 6000);
    }

    #[test]
    fn shares_above_denominator_are_rejected() {
        let shares = [U256::from(10_001u64), U256::ZERO, U256::ZERO];
        assert!(operator_bps_from_shares(shares, "0xrouter").is_err());
    }

    #[test]
    fn cell_round_trips_and_is_shared_across_clones() {
        let cell = OperatorShares::new(6000);
        let clone = cell.clone();
        cell.store(4000);
        assert_eq!(clone.bps(), 4000);
    }
}
