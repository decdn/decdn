//! Live per-MB delivery-rate floor (#1172, ADR 019 §3.1 / ADR 003).
//!
//! The seller raises its advertised `rate_per_mb` to a governance-set floor
//! before signing a `ProbeResponse` / `StreamResponse` and enforces the same
//! floor again at voucher settlement. The floor lives on-chain in
//! `PaymentChannel.getRateBounds()`; the node reads it once at startup and then
//! tracks `RateBoundsUpdated` events so a governance retune reaches a running
//! node without a restart.
//!
//! There is no governance ceiling. A seller self-clamping its own advertised
//! rate *downward* buys no on-chain safety — a seller never wants to charge
//! less — and buyer protection is the buyer seeing the signed rate in
//! `StreamResponse` before it pays. The absolute upper bound is the wire
//! constant [`decdn_protocol::MAX_RATE_PER_MB`], enforced at message
//! validation, not a tunable band.
//!
//! [`RateBounds`] is the shared handle both the probe/client handlers and the
//! rate-bounds watcher hold. A single value needs no cross-field consistency
//! machinery: one `AtomicU64` is read and written whole.
//!
//! Do **not** pin a quote-time floor across a stream and reuse it to accept
//! that stream's later vouchers. The hard per-byte floor enforced at voucher
//! acceptance (`handlers/client/voucher.rs`) mirrors the on-chain
//! `PaymentChannel._advanceClaimWatermark` check, which reads the **live**
//! `deliveryFloor` at settlement — there is no per-channel floor snapshot on
//! chain. A voucher priced below the live floor is unredeemable
//! (`RateFloorViolation`), so the acceptance check must read the live floor too;
//! pinning the quote-time floor would make the node countersign vouchers it
//! cannot redeem (see #1382 / #1388). Honouring the buyer across a governance
//! floor raise is a separate concern — it needs a graceful re-quote signal, not
//! a stale floor.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Cloneable live delivery-rate floor. All clones share one atomic cell.
#[derive(Clone, Debug)]
pub struct RateBounds {
    floor: Arc<AtomicU64>,
}

impl RateBounds {
    /// Seed the floor, typically from the startup `getRateBounds()` read.
    #[must_use]
    pub fn new(floor: u64) -> Self {
        Self {
            floor: Arc::new(AtomicU64::new(floor)),
        }
    }

    /// Current floor (minimum per-MB rate a voucher may pay).
    #[must_use]
    pub fn floor(&self) -> u64 {
        self.floor.load(Ordering::Relaxed)
    }

    /// Raise `rate` to the current floor, returning `(clamped, floor)`.
    ///
    /// Both values come from **one** load: every caller needs the floor as well
    /// as the result — to log which floor produced the decision — and a second
    /// `floor()` read could report a value a concurrent governance update had
    /// already replaced, i.e. a floor that never produced this clamp. Returning
    /// the pair is what stops the signing paths open-coding `raw.max(floor())`
    /// and drifting from each other.
    #[must_use]
    pub fn raise_to_floor(&self, rate: u64) -> (u64, u64) {
        let floor = self.floor();
        (rate.max(floor), floor)
    }

    /// Publish a new floor — called by the rate-bounds watcher on a
    /// `RateBoundsUpdated` event and by the periodic authoritative re-read.
    pub fn store(&self, floor: u64) {
        self.floor.store(floor, Ordering::Relaxed);
    }
}

/// Narrow the on-chain `uint256` delivery floor to the `u64` the clamp uses,
/// rejecting anything the node could not honour.
///
/// Two arms, both fail-closed, both unreachable against a correctly-deployed
/// `PaymentChannel` (which caps the floor at `MAX_RATE_PER_MB` itself) — so
/// reaching either means the deployment predates that cap, and the only remedy
/// is a governance `setRateBounds`. Lives here as a free function rather than
/// inline in bring-up so both arms are reachable from a unit test; inline they
/// sat behind an async chain read that no fixture could drive to a bad value.
///
/// `addr` appears in both messages because an operator seeing this cannot fix it
/// locally and needs to know which contract to point governance at.
pub fn on_chain_floor_to_u64(raw: alloy::primitives::U256, addr: &str) -> anyhow::Result<u64> {
    let floor = u64::try_from(raw).map_err(|_| {
        anyhow::anyhow!(
            "on-chain delivery floor {raw} at {addr} exceeds u64::MAX; refusing to start — \
             governance must call setRateBounds with a value <= MAX_RATE_PER_MB ({})",
            decdn_protocol::MAX_RATE_PER_MB
        )
    })?;
    anyhow::ensure!(
        floor <= decdn_protocol::MAX_RATE_PER_MB,
        "on-chain delivery floor {floor} at {addr} exceeds the wire cap MAX_RATE_PER_MB ({}); \
         every quote would be raised above what the wire schema carries, so no peer could \
         decode this node's responses. Refusing to start — governance must lower the floor.",
        decdn_protocol::MAX_RATE_PER_MB
    );
    Ok(floor)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;

    use super::*;

    const ADDR: &str = "0xabc";

    #[test]
    fn on_chain_floor_accepts_up_to_the_wire_cap() -> anyhow::Result<()> {
        assert_eq!(on_chain_floor_to_u64(U256::from(1u64), ADDR)?, 1);
        let cap = decdn_protocol::MAX_RATE_PER_MB;
        assert_eq!(
            on_chain_floor_to_u64(U256::from(cap), ADDR)?,
            cap,
            "the cap itself must be accepted — the contract guard is `>`, not `>=`"
        );
        Ok(())
    }

    #[test]
    fn on_chain_floor_rejects_above_the_wire_cap() -> anyhow::Result<()> {
        let over = U256::from(decdn_protocol::MAX_RATE_PER_MB + 1);
        let err = on_chain_floor_to_u64(over, ADDR)
            .err()
            .ok_or_else(|| anyhow::anyhow!("a floor above the wire cap must refuse startup"))?
            .to_string();
        assert!(err.contains(ADDR), "must name the contract: {err}");
        assert!(err.contains("MAX_RATE_PER_MB"), "must name the cap: {err}");
        assert!(err.contains("governance"), "must name the remedy: {err}");
        Ok(())
    }

    /// The `u64::try_from` arm. Distinct from the cap arm above: a value beyond
    /// `u64::MAX` cannot even be narrowed, so it must be caught before the
    /// comparison rather than wrapping into an in-range floor.
    #[test]
    fn on_chain_floor_rejects_above_u64_max() -> anyhow::Result<()> {
        let over = U256::from(u64::MAX) + U256::from(1u64);
        let err = on_chain_floor_to_u64(over, ADDR)
            .err()
            .ok_or_else(|| anyhow::anyhow!("a floor beyond u64::MAX must refuse startup"))?
            .to_string();
        assert!(
            err.contains("u64::MAX"),
            "must say why it could not narrow: {err}"
        );
        assert!(err.contains(ADDR), "must name the contract: {err}");
        Ok(())
    }

    #[test]
    fn raise_to_floor_lifts_rate_and_reports_the_floor_it_used() {
        let b = RateBounds::new(10);
        assert_eq!(b.raise_to_floor(5), (10, 10), "below floor lifts up");
        assert_eq!(
            b.raise_to_floor(50),
            (50, 10),
            "at or above floor unchanged"
        );
        assert_eq!(
            b.raise_to_floor(u64::MAX),
            (u64::MAX, 10),
            "no upper bound to clamp down to"
        );
    }

    #[test]
    fn store_updates_are_visible_through_all_clones() {
        let b = RateBounds::new(0);
        let clone = b.clone();
        b.store(25);
        // A clone shares the same cell, so the update through `b` is visible.
        assert_eq!(clone.floor(), 25);
        assert_eq!(clone.raise_to_floor(10), (25, 25));
        assert_eq!(clone.raise_to_floor(90), (90, 25));
    }

    /// `store` must write the newest value, not the largest. A `fetch_max`
    /// implementation would pass every other test here while latching the floor
    /// at a high-water mark, so a governance *reduction* would never reach the
    /// signing path — the exact bug `g_gov_02_rate_bounds` step 8 exists to
    /// catch end-to-end, pinned here as an instant unit assertion instead of a
    /// 60-second anvil poll.
    #[test]
    fn store_lowers_the_floor_as_well_as_raising_it() {
        let b = RateBounds::new(50);
        assert_eq!(b.raise_to_floor(10), (50, 50));
        b.store(1);
        assert_eq!(b.floor(), 1, "a lowered floor must replace, not max()");
        assert_eq!(
            b.raise_to_floor(10),
            (10, 1),
            "the configured rate governs again once the floor drops below it"
        );
    }
}
