//! Buy-side profitability gate for cache-miss relay legs (ADR 041).
//!
//! Node-local, config-selectable. Sibling to the ADR-040 cache policies, not part
//! of them: cache policy decides what a node keeps; this decides what a node buys
//! to serve. The policy returns a per-MB buy ceiling; the `node_origin` buy loop does
//! the candidate scan and the refusal.

use std::sync::Arc;

use decdn_common::config::resolved::ResolvedServeEconomics;

const BPS_DENOMINATOR: u64 = 10_000;

/// Inputs to the buy ceiling, gathered per cache-miss.
#[derive(Debug, Clone, Copy)]
pub struct ServeEconomicsCtx {
    /// The node's own sell rate, per MB, already raised to the on-chain floor.
    pub sell_rate_per_mb: u64,
    /// Operator basis points from `FeeRouter.getShares()[0]`; `(1 - f)` numerator.
    pub operator_bps: u16,
    /// ADR-040 frequency estimate for the hash; 0 = cold.
    pub heat_estimate: u32,
    /// The source node still has warming allowance: buy at market to warm.
    /// False once the source's allowance is spent: buy only at the amortized floor.
    pub warming_available: bool,
}

/// A node-local decision on the most the node pays upstream, per MB.
pub trait ServeEconomicsPolicy: Send + Sync + std::fmt::Debug {
    /// `None` = no economic ceiling (only the static ceiling applies).
    /// `Some(v)` = a real ceiling; `v` may be 0, refusing every paid candidate.
    fn max_buy_per_mb(&self, ctx: &ServeEconomicsCtx) -> Option<u64>;
}

/// Disables the economic gate; only `cache.max_rate_per_mb` bounds the buy.
#[derive(Debug)]
pub struct OffPolicy;

impl ServeEconomicsPolicy for OffPolicy {
    fn max_buy_per_mb(&self, _ctx: &ServeEconomicsCtx) -> Option<u64> {
        None
    }
}

/// Two-regime ceiling. `amortized = (operator_bps/10_000) · n_hat · sell` with
/// `n_hat = clamp(round(discount · heat), 1, n_max)`. While the source has warming
/// allowance, buy at the market price `max(sell, amortized)` (warming); once spent,
/// buy only at `amortized` (grief-proof). See ADR 041 § The buy ceiling.
#[derive(Debug)]
pub struct MarginPolicy {
    discount_bps: u32,
    n_max: u32,
}

impl MarginPolicy {
    /// `discount_bps` scales heat into the expected re-serve count `n_hat`,
    /// which `n_max` caps.
    #[must_use]
    pub const fn new(discount_bps: u32, n_max: u32) -> Self {
        Self {
            discount_bps,
            n_max,
        }
    }

    fn n_hat(&self, heat_estimate: u32) -> u64 {
        // round(discount · heat) with integer math: (discount_bps·heat + 5000) / 10_000
        let scaled = u64::from(self.discount_bps)
            .saturating_mul(u64::from(heat_estimate))
            .saturating_add(BPS_DENOMINATOR / 2)
            / BPS_DENOMINATOR;
        scaled.clamp(1, u64::from(self.n_max))
    }
}

impl ServeEconomicsPolicy for MarginPolicy {
    fn max_buy_per_mb(&self, ctx: &ServeEconomicsCtx) -> Option<u64> {
        let n_hat = self.n_hat(ctx.heat_estimate);
        let amortized = u64::from(ctx.operator_bps)
            .saturating_mul(n_hat)
            .saturating_mul(ctx.sell_rate_per_mb)
            / BPS_DENOMINATOR;
        // Warm at market while the source has allowance; else the amortized floor.
        Some(if ctx.warming_available {
            ctx.sell_rate_per_mb.max(amortized)
        } else {
            amortized
        })
    }
}

/// Build the configured policy. Unknown names are impossible: config validation
/// restricts `policy` to `"off"` | `"margin"` at load; anything else here
/// falls back to `OffPolicy` (safe: only the static ceiling applies).
#[must_use]
pub fn select_policy(cfg: &ResolvedServeEconomics) -> Arc<dyn ServeEconomicsPolicy> {
    match cfg.policy.as_str() {
        "margin" => Arc::new(MarginPolicy::new(cfg.discount_bps, cfg.n_max)),
        _ => Arc::new(OffPolicy),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(sell: u64, op_bps: u16, heat: u32, warm: bool) -> ServeEconomicsCtx {
        ServeEconomicsCtx {
            sell_rate_per_mb: sell,
            operator_bps: op_bps,
            heat_estimate: heat,
            warming_available: warm,
        }
    }

    #[test]
    fn off_policy_never_bounds() {
        assert_eq!(OffPolicy.max_buy_per_mb(&ctx(1000, 6000, 42, true)), None);
    }

    #[test]
    fn warm_cold_start_buys_at_market() {
        // warm + heat 0 -> n_hat 1 -> amortized 600; max(1000, 600) = 1000 (market)
        let p = MarginPolicy {
            discount_bps: 5000,
            n_max: 64,
        };
        assert_eq!(p.max_buy_per_mb(&ctx(1000, 6000, 0, true)), Some(1000));
    }

    #[test]
    fn spent_allowance_cold_drops_to_amortized() {
        // not warm + heat 0 -> amortized 600 (grief-proof floor)
        let p = MarginPolicy {
            discount_bps: 5000,
            n_max: 64,
        };
        assert_eq!(p.max_buy_per_mb(&ctx(1000, 6000, 0, false)), Some(600));
    }

    #[test]
    fn amortized_premium_overtakes_market_when_hot() {
        let p = MarginPolicy {
            discount_bps: 10_000,
            n_max: 4,
        }; // discount 1.0
        // heat 2 -> amortized 1200 > market 1000; both regimes coincide at 1200
        assert_eq!(p.max_buy_per_mb(&ctx(1000, 6000, 2, true)), Some(1200));
        assert_eq!(p.max_buy_per_mb(&ctx(1000, 6000, 2, false)), Some(1200));
        // clamp at n_max
        assert_eq!(p.max_buy_per_mb(&ctx(1000, 6000, 99, true)), Some(2400));
    }

    #[test]
    fn discount_derates_and_rounds() {
        // discount 0.5, heat 3 -> round(1.5) = 2 -> amortized 1200
        let p = MarginPolicy {
            discount_bps: 5000,
            n_max: 64,
        };
        assert_eq!(p.max_buy_per_mb(&ctx(1000, 6000, 3, false)), Some(1200));
        // heat 1 -> n_hat floored to 1 -> amortized 600
        assert_eq!(p.max_buy_per_mb(&ctx(1000, 6000, 1, false)), Some(600));
    }

    #[test]
    fn saturates_without_panic_on_huge_inputs() {
        let p = MarginPolicy {
            discount_bps: 10_000,
            n_max: u32::MAX,
        };
        let _ = p.max_buy_per_mb(&ctx(u64::MAX, 10_000, u32::MAX, true));
    }

    #[test]
    fn select_policy_maps_names() {
        use decdn_common::config::resolved::ResolvedServeEconomics;
        let r = |policy: &str| ResolvedServeEconomics {
            policy: policy.into(),
            discount_bps: 5000,
            n_max: 64,
            warming_budget: 1000,
            warming_refill: 1,
        };
        assert_eq!(
            select_policy(&r("off")).max_buy_per_mb(&ctx(1000, 6000, 9, true)),
            None
        );
        assert!(
            select_policy(&r("margin"))
                .max_buy_per_mb(&ctx(1000, 6000, 0, true))
                .is_some()
        );
    }
}
