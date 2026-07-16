//! Reporter-credibility weighting from on-chain settlement value (ADR 008
//! §Network Score Aggregation, §Distinct-Counterparty Discount, §Settled-Value
//! Time Decay).
//!
//! The arithmetic here is pure and chain-free so it is unit-testable without a
//! node: the [`SettlementSource`] trait (implemented in the `node` crate over
//! the incentive store + chain) yields raw [`SettlementRecord`]s, and the
//! functions in this module fold them into a reporter weight.

use iroh::PublicKey as NodeId;

/// Exponential settlement-decay rate per week (ADR 008: `lambda = 0.1`,
/// half-life ≈ 6.9 weeks).
pub(crate) const SETTLEMENT_DECAY_LAMBDA: f64 = 0.1;
/// Settlements older than this contribute `0` (ADR 008: 52 weeks).
pub(crate) const SETTLEMENT_MAX_AGE_WEEKS: f64 = 52.0;
/// Seconds in a week, for age conversion.
pub(crate) const SECONDS_PER_WEEK: f64 = 7.0 * 24.0 * 3600.0;
/// Seconds in a week as an integer, for tests working in `u64`.
#[cfg(test)]
pub(crate) const SECONDS_PER_WEEK_U64: u64 = 7 * 24 * 3600;
/// Default distinct-counterparty target for full diversity credit (ADR 008).
pub(crate) const DEFAULT_MIN_COUNTERPARTIES: u32 = 5;
/// Hard floor for `min_counterparties` (ADR 008 governance bound).
pub(crate) const MIN_COUNTERPARTIES_FLOOR: u32 = 2;
/// Maximum `reporter_weight` value, bounding the EWMA alpha multiplier (ADR
/// 008: `weight_cap = 3.0`).
pub(crate) const WEIGHT_CAP: f64 = 3.0;

/// One on-chain settlement attributed to a reporter (as client OR provider).
///
/// `amount_usdc` is in USDC base units (6-decimal). `age_secs` is
/// `now - settled_at` measured by the trait impl at query time, keeping this
/// crate clock-free and deterministic in tests. `staked_counterparty` is the
/// counterparty's Ethereum address iff the [`SettlementSource`] deemed it a
/// staked `CapacityBond` node (ADR 008 §Counterparty validation); `None`
/// excludes it from the distinct-counterparty count. How "staked" is resolved
/// (current vs at-settlement membership) is the source's concern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementRecord {
    /// Settlement amount in USDC base units.
    pub amount_usdc: u128,
    /// Age of the settlement in seconds at query time.
    pub age_secs: u64,
    /// Counterparty address if the source deemed it a staked node (see the
    /// type-level note on how membership is resolved).
    pub staked_counterparty: Option<[u8; 20]>,
}

/// Source of a reporter's settlement history (ADR 008 §Network Score
/// Aggregation). Implemented in the `node` crate over the incentive store and
/// chain bindings. Fail-closed: an unknown reporter yields an empty `Vec`,
/// producing `effective_settled_value = 0` and therefore `reporter_weight = 0`.
pub trait SettlementSource: Send + Sync {
    /// All of `reporter`'s settled channels within the 52-week window. Returns
    /// empty when the reporter `NodeId` has no known operator binding.
    fn settlements(&self, reporter: NodeId) -> Vec<SettlementRecord>;

    /// The node-local maximum observed effective settled value — the
    /// denominator in `reporter_weight`. It is local to each node and drifts
    /// down as values decay, so the impl recomputes it periodically / on each
    /// `ChannelSettled` (ADR 008 §Network Score Aggregation). Callers divide by
    /// `max(1.0, this)`.
    fn max_effective_settled_value(&self) -> f64;
}

// NB: the staked-reporter gate (ADR 008 §Gossip Protocol — reports accepted
// only from staked nodes) is enforced at the gossip layer via
// `decdn_gossip::StakedNodeSet` (implemented in `node` over the chain
// staker set), so this crate intentionally does not define a parallel trait.

/// `min(distinct_counterparties / min_counterparties, 1.0)` (ADR 008
/// §Distinct-Counterparty Discount). `min_counterparties` is clamped to the
/// hardcoded floor.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn diversity_factor(distinct: u32, min_counterparties: u32) -> f64 {
    let min = min_counterparties.max(MIN_COUNTERPARTIES_FLOOR);
    (f64::from(distinct) / f64::from(min)).clamp(0.0, 1.0)
}

/// `sum(amount_i * exp(-lambda * age_weeks_i)) * diversity_factor` over
/// settlements within the 52-week window (ADR 008 §Settled-Value Time Decay).
///
/// `distinct_counterparties` counts unique staked counterparty addresses within
/// the window; unstaked / `None` counterparties do not count.
// u128→f64 loses precision above 2^53 USDC base units (~9 billion USDC) — far
// above any plausible single settlement; the downstream weight is capped at 3×
// regardless, so drift cannot escape the bounded envelope.
#[allow(clippy::cast_precision_loss)]
pub fn effective_settled_value(records: &[SettlementRecord], min_counterparties: u32) -> f64 {
    let mut distinct: std::collections::HashSet<[u8; 20]> = std::collections::HashSet::new();
    let mut decayed_sum = 0.0_f64;
    for r in records {
        let age_weeks = r.age_secs as f64 / SECONDS_PER_WEEK;
        if age_weeks >= SETTLEMENT_MAX_AGE_WEEKS {
            continue;
        }
        decayed_sum += r.amount_usdc as f64 * (-SETTLEMENT_DECAY_LAMBDA * age_weeks).exp();
        if let Some(addr) = r.staked_counterparty {
            distinct.insert(addr);
        }
    }
    let distinct_count = u32::try_from(distinct.len()).unwrap_or(u32::MAX);
    decayed_sum * diversity_factor(distinct_count, min_counterparties)
}

/// `min(effective / max(1, max_observed), weight_cap)` (ADR 008 §Network Score
/// Aggregation). The `max(1, ..)` guard prevents division by zero at bootstrap.
pub(crate) fn reporter_weight(effective: f64, max_observed: f64) -> f64 {
    if !effective.is_finite() || effective <= 0.0 {
        return 0.0;
    }
    let denom = max_observed.max(1.0);
    (effective / denom).min(WEIGHT_CAP)
}

/// Compute a reporter's credibility weight from a [`SettlementSource`] (ADR 008
/// §Network Score Aggregation). The public entry point used by the node-side
/// aggregator: applies the distinct-counterparty discount and time decay, then
/// normalizes against the node-local max observed value and caps at 3×. Returns
/// `0.0` for reporters with no known settlement history (fail-closed).
pub fn compute_reporter_weight(
    source: &dyn SettlementSource,
    reporter: NodeId,
    min_counterparties: u32,
) -> f64 {
    let records = source.settlements(reporter);
    let effective = effective_settled_value(&records, min_counterparties);
    reporter_weight(effective, source.max_effective_settled_value())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    fn addr(n: u8) -> [u8; 20] {
        [n; 20]
    }

    fn rec(amount: u128, age_weeks: f64, cp: Option<[u8; 20]>) -> SettlementRecord {
        SettlementRecord {
            amount_usdc: amount,
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            age_secs: (age_weeks * SECONDS_PER_WEEK) as u64,
            staked_counterparty: cp,
        }
    }

    #[test]
    fn diversity_one_counterparty_is_one_fifth() {
        assert!(approx(diversity_factor(1, 5), 0.2));
    }

    #[test]
    fn diversity_five_or_more_is_full() {
        assert!(approx(diversity_factor(5, 5), 1.0));
        assert!(approx(diversity_factor(9, 5), 1.0));
    }

    #[test]
    fn diversity_respects_floor() {
        // min below the floor of 2 is raised to 2: 1/2 = 0.5.
        assert!(approx(diversity_factor(1, 1), 0.5));
        assert!(approx(diversity_factor(1, 0), 0.5));
    }

    #[test]
    fn decay_table_matches_adr() {
        // One settlement of 1.0 unit with 5 distinct counterparties → full
        // diversity, isolating the time-decay factor.
        let cps: Vec<[u8; 20]> = (0..5).map(addr).collect();
        let check = |age_weeks: f64, retained: f64| {
            // First counterparty carries amount 1; the rest carry 0 (present
            // only to supply full diversity so the time-decay factor is isolated).
            let records: Vec<SettlementRecord> = cps
                .iter()
                .enumerate()
                .map(|(i, c)| rec(u128::from(i == 0), age_weeks, Some(*c)))
                .collect();
            let eff = effective_settled_value(&records, 5);
            assert!(
                (eff - retained).abs() < 0.02,
                "age {age_weeks}: {eff} vs {retained}"
            );
        };
        check(1.0, 0.9048);
        check(7.0, 0.4966);
        check(20.0, 0.1353);
    }

    #[test]
    fn settlement_past_max_age_is_excluded() {
        let cps: Vec<[u8; 20]> = (0..5).map(addr).collect();
        // First counterparty's settlement is 60 weeks old (excluded); the other
        // four are fresh.
        let records: Vec<SettlementRecord> = cps
            .iter()
            .enumerate()
            .map(|(i, c)| rec(1_000, if i == 0 { 60.0 } else { 1.0 }, Some(*c)))
            .collect();
        // The 60-week record drops out; only the four fresh ones remain, but
        // their amount sum and diversity still yield a positive value.
        let eff = effective_settled_value(&records, 5);
        // 4 fresh records (amount 1000, age 1wk ≈ 0.9048) × diversity 4/5=0.8.
        let expected = 4.0 * 1000.0 * 0.9048_f64 * 0.8;
        assert!(
            (eff - expected).abs() < 5.0,
            "got {eff}, expected ~{expected}"
        );
    }

    #[test]
    fn wash_trade_single_counterparty_discounted_80pct() {
        // 10 settlements all with the same counterparty → diversity 0.2.
        let cp = addr(1);
        let records: Vec<SettlementRecord> = (0..10).map(|_| rec(100, 0.0, Some(cp))).collect();
        let eff = effective_settled_value(&records, 5);
        // sum = 10*100*1.0 = 1000; diversity 1/5 = 0.2 → 200.
        assert!(approx(eff, 200.0), "got {eff}");
    }

    #[test]
    fn unstaked_counterparties_do_not_count_for_diversity() {
        // 5 settlements but counterparties unstaked → distinct = 0 → factor 0.
        let records: Vec<SettlementRecord> = (0..5).map(|_| rec(100, 0.0, None)).collect();
        assert!(approx(effective_settled_value(&records, 5), 0.0));
    }

    #[test]
    fn reporter_weight_caps_at_three() {
        assert!(approx(reporter_weight(100.0, 10.0), 3.0));
    }

    #[test]
    fn reporter_weight_proportional_below_cap() {
        assert!(approx(reporter_weight(5.0, 10.0), 0.5));
    }

    #[test]
    fn reporter_weight_bootstrap_guard() {
        // max_observed below 1 is treated as 1 (no division blow-up).
        assert!(approx(reporter_weight(0.5, 0.0), 0.5));
    }

    #[test]
    fn reporter_weight_zero_for_no_value() {
        assert!(approx(reporter_weight(0.0, 100.0), 0.0));
        assert!(approx(reporter_weight(f64::NAN, 100.0), 0.0));
    }
}
