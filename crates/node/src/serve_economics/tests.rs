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
