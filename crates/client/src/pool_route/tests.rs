use super::*;

fn dep(chain_id: u64, last: u8) -> Deployment {
    Deployment {
        chain_id,
        payment_pool: Address::with_last_byte(last),
    }
}

fn provider(last: u8) -> Address {
    Address::repeat_byte(last)
}

fn hint(deployments: &[Deployment]) -> Vec<PoolAccount> {
    deployments.iter().map(|d| d.pool_account()).collect()
}

const A: (u64, u8) = (42_161, 0xa);
const B: (u64, u8) = (8_453, 0xb);
const C: (u64, u8) = (10, 0xc);

fn a() -> Deployment {
    dep(A.0, A.1)
}
fn b() -> Deployment {
    dep(B.0, B.1)
}
fn c() -> Deployment {
    dep(C.0, C.1)
}

#[test]
fn held_pools_lead_the_route_in_configured_order() {
    let route = PoolRoute::new(&[a(), b(), c()], |d| d == c() || d == b())
        .unwrap_or_else(|| PoolRoute::single(a()));
    assert_eq!(route.order(), &[b(), c(), a()]);
}

#[test]
fn an_empty_configuration_has_no_route() {
    assert!(PoolRoute::new(&[], |_| true).is_none());
}

#[test]
fn a_node_with_no_hint_gets_the_first_configured_pool() {
    let route = PoolRoute::new(&[a(), b()], |d| d == b()).unwrap_or_else(|| PoolRoute::single(a()));
    assert_eq!(route.order().first(), Some(&b()));
    assert_eq!(route.choose(None), Some(a()));
}

#[test]
fn a_hint_picks_the_first_route_pool_it_lists() {
    let route =
        PoolRoute::new(&[a(), b(), c()], |d| d == c()).unwrap_or_else(|| PoolRoute::single(a()));
    // The route is [c, a, b]: a held pool wins over configured order.
    assert_eq!(route.choose(Some(&hint(&[b(), c()]))), Some(c()));
    assert_eq!(route.choose(Some(&hint(&[b(), a()]))), Some(a()));
    assert_eq!(route.choose(Some(&hint(&[b()]))), Some(b()));
}

#[test]
fn a_hint_compares_addresses_without_regard_to_case() -> anyhow::Result<()> {
    let route = PoolRoute::single(dep(1, 0xab));
    let upper = PoolAccount::new("eip155:1:0x00000000000000000000000000000000000000AB")?;
    assert_eq!(route.choose(Some(&[upper])), Some(dep(1, 0xab)));
    Ok(())
}

#[test]
fn a_hint_with_no_common_pool_routes_nowhere() {
    let route = PoolRoute::new(&[a(), b()], |_| false).unwrap_or_else(|| PoolRoute::single(a()));
    assert_eq!(route.choose(Some(&hint(&[c()]))), None);
    assert_eq!(route.choose(Some(&[])), None);
    // Same contract address on another chain is another pool.
    assert_eq!(route.choose(Some(&hint(&[dep(1, A.1)]))), None);
}

#[test]
fn retry_skips_the_refused_pool() {
    let route =
        PoolRoute::new(&[a(), b(), c()], |_| false).unwrap_or_else(|| PoolRoute::single(a()));
    assert_eq!(route.retry(a(), &hint(&[a(), c()])), Some(c()));
    assert_eq!(route.retry(a(), &hint(&[a()])), None);
}

/// A discovery open names the route's most preferred contract: a held one
/// ahead of the configured order.
#[test]
fn the_route_names_its_most_preferred_contract_first() {
    let plain = PoolRoute::new(&[a(), b()], |_| false).unwrap_or_else(|| PoolRoute::single(c()));
    assert_eq!(plain.first(), a());
    let held_b =
        PoolRoute::new(&[a(), b()], |d| d == b()).unwrap_or_else(|| PoolRoute::single(c()));
    assert_eq!(held_b.first(), b());
    assert_eq!(PoolRoute::single(c()).first(), c());
}

fn passes(configured: &[Deployment]) -> PoolPasses {
    PoolPasses::new(PoolRoute::new(configured, |_| false).unwrap_or_else(|| PoolRoute::single(a())))
}

#[test]
fn the_first_pass_is_the_first_route_pool_a_holder_is_routed_to() {
    let passes = passes(&[a(), b()]);
    let only_b = hint(&[b()]);
    assert_eq!(passes.first([Some(only_b.as_slice())]), b());
    assert_eq!(passes.first([Some(only_b.as_slice()), None]), a());
    assert_eq!(passes.first(std::iter::empty()), a());
}

#[test]
fn each_pass_admits_only_the_nodes_routed_to_it() {
    let passes = passes(&[a(), b()]);
    let on_a = hint(&[a()]);
    let on_b = hint(&[b()]);
    passes.start(a());
    assert!(passes.admits(a(), provider(1), Some(&on_a)));
    assert!(passes.admits(a(), provider(2), None));
    assert!(!passes.admits(a(), provider(3), Some(&on_b)));
    assert!(!passes.admits(a(), provider(4), Some(&hint(&[c()]))));
    assert_eq!(passes.next(), Some(b()));
    passes.start(b());
    assert!(passes.admits(b(), provider(3), Some(&on_b)));
    assert!(!passes.admits(b(), provider(1), Some(&on_a)));
    assert_eq!(passes.next(), None);
}

#[test]
fn a_rediscovered_node_stays_admitted_in_its_pass() {
    let passes = passes(&[a()]);
    passes.start(a());
    assert!(passes.admits(a(), provider(1), None));
    assert!(passes.admits(a(), provider(1), None));
}

#[test]
fn a_declined_node_is_retried_once_on_a_listed_pool_then_dropped() {
    let passes = passes(&[a(), b(), c()]);
    passes.start(a());
    assert!(passes.admits(a(), provider(1), None));
    passes.declined(a(), provider(1), &hint(&[b(), c()]));
    // Dropped from its pass, kept for the retry.
    assert!(!passes.admits(a(), provider(1), None));
    assert_eq!(passes.next(), Some(b()));
    passes.start(b());
    assert!(passes.admits(b(), provider(1), None));
    // It refuses the retry too: no second retry, and no pass is left for it.
    passes.declined(b(), provider(1), &hint(&[c()]));
    assert!(!passes.admits(b(), provider(1), None));
    assert_eq!(passes.next(), None);
    assert!(!passes.admits(c(), provider(1), Some(&hint(&[c()]))));
}

#[test]
fn a_decline_naming_no_pool_the_client_holds_drops_the_node() {
    let passes = passes(&[a(), b()]);
    passes.start(a());
    assert!(passes.admits(a(), provider(1), None));
    passes.declined(a(), provider(1), &hint(&[c()]));
    assert_eq!(passes.next(), None);
    passes.start(b());
    assert!(!passes.admits(b(), provider(1), None));
}

#[test]
fn a_decline_naming_only_a_started_pass_drops_the_node() {
    let passes = passes(&[a(), b()]);
    passes.start(a());
    passes.start(b());
    assert!(passes.admits(b(), provider(1), Some(&hint(&[b()]))));
    passes.declined(b(), provider(1), &hint(&[a()]));
    assert_eq!(passes.next(), None);
}

#[test]
fn the_passes_end_after_every_pool_ran_once() {
    let passes = passes(&[a(), b()]);
    let mut ran = Vec::new();
    let mut pass = Some(passes.first([Some(hint(&[a()]).as_slice())]));
    while let Some(deployment) = pass {
        assert!(!ran.contains(&deployment), "a pool ran twice");
        ran.push(deployment);
        passes.start(deployment);
        // Every node refuses every pass and names every pool.
        for node in 1..=3 {
            if passes.admits(deployment, provider(node), Some(&hint(&[a(), b()]))) {
                passes.declined(deployment, provider(node), &hint(&[a(), b()]));
            }
        }
        pass = passes.next();
    }
    assert_eq!(ran, vec![a(), b()]);
}
