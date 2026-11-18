use alloy::primitives::Address;

use super::*;

const A: &str = "0x00000000000000000000000000000000000000aa";
const B: &str = "0x00000000000000000000000000000000000000bb";

fn select(pool: Option<&str>, rpc: Option<&str>) -> cli::PoolSelectArgs {
    cli::PoolSelectArgs {
        pool: pool.map(str::to_owned),
        pool_rpc_url: rpc.map(str::to_owned),
    }
}

fn chains(
    entries: &[(Option<&'static str>, Option<u64>, Vec<String>)],
) -> Vec<ConfiguredChainOwned> {
    entries
        .iter()
        .map(|(rpc, id, pools)| ConfiguredChainOwned {
            rpc: *rpc,
            id: *id,
            pools: pools.clone(),
        })
        .collect()
}

struct ConfiguredChainOwned {
    rpc: Option<&'static str>,
    id: Option<u64>,
    pools: Vec<String>,
}

fn view(owned: &[ConfiguredChainOwned]) -> Vec<ConfiguredChain<'_>> {
    owned
        .iter()
        .map(|c| ConfiguredChain {
            rpc_url: c.rpc,
            chain_id: c.id,
            payment_pools: &c.pools,
        })
        .collect()
}

/// Without `--pool`, every configured pool, in order, each with its chain's
/// RPC; an entry with no chain id is on the governance chain.
#[test]
fn every_configured_pool_in_order() {
    let owned = chains(&[
        (Some("http://a"), Some(421_614), vec![A.into(), B.into()]),
        (Some("http://b"), None, vec![A.into()]),
    ]);
    let pools = resolve_pools(&select(None, None), view(&owned), 11_155_111).unwrap();
    let got: Vec<_> = pools
        .iter()
        .map(|p| {
            (
                p.deployment.chain_id,
                p.deployment.payment_pool,
                p.rpc_url.as_str(),
            )
        })
        .collect();
    let a: Address = A.parse().unwrap();
    let b: Address = B.parse().unwrap();
    assert_eq!(
        got,
        vec![
            (421_614, a, "http://a"),
            (421_614, b, "http://a"),
            (11_155_111, a, "http://b"),
        ]
    );
}

/// `--pool` picks one pool; its RPC comes from the flag, else the entry that
/// lists it, else an entry on the same chain.
#[test]
fn pool_flag_picks_one_and_finds_its_rpc() {
    let owned = chains(&[(Some("http://a"), Some(421_614), vec![A.into()])]);
    let listed = format!("eip155:421614:{B}");
    let pools = resolve_pools(&select(Some(&listed), None), view(&owned), 1).unwrap();
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0].rpc_url, "http://a", "same chain's endpoint");
    assert_eq!(
        pools[0].deployment.payment_pool,
        B.parse::<Address>().unwrap()
    );

    let pools =
        resolve_pools(&select(Some(&listed), Some("http://flag")), view(&owned), 1).unwrap();
    assert_eq!(pools[0].rpc_url, "http://flag", "the flag wins");

    let elsewhere = format!("eip155:10:{A}");
    let err = resolve_pools(&select(Some(&elsewhere), None), view(&owned), 1).unwrap_err();
    assert!(err.to_string().contains("--pool-rpc-url"), "{err}");
}

/// A malformed or zero `--pool` and an empty configuration are errors.
#[test]
fn bad_pool_and_no_pool_are_errors() {
    let none = chains(&[]);
    assert!(resolve_pools(&select(Some(A), Some("http://x")), view(&none), 1).is_err());
    let zero = "eip155:1:0x0000000000000000000000000000000000000000";
    assert!(resolve_pools(&select(Some(zero), Some("http://x")), view(&none), 1).is_err());
    let err = resolve_pools(&select(None, None), view(&none), 1).unwrap_err();
    assert!(
        err.to_string().contains("no payment pool configured"),
        "{err}"
    );
}
